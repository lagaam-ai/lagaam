"""Spot queries whose row work no plan estimate can see.

The budget prices a query from Trino's own plan: the widest row count any
operator would build (see adapters/trino/plan.py). That covers every shape
where rows multiply through joins — products, low-cardinality keys,
correlated nested loops — because the planner estimates all of them.

One shape it cannot see is a row generator. UNNEST(sequence(1, 10000))
manufactures rows from an argument rather than from a table, and
contributes no operator the planner sizes: measured on Trino 476, such a
query over a 60,175-row table plans as 60,175 rows and produces 601
million. That is what this module detects, and all it detects.

table_scan_counts() serves a different gate: the IO plan reports one entry
per table however often the query reads it, so a self-join's *bytes* are
undercounted even though its rows are not.
"""

from collections.abc import Mapping
from datetime import date

import sqlglot
from sqlglot import exp

_GENERATORS = (exp.Unnest, exp.Explode, exp.Posexplode)

# Interval units with a fixed number of days, so a sequence() over them has a
# length arithmetic can find. MONTH and YEAR vary in length and are excluded.
_FIXED_INTERVAL_DAYS = {"DAY": 1, "WEEK": 7}

# A chain of CTEs each referenced twice doubles table_scan_counts' walk work
# per level: 24 links is a ~1,700-char request and 8M walk steps. Real queries
# measure in the dozens of references at most, so this leaves orders of
# magnitude of headroom while still bounding the walk to milliseconds.
_MAX_SCAN_COUNT = 10_000

# A relation an agent spells out inline is a lookup table, not a multiplier.
# Past this many rows it stops being one: crossing a 1.5M-row scan against
# 20,000 inline values is 30 billion rows the plan prices as one scan.
_MAX_INLINE_ROWS = 1000

# Counting stops here. A product this large is already past anything the row
# budget would clear against a real table, and past what a table-less query
# may invent, so the exact figure beyond it buys nothing.
_MAX_COUNTED_ROWS = 10_000_000

# Aliases resolve through one another; a chain this long is not analytics, and
# following it unbounded exhausts the interpreter's stack on 8 KB of SQL.
_MAX_ALIAS_DEPTH = 32

# Functions that reshape an array without changing how many rows it yields.
# Anything else feeding a generator is assumed to invent rows: a denylist is
# unsound here, because a function sqlglot does not model natively parses as
# Anonymous and would simply be invisible.
_ROW_PRESERVING_FUNCS = {
    "array_sort",
    "array_distinct",
    "reverse",
    "shuffle",
    # Trino's slice() parses as ArraySlice, whose sql_name is array_slice.
    "array_slice",
    "trim_array",
    "filter",
    "transform",
    "cast",
    "try_cast",
    "coalesce",
    "if",
    "nullif",
    # MAP(ARRAY[...], ARRAY[...]) is a lookup table written inline; its rows
    # are its key array's length, which the bounded-input check already caps.
    "map",
    "map_from_entries",
}


def _func_name(func: exp.Func) -> str:
    """The function's source name.

    Anonymous holds it in .name and its sql_name() is the useless literal
    "ANONYMOUS"; every other node holds its name in sql_name() while .name
    returns an *argument's* name — CAST(o.items AS ...) reports "items".
    """
    if isinstance(func, exp.Anonymous):
        return str(func.name or "").lower()
    return str(func.sql_name() or "").lower()


# Arguments that carry no rows of their own: a lambda is applied per element
# (confirmed on Trino 476 — transform(a, x -> sequence(1,10)) nests, it does
# not lengthen), and a datatype, a constant or a condition describes the
# reshaping rather than feeds it.
_NOT_A_ROW_SOURCE = (
    exp.Lambda,
    exp.DataType,
    exp.Literal,
    exp.Boolean,
    exp.Null,
    exp.Predicate,
)


def _func_arguments(func: exp.Func) -> list[exp.Expr]:
    """Every expression a function call feeds on.

    Reading .this and .expressions alone misses whole shapes: IF holds its
    branches under "true"/"false", so an unbounded one would go unchecked.
    Anonymous is the exception — its .this is the function's own name.
    """
    skip = {"this"} if isinstance(func, exp.Anonymous) else set()
    arguments: list[exp.Expr] = []
    for key, value in func.args.items():
        if key in skip:
            continue
        for item in value if isinstance(value, list) else [value]:
            if isinstance(item, exp.Expr):
                arguments.append(item)
    return arguments


def _generates_rows(tree: exp.Expr) -> bool:
    """True if the query invents rows the IO plan cannot see.

    UNNEST over a column expands rows of a table already in the plan; UNNEST
    over sequence(...) or repeat(col, 10000) manufactures them from an
    argument. Anything the gate cannot recognise counts as manufacturing:
    the plan prices scans, and a generator is not one.

    Bounded generators multiply where they meet: three 1000-row sequences
    cross-joined are a billion rows, each within the per-generator cap. The
    cap therefore binds their *product*. A generator inside a CTE multiplies
    once per reference to that CTE, not once per node in the tree.

    A generator joined to a table is not refused here for its size: it is a
    multiplier on rows the plan already counted, and generator_fanout() hands
    that multiplier to the caller so the plan's own estimate can carry it.
    What this refuses is a generator whose size cannot be read at all. Each
    UNION branch is judged on its own — branches add rows rather than multiply
    them, so a statement-wide product would overstate ordinary queries.
    """
    bodies = _cte_bodies(tree)
    # A doubling CTE chain re-walks each body once per reference, so the walk
    # grows exponentially in a query that stays under 1.5 KB: the same budget
    # that bounds the byte gate's walk bounds this one.
    budget = [_MAX_SCAN_COUNT]
    projections = _projected_expressions(tree)
    detached = _without_cte_bodies(tree)
    for branch in _union_branches(detached):
        product = _generator_product(branch, bodies, frozenset(), budget, projections)
        if product is None:
            return True
        # A branch that reads no table is only as large as what it invents,
        # and nothing downstream prices that — so the cap binds here.
        if product > _MAX_INLINE_ROWS and not _reads_a_table(
            branch, bodies, frozenset()
        ):
            return True
    return False


def _union_branches(node: exp.Expr) -> list[exp.Expr]:
    """Each arm of a set operation, or the query itself if there is none."""
    if isinstance(node, exp.Union):
        return [*_union_branches(node.this), *_union_branches(node.expression)]
    return [node]


def generator_fanout(sql: str, dialect: str) -> int:
    """How many times a generator multiplies each row the plan counted.

    The plan sizes orders CROSS JOIN UNNEST(sequence(1, 1000)) as the table
    alone — measured on Trino 476, 1,500,000 rows for 1.5 billion produced.
    Returning the multiplier lets the caller scale the plan's own estimate
    instead of the gate guessing what a table it cannot see costs: a spine
    of 744 hours is ordinary against a small table and ruinous against a
    large one, and only the plan knows which this is.

    1 for SQL with no generator, and for one the shape check has already
    refused — a query with no quote needs no multiplier.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return 1
    bodies = _cte_bodies(tree)
    budget = [_MAX_SCAN_COUNT]
    projections = _projected_expressions(tree)
    widest = 1
    for branch in _union_branches(_without_cte_bodies(tree)):
        if not _reads_a_table(branch, bodies, frozenset()):
            continue
        product = _generator_product(branch, bodies, frozenset(), budget, projections)
        if product is None:
            return 1
        widest = max(widest, product)
    return widest


def _reads_a_table(
    node: exp.Expr, bodies: Mapping[str, list[exp.Expr]], pending: frozenset[str]
) -> bool:
    """True if this subtree scans a table, following CTE references into it."""
    for table in node.find_all(exp.Table):
        # A table expression is not always a table: TABLE(sequence(...)) wraps
        # a generator in a node named for the keyword, and reading that as a
        # scan would lend the generator a row count nothing counted.
        if any(table.find_all(exp.GenerateSeries, *_GENERATORS)):
            continue
        parts = [part.name for part in table.parts]
        name = parts[0].lower()
        if len(parts) > 1 or name not in bodies:
            return True
        if name in pending:
            continue
        if any(
            _reads_a_table(body, bodies, pending | {name}) for body in bodies[name]
        ):
            return True
    return False


def _generator_product(
    node: exp.Expr,
    bodies: Mapping[str, list[exp.Expr]],
    pending: frozenset[str],
    budget: list[int],
    projections: Mapping[str, list[exp.Expr]],
) -> int | None:
    """Rows this subtree's generators multiply out to, or None past the cap.

    None also stands for a generator the gate cannot bound at all, and for a
    walk that ran out of budget: all three mean the same thing to the caller
    — no quote — so they share a return.
    """
    if budget[0] <= 0:
        return None
    product = 1
    wrapped: set[int] = set()
    loose: list[exp.GenerateSeries] = []
    references: list[str] = []
    # One walk, charged per node: a body re-read once per reference costs its
    # own size every time, and only counting the reads left that size — which
    # the attacker writes — outside the budget entirely.
    for child in node.walk():
        budget[0] -= 1
        if budget[0] <= 0:
            return None
        if isinstance(child, _GENERATORS):
            if not _expands_a_bounded_value(child, projections):
                return None
            product *= _generator_rows(child, projections)
            if product > _MAX_COUNTED_ROWS:
                return None
            wrapped.update(id(series) for series in child.find_all(exp.GenerateSeries))
        elif isinstance(child, exp.GenerateSeries):
            loose.append(child)
        elif isinstance(child, exp.Table):
            parts = [part.name for part in child.parts]
            name = parts[0].lower()
            if len(parts) == 1 and name in bodies and name not in pending:
                references.append(name)
    # A series in a table position manufactures rows without an UNNEST to
    # wrap it; guarding only the wrapper would leave the same sequence free.
    for series in loose:
        if id(series) in wrapped:
            continue
        length = _sequence_length(series)
        if length is None or length > _MAX_COUNTED_ROWS:
            return None
        product *= length
        if product > _MAX_COUNTED_ROWS:
            return None
    for name in references:
        # A name bound more than once costs whatever its dearest binding
        # costs: scope decides which one a reference reads, and charging the
        # cheapest is what let a decoy body hide a generator.
        widest = 1
        for body in bodies[name]:
            nested = _generator_product(
                body, bodies, pending | {name}, budget, projections
            )
            if nested is None:
                return None
            widest = max(widest, nested)
        product *= widest
        if product > _MAX_COUNTED_ROWS:
            return None
    return product


def _cte_bodies(tree: exp.Expr) -> dict[str, list[exp.Expr]]:
    """Every body each CTE name is bound to, in definition order.

    A nested WITH may re-bind a name the outer query also uses, and which
    body a given reference means is scope's answer, not a dict's. Keying
    flatly let the inner body silently replace the outer one — a decoy that
    hid a generator behind a name still referenced outside it. Keeping every
    binding lets a reader charge for the most expensive one instead of
    guessing, which neither under-counts the decoy shape nor refuses the
    ordinary query that reuses a name like "base" in a nested scope.
    """
    bodies: dict[str, list[exp.Expr]] = {}
    for cte in tree.find_all(exp.CTE):
        bodies.setdefault(cte.alias_or_name.lower(), []).append(cte.this)
    return bodies


def _expands_a_bounded_value(
    generator: exp.Expr,
    projections: Mapping[str, list[exp.Expr]] | None = None,
    seen: frozenset[str] = frozenset(),
) -> bool:
    """True if the generator's input is bounded by something already priced.

    Two bounded shapes: a literal the agent spelled out (UNNEST(ARRAY['O','F'])
    is a two-row lookup table), and a column of a table the IO plan already
    counted, reshaped by functions that cannot change its length.
    """
    inputs = [
        value
        for value in (generator.this, *(generator.expressions or []))
        if value is not None
    ]
    if not inputs:
        return False
    return all(_is_bounded_input(value, projections, seen) for value in inputs)


def _projected_expressions(tree: exp.Expr) -> dict[str, list[exp.Expr]]:
    """Every expression a subquery or CTE binds a name to, keyed by source.

    A generator fed by a column is priced as the table's own rows, which is
    right for a scanned column and wrong for one a projection built:
    repeat(k, 1000000) AS arr is a column by the time UNNEST reads it.

    Keys are "source.column" where the source is the CTE or derived-table
    alias the name is visible under, plus a bare "column" fallback for an
    unqualified reference. Keying on the column name alone made an alias
    poison every same-named column in the statement — UNNEST(o.items) is a
    scanned column of o whatever some unrelated CTE calls its aggregate.

    Both spellings of a binding count: SELECT expr AS name, and the column
    alias list on a CTE or derived table, which names the projections
    positionally and carries no Alias node at all.
    """
    projections: dict[str, list[exp.Expr]] = {}
    stars: list[tuple[str, exp.Select]] = []

    def bind(source: str | None, column: str, value: exp.Expr) -> None:
        key = f"{source.lower()}.{column.lower()}" if source else column.lower()
        projections.setdefault(key, []).append(value)

    def bind_alias_list(alias: exp.TableAlias | None, body: exp.Expr) -> None:
        if alias is None or not alias.columns:
            return
        select = body.find(exp.Select)
        if select is None:
            return
        source = alias.name
        for column, projection in zip(alias.columns, select.expressions, strict=False):
            bind(source, column.name, projection)

    for cte in tree.find_all(exp.CTE):
        bind_alias_list(cte.args.get("alias"), cte.this)
    for subquery in tree.find_all(exp.Subquery):
        bind_alias_list(subquery.args.get("alias"), subquery.this)

    for select in tree.find_all(exp.Select):
        source = _select_source_name(select)
        for projection in select.expressions:
            if isinstance(projection, exp.Alias):
                bind(source, projection.alias, projection.this)
                bind(None, projection.alias, projection.this)
            elif isinstance(projection, exp.Column) and source:
                # q.* parses as a Column whose name is the star, not a Star
                # node, so a qualified star re-exports like a bare one.
                if projection.name == "*":
                    stars.append((source, select))
                # SELECT arr FROM s re-exports the name under a new source,
                # so the outer scope can follow it back to what built it.
                # Binding it under its own source would only point at itself.
                elif projection.table.lower() != source.lower():
                    bind(source, projection.name, projection)
            elif isinstance(projection, exp.Star) and source:
                stars.append((source, select))
            elif source and _positional_name(select, projection):
                # A UNION arm names its columns by position from the first
                # arm; an unnamed projection in a later one still reaches the
                # outer scope under that name.
                bind(source, _positional_name(select, projection) or "", projection)
    # A star re-exports every name its own scope can see, so what an inner
    # projection built passes outward unnamed. Resolving each one to the
    # names below it keeps a manufactured array from shedding its history
    # by being selected with *.
    for source, select in stars:
        for inner in select.find_all(exp.Alias):
            bind(source, inner.alias, inner.this)
    return projections


def _positional_name(select: exp.Select, projection: exp.Expr) -> str | None:
    """The name a set operation gives this projection, taken from the first
    arm, which is where SQL fixes the output column names."""
    union = select.parent
    if not isinstance(union, exp.Union):
        return None
    first = union.this.find(exp.Select) if union.this else None
    if first is None or first is select:
        return None
    try:
        position = select.expressions.index(projection)
    except ValueError:
        return None
    if position >= len(first.expressions):
        return None
    named = first.expressions[position]
    return named.alias_or_name or None


def _column_key(column: exp.Column) -> str:
    """How this reference names itself: qualified where the SQL qualifies it."""
    source = column.table
    name = column.name.lower()
    return f"{source.lower()}.{name}" if source else name


def _column_bindings(
    column: exp.Column, projections: Mapping[str, list[exp.Expr]]
) -> list[exp.Expr]:
    """What a projection bound this reference to, if anything did.

    A qualified reference reads only bindings visible under that source, so
    an unrelated CTE binding the same name cannot speak for it. An
    unqualified one has no source to check and falls back to the bare name.
    """
    key = _column_key(column)
    bindings = projections.get(key)
    if bindings is not None:
        return bindings
    if column.table:
        return []
    # Unqualified, the SQL does not say which source this reads, so every
    # binding of the name answers for it: a projection that only forwards
    # a name (SELECT c FROM a) would otherwise lose the array behind it.
    # Bindings that resolve to this same reference are skipped — a name
    # forwarded from a table is a scanned column, not a manufactured one.
    name = column.name.lower()
    return [
        bound
        for binding_key, bindings in projections.items()
        if binding_key == name or binding_key.endswith(f".{name}")
        for bound in bindings
        if not (isinstance(bound, exp.Column) and _column_key(bound) == key)
    ]


def _select_source_name(select: exp.Select) -> str | None:
    """The alias a SELECT's rows are visible under, if it has one."""
    parent = select.parent
    while parent is not None:
        if isinstance(parent, exp.Subquery | exp.CTE):
            alias = parent.args.get("alias")
            return alias.name if alias is not None else None
        if isinstance(parent, exp.Select):
            return None
        parent = parent.parent
    return None


def _is_bounded_input(
    value: exp.Expr,
    projections: Mapping[str, list[exp.Expr]] | None = None,
    seen: frozenset[str] = frozenset(),
) -> bool:
    """True if this generator argument yields a length something else fixes."""
    projections = projections if projections is not None else {}
    if isinstance(value, exp.Array | exp.Struct):
        # A literal spells out its own length — but spelling out 20,000 of
        # them multiplies every scanned row by 20,000 just the same.
        if len(value.expressions or []) > _MAX_INLINE_ROWS:
            return False
        # The elements themselves must be literal: ARRAY[sequence(1, 1e9)]
        # spells out one element and yields a billion rows.
        return all(
            isinstance(element, exp.Literal)
            for element in (value.expressions or [])
        )
    if isinstance(value, exp.Column):
        # A name a projection bound is only as bounded as what it was bound
        # to; a name nothing in the statement binds is a scanned column, whose
        # rows the plan already counted.
        key = _column_key(value)
        # An alias defined in terms of itself has no length to read, and a
        # chain longer than any real query writes exhausts the stack: both
        # stop here rather than vouch for the column.
        if key in seen or len(seen) >= _MAX_ALIAS_DEPTH:
            return False
        return all(
            _is_bounded_input(bound, projections, seen | {key})
            for bound in _column_bindings(value, projections)
        )
    if isinstance(value, exp.GenerateSeries):
        # sequence() spells its own length out when both ends are literal, so
        # a date spine can be priced instead of refused. Anything the length
        # cannot be computed from — a column bound, a calendar step, a span
        # over the cap — stays unpriceable.
        length = _sequence_length(value)
        return length is not None and length <= _MAX_INLINE_ROWS
    if isinstance(value, exp.Func):
        if _func_name(value) not in _ROW_PRESERVING_FUNCS:
            return False
        arguments = [
            argument
            for argument in _func_arguments(value)
            if not isinstance(argument, _NOT_A_ROW_SOURCE)
        ]
        # A reshaping function with no argument left to check is reshaping a
        # literal, which cannot add rows.
        return all(
            _is_bounded_input(argument, projections, seen) for argument in arguments
        )
    return False


def _sequence_length(value: exp.GenerateSeries) -> int | None:
    """How many rows this sequence() yields, or None if that is not knowable.

    Only fixed strides count. A month or year step covers a variable number
    of days, so its length is not arithmetic on the endpoints and the gate
    keeps refusing it.
    """
    start = _literal_point(value.args.get("start"))
    end = _literal_point(value.args.get("end"))
    if start is None or end is None:
        return None
    step = value.args.get("step")
    stride = 1
    if isinstance(step, exp.Interval):
        unit = (step.text("unit") or "").upper()
        if unit not in _FIXED_INTERVAL_DAYS:
            return None
        # Trino writes an interval magnitude as a quoted literal: INTERVAL '1'.
        magnitude = _literal_int(step.this, allow_string=True)
        if magnitude is None or magnitude <= 0:
            return None
        stride = magnitude * _FIXED_INTERVAL_DAYS[unit]
    elif step is not None:
        magnitude = _literal_int(step)
        if magnitude is None or magnitude == 0:
            return None
        stride = abs(magnitude)
    span = abs(end - start)
    return span // stride + 1


def _literal_point(value: exp.Expr | None) -> int | None:
    """An endpoint as an integer: a plain number, or a date as its ordinal."""
    if value is None:
        return None
    if isinstance(value, exp.Cast):
        inner = value.this
        text = inner.name if isinstance(inner, exp.Literal) else None
        if text is None:
            return None
        try:
            return date.fromisoformat(text).toordinal()
        except ValueError:
            return None
    return _literal_int(value)


def _literal_int(value: exp.Expr | None, allow_string: bool = False) -> int | None:
    """A literal integer, or None for anything the agent could vary."""
    if isinstance(value, exp.Neg):
        magnitude = _literal_int(value.this, allow_string)
        return None if magnitude is None else -magnitude
    if not isinstance(value, exp.Literal):
        return None
    if value.is_string and not allow_string:
        return None
    try:
        return int(value.name)
    except ValueError:
        return None


def _generator_rows(
    generator: exp.Expr,
    projections: Mapping[str, list[exp.Expr]] | None = None,
    seen: frozenset[str] = frozenset(),
) -> int:
    """Rows a bounded generator yields: a literal array's length, or 1 for a
    scanned column, whose rows belong to a table the plan already priced.

    Every literal array under the generator counts, not only a direct child:
    MAP(ARRAY[...], ARRAY[...]) yields its key array's length, and reading
    only the top node would price a 5000-entry lookup as one row. A column a
    projection bound counts as whatever it was bound to — array_agg over a
    1000-row sequence is a 1000-element array by the time UNNEST reads it.
    """
    projections = projections if projections is not None else {}
    widest = 1
    for value in generator.find_all(exp.Array, exp.Struct):
        widest = max(widest, len(value.expressions or []))
    for series in generator.find_all(exp.GenerateSeries):
        length = _sequence_length(series)
        if length is not None:
            widest = max(widest, length)
    for column in generator.find_all(exp.Column):
        key = _column_key(column)
        if key in seen or len(seen) >= _MAX_ALIAS_DEPTH:
            continue
        for bound in _column_bindings(column, projections):
            widest = max(widest, _generator_rows(bound, projections, seen | {key}))
    return widest


def has_unpriceable_shape(sql: str, dialect: str) -> bool:
    """True if the plan's estimates would miss this query's real row work.

    Only row generators qualify. Measured on Trino 476,
    UNNEST(sequence(1, 10000)) over a 60,175-row table plans as 60,175 rows
    and produces 601 million: a generator manufactures rows from an argument
    and contributes no operator the planner sizes. Every other row explosion
    — products, low-cardinality join keys, correlated nested loops — the
    planner does size, and the budget prices from the plan itself.

    Unparseable SQL counts as unpriceable: if we cannot prove the shape is
    safe, we assume the risky answer. Nesting deep enough to exhaust the
    interpreter's stack raises RecursionError rather than a parser error, and
    that is the same answer — a shape nobody read is not a shape anybody
    vouched for. validate_query's depth cap stops such SQL earlier; this
    keeps the module's own contract if it is ever called without it.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return True
    return _generates_rows(tree)


def table_scan_counts(sql: str, dialect: str) -> dict[str, int]:
    """How many times the SQL reads each fully-qualified table.

    Measured against Trino 476, the IO plan reports one entry per table no
    matter how often the query reads it — a 4-times-referenced CTE and a
    3-way self-join both return a single entry, while processing 4x and 3x
    the rows. Comparing these counts against the entries the plan returned is
    what recovers the shortfall.

    A CTE reference is not skipped: it re-reads whatever the CTE body scans,
    so the body's tables count once per reference to it.

    Returns {} for unparseable SQL — the shape check has already refused it.
    """
    return _walk_scans(sql, dialect)[0]


def _walk_scans(sql: str, dialect: str) -> tuple[dict[str, int], bool]:
    """The read counts, and whether the walk budget ran out reaching them."""
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    # Nesting past the interpreter's stack raises RecursionError, not a
    # parser error; either way the shape check has already refused this SQL.
    except (sqlglot.errors.SqlglotError, RecursionError):
        return {}, False

    bodies = _cte_bodies(tree)
    counts: dict[str, int] = {}
    # A chain of CTEs each referenced twice doubles walk work per level of
    # depth; a running total caught before recursing bounds that growth
    # instead of only bounding the counts it would have produced.
    budget = [_MAX_SCAN_COUNT]

    def walk(node: exp.Expr, weight: int, pending: frozenset[str]) -> None:
        if budget[0] <= 0:
            return
        for table in node.find_all(exp.Table):
            if budget[0] <= 0:
                return
            parts = [part.name for part in table.parts]
            name = parts[0].lower()
            if len(parts) == 1 and name in bodies:
                # A CTE that references itself would recurse forever, and its
                # depth is the engine's business, not the quote's.
                if name in pending:
                    continue
                # A name bound more than once is counted through every body
                # it could mean: scope decides which, and skipping the others
                # let a decoy binding hide the reads behind the name.
                for body in bodies[name]:
                    budget[0] -= 1
                    walk(body, weight, pending | {name})
                continue
            key = ".".join(parts).lower()
            counts[key] = counts.get(key, 0) + weight
            budget[0] -= weight

    # From the query with its WITH detached: a CTE body counts once per
    # reference to it, not once where it is defined.
    walk(_without_cte_bodies(tree), 1, frozenset())
    return counts, budget[0] <= 0


def scan_counts_saturated(sql: str, dialect: str) -> bool:
    """True if counting this query's reads hit the walk budget.

    A saturated count is not a small count: the byte quote is scaled UP by
    how many reads the plan folded together, so counting fewer reads than
    the query really does makes it cheaper. Measured, a 1,471-character CTE
    chain reads a table 524,288 times and counts 3,328 — a 61 GiB query
    priced at 3 GiB. The caller denies on this rather than take the discount.
    """
    return _walk_scans(sql, dialect)[1]


def _without_cte_bodies(tree: exp.Expr) -> exp.Expr:
    """The query with its WITH clause detached, so CTE bodies are counted
    where they are referenced rather than once where they are defined."""
    copy = tree.copy()
    for with_clause in list(copy.find_all(exp.With)):
        with_clause.pop()
    return copy
