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

# Reads past this many stop being counted exactly. Walking each name once
# reaches numbers a re-walk never survived to produce — a 400-deep doubling
# chain is 2^399 — and a count that large would scale a byte quote through
# arbitrary-precision arithmetic to say what any budget already denies.
_MAX_COUNTED_READS = 1_000_000

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

# Stands for an alias that names more than one relation: not a CTE name, and
# not something a lookup may treat as an unbound (therefore scanned) column.
_AMBIGUOUS = "\x00ambiguous"

# Stands for "this alias reads that relation": one entry per alias instead of
# one per alias and column, so a wide query costs what it reads, not the
# product of its width and its aliases.
_ALIAS_OF = "\x00alias_of"

# Prefix for the by-bare-name index: an unqualified reference reads every
# binding of that name, and finding them by sweeping the keys made each
# lookup cost the whole statement's width.
_BY_NAME = "\x00by_name\x00"

# Steps a standalone resolve may take when no walk lends it one. Callers
# inside the gate share the walk's budget; this only bounds a direct call.
_MAX_RESOLVE_STEPS = 10_000

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
    """Each arm rows are ADDED across, or the query itself if there is none.

    Only UNION: an INTERSECT or EXCEPT arm cannot add rows to the result, so
    judging it separately would let each arm carry its own product. Left
    whole, the widest generator in either arm binds the statement — the
    conservative reading.
    """
    if isinstance(node, exp.Union):
        return [*_union_branches(node.this), *_union_branches(node.expression)]
    return [node]


def _projecting_arms(node: exp.Expr) -> list[exp.Expr]:
    """Every arm of a set operation that can put a value under a name.

    Wider than _union_branches on purpose: this answers "what could this
    name hold", and INTERSECT and EXCEPT arms project just as UNION arms do.
    A parenthesised arm parses as a Subquery, so one pair of brackets hid an
    arm from a walk that only recursed on Union — and the array it built
    read as a scanned column.
    """
    if isinstance(node, exp.SetOperation):
        return [
            *_projecting_arms(node.this),
            *_projecting_arms(node.expression),
        ]
    if isinstance(node, exp.Subquery):
        return _projecting_arms(node.this)
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
        product = _generator_product(
            branch, bodies, frozenset(), budget, projections, for_pricing=True
        )
        if product is None:
            return 1
        widest = max(widest, product)
    return widest


def _without_unmet_relations(node: exp.Expr) -> exp.Expr:
    """The subtree with the relations nothing joins to detached.

    A table under EXISTS or an IN predicate answers a question about each
    row; one in a scalar subquery in the SELECT list contributes a value to
    it. Neither pairs its rows with the branch's, so neither is a table a
    generator's rows can be a multiplier on.
    """
    copy = node.copy()
    for predicate in list(copy.find_all(exp.Exists, exp.In)):
        for relation in list(predicate.find_all(exp.Table)):
            relation.pop()
    for select in list(copy.find_all(exp.Select)):
        for projection in select.expressions:
            for subquery in list(projection.find_all(exp.Subquery)):
                for relation in list(subquery.find_all(exp.Table)):
                    relation.pop()
    return copy


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
    for_pricing: bool = False,
) -> int | None:
    """Rows this subtree's generators multiply out to, or None past the cap.

    None also stands for a generator the gate cannot bound at all, and for a
    walk that ran out of budget: all three mean the same thing to the caller
    — no quote — so they share a return.

    `for_pricing` asks for the multiplier a caller will apply to the plan's
    own estimate, so a generator whose rows collapse before they meet the
    branch contributes nothing. The refusal path leaves it False: whether a
    generator can be *sized* is a question about the generator, not about
    where it sits, and skipping one there would let an unbounded spine hide
    inside an aggregate.
    """
    if budget[0] <= 0:
        return None
    # Whether the plan will carry this branch's rows decides how large a
    # spelled-out spine may be: crossed with a table it is a multiplier the
    # budget applies to a real cardinality, and alone it is all there is.
    # It has to be a table the generator actually MEETS — one inside EXISTS,
    # an IN predicate or an uncorrelated scalar subquery carries nothing, and
    # counting it let a 10,000,000-row spine through where 1000 is the limit.
    priced = _reads_a_table(_without_unmet_relations(node), bodies, pending)
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
            # Resolving a generator's input shares the walk's budget: the
            # depth cap bounds a single chain of names, not how many chains a
            # wide statement writes, and the resolvers were the one path
            # nothing charged. 71 KB of CTEs and unnests cost a minute of CPU
            # before Trino was contacted.
            if not _expands_a_bounded_value(
                child, projections, budget=budget, priced_by_a_table=priced
            ):
                return None
            if for_pricing and not _multiplies_its_branch(child, node):
                continue
            product *= _generator_rows(child, projections, budget=budget)
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
        if for_pricing and not _multiplies_its_branch(series, node):
            continue
        length = _sequence_length(series)
        if length is None or length > _MAX_COUNTED_ROWS:
            return None
        product *= length
        if product > _MAX_COUNTED_ROWS:
            return None
    # The same name read twice reads the same bodies, so its cost is walked
    # once and applied per reference: re-walking spent the budget on the
    # body's WIDTH, which is the analyst's to write, and refused an ordinary
    # 400-column CTE read eight times before it reached any generator.
    per_name: dict[str, int] = {}
    for name in references:
        if name not in per_name:
            # A name bound more than once costs whatever its dearest binding
            # costs: scope decides which one a reference reads, and charging
            # the cheapest is what let a decoy body hide a generator.
            widest = 1
            for body in bodies[name]:
                nested = _generator_product(
                    body, bodies, pending | {name}, budget, projections, for_pricing
                )
                if nested is None:
                    return None
                # Rows leave a CTE through its own select, so an aggregate
                # there collapses them before any reference can multiply by
                # them — the sizing question stays open, only the multiplier.
                # Only where the body builds nothing of its own: an aggregate
                # over a generator already crossed with a table runs AFTER
                # those rows exist, and excusing it quoted 6 billion rows as
                # six million.
                if (
                    for_pricing
                    and _collapses_on_the_way_out(body)
                    and not _reads_a_table(body, bodies, pending | {name})
                ):
                    nested = 1
                widest = max(widest, nested)
            per_name[name] = widest
        product *= per_name[name]
        if product > _MAX_COUNTED_ROWS:
            return None
    return product


def _scans_a_table(select: exp.Select) -> bool:
    """True if this select reads a relation of its own, generators aside."""
    for table in select.find_all(exp.Table):
        if any(table.find_all(exp.GenerateSeries, *_GENERATORS)):
            continue
        return True
    return False


def _narrows_its_rows(select: exp.Select) -> bool:
    """True if fewer rows leave this select than its FROM produced.

    The question is cardinality, not syntax. A bare aggregate yields exactly
    one row. A GROUP BY yields one row per distinct key — a reduction only if
    the key is something other than what the generator itself enumerates:
    GROUP BY over a distinct spine is the identity, and reading it as a
    collapse priced 60 billion rows as six million. A window function is an
    aggregate node that adds a column and removes no row at all.
    """
    grouped = select.args.get("group")
    if grouped is not None:
        # ROLLUP, CUBE and GROUPING SETS emit the subtotal rows on top of the
        # groups, so they are the one grouping shape that can ADD rows. They
        # also keep their keys in their own args, leaving group.expressions
        # empty — which the identity test below would have read as "no keys".
        if any(
            grouped.args.get(shape)
            for shape in ("rollup", "cube", "grouping_sets", "totals")
        ):
            return False
        # Whether a GROUP BY reduces anything is a cardinality question, and
        # ADR 0004 keeps those with the plan. The one case SQL settles on its
        # own is a key list that names exactly the columns the generators in
        # this select produce: one row per row they made, the identity, which
        # read as a collapse priced 60 billion rows as six million. Anything
        # else — a key from a joined relation, an expression, a subset — is
        # charged as a reduction, the fail-safe direction for a multiplier.
        return not _groups_by_what_the_generators_make(select, grouped)
    # A window function carries an OVER clause; only a plain aggregate with
    # no grouping reduces the select to a single row.
    return any(
        aggregate.find_ancestor(exp.Window) is None
        for aggregate in select.find_all(exp.AggFunc, bfs=False)
        if _is_in_this_select(aggregate, select)
    )


def _groups_by_what_the_generators_make(
    select: exp.Select, grouped: exp.Group
) -> bool:
    """True if the group keys are exactly the columns this select's
    generators enumerate, so grouping returns one row per row they made."""
    keys = [key for key in grouped.expressions if isinstance(key, exp.Column)]
    if len(keys) != len(grouped.expressions) or not keys:
        return False
    produced: set[str] = set()
    relations = 0
    for source in select.find_all(exp.Table, exp.Unnest, bfs=False):
        relations += 1
        if not isinstance(source, exp.Unnest):
            continue
        alias = source.args.get("alias")
        for column in getattr(alias, "columns", []) or []:
            produced.add(column.name.lower())
    # Every relation in the FROM must be one of those generators: a joined
    # table brings rows the keys do not identify, and how many is the plan's
    # answer, not the SQL's.
    if relations != len(list(select.find_all(exp.Unnest, bfs=False))):
        return False
    return bool(produced) and {key.name.lower() for key in keys} == produced


def _is_in_this_select(node: exp.Expr, select: exp.Select) -> bool:
    """True if the nearest enclosing select is this one, not a nested query."""
    return node.find_ancestor(exp.Select) is select


def _collapses_on_the_way_out(body: exp.Expr) -> bool:
    """True if this scope's select narrows its rows before they leave it."""
    select = body if isinstance(body, exp.Select) else body.find(exp.Select)
    if select is None:
        return False
    return _narrows_its_rows(select)


def _multiplies_its_branch(generator: exp.Expr, root: exp.Expr) -> bool:
    """True if this generator's rows meet the branch's rows and multiply them.

    A generator in a FROM or a JOIN crosses whatever else the branch reads.
    Two shapes do not: rows an aggregate collapses on the way out (a spine
    counted into one number, then joined), and a predicate subquery, where
    EXISTS and IN ask whether a match exists rather than pairing with each
    one. Charging those anyway priced an ordinary 365-day report at 365x its
    real work, which the row budget then denied — over-quoting refuses a
    query as surely as under-quoting admits one.
    """
    # The branch's own select is where the widest step is built: an aggregate
    # there runs after the generator's rows already exist, so it spares the
    # engine nothing. Only a scope the rows must leave — a derived table, a
    # CTE, a subquery — can collapse them before they reach the branch.
    branch_select = root if isinstance(root, exp.Select) else root.find(exp.Select)
    node: exp.Expr | None = generator
    while node is not None and node is not root:
        parent = node.parent
        if isinstance(parent, exp.Select):
            # A subquery in the SELECT list is scalar: it contributes a value
            # to each row rather than rows of its own.
            if node.arg_key == "expressions":
                return False
            # A narrowing scope spares the work only where it builds nothing
            # of its own: one over a generator already crossed with a table
            # runs after those rows exist, and excusing it quoted 6 billion
            # rows as six million.
            if (
                parent is not branch_select
                and _narrows_its_rows(parent)
                and not _scans_a_table(parent)
            ):
                return False
        # EXISTS and IN test for a match; neither pairs the outer row with
        # every row the subquery could produce.
        if isinstance(parent, exp.Exists | exp.In):
            return False
        node = parent
    return True


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
    budget: list[int] | None = None,
    priced_by_a_table: bool = False,
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
    return all(
        _is_bounded_input(value, projections, seen, budget, priced_by_a_table)
        for value in inputs
    )


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
    bound_columns: dict[str, dict[str, list[exp.Expr]]] = {}
    stars: list[tuple[str, exp.Select]] = []

    def bind(source: str | None, column: str, value: exp.Expr) -> None:
        key = f"{source.lower()}.{column.lower()}" if source else column.lower()
        projections.setdefault(key, []).append(value)
        # The same binding under a bare-name index, so an unqualified
        # reference finds it by lookup rather than by sweeping every key.
        projections.setdefault(f"{_BY_NAME}{column.lower()}", []).append(value)
        if source:
            bound_columns.setdefault(source.lower(), {}).setdefault(
                column.lower(), []
            ).append(value)

    def bind_alias_list(alias: exp.TableAlias | None, body: exp.Expr) -> None:
        if alias is None or not alias.columns:
            return
        source = alias.name
        # Every arm binds the name, not just the first one found: a set
        # operation carries whatever any arm projects, and reading only the
        # leading Select let a later arm's manufactured array pass as a
        # scanned column.
        for branch in _projecting_arms(body):
            select = branch.find(exp.Select)
            if select is None:
                continue
            for column, projection in zip(
                alias.columns, select.expressions, strict=False
            ):
                bind(source, column.name, projection)

    for cte in tree.find_all(exp.CTE):
        bind_alias_list(cte.args.get("alias"), cte.this)
    for subquery in tree.find_all(exp.Subquery):
        bind_alias_list(subquery.args.get("alias"), subquery.this)
    # VALUES holds its own alias and column list, and its row is a projection
    # like any other: (VALUES (repeat(1, 10000))) AS v(arr) builds an array.
    for values in tree.find_all(exp.Values):
        alias = values.args.get("alias")
        if alias is None or not alias.columns:
            continue
        for row in values.expressions:
            items = row.expressions if isinstance(row, exp.Tuple) else [row]
            for column, item in zip(alias.columns, items, strict=False):
                bind(alias.name, column.name, item)

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
    # by being selected with *. A star whose FROM is a CTE name sees nothing
    # in its own subtree — the bindings live in that CTE's body under the
    # WITH — so the reference is followed there too.
    cte_bodies = {
        cte.alias_or_name.lower(): cte.this for cte in tree.find_all(exp.CTE)
    }

    # A body reached twice binds the same names both times, so visiting it
    # once per source is enough: without this a diamond of star CTEs walks
    # every path, and 1 KB of SQL costs seconds before the budgeted walk.
    visited: set[tuple[str, int]] = set()

    def bind_star(source: str, select: exp.Select) -> None:
        if (source, id(select)) in visited:
            return
        visited.add((source, id(select)))
        for inner in select.find_all(exp.Alias):
            bind(source, inner.alias, inner.this)
        for table in select.find_all(exp.Table):
            parts = [part.name for part in table.parts]
            name = parts[0].lower()
            if len(parts) > 1 or name not in cte_bodies:
                continue
            body = cte_bodies[name].find(exp.Select)
            if body is not None:
                bind_star(source, body)

    for source, select in stars:
        bind_star(source, select)

    # FROM a AS z asks for z.arr while the binding sits under the CTE's own
    # name. Only aliases that name exactly one CTE resolve: an alias used for
    # two relations, or one that shadows a real table, means more than a name
    # can say, and guessing merged unrelated scans with an aggregate's array.
    alias_sources = _alias_sources(tree)
    for alias, name in alias_sources.items():
        if name is _AMBIGUOUS:
            # A marker key: any column read through this alias resolves to
            # something the gate cannot read, which fails closed.
            projections[f"{alias}.{_AMBIGUOUS}"] = []
            continue
        # Recorded as an indirection rather than copied: writing every bound
        # column under every alias that names its relation is the alias x
        # column cross-product, which 3,000 of each turned into nine million
        # entries — seconds of CPU and gigabytes of RSS on SQL well inside the
        # length cap, spent before the walk budget is charged at all.
        if name in bound_columns:
            projections[f"{alias}.{_ALIAS_OF}"] = [exp.Identifier(this=name)]
    return projections


def _alias_sources(tree: exp.Expr) -> dict[str, str]:
    """Which CTE each table alias reads, for aliases that name exactly one.

    FROM a AS z asks for z.arr while the binding was recorded under the CTE's
    own name, so the alias needs resolving. Copying the bindings themselves
    was both quadratic in query width and blind to scope — it merged every
    relation sharing an alias letter. An alias used for two different CTEs
    resolves to neither: which one a reference means is more than a name can
    say, and guessing is what condemned unrelated scans.
    """
    cte_names = {cte.alias_or_name.lower() for cte in tree.find_all(exp.CTE)}
    # What each alias names, whether a CTE or a table: an alias that names two
    # relations is ambiguous whatever kind they are, while one that names a
    # single table is ordinary SQL and resolves to no CTE at all.
    named: dict[str, set[str]] = {}
    sources: dict[str, str] = {}
    for table in tree.find_all(exp.Table):
        parts = [part.name for part in table.parts]
        name = parts[0].lower()
        alias = table.alias.lower()
        if not alias or alias == name:
            continue
        named.setdefault(alias, set()).add(".".join(parts).lower())
        if len(parts) == 1 and name in cte_names:
            sources[alias] = name
    ambiguous = {alias for alias, relations in named.items() if len(relations) > 1}
    resolved = {
        alias: name for alias, name in sources.items() if alias not in ambiguous
    }
    # An alias naming more than one relation resolves to nothing readable.
    # It is recorded so a reference through it can be refused: an alias the
    # gate cannot follow is a shape it cannot size, and spelling that
    # ambiguity on purpose is how a manufactured array hid behind it.
    for alias in ambiguous:
        resolved.setdefault(alias, _AMBIGUOUS)
    return resolved


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
    column: exp.Column,
    projections: Mapping[str, list[exp.Expr]],
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
        # FROM cte AS z asks for z.arr while the binding sits under the CTE's
        # own name; the alias resolves through its recorded relation instead
        # of every column having been copied under it.
        source = column.table.lower()
        alias_of = projections.get(f"{source}.{_ALIAS_OF}")
        if alias_of:
            relation = alias_of[0].name.lower()
            return list(projections.get(f"{relation}.{column.name.lower()}", []))
        return []
    # Unqualified, the SQL does not say which source this reads, so every
    # binding of the name answers for it: a projection that only forwards
    # a name (SELECT c FROM a) would otherwise lose the array behind it.
    # Bindings that resolve to this same reference are skipped — a name
    # forwarded from a table is a scanned column, not a manufactured one.
    #
    # The name is looked up in an index built once with the bindings, rather
    # than swept for out of every key: an O(keys) scan per lookup sat inside
    # an unbudgeted recursion, and 37 KB of CTEs and unnests cost seconds of
    # CPU before the engine was ever asked. A name nothing binds indexes to an
    # empty list, so a miss is a lookup too.
    return [
        bound
        for bound in projections.get(f"{_BY_NAME}{column.name.lower()}", [])
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
    budget: list[int] | None = None,
    priced_by_a_table: bool = False,
) -> bool:
    """True if this generator argument yields a length something else fixes.

    The depth cap bounds how far a chain of names is followed; it does not
    bound how MANY there are, and breadth is what a wide statement of CTEs
    and unnests buys cheaply. Running out of budget answers False — an input
    nobody finished reading is not one the gate may vouch for.
    """
    projections = projections if projections is not None else {}
    budget = budget if budget is not None else [_MAX_RESOLVE_STEPS]
    budget[0] -= 1
    if budget[0] <= 0:
        return False
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
        # An alias naming more than one relation says nothing about which one
        # this reads, and a generator whose input cannot be identified is not
        # one the gate may vouch for.
        if value.table and f"{value.table.lower()}.{_AMBIGUOUS}" in projections:
            return False
        # A chain longer than any real query writes exhausts the stack, so it
        # stops here rather than vouch for the column.
        if len(seen) >= _MAX_ALIAS_DEPTH:
            return False
        bindings = _column_bindings(value, projections)
        if key in seen:
            # A name reached twice through nothing but other names is a
            # column some table supplies: a pipeline of CTEs forwarding an
            # array column revisits names without building anything. A cycle
            # that passes through an expression has a length nobody reads.
            return all(isinstance(bound, exp.Column) for bound in bindings)
        return all(
            _is_bounded_input(
                bound, projections, seen | {key}, budget, priced_by_a_table
            )
            for bound in bindings
        )
    if isinstance(value, exp.GenerateSeries):
        # sequence() spells its own length out when both ends are literal, so
        # a date spine can be priced instead of refused. Anything the length
        # cannot be computed from — a column bound, a calendar step, a span
        # over the cap — stays unpriceable.
        length = _sequence_length(value)
        if length is None:
            return False
        # The flat cap binds only where nothing downstream prices the rows.
        # Joined to a table, the size is a multiplier generator_fanout() hands
        # to the budget, which decides with the table's real cardinality in
        # hand — a seven-year daily spine against a 25-row table is 63,950
        # rows, and refusing it sight-unseen is the flat cap ADR 0006 dropped.
        if priced_by_a_table:
            return length <= _MAX_COUNTED_ROWS
        return length <= _MAX_INLINE_ROWS
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
            _is_bounded_input(argument, projections, seen, budget, priced_by_a_table)
            for argument in arguments
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
    budget: list[int] | None = None,
) -> int:
    """Rows a bounded generator yields: a literal array's length, or 1 for a
    scanned column, whose rows belong to a table the plan already priced.

    Every literal array under the generator counts, not only a direct child:
    MAP(ARRAY[...], ARRAY[...]) yields its key array's length, and reading
    only the top node would price a 5000-entry lookup as one row. A column a
    projection bound counts as whatever it was bound to — array_agg over a
    1000-row sequence is a 1000-element array by the time UNNEST reads it.

    Exhausting the budget returns the cap, not the width found so far: this
    figure becomes a multiplier, and a partial answer is a discount.
    """
    projections = projections if projections is not None else {}
    budget = budget if budget is not None else [_MAX_RESOLVE_STEPS]
    widest = 1
    for value in generator.find_all(exp.Array, exp.Struct):
        widest = max(widest, len(value.expressions or []))
    for series in generator.find_all(exp.GenerateSeries):
        length = _sequence_length(series)
        if length is not None:
            widest = max(widest, length)
    for column in generator.find_all(exp.Column):
        budget[0] -= 1
        if budget[0] <= 0:
            return _MAX_COUNTED_ROWS
        key = _column_key(column)
        if key in seen or len(seen) >= _MAX_ALIAS_DEPTH:
            continue
        for bound in _column_bindings(column, projections):
            widest = max(
                widest, _generator_rows(bound, projections, seen | {key}, budget)
            )
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

    # What one read of a name costs, walked once and then applied per
    # reference: re-walking a body for every reference spent the budget on the
    # body's width, so a 5,000-column CTE read 5,000 times took 16 seconds
    # before any engine was contacted. The counts are identical either way —
    # a read is still charged once per reference.
    per_name: dict[tuple[str, frozenset[str]], dict[str, int]] = {}

    def reads(node: exp.Expr, pending: frozenset[str]) -> dict[str, int]:
        found: dict[str, int] = {}
        for table in node.find_all(exp.Table):
            if budget[0] <= 0:
                return found
            parts = [part.name for part in table.parts]
            name = parts[0].lower()
            if len(parts) == 1 and name in bodies:
                # A CTE that references itself would recurse forever, and its
                # depth is the engine's business, not the quote's.
                if name in pending:
                    continue
                # Keyed by the enclosing scope as well: the same name inside
                # a different set of pending bindings can resolve elsewhere.
                memo_key = (name, pending)
                if memo_key not in per_name:
                    once: dict[str, int] = {}
                    # A name bound more than once is counted through every
                    # body it could mean: scope decides which, and skipping
                    # the others let a decoy binding hide the reads.
                    for body in bodies[name]:
                        budget[0] -= 1
                        for table_key, count in reads(body, pending | {name}).items():
                            once[table_key] = once.get(table_key, 0) + count
                    per_name[memo_key] = once
                for table_key, count in per_name[memo_key].items():
                    found[table_key] = found.get(table_key, 0) + count
                continue
            key = ".".join(parts).lower()
            found[key] = found.get(key, 0) + 1
            budget[0] -= 1
        return found

    # From the query with its WITH detached: a CTE body counts once per
    # reference to it, not once where it is defined.
    for table_key, count in reads(_without_cte_bodies(tree), frozenset()).items():
        counts[table_key] = counts.get(table_key, 0) + count
    # Counting each name once reaches figures a re-walk never survived to
    # produce: a 400-deep doubling chain is 2^399 reads, a 121-digit integer
    # that would then scale a byte quote through bignum arithmetic. Past this
    # many reads the exact number buys nothing — the query is already beyond
    # any budget — so it saturates, which denies rather than quotes.
    saturated = budget[0] <= 0 or any(
        count > _MAX_COUNTED_READS for count in counts.values()
    )
    return counts, saturated


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
