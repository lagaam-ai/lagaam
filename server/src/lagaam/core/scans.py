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
    cap therefore binds their *product* — counted across the whole statement,
    which overstates branches a UNION adds rather than multiplies, and the
    overstatement fails closed.
    """
    product = 1
    for generator in tree.find_all(*_GENERATORS):
        if not _expands_a_bounded_value(generator):
            return True
        product *= _generator_rows(generator)
        if product > _MAX_INLINE_ROWS:
            return True
    return False


def _expands_a_bounded_value(generator: exp.Expr) -> bool:
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
    return all(_is_bounded_input(value) for value in inputs)


def _is_bounded_input(value: exp.Expr) -> bool:
    """True if this generator argument yields a length something else fixes."""
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
        return True
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
        return all(_is_bounded_input(argument) for argument in arguments)
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


def _generator_rows(generator: exp.Expr) -> int:
    """Rows a bounded generator yields: a literal array's length, or 1 for a
    column, whose rows belong to a table the plan already priced.

    Every literal array under the generator counts, not only a direct child:
    MAP(ARRAY[...], ARRAY[...]) yields its key array's length, and reading
    only the top node would price a 5000-entry lookup as one row.
    """
    widest = 1
    for value in generator.find_all(exp.Array, exp.Struct):
        widest = max(widest, len(value.expressions or []))
    for series in generator.find_all(exp.GenerateSeries):
        length = _sequence_length(series)
        if length is not None:
            widest = max(widest, length)
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
    safe, we assume the risky answer.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
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
    except sqlglot.errors.SqlglotError:
        return {}, False

    bodies = {cte.alias_or_name.lower(): cte.this for cte in tree.find_all(exp.CTE)}
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
                budget[0] -= 1
                walk(bodies[name], weight, pending | {name})
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
