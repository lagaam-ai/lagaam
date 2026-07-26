"""Spot queries whose EXPLAIN byte estimate would misprice the real scan.

The quote is the sum of the IO plan's per-scan entries. Two shapes make that
sum a lie, and both are about rows rather than bytes:

- A join that is not an equi-join produces a product. Both inputs are still
  scanned exactly once, so the byte sum is *correct and irrelevant* while the
  row work is quadratic: `ON 1=1` and `ON a.k <> b.k` cost the same bytes as
  a healthy join and 10^11 times the work.
- A row generator — UNNEST(sequence(...)), UNNEST(repeat(col, 10000)) —
  manufactures rows from an argument and contributes no table entry at all,
  so it is invisible to the byte sum entirely.

A table read twice is a third: measured against Trino 476, the IO plan emits
one entry per *table*, not per scan — a self-join, a UNION of a table with
itself, and a twice-referenced CTE each report a single entry while
processing twice the rows. table_scan_counts() below recovers that shortfall
by counting the SQL's own references for the caller to scale the quote by.

We can't recover the true cost from the IO JSON, so we detect these shapes
from the SQL and fail safe. The rules below deliberately exempt the shapes
that look risky but aren't — a constant-row join, an UNNEST over a column or
a literal array — because a gate that blocks ordinary analytics is a failed
gate, not a strict one.
"""

from collections.abc import Iterator

import sqlglot
from sqlglot import exp

_GENERATORS = (exp.Unnest, exp.Explode, exp.Posexplode)

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
}


def _conjuncts(node: exp.Expr) -> Iterator[exp.Expr]:
    """The predicates a row must satisfy *all* of.

    Descends AND only. An OR branch, a NOT, or a CASE can be satisfied without
    its equality holding, so nothing under them constrains the join.
    """
    if isinstance(node, exp.And):
        yield from _conjuncts(node.left)
        yield from _conjuncts(node.right)
    elif isinstance(node, exp.Paren):
        yield from _conjuncts(node.this)
    else:
        yield node


def _predicate_sources(side: exp.Expr) -> set[str] | None:
    """The sources one side of an equality reads, or None if unhashable.

    A scalar subquery is opaque to the planner's join criteria, so an equality
    against one leaves a nested loop however it is written.
    """
    if side.find(exp.Select, exp.Subquery) is not None:
        return None
    return {
        column.table.lower()
        for column in side.find_all(exp.Column)
        if column.table
    }


def _joined_sources(predicate: exp.Expr) -> tuple[str, str] | None:
    """The two sources an equality lets the engine hash on, if any.

    Each side must read exactly one source, and the two must differ — that is
    what Trino compiles into join criteria. The keys themselves may be
    expressions: `ON year(a.d) = year(b.d) + 1` is a hash join, while
    `ON a.k + b.k = 5` mixes both sources into one side and is a nested loop.
    """
    if not isinstance(predicate, exp.EQ):
        return None
    left = _predicate_sources(predicate.left)
    right = _predicate_sources(predicate.right)
    if left is None or right is None:
        return None
    if len(left) != 1 or len(right) != 1 or left == right:
        return None
    return (left.pop(), right.pop())


def _equi_join_pairs(condition: exp.Expr | None) -> set[frozenset[str]]:
    """Source pairs this predicate equi-joins, ignoring anything it cannot."""
    if condition is None:
        return set()
    pairs = set()
    for conjunct in _conjuncts(condition):
        sources = _joined_sources(conjunct)
        if sources is not None:
            pairs.add(frozenset(sources))
    return pairs


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
    """
    for generator in tree.find_all(*_GENERATORS):
        if _expands_a_bounded_value(generator):
            continue
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


def _source_parts(select: exp.Select) -> list[exp.Expr]:
    """Every relation this SELECT reads directly: its FROM and its joins.

    sqlglot names the FROM arg "from_", and args.get() on a wrong key returns
    None rather than raising, which silently empties this list. Not find():
    that reaches into a subquery and would report its sources as ours.
    """
    from_part = select.args.get("from_")
    if from_part is None and select.args.get("from") is not None:
        raise AssertionError("sqlglot renamed the FROM arg key")
    return [
        part
        for part in [from_part, *(select.args.get("joins") or [])]
        if part is not None
    ]


def _source_alias(source: exp.Expr) -> str | None:
    """The name a predicate would use to refer to this join input."""
    if isinstance(source, exp.Alias | exp.Subquery) and source.alias:
        return str(source.alias).lower()
    if isinstance(source, exp.Table):
        return (source.alias or source.name).lower()
    return None


def _has_ambiguous_alias(tree: exp.Expr) -> bool:
    """True if two sources in one FROM answer to the same name.

    An alias is how a predicate names a source, so two sources sharing one
    means an equality that appears to constrain a join may constrain a
    different source entirely, leaving this one a product.
    """
    for select in tree.find_all(exp.Select):
        names = [
            _source_alias(part.this)
            for part in _source_parts(select)
            if part.this is not None
        ]
        present = [name for name in names if name is not None]
        if len(present) != len(set(present)):
            return True
    return False


def _has_product_join(tree: exp.Expr) -> bool:
    """True if any join pairs rows without an equality to match them on."""
    if _has_ambiguous_alias(tree):
        return True
    for join in tree.find_all(exp.Join):
        if join.args.get("using") is not None:
            continue
        if _bounded_source(join.this):
            continue
        alias = _source_alias(join.this)
        pairs = _equi_join_pairs(join.args.get("on"))
        if join.args.get("on") is None:
            # A comma join carries its equality in the enclosing WHERE, and
            # reads identically to the JOIN..ON form the planner rewrites it
            # to — but only for the source that equality actually names.
            select = join.find_ancestor(exp.Select)
            where = select.args.get("where") if select else None
            # where.this: this SELECT's own predicate, not a subquery's.
            pairs = _equi_join_pairs(where.this) if where is not None else set()
        if alias is not None and any(alias in pair for pair in pairs):
            continue
        return True
    return False


def _inline_cardinality(source: exp.Expr) -> int | None:
    """How many rows this join input contributes on its own, if a fixed count.

    None means "not an inline relation" — a real table, whose rows the IO plan
    already counted. A number is what the agent spelled out or generated, none
    of which the plan can see.
    """
    if isinstance(source, exp.Alias | exp.Subquery):
        source = source.this
    if isinstance(source, _GENERATORS):
        if _generates_rows(source):
            return None
        return _generator_rows(source)
    if isinstance(source, exp.Values):
        return len(source.expressions or [])
    if isinstance(source, exp.Query):
        # A subquery reading no table can only produce the rows it spells out.
        if list(source.find_all(exp.Table)):
            return None
        # A wrapper yields the product of what it reads, not the widest of
        # them, or nesting would launder the multiplier one level down.
        rows = 1
        for select in source.find_all(exp.Select):
            for part in _source_parts(select):
                if part.this is not None:
                    rows *= max(_inline_cardinality(part.this) or 1, 1)
        return rows
    return None


def _generator_rows(generator: exp.Expr) -> int:
    """Rows a bounded generator yields: a literal array's length, or 1 for a
    column, whose rows belong to a table the plan already priced."""
    widest = 1
    for value in (generator.this, *(generator.expressions or [])):
        if isinstance(value, exp.Array | exp.Struct):
            widest = max(widest, len(value.expressions or []))
    return widest


def _bounded_source(source: exp.Expr) -> bool:
    """True if this join input contributes a fixed or already-counted number
    of rows, so pairing against it is not a product.

    `CROSS JOIN (SELECT 0.2 AS rate)` is how an agent parameterizes a query,
    and `CROSS JOIN UNNEST(o.items)` is how it reads a nested column — the
    rows come from a table the IO plan already priced.
    """
    rows = _inline_cardinality(source)
    return rows is not None and rows <= _MAX_INLINE_ROWS


def _has_inline_row_product(tree: exp.Expr) -> bool:
    """True if one SELECT's inline relations multiply past the row cap.

    Each is bounded alone and nothing pairs them: two 1000-row VALUES against
    a scanned table is a million-fold multiplier the plan prices as one scan.
    Their product is the multiplier, so the product is what must be capped.
    """
    for select in tree.find_all(exp.Select):
        product = 1
        for part in _source_parts(select):
            if part.this is None:
                continue
            rows = _inline_cardinality(part.this)
            if rows is None:
                continue
            product *= max(rows, 1)
            if product > _MAX_INLINE_ROWS:
                return True
    return False


def _has_nested_loop_correlation(tree: exp.Expr) -> bool:
    """True if a correlated subquery is joined to its outer query by anything
    but an equality.

    A subquery that references an outer column runs once per outer row — the
    same product a join makes, with no exp.Join node anywhere to notice it.
    An equality lets the planner decorrelate it into a hash join; an
    inequality leaves the nested loop.
    """
    for subquery in tree.find_all(exp.Select):
        parent_select = subquery.parent_select
        if parent_select is None:
            continue
        own = {
            _source_alias(part.this)
            for part in _source_parts(subquery)
            if part.this is not None
        }
        # An outer column binds the subquery wherever it appears, not just in
        # WHERE: in a JOIN's ON it decorrelates to a join with no equality —
        # the same nested loop, one rewrite further away.
        where = subquery.args.get("where")
        predicates = [where.this if where is not None else None]
        having = subquery.args.get("having")
        predicates.append(having.this if having is not None else None)
        predicates.extend(
            join.args.get("on") for join in subquery.args.get("joins") or []
        )
        present = [predicate for predicate in predicates if predicate is not None]
        if not present:
            continue
        outer_refs = {
            column.table.lower()
            for predicate in present
            for column in predicate.find_all(exp.Column)
            if column.table and column.table.lower() not in own
        }
        if not outer_refs:
            continue
        pairs = {pair for predicate in present for pair in _equi_join_pairs(predicate)}
        if not any(outer_refs & set(pair) for pair in pairs):
            return True
    return False


def has_unpriceable_shape(sql: str, dialect: str) -> bool:
    """True if the IO byte sum would misprice this query.

    Unparseable SQL counts as unpriceable: if we can't prove the shape is
    safe, we assume the risky answer.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        return True

    if _generates_rows(tree):
        return True
    if _has_inline_row_product(tree):
        return True
    if _has_nested_loop_correlation(tree):
        return True
    return _has_product_join(tree)


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
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        return {}

    bodies = {cte.alias_or_name.lower(): cte.this for cte in tree.find_all(exp.CTE)}
    counts: dict[str, int] = {}

    def walk(node: exp.Expr, weight: int, pending: frozenset[str]) -> None:
        for table in node.find_all(exp.Table):
            parts = [part.name for part in table.parts]
            name = parts[0].lower()
            if len(parts) == 1 and name in bodies:
                # A CTE that references itself would recurse forever, and its
                # depth is the engine's business, not the quote's.
                if name in pending:
                    continue
                walk(bodies[name], weight, pending | {name})
                continue
            key = ".".join(parts).lower()
            counts[key] = counts.get(key, 0) + weight

    # From the query with its WITH detached: a CTE body counts once per
    # reference to it, not once where it is defined.
    walk(_without_cte_bodies(tree), 1, frozenset())
    return counts


def _without_cte_bodies(tree: exp.Expr) -> exp.Expr:
    """The query with its WITH clause detached, so CTE bodies are counted
    where they are referenced rather than once where they are defined."""
    copy = tree.copy()
    for with_clause in list(copy.find_all(exp.With)):
        with_clause.pop()
    return copy
