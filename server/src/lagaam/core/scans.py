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

A table read twice is NOT one of them, despite the obvious worry: measured
against Trino 476, the IO plan emits one entry per *scan operator*, so a
self-join, a UNION of a table with itself, and a twice-referenced CTE each
report two entries and the sum is already right.

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

# Functions whose row count comes from an argument rather than from the data:
# a column reference among their arguments bounds nothing. repeat(col, 10000)
# manufactures 10000 rows per input row while looking column-fed.
_CARDINALITY_FUNCS = {
    "sequence",
    "generate_series",
    "generate_timestamp_array",
    "repeat",
    "array_repeat",
    "ngrams",
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


def _joined_sources(predicate: exp.Expr) -> tuple[str, str] | None:
    """The two aliases a bare column-to-column equality relates, if any.

    Both sides must be plain columns: `a.k + b.k = 5` and `a.k = (SELECT ...)`
    are equalities the engine cannot hash on, so they leave a nested loop.
    """
    if not isinstance(predicate, exp.EQ):
        return None
    left, right = predicate.left, predicate.right
    if not (isinstance(left, exp.Column) and isinstance(right, exp.Column)):
        return None
    if not (left.table and right.table):
        return None
    one, two = left.table.lower(), right.table.lower()
    return None if one == two else (one, two)


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


def _generates_rows(tree: exp.Expr) -> bool:
    """True if the query invents rows the IO plan cannot see.

    UNNEST over a column expands rows of a table already in the plan; UNNEST
    over sequence(...) or repeat(...) manufactures them from an argument.
    """
    for generator in tree.find_all(*_GENERATORS):
        for func in generator.find_all(exp.Func):
            name = (func.sql_name() or func.name or "").lower()
            if name in _CARDINALITY_FUNCS:
                return True
        if _expands_a_bounded_value(generator):
            continue
        if not list(generator.find_all(exp.Column)):
            # No column feeding it: whatever it expands is literal or computed.
            return True
    return False


def _expands_a_bounded_value(generator: exp.Expr) -> bool:
    """True if the generator's input spells out its own length.

    UNNEST(ARRAY['O','F']) is a two-row lookup table an agent writes inline;
    it invents nothing the plan needs to price.
    """
    inputs = [generator.this, *(generator.expressions or [])]
    return bool(inputs) and all(
        isinstance(value, exp.Array | exp.Struct)
        for value in inputs
        if value is not None
    )


def _source_alias(source: exp.Expr) -> str | None:
    """The name a predicate would use to refer to this join input."""
    if isinstance(source, exp.Alias | exp.Subquery) and source.alias:
        return str(source.alias).lower()
    if isinstance(source, exp.Table):
        return (source.alias or source.name).lower()
    return None


def _has_product_join(tree: exp.Expr) -> bool:
    """True if any join pairs rows without an equality to match them on."""
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


def _bounded_source(source: exp.Expr) -> bool:
    """True if this join input contributes a fixed or already-counted number
    of rows, so pairing against it is not a product.

    `CROSS JOIN (SELECT 0.2 AS rate)` is how an agent parameterizes a query,
    and `CROSS JOIN UNNEST(o.items)` is how it reads a nested column — the
    rows come from a table the IO plan already priced.
    """
    if isinstance(source, exp.Alias | exp.Subquery):
        source = source.this
    if isinstance(source, _GENERATORS):
        return not _generates_rows(source)
    if isinstance(source, exp.Values):
        return True
    if isinstance(source, exp.Query):
        # A subquery reading no table can only produce the rows it spells out.
        return not list(source.find_all(exp.Table))
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
    return _has_product_join(tree)


def scan_multiplier(sql: str, dialect: str) -> int:
    """How many times over the IO byte sum may undercount this query.

    Measured against Trino 476: the plan emits one entry per distinct
    (table, column-set), so two scans of the same table reading the *same*
    columns collapse into one entry — `orders a JOIN orders b ON
    a.orderkey = b.orderkey` reports a single scan and bills half the work.
    Two scans reading different columns get an entry each and need no
    correction, but the plan does not say which case it is, so the quote is
    scaled by the worst one the SQL admits.

    Returns 1 for unparseable SQL: the shape check has already refused it.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        return 1

    counts: dict[str, int] = {}
    for table in tree.find_all(exp.Table):
        key = ".".join(part.name for part in table.parts).lower()
        counts[key] = counts.get(key, 0) + 1
    return max(counts.values(), default=1)
