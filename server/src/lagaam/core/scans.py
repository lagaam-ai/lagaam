"""Spot queries whose EXPLAIN byte estimate would misprice the real scan.

Trino's IO plan lists one entry per *distinct table*, not per scan operator,
and the quote is the sum of those entries. Three shapes make that sum a lie:

- A table read by several operators — a self-join, a UNION of a table with
  itself, a CTE referenced more than once, a scalar subquery over the same
  table — is billed once, so the quote can be an N× underestimate.
- A join that is not an equi-join produces a product: both inputs are still
  scanned exactly once, so the byte sum is *correct and irrelevant* while the
  row work is quadratic. `ON 1=1` and `ON a.k <> b.k` cost the same bytes as
  a healthy join and 10^11 times the work.
- A row generator like UNNEST(sequence(...)) contributes no table entry at
  all, so it is invisible to the byte sum entirely.

We can't recover the true cost from the IO JSON, so we detect these shapes
from the SQL and fail safe. The rules below deliberately exempt the shapes
that look risky but aren't — a constant-row join, an UNNEST over a column
already counted — because a gate that blocks ordinary analytics is a failed
gate, not a strict one.
"""

import sqlglot
from sqlglot import exp

# Row generators fed by a *column* are bounded by the table already counted;
# only a generated series invents rows the IO plan never sees.
_GENERATORS = (exp.Unnest, exp.Explode, exp.Posexplode)
_SERIES_FUNCS = {"sequence", "generate_series", "generate_timestamp_array"}


def _scan_key(table: exp.Table) -> str:
    """Full name of a table, so same-named tables in different catalogs or
    schemas are not mistaken for one table scanned twice."""
    return ".".join(part.name for part in table.parts).lower()


def _sources(node: exp.Expr) -> set[str]:
    """Table names and aliases a predicate's column references resolve to."""
    names = set()
    for column in node.find_all(exp.Column):
        if column.table:
            names.add(column.table.lower())
    return names


def _is_equi_join(condition: exp.Expr | None) -> bool:
    """True if the predicate joins two different sources by equality.

    A nested-loop join reads both sides once — so the IO byte sum is right and
    useless. Only an equality between columns of two distinct sources lets the
    engine hash or merge instead of comparing every pair.
    """
    if condition is None:
        return False
    for eq in condition.find_all(exp.EQ):
        if len(_sources(eq.left) | _sources(eq.right)) > 1:
            return True
    return False


def _generates_rows(tree: exp.Expr) -> bool:
    """True if the query invents rows the IO plan cannot see.

    UNNEST over a column expands rows of a table already in the plan; UNNEST
    over sequence(...) manufactures them from nothing.
    """
    for generator in tree.find_all(*_GENERATORS):
        for func in generator.find_all(exp.Anonymous, exp.GenerateSeries):
            name = (func.name or func.sql_name() or "").lower()
            if name in _SERIES_FUNCS:
                return True
        if not list(generator.find_all(exp.Column)):
            # No column feeding it: whatever it expands is literal or computed.
            return True
    return False


def _has_product_join(tree: exp.Expr) -> bool:
    """True if any join pairs rows without an equality to match them on."""
    for join in tree.find_all(exp.Join):
        if join.args.get("using") is not None:
            continue
        if _bounded_source(join.this):
            continue
        condition = join.args.get("on")
        if _is_equi_join(condition):
            continue
        # A comma join carries its equality in the enclosing WHERE, and reads
        # identically to the JOIN..ON form the planner would rewrite it to.
        select = join.find_ancestor(exp.Select)
        where = select.args.get("where") if select else None
        if condition is None and where is not None and _is_equi_join(where):
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
    if _has_product_join(tree):
        return True

    counts: dict[str, int] = {}
    for table in tree.find_all(exp.Table):
        key = _scan_key(table)
        counts[key] = counts.get(key, 0) + 1
    return any(n > 1 for n in counts.values())
