"""Spot queries whose EXPLAIN byte estimate would misprice the real scan.

Trino's IO plan lists one entry per *distinct table*, not per scan operator,
and the quote is the sum of those entries. Three shapes make that sum a lie:

- A table read by several operators — a self-join, a UNION of a table with
  itself, a CTE referenced more than once, a scalar subquery over the same
  table — is billed once, so the quote can be an N× underestimate.
- A cross join multiplies its inputs while the plan reports each one once, so
  three 100 MB tables quote as 300 MB and do 10^18 rows of work.
- A row generator like UNNEST(sequence(...)) contributes no table entry at
  all, so it is invisible to the byte sum entirely.

We can't recover the true cost from the IO JSON, so we detect these shapes
from the SQL and fail safe. This over-approximates — Trino may dedup or
optimize some of them — but a false "high risk" is recoverable by the agent
and a false "cheap" is not.
"""

import sqlglot
from sqlglot import exp

# Set-returning functions that multiply rows without appearing in the IO plan.
_GENERATORS = (exp.Unnest, exp.Explode, exp.Posexplode)


def _scan_key(table: exp.Table) -> str:
    """Full name of a table, so same-named tables in different catalogs or
    schemas are not mistaken for one table scanned twice."""
    return ".".join(part.name for part in table.parts).lower()


def _has_cross_join(tree: exp.Expr) -> bool:
    """True if any join produces a product rather than a matched subset.

    An explicit CROSS JOIN, and the comma join it desugars from, both parse as
    a join carrying neither ON nor USING.
    """
    for join in tree.find_all(exp.Join):
        if join.args.get("on") is None and join.args.get("using") is None:
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

    if tree.find(*_GENERATORS) is not None:
        return True
    if _has_cross_join(tree):
        return True

    counts: dict[str, int] = {}
    for table in tree.find_all(exp.Table):
        key = _scan_key(table)
        counts[key] = counts.get(key, 0) + 1
    return any(n > 1 for n in counts.values())
