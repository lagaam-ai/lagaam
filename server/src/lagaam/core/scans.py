"""Spot queries whose EXPLAIN byte estimate would undercount physical scans.

Trino's IO plan lists one entry per *distinct table*, not per scan operator.
So a table read by several operators — a self-join, a UNION of a table with
itself, a CTE referenced more than once, a scalar subquery over the same
table — is billed once, and the quote can be an N× underestimate at high
confidence. That is the one failure the cost gate must never make.

We can't recover the true count from the IO JSON, so we detect the shape from
the SQL and fail safe: any table (or CTE) referenced more than once means the
quote is untrustworthy. This over-approximates — Trino may dedup some of these
— but a false "high risk" is safe; a false "cheap" is not.
"""

import sqlglot
from sqlglot import exp


def has_repeated_scan(sql: str, dialect: str) -> bool:
    """True if any base table or CTE is referenced more than once.

    Unparseable SQL is treated as repeated: if we can't prove a single scan,
    we assume the risky answer.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        return True

    counts: dict[str, int] = {}
    for table in tree.find_all(exp.Table):
        name = table.name.lower()
        counts[name] = counts.get(name, 0) + 1
    return any(n > 1 for n in counts.values())
