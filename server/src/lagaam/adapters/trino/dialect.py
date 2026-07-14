"""The Trino dialect card: what an LLM must know to write Trino SQL."""

from lagaam.core.models import DialectCard

TRINO_DIALECT_CARD = DialectCard(
    engine="Trino",
    sqlglot_dialect="trino",
    rules=[
        "Names have three levels: catalog.schema.table",
        "Quote identifiers with double quotes, never backticks",
        "Strings use single quotes; || concatenates",
        "No implicit casts: use CAST(x AS type); integer division truncates",
        "Dates: date_trunc('day', ts), ts + INTERVAL '7' DAY, DATE '2026-01-01'",
        "Prefer approx_distinct(x) over COUNT(DISTINCT x) on large tables",
        "Filter on partition columns whenever the table has them",
        "Name columns explicitly; SELECT * is rejected",
        "Every query needs a LIMIT; one is added if missing",
    ],
)
