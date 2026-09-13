"""The Pinot dialect card: what an LLM must know to write Pinot SQL.

The sqlglot dialect is the generic one: measured against Pinot 1.5.1, 15
Pinot shapes re-rendered through validate_query executed unchanged on both
engines, and `mysql` produced byte-identical output.
"""

from lagaam.core.models import DialectCard

PINOT_DIALECT_CARD = DialectCard(
    engine="Pinot",
    sqlglot_dialect="",
    rules=[
        "Names have three levels: pinot.default.table — pinot is the only catalog",
        "Quote identifiers with double quotes; strings use single quotes",
        "Table and column names are case-insensitive",
        "Time columns are epoch numbers: convert with DATETIMECONVERT, DATETRUNC or ToDateTime",
        "Always filter on the table's time column — that is what prunes segments",
        "Prefer DISTINCTCOUNTHLL(x) over DISTINCTCOUNT(x) on large tables",
        "A type ending in [] is a multi-value column: use ARRAYLENGTH/ARRAY functions, not scalar comparisons",
        "Name columns explicitly; SELECT * is rejected",
        "Every query needs a LIMIT; one is added if missing",
        "Joins run on the multi-stage engine and are bounded by a row limit",
    ],
)
