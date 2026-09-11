"""Three-part names for the agent, two-part names for Pinot.

Pinot's namespace is exactly database.table; a third part is an HTTP 500 with
a non-JSON body on the broker's own parser. Core's grant format, allowlist,
cache key and describe_table are all three-part, so the adapter presents a
synthetic catalog named `pinot` and strips exactly that part here — from both
table references and fully qualified column references.

This runs after validate_query and check_tables_allowed, so a bare name can
only be a CTE the allowlist already vouched for — bare names are left alone,
and only a qualified name is stripped or refused. Refusing a qualified name
we do not recognise is the security boundary: no request is made at all.
"""

import sqlglot
from sqlglot import exp

from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.core.errors import SqlValidationError, TableNotFoundError

_DIALECT = PINOT_DIALECT_CARD.sqlglot_dialect


def two_part_sql(sql: str, catalog: str = "pinot") -> str:
    """Validated SQL with the synthetic catalog dropped from every table."""
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except sqlglot.errors.SqlglotError as exc:
        raise SqlValidationError(
            "The SQL uses a construct this server cannot re-read safely. "
            "Rewrite it more simply and retry."
        ) from exc

    for table in tree.find_all(exp.Table):
        found = table.catalog
        if not found:
            # Bare name: a CTE, unless LAGAAM_ALLOW_ALL_TABLES let a schema-only table through.
            if table.db:
                rendered = table.sql(dialect=_DIALECT)
                raise SqlValidationError(
                    f"Table {rendered} is not a {catalog}.<database>.<table> "
                    f"name. Name every table as {catalog}.<database>.<table>, "
                    "then retry."
                )
            continue
        if found.lower() != catalog.lower():
            raise TableNotFoundError(catalog=found, schema=table.db, table=table.name)
        table.set("catalog", None)

    for column in tree.find_all(exp.Column):
        col_catalog = column.args.get("catalog")
        if not isinstance(col_catalog, exp.Identifier):
            continue
        col_catalog_name = col_catalog.this
        if col_catalog_name.lower() != catalog.lower():
            raise TableNotFoundError(
                catalog=col_catalog_name, schema=column.db, table=column.table
            )
        column.set("catalog", None)

    return tree.sql(dialect=_DIALECT, comments=False)
