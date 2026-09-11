"""Three-part names for the agent, two-part names for Pinot.

Pinot's namespace is exactly database.table; a third part is an HTTP 500 with
a non-JSON body on the broker's own parser. Core's grant format, allowlist,
cache key and describe_table are all three-part, so the adapter presents a
synthetic catalog named `pinot` and strips exactly that part here.

This runs after validate_query and check_tables_allowed, so a bare name can
only be a CTE the allowlist already vouched for — bare names are left alone,
and only a qualified name is stripped or refused. Refusing a qualified name
we do not recognise is the security boundary: no request is made at all.
"""

import sqlglot
from sqlglot import exp

from lagaam.core.errors import TableNotFoundError


def two_part_sql(sql: str, catalog: str = "pinot") -> str:
    """Validated SQL with the synthetic catalog dropped from every table."""
    try:
        tree = sqlglot.parse_one(sql, dialect="")
    except sqlglot.errors.SqlglotError as exc:
        raise TableNotFoundError(catalog=catalog, schema="?", table="?") from exc

    for table in tree.find_all(exp.Table):
        found = table.catalog
        if not found:
            # A bare name here is a CTE; a base table without a catalog never
            # got past the allowlist, so refuse a schema-qualified bare table.
            if table.db:
                raise TableNotFoundError(
                    catalog="", schema=table.db, table=table.name
                )
            continue
        if found.lower() != catalog.lower():
            raise TableNotFoundError(
                catalog=found, schema=table.db, table=table.name
            )
        table.set("catalog", None)

    return tree.sql(dialect="", comments=False)
