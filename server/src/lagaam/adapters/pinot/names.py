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


def referenced_tables(sql: str, catalog: str = "pinot") -> list[tuple[str, str]] | None:
    """Every (database, table) this SQL reads, deduplicated and sorted.

    None means the SQL did not re-parse, which charges the whole table rather
    than quoting a query nobody read.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return None
    found: set[tuple[str, str]] = set()
    for table in tree.find_all(exp.Table):
        if not table.name:
            continue
        table_catalog = table.catalog
        if table_catalog and table_catalog.lower() != catalog.lower():
            raise TableNotFoundError(
                catalog=table_catalog, schema=table.db, table=table.name
            )
        # A bare name is a CTE the allowlist already vouched for, not a table.
        if not table.db:
            continue
        found.add((table.db, table.name))
    return sorted(found)


def referenced_columns(sql: str) -> frozenset[str] | None:
    """Lowercase bare names of every column this SQL mentions.

    None means a reference nobody can resolve to a column list — a star, or
    SQL that did not re-parse — and the caller charges whole segments for it.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return None
    for star in tree.find_all(exp.Star):
        # count(*) names no column; a projected star names all of them.
        if not isinstance(star.parent, exp.Count):
            return None
    for column in tree.find_all(exp.Column):
        if isinstance(column.this, exp.Star):
            return None
    return frozenset(
        column.name.lower()
        for column in tree.find_all(exp.Column)
        if column.name
    )
