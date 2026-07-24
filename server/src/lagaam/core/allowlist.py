"""Enforce an agent's table allowlist against the SQL it wants to run.

Least privilege: a hijacked agent can't read past its lane. The check runs on
the already-validated SQL and fails closed — any table it cannot resolve to a
fully-qualified name (unqualified, or unparseable SQL) is denied, because a
name we can't pin down is a name we can't vouch is in the allowlist.
"""

import sqlglot
from sqlglot import exp

from lagaam.core.errors import TableAccessDeniedError
from lagaam.core.identifiers import IdentifierError, normalize_grant, table_fqn
from lagaam.core.identity import AgentIdentity
from lagaam.core.models import CatalogInfo, CatalogMetadata, SchemaInfo


def check_tables_allowed(
    sql: str, dialect: str, identity: AgentIdentity
) -> None:
    """Raise TableAccessDeniedError if the SQL touches a disallowed table."""
    allowed = identity.normalized_allowlist()
    if allowed is None:
        return  # unrestricted

    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        raise TableAccessDeniedError(
            "The query could not be parsed to check table access, so it is "
            "denied. Send a single valid SELECT with fully-qualified names."
        )

    ctes = {c.alias_or_name.lower() for c in tree.find_all(exp.CTE)}
    for table in tree.find_all(exp.Table):
        # A bare reference to a CTE is a local alias, not a base table.
        if table.name.lower() in ctes and not table.catalog and not table.db:
            continue
        rendered = table.sql(dialect=dialect)
        try:
            fqn = table_fqn(table)
        except IdentifierError as exc:
            raise TableAccessDeniedError(
                f"Table '{rendered}' cannot be checked against your grant "
                f"({exc}). Use plain catalog.schema.table names."
            ) from exc
        if fqn not in allowed:
            raise TableAccessDeniedError(
                f"Access to {rendered} is not permitted for this agent. "
                "Query only the tables in your grant; call list_catalogs "
                "to see them."
            )


def table_parts_allowed(
    catalog: str, schema: str, table: str, allowed: frozenset[str] | set[str]
) -> bool:
    """Is this already-resolved three-part name in the grant?

    For names that arrive as parts rather than SQL — engine-reported metadata,
    and describe_table's arguments. Folded the same way ``table_fqn`` folds
    parsed SQL, so a connector reporting uppercase names (Oracle, Snowflake)
    still grounds an agent whose grant is written lowercase. A name no grant
    could express — non-ASCII, or one carrying a dot — matches nothing.
    """
    try:
        return normalize_grant(f"{catalog}.{schema}.{table}") in allowed
    except IdentifierError:
        return False


def filter_catalog_metadata(
    metadata: CatalogMetadata, identity: AgentIdentity
) -> CatalogMetadata:
    """Return only the catalogs/schemas/tables the agent may query.

    list_catalogs is the agent's grounding — showing tables outside the grant
    both leaks metadata and teaches the agent names it will only be denied on.
    Catalogs and schemas left with no visible tables are dropped entirely.
    """
    allowed = identity.normalized_allowlist()
    if allowed is None:
        return metadata

    catalogs: list[CatalogInfo] = []
    for catalog in metadata.catalogs:
        schemas: list[SchemaInfo] = []
        for schema in catalog.schemas:
            tables = [
                t
                for t in schema.tables
                if table_parts_allowed(catalog.name, schema.name, t, allowed)
            ]
            if tables:
                schemas.append(SchemaInfo(name=schema.name, tables=tables))
        if schemas:
            catalogs.append(
                CatalogInfo(
                    name=catalog.name, schemas=schemas, truncated=catalog.truncated
                )
            )
    return CatalogMetadata(catalogs=catalogs)
