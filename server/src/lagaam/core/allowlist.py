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

    for table in tree.find_all(exp.Table):
        # A bare reference to a CTE is a local alias, not a base table — but
        # only where that CTE is in scope. A tree-wide name set lets a CTE
        # buried in a subquery vouch for the same bare name in an outer scope,
        # where the engine resolves it to a real table.
        if not table.catalog and not table.db and _cte_in_scope(table):
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


def _cte_in_scope(table: exp.Table) -> bool:
    """Is this bare name declared by a WITH clause enclosing it?

    Walks outward from the reference, so a CTE only vouches for names inside
    the query that declares it. A CTE also cannot vouch for a reference in
    its own definition, which is where a self-referencing name resolves to
    the base table instead.
    """
    name = table.name.lower()
    node: exp.Expr | None = table
    while node is not None:
        # sqlglot spells the arg "with_" on Query nodes; accept both so a
        # rename cannot silently turn this check off.
        with_clause = node.args.get("with_") or node.args.get("with")
        if isinstance(with_clause, exp.With):
            recursive = bool(with_clause.args.get("recursive"))
            for cte in with_clause.expressions:
                if cte.alias_or_name.lower() != name:
                    continue
                # Only a RECURSIVE CTE binds its own name inside its body.
                if not recursive and _within(table, cte):
                    continue
                return True
        node = node.parent
    return False


def _within(node: exp.Expr, ancestor: exp.Expr) -> bool:
    """Does ``node`` sit inside ``ancestor``'s subtree?"""
    walk: exp.Expr | None = node
    while walk is not None:
        if walk is ancestor:
            return True
        walk = walk.parent
    return False


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
