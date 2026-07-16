"""Enforce an agent's table allowlist against the SQL it wants to run.

Least privilege: a hijacked agent can't read past its lane. The check runs on
the already-validated SQL and fails closed — any table it cannot resolve to a
fully-qualified name (unqualified, or unparseable SQL) is denied, because a
name we can't pin down is a name we can't vouch is in the allowlist.
"""

import sqlglot
from sqlglot import exp

from lagaam.core.errors import TableAccessDeniedError
from lagaam.core.identity import AgentIdentity


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
        if not table.catalog or not table.db:
            raise TableAccessDeniedError(
                f"Table '{table.name}' is not fully qualified, so access "
                "cannot be checked. Use catalog.schema.table names."
            )
        fqn = f"{table.catalog}.{table.db}.{table.name}".lower()
        if fqn not in allowed:
            raise TableAccessDeniedError(
                f"Access to {table.catalog}.{table.db}.{table.name} is not "
                "permitted for this agent. Query only the tables in your "
                "grant; call list_catalogs to see them."
            )
