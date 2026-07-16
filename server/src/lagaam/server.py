"""Lagaam MCP server: the only door agents use to reach the engines.

`create_server` takes any QueryEngine, so tests inject fakes and production
wiring (see __main__) injects the Trino adapter.
"""

import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from lagaam.core.allowlist import check_tables_allowed, filter_catalog_metadata
from lagaam.core.audit import AuditLog
from lagaam.core.budget import QueryBudget, enforce_budget
from lagaam.core.errors import LagaamError, TableNotFoundError
from lagaam.core.identity import AgentIdentity
from lagaam.core.models import CatalogMetadata, QueryResult, TableSchema
from lagaam.core.ports import QueryEngine
from lagaam.core.safety import validate_query
from lagaam.core.verification import verify_result

# Cap on rows returned when the budget sets no tighter row limit.
_DEFAULT_ROW_CAP = 1000

# Hints name MCP tools, so they live at the tool surface, not in core.
_RECOVERY_HINTS: dict[type[LagaamError], str] = {
    TableNotFoundError: (
        "Call list_catalogs to see the catalogs, schemas, and tables "
        "you have access to, then retry with an exact name."
    ),
}

F = TypeVar("F", bound=Callable[..., Awaitable[Any]])


def _instrumented(tool: str, identity: AgentIdentity, audit: AuditLog) -> Callable[[F], F]:
    """Boundary every tool gets: translate domain errors AND audit the call.

    Domain errors reach the agent as our teachable text (never a stack trace)
    and are logged as a denial with the reason; success is logged as allowed.
    A failed audit write can't break the call — AuditLog swallows sink errors.
    """

    def decorate(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                result = await func(*args, **kwargs)
                audit.record(identity.name, tool, "allowed", dict(kwargs))
                return result
            except LagaamError as exc:
                audit.record(
                    identity.name, tool, "denied", {**kwargs, "reason": str(exc)}
                )
                hint = _RECOVERY_HINTS.get(type(exc))
                raise ToolError(f"{exc} {hint}" if hint else str(exc)) from exc

        return wrapper  # type: ignore[return-value]

    return decorate


def create_server(
    engine: QueryEngine,
    budget: QueryBudget | None = None,
    identity: AgentIdentity | None = None,
    audit: AuditLog | None = None,
) -> FastMCP:
    budget = budget or QueryBudget()
    identity = identity or AgentIdentity(name="anonymous")
    audit = audit or AuditLog()
    # The row cap actually applied: the budget's, or the default if unset.
    row_cap = budget.max_rows or _DEFAULT_ROW_CAP
    mcp = FastMCP("lagaam", stateless_http=True, json_response=True)

    @mcp.tool()
    @_instrumented("list_catalogs", identity, audit)
    async def list_catalogs() -> CatalogMetadata:
        """List every catalog, schema, and table you are allowed to query.

        Call this first to ground yourself before describing tables or
        writing SQL — table names you have not seen here are guesses.
        """
        return filter_catalog_metadata(await engine.list_catalogs(), identity)

    @mcp.tool()
    @_instrumented("describe_table", identity, audit)
    async def describe_table(catalog: str, schema: str, table: str) -> TableSchema:
        """Get the exact columns and types of one table.

        Always describe a table before querying it; column names you have
        not seen here are guesses.
        """
        _require_table_allowed(catalog, schema, table)
        return await engine.describe_table(catalog, schema, table)

    @mcp.tool()
    @_instrumented("query_data", identity, audit)
    async def query_data(sql: str) -> QueryResult:
        """Run a read-only SELECT and get the rows back.

        Write a single SELECT in the engine's dialect. The query is checked
        for safety, priced against your budget, and executed with a row cap —
        so name the columns you need (no SELECT *), and add WHERE filters to
        keep the scan small. If it is rejected, the message says what to fix.
        Describe the tables first so column and table names are exact.
        """
        # validate (U3) -> allowlist (U7) -> estimate (U4) -> budget (U5) ->
        # execute. Inject cap +1 so execute can flag truncation; it returns
        # at most row_cap.
        dialect = engine.dialect().sqlglot_dialect
        safe_sql = validate_query(sql, dialect, default_limit=row_cap + 1)
        check_tables_allowed(safe_sql, dialect, identity)
        estimate = await engine.estimate_cost(safe_sql)
        enforce_budget(estimate, budget)
        result = await engine.execute(safe_sql, row_cap, budget.timeout_seconds)
        result.warnings = verify_result(result)
        return result

    def _require_table_allowed(catalog: str, schema: str, table: str) -> None:
        # describe_table takes parts, not SQL — build the SELECT the allowlist
        # check understands so one code path guards both tools.
        check_tables_allowed(
            f"SELECT 1 FROM {catalog}.{schema}.{table}", "trino", identity
        )

    return mcp
