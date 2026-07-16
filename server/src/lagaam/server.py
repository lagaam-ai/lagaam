"""Lagaam MCP server: the only door agents use to reach the engines.

`create_server` takes any QueryEngine, so tests inject fakes and production
wiring (see __main__) injects the Trino adapter.
"""

import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from lagaam.core.budget import QueryBudget, enforce_budget
from lagaam.core.errors import LagaamError, TableNotFoundError
from lagaam.core.models import CatalogMetadata, QueryResult, TableSchema
from lagaam.core.ports import QueryEngine
from lagaam.core.safety import validate_query

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


def _teachable(func: F) -> F:
    """Translate domain errors into agent-facing ToolErrors.

    Every tool gets this boundary: domain errors reach the agent as our
    teachable text (fact + recovery hint), never as the SDK's generic
    "Error executing tool ..." wrapper or a stack trace.
    """

    @functools.wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return await func(*args, **kwargs)
        except LagaamError as exc:
            hint = _RECOVERY_HINTS.get(type(exc))
            raise ToolError(f"{exc} {hint}" if hint else str(exc)) from exc

    return wrapper  # type: ignore[return-value]


def create_server(engine: QueryEngine, budget: QueryBudget | None = None) -> FastMCP:
    budget = budget or QueryBudget()
    # The row cap actually applied: the budget's, or the default if unset.
    row_cap = budget.max_rows or _DEFAULT_ROW_CAP
    mcp = FastMCP("lagaam", stateless_http=True, json_response=True)

    @mcp.tool()
    @_teachable
    async def list_catalogs() -> CatalogMetadata:
        """List every catalog, schema, and table you are allowed to query.

        Call this first to ground yourself before describing tables or
        writing SQL — table names you have not seen here are guesses.
        """
        return await engine.list_catalogs()

    @mcp.tool()
    @_teachable
    async def describe_table(catalog: str, schema: str, table: str) -> TableSchema:
        """Get the exact columns and types of one table.

        Always describe a table before querying it; column names you have
        not seen here are guesses.
        """
        return await engine.describe_table(catalog, schema, table)

    @mcp.tool()
    @_teachable
    async def query_data(sql: str) -> QueryResult:
        """Run a read-only SELECT and get the rows back.

        Write a single SELECT in the engine's dialect. The query is checked
        for safety, priced against your budget, and executed with a row cap —
        so name the columns you need (no SELECT *), and add WHERE filters to
        keep the scan small. If it is rejected, the message says what to fix.
        Describe the tables first so column and table names are exact.
        """
        # validate (U3) -> estimate (U4) -> enforce budget (U5) -> execute.
        # Inject the cap +1 so execute can see one row past it and flag
        # truncation; execute returns at most row_cap.
        safe_sql = validate_query(
            sql, engine.dialect().sqlglot_dialect, default_limit=row_cap + 1
        )
        estimate = await engine.estimate_cost(safe_sql)
        enforce_budget(estimate, budget)
        return await engine.execute(safe_sql, row_cap, budget.timeout_seconds)

    return mcp
