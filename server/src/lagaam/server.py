"""Lagaam MCP server: the only door agents use to reach the engines.

`create_server` takes any QueryEngine, so tests inject fakes and production
wiring (see __main__) injects the Trino adapter.
"""

import functools
import inspect
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from typing import Any, TypeVar

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from lagaam.core.allowlist import (
    check_tables_allowed,
    filter_catalog_metadata,
    table_parts_allowed,
)
from lagaam.core.audit import AuditLog
from lagaam.core.budget import QueryBudget, enforce_budget
from lagaam.core.errors import (
    LagaamError,
    TableAccessDeniedError,
    TableNotFoundError,
)
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

# The in-flight audit detail, so a tool can add what it actually did. Set by
# _instrumented before the tool body runs; a tool reached any other way has
# nothing to contribute to.
_AUDIT_DETAIL: ContextVar[dict[str, Any]] = ContextVar("audit_detail")


def _instrumented(
    tool: str, identity: AgentIdentity, audit: AuditLog
) -> Callable[[F], F]:
    """Boundary every tool gets: translate domain errors AND audit the call.

    Domain errors reach the agent as our teachable text (never a stack trace)
    and are logged as a denial with the reason; success is logged as allowed.
    An unexpected exception is logged as an error and replaced with generic
    text — a bug must not become an unaudited call or a leaked internal path.
    Cancellation is recorded and re-raised untouched: the SQL already reached
    the engine, so the trail must show it even though nobody is listening.
    A failed audit write can't break the call — AuditLog swallows sink errors.
    """

    def decorate(func: F) -> F:
        signature = inspect.signature(func)

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            bound = signature.bind(*args, **kwargs)
            # Derived from the real signature, so it cannot drift from it and
            # a positional call audits the same detail a keyword call does.
            detail: dict[str, Any] = dict(bound.arguments)
            _AUDIT_DETAIL.set(detail)
            recorded = False

            def record(outcome: str, extra: dict[str, Any]) -> None:
                nonlocal recorded
                recorded = True
                audit.record(identity.name, tool, outcome, {**detail, **extra})

            try:
                result = await func(*args, **kwargs)
                record("allowed", {})
                return result
            except LagaamError as exc:
                record("denied", {"reason": str(exc)})
                hint = _RECOVERY_HINTS.get(type(exc))
                raise ToolError(f"{exc} {hint}" if hint else str(exc)) from exc
            except Exception as exc:
                # Type only: the message can carry hostnames, paths, or tokens.
                record("error", {"error": type(exc).__name__})
                raise ToolError(
                    "The server hit an internal error handling this call. "
                    "Retry, and report it if it persists."
                ) from exc
            finally:
                # Cancellation is a BaseException, so it reaches neither
                # handler above — and it is exactly when a query is in flight.
                if not recorded:
                    record("cancelled", {})

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
    # Returned-row cap: distinct from max_rows, which gates rows *scanned*.
    row_cap = budget.max_returned_rows or _DEFAULT_ROW_CAP
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
        detail = _AUDIT_DETAIL.get({})
        safe_sql = validate_query(sql, dialect, default_limit=row_cap + 1)
        # The forensic question is what the engine ran, not what was asked.
        detail["executed_sql"] = safe_sql
        check_tables_allowed(safe_sql, dialect, identity)
        estimate = await engine.estimate_cost(safe_sql)
        detail["estimate"] = estimate.model_dump()
        enforce_budget(estimate, budget)
        result = await engine.execute(safe_sql, row_cap, budget.timeout_seconds)
        detail["row_count"] = result.row_count
        detail["truncated"] = result.truncated
        # Extend: an engine may have attached warnings of its own.
        result.warnings = [*result.warnings, *verify_result(result)]
        return result

    def _require_table_allowed(catalog: str, schema: str, table: str) -> None:
        # describe_table takes name parts, so authorize the parts themselves.
        # Round-tripping them through SQL text would check a name the adapter
        # never runs: "orders -- " parses as `orders` and quotes as itself.
        allowed = identity.normalized_allowlist()
        if allowed is None:
            return
        if not table_parts_allowed(catalog, schema, table, allowed):
            raise TableAccessDeniedError(
                f"Access to {catalog}.{schema}.{table} is not permitted for "
                "this agent. Query only the tables in your grant; call "
                "list_catalogs to see them."
            )

    return mcp
