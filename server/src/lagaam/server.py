"""Lagaam MCP server: the only door agents use to reach the engines.

`create_server` takes any QueryEngine, so tests inject fakes and production
wiring (see __main__) injects the Trino adapter.
"""

import functools
import inspect
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
from importlib.metadata import version
from typing import Annotated, Any, TypeVar

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations
from pydantic import Field

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

# No tool writes, and none can reach past the engine configured at startup.
_READ_ONLY = ToolAnnotations(readOnlyHint=True, openWorldHint=False)

_Catalog = Annotated[
    str,
    Field(
        description="Catalog name as list_catalogs shows it. "
        "On Pinot it is always `pinot`."
    ),
]
_Schema = Annotated[
    str,
    Field(
        description="Schema name as list_catalogs shows it. "
        "On Pinot a schema is a Pinot database, such as `default`."
    ),
]
_Table = Annotated[str, Field(description="Table name as list_catalogs shows it.")]
_Sql = Annotated[
    str,
    Field(
        description="One SELECT in the engine's dialect, naming each table "
        "as catalog.schema.table and each column it needs."
    ),
]

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
            # Derived from the real signature, so it cannot drift from it and
            # a positional call audits the same detail a keyword call does.
            bound = signature.bind_partial(*args, **kwargs)
            bound.apply_defaults()
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
    # FastMCP 1.x takes no version, so the handshake would report the SDK's own.
    mcp._mcp_server.version = version("lagaam")

    @mcp.tool(title="List catalogs", annotations=_READ_ONLY)
    @_instrumented("list_catalogs", identity, audit)
    async def list_catalogs() -> CatalogMetadata:
        """List every catalog, schema, and table you are allowed to query.

        Call this first to ground yourself before describing tables or
        writing SQL — table names you have not seen here are guesses.
        """
        return filter_catalog_metadata(await engine.list_catalogs(), identity)

    @mcp.tool(title="Describe table", annotations=_READ_ONLY)
    @_instrumented("describe_table", identity, audit)
    async def describe_table(
        catalog: _Catalog, schema: _Schema, table: _Table
    ) -> TableSchema:
        """Get the exact columns and types of one table. Read-only.

        Always describe a table before querying it; column names you have
        not seen here are guesses. It reads the engine's metadata and
        statistics, never the table's rows. A table outside your grant is
        refused before the engine is asked; a missing one errors with a
        pointer to list_catalogs. Answers are cached, 5 minutes by default,
        so a new column can lag. row_estimate is the engine's row-count
        statistic, null when it has none or can't be trusted (views, tables
        without stats, Pinot tables with a realtime part). Column comments
        are null when unset, and always on Pinot.
        """
        _require_table_allowed(catalog, schema, table)
        return await engine.describe_table(catalog, schema, table)

    @mcp.tool(title="Query data", annotations=_READ_ONLY)
    @_instrumented("query_data", identity, audit)
    async def query_data(sql: _Sql) -> QueryResult:
        """Run a read-only SELECT and get the rows back.

        Write a single SELECT in the engine's dialect. The query is checked
        for safety, priced against your budget, and executed with a row cap —
        so name the columns you need (no SELECT *), and add WHERE filters to
        keep the scan small. At most 1,000 rows come back by default (a
        larger LIMIT is lowered to the cap), and `truncated` marks a result
        the cap cut off. A query that touches any table outside your grant
        is refused before it runs. If it is rejected, the message says what
        to fix. Describe the tables first so column and table names are exact.
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
