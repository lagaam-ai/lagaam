"""Trino adapter for the QueryEngine port.

The trino dbapi client is blocking, so every call runs in a worker thread;
the async surface is what the MCP server needs. All engine failures leave
this module as LagaamError subclasses — raw trino exceptions never escape.
"""

import math
import os

import anyio.to_thread
import trino.dbapi
import trino.exceptions

from lagaam.adapters.trino.dialect import TRINO_DIALECT_CARD
from lagaam.adapters.trino.explain import finite_number, parse_io_estimate
from lagaam.core.errors import (
    EngineError,
    LagaamError,
    QueryFailedError,
    TableNotFoundError,
)
from lagaam.core.query_errors import hint_for_engine_error, is_self_correctable
from lagaam.core.identifiers import quote_identifier
from lagaam.core.scans import has_unpriceable_shape
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    ColumnInfo,
    CostEstimate,
    DialectCard,
    QueryResult,
    SchemaInfo,
    TableSchema,
)

_NOT_FOUND_ERRORS = {"CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND"}

# Present in every catalog; protocol plumbing, not grounding material.
_HIDDEN_SCHEMAS = {"information_schema"}


# trino.exceptions.HttpError derives from Exception, not from Error, so it
# escapes an `except Error` — and it is what a coordinator restart, an LB, or
# expired auth raises.
_ENGINE_FAILURES = (trino.exceptions.Error, trino.exceptions.HttpError, OSError)


def _detail(exc: Exception) -> str:
    """Agent-safe failure text: exc.message, never str(exc), which leaks
    the query id."""
    if isinstance(exc, trino.exceptions.HttpError):
        # HttpError.message carries the raw response body — a 401 body can
        # hold token material, so the agent gets the status class only.
        return "the query engine is not reachable right now"
    return getattr(exc, "message", None) or str(exc)


def _translate_error(exc: Exception) -> LagaamError:
    """Map a raw engine failure to a domain error.

    A recognised error_name (bad column, timeout, memory — regardless of the
    Trino subclass it arrives as) is the agent's to fix, so it becomes a
    teachable QueryFailedError. Everything else is an engine fault.
    """
    error_name = getattr(exc, "error_name", None)
    if is_self_correctable(error_name):
        return QueryFailedError(hint_for_engine_error(error_name))
    return EngineError(_detail(exc))


class TrinoEngine:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        user: str = "lagaam",
        max_tables_per_catalog: int = 1000,
    ) -> None:
        self._host = host
        self._port = port
        self._user = user
        self._max_tables = max_tables_per_catalog

    @classmethod
    def from_env(cls) -> "TrinoEngine":
        kwargs: dict[str, str | int] = {}
        if "TRINO_HOST" in os.environ:
            kwargs["host"] = os.environ["TRINO_HOST"]
        if "TRINO_PORT" in os.environ:
            kwargs["port"] = int(os.environ["TRINO_PORT"])
        if "TRINO_USER" in os.environ:
            kwargs["user"] = os.environ["TRINO_USER"]
        return cls(**kwargs)  # type: ignore[arg-type]

    async def list_catalogs(self) -> CatalogMetadata:
        try:
            return await anyio.to_thread.run_sync(self._list_catalogs)
        except _ENGINE_FAILURES as exc:
            raise EngineError(_detail(exc)) from exc

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        try:
            return await anyio.to_thread.run_sync(
                self._describe_table, catalog, schema, table
            )
        except _ENGINE_FAILURES as exc:
            raise EngineError(_detail(exc)) from exc

    def dialect(self) -> DialectCard:
        return TRINO_DIALECT_CARD

    async def estimate_cost(self, sql: str) -> CostEstimate:
        try:
            return await anyio.to_thread.run_sync(self._estimate_cost, sql)
        except _ENGINE_FAILURES as exc:
            raise _translate_error(exc) from exc

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        try:
            return await anyio.to_thread.run_sync(
                self._execute, sql, max_rows, timeout_seconds
            )
        except _ENGINE_FAILURES as exc:
            raise _translate_error(exc) from exc

    def _connect(
        self, session_properties: dict[str, str] | None = None
    ) -> trino.dbapi.Connection:
        return trino.dbapi.connect(
            host=self._host,
            port=self._port,
            user=self._user,
            session_properties=session_properties,
        )

    def _list_catalogs(self) -> CatalogMetadata:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SHOW CATALOGS")
            catalog_names = [row[0] for row in cur.fetchall()]

            # Serial queries are fine behind the cache; LIMIT bounds monster catalogs.
            hidden = ", ".join(f"'{s}'" for s in sorted(_HIDDEN_SCHEMAS))
            catalogs: list[CatalogInfo] = []
            for name in catalog_names:
                try:
                    cur.execute(
                        "SELECT table_schema, table_name "
                        f"FROM {quote_identifier(name)}.information_schema.tables "
                        f"WHERE table_schema NOT IN ({hidden}) "
                        "ORDER BY table_schema, table_name "
                        f"LIMIT {self._max_tables + 1}"
                    )
                    rows = cur.fetchall()
                except (trino.exceptions.TrinoQueryError, ValueError):
                    # One broken catalog must not cost the grounding for healthy ones.
                    continue
                truncated = len(rows) > self._max_tables
                schemas: dict[str, list[str]] = {}
                for table_schema, table_name in rows[: self._max_tables]:
                    schemas.setdefault(table_schema, []).append(table_name)
                catalogs.append(
                    CatalogInfo(
                        name=name,
                        schemas=[
                            SchemaInfo(name=s, tables=t) for s, t in schemas.items()
                        ],
                        truncated=truncated,
                    )
                )
            return CatalogMetadata(catalogs=catalogs)

    def _describe_table(self, catalog: str, schema: str, table: str) -> TableSchema:
        try:
            quoted = ".".join(quote_identifier(p) for p in (catalog, schema, table))
        except ValueError:
            raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"SHOW COLUMNS FROM {quoted}")
                rows = cur.fetchall()
            except trino.exceptions.TrinoUserError as exc:
                if exc.error_name in _NOT_FOUND_ERRORS:
                    raise TableNotFoundError(
                        catalog=catalog, schema=schema, table=table
                    ) from exc
                # exc.message, not str(exc): repr leaks query ids and noise.
                raise EngineError(exc.message or type(exc).__name__) from exc
            # SHOW COLUMNS rows: (column, type, extra, comment)
            columns = [
                ColumnInfo(name=row[0], type=row[1], comment=row[3] or None)
                for row in rows
            ]
            # Echo canonical names so the card matches list_catalogs output.
            return TableSchema(
                catalog=catalog.lower(),
                schema_name=schema.lower(),
                table=table.lower(),
                columns=columns,
                row_estimate=self._row_estimate(cur, quoted),
            )

    def _execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None
    ) -> QueryResult:
        # query_max_run_time caps wall-clock; Trino kills the query past it.
        # Round up: a sub-second budget must never render as "0s" (no cap).
        props = (
            {"query_max_run_time": f"{math.ceil(timeout_seconds)}s"}
            if timeout_seconds is not None
            else None
        )
        with self._connect(props) as conn:
            cur = conn.cursor()
            cur.execute(sql)
            # Fetch one extra to detect truncation without a second query.
            rows = cur.fetchmany(max_rows + 1)
            columns = [d[0] for d in cur.description or []]
        truncated = len(rows) > max_rows
        capped = [list(r) for r in rows[:max_rows]]
        return QueryResult(
            columns=columns,
            rows=capped,
            row_count=len(capped),
            truncated=truncated,
        )

    def _estimate_cost(self, sql: str) -> CostEstimate:
        # Repeated scans, cross joins, and row generators all break the IO
        # plan's byte sum — don't vouch for a quote it would misprice.
        if has_unpriceable_shape(sql, TRINO_DIALECT_CARD.sqlglot_dialect):
            return CostEstimate(confidence="low")
        # TYPE IO plans the query without running it; NEVER use ANALYZE here.
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(f"EXPLAIN (TYPE IO, FORMAT JSON) {sql}")
            row = cur.fetchone()
        # A cancelled or degenerate plan returns no row; that is no quote, not
        # a crash, and no quote fails safe at the gate.
        return parse_io_estimate(row[0]) if row else CostEstimate(confidence="low")

    @staticmethod
    def _row_estimate(cur: trino.dbapi.Cursor, quoted: str) -> int | None:
        """Best-effort row count from engine statistics; never fails a describe."""
        try:
            cur.execute(f"SHOW STATS FOR {quoted}")
            rows = cur.fetchall()
        except trino.exceptions.TrinoQueryError:
            return None  # views and stats-less connectors have no SHOW STATS
        for row in rows:
            # The summary row has column_name None and carries row_count.
            if row[0] is None:
                # SHOW STATS reports row_count as a DOUBLE, and a stats-less
                # table reports NaN — round() raises on that.
                count = finite_number(row[4])
                return round(count) if count is not None else None
        return None
