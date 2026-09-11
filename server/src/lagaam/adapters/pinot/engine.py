"""Pinot adapter for the QueryEngine port.

Pinot has no catalog hierarchy, no SHOW TABLES and no information_schema, so
grounding is controller REST and the catalog is synthetic: exactly one, named
`pinot`, whose schemas are Pinot databases. Names are three-part to the agent
and two-part to Pinot. Every failure leaves this module as a LagaamError —
httpx exceptions and broker messages never escape.
"""

import math
import os

import httpx

from lagaam.adapters.pinot.client import PinotClient, PinotTransportError
from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.adapters.pinot.metadata import table_names, table_schema
from lagaam.adapters.pinot.names import two_part_sql
from lagaam.adapters.pinot.response import parse_query_result, result_failure
from lagaam.core.errors import EngineError, QueryFailedError, TableNotFoundError
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    QueryResult,
    SchemaInfo,
    TableSchema,
)
from lagaam.core.query_errors import hint_for_engine_error, is_self_correctable

_UNREACHABLE = "the query engine is not reachable right now"

# Pinot's namespace is database.table and `default` always exists; 1.5.1 does
# expose GET /databases, but an older controller answering 404 still grounds.
_DEFAULT_DATABASE = "default"

# Pinot silently ignores an option name it does not know, so a typo here would
# disable a cap with no signal at all; the integration tests trip each one.
_OPT_MULTISTAGE = "useMultistageEngine"
_OPT_TIMEOUT_MS = "timeoutMs"
_OPT_MAX_ROWS_IN_JOIN = "maxRowsInJoin"
_OPT_MAX_ROWS_IN_WINDOW = "maxRowsInWindow"
_OPT_MAX_RESPONSE_BYTES = "maxQueryResponseSizeBytes"

# Backstop only: the broker must hit its own timeoutMs and answer first.
_TIMEOUT_GRACE_SECONDS = 5.0


class PinotEngine:
    CATALOG = "pinot"

    # A fixed ceiling on what one answer may weigh; the only byte-denominated
    # control Pinot offers, and the agent's row cap is not one.
    MAX_QUERY_RESPONSE_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        controller_url: str = "http://localhost:9000",
        broker_url: str = "http://localhost:8000",
        user: str | None = None,
        password: str | None = None,
        max_tables_per_catalog: int = 1000,
        max_intermediate_rows: int | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._controller_url = controller_url
        self._broker_url = broker_url
        self._max_tables = max_tables_per_catalog
        # The port's execute() carries no row budget, so the engine is told
        # once and spends it on the broker's own maxRowsInJoin/InWindow caps.
        self._max_intermediate_rows = max_intermediate_rows
        self._client = PinotClient(
            controller_url=controller_url,
            broker_url=broker_url,
            user=user,
            password=password,
            transport=transport,
        )

    @classmethod
    def from_env(
        cls, transport: httpx.AsyncBaseTransport | None = None
    ) -> "PinotEngine":
        rows = os.environ.get("LAGAAM_MAX_INTERMEDIATE_ROWS")
        return cls(
            controller_url=os.environ.get(
                "PINOT_CONTROLLER_URL", "http://localhost:9000"
            ),
            broker_url=os.environ.get("PINOT_BROKER_URL", "http://localhost:8000"),
            user=os.environ.get("PINOT_USER"),
            password=os.environ.get("PINOT_PASSWORD"),
            max_intermediate_rows=int(rows) if rows else None,
            transport=transport,
        )

    def dialect(self) -> DialectCard:
        return PINOT_DIALECT_CARD

    async def list_catalogs(self) -> CatalogMetadata:
        try:
            databases = await self._databases()
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
        schemas: list[SchemaInfo] = []
        truncated = False
        attempted = 0
        for database in databases:
            try:
                PinotClient.path_part(database)
            except ValueError:
                # Non-ASCII cannot be granted either — see core.normalize_grant.
                continue
            attempted += 1
            try:
                body = await self._client.controller_get("/tables", database=database)
            except PinotTransportError:
                # One broken database must not cost the grounding for healthy ones.
                continue
            if body is PinotClient.NotFound:
                continue
            names = table_names(body)
            budget = self._max_tables - sum(len(s.tables) for s in schemas)
            if len(names) > budget:
                truncated = True
                names = names[:budget]
            schemas.append(SchemaInfo(name=database, tables=names))
        if attempted and not schemas:
            raise EngineError(_UNREACHABLE)
        return CatalogMetadata(
            catalogs=[
                CatalogInfo(name=self.CATALOG, schemas=schemas, truncated=truncated)
            ]
        )

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        if catalog.lower() != self.CATALOG:
            raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
        try:
            part = PinotClient.path_part(table)
            PinotClient.path_part(schema)
        except ValueError as exc:
            raise TableNotFoundError(
                catalog=catalog, schema=schema, table=table
            ) from exc
        try:
            schema_json = await self._client.controller_get(
                f"/tables/{part}/schema", database=schema
            )
            if schema_json is PinotClient.NotFound:
                raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
            metadata_json = await self._client.controller_get(
                f"/tables/{part}/metadata", database=schema
            )
            config_json = await self._client.controller_get(
                f"/tables/{part}", database=schema
            )
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
        return table_schema(
            catalog,
            schema,
            table,
            schema_json,
            None if metadata_json is PinotClient.NotFound else metadata_json,
            None if config_json is PinotClient.NotFound else config_json,
        )

    async def estimate_cost(self, sql: str) -> CostEstimate:
        """No quotation exists yet — U11 builds it from segment metadata.

        Pinot 1.5.1 reports no bytes anywhere and a constant rowcount of 100
        per table scan, so there is nothing honest to return but "unknown",
        which the default budget denies. Guessing here would admit a query
        the gate exists to stop.
        """
        return CostEstimate(confidence="low")

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        two_part = two_part_sql(sql, self.CATALOG)
        options = self._query_options(timeout_seconds)
        client_timeout = (
            None
            if timeout_seconds is None
            else timeout_seconds + _TIMEOUT_GRACE_SECONDS
        )
        try:
            body = await self._client.broker_query(
                two_part, options, timeout_seconds=client_timeout
            )
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc

        failure = result_failure(body)
        if failure is not None:
            if is_self_correctable(failure):
                raise QueryFailedError(hint_for_engine_error(failure))
            raise EngineError(_UNREACHABLE)
        return parse_query_result(body, max_rows)

    def _query_options(self, timeout_seconds: float | None) -> str:
        """The reins, as Pinot's semicolon-separated option string."""
        options = [f"{_OPT_MULTISTAGE}=true"]
        if timeout_seconds is not None:
            # Round up: a sub-millisecond budget must never render as 0, which
            # Pinot reads as no cap rather than as no time.
            milliseconds = max(1, math.ceil(timeout_seconds * 1000))
            options.append(f"{_OPT_TIMEOUT_MS}={milliseconds}")
        if self._max_intermediate_rows is not None:
            options.append(f"{_OPT_MAX_ROWS_IN_JOIN}={self._max_intermediate_rows}")
            options.append(f"{_OPT_MAX_ROWS_IN_WINDOW}={self._max_intermediate_rows}")
        options.append(f"{_OPT_MAX_RESPONSE_BYTES}={self.MAX_QUERY_RESPONSE_BYTES}")
        return ";".join(options)

    async def _databases(self) -> list[str]:
        """Pinot databases, or just `default` on a controller without the endpoint."""
        body = await self._client.controller_get("/databases")
        if body is PinotClient.NotFound or not isinstance(body, list):
            return [_DEFAULT_DATABASE]
        names = sorted({d for d in body if isinstance(d, str) and d})
        return names or [_DEFAULT_DATABASE]
