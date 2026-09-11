"""Pinot adapter for the QueryEngine port.

Pinot has no catalog hierarchy, no SHOW TABLES and no information_schema, so
grounding is controller REST and the catalog is synthetic: exactly one, named
`pinot`, whose schemas are Pinot databases. Names are three-part to the agent
and two-part to Pinot. Every failure leaves this module as a LagaamError —
httpx exceptions and broker messages never escape.
"""

import os

import httpx

from lagaam.adapters.pinot.client import PinotClient, PinotTransportError
from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.adapters.pinot.metadata import table_names, table_schema
from lagaam.core.errors import EngineError, TableNotFoundError
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    SchemaInfo,
    TableSchema,
)

_UNREACHABLE = "the query engine is not reachable right now"

# Pinot's namespace is database.table and `default` always exists; 1.5.1 does
# expose GET /databases, but an older controller answering 404 still grounds.
_DEFAULT_DATABASE = "default"


class PinotEngine:
    CATALOG = "pinot"

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
    def from_env(cls) -> "PinotEngine":
        rows = os.environ.get("LAGAAM_MAX_INTERMEDIATE_ROWS")
        return cls(
            controller_url=os.environ.get(
                "PINOT_CONTROLLER_URL", "http://localhost:9000"
            ),
            broker_url=os.environ.get("PINOT_BROKER_URL", "http://localhost:8000"),
            user=os.environ.get("PINOT_USER"),
            password=os.environ.get("PINOT_PASSWORD"),
            max_intermediate_rows=int(rows) if rows else None,
        )

    def dialect(self) -> DialectCard:
        return PINOT_DIALECT_CARD

    async def list_catalogs(self) -> CatalogMetadata:
        schemas: list[SchemaInfo] = []
        truncated = False
        try:
            for database in await self._databases():
                body = await self._client.controller_get("/tables", database=database)
                if body is PinotClient.NotFound:
                    continue
                names = table_names(body)
                if len(names) > self._max_tables:
                    truncated = True
                    names = names[: self._max_tables]
                schemas.append(SchemaInfo(name=database, tables=names))
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
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

    async def _databases(self) -> list[str]:
        """Pinot databases, or just `default` on a controller without the endpoint."""
        body = await self._client.controller_get("/databases")
        if body is PinotClient.NotFound or not isinstance(body, list):
            return [_DEFAULT_DATABASE]
        names = sorted({d for d in body if isinstance(d, str) and d})
        return names or [_DEFAULT_DATABASE]
