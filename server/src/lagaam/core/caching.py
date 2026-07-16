"""Read-through TTL cache over any QueryEngine.

Agents re-ground on every turn, so the same metadata is requested over and
over; the cache keeps that off the engine. Composition, not inheritance:
this wraps a QueryEngine and is itself a QueryEngine, so the server never
knows whether it is talking to a cache or an adapter.

Failures are never cached — a table can appear right after a miss.
"""

import time
from collections.abc import Callable

from lagaam.core.models import (
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    QueryResult,
    TableSchema,
)
from lagaam.core.ports import QueryEngine


class CachingQueryEngine:
    def __init__(
        self,
        engine: QueryEngine,
        ttl_seconds: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._engine = engine
        self._ttl = ttl_seconds
        self._clock = clock
        self._catalogs: tuple[float, CatalogMetadata] | None = None
        self._tables: dict[tuple[str, str, str], tuple[float, TableSchema]] = {}

    def dialect(self) -> DialectCard:
        # Static engine knowledge; nothing to cache.
        return self._engine.dialect()

    async def estimate_cost(self, sql: str) -> CostEstimate:
        # Query-specific and cheap to plan; caching would risk stale quotes.
        return await self._engine.estimate_cost(sql)

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        # Results are never cached — a cache would serve stale data.
        return await self._engine.execute(sql, max_rows, timeout_seconds)

    def _fresh(self, stored_at: float) -> bool:
        return self._clock() - stored_at <= self._ttl

    async def list_catalogs(self) -> CatalogMetadata:
        if self._catalogs is not None and self._fresh(self._catalogs[0]):
            return self._catalogs[1]
        result = await self._engine.list_catalogs()
        self._catalogs = (self._clock(), result)
        return result

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        # Engines resolve identifiers case-insensitively; so must the key.
        key = (catalog.lower(), schema.lower(), table.lower())
        cached = self._tables.get(key)
        if cached is not None and self._fresh(cached[0]):
            return cached[1]
        result = await self._engine.describe_table(catalog, schema, table)
        self._tables[key] = (self._clock(), result)
        return result
