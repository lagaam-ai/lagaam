"""Hexagonal ports. Core depends only on these; adapters implement them.

Grows with the roadmap: explain/estimate_cost/execute land in U4-U6.
"""

from typing import Protocol, runtime_checkable

from lagaam.core.models import (
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    TableSchema,
)


@runtime_checkable
class QueryEngine(Protocol):
    async def list_catalogs(self) -> CatalogMetadata: ...

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema: ...

    # Sync: a dialect card is static engine knowledge, no I/O.
    def dialect(self) -> DialectCard: ...

    # Pre-execution QUOTATION: what this SQL would scan. sql is already
    # safety-validated (U3); the engine only sizes it, never runs it.
    async def estimate_cost(self, sql: str) -> CostEstimate: ...
