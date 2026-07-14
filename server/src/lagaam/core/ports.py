"""Hexagonal ports. Core depends only on these; adapters implement them.

U1 scope: metadata grounding. The port grows with the roadmap
(explain/estimate_cost/execute/dialect land in U3-U6).
"""

from typing import Protocol, runtime_checkable

from lagaam.core.models import CatalogMetadata, TableSchema


@runtime_checkable
class QueryEngine(Protocol):
    async def list_catalogs(self) -> CatalogMetadata: ...

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema: ...
