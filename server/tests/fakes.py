"""In-memory QueryEngine fake for unit tests. Mirrors the tpch.tiny shape."""

from lagaam.core.errors import TableNotFoundError
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    ColumnInfo,
    DialectCard,
    SchemaInfo,
    TableSchema,
)

_TABLES: dict[tuple[str, str, str], list[ColumnInfo]] = {
    ("tpch", "tiny", "orders"): [
        ColumnInfo(name="orderkey", type="bigint"),
        ColumnInfo(name="custkey", type="bigint"),
        ColumnInfo(name="orderdate", type="date", comment="order date"),
    ],
    ("tpch", "tiny", "lineitem"): [
        ColumnInfo(name="orderkey", type="bigint"),
        ColumnInfo(name="quantity", type="double"),
    ],
}


class FakeQueryEngine:
    async def list_catalogs(self) -> CatalogMetadata:
        return CatalogMetadata(
            catalogs=[
                CatalogInfo(
                    name="tpch",
                    schemas=[SchemaInfo(name="tiny", tables=["orders", "lineitem"])],
                )
            ]
        )

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        key = (catalog, schema, table)
        if key not in _TABLES:
            raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
        return TableSchema(
            catalog=catalog, schema_name=schema, table=table, columns=_TABLES[key]
        )

    def dialect(self) -> DialectCard:
        return DialectCard(
            engine="Fake",
            sqlglot_dialect="trino",
            rules=["Names are catalog.schema.table"],
        )
