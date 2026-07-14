"""Engine-agnostic domain models.

These are the shapes the MCP tool surface returns to agents; adapters
translate engine-specific metadata into them. Core never imports engine SDKs.
"""

from pydantic import BaseModel, ConfigDict, Field


class ColumnInfo(BaseModel):
    name: str
    type: str
    comment: str | None = None


class TableSchema(BaseModel):
    """Grounding card for one table: identity + columns."""

    # Agents see "schema"; the Python name avoids shadowing BaseModel.schema.
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    catalog: str
    schema_name: str = Field(alias="schema")
    table: str
    columns: list[ColumnInfo]
    comment: str | None = None

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}"


class SchemaInfo(BaseModel):
    name: str
    tables: list[str]


class CatalogInfo(BaseModel):
    name: str
    schemas: list[SchemaInfo]


class CatalogMetadata(BaseModel):
    catalogs: list[CatalogInfo]
