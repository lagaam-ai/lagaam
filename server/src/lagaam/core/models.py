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
    # From engine statistics when available; grounds LIMIT/filter decisions.
    row_estimate: int | None = None

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}"


class SchemaInfo(BaseModel):
    name: str
    tables: list[str]


class CatalogInfo(BaseModel):
    name: str
    schemas: list[SchemaInfo]
    # Listing was capped; unseen tables may still exist.
    truncated: bool = False


class CatalogMetadata(BaseModel):
    catalogs: list[CatalogInfo]


class DialectCard(BaseModel):
    """Engine-quirks brief injected into SQL-generation prompts."""

    engine: str
    # The sqlglot dialect id used for parse-validation of generated SQL.
    sqlglot_dialect: str
    rules: list[str]
