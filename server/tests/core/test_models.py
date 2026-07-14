"""Core domain models: engine-agnostic shapes the whole server speaks."""

import pytest
from pydantic import ValidationError

from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    ColumnInfo,
    SchemaInfo,
    TableSchema,
)


def test_table_schema_holds_fully_qualified_identity_and_columns() -> None:
    schema = TableSchema(
        catalog="tpch",
        schema_name="tiny",
        table="orders",
        columns=[
            ColumnInfo(name="orderkey", type="bigint"),
            ColumnInfo(name="orderdate", type="date", comment="order date"),
        ],
    )
    assert schema.fqn == "tpch.tiny.orders"
    assert [c.name for c in schema.columns] == ["orderkey", "orderdate"]
    assert schema.columns[0].comment is None


def test_schema_alias_round_trips_as_agent_facing_key() -> None:
    # Agents read and write the key "schema", never "schema_name".
    schema = TableSchema.model_validate(
        {"catalog": "tpch", "schema": "tiny", "table": "orders", "columns": []}
    )
    assert schema.schema_name == "tiny"
    dumped = schema.model_dump(by_alias=True)
    assert dumped["schema"] == "tiny"
    assert "schema_name" not in dumped


def test_row_estimate_is_optional_and_absent_by_default() -> None:
    schema = TableSchema(catalog="tpch", schema_name="tiny", table="orders", columns=[])
    assert schema.row_estimate is None
    with_estimate = schema.model_copy(update={"row_estimate": 15000})
    assert with_estimate.row_estimate == 15000


def test_catalog_listing_is_marked_when_truncated() -> None:
    catalog = CatalogInfo(name="huge", schemas=[], truncated=True)
    assert catalog.truncated
    assert CatalogInfo(name="small", schemas=[]).truncated is False


def test_catalog_metadata_nests_catalogs_schemas_tables() -> None:
    meta = CatalogMetadata(
        catalogs=[
            CatalogInfo(
                name="tpch",
                schemas=[SchemaInfo(name="tiny", tables=["orders", "lineitem"])],
            )
        ]
    )
    assert meta.catalogs[0].schemas[0].tables == ["orders", "lineitem"]


def test_models_reject_missing_required_fields() -> None:
    with pytest.raises(ValidationError):
        ColumnInfo(name="orderkey")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        TableSchema(catalog="tpch", schema_name="tiny", table="orders")  # type: ignore[call-arg]
