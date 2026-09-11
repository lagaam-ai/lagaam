"""Controller JSON to domain values. PURE: no I/O, and it never raises.

Pinot has no catalog hierarchy and no column comments, so a grounding card is
assembled from three separate controller documents. Each parser treats a shape
it cannot read as "no fact" — an empty list or None — because a grounding call
that crashes on an unexpected key costs an agent the names it needs, and a
missing number fails safe at the budget gate anyway.
"""

from typing import Any

from lagaam.core.models import ColumnInfo, TableSchema

# Pinot's three field-spec lists, in the order a card presents them.
_FIELD_SPEC_KEYS = ("dimensionFieldSpecs", "metricFieldSpecs", "dateTimeFieldSpecs")


def table_names(tables_json: Any) -> list[str]:
    """Bare table names from GET /tables, sorted. Never the _OFFLINE spelling."""
    if not isinstance(tables_json, dict):
        return []
    tables = tables_json.get("tables")
    if not isinstance(tables, list):
        return []
    return sorted(t for t in tables if isinstance(t, str) and t)


def table_types(config_json: Any) -> frozenset[str]:
    """Which halves this table has, from GET /tables/{t}: OFFLINE, REALTIME, or both."""
    if not isinstance(config_json, dict):
        return frozenset()
    return frozenset(
        key for key in ("OFFLINE", "REALTIME") if isinstance(config_json.get(key), dict)
    )


def row_estimate(metadata_json: Any, types: frozenset[str]) -> int | None:
    """Total rows from GET /tables/{t}/metadata, or None when it cannot be trusted.

    Measured: a REALTIME table serving 70 rows reported numRows 0, because a
    consuming segment's size is unknown until it is sealed. Zero would ground
    an agent on a falsehood, so a REALTIME half means None.
    """
    if "REALTIME" in types:
        return None
    if not isinstance(metadata_json, dict):
        return None
    rows = metadata_json.get("numRows")
    if isinstance(rows, bool) or not isinstance(rows, int):
        return None
    return rows


def table_schema(
    catalog: str,
    schema: str,
    table: str,
    schema_json: Any,
    metadata_json: Any,
    config_json: Any,
) -> TableSchema:
    """One grounding card, from the schema, metadata and config documents."""
    return TableSchema(
        catalog=catalog.lower(),
        schema=schema.lower(),
        table=table.lower(),
        columns=_columns(schema_json),
        row_estimate=row_estimate(metadata_json, table_types(config_json)),
    )


def _columns(schema_json: Any) -> list[ColumnInfo]:
    """Dimension + metric + dateTime specs as columns; Pinot has no comments."""
    if not isinstance(schema_json, dict):
        return []
    columns: list[ColumnInfo] = []
    for key in _FIELD_SPEC_KEYS:
        specs = schema_json.get(key)
        if not isinstance(specs, list):
            continue
        for spec in specs:
            if not isinstance(spec, dict):
                continue
            name = spec.get("name")
            data_type = spec.get("dataType")
            if isinstance(name, str) and name and isinstance(data_type, str):
                columns.append(ColumnInfo(name=name, type=data_type))
    return columns
