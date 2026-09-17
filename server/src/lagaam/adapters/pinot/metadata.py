"""Controller JSON to domain values. PURE: no I/O, and it never raises.

Pinot has no catalog hierarchy and no column comments, so a grounding card is
assembled from three separate controller documents. Each parser treats a shape
it cannot read as "no fact" — an empty list or None — because a grounding call
that crashes on an unexpected key costs an agent the names it needs, and a
missing number fails safe at the budget gate anyway.
"""

from collections.abc import Mapping
from dataclasses import dataclass
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
    an agent on a falsehood, so the count is a fact only where the config
    positively establishes an offline-only table: a REALTIME half, an unknown
    type set, or a config too broken to read all mean None. An empty set is
    what an unreadable config yields, and that config is the only thing that
    could have ruled out a consuming half.
    """
    if types != frozenset({"OFFLINE"}):
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
    """One grounding card, from the schema, metadata and config documents.

    `table` is echoed as given, because the caller resolves it to the
    controller's own spelling first and the REST paths are case-sensitive.
    """
    return TableSchema(
        catalog=catalog.lower(),
        schema=schema.lower(),
        table=table,
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
                columns.append(ColumnInfo(name=name, type=_column_type(spec, data_type)))
    return columns


def _column_type(spec: dict[str, Any], data_type: str) -> str:
    """`INT[]` for a multi-value column, the bare element type otherwise.

    Measured: a singleValueField=false column comes back as INT_ARRAY with an
    array cell, so a scalar type here invites a comparison that never matches.
    """
    # Only an explicit false is multi-value; an unreadable flag stays scalar.
    if spec.get("singleValueField") is False:
        return f"{data_type}[]"
    return data_type


@dataclass(frozen=True)
class SegmentFact:
    """One sealed segment's measurable size, as the controller reports it.

    Every field is optional because a fact the controller does not carry must
    stay absent rather than become a zero: a zero would quote a segment free.
    """

    name: str
    docs: int | None
    total_bytes: int | None
    start_ms: int | None
    end_ms: int | None
    column_bytes: Mapping[str, int]


@dataclass(frozen=True)
class TableFacts:
    """Everything about one table a quotation is built from."""

    table: str
    types: frozenset[str]
    time_column: str | None
    segments: tuple[SegmentFact, ...]
    # Lowercase names of the referenced columns this table actually carries,
    # empty when none was asked for or the schema could not be read. A
    # segment missing any of them is priced whole rather than per column.
    columns: frozenset[str] = frozenset()


def segment_facts(seg_metadata_json: Any, size_json: Any) -> list[SegmentFact]:
    """Per-segment docs, bytes, time range and per-column bytes.

    Bytes come from a different endpoint than docs, keyed by segment name, so
    a segment missing from the size report keeps its docs and loses its bytes.
    """
    if not isinstance(seg_metadata_json, dict):
        return []
    sizes = _segment_sizes(size_json)
    facts: list[SegmentFact] = []
    for key, body in seg_metadata_json.items():
        if not isinstance(body, dict):
            continue
        name = body.get("segmentName")
        if not isinstance(name, str) or not name:
            name = key if isinstance(key, str) else ""
        if not name:
            continue
        facts.append(
            SegmentFact(
                name=name,
                docs=_positive_int(body.get("totalDocs"), allow_zero=True),
                total_bytes=sizes.get(name),
                start_ms=_positive_int(body.get("startTimeMillis")),
                end_ms=_positive_int(body.get("endTimeMillis")),
                column_bytes=_column_bytes(body.get("columns")),
            )
        )
    return facts


def time_column(config_json: Any) -> str | None:
    """The OFFLINE half's time column, which is what prunes segments."""
    if not isinstance(config_json, dict):
        return None
    for key in ("OFFLINE", "REALTIME"):
        half = config_json.get(key)
        if not isinstance(half, dict):
            continue
        segments_config = half.get("segmentsConfig")
        if not isinstance(segments_config, dict):
            continue
        name = segments_config.get("timeColumnName")
        if isinstance(name, str) and name:
            return name
    return None


def schema_columns(schema_json: Any) -> dict[str, str]:
    """Lowercase column name to the schema's own spelling of it.

    The controller's `?columns=` filter is case-sensitive while SQL is not,
    so a referenced name has to be translated into this spelling before it
    can be asked for. A schema nobody could read yields nothing, which
    charges whole segments rather than a name the controller would drop.
    """
    return {column.name.lower(): column.name for column in _columns(schema_json)}


def table_facts(
    table: str,
    config_json: Any,
    seg_metadata_json: Any,
    size_json: Any,
    columns: frozenset[str] = frozenset(),
) -> TableFacts:
    """One table's type, time column and segments, from three documents."""
    return TableFacts(
        table=table,
        types=table_types(config_json),
        time_column=time_column(config_json),
        segments=tuple(segment_facts(seg_metadata_json, size_json)),
        columns=columns,
    )


def _segment_sizes(size_json: Any) -> dict[str, int]:
    """Segment name to reported bytes, from both halves of the size report."""
    if not isinstance(size_json, dict):
        return {}
    sizes: dict[str, int] = {}
    for key in ("offlineSegments", "realtimeSegments"):
        half = size_json.get(key)
        if not isinstance(half, dict):
            continue
        segments = half.get("segments")
        if not isinstance(segments, dict):
            continue
        for name, body in segments.items():
            if not isinstance(name, str) or not isinstance(body, dict):
                continue
            # Measured: a consuming segment reports -1, which is "unknown".
            reported = _positive_int(body.get("reportedSizeInBytes"), allow_zero=True)
            if reported is not None:
                sizes[name] = reported
    return sizes


def _column_bytes(columns_json: Any) -> Mapping[str, int]:
    """Per-column bytes, summing every index entry the segment carries.

    The entries differ by encoding — a RAW column has a forward_index and no
    dictionary — so the sum is over whatever is present, never a fixed set.
    """
    if not isinstance(columns_json, list):
        return {}
    totals: dict[str, int] = {}
    for column in columns_json:
        if not isinstance(column, dict):
            continue
        name = column.get("columnName")
        index_sizes = column.get("indexSizeMap")
        if not isinstance(name, str) or not name:
            continue
        if not isinstance(index_sizes, dict):
            continue
        total = 0
        for value in index_sizes.values():
            size = _positive_int(value, allow_zero=True)
            if size is not None:
                total += size
        totals[name] = total
    return totals


def _positive_int(value: Any, allow_zero: bool = False) -> int | None:
    """An int the controller means as a measurement, or None."""
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    if value < 0 or (value == 0 and not allow_zero):
        return None
    return value
