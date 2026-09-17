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
    # How many CONSUMING segments this table has, and the row bound on one of
    # them. A consuming segment reports nothing (0 docs, -1 bytes) until it
    # seals, so it is charged from the config or the quote goes low.
    consuming: int = 0
    flush_rows: int | None = None
    # False when the metadata response did not cover every sealed segment the
    # size report names: a confident sum over half a table is the one failure
    # the gate exists to prevent.
    complete: bool = True
    # Column sets proved unique on this table, lowercase. Empty is "no
    # evidence", which charges a join the product exactly as before.
    unique_keys: frozenset[frozenset[str]] = frozenset()


# Long.MIN_VALUE: what a consuming segment reports where a CRC would be.
_CONSUMING_CRC = -9223372036854775808

# The flush threshold, most specific location first. `.size` is the deprecated
# spelling of `.rows` and is what the bundled quickstart config actually uses.
_FLUSH_KEYS = (
    "realtime.segment.flush.threshold.rows",
    "realtime.segment.flush.threshold.size",
)


def segment_facts(seg_metadata_json: Any, size_json: Any) -> list[SegmentFact]:
    """Per-segment docs, bytes, time range and per-column bytes.

    Bytes come from a different endpoint than docs, keyed by segment name, so
    a segment missing from the size report keeps its docs and loses its bytes.

    A consuming segment is dropped rather than kept as a zero: it reports 0
    docs and -1 bytes permanently, and one None bytes makes the whole table's
    byte sum unknown. Only all four markers together identify it — an
    unreadable sealed segment keeps its Nones and poisons the sum, because an
    unreadable segment is not a free one.
    """
    if not isinstance(seg_metadata_json, dict):
        return []
    sizes = _segment_sizes(size_json)
    reported = _reported_sizes(size_json)
    facts: list[SegmentFact] = []
    for key, body in seg_metadata_json.items():
        if not isinstance(body, dict):
            continue
        name = body.get("segmentName")
        if not isinstance(name, str) or not name:
            name = key if isinstance(key, str) else ""
        if not name:
            continue
        if _is_consuming(body, reported.get(name)):
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


def consuming_count(externalview_json: Any, size_json: Any) -> int:
    """How many CONSUMING segments this table has, charged at the larger count.

    Externalview is the state of record but can lag, and missingSegments also
    counts a segment a server failed to report, so neither is authoritative
    and only the larger cannot under-charge. Neither readable is 0, which is
    the pre-U12 behaviour and is caught by the completeness check where it
    matters.
    """
    return max(_externalview_consuming(externalview_json), _missing_segments(size_json))


def flush_rows(config_json: Any) -> int | None:
    """The row bound on one consuming segment, from the REALTIME stream config.

    Values are JSON strings in every config measured. Anything that is not a
    positive int — a float, a byte-suffixed size, a negative, zero, absent —
    is None, and a None here takes the whole quote low rather than guessing.
    """
    if not isinstance(config_json, dict):
        return None
    half = config_json.get("REALTIME")
    if not isinstance(half, dict):
        return None
    for source in _stream_config_maps(half):
        for key in _FLUSH_KEYS:
            bound = _positive_int_or_digits(source.get(key))
            if bound is not None:
                return bound
    return None


def metadata_is_complete(seg_metadata_json: Any, size_json: Any) -> bool:
    """Does the metadata response cover every sealed segment the size report names?

    Measured: on a 2-server table /segments/{t}/metadata returns one server's
    half and alternates which half between identical calls, so a sum over it
    is a confident sum over a subset — an under-quote at confidence="high",
    the one failure mode the gate exists to prevent. A segment reporting -1
    bytes is consuming and is expected to carry no useful entry, so it is
    exempt.
    """
    named = {
        name
        for name, size in _reported_sizes(size_json).items()
        if size is not None and size >= 0
    }
    if not named:
        return True
    if not isinstance(seg_metadata_json, dict):
        return False
    present: set[str] = set()
    for key, body in seg_metadata_json.items():
        if isinstance(key, str):
            present.add(key)
        if isinstance(body, dict) and isinstance(body.get("segmentName"), str):
            present.add(body["segmentName"])
    return named <= present


def _is_consuming(body: dict[str, Any], reported: int | None) -> bool:
    """Rule 5's conjunction, all four markers together and never fewer."""
    return (
        body.get("totalDocs") == 0
        and "columns" not in body
        and body.get("crc") == _CONSUMING_CRC
        and reported == -1
    )


def _stream_config_maps(half: dict[str, Any]) -> list[dict[str, Any]]:
    """The stream config maps of one table half, current location first."""
    maps: list[dict[str, Any]] = []
    ingestion = half.get("ingestionConfig")
    if isinstance(ingestion, dict):
        stream = ingestion.get("streamIngestionConfig")
        if isinstance(stream, dict):
            configs = stream.get("streamConfigMaps")
            if isinstance(configs, list):
                maps.extend(entry for entry in configs if isinstance(entry, dict))
    index_config = half.get("tableIndexConfig")
    if isinstance(index_config, dict):
        legacy = index_config.get("streamConfigs")
        if isinstance(legacy, dict):
            maps.append(legacy)
    return maps


def _externalview_consuming(externalview_json: Any) -> int:
    """Segments of the REALTIME map any server calls CONSUMING."""
    if not isinstance(externalview_json, dict):
        return 0
    half = externalview_json.get("REALTIME")
    if not isinstance(half, dict):
        return 0
    return sum(
        1
        for states in half.values()
        if isinstance(states, dict) and "CONSUMING" in states.values()
    )


def _missing_segments(size_json: Any) -> int:
    """realtimeSegments.missingSegments, measured to equal the consuming count."""
    if not isinstance(size_json, dict):
        return 0
    half = size_json.get("realtimeSegments")
    if not isinstance(half, dict):
        return 0
    missing = _positive_int(half.get("missingSegments"), allow_zero=True)
    return missing or 0


def _reported_sizes(size_json: Any) -> dict[str, int]:
    """Segment name to reportedSizeInBytes verbatim, -1 included.

    _segment_sizes drops the -1 as "unknown"; this keeps it, because -1 is
    how a consuming segment is recognised and how a sealed one is named.
    """
    if not isinstance(size_json, dict):
        return {}
    reported: dict[str, int] = {}
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
            value = body.get("reportedSizeInBytes")
            if isinstance(value, bool) or not isinstance(value, int):
                continue
            reported[name] = value
    return reported


def _positive_int_or_digits(value: Any) -> int | None:
    """A positive int, or a string of digits meaning one. Nothing else."""
    if isinstance(value, str):
        return int(value) if value.isdigit() and int(value) > 0 else None
    return _positive_int(value)


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
    *,
    externalview_json: Any = None,
    schema_json: Any = None,
    table_metadata_json: Any = None,
) -> TableFacts:
    """One table's type, time column, segments and realtime facts.

    The three keyword documents are the U12 additions and default to None, so
    an OFFLINE caller that fetches none of them gets exactly the pre-U12
    facts: no consuming segments, no threshold, complete, no key evidence.
    """
    return TableFacts(
        table=table,
        types=table_types(config_json),
        time_column=time_column(config_json),
        segments=tuple(segment_facts(seg_metadata_json, size_json)),
        columns=columns,
        consuming=consuming_count(externalview_json, size_json),
        flush_rows=flush_rows(config_json),
        complete=metadata_is_complete(seg_metadata_json, size_json),
        unique_keys=upsert_keys(config_json, schema_json, table_metadata_json)
        or single_segment_unique_columns(
            seg_metadata_json, config_json, schema_json, size_json
        ),
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


def upsert_keys(
    config_json: Any, schema_json: Any, table_metadata_json: Any
) -> frozenset[frozenset[str]]:
    """An upsert table's primary key, as the one column set proved unique.

    Measured: GROUP BY pk HAVING count(*) > 1 returns nothing on the upsert
    table and 6-version rows on a byte-identical non-upsert table reading the
    same topic, and the PK self-join returns exactly the distinct-key count
    against 3,600 for the twin.

    Three documents have to agree. The config's upsertConfig and the
    metadata's PK map are each independently proof of an upsert table — the
    map is {} on every non-upsert table — so a disagreement between them is
    two documents contradicting each other about what this table is, which is
    not a basis for admitting a join. The full column list is the key and
    never a subset: a composite primary key is unique as a tuple.
    """
    if not isinstance(schema_json, dict):
        return frozenset()
    declared = _has_upsert_config(config_json)
    counted = _has_primary_key_counts(table_metadata_json)
    if not declared or not counted:
        return frozenset()
    columns = schema_json.get("primaryKeyColumns")
    if not isinstance(columns, list) or not columns:
        return frozenset()
    names = {
        column.lower()
        for column in columns
        if isinstance(column, str) and column and not isinstance(column, bool)
    }
    if len(names) != len(columns):
        return frozenset()
    return frozenset({frozenset(names)})


def single_segment_unique_columns(
    seg_metadata_json: Any, config_json: Any, schema_json: Any, size_json: Any
) -> frozenset[frozenset[str]]:
    """Columns whose cardinality equals their docs, on a one-sealed-segment table.

    cardinality is exactly count(DISTINCT col) — verified against the engine
    on four columns — and count(DISTINCT col) <= count(col) <= totalDocs, so
    equality forces every doc to be counted and every value to differ. That
    argument is the segment's, and it is the table's only where the two are
    the same rows: one sealed segment and nothing consuming.

    Measured: /segments/{t}/metadata on a multi-server table can return only
    one server's half, so counting sealed segments in that response alone
    lets a 2-segment table read as single-segment. The size report is the
    independent count: it must name exactly one sealed segment (a
    reportedSizeInBytes >= 0 entry), the metadata response must be complete
    against it, and the one metadata entry must be that same segment.

    Gated on nullability because the null caveat is unclosed (log §6): if
    cardinality counts a null or a default as a distinct value, a column with
    one null could report cardinality == totalDocs while two rows share the
    default. Where nullability cannot be established, nothing is yielded.

    A multi-value column's cardinality counts distinct entries, not rows —
    totalNumberOfEntries and maxNumberOfMultiValues are reported separately —
    so equality to totalDocs proves nothing there; such a column is never
    evidence.
    """
    if not isinstance(seg_metadata_json, dict):
        return frozenset()
    sealed_names = {
        name
        for name, size in _reported_sizes(size_json).items()
        if size is not None and size >= 0
    }
    if len(sealed_names) != 1:
        return frozenset()
    if not metadata_is_complete(seg_metadata_json, size_json):
        return frozenset()
    sealed = [
        (key, body)
        for key, body in seg_metadata_json.items()
        if isinstance(body, dict) and isinstance(body.get("columns"), list)
    ]
    consuming = [
        body
        for body in seg_metadata_json.values()
        if isinstance(body, dict) and not isinstance(body.get("columns"), list)
    ]
    if len(sealed) != 1 or consuming:
        return frozenset()
    (sole_name,) = sealed_names
    sole_key, sole_body = sealed[0]
    name_candidate = sole_body.get("segmentName")
    if not isinstance(name_candidate, str) or not name_candidate:
        name_candidate = sole_key if isinstance(sole_key, str) else ""
    if name_candidate != sole_name:
        return frozenset()
    docs = _positive_int(sole_body.get("totalDocs"))
    if docs is None:
        return frozenset()
    nullable_off = _null_handling_disabled(config_json)
    schema_nullable = _schema_nullable_columns(schema_json)
    keys: set[frozenset[str]] = set()
    for column in sole_body["columns"]:
        if not isinstance(column, dict):
            continue
        name = column.get("columnName")
        cardinality = _positive_int(column.get("cardinality"))
        if not isinstance(name, str) or not name or cardinality != docs:
            continue
        if _is_multi_valued(column, docs):
            continue
        spec = column.get("fieldSpec")
        not_null = isinstance(spec, dict) and spec.get("notNull") is True
        if not not_null and not (
            nullable_off and name.lower() not in schema_nullable
        ):
            continue
        keys.add(frozenset({name.lower()}))
    return frozenset(keys)


def _is_multi_valued(column: dict[str, Any], docs: int) -> bool:
    """A column whose per-row value count is not knowable as exactly one.

    `docs` is the segment's own validated totalDocs, not the column entry's
    copy of it, so a column entry missing or lying about its own totalDocs
    cannot dodge this gate.
    """
    spec = column.get("fieldSpec")
    if isinstance(spec, dict) and spec.get("singleValueField") is False:
        return True
    entries = column.get("totalNumberOfEntries")
    if isinstance(entries, int) and not isinstance(entries, bool) and entries != docs:
        return True
    max_mv = column.get("maxNumberOfMultiValues")
    return isinstance(max_mv, int) and not isinstance(max_mv, bool) and max_mv > 0


def _has_upsert_config(config_json: Any) -> bool:
    """Does either half's config carry an upsertConfig object?"""
    if not isinstance(config_json, dict):
        return False
    for key in ("REALTIME", "OFFLINE"):
        half = config_json.get(key)
        if isinstance(half, dict) and isinstance(half.get("upsertConfig"), dict):
            return True
    return False


def _has_primary_key_counts(table_metadata_json: Any) -> bool:
    """Is upsertPartitionToServerPrimaryKeyCountMap non-empty?

    It is {} on every non-upsert table, so a non-empty map is itself proof.
    It is never read as a count: it is per server, and replication > 1 is
    unmeasured (spec decision 6).
    """
    if not isinstance(table_metadata_json, dict):
        return False
    counts = table_metadata_json.get("upsertPartitionToServerPrimaryKeyCountMap")
    return isinstance(counts, dict) and bool(counts)


def _null_handling_disabled(config_json: Any) -> bool:
    """Is tableIndexConfig.nullHandlingEnabled explicitly false on a half?"""
    if not isinstance(config_json, dict):
        return False
    for key in ("REALTIME", "OFFLINE"):
        half = config_json.get(key)
        if not isinstance(half, dict):
            continue
        index_config = half.get("tableIndexConfig")
        if isinstance(index_config, dict) and index_config.get(
            "nullHandlingEnabled"
        ) is False:
            return True
    return False


def _schema_nullable_columns(schema_json: Any) -> frozenset[str]:
    """Lowercase names the schema marks nullable, which no gate may pass."""
    if not isinstance(schema_json, dict):
        return frozenset()
    nullable: set[str] = set()
    for key in _FIELD_SPEC_KEYS:
        specs = schema_json.get(key)
        if not isinstance(specs, list):
            continue
        for spec in specs:
            if not isinstance(spec, dict):
                continue
            name = spec.get("name")
            if isinstance(name, str) and name and spec.get("nullable") is True:
                nullable.add(name.lower())
    return frozenset(nullable)
