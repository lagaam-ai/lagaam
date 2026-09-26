"""Controller JSON to domain values. PURE: no I/O, and it never raises.

Pinot has no catalog hierarchy and no column comments, so a grounding card is
assembled from three separate controller documents. Each parser treats a shape
it cannot read as "no fact" — an empty list or None — because a grounding call
that crashes on an unexpected key costs an agent the names it needs, and a
missing number fails safe at the budget gate anyway.
"""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Final

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
    # How many CONSUMING segments this table has, and the row bound on each.
    # A consuming segment reports nothing (0 docs, -1 bytes) until it seals,
    # so it is charged at the flush threshold stored in its own ZK metadata
    # at creation — never the table config's, which an operator can lower
    # under a segment already consuming. One entry per consuming segment,
    # None where that segment's threshold could not be read, and a None
    # takes the whole quote low.
    consuming: int = 0
    consuming_rows: tuple[int | None, ...] = ()
    # False when the metadata response did not cover every sealed segment the
    # size report names: a confident sum over half a table is the one failure
    # the gate exists to prevent.
    complete: bool = True
    # Column sets proved unique on this table, lowercase. Empty is "no
    # evidence", which charges a join the product exactly as before.
    unique_keys: frozenset[frozenset[str]] = frozenset()


# Long.MIN_VALUE: what a consuming segment reports where a CRC would be.
_CONSUMING_CRC = -9223372036854775808

# The row threshold a consuming segment was created with, in its own LLC
# segment ZK metadata. `sizeThresholdToFlushSegment` is the in-JVM field name
# and appears in no controller response; this is the ZK simpleField spelling.
_STORED_FLUSH_KEY: Final = "segment.flush.threshold.size"


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


def stored_flush_rows(segment_zk_json: Any) -> int | None:
    """The row bound on one consuming segment, from that segment's own metadata.

    A consuming segment's row threshold is stored in its LLC segment ZK
    metadata at creation, and the table config is not a bound on it. Measured
    on `u12flush`: after a `PUT` lowering the config 100 -> 10, the live
    consuming segment's `segment.flush.threshold.size` stayed 100 and the
    segment sealed at exactly 100 — while the adapter, reading the config,
    charged 10. No reload changes it; only a `forceCommit` does, by sealing
    the segment so its replacement is born with the new value. Autotune
    (`...threshold.segment.size`) reaches the same place from the other end:
    it stores a row threshold the config never states at all.

    `GET /segments/{table}/{segmentName}/metadata` serves it, as a direct
    view of the ZK simpleFields. Values are JSON strings there; a plain int
    is accepted too. Anything that is not a positive int — a float, a
    byte-suffixed size, a negative, zero, absent — is None, and a None here
    takes the whole quote low rather than guessing.
    """
    if not isinstance(segment_zk_json, dict):
        return None
    return _positive_int_or_digits(segment_zk_json.get(_STORED_FLUSH_KEY))


def consuming_segment_names(externalview_json: Any) -> tuple[str, ...]:
    """The names of the REALTIME segments any server calls CONSUMING.

    The addresses the per-segment metadata fetch is made with, in the
    externalview's own order so a table's thresholds line up with its
    segments. `consuming_count` stays the count, because `missingSegments`
    can name more than the externalview does.
    """
    if not isinstance(externalview_json, dict):
        return ()
    half = externalview_json.get("REALTIME")
    if not isinstance(half, dict):
        return ()
    return tuple(
        name
        for name, states in half.items()
        if isinstance(name, str)
        and name
        and isinstance(states, dict)
        and "CONSUMING" in states.values()
    )


def metadata_is_complete(seg_metadata_json: Any, size_json: Any) -> bool:
    """Does the metadata response cover every sealed segment the size report names?

    Measured: on a 2-server table /segments/{t}/metadata returns one server's
    half and alternates which half between identical calls, so a sum over it
    is a confident sum over a subset — an under-quote at confidence="high",
    the one failure mode the gate exists to prevent. A segment reporting -1
    bytes is consuming and is expected to carry no useful entry, so it is
    exempt.

    Defined as "nothing missing", so the guard and the fetch that goes and
    gets the missing names can never disagree about which names those are.
    """
    return not missing_sealed_segments(seg_metadata_json, size_json)


def missing_sealed_segments(seg_metadata_json: Any, size_json: Any) -> frozenset[str]:
    """Sealed segments the size report names and the metadata response lacks.

    The addresses of the per-server fetch, and the reason the completeness
    guard refuses: a response missing these is one server's half of the table.

    Present means a dict body — the one shape `segment_facts` prices. A
    `{name: null}` counted present makes a sealed segment free at
    confidence="high"; counted missing it is fetched instead, which is the
    outcome the fan-out exists for.
    """
    named = {
        name
        for name, size in _reported_sizes(size_json).items()
        if size is not None and size >= 0
    }
    if not named:
        return frozenset()
    if not isinstance(seg_metadata_json, dict):
        return frozenset(named)
    present: set[str] = set()
    for key, body in seg_metadata_json.items():
        if not isinstance(body, dict):
            continue
        if isinstance(key, str):
            present.add(key)
        if isinstance(body.get("segmentName"), str):
            present.add(body["segmentName"])
    return frozenset(named - present)


def segments_by_server(servers_json: Any) -> dict[str, tuple[str, ...]]:
    """Which server holds which segments, from GET /segments/{t}/servers.

    The document is a list with one element per table type, so a hybrid
    table names a server twice and the two lists are unioned. Consuming
    names are in there too; `segment_facts` drops their entries later.
    Insertion order is the response's, which is the order a missing name is
    assigned to the first server holding it.
    """
    if not isinstance(servers_json, list):
        return {}
    by_server: dict[str, list[str]] = {}
    for element in servers_json:
        if not isinstance(element, dict):
            continue
        mapping = element.get("serverToSegmentsMap")
        if not isinstance(mapping, dict):
            continue
        for server, names in mapping.items():
            if not isinstance(server, str) or not server or not isinstance(names, list):
                continue
            held = by_server.setdefault(server, [])
            held.extend(name for name in names if isinstance(name, str) and name)
    return {server: tuple(names) for server, names in by_server.items() if names}


def assign_missing_to_servers(
    missing: frozenset[str], by_server: Mapping[str, tuple[str, ...]]
) -> dict[str, tuple[str, ...]]:
    """One server per missing name: the first in response order that holds it.

    Measured §3: on a replicated table the two replicas' entries for a segment
    are identical in crc, totalDocs and column index sizes, so asking a second
    holder of the same name buys nothing and costs a call. A name no server
    holds is simply absent — the completeness guard still catches it.
    """
    assigned: dict[str, list[str]] = {}
    outstanding = set(missing)
    for server, names in by_server.items():
        taken = [name for name in names if name in outstanding]
        if not taken:
            continue
        assigned[server] = taken
        outstanding.difference_update(taken)
    return {server: tuple(names) for server, names in assigned.items()}


def merge_segment_metadata(
    bulk: Any, per_server: Iterable[Any]
) -> dict[str, Any]:
    """Name-keyed union of the bulk response and the per-server answers.

    The bulk entry wins where both answered: it is the response the guard was
    measured against, and a replica's entry is identical to it anyway. A
    response that is not a dict contributes nothing rather than raising.
    """
    merged: dict[str, Any] = {}
    for response in per_server:
        if isinstance(response, dict):
            merged.update(response)
    if isinstance(bulk, dict):
        merged.update(bulk)
    return merged


def _is_consuming(body: dict[str, Any], reported: int | None) -> bool:
    """Rule 5's conjunction, all four markers together and never fewer."""
    return (
        body.get("totalDocs") == 0
        and "columns" not in body
        and body.get("crc") == _CONSUMING_CRC
        and reported == -1
    )


def _externalview_consuming(externalview_json: Any) -> int:
    """Segments of the REALTIME map any server calls CONSUMING."""
    return len(consuming_segment_names(externalview_json))


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
    consuming_segments_json: Mapping[str, Any] | None = None,
    unique_keys: frozenset[frozenset[str]] = frozenset(),
) -> TableFacts:
    """One table's type, time column, segments and realtime facts.

    The two keyword documents are the U12 additions and default to None, so
    an OFFLINE caller that fetches neither of them gets exactly the pre-U12
    facts: no consuming segments, no threshold, complete, no key evidence.
    No key is derived here: the caller passes `unique_keys`, as
    `keys.catalog_keys` proves them, and the default is none.

    `consuming_segments_json` maps a consuming segment's name to its own ZK
    metadata, as `GET /segments/{table}/{segmentName}/metadata` serves it. A
    segment the caller could not read is absent from the mapping and is
    charged at nothing known, which takes the quote low.
    """
    consuming = consuming_count(externalview_json, size_json)
    return TableFacts(
        table=table,
        types=table_types(config_json),
        time_column=time_column(config_json),
        segments=tuple(segment_facts(seg_metadata_json, size_json)),
        columns=columns,
        consuming=consuming,
        consuming_rows=_consuming_rows(
            consuming, externalview_json, consuming_segments_json
        ),
        complete=metadata_is_complete(seg_metadata_json, size_json),
        unique_keys=unique_keys,
    )


def _consuming_rows(
    consuming: int,
    externalview_json: Any,
    consuming_segments_json: Mapping[str, Any] | None,
) -> tuple[int | None, ...]:
    """One stored row threshold per consuming segment, None where unknown.

    The count is `consuming_count`'s, which is the larger of the externalview
    and `missingSegments`. Where `missingSegments` names more segments than
    the externalview does, the surplus has no name to fetch a threshold with
    and so is charged at nothing known — a None, which takes the quote low.
    """
    if consuming <= 0:
        return ()
    documents = consuming_segments_json or {}
    rows = [
        stored_flush_rows(documents.get(name))
        for name in consuming_segment_names(externalview_json)[:consuming]
    ]
    rows.extend([None] * (consuming - len(rows)))
    return tuple(rows)


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
