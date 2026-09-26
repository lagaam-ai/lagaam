"""Pure controller-JSON parsers, on JSON captured from live Pinot 1.5.1.

Every parser here must survive a shape it does not recognise: an unreadable
shape is "no fact" (empty list, None), which fails safe downstream, never an
exception that costs an agent its grounding.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.metadata import (
    consuming_count,
    assign_missing_to_servers,
    consuming_segment_names,
    merge_segment_metadata,
    metadata_is_complete,
    missing_sealed_segments,
    row_estimate,
    segment_facts,
    segments_by_server,
    stored_flush_rows,
    table_facts,
    table_names,
    table_schema,
    table_types,
    time_column,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def test_table_names_are_bare_and_sorted() -> None:
    names = table_names(load("tables.json"))
    assert names[0] == "airlineStats"
    assert "baseballStats" in names
    assert len(names) == 10
    assert names == sorted(names)
    assert not any(n.endswith("_OFFLINE") for n in names)


@pytest.mark.parametrize("body", [None, {}, {"tables": "nope"}, [], "junk"])
def test_table_names_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert table_names(body) == []


def test_table_types_reads_the_offline_key() -> None:
    assert table_types(load("tableconfig-airlineStats.json")) == frozenset({"OFFLINE"})


def test_table_types_reads_a_hybrid_config() -> None:
    both = {"OFFLINE": {"tableType": "OFFLINE"}, "REALTIME": {"tableType": "REALTIME"}}
    assert table_types(both) == frozenset({"OFFLINE", "REALTIME"})


@pytest.mark.parametrize("body", [None, {}, {"NONSENSE": {}}, "junk", []])
def test_table_types_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert table_types(body) == frozenset()


def test_row_estimate_is_numrows_for_an_offline_table() -> None:
    assert row_estimate(load("metadata-airlineStats.json"), frozenset({"OFFLINE"})) == 9746
    assert row_estimate(load("metadata-baseballStats.json"), frozenset({"OFFLINE"})) == 97889


def test_a_realtime_half_makes_the_row_count_unknown_rather_than_zero() -> None:
    # Measured: a REALTIME table serving 70 rows reported numRows 0, because
    # the controller only learns a segment's size once it is sealed. Zero is a
    # lie an agent would plan against; None says we do not know.
    body = load("rt-metadata.json")
    assert body["numRows"] == 0
    assert row_estimate(body, frozenset({"REALTIME"})) is None
    assert row_estimate(body, frozenset({"OFFLINE", "REALTIME"})) is None


def test_an_unknown_type_set_makes_the_row_count_unknown_rather_than_zero() -> None:
    # An empty set is what an unreadable config yields, and that config is the
    # only thing that could have ruled out a consuming REALTIME half.
    assert row_estimate(load("metadata-airlineStats.json"), frozenset()) is None
    assert row_estimate(load("rt-metadata.json"), frozenset()) is None


@pytest.mark.parametrize("config", [None, [], "junk", {}, {"NONSENSE": {}}])
def test_an_unreadable_config_makes_the_row_count_unknown(config: Any) -> None:
    metadata = load("metadata-airlineStats.json")
    assert metadata["numRows"] == 9746
    assert row_estimate(metadata, table_types(config)) is None


@pytest.mark.parametrize("body", [None, {}, {"numRows": "many"}, "junk", []])
def test_row_estimate_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert row_estimate(body, frozenset({"OFFLINE"})) is None


def test_table_schema_concatenates_all_three_field_specs() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    names = [c.name for c in card.columns]
    assert "Carrier" in names            # dimensionFieldSpecs
    assert "DaysSinceEpoch" in names     # dateTimeFieldSpecs
    assert len(names) == len(set(names))
    by_name = {c.name: c for c in card.columns}
    assert by_name["Carrier"].type == "STRING"
    assert by_name["ActualElapsedTime"].type == "INT"


def test_table_schema_echoes_the_controllers_table_spelling() -> None:
    # The controller's REST paths are case-sensitive, so a lowercased name in
    # the card is a name the agent cannot feed back — see the resolver in engine.
    card = table_schema(
        "pinot",
        "DEFAULT",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlineStats"
    assert card.row_estimate == 9746


# Every singleValueField=false field in the live airlineStats schema.
_MULTI_VALUE = {
    "DivAirportIDs": "INT[]",
    "DivAirportSeqIDs": "INT[]",
    "DivAirports": "STRING[]",
    "DivLongestGTimes": "INT[]",
    "DivTailNums": "STRING[]",
    "DivTotalGTimes": "LONG[]",
    "DivWheelsOffs": "INT[]",
    "DivWheelsOns": "INT[]",
    "RandomAirports": "STRING[]",
}


def test_a_multi_value_column_is_described_as_an_array() -> None:
    # Measured: SELECT DivAirportIDs ... LIMIT 1 returns an array cell with
    # columnDataTypes ["INT_ARRAY"], so describing it as INT invites a scalar
    # comparison that can never match.
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    by_name = {c.name: c.type for c in card.columns}
    for name, expected in _MULTI_VALUE.items():
        assert by_name[name] == expected


def test_a_single_value_column_keeps_its_scalar_type() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    by_name = {c.name: c.type for c in card.columns}
    assert by_name["Carrier"] == "STRING"
    assert by_name["ActualElapsedTime"] == "INT"
    assert by_name["DaysSinceEpoch"] == "INT"
    assert not any(
        t.endswith("[]") for n, t in by_name.items() if n not in _MULTI_VALUE
    )


def test_the_element_type_survives_the_array_marker() -> None:
    schema_json = {
        "dimensionFieldSpecs": [
            {"name": "tags", "dataType": "STRING", "singleValueField": False},
            {"name": "ids", "dataType": "LONG", "singleValueField": False},
        ]
    }
    card = table_schema("pinot", "default", "t", schema_json, {}, {})
    assert [c.type for c in card.columns] == ["STRING[]", "LONG[]"]


def test_an_explicit_single_value_flag_is_still_a_scalar() -> None:
    schema_json = {
        "dimensionFieldSpecs": [
            {"name": "a", "dataType": "STRING", "singleValueField": True},
            {"name": "b", "dataType": "STRING"},
        ]
    }
    card = table_schema("pinot", "default", "t", schema_json, {}, {})
    assert [c.type for c in card.columns] == ["STRING", "STRING"]


def test_a_flag_that_is_not_a_boolean_is_read_as_a_scalar() -> None:
    # Only an explicit false means multi-value; a shape we cannot read must
    # not invent an array type the engine will not agree with.
    schema_json = {
        "dimensionFieldSpecs": [{"name": "a", "dataType": "STRING", "singleValueField": "no"}]
    }
    card = table_schema("pinot", "default", "t", schema_json, {}, {})
    assert [c.type for c in card.columns] == ["STRING"]


def test_pinot_has_no_column_comments() -> None:
    card = table_schema(
        "pinot",
        "default",
        "baseballStats",
        load("schema-baseballStats.json"),
        load("metadata-baseballStats.json"),
        {"OFFLINE": {"tableType": "OFFLINE"}},
    )
    assert card.columns
    assert all(c.comment is None for c in card.columns)


def test_table_schema_survives_metadata_it_cannot_read() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        "not json at all",
        {"OFFLINE": {"tableType": "OFFLINE"}},
    )
    assert card.columns
    assert card.row_estimate is None


def test_table_schema_with_an_unreadable_schema_body_has_no_columns() -> None:
    card = table_schema("pinot", "default", "t", {"nonsense": 1}, {}, {})
    assert card.columns == []
    assert card.row_estimate is None


def test_segment_facts_carry_docs_bytes_time_and_per_column_bytes() -> None:
    facts = segment_facts(
        load("seg-metadata-airlineStats-columns.json"),
        load("size-airlineStats.json"),
    )
    assert len(facts) == 31
    one = next(f for f in facts if f.name == "airlineStats_OFFLINE_16071_16071_0")
    assert one.docs == 289
    assert one.total_bytes == 152577
    assert one.start_ms == 1388534400000
    assert one.end_ms == 1388534400000
    assert one.column_bytes == {"Carrier": 189, "DaysSinceEpoch": 28}
    assert sum(f.docs or 0 for f in facts) == 9746
    assert sum(f.total_bytes or 0 for f in facts) == 4861355


def test_segment_facts_sum_every_index_entry_not_a_fixed_set() -> None:
    """playerID is RAW-encoded: forward_index only, no dictionary."""
    facts = segment_facts(
        load("seg-metadata-baseballStats-columns.json"),
        load("size-baseballStats.json"),
    )
    assert len(facts) == 1
    only = facts[0]
    assert only.docs == 97889
    assert only.total_bytes == 3342450
    assert only.column_bytes["playerID"] == 542226
    assert only.column_bytes["teamID"] == 455 + 173683 + 97897


def test_a_segment_without_a_time_column_has_no_time_range() -> None:
    only = segment_facts(
        load("seg-metadata-baseballStats-columns.json"),
        load("size-baseballStats.json"),
    )[0]
    assert only.start_ms is None
    assert only.end_ms is None


def test_a_segment_the_size_report_never_mentions_has_no_bytes() -> None:
    facts = segment_facts(
        load("seg-metadata-airlineStats-columns.json"), {"offlineSegments": None}
    )
    assert len(facts) == 31
    assert all(f.total_bytes is None for f in facts)
    assert all(f.docs is not None for f in facts)


def test_a_consuming_segments_negative_size_is_no_fact_not_a_credit() -> None:
    facts = segment_facts(
        {"seg0": {"segmentName": "seg0", "totalDocs": 0}},
        {"offlineSegments": {"segments": {"seg0": {"reportedSizeInBytes": -1}}}},
    )
    assert facts[0].total_bytes is None


@pytest.mark.parametrize("body", [None, {}, [], "junk", {"a": "b"}])
def test_segment_facts_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert segment_facts(body, body) == []


def test_time_column_comes_from_the_offline_segments_config() -> None:
    assert time_column(load("tableconfig-airlineStats.json")) == "DaysSinceEpoch"
    assert time_column(load("tableconfig-baseballStats.json")) is None


@pytest.mark.parametrize("body", [None, {}, [], "junk", {"OFFLINE": "nope"}])
def test_time_column_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert time_column(body) is None


def test_table_facts_gather_type_time_column_and_segments() -> None:
    facts = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats.json"),
        load("seg-metadata-airlineStats-columns.json"),
        load("size-airlineStats.json"),
    )
    assert facts.table == "airlineStats"
    assert facts.types == frozenset({"OFFLINE"})
    assert facts.time_column == "DaysSinceEpoch"
    assert len(facts.segments) == 31


def test_consuming_count_reads_externalview_and_the_size_report_together() -> None:
    """6 ONLINE + 1 CONSUMING in externalview, missingSegments 1 in size."""
    assert (
        consuming_count(
            load("externalview-airlineStats-realtime.json"),
            load("size-airlineStats-realtime.json"),
        )
        == 1
    )


def test_consuming_count_reads_a_two_partition_size_report() -> None:
    assert consuming_count(None, load("size-u12upsert.json")) == 2


def test_a_disagreement_between_the_two_charges_the_larger() -> None:
    """Neither document is authoritative, so the larger cannot under-charge."""
    externalview = load("externalview-airlineStats-realtime.json")
    size = load("size-airlineStats-realtime.json")
    size["realtimeSegments"]["missingSegments"] = 4
    assert consuming_count(externalview, size) == 4
    size["realtimeSegments"]["missingSegments"] = 0
    assert consuming_count(externalview, size) == 1


@pytest.mark.parametrize("body", [None, {}, [], "junk", {"REALTIME": "nope"}])
def test_consuming_count_is_zero_when_neither_document_can_be_read(
    body: Any,
) -> None:
    assert consuming_count(body, body) == 0


def test_stored_flush_rows_reads_the_live_consuming_segments_own_threshold() -> None:
    """The capture off the live realtime instance's CONSUMING segment.

    Five keys, and the threshold among them is the one stored at creation.
    """
    body = load("segment-zk-airlineStats-consuming.json")
    assert body["segment.realtime.status"] == "IN_PROGRESS"
    assert stored_flush_rows(body) == 100


def test_stored_flush_rows_reads_the_threshold_as_a_plain_int_too() -> None:
    """Every capture spells it as a string; an int is accepted all the same."""
    assert stored_flush_rows({"segment.flush.threshold.size": 100}) == 100


def test_a_sealed_segment_carries_the_threshold_it_was_created_with() -> None:
    """Measured on u12flush: the segment sealed at exactly 100 while the
    table config said 10 for the last 50 of those rows."""
    assert (
        stored_flush_rows(
            {
                "segment.flush.threshold.size": "100",
                "segment.total.docs": "100",
                "segment.realtime.status": "DONE",
            }
        )
        == 100
    )


@pytest.mark.parametrize(
    "value", ["", "10.5", "500M", "-100", "0", "abc", None, True, 4.0, -100, 0]
)
def test_a_stored_threshold_that_is_not_a_positive_int_is_no_bound(
    value: Any,
) -> None:
    assert stored_flush_rows({"segment.flush.threshold.size": value}) is None


@pytest.mark.parametrize("body", [None, {}, [], "junk", {"other": "1"}])
def test_stored_flush_rows_never_raises_on_a_shape_it_cannot_read(
    body: Any,
) -> None:
    assert stored_flush_rows(body) is None


def test_the_consuming_entry_is_not_a_sealed_fact() -> None:
    """totalDocs 0, no columns, Long.MIN_VALUE crc and -1 bytes, all four."""
    facts = segment_facts(
        load("seg-metadata-airlineStats-realtime-columns.json"),
        load("size-airlineStats-realtime.json"),
    )
    assert len(facts) == 6
    assert all(fact.docs == 100 for fact in facts)
    assert all(fact.total_bytes is not None for fact in facts)
    assert sum(fact.total_bytes or 0 for fact in facts) == 517035
    assert not any("__0__6__" in fact.name for fact in facts)


def test_an_unreadable_sealed_segment_is_kept_and_still_poisons_the_sum() -> None:
    """Three of rule 5's four markers is not a consuming segment."""
    facts = segment_facts(
        {"seg0": {"segmentName": "seg0", "totalDocs": 0, "crc": 12345}},
        {"realtimeSegments": {"segments": {"seg0": {"reportedSizeInBytes": -1}}}},
    )
    assert len(facts) == 1
    assert facts[0].total_bytes is None


def test_metadata_is_complete_when_every_sealed_segment_is_present() -> None:
    assert metadata_is_complete(
        load("seg-metadata-airlineStats-realtime.json"),
        load("size-airlineStats-realtime.json"),
    )


def test_a_truncated_metadata_response_is_incomplete() -> None:
    """Measured: a 2-server table returns one server's half and alternates."""
    assert not metadata_is_complete(
        load("seg-metadata-u12upsert.json"), load("size-u12upsert.json")
    )


def test_a_consuming_segment_missing_from_metadata_does_not_make_it_incomplete() -> (
    None
):
    """A -1 in the size report is consuming, and is exempt by definition."""
    size = load("size-u12upsert.json")
    size["realtimeSegments"]["segments"] = {
        name: body
        for name, body in size["realtimeSegments"]["segments"].items()
        if body["reportedSizeInBytes"] != -1 and name.startswith("u12upsert__0__")
    }
    assert metadata_is_complete(load("seg-metadata-u12upsert.json"), size)


@pytest.mark.parametrize("body", [None, {}, [], "junk"])
def test_completeness_is_true_when_the_size_report_names_nothing(body: Any) -> None:
    """No named sealed segment is nothing to be missing — the pre-U12 behaviour."""
    assert metadata_is_complete(body, body)


def test_consuming_segment_names_come_from_the_externalview() -> None:
    """The names the per-segment metadata fetch is addressed with."""
    assert consuming_segment_names(
        load("externalview-airlineStats-realtime.json")
    ) == ("airlineStats__0__6__20260917T1214Z",)


@pytest.mark.parametrize("body", [None, {}, [], "junk", {"REALTIME": None}])
def test_consuming_segment_names_never_raises_on_a_shape_it_cannot_read(
    body: Any,
) -> None:
    assert consuming_segment_names(body) == ()


def test_table_facts_carry_the_realtime_numbers() -> None:
    facts = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats-realtime.json"),
        load("seg-metadata-airlineStats-realtime.json"),
        load("size-airlineStats-realtime.json"),
        externalview_json=load("externalview-airlineStats-realtime.json"),
        consuming_segments_json={
            "airlineStats__0__6__20260917T1214Z": load(
                "segment-zk-airlineStats-consuming.json"
            )
        },
    )
    assert facts.types == frozenset({"REALTIME"})
    assert len(facts.segments) == 6
    assert facts.consuming == 1
    assert facts.consuming_rows == (100,)
    assert facts.complete is True


def test_a_consuming_segment_nobody_read_has_an_unknown_threshold() -> None:
    """No ZK document for the segment externalview names: rows are unknown,
    which takes the whole quote low rather than trusting the config."""
    facts = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats-realtime.json"),
        load("seg-metadata-airlineStats-realtime.json"),
        load("size-airlineStats-realtime.json"),
        externalview_json=load("externalview-airlineStats-realtime.json"),
    )
    assert facts.consuming == 1
    assert facts.consuming_rows == (None,)


def test_a_consuming_segment_the_externalview_does_not_name_is_unknown() -> None:
    """missingSegments can exceed the CONSUMING names externalview carries.
    The count is still the larger one, and the segments nobody named have no
    threshold to read, so they are charged at nothing known."""
    facts = table_facts(
        "u12upsert",
        load("tableconfig-u12upsert.json"),
        load("seg-metadata-u12upsert.json"),
        load("size-u12upsert.json"),
        externalview_json={"REALTIME": {}},
    )
    # Two missingSegments, no CONSUMING name in the externalview at all.
    assert facts.consuming == 2
    assert facts.consuming_rows == (None, None)


def test_table_facts_without_the_new_documents_are_the_pre_u12_facts() -> None:
    """Every existing call site passes four positional arguments and no more."""
    facts = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats.json"),
        load("seg-metadata-airlineStats-columns.json"),
        load("size-airlineStats.json"),
    )
    assert facts.consuming == 0
    assert facts.consuming_rows == ()
    assert facts.complete is True
    assert len(facts.segments) == 31


def test_table_facts_carry_the_upsert_tables_realtime_numbers() -> None:
    facts = table_facts(
        "u12upsert",
        load("tableconfig-u12upsert.json"),
        load("seg-metadata-u12upsert.json"),
        load("size-u12upsert.json"),
        schema_json=load("schema-u12upsert.json"),
        table_metadata_json=load("metadata-u12upsert.json"),
    )
    assert facts.consuming == 2
    # No externalview and no segment metadata: two consuming segments whose
    # thresholds nobody read. The table config's 200 is not a substitute.
    assert facts.consuming_rows == (None, None)
    assert facts.complete is False


# --- U14: the segments the bulk metadata call left out, and where they live.

_U14_MISSING_ON_7051 = (
    "u14multi__0__0__20260920T1839Z",
    "u14multi__0__1__20260920T1840Z",
    "u14multi__0__2__20260920T1840Z",
)


def test_missing_sealed_segments_names_the_three_the_bulk_call_left_out() -> None:
    """The truncated bulk call is one server's half of a two-server table."""
    assert missing_sealed_segments(
        load("seg-metadata-u14multi-truncated.json"), load("size-u14multi.json")
    ) == frozenset(
        {
            "u14multi__0__0__20260920T1839Z",
            "u14multi__0__1__20260920T1840Z",
            "u14multi__0__2__20260920T1840Z",
        }
    )


def test_a_complete_response_is_missing_nothing() -> None:
    assert (
        missing_sealed_segments(
            load("seg-metadata-airlineStats-realtime.json"),
            load("size-airlineStats-realtime.json"),
        )
        == frozenset()
    )


def test_completeness_is_exactly_nothing_missing() -> None:
    """The guard and the fetch read the same names, so they cannot drift."""
    bulk = load("seg-metadata-u14multi-truncated.json")
    size = load("size-u14multi.json")
    assert metadata_is_complete(bulk, size) is not bool(
        missing_sealed_segments(bulk, size)
    )


@pytest.mark.parametrize("body", [None, {}, [], "junk"])
def test_an_unreadable_size_report_names_nothing_missing(body: Any) -> None:
    assert missing_sealed_segments(body, body) == frozenset()


def test_an_unreadable_metadata_response_is_missing_every_sealed_name() -> None:
    size = load("size-u14multi.json")
    assert missing_sealed_segments("junk", size) == frozenset(
        name
        for name, seg in size["realtimeSegments"]["segments"].items()
        if seg["reportedSizeInBytes"] >= 0
    )


@pytest.mark.parametrize("unreadable", [None, [], "unreadable", 123])
def test_a_sealed_segment_whose_entry_is_not_an_object_is_missing(
    unreadable: Any,
) -> None:
    """`segment_facts` prices only a dict body, so only a dict body is present.

    A `{name: null}` merged in otherwise makes the guard say complete while
    the pricing pass omits the segment — a confident under-quote.
    """
    size = load("size-u14multi.json")
    bulk = load("seg-metadata-u14multi-truncated.json")
    name = next(iter(bulk))
    bulk[name] = unreadable
    assert name in missing_sealed_segments(bulk, size)
    assert not metadata_is_complete(bulk, size)


def test_a_segment_priced_is_a_segment_counted_present() -> None:
    """The guard's names and the pricing pass's names cannot disagree."""
    size = load("size-u14multi.json")
    merged = merge_segment_metadata(
        load("seg-metadata-u14multi-truncated.json"),
        [{name: None for name in _U14_MISSING_ON_7051}],
    )
    priced = {fact.name for fact in segment_facts(merged, size)}
    assert not metadata_is_complete(merged, size)
    assert missing_sealed_segments(merged, size) == frozenset(_U14_MISSING_ON_7051)
    assert not (frozenset(_U14_MISSING_ON_7051) & priced)


def test_segments_by_server_parses_the_servers_document() -> None:
    by_server = segments_by_server(load("servers-u14multi.json"))
    assert [len(names) for names in by_server.values()] == [4, 8]
    assert list(by_server) == ["Server_172.18.0.4_7051", "Server_172.18.0.4_7052"]


@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        "junk",
        [],
        [{"tableName": "t"}],
        [{"serverToSegmentsMap": "junk"}],
        [{"serverToSegmentsMap": {"s": "notalist"}}],
        ["junk"],
    ],
)
def test_segments_by_server_reads_garbage_as_no_servers(body: Any) -> None:
    assert segments_by_server(body) == {}


def test_segments_by_server_unions_the_table_types() -> None:
    """One element per table type, and a hybrid table has two."""
    assert segments_by_server(
        [
            {"serverToSegmentsMap": {"s1": ["a"]}},
            {"serverToSegmentsMap": {"s1": ["b"], "s2": ["c"]}},
        ]
    ) == {"s1": ("a", "b"), "s2": ("c",)}


def test_assign_missing_picks_the_first_server_holding_each_name() -> None:
    """Replicas are byte-identical (measured §3), so the first one is enough."""
    assert assign_missing_to_servers(
        frozenset({"a", "b", "c"}),
        {"s1": ("a", "b"), "s2": ("b", "c"), "s3": ("a",)},
    ) == {"s1": ("a", "b"), "s2": ("c",)}


def test_a_server_holding_nothing_missing_is_absent() -> None:
    assert assign_missing_to_servers(frozenset({"a"}), {"s1": ("a",), "s2": ("z",)}) == {
        "s1": ("a",)
    }


def test_a_name_no_server_holds_is_simply_absent() -> None:
    """The completeness guard still catches it, so nothing is guessed here."""
    assert assign_missing_to_servers(frozenset({"gone"}), {"s1": ("a",)}) == {}


def test_assigning_nothing_missing_calls_nobody() -> None:
    assert assign_missing_to_servers(frozenset(), {"s1": ("a",)}) == {}


def test_the_u14multi_fixtures_assign_the_three_missing_names_to_7051() -> None:
    assignment = assign_missing_to_servers(
        missing_sealed_segments(
            load("seg-metadata-u14multi-truncated.json"), load("size-u14multi.json")
        ),
        segments_by_server(load("servers-u14multi.json")),
    )
    assert assignment == {
        "Server_172.18.0.4_7051": (
            "u14multi__0__0__20260920T1839Z",
            "u14multi__0__1__20260920T1840Z",
            "u14multi__0__2__20260920T1840Z",
        )
    }


def test_merge_keeps_the_bulk_entry_where_both_answered() -> None:
    bulk = {"a": {"segmentName": "a", "totalDocs": 1}}
    per_server = [{"a": {"segmentName": "a", "totalDocs": 999}, "b": {"segmentName": "b"}}]
    assert merge_segment_metadata(bulk, per_server) == {
        "a": {"segmentName": "a", "totalDocs": 1},
        "b": {"segmentName": "b"},
    }


@pytest.mark.parametrize("junk", [None, "junk", [], 7])
def test_merge_ignores_a_response_it_cannot_read(junk: Any) -> None:
    assert merge_segment_metadata({"a": {}}, [junk]) == {"a": {}}
    assert merge_segment_metadata(junk, [{"a": {}}]) == {"a": {}}


def test_merging_the_per_server_answer_completes_the_truncated_response() -> None:
    """The whole point: the guard passes and every sealed segment is priced."""
    size = load("size-u14multi.json")
    merged = merge_segment_metadata(
        load("seg-metadata-u14multi-truncated.json"),
        [load("seg-metadata-u14multi-7051.json")],
    )
    assert metadata_is_complete(merged, size)
    facts = segment_facts(merged, size)
    assert len(facts) == 10
    expected_pk = sum(
        size
        for body in (
            load("seg-metadata-u14multi-7051.json")
            | load("seg-metadata-u14multi-7052.json")
        ).values()
        for column in (body.get("columns") or [])
        if column["columnName"] == "pk"
        for size in column["indexSizeMap"].values()
    )
    assert sum(fact.column_bytes["pk"] for fact in facts) == expected_pk
    assert sum(fact.docs or 0 for fact in facts) == 1000
