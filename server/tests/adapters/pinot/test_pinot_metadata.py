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
    flush_rows,
    metadata_is_complete,
    row_estimate,
    segment_facts,
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


def test_flush_rows_accepts_the_deprecated_size_spelling() -> None:
    """The .size spelling is the deprecated one; a minimal hand-built config."""
    assert (
        flush_rows(
            {
                "REALTIME": {
                    "ingestionConfig": {
                        "streamIngestionConfig": {
                            "streamConfigMaps": [
                                {"realtime.segment.flush.threshold.size": "50000"}
                            ]
                        }
                    }
                }
            }
        )
        == 50000
    )


def test_flush_rows_reads_the_rows_spelling_as_a_string() -> None:
    assert flush_rows(load("tableconfig-airlineStats-realtime.json")) == 100
    assert flush_rows(load("tableconfig-u12upsert.json")) == 200


def test_flush_rows_falls_back_to_the_legacy_stream_config_location() -> None:
    """Absent on every config measured; read so an older cluster is not unbounded."""
    assert (
        flush_rows(
            {
                "REALTIME": {
                    "tableIndexConfig": {
                        "streamConfigs": {
                            "realtime.segment.flush.threshold.rows": "250"
                        }
                    }
                }
            }
        )
        == 250
    )


@pytest.mark.parametrize(
    "value", ["", "10.5", "500M", "-100", "0", "abc", None, True, 4.0]
)
def test_a_threshold_that_is_not_a_positive_int_is_no_bound(value: Any) -> None:
    assert (
        flush_rows(
            {
                "REALTIME": {
                    "ingestionConfig": {
                        "streamIngestionConfig": {
                            "streamConfigMaps": [
                                {"realtime.segment.flush.threshold.rows": value}
                            ]
                        }
                    }
                }
            }
        )
        is None
    )


@pytest.mark.parametrize("body", [None, {}, [], "junk", {"OFFLINE": {}}])
def test_flush_rows_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert flush_rows(body) is None


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


def test_table_facts_carry_the_realtime_numbers() -> None:
    facts = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats-realtime.json"),
        load("seg-metadata-airlineStats-realtime.json"),
        load("size-airlineStats-realtime.json"),
        externalview_json=load("externalview-airlineStats-realtime.json"),
    )
    assert facts.types == frozenset({"REALTIME"})
    assert len(facts.segments) == 6
    assert facts.consuming == 1
    assert facts.flush_rows == 100
    assert facts.complete is True
    assert facts.unique_keys == frozenset()


def test_table_facts_without_the_new_documents_are_the_pre_u12_facts() -> None:
    """Every existing call site passes four positional arguments and no more."""
    facts = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats.json"),
        load("seg-metadata-airlineStats-columns.json"),
        load("size-airlineStats.json"),
    )
    assert facts.consuming == 0
    assert facts.flush_rows is None
    assert facts.complete is True
    assert len(facts.segments) == 31
