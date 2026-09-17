"""The quotation arithmetic: a bound that is never lower than the truth.

Every number here is either hand-built to isolate one rule or read from the
JSON captured off live Pinot 1.5.1, never transcribed from prose.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.metadata import SegmentFact, TableFacts, table_facts
from lagaam.adapters.pinot.quote import quote, surviving_bytes, surviving_docs

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def seg(name: str, docs: int | None, total: int | None, **columns: int) -> SegmentFact:
    return SegmentFact(
        name=name,
        docs=docs,
        total_bytes=total,
        start_ms=None,
        end_ms=None,
        column_bytes=dict(columns),
    )


def facts(
    *segments: SegmentFact,
    types: frozenset[str] = frozenset({"OFFLINE"}),
    columns: frozenset[str] = frozenset(),
    consuming: int = 0,
    flush_rows: int | None = None,
    complete: bool = True,
) -> TableFacts:
    return TableFacts(
        table="t",
        types=types,
        time_column=None,
        segments=segments,
        columns=columns,
        consuming=consuming,
        flush_rows=flush_rows,
        complete=complete,
    )


def realtime() -> TableFacts:
    return table_facts(
        "airlineStats",
        load("tableconfig-airlineStats-realtime.json"),
        load("seg-metadata-airlineStats-realtime-columns.json"),
        load("size-airlineStats-realtime.json"),
        frozenset({"carrier", "dayssinceepoch"}),
        externalview_json=load("externalview-airlineStats-realtime.json"),
    )


def airline() -> TableFacts:
    return table_facts(
        "airlineStats",
        load("tableconfig-airlineStats.json"),
        load("seg-metadata-airlineStats-columns.json"),
        load("size-airlineStats.json"),
    )


def baseball() -> TableFacts:
    return table_facts(
        "baseballStats",
        load("tableconfig-baseballStats.json"),
        load("seg-metadata-baseballStats-columns.json"),
        load("size-baseballStats.json"),
    )


def test_surviving_docs_charge_the_largest_k_not_the_first_k() -> None:
    table = facts(seg("a", 10, 1), seg("b", 500, 1), seg("c", 20, 1))
    assert surviving_docs(table, 2) == 520
    assert surviving_docs(table, 1) == 500


def test_surviving_none_charges_every_segment() -> None:
    table = facts(seg("a", 10, 1), seg("b", 500, 1))
    assert surviving_docs(table, None) == 510


def test_a_surviving_count_over_the_segment_count_charges_them_all() -> None:
    table = facts(seg("a", 10, 1), seg("b", 500, 1))
    assert surviving_docs(table, 99) == 510


def test_docs_and_bytes_pick_their_largest_k_independently() -> None:
    """The oracle says how many survive, not which, so each takes its worst."""
    table = facts(seg("a", 100, 1, x=1), seg("b", 1, 100, x=100))
    assert surviving_docs(table, 1) == 100
    assert surviving_bytes(table, 1, frozenset({"x"})) == 100


def test_bytes_charge_only_the_referenced_columns() -> None:
    table = facts(seg("a", 10, 999, wanted=7, ignored=500))
    assert surviving_bytes(table, None, frozenset({"wanted"})) == 7


def test_a_table_matching_no_referenced_column_falls_back_to_whole_segments() -> None:
    table = facts(seg("a", 10, 999, other=7))
    assert surviving_bytes(table, None, frozenset({"absent"})) == 999


def test_a_segment_matching_no_referenced_column_is_charged_whole() -> None:
    table = facts(seg("a", 10, 100, x=5), seg("b", 10, 500_000))
    assert surviving_bytes(table, None, frozenset({"x"})) == 500_005

    table_no_size = facts(seg("a", 10, 100, x=5), seg("b", 10, None))
    assert surviving_bytes(table_no_size, None, frozenset({"x"})) is None


def test_a_segment_missing_one_of_the_tables_columns_is_charged_whole() -> None:
    """Measured: the controller answered ?columns=league&columns=playerid with
    league alone (36,739 bytes), so one match made the segment look priced
    while 578,965 bytes of playerID went uncharged."""
    table = facts(
        seg("a", 10, 500_000, league=36_739),
        columns=frozenset({"league", "playerid"}),
    )
    assert surviving_bytes(table, None, frozenset({"league", "playerid"})) == 500_000


def test_a_segment_carrying_every_resolved_column_is_charged_per_column() -> None:
    table = facts(
        seg("a", 10, 500_000, league=36_739, playerID=578_965 - 36_739),
        columns=frozenset({"league", "playerid"}),
    )
    assert surviving_bytes(table, None, frozenset({"league", "playerid"})) == 578_965


def test_without_a_resolved_column_set_one_match_still_prices_the_segment() -> None:
    """No schema, no way to know a column is missing rather than absent."""
    table = facts(seg("a", 10, 500_000, league=36_739))
    assert surviving_bytes(table, None, frozenset({"league", "playerid"})) == 36_739


def test_unresolvable_columns_fall_back_to_whole_segments() -> None:
    table = facts(seg("a", 10, 999, other=7))
    assert surviving_bytes(table, None, None) == 999


def test_bytes_are_none_when_the_fallback_has_no_segment_size() -> None:
    table = facts(seg("a", 10, None, other=7))
    assert surviving_bytes(table, None, None) is None


def test_a_segment_without_docs_makes_the_doc_bound_unknown() -> None:
    table = facts(seg("a", 10, 1), seg("b", None, 1))
    assert surviving_docs(table, None) is None


def test_the_airline_time_filter_bound_is_above_what_execution_scanned() -> None:
    """Measured: the same query really scanned 1,101 docs over 3 segments."""
    assert surviving_docs(airline(), 3) == 1234
    assert surviving_docs(airline(), None) == 9746


def test_the_airline_byte_bound_charges_only_the_two_captured_columns() -> None:
    wanted = frozenset({"carrier", "dayssinceepoch"})
    whole = surviving_bytes(airline(), None, wanted)
    assert whole is not None
    assert whole < 4861355
    assert surviving_bytes(airline(), 3, wanted) is not None


def test_quote_sums_over_tables_and_is_high_confidence_with_bytes() -> None:
    estimate = quote(
        [(airline(), 3), (baseball(), None)],
        frozenset({"carrier", "teamid"}),
        max_intermediate_rows=1234,
    )
    assert estimate.row_estimate == 1234 + 97889
    assert estimate.scanned_bytes is not None
    assert estimate.confidence == "high"
    assert estimate.max_intermediate_rows == 1234


def test_the_consuming_segment_is_charged_at_the_flush_threshold() -> None:
    """Six sealed at 100 docs, one consuming bounded at 100 by the config."""
    table = realtime()
    assert table.consuming == 1
    assert table.flush_rows == 100
    assert surviving_docs(table, None) == 700


def test_the_consuming_charge_survives_a_filter_that_prunes_every_sealed_segment(
) -> None:
    """Measured: a time filter can never remove a consuming segment's cost."""
    table = realtime()
    # Sealed k floors at one segment; the consuming charge is added outside it.
    assert surviving_docs(table, 0) == 100 + 100


def test_the_consuming_bytes_are_the_worst_sealed_ratio_ceiling_divided() -> None:
    table = realtime()
    assert surviving_bytes(table, None, None) == 517035 + 87136
    assert (
        surviving_bytes(table, None, frozenset({"carrier", "dayssinceepoch"}))
        == 548 + 114
    )


def test_the_ratio_is_per_segment_and_maximised_never_averaged() -> None:
    """A 10-doc 1000-byte segment beside a 1000-doc 1000-byte one bounds at 100/doc."""
    table = facts(
        seg("small", 10, 1000),
        seg("big", 1000, 1000),
        types=frozenset({"REALTIME"}),
        consuming=1,
        flush_rows=5,
    )
    assert surviving_bytes(table, None, None) == 2000 + 500


def test_the_bytes_product_is_ceiling_divided_in_integer_arithmetic() -> None:
    """3 x 7 / 2 is 10.5, and a bound may not be shaved to 10."""
    table = facts(
        seg("a", 2, 7), types=frozenset({"REALTIME"}), consuming=1, flush_rows=3
    )
    assert surviving_bytes(table, None, None) == 7 + 11


def test_more_than_one_consuming_segment_is_charged_once_each() -> None:
    table = facts(
        seg("a", 100, 1000),
        types=frozenset({"REALTIME"}),
        consuming=2,
        flush_rows=50,
    )
    assert surviving_docs(table, None) == 100 + 100
    assert surviving_bytes(table, None, None) == 1000 + 1000


def test_no_flush_threshold_makes_both_numbers_unknown() -> None:
    """Rule 7: nothing else in the catalog bounds a consuming segment."""
    table = facts(
        seg("a", 100, 1000), types=frozenset({"REALTIME"}), consuming=1
    )
    assert surviving_docs(table, None) is None
    assert surviving_bytes(table, None, None) is None


def test_no_consuming_segment_makes_the_threshold_irrelevant() -> None:
    table = facts(seg("a", 100, 1000), types=frozenset({"REALTIME"}))
    assert surviving_docs(table, None) == 100
    assert surviving_bytes(table, None, None) == 1000


def test_an_all_consuming_table_has_no_ratio_to_take() -> None:
    """Bytes only. Rows still hold: the threshold bounds them without a ratio."""
    table = facts(types=frozenset({"REALTIME"}), consuming=1, flush_rows=100)
    assert surviving_bytes(table, None, None) is None
    assert surviving_docs(table, None) == 100


def test_a_sealed_segment_with_zero_docs_is_no_basis_for_a_ratio() -> None:
    """Dividing by its docs is not a bound, it is a crash."""
    table = facts(
        seg("empty", 0, 900), types=frozenset({"REALTIME"}), consuming=1, flush_rows=10
    )
    assert surviving_bytes(table, None, None) is None


def test_incomplete_metadata_withholds_both_numbers() -> None:
    """A confident sum over half a table is the failure the gate exists to stop."""
    table = facts(seg("a", 100, 1000), complete=False)
    assert surviving_docs(table, None) is None
    assert surviving_bytes(table, None, None) is None


def test_a_realtime_table_now_quotes_at_high_confidence() -> None:
    """Replaces the blanket REALTIME guard: the type is no longer a reason."""
    table = realtime()
    estimate = quote(
        [(table, None)], frozenset({"carrier", "dayssinceepoch"}), 700
    )
    assert estimate.row_estimate == 700
    assert estimate.scanned_bytes == 662
    assert estimate.confidence == "high"


def test_a_realtime_table_nobody_can_bound_still_quotes_low() -> None:
    table = facts(
        seg("a", 100, 1000), types=frozenset({"REALTIME"}), consuming=1
    )
    estimate = quote([(table, None)], None, 100)
    assert estimate.scanned_bytes is None
    assert estimate.row_estimate is None
    assert estimate.confidence == "low"


def test_a_hybrid_table_is_charged_as_one_segment_set() -> None:
    """Both halves' sealed segments come from the same two documents; there is
    no per-half arithmetic. Not measurable live: -type HYBRID cannot run in a
    container on 1.5.1, so this is the unit test that stands for it."""
    offline = load("size-airlineStats.json")
    realtime_size = load("size-airlineStats-realtime.json")
    merged = {
        "offlineSegments": offline["offlineSegments"],
        "realtimeSegments": realtime_size["realtimeSegments"],
    }
    merged_metadata = {
        **load("seg-metadata-airlineStats-columns.json"),
        **load("seg-metadata-airlineStats-realtime-columns.json"),
    }
    table = table_facts(
        "airlineStats",
        load("tableconfig-airlineStats-realtime.json"),
        merged_metadata,
        merged,
        frozenset({"carrier", "dayssinceepoch"}),
        externalview_json=load("externalview-airlineStats-realtime.json"),
    )
    assert table.types == frozenset({"REALTIME"})
    assert len(table.segments) == 31 + 6
    assert table.consuming == 1
    assert surviving_docs(table, None) == 9746 + 600 + 100


def test_one_unpriceable_table_costs_the_whole_byte_quote() -> None:
    good = facts(seg("a", 10, 100, x=5))
    bad = facts(seg("b", 10, None, y=5))
    estimate = quote([(good, None), (bad, None)], None, max_intermediate_rows=20)
    assert estimate.scanned_bytes is None
    assert estimate.confidence == "low"
    assert estimate.row_estimate == 20


def test_quote_with_no_tables_is_low_confidence() -> None:
    assert quote([], frozenset(), None).confidence == "low"
