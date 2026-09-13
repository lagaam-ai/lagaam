"""The quotation arithmetic: a bound that is never lower than the truth.

Every number here is either hand-built to isolate one rule or read from the
JSON captured off live Pinot 1.5.1, never transcribed from prose.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.metadata import SegmentFact, TableFacts, table_facts
from lagaam.adapters.pinot.quote import (
    quote,
    surviving_bytes,
    surviving_docs,
    surviving_segments,
)

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


def facts(*segments: SegmentFact, types: frozenset[str] = frozenset({"OFFLINE"})) -> TableFacts:
    return TableFacts(table="t", types=types, time_column=None, segments=segments)


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


def test_a_realtime_half_is_quoted_low_however_good_the_numbers_look() -> None:
    """U12 charges consuming segments; until then a REALTIME half is unknown."""
    table = facts(seg("a", 10, 100, x=5), types=frozenset({"OFFLINE", "REALTIME"}))
    estimate = quote([(table, None)], frozenset({"x"}), max_intermediate_rows=10)
    assert estimate.confidence == "low"
    assert estimate.scanned_bytes is None


def test_one_unpriceable_table_costs_the_whole_byte_quote() -> None:
    good = facts(seg("a", 10, 100, x=5))
    bad = facts(seg("b", 10, None, y=5))
    estimate = quote([(good, None), (bad, None)], None, max_intermediate_rows=20)
    assert estimate.scanned_bytes is None
    assert estimate.confidence == "low"
    assert estimate.row_estimate == 20


def test_quote_with_no_tables_is_low_confidence() -> None:
    assert quote([], frozenset(), None).confidence == "low"


def test_a_time_filter_survives_three_of_thirty_one_segments() -> None:
    assert surviving_segments(load("explain-v1-timefilter.json")) == 3


def test_no_filter_survives_every_segment() -> None:
    assert surviving_segments(load("explain-v1-nofilter.json")) == 31


def test_a_limit_prune_counts_as_pruning_too() -> None:
    """Measured: this really does process one segment and scan ten docs."""
    assert surviving_segments(load("explain-v1-limitpruned.json")) == 1


def test_the_pruned_counters_are_maxed_never_summed() -> None:
    """ByServer is the total; ByValue and ByLimit break it down, so a sum
    would claim 56 of 31 pruned and quote a negative scan."""
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 31,
                "numSegmentsPrunedByServer": 28,
                "numSegmentsPrunedByValue": 28,
                "numSegmentsPrunedByLimit": 0,
                "numDocsScanned": 0,
            }
        )
        == 3
    )


def test_a_counter_only_ever_seen_alone_is_still_read() -> None:
    """Measurement 6: the same predicate once registered only as ByValue."""
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 31,
                "numSegmentsPrunedByServer": 0,
                "numSegmentsPrunedByValue": 28,
                "numDocsScanned": 0,
            }
        )
        == 3
    )


def test_every_segment_pruned_still_charges_one() -> None:
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 31,
                "numSegmentsPrunedByServer": 31,
                "numDocsScanned": 0,
            }
        )
        == 1
    )


def test_an_explain_that_scanned_anything_is_not_an_oracle() -> None:
    """EXPLAIN must never execute; if it did, we misread the statement."""
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 31,
                "numSegmentsPrunedByServer": 28,
                "numDocsScanned": 1,
            }
        )
        is None
    )


def test_an_explain_carrying_an_exception_is_no_oracle() -> None:
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 31,
                "numDocsScanned": 0,
                "exceptions": [{"errorCode": 150, "message": "multi-stage only"}],
            }
        )
        is None
    )


@pytest.mark.parametrize(
    "body",
    [
        None,
        {},
        [],
        "junk",
        {"numSegmentsQueried": 0, "numDocsScanned": 0},
        {"numSegmentsQueried": "31", "numDocsScanned": 0},
    ],
)
def test_surviving_segments_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert surviving_segments(body) is None
