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
