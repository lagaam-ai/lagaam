"""Broker JSON to a QueryResult, its failure, or its pruning counters.

Every fixture here is a real HTTP 200 from Pinot 1.5.1 — including the
failures, because every Pinot query error is an HTTP 200. An incomplete
result is a failure and not a warning: a trimmed GROUP BY returns plausible
wrong numbers, measured as 22 groups presented as complete.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.response import (
    ENGINE_FAULT,
    INCOMPLETE_RESULT,
    parse_query_result,
    result_failure,
    surviving_segments,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def test_a_clean_aggregation_carries_no_failure() -> None:
    assert result_failure(load("agg-groupby.json")) is None


def test_a_clean_aggregation_parses_into_rows_and_columns() -> None:
    result = parse_query_result(load("agg-groupby.json"), max_rows=10)
    assert result.columns == ["Carrier", "n"]
    assert result.rows[0] == ["WN", 2008]
    assert result.row_count == 5
    assert result.truncated is False
    assert result.warnings == []


def test_rows_are_capped_and_truncation_is_flagged() -> None:
    # The server asks for max_rows + 1, so more rows than the cap means more exist.
    result = parse_query_result(load("agg-groupby.json"), max_rows=3)
    assert result.row_count == 3
    assert result.truncated is True
    assert result.rows[-1] == ["OO", 1159]


def test_a_trimmed_group_by_is_a_failure_not_a_warning() -> None:
    # Measured: numGroupsLimit=2 returned 22 groups with HTTP 200, partialResult
    # true and numGroupsLimitReached true — plausible numbers that are wrong.
    body = load("numgroupslimit2.json")
    assert body["numGroupsLimitReached"] is True
    assert result_failure(body) == INCOMPLETE_RESULT


def test_a_timeout_is_classified_from_its_exception() -> None:
    assert result_failure(load("timeout1-mse.json")) == "EXCEEDED_TIME_LIMIT"


def test_an_exception_outranks_the_partial_flag() -> None:
    # The timeout body sets partialResult too; the named cause is the better hint.
    assert load("timeout1-mse.json")["partialResult"] is True
    assert result_failure(load("timeout1-mse.json")) != INCOMPLETE_RESULT


def test_an_ingestion_shaped_response_is_an_engine_fault() -> None:
    # Measured: INSERT INTO ... FROM FILE returns HTTP 200 with empty
    # exceptions[], a null requestId and an ingestion task schema. Core's AST
    # allowlist already denies INSERT, so this can only mean the broker
    # answered something that is not a query result.
    body = load("insert-from-file-mse.json")
    assert body["exceptions"] == []
    assert body["requestId"] is None
    assert result_failure(body) == ENGINE_FAULT


def test_a_real_result_with_a_request_id_is_not_an_engine_fault() -> None:
    assert load("agg-groupby.json")["requestId"]
    assert result_failure(load("agg-groupby.json")) is None


def test_the_group_warning_limit_becomes_a_warning_on_the_result() -> None:
    body = load("agg-groupby.json")
    body["numGroupsWarningLimitReached"] = True
    assert result_failure(body) is None
    result = parse_query_result(body, max_rows=10)
    assert len(result.warnings) == 1
    assert "group" in result.warnings[0].lower()


def test_an_unlimited_multistage_selection_parses_and_caps() -> None:
    # Measured: the multi-stage engine returned all 9,746 rows for a query
    # with no LIMIT. The adapter caps what it hands back either way.
    result = parse_query_result(load("nolimit-mse.json"), max_rows=5)
    assert result.row_count == 5
    assert result.truncated is True


@pytest.mark.parametrize("body", [None, "junk", [], 7])
def test_a_body_that_is_not_an_object_is_an_engine_fault(body: Any) -> None:
    assert result_failure(body) == ENGINE_FAULT


def test_a_missing_result_table_parses_as_no_rows() -> None:
    result = parse_query_result({"resultTable": None}, max_rows=10)
    assert result.columns == []
    assert result.rows == []
    assert result.row_count == 0
    assert result.truncated is False


def test_a_malformed_row_does_not_crash_the_parse() -> None:
    body = {
        "resultTable": {
            "dataSchema": {"columnNames": ["a"]},
            "rows": [["ok"], "not a row", ["fine"]],
        }
    }
    result = parse_query_result(body, max_rows=10)
    assert result.rows == [["ok"], ["fine"]]


def test_a_time_filter_survives_three_of_thirty_one_segments() -> None:
    assert surviving_segments(load("explain-v1-timefilter.json")) == 3


def test_no_filter_survives_every_segment() -> None:
    assert surviving_segments(load("explain-v1-nofilter.json")) == 31


def test_a_limit_prune_counts_as_pruning_too() -> None:
    """Measured: this really does process one segment and scan ten docs."""
    assert surviving_segments(load("explain-v1-limitpruned.json")) == 1


def test_an_untrusted_limit_prune_survives_every_segment() -> None:
    """Measured: the OFFSET query's EXPLAIN reports the same 30 ByServer /
    30 ByLimit as the bare LIMIT, then executes over 29 segments. ByServer
    is the total that ByLimit breaks down, so distrusting the limit prune
    has to discount ByServer by it as well."""
    assert (
        surviving_segments(load("explain-v1-limitpruned.json"), trust_limit_prune=False)
        == 31
    )
    assert (
        surviving_segments(load("explain-v1-limitpruned.json"), trust_limit_prune=True)
        == 1
    )


def test_an_untrusted_limit_prune_still_reads_a_value_prune() -> None:
    """Only the limit prune is offset-blind; a predicate's prune is real."""
    assert (
        surviving_segments(
            load("explain-v1-timefilter.json"), trust_limit_prune=False
        )
        == 3
    )


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


def test_an_unmeasured_broker_counter_is_not_read() -> None:
    """ByBroker has never been observed non-zero on 1.5.1 and is not known to
    be a breakdown of numSegmentsQueried; reading it risks under-counting."""
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 10,
                "numSegmentsPrunedByBroker": 21,
                "numSegmentsPrunedByServer": 0,
                "numDocsScanned": 0,
            }
        )
        == 10
    )


def test_an_unmeasured_invalid_counter_is_not_read() -> None:
    """PrunedInvalid has never been observed non-zero on 1.5.1 either."""
    assert (
        surviving_segments(
            {
                "numSegmentsQueried": 31,
                "numSegmentsPrunedInvalid": 30,
                "numSegmentsPrunedByValue": 3,
                "numDocsScanned": 0,
            }
        )
        == 28
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
