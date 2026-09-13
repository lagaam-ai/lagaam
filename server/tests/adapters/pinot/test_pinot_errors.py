"""Pinot errorCode plus message prefix to a core hint code.

Every case here is a real exceptions[] entry captured from Pinot 1.5.1: the
same logical error carries different codes per engine (a bad column is 710 on
the single-stage engine and 700 on the multi-stage one), and the multi-stage
engine folds bad column, bad function and unsupported DML into 700 alike — so
classification needs the code AND a message prefix.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.errors import classify
from lagaam.core.query_errors import hint_for_engine_error, is_self_correctable

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def first_exception(case: dict[str, Any]) -> tuple[int, str]:
    exc = case["exceptions"][0]
    return exc["errorCode"], exc["message"]


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("v1:bad column", "COLUMN_NOT_FOUND"),
        ("v1:unknown table", "TABLE_NOT_FOUND"),
        ("v1:syntax error", "SYNTAX_ERROR"),
        ("v1:unknown function", "FUNCTION_NOT_FOUND"),
        ("MSE:bad column", "COLUMN_NOT_FOUND"),
        ("MSE:unknown table", "TABLE_NOT_FOUND"),
        ("MSE:syntax error", "SYNTAX_ERROR"),
        ("MSE:unknown function", "FUNCTION_NOT_FOUND"),
    ],
)
def test_every_measured_error_classifies(case: str, expected: str) -> None:
    code, message = first_exception(load("errors.json")[case])
    assert classify(code, message) == expected


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("useMultistageEngine=true;maxRowsInJoin=5", "EXCEEDED_ROW_LIMIT"),
        ("useMultistageEngine=true;maxRowsInWindow=5", "EXCEEDED_ROW_LIMIT"),
        ("maxQueryResponseSizeBytes=100", "RESPONSE_TOO_LARGE"),
        ("maxServerResponseSizeBytes=100", "RESPONSE_TOO_LARGE"),
    ],
)
def test_every_measured_cap_classifies(case: str, expected: str) -> None:
    code, message = first_exception(load("query-options-matrix.json")[case])
    assert classify(code, message) == expected


def test_the_multistage_timeout_is_a_time_limit() -> None:
    assert classify(400, "BrokerTimeoutError: Timed out while planning query") == (
        "EXCEEDED_TIME_LIMIT"
    )


def test_the_single_stage_timeout_is_a_time_limit() -> None:
    assert classify(427, "1 servers [172.17.0.2_O] not responded") == (
        "EXCEEDED_TIME_LIMIT"
    )


def test_the_oversized_response_503_is_the_only_one_that_reads_as_too_large() -> None:
    # QueryScheduler reuses QUERY_CANCELLATION (503) for the size refusal, so
    # the measured message is the only thing separating the two meanings.
    code, message = first_exception(
        load("query-options-matrix.json")["maxQueryResponseSizeBytes=100"]
    )
    assert code == 503
    assert "exceeds threshold" in message
    assert classify(code, message) == "RESPONSE_TOO_LARGE"


def test_a_cancelled_leaf_is_not_blamed_on_the_query() -> None:
    # LeafOperator emits 503 for a cancellation; blaming the query would tell
    # the agent to shrink a result that was never too large.
    assert not is_self_correctable(
        classify(503, "Cancelled while waiting for leaf results")
    )


def test_a_bare_cancellation_503_is_not_self_correctable() -> None:
    assert not is_self_correctable(classify(503, "QueryCancellationError"))


def test_the_serialized_size_prefix_alone_reads_as_too_large() -> None:
    assert classify(503, "Serialized query response size 5190 is over budget") == (
        "RESPONSE_TOO_LARGE"
    )


def test_an_access_denial_is_a_permission_problem_the_agent_can_act_on() -> None:
    assert classify(180, "AccessDenied: no access to table airlineStats") == (
        "PERMISSION_DENIED"
    )
    assert is_self_correctable("PERMISSION_DENIED")


def test_other_validation_errors_are_not_supported_rather_than_a_guess() -> None:
    assert classify(700, "QueryValidationError: something else entirely") == (
        "NOT_SUPPORTED"
    )


def test_an_unmapped_code_is_not_blamed_on_the_agent() -> None:
    # Unknown maps to no core hint, so the engine takes the blame, not the query.
    assert not is_self_correctable(classify(999, "who knows"))


def test_the_new_core_codes_carry_agent_facing_hints() -> None:
    row_limit = hint_for_engine_error("EXCEEDED_ROW_LIMIT")
    assert "join" in row_limit
    assert is_self_correctable("EXCEEDED_ROW_LIMIT")
    too_large = hint_for_engine_error("RESPONSE_TOO_LARGE")
    assert "LIMIT" in too_large
    assert is_self_correctable("RESPONSE_TOO_LARGE")
    incomplete = hint_for_engine_error("INCOMPLETE_RESULT")
    assert "incomplete" in incomplete
    assert is_self_correctable("INCOMPLETE_RESULT")


def test_a_broker_message_never_becomes_the_agent_facing_text() -> None:
    # Broker messages name broker and server IPs, ports and request ids.
    code, message = first_exception(
        load("query-options-matrix.json")["maxQueryResponseSizeBytes=100"]
    )
    assert "Broker_172.17.0.2_8000" in message
    assert "172.17.0.2" not in hint_for_engine_error(classify(code, message))
