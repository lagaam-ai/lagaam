"""Query budgets: the gate that turns a CostEstimate into allow/deny.

The block message is part of the API — it must tell the agent exactly how to
shrink the query (add a filter, a LIMIT), the same way the domain errors do.
"""

import pytest
from pydantic import ValidationError

from lagaam.core.budget import (
    DEFAULT_MAX_SCAN_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    MAX_RETURNED_ROWS_CEILING,
    QueryBudget,
    enforce_budget,
)
from lagaam.core.errors import BudgetExceededError
from lagaam.core.models import CostEstimate


def under() -> CostEstimate:
    return CostEstimate(scanned_bytes=1_000_000, row_estimate=1000)


# --- what passes ---------------------------------------------------------


def test_estimate_within_budget_passes() -> None:
    enforce_budget(under(), QueryBudget(max_scan_bytes=5_000_000))


def test_no_limits_set_allows_anything_high_confidence() -> None:
    # An empty budget is "no scan/row limit"; a sized query is fine.
    enforce_budget(under(), QueryBudget())


def test_estimate_exactly_at_limit_passes() -> None:
    est = CostEstimate(scanned_bytes=5_000_000, row_estimate=10)
    enforce_budget(est, QueryBudget(max_scan_bytes=5_000_000))


# --- what is blocked, and how it teaches ---------------------------------


def test_over_scan_budget_blocks_with_actionable_message() -> None:
    est = CostEstimate(scanned_bytes=48 * 1024**3, row_estimate=2_000_000)
    with pytest.raises(BudgetExceededError) as exc:
        enforce_budget(est, QueryBudget(max_scan_bytes=5 * 1024**3))
    msg = str(exc.value)
    assert "48" in msg and "GB" in msg  # what it would scan
    assert "5" in msg  # the budget
    assert "filter" in msg.lower() or "limit" in msg.lower()  # how to fix


def test_over_row_budget_blocks() -> None:
    est = CostEstimate(scanned_bytes=1000, row_estimate=10_000_000)
    with pytest.raises(BudgetExceededError, match="row"):
        enforce_budget(est, QueryBudget(max_rows=1_000_000))


# --- the crux: low-confidence estimates fail safe ------------------------


def test_low_confidence_is_blocked_when_a_scan_budget_exists() -> None:
    # We could not size the query; with a scan budget in force, admitting it
    # would defeat the gate. Block and tell the agent to make it estimable.
    est = CostEstimate(confidence="low")
    with pytest.raises(BudgetExceededError, match="estimate"):
        enforce_budget(est, QueryBudget(max_scan_bytes=5_000_000))


def test_low_confidence_is_blocked_when_only_a_row_budget_exists() -> None:
    # Fail-safe is per dimension: an unestimable query must not slip past a
    # row budget just because no scan budget was set.
    est = CostEstimate(confidence="low")
    with pytest.raises(BudgetExceededError, match="row"):
        enforce_budget(est, QueryBudget(max_rows=100))


def test_missing_row_estimate_is_blocked_against_a_row_budget() -> None:
    # Bytes known, rows unknown: the row budget has nothing to check, so it
    # must fail safe rather than silently allow.
    est = CostEstimate(scanned_bytes=1000, row_estimate=None, confidence="high")
    with pytest.raises(BudgetExceededError, match="row"):
        enforce_budget(est, QueryBudget(max_rows=100))


def test_low_confidence_allowed_when_no_limits_set() -> None:
    # Nothing is gated, so there is nothing to fail safe on.
    enforce_budget(CostEstimate(confidence="low"), QueryBudget())


def test_budget_rejects_nonpositive_limits() -> None:
    with pytest.raises(ValueError):
        QueryBudget(max_scan_bytes=0)
    with pytest.raises(ValueError):
        QueryBudget(timeout_seconds=-1)


def test_from_env_reads_all_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAGAAM_MAX_SCAN_BYTES", "5368709120")
    monkeypatch.setenv("LAGAAM_MAX_ROWS", "1000000")
    monkeypatch.setenv("LAGAAM_MAX_RETURNED_ROWS", "500")
    monkeypatch.setenv("LAGAAM_QUERY_TIMEOUT", "30")
    budget = QueryBudget.from_env()
    assert budget.max_scan_bytes == 5368709120
    assert budget.max_rows == 1_000_000
    assert budget.max_returned_rows == 500
    assert budget.timeout_seconds == 30.0


def test_scan_row_budget_does_not_gate_returned_rows() -> None:
    # max_rows is a scan-estimate gate; the returned-row cap is its own knob.
    budget = QueryBudget(max_rows=1_000_000)
    assert budget.max_returned_rows is None


def test_from_env_falls_back_to_a_real_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An unconfigured server still has a gate: scan bytes and timeout default
    # to finite ceilings rather than to unlimited.
    for var in (
        "LAGAAM_MAX_SCAN_BYTES",
        "LAGAAM_MAX_ROWS",
        "LAGAAM_MAX_RETURNED_ROWS",
        "LAGAAM_QUERY_TIMEOUT",
    ):
        monkeypatch.delenv(var, raising=False)
    budget = QueryBudget.from_env()
    assert budget.max_scan_bytes == DEFAULT_MAX_SCAN_BYTES
    assert budget.timeout_seconds == DEFAULT_TIMEOUT_SECONDS
    assert budget.max_rows is None  # row scan stays ungated by default
    assert budget.max_returned_rows is None  # server applies its own row cap


def test_returned_row_cap_is_bounded_above() -> None:
    # Returned rows are materialized in this process, so an unbounded cap is
    # an OOM of the gate itself.
    with pytest.raises(ValidationError):
        QueryBudget(max_returned_rows=MAX_RETURNED_ROWS_CEILING + 1)


def test_from_env_rejects_garbage_with_a_clear_message(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAGAAM_MAX_SCAN_BYTES", "abc")
    with pytest.raises(ValueError, match="LAGAAM_MAX_SCAN_BYTES"):
        QueryBudget.from_env()


def test_from_env_rejects_nonpositive(monkeypatch: pytest.MonkeyPatch) -> None:
    # A 0 or negative limit is almost certainly a mistake; fail closed loudly.
    monkeypatch.setenv("LAGAAM_MAX_ROWS", "0")
    with pytest.raises(ValueError):
        QueryBudget.from_env()


def test_from_env_clamps_an_oversized_returned_row_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A deployment that worked yesterday must not fail to boot over a number
    # we can safely lower.
    monkeypatch.setenv("LAGAAM_MAX_RETURNED_ROWS", "500000")
    assert QueryBudget.from_env().max_returned_rows == MAX_RETURNED_ROWS_CEILING


def test_from_env_rejects_a_zero_scan_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An explicit 0 once fell through `or` to the permissive default, giving
    # an operator who asked to deny everything the opposite.
    monkeypatch.setenv("LAGAAM_MAX_SCAN_BYTES", "0")
    with pytest.raises(ValidationError):
        QueryBudget.from_env()


def test_from_env_honours_an_explicit_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAGAAM_MAX_SCAN_BYTES", "1024")
    monkeypatch.setenv("LAGAAM_QUERY_TIMEOUT", "5")
    budget = QueryBudget.from_env()
    assert budget.max_scan_bytes == 1024
    assert budget.timeout_seconds == 5.0


# --- widest-operator row budget -------------------------------------------


def test_a_query_within_the_intermediate_row_budget_passes() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    estimate = CostEstimate(scanned_bytes=10, max_intermediate_rows=240_700)
    enforce_budget(estimate, budget)


def test_a_product_over_the_intermediate_row_budget_is_denied() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    estimate = CostEstimate(scanned_bytes=10, max_intermediate_rows=902_625_000_0)
    with pytest.raises(BudgetExceededError) as err:
        enforce_budget(estimate, budget)
    message = str(err.value)
    assert "9,026,250,000" in message
    assert "1,000,000,000" in message
    # The agent must learn that a LIMIT cannot fix a product.
    assert "LIMIT" in message


def test_a_missing_intermediate_row_count_is_denied() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    with pytest.raises(BudgetExceededError):
        enforce_budget(CostEstimate(scanned_bytes=10), budget)


def test_a_low_confidence_estimate_is_denied_on_intermediate_rows() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    estimate = CostEstimate(confidence="low", max_intermediate_rows=5)
    with pytest.raises(BudgetExceededError):
        enforce_budget(estimate, budget)


def test_an_ungated_intermediate_row_dimension_admits_anything() -> None:
    budget = QueryBudget(max_scan_bytes=100)
    enforce_budget(
        CostEstimate(scanned_bytes=10, max_intermediate_rows=10**15), budget
    )


def test_the_env_budget_gates_intermediate_rows_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "LAGAAM_MAX_SCAN_BYTES",
        "LAGAAM_QUERY_TIMEOUT",
        "LAGAAM_MAX_ROWS",
        "LAGAAM_MAX_RETURNED_ROWS",
        "LAGAAM_MAX_INTERMEDIATE_ROWS",
    ):
        monkeypatch.delenv(name, raising=False)
    assert QueryBudget.from_env().max_intermediate_rows == 50_000_000


def test_the_env_budget_reads_an_explicit_intermediate_row_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAGAAM_MAX_INTERMEDIATE_ROWS", "5000")
    assert QueryBudget.from_env().max_intermediate_rows == 5000
