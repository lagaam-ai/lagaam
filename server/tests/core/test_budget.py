"""Query budgets: the gate that turns a CostEstimate into allow/deny.

The block message is part of the API — it must tell the agent exactly how to
shrink the query (add a filter, a LIMIT), the same way the domain errors do.
"""

import pytest

from lagaam.core.budget import QueryBudget, enforce_budget
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
    monkeypatch.setenv("LAGAAM_QUERY_TIMEOUT", "30")
    budget = QueryBudget.from_env()
    assert budget.max_scan_bytes == 5368709120
    assert budget.max_rows == 1_000_000
    assert budget.timeout_seconds == 30.0


def test_from_env_defaults_to_no_limits(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in ("LAGAAM_MAX_SCAN_BYTES", "LAGAAM_MAX_ROWS", "LAGAAM_QUERY_TIMEOUT"):
        monkeypatch.delenv(var, raising=False)
    budget = QueryBudget.from_env()
    assert budget == QueryBudget()


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
