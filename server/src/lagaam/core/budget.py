"""Query budgets: allow or deny a query from its pre-execution estimate.

The gate the whole design turns on. A CostEstimate (U4) meets a QueryBudget
here, before anything runs. Denials are teachable — they name the number that
blew the budget and how to shrink it — because the agent self-corrects on the
text.

Fail-safe rule: a low-confidence estimate means we could not size the query.
If a scan budget is in force, admitting it would defeat the gate, so we block
and tell the agent to make the query estimable. With no scan budget there is
nothing to fail safe on.
"""

import os

from pydantic import BaseModel, Field

from lagaam.core.cost import human_bytes
from lagaam.core.errors import BudgetExceededError
from lagaam.core.models import CostEstimate


class QueryBudget(BaseModel):
    """Per-query ceilings. Unset (None) means that dimension is not gated."""

    max_scan_bytes: int | None = Field(default=None, gt=0)
    max_rows: int | None = Field(default=None, gt=0)
    # Enforced at execution time (U6); validated here so it is coherent.
    timeout_seconds: float | None = Field(default=None, gt=0)

    @classmethod
    def from_env(cls) -> "QueryBudget":
        """Server-wide default budget from env; per-agent budgets arrive in U7."""
        return cls(
            max_scan_bytes=_int_env("LAGAAM_MAX_SCAN_BYTES"),
            max_rows=_int_env("LAGAAM_MAX_ROWS"),
            timeout_seconds=_float_env("LAGAAM_QUERY_TIMEOUT"),
        )


def _int_env(name: str) -> int | None:
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive integer, got {raw!r}")


def _float_env(name: str) -> float | None:
    raw = os.environ.get(name)
    if not raw:
        return None
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a positive number, got {raw!r}")


_UNESTIMABLE = (
    "add a filter on a partition or key column, avoid self-joins, and query a "
    "table with statistics so the cost can be predicted."
)


def enforce_budget(estimate: CostEstimate, budget: QueryBudget) -> None:
    """Raise BudgetExceededError if the estimate does not fit the budget.

    Fails safe per dimension: a budget you can't measure the query against is
    a budget you must block on. A low-confidence estimate, or a missing number
    for a gated dimension, is treated as over budget.
    """
    if budget.max_scan_bytes is not None:
        if estimate.confidence == "low" or estimate.scanned_bytes is None:
            raise BudgetExceededError(
                "The scan size could not be estimated, so this query cannot be "
                f"cleared against your scan budget. {_UNESTIMABLE}"
            )
        if estimate.scanned_bytes > budget.max_scan_bytes:
            raise BudgetExceededError(
                f"This query would scan {human_bytes(estimate.scanned_bytes)}, "
                f"over your budget of {human_bytes(budget.max_scan_bytes)}. "
                "Add a WHERE filter (a date range or key range cuts the scan "
                "most) or select fewer columns, then retry."
            )

    if budget.max_rows is not None:
        if estimate.confidence == "low" or estimate.row_estimate is None:
            raise BudgetExceededError(
                "The row count could not be estimated, so this query cannot be "
                f"cleared against your row budget. {_UNESTIMABLE}"
            )
        if estimate.row_estimate > budget.max_rows:
            raise BudgetExceededError(
                f"This query is estimated to read {estimate.row_estimate:,} "
                f"rows, over your budget of {budget.max_rows:,}. This counts "
                "rows scanned, not returned, so a LIMIT alone won't help — add "
                "a WHERE filter (a date or key range) to read fewer rows."
            )
