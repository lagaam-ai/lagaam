"""Turn Trino's EXPLAIN (TYPE IO, FORMAT JSON) into a CostEstimate.

Trino emits unknown estimates as the JSON string "NaN" (not a number), and a
stats-less scan can report a bogus size of 0.0 alongside a NaN row count. Both
mean "we cannot vouch for this" — so any NaN on any scanned table taints the
whole quote and drops confidence to low. The parser never throws: a shape it
doesn't recognise is treated as no-estimate, which fails safe at the gate.

The scan number is the sum of the per-table estimates (inputTableColumnInfos),
i.e. bytes read — not the top-level estimate, which is post-filter output.
"""

import json
import math
from typing import Any

from lagaam.core.models import CostEstimate


def finite_number(value: Any) -> float | None:
    """A finite, non-negative float, or None for NaN/Infinity/missing/junk.

    Infinity must be rejected too: Trino renders an unbounded cost as
    "Infinity", and letting it through would either crash round() or forge a
    huge high-confidence number. A negative size is nonsense — also None.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) and f >= 0 else None


def parse_io_estimate(io_json: str) -> CostEstimate:
    """Best-effort quotation from an IO plan; unknowns fail safe to low."""
    try:
        plan = json.loads(io_json)
        tables = plan["inputTableColumnInfos"]
    except (json.JSONDecodeError, TypeError, KeyError):
        return CostEstimate(confidence="low")
    # A JSON null is valid JSON with the key present, so it survives the
    # KeyError guard above and would break the iteration below.
    if not isinstance(tables, list):
        return CostEstimate(confidence="low")

    total_bytes = 0.0
    total_rows = 0.0
    trustworthy = bool(tables)
    for entry in tables:
        est = entry.get("estimate") if isinstance(entry, dict) else None
        if not isinstance(est, dict):
            trustworthy = False
            continue
        size = finite_number(est.get("outputSizeInBytes"))
        rows = finite_number(est.get("outputRowCount"))
        if size is None or rows is None:
            trustworthy = False
            continue
        # A scanned table never costs zero bytes: a stats-less connector
        # reports 0.0/0.0, and count(*) reports 0 bytes over many rows. Either
        # way the byte quote understates the scan — don't vouch for it.
        if size == 0:
            trustworthy = False
            continue
        total_bytes += size
        total_rows += rows

    if not trustworthy:
        return CostEstimate(confidence="low")
    return CostEstimate(
        scanned_bytes=round(total_bytes),
        row_estimate=round(total_rows),
    )
