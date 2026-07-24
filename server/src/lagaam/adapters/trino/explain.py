"""Turn Trino's EXPLAIN (TYPE IO, FORMAT JSON) into a CostEstimate.

Trino emits unknown estimates as the JSON string "NaN" (not a number), and a
stats-less scan reports 0.0 bytes over 0.0 rows. Both mean "we cannot vouch
for this" — so either one on any scanned table taints the whole quote and
drops confidence to low. Zero bytes over *real* rows is different: that is a
columnless scan (count(*)), which is priced from the row count rather than
trusted at zero. The parser never throws: a shape it doesn't recognise is
treated as no-estimate, which fails safe at the gate.

The scan number is the sum of the per-table estimates (inputTableColumnInfos),
i.e. bytes read — not the top-level estimate, which is post-filter output.
"""

import json

from lagaam.adapters.trino.numbers import finite_number
from lagaam.core.models import CostEstimate


# A columnless scan (count(*)) still touches every row: charge a nominal byte
# per row so it is priced rather than trusted at zero or blocked outright.
_BYTES_PER_ROW_FLOOR = 1.0


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
        if size == 0:
            if rows == 0:
                # 0 rows AND 0 bytes is what a stats-less connector reports
                # for a full scan; a truly empty table looks identical.
                trustworthy = False
                continue
            # count(*) reads no columns, so Trino reports 0 bytes over real
            # rows. The scan is not free — price it from the row count.
            size = rows * _BYTES_PER_ROW_FLOOR
        total_bytes += size
        total_rows += rows

    if not trustworthy:
        return CostEstimate(confidence="low")
    return CostEstimate(
        scanned_bytes=round(total_bytes),
        row_estimate=round(total_rows),
    )
