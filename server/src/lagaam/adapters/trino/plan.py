"""The widest row count anywhere in Trino's logical plan.

The IO plan prices bytes scanned, which is correct and irrelevant when the
row work is quadratic: a cross join reads both inputs exactly once and
produces their product. Trino's own optimizer already estimates that
product, per operator, and EXPLAIN (TYPE LOGICAL, FORMAT JSON) reports it.

The *maximum over operators* is the quantity, not the query's output count.
Measured on Trino 476, a 902,625,000-row cross join reports 10 output rows
under a LIMIT, 1 under count(*), and 15,000 under DISTINCT or GROUP BY —
while doing the full 902M rows of work either way. Only the widest
intermediate survives all four rewrites.

Unknown estimates arrive as the JSON string "NaN", and they propagate
upward: a filter Trino cannot size makes every operator above it unknown.
A join in that state is charged the product of what it joins, because the
alternative is pricing the laundered cross join at its children's size.
"""

import json
import math
from typing import Any

from lagaam.adapters.trino.numbers import finite_number

# Nodes whose output can exceed their inputs. An unknown estimate on one of
# these is a product, not a passthrough.
_JOIN_NODES = frozenset(
    {
        "Join",
        "InnerJoin",
        "CrossJoin",
        "LeftJoin",
        "RightJoin",
        "FullJoin",
        "SemiJoin",
        "IndexJoin",
        "NestedLoopJoin",
        "SpatialJoin",
        "CorrelatedJoin",
        "Apply",
        "ApplyNode",
    }
)

# A plan this deep is a machine's, not an analyst's, and recursing it would
# raise where the caller expects a number.
_MAX_PLAN_DEPTH = 400


def max_intermediate_rows(plan_json: str) -> float | None:
    """The widest row count any operator in this plan would produce.

    None means no operator carried a usable estimate — no quote, which the
    budget treats as a denial rather than as a cheap query.
    """
    try:
        root = json.loads(plan_json)
    except (json.JSONDecodeError, TypeError, ValueError, RecursionError):
        return None
    if not isinstance(root, dict):
        return None
    widest: list[float] = []
    _visit(root, widest, 0)
    return max(widest) if widest else None


def _visit(node: dict[str, Any], widest: list[float], depth: int) -> float | None:
    """This node's rows, recording every knowable count into ``widest``."""
    if depth > _MAX_PLAN_DEPTH:
        return None
    children = [
        _visit(child, widest, depth + 1)
        for child in _children(node)
    ]
    known = [rows for rows in children if rows is not None]
    rows = _own_estimate(node)
    if rows is None and known:
        # Only treat as join product if name is actually a string and known.
        node_name = node.get("name")
        if isinstance(node_name, str) and node_name in _JOIN_NODES:
            # A join whose size Trino could not estimate still pairs its
            # inputs; charging less than the product is how a laundered
            # cross join reads as the size of one of its tables.
            rows = 1.0
            for child_rows in known:
                rows *= child_rows
            # A product of very large numbers can overflow to infinity.
            # finite_number rejects infinities, so use it as a guard.
            if not math.isfinite(rows) or rows < 0:
                rows = None
        else:
            rows = max(known)
    if rows is not None:
        widest.append(rows)
    return rows


def _children(node: dict[str, Any]) -> list[dict[str, Any]]:
    children = node.get("children")
    if not isinstance(children, list):
        return []
    return [child for child in children if isinstance(child, dict)]


def _own_estimate(node: dict[str, Any]) -> float | None:
    """The greatest finite outputRowCount this node reports, if any.

    A node carries one estimate per plan alternative; the greatest is the
    one to price, since we cannot know which alternative runs.
    """
    estimates = node.get("estimates")
    if not isinstance(estimates, list):
        return None
    best: float | None = None
    for estimate in estimates:
        if not isinstance(estimate, dict):
            continue
        rows = finite_number(estimate.get("outputRowCount"))
        if rows is None:
            continue
        best = rows if best is None else max(best, rows)
    return best
