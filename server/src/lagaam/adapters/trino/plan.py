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

Exception: charging the product is right when the join itself is what Trino
could not size, and wrong when Trino merely stopped propagating stats. Two
structural signals, both read from the plan, tell those apart.

A side is *aggregation-bounded* when an Aggregate/Distinct/TopN/Limit — or a
row-preserving Window over a sized input — sits between it and the join,
seen through Exchange/Project wrappers. Such a side emits at most one row
per group, so the join cannot multiply and the product is invented cost.
This is the decorrelated-correlated-subquery shape.

Otherwise the join is trusted only when every equality key resolves, through
the plan's own "sym := source" assignments, to a base table column AND no
side is a scan whose estimate went NaN. The first conjunct kills the derived
key (substr/lower/cast renders "expr := substring(...)", and an alias back
to a plain name cannot hide it). The second kills filter laundering, where
the key stays a plain column but a regexp_like filter nulls a scan estimate
so a low-cardinality key multiplies unpriced. Both must hold, because each
alone has a measured counterexample. Trino stops estimating multi-way joins
past a depth, so this is what keeps an ordinary 4-table star join quotable.
"""

import json
import math
import re
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

# Collapse rows to at most one per group, or to a fixed cap.
_BOUNDING_NODES = frozenset(
    {"Aggregate", "DistinctLimit", "TopN", "Limit", "Distinct"}
)

# Of those, the ones that collapse rows per key rather than merely capping a
# count. Only a collapse contains dirt: a row cap leaves the other side's
# fan-out per join key completely free.
_COLLAPSING_NODES = frozenset({"Aggregate", "Distinct"})

# Trino splits these into partial and final stages, and the partial carries
# the cap as its own estimate even when its input went NaN — matching no set,
# it read as sized and the walk stopped above the dirt.
_PARTIAL_NODES = frozenset(
    {
        "LimitPartial",
        "TopNPartial",
        "AggregatePartial",
        "DistinctPartial",
        "DistinctLimitPartial",
    }
)

# Emit exactly one row per input row, so a sized input bounds them.
_ROW_PRESERVING_NODES = frozenset({"Window", "RowNumber", "TopNRanking", "Sort"})

# Cannot emit more rows than their widest input, so the walk sees through them.
_PASSTHROUGH_NODES = frozenset(
    {
        "LocalExchange",
        "RemoteExchange",
        "Exchange",
        "Project",
        "FilterProject",
        "Filter",
        "AssignUniqueId",
        "MarkDistinct",
        "EnforceSingleRow",
        "GroupId",
        "Output",
    }
)

# A plan this deep is a machine's, not an analyst's, and recursing it would
# raise where the caller expects a number.
_MAX_PLAN_DEPTH = 400

# How far the bounded/base-column walks descend before giving up (denying).
_MAX_WALK_DEPTH = 60


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
    _visit(root, widest, 0, _assignments(root, {}, 0))
    return max(widest) if widest else None


def _visit(
    node: dict[str, Any],
    widest: list[float],
    depth: int,
    assigned: dict[str, str],
) -> float | None:
    """This node's rows, recording every knowable count into ``widest``."""
    if depth > _MAX_PLAN_DEPTH:
        return None
    children = [
        _visit(child, widest, depth + 1, assigned)
        for child in _children(node)
    ]
    known = [rows for rows in children if rows is not None]
    rows = _own_estimate(node)
    if rows is None and known:
        # Only treat as join product if name is actually a string and known.
        node_name = node.get("name")
        if (
            isinstance(node_name, str)
            and node_name in _JOIN_NODES
            and not _join_cannot_multiply(node, assigned)
        ):
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


# One conjunct: "(name = name)" with optional whitespace around the operator.
_CONJUNCT = re.compile(r"\(\s*([^()=\s]+)\s*=\s*([^()=\s]+)\s*\)")

# One assignment line from a node's details: "expr := substring(col, 1, 1)".
_ASSIGNMENT = re.compile(r"^\s*([A-Za-z_][A-Za-z0-9_]*)\s*:=\s*(.+?)\s*$")

# A base-column source: "tpch:orderkey" or "orderkey" — a bare name, never a
# call, literal or operator. Anything else is a computed key.
_BASE_COLUMN_SOURCE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_]*(?::[A-Za-z_][A-Za-z0-9_]*)?$"
)


def _join_cannot_multiply(node: dict[str, Any], assigned: dict[str, str]) -> bool:
    """True if this NaN join provably cannot blow up, so max-of-children holds."""
    children = _children(node)
    if not children:
        return False
    # Dirt anywhere under either side means the product is unpriced, whatever
    # else the plan says. The walk already stops clean at a grouping collapse,
    # which is the only shape that genuinely contains it.
    if any(_has_nan_scan(child, 0) for child in children):
        return False
    if any(_aggregation_bounded(child, 0) for child in children):
        return True
    return _keys_are_base_columns(node, assigned)


def _aggregation_bounded(node: dict[str, Any], depth: int) -> bool:
    """True if an aggregation caps this side's rows, through wrapper nodes."""
    if depth > _MAX_WALK_DEPTH or not isinstance(node, dict):
        return False
    name = node.get("name")
    if not isinstance(name, str):
        return False
    if name in _BOUNDING_NODES:
        return True
    if name in _ROW_PRESERVING_NODES:
        return any(_is_sized(child, 0) for child in _children(node))
    if name in _JOIN_NODES:
        return False
    if name in _PASSTHROUGH_NODES:
        return any(_aggregation_bounded(child, depth + 1) for child in _children(node))
    return False


def _is_sized(node: dict[str, Any], depth: int) -> bool:
    """True if Trino put a finite row count on this subtree."""
    if depth > _MAX_WALK_DEPTH or not isinstance(node, dict):
        return False
    if _own_estimate(node) is not None:
        return True
    name = node.get("name")
    if not isinstance(name, str) or name in _JOIN_NODES:
        return False
    if name in _PASSTHROUGH_NODES or name in _ROW_PRESERVING_NODES or name in _BOUNDING_NODES:
        return any(_is_sized(child, depth + 1) for child in _children(node))
    return False


def _has_nan_scan(node: dict[str, Any], depth: int) -> bool:
    """True if a leaf scan under this side had its estimate nulled by a filter.

    Read negated by the caller, so every give-up returns True: an unread
    subtree must cost the exemption, not grant it.
    """
    if depth > _MAX_WALK_DEPTH or not isinstance(node, dict):
        return True
    name = node.get("name")
    if not isinstance(name, str):
        return True
    children = _children(node)
    if not children:
        return _reports_nan(node)
    # A grouping collapse caps this side per key, so dirt below it cannot
    # reach the join. A row cap does not: it bounds this side's own rows and
    # leaves the other side's fan-out per key untouched.
    if name in _COLLAPSING_NODES:
        return False
    # Descend through joins too: a laundered scan under a nested join is the
    # same dirt, and stopping here reads "did not look" as "clean".
    if (
        name in _JOIN_NODES
        or name in _PASSTHROUGH_NODES
        or name in _ROW_PRESERVING_NODES
        or name in _BOUNDING_NODES
        or name in _PARTIAL_NODES
    ):
        return any(_has_nan_scan(child, depth + 1) for child in children)
    # An unrecognised node is only trusted where Trino sized it anyway.
    return _own_estimate(node) is None


def _reports_nan(node: dict[str, Any]) -> bool:
    """True if any estimate alternative on this node is unknown."""
    estimates = node.get("estimates")
    if not isinstance(estimates, list):
        return False
    for estimate in estimates:
        if not isinstance(estimate, dict):
            continue
        rows = estimate.get("outputRowCount")
        if isinstance(rows, str) and rows == "NaN":
            return True
        if isinstance(rows, float) and math.isnan(rows):
            return True
    return False


def _keys_are_base_columns(node: dict[str, Any], assigned: dict[str, str]) -> bool:
    """True if every equality key resolves to a plain base table column."""
    descriptor = node.get("descriptor")
    if not isinstance(descriptor, dict):
        return False
    criteria = descriptor.get("criteria")
    if not isinstance(criteria, str) or criteria == "":
        return False
    conjuncts = _CONJUNCT.findall(criteria)
    if not conjuncts:
        return False
    for pair in conjuncts:
        for symbol in pair:
            source = assigned.get(symbol)
            # A key whose origin the plan never states is not a proven column.
            if source is None or not _BASE_COLUMN_SOURCE.match(source):
                return False
    return True


def _assignments(
    node: dict[str, Any], found: dict[str, str], depth: int
) -> dict[str, str]:
    """Every "symbol := source" the plan declares, gathered once up front."""
    if depth > _MAX_PLAN_DEPTH or not isinstance(node, dict):
        return found
    details = node.get("details")
    if isinstance(details, list):
        for line in details:
            if not isinstance(line, str):
                continue
            match = _ASSIGNMENT.match(line)
            if match is not None:
                found.setdefault(match.group(1), match.group(2))
    for child in _children(node):
        _assignments(child, found, depth + 1)
    return found


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
