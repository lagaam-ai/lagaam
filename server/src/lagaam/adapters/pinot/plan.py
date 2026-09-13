"""The widest row count anywhere in Pinot's multi-stage plan. PURE.

Pinot's Calcite cost model has no table statistics wired in: every table scan
reports rowcount 100.0 whatever the table's real size, and a 954-million-row
cross join is priced at 10,000. So this module reads the plan for shape only
— which tables, which joins, which conditions — and takes its sizes from the
segment metadata the quotation already fetched. The rowcount and cumulative
cost attributes are never read.

The rule is ADR 0004's, applied to a plan that carries shape but no sizes: a
join without an equality multiplies its inputs, a join with one cannot be
proven to, and everything else passes its widest input through.

rels[] is a flat topological list, not a tree. A node's children are its
"inputs" ids; a node with no inputs key at all consumes the node immediately
before it, which is how Calcite serialises a linear chain.
"""

import json
from typing import Any, Mapping

# A plan this deep is a machine's, not an analyst's.
_MAX_DEPTH = 400


def max_intermediate_rows(
    plan_json: str, leaf_docs: Mapping[str, int | None]
) -> int | None:
    """The widest row count any node in this plan would build, or None.

    None means the plan could not be read or a table could not be sized —
    no quote, which the budget treats as a denial rather than as cheap.
    """
    rels = _rels(plan_json)
    if rels is None:
        return None
    by_id: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    previous: dict[str, str | None] = {}
    last: str | None = None
    for rel in rels:
        rel_id = rel.get("id")
        if not isinstance(rel_id, str) or rel_id in by_id:
            return None
        by_id[rel_id] = rel
        previous[rel_id] = last
        order.append(rel_id)
        last = rel_id
    widest: list[int] = []
    memo: dict[str, int | None] = {}
    for rel_id in order:
        rows = _rows(rel_id, by_id, previous, leaf_docs, memo, widest, 0)
        if rows is None:
            return None
    return max(widest) if widest else None


def _rels(plan_json: str) -> list[dict[str, Any]] | None:
    try:
        body = json.loads(plan_json)
    except (json.JSONDecodeError, TypeError, ValueError, RecursionError):
        return None
    if not isinstance(body, dict):
        return None
    rels = body.get("rels")
    if not isinstance(rels, list) or not rels:
        return None
    if not all(isinstance(rel, dict) for rel in rels):
        return None
    return [rel for rel in rels if isinstance(rel, dict)]


def _rows(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    leaf_docs: Mapping[str, int | None],
    memo: dict[str, int | None],
    widest: list[int],
    depth: int,
) -> int | None:
    """This node's rows, recording every knowable count into ``widest``."""
    if depth > _MAX_DEPTH:
        return None
    if rel_id in memo:
        return memo[rel_id]
    # Claim the slot before recursing so a cycle terminates instead of hanging.
    memo[rel_id] = None
    rel = by_id.get(rel_id)
    if rel is None:
        return None
    children = _children(rel_id, rel, previous)
    if children is None:
        return None
    child_rows: list[int] = []
    for child in children:
        rows = _rows(child, by_id, previous, leaf_docs, memo, widest, depth + 1)
        if rows is None:
            return None
        child_rows.append(rows)
    if not child_rows:
        answer = _leaf_rows(rel, leaf_docs)
    elif _is_join(rel) and not _has_equality(rel.get("condition")):
        # A join the plan cannot prove is keyed pairs its inputs; charging
        # less is how a laundered cross join reads as one table's size.
        product = 1
        for rows in child_rows:
            product *= rows
        answer = product
    else:
        answer = max(child_rows)
    memo[rel_id] = answer
    if answer is not None:
        widest.append(answer)
    return answer


def _children(
    rel_id: str, rel: dict[str, Any], previous: dict[str, str | None]
) -> list[str] | None:
    """This node's input ids, or [] for a leaf, or None if unreadable."""
    inputs = rel.get("inputs")
    if inputs is None:
        # No inputs key: Calcite's shorthand for "the node just before me".
        before = previous.get(rel_id)
        return [] if before is None else [before]
    if not isinstance(inputs, list):
        return None
    if not all(isinstance(value, str) for value in inputs):
        return None
    return [value for value in inputs if isinstance(value, str)]


def _leaf_rows(
    rel: dict[str, Any], leaf_docs: Mapping[str, int | None]
) -> int | None:
    """A scan's rows: its table's surviving docs, looked up by two-part name."""
    table = rel.get("table")
    if not isinstance(table, list) or not table:
        return None
    parts = [part for part in table if isinstance(part, str) and part]
    if len(parts) != len(table):
        return None
    return leaf_docs.get(".".join(parts).lower())


def _is_join(rel: dict[str, Any]) -> bool:
    rel_op = rel.get("relOp")
    if not isinstance(rel_op, str):
        return False
    return rel_op.rsplit(".", 1)[-1].lower().endswith("join")


def _has_equality(condition: Any) -> bool:
    """True if an equality appears anywhere in this join condition.

    A cross join's condition is the bare literal true, with no op at all.
    """
    if not isinstance(condition, dict):
        return False
    op = condition.get("op")
    if isinstance(op, dict) and op.get("kind") == "EQUALS":
        return True
    operands = condition.get("operands")
    if isinstance(operands, list):
        return any(_has_equality(operand) for operand in operands)
    return False
