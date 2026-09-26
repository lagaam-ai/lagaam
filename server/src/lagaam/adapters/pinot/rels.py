"""The Calcite JSON graph primitives `plan.py` and `keys.py` both need.

Pinot's multi-stage `EXPLAIN … AS JSON` is a flat topological `rels[]` list,
not a tree. A node's children are its "inputs" ids; a node with no inputs key
at all consumes the node immediately before it, which is how Calcite
serialises a linear chain. These are the readers every plan consumer shares.
"""

import json
from typing import Any

# A plan this deep is a machine's, not an analyst's.
MAX_DEPTH = 400


def parse_rels(plan_json: str) -> list[dict[str, Any]] | None:
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


def index_rels(
    rels: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str | None], list[str]] | None:
    """by_id, each node's predecessor, and document order; None on a missing or repeated id."""
    by_id: dict[str, dict[str, Any]] = {}
    previous: dict[str, str | None] = {}
    order: list[str] = []
    last: str | None = None
    for rel in rels:
        rel_id = rel.get("id")
        if not isinstance(rel_id, str) or rel_id in by_id:
            return None
        by_id[rel_id] = rel
        previous[rel_id] = last
        order.append(rel_id)
        last = rel_id
    return by_id, previous, order


def children(
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


def suffix(rel: dict[str, Any]) -> str:
    """The bare class name of a node's relOp, lowercase."""
    rel_op = rel.get("relOp")
    if not isinstance(rel_op, str):
        return ""
    return rel_op.rsplit(".", 1)[-1].lower()
