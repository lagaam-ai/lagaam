"""The widest row count anywhere in Pinot's multi-stage plan. PURE.

Pinot's Calcite cost model has no table statistics wired in: every table scan
reports rowcount 100.0 whatever the table's real size, and a 954-million-row
cross join is priced at 10,000. So this module reads the plan for shape only
— which tables, which joins, which conditions — and takes its sizes from the
segment metadata the quotation already fetched. The rowcount and cumulative
cost attributes are never read.

The rule is ADR 0004's, applied to a plan that carries shape but no sizes: a
join or correlate is always the product of its inputs plus their sum, a union
(all or distinct) is the sum of its inputs, and everything else passes its
widest input through.

The sum rides on top of the product because Pinot MSE supports full, left and
right outer joins, whose unmatched rows are emitted on top of the matched
pairs: measured, a 1-row side FULL OUTER JOIN a 4-row side with no matching
key returned 5, where the product bounds only 4. The product alone under-bounds
exactly when a x b < a + b, i.e. when a side has 0 or 1 rows. joinType is not
read: it would be one more plan attribute to trust, and the additive term is
worth 0.0113% on these tables (954,026,194 against 954,133,829) while being
exact-safe on the degenerate ones.

The product holds for an equi-join too. The plan alone proves no join key, the
catalog can, and `keys.py` decides when it has. There are no cardinality
statistics anywhere in the pricing path, and an equality on a 14-distinct-value
column is a near-product, not a lookup — measured, `airlineStats a JOIN
airlineStats b ON a.Carrier = b.Carrier` builds 10,719,442 pairs over 9,746
rows. "Has an equality" is exactly the SQL-shape proxy ADR 0004 rejected.
"""

from collections.abc import Mapping
from typing import Any

from lagaam.adapters.pinot.keys import Evidence, covered_sides
from lagaam.adapters.pinot.rels import (
    MAX_DEPTH,
    children,
    index_rels,
    parse_rels,
)


def max_intermediate_rows(
    plan_json: str,
    leaf_docs: Mapping[str, int | None],
    unique_keys: Mapping[str, frozenset[frozenset[str]]] | None = None,
    key_ordinals: Mapping[str, Mapping[str, int]] | None = None,
) -> int | None:
    """The widest row count any node in this plan would build, or None.

    None means the plan could not be read or a table could not be sized —
    no quote, which the budget treats as a denial rather than as cheap.

    `unique_keys` maps a lowercase database.table to the column sets proved
    unique on it, from the catalog and never from the SQL's shape. A join
    whose equalities cover such a set matches at most one row per key value,
    so it is charged the other side rather than the product. Empty is the
    shipped behaviour: every join is the product.

    `key_ordinals` maps the same table name to each key column's scan
    ordinal, learned from the engine by the module-level function of that
    name. An operand is that key column only when it composes down to that
    ordinal: a projected field's *name* proves nothing, since a subquery may
    call any column anything. A table with keys but no ordinals here yields
    no evidence.
    """
    rels = parse_rels(plan_json)
    if rels is None:
        return None
    indexed = index_rels(rels)
    if indexed is None:
        return None
    by_id, previous, order = indexed
    evidence = Evidence(unique_keys or {}, key_ordinals or {})
    widest: list[int] = []
    memo: dict[str, int | None] = {}
    for rel_id in order:
        rows = _rows(rel_id, by_id, previous, leaf_docs, evidence, memo, widest, 0)
        if rows is None:
            return None
    return max(widest) if widest else None


def _rows(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    leaf_docs: Mapping[str, int | None],
    evidence: Evidence,
    memo: dict[str, int | None],
    widest: list[int],
    depth: int,
) -> int | None:
    """This node's rows, recording every knowable count into ``widest``."""
    if depth > MAX_DEPTH:
        return None
    if rel_id in memo:
        return memo[rel_id]
    # Claim the slot before recursing so a cycle terminates instead of hanging.
    memo[rel_id] = None
    rel = by_id.get(rel_id)
    if rel is None:
        return None
    child_ids = children(rel_id, rel, previous)
    if child_ids is None:
        return None
    child_rows: list[int] = []
    for child in child_ids:
        rows = _rows(
            child, by_id, previous, leaf_docs, evidence, memo, widest, depth + 1
        )
        if rows is None:
            return None
        child_rows.append(rows)
    if not child_rows:
        answer = _leaf_rows(rel, leaf_docs)
    elif _is_union(rel):
        # A distinct union still builds every input row before deduplicating.
        answer = sum(child_rows)
    elif _is_join(rel):
        # A join pairs its inputs unless the catalog proves a key covers one
        # side's equalities, in which case that side matches at most once.
        # The inputs are added on top of every branch because an outer join
        # also emits the rows that matched nothing.
        answer = _join_rows(rel_id, by_id, previous, evidence, child_ids, child_rows)
    else:
        answer = max(child_rows)
    memo[rel_id] = answer
    if answer is not None:
        widest.append(answer)
    return answer


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
    suffix = rel_op.rsplit(".", 1)[-1].lower()
    return suffix.endswith("join") or suffix.endswith("correlate")


def _is_union(rel: dict[str, Any]) -> bool:
    rel_op = rel.get("relOp")
    if not isinstance(rel_op, str):
        return False
    return rel_op.rsplit(".", 1)[-1].lower().endswith("union")


def _join_rows(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    evidence: Evidence,
    children: list[str],
    child_rows: list[int],
) -> int:
    """Rows this join builds: the product, or less on proven key evidence."""
    total = sum(child_rows)
    if len(children) != 2 or len(child_rows) != 2:
        product = 1
        for rows in child_rows:
            product *= rows
        return product + total
    left_rows, right_rows = child_rows
    left_covered, right_covered = covered_sides(
        rel_id, by_id, previous, evidence, children
    )
    if left_covered and right_covered:
        return min(left_rows, right_rows) + total
    if left_covered:
        return right_rows + total
    if right_covered:
        return left_rows + total
    return left_rows * right_rows + total
