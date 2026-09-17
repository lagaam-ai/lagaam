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

The product holds for an equi-join too. Nothing on 1.5.1 can prove a join key:
there are no cardinality statistics anywhere in the pricing path, and an
equality on a 14-distinct-value column is a near-product, not a lookup —
measured, `airlineStats a JOIN airlineStats b ON a.Carrier = b.Carrier` builds
10,719,442 pairs over 9,746 rows. "Has an equality" is exactly the SQL-shape
proxy ADR 0004 rejected.

rels[] is a flat topological list, not a tree. A node's children are its
"inputs" ids; a node with no inputs key at all consumes the node immediately
before it, which is how Calcite serialises a linear chain.
"""

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

# A plan this deep is a machine's, not an analyst's.
_MAX_DEPTH = 400

# The only node types a side may carry between the join and its one scan.
# Anything else — a join, a union, an aggregate, a correlate, a second scan —
# means the rows reaching the join are not that table's rows any more, and
# that table's key says nothing about them.
_PASS_THROUGH = ("logicalproject", "pinotlogicalexchange", "logicalfilter")

# Calcite's name for a field it synthesised rather than read from a column.
_SYNTHETIC_FIELD_PREFIX = "$f"


@dataclass(frozen=True)
class SideFields:
    """One join side's column list, and its table when it has one.

    ``fields`` and ``exprs`` come from the topmost ``LogicalProject`` on the
    side. They are the columns that side contributes to the join's operand
    numbering whatever lies beneath that project — a scan, a join, a union.

    ``table`` is the side's ``database.table``, lowercase, and only when the
    side is a straight chain of pass-through nodes down to exactly one scan.
    ``None`` means the rows reaching the join are not one table's rows, so no
    key of any table says anything about them: names still resolve, evidence
    cannot.
    """

    fields: list[str]
    exprs: list[Any]
    table: str | None


def max_intermediate_rows(
    plan_json: str,
    leaf_docs: Mapping[str, int | None],
    unique_keys: Mapping[str, frozenset[frozenset[str]]] | None = None,
) -> int | None:
    """The widest row count any node in this plan would build, or None.

    None means the plan could not be read or a table could not be sized —
    no quote, which the budget treats as a denial rather than as cheap.

    `unique_keys` maps a lowercase database.table to the column sets proved
    unique on it, from the catalog and never from the SQL's shape. A join
    whose equalities cover such a set matches at most one row per key value,
    so it is charged the other side rather than the product. Empty is the
    shipped behaviour: every join is the product.
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
    keys = unique_keys or {}
    widest: list[int] = []
    memo: dict[str, int | None] = {}
    for rel_id in order:
        rows = _rows(rel_id, by_id, previous, leaf_docs, keys, memo, widest, 0)
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
    unique_keys: Mapping[str, frozenset[frozenset[str]]],
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
        rows = _rows(
            child, by_id, previous, leaf_docs, unique_keys, memo, widest, depth + 1
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
        answer = _join_rows(rel_id, by_id, previous, unique_keys, children, child_rows)
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
    unique_keys: Mapping[str, frozenset[frozenset[str]]],
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
    left_covered, right_covered = _covered_sides(
        rel_id, by_id, previous, unique_keys, children
    )
    if left_covered and right_covered:
        return min(left_rows, right_rows) + total
    if left_covered:
        return right_rows + total
    if right_covered:
        return left_rows + total
    return left_rows * right_rows + total


def _covered_sides(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    unique_keys: Mapping[str, frozenset[frozenset[str]]],
    children: list[str],
) -> tuple[bool, bool]:
    """Does each side's equated column set cover a unique key of its table?

    Each side is judged on its own. A side with no table — a join, a union,
    anything but one scan below it — is never covered, but that does not
    silence the other side: a keyed table joined to a join is still evidence
    about the keyed table's own matches.
    """
    pairs = join_key_pairs(rel_id, by_id, previous, children)
    if not pairs:
        return (False, False)
    return (
        _side_covered(
            side_fields(children[0], by_id, previous),
            {left for left, _ in pairs if left is not None},
            unique_keys,
        ),
        _side_covered(
            side_fields(children[1], by_id, previous),
            {right for _, right in pairs if right is not None},
            unique_keys,
        ),
    )


def _side_covered(
    side: SideFields | None,
    equated: set[str],
    unique_keys: Mapping[str, frozenset[frozenset[str]]],
) -> bool:
    """Is this side one table whose key the join's equalities cover?"""
    if side is None or side.table is None or not equated:
        return False
    return _covers(unique_keys.get(side.table, frozenset()), equated)


def _covers(keys: frozenset[frozenset[str]], equated: set[str]) -> bool:
    """Is any known key set a subset of the columns this join equates?

    Subset and not equality: equating more columns than the key still leaves
    at most one match per key value.
    """
    return any(key and key <= equated for key in keys)


def join_key_pairs(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    children: list[str],
) -> list[tuple[str | None, str | None]]:
    """(left column, right column) pairs this join equates, lowercase.

    An operand index counts over left fields ++ right fields — verified on
    the two-key plan, where right teamID at right-index 1 resolved to global
    3 = 2 left fields + 1. The right fields list is alphabetised rather than
    in ON-clause order, so only the index may be read.

    The split between the two sides is the left side's field count, which
    the left side's top project carries whatever lies beneath it. So a side
    that is not one table still numbers the operands: its own column resolves
    to None — no table, no key, no evidence — while the other side's column
    resolves normally and can still be covered.

    No pairs at all, which charges the product, on: a condition that is not
    a usable EQUALS, a left side whose top node carries no fields, an index
    out of range, or an equality with both operands on one side.
    """
    if len(children) != 2:
        return []
    rel = by_id.get(rel_id)
    if rel is None:
        return []
    left = side_fields(children[0], by_id, previous)
    right = side_fields(children[1], by_id, previous)
    if left is None:
        # Without the left side's field count there is no split to number by.
        return []
    right_fields = right.fields if right is not None else []
    right_exprs = right.exprs if right is not None else []
    names = list(left.fields) + list(right_fields)
    exprs = list(left.exprs) + list(right_exprs)
    split = len(left.fields)
    pairs: list[tuple[str | None, str | None]] = []
    for first, second in _equalities(rel.get("condition")):
        left_index, right_index = sorted((first, second))
        if not (left_index < split <= right_index):
            # Both operands on one side is not a join key; it is a filter.
            return []
        if right_index >= len(names):
            return []
        pairs.append(
            (
                _column_at(left_index, names, exprs),
                _column_at(right_index, names, exprs),
            )
        )
    return pairs


def side_fields(
    rel_id: str, by_id: dict[str, dict[str, Any]], previous: dict[str, str | None]
) -> SideFields | None:
    """One side's column list, and its table when the side is one table.

    The names come from the topmost LogicalProject on the side — a scan
    carries no fields at all, and the exchange above it is pass-through.
    Those fields are the side's contribution to the operand numbering
    however the side produces its rows.

    The table is only set when the walk continues from that project through
    pass-through nodes to exactly one scan. A join, a union, an aggregate, a
    correlate or a second scan leaves the table None: those rows are not one
    table's rows. None for the whole side means no project was reached at
    all, so the side contributes no numbering either.
    """
    fields: list[str] | None = None
    exprs: list[Any] = []
    current: str | None = rel_id
    for _ in range(_MAX_DEPTH):
        if current is None:
            break
        rel = by_id.get(current)
        if rel is None:
            break
        suffix = _suffix(rel)
        if suffix == "pinotlogicaltablescan":
            if fields is None:
                return None
            return SideFields(fields, exprs, _scan_table(rel))
        if suffix not in _PASS_THROUGH:
            break
        if suffix == "logicalproject" and fields is None:
            names = rel.get("fields")
            if not isinstance(names, list) or not all(
                isinstance(name, str) for name in names
            ):
                return None
            fields = [name for name in names if isinstance(name, str)]
            raw = rel.get("exprs")
            exprs = list(raw) if isinstance(raw, list) else []
        children = _children(current, rel, previous)
        if children is None or len(children) != 1:
            break
        current = children[0]
    return None if fields is None else SideFields(fields, exprs, None)


def _scan_table(rel: dict[str, Any]) -> str | None:
    """A scan's database.table, lowercase, or None if it is unreadable."""
    table = rel.get("table")
    if not isinstance(table, list) or not table:
        return None
    parts = [part for part in table if isinstance(part, str) and part]
    if len(parts) != len(table):
        return None
    return ".".join(parts).lower()


def _equalities(condition: Any) -> list[tuple[int, int]]:
    """Operand index pairs of every usable EQUALS in this join condition.

    Only a top-level EQUALS, or one directly under a top-level AND. An OR
    anywhere, a NOT, or any other kind yields nothing at all — an OR of
    equalities is not a key, and that is ADR 0008's measured case.
    """
    if not isinstance(condition, dict):
        return []
    kind = _op_kind(condition)
    if kind == "EQUALS":
        pair = _operand_indexes(condition)
        return [pair] if pair else []
    if kind != "AND":
        return []
    operands = condition.get("operands")
    if not isinstance(operands, list):
        return []
    pairs: list[tuple[int, int]] = []
    for operand in operands:
        if not isinstance(operand, dict) or _op_kind(operand) != "EQUALS":
            return []
        pair = _operand_indexes(operand)
        if pair is None:
            return []
        pairs.append(pair)
    return pairs


def _op_kind(node: Any) -> str | None:
    if not isinstance(node, dict):
        return None
    op = node.get("op")
    if not isinstance(op, dict):
        return None
    kind = op.get("kind")
    return kind if isinstance(kind, str) else None


def _operand_indexes(node: dict[str, Any]) -> tuple[int, int] | None:
    """The two bare `input` indexes of an EQUALS, or None for anything else."""
    operands = node.get("operands")
    if not isinstance(operands, list) or len(operands) != 2:
        return None
    indexes: list[int] = []
    for operand in operands:
        if not isinstance(operand, dict):
            return None
        index = operand.get("input")
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            return None
        indexes.append(index)
    return (indexes[0], indexes[1])


def _column_at(index: int, names: list[str], exprs: list[Any]) -> str | None:
    """The column name at this global field index, or None if it is not one.

    A $f-prefixed name and an exprs entry carrying an op rather than a bare
    input both mark a synthesised field: upper(a.Carrier) became $f85 with an
    op of UPPER. Either marker disqualifies the operand.
    """
    if index >= len(names):
        return None
    name = names[index]
    if not name or name.startswith(_SYNTHETIC_FIELD_PREFIX):
        return None
    if index < len(exprs):
        expr = exprs[index]
        if not isinstance(expr, dict) or "op" in expr or "input" not in expr:
            return None
    return name.lower()


def _suffix(rel: dict[str, Any]) -> str:
    """The bare class name of a node's relOp, lowercase."""
    rel_op = rel.get("relOp")
    if not isinstance(rel_op, str):
        return ""
    return rel_op.rsplit(".", 1)[-1].lower()
