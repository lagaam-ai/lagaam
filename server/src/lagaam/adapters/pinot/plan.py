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

A catalog key is the one thing that lowers a join below the product, and it
is proven by scan ordinal rather than by name. A scan carries no column names
at all; the names on a LogicalProject are whatever the SQL called them, and
`SELECT Origin AS Carrier` is a field named Carrier over ordinal 62 while the
real Carrier is 18. So the engine learns each key column's ordinal with one
EXPLAIN of the key columns themselves (`key_ordinals`), and an operand must
compose down its side's chain to that same ordinal before it may be called
that key column.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from lagaam.adapters.pinot.rels import (
    MAX_DEPTH,
    children,
    index_rels,
    parse_rels,
    suffix,
)

# The only node types a side may carry between the join and its one scan.
# Anything else — a join, a union, an aggregate, a correlate, a second scan —
# means the rows reaching the join are not that table's rows any more, and
# that table's key says nothing about them.
_PASS_THROUGH = ("logicalproject", "pinotlogicalexchange", "logicalfilter")

_SCAN = "pinotlogicaltablescan"
_PROJECT = "logicalproject"


@dataclass(frozen=True)
class _Evidence:
    """The catalog's proof: which column sets are unique, and where they sit."""

    unique_keys: Mapping[str, frozenset[frozenset[str]]]
    key_ordinals: Mapping[str, Mapping[str, int]]


@dataclass(frozen=True)
class SideFields:
    """One join side, as the operand numbering and the evidence rule need it.

    ``width`` is the column count of the topmost ``LogicalProject`` on the
    side. It is the side's contribution to the join's operand numbering
    whatever lies beneath that project — a scan, a join, a union.

    ``chain`` is the ids of that project and everything below it down to the
    scan, top first, along which an operand index is composed. ``table`` is
    the side's ``database.table``, lowercase, and both are set only when the
    side is a straight chain of pass-through nodes to exactly one scan. A
    join, a union, an aggregate, a correlate or a second scan leaves them
    empty and ``None``: those rows are not one table's rows, so no key of any
    table says anything about them. The numbering survives; the evidence
    cannot.
    """

    width: int
    chain: tuple[str, ...]
    table: str | None


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
    evidence = _Evidence(unique_keys or {}, key_ordinals or {})
    widest: list[int] = []
    memo: dict[str, int | None] = {}
    for rel_id in order:
        rows = _rows(rel_id, by_id, previous, leaf_docs, evidence, memo, widest, 0)
        if rows is None:
            return None
    return max(widest) if widest else None


def key_ordinals(plan_json: str) -> dict[str, int] | None:
    """Each projected column's scan ordinal, from an EXPLAIN of those columns.

    The plan of ``SELECT <key columns> FROM db.t`` is one LogicalProject on a
    straight chain to one scan, and its ``exprs[i]`` is the bare scan ordinal
    of ``fields[i]``. That ordinal is what a join operand must compose down
    to before it may be called a key column; it is not the schema position —
    measured, airlineStats' Carrier is ordinal 18 and schema column 14 of 81.

    None for any other shape: an expression, a missing or mismatched exprs
    list, no project, or anything but exactly one scan below it.
    """
    rels = parse_rels(plan_json)
    if rels is None:
        return None
    indexed = index_rels(rels)
    if indexed is None:
        return None
    by_id, previous, order = indexed
    if not order:
        return None
    side = side_fields(order[-1], by_id, previous)
    if side is None or side.table is None or not side.chain:
        return None
    top = by_id.get(side.chain[0], {})
    if suffix(top) != _PROJECT:
        return None
    reads = _project_reads(top)
    names = top.get("fields")
    if reads is None or not isinstance(names, list) or len(names) != len(reads):
        return None
    ordinals: dict[str, int] = {}
    for name, ordinal in zip(names, reads, strict=True):
        if not isinstance(name, str) or not name:
            return None
        ordinals[name.lower()] = ordinal
    return ordinals or None


def _rows(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    leaf_docs: Mapping[str, int | None],
    evidence: _Evidence,
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
    evidence: _Evidence,
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
        rel_id, by_id, previous, evidence, children
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
    evidence: _Evidence,
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
            [left for left, _ in pairs if left is not None],
            evidence,
        ),
        _side_covered(
            side_fields(children[1], by_id, previous),
            [right for _, right in pairs if right is not None],
            evidence,
        ),
    )


def _side_covered(
    side: SideFields | None,
    equated: list[int],
    evidence: _Evidence,
) -> bool:
    """Is this side one table whose key the join's equated ordinals cover?

    A key column is named only by its ordinal: the operand reached the same
    scan position the catalog's EXPLAIN put that column at. A field name is
    never consulted, because a subquery may project any column under any
    name — SELECT Origin AS Carrier is a field called Carrier over ordinal
    62, and the real Carrier is 18.
    """
    if side is None or side.table is None or not equated:
        return False
    ordinals = evidence.key_ordinals.get(side.table)
    if not ordinals:
        return False
    reached = set(equated)
    named = {column for column, at in ordinals.items() if at in reached}
    return _covers(evidence.unique_keys.get(side.table, frozenset()), named)


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
) -> list[tuple[int | None, int | None]]:
    """(left ordinal, right ordinal) pairs this join equates.

    An operand index counts over left fields ++ right fields — verified on
    the two-key plan, where right teamID at right-index 1 resolved to global
    3 = 2 left fields + 1. The right fields list is alphabetised rather than
    in ON-clause order, so only the index may be read.

    Each index is then composed down its own side's chain to the scan it
    came from: a project turns it into that project's bare ``exprs[index]``
    input, a filter or an exchange passes it through, and at the scan it is
    the ordinal. That ordinal is the only thing a key may be proven by. An
    operand that cannot be composed — an expression, an unreadable exprs
    list, an out-of-range index, a side that is not one table — yields None
    on its own side, which leaves the other side's evidence intact.

    No pairs at all, which charges the product, on: a condition that is not
    a usable EQUALS, a left side whose top node carries no fields, an index
    past the last field, or an equality with both operands on one side.
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
    split = left.width
    width = split + (right.width if right is not None else 0)
    pairs: list[tuple[int | None, int | None]] = []
    for first, second in _equalities(rel.get("condition")):
        left_index, right_index = sorted((first, second))
        if not (left_index < split <= right_index):
            # Both operands on one side is not a join key; it is a filter.
            return []
        if right_index >= width:
            return []
        pairs.append(
            (
                _ordinal_at(left_index, left, by_id),
                _ordinal_at(right_index - split, right, by_id),
            )
        )
    return pairs


def _ordinal_at(
    index: int, side: SideFields | None, by_id: dict[str, dict[str, Any]]
) -> int | None:
    """The scan ordinal this side-local index composes down to, or None.

    Down the side's chain a project rewrites the index to its own bare
    ``exprs[index]`` input and a filter or exchange leaves it alone, so the
    index that arrives at the scan is the position the value was read from.
    None wherever the chain stops proving anything: no chain, an unreadable
    or short exprs list, an expression rather than a bare input, or an index
    no longer inside the project's fields.
    """
    if side is None or not side.chain:
        return None
    for rel_id in side.chain:
        rel = by_id.get(rel_id)
        if rel is None:
            return None
        if suffix(rel) != _PROJECT:
            continue
        reads = _project_reads(rel)
        if reads is None or not 0 <= index < len(reads):
            return None
        index = reads[index]
    return index


def _project_reads(rel: dict[str, Any]) -> list[int] | None:
    """This project's exprs as bare scan indexes, or None if any is not one.

    All or nothing: a missing, short or non-list exprs, or one entry that
    carries an op rather than a bare input, means no index through this
    project can be trusted, so none is offered.
    """
    fields = rel.get("fields")
    exprs = rel.get("exprs")
    if not isinstance(fields, list) or not isinstance(exprs, list):
        return None
    if len(exprs) < len(fields):
        return None
    reads: list[int] = []
    for expr in exprs[: len(fields)]:
        if not isinstance(expr, dict) or "op" in expr:
            return None
        index = expr.get("input")
        if isinstance(index, bool) or not isinstance(index, int) or index < 0:
            return None
        reads.append(index)
    return reads


def side_fields(
    rel_id: str, by_id: dict[str, dict[str, Any]], previous: dict[str, str | None]
) -> SideFields | None:
    """One side's width, its chain down to a scan, and that scan's table.

    The width comes from the topmost LogicalProject on the side — a scan
    carries no fields at all, and the exchange above it is pass-through.
    That width is the side's contribution to the operand numbering however
    the side produces its rows.

    The chain and the table are only set when the walk continues from that
    project through pass-through nodes to exactly one scan. A join, a union,
    an aggregate, a correlate or a second scan leaves the chain empty: those
    rows are not one table's rows, and there is no scan to compose an index
    down to. None for the whole side means no project was reached at all, so
    the side contributes no numbering either.
    """
    width: int | None = None
    chain: list[str] = []
    current: str | None = rel_id
    for _ in range(MAX_DEPTH):
        if current is None:
            break
        rel = by_id.get(current)
        if rel is None:
            break
        node_suffix = suffix(rel)
        if node_suffix == _SCAN:
            if width is None:
                return None
            table = _scan_table(rel)
            reachable: tuple[str, ...] = tuple(chain) if table is not None else ()
            return SideFields(width, reachable, table)
        if node_suffix not in _PASS_THROUGH:
            break
        if node_suffix == _PROJECT and width is None:
            names = rel.get("fields")
            if not isinstance(names, list) or not all(
                isinstance(name, str) for name in names
            ):
                return None
            width = len(names)
        if width is not None:
            chain.append(current)
        node_children = children(current, rel, previous)
        if node_children is None or len(node_children) != 1:
            break
        current = node_children[0]
    return None if width is None else SideFields(width, (), None)


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
