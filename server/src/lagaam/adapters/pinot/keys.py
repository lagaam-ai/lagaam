"""The whole rule for when a Pinot join may be charged less than the product,
per ADR 0009.

A catalog key is the one thing that lowers a join below the product, and it
is proven by scan ordinal rather than by name. A scan carries no column names
at all; the names on a LogicalProject are whatever the SQL called them, and
`SELECT Origin AS Carrier` is a field named Carrier over ordinal 62 while the
real Carrier is 18. So the engine learns each key column's ordinal with one
EXPLAIN of the key columns themselves (`key_ordinals`), and an operand must
compose down its side's chain to that same ordinal before it may be called
that key column.

Two other parts of the rule live here too. The catalog's proof that a column
set is unique (`catalog_keys`, `upsert_keys`, `single_segment_unique_columns`)
reads either an upsert table's primary key — proven only where
`upsertConfig` is present and no TTL reopens the key to duplicates — or a
single sealed segment's cardinality against its doc count. And the
key-ordinal EXPLAIN's own spelling (`key_columns`, `key_ordinal_sql`) is
built only from bare identifiers; `_is_bare_identifier` is the guard against
a controller-supplied database, table or column name that could otherwise
break out of the generated SQL.
"""

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Final

from lagaam.adapters.pinot.metadata import (
    _FIELD_SPEC_KEYS,
    _positive_int,
    _reported_sizes,
    metadata_is_complete,
)
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
class Evidence:
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


def covered_sides(
    rel_id: str,
    by_id: dict[str, dict[str, Any]],
    previous: dict[str, str | None],
    evidence: Evidence,
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
    evidence: Evidence,
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


def catalog_keys(
    config_json: Any,
    seg_metadata_json: Any,
    schema_json: Any,
    size_json: Any,
    table_metadata_json: Any,
) -> frozenset[frozenset[str]]:
    """Every column set the catalog proves unique on this table."""
    return upsert_keys(config_json, schema_json, table_metadata_json) or (
        single_segment_unique_columns(
            seg_metadata_json, config_json, schema_json, size_json
        )
    )


def upsert_keys(
    config_json: Any, schema_json: Any, table_metadata_json: Any
) -> frozenset[frozenset[str]]:
    """An upsert table's primary key, as the one column set proved unique.

    Measured: GROUP BY pk HAVING count(*) > 1 returns nothing on the upsert
    table and 6-version rows on a byte-identical non-upsert table reading the
    same topic, and the PK self-join returns exactly the distinct-key count
    against 3,600 for the twin.

    Three documents have to agree. The config's upsertConfig and the
    metadata's PK map are each independently proof of an upsert table — the
    map is {} on every non-upsert table — so a disagreement between them is
    two documents contradicting each other about what this table is, which is
    not a basis for admitting a join. The full column list is the key and
    never a subset: a composite primary key is unique as a tuple.

    The upsertConfig has to say the key is still unique, and not merely that
    the table is an upsert one. An `upsertConfig` whose `mode` is not FULL or
    PARTIAL, or which carries a `metadataTTL` or `deletedKeysTTL` greater
    than zero, yields nothing: measured on a FULL table at
    `metadataTTL: 60000`, key `K1` had two visible rows and the pk self-join
    returned 10 pairs against the 8 a unique pk gives. See
    `_has_upsert_config` for why a zero TTL is not a TTL.

    The PK-count map is not a correctness signal on that table either: it
    reported 6 against 8 rows and 7 distinct visible keys — wrong in both
    directions — while staying non-empty. It says "upsert table", nothing more.
    """
    if not isinstance(schema_json, dict):
        return frozenset()
    declared = _has_upsert_config(config_json)
    counted = _has_primary_key_counts(table_metadata_json)
    if not declared or not counted:
        return frozenset()
    columns = schema_json.get("primaryKeyColumns")
    if not isinstance(columns, list) or not columns:
        return frozenset()
    names = {
        column.lower()
        for column in columns
        if isinstance(column, str) and column and not isinstance(column, bool)
    }
    if len(names) != len(columns):
        return frozenset()
    return frozenset({frozenset(names)})


def upsert_config_present(config_json: Any) -> bool:
    """Could this table config's upsertConfig prove a unique primary key?

    The caller fetches two more documents on the strength of this, so it is
    public: an adapter that guessed would pay two controller calls per table.
    A config whose upsert mode or TTL already rules the key out is False
    here, and the two documents are not fetched at all.
    """
    return _has_upsert_config(config_json)


def single_segment_unique_columns(
    seg_metadata_json: Any, config_json: Any, schema_json: Any, size_json: Any
) -> frozenset[frozenset[str]]:
    """Columns whose cardinality equals their docs, on a one-sealed-segment table.

    cardinality is exactly count(DISTINCT col) — verified against the engine
    on four columns — and count(DISTINCT col) <= count(col) <= totalDocs, so
    equality forces every doc to be counted and every value to differ. That
    argument is the segment's, and it is the table's only where the two are
    the same rows: one sealed segment and nothing consuming.

    Measured: /segments/{t}/metadata on a multi-server table can return only
    one server's half, so counting sealed segments in that response alone
    lets a 2-segment table read as single-segment. The size report is the
    independent count: it must name exactly one sealed segment (a
    reportedSizeInBytes >= 0 entry), the metadata response must be complete
    against it, and the one metadata entry must be that same segment.

    A consuming segment can be invisible to metadata entirely — named only
    by a reportedSizeInBytes -1 entry in the size report, with no body at
    all on the metadata side. metadata_is_complete does not catch this (it
    only requires every *sealed* name to be present), so this function
    checks the size report for any -1 entry itself: such a segment holds
    rows the one sealed segment does not, so its presence alone voids the
    key regardless of what metadata says.

    Gated on nullability because the null caveat is unclosed (log §6): if
    cardinality counts a null or a default as a distinct value, a column with
    one null could report cardinality == totalDocs while two rows share the
    default. Where nullability cannot be established, nothing is yielded.

    A `schema_json` of None is a schema nobody read, and it yields nothing at
    all: the second gate clears a column that null handling is off for and the
    schema does not mark nullable, but an unread schema marks nothing nullable
    for want of evidence rather than for want of nullable columns. Reading
    absence as proof would let a plainly nullable column pass as a key.

    A multi-value column's cardinality counts distinct entries, not rows —
    totalNumberOfEntries and maxNumberOfMultiValues are reported separately —
    so equality to totalDocs proves nothing there; such a column is never
    evidence.
    """
    if not isinstance(seg_metadata_json, dict) or schema_json is None:
        return frozenset()
    reported = _reported_sizes(size_json)
    if any(size < 0 for size in reported.values()):
        return frozenset()
    sealed_names = {name for name, size in reported.items() if size is not None and size >= 0}
    if len(sealed_names) != 1:
        return frozenset()
    if not metadata_is_complete(seg_metadata_json, size_json):
        return frozenset()
    sealed = [
        (key, body)
        for key, body in seg_metadata_json.items()
        if isinstance(body, dict) and isinstance(body.get("columns"), list)
    ]
    consuming = [
        body
        for body in seg_metadata_json.values()
        if isinstance(body, dict) and not isinstance(body.get("columns"), list)
    ]
    if len(sealed) != 1 or consuming:
        return frozenset()
    (sole_name,) = sealed_names
    sole_key, sole_body = sealed[0]
    name_candidate = sole_body.get("segmentName")
    if not isinstance(name_candidate, str) or not name_candidate:
        name_candidate = sole_key if isinstance(sole_key, str) else ""
    if name_candidate != sole_name:
        return frozenset()
    docs = _positive_int(sole_body.get("totalDocs"))
    if docs is None:
        return frozenset()
    nullable_off = _null_handling_disabled(config_json)
    schema_nullable = _schema_nullable_columns(schema_json)
    keys: set[frozenset[str]] = set()
    for column in sole_body["columns"]:
        if not isinstance(column, dict):
            continue
        name = column.get("columnName")
        cardinality = _positive_int(column.get("cardinality"))
        if not isinstance(name, str) or not name or cardinality != docs:
            continue
        if _is_multi_valued(column, docs):
            continue
        spec = column.get("fieldSpec")
        not_null = isinstance(spec, dict) and spec.get("notNull") is True
        if not not_null and not (
            nullable_off and name.lower() not in schema_nullable
        ):
            continue
        keys.add(frozenset({name.lower()}))
    return frozenset(keys)


def _is_multi_valued(column: dict[str, Any], docs: int) -> bool:
    """A column whose per-row value count is not knowable as exactly one.

    `docs` is the segment's own validated totalDocs, not the column entry's
    copy of it, so a column entry missing or lying about its own totalDocs
    cannot dodge this gate.
    """
    spec = column.get("fieldSpec")
    if isinstance(spec, dict) and spec.get("singleValueField") is False:
        return True
    entries = column.get("totalNumberOfEntries")
    if isinstance(entries, int) and not isinstance(entries, bool) and entries != docs:
        return True
    max_mv = column.get("maxNumberOfMultiValues")
    return isinstance(max_mv, int) and not isinstance(max_mv, bool) and max_mv > 0


def _has_upsert_config(config_json: Any) -> bool:
    """Does either half carry an upsertConfig that keeps the key unique?

    Two ways an `upsertConfig` object is present and the primary key is still
    not unique in the query-visible view, both measured live on 1.5.1:

    `mode: NONE` is a valid Mode value that disables upsert entirely while
    the object stays in the config, so the mode must say FULL or PARTIAL
    (case-insensitively) and nothing else qualifies.

    A **set** `metadataTTL` or `deletedKeysTTL` evicts a key from the
    primary-key lookup map while the rows it pointed at stay queryable, so a
    key re-ingested after its window has two visible rows. Measured on
    `u12ttl` (`mode: FULL`, `metadataTTL: 60000`): 8 rows, 7 distinct pks,
    `GROUP BY pk HAVING count(*) > 1` returning `K1 -> 2`, both versions
    visible, and the pk self-join returning 10 pairs where a unique pk gives
    8 — while the adapter cut the widest join step from 80 to 24 on the
    strength of that key.

    Set means greater than zero. 1.5.1's `isTTLEnabled()` is
    `_metadataTTL > 0 || _deletedKeysTTL > 0` and `isOutOfMetadataTTL`
    returns false outright at `_metadataTTL <= 0`, and the controller
    materialises `metadataTTL: 0.0, deletedKeysTTL: 0.0` on every upsert
    config it serves — so reading a zero as a TTL would withhold the key
    from every upsert table there is, including the one the evidence was
    proved on.
    """
    if not isinstance(config_json, dict):
        return False
    for key in ("REALTIME", "OFFLINE"):
        half = config_json.get(key)
        if not isinstance(half, dict):
            continue
        upsert = half.get("upsertConfig")
        if not isinstance(upsert, dict):
            continue
        mode = upsert.get("mode")
        if not isinstance(mode, str) or mode.upper() not in ("FULL", "PARTIAL"):
            continue
        if any(_ttl_is_set(upsert.get(name)) for name in _UPSERT_TTL_KEYS):
            continue
        return True
    return False


# The two retention windows that let a primary key have two visible rows.
_UPSERT_TTL_KEYS: Final = ("metadataTTL", "deletedKeysTTL")


def _ttl_is_set(value: Any) -> bool:
    """Is this TTL a positive duration, and so actually in force?

    Anything that is not a number Pinot could read as one is treated as set:
    a value this adapter cannot interpret is not a value it may clear a key
    on. A bool is not a duration and is read the same way.
    """
    if value is None:
        return False
    if isinstance(value, bool):
        return True
    if isinstance(value, (int, float)):
        return value > 0
    return True


def _has_primary_key_counts(table_metadata_json: Any) -> bool:
    """Is upsertPartitionToServerPrimaryKeyCountMap non-empty?

    It is {} on every non-upsert table, so a non-empty map is itself proof.
    It is never read as a count: it is per server, and replication > 1 is
    unmeasured (spec decision 6).
    """
    if not isinstance(table_metadata_json, dict):
        return False
    counts = table_metadata_json.get("upsertPartitionToServerPrimaryKeyCountMap")
    return isinstance(counts, dict) and bool(counts)


def _null_handling_disabled(config_json: Any) -> bool:
    """Is tableIndexConfig.nullHandlingEnabled explicitly false on a half?"""
    if not isinstance(config_json, dict):
        return False
    for key in ("REALTIME", "OFFLINE"):
        half = config_json.get(key)
        if not isinstance(half, dict):
            continue
        index_config = half.get("tableIndexConfig")
        if isinstance(index_config, dict) and index_config.get(
            "nullHandlingEnabled"
        ) is False:
            return True
    return False


def _schema_nullable_columns(schema_json: Any) -> frozenset[str]:
    """Lowercase names the schema marks nullable, which no gate may pass."""
    if not isinstance(schema_json, dict):
        return frozenset()
    nullable: set[str] = set()
    for key in _FIELD_SPEC_KEYS:
        specs = schema_json.get(key)
        if not isinstance(specs, list):
            continue
        for spec in specs:
            if not isinstance(spec, dict):
                continue
            name = spec.get("name")
            if isinstance(name, str) and name and spec.get("nullable") is True:
                nullable.add(name.lower())
    return frozenset(nullable)


@dataclass(frozen=True)
class KeyColumns:
    """What the key-ordinal EXPLAIN of one table has to be spelled with.

    Both spellings come from the controller — the listing for the table, the
    schema for the columns — and never from the agent's SQL, which reaches
    the broker case-insensitively and would name a column the plan does not.
    """

    database: str
    table: str
    columns: tuple[str, ...]


def _is_bare_identifier(name: str) -> bool:
    """Is this a name the ordinals EXPLAIN can carry as it stands?

    A key column's spelling comes from the schema document and is
    interpolated into a SELECT list, so it is checked like any other name
    this module puts in a statement: a controller is trusted for facts, not
    for syntax, and a name carrying a comma or a comment marker would be a
    second clause rather than a column.
    """
    return bool(name) and name.isascii() and name.replace("_", "").isalnum()


def key_columns(
    database: str,
    spelled: str,
    spellings: Mapping[str, str],
    unique_keys: frozenset[frozenset[str]],
) -> KeyColumns | None:
    """What one table's key-ordinal EXPLAIN is spelled with, or None."""
    names = sorted({name for key in unique_keys for name in key})
    resolved = [spellings.get(name) for name in names]
    if not resolved or any(name is None for name in resolved):
        # A key column the schema does not name cannot be selected at all.
        return None
    subject_names = [database, spelled, *(name for name in resolved if name)]
    if not all(_is_bare_identifier(name) for name in subject_names):
        # Database, table and every key column reach the EXPLAIN raw.
        return None
    return KeyColumns(
        database=database,
        table=spelled,
        columns=tuple(name for name in resolved if name is not None),
    )


def key_ordinal_sql(subject: KeyColumns) -> str:
    """SELECT <key columns> FROM <database>.<table>, no LIMIT, no ORDER BY."""
    return f"SELECT {', '.join(subject.columns)} FROM {subject.database}.{subject.table}"
