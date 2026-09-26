"""The whole join-key rule: catalog proof, key-ordinal EXPLAIN spelling,
ordinals, and join coverage.

A key column is learned as its scan ordinal from an EXPLAIN of the key columns
themselves, and a join operand is resolved by composing its index down its own
side's chain to the scan it was read from.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.keys import (
    KeyColumns,
    catalog_keys,
    join_key_pairs,
    key_columns,
    key_ordinal_sql,
    key_ordinals,
    single_segment_unique_columns,
    upsert_keys,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def plan_cell(name: str) -> str:
    body = json.loads((FIXTURES / name).read_text())
    cell = body["resultTable"]["rows"][0][1]
    assert isinstance(cell, str)
    return cell


def join_pairs(name: str) -> list[tuple[int | None, int | None]]:
    """The scan ordinals the one join node in this plan equates."""
    rels = json.loads(plan_cell(name))["rels"]
    by_id = {rel["id"]: rel for rel in rels}
    previous: dict[str, str | None] = {}
    last: str | None = None
    for rel in rels:
        previous[rel["id"]] = last
        last = rel["id"]
    join = next(rel for rel in rels if rel.get("joinType") or "Join" in rel["relOp"])
    return join_key_pairs(join["id"], by_id, previous, join["inputs"])


def test_key_ordinals_reads_a_single_key_columns_scan_ordinal() -> None:
    """Carrier is ordinal 18, which is not its schema position of 14."""
    assert key_ordinals(plan_cell("explain-mse-keycols-airlineStats.json")) == {
        "carrier": 18
    }


def test_key_ordinals_reads_every_projected_key_column() -> None:
    assert key_ordinals(plan_cell("explain-mse-keycols-baseballStats.json")) == {
        "teamid": 26,
        "playerid": 17,
    }


_SCAN_T: dict[str, object] = {
    "id": "0",
    "relOp": "PinotLogicalTableScan",
    "table": ["default", "t"],
    "inputs": [],
}


@pytest.mark.parametrize(
    "rels",
    [
        # An expression rather than a bare input: no ordinal to learn.
        [
            _SCAN_T,
            {
                "id": "1",
                "relOp": "LogicalProject",
                "inputs": ["0"],
                "fields": ["k"],
                "exprs": [{"op": {"name": "UPPER"}, "operands": [{"input": 3}]}],
            },
        ],
        # exprs missing entirely.
        [_SCAN_T, {"id": "1", "relOp": "LogicalProject", "inputs": ["0"], "fields": ["k"]}],
        # exprs shorter than fields.
        [
            _SCAN_T,
            {
                "id": "1",
                "relOp": "LogicalProject",
                "inputs": ["0"],
                "fields": ["k", "j"],
                "exprs": [{"input": 3}],
            },
        ],
        # exprs is not a list.
        [
            _SCAN_T,
            {
                "id": "1",
                "relOp": "LogicalProject",
                "inputs": ["0"],
                "fields": ["k"],
                "exprs": {"input": 3},
            },
        ],
        # No project at all: a bare scan carries no field names.
        [_SCAN_T],
        # Two scans below the project: not one table's ordinals.
        [
            _SCAN_T,
            {
                "id": "1",
                "relOp": "PinotLogicalTableScan",
                "table": ["default", "u"],
                "inputs": [],
            },
            {
                "id": "2",
                "relOp": "LogicalJoin",
                "inputs": ["0", "1"],
                "joinType": "inner",
            },
            {
                "id": "3",
                "relOp": "LogicalProject",
                "inputs": ["2"],
                "fields": ["k"],
                "exprs": [{"input": 3}],
            },
        ],
    ],
)
def test_key_ordinals_of_a_shape_it_cannot_read_is_no_map(
    rels: list[dict[str, object]],
) -> None:
    assert key_ordinals(json.dumps({"rels": rels})) is None


@pytest.mark.parametrize("plan", ["", "not json", "[]", json.dumps({"rels": []})])
def test_key_ordinals_of_an_unreadable_plan_is_no_map(plan: str) -> None:
    assert key_ordinals(plan) is None


def test_a_plain_equi_join_resolves_both_operands_to_scan_ordinals() -> None:
    """Carrier is ordinal 18 on the left, teamID 26 on the right."""
    assert join_pairs("explain-mse-join-plain.json") == [(18, 26)]


def test_an_operand_index_counts_over_left_fields_then_right() -> None:
    """Right teamID at right-index 1 is global 3 = 2 left fields + 1.

    It composes down to ordinal 26; the second equality pairs Origin's 62
    with league's 14.
    """
    assert join_pairs("explain-mse-join-twokeys.json") == [(18, 26), (62, 14)]


def test_an_expression_operand_is_no_evidence() -> None:
    """upper(a.Carrier) carries an op rather than a bare input.

    That operand composes to nothing — no ordinal, so no key of that side
    can be named — while the other side's still reaches its scan.
    """
    assert join_pairs("explain-mse-join-expr.json") == [(None, 26)]


def test_a_left_join_resolves_exactly_as_an_inner_one_does() -> None:
    """joinType is not read: the rule is about the key, not the join kind."""
    assert join_pairs("explain-mse-join-left.json") == [(18, 26)]


def test_an_or_condition_is_no_evidence() -> None:
    """ADR 0008's OR case stands: an OR of equalities is not a key."""
    assert join_pairs("explain-mse-orjoin.json") == []


def test_a_cross_join_has_no_operands_to_resolve() -> None:
    assert join_pairs("explain-mse-crossjoin.json") == []


def test_catalog_keys_find_no_key_in_the_realtime_airlinestats_documents() -> None:
    assert (
        catalog_keys(
            config_json=load("tableconfig-airlineStats-realtime.json"),
            seg_metadata_json=load("seg-metadata-airlineStats-realtime.json"),
            schema_json=None,
            size_json=load("size-airlineStats-realtime.json"),
            table_metadata_json=None,
        )
        == frozenset()
    )


def test_an_upsert_tables_primary_key_is_evidence() -> None:
    """upsertConfig on the config, primaryKeyColumns on the schema, PK map non-empty."""
    assert upsert_keys(
        load("tableconfig-u12upsert.json"),
        load("schema-u12upsert.json"),
        load("metadata-u12upsert.json"),
    ) == frozenset({frozenset({"pk"})})


def test_the_non_upsert_twin_yields_no_evidence() -> None:
    assert (
        upsert_keys(
            load("tableconfig-u12plain.json"),
            load("schema-u12plain.json"),
            load("metadata-u12plain.json"),
        )
        == frozenset()
    )


def test_the_whole_primary_key_is_the_key_never_a_subset() -> None:
    """A composite key is unique as a tuple; a proper subset need not be."""
    schema = load("schema-u12upsert.json")
    schema["primaryKeyColumns"] = ["Region", "pk"]
    assert upsert_keys(
        load("tableconfig-u12upsert.json"), schema, load("metadata-u12upsert.json")
    ) == frozenset({frozenset({"region", "pk"})})


def test_a_config_and_a_pk_map_that_disagree_withhold_the_evidence() -> None:
    """Two documents disagreeing about what a table is, is not proof."""
    assert (
        upsert_keys(
            load("tableconfig-u12upsert.json"),
            load("schema-u12upsert.json"),
            load("metadata-u12plain.json"),
        )
        == frozenset()
    )
    assert (
        upsert_keys(
            load("tableconfig-u12plain.json"),
            load("schema-u12upsert.json"),
            load("metadata-u12upsert.json"),
        )
        == frozenset()
    )


@pytest.mark.parametrize("columns", [None, [], ["", "pk"], "pk", [1, 2], [None]])
def test_a_primary_key_list_that_is_not_names_yields_nothing(columns: Any) -> None:
    schema = load("schema-u12upsert.json")
    schema["primaryKeyColumns"] = columns
    assert (
        upsert_keys(
            load("tableconfig-u12upsert.json"), schema, load("metadata-u12upsert.json")
        )
        == frozenset()
    )


@pytest.mark.parametrize("body", [None, {}, [], "junk"])
def test_upsert_keys_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert upsert_keys(body, body, body) == frozenset()


def _upsert_config(**upsert: Any) -> dict[str, Any]:
    """A table config whose REALTIME half carries exactly this upsertConfig."""
    config = load("tableconfig-u12upsert.json")
    config["REALTIME"]["upsertConfig"] = upsert
    return config


@pytest.mark.parametrize("mode", ["FULL", "PARTIAL", "full", "Partial"])
def test_an_upserting_mode_still_proves_the_key(mode: str) -> None:
    """The mode is compared case-insensitively; both upserting modes qualify."""
    assert upsert_keys(
        _upsert_config(mode=mode),
        load("schema-u12upsert.json"),
        load("metadata-u12upsert.json"),
    ) == frozenset({frozenset({"pk"})})


@pytest.mark.parametrize("mode", ["NONE", "none", None, 1, "", "SOMETHING"])
def test_a_mode_that_is_not_an_upserting_one_proves_nothing(mode: Any) -> None:
    """Measured: `{"mode": "NONE"}` disables upsert while the object stays."""
    assert (
        upsert_keys(
            _upsert_config(mode=mode),
            load("schema-u12upsert.json"),
            load("metadata-u12upsert.json"),
        )
        == frozenset()
    )


@pytest.mark.parametrize("ttl", ["metadataTTL", "deletedKeysTTL"])
@pytest.mark.parametrize("value", [60000, 60000.0, 1, 0.5, "60000"])
def test_a_set_ttl_withholds_the_key(ttl: str, value: Any) -> None:
    """A TTL'd key is evicted from the lookup map while its old row stays
    visible, so the key is not unique in the query-visible view."""
    assert (
        upsert_keys(
            _upsert_config(mode="FULL", **{ttl: value}),
            load("schema-u12upsert.json"),
            load("metadata-u12upsert.json"),
        )
        == frozenset()
    )


@pytest.mark.parametrize("ttl", ["metadataTTL", "deletedKeysTTL"])
@pytest.mark.parametrize("value", [0, 0.0, -1, -1.0])
def test_a_ttl_of_zero_is_a_ttl_that_is_off(ttl: str, value: Any) -> None:
    """1.5.1's `isTTLEnabled()` is `_metadataTTL > 0 || _deletedKeysTTL > 0`
    and `isOutOfMetadataTTL` returns false outright at `_metadataTTL <= 0`,
    and the controller materialises `0.0` for both on every upsert config it
    serves — so treating 0 as set would withhold every upsert key there is.
    """
    assert upsert_keys(
        _upsert_config(mode="FULL", **{ttl: value}),
        load("schema-u12upsert.json"),
        load("metadata-u12upsert.json"),
    ) == frozenset({frozenset({"pk"})})


def test_the_live_u12upsert_config_carries_both_ttls_as_zero() -> None:
    """The captured config is the controller's own, defaults materialised."""
    upsert = load("tableconfig-u12upsert.json")["REALTIME"]["upsertConfig"]
    assert upsert["metadataTTL"] == 0.0
    assert upsert["deletedKeysTTL"] == 0.0


def test_the_measured_ttl_table_proves_no_key() -> None:
    """u12ttl, as captured live: `metadataTTL` 60 000 on a FULL upsert table
    where `SELECT pk, count(*) ... HAVING count(*) > 1` returned K1 -> 2 and
    the self-join returned 10 pairs against the 8 a unique pk would give."""
    assert (
        upsert_keys(
            load("tableconfig-u12ttl.json"),
            load("schema-u12upsert.json"),
            load("metadata-u12upsert.json"),
        )
        == frozenset()
    )


@pytest.mark.parametrize("upsert", [None, [], "FULL", 1])
def test_an_upsert_config_that_is_not_an_object_proves_nothing(upsert: Any) -> None:
    config = load("tableconfig-u12upsert.json")
    config["REALTIME"]["upsertConfig"] = upsert
    assert (
        upsert_keys(
            config, load("schema-u12upsert.json"), load("metadata-u12upsert.json")
        )
        == frozenset()
    )


def _size_naming(*names: str) -> dict[str, Any]:
    """A minimal size report naming exactly the given segments as sealed."""
    return {
        "realtimeSegments": {
            "segments": {name: {"reportedSizeInBytes": 100} for name in names}
        }
    }


def test_no_column_on_the_single_segment_table_reaches_its_doc_count() -> None:
    """Sound, and it finds nothing: the best ratio is playerID at 0.185."""
    assert (
        single_segment_unique_columns(
            load("seg-metadata-baseballStats-allcols.json"),
            load("tableconfig-baseballStats.json"),
            load("schema-baseballStats.json"),
            load("size-baseballStats.json"),
        )
        == frozenset()
    )


def test_a_column_whose_cardinality_equals_its_docs_is_a_key() -> None:
    """Gated on notNull, because a null could collide on the default value."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 3,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 3,
                    "totalDocs": 3,
                    "totalNumberOfEntries": 3,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 12},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                },
                {
                    "columnName": "city",
                    "cardinality": 2,
                    "totalDocs": 3,
                    "totalNumberOfEntries": 3,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "city",
                        "notNull": True,
                        "singleValueField": True,
                    },
                },
            ],
        }
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset(
        {frozenset({"id"})}
    )


def test_a_nullable_column_yields_nothing_however_unique_it_looks() -> None:
    """The null caveat is unclosed: cardinality may count a null as a value."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 3,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 3,
                    "totalDocs": 3,
                    "totalNumberOfEntries": 3,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 12},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": False,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


def test_null_handling_disabled_plus_a_non_nullable_schema_is_enough() -> None:
    """The second gate the spec allows: the table cannot store a null at all."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": False,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0")
    config = {"OFFLINE": {"tableIndexConfig": {"nullHandlingEnabled": False}}}
    schema = {"dimensionFieldSpecs": [{"name": "id", "dataType": "STRING"}]}
    assert single_segment_unique_columns(capture, config, schema, size) == frozenset(
        {frozenset({"id"})}
    )
    enabled = {"OFFLINE": {"tableIndexConfig": {"nullHandlingEnabled": True}}}
    assert single_segment_unique_columns(capture, enabled, schema, size) == frozenset()


def test_more_than_one_sealed_segment_proves_nothing() -> None:
    """Per-segment cardinality is the table's only where the two are one:
    summing Carrier over 31 segments gave 432 against a true 14."""
    capture = {
        f"seg{i}": {
            "segmentName": f"seg{i}",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
        for i in (0, 1)
    }
    size = _size_naming("seg0", "seg1")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


def test_a_consuming_segment_beside_the_sealed_one_proves_nothing() -> None:
    """The consuming segment's rows are not in any cardinality anyone can read."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        },
        "seg1": {"segmentName": "seg1", "totalDocs": 0, "crc": -9223372036854775808},
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


@pytest.mark.parametrize("body", [None, {}, [], "junk"])
def test_single_segment_unique_columns_never_raises(body: Any) -> None:
    assert single_segment_unique_columns(body, body, body, body) == frozenset()


def test_a_size_report_naming_two_sealed_segments_proves_nothing_f1() -> None:
    """F1: the metadata response can be one server's half of a 2-segment table.

    Metadata holds only one entry, matching seg0 exactly, but the size report
    names a second sealed segment the metadata call never returned.
    """
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0", "seg1")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


def test_a_size_report_naming_one_sealed_segment_matching_metadata_is_evidence_f1() -> (
    None
):
    """F1 happy path: size names exactly the one sealed segment metadata holds."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset(
        {frozenset({"id"})}
    )


def test_a_consuming_segment_named_only_by_size_proves_nothing() -> None:
    """A -1 entry the metadata never listed still holds rows no one can count.

    Metadata carries only the one sealed segment; the size report also names
    a second segment at reportedSizeInBytes -1, which metadata omits
    entirely (not even a consuming-shaped body). That segment's rows are
    invisible to metadata_is_complete's == 1 check, but they are still rows
    the sealed segment does not have, so the column is not a table key.
    """
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = {
        "realtimeSegments": {
            "segments": {
                "seg0": {"reportedSizeInBytes": 100},
                "seg1": {"reportedSizeInBytes": -1},
            }
        }
    }
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


@pytest.mark.parametrize("size", [None, {}])
def test_an_unreadable_size_report_proves_nothing_f1(size: Any) -> None:
    """F1: size_json unreadable means the one-sealed-segment claim is unverifiable."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 2,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 2,
                    "totalDocs": 2,
                    "totalNumberOfEntries": 2,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


def test_a_multi_value_column_is_never_a_key_f2() -> None:
    """F2: MV cardinality counts entries, not rows; three docs, four entries."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 3,
            "columns": [
                {
                    "columnName": "tags",
                    "cardinality": 3,
                    "totalDocs": 3,
                    "totalNumberOfEntries": 4,
                    "maxNumberOfMultiValues": 2,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "tags",
                        "notNull": True,
                        "singleValueField": False,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


def test_the_same_column_single_valued_is_evidence_f2() -> None:
    """F2 control: single-valued, entries == docs, no MV flag → still a key."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 3,
            "columns": [
                {
                    "columnName": "tags",
                    "cardinality": 3,
                    "totalDocs": 3,
                    "totalNumberOfEntries": 3,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "tags",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset(
        {frozenset({"tags"})}
    )


def test_max_multivalues_one_alone_excludes_the_column_f2() -> None:
    """F2: the maxNumberOfMultiValues flag alone is enough to exclude,
    even though totalNumberOfEntries == totalDocs on this entry."""
    capture = {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 5,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 5,
                    "totalDocs": 5,
                    "totalNumberOfEntries": 5,
                    "maxNumberOfMultiValues": 1,
                    "indexSizeMap": {"forward_index": 8},
                    "fieldSpec": {
                        "name": "id",
                        "notNull": True,
                        "singleValueField": True,
                    },
                }
            ],
        }
    }
    size = _size_naming("seg0")
    assert single_segment_unique_columns(capture, {}, {}, size) == frozenset()


def test_catalog_keys_carry_the_upsert_key() -> None:
    assert catalog_keys(
        config_json=load("tableconfig-u12upsert.json"),
        seg_metadata_json=load("seg-metadata-u12upsert.json"),
        schema_json=load("schema-u12upsert.json"),
        size_json=load("size-u12upsert.json"),
        table_metadata_json=load("metadata-u12upsert.json"),
    ) == frozenset({frozenset({"pk"})})


def _one_segment_capture(*, not_null: bool | None) -> dict[str, Any]:
    """One sealed segment whose `id` has cardinality == totalDocs."""
    spec: dict[str, Any] = {"name": "id", "singleValueField": True}
    if not_null is not None:
        spec["notNull"] = not_null
    return {
        "seg0": {
            "segmentName": "seg0",
            "totalDocs": 3,
            "columns": [
                {
                    "columnName": "id",
                    "cardinality": 3,
                    "totalDocs": 3,
                    "totalNumberOfEntries": 3,
                    "maxNumberOfMultiValues": 0,
                    "indexSizeMap": {"forward_index": 12},
                    "fieldSpec": spec,
                }
            ],
        }
    }


def test_an_unread_schema_establishes_no_nullability_f1() -> None:
    """F1: schema_json None is a schema nobody read, not a schema that says
    every column is non-nullable. notNull is absent and null handling is off,
    so the only thing that could clear the column is the nullable set — and
    with no schema that set is empty for want of evidence, not for want of
    nullable columns. Source (b) yields nothing."""
    config = {"OFFLINE": {"tableIndexConfig": {"nullHandlingEnabled": False}}}
    assert (
        catalog_keys(
            config_json=config,
            seg_metadata_json=_one_segment_capture(not_null=None),
            schema_json=None,
            size_json=_size_naming("seg0"),
            table_metadata_json=None,
        )
        == frozenset()
    )


def test_a_schema_that_says_the_column_cannot_be_null_is_evidence_f1() -> None:
    """F1 control: the same table, with the schema read and notNull true."""
    config = {"OFFLINE": {"tableIndexConfig": {"nullHandlingEnabled": False}}}
    schema = {"dimensionFieldSpecs": [{"name": "id", "dataType": "STRING"}]}
    assert catalog_keys(
        config_json=config,
        seg_metadata_json=_one_segment_capture(not_null=True),
        schema_json=schema,
        size_json=_size_naming("seg0"),
        table_metadata_json=None,
    ) == frozenset({frozenset({"id"})})


def test_key_columns_carry_the_catalog_spelling_sorted_by_lowercase_name() -> None:
    """Sorted by lowercase name (a, b), not by spelling (A, Z) — the two
    orders disagree here, so only the lowercase-name order passes."""
    keys = frozenset({frozenset({"a"}), frozenset({"b"})})
    subject = key_columns("default", "T", {"a": "Z", "b": "A"}, keys)
    assert subject == KeyColumns(database="default", table="T", columns=("Z", "A"))


def test_a_key_column_the_schema_does_not_name_gets_no_explain() -> None:
    keys = frozenset({frozenset({"b"})})
    assert key_columns("default", "T", {"a": "A"}, keys) is None


def test_no_key_gets_no_explain() -> None:
    assert key_columns("default", "T", {"a": "A"}, frozenset()) is None


@pytest.mark.parametrize(
    ("database", "spelled", "spelling"),
    [
        ("default", "T", "a,b"),
        ("default", "x--", "A"),
        ("ünï", "T", "A"),
        ("default", "", "A"),
    ],
)
def test_a_name_that_is_not_a_bare_identifier_gets_no_explain(
    database: str, spelled: str, spelling: str
) -> None:
    keys = frozenset({frozenset({"a"})})
    assert key_columns(database, spelled, {"a": spelling}, keys) is None


def test_a_composite_key_selects_both_its_columns() -> None:
    subject = key_columns(
        "default", "T", {"a": "A", "b": "B"}, frozenset({frozenset({"a", "b"})})
    )
    assert subject is not None
    assert subject.columns == ("A", "B")


def test_key_ordinal_sql_selects_the_key_columns_from_the_table() -> None:
    subject = KeyColumns(database="default", table="T", columns=("A", "B"))
    assert key_ordinal_sql(subject) == "SELECT A, B FROM default.T"
