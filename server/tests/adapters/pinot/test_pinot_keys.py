"""The scan ordinals a join-key proof is built from.

A key column is learned as its scan ordinal from an EXPLAIN of the key columns
themselves, and a join operand is resolved by composing its index down its own
side's chain to the scan it was read from.
"""

import json
from pathlib import Path

import pytest

from lagaam.adapters.pinot.keys import join_key_pairs, key_ordinals

FIXTURES = Path(__file__).parent / "fixtures"


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
