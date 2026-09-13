"""ADR 0004's product/max rule over a plan that carries shape but no sizes.

Pinot's Calcite rowcounts are a constant 100 per scan, so this module reads
only the shape — which tables, which joins, which conditions — and takes its
sizes from the segment metadata the quotation already fetched.
"""

import json
from pathlib import Path

import pytest

from lagaam.adapters.pinot.plan import max_intermediate_rows

FIXTURES = Path(__file__).parent / "fixtures"

AIRLINE_DOCS = 9746
BASEBALL_DOCS = 97889
LEAVES = {
    "default.airlinestats": AIRLINE_DOCS,
    "default.baseballstats": BASEBALL_DOCS,
}


def plan_cell(name: str) -> str:
    body = json.loads((FIXTURES / name).read_text())
    cell = body["resultTable"]["rows"][0][1]
    assert isinstance(cell, str)
    return cell


def test_a_cross_join_is_charged_the_product_of_its_children() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), LEAVES)
        == AIRLINE_DOCS * BASEBALL_DOCS
    )


def test_an_equi_join_is_charged_the_product_since_no_key_can_be_proven() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-equijoin.json"), LEAVES)
        == AIRLINE_DOCS * BASEBALL_DOCS
    )


def test_a_self_join_pairs_the_one_scan_calcite_folded_it_into() -> None:
    """Measured on 1.5.1: the LogicalJoin's two inputs are the same node id."""
    assert (
        max_intermediate_rows(
            plan_cell("explain-mse-selfjoin.json"),
            {"default.airlinestats": AIRLINE_DOCS},
        )
        == AIRLINE_DOCS * AIRLINE_DOCS
    )


def test_a_single_table_plan_is_charged_its_scan() -> None:
    assert (
        max_intermediate_rows(
            plan_cell("explain-mse-singletable.json"),
            {"default.airlinestats": 1234},
        )
        == 1234
    )


def test_a_table_with_no_leaf_size_makes_the_answer_unknown() -> None:
    assert max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), {}) is None
    assert (
        max_intermediate_rows(
            plan_cell("explain-mse-crossjoin.json"),
            {"default.airlinestats": AIRLINE_DOCS, "default.baseballstats": None},
        )
        is None
    )


def test_the_rowcount_attributes_are_never_read() -> None:
    """Pinot prices every scan at a constant 100; reading it admits a cross join."""
    answer = max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), LEAVES)
    assert answer not in (100, 10000)
    assert answer == AIRLINE_DOCS * BASEBALL_DOCS


def test_a_node_without_inputs_consumes_the_node_before_it() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "t"], "inputs": []},
                {"id": "1", "relOp": "LogicalProject"},
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.t": 42}) == 42


def test_a_join_whose_condition_nests_an_equality_is_still_the_product() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                {
                    "id": "2",
                    "relOp": "LogicalJoin",
                    "joinType": "inner",
                    "inputs": ["0", "1"],
                    "condition": {
                        "op": {"name": "AND", "kind": "AND"},
                        "operands": [
                            {"op": {"name": ">", "kind": "GREATER_THAN"}, "operands": []},
                            {"op": {"name": "=", "kind": "EQUALS"}, "operands": []},
                        ],
                    },
                },
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5000


def test_an_or_of_equalities_is_charged_the_product() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-orjoin.json"), LEAVES)
        == AIRLINE_DOCS * BASEBALL_DOCS
    )


def test_a_negated_equality_is_charged_the_product() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                {
                    "id": "2",
                    "relOp": "LogicalJoin",
                    "joinType": "inner",
                    "inputs": ["0", "1"],
                    "condition": {
                        "op": {"name": "NOT", "kind": "NOT"},
                        "operands": [
                            {"op": {"name": "=", "kind": "EQUALS"}, "operands": []},
                        ],
                    },
                },
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5000


def test_a_union_all_is_charged_the_sum_of_its_inputs() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-unionall.json"), LEAVES)
        == AIRLINE_DOCS + BASEBALL_DOCS
    )


def test_a_distinct_union_is_charged_the_sum_too() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                {
                    "id": "2",
                    "relOp": "LogicalUnion",
                    "all": False,
                    "inputs": ["0", "1"],
                },
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 510


def test_a_correlate_is_charged_the_product() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                {
                    "id": "2",
                    "relOp": "LogicalCorrelate",
                    "inputs": ["0", "1"],
                    "joinType": "inner",
                },
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5000


def test_an_inequality_join_is_charged_the_product() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                {
                    "id": "2",
                    "relOp": "LogicalJoin",
                    "joinType": "inner",
                    "inputs": ["0", "1"],
                    "condition": {"op": {"name": ">", "kind": "GREATER_THAN"}, "operands": []},
                },
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5000


@pytest.mark.parametrize(
    "plan", ["", "not json", "[]", "null", json.dumps({"rels": "nope"}), json.dumps({})]
)
def test_a_plan_that_cannot_be_read_is_no_answer(plan: str) -> None:
    assert max_intermediate_rows(plan, LEAVES) is None


def test_a_cycle_in_the_inputs_does_not_hang() -> None:
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "LogicalProject", "inputs": ["1"]},
                {"id": "1", "relOp": "LogicalProject", "inputs": ["0"]},
            ]
        }
    )
    assert max_intermediate_rows(plan, LEAVES) is None
