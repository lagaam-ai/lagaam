"""ADR 0004's product/max rule over a plan that carries shape but no sizes.

Pinot's Calcite rowcounts are a constant 100 per scan, so this module reads
only the shape — which tables, which joins, which conditions — and takes its
sizes from the segment metadata the quotation already fetched.
"""

import json
from pathlib import Path

import pytest

from lagaam.adapters.pinot.plan import join_key_pairs, max_intermediate_rows

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


def join_pairs(name: str) -> list[tuple[str | None, str | None]]:
    """The resolved equality pairs of the one join node in this plan."""
    rels = json.loads(plan_cell(name))["rels"]
    by_id = {rel["id"]: rel for rel in rels}
    previous: dict[str, str | None] = {}
    last: str | None = None
    for rel in rels:
        previous[rel["id"]] = last
        last = rel["id"]
    join = next(rel for rel in rels if rel.get("joinType") or "Join" in rel["relOp"])
    return join_key_pairs(join["id"], by_id, previous, join["inputs"])


def test_a_cross_join_is_charged_the_product_plus_its_children() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), LEAVES)
        == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS
    )


def test_an_equi_join_is_charged_the_product_since_no_key_can_be_proven() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-equijoin.json"), LEAVES)
        == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS
    )


def test_a_self_join_pairs_the_one_scan_calcite_folded_it_into() -> None:
    """Measured on 1.5.1: the LogicalJoin's two inputs are the same node id."""
    assert (
        max_intermediate_rows(
            plan_cell("explain-mse-selfjoin.json"),
            {"default.airlinestats": AIRLINE_DOCS},
        )
        == AIRLINE_DOCS * AIRLINE_DOCS + 2 * AIRLINE_DOCS
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
    assert answer == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


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
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5510


def test_an_or_of_equalities_is_charged_the_product() -> None:
    assert (
        max_intermediate_rows(plan_cell("explain-mse-orjoin.json"), LEAVES)
        == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS
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
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5510


def test_an_outer_joins_unmatched_rows_ride_on_top_of_the_product() -> None:
    """Measured on 1.5.1: a 1-row side FULL OUTER JOIN a 4-row side with no
    matching key returned 5, where the bare product bounds only 4. The
    unmatched rows of both sides pass through on top of the matched pairs,
    so a + b is added — without reading joinType, which is one more plan
    attribute to trust for a term worth 0.02% on real tables."""
    plan = json.dumps(
        {
            "rels": [
                {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                {
                    "id": "2",
                    "relOp": "LogicalJoin",
                    "joinType": "full",
                    "inputs": ["0", "1"],
                },
            ]
        }
    )
    assert max_intermediate_rows(plan, {"default.a": 1, "default.b": 4}) == 9


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
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5510


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
    assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5510


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


UPSERT_KEYS = {"default.airlinestats": frozenset({frozenset({"carrier"})})}
BOTH_KEYS = {
    "default.airlinestats": frozenset({frozenset({"carrier"})}),
    "default.baseballstats": frozenset({frozenset({"teamid"})}),
}


def test_a_plain_equi_join_resolves_both_operands_to_column_names() -> None:
    assert join_pairs("explain-mse-join-plain.json") == [("carrier", "teamid")]


def test_an_operand_index_counts_over_left_fields_then_right() -> None:
    """Right teamID at right-index 1 is global 3 = 2 left fields + 1."""
    assert join_pairs("explain-mse-join-twokeys.json") == [
        ("carrier", "teamid"),
        ("origin", "league"),
    ]


def test_an_expression_operand_is_no_evidence() -> None:
    """upper(a.Carrier) became $f85 with an op of UPPER.

    The operand resolves to None — that side has no column here, so no key
    of it can be covered — while the other side's column still resolves.
    """
    assert join_pairs("explain-mse-join-expr.json") == [(None, "teamid")]


def test_a_left_join_resolves_exactly_as_an_inner_one_does() -> None:
    """joinType is not read: the rule is about the key, not the join kind."""
    assert join_pairs("explain-mse-join-left.json") == [("carrier", "teamid")]


def test_an_or_condition_is_no_evidence() -> None:
    """ADR 0008's OR case stands: an OR of equalities is not a key."""
    assert join_pairs("explain-mse-orjoin.json") == []


def test_a_cross_join_has_no_operands_to_resolve() -> None:
    assert join_pairs("explain-mse-crossjoin.json") == []


def test_a_left_unique_key_charges_the_right_side_plus_the_inputs() -> None:
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-plain.json"), LEAVES, UPSERT_KEYS
    ) == BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_a_right_unique_key_charges_the_left_side_plus_the_inputs() -> None:
    keys = {"default.baseballstats": frozenset({frozenset({"teamid"})})}
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-plain.json"), LEAVES, keys
    ) == AIRLINE_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_both_sides_unique_charge_the_smaller_one_plus_the_inputs() -> None:
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-plain.json"), LEAVES, BOTH_KEYS
    ) == min(AIRLINE_DOCS, BASEBALL_DOCS) + AIRLINE_DOCS + BASEBALL_DOCS


def test_no_evidence_is_still_the_product() -> None:
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-plain.json"), LEAVES
    ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_a_key_on_a_column_the_join_does_not_equate_is_not_covered() -> None:
    keys = {"default.airlinestats": frozenset({frozenset({"origin"})})}
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-plain.json"), LEAVES, keys
    ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_a_join_equating_more_columns_than_the_key_still_covers_it() -> None:
    """Cover is subset, not equality: more equalities cannot mean more matches."""
    keys = {"default.airlinestats": frozenset({frozenset({"carrier"})})}
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-twokeys.json"), LEAVES, keys
    ) == BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_a_composite_key_needs_every_one_of_its_columns_equated() -> None:
    keys = {
        "default.airlinestats": frozenset({frozenset({"carrier", "dayssinceepoch"})})
    }
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-twokeys.json"), LEAVES, keys
    ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_a_composite_key_fully_equated_is_covered() -> None:
    keys = {"default.airlinestats": frozenset({frozenset({"carrier", "origin"})})}
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-twokeys.json"), LEAVES, keys
    ) == BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_an_expression_key_cannot_be_covered_however_unique_the_column() -> None:
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-expr.json"), LEAVES, UPSERT_KEYS
    ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_an_expression_on_one_side_still_lets_the_other_sides_key_bound_it() -> None:
    """upper(a.Carrier) = b.teamID still matches one b row per left row."""
    keys = {"default.baseballstats": frozenset({frozenset({"teamid"})})}
    assert max_intermediate_rows(
        plan_cell("explain-mse-join-expr.json"), LEAVES, keys
    ) == AIRLINE_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


def test_a_self_join_on_a_unique_key_is_charged_the_bound_not_the_product() -> None:
    """The shipped self-join plan names the same node id as both inputs."""
    keys = {"default.airlinestats": frozenset({frozenset({"carrier"})})}
    product = max_intermediate_rows(plan_cell("explain-mse-selfjoin.json"), LEAVES)
    bounded = max_intermediate_rows(
        plan_cell("explain-mse-selfjoin.json"), LEAVES, keys
    )
    assert product is not None and bounded is not None
    assert bounded < product


ABC_LEAVES = {"default.a": 10, "default.b": 500, "default.c": 7}

# The inner cross join of A and B: 10 * 500 + 10 + 500.
INNER_ROWS = 5510


def _scan(rel_id: str, table: str) -> dict[str, object]:
    return {
        "id": rel_id,
        "relOp": "org.apache.pinot.calcite.rel.logical.PinotLogicalTableScan",
        "table": ["default", table],
        "inputs": [],
    }


def _project(rel_id: str, source: str, fields: list[str]) -> dict[str, object]:
    return {
        "id": rel_id,
        "relOp": "org.apache.calcite.rel.logical.LogicalProject",
        "inputs": [source],
        "fields": fields,
        "exprs": [{"input": index} for index in range(len(fields))],
    }


def _equals(first: int, second: int) -> dict[str, object]:
    return {
        "op": {"name": "=", "kind": "EQUALS"},
        "operands": [{"input": first}, {"input": second}],
    }


def _join(
    rel_id: str, inputs: list[str], condition: dict[str, object] | None
) -> dict[str, object]:
    rel: dict[str, object] = {
        "id": rel_id,
        "relOp": "org.apache.calcite.rel.logical.LogicalJoin",
        "inputs": inputs,
        "joinType": "inner",
    }
    if condition is not None:
        rel["condition"] = condition
    return rel


def _join_beside_a_chain(
    left_input: str, condition: dict[str, object] | None
) -> str:
    """A ⨯ B under an outer join with C, the outer left input chosen by id.

    `left_input` is "5" for the project over the inner join — a straight
    left column list over rows that are not one table's — or "4" for the
    inner join node itself, which carries no fields at all.
    """
    rels = [
        _scan("0", "a"),
        _project("1", "0", ["k"]),
        _scan("2", "b"),
        _project("3", "2", ["x"]),
        _join("4", ["1", "3"], None),
        _project("5", "4", ["k", "x"]),
        _scan("6", "c"),
        _project("7", "6", ["k"]),
        _join("8", [left_input, "7"], condition),
    ]
    return json.dumps({"rels": rels})


def test_a_side_that_is_a_join_is_no_evidence_but_does_not_silence_the_other() -> None:
    """A's key cannot cover: the outer join's left side is a join, not A."""
    plan = _join_beside_a_chain("5", _equals(0, 2))
    no_evidence = INNER_ROWS * 7 + INNER_ROWS + 7
    assert max_intermediate_rows(plan, ABC_LEAVES) == no_evidence
    a_keys = {"default.a": frozenset({frozenset({"k"})})}
    assert max_intermediate_rows(plan, ABC_LEAVES, a_keys) == no_evidence


def test_a_keyed_table_beside_a_join_still_bounds_that_join() -> None:
    """C's side is a straight chain: its key bounds the outer join."""
    plan = _join_beside_a_chain("5", _equals(0, 2))
    c_keys = {"default.c": frozenset({frozenset({"k"})})}
    assert (
        max_intermediate_rows(plan, ABC_LEAVES, c_keys) == INNER_ROWS + INNER_ROWS + 7
    )


def test_a_left_side_without_fields_resolves_no_pairs() -> None:
    """A bare join at the top of the left side gives no split to number by."""
    plan = _join_beside_a_chain("4", _equals(0, 2))
    c_keys = {"default.c": frozenset({frozenset({"k"})})}
    unbounded = max_intermediate_rows(plan, ABC_LEAVES)
    assert unbounded == INNER_ROWS * 7 + INNER_ROWS + 7
    assert max_intermediate_rows(plan, ABC_LEAVES, c_keys) == unbounded


def test_an_operand_index_past_the_last_field_resolves_no_pairs() -> None:
    """Three fields in all, so global index 3 is nobody's column."""
    plan = _join_beside_a_chain("5", _equals(0, 3))
    c_keys = {"default.c": frozenset({frozenset({"k"})})}
    unbounded = max_intermediate_rows(plan, ABC_LEAVES)
    assert max_intermediate_rows(plan, ABC_LEAVES, c_keys) == unbounded
