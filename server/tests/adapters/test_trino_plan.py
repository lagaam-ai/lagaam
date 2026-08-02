"""The plan reader: what the widest operator in the plan would produce."""

import json

from lagaam.adapters.trino.plan import max_intermediate_rows


def _node(name: str, rows: object, children: list[dict] | None = None) -> dict:
    """One plan node. rows=None means the estimate list is empty."""
    estimates = [] if rows is None else [{"outputRowCount": rows}]
    return {"name": name, "estimates": estimates, "children": children or []}


def test_a_healthy_join_reports_its_own_row_count() -> None:
    # Measured on Trino 476: lineitem JOIN orders ON orderkey -> 60,175.
    plan = _node(
        "Output",
        60175.0,
        [
            _node(
                "InnerJoin",
                60175.0,
                [_node("ScanFilter", 60175.0), _node("TableScan", 15000.0)],
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0


def test_a_nan_join_is_charged_the_product_of_its_children() -> None:
    # The laundering shape: CROSS JOIN under a LIKE '%x%' filter reports NaN
    # at the join while still doing 60,175 x 15,000 rows of work.
    plan = _node(
        "Output",
        "NaN",
        [
            _node(
                "CrossJoin",
                "NaN",
                [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0


def test_a_nan_non_join_takes_the_widest_child() -> None:
    # A filter cannot manufacture rows, so an unknown one is not a product.
    plan = _node(
        "Output",
        "NaN",
        [_node("ScanFilterProject", "NaN", [_node("TableScan", 15000.0)])],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 15000.0


def test_the_widest_operator_wins_over_a_narrow_output() -> None:
    # A LIMIT collapses the output to 10 while the join still built 902M.
    plan = _node(
        "Output",
        10.0,
        [
            _node(
                "Limit",
                10.0,
                [
                    _node(
                        "CrossJoin",
                        902625000.0,
                        [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
                    )
                ],
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 902625000.0


def test_several_estimates_take_the_greatest_finite_one() -> None:
    node = {
        "name": "ScanFilterProject",
        "estimates": [
            {"outputRowCount": 15000.0},
            {"outputRowCount": "NaN"},
            {"outputRowCount": 60175.0},
        ],
        "children": [],
    }
    assert max_intermediate_rows(json.dumps(node)) == 60175.0


def test_an_unknown_node_name_is_not_treated_as_a_join() -> None:
    # Conservative default: a name we do not know cannot invent a product.
    plan = _node(
        "SomeFutureTrinoNode",
        "NaN",
        [_node("TableScan", 100.0), _node("TableScan", 200.0)],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 200.0


def test_a_plan_with_nothing_knowable_is_none() -> None:
    plan = _node("Output", "NaN", [_node("TableScan", "NaN")])
    assert max_intermediate_rows(json.dumps(plan)) is None


def test_malformed_input_is_none_not_a_crash() -> None:
    assert max_intermediate_rows("not json") is None
    assert max_intermediate_rows("null") is None
    assert max_intermediate_rows("[]") is None
    assert max_intermediate_rows(json.dumps({"name": "Output"})) is None


def test_junk_inside_a_well_formed_plan_is_survived() -> None:
    plan = {
        "name": "Output",
        "estimates": "not-a-list",
        "children": [
            {"name": "TableScan", "estimates": [{"outputRowCount": 42.0}]},
            "not-a-node",
            None,
        ],
    }
    assert max_intermediate_rows(json.dumps(plan)) == 42.0


def test_a_non_string_node_name_does_not_raise() -> None:
    # A node with name as list or dict should not raise TypeError on membership test.
    plan = {
        "name": ["CrossJoin"],
        "estimates": [{"outputRowCount": 100.0}],
        "children": [
            {"name": "TableScan", "estimates": [{"outputRowCount": 50.0}], "children": []}
        ],
    }
    assert max_intermediate_rows(json.dumps(plan)) == 100.0

    plan = {
        "name": {"type": "CrossJoin"},
        "estimates": [],
        "children": [
            {"name": "TableScan", "estimates": [{"outputRowCount": 75.0}], "children": []}
        ],
    }
    assert max_intermediate_rows(json.dumps(plan)) == 75.0


def test_a_join_product_that_overflows_to_infinity_does_not_contribute() -> None:
    # Two very large numbers multiply to infinity. The CrossJoin has no
    # own estimate, so it would compute 1e200 * 1e200 = inf as a product.
    # This infinity must be rejected (not recorded in widest).
    # The children's individual estimates are still recorded.
    plan = _node(
        "CrossJoin",
        "NaN",
        [
            _node("TableScan", 1e200),
            _node("TableScan", 1e200),
        ],
    )
    # The infinity product is rejected. The two TableScans with 1e200 are recorded.
    # Max is 1e200, not inf.
    assert max_intermediate_rows(json.dumps(plan)) == 1e200


def test_json_with_very_large_integers_does_not_raise() -> None:
    # A JSON integer too large to convert to float (e.g., 10**400) should
    # raise OverflowError in finite_number, which must be caught.
    plan = {
        "name": "TableScan",
        "estimates": [{"outputRowCount": 10**400}],
        "children": [],
    }
    assert max_intermediate_rows(json.dumps(plan)) is None


def test_json_nested_deeply_does_not_raise_recursion_error() -> None:
    # json.loads itself can raise RecursionError on deeply nested JSON.
    # We need to catch that at the json.loads call.
    # Simulate with a moderately deep structure that stresses json.loads.
    plan_json = '{"name": "TableScan", "estimates": [{"outputRowCount": 1.0}], "children": []}'
    # Build to just under where json.loads starts failing (around 5000)
    for _ in range(4800):
        plan_json = f'{{"name": "Project", "estimates": [], "children": [{plan_json}]}}'
    # This should return None, not raise RecursionError
    assert max_intermediate_rows(plan_json) is None


def test_a_pathologically_deep_plan_is_refused_rather_than_recursed() -> None:
    # Build a 4500-deep plan as a JSON string directly. The estimate sits at
    # the bottom, past our depth cap of 400, so nothing is recorded.
    plan_json = '{"name": "TableScan", "estimates": [{"outputRowCount": 1.0}], "children": []}'
    for _ in range(4500):
        plan_json = f'{{"name": "Project", "estimates": [], "children": [{plan_json}]}}'
    assert max_intermediate_rows(plan_json) is None
