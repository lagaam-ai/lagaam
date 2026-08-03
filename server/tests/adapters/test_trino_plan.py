"""The plan reader: what the widest operator in the plan would produce."""

import json

from lagaam.adapters.trino.plan import _has_nan_scan, max_intermediate_rows


def _node(
    name: str,
    rows: object,
    children: list[dict] | None = None,
    descriptor: object = None,
    details: object = None,
    estimates: object = None,
) -> dict:
    """One plan node. rows=None means the estimate list is empty."""
    if estimates is None:
        estimates = [] if rows is None else [{"outputRowCount": rows}]
    node = {"name": name, "estimates": estimates, "children": children or []}
    if descriptor is not None:
        node["descriptor"] = descriptor
    if details is not None:
        node["details"] = details
    return node


def _scan(name: str, rows: object, column: str = "linestatus") -> dict:
    """A leaf scan that assigns a plain base column, as Trino renders it."""
    return _node(name, rows, details=[f"{column} := tpch:{column}"])


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
                descriptor={},
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0


def test_a_nan_join_over_an_aggregation_takes_the_widest_child() -> None:
    # Correlated semi-join shape, measured live on Trino 476: the decorrelated
    # subquery arrives under an Aggregate, which emits at most one row per
    # group, so the join cannot blow up and the product would invent work.
    plan = _node(
        "Output",
        "NaN",
        [
            _node(
                "InnerJoin",
                "NaN",
                [
                    _node("Aggregate", "NaN", [_scan("TableScan", 1200000.0)]),
                    _scan("TableScan", 10000.0),
                ],
                descriptor={"criteria": "(suppkey_1 = suppkey)"},
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 1200000.0


def test_a_nan_join_between_two_plain_scans_is_charged_the_product() -> None:
    # The filter-laundering attack: the key stays a plain column but a
    # regexp_like filter nulls one side's estimate. Neither side is bounded
    # by an aggregation, so a low-cardinality key really does multiply.
    # Measured live: quoted 60,175 against a true 1,810,518,277 rows.
    plan = _node(
        "Output",
        "NaN",
        [
            _node(
                "InnerJoin",
                "NaN",
                [
                    _scan("ScanFilterProject", 60175.0),
                    _node(
                        "LocalExchange",
                        "NaN",
                        [
                            _node(
                                "RemoteExchange",
                                "NaN",
                                [
                                    _node(
                                        "ScanFilterProject",
                                        None,
                                        estimates=[
                                            {"outputRowCount": 60175.0},
                                            {"outputRowCount": "NaN"},
                                        ],
                                        details=["linestatus := tpch:linestatus"],
                                    )
                                ],
                            )
                        ],
                    ),
                ],
                descriptor={"criteria": "(linestatus = linestatus_10)"},
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 60175.0


def test_a_nan_join_sees_an_aggregation_through_exchange_wrappers() -> None:
    # The real plan nests Aggregate under LocalExchange -> RemoteExchange;
    # the walk must see through those or the exemption never fires.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _node(
                "LocalExchange",
                "NaN",
                [
                    _node(
                        "RemoteExchange",
                        "NaN",
                        [_node("Aggregate", "NaN", [_scan("TableScan", 1200000.0)])],
                    )
                ],
            ),
            _scan("TableScan", 10000.0),
        ],
        descriptor={"criteria": "(suppkey_1 = suppkey)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 1200000.0


def test_a_nan_join_whose_key_is_a_computed_expression_pays_the_product() -> None:
    # substr/lower/cast wrappers: both sides are sized, no NaN anywhere, but
    # the key is derived. The plan's own assignment gives it away.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _node(
                "ScanProject",
                60175.0,
                details=["expr := substring(linestatus, bigint '1', bigint '1')"],
            ),
            _node(
                "ScanProject",
                60175.0,
                details=["expr_17 := substring(linestatus, bigint '1', bigint '1')"],
            ),
        ],
        descriptor={"criteria": "(expr = expr_17)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 60175.0


def test_a_nan_join_on_base_columns_with_sized_sides_takes_the_widest() -> None:
    # Trino stops estimating multi-way joins past a depth: a 4-table star
    # join reports NaN above an already-sized join, with every side sized and
    # no derived key. Charging the product there denies ordinary analytics.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _node(
                "InnerJoin",
                3040568.0,
                [_scan("ScanFilter", 6001215.0, "orderkey"), _scan("ScanFilter", 10000.0, "suppkey")],
                descriptor={"criteria": "(suppkey_1 = suppkey)"},
            ),
            _node(
                "TableScan",
                729413.0,
                details=["orderkey_4 := tpch:orderkey"],
            ),
        ],
        descriptor={"criteria": "(orderkey = orderkey_4)"},
    )
    # The join takes max-of-children; the widest operator is the 6M leaf scan.
    assert max_intermediate_rows(json.dumps(plan)) == 6001215.0


def test_a_nan_join_with_missing_descriptor_is_charged_the_product() -> None:
    plan = _node(
        "CrossJoin",
        "NaN",
        [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0


def test_a_nan_join_with_non_dict_descriptor_is_charged_the_product() -> None:
    plan_string_descriptor = _node(
        "CrossJoin",
        "NaN",
        [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
        descriptor="not-a-dict",
    )
    assert max_intermediate_rows(json.dumps(plan_string_descriptor)) == 60175.0 * 15000.0

    plan_list_descriptor = _node(
        "CrossJoin",
        "NaN",
        [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
        descriptor=["criteria"],
    )
    assert max_intermediate_rows(json.dumps(plan_list_descriptor)) == 60175.0 * 15000.0


def test_a_nan_join_with_empty_criteria_is_charged_the_product() -> None:
    plan = _node(
        "CrossJoin",
        "NaN",
        [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
        descriptor={"criteria": ""},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0


def test_a_nan_join_with_a_shadowed_alias_key_is_charged_the_product() -> None:
    # The attacker aliases a derived key back to a plain column name. The
    # name is a lie; the assignment in the plan's details is not.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _node("ScanProject", 60175.0, details=["linestatus := lower(linestatus_3)"]),
            _node("ScanProject", 60175.0, details=["linestatus_10 := lower(linestatus_9)"]),
        ],
        descriptor={"criteria": "(linestatus = linestatus_10)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 60175.0


def test_a_nan_join_with_mixed_plain_and_derived_criteria_is_charged_the_product() -> None:
    # One conjunct plain, one derived: conservative choice denies the whole.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _node("ScanProject", 60175.0, details=["orderkey := tpch:orderkey"]),
            _node("ScanProject", 15000.0, details=["expr_9 := lower(linestatus)"]),
        ],
        descriptor={"criteria": "(orderkey = orderkey_4) AND (expr = expr_9)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0


def test_a_nan_join_with_a_row_preserving_window_side_takes_the_widest() -> None:
    # A Window emits exactly one row per input row, so a Window over a sized
    # input bounds that side even though Trino reports NaN above it.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _scan("ScanFilter", 150000.0, "custkey"),
            _node(
                "LocalExchange",
                "NaN",
                [_node("Window", None, [_scan("TableScan", 1500000.0, "custkey")])],
            ),
        ],
        descriptor={"criteria": "(custkey = custkey_1)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 1500000.0


def test_malformed_details_fall_through_to_the_product() -> None:
    # Anything unparseable must deny, never exempt.
    for details in ("not-a-list", [None, 42, {"a": 1}], [], ["no assignment here"]):
        plan = _node(
            "InnerJoin",
            "NaN",
            [_node("ScanProject", 60175.0), _node("ScanProject", 15000.0)],
            descriptor={"criteria": "(expr = expr_17)"},
            details=details,
        )
        assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0, details


def test_a_nan_join_with_unparseable_criteria_is_charged_the_product() -> None:
    for criteria in ("", "garbage", "(a <> b)", 42, None, ["(a = b)"]):
        plan = _node(
            "InnerJoin",
            "NaN",
            [_scan("TableScan", 60175.0), _scan("TableScan", 15000.0)],
            descriptor={"criteria": criteria},
        )
        assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0, criteria


def test_an_unknown_bounding_node_name_does_not_exempt() -> None:
    # A node we do not recognise cannot vouch for a side.
    plan = _node(
        "InnerJoin",
        "NaN",
        [
            _node("SomeFutureBoundingNode", "NaN", [_scan("TableScan", 60175.0)]),
            _scan("TableScan", 15000.0),
        ],
        descriptor={"criteria": "(expr = expr_17)"},
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


def test_a_laundered_scan_under_a_nested_join_still_pays_the_product() -> None:
    # The dirty leaf hides one join deeper than the guard used to look, so the
    # outer join read as clean and priced itself at max-of-children.
    laundered = _node(
        "Filter",
        "NaN",
        [_scan("TableScan", "NaN", "custkey")],
        details=["regexp_like(comment, 'x')"],
    )
    inner = _node(
        "InnerJoin",
        "NaN",
        [_scan("TableScan", 500000.0, "suppkey"), laundered],
        descriptor={"criteria": "(suppkey = custkey)"},
    )
    plan = _node(
        "InnerJoin",
        "NaN",
        [_scan("TableScan", 1000000.0, "orderkey"), inner],
        descriptor={"criteria": "(orderkey = suppkey)"},
    )
    # 1e6 * 5e5: the outer join pays the product rather than reading 1e6.
    assert max_intermediate_rows(json.dumps(plan)) == 500000000000.0


def test_an_aggregation_under_a_nested_join_still_takes_the_widest() -> None:
    # The mirror of the case above: an aggregation caps that side, so a NaN
    # scan beneath it cannot reach the join and must not deny the query.
    bounded = _node(
        "Aggregate",
        15000.0,
        [_node("Filter", "NaN", [_scan("TableScan", "NaN", "custkey")])],
    )
    inner = _node(
        "InnerJoin",
        "NaN",
        [_scan("TableScan", 5000.0, "suppkey"), bounded],
        descriptor={"criteria": "(suppkey = custkey)"},
    )
    plan = _node(
        "InnerJoin",
        "NaN",
        [_scan("TableScan", 1000000.0, "orderkey"), inner],
        descriptor={"criteria": "(orderkey = suppkey)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 1000000.0


def test_a_no_op_limit_does_not_exempt_a_laundered_join() -> None:
    # Measured on Trino 476 (tpch.tiny): LIMIT 1000000 over a 60,175-row table
    # caps nothing, but membership in the bounding set granted the exemption
    # before the dirt walk ran — quoting 60,175 for 1,810,518,277 real rows.
    # A laundered scan keeps a finite alternative beside the NaN one, which is
    # how the join still prices while reporting no usable estimate.
    laundered = _node(
        "ScanFilterProject",
        None,
        details=["linestatus := tpch:linestatus", "regexp_like(comment, 'x')"],
        estimates=[{"outputRowCount": 60175.0}, {"outputRowCount": "NaN"}],
    )
    plan = _node(
        "InnerJoin",
        "NaN",
        [_node("Limit", 60175.0, [_scan("TableScan", 60175.0)]), laundered],
        descriptor={"criteria": "(linestatus = linestatus_1)"},
    )
    # The product, not the widest child: an unbounded dirty side reaches it.
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 60175.0


def test_a_distinct_over_the_join_key_does_not_exempt_a_laundered_join() -> None:
    # DISTINCT bounds its own output, but on a superset of the join key it
    # still fans out — measured 7,681x under-report before this check.
    laundered = _node(
        "ScanFilterProject",
        None,
        details=["linestatus := tpch:linestatus", "regexp_like(comment, 'x')"],
        estimates=[{"outputRowCount": 60175.0}, {"outputRowCount": "NaN"}],
    )
    plan = _node(
        "InnerJoin",
        "NaN",
        [_node("Distinct", 30000.0, [_scan("TableScan", 60175.0)]), laundered],
        descriptor={"criteria": "(linestatus = linestatus_1)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 30000.0 * 60175.0


def test_a_join_beside_a_sized_unknown_node_takes_the_widest() -> None:
    # An unrecognised node Trino sized is trusted; only unsized ones deny.
    sized_unknown = _node("SomeFutureTrinoNode", 4000.0, [_scan("TableScan", 4000.0, "custkey")])
    plan = _node(
        "InnerJoin",
        "NaN",
        [_scan("TableScan", 1000000.0, "orderkey"), sized_unknown],
        descriptor={"criteria": "(orderkey = custkey)"},
    )
    assert max_intermediate_rows(json.dumps(plan)) == 1000000.0


def test_the_dirt_walk_gives_up_dirty_so_an_unread_subtree_denies() -> None:
    # _has_nan_scan is read negated, so every give-up must return True or the
    # walk grants the exemption it meant to withhold.
    buried = _scan("TableScan", 1.0, "custkey")
    for _ in range(70):
        buried = _node("Project", "NaN", [buried])
    assert _has_nan_scan(buried, 0) is True
    assert _has_nan_scan({"name": 42, "children": [], "estimates": []}, 0) is True
    # An unrecognised node Trino left unsized is dirt; a sized one is not.
    assert _has_nan_scan(_node("SomeFutureTrinoNode", "NaN", [_scan("TableScan", 5.0, "c")]), 0) is True
    assert _has_nan_scan(_node("SomeFutureTrinoNode", 4000.0, [_scan("TableScan", 5.0, "c")]), 0) is False
    # An aggregation caps its side, so dirt below it never reaches the join --
    # true even when Trino could not size the aggregation itself.
    assert _has_nan_scan(_node("Aggregate", 15000.0, [_scan("TableScan", "NaN", "custkey")]), 0) is False
    assert _has_nan_scan(_node("Aggregate", "NaN", [_scan("TableScan", "NaN", "custkey")]), 0) is False


def test_a_pathologically_deep_plan_is_refused_rather_than_recursed() -> None:
    # Build a 4500-deep plan as a JSON string directly. The estimate sits at
    # the bottom, past our depth cap of 400, so nothing is recorded.
    plan_json = '{"name": "TableScan", "estimates": [{"outputRowCount": 1.0}], "children": []}'
    for _ in range(4500):
        plan_json = f'{{"name": "Project", "estimates": [], "children": [{plan_json}]}}'
    assert max_intermediate_rows(plan_json) is None
