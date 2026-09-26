"""The Calcite JSON graph readers `plan.py` and `keys.py` both share."""

from lagaam.adapters.pinot.rels import children, index_rels, suffix


def test_index_rels_on_a_two_node_list_returns_by_id_previous_and_order() -> None:
    rels = [
        {"id": "0", "relOp": "PinotLogicalTableScan"},
        {"id": "1", "relOp": "LogicalProject"},
    ]
    result = index_rels(rels)
    assert result is not None
    by_id, previous, order = result
    assert set(by_id) == {"0", "1"}
    assert by_id["0"] is rels[0]
    assert by_id["1"] is rels[1]
    assert previous == {"0": None, "1": "0"}
    assert order == ["0", "1"]


def test_index_rels_of_a_repeated_id_is_none() -> None:
    rels = [
        {"id": "0", "relOp": "PinotLogicalTableScan"},
        {"id": "0", "relOp": "LogicalProject"},
    ]
    assert index_rels(rels) is None


def test_index_rels_of_a_non_string_id_is_none() -> None:
    rels = [{"id": 0, "relOp": "PinotLogicalTableScan"}]
    assert index_rels(rels) is None


def test_children_of_a_node_with_no_inputs_key_is_the_previous_node() -> None:
    rel: dict[str, object] = {"id": "1", "relOp": "LogicalProject"}
    assert children("1", rel, {"1": "0"}) == ["0"]


def test_children_of_a_node_with_no_inputs_key_and_no_predecessor_is_empty() -> None:
    rel: dict[str, object] = {"id": "0", "relOp": "PinotLogicalTableScan"}
    assert children("0", rel, {"0": None}) == []


def test_children_of_a_non_list_inputs_is_none() -> None:
    rel: dict[str, object] = {"id": "0", "relOp": "LogicalProject", "inputs": "nope"}
    assert children("0", rel, {"0": None}) is None


def test_suffix_of_a_relop_is_its_bare_lowercase_class_name() -> None:
    assert (
        suffix({"relOp": "org.apache.calcite.rel.logical.LogicalProject"})
        == "logicalproject"
    )


def test_suffix_of_a_missing_relop_is_empty() -> None:
    assert suffix({}) == ""
