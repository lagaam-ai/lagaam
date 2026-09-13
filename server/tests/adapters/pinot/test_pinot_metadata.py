"""Pure controller-JSON parsers, on JSON captured from live Pinot 1.5.1.

Every parser here must survive a shape it does not recognise: an unreadable
shape is "no fact" (empty list, None), which fails safe downstream, never an
exception that costs an agent its grounding.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.metadata import (
    row_estimate,
    table_names,
    table_schema,
    table_types,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def test_table_names_are_bare_and_sorted() -> None:
    names = table_names(load("tables.json"))
    assert names[0] == "airlineStats"
    assert "baseballStats" in names
    assert len(names) == 10
    assert names == sorted(names)
    assert not any(n.endswith("_OFFLINE") for n in names)


@pytest.mark.parametrize("body", [None, {}, {"tables": "nope"}, [], "junk"])
def test_table_names_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert table_names(body) == []


def test_table_types_reads_the_offline_key() -> None:
    assert table_types(load("tableconfig-airlineStats.json")) == frozenset({"OFFLINE"})


def test_table_types_reads_a_hybrid_config() -> None:
    both = {"OFFLINE": {"tableType": "OFFLINE"}, "REALTIME": {"tableType": "REALTIME"}}
    assert table_types(both) == frozenset({"OFFLINE", "REALTIME"})


@pytest.mark.parametrize("body", [None, {}, {"NONSENSE": {}}, "junk", []])
def test_table_types_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert table_types(body) == frozenset()


def test_row_estimate_is_numrows_for_an_offline_table() -> None:
    assert row_estimate(load("metadata-airlineStats.json"), frozenset({"OFFLINE"})) == 9746
    assert row_estimate(load("metadata-baseballStats.json"), frozenset({"OFFLINE"})) == 97889


def test_a_realtime_half_makes_the_row_count_unknown_rather_than_zero() -> None:
    # Measured: a REALTIME table serving 70 rows reported numRows 0, because
    # the controller only learns a segment's size once it is sealed. Zero is a
    # lie an agent would plan against; None says we do not know.
    body = load("rt-metadata.json")
    assert body["numRows"] == 0
    assert row_estimate(body, frozenset({"REALTIME"})) is None
    assert row_estimate(body, frozenset({"OFFLINE", "REALTIME"})) is None


@pytest.mark.parametrize("body", [None, {}, {"numRows": "many"}, "junk", []])
def test_row_estimate_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert row_estimate(body, frozenset({"OFFLINE"})) is None


def test_table_schema_concatenates_all_three_field_specs() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    names = [c.name for c in card.columns]
    assert "Carrier" in names            # dimensionFieldSpecs
    assert "DaysSinceEpoch" in names     # dateTimeFieldSpecs
    assert len(names) == len(set(names))
    by_name = {c.name: c for c in card.columns}
    assert by_name["Carrier"].type == "STRING"
    assert by_name["ActualElapsedTime"].type == "INT"


def test_table_schema_echoes_the_controllers_table_spelling() -> None:
    # The controller's REST paths are case-sensitive, so a lowercased name in
    # the card is a name the agent cannot feed back — see the resolver in engine.
    card = table_schema(
        "pinot",
        "DEFAULT",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlineStats"
    assert card.row_estimate == 9746


def test_pinot_has_no_column_comments() -> None:
    card = table_schema(
        "pinot",
        "default",
        "baseballStats",
        load("schema-baseballStats.json"),
        load("metadata-baseballStats.json"),
        {"OFFLINE": {"tableType": "OFFLINE"}},
    )
    assert card.columns
    assert all(c.comment is None for c in card.columns)


def test_table_schema_survives_metadata_it_cannot_read() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        "not json at all",
        {"OFFLINE": {"tableType": "OFFLINE"}},
    )
    assert card.columns
    assert card.row_estimate is None


def test_table_schema_with_an_unreadable_schema_body_has_no_columns() -> None:
    card = table_schema("pinot", "default", "t", {"nonsense": 1}, {}, {})
    assert card.columns == []
    assert card.row_estimate is None
