"""The Pinot dialect card, and what the generic sqlglot dialect does to Pinot SQL.

The card's sqlglot_dialect is the generic dialect (""), measured in the spike:
15 Pinot shapes re-rendered through validate_query executed unchanged on both
engines, and `mysql` produced identical output.
"""

import pytest

from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.core.cards import render_dialect_card
from lagaam.core.errors import SqlValidationError
from lagaam.core.safety import validate_query

_DIALECT = PINOT_DIALECT_CARD.sqlglot_dialect


def test_card_names_pinot_and_the_generic_sqlglot_dialect() -> None:
    assert PINOT_DIALECT_CARD.engine == "Pinot"
    assert PINOT_DIALECT_CARD.sqlglot_dialect == ""
    assert PINOT_DIALECT_CARD.rules


def test_card_teaches_the_three_part_name_the_agent_must_write() -> None:
    text = render_dialect_card(PINOT_DIALECT_CARD)
    assert "Pinot" in text.splitlines()[0]
    assert any("pinot.default.table" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_card_warns_that_time_filters_are_what_prune_segments() -> None:
    assert any("time column" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_card_explains_what_an_array_type_in_a_grounding_card_means() -> None:
    # describe_table renders a multi-value column as INT[] or STRING[]; the
    # card is where the agent learns that is not a scalar it can compare to.
    assert any("[]" in rule for rule in PINOT_DIALECT_CARD.rules)
    assert any("multi-value" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_measured_pinot_aggregation_survives_the_generic_dialect() -> None:
    # Measurements section 12: this exact shape re-rendered and executed on both engines.
    sql = validate_query(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier ORDER BY n DESC LIMIT 5",
        dialect=_DIALECT,
        default_limit=1000,
    )
    assert "pinot.default.airlineStats" in sql
    assert "LIMIT 5" in sql


def test_measured_pinot_time_filter_survives_the_generic_dialect() -> None:
    sql = validate_query(
        "SELECT Carrier FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073",
        dialect=_DIALECT,
        default_limit=5,
    )
    assert "BETWEEN 16071 AND 16073" in sql
    assert "LIMIT 5" in sql


def test_insert_from_file_is_rejected_under_the_generic_dialect() -> None:
    # Measurements section 5: the broker ACCEPTS this on both engines and
    # dispatches it as a Minion ingestion task. It must never reach the broker.
    with pytest.raises(SqlValidationError):
        validate_query(
            "INSERT INTO airlineStats FROM FILE 'file:///tmp/x.csv'",
            dialect=_DIALECT,
            default_limit=5,
        )
