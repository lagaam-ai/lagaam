"""Dialect cards: the engine-quirks brief injected into SQL-generation prompts."""

from lagaam.core.cards import render_dialect_card
from lagaam.core.models import DialectCard
from lagaam.core.ports import QueryEngine
from tests.fakes import FakeQueryEngine


def make_card() -> DialectCard:
    return DialectCard(
        engine="Trino",
        sqlglot_dialect="trino",
        rules=[
            "Names are catalog.schema.table",
            "Quote identifiers with double quotes, never backticks",
        ],
    )


def test_card_names_engine_and_sqlglot_dialect() -> None:
    card = make_card()
    assert card.engine == "Trino"
    assert card.sqlglot_dialect == "trino"


def test_render_leads_with_engine_and_lists_each_rule_once_per_line() -> None:
    text = render_dialect_card(make_card())
    lines = text.splitlines()
    assert "Trino" in lines[0]
    assert len(lines) == 1 + 2
    assert lines[1] == "- Names are catalog.schema.table"


def test_port_contract_includes_dialect() -> None:
    engine: QueryEngine = FakeQueryEngine()
    assert isinstance(engine, QueryEngine)
    card = engine.dialect()
    assert card.sqlglot_dialect
    assert card.rules, "an empty dialect card grounds nothing"
