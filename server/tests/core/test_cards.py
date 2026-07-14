"""Table cards: the compact prompt-ready rendering of a table's grounding."""

from lagaam.core.cards import render_card
from lagaam.core.models import ColumnInfo, TableSchema


def make_schema(**overrides: object) -> TableSchema:
    defaults: dict[str, object] = dict(
        catalog="tpch",
        schema_name="tiny",
        table="orders",
        columns=[
            ColumnInfo(name="orderkey", type="bigint"),
            ColumnInfo(name="orderdate", type="date", comment="order date"),
        ],
        row_estimate=15000,
    )
    defaults.update(overrides)
    return TableSchema(**defaults)  # type: ignore[arg-type]


def test_card_leads_with_fqn_and_row_estimate() -> None:
    card = render_card(make_schema())
    first_line = card.splitlines()[0]
    assert "tpch.tiny.orders" in first_line
    assert "~15,000 rows" in first_line


def test_card_lists_every_column_with_type_and_comment() -> None:
    card = render_card(make_schema())
    assert "orderkey bigint" in card
    assert "orderdate date" in card
    assert "order date" in card  # comments are grounding, never dropped


def test_card_omits_row_estimate_when_unknown() -> None:
    card = render_card(make_schema(row_estimate=None))
    assert "rows" not in card
    assert "None" not in card


def test_card_is_compact_one_line_per_column_plus_header() -> None:
    # Cards go into prompts; size is a feature.
    card = render_card(make_schema())
    assert len(card.splitlines()) == 1 + 2
