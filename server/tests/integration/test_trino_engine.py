"""Trino adapter against a real dockerized Trino (TPC-H catalog).

Run: docker compose --profile trino up -d   (from examples/)
Then: uv run pytest -m integration
"""

import pytest

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.errors import TableNotFoundError
from lagaam.core.ports import QueryEngine

pytestmark = pytest.mark.integration


@pytest.fixture
def engine(trino_ready: None) -> TrinoEngine:
    return TrinoEngine(host="localhost", port=8080, user="lagaam-test")


def test_trino_engine_satisfies_the_port(engine: TrinoEngine) -> None:
    assert isinstance(engine, QueryEngine)


async def test_describe_table_carries_row_estimate_from_stats(
    engine: TrinoEngine,
) -> None:
    schema = await engine.describe_table("tpch", "tiny", "orders")
    assert schema.row_estimate == 15000  # tpch tiny is deterministic


async def test_capped_listing_is_marked_truncated(trino_ready: None) -> None:
    capped = TrinoEngine(
        host="localhost", port=8080, user="lagaam-test", max_tables_per_catalog=5
    )
    meta = await capped.list_catalogs()
    tpch = next(c for c in meta.catalogs if c.name == "tpch")
    assert tpch.truncated
    assert sum(len(s.tables) for s in tpch.schemas) <= 5


async def test_list_catalogs_includes_tpch_tables(engine: TrinoEngine) -> None:
    meta = await engine.list_catalogs()
    tpch = next(c for c in meta.catalogs if c.name == "tpch")
    tiny = next(s for s in tpch.schemas if s.name == "tiny")
    assert {"orders", "lineitem", "customer"} <= set(tiny.tables)
    # information_schema is protocol noise, not grounding — must be filtered.
    assert all(s.name != "information_schema" for s in tpch.schemas)


async def test_describe_table_grounds_orders(engine: TrinoEngine) -> None:
    schema = await engine.describe_table("tpch", "tiny", "orders")
    assert schema.fqn == "tpch.tiny.orders"
    columns = {c.name: c.type for c in schema.columns}
    assert columns["orderkey"] == "bigint"
    assert columns["orderdate"] == "date"


async def test_describe_table_accepts_uppercase_spelling(
    engine: TrinoEngine,
) -> None:
    # Agents type freely; unquoted SQL semantics say ORDERS means orders.
    # The card echoes the canonical name so it matches list_catalogs output.
    schema = await engine.describe_table("TPCH", "Tiny", "ORDERS")
    assert schema.fqn == "tpch.tiny.orders"
    assert any(c.name == "orderkey" for c in schema.columns)


async def test_reserved_word_name_is_not_found_not_syntax_error(
    engine: TrinoEngine,
) -> None:
    # Unquoted this would be a Trino SYNTAX_ERROR (opaque to the agent).
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("tpch", "tiny", "order")


async def test_describe_missing_table_raises_domain_error(
    engine: TrinoEngine,
) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("tpch", "tiny", "no_such_table")


async def test_describe_missing_catalog_raises_domain_error(
    engine: TrinoEngine,
) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("no_such_catalog", "tiny", "orders")
