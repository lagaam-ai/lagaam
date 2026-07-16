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


def test_dialect_card_targets_trino(engine: TrinoEngine) -> None:
    card = engine.dialect()
    assert card.engine == "Trino"
    assert card.sqlglot_dialect == "trino"
    assert card.rules


def test_validated_sql_executes_on_trino(engine: TrinoEngine) -> None:
    # The canonicalized output (incl. injected LIMIT) must be real Trino SQL.
    import trino.dbapi

    from lagaam.core.safety import validate_query

    sql = validate_query(
        "select orderkey, totalprice from tpch.tiny.orders where orderkey < 100",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    assert "LIMIT 5" in sql
    with trino.dbapi.connect(host="localhost", port=8080, user="lagaam-test") as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    assert 0 < len(rows) <= 5


async def test_estimate_cost_quotes_a_real_scan(engine: TrinoEngine) -> None:
    est = await engine.estimate_cost(
        "SELECT orderkey FROM tpch.tiny.orders WHERE orderkey < 100 LIMIT 5"
    )
    assert est.confidence == "high"
    assert est.scanned_bytes is not None and est.scanned_bytes > 0
    assert est.row_estimate == 15000  # tpch tiny orders scan is deterministic


async def test_count_star_is_not_quoted_as_free(engine: TrinoEngine) -> None:
    # Regression: count(*) scans the whole table but Trino reports 0 bytes.
    # Must NOT come back as high-confidence 0 — that would slip past a budget.
    est = await engine.estimate_cost("SELECT count(*) FROM tpch.sf1.orders")
    assert est.confidence == "low"


async def test_self_join_is_not_quoted_as_a_single_scan(
    engine: TrinoEngine,
) -> None:
    # Two physical scans of lineitem; the IO plan bills one. Quoting it high
    # would undercount by 2x — must degrade to low.
    est = await engine.estimate_cost(
        "SELECT a.orderkey FROM tpch.sf1.lineitem a "
        "JOIN tpch.sf1.lineitem b ON a.orderkey = b.orderkey"
    )
    assert est.confidence == "low"


async def test_estimate_cost_of_join_sums_both_scans(engine: TrinoEngine) -> None:
    est = await engine.estimate_cost(
        "SELECT o.orderkey FROM tpch.tiny.orders o "
        "JOIN tpch.tiny.lineitem l ON o.orderkey = l.orderkey LIMIT 5"
    )
    assert est.confidence == "high"
    # Both tables are scanned; the quote must exceed either alone.
    assert est.scanned_bytes is not None and est.scanned_bytes > 300_000


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
