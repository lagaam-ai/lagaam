"""The query_data tool: validate -> estimate -> budget -> execute -> cap.

Exercised through a real in-memory MCP client, because what matters is the
contract an agent sees: structured rows on success, and teachable error text
it can self-correct on when a query is unsafe or too expensive.
"""

import pytest

from lagaam.core.budget import QueryBudget
from lagaam.core.models import CostEstimate, QueryResult
from tests.fakes import FakeQueryEngine
from tests.helpers import lagaam_client


async def test_query_data_returns_structured_rows() -> None:
    engine = FakeQueryEngine(
        result=QueryResult(
            columns=["orderkey"], rows=[[1], [2], [3]], row_count=3
        )
    )
    async with lagaam_client(engine) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT orderkey FROM tpch.tiny.orders"}
        )
        assert not result.isError
        assert result.structuredContent is not None
        assert result.structuredContent["columns"] == ["orderkey"]
        assert result.structuredContent["row_count"] == 3


async def test_query_data_is_exposed() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        tools = {t.name for t in (await client.list_tools()).tools}
        assert "query_data" in tools


async def test_unsafe_sql_is_rejected_before_touching_the_engine() -> None:
    # A validation failure must never reach execute(); the engine records calls.
    engine = FakeQueryEngine()
    async with lagaam_client(engine) as client:
        result = await client.call_tool("query_data", {"sql": "DROP TABLE orders"})
        assert result.isError
        text = result.content[0].text  # type: ignore[union-attr]
        assert "read-only" in text
    assert engine.executed == [], "unsafe SQL must not be executed"


async def test_select_star_is_rejected_with_guidance() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT * FROM tpch.tiny.orders"}
        )
        assert result.isError
        assert "name the columns" in result.content[0].text  # type: ignore[union-attr]


async def test_over_budget_query_is_blocked_before_execution() -> None:
    engine = FakeQueryEngine(
        estimate=CostEstimate(scanned_bytes=48 * 1024**3, row_estimate=2_000_000)
    )
    budget = QueryBudget(max_scan_bytes=5 * 1024**3)
    async with lagaam_client(engine, budget=budget) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT orderkey FROM tpch.tiny.orders"}
        )
        assert result.isError
        text = result.content[0].text  # type: ignore[union-attr]
        assert "budget" in text.lower()
        assert "48" in text and "GB" in text
    assert engine.executed == [], "over-budget SQL must not be executed"


async def test_results_are_capped_and_flagged_truncated() -> None:
    engine = FakeQueryEngine(
        result=QueryResult(
            columns=["x"], rows=[[i] for i in range(5)], row_count=5, truncated=True
        )
    )
    async with lagaam_client(engine) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT x FROM t"}
        )
        assert result.structuredContent is not None
        assert result.structuredContent["truncated"] is True


async def test_the_executed_sql_is_the_validated_sql() -> None:
    # LIMIT is injected by validation; the engine must run the canonical form.
    engine = FakeQueryEngine()
    async with lagaam_client(engine) as client:
        await client.call_tool(
            "query_data", {"sql": "select orderkey from tpch.tiny.orders"}
        )
    assert len(engine.executed) == 1
    assert "LIMIT" in engine.executed[0].upper()


async def test_injected_limit_is_cap_plus_one_for_truncation_detection() -> None:
    # The executed SQL must allow one row past the cap so execute() can tell
    # "exactly cap rows exist" from "more exist, truncated".
    engine = FakeQueryEngine(
        estimate=CostEstimate(scanned_bytes=1000, row_estimate=10)
    )
    budget = QueryBudget(max_rows=50)
    async with lagaam_client(engine, budget=budget) as client:
        await client.call_tool(
            "query_data", {"sql": "SELECT orderkey FROM tpch.tiny.orders"}
        )
    assert "LIMIT 51" in engine.executed[0].upper()
