"""MCP tool surface, exercised through a real in-memory client session.

The contract under test is what an *agent* sees: tool names, structured
content shapes, and error text it can self-correct on.
"""

from lagaam.core.errors import EngineError
from lagaam.core.models import (
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    TableSchema,
)
from tests.fakes import FakeQueryEngine
from tests.helpers import lagaam_client


class ExplodingEngine:
    """Engine whose backend is down — every call raises a domain error."""

    async def list_catalogs(self) -> CatalogMetadata:
        raise EngineError("connection refused")

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        raise EngineError("connection refused")

    def dialect(self) -> DialectCard:
        raise EngineError("connection refused")

    async def estimate_cost(self, sql: str) -> CostEstimate:
        raise EngineError("connection refused")


async def test_server_exposes_exactly_the_u1_tools() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        tools = (await client.list_tools()).tools
        assert sorted(t.name for t in tools) == ["describe_table", "list_catalogs"]
        for tool in tools:
            assert tool.description, f"{tool.name} needs an agent-facing description"


async def test_list_catalogs_returns_structured_catalog_tree() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        result = await client.call_tool("list_catalogs", {})
        assert not result.isError
        assert result.structuredContent is not None
        catalogs = result.structuredContent["catalogs"]
        assert catalogs[0]["name"] == "tpch"
        assert catalogs[0]["schemas"][0]["tables"] == ["orders", "lineitem"]


async def test_describe_table_returns_table_schema_with_agent_facing_keys() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        result = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "tiny", "table": "orders"},
        )
        assert not result.isError
        assert result.structuredContent is not None
        card = result.structuredContent
        assert card["catalog"] == "tpch"
        assert card["schema"] == "tiny"  # agent-facing key, not schema_name
        assert card["table"] == "orders"
        assert {c["name"] for c in card["columns"]} >= {"orderkey", "orderdate"}


async def test_describe_table_unknown_table_is_teachable_error() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        result = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "tiny", "table": "nope"},
        )
        assert result.isError
        text = result.content[0].text  # type: ignore[union-attr]
        assert "tpch.tiny.nope" in text
        assert "list_catalogs" in text, "error must tell the agent how to recover"


async def test_every_tool_translates_domain_errors_not_stack_traces() -> None:
    # The boundary must cover ALL tools, not just describe_table.
    async with lagaam_client(ExplodingEngine()) as client:
        for call in (("list_catalogs", {}),
                     ("describe_table", {"catalog": "c", "schema": "s", "table": "t"})):
            result = await client.call_tool(*call)
            assert result.isError
            text = result.content[0].text  # type: ignore[union-attr]
            # SDK v1.28 adds an "Error executing tool <name>:" prefix; revisit at v2.
            assert "connection refused" in text
            assert "retry" in text, "EngineError must tell the agent what to do"
            assert "Traceback" not in text
