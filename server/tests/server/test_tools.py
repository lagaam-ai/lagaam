"""MCP tool surface, exercised through a real in-memory client session.

The contract under test is what an *agent* sees: tool names, structured
content shapes, and error text it can self-correct on.
"""

from importlib.metadata import version

from lagaam.core.errors import EngineError
from lagaam.core.models import (
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    QueryResult,
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

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        raise EngineError("connection refused")


async def test_server_exposes_the_expected_tools() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        tools = (await client.list_tools()).tools
        assert sorted(t.name for t in tools) == [
            "describe_table",
            "list_catalogs",
            "query_data",
        ]
        for tool in tools:
            assert tool.description, f"{tool.name} needs an agent-facing description"


async def test_every_tool_is_annotated_read_only_with_a_title() -> None:
    # Clients read the hint to decide what needs a confirmation prompt.
    async with lagaam_client(FakeQueryEngine()) as client:
        tools = (await client.list_tools()).tools
    for tool in tools:
        assert tool.title, f"{tool.name} needs a human-readable title"
        assert tool.annotations is not None, f"{tool.name} has no annotations"
        assert tool.annotations.readOnlyHint is True, tool.name


async def test_every_tool_parameter_is_described_in_the_input_schema() -> None:
    async with lagaam_client(FakeQueryEngine()) as client:
        tools = (await client.list_tools()).tools
    params = {
        (tool.name, name): spec
        for tool in tools
        for name, spec in tool.inputSchema["properties"].items()
    }
    assert {name for _, name in params} == {"catalog", "schema", "table", "sql"}
    for (tool_name, name), spec in params.items():
        assert spec.get("description", "").strip(), f"{tool_name}.{name}"


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


async def test_the_handshake_names_lagaam_and_its_own_version() -> None:
    # Registries and clients read serverInfo; the SDK's own version is not ours.
    async with lagaam_client(FakeQueryEngine()) as client:
        info = (await client.initialize()).serverInfo
    assert (info.name, info.version) == ("lagaam", version("lagaam"))
