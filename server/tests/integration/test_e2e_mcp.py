"""End to end: MCP protocol -> Lagaam server -> Trino adapter -> real Trino."""

import pytest

from lagaam.adapters.trino.engine import TrinoEngine
from tests.helpers import lagaam_client

pytestmark = pytest.mark.integration


async def test_agent_can_ground_itself_end_to_end(trino_ready: None) -> None:
    engine = TrinoEngine(host="localhost", port=8080, user="lagaam-e2e")
    async with lagaam_client(engine) as client:
        catalogs = await client.call_tool("list_catalogs", {})
        assert not catalogs.isError
        assert catalogs.structuredContent is not None
        names = [c["name"] for c in catalogs.structuredContent["catalogs"]]
        assert "tpch" in names

        card = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "tiny", "table": "orders"},
        )
        assert not card.isError
        assert card.structuredContent is not None
        assert card.structuredContent["schema"] == "tiny"
        assert any(
            c["name"] == "orderkey" for c in card.structuredContent["columns"]
        )
