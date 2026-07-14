"""Shared test plumbing.

The only place that touches FastMCP internals (server._mcp_server) — if the
SDK renames it, fix it here once.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator

from mcp.client.session import ClientSession
from mcp.shared.memory import (
    create_connected_server_and_client_session as client_session,
)

from lagaam.core.ports import QueryEngine
from lagaam.server import create_server


@asynccontextmanager
async def lagaam_client(engine: QueryEngine) -> AsyncIterator[ClientSession]:
    """A real MCP client connected in-memory to a Lagaam server over `engine`."""
    server = create_server(engine)
    async with client_session(server._mcp_server) as client:
        yield client
