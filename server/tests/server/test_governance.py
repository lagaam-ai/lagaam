"""Identity, allowlists, and audit at the tool surface.

Exercised through a real MCP client: an agent with a table grant can only
reach its tables, and every call it makes lands in the audit trail — allowed
or denied — with enough to reconstruct what happened.
"""

import json

from lagaam.core.audit import AuditLog
from lagaam.core.identity import AgentIdentity
from tests.fakes import FakeQueryEngine
from tests.helpers import lagaam_client


def _agent(allowed: set[str] | None) -> AgentIdentity:
    return AgentIdentity(name="agent-7", allowed_tables=allowed)


async def test_query_to_allowed_table_succeeds() -> None:
    identity = _agent({"tpch.tiny.orders"})
    async with lagaam_client(FakeQueryEngine(), identity=identity) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT orderkey FROM tpch.tiny.orders"}
        )
        assert not result.isError


async def test_query_to_disallowed_table_is_blocked() -> None:
    identity = _agent({"tpch.tiny.orders"})
    engine = FakeQueryEngine()
    async with lagaam_client(engine, identity=identity) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT ssn FROM tpch.secret.pii"}
        )
        assert result.isError
        assert "not permitted" in result.content[0].text  # type: ignore[union-attr]
    assert engine.executed == [], "denied query must never execute"


async def test_describe_disallowed_table_is_blocked() -> None:
    identity = _agent({"tpch.tiny.orders"})
    async with lagaam_client(FakeQueryEngine(), identity=identity) as client:
        result = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "secret", "table": "pii"},
        )
        assert result.isError
        assert "not permitted" in result.content[0].text  # type: ignore[union-attr]


async def test_every_call_is_audited_with_identity_and_outcome() -> None:
    lines: list[str] = []
    audit = AuditLog(sink=lines.append, clock=lambda: 0.0)
    identity = _agent({"tpch.tiny.orders"})
    async with lagaam_client(
        FakeQueryEngine(), identity=identity, audit=audit
    ) as client:
        await client.call_tool(
            "query_data", {"sql": "SELECT orderkey FROM tpch.tiny.orders"}
        )
        await client.call_tool(
            "query_data", {"sql": "SELECT ssn FROM tpch.secret.pii"}
        )

    events = [json.loads(line) for line in lines]
    assert all(e["identity"] == "agent-7" for e in events)
    outcomes = [e["outcome"] for e in events]
    assert "allowed" in outcomes and "denied" in outcomes
    denied = next(e for e in events if e["outcome"] == "denied")
    assert "reason" in denied["detail"]


async def test_no_allowlist_leaves_the_agent_unrestricted() -> None:
    # Backwards compatible: an agent without a grant reaches any table.
    async with lagaam_client(
        FakeQueryEngine(), identity=_agent(None)
    ) as client:
        result = await client.call_tool(
            "query_data", {"sql": "SELECT ssn FROM anything.at.all"}
        )
        assert not result.isError


async def test_list_catalogs_shows_only_the_grant() -> None:
    # The fake exposes tpch.tiny.{orders,lineitem}; the grant covers one.
    identity = _agent({"tpch.tiny.orders"})
    async with lagaam_client(FakeQueryEngine(), identity=identity) as client:
        result = await client.call_tool("list_catalogs", {})
        assert not result.isError
        listing = result.content[0].text  # type: ignore[union-attr]
        assert "orders" in listing
        assert "lineitem" not in listing


async def test_list_catalogs_unrestricted_shows_everything() -> None:
    async with lagaam_client(FakeQueryEngine(), identity=_agent(None)) as client:
        result = await client.call_tool("list_catalogs", {})
        listing = result.content[0].text  # type: ignore[union-attr]
        assert "orders" in listing and "lineitem" in listing
