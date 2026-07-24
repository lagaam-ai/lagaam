"""Identity, allowlists, and audit at the tool surface.

Exercised through a real MCP client: an agent with a table grant can only
reach its tables, and every call it makes lands in the audit trail — allowed
or denied — with enough to reconstruct what happened.
"""

import json

from lagaam.core.audit import AuditLog
from lagaam.core.identity import AgentIdentity
from lagaam.core.models import QueryResult
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


# --- the audit boundary holds on every path ------------------------------


class CrashingEngine(FakeQueryEngine):
    """An engine that fails the way a bug does: not with a LagaamError."""

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        raise RuntimeError("connection refused: internal-host:8080 /etc/secret")


async def test_unexpected_failure_is_audited_as_an_error() -> None:
    # A bug must not become an unaudited call — that is the one moment the
    # trail matters most.
    lines: list[str] = []
    async with lagaam_client(
        CrashingEngine(),
        audit=AuditLog(sink=lines.append),
    ) as client:
        result = await client.call_tool("query_data", {"sql": "SELECT a FROM c.s.t"})
    assert result.isError
    records = [json.loads(line) for line in lines]
    assert [r["outcome"] for r in records] == ["error"]
    assert records[0]["detail"]["error"] == "RuntimeError"


async def test_unexpected_failure_text_never_reaches_the_agent() -> None:
    # The exception message can carry hostnames, paths, or credentials.
    async with lagaam_client(
        CrashingEngine(), audit=AuditLog(sink=lambda line: None)
    ) as client:
        result = await client.call_tool("query_data", {"sql": "SELECT a FROM c.s.t"})
    text = result.content[0].text
    assert "internal-host" not in text and "/etc/secret" not in text
    assert "internal error" in text


async def test_audit_records_the_sql_that_actually_ran() -> None:
    # Forensics needs what the engine executed, not what was asked for: the
    # two differ by the injected LIMIT, and by every rewrite in between.
    lines: list[str] = []
    async with lagaam_client(
        FakeQueryEngine(), audit=AuditLog(sink=lines.append)
    ) as client:
        await client.call_tool("query_data", {"sql": "select a from tpch.tiny.orders"})
    detail = json.loads(lines[0])["detail"]
    assert detail["sql"] == "select a from tpch.tiny.orders"
    assert detail["executed_sql"] == "SELECT a FROM tpch.tiny.orders LIMIT 1001"
    assert detail["estimate"]["scanned_bytes"] == 1_000_000
    assert detail["row_count"] == 1
