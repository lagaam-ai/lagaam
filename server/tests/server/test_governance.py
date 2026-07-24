"""Identity, allowlists, and audit at the tool surface.

Exercised through a real MCP client: an agent with a table grant can only
reach its tables, and every call it makes lands in the audit trail — allowed
or denied — with enough to reconstruct what happened.
"""

import json

import anyio

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


class SlowEngine(FakeQueryEngine):
    """Interleaves calls, so a leaking audit channel would cross-contaminate."""

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        await anyio.sleep(0.02)
        self.executed.append(sql)
        return QueryResult(columns=["n"], rows=[[1]], row_count=1)


async def test_concurrent_calls_each_audit_their_own_sql() -> None:
    # The audit detail is per-call state on a shared decorator; overlapping
    # agents must never end up in each other's trail.
    lines: list[str] = []
    async with lagaam_client(
        SlowEngine(), audit=AuditLog(sink=lines.append)
    ) as client:
        async with anyio.create_task_group() as tg:
            for i in range(20):
                tg.start_soon(
                    client.call_tool,
                    "query_data",
                    {"sql": f"SELECT c{i} FROM tpch.tiny.orders"},
                )
    records = [json.loads(line) for line in lines]
    assert len(records) == 20
    for record in records:
        column = record["detail"]["sql"].split()[1]
        assert column in record["detail"]["executed_sql"]
    assert len({r["detail"]["sql"] for r in records}) == 20


async def test_a_cancelled_call_is_still_audited() -> None:
    # The SQL reached the engine; nobody is listening for the answer. The
    # trail is the only place that call still exists.
    lines: list[str] = []
    engine = SlowEngine()
    async with lagaam_client(engine, audit=AuditLog(sink=lines.append)) as client:
        with anyio.move_on_after(0.005):
            await client.call_tool(
                "query_data", {"sql": "SELECT a FROM tpch.tiny.orders"}
            )
    records = [json.loads(line) for line in lines]
    assert [r["outcome"] for r in records] == ["cancelled"]
    assert "tpch.tiny.orders" in records[0]["detail"]["executed_sql"]


async def test_describe_table_authorizes_the_parts_it_will_query() -> None:
    # Round-tripping the parts through SQL text checked a name the adapter
    # never runs: "orders -- " parses as `orders` but is quoted verbatim.
    async with lagaam_client(
        FakeQueryEngine(),
        identity=_agent({"tpch.tiny.orders"}),
        audit=AuditLog(sink=lambda line: None),
    ) as client:
        result = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "tiny", "table": "orders -- "},
        )
    assert result.isError
    assert "not permitted" in result.content[0].text


async def test_describe_table_still_works_for_a_granted_table() -> None:
    async with lagaam_client(
        FakeQueryEngine(),
        identity=_agent({"tpch.tiny.orders"}),
        audit=AuditLog(sink=lambda line: None),
    ) as client:
        result = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "tiny", "table": "orders"},
        )
    assert not result.isError


async def test_describe_table_grant_check_folds_case() -> None:
    # An agent that spells the name in caps has still named its granted table.
    from lagaam.core.allowlist import table_parts_allowed

    assert table_parts_allowed("TPCH", "Tiny", "ORDERS", {"tpch.tiny.orders"})
    assert not table_parts_allowed("tpch", "tiny", "orders -- ", {"tpch.tiny.orders"})
    assert not table_parts_allowed("tpch", "tiny", "pii", {"tpch.tiny.orders"})
