"""End to end: MCP protocol -> Lagaam server -> an adapter -> a real engine."""

import pytest

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.budget import (
    DEFAULT_MAX_INTERMEDIATE_ROWS,
    DEFAULT_MAX_SCAN_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    QueryBudget,
)
from lagaam.core.identity import AgentIdentity
from tests.helpers import lagaam_client

pytestmark = pytest.mark.integration

_PINOT_GRANT = AgentIdentity(
    name="lagaam-e2e", allowed_tables=frozenset({"pinot.default.airlinestats"})
)

_BASEBALL_GRANT = AgentIdentity(
    name="lagaam-e2e", allowed_tables=frozenset({"pinot.default.baseballstats"})
)


def _pinot_engine() -> PinotEngine:
    return PinotEngine(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )


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


async def test_agent_can_ground_itself_on_pinot_end_to_end(pinot_ready: None) -> None:
    async with lagaam_client(_pinot_engine(), identity=_PINOT_GRANT) as client:
        catalogs = await client.call_tool("list_catalogs", {})
        assert not catalogs.isError
        assert catalogs.structuredContent is not None
        names = [c["name"] for c in catalogs.structuredContent["catalogs"]]
        assert names == ["pinot"]

        card = await client.call_tool(
            "describe_table",
            {"catalog": "pinot", "schema": "default", "table": "airlineStats"},
        )
        assert not card.isError
        assert card.structuredContent is not None
        assert card.structuredContent["schema"] == "default"
        assert card.structuredContent["row_estimate"] == 9746
        assert any(
            c["name"] == "Carrier" for c in card.structuredContent["columns"]
        )


async def test_the_grant_hides_every_pinot_table_it_does_not_name(
    pinot_ready: None,
) -> None:
    async with lagaam_client(_pinot_engine(), identity=_PINOT_GRANT) as client:
        catalogs = await client.call_tool("list_catalogs", {})
        assert catalogs.structuredContent is not None
        tables = catalogs.structuredContent["catalogs"][0]["schemas"][0]["tables"]
        assert [t.lower() for t in tables] == ["airlinestats"]

        denied = await client.call_tool(
            "describe_table",
            {"catalog": "pinot", "schema": "default", "table": "baseballStats"},
        )
        assert denied.isError
        text = " ".join(
            block.text for block in denied.content if hasattr(block, "text")
        )
        assert "is not permitted" in text


_PINOT_TWO_TABLE_GRANT = AgentIdentity(
    name="lagaam-e2e",
    allowed_tables=frozenset(
        {"pinot.default.airlinestats", "pinot.default.baseballstats"}
    ),
)


def _default_budget() -> QueryBudget:
    return QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


async def test_a_filtered_pinot_query_clears_the_default_budget_and_runs(
    pinot_ready: None,
) -> None:
    """U11's demo: the quotation is what lets a real query through the gate."""
    async with lagaam_client(
        _pinot_engine(), budget=_default_budget(), identity=_PINOT_GRANT
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": "SELECT Carrier, count(*) AS flights "
                "FROM pinot.default.airlineStats "
                "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 "
                "GROUP BY Carrier LIMIT 5"
            },
        )
        assert not answer.isError
        assert answer.structuredContent is not None
        assert answer.structuredContent["row_count"] > 0
        assert "Carrier" in answer.structuredContent["columns"]


async def test_an_unbounded_pinot_cross_join_is_denied_on_row_work(
    pinot_ready: None,
) -> None:
    """954 million rows built at the widest step, and a LIMIT does not help."""
    async with lagaam_client(
        _pinot_engine(),
        budget=_default_budget(),
        identity=_PINOT_TWO_TABLE_GRANT,
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": "SELECT count(*) FROM pinot.default.airlineStats a, "
                "pinot.default.baseballStats b LIMIT 10"
            },
        )
        assert answer.isError
        text = " ".join(
            block.text for block in answer.content if hasattr(block, "text")
        )
        assert "rows at its widest step" in text
        assert "LIMIT will not help" in text


async def test_a_later_cte_cannot_smuggle_an_ungranted_pinot_table(
    pinot_ready: None,
) -> None:
    """A CTE declared after a reference must not vouch for that bare name.

    The grant names baseballStats only, so `airlineStats` inside the first CTE
    is the physical table the broker would resolve. No scan dimensions are set,
    so the low-confidence denial cannot fire: this reaches the allowlist, which
    fails closed on the bare name rather than letting the later CTE vouch.
    """
    budget = QueryBudget(timeout_seconds=10)
    async with lagaam_client(
        _pinot_engine(), budget=budget, identity=_BASEBALL_GRANT
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": (
                    "WITH first AS (SELECT Carrier FROM airlineStats LIMIT 1),\n"
                    "     airlineStats AS (SELECT playerName AS Carrier "
                    "FROM pinot.default.baseballStats LIMIT 1)\n"
                    "SELECT Carrier FROM first LIMIT 1"
                )
            },
        )
        assert answer.isError
        text = " ".join(
            block.text for block in answer.content if hasattr(block, "text")
        )
        # Both phrasings are TableAccessDeniedError's; neither is the budget's.
        assert "cannot be checked against your grant" in text
        assert "could not be estimated" not in text


async def test_a_cte_declared_before_its_reference_still_grounds_on_pinot(
    pinot_ready: None,
) -> None:
    """The control: the legitimate ordering of the same query must still run."""
    budget = QueryBudget(timeout_seconds=10)
    async with lagaam_client(
        _pinot_engine(), budget=budget, identity=_BASEBALL_GRANT
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": (
                    "WITH airlineStats AS (SELECT playerName AS Carrier "
                    "FROM pinot.default.baseballStats LIMIT 1),\n"
                    "     first AS (SELECT Carrier FROM airlineStats LIMIT 1)\n"
                    "SELECT Carrier FROM first LIMIT 1"
                )
            },
        )
        assert not answer.isError
        assert answer.structuredContent is not None
        assert len(answer.structuredContent["rows"]) == 1


# U12's demo, against the STREAM instance on :9001/:8001. Three assertions
# through the same MCP round-trip the shipped demos use: data that landed
# seconds ago runs, a proven key admits a self-join, and the byte-identical
# twin without that key is denied on the row work it would build.

_REALTIME_GRANT = AgentIdentity(
    name="lagaam-e2e", allowed_tables=frozenset({"pinot.default.airlinestats"})
)

_UPSERT_PAIR_GRANT = AgentIdentity(
    name="lagaam-e2e",
    allowed_tables=frozenset({"pinot.default.u12upsert", "pinot.default.u12plain"}),
)


def _pinot_realtime_engine() -> PinotEngine:
    return PinotEngine(
        controller_url="http://localhost:9001", broker_url="http://localhost:8001"
    )


def _key_evidence_budget() -> QueryBudget:
    """The default budget with only the row ceiling lowered.

    Both self-joins scan the same handful of kilobytes and finish instantly,
    so scan bytes and timeout stay at their defaults: the row ceiling is the
    only dimension the key evidence moves, and it is the only one narrowed.
    """
    return QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=10_000,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


async def test_a_query_over_freshly_landed_realtime_rows_runs(
    pinot_realtime_ready: None,
) -> None:
    """The default budget, because fresh rows need no special dispensation."""
    async with lagaam_client(
        _pinot_realtime_engine(), budget=_default_budget(), identity=_REALTIME_GRANT
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": "SELECT Carrier, count(*) AS flights "
                "FROM pinot.default.airlineStats "
                "GROUP BY Carrier LIMIT 5"
            },
        )
        assert not answer.isError
        assert answer.structuredContent is not None
        assert answer.structuredContent["row_count"] > 0
        assert "Carrier" in answer.structuredContent["columns"]


async def test_an_upsert_primary_key_admits_the_self_join(
    pinot_realtime_ready: None,
) -> None:
    """A row ceiling below the product, so only the key's bound can clear it."""
    async with lagaam_client(
        _pinot_realtime_engine(),
        budget=_key_evidence_budget(),
        identity=_UPSERT_PAIR_GRANT,
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": "SELECT a.pk FROM pinot.default.u12upsert a "
                "JOIN pinot.default.u12upsert b ON a.pk = b.pk LIMIT 10"
            },
        )
        assert not answer.isError
        assert answer.structuredContent is not None
        assert answer.structuredContent["row_count"] > 0


async def test_the_same_join_without_the_key_is_denied(
    pinot_realtime_ready: None,
) -> None:
    """The same ceiling: same rows, same topic, same shape, no upsertConfig."""
    async with lagaam_client(
        _pinot_realtime_engine(),
        budget=_key_evidence_budget(),
        identity=_UPSERT_PAIR_GRANT,
    ) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": "SELECT a.pk FROM pinot.default.u12plain a "
                "JOIN pinot.default.u12plain b ON a.pk = b.pk LIMIT 10"
            },
        )
        assert answer.isError
        text = " ".join(
            block.text for block in answer.content if hasattr(block, "text")
        )
        assert "rows at its widest step" in text
        assert "LIMIT will not help" in text
