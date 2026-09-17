"""PinotEngine grounding over httpx.MockTransport, routing the real fixtures."""

import base64
import json
import os
import time
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import anyio
import httpx
import pytest

from lagaam.adapters.pinot import engine as engine_module
from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.core.budget import (
    DEFAULT_MAX_INTERMEDIATE_ROWS,
    DEFAULT_MAX_SCAN_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    QueryBudget,
    enforce_budget,
)
from lagaam.core.errors import (
    BudgetExceededError,
    EngineError,
    QueryFailedError,
    TableNotFoundError,
)
from lagaam.core.ports import QueryEngine

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


_ROUTES: dict[str, str] = {
    "/databases": "databases.json",
    "/tables": "tables.json",
    "/tables/airlineStats/schema": "schema-airlineStats.json",
    "/tables/airlineStats/metadata": "metadata-airlineStats.json",
    "/tables/airlineStats": "tableconfig-airlineStats.json",
}


def controller_handler(request: httpx.Request) -> httpx.Response:
    name = _ROUTES.get(request.url.path)
    if name is None:
        return httpx.Response(404, json={"code": 404, "error": "not found"})
    return httpx.Response(200, json=load(name))


def make_engine(handler: Any = controller_handler, **kwargs: Any) -> PinotEngine:
    return PinotEngine(
        controller_url="http://controller:9000",
        broker_url="http://broker:8000",
        transport=httpx.MockTransport(handler),
        **kwargs,
    )


def two_database_handler(
    databases: list[str], tables: dict[str, httpx.Response]
) -> Any:
    """Serve /databases, then route /tables by the `database` header."""

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/databases":
            return httpx.Response(200, json=databases)
        if request.url.path == "/tables":
            database = request.headers.get("database", "default")
            return tables.get(
                database, httpx.Response(404, json={"code": 404, "error": "no"})
            )
        return controller_handler(request)

    return handler


def tables_response(*names: str) -> httpx.Response:
    return httpx.Response(200, json={"tables": list(names)})


class _AsyncStream(httpx.AsyncByteStream):
    """A response body httpx reads one awaited chunk at a time."""

    def __init__(self, chunks: AsyncIterator[bytes]) -> None:
        self._chunks = chunks

    async def __aiter__(self) -> AsyncIterator[bytes]:
        async for chunk in self._chunks:
            yield chunk


def test_pinot_engine_satisfies_the_port() -> None:
    assert isinstance(make_engine(), QueryEngine)


def test_the_dialect_is_the_pinot_card() -> None:
    card = make_engine().dialect()
    assert card.engine == "Pinot"
    assert card.sqlglot_dialect == ""


async def test_list_catalogs_synthesises_one_catalog_over_the_databases() -> None:
    meta = await make_engine().list_catalogs()
    assert [c.name for c in meta.catalogs] == ["pinot"]
    catalog = meta.catalogs[0]
    assert [s.name for s in catalog.schemas] == ["default"]
    assert "airlineStats" in catalog.schemas[0].tables
    assert catalog.truncated is False


async def test_the_table_listing_is_sorted() -> None:
    tables = (await make_engine().list_catalogs()).catalogs[0].schemas[0].tables
    assert tables == sorted(tables)


async def test_the_table_listing_is_capped_and_flagged() -> None:
    engine = PinotEngine(
        controller_url="http://controller:9000",
        broker_url="http://broker:8000",
        max_tables_per_catalog=3,
        transport=httpx.MockTransport(controller_handler),
    )
    catalog = (await engine.list_catalogs()).catalogs[0]
    assert len(catalog.schemas[0].tables) == 3
    assert catalog.truncated is True


async def test_the_cap_is_one_budget_across_every_database() -> None:
    handler = two_database_handler(
        ["default", "analytics"],
        {
            "default": tables_response("a1", "a2", "a3", "a4"),
            "analytics": tables_response("b1", "b2", "b3", "b4"),
        },
    )
    catalog = (
        await make_engine(handler, max_tables_per_catalog=5).list_catalogs()
    ).catalogs[0]
    assert sum(len(s.tables) for s in catalog.schemas) == 5
    assert catalog.truncated is True


async def test_a_database_that_exactly_fills_the_budget_is_not_truncated() -> None:
    handler = two_database_handler(
        ["default", "analytics"],
        {
            "default": tables_response("a1", "a2"),
            "analytics": tables_response("b1", "b2"),
        },
    )
    catalog = (
        await make_engine(handler, max_tables_per_catalog=4).list_catalogs()
    ).catalogs[0]
    assert sum(len(s.tables) for s in catalog.schemas) == 4
    assert catalog.truncated is False


async def test_one_broken_database_does_not_cost_the_healthy_one() -> None:
    handler = two_database_handler(
        ["broken", "default"],
        {
            "broken": httpx.Response(500, text="controller:9000 internal failure"),
            "default": tables_response("airlineStats"),
        },
    )
    catalog = (await make_engine(handler).list_catalogs()).catalogs[0]
    assert [s.name for s in catalog.schemas] == ["default"]
    assert catalog.schemas[0].tables == ["airlineStats"]


async def test_a_non_ascii_database_is_skipped_rather_than_crashing() -> None:
    handler = two_database_handler(
        ["default", "ventas_españa"], {"default": tables_response("airlineStats")}
    )
    catalog = (await make_engine(handler).list_catalogs()).catalogs[0]
    assert [s.name for s in catalog.schemas] == ["default"]


async def test_every_database_failing_is_an_engine_error_not_an_empty_catalog() -> None:
    handler = two_database_handler(["default", "analytics"], {})
    with pytest.raises(EngineError, match="not reachable"):
        await make_engine(handler).list_catalogs()


async def test_a_database_with_no_tables_is_an_honestly_empty_catalog() -> None:
    handler = two_database_handler(["default"], {"default": tables_response()})
    catalog = (await make_engine(handler).list_catalogs()).catalogs[0]
    assert [s.name for s in catalog.schemas] == ["default"]
    assert catalog.schemas[0].tables == []
    assert catalog.truncated is False


async def test_a_missing_databases_endpoint_falls_back_to_default() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/databases":
            return httpx.Response(404, json={"code": 404, "error": "no"})
        return controller_handler(request)

    meta = await make_engine(handler).list_catalogs()
    assert [s.name for s in meta.catalogs[0].schemas] == ["default"]


async def test_an_unreachable_controller_is_an_engine_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(EngineError):
        await make_engine(handler).list_catalogs()


async def test_an_engine_error_never_carries_a_controller_detail() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="controller:9000 internal failure")

    with pytest.raises(EngineError) as caught:
        await make_engine(handler).list_catalogs()
    assert "controller" not in str(caught.value)
    assert "9000" not in str(caught.value)


async def test_describe_table_returns_the_grounding_card() -> None:
    card = await make_engine().describe_table("pinot", "default", "airlineStats")
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlineStats"
    assert card.row_estimate == 9746
    assert any(c.name == "Carrier" for c in card.columns)


async def test_an_unreadable_config_costs_the_row_count_not_the_columns() -> None:
    # A 404 on the config degrades to no type set, and without the config
    # nothing rules out a consuming REALTIME half whose numRows reads 0.
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables/airlineStats":
            return httpx.Response(404, json={"code": 404, "error": "not found"})
        return controller_handler(request)

    card = await make_engine(handler).describe_table("pinot", "default", "airlineStats")
    assert card.row_estimate is None
    assert any(c.name == "Carrier" for c in card.columns)


async def test_describe_table_accepts_any_spelling_of_the_name() -> None:
    card = await make_engine().describe_table("PINOT", "DEFAULT", "airlineStats")
    assert card.table == "airlineStats"


@pytest.mark.parametrize("spelling", ["airlinestats", "AIRLINESTATS", "AirLineStats"])
async def test_a_case_mismatched_name_resolves_to_the_controllers_spelling(
    spelling: str,
) -> None:
    # Controller paths are case-sensitive (/tables/airlinestats/schema is a
    # 404) but broker SQL and grants are not, so the agent's spelling is only
    # a request to be resolved against the listing.
    card = await make_engine().describe_table("pinot", "default", spelling)
    assert card.table == "airlineStats"
    assert any(c.name == "Carrier" for c in card.columns)


async def test_the_resolved_spelling_is_what_reaches_the_rest_paths() -> None:
    paths: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        paths.append(request.url.path)
        return controller_handler(request)

    await make_engine(handler).describe_table("pinot", "default", "AIRLINESTATS")
    assert "/tables/AIRLINESTATS/schema" not in paths
    assert "/tables/airlineStats/schema" in paths


async def test_the_card_echoes_the_spelling_list_catalogs_advertises() -> None:
    # The round trip that matters: whatever list_catalogs names, describe_table
    # must accept and echo back unchanged.
    engine = make_engine()
    listed = (await engine.list_catalogs()).catalogs[0].schemas[0].tables
    card = await engine.describe_table("pinot", "default", "airlinestats")
    assert card.table in listed


async def test_two_spellings_that_differ_only_by_case_are_refused_as_ambiguous() -> None:
    # Nothing in the request says which was meant, so guessing would ground
    # the agent on a table it did not ask for.
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables":
            return tables_response("airlineStats", "airlinestats")
        return controller_handler(request)

    with pytest.raises(TableNotFoundError):
        await make_engine(handler).describe_table("pinot", "default", "AirlineStats")


async def test_an_exact_match_is_taken_even_beside_a_case_variant() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables":
            return tables_response("airlineStats", "airlinestats")
        return controller_handler(request)

    card = await make_engine(handler).describe_table(
        "pinot", "default", "airlineStats"
    )
    assert card.table == "airlineStats"


async def test_a_name_absent_from_the_listing_is_a_missing_table() -> None:
    called: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        called.append(request.url.path)
        return controller_handler(request)

    with pytest.raises(TableNotFoundError):
        await make_engine(handler).describe_table("pinot", "default", "nosuchtable")
    assert "/tables/nosuchtable/schema" not in called


async def test_an_unreachable_controller_during_resolution_is_an_engine_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables":
            return httpx.Response(500, text="controller:9000 internal failure")
        return controller_handler(request)

    with pytest.raises(EngineError):
        await make_engine(handler).describe_table("pinot", "default", "airlineStats")


async def test_a_catalog_that_is_not_pinot_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("hive", "default", "airlineStats")


async def test_a_controller_404_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("pinot", "default", "nosuchtable")


async def test_a_name_that_cannot_be_a_url_part_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("pinot", "default", "../secrets")


async def test_a_schema_that_cannot_be_a_url_part_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("pinot", "españa", "airlineStats")


async def test_describe_table_sends_the_database_as_the_header() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("database"))
        return controller_handler(request)

    await make_engine(handler).describe_table("pinot", "default", "airlineStats")
    # The listing that resolves the spelling, then schema, metadata and config.
    assert seen == ["default", "default", "default", "default"]


async def test_from_env_reads_the_pinot_variables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PINOT_CONTROLLER_URL", "http://c:19000")
    monkeypatch.setenv("PINOT_BROKER_URL", "http://b:18000")
    monkeypatch.setenv("PINOT_USER", "lagaam")
    monkeypatch.setenv("PINOT_PASSWORD", "secret")
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return controller_handler(request)

    engine = PinotEngine.from_env(transport=httpx.MockTransport(handler))
    assert engine._controller_url == "http://c:19000"
    assert engine._broker_url == "http://b:18000"
    await engine.list_catalogs()
    expected = "Basic " + base64.b64encode(b"lagaam:secret").decode()
    assert seen and set(seen) == {expected}


def test_from_env_defaults_to_the_quickstart_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "PINOT_CONTROLLER_URL",
        "PINOT_BROKER_URL",
        "PINOT_USER",
        "PINOT_PASSWORD",
    ):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("LAGAAM_MAX_INTERMEDIATE_ROWS", raising=False)
    engine = PinotEngine.from_env()
    assert engine._controller_url == "http://localhost:9000"
    assert engine._broker_url == "http://localhost:8000"
    assert os.environ.get("PINOT_USER") is None


def test_from_env_takes_the_intermediate_row_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The port's execute() has no such parameter, so the engine learns the
    # budget at construction and spends it on the broker's own row limits.
    monkeypatch.setenv("LAGAAM_MAX_INTERMEDIATE_ROWS", "1234")
    assert PinotEngine.from_env()._max_intermediate_rows == 1234


def broker_engine(handler: Any, max_intermediate_rows: int | None = None) -> PinotEngine:
    return make_engine(handler, max_intermediate_rows=max_intermediate_rows)


def replying(body: Any) -> Any:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=body)

    handler.seen = seen  # type: ignore[attr-defined]
    return handler


async def test_execute_sends_two_part_sql_to_the_broker() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 5",
        max_rows=10,
    )
    assert handler.seen["url"] == "http://broker:8000/query/sql"
    assert handler.seen["body"]["sql"] == (
        "SELECT Carrier, COUNT(*) AS n FROM default.airlineStats "
        "GROUP BY Carrier LIMIT 5"
    )


async def test_execute_pins_the_multistage_engine_and_the_response_cap() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
        max_rows=10,
        timeout_seconds=30.0,
    )
    options = handler.seen["body"]["queryOptions"].split(";")
    assert "useMultistageEngine=true" in options
    assert "timeoutMs=30000" in options
    assert "maxQueryResponseSizeBytes=67108864" in options


async def test_the_option_carries_the_configured_ceiling_not_a_separate_literal() -> None:
    handler = replying(load("agg-groupby.json"))
    await make_engine(handler, max_response_bytes=4096).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    options = handler.seen["body"]["queryOptions"].split(";")
    assert "maxQueryResponseSizeBytes=4096" in options


async def test_a_response_over_the_ceiling_becomes_a_teachable_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=load("agg-groupby.json"))

    with pytest.raises(QueryFailedError, match="too large to send back"):
        await make_engine(handler, max_response_bytes=64).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_a_response_under_the_ceiling_is_returned() -> None:
    result = await make_engine(
        replying(load("agg-groupby.json")), max_response_bytes=1024 * 1024
    ).execute(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 5",
        max_rows=3,
    )
    assert result.row_count == 3


def test_the_default_ceiling_is_the_specs_fixed_constant() -> None:
    handler = replying(load("agg-groupby.json"))
    assert make_engine(handler)._max_response_bytes == PinotEngine.MAX_QUERY_RESPONSE_BYTES


async def test_the_client_read_timeout_outlives_the_brokers_own_deadline() -> None:
    # Equal deadlines let httpx give up before the broker's own timeout body
    # arrives, turning a teachable EXCEEDED_TIME_LIMIT into a bare EngineError.
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["body"] = json.loads(request.content)
        seen["extensions"] = dict(request.extensions)
        return httpx.Response(200, json=load("agg-groupby.json"))

    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
        max_rows=10,
        timeout_seconds=2.0,
    )
    options = seen["body"]["queryOptions"].split(";")
    assert "timeoutMs=2000" in options
    assert seen["extensions"]["timeout"]["read"] == 7.0


async def test_a_body_that_trickles_past_the_budget_is_a_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # httpx's timeout is per operation, so a body arriving in slow chunks
    # never trips it: measured, a 0.2s budget returned a result after 6.01s.
    # The grace is shortened here so the test costs a second, not six.
    monkeypatch.setattr(engine_module, "_TIMEOUT_GRACE_SECONDS", 0.3)

    async def chunks() -> Any:
        raw = json.dumps(load("agg-groupby.json")).encode()
        size = len(raw) // 4 + 1
        for start in range(0, len(raw), size):
            # Each read is well inside the per-operation timeout; only the
            # total outlasts the budget, which is exactly the hole being shut.
            await anyio.sleep(0.25)
            yield raw[start : start + size]

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_AsyncStream(chunks()))

    started = time.monotonic()
    with pytest.raises(QueryFailedError, match="took too long"):
        await make_engine(handler).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
            max_rows=10,
            timeout_seconds=0.05,
        )
    # Failed at the 0.35s deadline, not after the ~1.0s the body would take.
    assert time.monotonic() - started < 0.8


async def test_a_body_within_the_budget_still_answers() -> None:
    # Without this, the deadline test above could pass on a broken request.
    async def chunks() -> Any:
        raw = json.dumps(load("agg-groupby.json")).encode()
        await anyio.sleep(0.05)
        yield raw

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_AsyncStream(chunks()))

    result = await make_engine(handler).execute(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 5",
        max_rows=3,
        timeout_seconds=10.0,
    )
    assert result.row_count == 3


async def test_no_budget_means_no_deadline_to_outlast() -> None:
    # timeout_seconds=None is core declining to bound the query; the adapter
    # must not invent a bound of its own.
    async def chunks() -> Any:
        raw = json.dumps(load("agg-groupby.json")).encode()
        await anyio.sleep(0.05)
        yield raw

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, stream=_AsyncStream(chunks()))

    result = await make_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    assert result.row_count == 5


async def test_a_sub_millisecond_timeout_rounds_up_rather_than_to_zero() -> None:
    # timeoutMs=0 would be no cap at all, which is the opposite of a budget.
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
        max_rows=10,
        timeout_seconds=0.0004,
    )
    assert "timeoutMs=1" in handler.seen["body"]["queryOptions"].split(";")


async def test_no_timeout_means_no_timeout_option() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    assert "timeoutMs" not in handler.seen["body"]["queryOptions"]


async def test_the_row_budget_becomes_the_engines_own_row_limits() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler, max_intermediate_rows=50_000).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    options = handler.seen["body"]["queryOptions"].split(";")
    assert "maxRowsInJoin=50000" in options
    assert "maxRowsInWindow=50000" in options


async def test_no_row_budget_omits_the_row_limits() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    options = handler.seen["body"]["queryOptions"]
    assert "maxRowsInJoin" not in options
    assert "maxRowsInWindow" not in options


async def test_the_adapter_neither_adds_nor_removes_a_limit() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats", max_rows=10
    )
    assert "LIMIT" not in handler.seen["body"]["sql"].upper()


async def test_execute_returns_capped_rows() -> None:
    result = await broker_engine(replying(load("agg-groupby.json"))).execute(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 5",
        max_rows=3,
    )
    assert result.row_count == 3
    assert result.truncated is True
    assert result.columns == ["Carrier", "n"]


async def test_a_row_limit_failure_becomes_a_teachable_error() -> None:
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [
        {"message": "Cannot build in memory hash table for join operator", "errorCode": 245}
    ]
    with pytest.raises(QueryFailedError, match="distinct values"):
        await broker_engine(replying(body)).execute(
            "SELECT a.Carrier FROM pinot.default.airlineStats AS a "
            "JOIN pinot.default.baseballStats AS b ON a.Carrier = b.playerName LIMIT 5",
            max_rows=10,
        )


async def test_a_timeout_becomes_a_teachable_error() -> None:
    with pytest.raises(QueryFailedError, match="took too long"):
        await broker_engine(replying(load("timeout1-mse.json"))).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_a_trimmed_group_by_is_refused_rather_than_returned() -> None:
    with pytest.raises(QueryFailedError):
        await broker_engine(replying(load("numgroupslimit2.json"))).execute(
            "SELECT Origin, count(*) AS n FROM pinot.default.airlineStats "
            "GROUP BY Origin LIMIT 100",
            max_rows=100,
        )


async def test_a_broker_message_never_reaches_the_agent() -> None:
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [
        {
            "message": "Serialized query response size 5190 exceeds threshold 100 "
            "for requestId 786551596000000039 from broker Broker_172.17.0.2_8000",
            "errorCode": 503,
        }
    ]
    with pytest.raises(QueryFailedError) as caught:
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )
    assert "172.17.0.2" not in str(caught.value)
    assert "786551596000000039" not in str(caught.value)


async def test_a_cancelled_query_is_an_engine_error_not_an_oversized_result() -> None:
    # Pinot reuses errorCode 503 for a cancellation; telling the agent to
    # shrink its result would send it chasing a problem it does not have.
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [
        {"message": "Cancelled while waiting for leaf results", "errorCode": 503}
    ]
    with pytest.raises(EngineError):
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_an_oversized_response_503_is_still_teachable() -> None:
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [
        {
            "message": "Serialized query response size 5190 exceeds threshold 100 "
            "for requestId 786551596000000039 from broker Broker_172.17.0.2_8000",
            "errorCode": 503,
        }
    ]
    with pytest.raises(QueryFailedError, match="too large to send back"):
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_an_unmapped_failure_is_the_engines_fault_not_the_querys() -> None:
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [{"message": "who knows", "errorCode": 999}]
    with pytest.raises(EngineError):
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


@pytest.mark.parametrize("status", [401, 403])
async def test_a_refused_query_is_the_agents_to_fix_not_an_outage(status: int) -> None:
    # An outage tells the agent to retry unchanged, which never succeeds here.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="Permission denied for table airlineStats")

    with pytest.raises(QueryFailedError, match="refused access") as caught:
        await broker_engine(handler).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )
    assert "airlineStats" not in str(caught.value)


@pytest.mark.parametrize("status", [401, 403])
async def test_refused_controller_credentials_are_ours_to_fix_not_the_agents(
    status: int,
) -> None:
    # Grounding is the server's own connection, so a refusal there is an
    # operator's problem; the agent cannot rewrite its way out of it.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="Permission denied for table airlineStats")

    with pytest.raises(EngineError, match="credentials were refused") as caught:
        await make_engine(handler).describe_table("pinot", "default", "airlineStats")
    assert "airlineStats" not in str(caught.value)


@pytest.mark.parametrize("status", [401, 403])
async def test_refused_credentials_stop_list_catalogs_too(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="Permission denied")

    with pytest.raises(EngineError, match="credentials were refused"):
        await make_engine(handler).list_catalogs()


async def test_an_unreachable_broker_is_an_engine_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(EngineError):
        await broker_engine(handler).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_another_catalog_is_refused_before_the_broker_is_called() -> None:
    called: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        called.append(str(request.url))
        return httpx.Response(200, json=load("agg-groupby.json"))

    with pytest.raises(TableNotFoundError):
        await broker_engine(handler).execute(
            "SELECT a FROM hive.default.airlineStats LIMIT 5", max_rows=10
        )
    assert called == []


def _quote_routes(request: httpx.Request) -> httpx.Response:
    """Controller and broker answers for a quotation, from captured JSON."""
    path = request.url.path
    if path == "/tables":
        # The quotation resolves the controller's own spelling first.
        return httpx.Response(200, json={"tables": ["airlineStats", "baseballStats"]})
    if path == "/query/sql":
        body = json.loads(request.content)
        sql = body["sql"]
        if "AS JSON" in sql:
            return httpx.Response(200, json=load("explain-mse-singletable.json"))
        return httpx.Response(200, json=load("explain-v1-timefilter.json"))
    if path.endswith("/size"):
        return httpx.Response(200, json=load("size-airlineStats.json"))
    if path.startswith("/segments/"):
        return httpx.Response(200, json=load("seg-metadata-airlineStats-columns.json"))
    if path == "/tables/airlineStats":
        return httpx.Response(200, json=load("tableconfig-airlineStats.json"))
    return httpx.Response(404, json={})


async def test_estimate_cost_quotes_the_surviving_segments() -> None:
    engine = PinotEngine(transport=httpx.MockTransport(_quote_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10"
    )
    assert estimate.row_estimate == 1234
    assert estimate.scanned_bytes is not None
    assert estimate.confidence == "high"
    assert estimate.max_intermediate_rows == 1234


async def test_the_pruning_oracle_asks_only_the_single_stage_engine() -> None:
    """A multi-stage EXPLAIN would return no pruning counters at all."""
    seen: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            body = json.loads(request.content)
            seen.append(body.get("queryOptions", ""))
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch > 16090 LIMIT 10"
    )
    assert any("useMultistageEngine=true" not in o for o in seen)
    assert any("useMultistageEngine=true" in o for o in seen)


async def test_the_oracle_sends_a_two_part_name() -> None:
    seen: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            seen.append(json.loads(request.content)["sql"])
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    assert seen
    assert all("pinot.default" not in sql for sql in seen)
    assert all(sql.startswith("EXPLAIN") for sql in seen)


async def test_a_join_skips_the_oracle_and_charges_every_segment() -> None:
    """Single-stage EXPLAIN refuses a join outright, so k is all."""
    asked: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/tables":
            return httpx.Response(
                200, json={"tables": ["airlineStats", "baseballStats"]}
            )
        if path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            asked.append(sql)
            if "AS JSON" in sql:
                return httpx.Response(200, json=load("explain-mse-crossjoin.json"))
            return httpx.Response(
                200,
                json={"exceptions": [{"errorCode": 150, "message": "multi-stage only"}]},
            )
        if path.endswith("/externalview"):
            return httpx.Response(200, json={"OFFLINE": None, "REALTIME": None})
        if path.endswith("/size"):
            name = path.split("/")[2]
            return httpx.Response(200, json=load(f"size-{name}.json"))
        if path.startswith("/segments/"):
            name = path.split("/")[2]
            return httpx.Response(
                200, json=load(f"seg-metadata-{name}-columns.json")
            )
        name = path.split("/")[-1]
        return httpx.Response(200, json=load(f"tableconfig-{name}.json"))

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT count(*) FROM pinot.default.airlineStats a, "
        "pinot.default.baseballStats b LIMIT 10"
    )
    assert estimate.row_estimate == 9746 + 97889
    assert estimate.max_intermediate_rows == 9746 * 97889 + 9746 + 97889
    # The oracle is asked at most once, and never for a two-table query.
    assert sum(1 for sql in asked if "AS JSON" not in sql) == 0


def _baseball_routes(request: httpx.Request) -> httpx.Response:
    """The baseballStats quotation, schema included, from captured JSON."""
    path = request.url.path
    if path == "/query/sql":
        sql = json.loads(request.content)["sql"]
        if "AS JSON" in sql:
            return httpx.Response(200, json=load("explain-mse-singletable.json"))
        return httpx.Response(200, json=load("explain-v1-nofilter.json"))
    if path == "/tables":
        return httpx.Response(200, json={"tables": ["baseballStats"]})
    if path == "/tables/baseballStats/schema":
        return httpx.Response(200, json=load("schema-baseballStats.json"))
    if path.endswith("/size"):
        return httpx.Response(200, json=load("size-baseballStats.json"))
    if path.startswith("/segments/"):
        return httpx.Response(200, json=load("seg-metadata-baseballStats-columns.json"))
    if path == "/tables/baseballStats":
        return httpx.Response(200, json=load("tableconfig-baseballStats.json"))
    return httpx.Response(404, json={})


async def test_the_controller_is_asked_for_columns_in_its_own_spelling() -> None:
    """Measured: ?columns=playerid returns nothing, because the controller's
    column filter is case-sensitive while referenced_columns lowercases."""
    asked: list[httpx.URL] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/segments/"):
            asked.append(request.url)
        return _baseball_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT league, playerID FROM pinot.default.baseballStats LIMIT 10"
    )
    assert asked
    assert sorted(asked[0].params.get_list("columns")) == ["league", "playerID"]


async def test_a_column_belonging_to_no_table_is_not_asked_for() -> None:
    """A name the schema does not carry is another table's, or a literal."""
    asked: list[httpx.URL] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/segments/"):
            asked.append(request.url)
        return _baseball_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT league FROM pinot.default.baseballStats WHERE nosuchcolumn > 1 LIMIT 10"
    )
    assert asked
    assert asked[0].params.get_list("columns") == ["league"]


async def test_a_lowercase_table_is_quoted_on_the_controllers_spelling() -> None:
    """The broker executes any casing; the controller's REST paths are
    case-sensitive, so the raw spelling 404'd and quoted low — a valid query
    denied for nothing but the shape of its name."""
    asked: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables":
            return httpx.Response(200, json={"tables": ["airlineStats"]})
        asked.append(request.url.path)
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    lowered = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlinestats LIMIT 5"
    )
    canonical = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    assert lowered.confidence == "high"
    assert lowered.row_estimate == canonical.row_estimate
    assert lowered.scanned_bytes == canonical.scanned_bytes
    assert "/tables/airlineStats" in asked
    assert not any("airlinestats" in path for path in asked)


async def test_a_quotation_lists_the_tables_once_however_many_it_reads() -> None:
    """One listing per quotation, not one per table."""
    listings = 0

    def routes(request: httpx.Request) -> httpx.Response:
        nonlocal listings
        if request.url.path == "/tables":
            listings += 1
            return httpx.Response(200, json={"tables": ["airlineStats"]})
        return _selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlinestats a "
        "JOIN pinot.default.airlinestats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert listings == 1


async def test_a_mixed_case_self_join_is_charged_the_same_as_the_canonical_spelling() -> None:
    engine = PinotEngine(transport=httpx.MockTransport(_selfjoin_routes))
    mixed = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.AIRLINESTATS b ON a.Carrier = b.Carrier LIMIT 10"
    )
    canonical = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert mixed.row_estimate == canonical.row_estimate
    assert mixed.scanned_bytes == canonical.scanned_bytes
    assert mixed.max_intermediate_rows == canonical.max_intermediate_rows


async def test_a_table_the_controller_does_not_list_is_not_found() -> None:
    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables":
            return httpx.Response(200, json={"tables": ["airlineStats"]})
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    with pytest.raises(TableNotFoundError):
        await engine.estimate_cost(
            "SELECT x FROM pinot.default.nosuchtable LIMIT 5"
        )


def _limitpruned_routes(request: httpx.Request) -> httpx.Response:
    """The oracle answering with a limit prune, whatever the statement."""
    if request.url.path == "/query/sql":
        sql = json.loads(request.content)["sql"]
        if "AS JSON" in sql:
            return httpx.Response(200, json=load("explain-mse-singletable.json"))
        return httpx.Response(200, json=load("explain-v1-limitpruned.json"))
    return _quote_routes(request)


async def test_an_offset_does_not_get_to_keep_the_limit_prune() -> None:
    """Measured: the EXPLAIN prunes 30 of 31 either way, but the OFFSET query
    then scans 9,117 docs over 29 segments — the counter is offset-blind."""
    engine = PinotEngine(transport=httpx.MockTransport(_limitpruned_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10 OFFSET 9000"
    )
    assert estimate.row_estimate == 9746


async def test_without_an_offset_the_limit_prune_is_still_the_oracle() -> None:
    engine = PinotEngine(transport=httpx.MockTransport(_limitpruned_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    assert estimate.row_estimate is not None
    assert estimate.row_estimate < 9746


async def test_a_statement_with_no_limit_does_not_get_to_keep_the_prune() -> None:
    """Ruling 8.1. The single-stage EXPLAIN plans a LIMIT-less statement under
    Pinot's implicit default LIMIT 10 and reports numSegmentsPrunedByLimit for
    it; the multi-stage engine that runs the query has no such default and
    scans everything. Measured live: this shape quoted 422 against 9,746 docs
    scanned at high confidence. The port must not depend on its caller having
    injected a bound, so with no LIMIT every segment is charged."""
    engine = PinotEngine(transport=httpx.MockTransport(_limitpruned_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats"
    )
    assert estimate.row_estimate == 9746
    bounded = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    assert bounded.row_estimate == 422


async def test_a_fetch_first_bound_keeps_the_prune() -> None:
    """validate_query leaves FETCH FIRST n ROWS ONLY spelled as a FETCH, and
    it reaches the broker that way: a real bound, so the prune stands."""
    engine = PinotEngine(transport=httpx.MockTransport(_limitpruned_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats FETCH FIRST 10 ROWS ONLY"
    )
    assert estimate.row_estimate == 422


async def test_an_unpriceable_shape_is_refused_before_any_request() -> None:
    def routes(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"no request should be made, got {request.url}")

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT x FROM UNNEST(SEQUENCE(1, 100000)) AS t(x) LIMIT 10"
    )
    assert estimate.confidence == "low"
    assert estimate.scanned_bytes is None


async def test_a_foreign_catalog_is_refused_before_any_request() -> None:
    def routes(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"no request should be made, got {request.url}")

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    with pytest.raises(TableNotFoundError):
        await engine.estimate_cost("SELECT x FROM other.default.t LIMIT 10")


async def test_a_controller_that_cannot_be_reached_quotes_low_not_an_outage() -> None:
    """A quotation nobody could build is a denial, not an engine failure."""

    def routes(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("nope")

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    assert estimate.confidence == "low"
    assert estimate.scanned_bytes is None


async def test_refused_credentials_stop_a_quotation_too() -> None:
    """A 401/403 is an operator fault, not an unpriceable query."""

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables/airlineStats":
            return httpx.Response(403, text="Permission denied for table airlineStats")
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    with pytest.raises(EngineError, match="credentials were refused"):
        await engine.estimate_cost(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
        )


async def test_a_table_name_no_path_can_carry_is_not_found_before_any_request() -> (
    None
):
    def routes(request: httpx.Request) -> httpx.Response:
        raise AssertionError(f"no request should be made, got {request.url}")

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    with pytest.raises(TableNotFoundError):
        await engine.estimate_cost(
            'SELECT x FROM pinot.default."we%ird" LIMIT 10'
        )


async def test_the_explains_carry_their_own_deadline() -> None:
    seen: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            body = json.loads(request.content)
            seen.append(body.get("queryOptions", ""))
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch > 16090 LIMIT 10"
    )
    assert seen
    assert all("timeoutMs=10000" in options for options in seen)
    single_stage = [o for o in seen if "useMultistageEngine=true" not in o]
    multi_stage = [o for o in seen if "useMultistageEngine=true" in o]
    assert single_stage
    assert multi_stage


async def test_a_broker_that_hangs_on_explain_degrades_to_low() -> None:
    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            raise httpx.ReadTimeout("slow")
        return _quote_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    # The oracle degrades to None (charge every segment) and the multi-stage
    # plan is unreadable, but table facts alone still bound rows and bytes —
    # only the join-shape signal is lost, and estimate_cost does not raise.
    assert estimate.max_intermediate_rows is None


async def test_an_explain_that_trickles_past_the_deadline_degrades_to_low(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """httpx's timeout is a read-gap timeout, so a body arriving in slow
    chunks outlasts it: measured, a 1s timeout returned after 3.51s. The
    quotation's EXPLAINs need the same total deadline execute() uses."""
    monkeypatch.setattr(engine_module, "_EXPLAIN_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(engine_module, "_TIMEOUT_GRACE_SECONDS", 0.3)

    async def chunks() -> Any:
        raw = json.dumps(load("explain-v1-nofilter.json")).encode()
        size = len(raw) // 4 + 1
        for start in range(0, len(raw), size):
            # No single gap trips the per-operation timeout; only the total.
            await anyio.sleep(0.25)
            yield raw[start : start + size]

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            return httpx.Response(200, stream=_AsyncStream(chunks()))
        return _quote_routes(request)

    started = time.monotonic()
    estimate = await make_engine(routes).estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    # Both EXPLAINs give up at their 0.5s deadline, not after ~1s of body each.
    assert time.monotonic() - started < 2.0
    # A timeout degrades exactly like a transport failure: no plan, no oracle.
    assert estimate.max_intermediate_rows is None
    assert estimate.row_estimate == 9746


def _selfjoin_routes(request: httpx.Request) -> httpx.Response:
    """One table, read twice by a self-join, with the joins-refusing oracle."""
    path = request.url.path
    if path == "/tables":
        return httpx.Response(200, json={"tables": ["airlineStats"]})
    if path == "/query/sql":
        sql = json.loads(request.content)["sql"]
        if "AS JSON" in sql:
            return httpx.Response(200, json=load("explain-mse-selfjoin.json"))
        # 1.5.1's single-stage engine refuses a join outright, so no oracle.
        return httpx.Response(
            200,
            json={"exceptions": [{"errorCode": 150, "message": "multi-stage only"}]},
        )
    if path.endswith("/size"):
        return httpx.Response(200, json=load("size-airlineStats.json"))
    if path.startswith("/segments/"):
        return httpx.Response(200, json=load("seg-metadata-airlineStats-columns.json"))
    if path == "/tables/airlineStats":
        return httpx.Response(200, json=load("tableconfig-airlineStats.json"))
    return httpx.Response(404, json={})


async def test_a_self_join_is_charged_two_reads_and_the_product() -> None:
    """Calcite folds the repeated scan into one node; the SQL still reads twice."""
    engine = PinotEngine(transport=httpx.MockTransport(_selfjoin_routes))
    self_join = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    # The single-read figure for the same column, from the same fixtures.
    single = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    assert single.scanned_bytes is not None
    assert self_join.row_estimate == 2 * 9746
    assert self_join.scanned_bytes == 2 * single.scanned_bytes
    assert self_join.max_intermediate_rows == 9746 * 9746 + 2 * 9746
    assert self_join.confidence == "high"


def _is_keycols_explain(sql: str) -> bool:
    """The key-ordinal EXPLAIN: bare projection, no filter, no LIMIT, no join."""
    if "AS JSON FOR SELECT" not in sql:
        return False
    tail = sql.split("AS JSON FOR SELECT", 1)[1].upper()
    return not any(word in tail for word in (" JOIN ", " WHERE ", " LIMIT ", "("))


def _realtime_routes(request: httpx.Request) -> httpx.Response:
    """The realtime airlineStats table, from the U12 captures."""
    path = request.url.path
    if path == "/query/sql":
        sql = json.loads(request.content)["sql"]
        if "AS JSON" in sql:
            return httpx.Response(200, json=load("explain-mse-singletable.json"))
        return httpx.Response(200, json=load("explain-v1-realtime-nofilter.json"))
    if path == "/tables":
        return httpx.Response(200, json={"tables": ["airlineStats"]})
    if path == "/tables/airlineStats/externalview":
        return httpx.Response(200, json=load("externalview-airlineStats-realtime.json"))
    if path == "/tables/airlineStats/size":
        return httpx.Response(200, json=load("size-airlineStats-realtime.json"))
    if path == "/tables/airlineStats/schema":
        return httpx.Response(200, json=load("schema-airlineStats.json"))
    if path == "/segments/airlineStats/metadata":
        return httpx.Response(
            200, json=load("seg-metadata-airlineStats-realtime-columns.json")
        )
    if path == "/tables/airlineStats":
        return httpx.Response(200, json=load("tableconfig-airlineStats-realtime.json"))
    return httpx.Response(404, json={})


async def test_a_realtime_table_is_quoted_with_its_consuming_segment_charged() -> None:
    """The no-filter EXPLAIN prunes to 1 queried segment, of which 1 is the
    consuming one, so the sealed k is 0 and _k charges one sealed segment of
    100 docs plus the 100-row consuming charge (ruling 3.1)."""
    engine = PinotEngine(transport=httpx.MockTransport(_realtime_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats LIMIT 10"
    )
    assert estimate.row_estimate == 100 + 100
    assert estimate.scanned_bytes == 114 + 114
    assert estimate.confidence == "high"


async def test_a_realtime_statement_with_no_limit_is_charged_in_full() -> None:
    """The same ruling on the realtime shape: with no LIMIT the prune is not
    read, so 26 queried less 1 consuming leaves every sealed segment charged,
    exactly as the OFFSET case below."""
    engine = PinotEngine(transport=httpx.MockTransport(_realtime_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats"
    )
    assert estimate.row_estimate == 6 * 100 + 100
    assert estimate.scanned_bytes == 548 + 114
    assert estimate.confidence == "high"


async def test_the_whole_realtime_table_is_charged_when_no_prune_is_believed() -> None:
    """An OFFSET voids the limit prune: 26 queried less the 1 consuming leaves
    a sealed k of 25, which is every sealed segment the metadata carries."""
    engine = PinotEngine(transport=httpx.MockTransport(_realtime_routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats "
        "LIMIT 10 OFFSET 20"
    )
    assert estimate.row_estimate == 6 * 100 + 100
    assert estimate.scanned_bytes == 548 + 114
    assert estimate.confidence == "high"


async def test_the_sealed_k_is_queried_minus_the_consuming_counter() -> None:
    """The future-time EXPLAIN reports 1 queried, 1 consuming: k is 0 sealed,
    so one sealed segment at 100 docs plus the 100-row consuming charge."""

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if "AS JSON" in sql:
                return httpx.Response(200, json=load("explain-mse-singletable.json"))
            return httpx.Response(200, json=load("explain-v1-realtime-futuretime.json"))
        return _realtime_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch > 99999 LIMIT 10"
    )
    assert estimate.row_estimate == 100 + 100
    assert estimate.confidence == "high"


async def test_incomplete_segment_metadata_quotes_low() -> None:
    """The two-of-four u12upsert capture: a sum over half a table is no quote."""

    def routes(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/query/sql":
            return httpx.Response(200, json=load("explain-v1-realtime-nofilter.json"))
        if path == "/tables":
            return httpx.Response(200, json={"tables": ["u12upsert"]})
        if path == "/tables/u12upsert/externalview":
            return httpx.Response(200, json={"REALTIME": {}})
        if path == "/tables/u12upsert/size":
            return httpx.Response(200, json=load("size-u12upsert.json"))
        if path == "/tables/u12upsert/metadata":
            return httpx.Response(200, json=load("metadata-u12upsert.json"))
        if path == "/schemas/u12upsert":
            return httpx.Response(200, json=load("schema-u12upsert.json"))
        if path == "/tables/u12upsert/schema":
            return httpx.Response(200, json=load("schema-u12upsert.json"))
        if path == "/segments/u12upsert/metadata":
            return httpx.Response(200, json=load("seg-metadata-u12upsert.json"))
        if path == "/tables/u12upsert":
            return httpx.Response(200, json=load("tableconfig-u12upsert.json"))
        return httpx.Response(404, json={})

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost("SELECT pk FROM pinot.default.u12upsert LIMIT 10")
    assert estimate.row_estimate is None
    assert estimate.scanned_bytes is None
    assert estimate.confidence == "low"


async def test_the_schema_is_fetched_only_for_an_upsert_table() -> None:
    """A non-upsert table pays one extra call, not three."""
    seen: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return _realtime_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost("SELECT Carrier FROM pinot.default.airlineStats LIMIT 10")
    assert "/tables/airlineStats/externalview" in seen
    assert not any(path.startswith("/schemas/") for path in seen)
    assert "/tables/airlineStats/metadata" not in seen


async def test_the_schema_is_fetched_once_for_a_proven_non_upsert_key() -> None:
    """A single-sealed-segment table proves a key via notNull, not upsert
    config, so schema_json is None through table_facts and _record_keycols
    would re-fetch /tables/{t}/schema — but _table_facts already fetched it
    once, for the referenced-columns filter, and must not fetch it twice."""
    seg_metadata = load("seg-metadata-baseballStats-columns.json")
    (segment,) = seg_metadata.values()
    for column in segment["columns"]:
        if column["columnName"] == "playerID":
            column["cardinality"] = segment["totalDocs"]
            column["fieldSpec"]["notNull"] = True

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/segments/baseballStats/metadata":
            return httpx.Response(200, json=seg_metadata)
        return _baseball_routes(request)

    seen: list[str] = []

    def counting(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        return routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(counting))
    estimate = await engine.estimate_cost(
        "SELECT playerID FROM pinot.default.baseballStats LIMIT 10"
    )
    assert estimate.confidence == "high"
    assert seen.count("/tables/baseballStats/schema") == 1


async def test_an_upsert_table_pays_for_its_schema_and_its_metadata() -> None:
    """The two extra documents are fetched exactly where the config says
    they say something, and the schema path is the listing spelling — the
    config's own tableName is u12upsert_REALTIME and would 404."""
    seen: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        path = request.url.path
        if path == "/query/sql":
            return httpx.Response(200, json=load("explain-v1-realtime-nofilter.json"))
        if path == "/tables":
            return httpx.Response(200, json={"tables": ["u12upsert"]})
        if path == "/tables/u12upsert":
            return httpx.Response(200, json=load("tableconfig-u12upsert.json"))
        if path == "/schemas/u12upsert":
            return httpx.Response(200, json=load("schema-u12upsert.json"))
        if path == "/tables/u12upsert/metadata":
            return httpx.Response(200, json=load("metadata-u12upsert.json"))
        return httpx.Response(404, json={})

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost("SELECT pk FROM pinot.default.u12upsert LIMIT 10")
    assert "/schemas/u12upsert" in seen
    assert "/tables/u12upsert/metadata" in seen
    assert not any("u12upsert_REALTIME" in path for path in seen)


def _upsert_selfjoin_routes(request: httpx.Request) -> httpx.Response:
    """airlineStats' own captures, with the config and schema of an upsert
    table keyed on Carrier: the one shape that proves a join bound here."""
    path = request.url.path
    if path == "/query/sql":
        sql = json.loads(request.content)["sql"]
        if _is_keycols_explain(sql):
            return httpx.Response(200, json=load("explain-mse-keycols-airlineStats.json"))
        if "AS JSON" in sql:
            return httpx.Response(200, json=load("explain-mse-selfjoin.json"))
        return httpx.Response(
            200,
            json={"exceptions": [{"errorCode": 150, "message": "multi-stage only"}]},
        )
    if path == "/tables":
        return httpx.Response(200, json={"tables": ["airlineStats"]})
    if path == "/tables/airlineStats/externalview":
        return httpx.Response(200, json={"OFFLINE": None, "REALTIME": None})
    if path == "/tables/airlineStats/size":
        return httpx.Response(200, json=load("size-airlineStats.json"))
    if path in ("/tables/airlineStats/schema", "/schemas/airlineStats"):
        schema = load("schema-airlineStats.json")
        schema["primaryKeyColumns"] = ["Carrier"]
        return httpx.Response(200, json=schema)
    if path == "/tables/airlineStats/metadata":
        metadata = load("metadata-airlineStats.json")
        metadata["upsertPartitionToServerPrimaryKeyCountMap"] = {"0": {"Server_0": 50}}
        return httpx.Response(200, json=metadata)
    if path == "/segments/airlineStats/metadata":
        return httpx.Response(200, json=load("seg-metadata-airlineStats-columns.json"))
    if path == "/tables/airlineStats":
        config = load("tableconfig-airlineStats.json")
        config["OFFLINE"]["upsertConfig"] = {"mode": "FULL"}
        return httpx.Response(200, json=config)
    return httpx.Response(404, json={})


async def test_an_upsert_self_join_is_bounded_once_the_ordinals_are_learned() -> None:
    """Carrier is the proven key and the keycols EXPLAIN puts it at ordinal 18,
    which both operands compose down to: min plus the two inputs, not the product."""
    engine = PinotEngine(transport=httpx.MockTransport(_upsert_selfjoin_routes))
    estimate = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert estimate.max_intermediate_rows == 9746 + 9746 + 9746


async def test_a_key_whose_ordinals_never_arrived_is_charged_the_product() -> None:
    """A keycols EXPLAIN that errors leaves the table with no evidence at all."""

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if _is_keycols_explain(sql):
                return httpx.Response(
                    200,
                    json={"exceptions": [{"errorCode": 200, "message": "no"}]},
                )
        return _upsert_selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert estimate.max_intermediate_rows == 9746 * 9746 + 2 * 9746


async def test_the_keycols_explain_carries_no_limit_and_no_order_by() -> None:
    """A Sort above the project would make key_ordinals return None."""
    asked: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if _is_keycols_explain(sql):
                asked.append(sql)
        return _upsert_selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert asked
    for sql in asked:
        assert "LIMIT" not in sql.upper()
        assert "ORDER BY" not in sql.upper()
        assert sql.startswith("EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT")
        # The schema's spelling, never the agent's, and the two-part name.
        assert "Carrier FROM default.airlineStats" in sql


async def test_a_table_without_keys_is_never_asked_for_its_ordinals() -> None:
    """The extra EXPLAIN is paid for only where a key was actually proven."""
    asked: list[str] = []

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if _is_keycols_explain(sql):
                asked.append(sql)
        return _realtime_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    await engine.estimate_cost("SELECT Carrier FROM pinot.default.airlineStats LIMIT 10")
    assert asked == []


async def test_a_realtime_table_nobody_can_bound_is_quoted_low_and_denied() -> None:
    """A consuming segment with no flush threshold bounds nothing, and the
    gate denies the query rather than letting an unquotable scan through."""

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables/airlineStats":
            config = load("tableconfig-airlineStats-realtime.json")
            maps = config["REALTIME"]["ingestionConfig"]["streamIngestionConfig"][
                "streamConfigMaps"
            ]
            for stream in maps:
                for key in list(stream):
                    if key.startswith("realtime.segment.flush.threshold"):
                        del stream[key]
            return httpx.Response(200, json=config)
        return _realtime_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    )
    assert estimate.confidence == "low"
    with pytest.raises(BudgetExceededError, match="could not be estimated"):
        enforce_budget(
            estimate,
            QueryBudget(
                max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
                max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
                timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
            ),
        )


async def test_the_externalview_is_what_tells_consuming_from_online() -> None:
    """The size report names no missing segment here, so the consuming charge
    exists only because the externalview reports one CONSUMING replica."""

    def routes(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/tables/airlineStats/size":
            size = load("size-airlineStats-realtime.json")
            segments = size["realtimeSegments"]
            segments["missingSegments"] = 0
            del segments["segments"]["airlineStats__0__6__20260917T1214Z"]
            return httpx.Response(200, json=size)
        if path == "/segments/airlineStats/metadata":
            seg = load("seg-metadata-airlineStats-realtime-columns.json")
            del seg["airlineStats__0__6__20260917T1214Z"]
            return httpx.Response(200, json=seg)
        return _realtime_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats LIMIT 10"
    )
    # One sealed segment (k is 0) at 100 docs, plus the 100-row consuming charge.
    assert estimate.row_estimate == 100 + 100


async def test_a_key_column_that_is_not_a_bare_name_forfeits_the_evidence() -> None:
    """The key columns are interpolated into the ordinals EXPLAIN, so a name
    carrying a comma or a comment marker costs this table its evidence rather
    than reaching the broker as a second clause."""
    asked: list[str] = []
    injected = "Carrier, 1 FROM t --"

    def routes(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if _is_keycols_explain(sql):
                asked.append(sql)
        if path in ("/tables/airlineStats/schema", "/schemas/airlineStats"):
            schema = load("schema-airlineStats.json")
            # The schema both names the column and calls it the primary key.
            schema["primaryKeyColumns"] = [injected]
            schema["dimensionFieldSpecs"].append(
                {"name": injected, "dataType": "STRING"}
            )
            return httpx.Response(200, json=schema)
        return _upsert_selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert asked == []
    assert estimate.max_intermediate_rows == 9746 * 9746 + 2 * 9746


async def test_a_quoted_database_name_forfeits_the_keycols_evidence() -> None:
    """The database name is interpolated into the ordinals EXPLAIN exactly as
    the key columns are: a listing spelling carrying a comma and a comment
    marker must not reach the broker unquoted, so the table forfeits its
    evidence instead and the self-join is charged the product."""
    asked: list[str] = []
    injected_db = "d, 1 FROM x --"
    selfjoin_plan = json.loads(
        json.dumps(load("explain-mse-selfjoin.json")).replace("default", injected_db)
    )

    def routes(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if _is_keycols_explain(sql):
                asked.append(sql)
            elif "AS JSON" in sql:
                return httpx.Response(200, json=selfjoin_plan)
        return _upsert_selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        f'SELECT a.Carrier FROM pinot."{injected_db}".airlineStats a '
        f'JOIN pinot."{injected_db}".airlineStats b ON a.Carrier = b.Carrier LIMIT 10'
    )
    assert asked == []
    assert estimate.max_intermediate_rows == 9746 * 9746 + 2 * 9746


async def test_a_listing_spelling_with_a_comma_forfeits_the_keycols_evidence() -> None:
    """Same guard, from the table side: the controller's own listing spelling
    of the table is what reaches the ordinals EXPLAIN. No space, so the
    spelling still clears path_part and reaches _record_keycols rather than
    being refused earlier as an unusable REST path segment; the agent names
    the table with this exact spelling so _spelled resolves it without help.
    The shape-plan fixture is respelled to match, so the only thing under
    test is whether the keycols EXPLAIN is issued with this table name."""
    asked: list[str] = []
    injected_table = "airlineStats,1FROMx--"
    encoded_table = engine_module.PinotClient.path_part(injected_table)
    selfjoin_plan = json.loads(
        json.dumps(load("explain-mse-selfjoin.json")).replace(
            "airlineStats", injected_table
        )
    )

    def routes(request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/tables":
            return httpx.Response(200, json={"tables": [injected_table]})
        if path == "/query/sql":
            sql = json.loads(request.content)["sql"]
            if _is_keycols_explain(sql):
                asked.append(sql)
            elif "AS JSON" in sql:
                return httpx.Response(200, json=selfjoin_plan)
        if injected_table in path:
            rewritten = httpx.Request(
                request.method,
                str(request.url).replace(encoded_table, "airlineStats"),
                headers=request.headers,
                content=request.content,
            )
            return _upsert_selfjoin_routes(rewritten)
        return _upsert_selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        f'SELECT a.Carrier FROM pinot.default."{injected_table}" a '
        f'JOIN pinot.default."{injected_table}" b ON a.Carrier = b.Carrier LIMIT 10'
    )
    assert asked == []
    assert estimate.max_intermediate_rows == 9746 * 9746 + 2 * 9746


async def test_the_honest_spelling_still_learns_ordinals() -> None:
    """The new database/table guard must not reject an ordinary spelling."""
    engine = PinotEngine(transport=httpx.MockTransport(_upsert_selfjoin_routes))
    estimate = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert estimate.max_intermediate_rows == 9746 + 9746 + 9746


def _notnull_selfjoin_routes(request: httpx.Request) -> httpx.Response:
    """baseballStats with no upsert config at all: one sealed segment, null
    handling off, and a playerID the schema route says cannot be null. The
    only document that can establish that nullability is /tables/{t}/schema,
    which _table_facts fetches for the ?columns= filter."""
    path = request.url.path
    if path == "/query/sql":
        sql = json.loads(request.content)["sql"]
        if _is_keycols_explain(sql):
            return httpx.Response(
                200, json=load("explain-mse-keycols-baseballStats.json")
            )
        if "AS JSON" in sql:
            return httpx.Response(
                200, json=load("explain-mse-selfjoin-baseballStats.json")
            )
        return httpx.Response(
            200,
            json={"exceptions": [{"errorCode": 150, "message": "multi-stage only"}]},
        )
    if path == "/tables":
        return httpx.Response(200, json={"tables": ["baseballStats"]})
    if path == "/tables/baseballStats/externalview":
        return httpx.Response(200, json={"OFFLINE": None, "REALTIME": None})
    if path == "/tables/baseballStats/size":
        return httpx.Response(200, json=load("size-baseballStats.json"))
    if path == "/tables/baseballStats/schema":
        return httpx.Response(200, json=load("schema-baseballStats.json"))
    if path == "/segments/baseballStats/metadata":
        seg_metadata = load("seg-metadata-baseballStats-columns.json")
        (segment,) = seg_metadata.values()
        for column in segment["columns"]:
            if column["columnName"] == "playerID":
                column["cardinality"] = segment["totalDocs"]
                column["fieldSpec"]["notNull"] = True
        return httpx.Response(200, json=seg_metadata)
    if path == "/tables/baseballStats":
        return httpx.Response(200, json=load("tableconfig-baseballStats.json"))
    return httpx.Response(404, json={})


async def test_a_non_upsert_self_join_is_bounded_on_a_schema_proven_key_f1() -> None:
    """F1: a single-sealed-segment table with no upsertConfig proves its key
    through source (b), whose nullability gate reads the /tables/{t}/schema
    document the columns filter already fetched. playerID is at scan ordinal
    17 — the measured capture — which both operands compose down to: min plus
    the two inputs, not the product."""
    engine = PinotEngine(transport=httpx.MockTransport(_notnull_selfjoin_routes))
    estimate = await engine.estimate_cost(
        "SELECT a.playerID FROM pinot.default.baseballStats a "
        "JOIN pinot.default.baseballStats b ON a.playerID = b.playerID LIMIT 10"
    )
    assert estimate.max_intermediate_rows == 97889 + 97889 + 97889


async def test_a_schema_the_controller_will_not_serve_charges_the_product_f1() -> None:
    """F1: the same table with /tables/{t}/schema answering 404. Nothing
    establishes nullability, so source (b) yields no key and the self-join is
    charged the product a twin always costs."""

    def routes(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/tables/baseballStats/schema":
            return httpx.Response(404, json={"code": 404, "error": "not found"})
        return _notnull_selfjoin_routes(request)

    engine = PinotEngine(transport=httpx.MockTransport(routes))
    estimate = await engine.estimate_cost(
        "SELECT a.playerID FROM pinot.default.baseballStats a "
        "JOIN pinot.default.baseballStats b ON a.playerID = b.playerID LIMIT 10"
    )
    assert estimate.max_intermediate_rows == 97889 * 97889 + 2 * 97889
