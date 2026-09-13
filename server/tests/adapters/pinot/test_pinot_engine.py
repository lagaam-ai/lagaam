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
from lagaam.core.errors import EngineError, QueryFailedError, TableNotFoundError
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


async def test_estimate_cost_is_honest_that_it_cannot_price_yet() -> None:
    # U11 builds the quotation from segment metadata. Until then there is no
    # number, so confidence is low and the default budget denies the query.
    estimate = await make_engine().estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    assert estimate.confidence == "low"
    assert estimate.scanned_bytes is None
    assert estimate.row_estimate is None
    assert estimate.max_intermediate_rows is None


async def test_the_interim_estimate_is_denied_by_the_default_budget() -> None:
    from lagaam.core.budget import QueryBudget, enforce_budget
    from lagaam.core.errors import BudgetExceededError

    estimate = await make_engine().estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    with pytest.raises(BudgetExceededError, match="could not be estimated"):
        enforce_budget(estimate, QueryBudget.from_env())


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
