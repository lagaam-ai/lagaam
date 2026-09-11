"""PinotEngine grounding over httpx.MockTransport, routing the real fixtures."""

import base64
import json
import os
from pathlib import Path
from typing import Any

import httpx
import pytest

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
    assert card.table == "airlinestats"
    assert card.row_estimate == 9746
    assert any(c.name == "Carrier" for c in card.columns)


async def test_describe_table_accepts_any_spelling_of_the_name() -> None:
    card = await make_engine().describe_table("PINOT", "DEFAULT", "airlineStats")
    assert card.table == "airlinestats"


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
    assert seen == ["default", "default", "default"]


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


async def test_an_unmapped_failure_is_the_engines_fault_not_the_querys() -> None:
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [{"message": "who knows", "errorCode": 999}]
    with pytest.raises(EngineError):
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


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
