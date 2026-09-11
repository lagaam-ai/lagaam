"""PinotEngine grounding over httpx.MockTransport, routing the real fixtures."""

import json
import os
from pathlib import Path
from typing import Any

import httpx
import pytest

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.core.errors import EngineError, TableNotFoundError

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


def make_engine(handler: Any = controller_handler) -> PinotEngine:
    return PinotEngine(
        controller_url="http://controller:9000",
        broker_url="http://broker:8000",
        transport=httpx.MockTransport(handler),
    )


def test_pinot_engine_offers_the_grounding_half_of_the_port() -> None:
    # execute() lands with the broker in task 8; until then the isinstance
    # check against the whole QueryEngine protocol cannot pass.
    engine = make_engine()
    for name in ("list_catalogs", "describe_table", "dialect", "estimate_cost"):
        assert callable(getattr(engine, name))


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


def test_from_env_reads_the_pinot_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PINOT_CONTROLLER_URL", "http://c:19000")
    monkeypatch.setenv("PINOT_BROKER_URL", "http://b:18000")
    monkeypatch.setenv("PINOT_USER", "lagaam")
    monkeypatch.setenv("PINOT_PASSWORD", "secret")
    engine = PinotEngine.from_env()
    assert engine._controller_url == "http://c:19000"
    assert engine._broker_url == "http://b:18000"


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
