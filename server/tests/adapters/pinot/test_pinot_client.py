"""PinotClient over httpx.MockTransport — no network, no container.

Every assertion here is about the request the adapter builds and the
failure it raises; the JSON parsers are tested separately.
"""

import base64
import json
from typing import Any

import httpx
import pytest

from lagaam.adapters.pinot.client import (
    PinotClient,
    PinotResponseTooLarge,
    PinotTransportError,
)


def make_client(
    handler: Any,
    user: str | None = None,
    password: str | None = None,
    max_response_bytes: int = 64 * 1024 * 1024,
) -> PinotClient:
    return PinotClient(
        controller_url="http://controller:9000",
        broker_url="http://broker:8000",
        user=user,
        password=password,
        max_response_bytes=max_response_bytes,
        transport=httpx.MockTransport(handler),
    )


async def test_controller_get_returns_parsed_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://controller:9000/tables")
        return httpx.Response(200, json={"tables": ["airlineStats"]})

    body = await make_client(handler).controller_get("/tables")
    assert body == {"tables": ["airlineStats"]}


async def test_controller_get_sends_params_and_the_database_header() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["database"] = request.headers.get("database")
        return httpx.Response(200, json=[])

    await make_client(handler).controller_get(
        "/segments/airlineStats/metadata",
        params={"columns": "Carrier"},
        database="analytics",
    )
    assert seen["url"].endswith("/segments/airlineStats/metadata?columns=Carrier")
    assert seen["database"] == "analytics"


async def test_controller_404_returns_the_not_found_sentinel() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": 404, "error": "Table not found"})

    body = await make_client(handler).controller_get("/tables/nope/schema")
    assert body is PinotClient.NotFound


async def test_controller_500_is_a_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    with pytest.raises(PinotTransportError):
        await make_client(handler).controller_get("/tables")


async def test_broker_query_posts_sql_and_options() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"resultTable": None, "exceptions": []})

    await make_client(handler).broker_query(
        "SELECT a FROM default.t LIMIT 1", "useMultistageEngine=true;timeoutMs=1000"
    )
    assert seen["url"] == "http://broker:8000/query/sql"
    assert seen["body"] == {
        "sql": "SELECT a FROM default.t LIMIT 1",
        "queryOptions": "useMultistageEngine=true;timeoutMs=1000",
    }


async def test_basic_auth_is_sent_to_both_services() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, json={})

    client = make_client(handler, user="lagaam", password="secret")
    await client.controller_get("/tables")
    await client.broker_query("SELECT 1", "")
    expected = "Basic " + base64.b64encode(b"lagaam:secret").decode()
    assert seen == [expected, expected]


async def test_no_auth_header_without_credentials() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, json={})

    await make_client(handler).controller_get("/tables")
    assert seen == [None]


async def test_a_non_json_body_is_a_transport_error() -> None:
    # Measurements section 11: a three-part name is an HTTP 500 with a
    # non-JSON body, so a parse failure must fail closed, never crash.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="Table name containing more than one '.'")

    with pytest.raises(PinotTransportError):
        await make_client(handler).broker_query("SELECT 1", "")


async def test_a_connect_failure_is_a_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(PinotTransportError):
        await make_client(handler).controller_get("/tables")


async def test_a_body_over_the_ceiling_is_refused() -> None:
    # Measured on 1.5.1: the multi-stage engine ignores maxQueryResponseSizeBytes,
    # so the ceiling only exists if the client stops reading.
    big = json.dumps({"resultTable": {"rows": [["x" * 5000]]}})

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text=big)

    with pytest.raises(PinotResponseTooLarge):
        await make_client(handler, max_response_bytes=1000).broker_query("SELECT 1", "")


async def test_a_declared_content_length_over_the_ceiling_is_refused() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            headers={"content-length": "999999999"},
            json={"resultTable": {"rows": [[1]]}},
        )

    with pytest.raises(PinotResponseTooLarge):
        await make_client(handler, max_response_bytes=100).broker_query("SELECT 1", "")


async def test_a_body_under_the_ceiling_still_parses() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"resultTable": {"rows": [[1]]}})

    body = await make_client(handler, max_response_bytes=64 * 1024).broker_query(
        "SELECT 1", ""
    )
    assert body == {"resultTable": {"rows": [[1]]}}


async def test_a_non_json_body_under_the_ceiling_is_still_a_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="Table name containing more than one '.'")

    with pytest.raises(PinotTransportError):
        await make_client(handler, max_response_bytes=64 * 1024).broker_query(
            "SELECT 1", ""
        )


@pytest.mark.parametrize(
    "part", ["a/b", "a.b", "a b", "a\tb", "", "..", "a\nb"]
)
def test_path_part_rejects_anything_that_could_reshape_the_url(part: str) -> None:
    with pytest.raises(ValueError):
        PinotClient.path_part(part)


@pytest.mark.parametrize("part", ["汉", "españa", "naïve"])
def test_path_part_rejects_a_non_ascii_name(part: str) -> None:
    # httpx encodes headers as ASCII, so a non-ASCII name raises
    # UnicodeEncodeError mid-request — neither an HTTPError nor ours.
    with pytest.raises(ValueError):
        PinotClient.path_part(part)


def test_path_part_percent_encodes_a_legal_name() -> None:
    assert PinotClient.path_part("airlineStats") == "airlineStats"
    assert PinotClient.path_part("weird$name") == "weird%24name"
