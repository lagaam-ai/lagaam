import time

import httpx
import pytest
import trino.dbapi
import trino.exceptions


@pytest.fixture
def trino_ready() -> None:
    """Skip (don't fail) integration tests when Trino isn't up and finished booting."""
    try:
        info = httpx.get("http://localhost:8080/v1/info", timeout=2.0).json()
    except httpx.HTTPError:
        pytest.skip("Trino not reachable — docker compose --profile trino up -d")
    if info.get("starting", True):
        pytest.skip("Trino is still starting — retry in a few seconds")
    # starting=false still precedes node registration ("nodes is empty"), so
    # only a query that actually runs proves the coordinator is usable — and
    # SELECT 1 answers before the connectors do, so the probe reads a catalog
    # the tests actually use.
    deadline = time.monotonic() + 30
    while True:
        try:
            with trino.dbapi.connect(
                host="localhost", port=8080, user="lagaam-test"
            ) as conn:
                conn.cursor().execute(
                    "SELECT orderkey FROM tpch.tiny.orders LIMIT 1"
                ).fetchall()
            return
        except (trino.exceptions.Error, OSError):
            if time.monotonic() > deadline:
                pytest.skip("Trino never became queryable within 30s")
            time.sleep(1)


_PINOT_TABLES = ("airlineStats", "baseballStats")


@pytest.fixture
def pinot_ready() -> None:
    """Skip (don't fail) when Pinot isn't up, or hasn't loaded what we query.

    The quickstart answers its first query at ~42s but keeps bootstrapping
    tables for ~20 minutes, so a controller that is merely healthy can still
    show a partial catalog. Waiting on the two tables the tests actually
    query is the only honest readiness signal.
    """
    try:
        httpx.get("http://localhost:9000/health", timeout=2.0).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("Pinot not reachable — docker compose --profile pinot up -d")

    deadline = time.monotonic() + 300
    while True:
        try:
            answered = all(_pinot_answers(table) for table in _PINOT_TABLES)
        except httpx.HTTPError:
            answered = False
        if answered:
            return
        if time.monotonic() > deadline:
            pytest.skip(
                "Pinot never had airlineStats and baseballStats queryable "
                "within 300s — the quickstart loads tables for ~20 minutes"
            )
        time.sleep(2)


def _pinot_answers(table: str) -> bool:
    """Is this table queryable through the broker right now?"""
    response = httpx.post(
        "http://localhost:8000/query/sql",
        json={
            "sql": f"SELECT count(*) AS n FROM {table} LIMIT 1",
            "queryOptions": "useMultistageEngine=true",
        },
        timeout=10.0,
    )
    response.raise_for_status()
    body = response.json()
    if body.get("exceptions"):
        return False
    rows = (body.get("resultTable") or {}).get("rows") or []
    # A registered-but-loading table answers [[0]] with no exception, not an absent row.
    if not rows or not isinstance(rows[0], list) or not rows[0]:
        return False
    count = rows[0][0]
    if isinstance(count, bool) or not isinstance(count, int):
        return False
    return count > 0
