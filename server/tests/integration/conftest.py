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
