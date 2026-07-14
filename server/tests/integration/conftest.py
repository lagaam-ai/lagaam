import httpx
import pytest


@pytest.fixture
def trino_ready() -> None:
    """Skip (don't fail) integration tests when Trino isn't up and finished booting."""
    try:
        info = httpx.get("http://localhost:8080/v1/info", timeout=2.0).json()
    except httpx.HTTPError:
        pytest.skip("Trino not reachable — docker compose --profile trino up -d")
    if info.get("starting", True):
        pytest.skip("Trino is still starting — retry in a few seconds")
