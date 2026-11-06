"""TrinoEngine against a Trino that wants https and a password.

Run: examples/trino-auth/setup.sh
     docker compose -f examples/docker-compose.yml --profile trino-auth up -d
Then: uv run pytest -m integration tests/integration/test_trino_auth.py
"""

import os

import pytest

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.errors import EngineError
from tests.integration.conftest import TRINO_AUTH_CA, TRINO_AUTH_LOGIN

pytestmark = pytest.mark.integration


@pytest.fixture
def auth_env(monkeypatch: pytest.MonkeyPatch, trino_auth_ready: None) -> None:
    for name in list(os.environ):
        if name.startswith("TRINO_"):
            monkeypatch.delenv(name)
    monkeypatch.setenv("TRINO_PORT", "8443")
    monkeypatch.setenv("TRINO_USER", TRINO_AUTH_LOGIN[0])
    monkeypatch.setenv("TRINO_PASSWORD", TRINO_AUTH_LOGIN[1])
    monkeypatch.setenv("TRINO_VERIFY", str(TRINO_AUTH_CA))


async def test_password_over_https_lists_and_queries(auth_env: None) -> None:
    engine = TrinoEngine.from_env()

    catalogs = await engine.list_catalogs()
    result = await engine.execute("SELECT orderkey FROM tpch.tiny.orders LIMIT 3", max_rows=10)

    assert "tpch" in {c.name for c in catalogs.catalogs}
    assert len(result.rows) == 3


async def test_wrong_password_is_refused_naming_what_to_check(
    auth_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRINO_PASSWORD", "not-the-password")

    with pytest.raises(EngineError) as caught:
        await TrinoEngine.from_env().list_catalogs()

    assert "refused lagaam's login" in str(caught.value)
    assert "not-the-password" not in str(caught.value)


async def test_an_untrusted_cert_is_not_accepted_silently(
    auth_env: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Default verification must reject the self-signed cert, not wave it through.
    monkeypatch.delenv("TRINO_VERIFY")

    with pytest.raises(EngineError, match="not reachable"):
        await TrinoEngine.from_env().list_catalogs()
