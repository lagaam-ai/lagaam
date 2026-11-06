"""How TrinoEngine reaches a shared cluster: https, a login, a private CA.

A shared Trino refuses a password over plain http, so getting the scheme
wrong looks like an outage; and the password must never reach the agent.
"""

import os
from pathlib import Path
from typing import Any

import pytest
import requests
import trino.auth
import trino.dbapi
import trino.exceptions

from lagaam.adapters.trino.engine import TrinoEngine, _detail
from lagaam.core.errors import ConfigurationError, EngineError

_SECRET = "hunter2-s3cret"
_TOKEN = "eyJhbGciOiJIUzI1NiJ9.SECRET.sig"


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith("TRINO_"):
            monkeypatch.delenv(name)


def _connect_kwargs(
    monkeypatch: pytest.MonkeyPatch, engine: TrinoEngine
) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    def fake_connect(**kwargs: Any) -> None:
        captured.update(kwargs)

    monkeypatch.setattr(trino.dbapi, "connect", fake_connect)
    engine._connect()
    return captured


def test_no_auth_connects_over_http_as_before(monkeypatch: pytest.MonkeyPatch) -> None:
    kwargs = _connect_kwargs(monkeypatch, TrinoEngine.from_env())

    assert kwargs["http_scheme"] == "http"
    assert kwargs["auth"] is None
    assert kwargs["http_session"].verify is True
    assert (kwargs["host"], kwargs["port"], kwargs["user"]) == ("localhost", 8080, "lagaam")


def test_password_logs_in_as_trino_user_over_https(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRINO_USER", "analyst")
    monkeypatch.setenv("TRINO_PASSWORD", _SECRET)

    kwargs = _connect_kwargs(monkeypatch, TrinoEngine.from_env())

    assert kwargs["http_scheme"] == "https"
    assert kwargs["auth"] == trino.auth.BasicAuthentication("analyst", _SECRET)
    assert kwargs["user"] == "analyst"


def test_jwt_logs_in_with_the_token_over_https(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRINO_JWT", _TOKEN)

    kwargs = _connect_kwargs(monkeypatch, TrinoEngine.from_env())

    assert kwargs["http_scheme"] == "https"
    assert kwargs["auth"] == trino.auth.JWTAuthentication(_TOKEN)


def test_explicit_scheme_wins_over_the_auth_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A cluster that allows passwords over http behind its own TLS terminator.
    monkeypatch.setenv("TRINO_PASSWORD", _SECRET)
    monkeypatch.setenv("TRINO_HTTP_SCHEME", "HTTP")

    assert _connect_kwargs(monkeypatch, TrinoEngine.from_env())["http_scheme"] == "http"


def test_https_without_auth_is_allowed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRINO_HTTP_SCHEME", "https")

    kwargs = _connect_kwargs(monkeypatch, TrinoEngine.from_env())

    assert (kwargs["http_scheme"], kwargs["auth"]) == ("https", None)


@pytest.mark.parametrize("name", ["TRINO_PASSWORD", "TRINO_JWT", "TRINO_HTTP_SCHEME", "TRINO_VERIFY"])
def test_an_empty_var_counts_as_unset(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    # A plugin field left blank arrives as an empty string, not as no variable.
    monkeypatch.setenv(name, "")

    kwargs = _connect_kwargs(monkeypatch, TrinoEngine.from_env())

    assert (kwargs["http_scheme"], kwargs["auth"]) == ("http", None)
    assert kwargs["http_session"].verify is True


def test_password_and_jwt_together_is_a_config_error_naming_both(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRINO_PASSWORD", _SECRET)
    monkeypatch.setenv("TRINO_JWT", _TOKEN)

    with pytest.raises(ConfigurationError) as caught:
        TrinoEngine.from_env()

    message = str(caught.value)
    assert "TRINO_PASSWORD" in message and "TRINO_JWT" in message
    assert _SECRET not in message and _TOKEN not in message


def test_unknown_scheme_is_a_config_error_not_a_silent_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRINO_HTTP_SCHEME", "htps")

    with pytest.raises(ConfigurationError, match="TRINO_HTTP_SCHEME='htps'.*'http' or 'https'"):
        TrinoEngine.from_env()


@pytest.mark.parametrize(
    "value, expected", [("false", False), ("FALSE", False), ("0", False), ("true", True), ("1", True)]
)
def test_verify_reads_true_and_false(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: bool
) -> None:
    monkeypatch.setenv("TRINO_VERIFY", value)

    assert _connect_kwargs(monkeypatch, TrinoEngine.from_env())["http_session"].verify is expected


def test_verify_takes_a_ca_bundle_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    bundle = tmp_path / "ca.pem"
    bundle.write_text("-----BEGIN CERTIFICATE-----\n")
    monkeypatch.setenv("TRINO_VERIFY", str(bundle))

    assert _connect_kwargs(monkeypatch, TrinoEngine.from_env())["http_session"].verify == str(bundle)


def test_verify_that_is_neither_bool_nor_a_file_is_a_config_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A typo like "flase" must not quietly mean "verify against a missing file".
    monkeypatch.setenv("TRINO_VERIFY", "flase")

    with pytest.raises(ConfigurationError, match="TRINO_VERIFY='flase'"):
        TrinoEngine.from_env()


def test_ca_bundle_outranks_requests_ca_bundle_env(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # requests lets REQUESTS_CA_BUNDLE replace Session.verify on every request.
    corporate = tmp_path / "corporate.pem"
    corporate.write_text("x")
    ours = tmp_path / "trino-ca.pem"
    ours.write_text("x")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", str(corporate))
    monkeypatch.setenv("TRINO_VERIFY", str(ours))
    session = _connect_kwargs(monkeypatch, TrinoEngine.from_env())["http_session"]
    sent: dict[str, Any] = {}

    def fake_send(request: requests.PreparedRequest, **kwargs: Any) -> None:
        sent.update(kwargs)

    monkeypatch.setattr(session, "send", fake_send)
    session.get("https://trino.example:8443/v1/info")

    assert sent["verify"] == str(ours)


def test_the_engine_repr_does_not_show_the_password_or_token(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TRINO_PASSWORD", _SECRET)
    with_password = TrinoEngine.from_env()
    with_jwt = TrinoEngine(jwt=_TOKEN)

    for engine in (with_password, with_jwt):
        shown = repr(engine) + str(engine) + repr(vars(engine))
        assert _SECRET not in shown and _TOKEN not in shown


def test_wrong_credentials_tell_the_operator_what_to_check() -> None:
    refused = trino.exceptions.HttpError(
        f"error 401: b'Access Denied: Invalid credentials for {_SECRET}'"
    )

    detail = _detail(refused)

    assert "TRINO_PASSWORD" in detail
    assert _SECRET not in detail and "401" not in detail


async def test_a_refused_login_reaches_the_agent_without_the_password(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = TrinoEngine(password=_SECRET)

    def refused() -> None:
        raise trino.exceptions.HttpError("error 401: b'Access Denied: Invalid credentials'")

    engine._list_catalogs = refused  # type: ignore[method-assign,assignment]
    with pytest.raises(EngineError) as caught:
        await engine.list_catalogs()

    assert "refused lagaam's login" in str(caught.value)
    assert _SECRET not in str(caught.value)
