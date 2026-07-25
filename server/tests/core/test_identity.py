"""AgentIdentity: the principal and its table grant, from env in production."""

import pytest

from lagaam.core.errors import ConfigurationError
from lagaam.core.identity import AgentIdentity


def test_from_env_parses_a_comma_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAGAAM_AGENT_NAME", "analytics-bot")
    monkeypatch.setenv(
        "LAGAAM_ALLOWED_TABLES", "tpch.tiny.orders, tpch.tiny.lineitem"
    )
    identity = AgentIdentity.from_env()
    assert identity.name == "analytics-bot"
    assert identity.allowed_tables == {"tpch.tiny.orders", "tpch.tiny.lineitem"}


def test_unset_allowlist_refuses_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A gate that opens when unconfigured is not a gate: unrestricted access
    # has to be asked for, never inherited from an empty environment.
    monkeypatch.delenv("LAGAAM_ALLOWED_TABLES", raising=False)
    monkeypatch.delenv("LAGAAM_ALLOW_ALL_TABLES", raising=False)
    with pytest.raises(ConfigurationError, match="No table grant configured"):
        AgentIdentity.from_env()


def test_explicit_opt_out_is_unrestricted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LAGAAM_ALLOWED_TABLES", raising=False)
    monkeypatch.delenv("LAGAAM_AGENT_NAME", raising=False)
    monkeypatch.setenv("LAGAAM_ALLOW_ALL_TABLES", "true")
    identity = AgentIdentity.from_env()
    assert identity.allowed_tables is None  # unrestricted, not "no tables"
    assert identity.name == "anonymous"


def test_empty_allowlist_env_means_no_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # An explicit empty value is a real (deny-all) grant, distinct from unset.
    monkeypatch.setenv("LAGAAM_ALLOWED_TABLES", "")
    identity = AgentIdentity.from_env()
    assert identity.allowed_tables == set()


def test_normalized_allowlist_is_case_folded() -> None:
    identity = AgentIdentity(name="a", allowed_tables={"TPCH.Tiny.ORDERS"})
    assert identity.normalized_allowlist() == {"tpch.tiny.orders"}


def test_a_malformed_grant_from_env_is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # pydantic only wraps ValueError subclasses, so an IdentifierError that is
    # not one escapes the validator as a traceback instead of a config error.
    monkeypatch.setenv("LAGAAM_ALLOWED_TABLES", "tiny.orders")
    with pytest.raises(ConfigurationError, match="catalog.schema.table"):
        AgentIdentity.from_env()


def test_a_non_ascii_grant_from_env_is_a_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAGAAM_ALLOWED_TABLES", "tpch.tiny.ordеrs")
    with pytest.raises(ConfigurationError, match="non-ASCII"):
        AgentIdentity.from_env()
