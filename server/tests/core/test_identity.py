"""AgentIdentity: the principal and its table grant, from env in production."""

import pytest

from lagaam.core.identity import AgentIdentity


def test_from_env_parses_a_comma_list(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAGAAM_AGENT_NAME", "analytics-bot")
    monkeypatch.setenv(
        "LAGAAM_ALLOWED_TABLES", "tpch.tiny.orders, tpch.tiny.lineitem"
    )
    identity = AgentIdentity.from_env()
    assert identity.name == "analytics-bot"
    assert identity.allowed_tables == {"tpch.tiny.orders", "tpch.tiny.lineitem"}


def test_unset_allowlist_is_none_not_empty(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("LAGAAM_ALLOWED_TABLES", raising=False)
    monkeypatch.delenv("LAGAAM_AGENT_NAME", raising=False)
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
