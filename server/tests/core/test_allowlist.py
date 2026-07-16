"""Table allowlists: keep an agent in its lane.

A hijacked agent must not read tables outside its grant. The check runs on the
validated SQL and fails closed — an unqualified or unparseable reference it
cannot resolve is denied, never waved through.
"""

import pytest

from lagaam.core.allowlist import check_tables_allowed
from lagaam.core.errors import TableAccessDeniedError
from lagaam.core.identity import AgentIdentity


def guard(sql: str, allowed: set[str] | None) -> None:
    identity = AgentIdentity(name="agent-1", allowed_tables=allowed)
    check_tables_allowed(sql, dialect="trino", identity=identity)


# --- what passes ---------------------------------------------------------


def test_none_allowlist_permits_everything() -> None:
    # No allowlist configured = unrestricted (single-tenant default).
    guard("SELECT x FROM tpch.tiny.orders", allowed=None)


def test_allowed_table_passes() -> None:
    guard("SELECT x FROM tpch.tiny.orders", allowed={"tpch.tiny.orders"})


def test_join_of_two_allowed_tables_passes() -> None:
    guard(
        "SELECT o.x FROM tpch.tiny.orders o JOIN tpch.tiny.lineitem l ON o.k=l.k",
        allowed={"tpch.tiny.orders", "tpch.tiny.lineitem"},
    )


def test_allowlist_match_is_case_insensitive() -> None:
    guard("SELECT x FROM TPCH.TINY.ORDERS", allowed={"tpch.tiny.orders"})


def test_cte_name_is_not_treated_as_a_table() -> None:
    # A CTE is a local alias, not a base table — it must not need a grant.
    guard(
        "WITH picked AS (SELECT x FROM tpch.tiny.orders) SELECT x FROM picked",
        allowed={"tpch.tiny.orders"},
    )


# --- what is denied ------------------------------------------------------


def test_table_outside_allowlist_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError, match="tpch.tiny.customer"):
        guard("SELECT x FROM tpch.tiny.customer", allowed={"tpch.tiny.orders"})


def test_one_disallowed_table_in_a_join_denies_the_whole_query() -> None:
    with pytest.raises(TableAccessDeniedError, match="secret"):
        guard(
            "SELECT o.x FROM tpch.tiny.orders o "
            "JOIN tpch.secret.pii p ON o.k=p.k",
            allowed={"tpch.tiny.orders"},
        )


def test_disallowed_table_in_subquery_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError):
        guard(
            "SELECT x FROM tpch.tiny.orders WHERE k IN "
            "(SELECT k FROM tpch.secret.pii)",
            allowed={"tpch.tiny.orders"},
        )


def test_empty_allowlist_denies_all_tables() -> None:
    # An explicit empty set means "no tables" — distinct from None.
    with pytest.raises(TableAccessDeniedError):
        guard("SELECT x FROM tpch.tiny.orders", allowed=set())


def test_unqualified_table_is_denied_when_an_allowlist_exists() -> None:
    # Can't prove which table `orders` resolves to, so fail closed.
    with pytest.raises(TableAccessDeniedError):
        guard("SELECT x FROM orders", allowed={"tpch.tiny.orders"})


def test_unparseable_sql_is_denied() -> None:
    with pytest.raises(TableAccessDeniedError):
        guard("not valid sql !!!", allowed={"tpch.tiny.orders"})


def test_check_runs_on_the_canonicalized_sql_that_executes() -> None:
    # Defense in depth: the allowlist must see the SAME string the engine runs,
    # so validation's canonical output — not the raw input — is what we check.
    from lagaam.core.safety import validate_query

    safe = validate_query(
        'select ssn from tpch.secret."pii"', dialect="trino", default_limit=10
    )
    with pytest.raises(TableAccessDeniedError, match="pii"):
        guard(safe, allowed={"tpch.tiny.orders"})


def test_multi_statement_smuggle_is_denied() -> None:
    # A trailing statement's table must still be caught, not silently dropped.
    with pytest.raises(TableAccessDeniedError, match="pii"):
        guard(
            "SELECT x FROM tpch.tiny.orders; SELECT ssn FROM tpch.secret.pii",
            allowed={"tpch.tiny.orders"},
        )


def test_table_function_with_empty_name_is_denied() -> None:
    # A data-reading table function surfaces as an unqualified table -> deny.
    with pytest.raises(TableAccessDeniedError):
        guard(
            "SELECT * FROM TABLE(system.something(x => 1))",
            allowed={"tpch.tiny.orders"},
        )
