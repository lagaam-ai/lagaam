"""The one transformation between validated SQL and the broker.

Names are three-part to the agent and two-part to Pinot: a third part is an
HTTP 500 on the broker's own parser, so it is stripped here, on the AST of
SQL that has already been validated and allowlisted.
"""

import pytest

from lagaam.adapters.pinot.names import two_part_sql
from lagaam.core.errors import TableNotFoundError


def test_the_synthetic_catalog_is_dropped() -> None:
    assert two_part_sql(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    ) == "SELECT Carrier FROM default.airlineStats LIMIT 5"


def test_the_catalog_match_is_case_insensitive() -> None:
    assert two_part_sql(
        "SELECT Carrier FROM PINOT.Default.airlineStats LIMIT 5"
    ) == "SELECT Carrier FROM Default.airlineStats LIMIT 5"


def test_every_table_in_a_join_is_stripped() -> None:
    out = two_part_sql(
        "SELECT a.Carrier FROM pinot.default.airlineStats AS a "
        "JOIN pinot.default.baseballStats AS b ON a.Carrier = b.playerName LIMIT 5"
    )
    assert "pinot." not in out
    assert "default.airlineStats AS a" in out
    assert "default.baseballStats AS b" in out


def test_a_cte_reference_is_left_alone() -> None:
    # After validate_query and check_tables_allowed, a bare name can only be a
    # CTE — the allowlist refuses any base table that is not three parts.
    out = two_part_sql(
        "WITH recent AS (SELECT Carrier FROM pinot.default.airlineStats LIMIT 100) "
        "SELECT Carrier FROM recent LIMIT 5"
    )
    assert "FROM default.airlineStats" in out
    assert "FROM recent" in out


def test_a_subquery_table_is_stripped_too() -> None:
    out = two_part_sql(
        "SELECT n FROM (SELECT count(*) AS n FROM pinot.default.airlineStats) AS s "
        "LIMIT 5"
    )
    assert "pinot." not in out
    assert "default.airlineStats" in out


def test_another_catalog_is_refused_before_any_request() -> None:
    with pytest.raises(TableNotFoundError):
        two_part_sql("SELECT a FROM hive.default.airlineStats LIMIT 5")


def test_a_two_part_base_table_is_refused() -> None:
    # Validated SQL is always three-part, so a name with a schema but no
    # catalog never reached the allowlist as a base table; refuse it here too.
    with pytest.raises(TableNotFoundError):
        two_part_sql("SELECT a FROM default.airlineStats LIMIT 5")


def test_unparseable_sql_is_refused_rather_than_forwarded() -> None:
    with pytest.raises(TableNotFoundError):
        two_part_sql("SELECT FROM WHERE")


def test_comments_do_not_survive_the_rewrite() -> None:
    out = two_part_sql(
        "SELECT Carrier /* a note */ FROM pinot.default.airlineStats LIMIT 5"
    )
    assert "a note" not in out
