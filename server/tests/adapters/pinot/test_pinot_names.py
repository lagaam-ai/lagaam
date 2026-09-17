"""The one transformation between validated SQL and the broker.

Names are three-part to the agent and two-part to Pinot: a third part is an
HTTP 500 on the broker's own parser, so it is stripped here, on the AST of
SQL that has already been validated and allowlisted.
"""

import pytest

from lagaam.adapters.pinot.names import (
    referenced_columns,
    referenced_tables,
    two_part_sql,
)
from lagaam.core.errors import SqlValidationError, TableNotFoundError


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


def test_the_wrong_catalog_error_names_the_parts_as_written() -> None:
    # Exact string, so a malformed message (e.g. leading-dot placeholders)
    # can never come back unnoticed.
    with pytest.raises(TableNotFoundError) as exc_info:
        two_part_sql("SELECT a FROM other.default.airlineStats LIMIT 5")
    assert str(exc_info.value) == "Table other.default.airlineStats does not exist."


def test_a_two_part_base_table_is_refused_with_a_recovery_hint() -> None:
    # Reachable only under LAGAAM_ALLOW_ALL_TABLES, where check_tables_allowed
    # returns early; the agent needs to be told the required shape, not just
    # that the table is missing.
    with pytest.raises(SqlValidationError) as exc_info:
        two_part_sql("SELECT a FROM default.airlineStats LIMIT 5")
    message = str(exc_info.value)
    assert "pinot.<database>.<table>" in message
    assert "default.airlineStats" in message


def test_unparseable_sql_is_refused_rather_than_forwarded() -> None:
    with pytest.raises(SqlValidationError) as exc_info:
        two_part_sql("SELECT FROM WHERE")
    assert "cannot re-read" in str(exc_info.value)


def test_comments_do_not_survive_the_rewrite() -> None:
    out = two_part_sql(
        "SELECT Carrier /* a note */ FROM pinot.default.airlineStats LIMIT 5"
    )
    assert "a note" not in out


def test_a_four_part_qualified_column_is_stripped_along_with_the_table() -> None:
    out = two_part_sql(
        "SELECT pinot.default.airlineStats.Carrier "
        "FROM pinot.default.airlineStats LIMIT 5"
    )
    assert out == "SELECT default.airlineStats.Carrier FROM default.airlineStats LIMIT 5"
    assert "pinot." not in out


def test_a_column_qualified_with_another_catalog_is_refused() -> None:
    with pytest.raises(TableNotFoundError):
        two_part_sql(
            "SELECT hive.default.airlineStats.Carrier "
            "FROM pinot.default.airlineStats LIMIT 5"
        )


ACCEPTED_INPUTS = [
    "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
    "SELECT Carrier FROM PINOT.Default.airlineStats LIMIT 5",
    "SELECT a.Carrier FROM pinot.default.airlineStats AS a "
    "JOIN pinot.default.baseballStats AS b ON a.Carrier = b.playerName LIMIT 5",
    "WITH recent AS (SELECT Carrier FROM pinot.default.airlineStats LIMIT 100) "
    "SELECT Carrier FROM recent LIMIT 5",
    "SELECT n FROM (SELECT count(*) AS n FROM pinot.default.airlineStats) AS s "
    "LIMIT 5",
    "SELECT Carrier /* a note */ FROM pinot.default.airlineStats LIMIT 5",
    "SELECT pinot.default.airlineStats.Carrier "
    "FROM pinot.default.airlineStats LIMIT 5",
]


def test_every_accepted_input_loses_the_synthetic_catalog_everywhere() -> None:
    for sql in ACCEPTED_INPUTS:
        out = two_part_sql(sql)
        assert "pinot." not in out.lower(), sql


def test_referenced_tables_are_database_table_pairs() -> None:
    assert referenced_tables(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
    ) == [("default", "airlineStats")]


def test_referenced_tables_deduplicate_and_sort() -> None:
    assert referenced_tables(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.baseballStats b ON a.Carrier = b.teamID "
        "JOIN pinot.default.airlineStats c ON a.Carrier = c.Carrier LIMIT 10"
    ) == [("default", "airlineStats"), ("default", "baseballStats")]


def test_referenced_tables_fold_case_into_one_entry() -> None:
    assert referenced_tables(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.AIRLINESTATS b ON a.Carrier = b.Carrier LIMIT 1"
    ) == [("default", "airlineStats")]
    assert referenced_tables(
        "SELECT a.x FROM pinot.Default.t a JOIN pinot.default.T b ON a.x = b.x LIMIT 1"
    ) == [("Default", "t")]


def test_referenced_tables_refuse_a_foreign_catalog() -> None:
    with pytest.raises(TableNotFoundError):
        referenced_tables("SELECT x FROM other.default.t LIMIT 1")


def test_referenced_tables_are_none_when_the_sql_cannot_be_read() -> None:
    assert referenced_tables("SELECT FROM WHERE ((((") is None


def test_referenced_columns_are_lowercase_bare_names() -> None:
    assert referenced_columns(
        "SELECT a.Carrier, DaysSinceEpoch FROM pinot.default.airlineStats a "
        "WHERE a.Origin = 'SFO' LIMIT 10"
    ) == frozenset({"carrier", "dayssinceepoch", "origin"})


def test_a_star_makes_the_columns_unresolvable() -> None:
    """validate_query rejects SELECT *, but count(*) and a.* still parse."""
    assert referenced_columns("SELECT * FROM pinot.default.airlineStats LIMIT 1") is None
    assert referenced_columns(
        "SELECT a.* FROM pinot.default.airlineStats a LIMIT 1"
    ) is None


def test_a_count_star_is_not_an_unresolvable_column() -> None:
    assert referenced_columns(
        "SELECT count(*) FROM pinot.default.airlineStats LIMIT 1"
    ) == frozenset()


def test_referenced_columns_are_none_when_the_sql_cannot_be_read() -> None:
    assert referenced_columns("SELECT FROM WHERE ((((") is None
