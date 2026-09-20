"""The Pinot dialect card, and the sqlglot dialect that keeps the agent's spellings.

Every statement is re-rendered by sqlglot on the way to the broker. Under the
generic dialect that re-render rewrote fourteen valid Pinot shapes into
spellings Pinot rejects — or, for SUBSTR, into one it runs with different
semantics — and rejected a fifteenth outright. The `pinot` dialect preserves
what the agent wrote; the expected renderings below are the "After" table of
docs/superpowers/specs/2026-09-20-pinot-dialect-measurements.md, each one
measured against Pinot 1.5.1 on both engines.
"""

import pytest
import sqlglot
from sqlglot import exp
from sqlglot.dialects.dialect import Dialect

import lagaam.adapters.pinot  # noqa: F401  (registers the "pinot" dialect)
from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.adapters.pinot.names import referenced_columns, two_part_sql
from lagaam.core.cards import render_dialect_card
from lagaam.core.errors import SqlValidationError
from lagaam.core.safety import validate_query

_DIALECT = PINOT_DIALECT_CARD.sqlglot_dialect
_TABLE = "pinot.default.airlineStats"


def test_card_names_pinot_and_its_own_sqlglot_dialect() -> None:
    assert PINOT_DIALECT_CARD.engine == "Pinot"
    assert PINOT_DIALECT_CARD.sqlglot_dialect == "pinot"
    assert PINOT_DIALECT_CARD.rules


def test_the_dialect_is_registered_under_the_name_the_card_gives() -> None:
    assert Dialect.get_or_raise(_DIALECT) is not None


def test_card_teaches_the_three_part_name_the_agent_must_write() -> None:
    text = render_dialect_card(PINOT_DIALECT_CARD)
    assert "Pinot" in text.splitlines()[0]
    assert any("pinot.default.table" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_card_warns_that_time_filters_are_what_prune_segments() -> None:
    assert any("time column" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_card_explains_what_an_array_type_in_a_grounding_card_means() -> None:
    # describe_table renders a multi-value column as INT[] or STRING[]; the
    # card is where the agent learns that is not a scalar it can compare to.
    assert any("[]" in rule for rule in PINOT_DIALECT_CARD.rules)
    assert any("multi-value" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_card_names_the_cast_types_pinot_has() -> None:
    assert any(
        "CAST" in rule and "BIG_DECIMAL" in rule for rule in PINOT_DIALECT_CARD.rules
    )


# The fifteen shapes the generic dialect broke, with the rendering measured to
# run on Pinot 1.5.1 (measurements file, "After" table, column 2).
_PRESERVED = [
    ("CAST STRING", f"SELECT CAST(ArrDelay AS STRING) AS y FROM {_TABLE} LIMIT 1",
     "CAST(ArrDelay AS STRING) AS y"),
    ("JSON_EXTRACT_SCALAR",
     f"SELECT JSON_EXTRACT_SCALAR(Carrier, '$.a', 'STRING', 'x') AS y FROM {_TABLE} LIMIT 1",
     "JSON_EXTRACT_SCALAR(Carrier, '$.a', 'STRING', 'x') AS y"),
    ("SUBSTR 3", f"SELECT SUBSTR(Carrier, 0, 1) AS y FROM {_TABLE} LIMIT 1",
     "SUBSTR(Carrier, 0, 1) AS y"),
    ("SUBSTR 2", f"SELECT SUBSTR(Carrier, 1) AS y FROM {_TABLE} LIMIT 1",
     "SUBSTR(Carrier, 1) AS y"),
    ("STRPOS", f"SELECT STRPOS(Carrier, 'A') AS y FROM {_TABLE} LIMIT 1",
     "STRPOS(Carrier, 'A') AS y"),
    ("LOG10", f"SELECT LOG10(100) AS y FROM {_TABLE} LIMIT 1", "LOG10(100) AS y"),
    ("LOG2", f"SELECT LOG2(8) AS y FROM {_TABLE} LIMIT 1", "LOG2(8) AS y"),
    ("TRUNCATE 2", f"SELECT TRUNCATE(1.234, 2) AS y FROM {_TABLE} LIMIT 1",
     "TRUNCATE(1.234, 2) AS y"),
    ("TRUNCATE 1", f"SELECT TRUNCATE(1.234) AS y FROM {_TABLE} LIMIT 1",
     "TRUNCATE(1.234) AS y"),
    ("VAR_POP", f"SELECT VAR_POP(ArrDelay) AS y FROM {_TABLE}", "VAR_POP(ArrDelay) AS y"),
    ("VAR_SAMP", f"SELECT VAR_SAMP(ArrDelay) AS y FROM {_TABLE}",
     "VAR_SAMP(ArrDelay) AS y"),
    ("BOOL_AND", f"SELECT BOOL_AND(ArrDelay > -1000) AS y FROM {_TABLE}",
     "BOOL_AND(ArrDelay > -1000) AS y"),
    ("BOOL_OR", f"SELECT BOOL_OR(ArrDelay > 0) AS y FROM {_TABLE}",
     "BOOL_OR(ArrDelay > 0) AS y"),
    ("ARRAY literal", f"SELECT ARRAY['a','b'] AS y FROM {_TABLE} LIMIT 1",
     "ARRAY['a', 'b'] AS y"),
    ("ARRAY_AGG 2", f"SELECT ARRAY_AGG(Carrier, 'STRING') AS y FROM {_TABLE}",
     "ARRAY_AGG(Carrier, 'STRING') AS y"),
    ("ARRAY_AGG 3", f"SELECT ARRAY_AGG(Carrier, 'STRING', true) AS y FROM {_TABLE}",
     "ARRAY_AGG(Carrier, 'STRING', TRUE) AS y"),
]


@pytest.mark.parametrize(
    ("shape", "sql", "expected"),
    _PRESERVED,
    ids=[name for name, _, _ in _PRESERVED],
)
def test_the_spelling_the_agent_wrote_is_the_spelling_pinot_receives(
    shape: str, sql: str, expected: str
) -> None:
    assert expected in two_part_sql(validate_query(sql, _DIALECT, default_limit=5))


# Renames the generic dialect makes that Pinot accepts — measured `same` or
# `fixes`, so the dialect leaves them to sqlglot rather than preserving them.
_ACCEPTED_RENAMES = [
    ("IFNULL", f"SELECT IFNULL(ArrDelay, 0) AS y FROM {_TABLE} LIMIT 1",
     "COALESCE(ArrDelay, 0) AS y"),
    ("IF", f"SELECT IF(ArrDelay > 0, 'late', 'ok') AS y FROM {_TABLE} LIMIT 1",
     "CASE WHEN ArrDelay > 0 THEN 'late' ELSE 'ok' END AS y"),
    ("POW", f"SELECT POW(2, 3) AS y FROM {_TABLE} LIMIT 1", "POWER(2, 3) AS y"),
    ("DAYOFWEEK", f"SELECT DAYOFWEEK(CAST(DaysSinceEpoch AS TIMESTAMP)) AS y FROM {_TABLE} LIMIT 1",
     "DAY_OF_WEEK(CAST(DaysSinceEpoch AS TIMESTAMP)) AS y"),
]


@pytest.mark.parametrize(
    ("shape", "sql", "expected"),
    _ACCEPTED_RENAMES,
    ids=[name for name, _, _ in _ACCEPTED_RENAMES],
)
def test_the_renames_pinot_accepts_are_left_to_sqlglot(
    shape: str, sql: str, expected: str
) -> None:
    assert expected in two_part_sql(validate_query(sql, _DIALECT, default_limit=5))


def test_aggregates_stay_typed_so_the_scan_gate_can_see_them() -> None:
    # core/scans.py reads "this select collapses to one row" off find_all(AggFunc);
    # an Anonymous aggregate is invisible there and over-quotes.
    tree = sqlglot.parse_one(
        "SELECT ARRAY_AGG(c, 'STRING', TRUE) AS a, VAR_POP(x) AS v, "
        "BOOL_AND(x > 1) AS b FROM t",
        dialect=_DIALECT,
    )
    assert len(list(tree.find_all(exp.AggFunc))) == 3


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT JSON_EXTRACT_SCALAR(c) AS y FROM t",
        "SELECT JSON_EXTRACT_SCALAR(c, '$.a') AS y FROM t",
        "SELECT JSON_EXTRACT_SCALAR(c, '$.a', 'STRING') AS y FROM t",
        "SELECT JSON_EXTRACT_SCALAR(c, '$.a', 'STRING', 'x') AS y FROM t",
    ],
    ids=["arity1", "arity2", "arity3", "arity4"],
)
def test_a_kept_scalar_is_anonymous_with_every_argument_intact(sql: str) -> None:
    tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    call = tree.find(exp.Anonymous)
    assert call is not None
    assert call.name.upper() == "JSON_EXTRACT_SCALAR"
    assert len(call.expressions) == sql.count(",") + 1
    assert tree.sql(dialect=_DIALECT) == sql


@pytest.mark.parametrize(
    "call",
    ["ARRAY_AGG()", "arrayagg()", "ARRAYAGG( )", "ARRAY_AGG(/* nothing */)"],
)
def test_an_array_agg_with_no_argument_is_a_validation_error_not_a_crash(
    call: str,
) -> None:
    # Astra review of PR #37: the builder indexed args[0] before sqlglot checked
    # the required argument, so the agent was told "internal error, retry".
    with pytest.raises(SqlValidationError, match="could not be parsed as pinot"):
        validate_query(f"SELECT {call} FROM {_TABLE}", _DIALECT, default_limit=5)


def test_select_star_is_still_rejected() -> None:
    with pytest.raises(SqlValidationError):
        validate_query(f"SELECT * FROM {_TABLE}", _DIALECT, default_limit=5)


def test_insert_from_file_is_still_rejected() -> None:
    # Measurements section 5: the broker ACCEPTS this on both engines and
    # dispatches it as a Minion ingestion task. It must never reach the broker.
    with pytest.raises(SqlValidationError):
        validate_query(
            "INSERT INTO airlineStats FROM FILE 'file:///tmp/x.csv'",
            _DIALECT,
            default_limit=5,
        )


def test_a_missing_limit_is_still_injected() -> None:
    assert "LIMIT 5" in validate_query(
        f"SELECT Carrier FROM {_TABLE}", _DIALECT, default_limit=5
    )


def test_count_star_still_names_no_column() -> None:
    # exp.Count stays typed, so count(*) is not read as a projected star.
    assert referenced_columns(f"SELECT count(*) FROM {_TABLE}") is not None


def test_the_parse_error_names_the_dialect() -> None:
    with pytest.raises(SqlValidationError, match="as pinot"):
        validate_query("SELECT FROM WHERE", _DIALECT, default_limit=5)


def test_measured_pinot_aggregation_survives_the_dialect() -> None:
    sql = validate_query(
        f"SELECT Carrier, count(*) AS n FROM {_TABLE} "
        "GROUP BY Carrier ORDER BY n DESC LIMIT 5",
        dialect=_DIALECT,
        default_limit=1000,
    )
    assert "pinot.default.airlineStats" in sql
    assert "LIMIT 5" in sql


def test_measured_pinot_time_filter_survives_the_dialect() -> None:
    sql = validate_query(
        f"SELECT Carrier FROM {_TABLE} WHERE DaysSinceEpoch BETWEEN 16071 AND 16073",
        dialect=_DIALECT,
        default_limit=5,
    )
    assert "BETWEEN 16071 AND 16073" in sql
    assert "LIMIT 5" in sql
