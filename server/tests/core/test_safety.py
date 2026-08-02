"""SQL safety validation: parse, judge, constrain.

Every rejection message must tell the agent what to change — these texts
are part of the API, like the domain errors.
"""

import time

import pytest
import sqlglot

from lagaam.core.errors import SqlValidationError
from lagaam.core.safety import validate_query


def validate(sql: str) -> str:
    return validate_query(sql, dialect="trino")


# --- what passes ---------------------------------------------------------


def test_plain_select_with_limit_passes() -> None:
    sql = validate("SELECT orderkey FROM tpch.tiny.orders LIMIT 10")
    assert "orderkey" in sql.lower()
    assert "limit" in sql.lower()


def test_cte_and_union_pass() -> None:
    validate(
        "WITH big AS (SELECT orderkey FROM tpch.tiny.orders WHERE totalprice > 100) "
        "SELECT orderkey FROM big LIMIT 5"
    )
    validate(
        "SELECT orderkey FROM tpch.tiny.orders "
        "UNION ALL SELECT orderkey FROM tpch.tiny.lineitem LIMIT 5"
    )


def test_count_star_is_allowed() -> None:
    # The star lives inside a function, not as a projection.
    validate("SELECT count(*) FROM tpch.tiny.orders LIMIT 1")


def test_missing_limit_is_injected_not_rejected() -> None:
    sql = validate("SELECT orderkey FROM tpch.tiny.orders")
    assert "LIMIT 1000" in sql


def test_existing_limit_is_kept() -> None:
    sql = validate("SELECT orderkey FROM tpch.tiny.orders LIMIT 7")
    assert "LIMIT 7" in sql
    assert "1000" not in sql


def test_output_is_canonicalized_trino_sql() -> None:
    # What was validated is exactly what runs — the AST is re-rendered.
    sql = validate("select orderkey from tpch.tiny.orders limit 3")
    assert sql == 'SELECT orderkey FROM tpch.tiny.orders LIMIT 3'


# --- what is rejected, and how it teaches --------------------------------


def test_select_star_rejected_with_guidance() -> None:
    with pytest.raises(SqlValidationError, match="name the columns"):
        validate("SELECT * FROM tpch.tiny.orders LIMIT 5")


def test_qualified_star_rejected() -> None:
    with pytest.raises(SqlValidationError, match="name the columns"):
        validate("SELECT o.* FROM tpch.tiny.orders o LIMIT 5")


def test_star_in_subquery_rejected() -> None:
    with pytest.raises(SqlValidationError, match="name the columns"):
        validate(
            "SELECT orderkey FROM (SELECT * FROM tpch.tiny.orders) LIMIT 5"
        )


def test_deeply_qualified_star_rejected() -> None:
    # 4+ part names parent the star under exp.Dot, not Column.
    with pytest.raises(SqlValidationError, match="name the columns"):
        validate("SELECT a.b.c.d.* FROM t LIMIT 5")


def test_fetch_first_counts_as_a_limit() -> None:
    sql = validate("SELECT orderkey FROM tpch.tiny.orders FETCH FIRST 3 ROWS ONLY")
    assert "1000" not in sql


@pytest.mark.parametrize(
    "sql",
    [
        "INSERT INTO t VALUES (1)",
        "UPDATE t SET x = 1",
        "DELETE FROM t",
        "DROP TABLE t",
        "CREATE TABLE t (x int)",
        "ALTER TABLE t ADD COLUMN y int",
    ],
)
def test_writes_and_ddl_rejected(sql: str) -> None:
    with pytest.raises(SqlValidationError, match="read-only"):
        validate(sql)


def test_write_smuggled_into_cte_rejected() -> None:
    # find() must scan the whole tree, not just the root.
    with pytest.raises(SqlValidationError, match="read-only"):
        validate("WITH x AS (SELECT 1) INSERT INTO t SELECT * FROM x")


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT x FROM TABLE(system.query(query => 'DROP TABLE t'))",
        "SELECT x FROM mysql.system.query(query => 'DELETE FROM t')",
        "SELECT x FROM TABLE(exclude_columns(input => TABLE(tpch.tiny.orders), "
        "columns => DESCRIPTOR(orderkey)))",
    ],
)
def test_table_functions_rejected(sql: str) -> None:
    # Passthrough functions like system.query run their string argument on
    # the remote engine — a read-only bypass if ever allowed through.
    with pytest.raises(SqlValidationError, match="table functions"):
        validate(sql)


def test_unnest_is_not_a_table_function() -> None:
    validate("SELECT u FROM UNNEST(ARRAY[1, 2]) AS t(u) LIMIT 5")


@pytest.mark.parametrize("sql", ["SHOW CATALOGS", "SET SESSION x = 1"])
def test_loosely_parsed_commands_rejected(sql: str) -> None:
    # exp.Command is sqlglot's "parsed loosely" bucket — never trust it.
    with pytest.raises(SqlValidationError, match="read-only"):
        validate(sql)


def test_multiple_statements_rejected() -> None:
    with pytest.raises(SqlValidationError, match="one statement"):
        validate("SELECT 1; DROP TABLE t")


def test_unparseable_sql_fails_closed_with_position() -> None:
    with pytest.raises(SqlValidationError, match="could not be parsed"):
        validate("SELEC orderkey FRM orders")


def test_empty_input_rejected() -> None:
    with pytest.raises(SqlValidationError):
        validate("")


def test_tokenizer_error_fails_closed() -> None:
    # An unterminated literal raises TokenError, not ParseError — still ours.
    with pytest.raises(SqlValidationError, match="could not be parsed"):
        validate("'''")


def test_comments_are_stripped_from_the_executed_sql() -> None:
    # Comments carry nothing the engine needs, and kilobytes of them are how
    # an agent pushes the real query out of a truncated audit line.
    padded = "SELECT /* " + "x" * 5000 + " */ a FROM c.s.t WHERE k = 1"
    safe = validate_query(padded, dialect="trino", default_limit=10)
    assert "xxxx" not in safe
    assert safe == "SELECT a FROM c.s.t WHERE k = 1 LIMIT 10"


def test_grouping_sets_survive_the_parser_gap() -> None:
    # sqlglot 30.12/30.13 cannot read a LIMIT directly after GROUP BY
    # ROLLUP/CUBE/GROUPING SETS, though Trino runs it. Denying core BI SQL
    # over a parser bug is a gate failure, not strictness.
    for group in (
        "ROLLUP (a)",
        "CUBE (a, b)",
        "GROUPING SETS ((a), ())",
    ):
        for tail in ("", " LIMIT 10"):
            sql = f"SELECT a, count(*) FROM c.s.t GROUP BY {group}{tail}"
            safe = validate_query(sql, dialect="trino", default_limit=1001)
            # Every downstream gate re-parses this; it must read back.
            assert sqlglot.parse_one(safe, dialect="trino") is not None, sql


def test_a_user_limit_on_a_grouping_set_is_honoured() -> None:
    safe = validate_query(
        "SELECT a, count(*) FROM c.s.t GROUP BY ROLLUP (a) LIMIT 7",
        dialect="trino",
        default_limit=1001,
    )
    assert "LIMIT 7" in safe
    assert "1001" not in safe


def test_the_parser_workaround_does_not_excuse_bad_sql() -> None:
    # The fallback fires only on that exact tail, and only after the SQL has
    # already failed to parse — it is not a second chance for anything else.
    for sql in (
        "SELECT FROM WHERE GROUP BY ROLLUP (a) LIMIT 5",
        "DELETE FROM c.s.t",
        "SELECT * FROM c.s.t GROUP BY ROLLUP (a) LIMIT 5",
    ):
        with pytest.raises(SqlValidationError):
            validate_query(sql, dialect="trino", default_limit=1001)


def test_oversized_sql_is_refused_before_parsing() -> None:
    # sqlglot's parse is superlinear in nesting depth — 3 MB of nested
    # subqueries cost 5s and 12 MB cost 25s, before any check of ours runs.
    # The refusal has to come first, or the gate is the unbounded work.
    inner = "(VALUES (1))"
    for _ in range(14):
        inner = f"(SELECT 1 AS x FROM {inner} AS a CROSS JOIN {inner} AS b)"
    started = time.monotonic()
    with pytest.raises(SqlValidationError, match="characters"):
        validate_query(f"SELECT z.x FROM {inner} AS z", dialect="trino")
    assert time.monotonic() - started < 2.0


def test_a_long_but_ordinary_query_is_not_refused() -> None:
    # A big IN list is how a BI tool passes a filter set; Trino accepts far
    # more than this ceiling, so it must not catch real work.
    values = ",".join(str(i) for i in range(5000))
    safe = validate_query(
        f"SELECT o.k FROM c.s.orders o WHERE o.k IN ({values})",
        dialect="trino",
        default_limit=1001,
    )
    assert "LIMIT 1001" in safe


def _nested(depth: int) -> str:
    sql = "SELECT 1 AS x"
    for _ in range(depth):
        sql = f"SELECT x FROM ({sql}) t"
    return sql


def test_an_ordinary_nesting_depth_is_accepted() -> None:
    # Deeper than any human query, well inside the cap. The cap itself is
    # tuned to the worst-case shape (nested ARRAY literals go quadratic
    # around bracket depth 14), not to this shape's own, much higher, limit.
    validate_query(_nested(10), "trino")


def test_a_deeply_nested_query_is_refused_not_crashed() -> None:
    # Measured: ~100 levels (1,813 characters) blew the stack inside
    # tree.sql(). A tiny payload must not take the server down.
    with pytest.raises(SqlValidationError) as err:
        validate_query(_nested(300), "trino")
    assert "nested" in str(err.value).lower()


def test_a_very_deeply_nested_query_never_raises_recursionerror() -> None:
    for depth in (100, 500, 1000):
        with pytest.raises(SqlValidationError):
            validate_query(_nested(depth), "trino")


def _and_chain(terms: int) -> str:
    return "SELECT 1 FROM t WHERE " + " AND ".join(f"x{i} = {i}" for i in range(terms))


def test_a_bracket_free_and_chain_is_caught_by_nesting_not_brackets() -> None:
    # An AND chain nests the AST one level per term but opens no bracket at
    # all, so _bracket_depth reads 0 — only _too_deeply_nested can catch it.
    # 300 terms is small in characters (well under _MAX_SQL_CHARS) and shallow
    # in brackets (0), so this proves the nesting guard fires on its own.
    with pytest.raises(SqlValidationError) as err:
        validate_query(_and_chain(300), "trino")
    assert "nested" in str(err.value).lower()


def _nested_abs(depth: int) -> str:
    expr = "1"
    for _ in range(depth):
        expr = f"abs({expr})"
    return f"SELECT {expr} FROM t"


def _nested_case(depth: int) -> str:
    expr = "0"
    for _ in range(depth):
        expr = f"CASE WHEN 1=1 THEN ({expr}) ELSE 0 END"
    return f"SELECT {expr} FROM t"


def test_deeply_nested_function_calls_are_refused_not_crashed() -> None:
    # Measured: nested abs() blows the parser's own stack at 44 levels of
    # bracket nesting — well under the subquery shape's 118. The bracket
    # guard has to catch grammar nesting generally, not one SQL shape.
    with pytest.raises(SqlValidationError) as err:
        validate_query(_nested_abs(44), "trino")
    assert "nested" in str(err.value).lower()


def test_deeply_nested_case_expressions_are_refused_not_crashed() -> None:
    # Measured: nested CASE WHEN ... THEN (...) blows the parser's stack at
    # 27 levels of bracket nesting.
    with pytest.raises(SqlValidationError) as err:
        validate_query(_nested_case(27), "trino")
    assert "nested" in str(err.value).lower()


def _nested_array(depth: int) -> str:
    expr = "1"
    for _ in range(depth):
        expr = f"ARRAY[{expr}]"
    return f"SELECT {expr}"


def test_deeply_nested_arrays_are_refused_not_crashed() -> None:
    # ARRAY[...] nests exactly like a function call but was missed by a
    # guard that counted only ()/() — a 141-character query at depth 18
    # measured over 5s of CPU before this was caught. Depth 44 mirrors the
    # abs() test: far past the cap, and fast to reject.
    with pytest.raises(SqlValidationError) as err:
        validate_query(_nested_array(44), "trino")
    assert "nested" in str(err.value).lower()


def test_bracket_inside_a_string_literal_is_not_counted_as_nesting() -> None:
    # A bracket inside quoted text is data, not nesting depth.
    validate_query("SELECT s FROM t WHERE s = '((([[['", "trino")


def test_a_moderately_nested_array_is_refused_quickly_not_left_to_hang() -> None:
    # Nested ARRAY[...] goes quadratic fast: depth 15 measured at 0.76s and
    # depth 18 over 5s, on a 141-character query. The cap has to sit below
    # where that curve turns expensive, not just below where it crashes.
    started = time.monotonic()
    with pytest.raises(SqlValidationError):
        validate_query(_nested_array(16), "trino")
    assert time.monotonic() - started < 1.0
