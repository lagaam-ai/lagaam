"""SQL safety validation: parse, judge, constrain.

Every rejection message must tell the agent what to change — these texts
are part of the API, like the domain errors.
"""

import random
import time

import pytest
import sqlglot

from lagaam.core.errors import SqlValidationError
from lagaam.core.safety import _bracket_depth, validate_query


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
        "SELECT c.x INTO memory.default.owned FROM memory.default.src c",
        "SELECT c.x INTO TEMP TABLE t FROM memory.default.src c",
        "SELECT c.x INTO UNLOGGED t FROM memory.default.src c",
        'SELECT c.x INTO "cat"."sch"."tbl" FROM memory.default.src c',
        "SELECT 1 INTO t",
    ],
)
def test_select_into_rejected(sql: str) -> None:
    # sqlglot parses SELECT..INTO as a Select, so it passes the Query gate,
    # but the Trino generator renders it as CREATE TABLE .. AS SELECT. Trino
    # runs that write during EXPLAIN, before the budget gate is reached.
    with pytest.raises(SqlValidationError, match="read-only"):
        validate(sql)


def test_the_char_cap_bounds_what_is_emitted_not_only_what_arrived() -> None:
    # Rendering expands SQL — measured 1.33x on a wide coalesce — so an input
    # just under the cap left as 264,053 characters, which is what reaches
    # the engine and the audit line.
    from lagaam.core.safety import _MAX_SQL_CHARS

    sql = f"SELECT coalesce({','.join(['30'] * 66000)}) AS x FROM tpch.tiny.orders"
    assert len(sql) <= _MAX_SQL_CHARS
    with pytest.raises(SqlValidationError, match="characters"):
        validate(sql)


def test_rendered_sql_is_judged_not_only_the_parsed_tree() -> None:
    # The invariant this module documents: what executes is what was judged.
    # A construct that only becomes DDL at render time must not survive.
    rendered = []
    for sql in ("SELECT a FROM tpch.tiny.orders", "SELECT count(*) FROM tpch.tiny.orders"):
        rendered.append(validate(sql).lower())
    assert not any("create" in sql for sql in rendered)
    with pytest.raises(SqlValidationError, match="read-only"):
        validate("SELECT o.custkey INTO tpch.tiny.orders FROM tpch.tiny.orders o")


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
    # An unterminated literal is now refused by the pre-parse depth scanner,
    # which cannot trust a depth it stopped counting mid-literal. Reaching
    # the tokenizer at all would still be ours: it raises TokenError, not
    # ParseError, and both are caught.
    with pytest.raises(SqlValidationError, match="unterminated"):
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


@pytest.mark.parametrize(
    ("name", "prefix"),
    [
        ("block comment", "/* don't */"),
        ("line comment", "-- don't\n"),
        ("block comment, double quote", '/* say "hi */'),
        ("line comment, double quote", '-- say "hi\n'),
    ],
)
def test_an_apostrophe_in_a_comment_does_not_hide_the_nesting(
    name: str, prefix: str
) -> None:
    # One quote character inside a comment used to put the scanner in
    # "inside a literal" state for the rest of the query, so every bracket
    # after it was skipped and the depth read 0. Measured: this turned an
    # instant rejection into a 5.16s accepted parse at depth 18, and 97.8s
    # at depth 22 — before the allowlist, before any budget.
    sql = f"SELECT {prefix} {_nested_array(44).removeprefix('SELECT ')}"
    started = time.monotonic()
    with pytest.raises(SqlValidationError) as err:
        validate_query(sql, "trino")
    assert "nested" in str(err.value).lower()
    assert time.monotonic() - started < 1.0


def test_an_unterminated_quote_is_refused_rather_than_read_as_zero_depth() -> None:
    # The scanner cannot know what it did not see the end of. Any odd count
    # of unmatched quotes leaves it inside a literal at end of input, and the
    # depth it reports is a floor, not the truth — so it must not be trusted.
    with pytest.raises(SqlValidationError):
        validate_query(f"SELECT ' {_nested_array(44).removeprefix('SELECT ')}", "trino")


def test_a_comment_does_not_make_a_legitimate_query_unreadable() -> None:
    # Skipping comments must not swallow the SQL around them.
    validate_query("SELECT a /* keep */ FROM c.s.t WHERE b = 1 LIMIT 10", "trino")
    validate_query("SELECT a -- keep\nFROM c.s.t WHERE b = 1 LIMIT 10", "trino")


def test_a_comment_marker_inside_a_string_literal_is_still_data() -> None:
    # The reverse of the bug: '--' and '/*' inside a literal are characters,
    # not comment starts, so the brackets after them must still be counted.
    with pytest.raises(SqlValidationError) as err:
        validate_query(
            f"SELECT '-- /*' , {_nested_array(44).removeprefix('SELECT ')}", "trino"
        )
    assert "nested" in str(err.value).lower()


def test_the_scanner_never_under_reports_depth_on_mixed_text() -> None:
    # The bug class is "raw-text scanner disagrees with the parser", and a
    # point fix for comments does not retire it. Build text from the pieces
    # that have fooled the scanner — comments, quotes, escaped quotes,
    # delimiters as data — around a known nesting, and assert the scanner
    # never reads lower than the truth. Under-reporting is the direction
    # that lets a payload through; over-reporting only costs a rejection.
    rng = random.Random(20260809)
    noise = ["/* c */", "-- c\n", "'lit'", '"id"', "''''", "'-- /*'", "' ('", " "]
    for _ in range(400):
        real = rng.randint(1, 6)
        chunks = [rng.choice(noise) for _ in range(rng.randint(0, 5))]
        payload = "(" * real + "1" + ")" * real
        chunks.insert(rng.randint(0, len(chunks)), payload)
        sql = "SELECT " + " ".join(chunks)
        assert _bracket_depth(sql) >= real, sql


def test_the_depth_caps_are_the_committed_values() -> None:
    # A mutation run that dies between mutating and restoring leaves a
    # disabled guard in the working tree, and every behavioural test above
    # still passes with a huge cap because their payloads only grow. Pin the
    # numbers themselves so a stale mutation cannot ship green.
    from lagaam.core.safety import _MAX_BRACKET_DEPTH, _MAX_NESTING_DEPTH

    assert _MAX_BRACKET_DEPTH == 12
    assert _MAX_NESTING_DEPTH == 100
