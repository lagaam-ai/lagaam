"""Trino adapter against a real dockerized Trino (TPC-H catalog).

Run: docker compose --profile trino up -d   (from examples/)
Then: uv run pytest -m integration
"""

import pytest

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.budget import (
    DEFAULT_MAX_INTERMEDIATE_ROWS,
    DEFAULT_MAX_SCAN_BYTES,
    QueryBudget,
    enforce_budget,
)
from lagaam.core.errors import BudgetExceededError, TableNotFoundError
from lagaam.core.ports import QueryEngine

pytestmark = pytest.mark.integration


@pytest.fixture
def engine(trino_ready: None) -> TrinoEngine:
    return TrinoEngine(host="localhost", port=8080, user="lagaam-test")


def test_trino_engine_satisfies_the_port(engine: TrinoEngine) -> None:
    assert isinstance(engine, QueryEngine)


def test_dialect_card_targets_trino(engine: TrinoEngine) -> None:
    card = engine.dialect()
    assert card.engine == "Trino"
    assert card.sqlglot_dialect == "trino"
    assert card.rules


def test_validated_sql_executes_on_trino(engine: TrinoEngine) -> None:
    # The canonicalized output (incl. injected LIMIT) must be real Trino SQL.
    import trino.dbapi

    from lagaam.core.safety import validate_query

    sql = validate_query(
        "select orderkey, totalprice from tpch.tiny.orders where orderkey < 100",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    assert "LIMIT 5" in sql
    with trino.dbapi.connect(host="localhost", port=8080, user="lagaam-test") as conn:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
    assert 0 < len(rows) <= 5


async def test_estimate_cost_quotes_a_real_scan(engine: TrinoEngine) -> None:
    est = await engine.estimate_cost(
        "SELECT orderkey FROM tpch.tiny.orders WHERE orderkey < 100 LIMIT 5"
    )
    assert est.confidence == "high"
    assert est.scanned_bytes is not None and est.scanned_bytes > 0
    assert est.row_estimate == 15000  # tpch tiny orders scan is deterministic


async def test_count_star_is_not_quoted_as_free(engine: TrinoEngine) -> None:
    # count(*) scans the whole table but reads no columns, so Trino reports 0
    # bytes. Quoting that as free slips past any budget; blocking it outright
    # denies a query no rewrite can fix. It gets priced from its rows.
    est = await engine.estimate_cost("SELECT count(*) FROM tpch.sf1.orders")
    assert est.scanned_bytes is not None and est.scanned_bytes > 0
    assert est.row_estimate == 1_500_000


async def test_a_self_join_is_billed_for_both_scans(engine: TrinoEngine) -> None:
    # Trino emits one IO entry per distinct (table, column-set), so two scans
    # reading the SAME columns collapse into one and the plan bills half the
    # work. The quote is scaled by the repeat count to make up the difference.
    single = await engine.estimate_cost("SELECT orderkey FROM tpch.tiny.lineitem")
    self_join = await engine.estimate_cost(
        "SELECT a.orderkey FROM tpch.tiny.lineitem a "
        "JOIN tpch.tiny.lineitem b ON a.orderkey = b.orderkey"
    )
    assert self_join.confidence == "high"
    assert single.scanned_bytes is not None
    assert self_join.scanned_bytes == single.scanned_bytes * 2


async def test_a_product_join_is_refused_however_it_is_spelled(
    engine: TrinoEngine,
) -> None:
    # Both inputs are scanned once, so the byte sum is correct and says
    # nothing about the quadratic row work it hides. The plan does say it:
    # each of these prices at 225 billion rows against a 50 million budget.
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    for predicate in ("1 = 1", "a.orderkey <> b.custkey", "a.custkey = b.custkey OR 1 = 1"):
        est = await engine.estimate_cost(
            f"SELECT a.orderkey FROM tpch.sf1.orders a "
            f"JOIN tpch.sf1.customer b ON {predicate}"
        )
        assert est.max_intermediate_rows is not None, predicate
        assert est.max_intermediate_rows > DEFAULT_MAX_INTERMEDIATE_ROWS, predicate
        with pytest.raises(BudgetExceededError):
            enforce_budget(est, budget)


async def test_estimate_cost_of_join_sums_both_scans(engine: TrinoEngine) -> None:
    est = await engine.estimate_cost(
        "SELECT o.orderkey FROM tpch.tiny.orders o "
        "JOIN tpch.tiny.lineitem l ON o.orderkey = l.orderkey LIMIT 5"
    )
    assert est.confidence == "high"
    # Both tables are scanned; the quote must exceed either alone.
    assert est.scanned_bytes is not None and est.scanned_bytes > 300_000


async def test_execute_returns_capped_rows(engine: TrinoEngine) -> None:
    result = await engine.execute(
        "SELECT orderkey FROM tpch.tiny.orders ORDER BY orderkey", max_rows=5
    )
    assert result.columns == ["orderkey"]
    assert result.row_count == 5
    assert result.truncated  # tpch.tiny.orders has 15000 rows, far over 5


async def test_execute_not_truncated_when_rows_fit(engine: TrinoEngine) -> None:
    result = await engine.execute(
        "SELECT orderkey FROM tpch.tiny.orders WHERE orderkey = 1", max_rows=100
    )
    assert result.row_count == 1
    assert not result.truncated


async def test_execute_truncation_needs_a_limit_above_the_cap(
    engine: TrinoEngine,
) -> None:
    # The server injects cap+1 so this detection works; here we pass the SQL
    # already carrying a limit past the cap, as the server would.
    result = await engine.execute(
        "SELECT orderkey FROM tpch.tiny.orders ORDER BY orderkey LIMIT 6",
        max_rows=5,
    )
    assert result.row_count == 5
    assert result.truncated


async def test_describe_table_carries_row_estimate_from_stats(
    engine: TrinoEngine,
) -> None:
    schema = await engine.describe_table("tpch", "tiny", "orders")
    assert schema.row_estimate == 15000  # tpch tiny is deterministic


async def test_capped_listing_is_marked_truncated(trino_ready: None) -> None:
    capped = TrinoEngine(
        host="localhost", port=8080, user="lagaam-test", max_tables_per_catalog=5
    )
    meta = await capped.list_catalogs()
    tpch = next(c for c in meta.catalogs if c.name == "tpch")
    assert tpch.truncated
    assert sum(len(s.tables) for s in tpch.schemas) <= 5


async def test_list_catalogs_includes_tpch_tables(engine: TrinoEngine) -> None:
    meta = await engine.list_catalogs()
    tpch = next(c for c in meta.catalogs if c.name == "tpch")
    tiny = next(s for s in tpch.schemas if s.name == "tiny")
    assert {"orders", "lineitem", "customer"} <= set(tiny.tables)
    # information_schema is protocol noise, not grounding — must be filtered.
    assert all(s.name != "information_schema" for s in tpch.schemas)


async def test_describe_table_grounds_orders(engine: TrinoEngine) -> None:
    schema = await engine.describe_table("tpch", "tiny", "orders")
    assert schema.fqn == "tpch.tiny.orders"
    columns = {c.name: c.type for c in schema.columns}
    assert columns["orderkey"] == "bigint"
    assert columns["orderdate"] == "date"


async def test_describe_table_accepts_uppercase_spelling(
    engine: TrinoEngine,
) -> None:
    # Agents type freely; unquoted SQL semantics say ORDERS means orders.
    # The card echoes the canonical name so it matches list_catalogs output.
    schema = await engine.describe_table("TPCH", "Tiny", "ORDERS")
    assert schema.fqn == "tpch.tiny.orders"
    assert any(c.name == "orderkey" for c in schema.columns)


async def test_reserved_word_name_is_not_found_not_syntax_error(
    engine: TrinoEngine,
) -> None:
    # Unquoted this would be a Trino SYNTAX_ERROR (opaque to the agent).
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("tpch", "tiny", "order")


async def test_describe_missing_table_raises_domain_error(
    engine: TrinoEngine,
) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("tpch", "tiny", "no_such_table")


async def test_describe_missing_catalog_raises_domain_error(
    engine: TrinoEngine,
) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("no_such_catalog", "tiny", "orders")


async def test_bad_column_becomes_a_self_correctable_error(
    engine: TrinoEngine,
) -> None:
    from lagaam.core.errors import QueryFailedError

    with pytest.raises(QueryFailedError, match="describe_table"):
        await engine.execute(
            "SELECT no_such_column FROM tpch.tiny.orders LIMIT 1", max_rows=1
        )


async def test_syntax_error_becomes_a_self_correctable_error(
    engine: TrinoEngine,
) -> None:
    from lagaam.core.errors import QueryFailedError

    with pytest.raises(QueryFailedError):
        # Reaches execute already "validated"; a raw engine syntax reject still
        # translates to a hint rather than a raw code.
        await engine.execute("SELECT FROM tpch.tiny.orders", max_rows=1)


async def test_bad_column_in_estimate_is_self_correctable(
    engine: TrinoEngine,
) -> None:
    # EXPLAIN (in estimate_cost) rejects the bad column before execute is
    # ever reached — that path must translate too.
    from lagaam.core.errors import QueryFailedError

    with pytest.raises(QueryFailedError, match="describe_table"):
        await engine.estimate_cost(
            "SELECT no_such_column FROM tpch.tiny.orders LIMIT 1"
        )


async def test_timeout_is_self_correctable_and_leak_free(
    engine: TrinoEngine,
) -> None:
    # A real timeout arrives as base TrinoQueryError, not TrinoUserError; it
    # must still translate to the timeout hint and not leak the query id.
    from lagaam.core.errors import QueryFailedError

    with pytest.raises(QueryFailedError) as exc:
        await engine.execute(
            "SELECT count(*) FROM tpch.sf1000.lineitem a "
            "JOIN tpch.sf1000.orders b ON a.orderkey > b.orderkey",
            max_rows=1,
            timeout_seconds=1,
        )
    assert "query_id" not in str(exc.value)
    assert "filter" in str(exc.value).lower()


# Corpus measured live against Trino 476 (tpch.tiny). See
# docs/superpowers/specs/2026-08-03-plan-cardinality-measurements.md.
_LEGITIMATE = [
    ("healthy equi-join", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey"),
    ("3-way star join", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey JOIN tpch.tiny.customer c ON o.custkey = c.custkey"),
    ("self join", "SELECT a.orderkey FROM tpch.tiny.orders a JOIN tpch.tiny.orders b ON a.orderkey = b.orderkey"),
    ("cte referenced four times", "WITH t AS (SELECT orderkey FROM tpch.tiny.orders) SELECT a.orderkey FROM t a JOIN t b ON a.orderkey = b.orderkey JOIN t c ON a.orderkey = c.orderkey JOIN t d ON a.orderkey = d.orderkey"),
    ("group by", "SELECT l.linestatus, count(*) AS c FROM tpch.tiny.lineitem l GROUP BY l.linestatus"),
    ("count star", "SELECT count(*) AS c FROM tpch.tiny.lineitem"),
    ("distinct", "SELECT DISTINCT l.linestatus FROM tpch.tiny.lineitem l"),
    ("date filter", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.orderdate > DATE '1995-01-01'"),
    ("like filter", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.comment LIKE '%special%'"),
    ("window function", "SELECT l.orderkey, row_number() OVER (PARTITION BY l.orderkey ORDER BY l.linenumber) AS r FROM tpch.tiny.lineitem l"),
    ("union all", "SELECT orderkey FROM tpch.tiny.orders UNION ALL SELECT orderkey FROM tpch.tiny.orders"),
    ("lateral aggregate", "SELECT o.orderkey, t.c FROM tpch.tiny.orders o LEFT JOIN LATERAL (SELECT count(*) AS c FROM tpch.tiny.lineitem l WHERE l.orderkey = o.orderkey) t ON true"),
    ("correlated equality subquery", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.totalprice > (SELECT avg(l.extendedprice) FROM tpch.tiny.lineitem l WHERE l.orderkey = o.orderkey)"),
    ("scalar subquery", "SELECT orderkey FROM tpch.tiny.orders WHERE totalprice > (SELECT avg(totalprice) FROM tpch.tiny.orders)"),
    ("unnest a literal array", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN UNNEST(ARRAY['a','b']) AS u(n)"),
    ("constant cross join", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN (SELECT 0.2 AS rate) r"),
    ("two small values relations", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN (VALUES (1),(2)) AS a(x) CROSS JOIN (VALUES (1),(2)) AS b(y)"),
    ("semi join", "SELECT orderkey FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.lineitem)"),
    # The shape the NaN-with-criteria exemption exists for: Trino decorrelates
    # this into a plan whose top InnerJoin has equality keys but no estimate.
    # Charging the product there priced 12 billion rows against a real 10,000.
    (
        "doubly nested correlated semi join",
        "SELECT s.suppkey FROM tpch.sf1.supplier s WHERE s.suppkey IN ("
        "SELECT ps.suppkey FROM tpch.sf1.partsupp ps WHERE ps.supplycost < ("
        "SELECT avg(ps2.supplycost) FROM tpch.sf1.partsupp ps2 "
        "WHERE ps2.partkey = ps.partkey))",
    ),
]

# tpch.tiny is a toy scale (15,000-row orders); these prove the gate at sf1
# (1.5M-row orders, 6M-row lineitem), where legitimate analytics measures
# up to 6,001,215 rows at its widest operator.
_LEGITIMATE_AT_SCALE = [
    ("sf1 healthy join", "SELECT l.orderkey FROM tpch.sf1.lineitem l JOIN tpch.sf1.orders o ON l.orderkey = o.orderkey"),
    ("sf1 group by", "SELECT l.orderkey, count(*) AS c FROM tpch.sf1.lineitem l GROUP BY l.orderkey"),
    ("sf1 filtered scan", "SELECT o.orderkey FROM tpch.sf1.orders o WHERE o.orderdate > DATE '1995-01-01'"),
]

_EXPLOSIONS_AT_SCALE = [
    ("sf1 low-cardinality join", "SELECT l.orderkey FROM tpch.sf1.lineitem l JOIN tpch.sf1.orders o ON l.linestatus = o.orderstatus"),
]

_EXPLOSIONS = [
    ("cross join", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o"),
    ("cross join laundered by a like filter", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o WHERE o.comment LIKE '%special%'"),
    ("cross join laundered by a limit", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o LIMIT 10"),
    ("cross join laundered by an aggregate", "WITH t AS (SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o) SELECT count(*) AS c FROM t"),
    ("cross join laundered by distinct", "SELECT DISTINCT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o"),
    ("cross join laundered by a group by", "SELECT l.orderkey, count(*) AS c FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o GROUP BY l.orderkey"),
    ("join on a constant", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON 1 = 1"),
    ("join on a two-valued column", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.linestatus = o.orderstatus"),
    ("inequality join", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey < o.orderkey"),
    ("group by ordinal constant pin", "SELECT a.orderkey FROM tpch.tiny.orders a JOIN (SELECT 1 AS m, count(*) AS c FROM tpch.tiny.lineitem l GROUP BY l.orderkey) t ON t.m = 1"),
    ("correlated inequality subquery", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.totalprice > (SELECT avg(l.extendedprice) FROM tpch.tiny.lineitem l WHERE l.orderkey < o.orderkey)"),
    # Derived-key attacks: same join, wrapped in a scalar function. Trino
    # renders the criteria as "(expr = expr_N)" and reports a NaN estimate
    # bounded by tiny per-side scans (60,175) while doing the true product
    # of work. Measured live: 30,087x under-report for the substr wrapper.
    ("cross join laundered by substr on the join key", "SELECT a.orderkey FROM tpch.tiny.lineitem a JOIN tpch.tiny.lineitem b ON substr(a.linestatus,1,1)=substr(b.linestatus,1,1)"),
    ("cartesian laundered by a zero-length substr", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON substr(l.comment,1,0)=substr(o.comment,1,0)"),
    ("cross join laundered by lower on the join key", "SELECT a.orderkey FROM tpch.tiny.lineitem a JOIN tpch.tiny.lineitem b ON lower(a.linestatus)=lower(b.linestatus)"),
    ("cross join laundered by upper on the join key", "SELECT a.orderkey FROM tpch.tiny.lineitem a JOIN tpch.tiny.lineitem b ON upper(a.linestatus)=upper(b.linestatus)"),
]

_GENERATORS = [
    ("unnest a sequence", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN UNNEST(sequence(1, 10000)) AS u(n)"),
    ("unnest a repeat", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN UNNEST(repeat(l.linestatus, 10000)) AS u(n)"),
]


@pytest.mark.parametrize("label,sql", _LEGITIMATE, ids=[t[0] for t in _LEGITIMATE])
async def test_legitimate_shapes_clear_the_default_budget(
    label: str, sql: str, engine: TrinoEngine
) -> None:
    estimate = await engine.estimate_cost(sql)
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    enforce_budget(estimate, budget)


@pytest.mark.parametrize("label,sql", _EXPLOSIONS, ids=[t[0] for t in _EXPLOSIONS])
async def test_row_explosions_are_denied(label: str, sql: str, engine: TrinoEngine) -> None:
    estimate = await engine.estimate_cost(sql)
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    with pytest.raises(BudgetExceededError):
        enforce_budget(estimate, budget)


@pytest.mark.parametrize("label,sql", _GENERATORS, ids=[t[0] for t in _GENERATORS])
async def test_row_generators_are_denied_by_the_shape_check(
    label: str, sql: str, engine: TrinoEngine
) -> None:
    # The planner cannot see these, so scans.py must still refuse them.
    estimate = await engine.estimate_cost(sql)
    assert estimate.confidence == "low"


@pytest.mark.parametrize(
    "label,sql", _LEGITIMATE_AT_SCALE, ids=[t[0] for t in _LEGITIMATE_AT_SCALE]
)
async def test_legitimate_sf1_shapes_clear_the_default_budget(
    label: str, sql: str, engine: TrinoEngine
) -> None:
    # tiny alone cannot prove the gate survives real scale; sf1 does.
    estimate = await engine.estimate_cost(sql)
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    enforce_budget(estimate, budget)


@pytest.mark.parametrize(
    "label,sql", _EXPLOSIONS_AT_SCALE, ids=[t[0] for t in _EXPLOSIONS_AT_SCALE]
)
async def test_row_explosions_at_sf1_are_denied(
    label: str, sql: str, engine: TrinoEngine
) -> None:
    estimate = await engine.estimate_cost(sql)
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    with pytest.raises(BudgetExceededError):
        enforce_budget(estimate, budget)
