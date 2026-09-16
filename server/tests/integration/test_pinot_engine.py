"""Pinot adapter against a real dockerized Pinot 1.5.1 batch quickstart.

Run: docker compose --profile pinot up -d   (from examples/)
Then: uv run pytest -m integration
"""

import pytest

from lagaam.adapters.pinot.client import PinotClient
from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.adapters.pinot.names import two_part_sql
from lagaam.adapters.pinot.response import result_failure
from lagaam.core.budget import (
    DEFAULT_MAX_INTERMEDIATE_ROWS,
    DEFAULT_MAX_SCAN_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    QueryBudget,
    enforce_budget,
)
from lagaam.core.errors import (
    BudgetExceededError,
    QueryFailedError,
    TableNotFoundError,
)
from lagaam.core.ports import QueryEngine
from lagaam.core.safety import validate_query

pytestmark = pytest.mark.integration


def _engine() -> PinotEngine:
    return PinotEngine(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )


@pytest.fixture
def engine(pinot_ready: None) -> PinotEngine:
    return _engine()


def test_pinot_engine_satisfies_the_port(engine: PinotEngine) -> None:
    assert isinstance(engine, QueryEngine)


def test_dialect_card_targets_pinot(engine: PinotEngine) -> None:
    card = engine.dialect()
    assert card.engine == "Pinot"
    assert card.sqlglot_dialect == ""
    assert card.rules


async def test_list_catalogs_grounds_on_the_quickstart(engine: PinotEngine) -> None:
    meta = await engine.list_catalogs()
    assert [c.name for c in meta.catalogs] == ["pinot"]
    schemas = {s.name: s.tables for s in meta.catalogs[0].schemas}
    assert "default" in schemas
    tables = [t.lower() for t in schemas["default"]]
    assert "airlinestats" in tables
    assert "baseballstats" in tables


async def test_describe_table_reads_columns_and_the_row_count(
    engine: PinotEngine,
) -> None:
    card = await engine.describe_table("pinot", "default", "airlineStats")
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlineStats"
    # The controller's numRows matched count(*) exactly on this OFFLINE table.
    assert card.row_estimate == 9746
    names = {c.name for c in card.columns}
    assert "Carrier" in names
    assert "DaysSinceEpoch" in names


async def test_describe_table_accepts_the_name_it_just_returned(
    engine: PinotEngine,
) -> None:
    # The round trip the controller's case-sensitive REST paths used to break:
    # /tables/airlinestats/schema is a live 404, so a lowercased card name was
    # a name the agent could not feed back.
    first = await engine.describe_table("pinot", "default", "airlineStats")
    again = await engine.describe_table("pinot", "default", first.table)
    assert again.table == first.table
    assert {c.name for c in again.columns} == {c.name for c in first.columns}


async def test_describe_table_accepts_the_lowercase_grant_spelling(
    engine: PinotEngine,
) -> None:
    # Grants match case-insensitively, so the spelling an operator writes in
    # one must ground the agent just as well as the controller's own.
    card = await engine.describe_table("pinot", "default", "airlinestats")
    assert card.table == "airlineStats"
    assert card.row_estimate == 9746


async def test_the_listing_and_the_card_agree_on_the_spelling(
    engine: PinotEngine,
) -> None:
    listed = (await engine.list_catalogs()).catalogs[0]
    tables = {s.name: s.tables for s in listed.schemas}["default"]
    card = await engine.describe_table("pinot", "default", "AIRLINESTATS")
    assert card.table in tables


async def test_a_multi_value_column_is_grounded_as_an_array(
    engine: PinotEngine,
) -> None:
    # The broker answers these as INT_ARRAY/STRING_ARRAY with array cells, so
    # the card must not describe them as scalars the agent can compare.
    card = await engine.describe_table("pinot", "default", "airlineStats")
    by_name = {c.name: c.type for c in card.columns}
    assert by_name["DivAirportIDs"] == "INT[]"
    assert by_name["DivAirports"] == "STRING[]"
    assert by_name["Carrier"] == "STRING"

    result = await engine.execute(
        "SELECT DivAirportIDs FROM pinot.default.airlineStats LIMIT 1",
        max_rows=1,
        timeout_seconds=30.0,
    )
    assert isinstance(result.rows[0][0], list)


async def test_a_table_that_does_not_exist_says_so(engine: PinotEngine) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("pinot", "default", "nosuchtable")


async def test_a_catalog_that_is_not_pinot_says_so(engine: PinotEngine) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("hive", "default", "airlineStats")


async def test_validated_sql_executes_on_the_multistage_engine(
    engine: PinotEngine,
) -> None:
    sql = validate_query(
        "select Carrier, count(*) as n from pinot.default.airlineStats "
        "group by Carrier order by n desc",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    assert "LIMIT 5" in sql
    result = await engine.execute(sql, max_rows=5, timeout_seconds=30.0)
    assert result.columns == ["Carrier", "n"]
    assert result.row_count == 5
    assert result.rows[0][0] == "WN"


async def test_a_join_runs_because_every_query_is_multistage(
    engine: PinotEngine,
) -> None:
    # The single-stage engine rejects any join outright with errorCode 150.
    sql = validate_query(
        "select a.Carrier as c from pinot.default.airlineStats a "
        "join pinot.default.airlineStats b on a.Carrier = b.Carrier",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    result = await engine.execute(sql, max_rows=5, timeout_seconds=60.0)
    assert result.row_count > 0


async def test_without_the_multistage_option_the_same_join_is_refused(
    pinot_ready: None,
) -> None:
    # What useMultistageEngine=true buys: the option name is only honoured
    # because the join above runs at all, and this is the refusal it avoids.
    client = PinotClient(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )
    try:
        body = await client.broker_query(
            "SELECT a.Carrier AS c FROM airlineStats AS a "
            "JOIN airlineStats AS b ON a.Carrier = b.Carrier LIMIT 5",
            options="",
        )
    finally:
        await client.aclose()
    assert result_failure(body) == "SYNTAX_ERROR"


async def test_the_join_row_limit_is_a_real_backstop(pinot_ready: None) -> None:
    # maxRowsInJoin is only a cap if Pinot recognises the name — an unknown
    # option is silently ignored. Setting it low enough to trip proves it.
    engine = PinotEngine(
        controller_url="http://localhost:9000",
        broker_url="http://localhost:8000",
        max_intermediate_rows=5,
    )
    sql = validate_query(
        "select a.Carrier as c from pinot.default.airlineStats a "
        "join pinot.default.airlineStats b on a.Carrier = b.Carrier",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    with pytest.raises(QueryFailedError, match="distinct values"):
        await engine.execute(sql, max_rows=5, timeout_seconds=60.0)


async def test_the_window_row_limit_is_a_real_backstop(pinot_ready: None) -> None:
    # maxRowsInWindow is the other half of the same cap, and only a window
    # function reaches it: the join query above never builds a window cache.
    engine = PinotEngine(
        controller_url="http://localhost:9000",
        broker_url="http://localhost:8000",
        max_intermediate_rows=5,
    )
    sql = validate_query(
        "select Carrier, row_number() over "
        "(partition by Carrier order by DaysSinceEpoch) as r "
        "from pinot.default.airlineStats",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    with pytest.raises(QueryFailedError, match="too many rows"):
        await engine.execute(sql, max_rows=5, timeout_seconds=60.0)


async def test_the_same_window_query_runs_without_the_cap(
    engine: PinotEngine,
) -> None:
    # Without this, the window test above would pass on a broken query
    # rather than on the cap it is meant to prove.
    sql = validate_query(
        "select Carrier, row_number() over "
        "(partition by Carrier order by DaysSinceEpoch) as r "
        "from pinot.default.airlineStats",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    result = await engine.execute(sql, max_rows=5, timeout_seconds=60.0)
    assert result.row_count == 5


async def test_the_response_size_option_name_is_honoured_on_the_single_stage_engine(
    pinot_ready: None,
) -> None:
    # Kept because it proves something the client cap cannot: Pinot knows the
    # option NAME (an unknown one is silently ignored). Enforced on the
    # single-stage engine only — measured on 1.5.1, the multi-stage engine the
    # adapter uses accepts the option and returns all 97,889 rows anyway.
    client = PinotClient(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )
    try:
        body = await client.broker_query(
            "SELECT playerName, playerID, teamID, league FROM baseballStats "
            "LIMIT 100000",
            options="maxQueryResponseSizeBytes=100",
        )
    finally:
        await client.aclose()
    # errorCode 503 inside an HTTP 200, which the adapter reads as too large.
    assert result_failure(body) == "RESPONSE_TOO_LARGE"


async def test_the_same_query_answers_when_the_size_cap_is_generous(
    pinot_ready: None,
) -> None:
    # The 503 above must be the threshold talking, not the query failing.
    client = PinotClient(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )
    try:
        body = await client.broker_query(
            "SELECT playerName, playerID, teamID, league FROM baseballStats "
            "LIMIT 100000",
            options=f"maxQueryResponseSizeBytes={64 * 1024 * 1024}",
        )
    finally:
        await client.aclose()
    assert result_failure(body) is None


_SIZE_SQL = (
    "select playerName, playerID, teamID, league from pinot.default.baseballStats"
)


async def test_the_response_ceiling_is_a_real_backstop_on_the_multistage_engine(
    pinot_ready: None,
) -> None:
    # What the option alone cannot do: the multi-stage engine ignores it, so
    # the client's own ceiling is the only thing that stops the answer.
    engine = PinotEngine(
        controller_url="http://localhost:9000",
        broker_url="http://localhost:8000",
        max_response_bytes=10_000,
    )
    sql = validate_query(
        _SIZE_SQL, dialect=engine.dialect().sqlglot_dialect, default_limit=5000
    )
    with pytest.raises(QueryFailedError, match="too large to send back"):
        await engine.execute(sql, max_rows=5000, timeout_seconds=60.0)


async def test_the_same_query_returns_its_rows_under_the_default_ceiling(
    engine: PinotEngine,
) -> None:
    # Without this the ceiling test above could pass on a broken query.
    sql = validate_query(
        _SIZE_SQL, dialect=engine.dialect().sqlglot_dialect, default_limit=5000
    )
    result = await engine.execute(sql, max_rows=5000, timeout_seconds=60.0)
    assert result.row_count == 5000


async def test_a_bad_column_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="describe_table"):
        await engine.execute(
            "SELECT nosuchcolumn FROM pinot.default.airlineStats LIMIT 5",
            max_rows=5,
            timeout_seconds=30.0,
        )


async def test_a_bad_table_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="list_catalogs"):
        await engine.execute(
            "SELECT Carrier FROM pinot.default.nosuchtable LIMIT 5",
            max_rows=5,
            timeout_seconds=30.0,
        )


async def test_a_bad_function_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="dialect card"):
        await engine.execute(
            "SELECT nosuchfunc(Carrier) AS x FROM pinot.default.airlineStats LIMIT 5",
            max_rows=5,
            timeout_seconds=30.0,
        )


async def test_a_timeout_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="took too long"):
        await engine.execute(
            "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
            "GROUP BY Carrier LIMIT 5",
            max_rows=5,
            timeout_seconds=0.001,
        )


async def test_the_adapter_neither_adds_nor_removes_a_limit(
    engine: PinotEngine,
) -> None:
    # The multi-stage engine has no auto-limit — measured, a LIMIT-less
    # selection returned all 9,746 rows. The LIMIT is core's job, not ours,
    # and the adapter must not paper over its absence.
    result = await engine.execute(
        "SELECT Carrier FROM pinot.default.airlineStats",
        max_rows=20,
        timeout_seconds=60.0,
    )
    assert result.row_count == 20
    assert result.truncated is True


async def test_a_time_filter_quotes_less_than_no_filter(pinot_ready: None) -> None:
    engine = _engine()
    unfiltered = await engine.estimate_cost(
        "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 10"
    )
    filtered = await engine.estimate_cost(
        "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10"
    )
    assert unfiltered.row_estimate is not None
    assert filtered.row_estimate is not None
    assert filtered.row_estimate < unfiltered.row_estimate
    assert unfiltered.scanned_bytes is not None
    assert filtered.scanned_bytes is not None
    assert filtered.scanned_bytes < unfiltered.scanned_bytes
    assert filtered.confidence == "high"


async def test_the_quote_is_never_under_what_execution_scanned(
    pinot_ready: None,
) -> None:
    """The whole contract: a bound, never a guess."""
    sql = (
        "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10"
    )
    engine = _engine()
    estimate = await engine.estimate_cost(sql)
    body = await engine._client.broker_query(
        two_part_sql(sql, PinotEngine.CATALOG), "useMultistageEngine=true"
    )
    scanned = body["numDocsScanned"]
    assert scanned > 0
    assert estimate.row_estimate is not None
    assert estimate.row_estimate >= scanned


async def test_a_cross_join_quotes_the_product_of_both_tables(
    pinot_ready: None,
) -> None:
    engine = _engine()
    estimate = await engine.estimate_cost(
        "SELECT count(*) FROM pinot.default.airlineStats a, "
        "pinot.default.baseballStats b LIMIT 10"
    )
    airline = await engine.describe_table("pinot", "default", "airlineStats")
    baseball = await engine.describe_table("pinot", "default", "baseballStats")
    assert airline.row_estimate is not None
    assert baseball.row_estimate is not None
    assert (
        estimate.max_intermediate_rows
        == airline.row_estimate * baseball.row_estimate
    )
    assert estimate.max_intermediate_rows is not None
    assert estimate.max_intermediate_rows > 900_000_000


async def test_an_equi_join_is_charged_the_product_until_a_key_is_proven(
    pinot_ready: None,
) -> None:
    engine = _engine()
    estimate = await engine.estimate_cost(
        "SELECT count(*) FROM pinot.default.airlineStats a "
        "JOIN pinot.default.baseballStats b ON a.Carrier = b.teamID LIMIT 10"
    )
    assert estimate.max_intermediate_rows == 9746 * 97889


async def test_a_self_join_quote_is_never_under_what_execution_scanned(
    pinot_ready: None,
) -> None:
    """Shape 33 of the under-quote audit: 14 distinct carriers, 10.7M pairs."""
    sql = (
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    estimate = await _engine().estimate_cost(sql)
    assert estimate.row_estimate is not None
    # Measured: the multi-stage run scans the table twice, 19,492 docs.
    assert estimate.row_estimate >= 19492
    assert estimate.max_intermediate_rows is not None
    # The true pair count, computed in Pinot: sum(n*n) over Carrier groups.
    assert estimate.max_intermediate_rows >= 10_719_442


async def test_an_offset_is_quoted_above_what_it_really_scans(
    pinot_ready: None,
) -> None:
    """The limit prune is offset-blind: the EXPLAIN prunes 30 of 31 segments
    for this query exactly as for the bare LIMIT, and it then walks 9,117
    docs over 29 segments to reach row 9,000."""
    sql = "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10 OFFSET 9000"
    engine = _engine()
    estimate = await engine.estimate_cost(sql)
    body = await engine._client.broker_query(
        two_part_sql(sql, PinotEngine.CATALOG), "useMultistageEngine=true"
    )
    scanned = body["numDocsScanned"]
    assert scanned > 1000
    assert estimate.row_estimate is not None
    assert estimate.row_estimate >= scanned

    budget = QueryBudget(
        max_rows=1000,
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )
    with pytest.raises(BudgetExceededError):
        enforce_budget(estimate, budget)
