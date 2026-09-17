"""Pinot adapter against a real dockerized Pinot 1.5.1 batch quickstart.

Run: docker compose --profile pinot up -d   (from examples/)
Then: uv run pytest -m integration
"""

import pytest

from lagaam.adapters.pinot.client import PinotClient
from lagaam.adapters.pinot.engine import (
    _EXPLAIN_PRUNING,
    _EXPLAIN_TIMEOUT_MS,
    _OPT_TIMEOUT_MS,
    PinotEngine,
)
from lagaam.adapters.pinot.names import two_part_sql
from lagaam.adapters.pinot.response import (
    consuming_segments_queried,
    result_failure,
    surviving_segments,
)
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
        + airline.row_estimate
        + baseball.row_estimate
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
    assert estimate.max_intermediate_rows == 9746 * 97889 + 9746 + 97889


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
    # The product, plus the unmatched rows an outer join would ride on top.
    assert estimate.max_intermediate_rows == 9746 * 9746 + 2 * 9746


async def test_a_mixed_case_self_join_quotes_as_the_canonical_spelling(
    pinot_ready: None,
) -> None:
    engine = _engine()
    mixed = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.AIRLINESTATS b ON a.Carrier = b.Carrier LIMIT 10"
    )
    canonical = await engine.estimate_cost(
        "SELECT a.Carrier FROM pinot.default.airlineStats a "
        "JOIN pinot.default.airlineStats b ON a.Carrier = b.Carrier LIMIT 10"
    )
    assert mixed.row_estimate == canonical.row_estimate
    assert mixed.scanned_bytes == canonical.scanned_bytes
    assert mixed.max_intermediate_rows == canonical.max_intermediate_rows
    assert mixed.row_estimate == 2 * 9746


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


async def test_mixed_case_columns_are_quoted_at_their_real_size(
    pinot_ready: None,
) -> None:
    """The controller's ?columns= filter is case-sensitive, so a lowercased
    playerID returned nothing and the quote lost 578,965 bytes to 36,739."""
    engine = _engine()
    estimate = await engine.estimate_cost(
        "SELECT league, playerID FROM pinot.default.baseballStats LIMIT 10"
    )
    truth = await engine._client.controller_get(
        "/segments/baseballStats/metadata",
        params={"columns": ["league", "playerID"]},
        database="default",
    )
    charged = 0
    for body in truth.values():
        for column in body["columns"]:
            charged += sum(column["indexSizeMap"].values())
    assert charged >= 578_965
    assert estimate.scanned_bytes is not None
    assert estimate.scanned_bytes >= charged


async def test_a_lowercase_table_quotes_what_the_canonical_one_does(
    pinot_ready: None,
) -> None:
    """The broker executes either spelling; the controller's REST paths are
    case-sensitive, so the lowercase one used to 404 into a false denial."""
    engine = _engine()
    lowered = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlinestats LIMIT 5"
    )
    canonical = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    assert canonical.confidence == "high"
    assert lowered.confidence == "high"
    assert lowered.row_estimate == canonical.row_estimate
    assert lowered.scanned_bytes == canonical.scanned_bytes


def _realtime_engine() -> PinotEngine:
    return PinotEngine(
        controller_url="http://localhost:9001", broker_url="http://localhost:8001"
    )


async def test_a_realtime_table_grounds_without_a_row_count(
    pinot_realtime_ready: None,
) -> None:
    """The STREAM half of the profile answers, and a REALTIME table carries no
    grounding count: the controller's numRows is an OFFLINE fact, so a card
    built from a consuming table has to say None rather than guess."""
    engine = _realtime_engine()
    card = await engine.describe_table("pinot", "default", "airlineStats")
    assert card.table == "airlineStats"
    assert card.row_estimate is None

    result = await engine.execute(
        "SELECT count(*) FROM pinot.default.airlineStats LIMIT 1",
        max_rows=1,
        timeout_seconds=30.0,
    )
    assert result.rows[0][0] > 0


# --- U12 Task 8: the realtime charge and the key evidence, against the live
# STREAM instance on :9001/:8001. Every number below is derived from the
# cluster at test time; nothing is transcribed from a measurement log.


async def _realtime_count(engine: PinotEngine, table: str) -> int:
    """count(*) through the broker, on the engine these quotes bound."""
    body = await engine._client.broker_query(
        f"SELECT count(*) FROM {table}", "useMultistageEngine=true"
    )
    count = body["resultTable"]["rows"][0][0]
    assert isinstance(count, int) and not isinstance(count, bool)
    return count


async def _realtime_scanned(engine: PinotEngine, sql: str) -> int:
    """numDocsScanned from really executing the agent's own SQL.

    The quotation is a bound on execution, so the only honest comparison is
    against the same statement run on the same (multi-stage) engine the
    quote is written for — not against a count(*) of the table.
    """
    body = await engine._client.broker_query(
        two_part_sql(sql, PinotEngine.CATALOG), "useMultistageEngine=true"
    )
    assert not body.get("exceptions"), body.get("exceptions")
    scanned = body.get("numDocsScanned")
    assert isinstance(scanned, int) and not isinstance(scanned, bool)
    return scanned


async def _flush_rows(engine: PinotEngine, table: str) -> int:
    """The table's own flush threshold, from its live config."""
    config = await engine._client.controller_get(f"/tables/{table}", database="default")
    maps = config["REALTIME"]["ingestionConfig"]["streamIngestionConfig"][
        "streamConfigMaps"
    ]
    return int(maps[0]["realtime.segment.flush.threshold.rows"])


async def _externalview_states(engine: PinotEngine, table: str) -> dict[str, int]:
    """How many segment replicas sit in each state right now."""
    body = await engine._client.controller_get(
        f"/tables/{table}/externalview", database="default"
    )
    counts: dict[str, int] = {}
    for states in body["REALTIME"].values():
        for state in states.values():
            counts[state] = counts.get(state, 0) + 1
    return counts


async def _sealed_segment_docs(engine: PinotEngine, table: str) -> list[int]:
    """Per-segment docs for the sealed segments, from the controller.

    A consuming segment reports -1 bytes in /size and carries no useful
    entry in the metadata response, so the size report is what says which
    names are sealed.
    """
    metadata = await engine._client.controller_get(
        f"/segments/{table}/metadata", database="default"
    )
    size = await engine._client.controller_get(
        f"/tables/{table}/size", database="default"
    )
    reported = {
        name: body["reportedSizeInBytes"]
        for name, body in (size["realtimeSegments"]["segments"] or {}).items()
    }
    return [
        body["totalDocs"]
        for name, body in metadata.items()
        if reported.get(name, -1) >= 0
    ]


async def _sealed_docs(engine: PinotEngine, table: str) -> int:
    """Docs in the sealed segments only."""
    return sum(await _sealed_segment_docs(engine, table))


async def test_realtime_an_unfiltered_select_is_priced_not_denied(
    pinot_realtime_ready: None,
) -> None:
    """Shape (a). A table with a consuming segment quotes high, with bytes,
    and bounds what the engine really scans for that same statement."""
    engine = _realtime_engine()
    sql = "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats LIMIT 1000"
    estimate = await engine.estimate_cost(sql)
    assert estimate.confidence == "high"
    assert estimate.scanned_bytes is not None and estimate.scanned_bytes > 0
    assert estimate.row_estimate is not None
    # The bound that matters: it covers the execution, derived not transcribed.
    assert estimate.row_estimate >= await _realtime_scanned(engine, sql)
    # A LIMIT large enough to prune nothing also covers the whole live table.
    assert estimate.row_estimate >= await _realtime_count(engine, "airlineStats")


async def test_realtime_rows_are_the_sealed_sum_plus_the_consuming_charge(
    pinot_realtime_ready: None,
) -> None:
    """The quote's own arithmetic, with every term derived from the cluster:
    the k largest sealed segments' docs, plus consuming segments x this
    table's flush threshold.

    k is the survivor count the pruning oracle reports, net of the consuming
    segments it counts among them — those are charged by the threshold, not
    by their (zero) docs.
    """
    engine = _realtime_engine()
    sql = "SELECT Carrier FROM pinot.default.airlineStats LIMIT 1000"
    estimate = await engine.estimate_cost(sql)
    consuming = (await _externalview_states(engine, "airlineStats")).get("CONSUMING", 0)
    threshold = await _flush_rows(engine, "airlineStats")
    assert consuming > 0, "this shape is about the consuming charge"

    explain = await engine._explain(
        f"{_EXPLAIN_PRUNING}{two_part_sql(sql, PinotEngine.CATALOG)}",
        f"{_OPT_TIMEOUT_MS}={_EXPLAIN_TIMEOUT_MS}",
    )
    surviving = surviving_segments(explain, trust_limit_prune=True)
    assert surviving is not None
    sealed_k = max(1, surviving - consuming_segments_queried(explain))

    # The k largest sealed segments, from the controller's own docs counts.
    docs = await _sealed_segment_docs(engine, "airlineStats")
    largest = sum(sorted(docs, reverse=True)[:sealed_k])

    # Exact, because the design makes this term-by-term exact.
    assert estimate.row_estimate == largest + consuming * threshold
    assert estimate.row_estimate >= await _realtime_scanned(engine, sql)


async def test_realtime_the_consuming_charge_survives_an_impossible_filter(
    pinot_realtime_ready: None,
) -> None:
    """Shape (b). A predicate excluding every value prunes sealed segments
    but can never prune the consuming one, so the charge stays."""
    engine = _realtime_engine()
    sql = (
        "SELECT Carrier FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch > 99999 LIMIT 1000"
    )
    estimate = await engine.estimate_cost(sql)
    consuming = (await _externalview_states(engine, "airlineStats")).get("CONSUMING", 0)
    threshold = await _flush_rows(engine, "airlineStats")
    assert consuming > 0
    assert estimate.row_estimate is not None
    # The charge is unconditional: it is in the quote even though the filter
    # matches nothing at all.
    assert estimate.row_estimate >= consuming * threshold
    # And it still bounds the (empty) execution.
    assert estimate.row_estimate >= await _realtime_scanned(engine, sql)


async def test_realtime_a_selective_filter_shrinks_the_sealed_k(
    pinot_realtime_ready: None,
) -> None:
    """Shape (c). The consuming charge is unconditional; the sealed one is
    not — a value filter keeping only some segments quotes fewer rows."""
    engine = _realtime_engine()
    unfiltered_sql = "SELECT Carrier FROM pinot.default.airlineStats LIMIT 1000"
    # The low end of this table's own live time range, so some sealed
    # segments survive and some are pruned.
    body = await engine._client.broker_query(
        "SELECT min(DaysSinceEpoch) FROM airlineStats", "useMultistageEngine=true"
    )
    low = body["resultTable"]["rows"][0][0]
    filtered_sql = (
        "SELECT Carrier FROM pinot.default.airlineStats "
        f"WHERE DaysSinceEpoch = {low} LIMIT 1000"
    )
    unfiltered = await engine.estimate_cost(unfiltered_sql)
    filtered = await engine.estimate_cost(filtered_sql)
    assert unfiltered.row_estimate is not None
    assert filtered.row_estimate is not None
    assert filtered.row_estimate < unfiltered.row_estimate
    # Still a bound on what that filtered statement really scans.
    assert filtered.row_estimate >= await _realtime_scanned(engine, filtered_sql)


async def test_realtime_bytes_are_present_at_high_confidence(
    pinot_realtime_ready: None,
) -> None:
    """Shape (f). A consuming segment reports -1 bytes on every probe, so
    without the projection from sealed ratios this table would quote no
    bytes at all and be denied rather than priced."""
    engine = _realtime_engine()
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 1000"
    )
    assert estimate.confidence == "high"
    assert estimate.scanned_bytes is not None
    assert estimate.scanned_bytes > 0


async def test_realtime_describe_table_still_carries_no_row_estimate(
    pinot_realtime_ready: None,
) -> None:
    """Shape (g). Grounding is untouched: the controller's numRows is not a
    pre-execution fact for a REALTIME table, so the card withholds it."""
    engine = _realtime_engine()
    card = await engine.describe_table("pinot", "default", "airlineStats")
    assert card.row_estimate is None
    assert card.columns


async def test_realtime_an_offset_query_ignores_the_limit_prune(
    pinot_realtime_ready: None,
) -> None:
    """Shape (h). The planner prices OFFSET as though it were absent, so the
    limit prune is not believed and every sealed segment is charged."""
    engine = _realtime_engine()
    sql = "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10 OFFSET 5"
    estimate = await engine.estimate_cost(sql)
    sealed = await _sealed_docs(engine, "airlineStats")
    consuming = (await _externalview_states(engine, "airlineStats")).get("CONSUMING", 0)
    threshold = await _flush_rows(engine, "airlineStats")
    assert estimate.row_estimate is not None
    # Every sealed segment's docs plus the consuming charge — no prune.
    assert estimate.row_estimate >= sealed + consuming * threshold
    assert estimate.row_estimate >= await _realtime_scanned(engine, sql)


async def test_realtime_a_mixed_case_name_quotes_what_the_canonical_one_does(
    pinot_realtime_ready: None,
) -> None:
    """Shape (i). The broker runs either spelling; the controller's REST
    paths are case-sensitive, so the mixed-case one must not 404 into a
    false denial on the realtime instance either."""
    engine = _realtime_engine()
    mixed = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.AIRLINESTATS LIMIT 1000"
    )
    canonical = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 1000"
    )
    assert canonical.confidence == "high"
    assert mixed.confidence == "high"
    assert mixed.row_estimate == canonical.row_estimate
    assert mixed.scanned_bytes == canonical.scanned_bytes


async def test_realtime_a_split_metadata_table_is_denied_not_under_quoted(
    pinot_realtime_ready: None,
) -> None:
    """Shapes (d)/(e), as the live cluster actually presents them.

    u12upsert and u12plain sit on a 2-partition topic, so their segments
    land on two servers — and /segments/{t}/metadata returns only one
    server's half while /size names both. The segments in hand are then not
    the table, and the completeness guard withholds every number rather
    than summing a confident subset into an under-quote. That denial is the
    behaviour worth pinning: it is the one failure mode the gate exists to
    prevent, and it is what makes the upsert-vs-twin join contrast
    unobservable here (see the task-8 report).
    """
    engine = _realtime_engine()
    for table in ("u12upsert", "u12plain"):
        size = await engine._client.controller_get(
            f"/tables/{table}/size", database="default"
        )
        named = {
            name
            for name, body in (size["realtimeSegments"]["segments"] or {}).items()
            if body["reportedSizeInBytes"] >= 0
        }
        metadata = await engine._client.controller_get(
            f"/segments/{table}/metadata", database="default"
        )
        # Derived, not assumed: the metadata response really is short.
        assert not named <= set(metadata), (
            f"{table} metadata is complete after all — this test's premise is gone"
        )
        estimate = await engine.estimate_cost(
            f"SELECT pk FROM pinot.default.{table} LIMIT 1000"
        )
        assert estimate.confidence == "low"
        assert estimate.row_estimate is None
        assert estimate.scanned_bytes is None


async def test_realtime_the_upsert_pk_is_the_schema_s_primary_key(
    pinot_realtime_ready: None,
) -> None:
    """The evidence the key rule would read, if the table could be sized:
    u12upsert declares a primary key and its twin does not."""
    engine = _realtime_engine()
    upsert = await engine._client.controller_get("/schemas/u12upsert")
    plain = await engine._client.controller_get("/schemas/u12plain")
    assert upsert.get("primaryKeyColumns") == ["pk"]
    assert not plain.get("primaryKeyColumns")
    # And the upsert view really does collapse to one row per key, where the
    # twin on the same topic keeps every version.
    keys = await _realtime_count(engine, "u12upsert")
    versions = await _realtime_count(engine, "u12plain")
    assert keys < versions


async def test_realtime_a_bare_select_bounds_its_own_execution(
    pinot_realtime_ready: None,
) -> None:
    """The soundness rule, on the shape that used to break it (ruling 8.1).

    The pruning EXPLAIN of a bare select reports numSegmentsPrunedByLimit 5
    of 7 queried — Pinot planned it under its own implicit LIMIT 10 — while
    executing the same SQL on the multi-stage engine scans the whole table.
    `trust_limit_prune` now requires an explicit LIMIT, so the prune is not
    read and every sealed segment is charged.
    """
    engine = _realtime_engine()
    sql = "SELECT Carrier FROM pinot.default.airlineStats"
    estimate = await engine.estimate_cost(sql)
    assert estimate.confidence == "high"
    assert estimate.row_estimate is not None
    assert estimate.row_estimate >= await _realtime_scanned(engine, sql)


async def test_a_bare_select_bounds_its_own_execution_on_the_batch_instance(
    pinot_ready: None,
) -> None:
    """The same shape on the OFFLINE quickstart, where the under-quote was
    worst: 422 against 9,746 docs scanned, a 23x breach at high confidence."""
    engine = _engine()
    sql = "SELECT Carrier FROM pinot.default.airlineStats"
    estimate = await engine.estimate_cost(sql)
    body = await engine._client.broker_query(
        two_part_sql(sql, PinotEngine.CATALOG), "useMultistageEngine=true"
    )
    scanned = body["numDocsScanned"]
    assert scanned >= 9746
    assert estimate.confidence == "high"
    assert estimate.row_estimate is not None
    assert estimate.row_estimate >= scanned
