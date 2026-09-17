"""Broker JSON to a QueryResult, its failure, or its pruning counters. PURE.

Every Pinot query error is an HTTP 200 with a populated exceptions[], so the
body is the only signal there is. An incomplete result is a failure and not a
warning: measured, numGroupsLimit=2 returned 22 groups as though they were all
of them, with HTTP 200 and plausible-looking aggregates.

The pruning oracle reads an EXPLAIN envelope rather than a result one, but it
is the same broker answer parsed the same way, so it lives here too.
"""

from typing import Any

from lagaam.adapters.pinot.errors import classify
from lagaam.core.models import QueryResult

INCOMPLETE_RESULT = "INCOMPLETE_RESULT"

# Not a hint code core knows, so it reads as an engine fault, not the query's.
ENGINE_FAULT = "PINOT_ENGINE_FAULT"

_GROUP_WARNING = (
    "The engine approached its group limit on this query. The numbers are "
    "complete, but a broader grouping may not be — narrow the GROUP BY or add "
    "a filter before relying on it."
)


def result_failure(body: Any) -> str | None:
    """The hint code this response carries, or None when it can be trusted.

    Precedence follows the spec: a named exception beats a bare trust flag,
    because the exception says what to change and the flag only says something
    is wrong.
    """
    if not isinstance(body, dict):
        return ENGINE_FAULT

    exceptions = body.get("exceptions")
    if isinstance(exceptions, list) and exceptions:
        first = exceptions[0]
        if isinstance(first, dict):
            code = first.get("errorCode")
            message = first.get("message")
            if isinstance(code, int) and not isinstance(code, bool):
                return classify(code, message if isinstance(message, str) else "")
        return ENGINE_FAULT

    for flag in ("partialResult", "numGroupsLimitReached", "groupsTrimmed"):
        if body.get(flag) is True:
            return INCOMPLETE_RESULT

    # A query the broker answered without a requestId is not a query it ran:
    # measured, INSERT INTO ... FROM FILE comes back exactly this way.
    if body.get("requestId") is None:
        return ENGINE_FAULT

    return None


def parse_query_result(body: Any, max_rows: int) -> QueryResult:
    """Rows and columns from a trusted response, capped at max_rows.

    The server asks the engine for max_rows + 1, so more rows than the cap is
    how truncation is detected without a second query.
    """
    warnings: list[str] = []
    if isinstance(body, dict) and body.get("numGroupsWarningLimitReached") is True:
        warnings.append(_GROUP_WARNING)

    table = body.get("resultTable") if isinstance(body, dict) else None
    if not isinstance(table, dict):
        return QueryResult(columns=[], rows=[], row_count=0, warnings=warnings)

    schema = table.get("dataSchema")
    names = schema.get("columnNames") if isinstance(schema, dict) else None
    columns = (
        [c for c in names if isinstance(c, str)] if isinstance(names, list) else []
    )

    raw = table.get("rows")
    rows = [list(r) for r in raw if isinstance(r, list)] if isinstance(raw, list) else []

    truncated = len(rows) > max_rows
    capped = rows[:max_rows]
    return QueryResult(
        columns=columns,
        rows=capped,
        row_count=len(capped),
        truncated=truncated,
        warnings=warnings,
    )


# Only the counters measured to nest with numSegmentsQueried on 1.5.1. They
# are not additive: the server-side total and its by-value / by-limit
# breakdowns all appear at once, so the largest is read, not the sum.
_PRUNED_COUNTERS = (
    "numSegmentsPrunedByServer",
    "numSegmentsPrunedByValue",
    "numSegmentsPrunedByLimit",
)

# The counters left once the limit prune is not believed. ByServer is the
# server-side total that ByLimit breaks down, so it cannot be read either:
# measured, the limit-pruned EXPLAIN reports ByServer 30 and ByLimit 30 of
# 31 segments, and crediting ByServer would keep the very prune being
# distrusted. ByValue carries a predicate's own pruning independently.
_UNLIMITED_PRUNED_COUNTERS = ("numSegmentsPrunedByValue",)


def surviving_segments(
    explain_json: Any, *, trust_limit_prune: bool = True
) -> int | None:
    """How many segments survive the predicate, from a single-stage EXPLAIN.

    Only ByServer, ByValue and ByLimit are read, and the largest is taken
    rather than the sum: measured on 1.5.1 they nest, so a time filter
    reporting ByServer 28 with ByValue 28 of 31 segments would otherwise
    claim 56 pruned and quote a negative scan. ByBroker and Invalid are
    excluded because every fixture reports them 0, so neither is known to
    behave as a breakdown of numSegmentsQueried — and if the broker already
    reports numSegmentsQueried net of its own pruning, subtracting ByBroker
    again would under-count survivors. Leaving a counter unread can only
    charge more segments, never fewer, which is the fail-safe side.

    `trust_limit_prune` is False when the statement carries an OFFSET, which
    the planner prices as though it were absent: measured, `LIMIT 10 OFFSET
    9000` reports the same 30-of-31 limit prune as the bare `LIMIT 10` and
    then walks 9,117 docs over 29 segments. The caller decides, because only
    it has the SQL; this module sees a broker answer and nothing else.

    None means "no oracle" — the caller then charges every segment.
    """
    if not isinstance(explain_json, dict):
        return None
    if explain_json.get("exceptions"):
        return None
    # EXPLAIN must plan without running; anything scanned means we misread it.
    scanned = explain_json.get("numDocsScanned")
    if isinstance(scanned, bool) or not isinstance(scanned, int) or scanned != 0:
        return None
    queried = explain_json.get("numSegmentsQueried")
    if isinstance(queried, bool) or not isinstance(queried, int) or queried <= 0:
        return None
    pruned = 0
    counters = _PRUNED_COUNTERS if trust_limit_prune else _UNLIMITED_PRUNED_COUNTERS
    for key in counters:
        value = explain_json.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            continue
        pruned = max(pruned, value)
    return max(1, queried - min(pruned, queried))


def consuming_segments_queried(explain_json: Any) -> int:
    """How many of the queried segments were CONSUMING, from the same EXPLAIN.

    numSegmentsQueried includes consuming segments, so this is what has to be
    subtracted before the k-largest charge is applied to the sealed ones.

    Unreadable is 0 rather than None, deliberately: 0 leaves the sealed k
    larger and charges more segments, and the consuming segments themselves
    are charged unconditionally elsewhere — measured against the live
    instance, this counter stayed 1 both with no filter (26 segments queried,
    25 pruned ByServer, 24 ByLimit) and under a filter excluding every value
    (1 segment queried, the other 25 broker-pruned), so no predicate may ever
    reduce it.
    """
    if not isinstance(explain_json, dict) or explain_json.get("exceptions"):
        return 0
    scanned = explain_json.get("numDocsScanned")
    if isinstance(scanned, bool) or not isinstance(scanned, int) or scanned != 0:
        return 0
    consuming = explain_json.get("numConsumingSegmentsQueried")
    if isinstance(consuming, bool) or not isinstance(consuming, int):
        return 0
    return max(0, consuming)
