"""Broker JSON to a QueryResult, or the failure it carries. PURE, no I/O.

Every Pinot query error is an HTTP 200 with a populated exceptions[], so the
body is the only signal there is. An incomplete result is a failure and not a
warning: measured, numGroupsLimit=2 returned 22 groups as though they were all
of them, with HTTP 200 and plausible-looking aggregates.
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
