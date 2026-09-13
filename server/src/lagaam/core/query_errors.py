"""Map engine execution error codes to agent-recoverable hints.

Engine-agnostic: adapters pass the engine's error code (Trino's error_name,
Pinot's later) and get back a next action. An unknown code still returns an
actionable line, never nothing — a failed query should always tell the agent
what to try next.
"""

_HINTS: dict[str, str] = {
    "COLUMN_NOT_FOUND": (
        "A column in the query does not exist. Call describe_table to see the "
        "exact column names, then fix the query."
    ),
    "TABLE_NOT_FOUND": (
        "A table in the query does not exist. Call list_catalogs and "
        "describe_table to get exact names, then retry."
    ),
    "SCHEMA_NOT_FOUND": (
        "The schema does not exist. Call list_catalogs to see valid "
        "catalog.schema names, then retry."
    ),
    "EXCEEDED_GLOBAL_MEMORY_LIMIT": (
        "The query ran out of memory. Add a WHERE filter to read less, "
        "aggregate instead of returning raw rows, or reduce the join size."
    ),
    "EXCEEDED_TIME_LIMIT": (
        "The query took too long and was stopped. Narrow it with a WHERE "
        "filter (a date or key range) or a smaller scope, then retry."
    ),
    "SYNTAX_ERROR": (
        "The SQL is not valid for this engine's dialect. Check the dialect "
        "card's rules and fix the syntax, then retry."
    ),
    "PERMISSION_DENIED": (
        "The engine refused access to a table or column. Query only what you "
        "are permitted to; call list_catalogs to see your access."
    ),
    # Valid SQL the engine cannot plan — retrying it unchanged never works.
    "NOT_SUPPORTED": (
        "The engine parsed the query but cannot run this construct. Rewrite "
        "it a simpler way — flatten a correlated subquery into a join, or "
        "compute the inner result as a separate query — then retry."
    ),
    "FUNCTION_NOT_FOUND": (
        "A function in the query does not exist in this engine. Check the "
        "dialect card for the equivalent function name, then retry."
    ),
    # The planner gave up before the query ever ran, so retrying it unchanged
    # only spends the same time again.
    "OPTIMIZER_TIMEOUT": (
        "The engine could not finish planning this query. It is too complex "
        "to optimise — reduce the number of joins or CTEs, or split it into "
        "separate queries — then retry."
    ),
    # A hard backstop, not a truncation: the engine refused rather than
    # returning a partial answer, so the query has to build fewer rows.
    "EXCEEDED_ROW_LIMIT": (
        "The query built too many rows at one step and was stopped. Join on a "
        "column with more distinct values, filter each side before the join, "
        "or aggregate earlier, then retry."
    ),
    "RESPONSE_TOO_LARGE": (
        "The result was too large to send back. Return fewer columns, lower "
        "the LIMIT, or aggregate instead of returning raw rows, then retry."
    ),
    # The engine answered, but trimmed groups or servers on the way: the
    # numbers look complete and are not, so they are refused, not returned.
    "INCOMPLETE_RESULT": (
        "The engine returned an incomplete result — some groups or servers "
        "were dropped, so the numbers cannot be trusted. Add a WHERE filter "
        "to read less, or group by a column with fewer distinct values, then "
        "retry."
    ),
}

_GENERIC = (
    "The query failed to execute. Review the error, adjust the query, and "
    "retry; report it if it persists."
)


def hint_for_engine_error(error_code: str) -> str:
    """A next-action hint for an engine error code; generic if unmapped."""
    return _HINTS.get(error_code.upper(), _GENERIC)


def is_self_correctable(error_code: str | None) -> bool:
    """True if this code names a condition the agent can fix (has a hint).

    Codes we recognise are the agent's to correct; anything else is treated
    as an engine fault, not blamed on the query.
    """
    return error_code is not None and error_code.upper() in _HINTS
