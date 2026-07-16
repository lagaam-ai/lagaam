"""Verify a result before the agent trusts it.

A query can succeed and still mislead: zero rows read as "no data", a
truncated page read as the whole answer, a column that came back entirely
NULL. These produce warnings, not errors — the agent should double-check, not
be blocked. The warnings are phrased as next actions it can take.
"""

from lagaam.core.models import QueryResult


def verify_result(result: QueryResult) -> list[str]:
    """Return agent-facing warnings about ways this result could mislead."""
    warnings: list[str] = []

    if result.row_count == 0:
        # An empty result is genuinely ambiguous, so stop here — every column
        # is trivially all-NULL and would just add noise.
        warnings.append(
            "The query returned no rows. This may mean no data matches, or a "
            "filter/join is too strict — check your WHERE conditions."
        )
        return warnings

    if result.truncated:
        warnings.append(
            f"Only the first {result.row_count} rows are shown; more rows "
            "exist. Any total or count over these alone is incomplete — add a "
            "GROUP BY/aggregate or a tighter filter to get a complete answer."
        )
        # Can't claim a column is all-NULL when we haven't seen every row.
        return warnings

    for i, name in enumerate(result.columns):
        # Defensive: never crash the response on a ragged row from an engine.
        if all(i < len(row) and row[i] is None for row in result.rows):
            warnings.append(
                f"Column '{name}' is NULL in every row — the column may be "
                "wrong, or the join dropped it. Verify it with describe_table."
            )

    return warnings
