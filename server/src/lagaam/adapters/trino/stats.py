"""Trino's post-execution query stats to QueryActuals. PURE.

The dbapi cursor carries the coordinator's latest stats block once rows have
been fetched. Read after a capped fetch, the query can still be RUNNING —
measured on Trino 476, `LIMIT 1001` fetched as exactly 1001 rows still
reported RUNNING — so the state decides `partial`.
"""

from typing import Any

from lagaam.core.models import QueryActuals

# Counters the neutral fields don't cover, kept under Trino's own names.
_EXTRA_COUNTERS = ("cpuTimeMillis", "physicalInputBytes", "peakMemoryBytes")


def _count(stats: dict[str, Any], key: str) -> int | None:
    value = stats.get(key)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def actuals_from_stats(stats: Any) -> QueryActuals | None:
    """What the coordinator says the query did, or None with no stats at all.

    Unreadable counters are None, never 0: the audit line must not claim a
    query read nothing because the engine did not say what it read.
    """
    if not isinstance(stats, dict) or not stats:
        return None
    counters = {
        key: value
        for key in _EXTRA_COUNTERS
        if (value := _count(stats, key)) is not None
    }
    return QueryActuals(
        rows_scanned=_count(stats, "processedRows"),
        bytes_scanned=_count(stats, "processedBytes"),
        elapsed_ms=_count(stats, "elapsedTimeMillis"),
        partial=stats.get("state") != "FINISHED",
        engine_counters=counters,
    )
