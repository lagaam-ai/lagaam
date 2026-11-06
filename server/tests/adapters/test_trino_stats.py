"""Trino cursor stats to QueryActuals: the receipt for the audit line.

The stats blocks here are the shape Trino 476 returned through trino-python-
client 0.338 after a fetch, trimmed to the fields that matter.
"""

from typing import Any

import pytest

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.adapters.trino.stats import actuals_from_stats

_FINISHED: dict[str, Any] = {
    "queryId": "20261106_054418_00001_gzkuf",
    "state": "FINISHED",
    "cpuTimeMillis": 23,
    "wallTimeMillis": 122,
    "elapsedTimeMillis": 182,
    "processedRows": 15000,
    "processedBytes": 0,
    "physicalInputBytes": 0,
    "peakMemoryBytes": 1064,
    "rootStage": {"stageId": "0", "processedRows": 20},
}


def test_a_finished_query_maps_to_neutral_fields_and_native_counters() -> None:
    actuals = actuals_from_stats(_FINISHED)
    assert actuals is not None
    assert actuals.rows_scanned == 15000
    assert actuals.bytes_scanned == 0
    assert actuals.elapsed_ms == 182
    assert actuals.partial is False
    assert actuals.engine_counters == {
        "cpuTimeMillis": 23,
        "physicalInputBytes": 0,
        "peakMemoryBytes": 1064,
    }


def test_a_query_still_running_after_a_capped_fetch_is_partial() -> None:
    # Measured: LIMIT 1001 fetched as exactly 1001 rows still said RUNNING.
    actuals = actuals_from_stats({**_FINISHED, "state": "RUNNING"})
    assert actuals is not None and actuals.partial is True


@pytest.mark.parametrize("stats", [None, {}, "FINISHED", ["processedRows"]])
def test_no_stats_is_no_actuals(stats: Any) -> None:
    assert actuals_from_stats(stats) is None


@pytest.mark.parametrize("bad", [None, -1, "15000", 1.5, True])
def test_an_unreadable_counter_is_unknown_not_zero(bad: Any) -> None:
    actuals = actuals_from_stats(
        {**_FINISHED, "processedRows": bad, "cpuTimeMillis": bad}
    )
    assert actuals is not None
    assert actuals.rows_scanned is None
    assert "cpuTimeMillis" not in actuals.engine_counters


class _Cursor:
    description = [("x",)]

    def __init__(self, stats: Any) -> None:
        self.stats = stats

    def execute(self, sql: str) -> None:
        pass

    def fetchmany(self, n: int) -> list[list[Any]]:
        return [[1]]


class _Conn:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def __enter__(self) -> "_Conn":
        return self

    def __exit__(self, *a: Any) -> None:
        pass

    def cursor(self) -> _Cursor:
        return self._cursor


def test_execute_attaches_the_cursors_stats_as_actuals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = TrinoEngine()
    conn = _Conn(_Cursor(_FINISHED))
    monkeypatch.setattr(engine, "_connect", lambda props=None: conn)
    result = engine._execute("SELECT x FROM t", max_rows=10, timeout_seconds=None)
    assert result.actuals is not None
    assert result.actuals.rows_scanned == 15000


def test_execute_without_stats_still_returns_the_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Observation only: a missing receipt must never cost the agent its answer.
    engine = TrinoEngine()
    conn = _Conn(_Cursor(None))
    monkeypatch.setattr(engine, "_connect", lambda props=None: conn)
    result = engine._execute("SELECT x FROM t", max_rows=10, timeout_seconds=None)
    assert result.rows == [[1]]
    assert result.actuals is None
