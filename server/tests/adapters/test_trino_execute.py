"""Trino execute() plumbing that needs no live engine: the timeout mapping.

A sub-second timeout must never render as "0s" — Trino reads that as an
instant kill, silently breaking every real query.
"""

from typing import Any

import pytest

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.models import QueryResult


class _FakeCursor:
    description = [("x",)]

    def execute(self, sql: str) -> None:
        pass

    def fetchmany(self, n: int) -> list[list[Any]]:
        return [[1]]


class _FakeConn:
    def __init__(self, captured: dict[str, Any]) -> None:
        self._captured = captured

    def __enter__(self) -> "_FakeConn":
        return self

    def __exit__(self, *a: Any) -> None:
        pass

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()


@pytest.mark.parametrize(
    "timeout, expected",
    [(0.1, "1s"), (0.5, "1s"), (1.0, "1s"), (1.49, "2s"), (30.0, "30s")],
)
def test_timeout_rounds_up_never_to_zero(
    monkeypatch: pytest.MonkeyPatch, timeout: float, expected: str
) -> None:
    captured: dict[str, Any] = {}
    engine = TrinoEngine()

    def fake_connect(props: dict[str, str] | None = None) -> _FakeConn:
        captured["props"] = props
        return _FakeConn(captured)

    monkeypatch.setattr(engine, "_connect", fake_connect)
    result = engine._execute("SELECT x FROM t", max_rows=10, timeout_seconds=timeout)
    assert isinstance(result, QueryResult)
    assert captured["props"] == {"query_max_run_time": expected}


def test_no_timeout_sets_no_session_property(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, Any] = {}
    engine = TrinoEngine()
    monkeypatch.setattr(
        engine, "_connect", lambda props=None: (captured.update(props=props), _FakeConn(captured))[1]
    )
    engine._execute("SELECT x FROM t", max_rows=10, timeout_seconds=None)
    assert captured["props"] is None


def test_translate_error_maps_resource_limits_not_via_user_error() -> None:
    # A timeout arrives as base TrinoQueryError (not TrinoUserError) with an
    # error_name; it must still translate to a hint, and never leak query_id.
    from lagaam.adapters.trino.engine import _translate_error
    from lagaam.core.errors import EngineError, QueryFailedError

    class FakeTimeout(Exception):
        error_name = "EXCEEDED_TIME_LIMIT"
        message = "Query exceeded maximum time limit of 2.00s"

    translated = _translate_error(FakeTimeout())
    assert isinstance(translated, QueryFailedError)
    assert "query_id" not in str(translated)

    class FakeInfra(Exception):
        error_name = "GENERIC_INTERNAL_ERROR"
        message = "worker crashed"

    infra = _translate_error(FakeInfra())
    assert isinstance(infra, EngineError)
    assert "worker crashed" in str(infra)

def test_metadata_failures_use_message_not_repr() -> None:
    # str(TrinoQueryError) is a repr with query_id and full kwargs; the
    # metadata wrappers must strip it down the same way _translate_error does.
    from lagaam.adapters.trino.engine import _detail

    class FakeQueryError(Exception):
        message = "line 1:1: mismatched input"

        def __str__(self) -> str:
            return "TrinoQueryError(message=..., query_id=20260716_abc)"

    assert _detail(FakeQueryError()) == "line 1:1: mismatched input"


def test_unreachable_engine_does_not_name_the_host() -> None:
    # OSError is what a refused connection raises, and its text carries the
    # coordinator's address — internal topology the agent has no use for.
    import trino.exceptions

    from lagaam.adapters.trino.engine import _UNREACHABLE, _detail

    leaky = OSError("connection refused to trino-coordinator.internal:8080")
    assert _detail(leaky) == _UNREACHABLE
    http = trino.exceptions.Http503Error("error 503: token=secret-value")
    assert _detail(http) == _UNREACHABLE


# --- SHOW STATS row counts ------------------------------------------------


class _StatsCursor:
    """A cursor that replays one SHOW STATS result."""

    def __init__(self, rows: list[list[Any]]) -> None:
        self._rows = rows

    def execute(self, sql: str) -> None:
        pass

    def fetchall(self) -> list[list[Any]]:
        return self._rows


def _row_estimate(rows: list[list[Any]]) -> int | None:
    return TrinoEngine._row_estimate(_StatsCursor(rows), '"c"."s"."t"')


def test_row_estimate_reads_the_summary_row() -> None:
    # SHOW STATS: (column_name, data_size, distinct, nulls, row_count, ...)
    assert _row_estimate([["a", 1.0, 1.0, 0.0, None], [None, None, None, None, 15000.0]]) == 15000


def test_row_estimate_survives_a_nan_row_count() -> None:
    # Stats-less tables report NaN, and round(nan) raises — a cosmetic missing
    # estimate must not become a hard describe_table failure.
    assert _row_estimate([[None, None, None, None, float("nan")]]) is None


def test_row_estimate_survives_an_infinite_row_count() -> None:
    assert _row_estimate([[None, None, None, None, float("inf")]]) is None


def test_row_estimate_with_no_summary_row_is_none() -> None:
    assert _row_estimate([["a", 1.0, 1.0, 0.0, None]]) is None


def test_row_estimate_with_no_rows_is_none() -> None:
    assert _row_estimate([]) is None


# --- HTTP failures are engine failures ------------------------------------


async def test_http_error_becomes_a_domain_error() -> None:
    # trino.exceptions.HttpError derives from Exception, not Error, so it once
    # escaped every except clause: unaudited, and raw text to the agent.
    import trino.exceptions

    from lagaam.core.errors import EngineError

    engine = TrinoEngine()

    def boom() -> None:
        raise trino.exceptions.Http503Error("error 503: token=secret-value")

    engine._list_catalogs = boom  # type: ignore[method-assign]
    with pytest.raises(EngineError) as caught:
        await engine.list_catalogs()
    # The response body can carry credentials; the agent gets the class only.
    from lagaam.adapters.trino.engine import _UNREACHABLE

    assert "secret-value" not in str(caught.value)
    assert _UNREACHABLE in str(caught.value)
