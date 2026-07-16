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
    assert _detail(OSError("connection refused")) == "connection refused"
