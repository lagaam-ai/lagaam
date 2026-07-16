"""Audit log: a structured JSONL trail of every tool call.

Forensics for when a hijacked agent does damage anyway. Each event is one JSON
line to a pluggable sink; the record must carry who, what, the decision, and
the outcome — enough to reconstruct what happened without the raw session.
"""

import json
from pathlib import Path

import pytest

from lagaam.core.audit import AuditLog


def test_event_is_one_json_line_with_the_core_fields() -> None:
    lines: list[str] = []
    log = AuditLog(sink=lines.append, clock=lambda: 1_700_000_000.0)
    log.record(
        identity="agent-1",
        tool="query_data",
        outcome="allowed",
        detail={"sql": "SELECT x FROM t", "scanned_bytes": 1024},
    )
    assert len(lines) == 1
    event = json.loads(lines[0])
    assert event["identity"] == "agent-1"
    assert event["tool"] == "query_data"
    assert event["outcome"] == "allowed"
    assert event["detail"]["scanned_bytes"] == 1024
    assert event["ts"] == 1_700_000_000.0


def test_denied_events_record_the_reason() -> None:
    lines: list[str] = []
    log = AuditLog(sink=lines.append, clock=lambda: 0.0)
    log.record(
        identity="agent-1",
        tool="query_data",
        outcome="denied",
        detail={"reason": "over budget"},
    )
    event = json.loads(lines[0])
    assert event["outcome"] == "denied"
    assert event["detail"]["reason"] == "over budget"


def test_lines_are_newline_free_so_jsonl_stays_one_per_line() -> None:
    lines: list[str] = []
    log = AuditLog(sink=lines.append, clock=lambda: 0.0)
    log.record(
        identity="a",
        tool="query_data",
        outcome="allowed",
        detail={"sql": "SELECT 'a\nb'"},  # embedded newline must be escaped
    )
    assert lines[0].count("\n") == 0
    assert json.loads(lines[0])["detail"]["sql"] == "SELECT 'a\nb'"


def test_sink_failure_never_breaks_the_caller() -> None:
    # An audit sink that throws must not take the query down with it.
    def broken(_: str) -> None:
        raise OSError("disk full")

    log = AuditLog(sink=broken, clock=lambda: 0.0)
    log.record(identity="a", tool="query_data", outcome="allowed", detail={})


def test_default_sink_is_constructable() -> None:
    # The zero-arg default must not require wiring to exist.
    AuditLog()


def test_from_env_uses_a_file_when_configured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "audit.jsonl"
    monkeypatch.setenv("LAGAAM_AUDIT_LOG", str(path))
    log = AuditLog.from_env()
    log.record(identity="a", tool="query_data", outcome="allowed", detail={})
    log.record(identity="b", tool="list_catalogs", outcome="allowed", detail={})
    lines = path.read_text().strip().splitlines()
    assert len(lines) == 2
    assert json.loads(lines[0])["identity"] == "a"
