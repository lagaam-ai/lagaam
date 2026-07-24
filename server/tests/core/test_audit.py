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


def test_unserializable_detail_still_emits_an_event() -> None:
    # An agent-influenced value must not be able to delete its own audit line.
    class Hostile:
        def __str__(self) -> str:
            raise RuntimeError("no string for you")

    lines: list[str] = []
    AuditLog(sink=lines.append).record("a", "query_data", "allowed", {"x": Hostile()})
    record = json.loads(lines[0])
    assert record["outcome"] == "allowed"
    assert record["detail_error"] == "unserializable"


def test_oversized_value_keeps_both_ends_with_a_hash() -> None:
    # A megabyte IN-list is a disk-fill risk, not evidence — but the head and
    # tail are the evidence, so a padded middle must be what gets dropped.
    sql = "SELECT a FROM c.s.t WHERE k IN (" + ",".join(["1"] * 200_000) + ") LIMIT 7"
    lines: list[str] = []
    AuditLog(sink=lines.append).record("a", "query_data", "allowed", {"sql": sql})
    detail = json.loads(lines[0])["detail"]
    assert detail["sql"].startswith("SELECT a FROM c.s.t WHERE k IN (")
    assert detail["sql"].endswith(") LIMIT 7")
    assert "elided" in detail["sql"]
    assert detail["_truncated"]["sql"]["chars"] == len(sql)
    assert len(detail["_truncated"]["sql"]["sha256"]) == 16
    assert len(lines[0]) < 10_000


def test_a_padded_prefix_cannot_push_the_query_out_of_the_record() -> None:
    # Comments are stripped before this, but any long leading value must not
    # be able to hide what follows it.
    sql = "/* " + "x" * 6000 + " */ SELECT a FROM c.s.secret WHERE k = 1"
    lines: list[str] = []
    AuditLog(sink=lines.append).record("a", "query_data", "allowed", {"sql": sql})
    logged = json.loads(lines[0])["detail"]["sql"]
    assert "c.s.secret" in logged and "WHERE k = 1" in logged


def test_oversized_values_nested_in_dicts_and_lists_are_capped() -> None:
    # json.dumps(default=str) runs after the guard, so anything it would
    # stringify has to be capped here or the bound is not a bound.
    class Sprawling:
        def __str__(self) -> str:
            return "z" * 300_000

    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a",
        "query_data",
        "allowed",
        {
            "estimate": {"note": "n" * 200_000},
            "rows": ["r" * 200_000],
            "obj": Sprawling(),
        },
    )
    assert len(lines[0]) < 20_000
    marks = json.loads(lines[0])["detail"]["_truncated"]
    assert set(marks) == {"estimate.note", "rows[0]", "obj"}
