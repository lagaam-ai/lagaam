"""Audit log: a structured JSONL trail of every tool call.

Forensics for when a hijacked agent does damage anyway. Each event is one JSON
line to a pluggable sink; the record must carry who, what, the decision, and
the outcome — enough to reconstruct what happened without the raw session.
"""

import json
from pathlib import Path

import pytest

from lagaam.core.audit import AuditLog, _truncated


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


def test_a_value_that_cannot_describe_itself_loses_only_itself() -> None:
    # One hostile field must not take the rest of the record with it — the
    # sibling keys are the evidence.
    class Hostile:
        def __str__(self) -> str:
            raise RuntimeError("no string for you")

    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a", "query_data", "allowed", {"sql": "SELECT a FROM c.s.t", "x": Hostile()}
    )
    detail = json.loads(lines[0])["detail"]
    assert detail["sql"] == "SELECT a FROM c.s.t"
    assert detail["x"] == "<unstringable Hostile>"


def test_a_cycle_does_not_blind_the_record() -> None:
    cyclic: dict[str, object] = {}
    cyclic["self"] = cyclic
    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a", "query_data", "allowed", {"sql": "SELECT a FROM c.s.t", "c": cyclic}
    )
    detail = json.loads(lines[0])["detail"]
    assert detail["sql"] == "SELECT a FROM c.s.t"
    assert detail["c"]["self"] == "<circular>"


def test_deep_nesting_does_not_blind_the_record() -> None:
    deep: dict[str, object] = {}
    node = deep
    for _ in range(5000):
        child: dict[str, object] = {}
        node["n"] = child
        node = child
    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a", "query_data", "allowed", {"sql": "SELECT a FROM c.s.t", "d": deep}
    )
    detail = json.loads(lines[0])["detail"]
    assert detail["sql"] == "SELECT a FROM c.s.t"
    assert "too deeply nested" in json.dumps(detail["d"])


def test_a_huge_key_and_a_huge_container_are_both_bounded() -> None:
    # Per-value capping bounds one string; the record needs bounding too.
    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a",
        "query_data",
        "allowed",
        {"K" * 20_000: 1, "many": {str(i): "v" for i in range(100_000)}},
    )
    assert len(lines[0]) < 30_000


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
    # Whatever survives is capped, and the line as a whole is bounded — the
    # record-size budget stops before every field gets its own 4 KiB.
    assert len(lines[0]) < 70_000
    detail = json.loads(lines[0])["detail"]
    assert "estimate.note" in detail["_truncated"]
    assert len(detail["estimate"]["note"]) <= 4200


def test_two_long_keys_do_not_collide_into_one() -> None:
    # Capping the middle of a key makes distinct keys identical, and the
    # second silently overwrites the first — data loss in an evidence store.
    first = "A" * 3000 + "1" + "B" * 3000
    second = "A" * 3000 + "2" + "B" * 3000
    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a", "query_data", "allowed", {first: "value-one", second: "value-two"}
    )
    detail = json.loads(lines[0])["detail"]
    values = [v for k, v in detail.items() if k != "_truncated"]
    assert sorted(values) == ["value-one", "value-two"]


def test_a_wide_deep_structure_cannot_grow_the_line_without_bound() -> None:
    # Per-level caps multiply: 100 entries at depth 20 is 100^20 values, each
    # under the per-value cap. Only a total budget bounds the record.
    def fan(depth: int) -> object:
        if depth == 0:
            return "x" * 100
        return {str(i): fan(depth - 1) for i in range(100)}

    lines: list[str] = []
    AuditLog(sink=lines.append).record("a", "query_data", "allowed", {"d": fan(3)})
    assert len(lines[0]) < 100_000


def test_what_the_tool_did_survives_a_crowded_argument_list() -> None:
    # Entry capping drops by insertion order, and executed_sql is appended
    # last — the one field the record exists for must not be the one cut.
    detail: dict[str, object] = {f"arg{i}": i for i in range(150)}
    detail["executed_sql"] = "SELECT a FROM c.s.t LIMIT 10"
    lines: list[str] = []
    AuditLog(sink=lines.append).record("a", "query_data", "allowed", detail)
    logged = json.loads(lines[0])["detail"]
    assert logged["executed_sql"] == "SELECT a FROM c.s.t LIMIT 10"


def test_many_large_sibling_values_cannot_grow_the_line_without_bound() -> None:
    # Each value is individually under the per-value cap; only a total budget
    # stops 100 of them from becoming a 400 KB line.
    lines: list[str] = []
    AuditLog(sink=lines.append).record(
        "a",
        "query_data",
        "allowed",
        {f"k{i}": "x" * 4000 for i in range(100)},
    )
    assert len(lines[0]) < 80_000


def test_a_supplied_truncation_marker_cannot_be_forged() -> None:
    # A forged marker would show a forensic reader an elision and a fake hash
    # on SQL that was in fact logged in full.
    out = _truncated(
        {
            "executed_sql": "SELECT a FROM c.s.t",
            "_truncated": {"executed_sql": {"chars": 999999, "sha256": "deadbeef"}},
        }
    )
    assert "_truncated" not in out
    assert out["executed_sql"] == "SELECT a FROM c.s.t"


def test_a_real_truncation_still_marks_itself() -> None:
    out = _truncated({"executed_sql": "x" * 9000})
    assert out["_truncated"]["executed_sql"]["chars"] == 9000
