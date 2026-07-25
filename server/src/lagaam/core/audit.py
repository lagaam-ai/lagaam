"""Structured JSONL audit trail.

One JSON line per tool call to a pluggable sink (default: stderr). The record
is who / what / the decision / the outcome — enough for forensics without the
raw session. Auditing is a side effect: a sink that fails must never take the
query down with it, so record() swallows sink errors.
"""

import hashlib
import json
import os
import sys
import time
from collections.abc import Callable
from typing import Any

Sink = Callable[[str], None]


def _stderr_sink(line: str) -> None:
    print(line, file=sys.stderr, flush=True)


def _file_sink(path: str) -> Sink:
    def write(line: str) -> None:
        with open(path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    return write


# Agent SQL is unbounded (a big IN list is megabytes); a log line that size
# is a disk-fill risk, not evidence. Enough to identify the query, plus a
# hash so the full text is still matchable if it was captured elsewhere.
_MAX_VALUE_CHARS = 4096
_HALF = _MAX_VALUE_CHARS // 2
_ELISION = "…[{cut} chars elided]…"
# Per-value capping bounds one string, not the record: a detail with 100k
# small entries is the same disk-fill by another route.
_MAX_ENTRIES = 100
_MAX_DEPTH = 20


def _cap(value: str) -> tuple[str, dict[str, Any]] | None:
    """Shorten one oversized string, keeping both ends.

    Head and tail, not head alone: an agent that pads the front with comments
    would otherwise push the FROM and WHERE out of the record entirely.
    """
    if len(value) <= _MAX_VALUE_CHARS:
        return None
    cut = len(value) - 2 * _HALF
    kept = value[:_HALF] + _ELISION.format(cut=cut) + value[-_HALF:]
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
    return kept, {"chars": len(value), "sha256": digest}


def _capped(text: str, marks: dict[str, Any], path: str) -> str:
    """Shorten one string and note the cut under its path."""
    capped = _cap(text)
    if capped is None:
        return text
    marks[path] = capped[1]
    return capped[0]


def _bounded(
    value: Any,
    marks: dict[str, Any],
    path: str,
    seen: frozenset[int] = frozenset(),
    depth: int = 0,
) -> Any:
    """Recursively cap strings anywhere in the detail, recording what was cut.

    Non-JSON values are stringified here rather than left to json.dumps's
    ``default=str``, which would run *after* this guard and reintroduce the
    unbounded string it exists to prevent. Cycles, depth and container length
    are bounded too: an audit record that cannot be built is an audit record
    that goes missing, which is the failure this whole function prevents.
    """
    if isinstance(value, dict | list | tuple):
        if id(value) in seen:
            return "<circular>"
        if depth >= _MAX_DEPTH:
            return "<too deeply nested>"
        seen = seen | {id(value)}
    if isinstance(value, dict):
        items = list(value.items())[:_MAX_ENTRIES]
        out = {
            _capped(str(k), marks, f"{path}.<key>" if path else "<key>"): _bounded(
                v, marks, f"{path}.{k}" if path else str(k), seen, depth + 1
            )
            for k, v in items
        }
        if len(value) > _MAX_ENTRIES:
            marks[path or "<root>"] = {"entries": len(value)}
        return out
    if isinstance(value, list | tuple):
        out_list = [
            _bounded(v, marks, f"{path}[{i}]", seen, depth + 1)
            for i, v in enumerate(value[:_MAX_ENTRIES])
        ]
        if len(value) > _MAX_ENTRIES:
            marks[path] = {"entries": len(value)}
        return out_list
    if isinstance(value, str):
        return _capped(value, marks, path)
    if isinstance(value, int | float | bool) or value is None:
        return value
    try:
        text = str(value)
    except Exception:
        # A value that cannot describe itself must not take the record with it.
        return f"<unstringable {type(value).__name__}>"
    return _capped(text, marks, path)


def _truncated(detail: dict[str, Any]) -> dict[str, Any]:
    """Cap oversized values anywhere in the detail, marking what was cut."""
    marks: dict[str, Any] = {}
    out = _bounded(detail, marks, "")
    if marks:
        # One reserved key, so an agent-supplied field cannot forge a marker.
        out["_truncated"] = marks
    return dict(out)


class AuditLog:
    def __init__(
        self,
        sink: Sink | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._sink = sink or _stderr_sink
        self._clock = clock

    @classmethod
    def from_env(cls) -> "AuditLog":
        """LAGAAM_AUDIT_LOG is a file path; unset means stderr."""
        path = os.environ.get("LAGAAM_AUDIT_LOG")
        return cls(sink=_file_sink(path) if path else None)

    def record(
        self,
        identity: str,
        tool: str,
        outcome: str,
        detail: dict[str, Any],
    ) -> None:
        """Emit one audit event. Never raises — auditing must not break serving.

        The contract is load-bearing: the tool boundary records from a finally,
        so anything escaping here would replace the exception being handled.
        """
        try:
            timestamp = self._clock()
        except Exception:
            timestamp = 0.0
        header = {
            "ts": timestamp,
            "identity": identity,
            "tool": tool,
            "outcome": outcome,
        }
        try:
            # Compact, ASCII-safe, single line: JSONL invariant.
            line = json.dumps(
                {**header, "detail": _truncated(detail)},
                separators=(",", ":"),
                default=str,
            )
        except Exception:
            # An agent-influenced value must not be able to delete its own
            # audit line: drop the detail, keep the event.
            line = json.dumps(
                {**header, "detail_error": "unserializable"},
                separators=(",", ":"),
                default=str,
            )
        try:
            self._sink(line)
        except Exception:
            # A failed audit write must not fail the request it describes.
            pass
