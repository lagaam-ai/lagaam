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


def _bounded(value: Any, marks: dict[str, Any], path: str) -> Any:
    """Recursively cap strings anywhere in the detail, recording what was cut.

    Non-JSON values are stringified here rather than left to json.dumps's
    ``default=str``, which would run *after* this guard and reintroduce the
    unbounded string it exists to prevent.
    """
    if isinstance(value, dict):
        return {
            str(k): _bounded(v, marks, f"{path}.{k}" if path else str(k))
            for k, v in value.items()
        }
    if isinstance(value, list | tuple):
        return [
            _bounded(v, marks, f"{path}[{i}]") for i, v in enumerate(value)
        ]
    if isinstance(value, str | int | float | bool) or value is None:
        if not isinstance(value, str):
            return value
        capped = _cap(value)
        if capped is None:
            return value
        marks[path] = capped[1]
        return capped[0]
    text = str(value)
    capped = _cap(text)
    if capped is None:
        return text
    marks[path] = capped[1]
    return capped[0]


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
        """Emit one audit event. Never raises — auditing must not break serving."""
        header = {
            "ts": self._clock(),
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
