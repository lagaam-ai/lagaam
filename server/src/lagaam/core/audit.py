"""Structured JSONL audit trail.

One JSON line per tool call to a pluggable sink (default: stderr). The record
is who / what / the decision / the outcome — enough for forensics without the
raw session. Auditing is a side effect: a sink that fails must never take the
query down with it, so record() swallows sink errors.
"""

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
        event = {
            "ts": self._clock(),
            "identity": identity,
            "tool": tool,
            "outcome": outcome,
            "detail": detail,
        }
        try:
            # Compact, ASCII-safe, single line: JSONL invariant.
            line = json.dumps(event, separators=(",", ":"), default=str)
            self._sink(line)
        except Exception:
            # A failed audit write must not fail the request it describes.
            pass
