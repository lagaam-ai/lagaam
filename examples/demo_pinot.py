"""Three beats against a live Pinot realtime table: rows that landed seconds
ago get priced and run, a join with no proven key is blocked with the number,
and the same join on an upsert table runs on the strength of its key.

Run it yourself (needs the realtime profile up and bootstrapped):

    docker compose --profile pinot-realtime up -d   # from examples/
    ./pinot-realtime/bootstrap.sh                   # topics, tables, feed
    uv run --project ../server python demo_pinot.py

Every message you see is the real tool output an agent gets — nothing is
mocked, and the segment still being written is priced rather than skipped.
Pass --transcript PATH to also dump the session as JSON (that is how
docs/demo-pinot.gif is rendered).
"""

import argparse
import asyncio
import json
import logging
import sys
import time
from typing import Any

from mcp.shared.memory import (
    create_connected_server_and_client_session as client_session,
)
from mcp.types import CallToolResult

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.core.audit import AuditLog
from lagaam.core.budget import (
    DEFAULT_MAX_INTERMEDIATE_ROWS,
    DEFAULT_MAX_SCAN_BYTES,
    DEFAULT_TIMEOUT_SECONDS,
    QueryBudget,
)
from lagaam.core.cost import human_bytes
from lagaam.core.identity import AgentIdentity
from lagaam.server import create_server

CONTROLLER_URL = "http://localhost:9001"
BROKER_URL = "http://localhost:8001"

# The three tables the demo touches and nothing else: the grant is the demo's
# first claim, so it is written out rather than inherited from the environment.
GRANT = AgentIdentity(
    name="demo-agent",
    allowed_tables=frozenset(
        {
            "pinot.default.airlinestats",
            "pinot.default.u12upsert",
            "pinot.default.u12plain",
        }
    ),
)

FRESH_SQL = (
    "SELECT Carrier, count(*) AS flights FROM pinot.default.airlineStats "
    "GROUP BY Carrier LIMIT 5"
)
PLAIN_JOIN_SQL = (
    "SELECT a.pk FROM pinot.default.u12plain a "
    "JOIN pinot.default.u12plain b ON a.pk = b.pk LIMIT 10"
)
UPSERT_JOIN_SQL = (
    "SELECT a.pk FROM pinot.default.u12upsert a "
    "JOIN pinot.default.u12upsert b ON a.pk = b.pk LIMIT 10"
)

GREEN, RED, DIM, BOLD, CYAN, RESET = (
    "\x1b[32m", "\x1b[31m", "\x1b[2m", "\x1b[1m", "\x1b[36m", "\x1b[0m",
)


def default_budget() -> QueryBudget:
    """The shipped defaults, spelled out — never from_env, so a stray
    LAGAAM_* in a shell cannot quietly change what the demo demonstrates."""
    return QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


def key_evidence_budget() -> QueryBudget:
    """The defaults with only the row ceiling lowered to 10,000.

    Both joins scan a handful of kilobytes and finish instantly, so bytes and
    timeout stay at their defaults: the key evidence moves the row ceiling
    alone, and that is the only dimension narrowed. Without the narrowing the
    50M default admits the 641,600-row product and there is nothing to show.
    """
    return QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=10_000,
        timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
    )


def pinot_engine() -> PinotEngine:
    return PinotEngine(controller_url=CONTROLLER_URL, broker_url=BROKER_URL)


async def run_beats() -> tuple[CallToolResult, CallToolResult, CallToolResult]:
    """The three beats, in order, through a real MCP round trip.

    Returns the raw tool answers so the integration test can assert on the
    same objects the narration below formats — the demo and its test watch
    exactly one thing happen.
    """
    engine = pinot_engine()
    fresh_server = create_server(
        engine, budget=default_budget(), identity=GRANT
    )
    async with client_session(fresh_server._mcp_server) as client:
        fresh = await client.call_tool("query_data", {"sql": FRESH_SQL})

    joins_server = create_server(
        engine, budget=key_evidence_budget(), identity=GRANT
    )
    async with client_session(joins_server._mcp_server) as client:
        blocked = await client.call_tool("query_data", {"sql": PLAIN_JOIN_SQL})
        admitted = await client.call_tool("query_data", {"sql": UPSERT_JOIN_SQL})

    return fresh, blocked, admitted


def say(kind: str, text: str, log: list[dict[str, str]]) -> None:
    color = {"ok": GREEN, "err": RED, "sql": CYAN, "note": DIM, "head": BOLD}[kind]
    print(f"{color}{text}{RESET}")
    log.append({"kind": kind, "text": text})


def quote_line(estimate: dict[str, Any] | None) -> str:
    """The quotation the gate actually read, as the audit line recorded it."""
    if not estimate:
        return "quote: (none recorded)"
    rows = estimate.get("row_estimate")
    widest = estimate.get("max_intermediate_rows")
    scanned = estimate.get("scanned_bytes")
    parts = [f"{rows:,} rows scanned" if rows is not None else "rows unknown"]
    if scanned is not None:
        parts.append(human_bytes(scanned))
    if widest is not None:
        parts.append(f"{widest:,} at its widest step")
    parts.append(f'confidence "{estimate.get("confidence")}"')
    return "quote: " + ", ".join(parts)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transcript", help="also write the session as JSON")
    args = parser.parse_args()
    log: list[dict[str, str]] = []

    # The adapter's HTTP chatter is the point elsewhere; here it is noise over
    # the narration. Quieted in the narrated run only — run_beats() leaves
    # logging exactly as the caller set it.
    for noisy in ("httpx", "mcp.server.lowlevel.server"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # The quotation is not in the tool's answer — it is on the audit line,
    # which is where an operator would read it too.
    quotes: list[dict[str, Any]] = []

    def audit_sink(line: str) -> None:
        record = json.loads(line)
        detail = record.get("detail", {})
        if record.get("tool") == "query_data":
            quotes.append(detail.get("estimate") or {})

    engine = pinot_engine()
    say(
        "head",
        "lagaam — governed MCP server | Pinot 1.5.1 REALTIME, one segment still consuming",
        log,
    )

    beats = [
        (
            "Rows that landed seconds ago. Six sealed segments and one still "
            "being written, which reports zero docs and minus-one bytes — so "
            "it is charged at its own stored flush threshold, 100 rows, "
            "rather than quoted free:",
            FRESH_SQL,
            default_budget(),
        ),
        (
            "Now a self-join on a plain realtime table. No key anyone can "
            "prove, so every left row may match every right row (row ceiling: "
            "10,000):",
            PLAIN_JOIN_SQL,
            key_evidence_budget(),
        ),
        (
            "The same join, same rows, same topic — on the upsert twin, whose "
            "primary key the catalog proves and the scan ordinal confirms:",
            UPSERT_JOIN_SQL,
            key_evidence_budget(),
        ),
    ]

    for note, sql, budget in beats:
        print()
        log.append({"kind": "gap", "text": ""})
        say("note", f"# {note}", log)
        say("sql", f"query_data> {sql}", log)
        server = create_server(
            engine,
            budget=budget,
            identity=GRANT,
            audit=AuditLog(sink=audit_sink),
        )
        start = time.monotonic()
        async with client_session(server._mcp_server) as client:
            result = await client.call_tool("query_data", {"sql": sql})
        elapsed = time.monotonic() - start
        estimate = quotes[-1] if quotes else None
        say("note", f"    {quote_line(estimate)}", log)
        if result.isError:
            text = result.content[0].text  # type: ignore[union-attr]
            text = text.removeprefix("Error executing tool query_data: ")
            say("err", f"BLOCKED  {text}", log)
            continue
        data = result.structuredContent or {}
        say(
            "ok",
            f"OK  {data['row_count']} rows in {elapsed:.1f}s "
            f"(columns: {', '.join(data['columns'])})",
            log,
        )
        for row in data["rows"][:5]:
            say("ok", "    " + "  ".join(str(v) for v in row), log)

    print()
    say(
        "note",
        "# A consuming segment priced, not skipped. A product join stopped "
        "before it ran. A proven key let the same shape through.",
        log,
    )
    if args.transcript:
        with open(args.transcript, "w", encoding="utf-8") as fh:
            json.dump(log, fh, indent=2)
        print(f"{DIM}transcript -> {args.transcript}{RESET}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
