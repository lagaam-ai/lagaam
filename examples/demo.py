"""Three beats, one governed server: unsafe SQL rejected, an expensive query
blocked before it runs, a well-scoped query through in milliseconds.

Run it yourself (needs the Trino profile up):

    docker compose --profile trino up -d          # from examples/
    uv run --project ../server python demo.py

Every message you see is the real tool output an agent gets — nothing is
mocked. Pass --transcript PATH to also dump the session as JSON (that is
how docs/demo.gif is rendered).
"""

import argparse
import asyncio
import json
import sys
import time

from mcp.shared.memory import (
    create_connected_server_and_client_session as client_session,
)

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.audit import AuditLog
from lagaam.core.budget import QueryBudget
from lagaam.core.caching import CachingQueryEngine
from lagaam.core.identity import AgentIdentity
from lagaam.server import create_server

SCAN_BUDGET_BYTES = 25 * 1024 * 1024  # 25 MB — tiny on purpose: tpch.sf1 demo

BEATS = [
    (
        "The agent gets lazy and asks for everything:",
        "SELECT * FROM tpch.sf1.orders",
    ),
    (
        "It names its columns — but joins two whole tables:",
        "SELECT o.orderkey, l.extendedprice FROM tpch.sf1.orders o "
        "JOIN tpch.sf1.lineitem l ON o.orderkey = l.orderkey",
    ),
    (
        "It reads the hint and scopes the query down:",
        "SELECT year(orderdate) AS yr, CAST(round(sum(totalprice)) AS bigint) AS sales "
        "FROM tpch.sf1.orders GROUP BY 1 ORDER BY 1",
    ),
]

GREEN, RED, DIM, BOLD, CYAN, RESET = (
    "\x1b[32m", "\x1b[31m", "\x1b[2m", "\x1b[1m", "\x1b[36m", "\x1b[0m",
)


def say(kind: str, text: str, log: list[dict[str, str]]) -> None:
    color = {"ok": GREEN, "err": RED, "sql": CYAN, "note": DIM, "head": BOLD}[kind]
    print(f"{color}{text}{RESET}")
    log.append({"kind": kind, "text": text})


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--transcript", help="also write the session as JSON")
    args = parser.parse_args()
    log: list[dict[str, str]] = []

    engine = CachingQueryEngine(TrinoEngine())
    audit_path = "demo-audit.jsonl"

    def audit_sink(line: str) -> None:
        with open(audit_path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    server = create_server(
        engine,
        budget=QueryBudget(max_scan_bytes=SCAN_BUDGET_BYTES, max_returned_rows=10),
        identity=AgentIdentity(name="demo-agent"),
        audit=AuditLog(sink=audit_sink),
    )
    say("head", "lagaam — governed MCP server | agent scan budget: 25 MB", log)

    async with client_session(server._mcp_server) as client:
        for note, sql in BEATS:
            print()
            log.append({"kind": "gap", "text": ""})
            say("note", f"# {note}", log)
            say("sql", f"query_data> {sql}", log)
            start = time.monotonic()
            result = await client.call_tool("query_data", {"sql": sql})
            elapsed = time.monotonic() - start
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
            for row in data["rows"]:
                say("ok", "    " + "  ".join(str(v) for v in row), log)

    print()
    say("note", f"# Every call above is in {audit_path} — who, what, allowed/denied, why.", log)
    if args.transcript:
        with open(args.transcript, "w", encoding="utf-8") as fh:
            json.dump(log, fh, indent=2)
        print(f"{DIM}transcript -> {args.transcript}{RESET}", file=sys.stderr)


if __name__ == "__main__":
    asyncio.run(main())
