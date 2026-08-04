# Lagaam

A governed MCP server between your AI agents and your lakehouse. Every query
is schema-grounded, priced *before* it runs, checked against a budget, and
audited — and every rejection tells the agent how to fix its SQL.

Trino today; Pinot next.

## Quickstart

```bash
docker compose -f examples/docker-compose.yml --profile trino up -d
cd server && uv sync
LAGAAM_ALLOWED_TABLES=tpch.tiny.orders,tpch.tiny.lineitem \
  uv run python -m lagaam
```

The agent gets three tools — `list_catalogs`, `describe_table`, `query_data` —
and cannot reach the engine any other way.

## Configuration

| Env var | Meaning | Default |
|---|---|---|
| `LAGAAM_ALLOWED_TABLES` | Comma list of `catalog.schema.table` grants | **required** |
| `LAGAAM_ALLOW_ALL_TABLES` | `true` to run with no grant at all | off |
| `LAGAAM_AGENT_NAME` | Identity stamped on the audit trail | `anonymous` |
| `LAGAAM_MAX_SCAN_BYTES` | Scan-bytes budget per query, pre-execution | 50 GiB |
| `LAGAAM_MAX_ROWS` | Scanned-row estimate budget per query | ungated |
| `LAGAAM_MAX_INTERMEDIATE_ROWS` | Rows the engine would *build* at its widest step — not rows returned, so a `LIMIT` doesn't lower it | 50,000,000 |
| `LAGAAM_MAX_RETURNED_ROWS` | Rows returned to the agent per query — unset, the server applies its own 1000-row cap | `1000` (max `100000`) |
| `LAGAAM_QUERY_TIMEOUT` | Wall-clock seconds per query | `300` |
| `LAGAAM_METADATA_TTL` | Metadata cache TTL, seconds | `300` |
| `LAGAAM_AUDIT_LOG` | Audit JSONL file path | stderr |
| `TRINO_HOST` / `TRINO_PORT` / `TRINO_USER` | Trino coordinator | `localhost` / `8080` / `lagaam` |

**The server will not start without `LAGAAM_ALLOWED_TABLES`.** An agent that
can reach every table in every catalog is the thing this exists to prevent, so
that has to be asked for — set `LAGAAM_ALLOW_ALL_TABLES=true` if you mean it.

**The budget dimensions above apply whether or not you set them.** Leaving
`LAGAAM_MAX_SCAN_BYTES` and `LAGAAM_MAX_INTERMEDIATE_ROWS` unset gives you
their defaults, not an open gate — an unconfigured server refuses what it
cannot afford rather than waving it through. If queries are being denied and
you expected no limits, that is why; raise the dimension you mean to raise.

Cost quotes come from the engine's own plan estimates, and those need table
statistics: a table without stats cannot be priced, so its queries are refused
rather than guessed at. Run `ANALYZE` on your tables before pointing an agent
at them — a connector that has no statistics at all is effectively unusable
through the gate. That is deliberate: a query nobody can size is exactly the
kind that runs for $500.

## How it works

A `QueryEngine` port with a Trino adapter. Every `query_data` call walks one
pipeline: **validate (sqlglot AST) → table allowlist → cost quotation → budget
gate → execute (row cap + timeout) → verify → audit**.

The quote is the engine's own plan, not a guess from the SQL text:
`EXPLAIN (TYPE IO)` prices the bytes a query would scan, and
`EXPLAIN (TYPE LOGICAL)` prices the widest row count any operator would
build — the number a cross join blows and a `LIMIT` cannot hide.

Details in [docs/architecture.md](docs/architecture.md); the longer story in
[docs/vision.md](docs/vision.md).

Apache 2.0.
