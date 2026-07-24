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
| `LAGAAM_MAX_RETURNED_ROWS` | Rows returned to the agent per query | `1000` (max `100000`) |
| `LAGAAM_QUERY_TIMEOUT` | Wall-clock seconds per query | `300` |
| `LAGAAM_METADATA_TTL` | Metadata cache TTL, seconds | `300` |
| `LAGAAM_AUDIT_LOG` | Audit JSONL file path | stderr |
| `TRINO_HOST` / `TRINO_PORT` / `TRINO_USER` | Trino coordinator | `localhost` / `8080` / `lagaam` |

**The server will not start without `LAGAAM_ALLOWED_TABLES`.** An agent that
can reach every table in every catalog is the thing this exists to prevent, so
that has to be asked for — set `LAGAAM_ALLOW_ALL_TABLES=true` if you mean it.

Cost estimates come from engine statistics — run `ANALYZE` on your tables and
the quotes get sharp. Without stats the gate fails safe and asks the agent for
a query it can price.

## How it works

A `QueryEngine` port with a Trino adapter. Every `query_data` call walks one
pipeline: **validate (sqlglot AST) → table allowlist → cost quotation → budget
gate → execute (row cap + timeout) → verify → audit**.

Details in [docs/architecture.md](docs/architecture.md); the longer story in
[docs/vision.md](docs/vision.md).

Apache 2.0.
