# Roadmap

One unit at a time; each unit ends in something demoable or a failing test.

## Phase 1 — Governed MCP server v0.1
- U1  Skeleton: uv project, FastMCP server, Trino in Docker (TPC-H),
      `list_catalogs` + `describe_table` tools working end to end
- U2  Schema catalog layer: cached metadata, table cards for prompting
- U3  Dialect card (Trino) + sqlglot parse-validation + AST safety checks
- U4  EXPLAIN-based cost estimation (`CostEstimate` with confidence)
- U5  Query budgets: scan/row/timeout limits enforced pre-execution
- U6  `query_data` tool: full pipeline (validate → quote → execute → cap)
- U7  Audit log (structured JSONL → pluggable sink) + per-agent identity
      via bearer token + table allowlists
- U8  Result verification pass + error messages an agent can self-correct on
- Demo: agent asks a question; one query blocked with "estimated 48GB scan,
  budget is 5GB — add a date filter"; one succeeds. Recorded as GIF.

## Phase 2 — Launch
- README as landing page (GIF up top, quickstart ≤ 5 commands)
- Benchmarks vs raw MCP wrappers: cost-guard catch rate on a bad-query suite
- First public release; collect issues, fix fast, weekly release cadence

## Phase 3 — Depth
- Native Pinot adapter (realtime tables, upserts — the signature demo:
  "agent answers on data that landed 2 seconds ago")
- Operator v0: Agent CRD + reconcile → pod + identity + limits ConfigMap
- LiteLLM proxy wiring: token/dollar meter per agent
- `kubectl get agents` demo GIF

## Explicitly not now
Dashboard/UI, multi-tenancy, SSO, non-Trino/Pinot adapters (community can),
GraphRAG layer (future premium direction), managed cloud.
