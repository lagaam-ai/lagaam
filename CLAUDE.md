# CLAUDE.md — Lagaam

## What this is
Lagaam ("reins" in Hindi) is an open-source, self-hosted, Kubernetes-native
**governed agent layer for the open lakehouse**. It lets companies safely run
AI agents against their data platforms (Trino, Apache Pinot, Iceberg, Flink).

Positioning: Databricks built Genie/Agent Bricks for their walled garden.
Lagaam is that capability for everyone else's data — open, vendor-neutral.

Two halves:
- **Half A — Governed MCP analytics server** (`server/`, Python): agents query
  data through MCP tools with schema grounding, pre-execution cost estimation,
  scan limits, verification, and per-agent permissions.
- **Half B — Agent control plane** (`operator/`, Go, month 3+): agents declared
  as Kubernetes CRDs with token/dollar budgets, identity, HITL approvals,
  kill switches. LLM budget proxy = LiteLLM (reused, not built) in v0.1.

## Locked architecture decisions (do not relitigate)
- Hexagonal core: `QueryEngine` interface (port) + adapters. Trino adapter
  first; native Pinot adapter second (v0.2). Core never imports engine SDKs.
  `server/src/lagaam/core/` (grounding, cost guards, verification) is the IP —
  guard its boundaries hardest.
- Token cost = METER: cumulative counting, enforced at the LLM proxy.
- Query cost = QUOTATION: predicted pre-execution from the engine's own plan
  (bytes from EXPLAIN TYPE IO, widest-operator rows from EXPLAIN TYPE
  LOGICAL), enforced inside the MCP server before running anything. Cardinality
  is read from the plan, never inferred from SQL shape — see docs/adr/0004.
  Row generators are the one exception the planner cannot see (docs/adr/0006).
- Enforcement lives at the gates (proxy, MCP server). NEVER inside the agent
  pod — the pod is semi-trusted (prompt injection risk).
- Identity is the handshake: control plane issues agent identity; MCP server
  enforces table/schema permissions against it.
- SQL handling: LLM generates in the target dialect (dialect card injected
  into prompt). sqlglot is used for parse-validation, AST safety checks
  (no SELECT *, LIMIT present, no DDL/DML), and best-effort transpile only.
  Never write a custom SQL parser.
- Monorepo. Apache 2.0. Single star magnet.

## Stack
- Half A: Python 3.12+, package manager uv. Pinned deps live in
  server/pyproject.toml — read it there, don't duplicate versions here.
- Half B: Go + Kubebuilder (controller-runtime). Local cluster: kind.
- Local dev: Docker Compose profiles (trino / pinot / kafka) — run only what
  the current task needs; laptop RAM is finite.
- LLM: AWS Bedrock (Claude) primary; keep provider-agnostic via LiteLLM.

## Conventions
- **Mutation testing must never edit the shared working tree.** Run it in a
  throwaway `git worktree`, or guard mutate/restore with a shell `trap` so an
  interrupted run cannot leave a disabled guard on disk. An in-place `cp`
  restore has already failed once: the run died mid-mutation, left
  `_MAX_BRACKET_DEPTH` disabled, raced a concurrent session, and orphaned two
  pytest processes that pinned a CPU core each for five hours. The safety
  caps are pinned by value in the tests so a stale mutation fails loudly.
- Tests required for core/ logic (pytest). Adapters get integration tests
  against dockerized engines — `tests/integration/` needs docker, and the
  default pytest run deselects it.
- Conventional commits (feat:, fix:, docs:, chore:), atomic: one logical
  change + its tests per commit. Changes land via PR, never direct to main.
- Inline comments: one line max, only for constraints the code can't show.
- Type hints everywhere; mypy clean on core/.
- Every feature framed as developer pain relief in user-facing text
  ("stop your agent from running $500 queries"), never "governance/compliance".
  Reason: OSS is adopted for convenience, bought for governance. README and
  docs must speak developer-pain language.

## Scope discipline (critical)
v0.1 = Trino adapter + schema tools + EXPLAIN cost guard + query budget +
read-only enforcement + audit log. NOTHING ELSE.
Explicitly deferred: Pinot adapter (v0.2), operator/CRDs (month 3+),
UI/dashboard, multi-tenancy, RBAC beyond per-agent allowlists, GraphRAG.
If a task expands scope, say so and push back before implementing.
