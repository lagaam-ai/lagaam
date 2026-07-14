# Vision

## The problem
Companies want AI agents working on their data. Today that means handing an
LLM-driven process direct access to Trino, Pinot, or a lakehouse — and hoping.

What actually happens without guardrails:
- Agents write syntactically-valid but catastrophic SQL: full scans over
  petabyte Iceberg tables, missing partition filters, no LIMIT.
- One agent loop can burn a day's query budget in minutes. Nobody notices
  until the bill.
- Agents are anonymous: no identity, no per-agent permissions, no audit
  trail of who queried what and why.
- Text-to-SQL accuracy on real enterprise schemas is poor; wrong-but-plausible
  results flow into downstream decisions unverified.
- Data-embedded prompt injection can hijack an agent that reads untrusted rows.

Platform teams respond the only way they can: they don't give agents access
at all. The most valuable data in the company stays agent-free.

## The gap
Databricks solved this inside their walled garden (Genie, Agent Bricks,
Unity Catalog governance) — and it is their fastest-growing business.
Generic AI gateways govern prompts and API keys, not queries and tables.
Existing open-source MCP servers for Trino/Pinot are thin wrappers: they
expose query tools with no cost model, no verification, no identity.

Nobody has built the **data-platform-native governance layer** for the open
stack. That is Lagaam.

## What Lagaam is
An open-source, self-hosted layer with two halves:

1. **Governed MCP analytics server** — the only door agents use to reach
   Trino/Pinot. Schema-aware grounding, pre-execution cost quotation via
   EXPLAIN, scan/row limits, result verification, per-agent permissions,
   full audit log.
2. **Kubernetes-native agent control plane** — agents as CRDs with budgets
   (tokens/dollars/day), identity, human-in-the-loop approvals for risky
   actions, and kill switches. `kubectl get agents`.

## What Lagaam is not
- Not a chat-with-your-data app. It is infrastructure other apps sit on.
- Not a Databricks competitor on their platform — it is the open-stack
  equivalent for everyone outside the walled garden.
- Not a compliance product first. It is a developer tool that stops agents
  from doing expensive, dangerous, or dumb things. Governance is the
  by-product enterprises later pay for.

## Why now
- Agent adoption is exploding; agent-on-data governance is the named blocker
  in every enterprise conversation.
- MCP is now the standard tool protocol (Linux Foundation).
- The open lakehouse (Iceberg + Trino + streaming) is the default data
  architecture outside Databricks/Snowflake.
- No open-source project owns this category yet. The window is open.
