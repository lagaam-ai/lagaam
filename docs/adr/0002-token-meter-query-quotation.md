# 0002 — Token cost is a meter; query cost is a quotation

**Status:** Accepted

## Context

An agent spends in two currencies with opposite shapes. LLM token spend is
unpredictable per call but only dangerous cumulatively — it can only be
counted as it happens. A query is the opposite: a single call can dwarf a
month of token spend, but the engine can predict its cost before running it.

## Decision

- **Token cost = METER**: cumulative counting, enforced at the LLM proxy
  (LiteLLM, reused rather than built).
- **Query cost = QUOTATION**: predicted pre-execution via EXPLAIN, enforced
  inside the MCP server before anything runs.
- Enforcement lives at the gates (proxy, MCP server), never inside the
  agent pod — the pod runs LLM-driven logic and is prompt-injectable, so it
  is semi-trusted by construction.

## Consequences

- A runaway query costs nothing: it is refused before execution, with text
  the agent can self-correct on.
- The quotation is only as good as the engine's estimates, which makes
  estimate quality a first-class concern (ADR 0004).
- Two separately configured enforcement points; the control plane
  (month 3+) writes limits to both.
