# 0001 — Hexagonal core with a `QueryEngine` port

**Status:** Accepted

## Context

Lagaam must govern more than one engine — Trino first, a native Pinot
adapter in v0.2 — without the valuable logic (grounding, cost gating,
verification) forking per engine. Engine SDKs carry heavy dependencies and
change on their own schedules.

## Decision

The core depends only on a `QueryEngine` protocol (`list_catalogs`,
`describe_table`, `explain`, `estimate_cost`, `execute`, `dialect`).
Adapters implement it under `adapters/<engine>/`. The core never imports an
engine SDK.

## Consequences

- Core logic is unit-testable with fakes and mypy-clean in isolation;
  adapters get integration tests against dockerized engines.
- Adding an engine is one adapter, zero core changes.
- Engine-specific power must surface through engine-neutral fields:
  `CostEstimate.max_intermediate_rows` is filled by the Trino adapter from
  plan JSON, left `None` by adapters that cannot compute it — and the
  budget treats `None` as unmeasurable, failing safe rather than open.
