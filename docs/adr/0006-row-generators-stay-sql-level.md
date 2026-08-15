# 0006 — Row generators remain a SQL-level check

**Status:** Accepted

## Context

Plan-based gating (ADR 0004) covers every shape where rows multiply
through joins, because the planner estimates them. It cannot see rows
manufactured from an argument: measured on Trino 476,
`UNNEST(sequence(1, 10000))` over a 60,175-row table plans as 60,175 rows
and produces 601 million. `UNNEST(repeat(col, 10000))` behaves the same.

## Decision

Generator detection stays in `core/scans.py` as a sqlglot AST check — the
only SQL-shape analysis that survived the move to plan-based gating. It
flags `UNNEST`/`explode` over value-expanding functions (`sequence`,
`repeat`, `split`, ...), with an allowlist of row-preserving array
functions; an unrecognized function feeding a generator is assumed to
invent rows, because a denylist would silently miss anything sqlglot parses
as `Anonymous`.

Bounded generators — a literal array or a sequence whose length is spelled
out — are exempt individually, but their sizes multiply where they meet:
three 1000-row sequences cross-joined are a billion rows, each inside the
per-generator cap. The cap therefore also binds the *product* of bounded
generator sizes across the statement.

## Consequences

- A flagged query gets no quote (confidence "low"), which the budget
  denies with what-to-change text.
- The row-preserving allowlist must track Trino's array functions; an
  unknown-but-harmless function is over-blocked — the fail-safe direction.
- If a future Trino version starts pricing generators, this check becomes
  redundant, not wrong.
