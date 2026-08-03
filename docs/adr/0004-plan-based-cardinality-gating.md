# 0004 — Plan-based cardinality gating over SQL-shape heuristics

**Status:** Accepted

## Context

The gate must catch queries whose row work dwarfs their scanned bytes. It
used to test SQL-shape proxies: does a join have an equality, is a GROUP BY
single-keyed, is an inline relation small. Every proxy had a
counterexample, found one audit round at a time. The canonical one:
`JOIN orders o ON l.linestatus = o.orderstatus` is a plain equi-join on
bare columns that breaks no shape rule, and Trino plans it at 300,875,000
rows — the join key has two distinct values. Cardinality is a property of
the data, not of the syntax, and the engine's cost-based optimizer already
computes it.

## Decision

The Trino adapter runs `EXPLAIN (TYPE LOGICAL, FORMAT JSON)` and gates on
the plan's per-operator row estimates (ADR 0005) instead of on SQL shape.
The shape heuristics were deleted: `core/scans.py` dropped from 695 lines to
258, keeping only the row-generator check the planner cannot see. It has
since grown back to size a literal `sequence()` rather than refuse it.

## Consequences

- The join/product/correlation false positives and false negatives of the
  shape rules are gone by construction, not by patch.
- The gate now depends on engine statistics: a table or connector without
  stats cannot be priced and is denied. Fail-safe, but users must run
  `ANALYZE`, and a stats-less connector is effectively unusable through
  the gate.
- Row generators, which the planner cannot see, remain a SQL-level check
  (ADR 0006).
