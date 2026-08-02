# Plan-based cardinality gate

**Date:** 3 Aug 2026
**Status:** approved, not implemented
**Replaces:** the join/product/correlation half of `core/scans.py`

## Why

The cost gate must answer one question: *would this query do far more row
work than its scanned bytes suggest?* Today `core/scans.py` answers it by
parsing the SQL and testing proxies — does an equality exist, is an inline
relation under 1000 rows, is there exactly one GROUP BY key, does a LATERAL
reference an outer column.

Every proxy has a counterexample, and ten audit rounds found them one at a
time. Three examples, all measured on Trino 476:

| shape | gate says | Trino plans | error |
|---|---|---|---|
| `JOIN l ON l.linestatus = o.orderstatus` | allow | 300,875,000 rows | 400,016x |
| `... JOIN (SELECT 1 AS m ... GROUP BY l.orderkey) t ON t.m = 1` | allow | 225,000,000 rows | 299,951x |
| `LEFT JOIN LATERAL (... WHERE x = CAST(1 AS BIGINT)) ON true` | allow | product | 171,435x |

The first breaks no rule the gate has — it is an equi-join, on bare columns,
one source per side. It is unbounded only because `linestatus` has two
distinct values, which is a *statistical* fact the SQL text does not carry.

That is the root cause: **cardinality is a property of the data, and we were
inferring it from syntax.** Trino's cost-based optimizer already computes it.

## Measured basis

All numbers below are from live Trino 476 against `tpch.tiny` and the
stats-less `memory` connector. Nothing is assumed. Probe scripts and raw
output: see the measurement log committed alongside this spec.

### `EXPLAIN (TYPE LOGICAL, FORMAT JSON)` exposes per-operator rows

Each plan node carries `estimates[].outputRowCount`. Taking the **maximum
over all nodes** classified 31 of 32 shapes correctly:

| | max intermediate rows |
|---|---|
| 18 legitimate shapes | 15,000 – 240,700 |
| 13 explosion shapes | 225,000,000 – 902,625,000 |

A ~1000x separation, with no SQL parsing involved.

### Output cardinality alone is launderable — rejected

`SHOW STATS FOR (query)` gives only the *final* row count, which collapses:

| query | final rows | real work |
|---|---|---|
| cross join | 902,625,000 | 902M |
| cross join + `LIMIT 10` | 10 | 902M |
| cross join + `count(*)` | 1 | 902M |
| cross join + `DISTINCT` | 15,000 | 902M |
| cross join + `LIKE '%x%'` | NULL | 902M |

Per-operator maximum reports 902,625,000 for every one of these. This is the
whole reason the design reads the plan tree rather than the summary row.

### Estimate accuracy

Ratio of estimate to actual `count(*)`, 12 cases: 1.00x on eight, worst
over-estimate 1.50x (LATERAL), worst under-estimate 0.68x (the `linestatus`
product: 300M estimated against 440M real — still 5,000x above the healthy
join, far inside the separation margin).

### NaN is common and must fail safe

`outputRowCount` is `NaN` for opaque filters (`LIKE '%x%'`, `upper()`),
window functions, scalar subqueries, and UNNEST — 5 of 26 probes. On the
stats-less `memory` connector, column statistics are entirely absent yet
scans still report rows from split metadata (15,000) and the cross join is
still caught (225,000,000); its equi-join node, however, reports NaN.

### The planner cannot see row generators

`UNNEST(sequence(1, 10000))` plans as 60,175 rows; reality is 601 million.
`UNNEST(repeat(col, 10000))` likewise. **This is the one thing the plan
cannot replace, so the generator half of `scans.py` stays.**

## Design

### `adapters/trino/plan.py` — new, pure

```python
def max_intermediate_rows(plan_json: str) -> float | None
```

Sibling of `explain.py:parse_io_estimate`: plan JSON in, number out, no
engine SDK, never raises. `None` means "no usable estimate", which fails
safe at the budget.

Post-order walk over the plan tree:

- A node's rows are the greatest finite `outputRowCount` among its estimates.
- **A join node with no finite estimate is charged the product of its
  children's known rows.** This is what makes laundering visible: the
  `CrossJoin` above a `LIKE` filter reports NaN, and 60,175 x 15,000 is the
  902,625,000 that must be blocked.
- A non-join node with no finite estimate takes the maximum of its children
  (it cannot manufacture rows).
- The query's number is the maximum over every node.

Join node names are matched against a set (`CrossJoin`, `InnerJoin`,
`LeftJoin`, `RightJoin`, `FullJoin`, `Join`, `SemiJoin`, `IndexJoin`,
`NestedLoopJoin`, `SpatialJoin`, `ApplyNode`, `CorrelatedJoin`). An
unrecognised node name falls into the non-join branch — a conservative
default the integration test exercises against real plans.

Malformed JSON, a missing tree, or a cycle-free depth beyond a fixed cap
returns `None`.

### `core/models.py` — one field

`CostEstimate` gains `max_intermediate_rows: int | None`. Engine-neutral: an
adapter without plan estimates (Pinot, v0.2) leaves it `None`, which the
budget treats as unmeasurable.

### `adapters/trino/engine.py` — one extra call

`_estimate_cost` issues `EXPLAIN (TYPE LOGICAL, FORMAT JSON)` on the same
connection as the existing `TYPE IO` call (~30 ms, planning only) and fills
the new field.

**`_collapse_factor`, `_scaled`, `table_scan_counts` and `plan_entry_counts`
are kept.** They correct the *byte* quote, which the logical plan does not
supersede: measured on Trino 476, a 3-way self-join and a 4x-referenced CTE
each report one IO entry totalling 135,000 bytes — identical to a single
scan — so without the scaling the bytes are undercounted 3-4x. The logical
plan's per-operator rows do account for repeated reads (both shapes measure
15,000 rows, matching the single-scan case), so the scaling applies to bytes
only and the new row number is used unscaled.

### `core/budget.py` — one dial

`QueryBudget.max_intermediate_rows`, default `1_000_000_000`, env
`LAGAAM_MAX_INTERMEDIATE_ROWS`. The default sits between the measured bands:
4,100x above the largest legitimate shape, 4.5x below the smallest explosion.

`enforce_budget` gains a block following the existing fail-safe rule — low
confidence or a missing number, when the dimension is gated, is a denial.
The denial text names the number and what to change:

> This query would produce N rows at its widest point, over your budget of
> M. That is rows the engine builds internally, not rows returned, so a LIMIT
> will not help — join on a column with more distinct values, or add a WHERE
> filter before the join.

### `core/scans.py` — deletions

Deleted (the planner answers all of it): `_has_product_join`,
`_equi_join_pairs`, `_joined_sources`, `_predicate_sources`, `_conjuncts`,
`_has_ambiguous_alias`, `_pins_to_one_row`, `_pinned_body`, `_groups_on`,
`_is_bound_lateral`, `_has_nested_loop_correlation`, `_inline_cardinality`,
`_bounded_source`, `_has_inline_row_product`, `_direct_selects`,
`_reads_a_table`, `_source_alias`, and the `_MAX_INLINE_ROWS`, `_UNVISITED`,
`_IN_PROGRESS`, `_READS_TABLE` constants.

Kept because the planner cannot see generators: `_generates_rows`,
`_expands_a_bounded_value`, `_is_bounded_input`, `_func_name`,
`_func_arguments`, `_generator_rows`, `_ROW_PRESERVING_FUNCS`,
`_NOT_A_ROW_SOURCE`, `_GENERATORS`.

Kept because the byte quote still needs them: `table_scan_counts`,
`_without_cte_bodies`. `_source_parts` and `_with_clause` are kept only if a
surviving caller needs them; otherwise they go with the rest. Their
`AssertionError` tripwires against sqlglot renaming `from_`/`with_` must
survive wherever the functions do.

`has_unpriceable_shape` keeps its signature and becomes the generator check
alone. The module docstring is rewritten: it currently explains a
byte-sum-versus-rows theory that no longer describes what the module does.

These deletions close, by construction rather than by patch: F1 (GROUP BY
ordinal), F2 (LATERAL constant correlation), the `linestatus` low-cardinality
gap, the m13 mutation survivor (`_pins_to_one_row` ignoring its `on`
argument), and five of the six known false positives — all of which lived
inside the deleted code.

### `core/safety.py` — F3, a separate defect

A ~1,800-character query nested ~100 deep raises an uncaught `RecursionError`
out of `validate_query`. Reproduced: parsing succeeds and **`tree.sql()` at
`safety.py:145` blows the stack**, because sqlglot's generator recurses once
per nesting level. It escapes as a raw Python error rather than a
`SqlValidationError`.

Fix: measure AST depth after parsing and before rendering; past a fixed cap
(200, well above any human query and below sqlglot's practical limit),
raise `SqlValidationError` with what-to-change text. Depth is measured
iteratively so the check cannot itself recurse.

Two other round-10 claims did **not** reproduce and are not fixed: rendered
SQL was 1.0x input (not 275k chars) for GROUPING SETS, taking 0.04s, and a
228,918-character IN-list is refused instantly by the existing ceiling.

## Testing

No leniency. Each of these is a gate on completion, not a suggestion.

1. **Unit — `plan.py`**: captured real Trino plan JSON as fixtures, covering
   a healthy join, a cross join, a NaN-join-over-known-scans, a stats-less
   plan, malformed JSON, a null tree, and an unknown node name.
2. **Unit — `budget.py`**: the new dimension over/under/missing/low-confidence.
3. **Unit — `safety.py`**: nesting at the cap accepted, past it refused as
   `SqlValidationError` (never `RecursionError`).
4. **Integration — the 32-shape corpus against live Trino**: 18 legitimate
   shapes priced and admitted, 13 explosions blocked, 1 generator blocked by
   `scans.py`. This test is the design's warrant; it must run in CI against
   dockerized Trino.
5. **False-block rate** measured empirically on a *fresh* legitimate corpus
   (not the one the design was built from), end-to-end through the real
   in-process MCP server. Target: below the current 8.8%.
6. **Mutation testing**: revert each new guard individually and confirm the
   suite fails. A surviving mutation is a test pinning nothing — and it must
   pin the behaviour, not a proxy for it (round 8's lesson: asserting
   `max()` of a dict passed while the fix was reverted).
7. **mypy strict** clean on `lagaam.core`.

## Risks

- **NaN semantics are version-dependent.** The rule fails safe in every
  direction (NaN never reads as cheap), and the integration test runs against
  live Trino, so a change in Trino's estimator surfaces as a test failure
  rather than a silent hole.
- **The 1e9 default may not fit every deployment.** It is configurable, and
  the measured bands are documented here so an operator can reason about it.
- **One extra EXPLAIN per query** (~30 ms, planning only). Acceptable against
  a gate whose purpose is to prevent minutes of execution.

## Out of scope

The v0.2 performance cluster (query cancellation, EXPLAIN timeout, connection
reuse, logging) is untouched. This spec changes what the gate decides, not
how the adapter is operated.
