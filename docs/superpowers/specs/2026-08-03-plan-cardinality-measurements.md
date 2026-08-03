# Measured facts — Trino 476, tpch.tiny + memory (3 Aug 2026)

All numbers from live Trino. Nothing here is assumed.

## 1. SHOW STATS FOR (query) works on arbitrary SQL
Returns a summary row (column_name IS NULL) carrying row_count.
Cost: ~30 ms. Planning only, no execution.

## 2. Final row count is LAUNDERABLE — do not gate on it
| query | est final rows | real work |
|---|---|---|
| cross join | 902,625,000 | 902M |
| cross join LIMIT 10 | 10 | 902M |
| cross join + count(*) | 1 | 902M |
| cross join + DISTINCT | 15,000 | 902M |
| cross join + GROUP BY | 15,000 | 902M |
| cross join + LIKE '%x%' | NULL | 902M |

=> Gating on output cardinality is a proxy with counterexamples. Rejected.

## 3. EXPLAIN (TYPE LOGICAL, FORMAT JSON) gives PER-OPERATOR outputRowCount
The max over all plan nodes is NOT launderable: every laundering attempt
above still shows CrossJoin @ 902,625,000.

## 4. Max-intermediate-rows rule: 31/32 shapes correct
Legitimate shapes: 15,000 - 240,700 rows.
Explosions: 225,000,000 - 902,625,000 rows.
~1000x separation. Includes ALL of:
  - F1 GROUP BY 1 ordinal pin  -> 225,000,000 (CrossJoin)
  - F2 LATERAL constant corr   -> caught as join
  - linestatus low-card key    -> 300,875,000 (InnerJoin)
  - inequality join            -> 902,625,000
  - correlated inequality      -> 902,625,000 (LeftJoin)
  - all 5 laundering variants  -> 902,625,000

## 5. THE ONE FAILURE: row generators are invisible to the planner
  UNNEST(sequence(1,10000)) -> plan says 60,175. Reality: 601M.
  UNNEST(repeat(col,10000)) -> plan says 60,175.
=> _generates_rows in scans.py MUST STAY. Planner cannot replace it.

## 6. Estimate accuracy vs reality (ratio est/actual)
  healthy equi-join   1.00x      3-way star join   1.00x
  cross join          1.00x      GROUP BY 1 pin    1.00x
  filtered scan       1.00x      union all         1.00x
  selective filter    1.00x      group by high-card 1.00x
  two-predicate       1.24x      LATERAL           1.50x
  linestatus product  0.68x  (est 300M vs real 440M — still 5000x over healthy)
=> Estimates track truth closely. Worst under-estimate 0.68x, far inside
   the 1000x separation margin.

## 7. NaN / NULL is COMMON — must fail safe
NULL row_count in 5/26 probes: LIKE '%x%', upper() filter, window fn,
scalar subquery, UNNEST.
Stats-less connector (memory): column stats all NULL, but plan STILL reports
table rows (15,000) from split metadata, and still catches cross join (225M).
However the equi-join on memory goes NaN at the join node.
=> Rule: a join node whose estimate is NaN is charged the PRODUCT of its
   children's known rows. Verified this is what makes laundering visible.
   (Still true as the default. The narrow exemptions that came later are in
   ADR 0005; the "criteria absent" version in §11 was defeated — see the note
   there.)

## 8. TYPE LOGICAL JSON also carries outputSizeInBytes
Root estimate identical to TYPE IO's ({'outputRowCount': 60175.0,
'outputSizeInBytes': 2420508.0}). So bytes could come from one call — but
TYPE IO is already tested and feeds table identity, so keep both.

## 9. What this deletes from scans.py
Replaced by the planner (16 helpers):
  _has_product_join, _equi_join_pairs, _joined_sources, _predicate_sources,
  _conjuncts, _has_ambiguous_alias, _pins_to_one_row, _pinned_body,
  _groups_on, _is_bound_lateral, _has_nested_loop_correlation,
  _inline_cardinality, _bounded_source, _has_inline_row_product,
  _direct_selects, _reads_a_table, _source_parts, _with_clause,
  _source_alias
Also unnecessary: table_scan_counts + _collapse_factor in engine.py
  (per-operator rows already account for repeated reads: self-join 15,000,
   CTE-4x 15,000 — no shortfall to recover).

Kept (planner cannot do it):
  _generates_rows, _expands_a_bounded_value, _is_bounded_input,
  _func_name, _func_arguments, _generator_rows, _ROW_PRESERVING_FUNCS

---

## Addendum — measured during implementation (3 Aug 2026)

Facts discovered after the design was written, all on the same Trino 476.

### 10. The default budget the design proposed was wrong

The design set `DEFAULT_MAX_INTERMEDIATE_ROWS` to 1,000,000,000, above the
entire measured explosion band (225,000,000–902,625,000). Every attack shape
would have been admitted. Caught by the integration corpus.

Re-measured at a realistic scale, because the design corpus used only the
toy `tiny` schema:

| shape | scale | max intermediate rows |
|---|---|---|
| `lineitem JOIN orders ON orderkey` (legitimate) | sf1 | 6,001,215 |
| `GROUP BY orderkey` (legitimate) | sf1 | 6,001,215 |
| `JOIN ON l.linestatus = o.orderstatus` | sf1 | 3,000,607,500,000 |
| cross join | sf1 | 9,001,822,500,000 |
| `GROUP BY 1` ordinal pin | tiny | 225,000,000 |

Band: 6,001,215 legitimate → 225,000,000 cheapest explosion (37.5x).
Default set to **50,000,000**: 8.3x headroom above real work, 4.5x margin
below the cheapest attack, nearest the geometric midpoint (36,746,066).

### 11. A NaN join with equality criteria must not be charged a product

```sql
SELECT s.suppkey FROM tpch.sf1.supplier s
WHERE s.suppkey IN (
  SELECT ps.suppkey FROM tpch.sf1.partsupp ps
  WHERE ps.supplycost < (SELECT avg(ps2.supplycost)
                         FROM tpch.sf1.partsupp ps2 WHERE ps2.partkey = ps.partkey))
```

Estimated **12,000,000,000** rows; real execution **10,000 rows in 0.16s**.
Trino fails to propagate stats through the decorrelated plan, so the top
`InnerJoin` reports NaN and the product fallback invented the number.

The plan JSON distinguishes the two cases directly:

- laundered cross join: `{"name": "CrossJoin", "descriptor": {}}` — no
  criteria, so the product is the real cost.
- correlated semi-join: `{"name": "InnerJoin", "descriptor":
  {"criteria": "(suppkey_1 = suppkey)"}}` — a real equality key.

Charging the product only when criteria are absent fixes the false block
(12,000,000,000 → 1,200,000, admitted) with no attack regression: 14 attack
shapes including all five laundering variants remain denied.

> **Superseded — do not implement this rule.** Criteria text is a property of
> the join *key*, while the exploitable property is the NaN *estimate*, which
> a filter controls independently: wrapping the key in `substr`, or moving the
> wrapper into a `WHERE` on both sides, kept the criteria plain and
> under-quoted by 30,088x. The shipped rule is in ADR 0005 — a NaN join takes
> max-of-children only when a side is aggregation-bounded above the dirt, or
> every key resolves through the plan's own assignments to a base table column
> with no NaN-estimate leaf beneath. See `adapters/trino/plan.py`.

This does not generalise — five other correlated shapes, including TPC-H
Q17, already estimate correctly at 6,001,215.

### 12. sf100 has no statistics at all

`SHOW STATS FOR tpch.sf100.lineitem` returns NULL for every column, as does
`tpch.sf100.nation` (25 rows). Any sf100 query is therefore denied as
unmeasurable. Correct fail-safe behaviour, and the reason the docs tell
operators to run `ANALYZE`: a stats-less connector is unusable through this
gate by design.

### 13. Nesting: sqlglot breaks well before the input-size ceiling

`_MAX_SQL_CHARS` (200,000) does not bound the real hazard.

| shape | breaks at | payload |
|---|---|---|
| `SELECT x FROM (...) t` | render fails ~AST depth 263; parser ~loop 118 | 1,813 chars at depth 100 |
| `CASE WHEN ... THEN (...)` | ~bracket depth 27 | small |
| `abs(abs(...))` | ~bracket depth 45 | 263 chars |
| `ARRAY[ARRAY[...]]` | quadratic: 0.03s @10, 0.78s @15, 5.11s @18, >45s @22 | 141 chars at depth 18 |

The array shape is the worst and was invisible to a paren-only counter.
Caps chosen: `_MAX_BRACKET_DEPTH = 12` (all delimiters, string literals
skipped), `_MAX_NESTING_DEPTH = 100` (post-parse AST).

Verified after the fix: 48 attack combinations across 8 nesting shapes at
depths to 5000 — zero escapes, worst case 0.008s. Legitimate SQL measured at
bracket depth 1–4 against the cap of 12; 0 of 16 realistic queries blocked.

### 14. False-block rate

Measured on a fresh 55-query corpus written independently of the design:
**2/55 = 3.6%** (previous design: 8.8%). One was the NaN-join defect above,
now fixed; the other was sf100's missing statistics.
