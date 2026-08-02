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
