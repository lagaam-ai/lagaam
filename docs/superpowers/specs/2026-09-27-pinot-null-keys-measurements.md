# Pinot null handling and segment cardinality — 2026-09-27

Closes the gap recorded in `2026-09-17-pinot-realtime-measurements.md` §6:
"Whether `cardinality` counts a null or default as a distinct value."

Setting: Pinot 1.5.1 batch quickstart (controller :9000, broker :8000).
Three throwaway OFFLINE tables, one segment each, the same 10 rows ingested
with `POST /ingestFromFile` (JSON). Lagaam never sets `enableNullHandling`,
so the column "self-join" below uses exactly the options Lagaam executes with
(`useMultistageEngine=true`); "+NH" repeats it with `enableNullHandling=true`.

| table | null handling |
|---|---|
| `u15nulls` | `tableIndexConfig.nullHandlingEnabled: true` |
| `u15nullsoff` | `nullHandlingEnabled: false` |
| `u15nullscol` | schema `enableColumnBasedNullHandling: true`; the table flag is served as `false` |

Columns (10 docs): `id` 0–9; `s_two_nulls` 8 distinct + 2 nulls;
`s_one_null` 9 distinct + 1 null; `s_null_and_default` 8 distinct + 1 null +
the literal `"null"` (a STRING column's default); `i_two_nulls` INT, 8 + 2
nulls; `i_null_and_min` INT, 8 + 1 null + the literal -2147483648 (an INT
column's default); `s_nodict` raw (no dictionary), 8 + 2 nulls;
`s_nodict_distinct` raw, 10 distinct; `s_dup` 9 values, one repeated, no nulls.

## Result — identical on all three tables

| column | cardinality | count(DISTINCT) | self-join pairs | +NH |
|---|---|---|---|---|
| id | 10 | 10 | 10 | 10 |
| s_two_nulls | 9 | 9 | 12 | 8 |
| s_one_null | 10 | 10 | 10 | 9 |
| s_null_and_default | 9 | 9 | 12 | 9 |
| i_two_nulls | 9 | 9 | 12 | 8 |
| i_null_and_min | 9 | 9 | 12 | 9 |
| s_nodict | 9 | 9 | 12 | 8 |
| s_nodict_distinct | 10 | 10 | 10 | 10 |
| s_dup | 9 | 9 | 12 | 12 |

(`+NH` differs only on `u15nulls` and `u15nullscol`; on `u15nullsoff` it equals
the self-join column, since null handling is off in the table.)

1. **`cardinality` counts distinct stored values.** A null is stored as the
   column's default value and counted once. It equals `count(DISTINCT col)`
   under the semantics Lagaam executes with, for dictionary and raw columns,
   STRING and INT, under all three null-handling configurations.
2. **A collision always lowers cardinality.** Two nulls, or a null and a
   literal default, share one stored value: cardinality 9 of 10 every time.
3. **So `cardinality == totalDocs` means every stored value is distinct.** At
   most one row is null and no literal equals the default. A self-join then
   matches each row at most once whatever the query-time null mode: 10 pairs
   for 10 rows with null handling off at query time, 9 with it on (a null
   matches nothing).

Which direction each column measured: the only unique column that actually
holds a null is `s_one_null`, a dictionary STRING column (cardinality 10 with
one null). For INT columns (`i_two_nulls`, `i_null_and_min`) and the raw
column `s_nodict` only the collision direction was measured — two nulls, or a
null beside the default, read cardinality 9; no INT or raw column holding
exactly one null was built. `id` and `s_nodict_distinct` hold no nulls.

The nullability gate on source (b) (§7 rule 14; `single_segment_unique_columns`)
therefore protects against a collision that cannot occur. It also misfires in
both directions today, measured with main 2774925 on these tables
(`SELECT a.id FROM t a JOIN t b ON a.<col> = b.<col> LIMIT 10`, widest step):

| table | id | s_one_null | s_nodict_distinct | s_two_nulls | i_null_and_min |
|---|---|---|---|---|---|
| u15nulls | 120 | 120 | 120 | 120 | 120 |
| u15nullsoff | 30 | 30 | 30 | 120 | 120 |
| u15nullscol | 30 | 30 | 30 | 120 | 120 |

On `u15nulls` even `id`, which has no nulls, is refused a key (120 = the
product 100 + inputs 20). On `u15nullscol` the gate passes although the
schema enables null handling, because it reads only the table flag. Both are
outcomes of a gate built on an unmeasured risk.

## A raw column's cardinality can be an estimate

Everything above is exact because every column holds 10 rows. It does not
carry over to raw (no-dictionary) columns in general. Pinot 1.5.1's
`NoDictColumnStatisticsCollector` — used for a raw column when
`tableIndexConfig.optimizeNoDictStatsCollection` is true, or when the cluster
config `pinot.stats.optimize.no.dict.collection` enables it, which Lagaam
cannot see — counts exactly only up to 2,048 unique values, then reports
round(HLL × 1.1) capped at totalNumberOfEntries.

Two more throwaway OFFLINE tables, one segment each, 3,000 rows: column `c`
raw and `d` dictionary, both holding `u0`..`u2799` then `"hot"` 200 times
(2,801 distinct).

| table | optimizeNoDictStatsCollection | col | hasDictionary | totalDocs | cardinality | count(DISTINCT) | self-join pairs |
|---|---|---|---|---|---|---|---|
| u15rawskew | true | c | False | 3000 | **3000** | 2801 | 42800 |
| u15rawskew | true | d | True | 3000 | 2801 | 2801 | 42800 |
| u15rawexact | false | c | False | 3000 | 2801 | 2801 | 42800 |
| u15rawexact | false | d | True | 3000 | 2801 | 2801 | 42800 |

On `u15rawskew` the raw column's cardinality equals totalDocs although 200
rows share one value. **Main 2774925 under-quotes this shape**:
`SELECT a.c FROM pinot.default.u15rawskew a JOIN pinot.default.u15rawskew b
ON a.c = b.c LIMIT 10` is quoted 9,000 at its widest step against 42,800
pairs built, because source (b) takes `c` as a key. v0.2.1 differs from
2774925 only by the move of the rule into `keys.py`, so it carries the same
under-quote (measured on 2774925, not on the tag itself). The same query on
`d`, and on both columns of `u15rawexact`, is charged the product, 9,006,000.

So source (b) reads cardinality only where it is an exact count: a column
whose metadata entry says `hasDictionary: true`, whose dictionary size is the
number of distinct stored values. A raw column is never evidence, whatever
the table config says, because the cluster config can switch the estimate on
out of Lagaam's sight. With that rule `c` on `u15rawskew` is charged
9,006,000, and `s_nodict_distinct` on the three null tables — raw, and
exactly counted at 10 rows — loses its key (120 where it was 30).
