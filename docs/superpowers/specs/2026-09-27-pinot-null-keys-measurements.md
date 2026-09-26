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
