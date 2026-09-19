# The consuming-segment charge and join-key evidence

**Date:** 17 Sep 2026
**Status:** design, approved for implementation
**Measured basis:** `2026-09-17-pinot-realtime-measurements.md` (Pinot
1.5.1, live), on top of `2026-09-11-pinot-measurements.md`. Every number
below cites a rule or section of the realtime log; nothing is assumed.
Extends the shipped design in `2026-09-11-pinot-adapter-design.md` and
ADR 0008.

## Why

Two shapes are denied today, both honestly, and both are Pinot's signature
use.

**A REALTIME half is quoted low.** `quote.quote` sets `scanned_bytes` to
`None` whenever any table has a REALTIME half, so `confidence` is `"low"`
and the gate denies. It has to: a consuming segment reports `totalDocs: 0`
and `reportedSizeInBytes: -1`, and charging those as written would quote the
freshest data in the table free — measured, ten probes over 30 s while the
segment held 50 live docs never moved off zero (log §1.3, rule 7). So a
query over data that landed seconds ago — the reason to run Pinot — cannot
be priced, and cannot be admitted.

**Every join is the product.** `plan.py` charges a join `product + sum` of
its inputs, unconditionally, because ADR 0008 recorded that 1.5.1 exposes no
cardinality to prove a key. That is right about the *plan* — its Calcite
rowcounts are a constant 100 per scan — but it is not right about the
*catalog*. Two kinds of proof do exist and are now measured: an upsert
table's primary key, and a column whose per-segment `cardinality` equals its
`totalDocs` on a single-segment table. Without them, `a JOIN b ON a.pk =
b.pk` on a 100-key table is quoted 10,000 + 200 intermediate rows where the
engine builds 100 (log §2.1) — a denial under any default budget, of a query
that is a lookup.

Both denials are safe. Both make the engine unusable for what it is for.
This unit replaces each with a number that is still an upper bound.

## Locked decisions

### 1. The consuming-segment charge

`TableFacts` gains two fields.

**`consuming: int`** — how many CONSUMING segments the table has. Read from
`GET /tables/{t}/externalview`, counting entries of the `REALTIME` map whose
server-state map contains the string `"CONSUMING"` for any server (log
§1.2). Cross-checked against `/tables/{t}/size` →
`realtimeSegments.missingSegments`, which counts exactly the consuming
segments (rule 6: 1 with one, 2 with two). **A disagreement is charged at
the larger of the two.** Neither number is more authoritative than the
other — externalview is the state of record but can lag, and
`missingSegments` also counts a segment a server failed to report — so the
larger one is the only reading that cannot under-charge. An externalview
that cannot be read at all leaves `missingSegments` as the count; neither
readable leaves `consuming = 0`, which is the pre-U12 behaviour and is
caught by decision 4's completeness check when it matters.

**`flush_rows: int | None`** — the row bound on one consuming segment. Read
from the table config in this precedence, first hit wins:

1. `REALTIME.ingestionConfig.streamIngestionConfig.streamConfigMaps[0]`,
   key `realtime.segment.flush.threshold.rows`;
2. the same map, key `realtime.segment.flush.threshold.size` — the
   deprecated spelling, which Pinot reads as a row count and which **the
   bundled quickstart config actually uses** (rule 3);
3. `REALTIME.tableIndexConfig.streamConfigs`, the legacy top-level
   location, same two keys in the same order — absent on every config
   measured (log §1.1), read only so an older cluster is not silently
   unbounded.

Values are JSON **strings** in every config measured (`"100"`, `"50000"`),
so the parse accepts a string of digits or an int. **A value that is not a
positive int — a float, a byte-suffixed size, a negative, zero, an
unparseable string, or absent — is `None`.** Pinot rejects a config setting
more than one row bound (rule 3), so the precedence list resolves a
conflict that cannot occur; it exists to pick a location, not a winner.

**Sealed realtime segments are priced exactly like OFFLINE ones.** Their
metadata is complete and shape-identical — `totalDocs`, `startTimeMillis`,
`endTimeMillis`, and a `columns[]` carrying `indexSizeMap` (rule 1, log
§1.3). `segment_facts` already parses them correctly. Exactly one of the
seven captured entries — the consuming segment — lacks `columns`; the
other six carry it. `_segment_sizes` likewise already merged both
`offlineSegments` and `realtimeSegments` before this unit, so sealed
realtime bytes needed no new reader.

**The consuming segment's own metadata entry is dropped from the sealed
set.** Today `segment_facts` turns it into a `SegmentFact(docs=0,
total_bytes=None)` — `_positive_int(-1)` is `None` — and that single `None`
is what makes `surviving_bytes` return `None` for the whole table, which is
the mechanism behind the current REALTIME denial. A segment is recognised as
consuming by rule 5's conjunction, all four checked together: `totalDocs ==
0` **and** no `columns` key **and** `crc == -9223372036854775808` **and**
`reportedSizeInBytes == -1` in the size report. Dropped entries are not
counted toward `consuming` — that count comes from externalview and
`missingSegments` (above), so a metadata entry the size report happens not
to name cannot inflate it. An entry that looks unreadable but does **not**
satisfy all four is kept in the sealed set with its `None`s intact, and
poisons the sum exactly as it does today: an unreadable sealed segment is
not a free one.

**Rows** for a table with a REALTIME half are then:

```
rows = k-largest-by-docs over the SEALED segments, k = sealed_surviving
     + consuming × flush_rows
```

where `sealed_surviving = numSegmentsQueried − numConsumingSegmentsQueried`,
both read from the **same** single-stage EXPLAIN response the pruning oracle
already issues (rule 9; `numSegmentsQueried` includes consuming segments, so
subtracting is the only way to get a k that applies to sealed ones).
`numConsumingSegmentsQueried` absent or unreadable → treated as 0, which
leaves k larger and charges more. No EXPLAIN at all → k is all sealed
segments, as today.

The consuming term is added **unconditionally**, outside the k-largest
logic. Measured, `numConsumingSegmentsQueried` stayed 1 under both
`DaysSinceEpoch > 99999` and `DaysSinceEpoch < 1` — filters excluding every
possible value — while the broker pruned 6 of 7 segments (rule 8). A time
predicate can never remove a consuming segment's cost, so the oracle must
never be allowed to prune it away.

**The limit prune is trusted only under an explicit outermost LIMIT and no
OFFSET.** `numSegmentsPrunedByLimit` is computed by the single-stage
EXPLAIN under Pinot's *own* implicit `LIMIT 10` when the statement carries
none, while the multi-stage engine that runs the query scans everything:
measured, a LIMIT-less select quoted 200 against 600 rows scanned on the
realtime table and 422 against 9,746 on the batch one — a 23x breach at
`confidence="high"`. `names.has_offset` gains a sibling `has_limit`, and
`trust_limit_prune = has_limit(sql) and not has_offset(sql)`. Where the
prune is not believed, both `ByLimit` and `ByServer` are ignored —
`ByServer` is the server-side total `ByLimit` breaks down (measured, 30 and
30 of 31 together), so crediting it would reinstate the prune being
distrusted — leaving `ByValue` alone. In production
`validate_query` always injects a LIMIT before the port is called, so the
pipeline was never exposed — but a port must not depend on its caller for
a bound.

**`flush_rows` is `None` while `consuming > 0` → rows are `None` → the
quote is low.** There is no fallback: rule 7 is that a consuming segment
never reports anything, so nothing else in the catalog bounds it.
`consuming == 0` makes `flush_rows` irrelevant and the table is priced on
sealed segments alone — which is what a pure-OFFLINE table always was.

### 2. The consuming-segment bytes — the one extrapolated number

Say it plainly: **this is the only number in a Lagaam quotation that is a
projection rather than a measurement.** A consuming segment reports `-1`
bytes on every probe and has no `columns` block, so no measurement of its
bytes exists to read (log §6: the size-threshold path is unmeasured, and the
row-threshold path gives rows, not bytes).

```
consuming_bytes = consuming × ceil(flush_rows × max_ratio)

max_ratio = max over the table's SEALED segments with docs > 0 of
              (bytes charged for the referenced columns in that segment)
              ÷ (that segment's totalDocs)
```

The ratio is taken **per segment and then maximised**, never averaged, and
never computed from table totals. The numerator follows the same column
attribution the sealed charge uses, decided per segment: a segment carrying
every referenced column contributes its summed `indexSizeMap` bytes for
those columns; a segment that falls back to whole-segment pricing
contributes its `reportedSizeInBytes`. Mixing is fine — the maximum over a
mixed set is still ≥ every member — but the fallback ratio is the larger by
orders of magnitude and will dominate when any segment falls back, which is
the fail-safe direction.

Measured on the six sealed `airlineStats` realtime segments (rule 20):
whole-segment ratios 850.12 … **871.36** bytes/doc, a 2.5% spread; for the
two-column `Carrier` + `DaysSinceEpoch` projection, 0.61 … **1.14**
bytes/doc. At the measured threshold of 100 rows that is 87,136
whole-segment bytes or 114 column bytes per consuming segment.

The product is **ceiling-divided** — `ceil` on the whole product, computed
in integer arithmetic as `(flush_rows × numerator + docs − 1) // docs` per
candidate segment, so no float rounding can shave a byte off the bound.

**No sealed segment with `docs > 0` → bytes are `None` → the quote is
low.** A table that is all-consuming has no ratio to take, and inventing one
would be a guess. **So does any sealed segment whose doc count is missing
while `consuming > 0`**: a segment left out of the maximum could be the
densest on the table, and the ratio would then bound nothing. Today the row
path denies on the same segment, but the byte bound has to be defensible on
its own.

**`quote.quote`'s blanket REALTIME guard goes.** The shipped line — `None if
realtime else …`, which withholds `scanned_bytes` the moment any table has a
REALTIME half — was the placeholder this unit replaces, and its own comment
says so. It is removed, not weakened: a REALTIME table now reaches
`confidence="high"` on the strength of the sealed charge plus the
consuming charge, and falls to `"low"` only through the specific `None`s
above — no `flush_rows`, no ratio, incomplete metadata. The type of the
table stops being a reason on its own.

ADR 0009 records this as the only projection in the quotation, and why it is
accepted: a consuming segment lives in memory and is bounded in rows
*exactly* by the flush threshold (rule 2 — 25 of 25 sealed segments held
exactly 100 docs at threshold 100, so the seal is at the threshold and not
below it), and the ratio is the worst observed **on the same table and the
same columns**, not a constant or a cross-table average. The bound can only
fail if a consuming segment's per-row encoding is worse than every sealed
segment of the same table — which the 2.5% observed spread makes implausible
but does not exclude. That residual risk is why it is recorded in an ADR
rather than left in a docstring.

### 3. Hybrid tables

A hybrid table has both an `OFFLINE` and a `REALTIME` key in `GET
/tables/{t}` — `metadata.table_types` already returns both.

Both halves' sealed segments come from the **same** `/segments/{t}/metadata`
response (the endpoint is per table, not per half) and from **both** halves
of `/tables/{t}/size` — `_segment_sizes` already reads `offlineSegments` and
`realtimeSegments` both. They are charged as **one segment set**, k-largest
across the union, plus the consuming charge from decisions 1 and 2. There is
no per-half arithmetic and no attempt to attribute a predicate to one half:
the segment set is the segment set, and charging the k largest across it is
an upper bound on any k that could survive, exactly as it is for a single
half.

`flush_rows` is read from the `REALTIME` half's config only; an OFFLINE half
has no stream config and no consuming segments.

**This is not measurable locally.** `-type HYBRID` is broken in a container
on 1.5.1 — it shells out to the `docker` CLI to start Kafka and dies without
a mounted socket (log §0, and the 2026-09-11 log §10). So decision 3 is
covered by **unit tests over captured JSON**: a synthesised size report
carrying both `offlineSegments` and `realtimeSegments`, built from the real
`01-tablesize.json` and an OFFLINE size report from the 2026-09-11 fixtures.
The ADR states that the hybrid path is unit-tested and not live-tested, so a
reader knows the difference.

### 4. Metadata completeness is a precondition

**The set of segment names in `/segments/{t}/metadata` must cover every
sealed segment `/tables/{t}/size` lists** — across both the
`offlineSegments.segments` and `realtimeSegments.segments` keys. A sealed
segment named by the size report and missing from the metadata response
makes **rows and bytes both `None`**, so the quote is low and the gate
denies.

"Sealed" here is decided from the size report itself: a segment whose
`reportedSizeInBytes` is `-1` is consuming and is expected to be absent from
a useful metadata entry (it is present but empty — rule 5), so it is exempt.
Every other named segment must appear.

Measured (log §4): on a 2-server table `/segments/{t}/metadata` returns
**one server's half, and alternates which half between identical calls** —
three consecutive calls on `u12plain` returned partition 0's two, then
partition 1's two, then partition 0's again. The captured
`02-upsert-segmetadata.json` has it frozen: two entries, both from partition
0, against four in `02-upsert-size.json` and `numSegments: 4` in the table
metadata.

**A table whose segments span servers is therefore denied**, and the fix is
not to relax the check but to remove the cause: the `pinot-realtime`
profile's tables use a single-partition topic and a single replica group,
so each lives on one server and its metadata response is complete. The
STREAM quickstart runs four servers, and a two-partition topic put the
upsert pair's segments on two of them — both tables denied, correctly, and
the join demo unobservable until they were pinned. Fetching each segment's
metadata individually is what would lift the denial for a genuinely
multi-server table, and that is U13.

**This fixes a latent under-quote on the OFFLINE path too**, and that is the
more important half of this decision. The shipped `surviving_docs` sums
`totalDocs` over whatever segments it was handed and returns a confident
number; on any multi-server OFFLINE deployment it would have been summing
half the table at `confidence="high"` — an under-quote that admits the query
the gate exists to stop. The batch instance is single-server, which is why
the 2026-09-11 spike never saw it. The check applies to every table, both
halves, regardless of type.

### 5. Join key evidence

Two pieces, both needed: resolving a join's equality operands to column
names, and knowing which column sets are unique.

**Operand resolution** (`plan.py`, pure). For a join node:

- Its `condition` yields candidate equalities: **only a top-level `EQUALS`,
  or an `EQUALS` directly under a top-level `AND`**. An `OR` anywhere, a
  `NOT`, a nested `AND` under an `OR`, or any other `op.kind` yields no
  evidence at all. (ADR 0008's `OR` case stands: an `OR` of equalities is
  not a key.)
- Each `EQUALS` has two operands, each an `{"input": n}` index. **`n`
  indexes the concatenation left-fields ++ right-fields** — verified on the
  two-key case where right `teamID` at right-index 1 resolved to global
  index 3 = 2 left fields + 1 (rule 16, log §5.1). The right-side `fields`
  list is alphabetised rather than in ON-clause order, so only the index may
  be used, never the position in the SQL.
- **A side's field list is the `fields` of the `LogicalProject` at the top
  of that side.** A scan node carries no `fields` at all; the names come
  from the project immediately above it, reached by walking down from the
  join through the exchange (rules 16 and the scan shape in §5.1).
- **A side yields evidence only when it is a straight chain** — project,
  exchange and filter nodes only — down to **exactly one** scan. A side
  containing a join, a union, an aggregate, a correlate, a second scan, or
  any node type not on that list yields nothing, because the rows reaching
  the join are then not that table's rows and its key says nothing about
  them.
- **An operand naming a `$f`-prefixed field, or one whose corresponding
  `exprs` entry carries an `op` key rather than a bare `input`, is an
  expression and yields nothing** (rule 17: `upper(a.Carrier)` became
  `$f85` with an `op` of `UPPER`). Both markers are checked; either one
  disqualifies the operand.
- Each surviving equality contributes an ordered pair of **scan ordinals**,
  one per side. Ordinals that cannot be resolved — index out of range, a
  missing `fields` list, a non-bare `exprs` entry — contribute nothing.

**A projected name proves nothing; only the ordinal does.** This is the
correction the implementation forced. A `LogicalProject`'s field names are
whatever the SQL called the columns, so `SELECT Origin AS Carrier` projects
a field named `Carrier` reading ordinal 62 while the real `Carrier` is 18 —
indistinguishable by name, and with the `Carrier` key it quoted 205,524
against a true 954,133,829 (live capture). Nor is the ordinal the schema
position: `Carrier` is ordinal 18 and schema column 14 of 81, and
`baseballStats`' `teamID` is ordinal 26 on a 25-column schema, so no static
mapping exists.

So the engine **learns each keyed table's key-column ordinals from the
engine itself**, with one extra multi-stage EXPLAIN per keyed table:
`EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT <key columns>
FROM db.t`, parsed by a pure `plan.key_ordinals(plan_json) -> dict[str,
int] | None` reading its top project (fields → bare `exprs[i].input`, on a
straight chain to exactly one scan; anything else `None`). Every identifier
that EXPLAIN interpolates — database, table and each column — must be a
bare identifier, and any that is not forfeits that table's evidence.
`max_intermediate_rows` gains `key_ordinals: Mapping[str, Mapping[str,
int]] | None`, keyed lowercase table → lowercase column → ordinal.

In the query plan the walker composes each operand's index **down** the
side's chain — a project maps an index to its bare `exprs[index].input`,
filters and exchanges pass it through — to the scan ordinal, and the
operand names key column `c` only when that ordinal equals
`key_ordinals[table][c]`. The equated column set is built from those
matches and never from field names. No ordinals for a table → no evidence
for it. Fail-closed on every doubt: a non-bare expr, a missing or non-list
`exprs`, a length mismatch, an index out of range. The alias spoof composes
to 62 ≠ 18 and is refused.

**Evidence is one-sided.** Each side is judged alone. The operand split
index comes from the **left** side's top node when that node is a
`LogicalProject` — its `fields` are the join's left column list whatever
lies beneath — and when the left top node carries no `fields` at all (a
bare join or exchange at the top), there are no pairs. A side that is not a
straight chain to exactly one scan has no table and can never be covered,
but it does not silence the other side: a keyed table joined to a join is
still evidence about the keyed table's own matches.

**An expression operand likewise silences only its own side.**
`upper(a.Carrier) = b.teamID`, with `teamID` a proven key on `b`, still
bounds the join to left + inputs: each left row matches at most one right
row whatever its expression value.

**Unique-key evidence** (`engine.py` gathers, `plan.py` consumes).
`max_intermediate_rows` gains a parameter
`unique_keys: Mapping[str, frozenset[frozenset[str]]]`, keyed by lowercase
`database.table` exactly as `leaf_docs` already is, whose value is the set
of column sets known unique on that table. Two sources:

**(a) An upsert table's primary key.** When the table config carries an
`upsertConfig` object **that keeps the key unique**, the schema's
`primaryKeyColumns` is a unique key set.

*Corrected after review.* "Any mode" was wrong twice, and both ways were
reproduced live on 1.5.1. `mode: NONE` is a valid `Mode` value that disables
upsert entirely while the object stays in the config. And a **set**
`metadataTTL` or `deletedKeysTTL` evicts a key from the primary-key lookup
map while the rows it pointed at stay queryable, so a key re-ingested after
its window has two visible rows. Measured on table `u12ttl`
(`mode: FULL, metadataTTL: 60000, enableSnapshot: true`): 8 rows, 7 distinct
`pk`, `GROUP BY pk HAVING count(*) > 1` returning `K1 -> 2` with **both
versions visible**, and `a JOIN b ON a.pk = b.pk` returning **10 pairs**
where a unique `pk` gives 8 — while the adapter cut the widest join step
from 80 to 24 on the strength of that key. The gate is therefore: `mode`
(case-insensitive) is `FULL` or `PARTIAL`, **and** neither TTL is greater
than zero; anything else yields no evidence.

**Zero is not a TTL.** 1.5.1's `isTTLEnabled()` is
`_metadataTTL > 0 || _deletedKeysTTL > 0` and `isOutOfMetadataTTL` returns
false outright at `_metadataTTL <= 0` (bytecode, `BasePartitionUpsertMetadataManager`),
and the controller materialises `metadataTTL: 0.0, deletedKeysTTL: 0.0` on
every upsert config it serves — including `u12upsert`'s. Reading a zero as a
TTL would withhold the key from every upsert table there is. A TTL value
that is not a number the adapter can read as one is treated as set, because
a value it cannot interpret is not one it may clear a key on.

`upsertPartitionToServerPrimaryKeyCountMap` stays a "this is an upsert
table" signal and nothing more: on `u12ttl` it reported **6** against 8 rows
and 7 distinct visible keys — wrong in both directions — while staying
non-empty.

The schema is a separate document — `GET /schemas/{schemaName}`, where
`schemaName` is the table config's `segmentsConfig.schemaName` when present
and **the controller's own spelling of the table name** otherwise.
`segmentsConfig.schemaName` was absent on all four configs captured, so the
fallback is the path that will normally run, and it must use the resolved
listing spelling `engine.py` already computes — never the config's
`tableName`, which comes back suffixed (`"u12upsert_REALTIME"`,
`"airlineStats_OFFLINE"`) and would 404. Measured: `GET /schemas/u12upsert`
returns `schemaName: "u12upsert"`, `primaryKeyColumns: ["pk"]`.

The **full** column list is the key, never a subset: `primaryKeyColumns` is
unique as a tuple and any proper subset of a composite key need not be. A
`primaryKeyColumns` that is absent, empty, or not a list of non-empty
strings yields no evidence, even with an `upsertConfig` present.

*Measured (rule 11, log §2.1): `GROUP BY pk HAVING count(*) > 1` returns
nothing on the upsert table and 6-version rows on a byte-identical
non-upsert table reading the same topic; `a JOIN b ON a.pk = b.pk` returns
exactly 100 = the distinct-key count, against 3,600 for the plain twin.*

`upsertPartitionToServerPrimaryKeyCountMap` being non-empty is the
belt-and-braces confirmation — it is `{}` on every non-upsert table, so a
non-empty map is itself proof of upsert (rule 12). It is read and, when it
disagrees with the config (a non-empty map on a table whose config shows no
`upsertConfig`, or the reverse), **the evidence is withheld**: two documents
disagreeing about what a table is, is not a basis for admitting a join. It
is never used as a *count* — see decision 6.

**(b) A single-segment table's unique columns.** For a table with **exactly
one sealed segment and no consuming segment**, any column whose per-segment
`cardinality` equals that segment's `totalDocs` is a single-column unique
key.

**The metadata response alone cannot establish "exactly one sealed
segment"** — it is the document decision 4 documents as truncated on a
multi-server table, so a 2-segment table would read as single-segment. The
**size report** is the independent count: source (b) yields nothing unless
`metadata_is_complete(seg_metadata_json, size_json)` holds, the size report
names exactly one sealed (non `-1`) segment, and that segment is the one
sealed metadata entry. Source (b) therefore takes `size_json` as a fourth
document.

**Any `-1` entry in the size report voids source (b) outright.** A
consuming segment can be invisible to the metadata response entirely —
named only by its `-1` size entry, with no body on the metadata side — and
`metadata_is_complete` does not catch that, since it only requires every
*sealed* name to be present. Such a segment means the table holds rows the
one sealed segment does not, so the column is not a table key whatever the
metadata says.

**A multi-value column is never evidence.** Its `cardinality` counts
distinct *entries*, not rows, so equality with `totalDocs` proves nothing —
and `airlineStats` carries nine such columns. A column is excluded when its
`fieldSpec.singleValueField` is `false`, or its segment entry's
`totalNumberOfEntries` differs from `totalDocs`, or its
`maxNumberOfMultiValues` is above 0.

*Measured (rule 14, log §3.1): `cardinality` is exactly
`count(DISTINCT col)` — 18,107 / 149 / 7 / 67 against the engine, all
exact.* Equality with `totalDocs` can only hold when every doc carries a
distinct value: `count(DISTINCT col) ≤ count(col) ≤ totalDocs`, so equality
forces every doc to be counted and every value to differ. The
one-segment-one-table condition is what makes the segment's distinct count
the table's: rule 15 shows why anything else fails — summing per-segment
`Carrier` cardinality over 31 segments gives 432 against a true
`distinctcount` of 14.

**The null caveat is live** (log §6). Every column measured had `count(col)
== totalDocs`, so whether `cardinality` counts a null or default as a
distinct value was never exercised. If it does, a column with one null row
could report `cardinality == totalDocs` while two rows share the default
value — and the join would be admitted on a key that is not one. Source (b)
is therefore **gated on the table's null handling**: the column's
`fieldSpec.notNull` is true, or the table config's
`tableIndexConfig.nullHandlingEnabled` is false and the column is not
nullable in the schema. Where neither can be established, source (b) yields
nothing. Source (a) is unaffected — an upsert primary key cannot be null.
Closing the caveat properly means measuring a table with real nulls, which
is U13 or later; until then (b) is deliberately hard to trigger, and it
finds nothing on the quickstart data anyway (rule 14: 0 of 2,604 per-column
entries).

**The join rule.** At a join node with resolved equality pairs:

- If the **left** side's equalities cover a unique key set of the left
  side's table — i.e. some key set in `unique_keys[left_table]` is a subset
  of the columns the left ordinals in the pairs resolve to, by ordinal
  match against `key_ordinals[left_table]` — then each right row
  matches at most one left row, so the join emits at most `right` rows:
  charge **`right + left + right`** (the inputs added on top, for the same
  reason ADR 0008 adds them: an outer join emits unmatched rows above the
  matched ones, and the additive term is exact-safe on a side of 0 or 1
  rows).
- Symmetrically if the **right** side's equalities cover a unique key of the
  right side's table: charge **`left + left + right`**.
- If **both** sides do: charge **`min(left, right) + left + right`**.
- Otherwise: **`left × right + left + right`**, exactly as today.

"Cover" means subset, not equality: a join equating more columns than the
key still has at most one match per key value.

*Sanity against the measurement: the upsert self-join has both sides unique
on `pk`, 100 rows each, so `min(100,100) + 100 + 100 = 300` against the 100
the engine built — a bound, not the answer. The plain twin has no evidence
and is charged `600 × 600 + 1200 = 361,200` against 3,600 built. The
product rule survives where it must.*

**Semi-joins and aggregated join inputs stay denied.** `EXPLAIN … AS JSON`
fails on both with errorCode 450 and `resultTable: null` — any plan with a
`PIPELINE_BREAKER` exchange cannot be serialised on 1.5.1 (rule 18, log
§5.2). `max_intermediate_rows` receives no parseable plan and returns
`None`, which it already does. Nothing in this unit tries to read a text
plan.

### 6. The PK-count map is informational only

On an upsert table, sealed `totalDocs` over-counts: `u12upsert__0__0__…`
reports 200 against a table-wide `count(*)` of 100 (rule 13, log §2.2).
**`totalDocs` is kept as the row bound**, unchanged — it is safe, being an
over-count, and replacing it with the distinct-key figure would be
substituting a smaller number into an upper bound.

`upsertPartitionToServerPrimaryKeyCountMap` is read in this unit only as the
upsert confirmation of decision 5(a). It is **not** charged as a doc count.
Two reasons: it is per *server*, and with replication > 1 naive summation
across servers would double-count (log §6, unmeasured); and it counts
distinct keys, not rows scanned, so it does not bound the work a query does
even where it is exact.

### 7. Environment and tests

**A `pinot-realtime` compose profile** in `examples/docker-compose.yml`,
reproducing log §9: `apache/kafka:3.9.0` in KRaft mode with
`KAFKA_LISTENERS` on the service hostname (not `0.0.0.0` — the entrypoint
copies it into `advertised.listeners` and dies on the meta-address), on its
own compose network; `apachepinot/pinot:release-1.5.1` with `QuickStart
-type STREAM -kafkaBrokerList <kafka>:9092`, `JAVA_OPTS=-Xms512M -Xmx2G
-XX:+UseG1GC`, host ports **9001** (controller) and **8001** (broker). The
non-default ports are deliberate: the OFFLINE integration suite needs the
batch profile on 9000/8000 at the same time, and §8 of the log shows both
Pinot instances plus Kafka and Trino fit in 7.653 GiB at ≈6.4 GiB — with
thin enough headroom that the heap cap is not optional. Pinot's service
declares a dependency on Kafka's health check, since a Pinot that starts
before Kafka is reachable falls back to its own embedded broker.

**A bootstrap script** — `examples/pinot-realtime/bootstrap.sh`, or a small
Python script under `examples/` if the retry logic outgrows shell — which
creates the two topics, posts the realtime `airlineStats` schema and config
(flush threshold lowered to `.rows: "100"`, broker list pointed at the
compose service), posts the `u12upsert` and `u12plain` schemas and configs,
and streams the sample data from
`/opt/pinot/examples/stream/airlineStats/rawdata/airlineStats_data.json`
plus the keyed upsert feed. `tenants: {}` is required on every POSTed table
config (log §2). The script is idempotent: a table that already exists is
left alone.

**Integration tests** under a `pinot_realtime_ready` fixture, beside the
existing `pinot_ready` in `server/tests/integration/conftest.py` and built
the same way: probe the controller at `http://localhost:9001/health`, **skip
rather than fail** when it is unreachable, then poll the broker at
`http://localhost:8001` until the realtime table answers a positive
`count(*)` **and at least one segment is sealed** (an `ONLINE` entry in
`/tables/airlineStats/externalview`). Both conditions are needed: a table
that answers a count may still be all-consuming, and a quote against an
all-consuming table exercises the `None` path rather than the charge.

**E2E demo**, three assertions:

1. A query over rows that landed seconds ago is quoted and **runs under the
   default budget** — 600 rows quoted against 600 scanned. Today it is
   denied.
2. The upsert-PK self-join `u12upsert a JOIN u12upsert b ON a.pk = b.pk` is
   **admitted**, quoted 2,400.
3. The same join on the non-upsert twin `u12plain` is **denied** — same
   topic, same rows, same shape, no key evidence — quoted 641,600 on "rows
   at its widest step", where the engine builds 100 true pairs.

Assertion 3 is the one that shows the evidence is doing the work and not the
shape, and it **cannot be shown under the default budget**: the twin's
product on these 800-doc tables is 641,600, far below the 50,000,000
default, so the default would admit it. The join pair therefore runs under
an explicit `QueryBudget` with `max_intermediate_rows=10_000` and scan
bytes and timeout left at their default constants — the row ceiling is the
only dimension key evidence moves, so it is the only one narrowed.
Assertion 1 keeps the default budget throughout.

### 8. ADR 0009, and a correction to ADR 0008

**ADR 0009 — "A consuming segment is charged at its flush threshold, and a
join key must be proved by the catalog"** records decisions 1, 2 and 5: the
unconditional consuming charge and why a time filter cannot remove it; the
bytes projection as the single non-measured number in the quotation, with
the argument for accepting it and the residual risk; and the two sources of
key evidence with the null caveat gating source (b). It also records that
the hybrid path (decision 3) is unit-tested over captured JSON and not
live-tested, because `-type HYBRID` cannot run in a container on 1.5.1.

**ADR 0008 is corrected.** Its clause excluding `ByBroker` and `Invalid`
because "neither was observed non-zero and neither is known to be a
breakdown of `numSegmentsQueried`" is half false on 1.5.1. On a REALTIME
table with `routing.segmentPrunerTypes: ["time"]`, `ByBroker` is **3 and 6**
on the two time-filtered EXPLAINs, and the only non-zero pruning counter in
those rows (log §1.6). The correction replaces the justification, not the
decision:
`numSegmentsQueried` there is **already net of** broker pruning (4 = 7−3,
1 = 7−6), so subtracting `ByBroker` again would under-charge. **The decision
not to read it stands**, on the stronger ground that it is not a breakdown
of `numSegmentsQueried` but a deduction already applied to it. ADR 0008's
"Consequences" line about a broker-pruned partitioned table being
over-charged "until `numSegmentsPrunedByBroker` is measured against a table
that actually trips it" is likewise updated: it has now been measured, and
the over-charge is permanent and intended.

### 9. Core gains nothing

No new hint codes, no core changes. Every number here is an adapter number
feeding the existing `CostEstimate`, and every failure is the existing one:
a bound nobody could compute is `None`, `confidence` is `"low"`, and the
budget gate denies. ADR 0001 holds.

## Architecture and data flow

```
metadata.py   PURE, +
                consuming_count(externalview_json, size_json) -> int
                flush_rows(config_json) -> int | None
                upsert_keys(config_json, schema_json, table_metadata_json)
                                             -> frozenset[frozenset[str]]
                single_segment_unique_columns(seg_metadata_json, config_json,
                                              schema_json, size_json)
                                             -> frozenset[frozenset[str]]
                metadata_is_complete(seg_metadata_json, size_json) -> bool
                upsert_config_present(config_json) -> bool
              TableFacts gains: consuming, flush_rows, complete, unique_keys

response.py   PURE, + consuming_segments_queried(explain_json) -> int
                reads numConsumingSegmentsQueried beside the pruning
                counters it already reads, from the same response;
                surviving_segments() gains trust_limit_prune

names.py      PURE, + has_limit(sql) -> bool, beside has_offset

quote.py      PURE, + the consuming charge in surviving_docs/surviving_bytes
                and the bytes-per-doc ratio; the completeness gate in quote()

plan.py       PURE, + operand resolution (join condition -> scan-ordinal
                pairs per side), the unique_keys and key_ordinals
                parameters on max_intermediate_rows(), and
                key_ordinals(plan_json) -> dict[str, int] | None;
                the join arithmetic of decision 5

engine.py     + controller documents per table, fetched once each:
                /tables/{t}/externalview, and /schemas/{schemaName} and
                /tables/{t}/metadata for the key evidence (the schema is
                memoised, so it is read once per table per quotation);
                one extra keycols EXPLAIN per table with proven keys;
                composition of the above; passes unique_keys, key_ordinals
                and leaf_docs into the plan walk

client.py     unchanged
```

`upsert_keys` takes three documents rather than two: the PK-count map in
the table metadata is the second document that has to agree that the table
is an upsert table. `single_segment_unique_columns` takes four: the schema
is what the nullability gate reads, and the size report is the independent
segment count decision 4 showed the metadata response cannot supply.

**Purity is unchanged.** Only `engine.py` and `client.py` touch the network;
every module above takes parsed JSON and returns domain values, and is unit
tested against the spike's captures. The new fetches are per table per
quotation, alongside the four already made (`/tables/{t}`,
`/tables/{t}/schema`, `/segments/{t}/metadata`, `/tables/{t}/size`).
`/tables/{t}/externalview` is unconditional; `/schemas/{schemaName}` and
`/tables/{t}/metadata` are fetched only where the config already shows an
`upsertConfig`. So a non-upsert table pays one extra call and an upsert
table three. Source (b) adds none: it reads the `/tables/{t}/schema`
document the column resolution already fetched, which is memoised so it is
read once per table per quotation. A table with proven keys pays one
further broker EXPLAIN, for its key-column ordinals.

`consuming_count` needs both documents, so `engine.py` passes the size JSON
it already fetches into it rather than re-reading. `metadata_is_complete`
takes the same two documents `segment_facts` does.

## Rules — the fail-safe direction for every new number

Every rule below answers the same question: which way does it err when the
input is missing or unreadable?

| number | unreadable / absent → | why that is safe |
|---|---|---|
| `consuming` | the larger of externalview and `missingSegments`; neither readable → 0 | a larger count charges more; 0 only where nothing said otherwise, and decision 4 catches the case that matters |
| `flush_rows` | `None` → rows `None` → low | nothing else bounds a consuming segment (rule 7) |
| `numConsumingSegmentsQueried` | 0 → sealed k is larger | charges more sealed segments |
| bytes ratio | no sealed segment with docs > 0, or any sealed segment with no doc count → bytes `None` → low | no basis for a ratio is not a licence to invent one; the uncounted segment could be the densest |
| metadata completeness | any sealed segment in `size` missing from metadata → rows and bytes `None` → low | the alternative is a confident sum over a subset |
| limit prune | no explicit LIMIT, or an OFFSET → `ByLimit` and `ByServer` unread | the EXPLAIN pruned under Pinot's implicit `LIMIT 10`; the engine that runs it has none |
| operand resolution | any doubt — expression, `$f` name, non-chain side, unresolvable index, `OR` — → no evidence for that side | no evidence means the product, which is the current behaviour |
| key ordinals | no ordinals for a table, or an identifier that is not bare → no evidence for it | a projected name is spoofable by alias; an ordinal is not |
| unique key (a) | config and PK map disagree → withheld | two documents disagreeing is not proof |
| unique key (b) | nullability not establishable, any `-1` in the size report, more than one sealed segment named, or a multi-value column → withheld | the null caveat (log §6) is unclosed, and a consuming segment holds rows the sealed one does not |
| plan unreadable (450) | `max_intermediate_rows` `None` → low | unchanged from today |

The consuming charge is the **only** place in this design where a number
grows a quotation rather than shrinking it, and the two places where a
quotation shrinks — decision 5's join rule and nothing else — both require
positive catalog evidence, never a shape.

## Testing

**Unit**, over the spike's captured JSON copied into
`server/tests/adapters/pinot/fixtures/`:

- `consuming_count` on `01-externalview.json` (1 of 7) with
  `01-tablesize.json` (`missingSegments: 1`) — agreement; and on
  `02-upsert-size.json` (`missingSegments: 2`) — the two-partition case.
  A hand-edited disagreement takes the larger.
- `flush_rows` on `01-bundled-airlineStats-realtime-config.json` (the
  deprecated `.threshold.size: "50000"` → 50000), on
  `01-posted-airlineStats-realtime-config.json` (`.rows: "100"` → 100), on
  `02-upsert-config-readback.json` (`"200"`), and on hand-built configs for
  the legacy location, a non-numeric value, and absence.
- The consuming charge and the ratio on `01-*`: the 600-row moment gives 6
  sealed × 100 docs + 1 × 100 consuming = 700 rows; whole-segment bytes
  517,035 + ceil(100 × 871.36) = 604,171; the two-column projection gives
  the `indexSizeMap` sum + ceil(100 × 1.14) = +114.
- Completeness: `02-upsert-segmetadata.json` (2 entries) against
  `02-upsert-size.json` (4 named, 2 at `-1`) → **incomplete**, rows and
  bytes `None`. The same pair with the two consuming entries removed from
  the size report → complete.
- Hybrid: a size report carrying both `offlineSegments` and
  `realtimeSegments`, synthesised from `01-tablesize.json` and an OFFLINE
  fixture, charged as one segment set.
- Operand resolution on `04-join-plain-plan.json` (`Carrier`/`teamID`),
  `04-join-twokeys-plan.json` (the index arithmetic: `$3` → right `teamID`,
  `$2` → right `league`), `04-join-expr-plan.json` (`$f85` → no evidence),
  `04-join-left-plan.json` (`joinType: "left"` resolves the same way).
- The join arithmetic: both-unique, one-side-unique on each side, neither,
  and an `OR` condition — against hand-built `unique_keys` and leaf sizes.
- Unique keys: `02-upsert-config-readback.json` +
  `02-upsert-schema-readback.json` → `{{"pk"}}`; the plain twin → empty;
  `03-baseballStats-segmetadata-allcols.json` (1 segment, 97,889 docs) →
  empty, because no column reaches its `totalDocs`; a hand-built
  single-segment capture where one does → that column.

**Integration**, on the realtime instance under `pinot_realtime_ready`:

- A REALTIME table quotes at `confidence="high"` with a row estimate ≥ the
  `numDocsScanned` the same query reports after executing.
- The consuming charge is present: the quote for a filter that prunes every
  sealed segment is still ≥ `flush_rows`.
- The upsert self-join's `max_intermediate_rows` is below the default
  budget; the plain twin's is above it.
- A table whose metadata response is truncated (the two-server `u12plain`)
  quotes `"low"` rather than confidently wrong — the direct regression test
  for decision 4.

**The OFFLINE regression suite is unchanged and must stay green**, on the
batch instance at 9000/8000. Decision 4 is the only change that touches the
OFFLINE path, and its effect there is to withhold a quote that was
previously wrong — on the single-server quickstart, where metadata returns
31 of 31, nothing changes at all.

**E2E**: the three demo assertions of decision 7, through the MCP client
round-trip.

## Units

This is **U12**, the unit the shipped design named "consuming-segment bound
and actuals". It delivers the bound, the key evidence, decision 4's
completeness fix, the compose profile and the tests. The "actuals" half of
that line — `numEntriesScannedInFilter` and freshness surfaced as warnings,
estimate vs `numDocsScanned` on the audit line — moves to **U13** alongside
docs and the demo, which the 2026-09-11 design already scheduled there.

**U13 — docs and demo**: the README section in developer-pain language, the
configuration table rows for the realtime profile, the realtime GIF, the
post-execution actuals on the audit line, and the null-handling measurement
that would let decision 5(b) relax its gate.

## What this design does not do

- **It does not cover hybrid tables live.** Decision 3's arithmetic is
  unit-tested over synthesised JSON and never exercised against a running
  hybrid table, because `-type HYBRID` cannot start in a container on 1.5.1.
  The first real hybrid deployment is the first test of it.
- **It does not use the PK-count map as a doc count.** It is per server and
  unmeasured under replication > 1 (decision 6), so it confirms that a table
  is an upsert table and nothing else.
- **It does not measure a consuming segment's bytes.** Decision 2 is a
  projection from sealed segments of the same table, and is the only one in
  the quotation. Nothing on 1.5.1 reports a consuming segment's size, so
  nothing here can be validated against the thing it bounds.
- **It does not close the null caveat.** Decision 5(b) is gated rather than
  proven; the measurement that would settle it needs a table with real
  nulls, which neither quickstart dataset has. Gated as shipped, it finds
  nothing on either dataset.
- **It does not quote a genuinely multi-server table.** Decision 4 denies
  one rather than summing a subset of its segments, and the demo tables are
  pinned to one server apiece instead. Fetching each segment's metadata
  individually would lift the denial, and that is U13.
- **It does not prove a key by column name.** Only a scan ordinal the
  engine itself reported counts, which costs one extra EXPLAIN per keyed
  table and yields nothing for a table whose key-column plan cannot be
  read.
- **It does not read text plans.** Semi-joins and aggregated join inputs
  stay denied, since their JSON plan cannot be obtained on 1.5.1.
- **It does not make an upsert table's rows exact.** Sealed `totalDocs`
  over-counts an upsert table by design (decision 6), and an over-count is
  what an upper bound is made of.
- **It does not touch grounding.** `metadata.row_estimate` still returns
  `None` for any table with a REALTIME half, so `describe_table` carries no
  row count there. That rule is about a *card an agent reads*, not a bound a
  gate enforces, and its reason is unchanged: `/tables/{t}/metadata` →
  `numRows` is a live count including consuming rows (log §1.5), so it is
  not a pre-execution fact. Grounding a realtime table with a row estimate —
  from the sealed sum plus the consuming bound, say — would be a separate
  decision with a separate justification, and this unit does not make it.
