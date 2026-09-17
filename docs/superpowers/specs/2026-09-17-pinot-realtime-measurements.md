# Pinot realtime, upsert and join-key measurements

Measured on **Apache Pinot 1.5.1** on 2026-09-17, against a purpose-built
STREAM cluster (`u12-pinot`, controller :9001 / broker :8001, Kafka 3.9.0
KRaft beside it) plus the pre-existing batch instance from the 2026-09-11
spike (`lagaam-pinot`, :9000 / :8000). Nothing quoted from memory. Every
number below was read from a captured JSON file in
`.superpowers/spike-u12/`; the file is named per answer.

This log is facts only. The design they support is
`2026-09-17-pinot-realtime-and-key-evidence-design.md`.

## 0. Quickstart types

`QuickStart -help` (`00-quickstart-help.txt`) lists exactly one `-type` line:

```
-type=<_type>   Type of quickstart, supported: STREAM/BATCH/HYBRID
```

**There is no UPSERT quickstart type on 1.5.1** — no `UPSERT`, no
`REALTIME_UPSERT`, no variant. An upsert table has to be hand-built: schema
and table config POSTed, rows produced with `kafka-console-producer.sh`.
`-type HYBRID` remains broken in a container for the reason the 2026-09-11
log records (§10 there): the stream quickstarts shell out to the `docker`
CLI rather than embedding Kafka.

## 1. Where the realtime facts live

### 1.1 The flush threshold, and where it is written

The STREAM quickstart's bundled realtime config
(`01-bundled-airlineStats-realtime-config.json`) carries the stream config
at `ingestionConfig.streamIngestionConfig.streamConfigMaps[0]`. The legacy
`tableIndexConfig.streamConfigs` path is **absent** — confirmed on the
bundled config and again on readback (`01-tableconfig-readback.json`,
`02-upsert-config-readback.json`, where `tableIndexConfig` holds only index
keys: `rangeIndexVersion`, `nullHandlingEnabled`, `optimizeDictionary`, …).

| key | bundled value |
|---|---|
| `realtime.segment.flush.threshold.time` | `"3600000"` |
| `realtime.segment.flush.threshold.size` | `"50000"` |
| `realtime.segment.flush.threshold.rows` | **absent** |
| `realtime.segment.flush.threshold.segment.size` | **absent** |
| `realtime.segment.flush.threshold.desired.size` | **absent** |

`...threshold.size` is the deprecated spelling of `...threshold.rows`;
Pinot reads it as a row count. **The bundled quickstart config uses the
deprecated spelling**, so a reader that accepts only `.rows` finds nothing
on the one config a fixture is most likely to meet.

**Validation rule** (HTTP 400 on POST): setting both `.threshold.rows` and
`.threshold.segment.size` is rejected —

```
"Only 1 of flush threshold (rows: 100, segment rows: -1, segment size: 524288000) can be set"
```

So at most one row bound and one size bound is ever present, which keeps
the row bound unambiguous.

The config POSTed for this spike
(`01-posted-airlineStats-realtime-config.json`) set
`"realtime.segment.flush.threshold.rows": "100"` and `.threshold.time:
"24h"`, and reads back verbatim at the same path:

```json
"ingestionConfig": {"streamIngestionConfig": {"streamConfigMaps": [{
  "streamType": "kafka",
  "stream.kafka.topic.name": "flights-realtime",
  "stream.kafka.broker.list": "u12-kafka:9092",
  "realtime.segment.flush.threshold.time": "24h",
  "realtime.segment.flush.threshold.rows": "100"}]}}
```

Note the value is a **string**, not a number, in every config measured.

`segmentsConfig` on that table is `{timeColumnName: "DaysSinceEpoch",
retentionTimeUnit: "DAYS", retentionTimeValue: "5", replication: "1"}` and
`routing` is `{"segmentPrunerTypes": ["time"]}` — which is what makes §1.6's
broker pruning fire.

### 1.2 Which endpoint tells CONSUMING from ONLINE

**Only `GET /tables/{t}/externalview` and `GET /tables/{t}/idealstate`**
carry the state. Both return the same shape — a per-segment map of server to
state string (`01-externalview.json`, `01-idealstate.json`, identical
content at this moment):

```json
{"OFFLINE": null,
 "REALTIME": {
   "airlineStats__0__0__20260917T1213Z": {"Server_172.19.0.3_7051": "ONLINE"},
   "airlineStats__0__5__20260917T1214Z": {"Server_172.19.0.3_7051": "ONLINE"},
   "airlineStats__0__6__20260917T1214Z": {"Server_172.19.0.3_7051": "CONSUMING"}}}
```

`GET /segments/{t}` returns **names only, no state** —
`[{"REALTIME": ["airlineStats__0__0__…", …]}]` (`01-segments-list.json`).
`/segments/{t}/metadata` carries no state field either. And a segment's
**name cannot be used**: `__0__6__` numbering does not say which is sealed.

### 1.3 Sealed vs consuming segment metadata

At the 600-row moment (`01-segmetadata-nocolumns.json` with
`01-externalview.json`): 6 ONLINE + 1 CONSUMING.

| | sealed (ONLINE) | consuming |
|---|---|---|
| `totalDocs` | **exactly 100** (each of 6) | **0** |
| `startTimeMillis` / `endTimeMillis` | real epoch ms (1388534400000 / 1388620800000) | **null** |
| `columns` key | present, full per-column detail | **key entirely absent** |
| `segmentVersion` | `"v3"` | **null** |
| `crc` | real (218316842, 3259008462, …) | `-9223372036854775808` (Long.MIN_VALUE) |
| `startOffset` / `endOffset` | present | **null** |

**A sealed REALTIME segment's metadata is shape-identical to an OFFLINE
one.** With `?columns=Carrier&columns=DaysSinceEpoch`
(`01-segmetadata-twocols.json`), each sealed segment returns a `columns[]`
whose entries carry exactly the keys the OFFLINE path already parses:

```json
{"columnName": "Carrier", "cardinality": 10, "totalDocs": 100,
 "totalNumberOfEntries": 100, "minValue": "MQ", "maxValue": "WN",
 "sorted": false, "minMaxValueInvalid": false, "hasDictionary": true,
 "indexSizeMap": {"dictionary": 28, "forward_index": 58}}
```

The consuming segment returns **no `columns` key at all**, so a `?columns=`
request against it cannot be told apart from "the column is not carried".

**A consuming segment never reports anything non-zero.** Probed 10 times
over 30 s during active ingestion while it demonstrably held 50 live docs
(`01c-consuming-retry-probe.txt`):

```
probe0: airlineStats__0__8__20260917T1215Z totalDocs=0 start=None end=None cols=False bytes=-1 | live docs in consuming=50
…
probe9: airlineStats__0__8__20260917T1215Z totalDocs=0 start=None end=None cols=False bytes=-1 | live docs in consuming=50
```

Identical on all ten. This is not a race — it is permanent until the
segment seals. There is no retry-until-populated strategy.

### 1.4 `/tables/{t}/size`

`01-tablesize.json`. `realtimeSegments.segments[<seg>]` gives per-segment
`reportedSizeInBytes`, and **lists the consuming segment too**, at `-1`:

```
airlineStats__0__0__20260917T1213Z  85495
airlineStats__0__1__20260917T1214Z  86699
airlineStats__0__2__20260917T1214Z  85893
airlineStats__0__3__20260917T1214Z  86800
airlineStats__0__4__20260917T1214Z  87136
airlineStats__0__5__20260917T1214Z  85012
airlineStats__0__6__20260917T1214Z  -1     <- the CONSUMING one
```

The consuming segment reports `-1` for all three of
`reportedSizeInBytes`, `estimatedSizeInBytes`,
`maxReportedSizePerReplicaInBytes`. Table-level
`realtimeSegments.reportedSizeInBytes = 517035` = the sum of the six sealed
only; the `-1` is not added in. And
**`realtimeSegments.missingSegments = 1` counts exactly the consuming
segments** — 2 on the two-partition upsert table with two consuming
segments (`02-upsert-size.json`).

The size report is therefore the one endpoint that names **every** segment,
sealed and consuming alike. That matters in §4.

### 1.5 `/tables/{t}/metadata`

`01-tablemetadata.json` at the 600-row moment:

```json
{"tableName": "airlineStats_REALTIME", "diskSizeInBytes": 517035,
 "numSegments": 7, "numRows": 600, "columnCardinalityMap": {},
 "upsertPartitionToServerPrimaryKeyCountMap": {}}
```

- **`numRows` counts consuming rows too.** 600 here matched all rows
  including the consuming segment's (which was 0 at that instant); at the
  850-row moment `count(*)` was 850 while sealed `totalDocs` summed to 800.
  It is a *live* count, not a metadata sum — so it is **not** a
  pre-execution fact the way sealed `totalDocs` is, and cannot be charged
  as a bound.
- `columnCardinalityMap: {}` — **empty** on the bare call, on both the
  realtime table and batch `airlineStats`
  (`03-airlineStats-tablemetadata.json`). It populates only when `?columns=`
  is asked for, and then it is a per-segment mean (§3.3).
- `upsertPartitionToServerPrimaryKeyCountMap: {}` on a non-upsert table.

### 1.6 Single-stage EXPLAIN: pruning and consuming counters

`01-explain-and-counters-600rows.json`, broker :8001, 7 segments (6 sealed
+ 1 consuming). All three rows are `EXPLAIN PLAN FOR`, `numDocsScanned: 0`:

| query | segsQueried | ByServer | ByValue | ByLimit | **ByBroker** | consumingQueried | consumingProcessed |
|---|---|---|---|---|---|---|---|
| no filter | 7 | 6 | 0 | 5 | 0 | 1 | 0 |
| `DaysSinceEpoch < 16072` | **4** | 1 | 0 | 0 | **3** | 1 | 0 |
| `DaysSinceEpoch > 99999` | **1** | 1 | 0 | 0 | **6** | 1 | 0 |

**`numSegmentsQueried` includes the consuming segment.** With the
impossible filter, 6 of 7 were pruned by the broker and the 1 left queried
*is* the consuming segment.

**Consuming segments are never pruned by a time predicate.**
`numConsumingSegmentsQueried` stayed **1** under both `DaysSinceEpoch >
99999` and `DaysSinceEpoch < 1` — filters excluding every possible value —
while `numSegmentsPrunedByBroker` went to 6. Both returned `count(*)` = 0,
so the consuming segment was routed to, queried, and matched nothing. A
time filter **cannot** remove a consuming segment's cost.

> **Defect A — this contradicts ADR 0008 as written.** ADR 0008 excludes
> `ByBroker` and `Invalid` because "neither was observed non-zero and
> neither is known to be a breakdown of `numSegmentsQueried`". On a REALTIME table with
> `routing.segmentPrunerTypes: ["time"]` it is **3 and 6 here**, and it is
> the *only* non-zero pruning counter in those two rows. But the counters
> do **not** nest the way the OFFLINE measurements suggested: here
> `numSegmentsQueried` is already **net of** broker pruning (4 = 7−3, and
> 1 = 7−6). Subtracting `ByBroker` again would under-charge. The shipped
> `_surviving` computation (queried minus the largest of
> ByServer/ByValue/ByLimit, floored at 1) yields 4−1 = 3 and 1−1 → 1 on
> these two, against 1 and 0 truly surviving — over-charging, which is
> safe. **Not reading `ByBroker` stays correct; only the ADR's stated
> justification is wrong.**

### 1.7 Multi-stage execution vs sealed totalDocs

`01-explain-and-counters-600rows.json` plus the 850-row (`01b-*`) and
2500-row (`01d-*`) captures. Both engines agree on the answer exactly:

| moment | sealed Σ`totalDocs` | `count(*)` (SSE & MSE) | **docs in consuming** | threshold |
|---|---|---|---|---|
| 600 rows | 600 (6×100) | 600 | 0 | 100 |
| 850 rows | 800 (8×100) | 850 | **50** | 100 |
| 2500 rows | 2500 (25×100) | 2500 | 0 | 100 |

`numDocsScanned` = 850 = `count(*)`, `numSegmentsProcessed` = 9, and
`numConsumingSegmentsProcessed` = **0** in every case — the consuming
counter stays 0 even when the consuming segment demonstrably contributed
50 docs. **`numConsumingSegmentsProcessed` cannot be used to detect
consuming work.**

MSE differs from SSE only in pruning attribution. On the time-filtered
count, MSE reported `numSegmentsQueried: 7, ByServer: 4, ByValue: 3,
ByBroker: 0` where SSE reported `queried: 4, ByBroker: 3` — same answer
(289 docs), different accounting. **MSE does no broker-side time pruning**,
so MSE's `numSegmentsQueried` is the gross count.

**The flush threshold bounds the consuming segment exactly.** At 2,500 rows
pushed: **25 sealed segments, every one exactly 100 docs** — distinct
`totalDocs` values across all 26 metadata entries are `[0, 100]`, the 0
being the consuming one (`01d-segmetadata-2500.json`, with
`01d-externalview-2500.json` showing 25 ONLINE + 1 CONSUMING). A segment
seals at *exactly* the threshold, not at "≤ threshold". Over three
independent samples the consuming residual was 0, 50, 0 — always < 100.

## 2. Upsert tables

No UPSERT quickstart exists, so `u12upsert` was built by hand
(`02-upsert-schema-posted.json`, `02-upsert-table-posted.json`) alongside an
otherwise identical non-upsert `u12plain` reading the **same Kafka topic**
(`02-plain-*.json`) for contrast. Two partitions, 600 rows = 100 distinct
PKs × 6 versions each, produced keyed by `pk`.

`tenants: {}` is a **required** property on a POSTed table config (400
otherwise) — worth knowing for any fixture.

The upsert marker in the config is minimal on POST and verbose on readback.
POSTed (`02-upsert-table-posted.json`): `"upsertConfig": {"mode": "FULL"}`.
Read back (`02-upsert-config-readback.json`): the same key filled out with
`snapshot`, `hashFunction: "NONE"`, `consistencyMode: "NONE"`,
`metadataTTL: 0.0`, `deletedKeysTTL: 0.0`, `upsertViewRefreshIntervalMs`
and eleven more defaults. The primary key list is **not** in the table
config at all: it is `primaryKeyColumns` on the **schema**
(`02-upsert-schema-readback.json`, `schemaName: "u12upsert"`,
`primaryKeyColumns: ["pk"]`), fetched from `GET /schemas/{schemaName}`.

**`segmentsConfig.schemaName` is absent on every config captured** — all
four of `01-posted-airlineStats-realtime-config.json`,
`01-tableconfig-readback.json`, `02-upsert-config-readback.json` and
`03-airlineStats-tableconfig.json`. The schema is named after the table in
each case. Note that a config read back carries the **suffixed**
`tableName` (`"u12upsert_REALTIME"`, `"airlineStats_OFFLINE"`,
`"airlineStats_REALTIME"`) though `/tables` lists it bare, so the config's
own `tableName` is not a usable schema path.

### 2.1 Measured (`02-upsert-query-results.txt`)

| measurement | upsert (`u12upsert`) | plain (`u12plain`) |
|---|---|---|
| `schema.primaryKeyColumns` | `["pk"]` | absent |
| `upsertConfig.mode` | `FULL` | absent |
| `routing.instanceSelectorType` | `strictReplicaGroup` | default |
| **`upsertPartitionToServerPrimaryKeyCountMap`** | **`{"0": {"Server_…7051": 50}, "1": {"Server_…7052": 50}}`** | **`{}`** |
| `/tables/{t}/metadata` `numRows` | 400 | 400 |
| `count(*)` (SSE = MSE) | **100** | **600** |
| `numDocsScanned` on that count | **100** | 600 |
| `distinctcount(pk)` | 100 | 100 |
| `GROUP BY pk HAVING count(*) > 1` | **`[]` — no rows** | `[["key067",6],["key066",6],…]` |
| **self-join `a JOIN b ON a.pk = b.pk`** | **100** | **3600** |

- `upsertPartitionToServerPrimaryKeyCountMap` **does** report distinct PK
  counts, keyed by stream partition then by server. Σ = 50+50 = **100** =
  the exact distinct-key count. It is `{}` on every non-upsert table, so a
  non-empty map is itself proof the table is an upsert table.
- `count(*)` **is** the distinct-key count (100), and `numDocsScanned` is
  **also** 100 — upsert filters to the winning version *before* the scan
  counter.
- The `HAVING count(*) > 1` probe returns **nothing** on the upsert table
  and 6-version rows on the identical plain table: the primary key is
  genuinely unique in the query-visible view.
- **The self-join on the primary key returns exactly 100 = the
  distinct-key count**, against **3,600** for the byte-identical plain
  table (600 × 6). The key bounds the join at `|distinct pk|`, not the
  product.

### 2.2 The caveat that matters for pricing

A **sealed upsert segment's `totalDocs` reports all versions, not the
visible rows**: `u12upsert__0__0__20260917T1217Z` reports `totalDocs: 200`
(`02-upsert-segmetadata.json`) while the whole table's `count(*)` is 100.
Segment metadata **over-counts** an upsert table — safe for an upper bound,
but sealed `totalDocs` and `count(*)` legitimately disagree there, and the
PK-count map is the only source of the true distinct count.

## 3. Column cardinality as uniqueness evidence (batch instance :9000)

### 3.1 `cardinality` is exactly `count(DISTINCT col)`

On single-segment `baseballStats` (1 segment, 97,889 docs,
`03-baseballStats-segmetadata-allcols.json`), verified against the engine:

| column | segment `cardinality` | `distinctcount(col)` | match |
|---|---|---|---|
| `playerID` | 18,107 | 18,107 | yes |
| `teamID` | 149 | 149 | yes |
| `league` | 7 | 7 | yes |
| `homeRuns` | 67 | 67 | yes |

`count(col)` = 97,889 = `totalDocs` for all four, i.e. **no nulls in this
data**, so this measurement does not settle whether `cardinality` counts a
null or default as a distinct value. See §6.

### 3.2 Is any column unique within a segment?

**No, not on this data.** Across all **25 columns** of the single-segment
`baseballStats`, the highest cardinality is `playerID` at 18,107 of 97,889
docs — ratio 0.185. Across all 31 segments of `airlineStats`
(`03-airlineStats-segmetadata-allcols.json`), **0 of 2,604 per-column
entries** have `cardinality == totalDocs`. The rule is sound; it simply
finds no key on the quickstart data.

### 3.3 Cross-segment uniqueness for a multi-segment table

**Nothing in the metadata proves it.**

- `segmentPartitionConfig`: **null** on `airlineStats`
  (`03-airlineStats-tableconfig.json`), and **0 of 2,604 per-column entries
  carry a `partitionFunction` or `partitions`** — no partition-based
  argument is available to exercise.
- `sorted`: **420** per-column entries are `sorted: true`, but
  sorted-within-segment says nothing about uniqueness (`DaysSinceEpoch` is
  sorted with cardinality 1 on a 100-doc realtime segment —
  `01-segmetadata-twocols.json`).
- `columnCardinalityMap` in `/tables/{t}/metadata` is `{}` unless columns
  are requested, and when populated is a **per-segment mean**: summing
  per-segment `Carrier` cardinality across 31 segments gives **432**
  (mean **13.94**, the figure ADR 0008 quotes) against a true table-wide
  `distinctcount(Carrier)` of **14**. Summing overshoots by 31×; the mean
  is not a table-wide distinct count either.

Per-segment cardinality is therefore evidence **only** for a table with
exactly one segment, where "the segment" and "the table" are the same rows.

## 4. Defect B — `/segments/{t}/metadata` is incomplete on a multi-server table

Not in the spike brief, but it threatens the shipped OFFLINE quotation
path, so it is measured here.

**`GET /segments/{t}/metadata` returns only ONE server's segments, and
which server it picks alternates between identical calls.**

Measured on `u12plain` (4 segments across 2 servers), three consecutive
calls:

```
meta -> 2 ['u12plain__0__0__…', 'u12plain__0__1__…']
meta -> 2 ['u12plain__1__0__…', 'u12plain__1__1__…']
meta -> 2 ['u12plain__0__0__…', 'u12plain__0__1__…']
```

The captured `02-upsert-segmetadata.json` shows the same truncation frozen
in place: two entries, `u12upsert__0__0__…` and `u12upsert__0__1__…`, both
from partition 0 — while `02-upsert-size.json` lists all **four**
(`u12upsert__0__0__…` 7422, `u12upsert__1__0__…` 7442, and the two
consuming at `-1`), and `/tables/u12upsert/metadata` reports
`numSegments: 4`.

`GET /segments/u12plain` lists all 4; `GET /tables/u12plain/size` reports
all 4; `GET /segments/u12plain/servers` shows the split (`Server_…7052` → 2
segments, `Server_…7053` → 2). Adding `?columns=pk` does not change it.

The batch instance is **single-server** (`airlineStats` OFFLINE: 31
segments, 1 server, metadata returns 31 of 31 on three consecutive calls),
which is why the 2026-09-11 spike never saw this. On any multi-server
deployment `segment_facts` silently receives **half the segments** and
`surviving_docs` / `surviving_bytes` return a confident sum over a subset —
an **under-quote at `confidence="high"`**, the one failure mode the design
exists to prevent.

`/tables/{t}/size` is complete and keyed by segment name, so the segment
set from `size` (or from `GET /segments/{t}`) can be compared against the
metadata response to detect the truncation and fail closed.

## 5. Plan shape for join keys (batch instance, MSE `EXPLAIN … AS JSON`)

### 5.1 The field-index mapping rule

From `04-join-plain-plan.json`
(`airlineStats a JOIN baseballStats b ON a.Carrier = b.teamID`):

```
id 0  PinotLogicalTableScan  table=["default","airlineStats"]   inputs=[]  (no `fields`)
id 1  LogicalProject         fields=["Carrier"]  exprs=[{"input":18,"name":"$18"}]
id 2  PinotLogicalExchange
id 3  PinotLogicalTableScan  table=["default","baseballStats"]  inputs=[]  (no `fields`)
id 4  LogicalProject         fields=["teamID"]   exprs=[{"input":26,"name":"$26"}]
id 5  PinotLogicalExchange
id 6  LogicalJoin  inputs=["2","5"]  joinType="inner"
        condition={"op":{"kind":"EQUALS"},"operands":[{"input":0},{"input":1}]}
```

Rules verified:

1. **A scan node carries no `fields`.** `PinotLogicalTableScan` has only
   `table` and `inputs: []`. Names come from the `LogicalProject`
   immediately above it, which does carry `fields`.
2. **A join's `condition.operands[].input` indexes the concatenation of its
   two inputs' field lists — left first, then right.** Proven by the
   two-key case (`04-join-twokeys-plan.json`), the discriminating test:
   left `fields=["Carrier","Origin"]` (2 fields), right
   `fields=["league","teamID"]`, condition `AND(=($0,$3), =($1,$2))`.
   Right-side `teamID` is at right-index 1 → global **2+1 = 3**; `league`
   at right-index 0 → global **2+0 = 2**. A naive "one field per side"
   reading would be wrong. Note the right-side `fields` list is
   **alphabetised** (`league` before `teamID`), *not* in ON-clause order —
   only the index is trustworthy, never the position in the SQL.
3. **Resolving an operand index to a column name means walking down from
   the join through the `PinotLogicalExchange` to the `LogicalProject`**
   whose `fields` list it indexes. The exchange carries no `fields` and is
   pass-through.
4. **An expression key is a synthesised field, not a column name.**
   `04-join-expr-plan.json` (`ON upper(a.Carrier) = b.teamID`): the left
   project becomes `fields=["Carrier","$f85"]` with
   `exprs=[{"input":18,"name":"$18"}, {"op":{"name":"UPPER","kind":"OTHER_FUNCTION"},"operands":[{"input":18}]}]`,
   and the join condition is `=($1,$2)` — pointing at **`$f85`**, a
   generated name with a `$f` prefix whose `exprs` entry carries an `op`
   instead of a bare `input`. Either marker identifies an expression, to
   which no per-column evidence applies.
5. **`joinType` is an explicit string** — `"inner"` / `"left"`
   (`04-join-left-plan.json`, otherwise structurally identical to the
   plain join).

### 5.2 Defect C — aggregated and semi-joins cannot be read as JSON

`EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR` **fails outright** on
both the aggregated-right-side join and the `WHERE … IN (SELECT …)`
semi-join:

```
InternalError: Error while planning query. ...
type not serializable as JSON: PIPELINE_BREAKER
(type org.apache.pinot.calcite.rel.logical.PinotRelExchangeType)
```

errorCode **450**, `resultTable: null`. **Any plan containing a
`PIPELINE_BREAKER` exchange cannot be serialised to JSON on 1.5.1.**

The text (non-JSON) `EXPLAIN PLAN FOR` does work for both
(`04-pipeline-breaker-textplans.json`), and shows two further facts:

- Both queries produce the **same plan** — `joinType=[semi]` with the right
  side under `PinotLogicalExchange(distribution=[broadcast],
  relExchangeType=[PIPELINE_BREAKER])`.
- **The `GROUP BY teamID` was eliminated entirely.** The
  aggregated-right-side join contains no aggregate node at all: Calcite
  rewrote `JOIN (SELECT teamID, count(*) … GROUP BY teamID) ON a.Carrier =
  b.teamID` into a semi-join, because only the grouping key was consumed.
  The intended "a GROUP BY key is unique on that side" evidence **is not
  present in this plan to be read**.

## 6. What cannot be proven

- **Whether `cardinality` counts a null or default as a distinct value.**
  Every column measured had `count(col) == totalDocs` (no nulls in either
  quickstart dataset), so the null case was never exercised. Until a table
  with real nulls is measured, `cardinality == totalDocs` reads as "at most
  this many distinct non-null values, plus possibly a default". It is still
  sound as *uniqueness* evidence only if a null cannot collide, which is
  unverified. **This is the gap to close before §7 rule 14 admits a join on
  cardinality alone.**
- **Whether a GROUP BY makes its key readable as unique in the plan.**
  Calcite eliminated the aggregate when only the key was consumed, and the
  rewritten plan could not be serialised to JSON at all. No aggregate node
  with `group` indices was ever produced. Whether such a node carries
  `group`, and how those indices map to names, is **unmeasured**.
- **Whether a semi-join's or aggregated join's shape can be priced from the
  JSON plan.** It cannot be obtained on 1.5.1 (errorCode 450). Only the
  text plan is available, and parsing text plans is a different and more
  fragile contract than the JSON one.
- **Cross-segment uniqueness for a multi-segment table.** No partition
  config, no sorted-column semantics and no cardinality aggregate
  establishes it. `segmentPartitionConfig` was null and no column in 31
  segments carried a `partitionFunction`, so the partition-based argument
  was never exercised against a table that uses it.
- **`realtime.segment.flush.threshold.segment.size` / `.desired.size` as a
  byte bound.** Neither appeared in any config measured, and Pinot refuses
  a size bound alongside a row bound, so the size-threshold path to a
  *bytes* figure is unmeasured. A consuming segment's **rows** can be
  bounded from the config; its **bytes** have no measured basis. (A bytes
  bound could be synthesised as `threshold.rows × max(sealed
  bytes-per-doc)` — the ratios are in §7 rule 2 — but that product was not
  validated against a consuming segment, because a consuming segment
  reports no bytes to validate it against.)
- **A hybrid table (OFFLINE + REALTIME halves on one table).** `-type
  HYBRID` cannot run in a container on 1.5.1 (§0), and hand-building both
  halves was out of the spike's time budget. Every realtime measurement
  here is on a REALTIME-only table.
- **Behaviour with replication > 1.** Everything measured ran at
  `replication: "1"`. How `reportedSizePerReplicaInBytes` and the PK-count
  map (which is per *server*) behave with real replicas is unmeasured —
  naive summation of the PK map across servers would double-count.

## 7. What can be built on — the rules

Each rule with the measurement behind it.

1. **A sealed realtime segment is priced exactly like an OFFLINE one.** Its
   `/segments/{t}/metadata` entry carries `totalDocs`, `startTimeMillis`,
   `endTimeMillis` and a full `columns[]` with per-column `indexSizeMap` —
   the same keys `metadata.segment_facts` already parses. *Measured: 25
   sealed segments each with complete metadata
   (`01d-segmetadata-2500.json`), and the per-column block quoted in §1.3.*

2. **A consuming segment holds at most
   `realtime.segment.flush.threshold.rows` docs.** *Measured: with
   threshold 100, every one of 25 sealed segments held exactly 100 docs,
   and the consuming segment held 50 at the one sampled moment it was
   non-empty (850 total = 8×100 sealed + 50).* The seal is at exactly the
   threshold, so the threshold is a true upper bound, not an
   approximation.

3. **Read the threshold from
   `ingestionConfig.streamIngestionConfig.streamConfigMaps[0]`, accepting
   `realtime.segment.flush.threshold.rows` OR the deprecated
   `realtime.segment.flush.threshold.size` as the row bound.** *Measured:
   the bundled quickstart config uses the deprecated spelling and the
   posted one uses `.rows`; both round-trip verbatim through `GET
   /tables/{t}`; Pinot rejects a config setting more than one row bound, so
   at most one is present.* Values are JSON **strings**. The legacy
   `tableIndexConfig.streamConfigs` location was absent on every config
   measured.

4. **Identify consuming segments from `/tables/{t}/externalview`
   (`REALTIME.<segName>.<server> == "CONSUMING"`), never from
   `/segments/{t}/metadata`.** *Measured: the metadata endpoint has no
   state field, and a consuming segment is indistinguishable there from an
   unreadable one — both give `totalDocs: 0` and no `columns`.*

5. **A consuming segment can also be recognised without externalview**, as
   a belt-and-braces check: `totalDocs == 0` **and** no `columns` key
   **and** `crc == -9223372036854775808` **and** `reportedSizeInBytes ==
   -1` in `/tables/{t}/size`. *Measured on 10 consecutive probes
   (`01c-consuming-retry-probe.txt`).*

6. **`realtimeSegments.missingSegments` in `/tables/{t}/size` equals the
   number of consuming segments.** *Measured: 1 with one consuming segment
   (`01-tablesize.json`), 2 with two (`02-upsert-size.json`).* A cheap
   cross-check on the externalview count.

7. **Never expect a consuming segment to report anything.** *Measured: 10
   probes over 30 s during live ingestion while it held 50 docs —
   `totalDocs` stayed 0, times stayed null, bytes stayed −1.* There is no
   retry strategy; it must be charged from the config or not at all.

8. **A time filter cannot prune a consuming segment.** *Measured:
   `numConsumingSegmentsQueried` stayed 1 under both `DaysSinceEpoch >
   99999` and `DaysSinceEpoch < 1`.* A consuming charge must be added
   **unconditionally**, outside any k-largest-surviving logic — the oracle
   can never prune it away.

9. **`numSegmentsQueried` includes consuming segments**, and
   `numConsumingSegmentsQueried` says how many of them. *Measured: 7
   queried with 1 consuming on the 7-segment table.* So
   `numSegmentsQueried − numConsumingSegmentsQueried` is the right k to
   apply to the sealed segments' k-largest charge.

10. **`numConsumingSegmentsProcessed` is useless as a signal.** *Measured:
    0 while the consuming segment contributed 50 docs to `count(*)`.* Do
    not read it.

11. **An upsert table's primary key is provable join-key evidence.**
    `schema.primaryKeyColumns` + `upsertConfig` on the table config ⇒ that
    column set is unique in the query-visible view. *Measured: `GROUP BY pk
    HAVING count(*) > 1` returns nothing, and `a JOIN b ON a.pk = b.pk`
    returns exactly 100 = the distinct-key count, against 3,600 for a
    byte-identical non-upsert table reading the same topic.*

12. **`upsertPartitionToServerPrimaryKeyCountMap` gives the true
    distinct-key count**, as `{partition: {server: count}}` to be summed
    over partitions. *Measured: `{"0":{…:50},"1":{…:50}}` = 100 distinct
    keys.* It is `{}` on every non-upsert table, so a non-empty map is
    itself proof of an upsert table.

13. **On an upsert table, sealed `totalDocs` over-counts** (200 on disk vs
    100 visible) — safe as an upper bound, but the PK-count map, not
    segment metadata, is the source for a distinct-key figure.

14. **`cardinality == totalDocs` on a single-segment table is sound
    evidence of uniqueness for that column** — `cardinality` is exactly
    `count(DISTINCT col)`, *verified against the engine on 4 columns
    (18,107 / 149 / 7 / 67, all exact)*. Equality can only hold when every
    doc carries a distinct value. But it finds **nothing** on the
    quickstart data: 0 of 2,604 per-column entries on `airlineStats`, and
    the best ratio on `baseballStats` is 0.185. **Subject to the null
    caveat in §6.**

15. **Never sum or average per-segment `cardinality` across segments.**
    *Measured: Σ per-segment `Carrier` cardinality = 432 over 31 segments
    (mean 13.94) against a true `distinctcount` of 14.*

16. **Join operand resolution:** a scan has no `fields`; take names from
    the `LogicalProject` above it; a join's `condition.operands[].input`
    indexes **left fields ++ right fields**. *Verified on the two-key case
    where right `teamID` resolved to global index 3 = 2 left fields + right
    index 1.* The right `fields` list is alphabetised, so only the index is
    trustworthy.

17. **An operand naming a `$f`-prefixed field, or whose `exprs` entry
    carries an `op`, is an expression and not a column** — no column-level
    key evidence may be applied to it. *Verified: `upper(a.Carrier)` became
    `$f85` with an `op` of `UPPER`.*

18. **Guard the JSON plan path against `PIPELINE_BREAKER`.** *Measured:
    semi-joins and aggregated join inputs make `EXPLAIN … AS JSON` fail
    with errorCode 450 and `resultTable: null`.* `plan.max_intermediate_rows`
    receives no parseable plan and returns `None` (deny), which it already
    does — but no join query may be assumed to yield a JSON plan.

19. **Before trusting a segment-metadata sum, check its segment set against
    `/tables/{t}/size` (or `GET /segments/{t}`).** *Measured: on a
    2-server table `/segments/{t}/metadata` returns half the segments and
    alternates which half between identical calls (§4).* Without this
    check a multi-server deployment under-quotes at `confidence="high"`.

20. **Sealed bytes-per-doc, for the one number that has to be
    extrapolated.** *Measured on the six sealed `airlineStats` realtime
    segments (`01-tablesize.json` ÷ `01-segmetadata-nocolumns.json`):
    whole-segment ratios 850.12, 854.95, 858.93, 866.99, 868.00, 871.36
    bytes/doc — a 2.5% spread, max **871.36**. For the two-column
    projection `Carrier` + `DaysSinceEpoch`
    (`01-segmetadata-twocols.json`, summing `indexSizeMap`): 0.61, 0.77,
    0.92, 0.96, 1.08, **1.14** bytes/doc.* At the measured threshold of 100
    rows the max-ratio bound is 87,136 whole-segment bytes, or 114 bytes
    for those two columns.

## 8. Memory

`05-docker-stats.txt`, all four containers up and both Pinot clusters
serving:

```
u12-pinot     1.16 GiB  / 7.653 GiB
u12-kafka     385.5 MiB / 7.653 GiB
lagaam-pinot  1.856 GiB / 7.653 GiB
lagaam-trino  3.024 GiB / 7.653 GiB
```

Total ≈ 6.4 GiB against Docker's 7.653 GiB ceiling. `u12-pinot` with
`-Xmx2G` peaked around 1.2 GiB — the STREAM quickstart runs 4 servers, 1
broker, 1 controller and 1 minion in one container and still fits. Headroom
is thin: a second `u12-*` Pinot alongside would not fit, and a compose
profile should not assume the Trino profile is down.

## 9. Environment — the exact recipe

This is what a `pinot-realtime` compose profile has to reproduce. Both
`u12-kafka` and `u12-pinot` were left running; nothing pre-existing
(`lagaam-pinot`, `lagaam-trino`) was stopped or modified.

```bash
docker network create u12-net

docker run -d --name u12-kafka --network u12-net --hostname u12-kafka \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://u12-kafka:9092,CONTROLLER://u12-kafka:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://u12-kafka:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@u12-kafka:9093 \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
  -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
  -e CLUSTER_ID=u12ClusterIdXXXXXXXXXX \
  apache/kafka:3.9.0
```

**`KAFKA_LISTENERS` must use the hostname, not `0.0.0.0`** — the
`apache/kafka:3.9.0` entrypoint copies it into `advertised.listeners`
during storage format and dies on "cannot use the nonroutable meta-address
0.0.0.0". **Wait for "Kafka Server started" before starting Pinot**, or the
quickstart's reachability probe fails and it falls back to its own embedded
Kafka.

```bash
docker exec u12-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server u12-kafka:9092 --create --topic flights-realtime \
  --partitions 1 --replication-factor 1
docker exec u12-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server u12-kafka:9092 --create --topic u12-upsert \
  --partitions 2 --replication-factor 1

docker run -d --name u12-pinot --network u12-net \
  -p 9001:9000 -p 8001:8000 \
  -e JAVA_OPTS="-Xms512M -Xmx2G -XX:+UseG1GC" \
  apachepinot/pinot:release-1.5.1 QuickStart -type STREAM \
  -kafkaBrokerList u12-kafka:9092
```

**Ports:** `u12-pinot` controller **:9001**, broker **:8001** on the host —
deliberately not 9000/8000, so the batch instance the OFFLINE tests use can
keep running. `u12-kafka` is network-internal only (`u12-net`, no host
port).

**The quickstart's own table bootstrap failed here** (a transient Kafka
`TimeoutException` while all 7 Pinot components started at once) and it
deleted its `/tmp/<epoch>/` staging dir. The cluster itself came up fine.
The bundled example data survives in the image at
`/opt/pinot/examples/stream/airlineStats/rawdata/airlineStats_data.json`,
and all three tables were created by hand against the running controller —
the more controllable path anyway, since it is how the flush threshold gets
lowered.

**Tables POSTed** (all to `http://localhost:9001`):

| file | what |
|---|---|
| `01-bundled-airlineStats-schema.json` | extracted from the image (`schemaName: "airlineStats"`), POSTed to `/schemas` unchanged |
| `01-posted-airlineStats-realtime-config.json` | the bundled config with `stream.kafka.broker.list → u12-kafka:9092`, `stream.kafka.zk.broker.url` removed, `.threshold.size` replaced by `.threshold.rows: "100"`, `.threshold.time: "24h"` |
| `02-upsert-schema-posted.json` / `02-upsert-table-posted.json` | `u12upsert`: `primaryKeyColumns: ["pk"]`, `upsertConfig: {"mode": "FULL"}`, `routing.instanceSelectorType: strictReplicaGroup`, topic `u12-upsert`, `.threshold.rows: "200"` |
| `02-plain-schema-posted.json` / `02-plain-table-posted.json` | `u12plain`: identical but no `primaryKeyColumns`, no `upsertConfig`, same topic — the contrast table |

**Loading data:**

```bash
docker exec u12-pinot bash -lc \
  'head -2500 /opt/pinot/examples/stream/airlineStats/rawdata/airlineStats_data.json > /tmp/u12_feed.json'
docker cp u12-pinot:/tmp/u12_feed.json - | docker cp - u12-kafka:/tmp/   # via host
docker exec u12-kafka bash -lc \
  'head -600 /tmp/u12_feed.json | /opt/kafka/bin/kafka-console-producer.sh \
     --bootstrap-server u12-kafka:9092 --topic flights-realtime'

# upsert feed: 100 distinct pks x 6 versions, keyed so partitioning is by pk
docker exec u12-kafka bash -lc \
  '/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server u12-kafka:9092 \
     --topic u12-upsert --property parse.key=true \
     --property key.separator="<TAB>" < /tmp/u12up_feed.txt'
```

Offsets are checked with `kafka-get-offsets.sh` — the
`kafka.tools.GetOffsetShell` class is gone in 3.9.0.
