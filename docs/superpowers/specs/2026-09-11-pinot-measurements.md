# Apache Pinot measurement spike for Lagaam

Measured on **Apache Pinot 1.5.1** on 2026-09-11. Nothing quoted from memory.
No file in /Users/muditkapoor/Documents/code/lagaam was touched.

## 0. Environment, image, version, startup

`apachepinot/pinot:latest` is a **nightly SNAPSHOT** (resolved to
`1.6.0-SNAPSHOT-ceb1fabf4e-20260909`), NOT a release. Latest stable = **1.5.1**.

| Source | Finding | Evidence |
|---|---|---|
| Docker Hub tags API | `latest` = 1.6.0-SNAPSHOT nightly | `00-dockerhub-tags.json` |
| Docker Hub `name=release` | newest = `release-1.5.1`, pushed 2026-07-01 | `00-dockerhub-tags-release.jsonl` |
| GitHub releases API | `release-1.5.1`, published 2026-06-05, stable | inline |
| `docker manifest inspect` | multi-arch linux/amd64 + linux/arm64 | `00-manifest-release-1.5.1.json` |

**Tag pulled: `apachepinot/pinot:release-1.5.1`**, digest
`sha256:dbb65d3732eea6edf2619b0b734f13466bf1c6fd73ab9abdfa98e08a807499b2`, arch arm64,
1,062,005,982 bytes. Evidence `00-docker-pull.txt`. Entrypoint `./bin/pinot-admin.sh`.

`GET :9000/version` returns a **map of component -> version**, every value
`1.5.1-020ff0d0538b2079d4cf4cb2676a191c87c95d4d` (`00-controller-version.json`).
**Broker has no /version**: `:8000/version` -> `{"code":404,"error":"HTTP 404 Not Found"}`
(`00-broker-version.json`). Version discovery must use the controller.

### Start command

```bash
docker run -d --name lagaam-pinot \
  -p 9000:9000 -p 8000:8000 -p 8099:8099 \
  -e JAVA_OPTS="-Xms1G -Xmx3G -XX:+UseG1GC -Dlog4j2.configurationFile=/opt/pinot/conf/quickstart-log4j2.xml" \
  apachepinot/pinot:release-1.5.1 QuickStart -type BATCH
```

`QuickStart -help` (`00-quickstart-help.txt`) confirms `-type` accepts **STREAM/BATCH/HYBRID**
plus `-kafkaBrokerList`, `-bootstrapTableDir`, `-zkUrl`, `-tmpDir`, `-configFile`.
The image **honours JAVA_OPTS** — the whole quickstart stayed inside 3 GiB.

- **Time to first successful query: 42 s** (`00-ready-seconds.txt`).
- Bootstrapping continues after that: at 42 s three tables existed
  (airlineStats, baseballStats, clickstreamFunnel); by ~20 min all ten
  (+ billing, dimBaseballTeams, fineFoodReviews, githubComplexTypeEvents, githubEvents,
  starbucksStores, testUnnest). **An adapter listing tables at startup can see a partial catalog.**
- Memory (`00-docker-stats.txt`, `00-docker-stats-final.txt`):

| Container | At readiness | End of spike |
|---|---|---|
| lagaam-pinot | 1.422 GiB / 7.653 GiB | 1.805 GiB |
| lagaam-trino (pre-existing, untouched) | 836 MiB | 2.55 GiB |

## 1. Catalog grounding via controller REST

**Answer.** Everything `describe_table` needs is available in **three calls per table**.
- **Total docs per table -> `GET /tables/{t}/metadata` -> `$.numRows`** (one call).
- **Per-segment docs + time range -> `GET /segments/{t}/metadata`** (bulk) ->
  `$[<seg>].totalDocs`, `.startTimeMillis`, `.endTimeMillis`, `.timeColumn`, `.timeUnit`.
  Per-segment **bytes** are NOT there — they come from `GET /tables/{t}/size` ->
  `$.offlineSegments.segments[<seg>].reportedSizeInBytes`.

| Endpoint | Exists | Key paths | Evidence |
|---|---|---|---|
| `GET /tables` | yes | `$.tables[]` (bare names, no type suffix) | `01-tables.json` |
| `GET /tables/{t}/schema` | yes | `$.dimensionFieldSpecs[]`, `$.metricFieldSpecs[]`, `$.dateTimeFieldSpecs[]`; each `{name,dataType,fieldType}` | `01-schema-*.json` |
| `GET /tables/{t}` | yes | keyed `$.OFFLINE` / `$.REALTIME` | `01-tableconfig-*.json` |
| `GET /tables/{t}/size` | yes | below | `01-size-*.json` |
| `GET /tables/{t}/metadata` | **yes** | below | `01-metadata-*.json` |
| `GET /segments/{t}` | yes | `$[0].OFFLINE[]` | `01-segments-*.json` |
| `GET /segments/{t}/metadata` | yes (bulk) | `$[<seg>]...` | `01-seg-metadata-bulk*.json` |
| `GET /segments/{t}/{seg}/metadata` | yes (single) | flat dotted-string map | `01-seg-metadata-single.json` |

### `/tables/{t}/metadata` — the important one

```json
{ "tableName":"baseballStats_OFFLINE", "diskSizeInBytes":3342450, "numSegments":1,
  "numRows":97889, "columnLengthMap":{}, "columnCardinalityMap":{},
  "maxNumMultiValuesMap":{}, "columnIndexSizeMap":{},
  "upsertPartitionToServerPrimaryKeyCountMap":{} }
```

`tableName` comes back **with the `_OFFLINE` suffix** though `/tables` listed it without.
The `*Map` fields are **empty unless `?columns=` is passed**, and then the values are
**per-segment MEANS, not totals** (`01-metadata-airlineStats-columns.json`):
`"columnCardinalityMap":{"Carrier":13.935483870967742,"DaysSinceEpoch":1.0}` — 13.94 is the
mean over 31 segments, not the table's distinct count.

### `/tables/{t}/size` — exact byte field names

Top level `$.tableName`, `$.reportedSizeInBytes`, `$.estimatedSizeInBytes`,
`$.reportedSizePerReplicaInBytes`. Split: `$.offlineSegments` / `$.realtimeSegments` (each
`null` when absent — realtimeSegments was `null` for the OFFLINE quickstart tables). Each has
`reportedSizeInBytes`, `estimatedSizeInBytes`, `missingSegments`,
`reportedSizePerReplicaInBytes`, `segments`:

```
$.offlineSegments.segments[<seg>].reportedSizeInBytes = 3342450
$.offlineSegments.segments[<seg>].estimatedSizeInBytes
$.offlineSegments.segments[<seg>].maxReportedSizePerReplicaInBytes
$.offlineSegments.segments[<seg>].serverInfo["Server_172.17.0.2_7050"].diskSizeInBytes
```
`reportedSizeInBytes` is measured; `estimatedSizeInBytes` is extrapolated when replicas
do not report (`missingSegments` counts those).

### `/segments/{t}/metadata` (bulk)

```
$[<seg>].totalDocs = 289          $[<seg>].timeColumn = "DaysSinceEpoch"
$[<seg>].timeUnit = "DAYS"        $[<seg>].timeGranularitySec = 86400
$[<seg>].startTimeMillis = 1388534400000   $[<seg>].startTimeReadable
$[<seg>].endTimeMillis   = 1388534400000   $[<seg>].endTimeReadable
$[<seg>].segmentVersion = "v3"    $[<seg>].crc / .dataCrc / .creationTimeMillis
$[<seg>].star-tree-index[] = [{dimension-columns, metric-aggregations, max-leaf-records}]
$[<seg>].columns[] = []   <-- EMPTY unless ?columns= passed
$[<seg>].indexes   = {}   <-- EMPTY unless ?columns= passed
```

With `?columns=Carrier&columns=DaysSinceEpoch` (`01-seg-metadata-bulk-columns.json`) the
per-column block is exactly what a quotation needs:

```
$[<seg>].columns[i].cardinality  = 14 (Carrier) / 1 (DaysSinceEpoch)
$[<seg>].columns[i].minValue = "AA" / 16071      .maxValue = "WN" / 16071
$[<seg>].columns[i].totalDocs = 289              .totalNumberOfEntries = 289
$[<seg>].columns[i].sorted = false / true        .hasDictionary = true
$[<seg>].columns[i].bitsPerElement = 4 / 1       .columnMaxLength = 2
$[<seg>].columns[i].maxNumberOfMultiValues = 0
$[<seg>].columns[i].minMaxValueInvalid = false   <-- trust flag for min/max
$[<seg>].columns[i].indexSizeMap = {"dictionary":36,"forward_index":153}  <-- BYTES per index per column
$[<seg>].columns[i].partitions / .partitionFunction
$[<seg>].indexes[<col>] = {"inverted-index":"YES"|"NO","range-index","bloom-filter",
                           "json-index","text-index","fst-index","h3-index",
                           "dictionary","forward-index","null-value-vector-reader"}
```

`indexSizeMap` is the **per-column, per-index byte count** — raw material for a bytes quote
charging only the columns a query touches.

`GET /segments/{t}/{seg}/metadata` (single) returns a **different flatter shape**: dotted keys
with **string values** — `"segment.total.docs":"289"`, `"segment.size.in.bytes":"43574"`,
`"segment.start.time":"1388534400000"`, `"segment.time.unit":"MILLISECONDS"`,
`"segment.start.time.raw":"16071"`, `"segment.index.version":"v3"`, `"segment.download.url"`.
It carries `segment.size.in.bytes` which the bulk form does not, but numbers are strings and
the time unit differs. **Prefer bulk + `/tables/{t}/size`** — one request instead of N.

### Table config

```
$.OFFLINE.tableType = "OFFLINE"
$.OFFLINE.segmentsConfig.timeColumnName = "DaysSinceEpoch"   .timeType = "DAYS"   .replication = "1"
$.OFFLINE.tableIndexConfig.invertedIndexColumns[] = ["teamID"]   (baseballStats)
$.OFFLINE.tableIndexConfig.rangeIndexVersion = 2
$.OFFLINE.tableIndexConfig.starTreeIndexConfigs[] = [{dimensionsSplitOrder, functionColumnPairs, maxLeafRecords}]
$.OFFLINE.tableIndexConfig.enableDynamicStarTreeCreation      .loadMode = "MMAP"|"HEAP"
$.OFFLINE.tableIndexConfig.tierOverwrites.{hotTier,coldTier}.starTreeIndexConfigs
$.OFFLINE.fieldConfigList[] = [{name, encodingType:"RAW", indexTypes:[]}]
$.OFFLINE.isDimTable = false
```

There is **no sorted-column key in table config**; sortedness is a *segment* property
(`$[<seg>].columns[i].sorted`). `segmentPartitionConfig` absent on these tables; per-column
`partitions`/`partitionFunction` existed but were `null`.

**Schema -> describe_table:** `TableSchema.columns` = concat of `dimensionFieldSpecs` +
`metricFieldSpecs` + `dateTimeFieldSpecs` (the last absent entirely on baseballStats). Pinot
has **no column comment field**, so `ColumnInfo.comment` is always None. `row_estimate` maps to
`/tables/{t}/metadata -> numRows`.

**No catalog/schema hierarchy.** Pinot is flat; MSE plans show implicit schema `default`
(`PinotLogicalTableScan(table=[[default, airlineStats]])`). `list_catalogs` must synthesise one.

## 2. Broker query surface

`POST /query/sql {"sql":"..."}` returns rows under `resultTable` and a large flat metadata
block **as siblings of resultTable at the top level** (not nested under `stats`).

### Full raw response — aggregation (`02-agg-groupby.json`)

`SELECT Carrier, count(*) AS n FROM airlineStats GROUP BY Carrier ORDER BY n DESC LIMIT 5`

```json
{
  "resultTable": {
    "dataSchema": {"columnNames":["Carrier","n"],"columnDataTypes":["STRING","LONG"]},
    "rows": [["WN",2008],["EV",1195],["OO",1159],["DL",1131],["AA",905]]
  },
  "numRowsResultSet": 5,
  "partialResult": false,
  "exceptions": [],
  "numGroupsLimitReached": false,
  "numGroupsWarningLimitReached": false,
  "timeUsedMs": 14,
  "requestId": "786551596000000011",
  "clientRequestId": null,
  "brokerId": "Broker_172.17.0.2_8000",
  "numDocsScanned": 9746,
  "totalDocs": 9746,
  "numEntriesScannedInFilter": 0,
  "numEntriesScannedPostFilter": 9746,
  "numServersQueried": 1,
  "numServersResponded": 1,
  "numSegmentsQueried": 31,
  "numSegmentsProcessed": 31,
  "numSegmentsMatched": 31,
  "numConsumingSegmentsQueried": 0,
  "numConsumingSegmentsProcessed": 0,
  "numConsumingSegmentsMatched": 0,
  "minConsumingFreshnessTimeMs": 0,
  "numSegmentsPrunedByBroker": 0,
  "numSegmentsPrunedByServer": 0,
  "numSegmentsPrunedInvalid": 0,
  "numSegmentsPrunedByLimit": 0,
  "numSegmentsPrunedByValue": 0,
  "brokerReduceTimeMs": 0,
  "offlineThreadCpuTimeNs": 0,
  "realtimeThreadCpuTimeNs": 0,
  "offlineSystemActivitiesCpuTimeNs": 0,
  "realtimeSystemActivitiesCpuTimeNs": 0,
  "offlineResponseSerializationCpuTimeNs": 0,
  "realtimeResponseSerializationCpuTimeNs": 0,
  "offlineTotalCpuTimeNs": 0,
  "realtimeTotalCpuTimeNs": 0,
  "explainPlanNumEmptyFilterSegments": 0,
  "explainPlanNumMatchAllFilterSegments": 0,
  "traceInfo": {},
  "tablesQueried": ["airlineStats"],
  "offlineThreadMemAllocatedBytes": 0,
  "realtimeThreadMemAllocatedBytes": 0,
  "offlineResponseSerMemAllocatedBytes": 0,
  "realtimeResponseSerMemAllocatedBytes": 0,
  "offlineTotalMemAllocatedBytes": 0,
  "realtimeTotalMemAllocatedBytes": 0,
  "pools": [-1],
  "rlsFiltersApplied": false,
  "groupsTrimmed": false
}
```

That is **every** metadata field on 1.5.1. For Lagaam:
- Result shape: `resultTable.dataSchema.columnNames[]`, `.columnDataTypes[]`, `.rows[][]`,
  `numRowsResultSet` -> `QueryResult.columns/rows/row_count`.
- Actual cost (post-execution, for the audit log): `numDocsScanned`, `totalDocs`,
  `numEntriesScannedInFilter`, `numEntriesScannedPostFilter`.
- Segment fan-out: `numSegmentsQueried`/`Processed`/`Matched` + all five pruning counters
  (`numSegmentsPrunedByBroker`, `ByServer`, `ByValue`, `ByLimit`, `PrunedInvalid`) — **all
  present on 1.5.1**.
- Trust flags -> `QueryResult.warnings`: `partialResult`, `numGroupsLimitReached`,
  `numGroupsWarningLimitReached`, `groupsTrimmed`, `exceptions[]`.
- Realtime freshness: `numConsumingSegments*`, `minConsumingFreshnessTimeMs`.
- **No bytes-scanned field exists.** Nearest are the `*MemAllocatedBytes` counters, all `0`
  throughout on this single-node quickstart. **Pinot never reports bytes read.**

Selection with time filter (`02-select-timefilter.json`), identical shape:
`numDocsScanned: 15`, `totalDocs: 9746`, `numEntriesScannedPostFilter: 45`,
`numSegmentsProcessed: 3` of `numSegmentsQueried: 31`, `numSegmentsPrunedByServer: 28`,
`numSegmentsPrunedByValue: 28`.

## 3. Pre-execution plan info — THE CRUX

**Pinot 1.5.1 cannot quote bytes for a query before execution, and cannot quote rows in any
trustworthy way. A Lagaam quotation must be computed from segment metadata plus the SQL's
time/partition predicates.**

### v1 EXPLAIN — operator shapes; doc counts only when the filter is trivial

Shape: `columnNames = ["Operator","Operator_Id","Parent_Id"]`, one row per node.

`EXPLAIN PLAN FOR SELECT Carrier, count(*) FROM airlineStats WHERE DaysSinceEpoch > 16080 GROUP BY Carrier`
(`03-explain-v1-groupby.json`):

```
['BROKER_REDUCE(limit:10)', 1, 0]
['COMBINE_GROUP_BY', 2, 1]
['PLAN_START(numSegmentsForThisPlan:1)', -1, -1]
['GROUP_BY(groupKeys:Carrier, aggregations:count(*))', 3, 2]
['PROJECT(Carrier)', 4, 3]
['DOC_ID_SET', 5, 4]
['FILTER_MATCH_ENTIRE_SEGMENT(docs:322)', 6, 5]
```

Numbers appear in only two conditional places:
1. `PLAN_START(numSegmentsForThisPlan:N)` — segments sharing this plan shape. Plans are
   **deduplicated by shape**, so N is not the table's segment count.
2. `docs:N` — **only** in `FILTER_MATCH_ENTIRE_SEGMENT`, i.e. only when the predicate matches
   the whole segment and no filtering is needed.

**With a real predicate the doc count disappears:**
```
WHERE Carrier='WN' (no index)  -> ["FILTER_FULL_SCAN(operator:EQ,predicate:Carrier = 'WN')",6,5]   NO cardinality
WHERE teamID='BOS' (inverted)  -> ['FAST_FILTERED_COUNT',3,2]
                                  ["FILTER_INVERTED_INDEX(indexLookUp:inverted_index,operator:EQ,predicate:teamID = 'BOS')",4,3]  NO cardinality
no WHERE, count(*) star-tree   -> ['FILTER_STARTREE_INDEX',6,5]                                    NO cardinality
```
Evidence `03-explain-v1-carrier.json`, `03-explain-v1-invindex.json`, `03-explain-v1-fullscan.json`.
v1 EXPLAIN is a **shape oracle, not a cost oracle**.

**Useful side effect: v1 EXPLAIN performs real segment pruning and reports it in the response
metadata, without executing.** `03-explain-v1-verbose.json` (`WHERE DaysSinceEpoch > 16090`):
`numSegmentsQueried: 31, numSegmentsPrunedByServer: 20, numSegmentsPrunedByValue: 20,
numDocsScanned: 0`. `03-explain-v1-select.json` similarly reported `numSegmentsPrunedByValue: 10`.
**This is a pre-execution measurement of how many segments a time predicate eliminates,
straight from the engine — the single most valuable pre-execution signal available.**

`EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR` is accepted on v1 but returns the same physical plan
(`03-explain-v1-without-impl.json`). `explainPlanVerbose=true` is accepted.

### MSE — Calcite rowcounts exist but are FABRICATED

Both activation methods work and give identical plans:
`"queryOptions":"useMultistageEngine=true"` (`03-explain-mse-join-queryoptions.json`) and
`SET useMultistageEngine=true;` prefix (`03-explain-mse-join-setprefix.json`).
Shape differs: `columnNames = ["SQL","PLAN","RULE_TIMINGS"]`, one row, plan as a newline
string in `resultTable.rows[0][1]`.

| Variant | Supported | Rowcounts | Evidence |
|---|---|---|---|
| `EXPLAIN PLAN FOR` | yes | no | `03-explain-mse-join-queryoptions.json` |
| `EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR` | yes | **yes — but fake** | `03-explain-mse-all-attributes.json` |
| `EXPLAIN PLAN EXCLUDING ATTRIBUTES FOR` | yes | no (bare names) | `03-explain-mse-excluding-attributes.json` |
| `EXPLAIN IMPLEMENTATION PLAN FOR` | yes | no (mailbox/stage tree w/ host:port) | `03-explain-mse-implementation.json` |
| `EXPLAIN PLAN WITHOUT IMPLEMENTATION FOR` | yes | no | `03-explain-mse-without-impl.json` |
| `EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR` | yes | **no** — `rels[]` has no cost fields | `03-explain-mse-json.json` |

`INCLUDING ALL ATTRIBUTES` emits Calcite cost:
```
PinotLogicalAggregate(...aggType=[FINAL]): rowcount = 15.0, cumulative cost = 13624.59, id = 349
  PinotLogicalExchange(distribution=[hash[0]]): rowcount = 150.0, cumulative cost = 13607.71, id = 339
    PinotLogicalAggregate(...aggType=[LEAF]): rowcount = 150.0, cumulative cost = 7594.95, id = 330
      LogicalJoin(condition=[=($1, $2)], joinType=[inner]): rowcount = 1500.0, cumulative cost = 7426.20, id = 322
        ...
            PinotLogicalTableScan(table=[[default, airlineStats]]): rowcount = 100.0, cumulative cost = 100.0, id = 298
```

**These are not statistics. Every table scan reports exactly `rowcount = 100.0` regardless of
real size:**

| Query | Real rows | TableScan rowcount | Evidence |
|---|---|---|---|
| `count(*) FROM baseballStats` | **97,889** | `100.0` | `03-explain-mse-baseball-count.json` |
| `count(*) FROM airlineStats` | **9,746** | `100.0` | `03-explain-mse-airline-count.json` |

Derived numbers inherit the fiction: the cross join
`SELECT count(*) FROM airlineStats a, baseballStats b` (`03-explain-mse-crossjoin.json`)
reports `LogicalJoin: rowcount = 10000.0` = 100 x 100, whereas the true product is
9,746 x 97,889 = **954,024,994** — five orders of magnitude larger. A selective filter gets a
flat 50% guess: `LogicalFilter(condition=[>($22, 16090)]): rowcount = 50.0` on a 100.0 input
(`03-explain-mse-airline-filter.json`), true selectivity ~11/31 segments.

**Conclusion. Pinot 1.5.1 has no table statistics wired into its Calcite cost model. The
rowcount/cumulative-cost attributes are structurally real but numerically constant, so they
are worse than useless for a budget gate — a gate reading them would price a 954-million-row
cross join at 10,000 rows and admit it. `EXPLAIN ... INCLUDING ALL ATTRIBUTES` must NOT be
used as a cost source.** No equivalent of Trino's `EXPLAIN (TYPE IO/LOGICAL)` exists.

What MSE EXPLAIN *is* good for: **shape analysis without executing** — joins, `joinType`,
whether a join has an equality (`condition=[true]` marks a cross join; the JSON form exposes
`rels[].joinType` and `rels[].condition.op.kind`), window functions, tables touched. That is
Lagaam's "unpriceable shape" detection, and the plan is more reliable than the SQL text.

## 4. Auto-LIMIT and caps

### The two engines differ — a safety hazard

| Engine | `SELECT Carrier, Origin FROM airlineStats` (no LIMIT) | rows |
|---|---|---|
| v1 (default) | auto-limited | **10** |
| MSE | **not limited** | **9,746 — the entire table** |

Evidence `04-nolimit-v1.json`, `04-nolimit-mse.json`.

v1 metadata on that query: `numDocsScanned: 10`, `numSegmentsProcessed: 1` of 31,
`numSegmentsPrunedByLimit: 30`. **There is NO metadata flag saying "I truncated your result."**
`partialResult` stays false, `numGroupsLimitReached` stays false. Only signal is
`numRowsResultSet == 10` exactly. Default limit **10**, confirmed by `BROKER_REDUCE(limit:10)`
in every v1 EXPLAIN without a LIMIT vs `BROKER_REDUCE(limit:5)` with `LIMIT 5`.

**Implication:** Lagaam must inject an explicit LIMIT and set caps itself. It can never rely on
Pinot's default, because any join forces MSE (see §5) which silently removes the cap. The
`max_rows + 1` truncation trick works only if the LIMIT is written into the SQL.

### Query options verified on 1.5.1

Body form `"queryOptions":"k=v;k2=v2"` (semicolons), or `SET k=v;` prefix.
Matrix: `04-query-options-matrix.json`.

| Option | Accepted | Enforced | Effect |
|---|---|---|---|
| `timeoutMs` | yes | **yes** | below |
| `numGroupsLimit` | yes | **yes** | below |
| `maxRowsInJoin` (MSE) | yes | **yes** | errorCode 245 |
| `maxRowsInWindow` (MSE) | yes | **yes** | errorCode 245 |
| `maxQueryResponseSizeBytes` | yes | **yes** | errorCode 503 |
| `maxServerResponseSizeBytes` | yes | **yes** | errorCode 503 |
| `minSegmentGroupTrimSize` | yes | no visible effect here | 100 rows, groupsTrimmed:false |
| `minServerGroupTrimSize` | yes | no visible effect here | 100 rows, groupsTrimmed:false |
| `explainPlanVerbose` | yes | yes | changed EXPLAIN grouping |
| **`bogusOptionName`** | **silently accepted** | — | **ran normally, no error** |

**An unrecognised query option is silently ignored** — a typo disables a safety cap with no
signal. The adapter must treat its cap list as verified-by-test constants.

### `timeoutMs=1` — error shape

Both engines return **HTTP 200** with populated `exceptions[]` and `partialResult: true`.

v1 (`04-timeout1-v1.json`):
```json
{"exceptions":[{"message":"1 servers [172.17.0.2_O] not responded","errorCode":427}],
 "partialResult":true,"timeUsedMs":5}
```
MSE (`04-timeout1-mse.json`):
```json
{"exceptions":[{"message":"BrokerTimeoutError: Timed out while planning query","errorCode":400}],
 "partialResult":true}
```
`errorCode` differs by engine — **427** on v1, **400** on MSE. The v1 message names an
internal server host.

### `numGroupsLimit=2` — the flag does flip

`SELECT Origin, count(*) FROM airlineStats GROUP BY Origin LIMIT 100`
(`04-groupby-baseline.json` vs `04-numgroupslimit2.json`):

| | baseline | numGroupsLimit=2 |
|---|---|---|
| rows | 100 | **22** |
| `numGroupsLimitReached` | false | **true** |
| `partialResult` | false | **true** |
| `numDocsScanned` | 3005 | 3005 (unchanged) |

**Yes, `numGroupsLimitReached` flips to true, and `partialResult` with it.** The query still
returns **HTTP 200 with wrong-but-plausible aggregates** — 22 groups presented as complete.
Both must be surfaced as warnings, arguably as hard failures. Note it does not reduce work.

### maxRowsInJoin / maxRowsInWindow (MSE)

```
maxRowsInJoin=5   -> 245: "Cannot build in memory hash table for join operator, reached number of rows limit: 5.
                           Consider increasing the limit ... via: - The query option 'maxRowsInJoin'
                           - The hint 'max_rows_in_join' in the 'joinOptions' ..."
maxRowsInWindow=5 -> 245: "Cannot build in memory window cache for WINDOW operator, reach number of rows limit: 5"
```
**Hard failures, not truncations** — genuine fail-closed backstop for `max_intermediate_rows`.

### maxQueryResponseSizeBytes / maxServerResponseSizeBytes

```
503: "Serialized query response size 5190 exceeds threshold 100 for requestId ... from broker Broker_172.17.0.2_8000"
```
Both accepted and enforced — the only byte-denominated control Pinot offers.

## 5. Read-only surface — what the AST allowlist must deny

**One statement kind gets through that Trino never had to consider:
`INSERT INTO ... FROM FILE ...`. The broker parses it, accepts it, and dispatches it as an
ingestion task — on BOTH engines.**

| Statement | v1 | MSE |
|---|---|---|
| `INSERT INTO airlineStats FROM FILE 'file:///tmp/x.csv'` | **ACCEPTED** | **ACCEPTED** |
| `INSERT INTO airlineStats (Carrier) VALUES ('XX')` | rejected 150 | rejected 150 |
| `DELETE FROM ...` | rejected 150 | rejected **700** |
| `UPDATE ... SET ...` | rejected 150 | rejected **700** |
| `CREATE TABLE foo (a INT)` | rejected 150 | rejected 150 |
| `DROP TABLE airlineStats` | rejected 150 | rejected 150 |
| `SET useMultistageEngine=true` (alone) | rejected 150 | rejected 150 |
| `SHOW TABLES` | rejected 150 | rejected 150 |
| `SELECT * FROM airlineStats LIMIT 1` | accepted | accepted |

Evidence `05-nonselect-statements.json`, `05-insert-from-file-v1.json`, `05-insert-from-file-MSE.json`.

### The one that matters

Both engines returned **HTTP 200, `exceptions: []`**, with a giveaway schema:
```json
{"resultTable":{"dataSchema":{"columnNames":["tableName","taskJobName"],
                              "columnDataTypes":["STRING","STRING"]},"rows":[]},
 "numRowsResultSet":0,"exceptions":[],"requestId":null,"brokerId":null,"tablesQueried":[]}
```
`["tableName","taskJobName"]` shows the broker routed it to the **Minion ingestion task** path.
Zero rows only because this quickstart has no Minion task executor — **not** because it was
refused. On a cluster with Minion enabled this is a live write path reachable through the same
`/query/sql` endpoint Lagaam uses for reads.

**Requirement: the Pinot AST allowlist must explicitly deny `INSERT` in all forms.**

### Other allowlist notes

- `DELETE`/`UPDATE` are rejected on v1 by *Calcite type-cast accidents*
  (`class org.apache.calcite.sql.SqlDelete cannot be cast to ... SqlSelect`), not a deliberate
  read-only check. Deny them in the AST; do not rely on the engine.
- **`SHOW TABLES` and `information_schema` do not exist** on either engine:
  `"SQLParsingError: ... SHOW TABLES: From line 1, column 1 to line 1, column 4: Non-query
  expression encountered in illegal context", errorCode 150`. Catalog discovery **must** use
  controller REST (§1) — there is no SQL path.
- `SET x=y` standalone is rejected (`"SqlNode with executable statement not found!"`); valid
  only as a **prefix**.
- **`SELECT *` is accepted** and unbounded on MSE — Lagaam's no-`SELECT *` rule matters more here.
- **JOINs force MSE.** v1 rejects any join:
  `"It seems that the query is only supported by the multi-stage query engine ...", errorCode 150`
  (`05-join-v1.json`); MSE accepts (`05-join-mse.json`). **Any join lands on the engine with no
  auto-LIMIT — the two hazards compound.**

## 6. Segment pruning is observable

**Yes, clearly — and it is the mechanism a Pinot quotation should be built on.**

### Time filter vs none (airlineStats, time column DaysSinceEpoch, 31 segments)

| | A: `WHERE Carrier='WN'` | B: `... AND DaysSinceEpoch BETWEEN 16071 AND 16073` |
|---|---|---|
| `numSegmentsQueried` | 31 | 31 |
| `numSegmentsProcessed` | 31 | **3** |
| `numSegmentsMatched` | 31 | **3** |
| `numSegmentsPrunedByServer` | 0 | **28** |
| `numDocsScanned` | 2008 | 260 |
| `numEntriesScannedInFilter` | 9746 | **1101** |
| result | 2008 | 260 |

Evidence `06-prune-notimefilter.json`, `06-prune-timefilter.json`. The time predicate
eliminated 28 of 31 segments before any scanning. `numSegmentsQueried` stays 31 in both — it
is the denominator; **`numSegmentsProcessed` is the numerator reflecting real work**.

**The counter that moves depends on where pruning happens**: in §2 the same predicate showed
`numSegmentsPrunedByValue: 28`, here it registered as `numSegmentsPrunedByServer: 28` with
`ByValue: 0`. **Sum the pruning counters**, or better use
`numSegmentsProcessed / numSegmentsQueried` as the surviving fraction.

### Inverted index vs none (baseballStats, invertedIndexColumns ["teamID"])

| | C: `WHERE teamID='BOS'` (inverted) | D: `WHERE league='AL'` (no index) |
|---|---|---|
| `numEntriesScannedInFilter` | **0** | **97,889** |
| `numDocsScanned` | 4,130 | 44,369 |
| `totalDocs` | 97,889 | 97,889 |

Evidence `06-idx-inverted.json`, `06-idx-noindex.json`. **Yes, `numEntriesScannedInFilter`
differs decisively**: an index lookup scans **zero** entries in the filter phase; an unindexed
column scans **every row**. And it is **predictable before execution** from
`$[<seg>].indexes[<col>]["inverted-index"] == "YES"` (§1) — a quotation can decide "full filter
scan of N docs" vs "index lookup" without running anything.

## 7. Errors

**All query errors return HTTP 200 with a populated `exceptions[]`.** Never branch on HTTP
status. Fields: `exceptions[i].message` (string), `exceptions[i].errorCode` (int). No
`errorName`, no SQLSTATE, no structured position. Evidence `07-errors.json`.

| Condition | v1 | MSE |
|---|---|---|
| bad column | **710** `UnknownColumnError: Unknown columnName 'nosuchcolumn' found in the query` | **700** `QueryValidationError: ... The definition of column 'nosuchcolumn' depends on itself ...` |
| unknown table | **190** `TableDoesNotExistError` | **190** `TableDoesNotExistError: ... Object 'nosuchtable' not found.` |
| syntax error | **150** `SQLParsingError: Caught exception while parsing query: ... Encountered "" at line 1, column 8.` | **150** identical |
| unknown function | **700** `Unsupported function: nosuchfunc` | **700** `QueryValidationError: ... No match found for function signature nosuchfunc(<CHARACTER>)` |
| timeout | **427** `1 servers [<host>_O] not responded` | **400** `BrokerTimeoutError: Timed out while planning query` |
| join row limit | — | **245** |
| window row limit | — | **245** |
| response too large | **503** | **503** |
| join on v1 | **150** `It seems that the query is only supported by the multi-stage query engine...` | n/a |

Observed errorCode set on 1.5.1: **150** parsing, **190** table not found, **245** execution
resource limit, **400** broker timeout, **427** server not responded, **503** response size,
**700** validation/unsupported function, **710** unknown column.

Two things the adapter's `_translate_error` equivalent must handle:
1. **The same logical error has different codes per engine** (bad column 710 on v1, 700 on MSE),
   and MSE folds bad-column, bad-function and unsupported-DML all into 700. Classification needs
   `errorCode` **plus** a message prefix (`UnknownColumnError:`, `QueryValidationError:`,
   `TableDoesNotExistError:`, `SQLParsingError:`, `BrokerTimeoutError:`).
2. **Messages leak internal topology** — `Broker_172.17.0.2_8000`, `1 servers [172.17.0.2_O]`,
   `Server_172.17.0.2_7050`, `requestId`. The Trino `_detail()` discipline applies with more
   force: curate hints, never pass raw text to the agent.

Self-correctable -> `QueryFailedError`: 150, 190, 700, 710.
Engine/budget faults -> `EngineError` / budget denial: 245, 400, 427, 503.

## 8. Auth

**Yes — HTTP Basic auth is supported on both the broker query endpoint and the controller REST
API in the 1.x line including 1.5.1.** The quickstart runs with it off (every call in this spike
was unauthenticated), but the config keys exist.

Docs consulted (site states `PINOT_VERSION=1.5.1`):
- https://docs.pinot.apache.org/operate-pinot/security/authentication/basic-auth-access-control
- https://docs.pinot.apache.org/operate-pinot/security/authentication/zkbasicauthaccesscontrol
- https://docs.pinot.apache.org/operate-pinot/security/access-control
- https://docs.pinot.apache.org/start-here/pinot-versions

The docs' own broker example is exactly Lagaam's shape:
```
curl http://localhost:8000/query/sql \
  -H 'Authorization: Basic dXNlcjpzZWNyZXQ=' \
  -d '{"sql":"SELECT * FROM baseballStats"}'
```

Controller (`controller.conf`):
```properties
controller.admin.access.control.factory.class=org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory
controller.admin.access.control.principals=admin,user
controller.admin.access.control.principals.admin.password=verysecret
controller.admin.access.control.principals.user.password=secret
controller.admin.access.control.principals.user.tables=myusertable,baseballStats
controller.admin.access.control.principals.user.excludeTables=excludedTable
controller.admin.access.control.principals.user.permissions=READ
```

Broker (`broker.conf`) — **the key is `...access.control.class`, NOT `...factory.class`**; the
docs flag this asymmetry and using the wrong one silently leaves auth off:
```properties
pinot.broker.access.control.class=org.apache.pinot.broker.broker.BasicAuthAccessControlFactory
pinot.broker.access.control.principals=admin,user
pinot.broker.access.control.principals.admin.password=verysecret
pinot.broker.access.control.principals.user.password=secret
pinot.broker.access.control.principals.user.tables=baseballStats,otherstuff
```

Broker has **no `.permissions` key** — broker access is always READ. Bad credentials -> **401**;
valid credentials without table/permission access -> **403**. Basic auth exists since 0.8.0.

Classes: `org.apache.pinot.controller.api.access.BasicAuthAccessControlFactory`,
`org.apache.pinot.broker.broker.BasicAuthAccessControlFactory`, ZK variants
`ZkBasicAuthAccessControlFactory` (0.10.0+, bcrypt creds in the Helix PropertyStore,
hot-reloadable), `org.apache.pinot.server.access.BasicAuthAccessFactory` for the server admin
API, and `AllowAllAccessFactory` as the default when unset — what this quickstart runs.

**Bearer tokens: not natively verified.** Pinot "tokens" are base64 `user:password` tuples
(RFC 7617). `Authorization: Bearer` appears only client-side; the docs state accepting it in
Swagger "only changes how Swagger documents and collects the header; Pinot's runtime
authentication behavior is unchanged." Real Bearer/OIDC needs a custom `AccessControlFactory`.

**Bonus for Lagaam: row-level security exists (1.4.0+)** via
`pinot.broker.access.control.principals.<user>.<tableName>.rls=<predicate>`, and the broker
response carries **`rlsFiltersApplied`** — already visible in every response in §2 (`false`
here). If RLS is on, a quotation from full segment metadata **overestimates**, since the user
sees a filtered subset.

**Consequence:** the adapter needs username/password threaded into **both** broker query calls
and controller metadata calls — separate services with separate principal lists.

## 9. Python client: pinotdb vs plain httpx

### pinotdb on PyPI (`09-pypi-pinotdb.json`)

| | |
|---|---|
| Latest | **9.1.2** |
| Released | **2026-05-25** |
| Python | `>=3.10,<4` |
| Core deps | **`httpx>=0.28.1,<0.29`**, `ciso8601`, `h11==0.16.0`, `urllib3==2.7.0` |
| `[sqlalchemy]` extra | `sqlalchemy>=2.0,<3`, `requests>=2.25.0`, `greenlet` |
| Home | https://github.com/startreedata/pinot-dbapi (StarTree, not Apache) |

**It depends on `httpx`, not `requests`** — `requests` only via the optional sqlalchemy extra.
Actively maintained (7.0.0, 8.0.0, 9.0.0, 9.1.0, 9.1.1, 9.1.2 all in 2026).

### Measured against this container (`09-pinotdb-test.txt`)

- **Query options and MSE are first-class constructor params**: `Cursor.__init__` takes
  `use_multistage_engine=False`, `query_options=None`, plus `username`, `password`, `timeout`,
  `extra_request_headers`, `ignore_exception_error_codes`, `acceptable_respond_fraction`,
  `preserve_types`, `session`. Verified:
  `connect(..., use_multistage_engine=True, query_options="numGroupsLimit=2")` returned 2 rows.
- **Full metadata preserved**: `cursor.raw_query_response` is the complete dict;
  `cursor.query_stats` exposes `numDocsScanned` (measured 9746), `numEntriesScannedInFilter`,
  `numSegmentsQueried`, `groupsTrimmed`. **Nothing from §2 is lost.**
- **Errors** raise `pinotdb.exceptions.DatabaseError` whose payload is the exception dict
  `{'errorCode': 710, 'message': "UnknownColumnError: ..."}`. DB-API classes exist
  (`Error`, `DatabaseError`, `ProgrammingError`, `OperationalError`, ...) **but all query errors
  collapse to `DatabaseError`** — no per-errorCode subclass and **no `.errorCode` attribute**.
- `description` uses pinotdb enums (`Type.STRING`, `Type.NUMBER`), not Pinot's richer
  `columnDataTypes` (`LONG`, `INT`, `BYTES`) — those are in `raw_query_response`.

### Recommendation: **use plain `httpx`, not pinotdb**

The Trino adapter uses the official `trino` client because Trino's protocol is genuinely
non-trivial — multi-request `nextUri` polling, retry semantics, session negotiation. **None of
that applies here**: Pinot is one POST, one JSON response. Reasons:

1. **The adapter needs controller REST anyway.** Everything in §1 is controller REST on 9000,
   which pinotdb does not cover at all (broker-only `/query/sql`). Since §5 proved there is no
   `SHOW TABLES` or `information_schema`, catalog grounding *must* be REST. Adding pinotdb
   means two HTTP clients for one engine.
2. **Its value is a DB-API veneer Lagaam does not want.** The port is async; pinotdb's sync
   cursor would need `anyio.to_thread` wrapping exactly as Trino does — but only because the
   library is sync, not because the protocol is. `httpx.AsyncClient` is genuinely async.
3. **Its error handling loses the field Lagaam classifies on.** §7 needs `errorCode` *and* a
   message prefix, per engine. pinotdb flattens everything into `DatabaseError` with no
   `.errorCode`, so the adapter would parse a repr'd dict back into structure it already had.
4. **Hard transitive pins** — `h11==0.16.0`, `urllib3==2.7.0`, `httpx<0.29` — to inherit for a
   client saving maybe 60 lines.
5. **Not an Apache artifact** — StarTree's pinot-dbapi. No "official client" parity argument.

Worth borrowing: it confirms the request shape (`{"sql":..., "queryOptions":"..."}`) and that
`queryOptions` is the right channel for MSE and caps — both verified independently in §3/§4.

Caveat for httpx: **every error path returns HTTP 200**, so `raise_for_status()` is meaningless.
Check `exceptions[]`, `partialResult`, `numGroupsLimitReached`, `groupsTrimmed` on every response.

## 10. Realtime / hybrid

**`-type STREAM` and `-type HYBRID` exist; I ran a REALTIME table successfully and it fits in
8 GiB. But `-type HYBRID` is BROKEN inside a container on 1.5.1, and the realtime finding is
the most important trust result in this spike.**

### -type HYBRID fails in a container on 1.5.1 — Kafka is no longer embedded

`docker run ... QuickStart -type HYBRID` exited 0 after failing to start Kafka
(`10-hybrid-logs-tail.txt`):
```
***** Starting Kafka *****
java.lang.RuntimeException: Failed to initialize Kafka for HybridQuickstart
  at org.apache.pinot.plugin.stream.kafka30.server.KafkaServerStartable.start(...)
Caused by: java.lang.IllegalStateException: Docker daemon is not running or not reachable.
  Quickstart starts Kafka in Docker on localhost:19092. Start Docker, or pass
  -kafkaBrokerList <host:port> to use an external Kafka broker.
Caused by: java.io.IOException: Cannot run program "docker": ... No such file or directory
```
On 1.5.1 the stream quickstarts **shell out to the `docker` CLI** rather than embedding Kafka,
so they cannot work inside a container without a mounted Docker socket.

`-kafkaBrokerList` alone does **not** fix it: with a reachable external Kafka the quickstart
still failed to create tables, because the bundled table config **hardcodes**
`"stream.kafka.broker.list": "localhost:19092"` and the flag does not rewrite it
(`TransientConsumerException: java.util.concurrent.TimeoutException`).

### What worked

Kafka 3.9.0 (KRaft) on a user-defined network; Pinot `-type STREAM` on that network with
`-Xmx2G`; topic `flights-realtime` pre-created; the bundled realtime table config rewritten to
point `stream.kafka.broker.list` at the reachable broker, then POSTed to `/schemas` and `/tables`:
```
{"unrecognizedProperties":{},"status":"airlineStats successfully added"}
{"unrecognizedProperties":{},"status":"Table airlineStats_REALTIME successfully added"}
```
Data streamed in with Pinot's own `StreamAvroIntoKafka`; rows queryable within seconds.
Memory with all four containers running (`10-mem-with-realtime.txt`):

| Container | Mem |
|---|---|
| pinot-rt-probe (realtime Pinot) | 1.274 GiB |
| lagaam-kafka | 359 MiB |
| lagaam-pinot (batch) | 1.804 GiB |
| lagaam-trino | 2.564 GiB |
| **total** | **~6.0 GiB of 7.653 GiB** |

**A realtime Pinot plus Kafka costs ~1.6 GiB and does fit alongside the batch instance.** Both
probe containers and the network were removed afterwards; lagaam-pinot and lagaam-trino unaffected.

### CRITICAL: REALTIME metadata is EMPTY while data is queryable

Measured at one moment (`10-rt-metadata.json`, `10-rt-size.json`, `10-rt-seg-metadata.json`,
`10-rt-query.json`) — the table demonstrably held **70 rows** and was serving them:

| Source | Reports |
|---|---|
| `POST /query/sql SELECT count(*)` | **70 rows**, `totalDocs: 70` |
| `/tables/{t}/metadata` -> `numRows` | **0** |
| `/tables/{t}/metadata` -> `diskSizeInBytes` | **0** |
| `/tables/{t}/metadata` -> `numSegments` | 10 |
| `/tables/{t}/size` -> `reportedSizeInBytes` | **-1** |
| `/tables/{t}/size` -> `estimatedSizeInBytes` | **-1** |
| `.realtimeSegments.missingSegments` | **10** (all) |
| `/segments/{t}/metadata` -> `totalDocs` | **0** |
| `... .startTimeMillis` / `.endTimeMillis` / `.timeColumn` | **null** |
| `... .startOffset` / `.endOffset` / `.segmentVersion` | **null** |

Also `$.offlineSegments` is `null` and `$.realtimeSegments` populated (mirroring §1 where
`realtimeSegments` was null) — **the split is reliable, the numbers are not.**

**Consuming (in-memory, not yet flushed) segments report zero docs, zero/-1 bytes, and no time
range.** The controller only learns a realtime segment's true size and time range once it is
sealed. So for REALTIME/HYBRID:

- A segment-metadata quotation **systematically under-quotes**, by exactly the freshest and
  usually most-queried data. This is the failure mode a budget gate must never have: it admits
  the expensive query.
- Time-range pruning (§6) **cannot be simulated** for consuming segments — `startTimeMillis` is
  null, so the adapter cannot tell whether a time predicate excludes them.
- Query-time signals are the only honest ones: `numConsumingSegmentsQueried` was **10** with
  `minConsumingFreshnessTimeMs: 1789082201338` — a *response* knows about consuming segments
  even though the *metadata* does not.

**Consequence for Lagaam:** a table with a REALTIME half must be quoted at
**`confidence: "low"`** unless the time predicate provably excludes the consuming window.
`CostEstimate` already fails safe on low confidence — exactly right here. Detection is easy:
`GET /tables/{t}` returning a `$.REALTIME` key, or `/tables/{t}/size` returning
`realtimeSegments != null` / `reportedSizeInBytes == -1`.

## What a Pinot quotation can be built from

Pinot 1.5.1 offers **no pre-execution cost estimate of any kind**. There is no analogue of
`EXPLAIN (TYPE IO)` — no endpoint, option or statement returns bytes-to-be-scanned, and the only
rowcounts available (MSE Calcite attributes) are a hardcoded constant 100 per table scan (§3).
A quotation must be **synthesised by the adapter** from static segment metadata plus predicates.

### Numbers available BEFORE execution

| Number | Endpoint | JSON path | Trust |
|---|---|---|---|
| Total rows in table | `GET /tables/{t}/metadata` | `$.numRows` | **High** OFFLINE — matched `count(*)` exactly (97,889 / 9,746). **Zero and wrong** for REALTIME consuming. |
| Total bytes on disk | `GET /tables/{t}/metadata` | `$.diskSizeInBytes` | **High** OFFLINE (3,342,450 = `/size`). `0` REALTIME. |
| Total bytes + replica detail | `GET /tables/{t}/size` | `$.reportedSizeInBytes`, `$.estimatedSizeInBytes` | **High** OFFLINE. `-1` when consuming/missing; `missingSegments` says how many. |
| Per-segment bytes | `GET /tables/{t}/size` | `$.offlineSegments.segments[<s>].reportedSizeInBytes` | **High** OFFLINE. `-1` REALTIME. |
| Per-segment doc count | `GET /segments/{t}/metadata` | `$[<s>].totalDocs` | **High** OFFLINE (summed to table total). `0` REALTIME consuming. |
| Per-segment time range | `GET /segments/{t}/metadata` | `$[<s>].startTimeMillis`, `.endTimeMillis`, `.timeColumn`, `.timeUnit` | **High** OFFLINE — makes predicate pruning simulable. `null` REALTIME consuming. |
| Segment count | `GET /tables/{t}/metadata` | `$.numSegments` | High |
| **Per-column, per-index bytes** | `GET /segments/{t}/metadata?columns=` | `$[<s>].columns[i].indexSizeMap.{dictionary,forward_index,...}` | **High** — exact bytes; basis for charging only projected/filtered columns. |
| Column cardinality (per segment) | `GET /segments/{t}/metadata?columns=` | `$[<s>].columns[i].cardinality` | **Medium** — exact per segment, must be combined; the table-level `columnCardinalityMap` is a **mean, not a total**. |
| Column min/max | `GET /segments/{t}/metadata?columns=` | `.minValue`/`.maxValue`, guarded by `.minMaxValueInvalid` | **Medium-High** — usable for range pruning when `minMaxValueInvalid == false`. |
| Column sortedness | `GET /segments/{t}/metadata?columns=` | `$[<s>].columns[i].sorted` | High |
| Index presence per column | `GET /segments/{t}/metadata?columns=` | `$[<s>].indexes[<col>]["inverted-index"\|"range-index"\|...]` | **High** — §6 proved inverted -> `numEntriesScannedInFilter == 0`, none -> full table scan. Directly predicts filter cost. |
| Star-tree presence | `GET /tables/{t}` or segment metadata | `.starTreeIndexConfigs[]`, `$[<s>].star-tree-index[]` | **High** for existence; **cost impact not quantifiable** — a star-tree-answered aggregation reads a pre-aggregated tree, so doc-count quotes over-charge. |
| Time column + type | `GET /tables/{t}` | `$.OFFLINE.segmentsConfig.timeColumnName`, `.timeType` | **High** — key for matching predicates to segment time ranges. |
| Table type | `GET /tables/{t}` | presence of `$.OFFLINE` / `$.REALTIME` | **High** — the confidence switch. |
| **Segments surviving a predicate** | `EXPLAIN PLAN FOR <sql>` (v1) | response `numSegmentsQueried` vs `numSegmentsPrunedByServer`/`ByValue` | **High and engine-authoritative** — measured 20/31 and 10/31 pruned with `numDocsScanned: 0`. **The engine does the pruning arithmetic without executing.** |
| Access method per predicate | `EXPLAIN PLAN FOR <sql>` (v1) | `resultTable.rows[][0]` — `FILTER_FULL_SCAN` / `FILTER_INVERTED_INDEX` / `FILTER_STARTREE_INDEX` / `FILTER_MATCH_ENTIRE_SEGMENT(docs:N)` | **High** for method; `docs:N` only for whole-segment matches. |
| Query shape (joins, cross joins, windows) | `EXPLAIN PLAN ... FOR` (MSE) | plan text, or `rels[].joinType` / `rels[].condition` in JSON form | **High** — the reliable use of MSE EXPLAIN. |
| MSE `rowcount` / `cumulative cost` | `EXPLAIN PLAN INCLUDING ALL ATTRIBUTES FOR` | plan text | **UNUSABLE — do not use.** Constant `100.0` per scan; priced a 954M-row cross join at 10,000. |

### Recommended quotation algorithm

1. Read table type. If a REALTIME half exists (or `/size` reports `-1` / `missingSegments > 0`),
   the consuming portion is unmeasurable -> **`confidence: "low"`** unless the time predicate
   provably excludes it.
2. Run **v1 `EXPLAIN PLAN FOR`**. Costs no execution (`numDocsScanned: 0`) and yields two things
   at once: the **surviving segment count** from the response's pruning counters, and the
   **access method** per filter from the operator rows.
3. `row_estimate` ~ sum of `totalDocs` over surviving segments (from `/segments/{t}/metadata`,
   filtered by step 2's pruning outcome).
4. `scanned_bytes` ~ sum over surviving segments of `indexSizeMap` entries for **only the columns
   the query references** (projected + filtered) — which is why `?columns=` matters. Fall back to
   `reportedSizeInBytes` prorated by surviving-segment fraction when column attribution is not
   possible. **Always an adapter-computed estimate, never an engine number** — so its honest
   confidence ceiling is lower than Trino's.
5. `max_intermediate_rows`: **no pre-execution source exists.** MSE's Calcite rowcounts are
   fabricated (§3). Sound options: (a) compute the join product from participating tables'
   `numRows` the way Lagaam's NaN-join rule already does, and (b) enforce at runtime with
   `maxRowsInJoin` / `maxRowsInWindow`, which fail **hard** with errorCode 245 rather than
   truncating (§4) — a genuine fail-closed backstop.
6. `confidence: "high"` only for an OFFLINE-only table where step 4 produced a byte number;
   otherwise `"low"`, which the existing `CostEstimate` validator already handles.

### Non-negotiable adapter requirements this spike established

- **Always inject an explicit LIMIT.** MSE has no auto-limit (returned all 9,746 rows); v1's
  limit of 10 is silent and unflagged (§4).
- **Deny `INSERT` in the AST allowlist.** `INSERT INTO ... FROM FILE` is accepted on both engines
  and returns a success-shaped response (§5). Trino never needed this.
- **Never branch on HTTP status.** Every error is HTTP 200 with `exceptions[]` (§7).
- **Treat `partialResult`, `numGroupsLimitReached`, `numGroupsWarningLimitReached` and
  `groupsTrimmed` as correctness failures**, not warnings — a trimmed GROUP BY returns plausible
  wrong numbers (§4).
- **Verify query-option names against a tested constant list.** Unknown options are silently
  ignored, so a typo disables a cap with no error (§4).
- **Catalog grounding must use controller REST.** No `SHOW TABLES`, no `information_schema` (§5),
  no catalog/schema hierarchy — synthesise one (§1).
- **Sanitise error text before it reaches the agent.** Messages embed broker/server IPs, ports
  and request ids (§7).

## Container left running

`lagaam-pinot` is **still running** — Pinot 1.5.1 batch quickstart, controller on
http://localhost:9000, broker on http://localhost:8000 (server admin 8099), all ten quickstart
tables loaded. The pre-existing `lagaam-trino` container was not touched. The temporary
`pinot-hybrid-probe`, `pinot-rt-probe`, `lagaam-kafka` containers and the `pinotnet` network
created for §10 were removed.

## 11. Namespace depth (measured after the main spike)

Broker `POST /query/sql`, both engines unless noted:

| spelling | v1 | MSE |
|---|---|---|
| `FROM baseballStats` | OK | OK |
| `FROM baseballStats_OFFLINE` | OK (`tablesQueried: ["baseballStats"]`) | OK (`tablesQueried: ["baseballStats_OFFLINE"]`) |
| `FROM default.baseballStats` | **OK** | **OK** |
| `FROM "default"."baseballStats"` | OK | OK |
| `FROM pinot.default.baseballStats` | **HTTP 500, non-JSON**: `Table name: 'pinot.default.baseballStats' containing more than one '.' is not allowed` | ERR 190 `Object 'pinot' not found` |
| `FROM nosuchdb.baseballStats` | ERR 190 TableDoesNotExistError | ERR 190 |
| `FROM BASEBALLSTATS`, `baseballstats`, `"BaseballStats"` | OK — table names are case-insensitive, quoted or not | OK |
| `SELECT playername` (wrong case) | OK — column names are case-insensitive | OK |
| `SET database='default'; SELECT ...` | OK | OK |
| HTTP header `database: default` | OK | — |
| HTTP header `database: nosuchdb` | ERR 190 | — |

Controller: `GET /tables/default.baseballStats/schema` → 200; `GET /tables/baseballStats/schema`
with header `database: nosuchdb` → 404 `Schema not found for table: nosuchdb.baseballStats`.

Pinot's namespace is exactly two levels, `database.table`, default database `default`.

## 12. sqlglot re-render survives Pinot

`validate_query(sql, dialect, default_limit=5)` output was submitted to the broker on both
engines for 15 shapes: aggregation with GROUP BY/ORDER BY, a time BETWEEN filter,
`DATETIMECONVERT(...)`, `DATETRUNC(...)`, `DISTINCTCOUNTHLL`, `CASE WHEN`, a LIMIT-less
selection, an MSE join, `ToDateTime(...)`, `IN (...)`, `LIKE`, `REGEXP_LIKE`, `ARRAYLENGTH`,
`CAST(... AS DOUBLE)`. **Every re-rendered query executed on both engines**, under both the
generic (`""`) and `mysql` sqlglot dialects, with identical output between the two dialects.
The only rewrites were cosmetic: `count(*)` → `COUNT(*)`, `a` → `AS a`, `ToDateTime` →
`TODATETIME` (Pinot functions are case-insensitive). `SET k=v; SELECT ...` is rejected by
`validate_query` as two statements — options must travel in `queryOptions`, never in the SQL.

## 13. Response size

`LIMIT 100000` on baseballStats returned all 97,889 rows, 2,404,271 bytes, on both engines —
there is no default broker response cap. `maxQueryResponseSizeBytes=1000000` refused it with
errorCode 503 and `partialResult: true`. `numRowsResultSet` is present on every response.
