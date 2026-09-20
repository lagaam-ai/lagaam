# Pinot multi-server metadata measurements — 2026-09-21

Setting: the realtime profile (`examples/docker-compose.yml --profile
pinot-realtime`, STREAM quickstart, Pinot 1.5.1, controller `localhost:9001`,
four servers 7050–7053). Two throwaway tables, both copies of `u12plain`'s
config with the replica-group pin (`instanceAssignmentConfigMap`, `routing`)
removed, `realtime.segment.flush.threshold.rows = 100`, a 2-partition topic,
1,100 keyed rows fed:

- `u14multi`, replication 1: 10 sealed segments (3 on 7051, 7 on 7052) plus
  one consuming per partition.
- `u14rep`, replication 2: the same 10 sealed, every one on exactly two
  servers (7050/7051/7052/7053 hold 4/8/8/4 names).

## 1. What the shipped adapter does (main 08514d5)

```
SELECT pk FROM pinot.default.u14multi LIMIT 5 -> scanned_bytes=None row_estimate=None max_intermediate_rows=None confidence='low'
SELECT pk FROM pinot.default.u12plain LIMIT 5 -> scanned_bytes=1582 row_estimate=400 max_intermediate_rows=400 confidence='high'
```

Same query, same schema, same flush threshold. The one-server table is
quoted; the two-server table is not, so under any row or byte budget every
query on it is denied. That is the completeness guard (ADR 0009) doing what
it was built to do, and it means every real deployment — a cluster is a
cluster because it has more than one server — is denied wholesale.

## 2. The bulk endpoint is one server's view, and no parameter changes that

`GET /segments/{t}/metadata` on `u14multi`, three consecutive calls: 8 of
10 sealed each time, all eight from server 7052 (stable this time; the
2026-09-17 log saw it alternate). `?columns=pk`: 8. `?segments=<all 12
names>`: 8, the same eight — the filter is applied *after* one server is
chosen. On `u14rep` (replication 2) the bulk call returns **4 of 12**.

`?segments=` with names that all live on one server answers for all of
them: `[0__0]` → 1, `[0__0, 0__1]` → 2; mixed `[0__0, 1__6]` → 1.

## 3. Per-server asks are complete and stable

`GET /segments/{t}/servers` returns `[{"tableName": "u14multi_REALTIME",
"serverToSegmentsMap": {server: [names...]}}]` — one element per table
type, consuming names included. On the batch instance (`airlineStats`
OFFLINE, 31 segments, 1 server) the shape is the same.

For each server, `GET /segments/{t}/metadata?segments=<its names>&columns=pk`
returned exactly its names, three trials each, on both tables. The union
covered every sealed name from `/tables/{t}/size` every time. Entries have
the bulk endpoint's shape (`segmentName`, `totalDocs`, `crc`,
`startTimeMillis`, `columns[].indexSizeMap`, …); a consuming segment's entry
carries rule 5's markers (`totalDocs 0`, `crc -9223372036854775808`, no
`columns`). On `u14rep`, the two replicas' entries for a segment are
identical in crc, totalDocs and column index sizes for all 10 sealed
segments, so a name-keyed merge that keeps the first entry loses nothing.

## 4. Alternatives measured and not chosen

- `GET /segments/{t}/zkmetadata`: one call, complete (12 of 12), carries
  `segment.total.docs` and `segment.size.in.bytes` — but no per-column
  bytes, so a column projection would be charged whole segments.
- `GET /segments/{t}_REALTIME/{seg}/metadata?columns=pk`: complete and
  full-fidelity per segment (docs 100, bytes 7480, pk `indexSizeMap`
  `{dictionary: 708, forward_index: 808}`), but one call per segment, and
  it 500s on the untyped name ("Ideal state does not exist for table").

Per-server asks give the bulk endpoint's own fidelity in S calls, S = servers
holding a missing segment.
