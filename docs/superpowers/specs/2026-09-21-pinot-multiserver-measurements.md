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

## 5. Round 2 (Astra review): the answer is one server's, and small asks lose

Measured 2026-09-21 evening, same tables, 40 trials per ask unless stated.

`GET /segments/{t}/metadata` never returns a union. Every answer is exactly
one server's response — the bulk call on `u14multi` over 20 trials returned
the 7-segment server 19 times and the 3-segment server once; on `u14rep`
15 and 5. A filter naming one holder's names comes back whole because only
that holder has work to do for it, and the server with the most work answers
last. When an empty responder lands last, the answer is `{}`:

| ask | short answers | note |
|---|---|---|
| `u14multi` 7051, 3 names | 0/40 | |
| `u14multi` 7052, 7 names | 0/40 | |
| `u14rep` 7050/7051/7052 (3, 7, 7 names) | 0/40 each | |
| `u14rep` 7053, 3 names | 1/40 (`{}`) | an immediate identical retry returned all 3 |
| single-name ask, `u14multi` | 6/30 empty | |
| single-name ask, `u14rep` | 4/30 empty | |

Pinot 1.5.1's `TableMetadataReader.fetchAndAggregateMetadata` reads as a
per-property union over `_httpResponses.values()`, which does not match
what this cluster returns; the mechanism is not established here, only the
behaviour. The four servers share one host with distinct admin ports
(7500–7503) — a production cluster with one host per server is not
measured.

Consequences for the adapter: a short answer is a random event, not a
verdict, and repeating the identical ask is an independent trial (1/1 fixed
above). Asks are made as large as the URL cap allows, so only a server's
last batch is small. Retrying a short answer twice takes a 20% empty rate
(the worst measured, single names) to under 1%; what is still short after
that stays `low`. Controller log shows 0 "falling back to legacy" lines
across all of this, so the per-segment fallback path was never taken.

## 6. Round 3: the rule implemented, and what it cost

Measured 2026-09-21, same cluster, after the retry landed. Forty live
`estimate_cost` trials per table, before and after:

| table | `low` before | `low` after | the `high` answer, both ways |
|---|---|---|---|
| `u14multi` | 1/40 | 0/40 | `scanned_bytes=4548 row_estimate=300` |
| `u14rep` | 2/40 | 0/40 | `scanned_bytes=6064 row_estimate=400` |
| `u12plain` | 0/40 | 0/40 | `scanned_bytes=1582 row_estimate=400` |

The quoted numbers are identical in every `high` answer either way: the rule
changes how often a quote arrives, never what it says.

**A short answer is always a whole `{}`, never a partial.** Probing
`u14rep`'s per-server asks directly, 80 raw asks (20 trials x 4 servers):
10 came back short, and all 10 were empty — including the 8-name ask on
7051, twice. So §5's "an empty responder lands last" is the whole of the
effect; no ask has ever returned some of its names.

**The rate is higher than §5 measured.** §5 saw `u14rep`'s per-server asks
short 1/40 on 7053 and 0/40 on the other three. This round saw 10/80 across
the same four — about 12%, against §5's ~0.6%. Same cluster, same tables,
same day. Whatever selects the answering server is not stable between
sessions, so no rate here should be read as a constant; what is stable is
that an identical retry is an independent trial.

The 20x loop of the six u14 integration tests: 0 failures. With only the
engine's retry loop disabled (the constant left in place, so the tests still
import), 1 of 10 runs failed — `test_realtime_the_facts_cover_every_sealed
_segment[u14rep]`, which is the flake round 3 exists to remove.

**The integration oracle had the same bug as the adapter.** Two helpers in
`tests/integration/test_pinot_engine.py` (`_per_server_pk_bytes`,
`_sealed_segment_docs_per_server`) made their own un-retried per-server asks
and then indexed the result, so a short answer failed the test for the
adapter's correct behaviour. Both now retry on the same bound the adapter
does. A test oracle that reads the cluster the adapter's way has to read it
as carefully.
