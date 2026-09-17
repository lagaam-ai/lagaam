# Pinot realtime and join-key evidence Implementation Plan (U12)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A Pinot table with a REALTIME half is quoted at `confidence="high"` — its consuming segments charged unconditionally at the stream's flush threshold in rows and at the worst sealed bytes-per-doc ratio in bytes — and a join whose key the catalog can prove (an upsert table's primary key, or a single-segment table's unique column) is charged `right + left + right` instead of the product, so the two shapes Pinot exists for stop being denied while every number stays an upper bound.

**Architecture:** Four pure modules grow and one composes. `metadata.py` gains the realtime facts (`consuming_count`, `flush_rows`, the consuming-entry drop in `segment_facts`, `metadata_is_complete`) and the key evidence (`upsert_keys`, `single_segment_unique_columns`), and `TableFacts` carries `consuming`, `flush_rows`, `complete` and `unique_keys`. `response.py` reads `numConsumingSegmentsQueried` beside the pruning counters it already reads, from the same EXPLAIN response. `quote.py` adds the completeness precondition, the unconditional consuming row charge, the ceiling-divided bytes projection, and loses the blanket REALTIME → low guard. `plan.py` resolves a join's equality operands to column-name pairs through each side's `LogicalProject` `fields`, and applies the unique-key join rule. `engine.py` fetches two more controller documents per table — `/tables/{t}/externalview` always, `/schemas/{schemaName}` only when the config shows an `upsertConfig` — and passes `unique_keys` into the plan walk. Core gains nothing.

**Tech Stack:** Python 3.12+, uv, httpx, sqlglot, pydantic, pytest (+ `pytest-asyncio` auto mode), mypy strict over `lagaam.core` and `lagaam.adapters.pinot`. Live engines for the integration suite: Apache Pinot 1.5.1 batch quickstart on controller `http://localhost:9000` / broker `http://localhost:8000` (the OFFLINE suite, unchanged), and a new STREAM instance on controller `http://localhost:9001` / broker `http://localhost:8001` with Kafka 3.9.0 KRaft beside it.

**Spec:** docs/superpowers/specs/2026-09-17-pinot-realtime-and-key-evidence-design.md

## Global Constraints

- Work only in the worktree `/Users/muditkapoor/Documents/code/lagaam-pinot` on branch `feat/pinot-realtime-keys` (off the U11 merge at 94891a8); never push, never `git stash`.
- Conventional atomic commits (one logical change plus its tests), each message ending with the trailer line `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Inline comments are one line maximum, and only for a constraint the code cannot show; never restate what the line does.
- Type hints everywhere; `uv run mypy` clean (`lagaam.adapters.pinot` is in the mypy packages, strict mode).
- `uv run pytest -q` green after every task; `uv run pytest -q -m integration` green after every task that touches the integration suite.
- Core never imports httpx or a Pinot shape, and core gains NOTHING in this plan — no new function, field, error code or hint. Every number here is an adapter number feeding the existing `CostEstimate` (spec decision 9).
- Fail-safe rule: an unmeasurable number is `None` and the confidence is `"low"`, never a guess; where a bound exists it is charged rather than skipped. The consuming charge is the only place in this unit where a number *grows* a quotation, and the join rule is the only place one shrinks — and it shrinks only on positive catalog evidence, never on a SQL shape.
- The multi-stage `EXPLAIN`'s `rowcount` and `cumulative cost` attributes are never read — measured constant `100.0` per table scan, which priced a 954-million-row cross join at 10,000.
- The sqlglot dialect is `PINOT_DIALECT_CARD.sqlglot_dialect`; no module in this unit parses SQL with any other.
- Names sent to the broker are two-part, via `names.two_part_sql` — a three-part name is an HTTP 500 on the broker's own parser.
- `numConsumingSegmentsProcessed` is never read: measured 0 while the consuming segment contributed 50 docs to `count(*)` (log §1.7, rule 10).

---

## Measured facts this plan is built on

Every number below is read from a captured JSON file in
`/Users/muditkapoor/Documents/code/lagaam/.superpowers/spike-u12/` and cited to a
section or rule of `docs/superpowers/specs/2026-09-17-pinot-realtime-measurements.md`.
Nothing here is assumed, and no task asserts a literal that is not in this list.

**The 600-row moment on the realtime `airlineStats`** (`01-externalview.json`,
`01-segmetadata-nocolumns.json`, `01-tablesize.json`, `01-tablemetadata.json`),
which every unit test of the consuming charge is built on:

| fact | value | source |
|---|---|---|
| segments in externalview | 7 — 6 `ONLINE`, 1 `CONSUMING` | log §1.2, §1.3 |
| the consuming one | `airlineStats__0__6__20260917T1214Z` | log §1.2 |
| sealed `totalDocs` | exactly **100** each, 600 total | log §1.3, rule 2 |
| consuming `totalDocs` | **0**, `columns` key absent, `crc` `-9223372036854775808` | log §1.3, rule 5 |
| sealed `reportedSizeInBytes` | 85012, 85495, 85893, 86699, 86800, 87136 — **517,035** total | log §1.4 |
| consuming `reportedSizeInBytes` | **−1**, and not added into the table total | log §1.4 |
| `realtimeSegments.missingSegments` | **1** = the consuming count | log §1.4, rule 6 |
| `/tables/{t}/metadata` `numRows` | 600 — a *live* count, never charged | log §1.5 |

**The flush threshold, and where it is written** (log §1.1, rule 3). Values are
JSON **strings** in every config measured:

| fixture | location | key | value |
|---|---|---|---|
| `01-bundled-airlineStats-realtime-config.json` | `ingestionConfig.streamIngestionConfig.streamConfigMaps[0]` | `realtime.segment.flush.threshold.size` (deprecated spelling) | `"50000"` |
| `01-posted-airlineStats-realtime-config.json` | same | `realtime.segment.flush.threshold.rows` | `"100"` |
| `02-upsert-config-readback.json` | same | `realtime.segment.flush.threshold.rows` | `"200"` |

`tableIndexConfig.streamConfigs` — the legacy location — is **absent on every
config measured**; it is read only so an older cluster is not silently unbounded.
Pinot rejects a config setting more than one row bound (HTTP 400, "Only 1 of
flush threshold … can be set"), so the precedence list picks a location, never a
winner.

**The pruning and consuming counters** (`01-explain-and-counters-600rows.json`,
log §1.6, rules 8 and 9). All three rows are `EXPLAIN PLAN FOR`,
`numDocsScanned: 0`, on the 7-segment table:

| query | `numSegmentsQueried` | ByServer | ByValue | ByLimit | ByBroker | **`numConsumingSegmentsQueried`** |
|---|---|---|---|---|---|---|
| `SELECT Carrier … LIMIT 10` | 7 | 6 | 0 | 5 | 0 | **1** |
| `… WHERE DaysSinceEpoch < 16072 LIMIT 10` | **4** | 1 | 0 | 0 | **3** | **1** |
| `… WHERE DaysSinceEpoch > 99999 LIMIT 10` | **1** | 1 | 0 | 0 | **6** | **1** |

`numSegmentsQueried` **includes** the consuming segment, so
`sealed_surviving = numSegmentsQueried − numConsumingSegmentsQueried`. The
consuming counter stayed **1** under filters excluding every possible value while
the broker pruned 6 of 7 — a time predicate can never remove a consuming
segment's cost, so its charge is added outside the k-largest logic. On the
existing OFFLINE fixtures (`explain-v1-timefilter.json` and its siblings)
`numConsumingSegmentsQueried` is **0**, so those tests are untouched.

**The bytes-per-doc ratios**, recomputed here from the captures rather than
transcribed (log §1.4 ÷ §1.3 for whole-segment, `01-segmetadata-twocols.json`
`indexSizeMap` sums ÷ §1.3 for the projection — rule 20):

| segment | docs | bytes | whole ratio | `Carrier`+`DaysSinceEpoch` bytes | column ratio |
|---|---|---|---|---|---|
| `…__0__0__…` | 100 | 85495 | 854.95 | 92 | 0.92 |
| `…__0__1__…` | 100 | 86699 | 866.99 | 96 | 0.96 |
| `…__0__2__…` | 100 | 85893 | 858.93 | 108 | 1.08 |
| `…__0__3__…` | 100 | 86800 | 868.00 | 77 | 0.77 |
| `…__0__4__…` | 100 | **87136** | **871.36** | **114** | **1.14** |
| `…__0__5__…` | 100 | 85012 | 850.12 | 61 | 0.61 |

Max whole-segment ratio **871.36** (a 2.5% spread), max column ratio **1.14**.
At the measured threshold of 100 rows: `ceil(100 × 87136 / 100)` = **87,136**
whole-segment bytes and `ceil(100 × 114 / 100)` = **114** column bytes per
consuming segment. The two-column sum over the six sealed segments is **548**.
The ratio is taken **per segment and then maximised**, never averaged, and the
product is ceiling-divided in integer arithmetic —
`(flush_rows × numerator + docs − 1) // docs` — so no float rounding can shave a
byte off the bound.

**Upsert evidence** (log §2, §2.1, rules 11–13):

| fact | `u12upsert` | `u12plain` |
|---|---|---|
| `schema.primaryKeyColumns` (`02-upsert-schema-readback.json`) | `["pk"]` | absent |
| `upsertConfig.mode` (`02-upsert-config-readback.json`) | `"FULL"` | absent |
| `upsertPartitionToServerPrimaryKeyCountMap` | `{"0":{"Server_…7051":50},"1":{"Server_…7052":50}}` | `{}` |
| `count(*)` | **100** | **600** |
| self-join `a JOIN b ON a.pk = b.pk` | **100** | **3,600** |

`segmentsConfig.schemaName` is **absent on all four configs captured**, so the
fallback path — the controller's own listing spelling of the table — is the one
that normally runs. A config read back carries the **suffixed** `tableName`
(`"u12upsert_REALTIME"`), which would 404 as a schema path. Sealed `totalDocs`
on `u12upsert__0__0__…` is **200** against a table-wide `count(*)` of 100: an
over-count, which is what an upper bound is made of, and it is kept unchanged.

**Metadata truncation** (log §4, rule 19). `02-upsert-segmetadata.json` holds two
entries (`u12upsert__0__0__…` with `totalDocs: 200` and `u12upsert__0__1__…`,
consuming) against **four** named in `02-upsert-size.json` — two sealed at 7422
and 7442, two consuming at −1 — and `numSegments: 4` in the table metadata. On a
2-server table the metadata endpoint alternates which server's half it returns
between identical calls. The batch instance is single-server, which is why the
2026-09-11 spike never saw it.

**Cardinality as uniqueness evidence** (log §3, rules 14–15).
`03-baseballStats-segmetadata-allcols.json`: **one** segment, **97,889** docs, 25
columns; `cardinality` is exactly `count(DISTINCT col)` — `playerID` 18,107,
`teamID` 149, `league` 7, `homeRuns` 67, all verified against the engine. **No
column reaches its `totalDocs`**: the best ratio is `playerID` at 0.185, and 0 of
2,604 per-column entries on `airlineStats` qualify. Every per-column entry
carries a `fieldSpec` with `notNull` (false on all of them here) — which is why
source (b) is gated and finds nothing on this data. Summing per-segment
cardinality across segments is never done: 432 over 31 segments against a true
`distinctcount(Carrier)` of 14.

**Plan shape** (log §5.1, rules 16–17):

- `04-join-plain-plan.json` — left project `fields: ["Carrier"]`, right project
  `fields: ["teamID"]`, join condition `EQUALS` on `{"input":0}`/`{"input":1}`.
- `04-join-twokeys-plan.json` — the discriminating case: left
  `fields: ["Carrier","Origin"]` (2 fields), right `fields: ["league","teamID"]`
  (alphabetised, *not* ON-clause order), condition `AND(=($0,$3), =($1,$2))`.
  Right `teamID` at right-index 1 resolves to global **3** = 2 left + 1.
- `04-join-expr-plan.json` — left `fields: ["Carrier","$f85"]` whose second
  `exprs` entry carries `{"op": {"name": "UPPER", …}}` instead of a bare
  `input`; the condition is `=($1,$2)`, pointing at `$f85`.
- `04-join-left-plan.json` — `joinType: "left"`, otherwise structurally
  identical to the plain join.
- A `PinotLogicalTableScan` carries **no `fields`**; the names come from the
  `LogicalProject` immediately above it, reached through the pass-through
  `PinotLogicalExchange`.
- Semi-joins and aggregated join inputs cannot be serialised as JSON at all
  (errorCode 450, `PIPELINE_BREAKER`, log §5.2) — `max_intermediate_rows`
  receives no parseable plan and returns `None`, which it already does.

**Memory** (log §8, `05-docker-stats.txt`): `u12-pinot` 1.16 GiB, `u12-kafka`
385.5 MiB, `lagaam-pinot` 1.856 GiB, `lagaam-trino` 3.024 GiB — ≈6.4 GiB against
Docker's 7.653 GiB ceiling. The `-Xmx2G` cap is not optional, and the realtime
profile must not assume the Trino profile is down.

---

## File Structure

**Create**

| path | responsibility |
|---|---|
| `examples/pinot-realtime/bootstrap.sh` | Idempotent bootstrap for the `pinot-realtime` profile: create the two Kafka topics, POST the three schemas and table configs, stream the sample rows. A table that already exists is left alone. |
| `examples/pinot-realtime/airlineStats-schema.json` | The bundled realtime schema, extracted from the image, POSTed unchanged. |
| `examples/pinot-realtime/airlineStats-table.json` | The realtime `airlineStats` config with `.threshold.rows: "100"` and the compose broker list. |
| `examples/pinot-realtime/u12upsert-schema.json` | `primaryKeyColumns: ["pk"]`. |
| `examples/pinot-realtime/u12upsert-table.json` | `upsertConfig: {"mode": "FULL"}`, `routing.instanceSelectorType: strictReplicaGroup`, topic `u12-upsert`. |
| `examples/pinot-realtime/u12plain-schema.json` | The contrast schema: no `primaryKeyColumns`. |
| `examples/pinot-realtime/u12plain-table.json` | The contrast config: same topic, no `upsertConfig`. |
| `server/tests/adapters/pinot/fixtures/externalview-airlineStats-realtime.json` | `GET /tables/airlineStats/externalview` on :9001 — 6 `ONLINE` + 1 `CONSUMING`. |
| `server/tests/adapters/pinot/fixtures/seg-metadata-airlineStats-realtime.json` | `GET /segments/airlineStats/metadata` on :9001 — 7 entries, 6 sealed at 100 docs, 1 consuming. |
| `server/tests/adapters/pinot/fixtures/seg-metadata-airlineStats-realtime-columns.json` | The same with `?columns=Carrier&columns=DaysSinceEpoch`. |
| `server/tests/adapters/pinot/fixtures/size-airlineStats-realtime.json` | `GET /tables/airlineStats/size` on :9001 — `missingSegments: 1`, the consuming one at −1. |
| `server/tests/adapters/pinot/fixtures/tableconfig-airlineStats-realtime.json` | The posted realtime config: `.threshold.rows: "100"`. |
| `server/tests/adapters/pinot/fixtures/tableconfig-airlineStats-realtime-bundled.json` | The bundled config: the deprecated `.threshold.size: "50000"`. |
| `server/tests/adapters/pinot/fixtures/tableconfig-u12upsert.json` | `upsertConfig`, `.threshold.rows: "200"`, suffixed `tableName`. |
| `server/tests/adapters/pinot/fixtures/tableconfig-u12plain.json` | The byte-identical twin with no `upsertConfig`. |
| `server/tests/adapters/pinot/fixtures/schema-u12upsert.json` | `GET /schemas/u12upsert` — `primaryKeyColumns: ["pk"]`. |
| `server/tests/adapters/pinot/fixtures/schema-u12plain.json` | `GET /schemas/u12plain` — no `primaryKeyColumns`. |
| `server/tests/adapters/pinot/fixtures/seg-metadata-u12upsert.json` | The truncated two-of-four response: one sealed at 200 docs, one consuming. |
| `server/tests/adapters/pinot/fixtures/size-u12upsert.json` | All four segments, `missingSegments: 2`. |
| `server/tests/adapters/pinot/fixtures/metadata-u12upsert.json` | `numSegments: 4`, the non-empty `upsertPartitionToServerPrimaryKeyCountMap`. |
| `server/tests/adapters/pinot/fixtures/metadata-u12plain.json` | The same shape with an empty PK map. |
| `server/tests/adapters/pinot/fixtures/seg-metadata-baseballStats-allcols.json` | One segment, 97,889 docs, 25 columns with `cardinality` and `fieldSpec.notNull` — the single-segment uniqueness case that finds nothing. |
| `server/tests/adapters/pinot/fixtures/explain-v1-realtime-nofilter.json` | Single-stage EXPLAIN on :8001 — queried 7, consuming 1. |
| `server/tests/adapters/pinot/fixtures/explain-v1-realtime-futuretime.json` | The same under `DaysSinceEpoch > 99999` — queried 1, ByBroker 6, consuming 1. |
| `server/tests/adapters/pinot/fixtures/explain-mse-join-plain.json` | The baseline MSE join plan: one field a side, `Carrier`/`teamID`. |
| `server/tests/adapters/pinot/fixtures/explain-mse-join-twokeys.json` | The two-key MSE plan: the global-index arithmetic. |
| `server/tests/adapters/pinot/fixtures/explain-mse-join-expr.json` | The `$f85` expression plan. |
| `server/tests/adapters/pinot/fixtures/explain-mse-join-left.json` | `joinType: "left"`, resolved the same way. |
| `docs/adr/0009-consuming-segments-and-proven-join-keys.md` | The new ADR: decisions 1, 2 and 5, with the residual risk of the one projection. |

**Modify**

| path | change |
|---|---|
| `server/src/lagaam/adapters/pinot/metadata.py` | Add `consuming_count`, `flush_rows`, `metadata_is_complete`, `upsert_keys`, `single_segment_unique_columns`; drop the consuming entry from `segment_facts`; grow `TableFacts` with `consuming`, `flush_rows`, `complete`, `unique_keys`. Still PURE, still never raises. |
| `server/src/lagaam/adapters/pinot/response.py` | Add `consuming_segments_queried(explain_json)`, read from the same EXPLAIN body `surviving_segments` already parses. `surviving_segments` itself is unchanged. |
| `server/src/lagaam/adapters/pinot/quote.py` | Add the completeness precondition, the unconditional consuming row charge, the ceiling-divided bytes projection; remove the blanket REALTIME → low guard. |
| `server/src/lagaam/adapters/pinot/plan.py` | Add operand resolution (`join_key_pairs`) and the `unique_keys` parameter on `max_intermediate_rows`, with the join arithmetic of spec decision 5. |
| `server/src/lagaam/adapters/pinot/engine.py` | Fetch `/tables/{t}/externalview` per table and `/schemas/{schemaName}` only for an upsert config; build `unique_keys` and pass it to the plan walk; `path_part` on every new path. |
| `server/tests/adapters/pinot/test_pinot_metadata.py` | Tests for the five new parsers and the consuming drop. |
| `server/tests/adapters/pinot/test_pinot_response.py` | Tests for `consuming_segments_queried`. |
| `server/tests/adapters/pinot/test_pinot_quote.py` | Tests for the consuming charge, the ratio, the completeness gate, hybrid, and the removed REALTIME guard. |
| `server/tests/adapters/pinot/test_pinot_plan.py` | Tests for operand resolution and the unique-key join rule. |
| `server/tests/adapters/pinot/test_pinot_engine.py` | Unit tests for the composed realtime quotation over `MockTransport`. |
| `server/tests/integration/conftest.py` | Add the `pinot_realtime_ready` fixture beside `pinot_ready`. |
| `server/tests/integration/test_pinot_engine.py` | The realtime and key-evidence assertions against :9001/:8001. |
| `server/tests/integration/test_e2e_mcp.py` | The three demo assertions of spec decision 7. |
| `examples/docker-compose.yml` | The `pinot-realtime` profile: Kafka 3.9.0 KRaft plus a second Pinot on 9001/8001. |
| `docs/adr/0008-pinot-quotation-is-adapter-synthesised.md` | Correct the `ByBroker`/`Invalid` justification and the "until measured" consequence. |
| `docs/adr/README.md` | The 0009 index row. |
| `docs/superpowers/specs/2026-09-17-pinot-realtime-and-key-evidence-design.md` | Reconcile with anything that shipped differently. |
| `README.md` | The Pinot status sentence, which still says the quotation is not built. |

---

## Task 1: Realtime facts in `metadata.py` — the consuming count, the flush threshold, completeness

**Files:** `server/src/lagaam/adapters/pinot/metadata.py`,
`server/tests/adapters/pinot/test_pinot_metadata.py`, six new fixtures.

**Interfaces:**
- Consumes: parsed JSON from `GET /tables/{t}/externalview`, `GET /tables/{t}/size`,
  `GET /tables/{t}`, `GET /segments/{t}/metadata`.
- Produces:
  - `def consuming_count(externalview_json: Any, size_json: Any) -> int` — the larger of
    the externalview CONSUMING count and `realtimeSegments.missingSegments`; 0 when
    neither is readable (spec decision 1).
  - `def flush_rows(config_json: Any) -> int | None` — the row bound on one consuming
    segment, from the `REALTIME` half's stream config; `None` when it is not a positive int.
  - `def metadata_is_complete(seg_metadata_json: Any, size_json: Any) -> bool` — every
    sealed segment the size report names appears in the metadata response (spec decision 4).
  - `TableFacts` grows `consuming: int = 0`, `flush_rows: int | None = None`,
    `complete: bool = True`, `unique_keys: frozenset[frozenset[str]] = frozenset()` —
    all defaulted, so every existing construction in tests and `quote.py` still type-checks.
  - `def table_facts(table, config_json, seg_metadata_json, size_json, columns=frozenset(), *, externalview_json: Any = None, schema_json: Any = None, table_metadata_json: Any = None) -> TableFacts`
    — the three new documents are keyword arguments defaulting to `None`, so every existing
    call site keeps working and an OFFLINE table that fetches none of them is unchanged.
  - `segment_facts` drops the consuming entry from the sealed set (rule 5's four-way
    conjunction), so its `None` bytes stop poisoning `surviving_bytes`.

Two facts about the shipped code that this task does **not** change, verified by reading it:

- `_segment_sizes` already merges **both** `offlineSegments` and `realtimeSegments`
  halves of the size report, so a **sealed realtime segment's bytes already reach
  `SegmentFact.total_bytes` today** — nothing in this task or decision 3 needs to add
  realtime size reading. The hybrid arithmetic of decision 3 is therefore already
  satisfied by the existing `_segment_sizes` plus the k-largest over the union; Task 4
  adds the unit test that pins it.
- `_positive_int(-1)` is `None`, which is exactly why the consuming entry has to be
  *dropped* rather than repaired: a kept entry with `total_bytes=None` makes
  `surviving_bytes` return `None` for the whole table.

- [ ] **Step 1 — capture the fixtures.** The spike's captures are the source; copy them
  under the fixture naming the existing files use. From the worktree root:
  ```bash
  SPIKE=/Users/muditkapoor/Documents/code/lagaam/.superpowers/spike-u12
  FIX=server/tests/adapters/pinot/fixtures
  cp "$SPIKE/01-externalview.json"                         "$FIX/externalview-airlineStats-realtime.json"
  cp "$SPIKE/01-segmetadata-nocolumns.json"                "$FIX/seg-metadata-airlineStats-realtime.json"
  cp "$SPIKE/01-segmetadata-twocols.json"                  "$FIX/seg-metadata-airlineStats-realtime-columns.json"
  cp "$SPIKE/01-tablesize.json"                            "$FIX/size-airlineStats-realtime.json"
  cp "$SPIKE/01-posted-airlineStats-realtime-config.json"  "$FIX/tableconfig-airlineStats-realtime.json"
  cp "$SPIKE/01-bundled-airlineStats-realtime-config.json" "$FIX/tableconfig-airlineStats-realtime-bundled.json"
  cp "$SPIKE/02-upsert-config-readback.json"               "$FIX/tableconfig-u12upsert.json"
  cp "$SPIKE/02-upsert-segmetadata.json"                   "$FIX/seg-metadata-u12upsert.json"
  cp "$SPIKE/02-upsert-size.json"                          "$FIX/size-u12upsert.json"
  ```
  Confirm, before writing a line of test:
  ```bash
  python3 - <<'PY'
  import json, pathlib
  fix = pathlib.Path("server/tests/adapters/pinot/fixtures")
  ev = json.loads((fix / "externalview-airlineStats-realtime.json").read_text())
  states = [s for m in ev["REALTIME"].values() for s in m.values()]
  print("externalview", len(ev["REALTIME"]), states.count("ONLINE"), states.count("CONSUMING"))
  size = json.loads((fix / "size-airlineStats-realtime.json").read_text())
  print("missingSegments", size["realtimeSegments"]["missingSegments"])
  print("reported", sorted(b["reportedSizeInBytes"] for b in size["realtimeSegments"]["segments"].values()))
  meta = json.loads((fix / "seg-metadata-airlineStats-realtime.json").read_text())
  print("docs", sorted(b["totalDocs"] for b in meta.values()))
  print("crcs", sorted({b["crc"] for b in meta.values() if b["totalDocs"] == 0}))
  print("columns keys", sorted({("columns" in b) for b in meta.values()}))
  cfg = json.loads((fix / "tableconfig-airlineStats-realtime.json").read_text())
  m = cfg["REALTIME"]["ingestionConfig"]["streamIngestionConfig"]["streamConfigMaps"][0]
  print("posted threshold", m.get("realtime.segment.flush.threshold.rows"))
  bundled = json.loads((fix / "tableconfig-airlineStats-realtime-bundled.json").read_text())
  bm = bundled["REALTIME"]["ingestionConfig"]["streamIngestionConfig"]["streamConfigMaps"][0]
  print("bundled threshold", bm.get("realtime.segment.flush.threshold.size"),
        bm.get("realtime.segment.flush.threshold.rows"))
  up = json.loads((fix / "tableconfig-u12upsert.json").read_text())
  um = up["REALTIME"]["ingestionConfig"]["streamIngestionConfig"]["streamConfigMaps"][0]
  print("upsert threshold", um.get("realtime.segment.flush.threshold.rows"))
  usize = json.loads((fix / "size-u12upsert.json").read_text())
  print("upsert size names", sorted(usize["realtimeSegments"]["segments"]),
        usize["realtimeSegments"]["missingSegments"])
  print("upsert meta names", sorted(json.loads((fix / "seg-metadata-u12upsert.json").read_text())))
  PY
  ```
  Expected exactly (log §1.2, §1.3, §1.4, §1.1, §4):
  ```
  externalview 7 6 1
  missingSegments 1
  reported [-1, 85012, 85495, 85893, 86699, 86800, 87136]
  docs [0, 100, 100, 100, 100, 100, 100]
  crcs [-9223372036854775808]
  columns keys [False]
  posted threshold 100
  bundled threshold 50000 None
  upsert threshold 200
  upsert size names ['u12upsert__0__0__20260917T1217Z', 'u12upsert__0__1__20260917T1217Z', 'u12upsert__1__0__20260917T1217Z', 'u12upsert__1__1__20260917T1217Z'] 2
  upsert meta names ['u12upsert__0__0__20260917T1217Z', 'u12upsert__0__1__20260917T1217Z']
  ```
  `columns keys [False]` is the no-`?columns=` capture: **no** segment carries a
  `columns` key there, so rule 5's "no `columns` key" test must be run against the
  `-columns.json` capture, where the six sealed segments do carry one and the
  consuming one still does not. Confirm that too:
  ```bash
  python3 -c "
  import json, pathlib
  m = json.loads((pathlib.Path('server/tests/adapters/pinot/fixtures') / 'seg-metadata-airlineStats-realtime-columns.json').read_text())
  print(sorted((k[-14:], b['totalDocs'], 'columns' in b) for k, b in m.items()))
  print(sum(sum(c['indexSizeMap'].values()) for b in m.values() for c in b.get('columns', [])))"
  ```
  Expect seven entries, six with `columns` present at 100 docs and one without at 0,
  and the two-column byte total **548** (log §1.3 with rule 20).

- [ ] **Step 2 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_metadata.py`:
  ```python
  def test_consuming_count_reads_externalview_and_the_size_report_together() -> None:
      """6 ONLINE + 1 CONSUMING in externalview, missingSegments 1 in size."""
      assert (
          consuming_count(
              load("externalview-airlineStats-realtime.json"),
              load("size-airlineStats-realtime.json"),
          )
          == 1
      )


  def test_consuming_count_reads_a_two_partition_size_report() -> None:
      assert consuming_count(None, load("size-u12upsert.json")) == 2


  def test_a_disagreement_between_the_two_charges_the_larger() -> None:
      """Neither document is authoritative, so the larger cannot under-charge."""
      externalview = load("externalview-airlineStats-realtime.json")
      size = load("size-airlineStats-realtime.json")
      size["realtimeSegments"]["missingSegments"] = 4
      assert consuming_count(externalview, size) == 4
      size["realtimeSegments"]["missingSegments"] = 0
      assert consuming_count(externalview, size) == 1


  @pytest.mark.parametrize("body", [None, {}, [], "junk", {"REALTIME": "nope"}])
  def test_consuming_count_is_zero_when_neither_document_can_be_read(
      body: Any,
  ) -> None:
      assert consuming_count(body, body) == 0


  def test_flush_rows_accepts_the_deprecated_size_spelling() -> None:
      """The bundled quickstart config is the one a fixture is likeliest to meet."""
      assert flush_rows(load("tableconfig-airlineStats-realtime-bundled.json")) == 50000


  def test_flush_rows_reads_the_rows_spelling_as_a_string() -> None:
      assert flush_rows(load("tableconfig-airlineStats-realtime.json")) == 100
      assert flush_rows(load("tableconfig-u12upsert.json")) == 200


  def test_flush_rows_falls_back_to_the_legacy_stream_config_location() -> None:
      """Absent on every config measured; read so an older cluster is not unbounded."""
      assert (
          flush_rows(
              {
                  "REALTIME": {
                      "tableIndexConfig": {
                          "streamConfigs": {
                              "realtime.segment.flush.threshold.rows": "250"
                          }
                      }
                  }
              }
          )
          == 250
      )


  @pytest.mark.parametrize(
      "value", ["", "10.5", "500M", "-100", "0", "abc", None, True, 4.0]
  )
  def test_a_threshold_that_is_not_a_positive_int_is_no_bound(value: Any) -> None:
      assert (
          flush_rows(
              {
                  "REALTIME": {
                      "ingestionConfig": {
                          "streamIngestionConfig": {
                              "streamConfigMaps": [
                                  {"realtime.segment.flush.threshold.rows": value}
                              ]
                          }
                      }
                  }
              }
          )
          is None
      )


  @pytest.mark.parametrize("body", [None, {}, [], "junk", {"OFFLINE": {}}])
  def test_flush_rows_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
      assert flush_rows(body) is None


  def test_the_consuming_entry_is_not_a_sealed_fact() -> None:
      """totalDocs 0, no columns, Long.MIN_VALUE crc and -1 bytes, all four."""
      facts = segment_facts(
          load("seg-metadata-airlineStats-realtime-columns.json"),
          load("size-airlineStats-realtime.json"),
      )
      assert len(facts) == 6
      assert all(fact.docs == 100 for fact in facts)
      assert all(fact.total_bytes is not None for fact in facts)
      assert sum(fact.total_bytes or 0 for fact in facts) == 517035
      assert not any("__0__6__" in fact.name for fact in facts)


  def test_an_unreadable_sealed_segment_is_kept_and_still_poisons_the_sum() -> None:
      """Three of rule 5's four markers is not a consuming segment."""
      facts = segment_facts(
          {"seg0": {"segmentName": "seg0", "totalDocs": 0, "crc": 12345}},
          {"realtimeSegments": {"segments": {"seg0": {"reportedSizeInBytes": -1}}}},
      )
      assert len(facts) == 1
      assert facts[0].total_bytes is None


  def test_metadata_is_complete_when_every_sealed_segment_is_present() -> None:
      assert metadata_is_complete(
          load("seg-metadata-airlineStats-realtime.json"),
          load("size-airlineStats-realtime.json"),
      )


  def test_a_truncated_metadata_response_is_incomplete() -> None:
      """Measured: a 2-server table returns one server's half and alternates."""
      assert not metadata_is_complete(
          load("seg-metadata-u12upsert.json"), load("size-u12upsert.json")
      )


  def test_a_consuming_segment_missing_from_metadata_does_not_make_it_incomplete() -> (
      None
  ):
      """A -1 in the size report is consuming, and is exempt by definition."""
      size = load("size-u12upsert.json")
      size["realtimeSegments"]["segments"] = {
          name: body
          for name, body in size["realtimeSegments"]["segments"].items()
          if body["reportedSizeInBytes"] != -1 and name.startswith("u12upsert__0__")
      }
      assert metadata_is_complete(load("seg-metadata-u12upsert.json"), size)


  @pytest.mark.parametrize("body", [None, {}, [], "junk"])
  def test_completeness_is_true_when_the_size_report_names_nothing(body: Any) -> None:
      """No named sealed segment is nothing to be missing — the pre-U12 behaviour."""
      assert metadata_is_complete(body, body)


  def test_table_facts_carry_the_realtime_numbers() -> None:
      facts = table_facts(
          "airlineStats",
          load("tableconfig-airlineStats-realtime.json"),
          load("seg-metadata-airlineStats-realtime.json"),
          load("size-airlineStats-realtime.json"),
          externalview_json=load("externalview-airlineStats-realtime.json"),
      )
      assert facts.types == frozenset({"REALTIME"})
      assert len(facts.segments) == 6
      assert facts.consuming == 1
      assert facts.flush_rows == 100
      assert facts.complete is True
      assert facts.unique_keys == frozenset()


  def test_table_facts_without_the_new_documents_are_the_pre_u12_facts() -> None:
      """Every existing call site passes four positional arguments and no more."""
      facts = table_facts(
          "airlineStats",
          load("tableconfig-airlineStats.json"),
          load("seg-metadata-airlineStats-columns.json"),
          load("size-airlineStats.json"),
      )
      assert facts.consuming == 0
      assert facts.flush_rows is None
      assert facts.complete is True
      assert len(facts.segments) == 31
  ```
  Extend the module's import to
  `from lagaam.adapters.pinot.metadata import (consuming_count, flush_rows, metadata_is_complete, row_estimate, segment_facts, table_facts, table_names, table_schema, table_types, time_column)`.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_metadata.py`
  Expect `ImportError: cannot import name 'consuming_count' from 'lagaam.adapters.pinot.metadata'`.

- [ ] **Step 4 — implementation.** In `server/src/lagaam/adapters/pinot/metadata.py`, add the
  four new `TableFacts` fields after `columns`:
  ```python
      # How many CONSUMING segments this table has, and the row bound on one of
      # them. A consuming segment reports nothing (0 docs, -1 bytes) until it
      # seals, so it is charged from the config or the quote goes low.
      consuming: int = 0
      flush_rows: int | None = None
      # False when the metadata response did not cover every sealed segment the
      # size report names: a confident sum over half a table is the one failure
      # the gate exists to prevent.
      complete: bool = True
      # Column sets proved unique on this table, lowercase. Empty is "no
      # evidence", which charges a join the product exactly as before.
      unique_keys: frozenset[frozenset[str]] = frozenset()
  ```
  Replace the body of `segment_facts`'s loop so a consuming entry is dropped, and add the
  four new functions plus two private helpers at the end of the module:
  ```python
  # Long.MIN_VALUE: what a consuming segment reports where a CRC would be.
  _CONSUMING_CRC = -9223372036854775808

  # The flush threshold, most specific location first. `.size` is the deprecated
  # spelling of `.rows` and is what the bundled quickstart config actually uses.
  _FLUSH_KEYS = (
      "realtime.segment.flush.threshold.rows",
      "realtime.segment.flush.threshold.size",
  )


  def segment_facts(seg_metadata_json: Any, size_json: Any) -> list[SegmentFact]:
      """Per-segment docs, bytes, time range and per-column bytes.

      Bytes come from a different endpoint than docs, keyed by segment name, so
      a segment missing from the size report keeps its docs and loses its bytes.

      A consuming segment is dropped rather than kept as a zero: it reports 0
      docs and -1 bytes permanently, and one None bytes makes the whole table's
      byte sum unknown. Only all four markers together identify it — an
      unreadable sealed segment keeps its Nones and poisons the sum, because an
      unreadable segment is not a free one.
      """
      if not isinstance(seg_metadata_json, dict):
          return []
      sizes = _segment_sizes(size_json)
      reported = _reported_sizes(size_json)
      facts: list[SegmentFact] = []
      for key, body in seg_metadata_json.items():
          if not isinstance(body, dict):
              continue
          name = body.get("segmentName")
          if not isinstance(name, str) or not name:
              name = key if isinstance(key, str) else ""
          if not name:
              continue
          if _is_consuming(body, reported.get(name)):
              continue
          facts.append(
              SegmentFact(
                  name=name,
                  docs=_positive_int(body.get("totalDocs"), allow_zero=True),
                  total_bytes=sizes.get(name),
                  start_ms=_positive_int(body.get("startTimeMillis")),
                  end_ms=_positive_int(body.get("endTimeMillis")),
                  column_bytes=_column_bytes(body.get("columns")),
              )
          )
      return facts


  def consuming_count(externalview_json: Any, size_json: Any) -> int:
      """How many CONSUMING segments this table has, charged at the larger count.

      Externalview is the state of record but can lag, and missingSegments also
      counts a segment a server failed to report, so neither is authoritative
      and only the larger cannot under-charge. Neither readable is 0, which is
      the pre-U12 behaviour and is caught by the completeness check where it
      matters.
      """
      return max(_externalview_consuming(externalview_json), _missing_segments(size_json))


  def flush_rows(config_json: Any) -> int | None:
      """The row bound on one consuming segment, from the REALTIME stream config.

      Values are JSON strings in every config measured. Anything that is not a
      positive int — a float, a byte-suffixed size, a negative, zero, absent —
      is None, and a None here takes the whole quote low rather than guessing.
      """
      if not isinstance(config_json, dict):
          return None
      half = config_json.get("REALTIME")
      if not isinstance(half, dict):
          return None
      for source in _stream_config_maps(half):
          for key in _FLUSH_KEYS:
              bound = _positive_int_or_digits(source.get(key))
              if bound is not None:
                  return bound
      return None


  def metadata_is_complete(seg_metadata_json: Any, size_json: Any) -> bool:
      """Does the metadata response cover every sealed segment the size report names?

      Measured: on a 2-server table /segments/{t}/metadata returns one server's
      half and alternates which half between identical calls, so a sum over it
      is a confident sum over a subset — an under-quote at confidence="high",
      the one failure mode the gate exists to prevent. A segment reporting -1
      bytes is consuming and is expected to carry no useful entry, so it is
      exempt.
      """
      named = {
          name
          for name, size in _reported_sizes(size_json).items()
          if size is not None and size >= 0
      }
      if not named:
          return True
      if not isinstance(seg_metadata_json, dict):
          return False
      present: set[str] = set()
      for key, body in seg_metadata_json.items():
          if isinstance(key, str):
              present.add(key)
          if isinstance(body, dict) and isinstance(body.get("segmentName"), str):
              present.add(body["segmentName"])
      return named <= present


  def _is_consuming(body: dict[str, Any], reported: int | None) -> bool:
      """Rule 5's conjunction, all four markers together and never fewer."""
      return (
          body.get("totalDocs") == 0
          and "columns" not in body
          and body.get("crc") == _CONSUMING_CRC
          and reported == -1
      )


  def _stream_config_maps(half: dict[str, Any]) -> list[dict[str, Any]]:
      """The stream config maps of one table half, current location first."""
      maps: list[dict[str, Any]] = []
      ingestion = half.get("ingestionConfig")
      if isinstance(ingestion, dict):
          stream = ingestion.get("streamIngestionConfig")
          if isinstance(stream, dict):
              configs = stream.get("streamConfigMaps")
              if isinstance(configs, list):
                  maps.extend(entry for entry in configs if isinstance(entry, dict))
      index_config = half.get("tableIndexConfig")
      if isinstance(index_config, dict):
          legacy = index_config.get("streamConfigs")
          if isinstance(legacy, dict):
              maps.append(legacy)
      return maps


  def _externalview_consuming(externalview_json: Any) -> int:
      """Segments of the REALTIME map any server calls CONSUMING."""
      if not isinstance(externalview_json, dict):
          return 0
      half = externalview_json.get("REALTIME")
      if not isinstance(half, dict):
          return 0
      return sum(
          1
          for states in half.values()
          if isinstance(states, dict) and "CONSUMING" in states.values()
      )


  def _missing_segments(size_json: Any) -> int:
      """realtimeSegments.missingSegments, measured to equal the consuming count."""
      if not isinstance(size_json, dict):
          return 0
      half = size_json.get("realtimeSegments")
      if not isinstance(half, dict):
          return 0
      missing = _positive_int(half.get("missingSegments"), allow_zero=True)
      return missing or 0


  def _reported_sizes(size_json: Any) -> dict[str, int]:
      """Segment name to reportedSizeInBytes verbatim, -1 included.

      _segment_sizes drops the -1 as "unknown"; this keeps it, because -1 is
      how a consuming segment is recognised and how a sealed one is named.
      """
      if not isinstance(size_json, dict):
          return {}
      reported: dict[str, int] = {}
      for key in ("offlineSegments", "realtimeSegments"):
          half = size_json.get(key)
          if not isinstance(half, dict):
              continue
          segments = half.get("segments")
          if not isinstance(segments, dict):
              continue
          for name, body in segments.items():
              if not isinstance(name, str) or not isinstance(body, dict):
                  continue
              value = body.get("reportedSizeInBytes")
              if isinstance(value, bool) or not isinstance(value, int):
                  continue
              reported[name] = value
      return reported


  def _positive_int_or_digits(value: Any) -> int | None:
      """A positive int, or a string of digits meaning one. Nothing else."""
      if isinstance(value, str):
          return int(value) if value.isdigit() and int(value) > 0 else None
      return _positive_int(value)
  ```
  And grow `table_facts`:
  ```python
  def table_facts(
      table: str,
      config_json: Any,
      seg_metadata_json: Any,
      size_json: Any,
      columns: frozenset[str] = frozenset(),
      *,
      externalview_json: Any = None,
      schema_json: Any = None,
      table_metadata_json: Any = None,
  ) -> TableFacts:
      """One table's type, time column, segments and realtime facts.

      The three keyword documents are the U12 additions and default to None, so
      an OFFLINE caller that fetches none of them gets exactly the pre-U12
      facts: no consuming segments, no threshold, complete, no key evidence.
      """
      return TableFacts(
          table=table,
          types=table_types(config_json),
          time_column=time_column(config_json),
          segments=tuple(segment_facts(seg_metadata_json, size_json)),
          columns=columns,
          consuming=consuming_count(externalview_json, size_json),
          flush_rows=flush_rows(config_json),
          complete=metadata_is_complete(seg_metadata_json, size_json),
          # Task 2 replaces this literal with the two key-evidence sources; the
          # keyword documents are already threaded so that task touches one line.
          unique_keys=frozenset(),
      )
  ```
  `schema_json` and `table_metadata_json` are accepted and unread in this task: Task 2 is
  what reads them, and threading them now is what keeps that task to a one-line change
  here. mypy strict accepts an unused parameter; ruff's unused-argument rule does not
  apply to a keyword the signature exists to carry.

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`
  The existing `test_pinot_quote.py` and `test_pinot_engine.py` must stay green: the new
  `TableFacts` fields are all defaulted and `segment_facts` drops nothing on an OFFLINE
  fixture, where no entry satisfies all four markers.

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/metadata.py \
          server/tests/adapters/pinot/test_pinot_metadata.py \
          server/tests/adapters/pinot/fixtures/externalview-airlineStats-realtime.json \
          server/tests/adapters/pinot/fixtures/seg-metadata-airlineStats-realtime.json \
          server/tests/adapters/pinot/fixtures/seg-metadata-airlineStats-realtime-columns.json \
          server/tests/adapters/pinot/fixtures/size-airlineStats-realtime.json \
          server/tests/adapters/pinot/fixtures/tableconfig-airlineStats-realtime.json \
          server/tests/adapters/pinot/fixtures/tableconfig-airlineStats-realtime-bundled.json \
          server/tests/adapters/pinot/fixtures/tableconfig-u12upsert.json \
          server/tests/adapters/pinot/fixtures/seg-metadata-u12upsert.json \
          server/tests/adapters/pinot/fixtures/size-u12upsert.json
  git commit -m "feat(pinot): read the consuming count, the flush threshold and metadata completeness

  A consuming segment reports totalDocs 0 and -1 bytes permanently — ten
  probes over 30s while it held 50 live docs never moved off zero — so it
  cannot be priced from its own metadata and must not be kept as a zero
  either: one None bytes is what makes a whole REALTIME table unquotable
  today. It is dropped from the sealed set on all four of rule 5's markers
  together, counted from externalview cross-checked with missingSegments at
  the larger of the two, and bounded in rows by the stream's flush
  threshold, which the bundled quickstart config writes under the deprecated
  .size spelling.

  Completeness is the other half, and it fixes a latent OFFLINE under-quote:
  on a 2-server table /segments/{t}/metadata returns one server's half and
  alternates which half between identical calls, so the shipped sum would
  have been a confident sum over a subset. The size report names every
  segment, so the two sets are compared and a gap takes the quote low.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 2: Key evidence in `metadata.py` — the upsert primary key and the single-segment unique column

**Files:** `server/src/lagaam/adapters/pinot/metadata.py`,
`server/tests/adapters/pinot/test_pinot_metadata.py`, four new fixtures.

**Interfaces:**
- Consumes: `GET /tables/{t}`, `GET /schemas/{schemaName}`, `GET /tables/{t}/metadata`,
  `GET /segments/{t}/metadata` (all columns).
- Produces:
  - `def upsert_keys(config_json: Any, schema_json: Any, table_metadata_json: Any) -> frozenset[frozenset[str]]`
    — `{frozenset(primaryKeyColumns lowercased)}` when the config carries an
    `upsertConfig` object **and** the schema's `primaryKeyColumns` is a non-empty list of
    non-empty strings **and** `upsertPartitionToServerPrimaryKeyCountMap` is non-empty;
    `frozenset()` otherwise. The two documents disagreeing — a non-empty map with no
    `upsertConfig`, or an `upsertConfig` with an empty map — withholds the evidence.
  - `def single_segment_unique_columns(seg_metadata_json: Any, config_json: Any, schema_json: Any) -> frozenset[frozenset[str]]`
    — one single-column key set per column whose per-segment `cardinality` equals that
    segment's `totalDocs`, **only** when the response holds exactly one sealed segment and
    the table has no consuming segment, and **only** for a column whose nullability is
    established: its `fieldSpec.notNull` is true, or the half's
    `tableIndexConfig.nullHandlingEnabled` is false and the schema does not mark the column
    nullable. Where neither can be established it yields nothing.
- The set of sealed segments and the consuming count both come from what this module
  already computes: `single_segment_unique_columns` calls `segment_facts`-style parsing
  over the same response and requires `len(sealed) == 1`, and the consuming test is
  `"columns" not in body` on the remaining entries — it takes no externalview, because it
  is called from `table_facts` which has the consuming count already (see Step 4).

The null caveat is why source (b) is deliberately hard to trip: every column measured had
`count(col) == totalDocs`, so whether `cardinality` counts a null or a default as a
distinct value was never exercised (log §6). It finds nothing on the quickstart data
anyway — best ratio `playerID` at 0.185 of 97,889 docs, and 0 of 2,604 per-column entries
on `airlineStats` (log §3.2, rule 14).

- [ ] **Step 1 — capture the fixtures.**
  ```bash
  SPIKE=/Users/muditkapoor/Documents/code/lagaam/.superpowers/spike-u12
  FIX=server/tests/adapters/pinot/fixtures
  cp "$SPIKE/02-upsert-schema-readback.json"              "$FIX/schema-u12upsert.json"
  cp "$SPIKE/02-plain-schema-posted.json"                 "$FIX/schema-u12plain.json"
  cp "$SPIKE/02-plain-table-posted.json"                  "$FIX/tableconfig-u12plain.json"
  cp "$SPIKE/02-upsert-tablemetadata.json"                "$FIX/metadata-u12upsert.json"
  cp "$SPIKE/02-plain-tablemetadata.json"                 "$FIX/metadata-u12plain.json"
  cp "$SPIKE/03-baseballStats-segmetadata-allcols.json"   "$FIX/seg-metadata-baseballStats-allcols.json"
  ```
  Confirm:
  ```bash
  python3 - <<'PY'
  import json, pathlib
  fix = pathlib.Path("server/tests/adapters/pinot/fixtures")
  load = lambda n: json.loads((fix / n).read_text())
  up, plain = load("schema-u12upsert.json"), load("schema-u12plain.json")
  print("pk", up.get("primaryKeyColumns"), plain.get("primaryKeyColumns"))
  print("upsertConfig", "upsertConfig" in load("tableconfig-u12upsert.json")["REALTIME"],
        "upsertConfig" in load("tableconfig-u12plain.json")["REALTIME"])
  print("pkmap", load("metadata-u12upsert.json")["upsertPartitionToServerPrimaryKeyCountMap"],
        load("metadata-u12plain.json")["upsertPartitionToServerPrimaryKeyCountMap"])
  bb = load("seg-metadata-baseballStats-allcols.json")
  only = next(iter(bb.values()))
  print("segments", len(bb), "docs", only["totalDocs"], "columns", len(only["columns"]))
  print("best", max((c["cardinality"] / only["totalDocs"], c["columnName"]) for c in only["columns"]))
  print("notNull", sorted({c["fieldSpec"]["notNull"] for c in only["columns"]}))
  PY
  ```
  Expect (log §2.1, §3.1, §3.2):
  ```
  pk ['pk'] None
  upsertConfig True False
  pkmap {'0': {'Server_172.19.0.3_7051': 50}, '1': {'Server_172.19.0.3_7052': 50}} {}
  segments 1 docs 97889 columns 25
  best (0.18497481841677818, 'playerID')
  notNull [False]
  ```
  `tableconfig-u12plain.json` is the POSTed config rather than a readback — it is the
  byte-identical twin on purpose, and what matters of it is the absent `upsertConfig`.

- [ ] **Step 2 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_metadata.py`:
  ```python
  def test_an_upsert_tables_primary_key_is_evidence() -> None:
      """upsertConfig on the config, primaryKeyColumns on the schema, PK map non-empty."""
      assert upsert_keys(
          load("tableconfig-u12upsert.json"),
          load("schema-u12upsert.json"),
          load("metadata-u12upsert.json"),
      ) == frozenset({frozenset({"pk"})})


  def test_the_non_upsert_twin_yields_no_evidence() -> None:
      assert (
          upsert_keys(
              load("tableconfig-u12plain.json"),
              load("schema-u12plain.json"),
              load("metadata-u12plain.json"),
          )
          == frozenset()
      )


  def test_the_whole_primary_key_is_the_key_never_a_subset() -> None:
      """A composite key is unique as a tuple; a proper subset need not be."""
      schema = load("schema-u12upsert.json")
      schema["primaryKeyColumns"] = ["Region", "pk"]
      assert upsert_keys(
          load("tableconfig-u12upsert.json"), schema, load("metadata-u12upsert.json")
      ) == frozenset({frozenset({"region", "pk"})})


  def test_a_config_and_a_pk_map_that_disagree_withhold_the_evidence() -> None:
      """Two documents disagreeing about what a table is, is not proof."""
      assert (
          upsert_keys(
              load("tableconfig-u12upsert.json"),
              load("schema-u12upsert.json"),
              load("metadata-u12plain.json"),
          )
          == frozenset()
      )
      assert (
          upsert_keys(
              load("tableconfig-u12plain.json"),
              load("schema-u12upsert.json"),
              load("metadata-u12upsert.json"),
          )
          == frozenset()
      )


  @pytest.mark.parametrize("columns", [None, [], ["", "pk"], "pk", [1, 2], [None]])
  def test_a_primary_key_list_that_is_not_names_yields_nothing(columns: Any) -> None:
      schema = load("schema-u12upsert.json")
      schema["primaryKeyColumns"] = columns
      assert (
          upsert_keys(
              load("tableconfig-u12upsert.json"), schema, load("metadata-u12upsert.json")
          )
          == frozenset()
      )


  @pytest.mark.parametrize("body", [None, {}, [], "junk"])
  def test_upsert_keys_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
      assert upsert_keys(body, body, body) == frozenset()


  def test_no_column_on_the_single_segment_table_reaches_its_doc_count() -> None:
      """Sound, and it finds nothing: the best ratio is playerID at 0.185."""
      assert (
          single_segment_unique_columns(
              load("seg-metadata-baseballStats-allcols.json"),
              load("tableconfig-baseballStats.json"),
              load("schema-baseballStats.json"),
          )
          == frozenset()
      )


  def test_a_column_whose_cardinality_equals_its_docs_is_a_key() -> None:
      """Gated on notNull, because a null could collide on the default value."""
      capture = {
          "seg0": {
              "segmentName": "seg0",
              "totalDocs": 3,
              "columns": [
                  {
                      "columnName": "id",
                      "cardinality": 3,
                      "totalDocs": 3,
                      "indexSizeMap": {"forward_index": 12},
                      "fieldSpec": {"name": "id", "notNull": True},
                  },
                  {
                      "columnName": "city",
                      "cardinality": 2,
                      "totalDocs": 3,
                      "indexSizeMap": {"forward_index": 8},
                      "fieldSpec": {"name": "city", "notNull": True},
                  },
              ],
          }
      }
      assert single_segment_unique_columns(capture, {}, {}) == frozenset(
          {frozenset({"id"})}
      )


  def test_a_nullable_column_yields_nothing_however_unique_it_looks() -> None:
      """The null caveat is unclosed: cardinality may count a null as a value."""
      capture = {
          "seg0": {
              "segmentName": "seg0",
              "totalDocs": 3,
              "columns": [
                  {
                      "columnName": "id",
                      "cardinality": 3,
                      "totalDocs": 3,
                      "indexSizeMap": {"forward_index": 12},
                      "fieldSpec": {"name": "id", "notNull": False},
                  }
              ],
          }
      }
      assert single_segment_unique_columns(capture, {}, {}) == frozenset()


  def test_null_handling_disabled_plus_a_non_nullable_schema_is_enough() -> None:
      """The second gate the spec allows: the table cannot store a null at all."""
      capture = {
          "seg0": {
              "segmentName": "seg0",
              "totalDocs": 2,
              "columns": [
                  {
                      "columnName": "id",
                      "cardinality": 2,
                      "totalDocs": 2,
                      "indexSizeMap": {"forward_index": 8},
                      "fieldSpec": {"name": "id", "notNull": False},
                  }
              ],
          }
      }
      config = {"OFFLINE": {"tableIndexConfig": {"nullHandlingEnabled": False}}}
      schema = {"dimensionFieldSpecs": [{"name": "id", "dataType": "STRING"}]}
      assert single_segment_unique_columns(capture, config, schema) == frozenset(
          {frozenset({"id"})}
      )
      enabled = {"OFFLINE": {"tableIndexConfig": {"nullHandlingEnabled": True}}}
      assert single_segment_unique_columns(capture, enabled, schema) == frozenset()


  def test_more_than_one_sealed_segment_proves_nothing() -> None:
      """Per-segment cardinality is the table's only where the two are one:
      summing Carrier over 31 segments gave 432 against a true 14."""
      capture = {
          f"seg{i}": {
              "segmentName": f"seg{i}",
              "totalDocs": 2,
              "columns": [
                  {
                      "columnName": "id",
                      "cardinality": 2,
                      "totalDocs": 2,
                      "indexSizeMap": {"forward_index": 8},
                      "fieldSpec": {"name": "id", "notNull": True},
                  }
              ],
          }
          for i in (0, 1)
      }
      assert single_segment_unique_columns(capture, {}, {}) == frozenset()


  def test_a_consuming_segment_beside_the_sealed_one_proves_nothing() -> None:
      """The consuming segment's rows are not in any cardinality anyone can read."""
      capture = {
          "seg0": {
              "segmentName": "seg0",
              "totalDocs": 2,
              "columns": [
                  {
                      "columnName": "id",
                      "cardinality": 2,
                      "totalDocs": 2,
                      "indexSizeMap": {"forward_index": 8},
                      "fieldSpec": {"name": "id", "notNull": True},
                  }
              ],
          },
          "seg1": {"segmentName": "seg1", "totalDocs": 0, "crc": -9223372036854775808},
      }
      assert single_segment_unique_columns(capture, {}, {}) == frozenset()


  @pytest.mark.parametrize("body", [None, {}, [], "junk"])
  def test_single_segment_unique_columns_never_raises(body: Any) -> None:
      assert single_segment_unique_columns(body, body, body) == frozenset()


  def test_table_facts_carry_the_upsert_key() -> None:
      facts = table_facts(
          "u12upsert",
          load("tableconfig-u12upsert.json"),
          load("seg-metadata-u12upsert.json"),
          load("size-u12upsert.json"),
          schema_json=load("schema-u12upsert.json"),
          table_metadata_json=load("metadata-u12upsert.json"),
      )
      assert facts.unique_keys == frozenset({frozenset({"pk"})})
      assert facts.consuming == 2
      assert facts.flush_rows == 200
      assert facts.complete is False
  ```
  Extend the module's import with `single_segment_unique_columns` and `upsert_keys`.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_metadata.py`
  Expect `ImportError: cannot import name 'upsert_keys' from 'lagaam.adapters.pinot.metadata'`.

- [ ] **Step 4 — implementation.** Append to `server/src/lagaam/adapters/pinot/metadata.py`:
  ```python
  def upsert_keys(
      config_json: Any, schema_json: Any, table_metadata_json: Any
  ) -> frozenset[frozenset[str]]:
      """An upsert table's primary key, as the one column set proved unique.

      Measured: GROUP BY pk HAVING count(*) > 1 returns nothing on the upsert
      table and 6-version rows on a byte-identical non-upsert table reading the
      same topic, and the PK self-join returns exactly the distinct-key count
      against 3,600 for the twin.

      Three documents have to agree. The config's upsertConfig and the
      metadata's PK map are each independently proof of an upsert table — the
      map is {} on every non-upsert table — so a disagreement between them is
      two documents contradicting each other about what this table is, which is
      not a basis for admitting a join. The full column list is the key and
      never a subset: a composite primary key is unique as a tuple.
      """
      if not isinstance(schema_json, dict):
          return frozenset()
      declared = _has_upsert_config(config_json)
      counted = _has_primary_key_counts(table_metadata_json)
      if not declared or not counted:
          return frozenset()
      columns = schema_json.get("primaryKeyColumns")
      if not isinstance(columns, list) or not columns:
          return frozenset()
      names = {
          column.lower()
          for column in columns
          if isinstance(column, str) and column and not isinstance(column, bool)
      }
      if len(names) != len(columns):
          return frozenset()
      return frozenset({frozenset(names)})


  def single_segment_unique_columns(
      seg_metadata_json: Any, config_json: Any, schema_json: Any
  ) -> frozenset[frozenset[str]]:
      """Columns whose cardinality equals their docs, on a one-sealed-segment table.

      cardinality is exactly count(DISTINCT col) — verified against the engine
      on four columns — and count(DISTINCT col) <= count(col) <= totalDocs, so
      equality forces every doc to be counted and every value to differ. That
      argument is the segment's, and it is the table's only where the two are
      the same rows: one sealed segment and nothing consuming.

      Gated on nullability because the null caveat is unclosed (log §6): if
      cardinality counts a null or a default as a distinct value, a column with
      one null could report cardinality == totalDocs while two rows share the
      default. Where nullability cannot be established, nothing is yielded.
      """
      if not isinstance(seg_metadata_json, dict):
          return frozenset()
      sealed = [
          body
          for body in seg_metadata_json.values()
          if isinstance(body, dict) and isinstance(body.get("columns"), list)
      ]
      consuming = [
          body
          for body in seg_metadata_json.values()
          if isinstance(body, dict) and not isinstance(body.get("columns"), list)
      ]
      if len(sealed) != 1 or consuming:
          return frozenset()
      docs = _positive_int(sealed[0].get("totalDocs"))
      if docs is None:
          return frozenset()
      nullable_off = _null_handling_disabled(config_json)
      schema_nullable = _schema_nullable_columns(schema_json)
      keys: set[frozenset[str]] = set()
      for column in sealed[0]["columns"]:
          if not isinstance(column, dict):
              continue
          name = column.get("columnName")
          cardinality = _positive_int(column.get("cardinality"))
          if not isinstance(name, str) or not name or cardinality != docs:
              continue
          spec = column.get("fieldSpec")
          not_null = isinstance(spec, dict) and spec.get("notNull") is True
          if not not_null and not (
              nullable_off and name.lower() not in schema_nullable
          ):
              continue
          keys.add(frozenset({name.lower()}))
      return frozenset(keys)


  def _has_upsert_config(config_json: Any) -> bool:
      """Does either half's config carry an upsertConfig object?"""
      if not isinstance(config_json, dict):
          return False
      for key in ("REALTIME", "OFFLINE"):
          half = config_json.get(key)
          if isinstance(half, dict) and isinstance(half.get("upsertConfig"), dict):
              return True
      return False


  def _has_primary_key_counts(table_metadata_json: Any) -> bool:
      """Is upsertPartitionToServerPrimaryKeyCountMap non-empty?

      It is {} on every non-upsert table, so a non-empty map is itself proof.
      It is never read as a count: it is per server, and replication > 1 is
      unmeasured (spec decision 6).
      """
      if not isinstance(table_metadata_json, dict):
          return False
      counts = table_metadata_json.get("upsertPartitionToServerPrimaryKeyCountMap")
      return isinstance(counts, dict) and bool(counts)


  def _null_handling_disabled(config_json: Any) -> bool:
      """Is tableIndexConfig.nullHandlingEnabled explicitly false on a half?"""
      if not isinstance(config_json, dict):
          return False
      for key in ("REALTIME", "OFFLINE"):
          half = config_json.get(key)
          if not isinstance(half, dict):
              continue
          index_config = half.get("tableIndexConfig")
          if isinstance(index_config, dict) and index_config.get(
              "nullHandlingEnabled"
          ) is False:
              return True
      return False


  def _schema_nullable_columns(schema_json: Any) -> frozenset[str]:
      """Lowercase names the schema marks nullable, which no gate may pass."""
      if not isinstance(schema_json, dict):
          return frozenset()
      nullable: set[str] = set()
      for key in _FIELD_SPEC_KEYS:
          specs = schema_json.get(key)
          if not isinstance(specs, list):
              continue
          for spec in specs:
              if not isinstance(spec, dict):
                  continue
              name = spec.get("name")
              if isinstance(name, str) and name and spec.get("nullable") is True:
                  nullable.add(name.lower())
      return frozenset(nullable)
  ```
  Then replace the `unique_keys=frozenset()` line Task 1 left in `table_facts` with:
  ```python
          unique_keys=upsert_keys(config_json, schema_json, table_metadata_json)
          or single_segment_unique_columns(seg_metadata_json, config_json, schema_json),
  ```
  The upsert source wins where both exist; `or` on a frozenset is the empty-set
  fallthrough, and `single_segment_unique_columns` runs on the same segment metadata
  response `segment_facts` already received, so no fourth document is fetched.

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/metadata.py \
          server/tests/adapters/pinot/test_pinot_metadata.py \
          server/tests/adapters/pinot/fixtures/schema-u12upsert.json \
          server/tests/adapters/pinot/fixtures/schema-u12plain.json \
          server/tests/adapters/pinot/fixtures/tableconfig-u12plain.json \
          server/tests/adapters/pinot/fixtures/metadata-u12upsert.json \
          server/tests/adapters/pinot/fixtures/metadata-u12plain.json \
          server/tests/adapters/pinot/fixtures/seg-metadata-baseballStats-allcols.json
  git commit -m "feat(pinot): read the two kinds of join-key evidence the catalog does carry

  ADR 0008 recorded that 1.5.1 proves no join key. That is right about the
  plan — its Calcite rowcounts are a constant 100 per scan — and wrong about
  the catalog. An upsert table's primary key is unique in the query-visible
  view: GROUP BY pk HAVING count(*) > 1 returns nothing where the
  byte-identical non-upsert twin on the same topic returns six versions per
  key. And on a table with exactly one sealed segment, a column whose
  cardinality equals its totalDocs is unique, because cardinality is exactly
  count(DISTINCT col) and count(DISTINCT col) <= count(col) <= totalDocs.

  Both are gated harder than the argument needs. The upsert key wants the
  config, the schema and a non-empty PK count map to agree, because two
  documents contradicting each other about what a table is, is not proof.
  The cardinality rule wants the column's nullability established, because
  whether a null counts as a distinct value was never measured — which is
  why it finds nothing on the quickstart data, and is meant to.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 3: `consuming_segments_queried` in `response.py`

**Files:** `server/src/lagaam/adapters/pinot/response.py`,
`server/tests/adapters/pinot/test_pinot_response.py`, two new fixtures.

**Interfaces:**
- Consumes: the same single-stage `EXPLAIN` body `surviving_segments` already parses.
- Produces: `def consuming_segments_queried(explain_json: Any) -> int` — 0 for absent,
  unreadable, negative or non-int, and 0 for a body that fails the same guards
  `surviving_segments` applies (not a dict, `exceptions` populated, `numDocsScanned != 0`).
  0 is the fail-safe reading: it leaves the sealed k *larger*, charging more segments.
- `surviving_segments` is **unchanged**, and no counter it reads is touched.

- [ ] **Step 1 — capture the fixtures.** These are raw broker answers, which the spike
  captured only as a `stats` digest (`01-explain-and-counters-600rows.json`), so they are
  re-captured against the live realtime instance the Task 7 profile brings up:
  ```bash
  FIX=server/tests/adapters/pinot/fixtures
  curl -s -X POST http://localhost:8001/query/sql -H 'Content-Type: application/json' \
    -d '{"sql":"EXPLAIN PLAN FOR SELECT Carrier FROM airlineStats LIMIT 10"}' \
    | python3 -m json.tool > "$FIX/explain-v1-realtime-nofilter.json"
  curl -s -X POST http://localhost:8001/query/sql -H 'Content-Type: application/json' \
    -d '{"sql":"EXPLAIN PLAN FOR SELECT Carrier FROM airlineStats WHERE DaysSinceEpoch > 99999 LIMIT 10"}' \
    | python3 -m json.tool > "$FIX/explain-v1-realtime-futuretime.json"
  ```
  Confirm against log §1.6 — these are the numbers every later test asserts:
  ```bash
  python3 -c "
  import json, pathlib
  fix = pathlib.Path('server/tests/adapters/pinot/fixtures')
  for name in ('explain-v1-realtime-nofilter.json', 'explain-v1-realtime-futuretime.json'):
      b = json.loads((fix / name).read_text())
      print(name, b['numDocsScanned'], b['numSegmentsQueried'],
            b['numConsumingSegmentsQueried'], b['numSegmentsPrunedByBroker'],
            b['numSegmentsPrunedByServer'], b['numSegmentsPrunedByLimit'])"
  ```
  Expect `explain-v1-realtime-nofilter.json 0 7 1 0 6 5` and
  `explain-v1-realtime-futuretime.json 0 1 1 6 1 0`. If the segment count has moved on
  from the captured 7 (the stream keeps sealing segments), re-run the Task 7 bootstrap
  against a fresh topic before capturing, so the fixture's 7-and-1 matches the segment
  metadata fixtures of Task 1 — a fixture set that disagrees with itself prices nothing.

- [ ] **Step 2 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_response.py`:
  ```python
  def test_the_consuming_counter_is_read_from_the_same_explain_body() -> None:
      assert consuming_segments_queried(load("explain-v1-realtime-nofilter.json")) == 1
      assert surviving_segments(load("explain-v1-realtime-nofilter.json")) == 2


  def test_a_filter_excluding_every_value_cannot_prune_the_consuming_segment() -> None:
      """Measured: ByBroker went to 6 of 7 and the consuming counter stayed 1."""
      body = load("explain-v1-realtime-futuretime.json")
      assert consuming_segments_queried(body) == 1
      assert body["numSegmentsQueried"] == 1


  def test_an_offline_explain_reports_no_consuming_segments() -> None:
      """The shipped OFFLINE fixtures are untouched by this counter."""
      assert consuming_segments_queried(load("explain-v1-timefilter.json")) == 0
      assert consuming_segments_queried(load("explain-v1-nofilter.json")) == 0


  @pytest.mark.parametrize(
      "body",
      [
          None,
          {},
          [],
          "junk",
          {"numConsumingSegmentsQueried": "1"},
          {"numConsumingSegmentsQueried": -1},
          {"numConsumingSegmentsQueried": True},
          {"numConsumingSegmentsQueried": 1, "exceptions": [{"errorCode": 150}]},
          {"numConsumingSegmentsQueried": 1, "numDocsScanned": 5},
      ],
  )
  def test_an_unreadable_consuming_counter_is_zero(body: Any) -> None:
      """Zero leaves the sealed k larger, which charges more segments."""
      assert consuming_segments_queried(body) == 0
  ```
  The two bodies carrying `numDocsScanned` need it set to 0 where the guard should pass:
  add `"numDocsScanned": 0` to the `"1"`, `-1` and `True` cases, so what each one proves
  is the type check and not the scan guard.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_response.py`
  Expect `ImportError: cannot import name 'consuming_segments_queried' from 'lagaam.adapters.pinot.response'`.

- [ ] **Step 4 — implementation.** Append to `server/src/lagaam/adapters/pinot/response.py`:
  ```python
  def consuming_segments_queried(explain_json: Any) -> int:
      """How many of the queried segments were CONSUMING, from the same EXPLAIN.

      numSegmentsQueried includes consuming segments, so this is what has to be
      subtracted before the k-largest charge is applied to the sealed ones.

      Unreadable is 0 rather than None, deliberately: 0 leaves the sealed k
      larger and charges more segments, and the consuming segments themselves
      are charged unconditionally elsewhere — measured, this counter stayed 1
      under filters excluding every possible value while the broker pruned 6 of
      7 segments, so no predicate may ever reduce it.
      """
      if not isinstance(explain_json, dict) or explain_json.get("exceptions"):
          return 0
      scanned = explain_json.get("numDocsScanned")
      if isinstance(scanned, bool) or not isinstance(scanned, int) or scanned != 0:
          return 0
      consuming = explain_json.get("numConsumingSegmentsQueried")
      if isinstance(consuming, bool) or not isinstance(consuming, int):
          return 0
      return max(0, consuming)
  ```

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/response.py \
          server/tests/adapters/pinot/test_pinot_response.py \
          server/tests/adapters/pinot/fixtures/explain-v1-realtime-nofilter.json \
          server/tests/adapters/pinot/fixtures/explain-v1-realtime-futuretime.json
  git commit -m "feat(pinot): read numConsumingSegmentsQueried beside the pruning counters

  numSegmentsQueried includes the consuming segments, so the k the sealed
  charge applies to is queried minus this. It comes from the EXPLAIN response
  the pruning oracle already issues, and surviving_segments is untouched.

  Unreadable is 0 rather than None on purpose: 0 leaves the sealed k larger,
  and the consuming segments are charged unconditionally elsewhere anyway.
  Measured, the counter stayed 1 under DaysSinceEpoch > 99999 while the
  broker pruned 6 of 7 segments — a predicate can shrink the sealed k and
  never the consuming one.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 4: The consuming charge, the bytes projection and the completeness gate in `quote.py`

**Files:** `server/src/lagaam/adapters/pinot/quote.py`,
`server/tests/adapters/pinot/test_pinot_quote.py`.

**Interfaces:**
- Consumes: `TableFacts` as Tasks 1–2 leave it (`consuming`, `flush_rows`, `complete`),
  and the sealed surviving count the caller computes.
- Produces:
  - `def surviving_docs(facts: TableFacts, surviving: int | None) -> int | None` — same
    signature, new arithmetic: `None` when `facts.complete` is false; the k-largest sum
    over the sealed segments **plus** `facts.consuming * facts.flush_rows`; `None` when
    `facts.consuming > 0` and `facts.flush_rows` is `None`.
  - `def surviving_bytes(facts: TableFacts, surviving: int | None, columns: frozenset[str] | None) -> int | None`
    — same signature, plus the consuming projection: the k-largest sum over the sealed
    segments plus `facts.consuming * max over sealed segments with docs > 0 of
    ceil(flush_rows * charged_bytes / docs)`; `None` when `facts.complete` is false, when
    `facts.consuming > 0` and `flush_rows` is `None`, and when `facts.consuming > 0` with
    no sealed segment carrying `docs > 0`.
  - `def quote(tables, columns, max_intermediate_rows) -> CostEstimate` — the blanket
    `None if realtime else …` guard is **removed**; `confidence` is still `"low"` exactly
    when `scanned_bytes` is `None`.
- **`surviving` semantics on the caller's side.** `surviving` is now the *sealed*
  surviving count: `numSegmentsQueried − numConsumingSegmentsQueried`, floored at 0, which
  Task 6 computes. `_k` is unchanged and still maps `None` to "all sealed segments" — so
  **a `None` surviving charges every sealed segment and then adds the consuming charge on
  top**, which is the no-oracle case and the largest number this module can produce for a
  given table. A `surviving` of 0 (every sealed segment pruned, only the consuming one
  queried) reaches `_k` as 0 and `_k`'s existing `max(1, surviving)` charges one sealed
  segment: the floor stays, because the oracle has been seen to prune every segment and
  something is always read.

**The arithmetic, with its numbers** (log §1.3, §1.4, rule 20, all recomputed in Step 1):

```
rows  = Σ k-largest sealed docs                     + consuming × flush_rows
bytes = Σ k-largest sealed charged-bytes            + consuming × max_per_segment_bound

max_per_segment_bound = max over sealed segments with docs > 0 of
                          (flush_rows × charged_bytes + docs − 1) // docs
```

`charged_bytes` for a segment is **the same per-segment choice `_column_sizes` already
makes**: the summed `indexSizeMap` bytes of the referenced columns when the segment
carries every one of them, and `reportedSizeInBytes` when it falls back. Mixing is fine —
a maximum over a mixed set is still ≥ every member — and the fallback ratio is larger by
three orders of magnitude, so it dominates whenever any segment falls back, which is the
fail-safe direction. The division is integer with a manual ceiling, so no float rounding
can shave a byte off the bound.

On the 600-row moment: six sealed at 100 docs each, `consuming = 1`, `flush_rows = 100`.
Whole-segment: rows `600 + 100 = 700`, bytes `517,035 + 87,136 = 604,171`. Two-column
`Carrier` + `DaysSinceEpoch`: bytes `548 + 114 = 662`.

- [ ] **Step 1 — confirm the numbers from the fixtures before asserting them.**
  ```bash
  python3 - <<'PY'
  import json, pathlib
  fix = pathlib.Path("server/tests/adapters/pinot/fixtures")
  meta = json.loads((fix / "seg-metadata-airlineStats-realtime-columns.json").read_text())
  size = json.loads((fix / "size-airlineStats-realtime.json").read_text())
  reported = {n: b["reportedSizeInBytes"] for n, b in size["realtimeSegments"]["segments"].items()}
  sealed = {n: b for n, b in meta.items() if "columns" in b}
  whole = {n: reported[n] for n in sealed}
  cols = {n: sum(sum(c["indexSizeMap"].values()) for c in b["columns"]) for n, b in sealed.items()}
  docs = {n: b["totalDocs"] for n, b in sealed.items()}
  ceil = lambda num, d: (100 * num + d - 1) // d
  print("sealed", len(sealed), "docs", sum(docs.values()))
  print("whole sum", sum(whole.values()), "bound", max(ceil(whole[n], docs[n]) for n in sealed))
  print("cols sum", sum(cols.values()), "bound", max(ceil(cols[n], docs[n]) for n in sealed))
  PY
  ```
  Expect exactly:
  ```
  sealed 6 docs 600
  whole sum 517035 bound 87136
  cols sum 548 bound 114
  ```

- [ ] **Step 2 — failing test.** In `server/tests/adapters/pinot/test_pinot_quote.py`,
  extend `facts()` with the new keyword arguments and add the tests:
  ```python
  def facts(
      *segments: SegmentFact,
      types: frozenset[str] = frozenset({"OFFLINE"}),
      columns: frozenset[str] = frozenset(),
      consuming: int = 0,
      flush_rows: int | None = None,
      complete: bool = True,
  ) -> TableFacts:
      return TableFacts(
          table="t",
          types=types,
          time_column=None,
          segments=segments,
          columns=columns,
          consuming=consuming,
          flush_rows=flush_rows,
          complete=complete,
      )


  def realtime() -> TableFacts:
      return table_facts(
          "airlineStats",
          load("tableconfig-airlineStats-realtime.json"),
          load("seg-metadata-airlineStats-realtime-columns.json"),
          load("size-airlineStats-realtime.json"),
          frozenset({"carrier", "dayssinceepoch"}),
          externalview_json=load("externalview-airlineStats-realtime.json"),
      )


  def test_the_consuming_segment_is_charged_at_the_flush_threshold() -> None:
      """Six sealed at 100 docs, one consuming bounded at 100 by the config."""
      table = realtime()
      assert table.consuming == 1
      assert table.flush_rows == 100
      assert surviving_docs(table, None) == 700


  def test_the_consuming_charge_survives_a_filter_that_prunes_every_sealed_segment(
  ) -> None:
      """Measured: a time filter can never remove a consuming segment's cost."""
      table = realtime()
      # Sealed k floors at one segment; the consuming charge is added outside it.
      assert surviving_docs(table, 0) == 100 + 100


  def test_the_consuming_bytes_are_the_worst_sealed_ratio_ceiling_divided() -> None:
      table = realtime()
      assert surviving_bytes(table, None, None) == 517035 + 87136
      assert (
          surviving_bytes(table, None, frozenset({"carrier", "dayssinceepoch"}))
          == 548 + 114
      )


  def test_the_ratio_is_per_segment_and_maximised_never_averaged() -> None:
      """A 10-doc 1000-byte segment beside a 1000-doc 1000-byte one bounds at 100/doc."""
      table = facts(
          seg("small", 10, 1000),
          seg("big", 1000, 1000),
          types=frozenset({"REALTIME"}),
          consuming=1,
          flush_rows=5,
      )
      assert surviving_bytes(table, None, None) == 2000 + 500


  def test_the_bytes_product_is_ceiling_divided_in_integer_arithmetic() -> None:
      """3 x 7 / 2 is 10.5, and a bound may not be shaved to 10."""
      table = facts(
          seg("a", 2, 7), types=frozenset({"REALTIME"}), consuming=1, flush_rows=3
      )
      assert surviving_bytes(table, None, None) == 7 + 11


  def test_more_than_one_consuming_segment_is_charged_once_each() -> None:
      table = facts(
          seg("a", 100, 1000),
          types=frozenset({"REALTIME"}),
          consuming=2,
          flush_rows=50,
      )
      assert surviving_docs(table, None) == 100 + 100
      assert surviving_bytes(table, None, None) == 1000 + 1000


  def test_no_flush_threshold_makes_both_numbers_unknown() -> None:
      """Rule 7: nothing else in the catalog bounds a consuming segment."""
      table = facts(
          seg("a", 100, 1000), types=frozenset({"REALTIME"}), consuming=1
      )
      assert surviving_docs(table, None) is None
      assert surviving_bytes(table, None, None) is None


  def test_no_consuming_segment_makes_the_threshold_irrelevant() -> None:
      table = facts(seg("a", 100, 1000), types=frozenset({"REALTIME"}))
      assert surviving_docs(table, None) == 100
      assert surviving_bytes(table, None, None) == 1000


  def test_an_all_consuming_table_has_no_ratio_to_take() -> None:
      """Bytes only. Rows still hold: the threshold bounds them without a ratio."""
      table = facts(types=frozenset({"REALTIME"}), consuming=1, flush_rows=100)
      assert surviving_bytes(table, None, None) is None
      assert surviving_docs(table, None) == 100


  def test_a_sealed_segment_with_zero_docs_is_no_basis_for_a_ratio() -> None:
      """Dividing by its docs is not a bound, it is a crash."""
      table = facts(
          seg("empty", 0, 900), types=frozenset({"REALTIME"}), consuming=1, flush_rows=10
      )
      assert surviving_bytes(table, None, None) is None


  def test_incomplete_metadata_withholds_both_numbers() -> None:
      """A confident sum over half a table is the failure the gate exists to stop."""
      table = facts(seg("a", 100, 1000), complete=False)
      assert surviving_docs(table, None) is None
      assert surviving_bytes(table, None, None) is None


  def test_a_realtime_table_now_quotes_at_high_confidence() -> None:
      """Replaces the blanket REALTIME guard: the type is no longer a reason."""
      table = realtime()
      estimate = quote(
          [(table, None)], frozenset({"carrier", "dayssinceepoch"}), 700
      )
      assert estimate.row_estimate == 700
      assert estimate.scanned_bytes == 662
      assert estimate.confidence == "high"


  def test_a_realtime_table_nobody_can_bound_still_quotes_low() -> None:
      table = facts(
          seg("a", 100, 1000), types=frozenset({"REALTIME"}), consuming=1
      )
      estimate = quote([(table, None)], None, 100)
      assert estimate.scanned_bytes is None
      assert estimate.row_estimate is None
      assert estimate.confidence == "low"


  def test_a_hybrid_table_is_charged_as_one_segment_set() -> None:
      """Both halves' sealed segments come from the same two documents; there is
      no per-half arithmetic. Not measurable live: -type HYBRID cannot run in a
      container on 1.5.1, so this is the unit test that stands for it."""
      offline = load("size-airlineStats.json")
      realtime_size = load("size-airlineStats-realtime.json")
      merged = {
          "offlineSegments": offline["offlineSegments"],
          "realtimeSegments": realtime_size["realtimeSegments"],
      }
      merged_metadata = {
          **load("seg-metadata-airlineStats-columns.json"),
          **load("seg-metadata-airlineStats-realtime-columns.json"),
      }
      table = table_facts(
          "airlineStats",
          load("tableconfig-airlineStats-realtime.json"),
          merged_metadata,
          merged,
          frozenset({"carrier", "dayssinceepoch"}),
          externalview_json=load("externalview-airlineStats-realtime.json"),
      )
      assert table.types == frozenset({"REALTIME"})
      assert len(table.segments) == 31 + 6
      assert table.consuming == 1
      assert surviving_docs(table, None) == 9746 + 600 + 100
  ```
  Delete `test_a_realtime_half_is_quoted_low_however_good_the_numbers_look` — it asserts
  the removed guard, and `test_a_realtime_table_now_quotes_at_high_confidence` is its
  replacement. Extend the module's import with `table_facts` (already imported) and
  nothing else; `quote`, `surviving_bytes` and `surviving_docs` keep their names.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_quote.py`
  Expect `assert 600 == 700` from
  `test_the_consuming_segment_is_charged_at_the_flush_threshold` — the sealed sum is
  right and the consuming charge is missing.

- [ ] **Step 4 — implementation.** In `server/src/lagaam/adapters/pinot/quote.py`, replace
  `surviving_docs`, `surviving_bytes` and the `realtime` lines of `quote`, and add two
  private helpers:
  ```python
  def surviving_docs(facts: TableFacts, surviving: int | None) -> int | None:
      """Docs in the k largest sealed segments, plus the consuming charge.

      One unknown segment poisons the sum: the others do not bound it. So does
      incomplete metadata, which means the segments in hand are not the table.

      The consuming term is added outside the k-largest logic and is never
      pruned: measured, numConsumingSegmentsQueried stayed 1 under filters
      excluding every possible value while the broker pruned 6 of 7 segments.
      """
      if not facts.complete:
          return None
      consuming = _consuming_docs(facts)
      if consuming is None:
          return None
      counts = [segment.docs for segment in facts.segments]
      if any(count is None for count in counts):
          return None
      known = sorted((count for count in counts if count is not None), reverse=True)
      if not known:
          return consuming if facts.consuming else None
      return sum(known[: _k(surviving, len(known))]) + consuming


  def surviving_bytes(
      facts: TableFacts, surviving: int | None, columns: frozenset[str] | None
  ) -> int | None:
      """Bytes in the k largest sealed segments, plus the consuming projection.

      A segment matching none of the referenced columns is charged whole,
      and unresolvable columns fall back to whole segments table-wide.

      The consuming term is the one projected number in a Lagaam quotation: a
      consuming segment reports -1 bytes on every probe, so its bytes are
      flush_rows times the worst bytes-per-doc ratio observed on this table's
      own sealed segments, taken per segment and maximised, never averaged. No
      sealed segment with docs > 0 leaves nothing to take a ratio from, and
      inventing one would be a guess.
      """
      if not facts.complete:
          return None
      sizes = _column_sizes(facts, columns)
      if sizes is None:
          sizes = [segment.total_bytes for segment in facts.segments]
      if any(size is None for size in sizes):
          return None
      consuming = _consuming_bytes(facts, sizes)
      if consuming is None:
          return None
      known = sorted((size for size in sizes if size is not None), reverse=True)
      if not known:
          return None
      return sum(known[: _k(surviving, len(known))]) + consuming


  def _consuming_docs(facts: TableFacts) -> int | None:
      """Rows the consuming segments may hold, or None if nothing bounds them."""
      if facts.consuming <= 0:
          return 0
      if facts.flush_rows is None:
          return None
      return facts.consuming * facts.flush_rows


  def _consuming_bytes(facts: TableFacts, sizes: list[int | None]) -> int | None:
      """Bytes the consuming segments may hold, projected from sealed ratios.

      `sizes` is per segment in facts.segments order and carries the same
      per-segment column-or-whole choice the sealed charge made, so the ratio
      is taken over exactly the bytes being charged.
      """
      if facts.consuming <= 0:
          return 0
      if facts.flush_rows is None:
          return None
      bounds = [
          (facts.flush_rows * size + segment.docs - 1) // segment.docs
          for segment, size in zip(facts.segments, sizes)
          if segment.docs and size is not None
      ]
      if not bounds:
          return None
      return facts.consuming * max(bounds)
  ```
  And in `quote`, delete the `realtime = any(...)` line with its two-line comment and the
  conditional, leaving:
  ```python
      rows = _total(surviving_docs(facts, k) for facts, k in tables)
      total_bytes = _total(surviving_bytes(facts, k, columns) for facts, k in tables)
  ```
  Update the module docstring's closing paragraph to name the one projection:
  ```python
  A number that cannot be bounded is None, which the budget gate denies. That
  is the whole contract: this module never returns a figure it cannot defend —
  with one named exception, the consuming segments' bytes, which no endpoint
  reports and which are projected from the worst bytes-per-doc ratio among
  this table's own sealed segments (ADR 0009).
  ```

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`
  Every other test in `test_pinot_quote.py` must pass unchanged: an OFFLINE table has
  `consuming == 0` and `complete is True`, so `_consuming_docs` and `_consuming_bytes`
  both return 0 and the sums are the shipped ones.

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/quote.py \
          server/tests/adapters/pinot/test_pinot_quote.py
  git commit -m "feat(pinot): charge consuming segments at the flush threshold, in rows and bytes

  The blanket REALTIME guard goes. It withheld scanned_bytes the moment any
  table had a REALTIME half, which denied every query over data that landed
  seconds ago — the reason to run Pinot at all. In its place: the consuming
  segments are charged unconditionally, outside the k-largest logic, because
  a time predicate cannot prune one; rows at the stream's flush threshold,
  which bounds a consuming segment exactly (25 of 25 sealed segments held
  exactly 100 docs at threshold 100); bytes at that threshold times the worst
  bytes-per-doc ratio among this table's own sealed segments, taken per
  segment and maximised, ceiling-divided in integer arithmetic.

  The bytes are the one projected number in a Lagaam quotation and ADR 0009
  says so: nothing on 1.5.1 reports a consuming segment's size, so nothing
  can validate it against the thing it bounds. Everything else fails the way
  it always has — no threshold, no sealed ratio, or incomplete metadata is
  None, and None is a denial.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 5: Operand resolution and the unique-key join rule in `plan.py`

**Files:** `server/src/lagaam/adapters/pinot/plan.py`,
`server/tests/adapters/pinot/test_pinot_plan.py`, three new fixtures.

**Interfaces:**
- Consumes: the multi-stage `EXPLAIN … AS JSON` `rels[]` string, the per-table leaf sizes
  `max_intermediate_rows` already takes, and the new `unique_keys` mapping.
- Produces:
  - `def join_key_pairs(rel_id: str, by_id: dict[str, dict[str, Any]], previous: dict[str, str | None], children: list[str]) -> list[tuple[str, str]]`
    — ordered (left column, right column) pairs, lowercase, for one join node; `[]` for
    any doubt at all.
  - `def side_table_and_fields(rel_id: str, by_id: dict[str, dict[str, Any]], previous: dict[str, str | None]) -> tuple[str, list[str], list[Any]] | None`
    — one side's `database.table` (lowercase), its top `LogicalProject`'s `fields`, and
    that project's `exprs`; `None` when the side is not a straight chain to exactly one
    scan.
  - `def max_intermediate_rows(plan_json: str, leaf_docs: Mapping[str, int | None], unique_keys: Mapping[str, frozenset[frozenset[str]]] = {}) -> int | None`
    — the third parameter defaults to an empty mapping, so every existing call keeps the
    product rule exactly.
- A mutable default is not used: the signature is written
  `unique_keys: Mapping[str, frozenset[frozenset[str]]] | None = None` and normalised to
  `{}` in the body, which is what the spec's `= {}` means in a `Mapping` annotation and
  what mypy strict will accept without a `B006`-shaped smell.

**The rules, each with its measurement** (log §5.1, rules 16–17):

- An operand index counts over **left fields ++ right fields**. Verified on the two-key
  plan: right `teamID` at right-index 1 resolves to global **3** = 2 left fields + 1, and
  right `league` at right-index 0 to global 2. The right `fields` list is *alphabetised*
  (`league` before `teamID`) and not in ON-clause order, so only the index is trustworthy.
- A side's names come from the `LogicalProject` at the top of that side. A
  `PinotLogicalTableScan` carries no `fields` at all, and the exchange between them is
  pass-through.
- A side yields evidence only on a straight chain of `LogicalProject`,
  `PinotLogicalExchange` and `LogicalFilter` nodes down to **exactly one** scan. A join, a
  union, an aggregate, a correlate, a second scan or any other node type yields nothing:
  the rows reaching the join are then not that table's rows.
- An operand naming a `$f`-prefixed field, or whose corresponding `exprs` entry carries an
  `op` key rather than a bare `input`, is an expression: `upper(a.Carrier)` became `$f85`
  with an `op` of `UPPER`. Both markers are checked, either one disqualifies.
- Only a top-level `EQUALS`, or an `EQUALS` directly under a top-level `AND`, is a
  candidate. An `OR`, a `NOT`, a nested `AND` under an `OR`, or any other `op.kind` yields
  no evidence at all — ADR 0008's `OR` case stands.

**The join arithmetic** (spec decision 5), written out so nothing is inferred:

| evidence | charge |
|---|---|
| the left side's pair columns cover a key of the left table | `right + left + right` |
| the right side's pair columns cover a key of the right table | `left + left + right` |
| both | `min(left, right) + left + right` |
| neither, or no pairs at all | `left × right + left + right` |

"Cover" is subset, not equality: a join equating more columns than the key still has at
most one match per key value. The `+ left + right` term is on every branch for the reason
ADR 0008 already gives — an outer join emits its unmatched rows above the matched ones,
and the additive term is exact-safe on a side of 0 or 1 rows. Sanity against the
measurement: the upsert self-join is both-unique on `pk` at 100 rows a side, so
`min(100,100) + 100 + 100 = 300` against the 100 the engine built — a bound, not the
answer; the plain twin has no evidence and is charged `600 × 600 + 600 + 600 = 361,200`
against 3,600 built.

- [ ] **Step 1 — capture the fixtures.** The spike's plan captures are already the
  `{"rels": [...]}` shape this module parses, so they are copied and wrapped exactly as
  the shipped `explain-mse-*.json` fixtures are — those hold the whole broker answer with
  the plan string in `resultTable.rows[0][1]`, which `plan_cell()` reads:
  ```bash
  SPIKE=/Users/muditkapoor/Documents/code/lagaam/.superpowers/spike-u12
  FIX=server/tests/adapters/pinot/fixtures
  python3 - <<'PY'
  import json, pathlib
  spike = pathlib.Path("/Users/muditkapoor/Documents/code/lagaam/.superpowers/spike-u12")
  fix = pathlib.Path("server/tests/adapters/pinot/fixtures")
  shipped = json.loads((fix / "explain-mse-equijoin.json").read_text())
  for source, target in (
      ("04-join-plain-plan.json", "explain-mse-join-plain.json"),
      ("04-join-twokeys-plan.json", "explain-mse-join-twokeys.json"),
      ("04-join-expr-plan.json", "explain-mse-join-expr.json"),
      ("04-join-left-plan.json", "explain-mse-join-left.json"),
  ):
      body = json.loads((spike / source).read_text())
      envelope = json.loads(json.dumps(shipped))
      envelope["resultTable"]["rows"] = [["LogicalProject", json.dumps(body)]]
      (fix / target).write_text(json.dumps(envelope, indent=2) + "\n")
      print(target, len(body["rels"]))
  PY
  ```
  Expect `11` rels for each of the four. Confirm the discriminating numbers:
  ```bash
  python3 -c "
  import json, pathlib
  fix = pathlib.Path('server/tests/adapters/pinot/fixtures')
  rels = json.loads(json.loads((fix / 'explain-mse-join-twokeys.json').read_text())['resultTable']['rows'][0][1])['rels']
  by = {r['id']: r for r in rels}
  print('left fields', by['1']['fields'], 'right fields', by['4']['fields'])
  print('cond', json.dumps(by['6']['condition']))
  expr = json.loads(json.loads((fix / 'explain-mse-join-expr.json').read_text())['resultTable']['rows'][0][1])['rels']
  print('expr fields', expr[1]['fields'], 'exprs[1] keys', sorted(expr[1]['exprs'][1]))
  left = json.loads(json.loads((fix / 'explain-mse-join-left.json').read_text())['resultTable']['rows'][0][1])['rels']
  print('joinType', left[6]['joinType'])"
  ```
  Expect left fields `['Carrier', 'Origin']`, right fields `['league', 'teamID']`, the
  condition an `AND` of `=($0,$3)` and `=($1,$2)`, expr fields `['Carrier', '$f85']` with
  `exprs[1]` carrying `op` and `operands` (no `input`), and `joinType left`.
  `explain-mse-join-plain.json` is the fourth file; the File Structure table names the
  three discriminating ones and this is the baseline beside them.

- [ ] **Step 2 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_plan.py`:
  ```python
  UPSERT_KEYS = {"default.airlinestats": frozenset({frozenset({"carrier"})})}
  BOTH_KEYS = {
      "default.airlinestats": frozenset({frozenset({"carrier"})}),
      "default.baseballstats": frozenset({frozenset({"teamid"})}),
  }


  def test_a_plain_equi_join_resolves_both_operands_to_column_names() -> None:
      assert join_pairs("explain-mse-join-plain.json") == [("carrier", "teamid")]


  def test_an_operand_index_counts_over_left_fields_then_right() -> None:
      """Right teamID at right-index 1 is global 3 = 2 left fields + 1."""
      assert join_pairs("explain-mse-join-twokeys.json") == [
          ("carrier", "teamid"),
          ("origin", "league"),
      ]


  def test_an_expression_operand_is_no_evidence() -> None:
      """upper(a.Carrier) became $f85 with an op of UPPER."""
      assert join_pairs("explain-mse-join-expr.json") == []


  def test_a_left_join_resolves_exactly_as_an_inner_one_does() -> None:
      """joinType is not read: the rule is about the key, not the join kind."""
      assert join_pairs("explain-mse-join-left.json") == [("carrier", "teamid")]


  def test_an_or_condition_is_no_evidence() -> None:
      """ADR 0008's OR case stands: an OR of equalities is not a key."""
      assert join_pairs("explain-mse-orjoin.json") == []


  def test_a_cross_join_has_no_operands_to_resolve() -> None:
      assert join_pairs("explain-mse-crossjoin.json") == []


  def test_a_left_unique_key_charges_the_right_side_plus_the_inputs() -> None:
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-plain.json"), LEAVES, UPSERT_KEYS
      ) == BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_a_right_unique_key_charges_the_left_side_plus_the_inputs() -> None:
      keys = {"default.baseballstats": frozenset({frozenset({"teamid"})})}
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-plain.json"), LEAVES, keys
      ) == AIRLINE_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_both_sides_unique_charge_the_smaller_one_plus_the_inputs() -> None:
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-plain.json"), LEAVES, BOTH_KEYS
      ) == min(AIRLINE_DOCS, BASEBALL_DOCS) + AIRLINE_DOCS + BASEBALL_DOCS


  def test_no_evidence_is_still_the_product() -> None:
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-plain.json"), LEAVES
      ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_a_key_on_a_column_the_join_does_not_equate_is_not_covered() -> None:
      keys = {"default.airlinestats": frozenset({frozenset({"origin"})})}
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-plain.json"), LEAVES, keys
      ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_a_join_equating_more_columns_than_the_key_still_covers_it() -> None:
      """Cover is subset, not equality: more equalities cannot mean more matches."""
      keys = {"default.airlinestats": frozenset({frozenset({"carrier"})})}
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-twokeys.json"), LEAVES, keys
      ) == BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_a_composite_key_needs_every_one_of_its_columns_equated() -> None:
      keys = {
          "default.airlinestats": frozenset({frozenset({"carrier", "dayssinceepoch"})})
      }
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-twokeys.json"), LEAVES, keys
      ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_a_composite_key_fully_equated_is_covered() -> None:
      keys = {"default.airlinestats": frozenset({frozenset({"carrier", "origin"})})}
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-twokeys.json"), LEAVES, keys
      ) == BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_an_expression_key_cannot_be_covered_however_unique_the_column() -> None:
      assert max_intermediate_rows(
          plan_cell("explain-mse-join-expr.json"), LEAVES, UPSERT_KEYS
      ) == AIRLINE_DOCS * BASEBALL_DOCS + AIRLINE_DOCS + BASEBALL_DOCS


  def test_a_self_join_on_a_unique_key_is_charged_the_bound_not_the_product() -> None:
      """The shipped self-join plan names the same node id as both inputs."""
      keys = {"default.airlinestats": frozenset({frozenset({"carrier"})})}
      product = max_intermediate_rows(plan_cell("explain-mse-selfjoin.json"), LEAVES)
      bounded = max_intermediate_rows(
          plan_cell("explain-mse-selfjoin.json"), LEAVES, keys
      )
      assert product is not None and bounded is not None
      assert bounded < product


  def test_a_side_that_is_not_one_scan_yields_no_evidence() -> None:
      """A join under a join: those rows are no longer that table's rows."""
      rels = json.loads(plan_cell("explain-mse-join-plain.json"))["rels"]
      nested = {
          "rels": rels
          + [
              {
                  "id": "11",
                  "relOp": "org.apache.pinot.calcite.rel.logical.PinotLogicalExchange",
                  "inputs": ["6"],
              },
              {
                  "id": "12",
                  "relOp": "org.apache.calcite.rel.logical.LogicalJoin",
                  "inputs": ["11", "5"],
                  "joinType": "inner",
                  "condition": {
                      "op": {"name": "=", "kind": "EQUALS"},
                      "operands": [{"input": 0}, {"input": 1}],
                  },
              },
          ]
      }
      widest = max_intermediate_rows(json.dumps(nested), LEAVES, BOTH_KEYS)
      unbounded = max_intermediate_rows(json.dumps(nested), LEAVES)
      assert widest == unbounded
  ```
  Add the helper beside `plan_cell`:
  ```python
  def join_pairs(name: str) -> list[tuple[str, str]]:
      """The resolved equality pairs of the one join node in this plan."""
      rels = json.loads(plan_cell(name))["rels"]
      by_id = {rel["id"]: rel for rel in rels}
      previous: dict[str, str | None] = {}
      last: str | None = None
      for rel in rels:
          previous[rel["id"]] = last
          last = rel["id"]
      join = next(rel for rel in rels if rel.get("joinType") or "Join" in rel["relOp"])
      return join_key_pairs(join["id"], by_id, previous, join["inputs"])
  ```
  and extend the import to
  `from lagaam.adapters.pinot.plan import join_key_pairs, max_intermediate_rows`.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_plan.py`
  Expect `ImportError: cannot import name 'join_key_pairs' from 'lagaam.adapters.pinot.plan'`.

- [ ] **Step 4 — implementation.** In `server/src/lagaam/adapters/pinot/plan.py`, add the
  node-type constants under `_MAX_DEPTH`:
  ```python
  # The only node types a side may carry between the join and its one scan.
  # Anything else — a join, a union, an aggregate, a correlate, a second scan —
  # means the rows reaching the join are not that table's rows any more, and
  # that table's key says nothing about them.
  _PASS_THROUGH = ("logicalproject", "pinotlogicalexchange", "logicalfilter")

  # Calcite's name for a field it synthesised rather than read from a column.
  _SYNTHETIC_FIELD_PREFIX = "$f"
  ```
  Change `max_intermediate_rows`'s signature and thread the mapping through `_rows`:
  ```python
  def max_intermediate_rows(
      plan_json: str,
      leaf_docs: Mapping[str, int | None],
      unique_keys: Mapping[str, frozenset[frozenset[str]]] | None = None,
  ) -> int | None:
      """The widest row count any node in this plan would build, or None.

      None means the plan could not be read or a table could not be sized —
      no quote, which the budget treats as a denial rather than as cheap.

      `unique_keys` maps a lowercase database.table to the column sets proved
      unique on it, from the catalog and never from the SQL's shape. A join
      whose equalities cover such a set matches at most one row per key value,
      so it is charged the other side rather than the product. Empty is the
      shipped behaviour: every join is the product.
      """
  ```
  and inside, after `previous` is built, pass `unique_keys or {}` into each `_rows` call.
  Replace the `_is_join` branch of `_rows` with:
  ```python
      elif _is_join(rel):
          # A join pairs its inputs unless the catalog proves a key covers one
          # side's equalities, in which case that side matches at most once.
          # The inputs are added on top of every branch because an outer join
          # also emits the rows that matched nothing.
          answer = _join_rows(rel_id, by_id, previous, unique_keys, children, child_rows)
  ```
  and add, at the end of the module:
  ```python
  def _join_rows(
      rel_id: str,
      by_id: dict[str, dict[str, Any]],
      previous: dict[str, str | None],
      unique_keys: Mapping[str, frozenset[frozenset[str]]],
      children: list[str],
      child_rows: list[int],
  ) -> int:
      """Rows this join builds: the product, or less on proven key evidence."""
      total = sum(child_rows)
      if len(children) != 2 or len(child_rows) != 2:
          product = 1
          for rows in child_rows:
              product *= rows
          return product + total
      left_rows, right_rows = child_rows
      left_covered, right_covered = _covered_sides(
          rel_id, by_id, previous, unique_keys, children
      )
      if left_covered and right_covered:
          return min(left_rows, right_rows) + total
      if left_covered:
          return right_rows + total
      if right_covered:
          return left_rows + total
      return left_rows * right_rows + total


  def _covered_sides(
      rel_id: str,
      by_id: dict[str, dict[str, Any]],
      previous: dict[str, str | None],
      unique_keys: Mapping[str, frozenset[frozenset[str]]],
      children: list[str],
  ) -> tuple[bool, bool]:
      """Does each side's equated column set cover a unique key of its table?"""
      pairs = join_key_pairs(rel_id, by_id, previous, children)
      if not pairs:
          return (False, False)
      left_side = side_table_and_fields(children[0], by_id, previous)
      right_side = side_table_and_fields(children[1], by_id, previous)
      if left_side is None or right_side is None:
          return (False, False)
      left_columns = {left for left, _ in pairs}
      right_columns = {right for _, right in pairs}
      return (
          _covers(unique_keys.get(left_side[0], frozenset()), left_columns),
          _covers(unique_keys.get(right_side[0], frozenset()), right_columns),
      )


  def _covers(keys: frozenset[frozenset[str]], equated: set[str]) -> bool:
      """Is any known key set a subset of the columns this join equates?

      Subset and not equality: equating more columns than the key still leaves
      at most one match per key value.
      """
      return any(key and key <= equated for key in keys)


  def join_key_pairs(
      rel_id: str,
      by_id: dict[str, dict[str, Any]],
      previous: dict[str, str | None],
      children: list[str],
  ) -> list[tuple[str, str]]:
      """(left column, right column) pairs this join equates, lowercase.

      An operand index counts over left fields ++ right fields — verified on
      the two-key plan, where right teamID at right-index 1 resolved to global
      3 = 2 left fields + 1. The right fields list is alphabetised rather than
      in ON-clause order, so only the index may be read.

      Any doubt yields no pairs at all, which charges the product: an OR, an
      expression operand, a side that is not one scan, an index out of range.
      """
      if len(children) != 2:
          return []
      rel = by_id.get(rel_id)
      if rel is None:
          return []
      left = side_table_and_fields(children[0], by_id, previous)
      right = side_table_and_fields(children[1], by_id, previous)
      if left is None or right is None:
          return []
      _, left_fields, left_exprs = left
      _, right_fields, right_exprs = right
      names = list(left_fields) + list(right_fields)
      exprs = list(left_exprs) + list(right_exprs)
      split = len(left_fields)
      pairs: list[tuple[str, str]] = []
      for first, second in _equalities(rel.get("condition")):
          left_index, right_index = sorted((first, second))
          if not (left_index < split <= right_index):
              # Both operands on one side is not a join key; it is a filter.
              return []
          left_name = _column_at(left_index, names, exprs)
          right_name = _column_at(right_index, names, exprs)
          if left_name is None or right_name is None:
              return []
          pairs.append((left_name, right_name))
      return pairs


  def side_table_and_fields(
      rel_id: str, by_id: dict[str, dict[str, Any]], previous: dict[str, str | None]
  ) -> tuple[str, list[str], list[Any]] | None:
      """One side's table, its top project's fields and that project's exprs.

      A scan carries no fields at all: the names come from the LogicalProject
      immediately above it, reached through the pass-through exchange. The
      walk stops at the first node type not on the pass-through list, and a
      side reaching anything but exactly one scan yields nothing.
      """
      fields: list[str] | None = None
      exprs: list[Any] = []
      current: str | None = rel_id
      for _ in range(_MAX_DEPTH):
          if current is None:
              return None
          rel = by_id.get(current)
          if rel is None:
              return None
          suffix = _suffix(rel)
          if suffix == "pinotlogicaltablescan":
              table = rel.get("table")
              if not isinstance(table, list) or not table:
                  return None
              parts = [part for part in table if isinstance(part, str) and part]
              if len(parts) != len(table) or fields is None:
                  return None
              return (".".join(parts).lower(), fields, exprs)
          if suffix not in _PASS_THROUGH:
              return None
          if suffix == "logicalproject" and fields is None:
              names = rel.get("fields")
              if not isinstance(names, list) or not all(
                  isinstance(name, str) for name in names
              ):
                  return None
              fields = [name for name in names if isinstance(name, str)]
              raw = rel.get("exprs")
              exprs = list(raw) if isinstance(raw, list) else []
          children = _children(current, rel, previous)
          if children is None or len(children) != 1:
              return None
          current = children[0]
      return None


  def _equalities(condition: Any) -> list[tuple[int, int]]:
      """Operand index pairs of every usable EQUALS in this join condition.

      Only a top-level EQUALS, or one directly under a top-level AND. An OR
      anywhere, a NOT, or any other kind yields nothing at all — an OR of
      equalities is not a key, and that is ADR 0008's measured case.
      """
      if not isinstance(condition, dict):
          return []
      kind = _op_kind(condition)
      if kind == "EQUALS":
          pair = _operand_indexes(condition)
          return [pair] if pair else []
      if kind != "AND":
          return []
      operands = condition.get("operands")
      if not isinstance(operands, list):
          return []
      pairs: list[tuple[int, int]] = []
      for operand in operands:
          if not isinstance(operand, dict) or _op_kind(operand) != "EQUALS":
              return []
          pair = _operand_indexes(operand)
          if pair is None:
              return []
          pairs.append(pair)
      return pairs


  def _op_kind(node: Any) -> str | None:
      if not isinstance(node, dict):
          return None
      op = node.get("op")
      if not isinstance(op, dict):
          return None
      kind = op.get("kind")
      return kind if isinstance(kind, str) else None


  def _operand_indexes(node: dict[str, Any]) -> tuple[int, int] | None:
      """The two bare `input` indexes of an EQUALS, or None for anything else."""
      operands = node.get("operands")
      if not isinstance(operands, list) or len(operands) != 2:
          return None
      indexes: list[int] = []
      for operand in operands:
          if not isinstance(operand, dict):
              return None
          index = operand.get("input")
          if isinstance(index, bool) or not isinstance(index, int) or index < 0:
              return None
          indexes.append(index)
      return (indexes[0], indexes[1])


  def _column_at(index: int, names: list[str], exprs: list[Any]) -> str | None:
      """The column name at this global field index, or None if it is not one.

      A $f-prefixed name and an exprs entry carrying an op rather than a bare
      input both mark a synthesised field: upper(a.Carrier) became $f85 with an
      op of UPPER. Either marker disqualifies the operand.
      """
      if index >= len(names):
          return None
      name = names[index]
      if not name or name.startswith(_SYNTHETIC_FIELD_PREFIX):
          return None
      if index < len(exprs):
          expr = exprs[index]
          if not isinstance(expr, dict) or "op" in expr or "input" not in expr:
              return None
      return name.lower()


  def _suffix(rel: dict[str, Any]) -> str:
      """The bare class name of a node's relOp, lowercase."""
      rel_op = rel.get("relOp")
      if not isinstance(rel_op, str):
          return ""
      return rel_op.rsplit(".", 1)[-1].lower()
  ```
  `_is_join` and `_is_union` keep their bodies; `_suffix` is available to them but
  rewriting them is not part of this task.

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`
  Every shipped `test_pinot_plan.py` test must pass unchanged: the default `unique_keys`
  is empty, `_covered_sides` returns `(False, False)` and `_join_rows` falls through to
  the product plus the sum.

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/plan.py \
          server/tests/adapters/pinot/test_pinot_plan.py \
          server/tests/adapters/pinot/fixtures/explain-mse-join-plain.json \
          server/tests/adapters/pinot/fixtures/explain-mse-join-twokeys.json \
          server/tests/adapters/pinot/fixtures/explain-mse-join-expr.json \
          server/tests/adapters/pinot/fixtures/explain-mse-join-left.json
  git commit -m "feat(pinot): charge a join its bound when the catalog proves the key

  A join equating a column set the catalog proves unique matches at most one
  row per key value, so it is charged the other side plus both inputs rather
  than the product. Both sides proven takes the smaller. No evidence is still
  the product, which is the rule and not the exception: this shrinks a quote
  only on positive catalog evidence, never on a SQL shape, which is the thing
  ADR 0004 rejected.

  Resolving an operand to a column name is where the care is. The index
  counts over left fields ++ right fields — the two-key plan proves it, with
  right teamID at right-index 1 landing on global 3 — the names come from the
  LogicalProject above a scan that carries none, the right-hand list is
  alphabetised so only the index may be read, and a side is evidence only
  when it is a straight chain to exactly one scan. An expression operand, an
  OR, an out-of-range index or a side carrying a second join yields no pairs
  at all, and no pairs is the product.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 6: `engine.py` — two more documents per table, and the composition

**Files:** `server/src/lagaam/adapters/pinot/engine.py`,
`server/tests/adapters/pinot/test_pinot_engine.py`.

**Interfaces:**
- Consumes: the same validated SQL; no port change.
- Produces: `estimate_cost` unchanged in signature, with three additions inside —
  - `_table_facts` fetches `GET /tables/{t}/externalview` for **every** table, and
    `GET /schemas/{schemaName}` plus `GET /tables/{t}/metadata` **only** when the config
    carries an `upsertConfig`. `schemaName` is the config's `segmentsConfig.schemaName`
    when present and **the resolved listing spelling** otherwise — never the config's own
    `tableName`, which comes back suffixed (`"u12upsert_REALTIME"`) and would 404. Every
    new path goes through `PinotClient.path_part`, and `PinotForbidden` /
    `PinotTransportError` are handled exactly as at the existing sites: the forbidden
    raises `EngineError(_CREDENTIALS_REFUSED)` out of `estimate_cost`'s existing `except`,
    and a transport failure degrades the whole quotation to `confidence="low"`.
  - `_surviving` returns the **sealed** count: `surviving_segments(...)` minus
    `consuming_segments_queried(...)`, floored at 0, both read from the one EXPLAIN body
    it already has. It returns `None` exactly where it does today (not one table, no
    oracle, a transport failure), and `None` still means "charge every sealed segment".
  - `_widest_rows` builds `unique_keys` keyed lowercase `database.table` — the same key
    `leaf_docs` uses — from each table's `TableFacts.unique_keys`, and passes it to
    `max_intermediate_rows`.
- Two new private helpers:
  - `def _schema_name(config_json: Any, resolved: str) -> str` — the config's
    `segmentsConfig.schemaName` from either half when it is a non-empty string, else
    `resolved`.
  - `def _has_upsert(config_json: Any) -> bool` — reuses `metadata._has_upsert_config`
    through a public re-export; add `upsert_config_present` to `metadata.py` as the public
    name (`def upsert_config_present(config_json: Any) -> bool: return _has_upsert_config(config_json)`)
    so `engine.py` imports no underscore.

Cost: a pure-OFFLINE non-upsert table pays **one** extra controller call per quotation
(externalview), an upsert table three (externalview, schema, table metadata). The
`/tables/{t}` config is already fetched and is what decides which.

- [ ] **Step 1 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_engine.py`,
  in that module's existing `MockTransport` style:
  ```python
  def _realtime_routes(request: httpx.Request) -> httpx.Response:
      """The realtime airlineStats table, from the U12 captures."""
      path = request.url.path
      if path == "/query/sql":
          sql = json.loads(request.content)["sql"]
          if "AS JSON" in sql:
              return httpx.Response(200, json=load("explain-mse-singletable.json"))
          return httpx.Response(200, json=load("explain-v1-realtime-nofilter.json"))
      if path == "/tables":
          return httpx.Response(200, json={"tables": ["airlineStats"]})
      if path == "/tables/airlineStats/externalview":
          return httpx.Response(200, json=load("externalview-airlineStats-realtime.json"))
      if path == "/tables/airlineStats/size":
          return httpx.Response(200, json=load("size-airlineStats-realtime.json"))
      if path == "/tables/airlineStats/schema":
          return httpx.Response(200, json=load("schema-airlineStats.json"))
      if path == "/segments/airlineStats/metadata":
          return httpx.Response(
              200, json=load("seg-metadata-airlineStats-realtime-columns.json")
          )
      if path == "/tables/airlineStats":
          return httpx.Response(200, json=load("tableconfig-airlineStats-realtime.json"))
      return httpx.Response(404, json={})


  async def test_a_realtime_table_is_quoted_with_its_consuming_segment_charged(
  ) -> None:
      """6 sealed x 100 docs + 1 consuming x 100 threshold; bytes 548 + 114."""
      engine = PinotEngine(transport=httpx.MockTransport(_realtime_routes))
      estimate = await engine.estimate_cost(
          "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats LIMIT 10"
      )
      assert estimate.row_estimate == 700
      assert estimate.scanned_bytes == 548 + 114
      assert estimate.confidence == "high"


  async def test_the_sealed_k_is_queried_minus_the_consuming_counter() -> None:
      """The EXPLAIN reports 7 queried, 1 consuming, 6 pruned by server: k is 1
      sealed segment charged at 100 docs, plus the 100-row consuming charge."""
      def routes(request: httpx.Request) -> httpx.Response:
          if request.url.path == "/query/sql":
              sql = json.loads(request.content)["sql"]
              if "AS JSON" in sql:
                  return httpx.Response(200, json=load("explain-mse-singletable.json"))
              return httpx.Response(
                  200, json=load("explain-v1-realtime-futuretime.json")
              )
          return _realtime_routes(request)

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch > 99999 LIMIT 10"
      )
      assert estimate.row_estimate == 100 + 100
      assert estimate.confidence == "high"


  async def test_incomplete_segment_metadata_quotes_low() -> None:
      """The two-of-four u12upsert capture: a sum over half a table is no quote."""
      def routes(request: httpx.Request) -> httpx.Response:
          path = request.url.path
          if path == "/query/sql":
              return httpx.Response(200, json=load("explain-v1-realtime-nofilter.json"))
          if path == "/tables":
              return httpx.Response(200, json={"tables": ["u12upsert"]})
          if path == "/tables/u12upsert/externalview":
              return httpx.Response(200, json={"REALTIME": {}})
          if path == "/tables/u12upsert/size":
              return httpx.Response(200, json=load("size-u12upsert.json"))
          if path == "/tables/u12upsert/metadata":
              return httpx.Response(200, json=load("metadata-u12upsert.json"))
          if path == "/schemas/u12upsert":
              return httpx.Response(200, json=load("schema-u12upsert.json"))
          if path == "/tables/u12upsert/schema":
              return httpx.Response(200, json=load("schema-u12upsert.json"))
          if path == "/segments/u12upsert/metadata":
              return httpx.Response(200, json=load("seg-metadata-u12upsert.json"))
          if path == "/tables/u12upsert":
              return httpx.Response(200, json=load("tableconfig-u12upsert.json"))
          return httpx.Response(404, json={})

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT pk FROM pinot.default.u12upsert LIMIT 10"
      )
      assert estimate.row_estimate is None
      assert estimate.scanned_bytes is None
      assert estimate.confidence == "low"


  async def test_the_schema_is_fetched_only_for_an_upsert_table() -> None:
      """A pure-OFFLINE table pays one extra call, not three."""
      seen: list[str] = []

      def routes(request: httpx.Request) -> httpx.Response:
          seen.append(request.url.path)
          return _realtime_routes(request)

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      )
      assert "/tables/airlineStats/externalview" in seen
      assert not any(path.startswith("/schemas/") for path in seen)


  async def test_an_upsert_self_join_is_admitted_as_the_other_side_plus_inputs() -> None:
      """The same fixtures, quoted with and without the PK evidence."""
      def routes(request: httpx.Request) -> httpx.Response:
          path = request.url.path
          if path == "/query/sql":
              sql = json.loads(request.content)["sql"]
              if "AS JSON" in sql:
                  return httpx.Response(200, json=load("explain-mse-join-plain.json"))
              return httpx.Response(200, json=load("explain-v1-realtime-nofilter.json"))
          if path == "/tables":
              return httpx.Response(
                  200, json={"tables": ["airlineStats", "baseballStats"]}
              )
          if path.endswith("/externalview"):
              return httpx.Response(200, json={"REALTIME": None, "OFFLINE": None})
          if path.startswith("/schemas/"):
              return httpx.Response(200, json=load("schema-u12upsert.json"))
          if "baseballStats" in path:
              return _baseball_routes(request)
          return _airline_routes(request)

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT a.Carrier FROM pinot.default.airlineStats a "
          "JOIN pinot.default.baseballStats b ON a.Carrier = b.teamID LIMIT 10"
      )
      assert estimate.max_intermediate_rows == 9746 * 97889 + 9746 + 97889
  ```
  `_airline_routes` and `_baseball_routes` are the existing per-table route helpers in that
  module; if they are inlined there today, extract them first in this step with no change
  of behaviour. The last test asserts the **product**, because `airlineStats` carries no
  `upsertConfig` — it is the non-upsert twin of the pair, and the admitted case is the
  integration test of Task 8, which has a real upsert table to point at. Add the bounded
  half here only if a hand-built `TableFacts` can be routed through `MockTransport`
  without inventing a fixture; it cannot, so the bound is proved in `test_pinot_plan.py`
  (Task 5) and live in Task 8.

- [ ] **Step 2 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_engine.py`
  Expect `assert 600 == 700` from
  `test_a_realtime_table_is_quoted_with_its_consuming_segment_charged`: without the
  externalview fetch `consuming` is 0 and the consuming charge is missing.

- [ ] **Step 3 — implementation.** In `server/src/lagaam/adapters/pinot/engine.py`, extend
  the metadata import with `upsert_config_present`, the response import with
  `consuming_segments_queried`, and add the two module-level helpers after `_plan_cell`:
  ```python
  def _schema_name(config_json: Any, resolved: str) -> str:
      """The schema document's own name for this table.

      segmentsConfig.schemaName was absent on all four configs captured, so the
      fallback is the path that normally runs — and it has to be the resolved
      listing spelling, never the config's own tableName, which comes back
      suffixed ("u12upsert_REALTIME") and would 404 as a schema path.
      """
      if isinstance(config_json, dict):
          for key in ("REALTIME", "OFFLINE"):
              half = config_json.get(key)
              if not isinstance(half, dict):
                  continue
              segments_config = half.get("segmentsConfig")
              if not isinstance(segments_config, dict):
                  continue
              name = segments_config.get("schemaName")
              if isinstance(name, str) and name:
                  return name
      return resolved
  ```
  Replace the tail of `_table_facts`, from the `config_json` fetch onward:
  ```python
          config_json = await self._client.controller_get(
              f"/tables/{part}", database=database
          )
          config = None if config_json is PinotClient.NotFound else config_json
          seg_json = await self._client.controller_get(
              f"/segments/{part}/metadata", params=params, database=database
          )
          size_json = await self._client.controller_get(
              f"/tables/{part}/size", database=database
          )
          externalview_json = await self._client.controller_get(
              f"/tables/{part}/externalview", database=database
          )
          schema_json: Any = None
          table_metadata_json: Any = None
          if upsert_config_present(config):
              # Two more documents, only where the config says they say something:
              # primaryKeyColumns lives on the schema, and the PK count map on the
              # table metadata is the second document that has to agree.
              resolved_name = _spelled(self.CATALOG, database, table, listings[database])
              schema_part = PinotClient.path_part(_schema_name(config, resolved_name))
              schema_body = await self._client.controller_get(
                  f"/schemas/{schema_part}", database=database
              )
              schema_json = None if schema_body is PinotClient.NotFound else schema_body
              metadata_body = await self._client.controller_get(
                  f"/tables/{part}/metadata", database=database
              )
              table_metadata_json = (
                  None if metadata_body is PinotClient.NotFound else metadata_body
              )
          return table_facts(
              table,
              config,
              None if seg_json is PinotClient.NotFound else seg_json,
              None if size_json is PinotClient.NotFound else size_json,
              frozenset(resolved),
              externalview_json=(
                  None
                  if externalview_json is PinotClient.NotFound
                  else externalview_json
              ),
              schema_json=schema_json,
              table_metadata_json=table_metadata_json,
          )
  ```
  `PinotClient.path_part` raising `ValueError` on the schema name is the same case the
  method already handles for the table name, and is caught by the existing guard at the
  top of `_table_facts`: a schema name the config supplies that no URL path can carry is a
  `TableNotFoundError`, exactly as the table name is. Wrap the `path_part(_schema_name(...))`
  call in the same `try/except ValueError` and raise the same error.

  Replace `_surviving`'s return:
  ```python
          sealed = surviving_segments(body, trust_limit_prune=trust_limit_prune)
          if sealed is None:
              return None
          # numSegmentsQueried includes the consuming segments, so the k that
          # applies to sealed ones is what is left after subtracting them. The
          # consuming charge is added by quote.py, outside this k entirely.
          return max(0, sealed - consuming_segments_queried(body))
  ```
  And in `_widest_rows`, build the key evidence beside the leaf sizes:
  ```python
          leaves: dict[str, int | None] = {}
          keys: dict[str, frozenset[frozenset[str]]] = {}
          for database, facts, surviving in tables:
              name = f"{database}.{facts.table}".lower()
              leaves[name] = surviving_docs(facts, surviving)
              if facts.unique_keys:
                  keys[name] = facts.unique_keys
  ```
  and pass it through: `return max_intermediate_rows(cell, leaves, keys)`.

  Finally, in `metadata.py`, add the public re-export beside `upsert_keys`:
  ```python
  def upsert_config_present(config_json: Any) -> bool:
      """Does either half of this table config carry an upsertConfig object?

      The caller fetches two more documents on the strength of this, so it is
      public: an adapter that guessed would pay two controller calls per table.
      """
      return _has_upsert_config(config_json)
  ```

- [ ] **Step 4 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`
  Every shipped `test_pinot_engine.py` test must pass: the OFFLINE route helpers return
  404 for `/tables/{t}/externalview`, which `controller_get` turns into `NotFound` and
  `consuming_count` reads as 0.

- [ ] **Step 5 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/engine.py \
          server/src/lagaam/adapters/pinot/metadata.py \
          server/tests/adapters/pinot/test_pinot_engine.py
  git commit -m "feat(pinot): compose the realtime charge and the key evidence in estimate_cost

  Two more controller documents per table, both cheap and one conditional:
  externalview always, because it is the only endpoint that tells CONSUMING
  from ONLINE; the schema and the table metadata only where the table config
  carries an upsertConfig, because that is the only case in which they say
  anything. The schema path is the controller's own listing spelling — the
  config's tableName comes back suffixed and would 404 — and every new path
  goes through path_part like the others.

  The sealed k is numSegmentsQueried minus numConsumingSegmentsQueried from
  the one EXPLAIN body the oracle already issues, floored at 0, so the
  k-largest charge applies to sealed segments and the consuming charge is
  added outside it. The unique keys are keyed the way leaf_docs already is,
  lowercase database.table, and go to the plan walk.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 7: The `pinot-realtime` profile, its bootstrap and its readiness fixture

**Files:** `examples/docker-compose.yml`, `examples/pinot-realtime/bootstrap.sh`,
`examples/pinot-realtime/{airlineStats-schema.json,airlineStats-table.json,u12upsert-schema.json,u12upsert-table.json,u12plain-schema.json,u12plain-table.json}`,
`server/tests/integration/conftest.py`.

**Interfaces:**
- Consumes: nothing in Python. This task is environment.
- Produces:
  - `docker compose --profile pinot-realtime up -d` brings up `u12-kafka` and `u12-pinot`
    with the controller on host **:9001** and the broker on host **:8001**, beside the
    existing batch profile on 9000/8000.
  - `examples/pinot-realtime/bootstrap.sh` — idempotent: a table that already exists is
    left alone, and the script may be re-run.
  - `def pinot_realtime_ready() -> None` in `server/tests/integration/conftest.py`, beside
    `pinot_ready` and built the same way: skip (never fail) when :9001 is unreachable,
    then poll until the realtime `airlineStats` answers a **positive** `count(*)` **and**
    its externalview carries at least one `ONLINE` segment **and** `u12upsert` answers.
    All three: a table that answers a count may still be all-consuming, and a quote against
    an all-consuming table exercises the `None` path rather than the charge.

**Memory** (log §8, `05-docker-stats.txt`): `u12-pinot` 1.16 GiB, `u12-kafka` 385.5 MiB,
`lagaam-pinot` 1.856 GiB, `lagaam-trino` 3.024 GiB — ≈6.4 GiB against Docker's 7.653 GiB
ceiling. `-Xmx2G` is not optional, and the profile must not assume the Trino profile is
down.

- [ ] **Step 1 — the compose profile.** Append to `examples/docker-compose.yml`, after the
  `pinot` service:
  ```yaml
  # The STREAM instance, for the U12 realtime and upsert tests. Deliberately not
  # on 9000/8000: the OFFLINE suite needs the batch profile up at the same time,
  # and both Pinots plus Kafka and Trino fit in 7.653 GiB only at ~6.4 GiB, so
  # the heap cap is not optional. Bootstrap the tables afterwards:
  #   docker compose --profile pinot-realtime up -d && examples/pinot-realtime/bootstrap.sh
  u12-kafka:
    image: apache/kafka:3.9.0
    container_name: u12-kafka
    hostname: u12-kafka
    profiles: ["pinot-realtime"]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: "broker,controller"
      # The hostname, never 0.0.0.0: the entrypoint copies this into
      # advertised.listeners during storage format and dies on the meta-address.
      KAFKA_LISTENERS: "PLAINTEXT://u12-kafka:9092,CONTROLLER://u12-kafka:9093"
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://u12-kafka:9092"
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT"
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@u12-kafka:9093"
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      CLUSTER_ID: "u12ClusterIdXXXXXXXXXX"
    healthcheck:
      test:
        - "CMD"
        - "/opt/kafka/bin/kafka-topics.sh"
        - "--bootstrap-server"
        - "u12-kafka:9092"
        - "--list"
      interval: 5s
      timeout: 5s
      retries: 30
  u12-pinot:
    image: apachepinot/pinot:release-1.5.1
    container_name: u12-pinot
    profiles: ["pinot-realtime"]
    # A Pinot that starts before Kafka is reachable falls back to its own
    # embedded broker, and then no table this profile creates ever ingests.
    depends_on:
      u12-kafka:
        condition: service_healthy
    command:
      ["QuickStart", "-type", "STREAM", "-kafkaBrokerList", "u12-kafka:9092"]
    environment:
      # The STREAM quickstart runs 4 servers, a broker, a controller and a
      # minion in one container and peaks near 1.2 GiB with this cap.
      JAVA_OPTS: "-Xms512M -Xmx2G -XX:+UseG1GC"
    ports:
      - "9001:9000"
      - "8001:8000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
      interval: 5s
      timeout: 3s
      retries: 60
  ```

- [ ] **Step 2 — the table documents.** Create the six files under
  `examples/pinot-realtime/`, verbatim from the spike's POSTed bodies. The realtime
  `airlineStats` schema is extracted from the image rather than transcribed, because it is
  84 columns:
  ```bash
  mkdir -p examples/pinot-realtime
  docker exec u12-pinot cat \
    /opt/pinot/examples/stream/airlineStats/airlineStats_schema.json \
    > examples/pinot-realtime/airlineStats-schema.json
  python3 -c "
  import json, pathlib
  p = pathlib.Path('examples/pinot-realtime/airlineStats-schema.json')
  s = json.loads(p.read_text())
  print(s['schemaName'], len(s['dimensionFieldSpecs']), len(s['dateTimeFieldSpecs']))"
  ```
  Expect `airlineStats` and a non-zero column count. Then, by hand:

  `examples/pinot-realtime/airlineStats-table.json` — the bundled config with the broker
  list pointed at the compose service, `stream.kafka.zk.broker.url` removed, and the
  deprecated `.threshold.size` replaced by `.threshold.rows: "100"` so a segment seals
  every 100 rows and the tests have sealed segments in seconds rather than at 50,000:
  ```json
  {
    "tableName": "airlineStats",
    "tableType": "REALTIME",
    "tenants": {},
    "segmentsConfig": {
      "timeColumnName": "DaysSinceEpoch",
      "retentionTimeUnit": "DAYS",
      "retentionTimeValue": "5",
      "replication": "1"
    },
    "tableIndexConfig": {},
    "routing": {"segmentPrunerTypes": ["time"]},
    "ingestionConfig": {
      "streamIngestionConfig": {
        "streamConfigMaps": [
          {
            "streamType": "kafka",
            "stream.kafka.topic.name": "flights-realtime",
            "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
            "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
            "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
            "stream.kafka.broker.list": "u12-kafka:9092",
            "realtime.segment.flush.threshold.time": "24h",
            "realtime.segment.flush.threshold.rows": "100"
          }
        ]
      },
      "transformConfigs": [
        {"columnName": "ts", "transformFunction": "fromEpochDays(DaysSinceEpoch)"},
        {"columnName": "tsRaw", "transformFunction": "fromEpochDays(DaysSinceEpoch)"}
      ]
    },
    "fieldConfigList": [
      {
        "name": "ts",
        "encodingType": "DICTIONARY",
        "indexTypes": ["TIMESTAMP"],
        "timestampConfig": {"granularities": ["DAY", "WEEK", "MONTH"]}
      }
    ],
    "metadata": {"customConfigs": {}}
  }
  ```
  `examples/pinot-realtime/u12upsert-schema.json`:
  ```json
  {
    "schemaName": "u12upsert",
    "primaryKeyColumns": ["pk"],
    "dimensionFieldSpecs": [
      {"name": "pk", "dataType": "STRING"},
      {"name": "label", "dataType": "STRING"}
    ],
    "metricFieldSpecs": [{"name": "val", "dataType": "INT"}],
    "dateTimeFieldSpecs": [
      {
        "name": "ts",
        "dataType": "LONG",
        "format": "1:MILLISECONDS:EPOCH",
        "granularity": "1:MILLISECONDS"
      }
    ]
  }
  ```
  `examples/pinot-realtime/u12upsert-table.json`:
  ```json
  {
    "tableName": "u12upsert",
    "tableType": "REALTIME",
    "tenants": {},
    "segmentsConfig": {"timeColumnName": "ts", "replication": "1"},
    "tableIndexConfig": {"nullHandlingEnabled": false},
    "upsertConfig": {"mode": "FULL"},
    "routing": {"instanceSelectorType": "strictReplicaGroup"},
    "ingestionConfig": {
      "streamIngestionConfig": {
        "streamConfigMaps": [
          {
            "streamType": "kafka",
            "stream.kafka.topic.name": "u12-upsert",
            "stream.kafka.decoder.class.name": "org.apache.pinot.plugin.stream.kafka.KafkaJSONMessageDecoder",
            "stream.kafka.consumer.factory.class.name": "org.apache.pinot.plugin.stream.kafka30.KafkaConsumerFactory",
            "stream.kafka.consumer.prop.auto.offset.reset": "smallest",
            "stream.kafka.broker.list": "u12-kafka:9092",
            "realtime.segment.flush.threshold.rows": "200",
            "realtime.segment.flush.threshold.time": "24h"
          }
        ]
      }
    },
    "metadata": {"customConfigs": {}}
  }
  ```
  `examples/pinot-realtime/u12plain-schema.json` is `u12upsert-schema.json` with
  `"schemaName": "u12plain"` and **no** `primaryKeyColumns` key.
  `examples/pinot-realtime/u12plain-table.json` is `u12upsert-table.json` with
  `"tableName": "u12plain"`, **no** `upsertConfig` and **no** `routing` — the same topic,
  the same threshold, the same rows, so the only difference between the pair is the
  evidence. `tenants: {}` is required on every POSTed config: 400 otherwise.

- [ ] **Step 3 — the bootstrap script.** Create `examples/pinot-realtime/bootstrap.sh`,
  `chmod +x`:
  ```bash
  #!/usr/bin/env bash
  # Bootstrap the pinot-realtime profile: two topics, three tables, sample rows.
  #
  #   docker compose --profile pinot-realtime up -d
  #   examples/pinot-realtime/bootstrap.sh
  #
  # Idempotent: a topic or table that already exists is left alone, so a re-run
  # after a partial failure finishes the job rather than doubling the data.
  set -euo pipefail

  CONTROLLER="${PINOT_REALTIME_CONTROLLER:-http://localhost:9001}"
  KAFKA="${U12_KAFKA_CONTAINER:-u12-kafka}"
  PINOT="${U12_PINOT_CONTAINER:-u12-pinot}"
  HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  ROWS="${U12_AIRLINE_ROWS:-600}"

  wait_for_controller() {
    for _ in $(seq 1 60); do
      if curl -sf "${CONTROLLER}/health" >/dev/null; then return 0; fi
      sleep 2
    done
    echo "controller ${CONTROLLER} never answered /health" >&2
    exit 1
  }

  create_topic() {
    docker exec "${KAFKA}" /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server "${KAFKA}:9092" --create --if-not-exists \
      --topic "$1" --partitions "$2" --replication-factor 1
  }

  table_exists() {
    curl -sf "${CONTROLLER}/tables/$1" | grep -q '"tableName"'
  }

  post_table() {
    local name="$1" schema="$2" table="$3"
    if table_exists "${name}"; then
      echo "table ${name} exists — left alone"
      return 0
    fi
    curl -sf -X POST "${CONTROLLER}/schemas" \
      -H 'Content-Type: application/json' --data-binary "@${HERE}/${schema}" >/dev/null
    curl -sf -X POST "${CONTROLLER}/tables" \
      -H 'Content-Type: application/json' --data-binary "@${HERE}/${table}" >/dev/null
    echo "table ${name} created"
  }

  wait_for_controller
  create_topic flights-realtime 1
  create_topic u12-upsert 2

  post_table airlineStats airlineStats-schema.json airlineStats-table.json
  post_table u12upsert    u12upsert-schema.json    u12upsert-table.json
  post_table u12plain     u12plain-schema.json     u12plain-table.json

  # The flight feed: the image's own sample data, capped so the table seals a
  # predictable number of 100-row segments and still leaves one consuming.
  docker exec "${PINOT}" bash -lc \
    "head -${ROWS} /opt/pinot/examples/stream/airlineStats/rawdata/airlineStats_data.json \
       > /tmp/u12_feed.json"
  docker cp "${PINOT}:/tmp/u12_feed.json" /tmp/u12_feed.json
  docker cp /tmp/u12_feed.json "${KAFKA}:/tmp/u12_feed.json"
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA}:9092 \
       --topic flights-realtime < /tmp/u12_feed.json"

  # The upsert feed: 100 distinct keys x 6 versions, keyed by pk so both
  # partitions get whole keys and the upsert view is the last version of each.
  python3 - > /tmp/u12up_feed.txt <<'PY'
  import json, time
  now = int(time.time() * 1000)
  for version in range(6):
      for key in range(100):
          pk = f"key{key:03d}"
          row = {"pk": pk, "label": f"v{version}", "val": version, "ts": now + version}
          print(f"{pk}\t{json.dumps(row)}")
  PY
  docker cp /tmp/u12up_feed.txt "${KAFKA}:/tmp/u12up_feed.txt"
  docker exec "${KAFKA}" bash -lc \
    "/opt/kafka/bin/kafka-console-producer.sh --bootstrap-server ${KAFKA}:9092 \
       --topic u12-upsert --property parse.key=true --property key.separator=\$'\t' \
       < /tmp/u12up_feed.txt"

  echo "bootstrapped — controller ${CONTROLLER}, broker http://localhost:8001"
  ```
  Run it and confirm the numbers the tests will assert (log §1.3, §2.1):
  ```bash
  docker compose --profile pinot-realtime -f examples/docker-compose.yml up -d
  examples/pinot-realtime/bootstrap.sh
  sleep 30
  curl -s -X POST http://localhost:8001/query/sql -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT count(*) FROM airlineStats"}' | python3 -c "import json,sys; print('airlineStats', json.load(sys.stdin)['resultTable']['rows'])"
  curl -s -X POST http://localhost:8001/query/sql -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT count(*) FROM u12upsert"}'   | python3 -c "import json,sys; print('u12upsert', json.load(sys.stdin)['resultTable']['rows'])"
  curl -s -X POST http://localhost:8001/query/sql -H 'Content-Type: application/json' \
    -d '{"sql":"SELECT count(*) FROM u12plain"}'    | python3 -c "import json,sys; print('u12plain', json.load(sys.stdin)['resultTable']['rows'])"
  curl -s http://localhost:9001/tables/airlineStats/externalview \
    | python3 -c "import json,sys; ev=json.load(sys.stdin)['REALTIME']; s=[x for m in ev.values() for x in m.values()]; print('ONLINE', s.count('ONLINE'), 'CONSUMING', s.count('CONSUMING'))"
  ```
  Expect `airlineStats [[600]]`, `u12upsert [[100]]` (the distinct-key count),
  `u12plain [[600]]` (every version), and at least one `ONLINE` beside the `CONSUMING`
  ones. `u12upsert` at 600 rather than 100 means the upsert config did not take — check
  `routing.instanceSelectorType` and re-create the table.

- [ ] **Step 4 — the readiness fixture.** Append to `server/tests/integration/conftest.py`:
  ```python
  _PINOT_REALTIME_CONTROLLER = "http://localhost:9001"
  _PINOT_REALTIME_BROKER = "http://localhost:8001"


  @pytest.fixture
  def pinot_realtime_ready() -> None:
      """Skip (don't fail) when the STREAM instance isn't up and ingesting.

      Three conditions, because two of them can be true of a useless table: a
      table answering a positive count may still be entirely consuming, and a
      quote against an all-consuming table exercises the None path rather than
      the charge these tests exist to prove. So a sealed segment is required
      too, and the upsert table has to answer at all.
      """
      try:
          httpx.get(
              f"{_PINOT_REALTIME_CONTROLLER}/health", timeout=2.0
          ).raise_for_status()
      except httpx.HTTPError:
          pytest.skip(
              "Pinot STREAM instance not reachable — docker compose "
              "--profile pinot-realtime up -d && examples/pinot-realtime/bootstrap.sh"
          )

      deadline = time.monotonic() + 300
      while True:
          try:
              ready = (
                  _pinot_realtime_answers("airlineStats")
                  and _pinot_realtime_answers("u12upsert")
                  and _pinot_has_sealed_segment("airlineStats")
              )
          except httpx.HTTPError:
              ready = False
          if ready:
              return
          if time.monotonic() > deadline:
              pytest.skip(
                  "the STREAM instance never had a sealed airlineStats segment "
                  "and a queryable u12upsert within 300s — run "
                  "examples/pinot-realtime/bootstrap.sh"
              )
          time.sleep(2)


  def _pinot_realtime_answers(table: str) -> bool:
      """Is this table on the STREAM broker answering a positive count?"""
      response = httpx.post(
          f"{_PINOT_REALTIME_BROKER}/query/sql",
          json={
              "sql": f"SELECT count(*) AS n FROM {table} LIMIT 1",
              "queryOptions": "useMultistageEngine=true",
          },
          timeout=10.0,
      )
      response.raise_for_status()
      body = response.json()
      if body.get("exceptions"):
          return False
      rows = (body.get("resultTable") or {}).get("rows") or []
      if not rows or not isinstance(rows[0], list) or not rows[0]:
          return False
      count = rows[0][0]
      if isinstance(count, bool) or not isinstance(count, int):
          return False
      return count > 0


  def _pinot_has_sealed_segment(table: str) -> bool:
      """Has at least one segment sealed? Only externalview carries the state."""
      response = httpx.get(
          f"{_PINOT_REALTIME_CONTROLLER}/tables/{table}/externalview", timeout=10.0
      )
      response.raise_for_status()
      half = response.json().get("REALTIME")
      if not isinstance(half, dict):
          return False
      return any(
          "ONLINE" in states.values()
          for states in half.values()
          if isinstance(states, dict)
      )
  ```

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q && uv run pytest -q -m integration`
  Nothing yet uses `pinot_realtime_ready`; this step proves the fixture imports and the
  existing suites are untouched.

- [ ] **Step 6 — commit.**
  ```bash
  git add examples/docker-compose.yml examples/pinot-realtime/ \
          server/tests/integration/conftest.py
  git commit -m "test(pinot): a pinot-realtime compose profile with Kafka, upsert and its twin

  The realtime and key-evidence rules need a stream to measure against, and
  the batch quickstart has none: -type HYBRID shells out to the docker CLI
  and dies in a container, and there is no UPSERT quickstart type at all on
  1.5.1. So the profile runs apache/kafka 3.9.0 in KRaft beside a second
  Pinot on 9001/8001 — deliberately not 9000/8000, because the OFFLINE suite
  needs the batch instance up at the same time and both fit in 7.653 GiB only
  with the 2G heap cap.

  KAFKA_LISTENERS carries the hostname rather than 0.0.0.0: the entrypoint
  copies it into advertised.listeners and dies on the meta-address. Pinot
  waits on Kafka's health check, because a Pinot that starts first falls back
  to its own embedded broker and then nothing ever ingests. The bootstrap
  posts three tables — the realtime airlineStats at a 100-row flush
  threshold, an upsert table, and its byte-identical non-upsert twin on the
  same topic — and is idempotent.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 8: Integration tests against the STREAM instance

**Files:** `server/tests/integration/test_pinot_engine.py`.

**Interfaces:**
- Consumes: `pinot_realtime_ready` from Task 7, `PinotEngine` pointed at :9001/:8001.
- Produces: no source change. Every assertion is derived from the live cluster —
  `describe_table`, a live `count(*)`, the controller's own documents — and never from a
  literal transcribed out of the log, because the stream keeps sealing segments and a
  hard-coded 600 would be stale by the second run.

- [ ] **Step 1 — failing test.** Append to `server/tests/integration/test_pinot_engine.py`:
  ```python
  def _realtime_engine() -> PinotEngine:
      return PinotEngine(
          controller_url="http://localhost:9001", broker_url="http://localhost:8001"
      )


  async def _live_count(engine: PinotEngine, table: str) -> int:
      body = await engine._client.broker_query(
          f"SELECT count(*) FROM {table}", "useMultistageEngine=true"
      )
      count = body["resultTable"]["rows"][0][0]
      assert isinstance(count, int)
      return count


  async def _sealed_docs(engine: PinotEngine, table: str) -> int:
      """Docs in the sealed segments only, from the controller's own documents."""
      metadata = await engine._client.controller_get(
          f"/segments/{table}/metadata", database="default"
      )
      size = await engine._client.controller_get(
          f"/tables/{table}/size", database="default"
      )
      reported = {
          name: body["reportedSizeInBytes"]
          for name, body in (size["realtimeSegments"]["segments"] or {}).items()
      }
      return sum(
          body["totalDocs"]
          for name, body in metadata.items()
          if reported.get(name, -1) >= 0
      )


  async def test_a_realtime_table_quotes_at_high_confidence(
      pinot_realtime_ready: None,
  ) -> None:
      """The U12 demo: a table with a consuming segment is priced, not denied."""
      engine = _realtime_engine()
      estimate = await engine.estimate_cost(
          "SELECT Carrier, DaysSinceEpoch FROM pinot.default.airlineStats LIMIT 10"
      )
      assert estimate.confidence == "high"
      assert estimate.scanned_bytes is not None and estimate.scanned_bytes > 0
      assert estimate.row_estimate is not None
      assert estimate.row_estimate >= await _live_count(engine, "airlineStats")


  async def test_the_consuming_charge_is_at_least_the_rows_no_sealed_segment_holds(
      pinot_realtime_ready: None,
  ) -> None:
      """count(*) minus the sealed docs is what lives in the consuming segments."""
      engine = _realtime_engine()
      estimate = await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      )
      live = await _live_count(engine, "airlineStats")
      sealed = await _sealed_docs(engine, "airlineStats")
      assert estimate.row_estimate is not None
      assert estimate.row_estimate >= live
      assert estimate.row_estimate - sealed >= live - sealed


  async def test_a_filter_excluding_every_value_keeps_the_consuming_charge(
      pinot_realtime_ready: None,
  ) -> None:
      """Measured: the broker pruned 6 of 7 segments and the consuming counter
      stayed 1, so the charge cannot be pruned away by a time predicate."""
      engine = _realtime_engine()
      config = await engine._client.controller_get(
          "/tables/airlineStats", database="default"
      )
      threshold = int(
          config["REALTIME"]["ingestionConfig"]["streamIngestionConfig"][
              "streamConfigMaps"
          ][0]["realtime.segment.flush.threshold.rows"]
      )
      estimate = await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch > 99999 LIMIT 10"
      )
      assert estimate.row_estimate is not None
      assert estimate.row_estimate >= threshold


  async def test_a_time_filter_still_shrinks_the_sealed_k(
      pinot_realtime_ready: None,
  ) -> None:
      """The consuming charge is unconditional; the sealed charge is not."""
      engine = _realtime_engine()
      unfiltered = await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      )
      filtered = await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch > 99999 LIMIT 10"
      )
      assert unfiltered.row_estimate is not None
      assert filtered.row_estimate is not None
      assert filtered.row_estimate < unfiltered.row_estimate


  async def test_an_upsert_primary_key_self_join_is_charged_its_bound(
      pinot_realtime_ready: None,
  ) -> None:
      """Both sides unique on pk: min(left, right) + left + right, not the product."""
      engine = _realtime_engine()
      estimate = await engine.estimate_cost(
          "SELECT a.pk FROM pinot.default.u12upsert a "
          "JOIN pinot.default.u12upsert b ON a.pk = b.pk LIMIT 10"
      )
      side = await engine.estimate_cost(
          "SELECT pk FROM pinot.default.u12upsert LIMIT 10"
      )
      assert side.row_estimate is not None
      assert estimate.max_intermediate_rows == 3 * side.row_estimate
      assert estimate.max_intermediate_rows < side.row_estimate * side.row_estimate


  async def test_the_non_upsert_twin_is_still_charged_the_product(
      pinot_realtime_ready: None,
  ) -> None:
      """Same topic, same rows, same shape, no evidence — assertion 3 of the demo."""
      engine = _realtime_engine()
      estimate = await engine.estimate_cost(
          "SELECT a.pk FROM pinot.default.u12plain a "
          "JOIN pinot.default.u12plain b ON a.pk = b.pk LIMIT 10"
      )
      side = await engine.estimate_cost(
          "SELECT pk FROM pinot.default.u12plain LIMIT 10"
      )
      assert side.row_estimate is not None
      assert (
          estimate.max_intermediate_rows
          == side.row_estimate * side.row_estimate + 2 * side.row_estimate
      )


  async def test_the_upsert_bound_is_still_above_what_the_engine_builds(
      pinot_realtime_ready: None,
  ) -> None:
      """A bound, not the answer: the engine builds the distinct-key count."""
      engine = _realtime_engine()
      estimate = await engine.estimate_cost(
          "SELECT a.pk FROM pinot.default.u12upsert a "
          "JOIN pinot.default.u12upsert b ON a.pk = b.pk LIMIT 10"
      )
      built = await engine._client.broker_query(
          "SELECT count(*) FROM u12upsert a JOIN u12upsert b ON a.pk = b.pk",
          "useMultistageEngine=true",
      )
      assert estimate.max_intermediate_rows is not None
      assert estimate.max_intermediate_rows >= built["resultTable"]["rows"][0][0]


  async def test_describe_table_still_has_no_row_estimate_for_a_realtime_table(
      pinot_realtime_ready: None,
  ) -> None:
      """Grounding is untouched: numRows is a live count, not a pre-execution fact."""
      engine = _realtime_engine()
      card = await engine.describe_table("pinot", "default", "airlineStats")
      assert card.row_estimate is None
      assert card.columns
  ```
  `_sealed_docs` reads the same two documents `metadata_is_complete` compares, so a run in
  which the metadata response is truncated makes the quote low and the first two tests
  fail loudly rather than silently — which is the right failure, since the STREAM
  quickstart's `airlineStats` is single-partition and its metadata is complete.

- [ ] **Step 2 — run, expect failure.**
  `cd server && uv run pytest -q -m integration tests/integration/test_pinot_engine.py`
  Without the profile up they **skip**, which is not a pass: bring it up first
  (`docker compose --profile pinot-realtime -f ../examples/docker-compose.yml up -d` and
  `../examples/pinot-realtime/bootstrap.sh`). With it up and Tasks 1–6 unimplemented,
  expect `AssertionError` on `estimate.confidence == "high"` — the shipped code quotes a
  REALTIME half low.

- [ ] **Step 3 — run, expect PASS.**
  `cd server && uv run pytest -q -m integration`
  Both instances must be up: the OFFLINE suite on 9000/8000 is unchanged and must stay
  green in the same run.

- [ ] **Step 4 — commit.**
  ```bash
  git add server/tests/integration/test_pinot_engine.py
  git commit -m "test(pinot): the realtime charge and the key evidence against a live stream

  Every number here is derived from the cluster rather than transcribed: the
  live count, the sealed docs from the same two controller documents the
  completeness check compares, the flush threshold from the table's own
  config. The stream keeps sealing segments, so a literal from the
  measurement log would be stale by the second run.

  What they pin: a REALTIME table quotes high and above its live count; the
  consuming charge survives a filter excluding every value while a time
  filter still shrinks the sealed k; the upsert self-join is charged three
  times one side where the twin on the same topic is charged the product; and
  describe_table still carries no row estimate for a realtime table, because
  numRows is a live count and not a pre-execution fact.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 9: The e2e demo — three assertions through the MCP round-trip

**Files:** `server/tests/integration/test_e2e_mcp.py`.

**Interfaces:**
- Consumes: `pinot_realtime_ready`, the existing `lagaam_client` / `_default_budget`
  helpers in that module.
- Produces: three tests, spec decision 7. No source change.

- [ ] **Step 1 — failing test.** Append to `server/tests/integration/test_e2e_mcp.py`,
  after the existing Pinot demos:
  ```python
  _REALTIME_GRANT = AgentIdentity(
      name="lagaam-e2e", allowed_tables=frozenset({"pinot.default.airlinestats"})
  )
  _UPSERT_GRANT = AgentIdentity(
      name="lagaam-e2e", allowed_tables=frozenset({"pinot.default.u12upsert"})
  )
  _PLAIN_GRANT = AgentIdentity(
      name="lagaam-e2e", allowed_tables=frozenset({"pinot.default.u12plain"})
  )


  def _realtime_engine() -> PinotEngine:
      return PinotEngine(
          controller_url="http://localhost:9001", broker_url="http://localhost:8001"
      )


  async def test_a_query_over_freshly_landed_rows_runs_under_the_default_budget(
      pinot_realtime_ready: None,
  ) -> None:
      """U12's demo: today this is denied, because a REALTIME half quotes low."""
      async with lagaam_client(
          _realtime_engine(), budget=_default_budget(), identity=_REALTIME_GRANT
      ) as client:
          answer = await client.call_tool(
              "query_data",
              {
                  "sql": "SELECT Carrier, count(*) AS flights "
                  "FROM pinot.default.airlineStats "
                  "GROUP BY Carrier LIMIT 5"
              },
          )
          assert not answer.isError
          assert answer.structuredContent is not None
          assert answer.structuredContent["row_count"] > 0
          assert "Carrier" in answer.structuredContent["columns"]


  async def test_an_upsert_primary_key_self_join_is_admitted(
      pinot_realtime_ready: None,
  ) -> None:
      """The key is the catalog's, not the SQL's: upsertConfig plus the schema."""
      async with lagaam_client(
          _realtime_engine(), budget=_default_budget(), identity=_UPSERT_GRANT
      ) as client:
          answer = await client.call_tool(
              "query_data",
              {
                  "sql": "SELECT a.pk, b.val FROM pinot.default.u12upsert a "
                  "JOIN pinot.default.u12upsert b ON a.pk = b.pk LIMIT 5"
              },
          )
          assert not answer.isError
          assert answer.structuredContent is not None
          assert answer.structuredContent["row_count"] > 0


  async def test_the_same_join_on_the_non_upsert_twin_is_denied(
      pinot_realtime_ready: None,
  ) -> None:
      """Same topic, same rows, same shape. The evidence is doing the work."""
      async with lagaam_client(
          _realtime_engine(), budget=_default_budget(), identity=_PLAIN_GRANT
      ) as client:
          answer = await client.call_tool(
              "query_data",
              {
                  "sql": "SELECT a.pk, b.val FROM pinot.default.u12plain a "
                  "JOIN pinot.default.u12plain b ON a.pk = b.pk LIMIT 5"
              },
          )
          assert answer.isError
          text = " ".join(
              block.text for block in answer.content if hasattr(block, "text")
          )
          assert "rows at its widest step" in text
  ```
  If `u12plain`'s product lands **under** the default budget — 600 × 600 + 1,200 =
  361,200, which is below a default of 1,000,000 — the twin is admitted and assertion 3
  fails. Fix it by raising the row count rather than lowering the budget: re-run the Task 7
  bootstrap with `U12_AIRLINE_ROWS` untouched and the upsert feed widened to 300 keys × 6
  versions (1,800 rows → 3,240,000 + 3,600), and re-check with
  `SELECT count(*) FROM u12plain`. Assert the denial on the gate's own words rather than a
  number, as the shipped cross-join demo does.

- [ ] **Step 2 — run, expect PASS.**
  `cd server && uv run pytest -q -m integration tests/integration/test_e2e_mcp.py`

- [ ] **Step 3 — full suite.**
  `cd server && uv run pytest -q && uv run pytest -q -m integration && uv run mypy`

- [ ] **Step 4 — commit.**
  ```bash
  git add server/tests/integration/test_e2e_mcp.py
  git commit -m "test(pinot): the U12 demo — fresh rows run, the proved join runs, the twin does not

  Three assertions through the MCP round-trip, and the third is the one that
  matters: the same join, the same shape, the same rows from the same Kafka
  topic, denied on the table without an upsertConfig. The quotation shrinks
  on catalog evidence and on nothing else, which is what separates this from
  the SQL-shape proxy ADR 0004 rejected.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 10: ADR 0009, the ADR 0008 correction, and the spec reconciliation

**Files:** `docs/adr/0009-consuming-segments-and-proven-join-keys.md`,
`docs/adr/0008-pinot-quotation-is-adapter-synthesised.md`, `docs/adr/README.md`,
`README.md`, `docs/superpowers/specs/2026-09-17-pinot-realtime-and-key-evidence-design.md`.

**Interfaces:** documentation only. No source, no test.

- [ ] **Step 1 — the ADR.** Create
  `docs/adr/0009-consuming-segments-and-proven-join-keys.md`, in the Context / Decision /
  Consequences shape 0004 and 0008 use:
  ```markdown
  # 0009 — A consuming segment is charged at its flush threshold, and a join key must be proved by the catalog

  **Status:** Accepted

  ## Context

  ADR 0008 left two shapes denied, both honestly, and both Pinot's signature
  use.

  A REALTIME half was quoted `"low"` unconditionally. It had to be: a
  consuming segment reports `totalDocs: 0` and `reportedSizeInBytes: -1`, and
  charging those as written would quote the freshest data in the table free.
  Measured, ten probes over 30 s while the segment demonstrably held 50 live
  docs never moved either number. So a query over rows that landed seconds
  ago — the reason to run Pinot — could not be priced, and could not be
  admitted.

  Every join was the product, because ADR 0008 recorded that 1.5.1 exposes no
  cardinality to prove a key. That is right about the *plan*: its Calcite
  rowcounts are a constant 100 per scan. It is not right about the *catalog*.

  ## Decision

  **A consuming segment is charged at the stream's flush threshold, in rows,
  unconditionally.** `realtime.segment.flush.threshold.rows` — or the
  deprecated `.threshold.size`, which the bundled quickstart config actually
  uses — bounds a consuming segment exactly: 25 of 25 sealed segments held
  exactly 100 docs at a threshold of 100, so a segment seals *at* the
  threshold rather than below it. The charge is added outside the k-largest
  pruning logic, because measured, `numConsumingSegmentsQueried` stayed 1
  under `DaysSinceEpoch > 99999` and `DaysSinceEpoch < 1` — filters excluding
  every possible value — while the broker pruned 6 of 7 segments. A time
  predicate cannot remove a consuming segment's cost, so the oracle must
  never be allowed to prune it away. No threshold in the config means no
  bound, which means `None`, which the gate denies.

  **Its bytes are the one projected number in a Lagaam quotation.** No
  endpoint on 1.5.1 reports a consuming segment's size, so there is nothing
  to measure and nothing to validate a projection against. The charge is
  `flush_rows × max over the table's own sealed segments of (charged bytes ÷
  docs)`, taken per segment and maximised rather than averaged, over exactly
  the bytes the sealed charge attributes — the referenced columns where a
  segment carries them all, the whole segment where it falls back — and
  ceiling-divided in integer arithmetic. Measured on six sealed realtime
  segments: whole-segment ratios 850.12 to 871.36 bytes/doc, a 2.5% spread;
  for a two-column projection, 0.61 to 1.14. No sealed segment with docs > 0
  means no ratio, which means `None`.

  **A join key is charged as a key only where the catalog proves it.** Two
  sources. An upsert table's `primaryKeyColumns`, where the table config's
  `upsertConfig` and the non-empty `upsertPartitionToServerPrimaryKeyCountMap`
  agree that the table is one — measured, `GROUP BY pk HAVING count(*) > 1`
  returns nothing on the upsert table and six versions per key on a
  byte-identical non-upsert twin reading the same topic, and the PK self-join
  returns exactly the distinct-key count against 3,600 for the twin. And a
  column whose per-segment `cardinality` equals its `totalDocs` on a table
  with exactly one sealed segment and nothing consuming, since `cardinality`
  is exactly `count(DISTINCT col)` — verified against the engine on four
  columns — and `count(DISTINCT col) ≤ count(col) ≤ totalDocs` forces every
  value to differ at equality.

  Where a join's equalities cover such a key set on one side, that side
  matches at most once and the join is charged the other side plus both
  inputs; both sides, the smaller plus both inputs; otherwise the product
  plus both inputs, as before. Resolving an operand to a column name is the
  careful half: the index counts over left fields ++ right fields, the names
  come from the `LogicalProject` above a scan that carries none, the
  right-hand list is alphabetised so only the index may be read, and any
  doubt — an expression operand, an `OR`, a side that is not a straight chain
  to exactly one scan — yields no evidence and therefore the product.

  ## Consequences

  - **The bytes projection can fail in exactly one way**: a consuming
    segment whose per-row encoding is worse than every sealed segment of the
    same table. The 2.5% observed spread makes that implausible and does not
    exclude it. It is the only number in the quotation that is not a
    measurement, which is why it is recorded here rather than left in a
    docstring.
  - **The null caveat on the cardinality source is open.** Whether
    `cardinality` counts a null or a default as a distinct value was never
    exercised — every column measured had `count(col) == totalDocs`. Source
    (b) is therefore gated on the column's nullability and finds nothing on
    either quickstart dataset: 0 of 2,604 per-column entries on
    `airlineStats`, and a best ratio of 0.185 on `baseballStats`. Closing it
    needs a table with real nulls, which is U13 or later.
  - **The hybrid path is unit-tested and not live-tested.** Both halves'
    sealed segments come from the same two documents and are charged as one
    segment set, but `-type HYBRID` cannot start in a container on 1.5.1 —
    it shells out to the `docker` CLI to run Kafka — so the arithmetic is
    proved over synthesised JSON and the first real hybrid deployment is the
    first live test of it.
  - **Metadata completeness is now a precondition, and it withholds quotes
    that were previously wrong.** On a multi-server table
    `/segments/{t}/metadata` returns one server's segments and alternates
    which between identical calls, so the shipped sum was a confident sum
    over a subset — an under-quote at `confidence="high"`. On the
    single-server quickstart nothing changes.
  - **An upsert table is over-charged in rows**, because a sealed upsert
    segment's `totalDocs` counts every version: 200 on disk against a visible
    `count(*)` of 100. That is what an upper bound is made of, and the
    PK-count map is not substituted for it — it is per server and unmeasured
    under replication > 1.
  - **Semi-joins and aggregated join inputs stay denied.** Their JSON plan
    cannot be obtained at all on 1.5.1: any plan carrying a
    `PIPELINE_BREAKER` exchange fails to serialise with errorCode 450, so
    `max_intermediate_rows` gets nothing to read and returns `None`.
  ```

- [ ] **Step 2 — correct ADR 0008.** In
  `docs/adr/0008-pinot-quotation-is-adapter-synthesised.md`, replace this sentence of the
  Decision section:
  > `ByBroker` and `Invalid` are not: neither was observed non-zero and neither is known
  > to be a breakdown of `numSegmentsQueried` (a broker that reports `numSegmentsQueried`
  > already net of its own pruning would be under-counted by subtracting `ByBroker`
  > again).

  with:
  > `ByBroker` and `Invalid` are not, and `ByBroker`'s reason is now measured rather than
  > assumed: on a REALTIME table with `routing.segmentPrunerTypes: ["time"]` it reaches 3
  > and 6 of 7 segments and is the only non-zero pruning counter in those rows — but
  > `numSegmentsQueried` there is already **net of** it (4 = 7−3, 1 = 7−6), so it is not a
  > breakdown of that number but a deduction already applied to it, and subtracting it
  > again would under-charge. `Invalid` was never observed non-zero. Not reading either
  > stays correct, on the stronger ground (ADR 0009).

  And replace this Consequences sentence:
  > A broker-pruned partitioned table is over-charged too, until
  > `numSegmentsPrunedByBroker` is measured against a table that actually trips it and
  > re-added with a fixture.

  with:
  > A broker-pruned table is over-charged too, permanently and intentionally:
  > `numSegmentsPrunedByBroker` has now been measured on a table that trips it, and
  > `numSegmentsQueried` is already net of it, so there is nothing left to subtract.

  Finally, replace the closing paragraph:
  > A REALTIME half is quoted `"low"` until U12 charges consuming segments at the stream's
  > flush threshold: a consuming segment reports 0 docs and -1 bytes, and charging those as
  > written would quote it free.

  with:
  > A REALTIME half was quoted `"low"` here; ADR 0009 replaces that with the
  > flush-threshold charge, and supersedes the "until key evidence exists" clause above
  > with the two catalog sources that now exist.

- [ ] **Step 3 — the index row.** Append to the table in `docs/adr/README.md`:
  ```markdown
  | [0009](0009-consuming-segments-and-proven-join-keys.md) | A consuming segment is charged at its flush threshold, and a join key must be proved by the catalog |
  ```

- [ ] **Step 4 — the README status sentence.** In `README.md`'s Status section, replace:
  > `LAGAAM_ENGINE=pinot` starts the native Pinot adapter in its experimental state:
  > grounding and execution work, but the quotation is not built yet, so `query_data` is
  > denied under the default budget until it is. On deck
  > ([roadmap](docs/roadmap.md)): the Pinot quotation from segment metadata, then a
  > Kubernetes control plane — agents as CRDs with token/dollar budgets and kill switches.

  with:
  > `LAGAAM_ENGINE=pinot` starts the native Pinot adapter: grounding, execution and a
  > quotation synthesised from segment metadata and the broker's own pruning oracle, so a
  > filtered query clears the default budget and an unbounded cross join is denied with the
  > number that blew it. A REALTIME table is priced too — its consuming segments charged at
  > the stream's flush threshold — and a join is charged its bound rather than the product
  > where the catalog proves the key. On deck ([roadmap](docs/roadmap.md)): post-execution
  > actuals on the audit line, then a Kubernetes control plane — agents as CRDs with
  > token/dollar budgets and kill switches.

  Update the test counts in the same paragraph from the real numbers:
  ```bash
  cd server && uv run pytest -q 2>&1 | tail -2 && uv run pytest -q -m integration 2>&1 | tail -2
  ```
  and write what those two lines report, never an estimate.

- [ ] **Step 5 — reconcile the spec.** In
  `docs/superpowers/specs/2026-09-17-pinot-realtime-and-key-evidence-design.md`, bring the
  Architecture block's signatures into line with what shipped — `upsert_keys` takes three
  documents rather than two (`config_json, schema_json, table_metadata_json`, since the PK
  count map is the second document that has to agree), and
  `single_segment_unique_columns` takes three (`seg_metadata_json, config_json,
  schema_json`, the schema being what the nullability gate reads). Add a line to decision
  1 recording that `_segment_sizes` already merged both halves of the size report before
  this unit, so sealed realtime bytes needed no new reader. Change nothing else: a spec
  is reconciled where the implementation taught it something, not rewritten to match.

- [ ] **Step 6 — run.**
  `cd server && uv run pytest -q && uv run mypy`
  Documentation only, so this is a regression check rather than a new assertion.

- [ ] **Step 7 — commit.**
  ```bash
  git add docs/adr/0009-consuming-segments-and-proven-join-keys.md \
          docs/adr/0008-pinot-quotation-is-adapter-synthesised.md \
          docs/adr/README.md README.md \
          docs/superpowers/specs/2026-09-17-pinot-realtime-and-key-evidence-design.md
  git commit -m "docs(pinot): ADR 0009 for the consuming charge and proved join keys

  Two decisions worth a record. The consuming-segment bytes are the only
  number in a Lagaam quotation that is a projection rather than a
  measurement, and the argument for accepting it — a threshold that bounds
  rows exactly, a ratio taken from the same table's own sealed segments, a
  2.5% observed spread — belongs somewhere a reader can weigh it, along with
  the one way it can fail. And a join key is now charged as a key, which is
  the first time anything in this adapter shrinks a quotation; the evidence
  is the catalog's and never the SQL's, and the ADR says which two sources
  count and why the second is gated to the point of finding nothing.

  ADR 0008's ByBroker justification was half false on 1.5.1 — it reaches 6 of
  7 on a time-pruned REALTIME table — so the justification is replaced while
  the decision stands: numSegmentsQueried is already net of it. The README
  stops saying the Pinot quotation is not built.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Done when

- `cd server && uv run pytest -q` green, and `uv run pytest -q -m integration` green with
  **both** Pinot instances up — the batch profile on 9000/8000 and the realtime profile on
  9001/8001, plus `examples/pinot-realtime/bootstrap.sh` having run.
- `cd server && uv run mypy` clean.
- `git diff --stat origin/main -- server/src/lagaam/core` is **empty**: core gained
  nothing, which is spec decision 9.
- `grep -rn "rowcount" server/src/lagaam/adapters/pinot` returns only the comments saying
  it is never read — no code path reads a Calcite rowcount.
- `grep -rn "numConsumingSegmentsProcessed" server/src` returns nothing: measured 0 while
  the consuming segment contributed 50 docs, so it is never read.
- `grep -rn "None if realtime else" server/src/lagaam/adapters/pinot/quote.py` returns
  nothing: the blanket REALTIME guard is gone, not weakened.
- `grep -rn "TBD\|TODO\|FIXME" server/src/lagaam/adapters/pinot docs/adr/0009-*.md`
  returns nothing.
- `estimate_cost` on the realtime `airlineStats` returns `confidence="high"` with a row
  estimate at or above the table's live `count(*)`, and the same query under
  `DaysSinceEpoch > 99999` still carries at least the flush threshold in rows.
- The upsert self-join's `max_intermediate_rows` is three times one side's row estimate;
  the non-upsert twin's is the product plus both inputs.
- `docs/adr/README.md` lists 0009, and `grep -n "quotation is not built" README.md`
  returns nothing.

---

## Self-review

Run before the first task, and again before the plan is called done.

**Spec coverage — every locked decision has a task.**

| spec decision | task |
|---|---|
| 1. The consuming-segment charge (`consuming`, `flush_rows`, the consuming drop, sealed realtime priced as OFFLINE) | Task 1 (facts), Task 4 (rows), Task 6 (the sealed k) |
| 2. The consuming-segment bytes, and the removal of the blanket REALTIME guard | Task 4 |
| 3. Hybrid tables charged as one segment set | Task 4's `test_a_hybrid_table_is_charged_as_one_segment_set`, over a synthesised size report — `_segment_sizes` already merged both halves before this unit |
| 4. Metadata completeness as a precondition | Task 1 (`metadata_is_complete`), Task 4 (the gate), Task 6 (the low quote end to end) |
| 5. Join key evidence — operand resolution, both unique-key sources, the join arithmetic | Task 2 (the evidence), Task 5 (the resolution and the rule), Task 6 (the composition) |
| 6. The PK-count map is informational only | Task 2 — read as the upsert confirmation in `_has_primary_key_counts`, charged nowhere; the Done-when grep proves no doc count comes from it |
| 7. Environment and tests — the profile, the bootstrap, the fixture, the three e2e assertions | Tasks 7, 8, 9 |
| 8. ADR 0009 and the ADR 0008 correction | Task 10 |
| 9. Core gains nothing | every task; the Done-when `git diff --stat origin/main -- server/src/lagaam/core` is the proof |

**Placeholder scan.** `grep -n "TBD\|TODO\|similar to Task\|as above\|write tests for" ` over
this file returns only the Done-when line that greps the source for them. Every code step
carries its code; every name used in a later task is defined with its signature in an
earlier task's Interfaces.

**Type and name consistency across tasks.**
- `upsert_keys(config_json, schema_json, table_metadata_json)` — three documents, the same
  three in Task 1's `table_facts` call, Task 2's definition and Task 2's tests. The spec's
  Architecture block shows two; Task 10 Step 5 reconciles it.
- `single_segment_unique_columns(seg_metadata_json, config_json, schema_json)` — three,
  likewise, and likewise reconciled.
- `max_intermediate_rows(plan_json, leaf_docs, unique_keys=None)` — the third parameter is
  `Mapping[...] | None` in the signature and `{}` in the body, which is what the spec's
  `= {}` means without a mutable default.
- `TableFacts.consuming: int`, `flush_rows: int | None`, `complete: bool`,
  `unique_keys: frozenset[frozenset[str]]` — every one defaulted, so Task 4's hand-built
  `facts()` helper and the shipped `test_pinot_engine.py` constructions both keep working.
- `consuming_segments_queried` returns `int` and never `None`; `surviving_segments` still
  returns `int | None`, and Task 6 subtracts only after the `None` check.
- `unique_keys` is keyed lowercase `database.table` in Task 6 and read by exactly that key
  in Task 5 — the same key `leaf_docs` already uses, so the two mappings cannot drift.

**Number consistency with the log.** Every literal asserted in a test appears in the
"Measured facts" table above with its §: 6 sealed / 1 consuming and 100 docs each (§1.2,
§1.3); 517,035 and the six per-segment sizes (§1.4); `missingSegments` 1 and 2 (§1.4, rule
6); the `-9223372036854775808` crc (§1.3, rule 5); thresholds 50000 / 100 / 200 (§1.1);
the counters 7/1 and 1/1 with ByBroker 6 (§1.6); 548 and the max column bound 114, 87,136
whole (rule 20, recomputed in Task 4 Step 1); `primaryKeyColumns: ["pk"]`, the PK map, 100
vs 600 and the 100 vs 3,600 self-joins (§2.1); 97,889 docs / 25 columns / best ratio 0.185
/ 0 of 2,604 (§3.1, §3.2); two-of-four metadata against four in size (§4); the left/right
field lists and the global index 3 (§5.1, rule 16). Task 8's live assertions derive every
number from the cluster instead, because the stream keeps sealing segments.

**Two things this plan deliberately does not do**, both from the spec's own "What this
design does not do": it does not close the null caveat (source (b) stays gated and finds
nothing), and it does not ground a realtime table with a row estimate —
`metadata.row_estimate` still returns `None` for any table with a REALTIME half, which
Task 8's last test pins.
