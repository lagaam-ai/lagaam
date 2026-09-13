# Native Pinot adapter

**Date:** 11 Sep 2026
**Status:** design, approved for implementation
**Measured basis:** `2026-09-11-pinot-measurements.md` (Pinot 1.5.1, live).
Every number below is from that log; nothing is assumed.

## Why

Lagaam prices a query before it runs and blocks it if it is over budget. On
Trino the price comes from the engine's own plan. Pinot has no such plan:
its multi-stage `EXPLAIN` reports `rowcount = 100.0` for every table scan —
a 9,746-row table and a 97,889-row table both say 100 — and priced a
954,024,994-row cross join at 10,000. There is no byte estimate anywhere.
The single-stage `EXPLAIN` names operators and, for a whole-segment match,
a doc count; with a real predicate the count is gone.

The danger model also differs. A bad Trino query wastes money; a bad Pinot
query degrades a shared, user-facing serving path. The multi-stage engine
(MSE), which every join requires, returns an unbounded result when no LIMIT
is written (measured: all 9,746 rows), while the single-stage engine caps at
10 rows silently, with no flag. And the broker's query endpoint accepts
`INSERT INTO t FROM FILE ...` on both engines and dispatches it as an
ingestion task.

So the adapter has to build the quotation itself, from segment metadata the
controller exposes and from the one pre-execution signal the engine gives
honestly: single-stage `EXPLAIN` performs real segment pruning and reports
it in the response without scanning a document.

## Locked decisions

1. **Zero core changes for the engine.** ADR 0001 holds: the adapter
   implements `QueryEngine`; core gains at most engine-agnostic error
   codes in `query_errors._HINTS`.
2. **Plain `httpx`, not `pinotdb`.** Pinot is one POST and one JSON
   response; catalog grounding needs controller REST that `pinotdb` does
   not cover; `pinotdb` flattens the `errorCode` the adapter classifies on.
   `httpx` becomes a runtime dependency of the server package.
3. **Every query executes on the multi-stage engine.** One set of
   semantics, joins work, and the engine's row-limit backstops
   (`maxRowsInJoin`, `maxRowsInWindow`) fail hard (errorCode 245) rather
   than truncate. The single-stage engine is used for one thing only:
   `EXPLAIN PLAN FOR` as a pruning oracle.
4. **Names are three-part to the agent, two-part to Pinot.** Pinot's
   namespace is `database.table` (default database `default`); a third
   part is an HTTP 500. Core's grant format, allowlist, cache key and the
   `describe_table` tool are all three-part, so the adapter presents a
   synthetic catalog named `pinot` and strips exactly that catalog part
   from every table and column reference before submission, on the
   sqlglot AST of already-validated SQL. A three-part reference with the
   wrong catalog, or a four-part column qualified with the wrong catalog,
   is `TableNotFoundError` before any request is made. A reference with no
   catalog at all — reachable only under `LAGAAM_ALLOW_ALL_TABLES`, where
   the allowlist check returns early — is `SqlValidationError` naming the
   required `pinot.<database>.<table>` form; unparseable SQL is the same
   error with core's re-read text.
5. **sqlglot dialect is the generic one (`""`).** Measured: 15 Pinot
   shapes re-rendered through `validate_query` executed on both engines
   with only cosmetic rewrites; `mysql` produced identical output and
   buys nothing. Query options never travel in the SQL (`SET k=v;` is a
   second statement and is rejected); they travel in `queryOptions`.
6. **A quotation is an upper bound, never a guess.** Where a number cannot
   be bounded it is `None`, and `confidence` is `"low"` — the existing
   budget gate denies that. Where it can be bounded but not measured (a
   consuming segment), the bound is charged.
7. **An incomplete result is a failure, not a warning.** `partialResult`,
   `numGroupsLimitReached`, `groupsTrimmed`, or a non-empty `exceptions[]`
   on an HTTP 200 raises `QueryFailedError`. A trimmed GROUP BY returns
   plausible wrong numbers (measured: 22 groups presented as complete).
8. **Error text never reaches the agent raw.** Broker messages embed
   broker and server IPs, ports and request ids. Agents get the curated
   hint for the code; operators get the code on the audit line.

## Components

```
server/src/lagaam/adapters/pinot/
  client.py     httpx wiring: base URLs, basic auth, timeouts; the only
                module that knows an HTTP verb. Raises PinotTransportError
                (adapter-private) on transport failure.
  metadata.py   PURE. Controller JSON -> TableSchema / table facts
                (type, time column, per-segment docs, bytes, time range,
                per-column index bytes). Never raises on shape; a shape it
                cannot read is "no fact", which fails safe downstream.
  response.py   PURE. Broker JSON -> QueryResult, or the failure it
                carries (exceptions[], partialResult, group-limit flags).
  errors.py     errorCode + message prefix -> core hint code (see table).
  plan.py       PURE. MSE EXPLAIN JSON (rels[]) -> max intermediate rows
                given leaf sizes; the ADR 0004 rule applied to a plan that
                carries shape but no sizes.
  quote.py      PURE. Table facts + pruning outcome + referenced columns
                -> CostEstimate. The IP of this adapter.
  dialect.py    PINOT_DIALECT_CARD.
  engine.py     PinotEngine: composes the above into the port.
```

Pure modules take JSON strings or parsed dicts and return domain values;
they are unit-tested on the raw responses captured in the measurement
spike (`tests/adapters/pinot/fixtures/*.json`). Only `client.py` and
`engine.py` touch the network, and they are covered by the integration
suite against the dockerized quickstart.

## Grounding: `list_catalogs` and `describe_table`

- Catalog: exactly one, named `pinot`.
- Schema: the Pinot database. `default` always; others as the controller
  reports them (U9 measures `GET /databases`; if it is absent on 1.5.1,
  the adapter lists `default` only and says so).
- Tables: `GET /tables` → `$.tables[]` (bare names, no `_OFFLINE` suffix).
  Ordered, capped at `max_tables_per_catalog` with `truncated` set, like
  the Trino adapter.
- `describe_table(catalog, schema, table)` first **resolves the spelling**:
  the controller's REST paths are case-sensitive (`/tables/airlinestats/schema`
  is a live 404 for a table listed as `airlineStats`) while broker SQL and
  grants are not. The agent's spelling is therefore a request, not an
  address: one extra `GET /tables` matches it case-insensitively against
  the listing, and the controller's spelling is what goes into every
  subsequent REST path. Two listed names differing only by case are
  refused as `TableNotFoundError` — nothing in the request says which was
  meant. The fetch is one call per `describe_table`; `CachingQueryEngine`
  above caches the resulting card.
- Then `GET /tables/{t}/schema` →
  columns are `dimensionFieldSpecs` + `metricFieldSpecs` +
  `dateTimeFieldSpecs`, each `{name, dataType}`; Pinot has no column
  comments. A spec with `singleValueField: false` is a multi-value column
  and renders as `<dataType>[]` (`INT[]`, `STRING[]`) — measured, the
  broker answers those as `INT_ARRAY`/`STRING_ARRAY` with array cells, so
  a scalar type would invite a comparison that can never match. Only an
  explicit `false` counts; an unreadable flag stays scalar.
  `row_estimate` = `GET /tables/{t}/metadata` → `$.numRows`,
  which matched `count(*)` exactly on OFFLINE tables and is **0** while a
  REALTIME table is consuming, so a REALTIME half sets `row_estimate` to
  `None` rather than 0.
- A catalog other than `pinot`, a name absent from the listing, or a
  controller 404, is `TableNotFoundError`. The card echoes the
  **controller's** spelling for `table` — not a lowercased one — so
  `list_catalogs` and `describe_table` agree and a returned name can be
  fed straight back. `catalog` stays `pinot` and `schema` stays the
  database as the controller reports it; core's cache key lowercases all
  three, so a grant written in any case still matches.

Table names arrive from agents as name parts and are placed in URL paths;
`client.py` percent-encodes and rejects a part containing `/`, `.` or
whitespace before building a URL.

## Execution: `execute(sql, max_rows, timeout_seconds)`

1. Parse with the generic dialect; for every `exp.Table` and qualified
   `exp.Column` whose catalog is `pinot`, drop the catalog; any other
   catalog → `TableNotFoundError`; no catalog at all (bare table, only
   reachable under `LAGAAM_ALLOW_ALL_TABLES`) or unparseable SQL →
   `SqlValidationError`. Re-render. (Validated SQL already carries a
   LIMIT; the adapter adds none and removes none.)
2. `POST {broker}/query/sql` with
   `{"sql": ..., "queryOptions": "useMultistageEngine=true;timeoutMs=...;maxRowsInJoin=...;maxRowsInWindow=...;maxQueryResponseSizeBytes=..."}`.
   `timeoutMs` is the budget's timeout rounded up to whole milliseconds;
   the two row limits are the budget's `max_intermediate_rows` when set.
   `maxQueryResponseSizeBytes` is sent at a fixed 64 MiB but, measured on
   the multi-stage engine the adapter executes on, is accepted by name and
   enforces nothing (97,889 rows returned under a 100-byte cap — see
   measurements §4 and §13). The real ceiling is client-side:
   `PinotClient.broker_query` streams the body and raises
   `PinotResponseTooLarge` past `max_response_bytes`, which `execute` maps
   to `RESPONSE_TOO_LARGE`.
   The whole call — submission through body read — is wrapped in
   `anyio.fail_after(timeout_seconds + 5s grace)` whenever a budget
   exists. httpx's timeout is **per operation** and resets on every read,
   so a body arriving in slow chunks outlives it: measured, four chunks
   1.5s apart returned a successful result 6.01s into a 0.2s budget.
   Expiry raises `QueryFailedError(EXCEEDED_TIME_LIMIT)`. The
   per-operation httpx timeout stays — it still catches a stalled single
   read sooner. `timeout_seconds=None` means no deadline: that is core
   declining to bound the query, and the adapter invents no bound of its own.
3. `response.py` reads `resultTable.dataSchema.columnNames`,
   `resultTable.rows`, `numRowsResultSet`. Rows are capped at `max_rows`
   with `truncated` from the +1 the server already asked for.
4. Failure precedence: transport error → `EngineError`; `exceptions[]`
   non-empty → classified by `errors.py`; `partialResult` /
   `numGroupsLimitReached` / `groupsTrimmed` → `QueryFailedError` with an
   incomplete-result hint. `numGroupsWarningLimitReached` becomes a
   warning on the result. Never branch on HTTP status: every error is a
   200.

## Quotation: `estimate_cost(sql)`

Inputs, all pre-execution:

| input | source | trust |
|---|---|---|
| tables referenced | the validated SQL's `exp.Table` nodes | exact |
| columns referenced | every `exp.Column` name in the SQL, matched by lowercase name against each referenced table's schema; an unresolvable name charges the whole segment | over-approximate |
| table type | `GET /tables/{t}` has `$.OFFLINE` and/or `$.REALTIME` | exact |
| per-segment docs, time range | `GET /segments/{t}/metadata?columns=<referenced>` → `totalDocs`, `startTimeMillis`, `endTimeMillis` | exact for sealed; **0 / null for consuming** |
| per-segment, per-column bytes | same call → `columns[i].indexSizeMap` (sum of its entries) | exact for sealed |
| per-segment bytes | `GET /tables/{t}/size` → `offlineSegments.segments[s].reportedSizeInBytes` | exact for sealed; **-1 for consuming** |
| segments surviving the predicate | single-stage `EXPLAIN PLAN FOR <two-part sql>` → response `numSegmentsQueried` minus the pruned counters; only for a query that references exactly one table (a join is an MSE-only statement and single-stage EXPLAIN refuses it) | engine-authoritative; `numDocsScanned` is 0, nothing runs |
| plan shape | `EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR` on MSE → `rels[]` with `joinType` and the join condition; the `rowcount` attributes are **never read** | exact for shape |

Rules:

- **Surviving segments are the largest k.** The pruning oracle says how
  many segments survive, not which. Charging the k largest by docs (and,
  separately, by bytes) is an upper bound on any k that could survive. A
  multi-table query, or one whose `EXPLAIN` failed, has k = all.
- `row_estimate` = Σ over referenced tables of docs in its surviving set.
- `scanned_bytes` = Σ over referenced tables, over its surviving set, of
  the referenced columns' `indexSizeMap` bytes; a table whose column
  attribution fails is charged `reportedSizeInBytes` of the surviving set.
- `max_intermediate_rows`: `plan.py` walks `rels[]` post-order. A scan
  node is its table's surviving docs; a join whose condition has no
  equality is the product of its children; a join with an equality is the
  max of its children (the ADR 0004 NaN-join rule); every other node is
  the max of its children. The answer is the max over all nodes. A plan
  that cannot be fetched or read is `None`.
- `core.scans.has_unpriceable_shape` and `generator_fanout` run first with
  the generic dialect exactly as the Trino adapter runs them; a flagged
  shape is `CostEstimate(confidence="low")` before any request.
- **Consuming segments are charged at their bound**, not skipped. A
  REALTIME table's `streamConfigs` carries the flush threshold
  (`realtime.segment.flush.threshold.rows`, or a size threshold); each
  consuming segment (`numSegments` from `/tables/{t}/metadata` minus the
  sealed ones the size report lists, or `realtimeSegments.missingSegments`)
  is charged `threshold.rows` docs, and bytes equal to that row count times
  the mean bytes-per-row of the table's sealed segments. With no sealed
  segment to take a mean from, bytes are `None`. A pure-OFFLINE table has
  no consuming segments and none of this applies. (U12; U11 ships OFFLINE
  quotation with any REALTIME half quoted `"low"`.)
- `confidence` is `"high"` only when `scanned_bytes` was produced; the
  model's validator already enforces that.

Two caveats are documented rather than solved: a star-tree-answered
aggregation reads a pre-aggregated tree and is over-charged by a doc-count
quote; a table under row-level security is over-charged because the agent
sees a filtered subset. Both are denials the operator can raise a budget
for, never admissions.

## Error mapping

| broker | hint code (core) | raised as |
|---|---|---|
| 150 `SQLParsingError` | `SYNTAX_ERROR` | `QueryFailedError` |
| 190 `TableDoesNotExistError` | `TABLE_NOT_FOUND` | `QueryFailedError` |
| 710 `UnknownColumnError` (single-stage), or 700 whose message says a column `depends on itself` (the multi-stage spelling of an unknown column) | `COLUMN_NOT_FOUND` | `QueryFailedError` |
| 700 `Unsupported function`, `No match found for function` | `FUNCTION_NOT_FOUND` | `QueryFailedError` |
| 700 other `QueryValidationError` | `NOT_SUPPORTED` | `QueryFailedError` |
| 245 join/window row limit | `EXCEEDED_ROW_LIMIT` (new, engine-agnostic) | `QueryFailedError` |
| 400 `BrokerTimeoutError`, 427 servers not responded | `EXCEEDED_TIME_LIMIT` | `QueryFailedError` |
| 180 `AccessDenied` | `PERMISSION_DENIED` | `QueryFailedError` |
| 503 whose message carries `exceeds threshold` or `Serialized query response size` (the oversized-response refusal, single-stage engine only) | `RESPONSE_TOO_LARGE` (new) | `QueryFailedError` |
| 503 otherwise — it is `QUERY_CANCELLATION`, which `QueryScheduler` reuses for the size refusal and `LeafOperator` emits as `Cancelled while waiting for leaf results` | — (no core hint) | `EngineError` — a cancellation is not the query's fault |
| broker HTTP 401/403 (auth filter, or either request handler's `WebApplicationException(FORBIDDEN)` for a table refusal) → adapter-private `PinotForbidden`, never carrying the response body | `PERMISSION_DENIED` | `QueryFailedError` |
| controller HTTP 401/403 during grounding → `PinotForbidden` | — | `EngineError("Lagaam's Pinot credentials were refused")` — our text, never the body; the agent cannot rewrite its way out of the server's own credentials |
| exceeded total deadline (`anyio.fail_after`) | `EXCEEDED_TIME_LIMIT` | `QueryFailedError` |
| — client-side ceiling (`PinotResponseTooLarge`, adapter-private): the multi-stage engine does not enforce `maxQueryResponseSizeBytes` | `RESPONSE_TOO_LARGE` (new) | `QueryFailedError` |
| HTTP 200 with `partialResult`, `numGroupsLimitReached` or `groupsTrimmed` true | `INCOMPLETE_RESULT` (new) | `QueryFailedError` |
| anything else, transport, non-JSON body | — | `EngineError("the query engine is not reachable right now")` |

The hint is core's text; the broker message is discarded. The code and
the message prefix go nowhere but the exception's `__cause__`, which the
server layer already keeps out of the agent's view.

## Dialect card

`engine="Pinot"`, `sqlglot_dialect=""`, rules (the agent-facing knowledge
the measurements support): names are `pinot.default.table`; identifiers in
double quotes, strings in single quotes; table and column names are
case-insensitive; time columns are epoch numbers, convert with
`DATETIMECONVERT`/`DATETRUNC`/`ToDateTime`; always filter on the table's
time column — that is what prunes segments; prefer `DISTINCTCOUNTHLL` over
`DISTINCTCOUNT`; a type ending in `[]` is a multi-value column, so use
`ARRAYLENGTH`/ARRAY functions rather than scalar comparisons; every query
needs a LIMIT and one is added if missing; `SELECT *` is rejected; joins
run on the multi-stage engine and are bounded by a row limit.

## Configuration and wiring

`__main__` selects the adapter with `LAGAAM_ENGINE` (`trino`, the default,
or `pinot`). Pinot reads `PINOT_CONTROLLER_URL` (`http://localhost:9000`),
`PINOT_BROKER_URL` (`http://localhost:8000`), `PINOT_USER` and
`PINOT_PASSWORD` (HTTP basic auth on both services; unset means none).
The `LAGAAM_*` budget and grant variables are unchanged; a Pinot grant is
written `pinot.default.airlinestats`.

`examples/docker-compose.yml` gains a `pinot` profile:
`apachepinot/pinot:release-1.5.1`, `QuickStart -type BATCH`,
`JAVA_OPTS=-Xms1G -Xmx3G`, ports 9000 and 8000, health check on the
controller. First query answers at ~42 s; the ten sample tables finish
loading over ~20 minutes, so the integration fixture waits for
`airlineStats` and `baseballStats` specifically.

## Testing

- Unit: every pure module against the captured JSON (`fixtures/`), plus
  the catalog-strip rewrite, the query-option string, the error table
  row by row, and the quotation arithmetic (largest-k bound, column
  attribution, the product/max rule) on hand-built facts.
- Integration (`-m integration`, `pinot_ready` fixture that skips when the
  controller is unreachable): the port is satisfied; grounding round-trip;
  `describe_table` accepts the name it just returned and the lowercase
  grant spelling, and agrees with the listing; a multi-value column grounds
  as `INT[]`/`STRING[]` and its cells come back as arrays;
  a validated query executes with a LIMIT on MSE; each query option trips
  when set low; `numGroupsLimit=2` raises; a bad column, table, function
  and a timeout each map to their hint; the quotation for a time-filtered
  `airlineStats` query charges fewer segments than the unfiltered one and
  never fewer than `numSegmentsProcessed` reports after execution; a
  cross join quotes the product of the two tables' doc counts.
- E2E: the MCP client round-trip in `tests/integration/test_e2e_mcp.py`
  parameterized over both engines.

## Units

- **U9 — skeleton and grounding.** `client`, `metadata`, `dialect`,
  `engine.list_catalogs`/`describe_table`, compose profile, `__main__`
  selection, integration fixture. Demoable: an agent grounds itself on
  Pinot through the MCP tools.
- **U10 — execution.** Catalog strip, query options and their pinned
  names, `response`, `errors`, result-trust failures. Demoable: a
  validated query runs with reins on; a join hits `maxRowsInJoin`.
- **U11 — quotation, OFFLINE.** `quote`, `plan`, the pruning oracle, the
  largest-k bound, column attribution. Demoable: the unfiltered query is
  blocked with the number and the fix; the time-filtered one runs.
- **U12 — consuming-segment bound and actuals.** The flush-threshold
  charge for REALTIME tables; `numEntriesScannedInFilter` and freshness
  surfaced as warnings; estimate vs `numDocsScanned` on the audit line.
- **U13 — docs and demo.** README section in developer-pain language,
  configuration table rows, the realtime GIF (needs an external Kafka:
  the 1.5.1 stream quickstart shells out to the `docker` CLI and cannot
  run inside a container).

## Out of scope

Pinot's own auth beyond basic; row-level security awareness; the
single-stage engine for execution; Pinot databases other than the ones
the controller lists; star-tree-aware pricing; upsert-aware doc counts.
