# Plan — quote a multi-server Pinot table (2026-09-21)

Branch `feat/pinot-multiserver-metadata`, worktree
`/Users/muditkapoor/Documents/code/lagaam-multiserver`. Measurements:
`docs/superpowers/specs/2026-09-21-pinot-multiserver-measurements.md`.
Fixtures already copied into `server/tests/adapters/pinot/fixtures/`:
`servers-u14multi.json`, `size-u14multi.json`,
`seg-metadata-u14multi-truncated.json` (the bulk call, 8 of 10 sealed,
`columns=pk`), `seg-metadata-u14multi-7051.json` / `-7052.json` (per-server
asks, `columns=pk`), `tableconfig-u14multi.json`, `schema-u14multi.json`,
`externalview-u14multi.json`.

## The pain

A Pinot cluster with more than one server is quoted `confidence="low"` for
every query, so every query is denied. Cause: `GET /segments/{t}/metadata`
returns one server's segments (ADR 0009 §4 of the 2026-09-17 log), and the
completeness guard correctly refuses to sum a subset. The guard stays. What
changes is that the adapter goes and gets the segments the guard says are
missing, from the servers that hold them.

## The rule

When the bulk metadata response does not cover every sealed segment the
size report names:

1. `GET /segments/{part}/servers` → for each table type, `serverToSegmentsMap`.
2. Take the sealed names the bulk response lacks. Group them by a server
   that holds them: walk servers in the response's order, assign each
   missing name to the first server listing it (replicas are identical,
   measured §3). Servers with nothing assigned make no call.
3. For each assigned server, `GET /segments/{part}/metadata` with
   `params={"segments": [names...], "columns": [...same as the bulk call...]}`
   (`controller_get` already takes `dict[str, str | list[str]]`).
4. Merge name-keyed into the bulk response, bulk entries winning. Hand the
   merged dict to `table_facts` exactly as before — the same
   `metadata_is_complete` runs on it and still decides.

Bounds, all of which leave the quote where it is today (`low`) rather than
guessing: more than `_MAX_METADATA_SERVERS = 32` servers assigned → no
fan-out; any per-server call that returns `NotFound`, a non-dict, or raises
`PinotTransportError` → stop, keep what the bulk call gave; the whole
fan-out under `anyio.fail_after(_METADATA_FANOUT_SECONDS = 10.0)` (the
EXPLAIN deadline's sibling), timeout → keep the bulk response. Calls are
sequential; 32 is the cap so the worst case is bounded, not a target.

A single-server table (the batch quickstart, every existing integration
test) is complete on the bulk call and makes **no new request**.

## Tasks (strict TDD)

1. `metadata.py`: pure functions, fixture-tested in
   `tests/adapters/pinot/test_pinot_metadata.py`:
   - `missing_sealed_segments(seg_metadata_json, size_json) -> frozenset[str]`
     — the names `metadata_is_complete` is missing; refactor
     `metadata_is_complete` to be `not missing_sealed_segments(...)` so the
     two cannot drift.
   - `segments_by_server(servers_json) -> dict[str, tuple[str, ...]]` —
     parses the `/servers` list; tolerant of garbage (non-list, missing
     keys → empty).
   - `assign_missing_to_servers(missing, by_server) -> dict[str, tuple[str, ...]]`
     — first server holding each name, in response order; names nobody
     holds are simply absent (the guard will still catch them).
   - `merge_segment_metadata(bulk, per_server: Iterable[Any]) -> dict[str, Any]`
     — name-keyed union, bulk wins, non-dict inputs ignored.
   Fixture assertions: truncated bulk is missing exactly the 3 sealed
   segments on 7051; servers fixture parses to 2 servers with 4 and 8
   names; merging truncated + 7051 response makes `metadata_is_complete`
   true and `segment_facts` yield 10 sealed facts whose `column_bytes["pk"]`
   sum equals the fixture sum (compute it in the test from the fixture, do
   not hard-code a guess); `surviving_docs`-style totals over the merged
   set equal 1,000 docs.
2. `engine.py` `_table_facts`: after `seg_json`/`size_json` are fetched,
   `seg_json = await self._complete_segment_metadata(database, part, seg_json, size_json, params)`.
   Engine tests in `tests/adapters/pinot/test_pinot_engine.py` with the
   fake client the file already uses (read how it records requests):
   - complete bulk → no `/servers` request made;
   - truncated bulk (fixture) → one `/servers` request, then exactly one
     `/segments/u14multi/metadata` request per server that holds a
     missing name, whose `segments` param is exactly that server's missing
     names and whose `columns` param equals the bulk call's; the resulting
     `TableFacts.complete` is True and the quote is `high`;
   - a server whose names are all present already → no call to it;
   - 33 servers each holding one missing name → no fan-out, `complete`
     False;
   - per-server call raising `PinotTransportError` / returning NotFound →
     `complete` False, no exception escapes;
   - fan-out exceeding the deadline (fake client sleeps) → `complete` False.
3. Realtime profile: `examples/pinot-realtime/bootstrap.sh` creates
   `u14multi` (2-partition topic `u14-multi`, config = u12plain minus the
   pin, flush 100, replication 1) and `u14rep` (topic `u14-rep`, replication
   2), fed from the same 1,100-row keyed feed (500 rows keys `k0/k1`, then
   600 rows keys `x0..x9` — `k0`/`k1` hash to one partition, the ten keys
   spread; keep the feed shape so segment counts stay 10 sealed). Schema
   files `u14multi-schema.json`, `u14multi-table.json`, `u14rep-*.json`
   next to the u12 ones (the scratchpad copies are at
   `/private/tmp/claude-501/-Users-muditkapoor-Downloads/bfad1165-ba07-4099-96ea-4f6d98672b49/scratchpad/u14*.json`
   — the *table* json there was posted with `tableName` only; write proper
   files). Idempotent like the existing steps (`feed_needed`). The tables
   ALREADY EXIST on the running instance (:9001) with exactly this data, so
   do not re-bootstrap; just make the script produce the same state on a
   fresh instance and dry-read it for consistency. Extend
   `pinot_realtime_ready` (tests/integration/conftest.py) to also wait for
   `u14multi`/`u14rep` to answer.
4. Integration (`tests/integration/test_pinot_engine.py`, realtime engine
   on :9001/:8001): for each of `u14multi`, `u14rep`:
   `estimate_cost("SELECT pk FROM pinot.default.<t> LIMIT 5")` is
   `confidence="high"`; `row_estimate >= numDocsScanned` of the executed
   `SELECT pk FROM <t> LIMIT 5` without LIMIT prune (use the same pattern
   the U12 tests use for `_realtime_scanned`); `scanned_bytes` equals the
   sum over the 10 sealed segments' pk `indexSizeMap` (read live from the
   per-server endpoint in the test, not hard-coded) plus the consuming
   charge; and the request log, if the engine exposes one — otherwise
   assert via the fact that the quote is high, since main quotes it low.
   Also assert `u12plain` still quotes exactly as before (`scanned_bytes
   1582`, `row_estimate 400`).
5. Docs: ADR 0011 "Missing segment metadata is fetched from the servers
   that hold it" (house style; the pain; the rule; the bounds; why not
   zkmetadata / per-segment; measured numbers), row in `docs/adr/README.md`;
   in ADR 0009 change the "per-segment metadata fetch that would restore
   those tables is U13" consequence to point at 0011; grep `multi-server`
   across README.md and docs/ and fix any sentence that says such tables
   are denied.
6. Commits (conventional, atomic, each ending with
   `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`):
   1. `feat(pinot): fetch the segments the bulk metadata call left out, per server`
   2. `test(pinot): a two-server and a replicated table quote high on the realtime profile`
      (bootstrap + conftest + integration)
   3. `docs(adr): 0011 — missing segment metadata comes from the servers that hold it`
   Do not push. Do not open a PR. Do not start/stop/restart containers.

## Report back with

unit count before/after, `uv run pytest -q -m integration` full output tail
(Trino + both Pinots are up; `docker start lagaam-trino` is NOT yours to run
— if Trino is down, report it), mypy output, the live `estimate_cost` lines
for `u14multi`, `u14rep`, `u12plain` before (main) and after, and anything
measured that contradicts the plan.
