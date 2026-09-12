# Pinot adapter U11 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `PinotEngine.estimate_cost(sql)` returns a real upper-bound `CostEstimate` for OFFLINE tables, synthesised from controller segment metadata and the single-stage `EXPLAIN` pruning oracle, so a filtered Pinot query clears the default budget and an unbounded cross join is denied with the number that blew it.

**Architecture:** Two new pure modules — `quote.py` (table facts + pruning outcome + referenced columns → `CostEstimate`) and `plan.py` (the multi-stage `EXPLAIN` `rels[]` + leaf sizes → max intermediate rows) — sit beside the existing pure `metadata.py`, which grows the per-segment and per-column fact parsers. `engine.py` is the only new caller of the network: it runs core's shape checks first, fetches table config, segment metadata (`?columns=`), and `/size` from the controller, asks the broker for a single-stage `EXPLAIN PLAN FOR` pruning count and a multi-stage `rels[]` plan, and hands all of it to the pure modules. Core gains nothing; the quotation is entirely adapter-side, which is why its honest confidence ceiling is lower than Trino's.

**Tech Stack:** Python 3.12+, uv, httpx, sqlglot, pydantic, pytest (+ `pytest-asyncio` auto mode), mypy strict over `lagaam.core` and `lagaam.adapters.pinot`. Live engine for the integration suite: Apache Pinot 1.5.1 batch quickstart, controller `http://localhost:9000`, broker `http://localhost:8000`.

**Spec:** docs/superpowers/specs/2026-09-11-pinot-adapter-design.md

## Global Constraints

- Work only in the worktree `/Users/muditkapoor/Documents/code/lagaam-pinot` on branch `feat/pinot-adapter`; never push, never `git stash`.
- Conventional atomic commits (one logical change plus its tests), each message ending with the trailer line `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Inline comments are one line maximum, and only for a constraint the code cannot show; never restate what the line does.
- Type hints everywhere; `uv run mypy` clean (`lagaam.adapters.pinot` is in the mypy packages, strict mode).
- `uv run pytest -q` green after every task; `uv run pytest -q -m integration` green after every task that touches the integration suite.
- Core never imports httpx or a Pinot shape, and core gains NOTHING in this plan — no new function, field, error code or hint.
- Fail-safe rule: an unmeasurable number is `None` and the confidence is `"low"`, never a guess; where a bound exists it is charged rather than skipped.
- The multi-stage `EXPLAIN`'s `rowcount` and `cumulative cost` attributes are never read — measured constant `100.0` per table scan, which priced a 954-million-row cross join at 10,000.
- The pruning oracle runs only for SQL that references exactly one table, and never executes anything: the engine asserts `numDocsScanned == 0` in the `EXPLAIN` response and discards the outcome if it is not.
- Names sent to the broker are two-part, via `names.two_part_sql` — a three-part name is an HTTP 500 on the broker's own parser.
- A query that core's shape checks flag (`has_unpriceable_shape` or `scan_counts_saturated`) returns `CostEstimate(confidence="low")` before any request is made.

---

## Measured facts this plan is built on

Confirmed live against the running Pinot 1.5.1 on 13 Sep 2026, beyond what
`docs/superpowers/specs/2026-09-11-pinot-measurements.md` records. These are the
numbers the tasks below assert; nothing here is assumed.

**The pruning counters are nested, not additive.** `numSegmentsPrunedByServer` is
the *total* of the server-side pruning; `numSegmentsPrunedByValue` and
`numSegmentsPrunedByLimit` are non-overlapping breakdowns *of that total*.
Measured on `airlineStats` (31 segments):

| query (under `EXPLAIN PLAN FOR`) | Queried | ByServer | ByValue | ByLimit | ByBroker |
|---|---|---|---|---|---|
| `SELECT Carrier ... GROUP BY Carrier LIMIT 10` (no filter) | 31 | 0 | 0 | 0 | 0 |
| `... WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 ...` | 31 | 28 | 28 | 0 | 0 |
| `SELECT Carrier FROM airlineStats LIMIT 10` | 31 | 30 | 0 | 30 | 0 |

Summing them would charge 28 + 28 = 56 pruned of 31 — a negative surviving count,
and an under-quote. **So the surviving count is
`numSegmentsQueried - max(ByBroker, ByServer, ByValue, ByLimit)`**, clamped to at
least 1. `max` is exact where the counters nest and conservative where they do
not: the measurement log's §6 records the same predicate registering once as
`ByValue: 28` and once as `ByServer: 28` with `ByValue: 0`, so no single counter
can be read alone, and `max` never exceeds the true pruned count when the
counters are breakdowns of one another.

**`numSegmentsProcessed` is 0 under `EXPLAIN`** (nothing executes), in every shape
measured. The measurement log's §6 suggestion of `numSegmentsProcessed /
numSegmentsQueried` as the surviving fraction is therefore an *execution-time*
observation only, unusable pre-execution. This plan reads the pruned counters.

**The `ByLimit` prune must not be trusted, and `max` handles it correctly
anyway.** An unfiltered `SELECT ... LIMIT 10` prunes 30 of 31 segments by limit,
and real execution confirms it (`numSegmentsProcessed: 1`, `numDocsScanned: 10`).
That is a genuine bound on this query, and charging one segment for it is sound.

**The largest-k bound is real and tight.** `airlineStats` segment docs sorted
descending are `[422, 409, 403, ...]`, summing to 9,746 over 31 segments. For the
time-filtered query, k = 3 and the three largest sum to **1,234 docs**; real
execution scanned **1,101**. The bound holds and is 12% loose — exactly the
"upper bound, never a guess" the spec asks for. Segment bytes for the same k = 3
sum to 530,213 of the table's 4,861,355.

**`?columns=` takes repeated parameters and httpx renders a list value as
repeated parameters** (`httpx.URL('http://x/p', params={'columns': ['A','B']})`
→ `http://x/p?columns=A&columns=B`), so `client.controller_get` needs its `params`
type widened to accept a list value and nothing else. `columns=*` returns all 84
`airlineStats` columns; this plan never sends `*`, because charging only the
referenced columns is the point.

**`rels[]` is a flat, topologically ordered list, not a tree.** A node's children
are its `inputs` (a list of id strings); **a node with no `inputs` key at all
implicitly consumes the immediately preceding node**, and a node with
`"inputs": []` is a leaf. Measured cross join and equi join on
`airlineStats × baseballStats`:

```
id 0  PinotLogicalTableScan  table=[default, airlineStats]   inputs=[]
id 1  LogicalProject                                          (implicit input 0)
id 2  PinotLogicalExchange                                    (implicit input 1)
id 3  PinotLogicalTableScan  table=[default, baseballStats]   inputs=[]
id 4  LogicalProject                                          (implicit input 3)
id 5  PinotLogicalExchange                                    (implicit input 4)
id 6  LogicalJoin  joinType=inner  inputs=['2','5']
      cross: condition={"literal": true, "type": {...}}          <- no "op"
      equi:  condition={"op": {"name":"=","kind":"EQUALS",...},
                        "operands":[{"input":0,...},{"input":1,...}]}
id 7..9  PinotLogicalAggregate / Exchange / Aggregate
```

So **a join has an equality iff its `condition` carries an `op` whose `kind` is
`EQUALS`, or whose `operands` contain such an op**; a cross join's condition is
the bare `{"literal": true}` with no `op` key at all.

**Single-stage `EXPLAIN` refuses a join**, with errorCode 150 and the message
"only supported by the multi-stage query engine" — which is why the oracle runs
only for single-table SQL and k = all otherwise.

**`baseballStats` has no time column** (`timeColumn: null`, `startTimeMillis:
null`) and one segment of 97,889 docs, and its `playerID` is RAW-encoded so its
`indexSizeMap` is `{"forward_index": 542226}` with no `dictionary` entry — the
per-column parser must sum whatever entries are present rather than expect a
fixed set of keys.

**The true cross-join product** of the two tables is 9,746 × 97,889 =
**954,026,194**. (The design spec and measurement log both say 954,024,994; that
is an arithmetic slip in the prose, not in any measured number. Tests assert the
product computed from the fixtures, never the transcribed literal.)

---

## File Structure

**Create**

| path | responsibility |
|---|---|
| `server/src/lagaam/adapters/pinot/quote.py` | PURE. `SegmentFact`/`TableFacts` + pruning outcome + referenced columns → `CostEstimate`. The largest-k bound, column attribution with whole-segment fallback, the REALTIME → low rule. The IP of this adapter. |
| `server/src/lagaam/adapters/pinot/plan.py` | PURE. Multi-stage `EXPLAIN ... AS JSON` `rels[]` + a leaf-size lookup → max intermediate rows. ADR 0004's product/max rule over a plan that carries shape but no sizes. |
| `server/tests/adapters/pinot/test_pinot_quote.py` | Unit tests for `quote.py` on hand-built facts and on the real fixtures. |
| `server/tests/adapters/pinot/test_pinot_plan.py` | Unit tests for `plan.py` on the captured `rels[]` fixtures. |
| `server/tests/adapters/pinot/fixtures/seg-metadata-airlineStats-columns.json` | `GET /segments/airlineStats/metadata?columns=Carrier&columns=DaysSinceEpoch`, 31 segments. |
| `server/tests/adapters/pinot/fixtures/seg-metadata-baseballStats-columns.json` | `GET /segments/baseballStats/metadata?columns=teamID&columns=playerID`, 1 segment, no time column. |
| `server/tests/adapters/pinot/fixtures/size-airlineStats.json` | `GET /tables/airlineStats/size`. |
| `server/tests/adapters/pinot/fixtures/size-baseballStats.json` | `GET /tables/baseballStats/size`. |
| `server/tests/adapters/pinot/fixtures/explain-v1-timefilter.json` | Single-stage `EXPLAIN` response, 28 of 31 pruned. |
| `server/tests/adapters/pinot/fixtures/explain-v1-nofilter.json` | Single-stage `EXPLAIN` response, 0 pruned. |
| `server/tests/adapters/pinot/fixtures/explain-v1-limitpruned.json` | Single-stage `EXPLAIN` response, 30 pruned by limit, 0 by value. |
| `server/tests/adapters/pinot/fixtures/explain-mse-crossjoin.json` | Multi-stage `rels[]`, join condition `{"literal": true}`. |
| `server/tests/adapters/pinot/fixtures/explain-mse-equijoin.json` | Multi-stage `rels[]`, join condition with `kind: EQUALS`. |
| `server/tests/adapters/pinot/fixtures/explain-mse-singletable.json` | Multi-stage `rels[]`, one scan, no join. |
| `server/tests/adapters/pinot/fixtures/tableconfig-baseballStats.json` | `GET /tables/baseballStats`, for a second OFFLINE config. |

**Modify**

| path | change |
|---|---|
| `server/src/lagaam/adapters/pinot/metadata.py` | Add `SegmentFact`, `segment_facts(seg_metadata_json, size_json)`, `table_facts(...)`, `time_column(config_json)`. Still PURE, still never raises on a shape it cannot read. |
| `server/src/lagaam/adapters/pinot/names.py` | Add `referenced_tables(sql, catalog)` and `referenced_columns(sql)` over the validated SQL's AST. |
| `server/src/lagaam/adapters/pinot/client.py` | Widen `controller_get`'s `params` to accept a list value (repeated query parameters for `?columns=`). No new method. |
| `server/src/lagaam/adapters/pinot/engine.py` | Replace the stub `estimate_cost` with the composition; add the private fetch helpers and the `EXPLAIN` calls. |
| `server/tests/adapters/pinot/test_pinot_metadata.py` | Tests for the new parsers. |
| `server/tests/adapters/pinot/test_pinot_names.py` | Tests for the two new extractors. |
| `server/tests/adapters/pinot/test_pinot_client.py` | Test that a list param renders as repeated parameters. |
| `server/tests/adapters/pinot/test_pinot_engine.py` | Unit tests for the composed quotation over `MockTransport` routing the real fixtures. |
| `server/tests/integration/test_pinot_engine.py` | The demo assertions against live Pinot. |
| `server/tests/integration/test_e2e_mcp.py` | **Replace** `test_query_data_on_pinot_is_denied_until_the_quotation_lands` with a pair: the filtered query runs, the cross join is denied. |
| `server/tests/integration/conftest.py` | Nothing — `pinot_ready` already waits for both tables. |
| `docs/superpowers/specs/2026-09-11-pinot-adapter-design.md` | Reconcile the Quotation section with what shipped (the `max` pruning rule, `numSegmentsProcessed` unusable pre-execution). |
| `docs/adr/0008-pinot-quotation-is-adapter-synthesised.md` | New ADR for the one genuinely new decision. |

---

## Task 1: Segment and table facts in `metadata.py`

**Files:** `server/src/lagaam/adapters/pinot/metadata.py`, `server/tests/adapters/pinot/test_pinot_metadata.py`, four new fixtures.

**Interfaces:**
- Consumes: parsed JSON from `GET /segments/{t}/metadata?columns=...`, `GET /tables/{t}/size`, `GET /tables/{t}`.
- Produces:
  - `@dataclass(frozen=True) class SegmentFact: name: str; docs: int | None; total_bytes: int | None; start_ms: int | None; end_ms: int | None; column_bytes: Mapping[str, int]`
  - `def segment_facts(seg_metadata_json: Any, size_json: Any) -> list[SegmentFact]`
  - `def time_column(config_json: Any) -> str | None`
  - `@dataclass(frozen=True) class TableFacts: table: str; types: frozenset[str]; time_column: str | None; segments: tuple[SegmentFact, ...]`
  - `def table_facts(table: str, config_json: Any, seg_metadata_json: Any, size_json: Any) -> TableFacts`

- [ ] **Step 1 — capture the fixtures.** Run, from the worktree root, against the live Pinot:
  ```bash
  cd server/tests/adapters/pinot/fixtures
  curl -s "http://localhost:9000/segments/airlineStats/metadata?columns=Carrier&columns=DaysSinceEpoch" \
    | python3 -m json.tool > seg-metadata-airlineStats-columns.json
  curl -s "http://localhost:9000/segments/baseballStats/metadata?columns=teamID&columns=playerID" \
    | python3 -m json.tool > seg-metadata-baseballStats-columns.json
  curl -s "http://localhost:9000/tables/airlineStats/size"   | python3 -m json.tool > size-airlineStats.json
  curl -s "http://localhost:9000/tables/baseballStats/size"  | python3 -m json.tool > size-baseballStats.json
  curl -s "http://localhost:9000/tables/baseballStats"       | python3 -m json.tool > tableconfig-baseballStats.json
  ```
  Confirm `seg-metadata-airlineStats-columns.json` has 31 top-level keys, that
  `airlineStats_OFFLINE_16071_16071_0` reports `totalDocs: 289` with
  `Carrier.indexSizeMap == {"dictionary": 36, "forward_index": 153}`, and that
  `size-airlineStats.json` reports that segment at
  `reportedSizeInBytes: 152577` and the table at `4861355`.

- [ ] **Step 2 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_metadata.py`:
  ```python
  def test_segment_facts_carry_docs_bytes_time_and_per_column_bytes() -> None:
      facts = segment_facts(
          load("seg-metadata-airlineStats-columns.json"),
          load("size-airlineStats.json"),
      )
      assert len(facts) == 31
      one = next(f for f in facts if f.name == "airlineStats_OFFLINE_16071_16071_0")
      assert one.docs == 289
      assert one.total_bytes == 152577
      assert one.start_ms == 1388534400000
      assert one.end_ms == 1388534400000
      assert one.column_bytes == {"Carrier": 189, "DaysSinceEpoch": 28}
      assert sum(f.docs or 0 for f in facts) == 9746
      assert sum(f.total_bytes or 0 for f in facts) == 4861355


  def test_segment_facts_sum_every_index_entry_not_a_fixed_set() -> None:
      """playerID is RAW-encoded: forward_index only, no dictionary."""
      facts = segment_facts(
          load("seg-metadata-baseballStats-columns.json"),
          load("size-baseballStats.json"),
      )
      assert len(facts) == 1
      only = facts[0]
      assert only.docs == 97889
      assert only.total_bytes == 3342450
      assert only.column_bytes["playerID"] == 542226
      assert only.column_bytes["teamID"] == 455 + 173683 + 97897


  def test_a_segment_without_a_time_column_has_no_time_range() -> None:
      only = segment_facts(
          load("seg-metadata-baseballStats-columns.json"),
          load("size-baseballStats.json"),
      )[0]
      assert only.start_ms is None
      assert only.end_ms is None


  def test_a_segment_the_size_report_never_mentions_has_no_bytes() -> None:
      facts = segment_facts(
          load("seg-metadata-airlineStats-columns.json"), {"offlineSegments": None}
      )
      assert len(facts) == 31
      assert all(f.total_bytes is None for f in facts)
      assert all(f.docs is not None for f in facts)


  def test_a_consuming_segments_negative_size_is_no_fact_not_a_credit() -> None:
      facts = segment_facts(
          {"seg0": {"segmentName": "seg0", "totalDocs": 0}},
          {"offlineSegments": {"segments": {"seg0": {"reportedSizeInBytes": -1}}}},
      )
      assert facts[0].total_bytes is None


  @pytest.mark.parametrize("body", [None, {}, [], "junk", {"a": "b"}])
  def test_segment_facts_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
      assert segment_facts(body, body) == []


  def test_time_column_comes_from_the_offline_segments_config() -> None:
      assert time_column(load("tableconfig-airlineStats.json")) == "DaysSinceEpoch"
      assert time_column(load("tableconfig-baseballStats.json")) is None


  @pytest.mark.parametrize("body", [None, {}, [], "junk", {"OFFLINE": "nope"}])
  def test_time_column_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
      assert time_column(body) is None


  def test_table_facts_gather_type_time_column_and_segments() -> None:
      facts = table_facts(
          "airlineStats",
          load("tableconfig-airlineStats.json"),
          load("seg-metadata-airlineStats-columns.json"),
          load("size-airlineStats.json"),
      )
      assert facts.table == "airlineStats"
      assert facts.types == frozenset({"OFFLINE"})
      assert facts.time_column == "DaysSinceEpoch"
      assert len(facts.segments) == 31
  ```
  Extend the module's import to
  `from lagaam.adapters.pinot.metadata import (row_estimate, segment_facts, table_facts, table_names, table_schema, table_types, time_column)`.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_metadata.py`
  Expect `ImportError: cannot import name 'segment_facts' from 'lagaam.adapters.pinot.metadata'`.

- [ ] **Step 4 — implementation.** Add to `server/src/lagaam/adapters/pinot/metadata.py`, after the imports:
  ```python
  from dataclasses import dataclass
  from typing import Any, Mapping
  ```
  and at the end of the module:
  ```python
  @dataclass(frozen=True)
  class SegmentFact:
      """One sealed segment's measurable size, as the controller reports it.

      Every field is optional because a fact the controller does not carry must
      stay absent rather than become a zero: a zero would quote a segment free.
      """

      name: str
      docs: int | None
      total_bytes: int | None
      start_ms: int | None
      end_ms: int | None
      column_bytes: Mapping[str, int]


  @dataclass(frozen=True)
  class TableFacts:
      """Everything about one table a quotation is built from."""

      table: str
      types: frozenset[str]
      time_column: str | None
      segments: tuple[SegmentFact, ...]


  def segment_facts(seg_metadata_json: Any, size_json: Any) -> list[SegmentFact]:
      """Per-segment docs, bytes, time range and per-column bytes.

      Bytes come from a different endpoint than docs, keyed by segment name, so
      a segment missing from the size report keeps its docs and loses its bytes.
      """
      if not isinstance(seg_metadata_json, dict):
          return []
      sizes = _segment_sizes(size_json)
      facts: list[SegmentFact] = []
      for key, body in seg_metadata_json.items():
          if not isinstance(body, dict):
              continue
          name = body.get("segmentName")
          if not isinstance(name, str) or not name:
              name = key if isinstance(key, str) else ""
          if not name:
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


  def time_column(config_json: Any) -> str | None:
      """The OFFLINE half's time column, which is what prunes segments."""
      if not isinstance(config_json, dict):
          return None
      for key in ("OFFLINE", "REALTIME"):
          half = config_json.get(key)
          if not isinstance(half, dict):
              continue
          segments_config = half.get("segmentsConfig")
          if not isinstance(segments_config, dict):
              continue
          name = segments_config.get("timeColumnName")
          if isinstance(name, str) and name:
              return name
      return None


  def table_facts(
      table: str,
      config_json: Any,
      seg_metadata_json: Any,
      size_json: Any,
  ) -> TableFacts:
      """One table's type, time column and segments, from three documents."""
      return TableFacts(
          table=table,
          types=table_types(config_json),
          time_column=time_column(config_json),
          segments=tuple(segment_facts(seg_metadata_json, size_json)),
      )


  def _segment_sizes(size_json: Any) -> dict[str, int]:
      """Segment name to reported bytes, from both halves of the size report."""
      if not isinstance(size_json, dict):
          return {}
      sizes: dict[str, int] = {}
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
              # Measured: a consuming segment reports -1, which is "unknown".
              reported = _positive_int(body.get("reportedSizeInBytes"), allow_zero=True)
              if reported is not None:
                  sizes[name] = reported
      return sizes


  def _column_bytes(columns_json: Any) -> Mapping[str, int]:
      """Per-column bytes, summing every index entry the segment carries.

      The entries differ by encoding — a RAW column has a forward_index and no
      dictionary — so the sum is over whatever is present, never a fixed set.
      """
      if not isinstance(columns_json, list):
          return {}
      totals: dict[str, int] = {}
      for column in columns_json:
          if not isinstance(column, dict):
              continue
          name = column.get("columnName")
          index_sizes = column.get("indexSizeMap")
          if not isinstance(name, str) or not name:
              continue
          if not isinstance(index_sizes, dict):
              continue
          total = 0
          for value in index_sizes.values():
              size = _positive_int(value, allow_zero=True)
              if size is not None:
                  total += size
          totals[name] = total
      return totals


  def _positive_int(value: Any, allow_zero: bool = False) -> int | None:
      """An int the controller means as a measurement, or None."""
      if isinstance(value, bool) or not isinstance(value, int):
          return None
      if value < 0 or (value == 0 and not allow_zero):
          return None
      return value
  ```

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/metadata.py \
          server/tests/adapters/pinot/test_pinot_metadata.py \
          server/tests/adapters/pinot/fixtures/seg-metadata-airlineStats-columns.json \
          server/tests/adapters/pinot/fixtures/seg-metadata-baseballStats-columns.json \
          server/tests/adapters/pinot/fixtures/size-airlineStats.json \
          server/tests/adapters/pinot/fixtures/size-baseballStats.json \
          server/tests/adapters/pinot/fixtures/tableconfig-baseballStats.json
  git commit -m "feat(pinot): read per-segment docs, bytes and per-column bytes

  A Pinot quotation has to be synthesised, so the facts it is synthesised
  from come first. Docs and time ranges are in the segment metadata, bytes
  are in a different endpoint keyed by segment name, and per-column bytes
  only appear when ?columns= is passed. A fact the controller does not carry
  stays None rather than becoming a zero, because a zero quotes a segment
  free — a consuming segment's -1 is the case that matters. Column bytes sum
  whatever index entries are present: a RAW column has a forward_index and no
  dictionary.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 2: Referenced tables and columns from the validated SQL

**Files:** `server/src/lagaam/adapters/pinot/names.py`, `server/tests/adapters/pinot/test_pinot_names.py`.

**Interfaces:**
- Consumes: already-validated SQL (three-part names, LIMIT present, no `SELECT *`).
- Produces:
  - `def referenced_tables(sql: str, catalog: str = "pinot") -> list[tuple[str, str]] | None` — `(database, table)` pairs, deduplicated, sorted. `None` means the SQL could not be read, so the caller must charge everything.
  - `def referenced_columns(sql: str) -> frozenset[str] | None` — lowercase bare column names. `None` means an unresolvable reference (a bare `*`, or SQL that would not re-parse), so the caller charges whole segments.

Column attribution is deliberately over-approximate: every `exp.Column` name in
the statement counts against every referenced table, matched later by lowercase
name against that table's own per-column bytes. A name that matches no table
costs nothing there; a table that matches no name at all falls back to whole
segments.

- [ ] **Step 1 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_names.py`:
  ```python
  def test_referenced_tables_are_database_table_pairs() -> None:
      assert referenced_tables(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      ) == [("default", "airlineStats")]


  def test_referenced_tables_deduplicate_and_sort() -> None:
      assert referenced_tables(
          "SELECT a.Carrier FROM pinot.default.airlineStats a "
          "JOIN pinot.default.baseballStats b ON a.Carrier = b.teamID "
          "JOIN pinot.default.airlineStats c ON a.Carrier = c.Carrier LIMIT 10"
      ) == [("default", "airlineStats"), ("default", "baseballStats")]


  def test_referenced_tables_refuse_a_foreign_catalog() -> None:
      with pytest.raises(TableNotFoundError):
          referenced_tables("SELECT x FROM other.default.t LIMIT 1")


  def test_referenced_tables_are_none_when_the_sql_cannot_be_read() -> None:
      assert referenced_tables("SELECT FROM WHERE ((((") is None


  def test_referenced_columns_are_lowercase_bare_names() -> None:
      assert referenced_columns(
          "SELECT a.Carrier, DaysSinceEpoch FROM pinot.default.airlineStats a "
          "WHERE a.Origin = 'SFO' LIMIT 10"
      ) == frozenset({"carrier", "dayssinceepoch", "origin"})


  def test_a_star_makes_the_columns_unresolvable() -> None:
      """validate_query rejects SELECT *, but count(*) and a.* still parse."""
      assert referenced_columns("SELECT * FROM pinot.default.airlineStats LIMIT 1") is None
      assert referenced_columns(
          "SELECT a.* FROM pinot.default.airlineStats a LIMIT 1"
      ) is None


  def test_a_count_star_is_not_an_unresolvable_column() -> None:
      assert referenced_columns(
          "SELECT count(*) FROM pinot.default.airlineStats LIMIT 1"
      ) == frozenset()


  def test_referenced_columns_are_none_when_the_sql_cannot_be_read() -> None:
      assert referenced_columns("SELECT FROM WHERE ((((") is None
  ```
  Extend the import to include `referenced_columns` and `referenced_tables`, and
  ensure `pytest` and `TableNotFoundError` are imported in that module.

- [ ] **Step 2 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_names.py`
  Expect `ImportError: cannot import name 'referenced_tables'`.

- [ ] **Step 3 — implementation.** Append to `server/src/lagaam/adapters/pinot/names.py`:
  ```python
  def referenced_tables(sql: str, catalog: str = "pinot") -> list[tuple[str, str]] | None:
      """Every (database, table) this SQL reads, deduplicated and sorted.

      None means the SQL did not re-parse, which charges the whole table rather
      than quoting a query nobody read.
      """
      try:
          tree = sqlglot.parse_one(sql, dialect=_DIALECT)
      except (sqlglot.errors.SqlglotError, RecursionError):
          return None
      found: set[tuple[str, str]] = set()
      for table in tree.find_all(exp.Table):
          if not table.name:
              continue
          table_catalog = table.catalog
          if table_catalog and table_catalog.lower() != catalog.lower():
              raise TableNotFoundError(
                  catalog=table_catalog, schema=table.db, table=table.name
              )
          # A bare name is a CTE the allowlist already vouched for, not a table.
          if not table.db:
              continue
          found.add((table.db, table.name))
      return sorted(found)


  def referenced_columns(sql: str) -> frozenset[str] | None:
      """Lowercase bare names of every column this SQL mentions.

      None means a reference nobody can resolve to a column list — a star, or
      SQL that did not re-parse — and the caller charges whole segments for it.
      """
      try:
          tree = sqlglot.parse_one(sql, dialect=_DIALECT)
      except (sqlglot.errors.SqlglotError, RecursionError):
          return None
      for star in tree.find_all(exp.Star):
          # count(*) names no column; a projected star names all of them.
          if not isinstance(star.parent, exp.Count):
              return None
      for column in tree.find_all(exp.Column):
          if isinstance(column.this, exp.Star):
              return None
      return frozenset(
          column.name.lower()
          for column in tree.find_all(exp.Column)
          if column.name
      )
  ```

- [ ] **Step 4 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 5 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/names.py server/tests/adapters/pinot/test_pinot_names.py
  git commit -m "feat(pinot): read the tables and columns a validated query touches

  The quotation charges per table and, inside a table, per column, so both
  lists come off the same AST the catalog strip already walks. Columns are
  deliberately over-approximate — every column name in the statement counts
  against every table, matched by lowercase name later — because charging a
  column to the wrong table only ever over-quotes. A star, or SQL that will
  not re-parse, resolves to nothing at all, which charges whole segments.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 3: The largest-k bound and column attribution in `quote.py`

**Files:** `server/src/lagaam/adapters/pinot/quote.py`, `server/tests/adapters/pinot/test_pinot_quote.py`.

**Interfaces:**
- Consumes: `TableFacts` (Task 1), referenced columns (Task 2), a surviving-segment count per table.
- Produces:
  - `def surviving_docs(facts: TableFacts, surviving: int | None) -> int | None` — the sum of the k largest segments' docs; `surviving is None` means all.
  - `def surviving_bytes(facts: TableFacts, surviving: int | None, columns: frozenset[str] | None) -> int | None`
  - `def quote(tables: Sequence[tuple[TableFacts, int | None]], columns: frozenset[str] | None, max_intermediate_rows: int | None) -> CostEstimate`

The two `surviving_*` functions sort **independently** — the k largest by docs
and, separately, the k largest by bytes. The pruning oracle says *how many*
segments survive, not *which*, so each dimension takes its own worst case.

- [ ] **Step 1 — failing test.** Create `server/tests/adapters/pinot/test_pinot_quote.py`:
  ```python
  """The quotation arithmetic: a bound that is never lower than the truth.

  Every number here is either hand-built to isolate one rule or read from the
  JSON captured off live Pinot 1.5.1, never transcribed from prose.
  """

  import json
  from pathlib import Path
  from typing import Any

  import pytest

  from lagaam.adapters.pinot.metadata import SegmentFact, TableFacts, table_facts
  from lagaam.adapters.pinot.quote import quote, surviving_bytes, surviving_docs

  FIXTURES = Path(__file__).parent / "fixtures"


  def load(name: str) -> Any:
      return json.loads((FIXTURES / name).read_text())


  def seg(name: str, docs: int | None, total: int | None, **columns: int) -> SegmentFact:
      return SegmentFact(
          name=name,
          docs=docs,
          total_bytes=total,
          start_ms=None,
          end_ms=None,
          column_bytes=dict(columns),
      )


  def facts(*segments: SegmentFact, types: frozenset[str] = frozenset({"OFFLINE"})) -> TableFacts:
      return TableFacts(table="t", types=types, time_column=None, segments=segments)


  def airline() -> TableFacts:
      return table_facts(
          "airlineStats",
          load("tableconfig-airlineStats.json"),
          load("seg-metadata-airlineStats-columns.json"),
          load("size-airlineStats.json"),
      )


  def baseball() -> TableFacts:
      return table_facts(
          "baseballStats",
          load("tableconfig-baseballStats.json"),
          load("seg-metadata-baseballStats-columns.json"),
          load("size-baseballStats.json"),
      )


  def test_surviving_docs_charge_the_largest_k_not_the_first_k() -> None:
      table = facts(seg("a", 10, 1), seg("b", 500, 1), seg("c", 20, 1))
      assert surviving_docs(table, 2) == 520
      assert surviving_docs(table, 1) == 500


  def test_surviving_none_charges_every_segment() -> None:
      table = facts(seg("a", 10, 1), seg("b", 500, 1))
      assert surviving_docs(table, None) == 510


  def test_a_surviving_count_over_the_segment_count_charges_them_all() -> None:
      table = facts(seg("a", 10, 1), seg("b", 500, 1))
      assert surviving_docs(table, 99) == 510


  def test_docs_and_bytes_pick_their_largest_k_independently() -> None:
      """The oracle says how many survive, not which, so each takes its worst."""
      table = facts(seg("a", 100, 1, x=1), seg("b", 1, 100, x=100))
      assert surviving_docs(table, 1) == 100
      assert surviving_bytes(table, 1, frozenset({"x"})) == 100


  def test_bytes_charge_only_the_referenced_columns() -> None:
      table = facts(seg("a", 10, 999, wanted=7, ignored=500))
      assert surviving_bytes(table, None, frozenset({"wanted"})) == 7


  def test_a_table_matching_no_referenced_column_falls_back_to_whole_segments() -> None:
      table = facts(seg("a", 10, 999, other=7))
      assert surviving_bytes(table, None, frozenset({"absent"})) == 999


  def test_unresolvable_columns_fall_back_to_whole_segments() -> None:
      table = facts(seg("a", 10, 999, other=7))
      assert surviving_bytes(table, None, None) == 999


  def test_bytes_are_none_when_the_fallback_has_no_segment_size() -> None:
      table = facts(seg("a", 10, None, other=7))
      assert surviving_bytes(table, None, None) is None


  def test_a_segment_without_docs_makes_the_doc_bound_unknown() -> None:
      table = facts(seg("a", 10, 1), seg("b", None, 1))
      assert surviving_docs(table, None) is None


  def test_the_airline_time_filter_bound_is_above_what_execution_scanned() -> None:
      """Measured: the same query really scanned 1,101 docs over 3 segments."""
      assert surviving_docs(airline(), 3) == 1234
      assert surviving_docs(airline(), None) == 9746


  def test_the_airline_byte_bound_charges_only_the_two_captured_columns() -> None:
      wanted = frozenset({"carrier", "dayssinceepoch"})
      whole = surviving_bytes(airline(), None, wanted)
      assert whole is not None
      assert whole < 4861355
      assert surviving_bytes(airline(), 3, wanted) is not None


  def test_quote_sums_over_tables_and_is_high_confidence_with_bytes() -> None:
      estimate = quote(
          [(airline(), 3), (baseball(), None)],
          frozenset({"carrier", "teamid"}),
          max_intermediate_rows=1234,
      )
      assert estimate.row_estimate == 1234 + 97889
      assert estimate.scanned_bytes is not None
      assert estimate.confidence == "high"
      assert estimate.max_intermediate_rows == 1234


  def test_a_realtime_half_is_quoted_low_however_good_the_numbers_look() -> None:
      """U12 charges consuming segments; until then a REALTIME half is unknown."""
      table = facts(seg("a", 10, 100, x=5), types=frozenset({"OFFLINE", "REALTIME"}))
      estimate = quote([(table, None)], frozenset({"x"}), max_intermediate_rows=10)
      assert estimate.confidence == "low"
      assert estimate.scanned_bytes is None


  def test_one_unpriceable_table_costs_the_whole_byte_quote() -> None:
      good = facts(seg("a", 10, 100, x=5))
      bad = facts(seg("b", 10, None, y=5))
      estimate = quote([(good, None), (bad, None)], None, max_intermediate_rows=20)
      assert estimate.scanned_bytes is None
      assert estimate.confidence == "low"
      assert estimate.row_estimate == 20


  def test_quote_with_no_tables_is_low_confidence() -> None:
      assert quote([], frozenset(), None).confidence == "low"
  ```

- [ ] **Step 2 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_quote.py`
  Expect `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.quote'`.

- [ ] **Step 3 — implementation.** Create `server/src/lagaam/adapters/pinot/quote.py`:
  ```python
  """Table facts to a CostEstimate. PURE: no I/O, and it never guesses.

  Pinot reports no bytes and no trustworthy rows before execution, so the
  quotation is synthesised here from static segment metadata plus one
  engine-authoritative number: how many segments survive the predicate.

  The oracle says how many survive, not which. Charging the k largest is
  therefore an upper bound on any k that could survive — and docs and bytes
  take their own k largest, because the segment with the most rows is not
  always the one with the most bytes.

  A number that cannot be bounded is None, which the budget gate denies. That
  is the whole contract: this module never returns a figure it cannot defend.
  """

  from collections.abc import Sequence

  from lagaam.adapters.pinot.metadata import TableFacts
  from lagaam.core.models import CostEstimate


  def surviving_docs(facts: TableFacts, surviving: int | None) -> int | None:
      """Docs in the k largest segments by docs, or None if any is unknown.

      One unknown segment poisons the sum: the others do not bound it.
      """
      counts = [segment.docs for segment in facts.segments]
      if not counts or any(count is None for count in counts):
          return None
      known = sorted((count for count in counts if count is not None), reverse=True)
      return sum(known[: _k(surviving, len(known))])


  def surviving_bytes(
      facts: TableFacts, surviving: int | None, columns: frozenset[str] | None
  ) -> int | None:
      """Bytes in the k largest segments, charging only referenced columns.

      A table whose column attribution finds nothing is charged whole segments
      rather than nothing at all — the fallback the spec requires.
      """
      sizes = _column_sizes(facts, columns)
      if sizes is None:
          sizes = [segment.total_bytes for segment in facts.segments]
      if not sizes or any(size is None for size in sizes):
          return None
      known = sorted((size for size in sizes if size is not None), reverse=True)
      return sum(known[: _k(surviving, len(known))])


  def quote(
      tables: Sequence[tuple[TableFacts, int | None]],
      columns: frozenset[str] | None,
      max_intermediate_rows: int | None,
  ) -> CostEstimate:
      """One CostEstimate over every table the query reads.

      Rows and bytes each fail independently, and both fail whole: one table
      nobody could size makes the sum a bound on nothing.
      """
      if not tables:
          return CostEstimate(
              max_intermediate_rows=max_intermediate_rows, confidence="low"
          )
      # A consuming segment reports zero docs and -1 bytes, so a REALTIME half
      # is an unbounded unknown until U12 charges it at its flush threshold.
      realtime = any("REALTIME" in facts.types for facts, _ in tables)
      rows = _total(surviving_docs(facts, k) for facts, k in tables)
      total_bytes = (
          None
          if realtime
          else _total(surviving_bytes(facts, k, columns) for facts, k in tables)
      )
      return CostEstimate(
          scanned_bytes=total_bytes,
          row_estimate=rows,
          max_intermediate_rows=max_intermediate_rows,
          confidence="low" if total_bytes is None else "high",
      )


  def _k(surviving: int | None, available: int) -> int:
      """How many segments to charge: all of them unless the oracle said fewer."""
      if surviving is None or surviving >= available:
          return available
      # The oracle has been seen to prune every segment; something is always read.
      return max(1, surviving)


  def _column_sizes(
      facts: TableFacts, columns: frozenset[str] | None
  ) -> list[int | None] | None:
      """Per-segment bytes for the referenced columns, or None to fall back."""
      if columns is None:
          return None
      sizes: list[int | None] = []
      matched = False
      for segment in facts.segments:
          total = 0
          for name, size in segment.column_bytes.items():
              if name.lower() in columns:
                  total += size
                  matched = True
          sizes.append(total)
      return sizes if matched else None


  def _total(values: Iterable[int | None]) -> int | None:
      """Sum, unless any part is unknown — then the whole sum is unknown."""
      total = 0
      for value in values:
          if value is None:
              return None
          total += value
      return total
  ```
  The module's first import line is
  `from collections.abc import Iterable, Sequence`.

- [ ] **Step 4 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 5 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/quote.py server/tests/adapters/pinot/test_pinot_quote.py
  git commit -m "feat(pinot): bound a scan by the largest segments that could survive

  The pruning oracle says how many segments survive a predicate, never which,
  so charging the k largest is the only sound reading of it — and docs and
  bytes each take their own k largest, because the segment with the most rows
  is not the one with the most bytes. Bytes charge only the columns the query
  names; a table that matches none of them is charged whole segments rather
  than nothing. One segment nobody could size makes the whole sum unknown,
  because the rest do not bound it. A REALTIME half is quoted low until U12
  charges its consuming segments at the flush threshold.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 4: `plan.py` — max intermediate rows from the multi-stage `rels[]`

**Files:** `server/src/lagaam/adapters/pinot/plan.py`, `server/tests/adapters/pinot/test_pinot_plan.py`, three new fixtures.

**Interfaces:**
- Consumes: the `PLAN` cell of a multi-stage `EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR` response (a JSON string), plus a leaf-size lookup.
- Produces: `def max_intermediate_rows(plan_json: str, leaf_docs: Mapping[str, int | None]) -> int | None` — keys of `leaf_docs` are lowercase `"database.table"`.

The rule, per ADR 0004: a scan node is its table's surviving docs; a join whose
condition carries no equality is the **product** of its children; a join with an
equality is the **max**; every other node is the max of its children; the answer
is the max over all nodes. Anything unreadable is `None`.

- [ ] **Step 1 — capture the fixtures.** From `server/tests/adapters/pinot/fixtures`:
  ```bash
  mse () { curl -s -X POST http://localhost:8000/query/sql -H 'Content-Type: application/json' \
      -d "{\"sql\":\"$1\",\"queryOptions\":\"useMultistageEngine=true\"}" | python3 -m json.tool; }
  mse "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT count(*) FROM default.airlineStats a, default.baseballStats b" > explain-mse-crossjoin.json
  mse "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT count(*) FROM default.airlineStats a JOIN default.baseballStats b ON a.Carrier = b.teamID" > explain-mse-equijoin.json
  mse "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR SELECT Carrier, count(*) FROM default.airlineStats WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10" > explain-mse-singletable.json
  ```
  Confirm each has `resultTable.dataSchema.columnNames == ["SQL", "PLAN", "RULE_TIMINGS"]`,
  that the cross join's `LogicalJoin` condition is `{"literal": true, ...}` with no
  `op` key, and that the equi join's carries `"kind": "EQUALS"`.

- [ ] **Step 2 — failing test.** Create `server/tests/adapters/pinot/test_pinot_plan.py`:
  ```python
  """ADR 0004's product/max rule over a plan that carries shape but no sizes.

  Pinot's Calcite rowcounts are a constant 100 per scan, so this module reads
  only the shape — which tables, which joins, which conditions — and takes its
  sizes from the segment metadata the quotation already fetched.
  """

  import json
  from pathlib import Path
  from typing import Any

  import pytest

  from lagaam.adapters.pinot.plan import max_intermediate_rows

  FIXTURES = Path(__file__).parent / "fixtures"

  AIRLINE_DOCS = 9746
  BASEBALL_DOCS = 97889
  LEAVES = {
      "default.airlinestats": AIRLINE_DOCS,
      "default.baseballstats": BASEBALL_DOCS,
  }


  def plan_cell(name: str) -> str:
      body = json.loads((FIXTURES / name).read_text())
      cell = body["resultTable"]["rows"][0][1]
      assert isinstance(cell, str)
      return cell


  def test_a_cross_join_is_charged_the_product_of_its_children() -> None:
      assert (
          max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), LEAVES)
          == AIRLINE_DOCS * BASEBALL_DOCS
      )


  def test_an_equi_join_is_charged_the_max_of_its_children() -> None:
      assert (
          max_intermediate_rows(plan_cell("explain-mse-equijoin.json"), LEAVES)
          == BASEBALL_DOCS
      )


  def test_a_single_table_plan_is_charged_its_scan() -> None:
      assert (
          max_intermediate_rows(
              plan_cell("explain-mse-singletable.json"),
              {"default.airlinestats": 1234},
          )
          == 1234
      )


  def test_a_table_with_no_leaf_size_makes_the_answer_unknown() -> None:
      assert max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), {}) is None
      assert (
          max_intermediate_rows(
              plan_cell("explain-mse-crossjoin.json"),
              {"default.airlinestats": AIRLINE_DOCS, "default.baseballstats": None},
          )
          is None
      )


  def test_the_rowcount_attributes_are_never_read() -> None:
      """Pinot prices every scan at a constant 100; reading it admits a cross join."""
      answer = max_intermediate_rows(plan_cell("explain-mse-crossjoin.json"), LEAVES)
      assert answer not in (100, 10000)
      assert answer == AIRLINE_DOCS * BASEBALL_DOCS


  def test_a_node_without_inputs_consumes_the_node_before_it() -> None:
      plan = json.dumps(
          {
              "rels": [
                  {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "t"], "inputs": []},
                  {"id": "1", "relOp": "LogicalProject"},
              ]
          }
      )
      assert max_intermediate_rows(plan, {"default.t": 42}) == 42


  def test_a_join_whose_condition_nests_an_equality_is_a_max() -> None:
      plan = json.dumps(
          {
              "rels": [
                  {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                  {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                  {
                      "id": "2",
                      "relOp": "LogicalJoin",
                      "joinType": "inner",
                      "inputs": ["0", "1"],
                      "condition": {
                          "op": {"name": "AND", "kind": "AND"},
                          "operands": [
                              {"op": {"name": ">", "kind": "GREATER_THAN"}, "operands": []},
                              {"op": {"name": "=", "kind": "EQUALS"}, "operands": []},
                          ],
                      },
                  },
              ]
          }
      )
      assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 500


  def test_an_inequality_join_is_charged_the_product() -> None:
      plan = json.dumps(
          {
              "rels": [
                  {"id": "0", "relOp": "PinotLogicalTableScan", "table": ["default", "a"], "inputs": []},
                  {"id": "1", "relOp": "PinotLogicalTableScan", "table": ["default", "b"], "inputs": []},
                  {
                      "id": "2",
                      "relOp": "LogicalJoin",
                      "joinType": "inner",
                      "inputs": ["0", "1"],
                      "condition": {"op": {"name": ">", "kind": "GREATER_THAN"}, "operands": []},
                  },
              ]
          }
      )
      assert max_intermediate_rows(plan, {"default.a": 10, "default.b": 500}) == 5000


  @pytest.mark.parametrize(
      "plan", ["", "not json", "[]", "null", json.dumps({"rels": "nope"}), json.dumps({})]
  )
  def test_a_plan_that_cannot_be_read_is_no_answer(plan: str) -> None:
      assert max_intermediate_rows(plan, LEAVES) is None


  def test_a_cycle_in_the_inputs_does_not_hang() -> None:
      plan = json.dumps(
          {
              "rels": [
                  {"id": "0", "relOp": "LogicalProject", "inputs": ["1"]},
                  {"id": "1", "relOp": "LogicalProject", "inputs": ["0"]},
              ]
          }
      )
      assert max_intermediate_rows(plan, LEAVES) is None
  ```

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_plan.py`
  Expect `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.plan'`.

- [ ] **Step 4 — implementation.** Create `server/src/lagaam/adapters/pinot/plan.py`:
  ```python
  """The widest row count anywhere in Pinot's multi-stage plan. PURE.

  Pinot's Calcite cost model has no table statistics wired in: every table scan
  reports rowcount 100.0 whatever the table's real size, and a 954-million-row
  cross join is priced at 10,000. So this module reads the plan for shape only
  — which tables, which joins, which conditions — and takes its sizes from the
  segment metadata the quotation already fetched. The rowcount and cumulative
  cost attributes are never read.

  The rule is ADR 0004's, applied to a plan that carries shape but no sizes: a
  join without an equality multiplies its inputs, a join with one cannot be
  proven to, and everything else passes its widest input through.

  rels[] is a flat topological list, not a tree. A node's children are its
  "inputs" ids; a node with no inputs key at all consumes the node immediately
  before it, which is how Calcite serialises a linear chain.
  """

  import json
  from typing import Any, Mapping

  # A plan this deep is a machine's, not an analyst's.
  _MAX_DEPTH = 400


  def max_intermediate_rows(
      plan_json: str, leaf_docs: Mapping[str, int | None]
  ) -> int | None:
      """The widest row count any node in this plan would build, or None.

      None means the plan could not be read or a table could not be sized —
      no quote, which the budget treats as a denial rather than as cheap.
      """
      rels = _rels(plan_json)
      if rels is None:
          return None
      by_id: dict[str, dict[str, Any]] = {}
      order: list[str] = []
      previous: dict[str, str | None] = {}
      last: str | None = None
      for rel in rels:
          rel_id = rel.get("id")
          if not isinstance(rel_id, str) or rel_id in by_id:
              return None
          by_id[rel_id] = rel
          previous[rel_id] = last
          order.append(rel_id)
          last = rel_id
      widest: list[int] = []
      memo: dict[str, int | None] = {}
      for rel_id in order:
          rows = _rows(rel_id, by_id, previous, leaf_docs, memo, widest, 0)
          if rows is None:
              return None
      return max(widest) if widest else None


  def _rels(plan_json: str) -> list[dict[str, Any]] | None:
      try:
          body = json.loads(plan_json)
      except (json.JSONDecodeError, TypeError, ValueError, RecursionError):
          return None
      if not isinstance(body, dict):
          return None
      rels = body.get("rels")
      if not isinstance(rels, list) or not rels:
          return None
      if not all(isinstance(rel, dict) for rel in rels):
          return None
      return [rel for rel in rels if isinstance(rel, dict)]


  def _rows(
      rel_id: str,
      by_id: dict[str, dict[str, Any]],
      previous: dict[str, str | None],
      leaf_docs: Mapping[str, int | None],
      memo: dict[str, int | None],
      widest: list[int],
      depth: int,
  ) -> int | None:
      """This node's rows, recording every knowable count into ``widest``."""
      if depth > _MAX_DEPTH:
          return None
      if rel_id in memo:
          return memo[rel_id]
      # Claim the slot before recursing so a cycle terminates instead of hanging.
      memo[rel_id] = None
      rel = by_id.get(rel_id)
      if rel is None:
          return None
      children = _children(rel_id, rel, previous)
      if children is None:
          return None
      child_rows: list[int] = []
      for child in children:
          rows = _rows(child, by_id, previous, leaf_docs, memo, widest, depth + 1)
          if rows is None:
              return None
          child_rows.append(rows)
      if not child_rows:
          answer = _leaf_rows(rel, leaf_docs)
      elif _is_join(rel) and not _has_equality(rel.get("condition")):
          # A join the plan cannot prove is keyed pairs its inputs; charging
          # less is how a laundered cross join reads as one table's size.
          product = 1
          for rows in child_rows:
              product *= rows
          answer = product
      else:
          answer = max(child_rows)
      memo[rel_id] = answer
      if answer is not None:
          widest.append(answer)
      return answer


  def _children(
      rel_id: str, rel: dict[str, Any], previous: dict[str, str | None]
  ) -> list[str] | None:
      """This node's input ids, or [] for a leaf, or None if unreadable."""
      inputs = rel.get("inputs")
      if inputs is None:
          # No inputs key: Calcite's shorthand for "the node just before me".
          before = previous.get(rel_id)
          return [] if before is None else [before]
      if not isinstance(inputs, list):
          return None
      if not all(isinstance(value, str) for value in inputs):
          return None
      return [value for value in inputs if isinstance(value, str)]


  def _leaf_rows(
      rel: dict[str, Any], leaf_docs: Mapping[str, int | None]
  ) -> int | None:
      """A scan's rows: its table's surviving docs, looked up by two-part name."""
      table = rel.get("table")
      if not isinstance(table, list) or not table:
          return None
      parts = [part for part in table if isinstance(part, str) and part]
      if len(parts) != len(table):
          return None
      return leaf_docs.get(".".join(parts).lower())


  def _is_join(rel: dict[str, Any]) -> bool:
      rel_op = rel.get("relOp")
      if not isinstance(rel_op, str):
          return False
      return rel_op.rsplit(".", 1)[-1].lower().endswith("join")


  def _has_equality(condition: Any) -> bool:
      """True if an equality appears anywhere in this join condition.

      A cross join's condition is the bare literal true, with no op at all.
      """
      if not isinstance(condition, dict):
          return False
      op = condition.get("op")
      if isinstance(op, dict) and op.get("kind") == "EQUALS":
          return True
      operands = condition.get("operands")
      if isinstance(operands, list):
          return any(_has_equality(operand) for operand in operands)
      return False
  ```
  Note `_JOIN_OPS` is not needed — `_is_join` matches on the relOp's suffix —
  so do not add it. The module's imports are exactly
  `import json` and `from typing import Any, Mapping`.

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/plan.py \
          server/tests/adapters/pinot/test_pinot_plan.py \
          server/tests/adapters/pinot/fixtures/explain-mse-crossjoin.json \
          server/tests/adapters/pinot/fixtures/explain-mse-equijoin.json \
          server/tests/adapters/pinot/fixtures/explain-mse-singletable.json
  git commit -m "feat(pinot): price the widest step from the plan's shape, not its rowcounts

  Pinot's Calcite cost model has no statistics wired in: every table scan
  reports 100 rows whatever the table holds, and it prices a 954-million-row
  cross join at 10,000. A gate reading those numbers would admit exactly the
  query it exists to stop, so this walker reads the plan for shape only and
  takes its sizes from the segment metadata. ADR 0004's rule then applies: a
  join with no equality in its condition is the product of its inputs, one
  with an equality is their max.

  rels[] is a flat topological list, and a node with no inputs key consumes
  the node before it — Calcite's shorthand for a linear chain. A cycle in the
  ids answers None rather than recursing forever.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 5: The pruning oracle

**Files:** `server/src/lagaam/adapters/pinot/quote.py`, `server/src/lagaam/adapters/pinot/client.py`, `server/tests/adapters/pinot/test_pinot_quote.py`, `server/tests/adapters/pinot/test_pinot_client.py`, three new fixtures.

**Interfaces:**
- Consumes: a parsed single-stage `EXPLAIN PLAN FOR` broker response.
- Produces: `def surviving_segments(explain_json: Any) -> int | None` in `quote.py` — `None` means "could not read it", which the caller turns into k = all.
- Also: `client.controller_get`'s `params` widened to `dict[str, str | list[str]] | None`.

**The rule, and why.** Surviving =
`numSegmentsQueried - max(numSegmentsPrunedByBroker, numSegmentsPrunedByServer,
numSegmentsPrunedByValue, numSegmentsPrunedByLimit)`, clamped to at least 1.
Measured (see "Measured facts" above), `ByServer` is the total of the server-side
pruning and `ByValue`/`ByLimit` are non-overlapping breakdowns of it: the
time-filtered query reports `ByServer: 28, ByValue: 28, ByLimit: 0`, so summing
would claim 56 of 31 segments pruned and quote a negative scan. Measurement §6
separately records the same predicate registering only as `ByValue` with
`ByServer: 0`, so no single counter can be read alone either. `max` is exact
where they nest and conservative where they do not — it never claims more pruned
than actually was, which is what keeps the quote an upper bound.

- [ ] **Step 1 — capture the fixtures.** From `server/tests/adapters/pinot/fixtures`:
  ```bash
  v1 () { curl -s -X POST http://localhost:8000/query/sql -H 'Content-Type: application/json' \
      -d "{\"sql\":\"$1\"}" | python3 -m json.tool; }
  v1 "EXPLAIN PLAN FOR SELECT Carrier, count(*) FROM default.airlineStats WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10" > explain-v1-timefilter.json
  v1 "EXPLAIN PLAN FOR SELECT Carrier, count(*) FROM default.airlineStats GROUP BY Carrier LIMIT 10" > explain-v1-nofilter.json
  v1 "EXPLAIN PLAN FOR SELECT Carrier FROM default.airlineStats LIMIT 10" > explain-v1-limitpruned.json
  ```
  Confirm the three report, respectively:
  `ByServer 28 / ByValue 28 / ByLimit 0`, `0 / 0 / 0`, and `30 / 0 / 30`; and that
  all three report `numSegmentsQueried: 31` and `numDocsScanned: 0`.

- [ ] **Step 2 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_quote.py`
  (add `surviving_segments` to the `quote` import):
  ```python
  def test_a_time_filter_survives_three_of_thirty_one_segments() -> None:
      assert surviving_segments(load("explain-v1-timefilter.json")) == 3


  def test_no_filter_survives_every_segment() -> None:
      assert surviving_segments(load("explain-v1-nofilter.json")) == 31


  def test_a_limit_prune_counts_as_pruning_too() -> None:
      """Measured: this really does process one segment and scan ten docs."""
      assert surviving_segments(load("explain-v1-limitpruned.json")) == 1


  def test_the_pruned_counters_are_maxed_never_summed() -> None:
      """ByServer is the total; ByValue and ByLimit break it down, so a sum
      would claim 56 of 31 pruned and quote a negative scan."""
      assert (
          surviving_segments(
              {
                  "numSegmentsQueried": 31,
                  "numSegmentsPrunedByServer": 28,
                  "numSegmentsPrunedByValue": 28,
                  "numSegmentsPrunedByLimit": 0,
                  "numDocsScanned": 0,
              }
          )
          == 3
      )


  def test_a_counter_only_ever_seen_alone_is_still_read() -> None:
      """Measurement 6: the same predicate once registered only as ByValue."""
      assert (
          surviving_segments(
              {
                  "numSegmentsQueried": 31,
                  "numSegmentsPrunedByServer": 0,
                  "numSegmentsPrunedByValue": 28,
                  "numDocsScanned": 0,
              }
          )
          == 3
      )


  def test_every_segment_pruned_still_charges_one() -> None:
      assert (
          surviving_segments(
              {
                  "numSegmentsQueried": 31,
                  "numSegmentsPrunedByServer": 31,
                  "numDocsScanned": 0,
              }
          )
          == 1
      )


  def test_an_explain_that_scanned_anything_is_not_an_oracle() -> None:
      """EXPLAIN must never execute; if it did, we misread the statement."""
      assert (
          surviving_segments(
              {
                  "numSegmentsQueried": 31,
                  "numSegmentsPrunedByServer": 28,
                  "numDocsScanned": 1,
              }
          )
          is None
      )


  def test_an_explain_carrying_an_exception_is_no_oracle() -> None:
      assert (
          surviving_segments(
              {
                  "numSegmentsQueried": 31,
                  "numDocsScanned": 0,
                  "exceptions": [{"errorCode": 150, "message": "multi-stage only"}],
              }
          )
          is None
      )


  @pytest.mark.parametrize(
      "body",
      [
          None,
          {},
          [],
          "junk",
          {"numSegmentsQueried": 0, "numDocsScanned": 0},
          {"numSegmentsQueried": "31", "numDocsScanned": 0},
      ],
  )
  def test_surviving_segments_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
      assert surviving_segments(body) is None
  ```
  And append to `server/tests/adapters/pinot/test_pinot_client.py` a test in that
  module's existing `MockTransport` style, asserting that
  `controller_get("/segments/t/metadata", params={"columns": ["A", "B"]})` issues a
  request whose URL is `.../segments/t/metadata?columns=A&columns=B`.

- [ ] **Step 3 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_quote.py tests/adapters/pinot/test_pinot_client.py`
  Expect `ImportError: cannot import name 'surviving_segments'`.

- [ ] **Step 4 — implementation.** In `client.py`, widen the signature only:
  ```python
      async def controller_get(
          self,
          path: str,
          params: dict[str, str | list[str]] | None = None,
          database: str | None = None,
      ) -> Any:
  ```
  (httpx renders a list value as repeated query parameters, which is exactly what
  `?columns=A&columns=B` needs; the body of the method is unchanged.)

  Append to `quote.py`:
  ```python
  # Every counter Pinot may report a prune under. They are not additive: the
  # server-side total and its by-value / by-limit breakdowns all appear at once.
  _PRUNED_COUNTERS = (
      "numSegmentsPrunedByBroker",
      "numSegmentsPrunedByServer",
      "numSegmentsPrunedByValue",
      "numSegmentsPrunedByLimit",
      "numSegmentsPrunedInvalid",
  )


  def surviving_segments(explain_json: Any) -> int | None:
      """How many segments survive the predicate, from a single-stage EXPLAIN.

      Measured on 1.5.1, the pruning counters nest rather than add: a time
      filter reported ByServer 28 with ByValue 28 of 31 segments, so summing
      would claim 56 pruned and quote a negative scan. The largest single
      counter is exact where they nest and conservative where they do not.

      None means "no oracle" — the caller then charges every segment.
      """
      if not isinstance(explain_json, dict):
          return None
      if explain_json.get("exceptions"):
          return None
      # EXPLAIN must plan without running; anything scanned means we misread it.
      scanned = explain_json.get("numDocsScanned")
      if isinstance(scanned, bool) or not isinstance(scanned, int) or scanned != 0:
          return None
      queried = explain_json.get("numSegmentsQueried")
      if isinstance(queried, bool) or not isinstance(queried, int) or queried <= 0:
          return None
      pruned = 0
      for key in _PRUNED_COUNTERS:
          value = explain_json.get(key)
          if isinstance(value, bool) or not isinstance(value, int) or value < 0:
              continue
          pruned = max(pruned, value)
      return max(1, queried - min(pruned, queried))
  ```

- [ ] **Step 5 — run, expect PASS.**
  `cd server && uv run pytest -q tests/adapters/pinot/ && uv run mypy`

- [ ] **Step 6 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/quote.py \
          server/src/lagaam/adapters/pinot/client.py \
          server/tests/adapters/pinot/test_pinot_quote.py \
          server/tests/adapters/pinot/test_pinot_client.py \
          server/tests/adapters/pinot/fixtures/explain-v1-timefilter.json \
          server/tests/adapters/pinot/fixtures/explain-v1-nofilter.json \
          server/tests/adapters/pinot/fixtures/explain-v1-limitpruned.json
  git commit -m "feat(pinot): read how many segments survive from the single-stage EXPLAIN

  The one honest pre-execution signal Pinot gives: EXPLAIN performs real
  segment pruning and reports it without scanning a document. Measured, a time
  filter on airlineStats prunes 28 of 31 before anything runs.

  The counters are maxed, not summed. numSegmentsPrunedByServer is the total
  of the server-side pruning and ByValue and ByLimit are breakdowns of it — the
  time filter reports 28 and 28 at once, so a sum would claim 56 of 31
  segments pruned and quote a negative scan. Measurement 6 separately caught
  the same predicate registering only as ByValue with ByServer 0, so no single
  counter can be read alone either. The largest is exact where they nest and
  conservative where they do not.

  numSegmentsProcessed is not read: it is 0 under EXPLAIN in every shape
  measured, because nothing executes. An EXPLAIN that scanned a document, or
  carried an exception, is no oracle at all and charges every segment.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 6: `engine.estimate_cost` composition

**Files:** `server/src/lagaam/adapters/pinot/engine.py`, `server/tests/adapters/pinot/test_pinot_engine.py`.

**Interfaces:**
- Consumes: validated SQL.
- Produces: `async def estimate_cost(self, sql: str) -> CostEstimate` — the port method, replacing the stub.

Order of operations, all before any network call where possible:
1. `has_unpriceable_shape(sql, dialect)` or `scan_counts_saturated(sql, dialect)` → `CostEstimate(confidence="low")`.
2. `two_part_sql(sql, self.CATALOG)` and `referenced_tables(sql, self.CATALOG)`; `None` tables → low.
3. Per table: `GET /tables/{t}`, `GET /segments/{t}/metadata?columns=<referenced>`, `GET /tables/{t}/size` → `table_facts`.
4. Pruning oracle, **only when exactly one table is referenced**: `EXPLAIN PLAN FOR <two-part sql>` on the broker with no multi-stage option; otherwise k = all.
5. `EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR <two-part sql>` on the multi-stage engine → `plan.max_intermediate_rows` with the per-table surviving docs as leaf sizes.
6. `quote.quote(...)`, then multiply `max_intermediate_rows` by `generator_fanout(sql, dialect)` exactly as the Trino adapter does.

Every transport failure inside `estimate_cost` degrades to a low-confidence quote
rather than raising: a quotation that cannot be built is a denial at the gate,
which is the safe answer, and an `EngineError` would read to the agent as an
outage rather than as an unpriceable query.

- [ ] **Step 1 — failing test.** Append to `server/tests/adapters/pinot/test_pinot_engine.py`, in that module's existing `MockTransport` style. Route by path and body:
  ```python
  def _quote_routes(request: httpx.Request) -> httpx.Response:
      """Controller and broker answers for a quotation, from captured JSON."""
      path = request.url.path
      if path == "/query/sql":
          body = json.loads(request.content)
          sql = body["sql"]
          if "AS JSON" in sql:
              return httpx.Response(200, json=load("explain-mse-singletable.json"))
          return httpx.Response(200, json=load("explain-v1-timefilter.json"))
      if path.endswith("/size"):
          return httpx.Response(200, json=load("size-airlineStats.json"))
      if path.startswith("/segments/"):
          return httpx.Response(200, json=load("seg-metadata-airlineStats-columns.json"))
      if path == "/tables/airlineStats":
          return httpx.Response(200, json=load("tableconfig-airlineStats.json"))
      return httpx.Response(404, json={})


  async def test_estimate_cost_quotes_the_surviving_segments() -> None:
      engine = PinotEngine(transport=httpx.MockTransport(_quote_routes))
      estimate = await engine.estimate_cost(
          "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10"
      )
      assert estimate.row_estimate == 1234
      assert estimate.scanned_bytes is not None
      assert estimate.confidence == "high"
      assert estimate.max_intermediate_rows == 1234


  async def test_the_pruning_oracle_asks_only_the_single_stage_engine() -> None:
      """A multi-stage EXPLAIN would return no pruning counters at all."""
      seen: list[str] = []

      def routes(request: httpx.Request) -> httpx.Response:
          if request.url.path == "/query/sql":
              body = json.loads(request.content)
              seen.append(body.get("queryOptions", ""))
          return _quote_routes(request)

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch > 16090 LIMIT 10"
      )
      assert any("useMultistageEngine=true" not in o for o in seen)
      assert any("useMultistageEngine=true" in o for o in seen)


  async def test_the_oracle_sends_a_two_part_name() -> None:
      seen: list[str] = []

      def routes(request: httpx.Request) -> httpx.Response:
          if request.url.path == "/query/sql":
              seen.append(json.loads(request.content)["sql"])
          return _quote_routes(request)

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      )
      assert seen
      assert all("pinot.default" not in sql for sql in seen)
      assert all(sql.startswith("EXPLAIN") for sql in seen)


  async def test_a_join_skips_the_oracle_and_charges_every_segment() -> None:
      """Single-stage EXPLAIN refuses a join outright, so k is all."""
      asked: list[str] = []

      def routes(request: httpx.Request) -> httpx.Response:
          path = request.url.path
          if path == "/query/sql":
              sql = json.loads(request.content)["sql"]
              asked.append(sql)
              if "AS JSON" in sql:
                  return httpx.Response(200, json=load("explain-mse-crossjoin.json"))
              return httpx.Response(
                  200,
                  json={"exceptions": [{"errorCode": 150, "message": "multi-stage only"}]},
              )
          if path.endswith("/size"):
              name = path.split("/")[2]
              return httpx.Response(200, json=load(f"size-{name}.json"))
          if path.startswith("/segments/"):
              name = path.split("/")[2]
              return httpx.Response(
                  200, json=load(f"seg-metadata-{name}-columns.json")
              )
          name = path.split("/")[-1]
          return httpx.Response(200, json=load(f"tableconfig-{name}.json"))

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT count(*) FROM pinot.default.airlineStats a, "
          "pinot.default.baseballStats b LIMIT 10"
      )
      assert estimate.row_estimate == 9746 + 97889
      assert estimate.max_intermediate_rows == 9746 * 97889
      # The oracle is asked at most once, and never for a two-table query.
      assert sum(1 for sql in asked if "AS JSON" not in sql) == 0


  async def test_an_unpriceable_shape_is_refused_before_any_request() -> None:
      def routes(request: httpx.Request) -> httpx.Response:
          raise AssertionError(f"no request should be made, got {request.url}")

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT x FROM UNNEST(SEQUENCE(1, 100000)) AS t(x) LIMIT 10"
      )
      assert estimate.confidence == "low"
      assert estimate.scanned_bytes is None


  async def test_a_foreign_catalog_is_refused_before_any_request() -> None:
      def routes(request: httpx.Request) -> httpx.Response:
          raise AssertionError(f"no request should be made, got {request.url}")

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      with pytest.raises(TableNotFoundError):
          await engine.estimate_cost("SELECT x FROM other.default.t LIMIT 10")


  async def test_a_controller_that_cannot_be_reached_quotes_low_not_an_outage() -> None:
      """A quotation nobody could build is a denial, not an engine failure."""

      def routes(request: httpx.Request) -> httpx.Response:
          raise httpx.ConnectError("nope")

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      )
      assert estimate.confidence == "low"
      assert estimate.scanned_bytes is None


  async def test_a_realtime_half_is_quoted_low() -> None:
      def routes(request: httpx.Request) -> httpx.Response:
          if request.url.path == "/tables/airlineStats":
              config = load("tableconfig-airlineStats.json")
              config["REALTIME"] = config["OFFLINE"]
              return httpx.Response(200, json=config)
          return _quote_routes(request)

      engine = PinotEngine(transport=httpx.MockTransport(routes))
      estimate = await engine.estimate_cost(
          "SELECT Carrier FROM pinot.default.airlineStats LIMIT 10"
      )
      assert estimate.confidence == "low"
      assert estimate.scanned_bytes is None
  ```
  Ensure the module imports `json`, `httpx`, `pytest` and `TableNotFoundError`,
  and has a `load()` helper reading from `fixtures/` (add one if it lacks one).

- [ ] **Step 2 — run, expect failure.**
  `cd server && uv run pytest -q tests/adapters/pinot/test_pinot_engine.py`
  Expect the new tests to fail with `assert 'low' == 'high'` / `assert None is not None`,
  because `estimate_cost` still returns the U10 stub.

- [ ] **Step 3 — implementation.** In `engine.py`, extend the imports:
  ```python
  from lagaam.adapters.pinot.metadata import TableFacts, table_facts, table_names, table_schema
  from lagaam.adapters.pinot.names import referenced_columns, referenced_tables, two_part_sql
  from lagaam.adapters.pinot.plan import max_intermediate_rows
  from lagaam.adapters.pinot.quote import quote, surviving_docs, surviving_segments
  from lagaam.core.scans import (
      generator_fanout,
      has_unpriceable_shape,
      scan_counts_saturated,
  )
  ```
  Add the module constant, beside the other option constants:
  ```python
  # The pruning oracle. Single-stage only: the multi-stage engine reports no
  # pruning counters, and it refuses nothing that would reveal them.
  _EXPLAIN_PRUNING = "EXPLAIN PLAN FOR "
  _EXPLAIN_SHAPE = "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR "
  ```
  Replace the stub `estimate_cost` with:
  ```python
      async def estimate_cost(self, sql: str) -> CostEstimate:
          """An upper bound on what this SQL would scan, synthesised here.

          Pinot reports no bytes and a constant rowcount of 100 per scan, so
          every number below comes from segment metadata plus one honest
          engine signal: how many segments survive the predicate. A number
          that cannot be bounded is withheld, and the gate denies on that.
          """
          dialect = PINOT_DIALECT_CARD.sqlglot_dialect
          # Generators and a saturated read count both break the byte sum in
          # ways no scaling repairs — don't vouch for a quote at all.
          if has_unpriceable_shape(sql, dialect) or scan_counts_saturated(sql, dialect):
              return CostEstimate(confidence="low")
          two_part = two_part_sql(sql, self.CATALOG)
          tables = referenced_tables(sql, self.CATALOG)
          if not tables:
              return CostEstimate(confidence="low")
          columns = referenced_columns(sql)
          try:
              facts = [
                  (database, await self._table_facts(database, table, columns))
                  for database, table in tables
              ]
          except PinotTransportError:
              # A quotation nobody could build is a denial at the gate, which
              # is the safe answer; an EngineError would read as an outage.
              return CostEstimate(confidence="low")
          surviving = await self._surviving(two_part, len(tables))
          located = [(database, fact, surviving) for database, fact in facts]
          widest = await self._widest_rows(two_part, located)
          estimate = quote(
              [(fact, k) for _, fact, k in located], columns, widest
          )
          if estimate.max_intermediate_rows is None:
              return estimate
          fanout = generator_fanout(sql, dialect)
          return estimate.model_copy(
              update={"max_intermediate_rows": estimate.max_intermediate_rows * fanout}
          )

      async def _table_facts(
          self, database: str, table: str, columns: frozenset[str] | None
      ) -> TableFacts:
          """One table's config, segment metadata and size, from the controller."""
          part = PinotClient.path_part(table)
          params: dict[str, str | list[str]] | None = (
              {"columns": sorted(columns)} if columns else None
          )
          config_json = await self._client.controller_get(
              f"/tables/{part}", database=database
          )
          seg_json = await self._client.controller_get(
              f"/segments/{part}/metadata", params=params, database=database
          )
          size_json = await self._client.controller_get(
              f"/tables/{part}/size", database=database
          )
          return table_facts(
              table,
              None if config_json is PinotClient.NotFound else config_json,
              None if seg_json is PinotClient.NotFound else seg_json,
              None if size_json is PinotClient.NotFound else size_json,
          )

      async def _surviving(self, two_part: str, table_count: int) -> int | None:
          """Segments surviving the predicate, or None meaning "charge them all".

          Only ever asked for a single-table query: the single-stage engine
          refuses a join outright, and it is the only engine that prunes.
          """
          if table_count != 1:
              return None
          try:
              body = await self._client.broker_query(
                  f"{_EXPLAIN_PRUNING}{two_part}", ""
              )
          except (PinotTransportError, PinotResponseTooLarge):
              return None
          return surviving_segments(body)

      async def _widest_rows(
          self, two_part: str, tables: list[tuple[str, TableFacts, int | None]]
      ) -> int | None:
          """Rows the widest plan node would build, or None if unreadable.

          Keyed as the plan spells a scan's table: [database, table], lowered.
          """
          leaves: dict[str, int | None] = {}
          for database, facts, surviving in tables:
              leaves[f"{database}.{facts.table}".lower()] = surviving_docs(
                  facts, surviving
              )
          try:
              body = await self._client.broker_query(
                  f"{_EXPLAIN_SHAPE}{two_part}", f"{_OPT_MULTISTAGE}=true"
              )
          except (PinotTransportError, PinotResponseTooLarge):
              return None
          cell = _plan_cell(body)
          if cell is None:
              return None
          return max_intermediate_rows(cell, leaves)
  ```
  and at module scope:
  ```python
  def _plan_cell(body: object) -> str | None:
      """The PLAN column of a multi-stage EXPLAIN answer: one row, one string."""
      if not isinstance(body, dict) or body.get("exceptions"):
          return None
      result = body.get("resultTable")
      if not isinstance(result, dict):
          return None
      rows = result.get("rows")
      if not isinstance(rows, list) or not rows:
          return None
      first = rows[0]
      if not isinstance(first, list) or len(first) < 2:
          return None
      return first[1] if isinstance(first[1], str) else None
  ```
  The leaf key carries each table's own database, matching how the plan spells
  a scan (`table=[default, airlineStats]` → `"default.airlinestats"`), so a
  query across two Pinot databases keys correctly without a special case.

- [ ] **Step 4 — run, expect PASS.**
  `cd server && uv run pytest -q && uv run mypy`

- [ ] **Step 5 — commit.**
  ```bash
  git add server/src/lagaam/adapters/pinot/engine.py server/tests/adapters/pinot/test_pinot_engine.py
  git commit -m "feat(pinot): quote a query before it runs

  The composition: core's shape checks first and without a request, then the
  tables and columns off the validated SQL, then three controller documents
  per table, then the two EXPLAINs — single-stage for how many segments
  survive, multi-stage for the join shape. The pruning oracle is asked only
  for a single-table query, because the single-stage engine refuses a join
  outright, and a join therefore charges every segment.

  A transport failure inside the quotation returns a low-confidence estimate
  rather than an EngineError. The gate denies on it either way, but a denial
  tells the agent its query is unpriceable, and an outage tells it to come
  back later — and only one of those is true.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 7: Integration tests against live Pinot

**Files:** `server/tests/integration/test_pinot_engine.py`.

**Interfaces:** Consumes `PinotEngine` against the running quickstart; produces no production code.

- [ ] **Step 1 — failing test.** Append, in the module's existing style (`pytestmark = pytest.mark.integration`, the `pinot_ready` fixture):
  ```python
  async def test_a_time_filter_quotes_less_than_no_filter(pinot_ready: None) -> None:
      engine = _engine()
      unfiltered = await engine.estimate_cost(
          "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
          "GROUP BY Carrier LIMIT 10"
      )
      filtered = await engine.estimate_cost(
          "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10"
      )
      assert unfiltered.row_estimate is not None
      assert filtered.row_estimate is not None
      assert filtered.row_estimate < unfiltered.row_estimate
      assert unfiltered.scanned_bytes is not None
      assert filtered.scanned_bytes is not None
      assert filtered.scanned_bytes < unfiltered.scanned_bytes
      assert filtered.confidence == "high"


  async def test_the_quote_is_never_under_what_execution_scanned(
      pinot_ready: None,
  ) -> None:
      """The whole contract: a bound, never a guess."""
      sql = (
          "SELECT Carrier, count(*) FROM pinot.default.airlineStats "
          "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 GROUP BY Carrier LIMIT 10"
      )
      engine = _engine()
      estimate = await engine.estimate_cost(sql)
      body = await engine._client.broker_query(
          two_part_sql(sql, PinotEngine.CATALOG), "useMultistageEngine=true"
      )
      scanned = body["numDocsScanned"]
      assert scanned > 0
      assert estimate.row_estimate is not None
      assert estimate.row_estimate >= scanned


  async def test_a_cross_join_quotes_the_product_of_both_tables(
      pinot_ready: None,
  ) -> None:
      engine = _engine()
      estimate = await engine.estimate_cost(
          "SELECT count(*) FROM pinot.default.airlineStats a, "
          "pinot.default.baseballStats b LIMIT 10"
      )
      airline = await engine.describe_table("pinot", "default", "airlineStats")
      baseball = await engine.describe_table("pinot", "default", "baseballStats")
      assert airline.row_estimate is not None
      assert baseball.row_estimate is not None
      assert (
          estimate.max_intermediate_rows
          == airline.row_estimate * baseball.row_estimate
      )
      assert estimate.max_intermediate_rows is not None
      assert estimate.max_intermediate_rows > 900_000_000


  async def test_an_equi_join_is_not_charged_the_product(pinot_ready: None) -> None:
      engine = _engine()
      estimate = await engine.estimate_cost(
          "SELECT count(*) FROM pinot.default.airlineStats a "
          "JOIN pinot.default.baseballStats b ON a.Carrier = b.teamID LIMIT 10"
      )
      assert estimate.max_intermediate_rows is not None
      assert estimate.max_intermediate_rows < 9746 * 97889
  ```
  Add a module-level `_engine()` helper returning
  `PinotEngine(controller_url="http://localhost:9000", broker_url="http://localhost:8000")`
  if the module has no equivalent, and import `two_part_sql`. If reaching into
  `engine._client` reads badly on review, execute through `engine.execute(...)` and
  assert against `describe_table`'s row estimate instead — but the `numDocsScanned`
  comparison is the assertion the spec names, so keep a form of it.

- [ ] **Step 2 — run, expect failure.**
  `cd server && uv run pytest -q -m integration tests/integration/test_pinot_engine.py`
  Expect failures only if Task 6 is incomplete; if Pinot is not running the suite
  skips, which is not a pass — confirm the container is up first with
  `curl -s http://localhost:9000/health`.

- [ ] **Step 3 — run, expect PASS.**
  `cd server && uv run pytest -q && uv run pytest -q -m integration tests/integration/test_pinot_engine.py && uv run mypy`

- [ ] **Step 4 — commit.**
  ```bash
  git add server/tests/integration/test_pinot_engine.py
  git commit -m "test(integration): the quotation bounds what Pinot actually scans

  The assertions the design asks for, against a live 1.5.1: the time-filtered
  airlineStats query quotes fewer docs and fewer bytes than the unfiltered
  one, and never fewer docs than numDocsScanned reports once it has run. A
  cross join quotes the product of both tables' doc counts — 954 million,
  which the default intermediate-row budget denies — while the same two tables
  joined on a key are not charged the product.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 8: E2E — the filtered query runs, the cross join is denied

**Files:** `server/tests/integration/test_e2e_mcp.py`.

This task **replaces** `test_query_data_on_pinot_is_denied_until_the_quotation_lands`,
the interim denial test landed in U10. That test asserts `query_data` on Pinot is
refused with "could not be estimated"; U11 is precisely the change that makes it
false, so it is deleted here rather than adjusted.

**Interfaces:** Consumes the MCP client over `PinotEngine`; produces no production code.

- [ ] **Step 1 — replace the test.** In `server/tests/integration/test_e2e_mcp.py`, delete
  `test_query_data_on_pinot_is_denied_until_the_quotation_lands` in full (including its
  docstring) and put in its place:
  ```python
  _PINOT_TWO_TABLE_GRANT = AgentIdentity(
      name="lagaam-e2e",
      allowed_tables=frozenset(
          {"pinot.default.airlinestats", "pinot.default.baseballstats"}
      ),
  )


  def _default_budget() -> QueryBudget:
      return QueryBudget(
          max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
          max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
          timeout_seconds=DEFAULT_TIMEOUT_SECONDS,
      )


  async def test_a_filtered_pinot_query_clears_the_default_budget_and_runs(
      pinot_ready: None,
  ) -> None:
      """U11's demo: the quotation is what lets a real query through the gate."""
      async with lagaam_client(
          _pinot_engine(), budget=_default_budget(), identity=_PINOT_GRANT
      ) as client:
          answer = await client.call_tool(
              "query_data",
              {
                  "sql": "SELECT Carrier, count(*) AS flights "
                  "FROM pinot.default.airlineStats "
                  "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073 "
                  "GROUP BY Carrier LIMIT 5"
              },
          )
          assert not answer.isError
          assert answer.structuredContent is not None
          assert answer.structuredContent["row_count"] > 0
          assert "Carrier" in answer.structuredContent["columns"]


  async def test_an_unbounded_pinot_cross_join_is_denied_on_row_work(
      pinot_ready: None,
  ) -> None:
      """954 million rows built at the widest step, and a LIMIT does not help."""
      async with lagaam_client(
          _pinot_engine(),
          budget=_default_budget(),
          identity=_PINOT_TWO_TABLE_GRANT,
      ) as client:
          answer = await client.call_tool(
              "query_data",
              {
                  "sql": "SELECT count(*) FROM pinot.default.airlineStats a, "
                  "pinot.default.baseballStats b LIMIT 10"
              },
          )
          assert answer.isError
          text = " ".join(
              block.text for block in answer.content if hasattr(block, "text")
          )
          assert "rows at its widest step" in text
          assert "LIMIT will not help" in text
  ```

- [ ] **Step 2 — run, expect PASS.**
  `cd server && uv run pytest -q -m integration tests/integration/test_e2e_mcp.py`
  Confirm the old test name no longer appears:
  `grep -c "denied_until_the_quotation_lands" tests/integration/test_e2e_mcp.py` → `0`.

- [ ] **Step 3 — full suite.**
  `cd server && uv run pytest -q && uv run pytest -q -m integration && uv run mypy`

- [ ] **Step 4 — commit.**
  ```bash
  git add server/tests/integration/test_e2e_mcp.py
  git commit -m "test(e2e): a filtered Pinot query now runs, and a cross join still cannot

  Replaces test_query_data_on_pinot_is_denied_until_the_quotation_lands, which
  asserted that every Pinot query_data was refused for want of a quotation.
  U11 is the change that makes it false, so it goes rather than bends.

  What replaces it is the demo: under the budget's own defaults a filtered
  airlineStats query clears the gate and returns rows, while an unbounded
  cross join is denied on the intermediate-row ceiling with the text that
  tells the agent a LIMIT will not save it.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Task 9: Reconcile the spec, and record the one new decision

**Files:** `docs/superpowers/specs/2026-09-11-pinot-adapter-design.md`, `docs/adr/0008-pinot-quotation-is-adapter-synthesised.md`, `docs/adr/README.md`.

Two things shipped differently from the spec's Quotation section, both because
the live engine said so. Neither is a design change; both need to be written down
where the next reader looks.

- [ ] **Step 1 — reconcile the spec.** In the Quotation section's input table, the
  "segments surviving the predicate" row currently reads "`numSegmentsQueried`
  minus the pruned counters". Replace with "`numSegmentsQueried` minus the
  **largest** pruned counter (they nest: `ByServer` is the total and
  `ByValue`/`ByLimit` break it down, so a sum double-counts), floored at 1".
  Add, under Rules, one bullet:
  ```markdown
  - `numSegmentsProcessed` is **not** a pre-execution signal: measured under
    `EXPLAIN` it is 0 in every shape, because nothing runs. The measurement
    log's "use `numSegmentsProcessed / numSegmentsQueried`" is an
    execution-time observation only. The pruned counters are what EXPLAIN
    reports.
  ```
  Also correct the cross-join product in the "Why" section from 954,024,994 to
  954,026,194 (9,746 × 97,889), and the same figure in the measurement log's §3
  table and §3 conclusion.

- [ ] **Step 2 — the ADR.** Create `docs/adr/0008-pinot-quotation-is-adapter-synthesised.md`,
  in the style of `docs/adr/0004-plan-based-cardinality-gating.md`:
  ```markdown
  # 8. A Pinot quotation is synthesised by the adapter, not read from the engine

  Date: 2026-09-13

  ## Status

  Accepted.

  ## Context

  ADR 0002 says query cost is a QUOTATION predicted from the engine's own
  plan. On Trino that is literal: `EXPLAIN (TYPE IO)` reports bytes and
  `TYPE LOGICAL` reports per-operator rows.

  Pinot 1.5.1 reports neither. There is no endpoint, option or statement that
  returns bytes-to-be-scanned. The only rowcounts available are Calcite
  attributes with no table statistics behind them: every table scan reports
  exactly 100.0 whatever the table holds, and a cross join of a 9,746-row and
  a 97,889-row table is priced at 10,000 against a true product of
  954,026,194. A gate reading those numbers would admit the query it exists
  to stop.

  What Pinot does give honestly is segment metadata — per-segment docs,
  bytes, time ranges and per-column index bytes — and one pre-execution
  signal: the single-stage `EXPLAIN PLAN FOR` performs real segment pruning
  and reports it without scanning a document.

  ## Decision

  The Pinot adapter synthesises the quotation itself. Segment metadata
  supplies the sizes; the single-stage EXPLAIN supplies how many segments
  survive the predicate; the multi-stage EXPLAIN supplies join shape and
  nothing else. Its `rowcount` and `cumulative cost` attributes are never
  read.

  The oracle reports how many segments survive, not which, so the adapter
  charges the k largest — independently by docs and by bytes, since the
  segment with the most rows is not the one with the most bytes. That is an
  upper bound on any k that could survive.

  The pruned counters nest rather than add: measured, a time filter reports
  `numSegmentsPrunedByServer: 28` and `numSegmentsPrunedByValue: 28` of 31
  segments at once, so the surviving count is queried minus the *largest*
  counter, floored at 1.

  Because the number is ours rather than the engine's, its honest confidence
  ceiling is lower than Trino's, and anything unmeasurable stays `None` —
  which the budget gate denies.

  ## Consequences

  A star-tree-answered aggregation reads a pre-aggregated tree and is
  over-charged by a doc-count quote. A table under row-level security is
  over-charged because the agent sees a filtered subset. Both are denials an
  operator can raise a budget for, never admissions.

  A REALTIME half is quoted `"low"` until U12 charges consuming segments at
  the stream's flush threshold: a consuming segment reports 0 docs and -1
  bytes, and charging those as written would quote it free.
  ```
  Add the row to `docs/adr/README.md`'s index, matching its existing format.

- [ ] **Step 3 — run.**
  `cd server && uv run pytest -q && uv run mypy`
  (Docs-only, but the suite must stay green at every commit.)

- [ ] **Step 4 — commit.**
  ```bash
  git add docs/adr/0008-pinot-quotation-is-adapter-synthesised.md docs/adr/README.md \
          docs/superpowers/specs/2026-09-11-pinot-adapter-design.md \
          docs/superpowers/specs/2026-09-11-pinot-measurements.md
  git commit -m "docs(pinot): record that the quotation is ours, not the engine's

  ADR 0008 for the decision the Pinot adapter could not avoid: Pinot reports
  no bytes and a constant rowcount, so the quotation is synthesised from
  segment metadata and the one honest pre-execution signal, the single-stage
  EXPLAIN's pruning.

  Two corrections to the design spec from the live engine. The pruned
  counters nest rather than add — ByServer is the total, ByValue and ByLimit
  break it down — so the surviving count subtracts the largest, not the sum.
  And numSegmentsProcessed is 0 under EXPLAIN in every shape measured, so it
  is not a pre-execution signal at all. The cross-join product is corrected
  to 954,026,194 in both documents.

  Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>"
  ```

---

## Done when

- `uv run pytest -q` and `uv run pytest -q -m integration` both green; `uv run mypy` clean.
- `estimate_cost` returns `confidence="high"` with real bytes and rows for a filtered OFFLINE query, and `"low"` for anything it cannot bound.
- `grep -rn "denied_until_the_quotation_lands" server/tests` returns nothing.
- `grep -rn "rowcount" server/src/lagaam/adapters/pinot` returns only comments saying it is never read.
- `git diff --stat main -- server/src/lagaam/core` is empty: core gained nothing.
