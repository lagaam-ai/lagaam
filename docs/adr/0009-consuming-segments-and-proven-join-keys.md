# 0009 — A consuming segment is charged at its flush threshold, and a key is proven by the catalog and the scan ordinal

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
It is also not enough to know which columns are keys, because a plan names
columns whatever the SQL called them: `SELECT Origin AS Carrier` projects a
field named `Carrier` reading ordinal 62, while the real `Carrier` is 18.
A key proven by name would have been a key spoofable by alias.

## Decision

**A consuming segment is charged at the stream's flush threshold, in rows,
unconditionally.** `realtime.segment.flush.threshold.rows` — or the
deprecated `.threshold.size`, which the bundled quickstart config actually
uses — bounds a consuming segment exactly: 25 of 25 sealed segments held
exactly 100 docs at a threshold of 100, so a segment seals *at* the
threshold rather than below it. The table's charge is `consuming ×
flush_rows` on top of the k-largest sealed charge, where the sealed k is
`numSegmentsQueried − numConsumingSegmentsQueried` from the same
single-stage EXPLAIN the pruning oracle already issues. The consuming term
is added outside the pruning logic, because measured,
`numConsumingSegmentsQueried` stayed 1 under `DaysSinceEpoch > 99999` and
`DaysSinceEpoch < 1` — filters excluding every possible value — while the
broker pruned 6 of 7 segments (log §1.6). A time predicate cannot remove a
consuming segment's cost, so the oracle must never be allowed to prune it
away. No threshold in the config means no bound, which means `None`, which
the gate denies.

**The limit prune is trusted only under an explicit outermost LIMIT and no
OFFSET.** The single-stage EXPLAIN prunes against Pinot's *own* implicit
`LIMIT 10` when the statement carries none, while the multi-stage engine
that actually runs the query scans everything: measured, a LIMIT-less
select quoted 200 against 600 rows scanned on the realtime table and 422
against 9,746 on the batch one — a 23x breach at `confidence="high"`. An
OFFSET is worse still: the planner prices it as though it were absent, so
`LIMIT 10 OFFSET 9000` reports the same 30-of-31 prune as a bare `LIMIT 10`
and then walks 9,117 docs over 29 segments. Where the prune is not
believed, **both `ByLimit` and `ByServer` are ignored** — `ByServer` is the
server-side total that `ByLimit` breaks down (measured, 30 and 30 of 31
together), so crediting it would reinstate the very prune being
distrusted — leaving `ByValue`, which carries a predicate's own pruning
independently. The pipeline always injects a LIMIT before this port is
called, but a port must not depend on its caller for a bound.

**Its bytes are the one projected number in a Lagaam quotation.** No
endpoint on 1.5.1 reports a consuming segment's size, so there is nothing
to measure and nothing to validate a projection against. The charge is
`flush_rows × ceil(max over the table's own sealed segments of charged
bytes ÷ docs)` per consuming segment, taken per segment and maximised
rather than averaged, over exactly the bytes the sealed charge attributes —
the referenced columns where a segment carries them all, the whole segment
where it falls back — and ceiling-divided in integer arithmetic. Measured
on six sealed realtime segments: whole-segment ratios 850.12 to 871.36
bytes/doc, a 2.5% spread; for a two-column projection, 0.61 to 1.14. No
sealed segment with `docs > 0` means no ratio, which means `None`; so does
any sealed segment whose doc count is missing, since the segment nobody
counted could be the densest one.

**Metadata completeness is a precondition.** The segment names in
`/segments/{t}/metadata` must cover every sealed segment `/tables/{t}/size`
lists, or rows and bytes are both `None`. Measured (log §4): on a
multi-server table that endpoint returns one server's half and alternates
which half between identical calls — three consecutive calls on a 4-segment
table returned partition 0's two, then partition 1's two, then partition
0's again. The shipped sum was therefore a confident sum over a subset: an
under-quote at `confidence="high"`. A table whose segments span servers is
now denied, and the `pinot-realtime` profile pins each table to one server
with a single replica group so the demo tables are complete. Fetching each
segment's metadata individually is the path that would lift the denial, and
it is U13.

**A join key is charged as a key only where the catalog proves it, and only
where the scan ordinal agrees.** Two sources of evidence:

(a) **An upsert table's full primary key**, where the table config's
`upsertConfig` and a non-empty `upsertPartitionToServerPrimaryKeyCountMap`
in the table metadata agree that the table is one — two documents
disagreeing about what a table is withholds the evidence. Measured,
`GROUP BY pk HAVING count(*) > 1` returns nothing on the upsert table and
six versions per key on a byte-identical non-upsert twin reading the same
topic, and the PK self-join returns exactly the distinct-key count against
3,600 for the twin. The full column list is the key and never a subset: a
composite primary key is unique as a tuple.

(b) **A single-segment column whose `cardinality` equals its `totalDocs`**,
since `cardinality` is exactly `count(DISTINCT col)` — verified against the
engine on four columns — and `count(DISTINCT col) ≤ count(col) ≤ totalDocs`
forces every value to differ at equality. That argument is the segment's,
and it is the table's only where the two are the same rows, so the source is
gated hard: the size report must name exactly one sealed segment, the
metadata response must be complete against it, that one entry must be that
segment, and the size report must carry no `-1` entry at all — a consuming
segment holds rows the sealed segment does not, whatever the metadata says.
The column must be single-valued (a multi-value column's cardinality counts
entries, not rows) and its nullability must be establishable, because the
null caveat is open.

**A projected name proves nothing; the ordinal does.** The engine learns
each keyed table's key-column scan ordinals with one extra EXPLAIN of the
key columns alone (`EXPLAIN … FOR SELECT <key columns> FROM db.t`), built
only from bare identifiers the schema and the listing supplied — any
database, table or column name that is not a bare identifier forfeits that
table's evidence. The walker then composes each equality operand's index
*down* its side's chain to the scan — a project maps an index to its bare
`exprs[index].input`, filters and exchanges pass it through — and the
operand names a key column only when that composed ordinal equals the one
the catalog's EXPLAIN reported. The equated set is built from those matches
and never from field names. A table with keys but no ordinals yields
nothing, and every doubt fails closed: a non-bare expression, a missing or
short `exprs`, an index out of range, an `OR` anywhere in the condition.
The alias spoof composes to 62 against the real 18 and is refused.

**Evidence is one-sided.** Each side of a join is judged alone: a keyed
table beside a join is still evidence about its own matches, even though
the side that is not a straight chain to exactly one scan can never be
covered. An expression operand likewise silences only its own side —
`upper(a.Carrier) = b.teamID` with `teamID` proven on `b` still bounds the
join, since each left row matches at most one right row whatever its
expression value.

**The arithmetic.** Where one side's equalities cover a unique key set of
that side's table, the join emits at most the *other* side's rows, and is
charged **other side + both inputs**. Where both do, **the smaller side +
both inputs**. Otherwise **the product + both inputs**, exactly as ADR 0008
had it. The inputs ride on top for ADR 0008's reason: an outer join emits
unmatched rows above the matched ones.

**Hybrid tables are charged as one segment set** — both halves' sealed
segments from the same metadata and size responses, k-largest across the
union, plus the consuming charge — and this path is unit-tested only.

## Consequences

- **The bytes projection can fail in exactly one way**: a consuming segment
  whose per-row encoding is worse than every sealed segment of the same
  table. The 2.5% observed spread makes that implausible and does not
  exclude it. It is the only number in the quotation that is not a
  measurement, which is why it is recorded here rather than left in a
  docstring.
- **A consuming segment is over-charged whenever it is not full.** It is
  charged its whole flush threshold from the moment it exists, so a segment
  holding one row is priced at a hundred. That is what an upper bound on
  data nothing reports is made of.
- **An upsert table is over-charged in rows**, because a sealed upsert
  segment's `totalDocs` counts every version: 200 on disk against a visible
  `count(*)` of 100. The PK-count map is not substituted for it — it is per
  server, unmeasured under replication > 1, and counts distinct keys rather
  than rows scanned.
- **A join of two real-sized tables without a key is still denied.** The
  product rule survives wherever the catalog proves nothing, which is most
  places: the e2e demo has to lower the row ceiling to 10,000 to show the
  twin denied, because the twin's product on 800-doc tables is 641,600 and
  the 50M default would admit it. Under that ceiling the upsert self-join
  is quoted 2,400 and runs, the twin is quoted 641,600 and is denied on
  "rows at its widest step", and the engine builds 100 true pairs. The
  freshness query runs under the *default* budget: 600 rows quoted against
  600 scanned.
- **The null caveat on source (b) is open.** Whether `cardinality` counts a
  null or a default as a distinct value was never exercised — every column
  measured had `count(col) == totalDocs`. Source (b) is therefore gated on
  nullability and finds nothing on either quickstart dataset: 0 of 2,604
  per-column entries on `airlineStats`, and a best ratio of 0.185 on
  `baseballStats`. Closing it needs a table with real nulls, which is U13
  or later.
- **A multi-server table is denied rather than under-quoted.** That is the
  completeness precondition doing its job, and it withholds quotes that
  were previously confidently wrong. On the single-server quickstart
  nothing changes; the per-segment metadata fetch that would restore those
  tables is U13.
- **The hybrid path is unit-tested and not live-tested.** `-type HYBRID`
  cannot start in a container on 1.5.1 — it shells out to the `docker` CLI
  to run Kafka — so the arithmetic is proved over synthesised JSON and the
  first real hybrid deployment is the first live test of it.
- **Semi-joins and aggregated join inputs stay denied.** Their JSON plan
  cannot be obtained at all on 1.5.1: any plan carrying a `PIPELINE_BREAKER`
  exchange fails to serialise with errorCode 450, so `max_intermediate_rows`
  gets nothing to read and returns `None`.
