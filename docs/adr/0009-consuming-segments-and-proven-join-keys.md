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

**A consuming segment is charged at the flush threshold stored in its own
segment metadata, in rows, unconditionally.** A threshold bounds a consuming
segment exactly: 25 of 25 sealed segments held exactly 100 docs at a
threshold of 100, so a segment seals *at* the threshold rather than below it.

*Corrected after review.* The threshold was read from the **table config**,
and a config is not a bound on a segment already consuming. Each consuming
segment stores the threshold it was created with in its LLC segment ZK
metadata, and no later config change reaches it. Measured on `u12flush`: a
`PUT` lowering `realtime.segment.flush.threshold.rows` from 100 to 10 was
accepted, the live segment's `segment.flush.threshold.size` stayed **100**,
it went on to hold 70 rows while still CONSUMING, and it sealed at **exactly
100** — while the adapter, reading the config, quoted **110 rows against 170
scanned at `confidence="high"`**. No reload helps; only `forceCommit` does,
by sealing the segment so its replacement is born with the new value (which
the replacement was, at 10). Autotune
(`realtime.segment.flush.threshold.segment.size`) reaches the same place
from the other end: with no row threshold in the config at all, the next
segment was created with a stored threshold of 100 that the config never
states. The worst case is `stored − config` rows per consuming segment — 90
here — and it is unbounded in general.

So the source is `GET /segments/{table}/{segmentName}/metadata` →
`segment.flush.threshold.size`, one call per CONSUMING segment named in the
externalview. That endpoint is a direct view of the segment's ZK
simpleFields and was the only one of five probed that exposes the value;
the key is the ZK spelling and not the in-JVM `sizeThresholdToFlushSegment`,
which appears in no controller response. A consuming segment's live row
count is not exposed anywhere, which is why the threshold is still the only
bound there is. A 404 on that path is a threshold nobody knows and takes the
quote low; a 403 is a refusal and raises, as every other controller refusal
in this adapter does.

The table's charge is the **sum of the consuming segments' own stored
thresholds** — never a count times one of them, since two segments born
either side of a config change hold different bounds and neither bounds the
other — on top of the k-largest sealed charge, where the sealed k is
`numSegmentsQueried − numConsumingSegmentsQueried` from the same
single-stage EXPLAIN the pruning oracle already issues. The consuming term
is added outside the pruning logic, because measured,
`numConsumingSegmentsQueried` stayed 1 under `DaysSinceEpoch > 99999` and
`DaysSinceEpoch < 1` — filters excluding every possible value — while the
broker pruned 6 of 7 segments (log §1.6). A time predicate cannot remove a
consuming segment's cost, so the oracle must never be allowed to prune it
away. A consuming segment whose own metadata carries no readable threshold
has no bound, which means `None`, which the gate denies — and the table
config may not stand in for it.

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
`ceil(that segment's stored threshold × the max over the table's own sealed
segments of charged bytes ÷ docs)` per consuming segment, summed — each
segment at its own threshold — taken per sealed segment and maximised
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
with a single replica group so the demo tables are complete. ADR 0011 lifts
the denial: the missing sealed segments are fetched from the servers that
hold them, per server rather than per segment, and this precondition then
runs unchanged on the completed document. The per-segment fetch the
consuming charge makes is over consuming segments only, and answers a
different question (the stored threshold, which the bulk endpoint does not
carry at all).

**A join key is charged as a key only where the catalog proves it, and only
where the scan ordinal agrees.** Two sources of evidence:

(a) **An upsert table's full primary key**, where the table config's
`upsertConfig` and a non-empty `upsertPartitionToServerPrimaryKeyCountMap`
in the table metadata agree that the table is one — two documents
disagreeing about what a table is withholds the evidence — **and the
`upsertConfig` is one under which the key is still unique**: `mode`
(case-insensitive) is `FULL` or `PARTIAL`, and neither `metadataTTL` nor
`deletedKeysTTL` is greater than zero.

*The TTL condition was added after review, on a live reproduction.* A set
`metadataTTL` evicts a key from the primary-key lookup map while the rows it
pointed at stay queryable, so a key re-ingested after its window has two
visible rows. On table `u12ttl` (`mode: FULL, metadataTTL: 60000`): 8 rows,
7 distinct `pk`, `GROUP BY pk HAVING count(*) > 1` returning `K1 -> 2` with
**both versions visible**, and the `pk` self-join returning **10 pairs**
against the 8 a unique `pk` gives — while the adapter cut the widest join
step from 80 to 24 on the strength of that key. `mode: NONE` is the other
way in: a valid `Mode` value that disables upsert while the object stays.
`deletedKeysTTL` opens the same window through dropped delete tombstones.
The PK-count map is no help here either — it reported **6** against those 8
rows and 7 distinct visible keys, wrong in both directions, while staying
non-empty, so it confirms "upsert table" and never correctness.

**Zero is not a TTL.** 1.5.1's `isTTLEnabled()` is
`_metadataTTL > 0 || _deletedKeysTTL > 0`, `isOutOfMetadataTTL` returns
false outright at `_metadataTTL <= 0`, and the controller materialises
`metadataTTL: 0.0, deletedKeysTTL: 0.0` on every upsert config it serves, so
reading a zero as set would withhold every upsert key there is. A TTL value
the adapter cannot read as a number is treated as set.

Measured,
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
null caveat is open — and establishing it takes a schema document that was
actually read, since an unread schema marks no column nullable for want of
evidence rather than for want of nullable columns.

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
- **A realtime table costs one extra controller call per consuming
  segment.** A table with no consuming segment pays nothing for it, and a
  quotation over such a table makes exactly the requests it made before.
- **A consuming segment is over-charged whenever it is not full.** It is
  charged its whole stored flush threshold from the moment it exists, so a
  segment holding one row is priced at a hundred. That is what an upper bound on
  data nothing reports is made of.
- **An upsert table is over-charged in rows**, because a sealed upsert
  segment's `totalDocs` counts every version: 200 on disk against a visible
  `count(*)` of 100. The PK-count map is not substituted for it — it is per
  server, unmeasured under replication > 1, and counts distinct keys rather
  than rows scanned.
- **A TTL'd upsert table joins at the product.** Its primary key is not
  provable unique from metadata at all — the lookup map has forgotten keys
  whose rows are still queryable, and no endpoint reports which — so the
  evidence is withheld rather than weakened. The same holds for
  `mode: NONE`. An operator who wants the key priced as a key must run
  without a TTL.
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
  nothing changes. **Superseded by ADR 0011**, which restores those tables
  by fetching the missing sealed segments from the servers that hold them —
  the precondition itself is unchanged and still decides, on a document
  that is now complete.
- **The hybrid path is unit-tested and not live-tested.** `-type HYBRID`
  cannot start in a container on 1.5.1 — it shells out to the `docker` CLI
  to run Kafka — so the arithmetic is proved over synthesised JSON and the
  first real hybrid deployment is the first live test of it.
- **Semi-joins and aggregated join inputs stay denied.** Their JSON plan
  cannot be obtained at all on 1.5.1: any plan carrying a `PIPELINE_BREAKER`
  exchange fails to serialise with errorCode 450, so `max_intermediate_rows`
  gets nothing to read and returns `None`.

## Amendment 2026-09-25 — a pruned segment is assumed consuming first

The sealed k was `surviving − numConsumingSegmentsQueried`, on the reading
that a consuming segment is always among the survivors. Measured on
`u14multi` after its 24 h time flush, it is not: the server prunes an empty
consuming segment (14 queried, 2 pruned ByServer, 2 consuming) and the
subtraction then removed two *sealed* segments from the charge — 100 real
docs, 1,532 bytes — covered only by the consuming projection's grace.
`airlineStats` shows the same shape (7 queried, 1 pruned, 1 consuming).
Only the consuming segments the pruning provably left may be subtracted:
`max(0, consuming − pruned)`. When pruning happens this charges up to
`consuming` more sealed segments than before, which is the fail-safe side.
Measurements: `docs/superpowers/specs/2026-09-21-pinot-multiserver-measurements.md` §7.

2026-09-27: the rule this ADR decides lives in `server/src/lagaam/adapters/pinot/keys.py`.
