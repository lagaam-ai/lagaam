# 0008 — A Pinot quotation is synthesised by the adapter, not read from the engine

**Status:** Accepted

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
counter, floored at 1. Only `ByServer`, `ByValue` and `ByLimit` are read —
the three measured to nest on 1.5.1. `ByBroker` and `Invalid` are not:
neither was observed non-zero and neither is known to be a breakdown of
`numSegmentsQueried` (a broker that reports `numSegmentsQueried` already
net of its own pruning would be under-counted by subtracting `ByBroker`
again). A counter left unread can only leave more segments charged, never
fewer.

Column attribution is decided per segment, not per table: a segment in
which none of the query's columns are found is charged its whole size, and
a segment with no size makes the sum unknown.

Every table is charged once per read. The plan folds a repeated scan into a
single node — measured, a self-join's `LogicalJoin` names the same node id as
both its inputs — and the SQL's own table list dedupes, so a table read N
times would otherwise be charged once: a self-join quoted 9,746 rows against
19,492 scanned, and `UNION ALL` of 60 identical arms quoted 1/60th of the
bytes. Core's `table_scan_counts` supplies the count, and the table's facts
are charged once per read. Docs and bytes both scale; the plan walk's leaf
sizes stay per single read, since the walker multiplies or sums the folded
node as the plan references it.

Every join and `Correlate` is the product of its inputs plus their sum: the
unmatched rows of an outer join ride on top of the product, so the sum is
added. A `UNION`, all or distinct, is the sum, since a distinct union still
builds every input row before deduplicating. There is no max branch: 1.5.1 exposes no cardinality
anywhere in the pricing path — segment metadata carries docs, bytes and time
ranges but never distinct counts, and the plan's own rowcount is a constant
100 — so no equality can be shown to be a key. Measured, `ON a.Carrier =
b.Carrier` builds 10,719,442 pairs over 9,746 rows, because `Carrier` has 14
distinct values; charging the max quoted 9,746, a 1,100x under-quote. Taking
"has a conjunctive equality" as proof of a key is exactly the SQL-shape proxy
ADR 0004 rejected. The measured `OR` case remains evidence of the same thing
(`ON a.Carrier = b.teamID OR a.Origin = b.playerID`, 954,026,194 worst case),
but it is no longer the exception: the product is the rule.

Because the number is ours rather than the engine's, its honest confidence
ceiling is lower than Trino's, and anything unmeasurable stays `None` —
which the budget gate denies. The quotation has its own deadline for the
same reason: both EXPLAINs carry `timeoutMs=10000` and a client-side
backstop, because a timeout is a quote nobody could build, which the gate
denies exactly as it denies any other unbounded number.

## Consequences

A star-tree-answered aggregation reads a pre-aggregated tree and is
over-charged by a doc-count quote. A table under row-level security is
over-charged because the agent sees a filtered subset. A broker-pruned
partitioned table is over-charged too, until `numSegmentsPrunedByBroker`
is measured against a table that actually trips it and re-added with a
fixture. All three are denials an operator can raise a budget for, never
admissions.

A join of two real-sized tables is denied under the default budget until
key evidence exists — a single-segment column whose cardinality equals its
docs, or an upsert table's primary key, both U12 or later. That is a denial
an operator can raise a budget for, and Pinot's own `maxRowsInJoin` backstop
remains as the second line. An admitted join was never bounded by us anyway.

A REALTIME half is quoted `"low"` until U12 charges consuming segments at
the stream's flush threshold: a consuming segment reports 0 docs and -1
bytes, and charging those as written would quote it free.
