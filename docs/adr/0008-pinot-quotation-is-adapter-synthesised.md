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

The multi-stage plan walk applies ADR 0004's product/max rule to shape
alone. An equality in a join or `Correlate` condition counts only when it
is reached through AND alone — `OR` or `NOT` of an equality is charged the
product, not the max, because neither can prove every matched pair shares
a key (measured: `ON a.Carrier = b.teamID OR a.Origin = b.playerID` would
otherwise quote 97,889 for a 954,026,194 worst case). A `UNION`, all or
distinct, is charged the sum of its inputs, since a distinct union still
builds every input row before deduplicating. Calcite's `Correlate` takes
the product branch beside the joins.

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

A REALTIME half is quoted `"low"` until U12 charges consuming segments at
the stream's flush threshold: a consuming segment reports 0 docs and -1
bytes, and charging those as written would quote it free.
