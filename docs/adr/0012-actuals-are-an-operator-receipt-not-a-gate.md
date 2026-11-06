# 0012 — Post-execution actuals are an operator's receipt, never a gate or an agent input

**Status:** Accepted

## Context

ADR 0002 makes query cost a quotation: priced before execution, enforced
before anything runs. Nobody could see how good a quotation was. The audit
line carried the estimate and the row count returned, but not what the
engine read, so an operator setting `LAGAAM_MAX_SCAN_BYTES` had no way to
tell a quote that was 10x high from one that was right.

Both engines already report what a query did once it has run. Trino's
dbapi cursor carries the coordinator's stats block after a fetch
(`processedRows`, `processedBytes`, `elapsedTimeMillis`, `cpuTimeMillis`,
`state`). Pinot's broker response carries `numDocsScanned`, `totalDocs`,
`numEntriesScannedInFilter`, `numEntriesScannedPostFilter`, `timeUsedMs`
and the segment fan-out (measurements §2). They do not report the same
things: Pinot has no bytes counter at all, and a Trino fetch capped at the
row limit can be read while the query still says `RUNNING` (measured on
Trino 476, `LIMIT 1001` fetched as exactly 1001 rows).

Three choices had to be made: where the numbers live, who sees them, and
whether anything acts on them.

## Decision

- **A port-level `QueryActuals` rides on `QueryResult`.** Neutral fields
  for what both engines can say: `rows_scanned` (Trino `processedRows`,
  Pinot `numDocsScanned`, the same sense as `CostEstimate.row_estimate`),
  `bytes_scanned`, `elapsed_ms`. Anything else goes in `engine_counters`
  under the engine's own name, so forensics never depends on a mapping
  someone has to remember. Every field is optional, and unknown is `None`,
  never `0`: Pinot's `bytes_scanned` is null because a zero would claim a
  scan that cost nothing.
- **`partial` says when a count is a floor.** Trino stats read before the
  query reached `FINISHED` are marked partial. Pinot answers once, after
  the query ends, so its actuals never are.
- **The agent never sees them.** The field is excluded from serialisation
  and from the tool's output schema. The agent's job is the answer; a
  receipt it cannot act on is tokens spent and a new surface to reason
  about.
- **Nothing gates on them.** The server records `actuals` next to
  `estimate` on the `query_data` audit line and does nothing else. A query
  that was refused never ran, so its line has no `actuals` key; one that
  ran on an engine that reported nothing has `actuals: null`.

## Consequences

- The quote and the receipt sit on one line, so a quotation that drifts
  from reality shows up in the trail operators already keep.
- Enforcement stays where ADR 0002 put it, before execution. Using actuals
  to block a later query would be a meter, and the meter lives at the LLM
  proxy.
- The spec's "numEntriesScannedInFilter and freshness surfaced as warnings"
  is recorded in `engine_counters` and not turned into an agent warning:
  the spec names the signals but no rule for when they mean the agent
  should distrust a result. That needs a measured threshold first.
- Connectors that generate rows rather than read them report zero bytes:
  Trino's `tpch` gives `processedBytes: 0` against a 270 KB quote. That is
  the engine's number, logged as given.
