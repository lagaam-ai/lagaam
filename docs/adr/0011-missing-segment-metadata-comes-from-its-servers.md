# 0011 — Missing segment metadata is fetched from the servers that hold it

**Status:** Accepted

## Context

ADR 0009 made metadata completeness a precondition: the segment names in
`GET /segments/{t}/metadata` must cover every sealed segment
`GET /tables/{t}/size` lists, or rows and bytes are both `None`. That
precondition was right and it is still right — the shipped sum before it was
a confident sum over a subset, an under-quote at `confidence="high"`, which
is the one failure mode the gate exists to prevent.

What it also did was deny every real deployment. A cluster is a cluster
because it has more than one server, and measured on the realtime profile
(Pinot 1.5.1, four servers):

```
SELECT pk FROM pinot.default.u14multi LIMIT 5 -> scanned_bytes=None row_estimate=None confidence='low'
SELECT pk FROM pinot.default.u12plain LIMIT 5 -> scanned_bytes=1582 row_estimate=400 confidence='high'
```

Same schema, same flush threshold, same query. `u12plain`'s segments sit on
one server; `u14multi`'s ten sealed segments split 3/7 across two. Under any
row or byte budget every query on the second table is denied. ADR 0009's own
consequence called that "denied rather than under-quoted" and left the fetch
that would lift it to a later unit. This is that unit.

The endpoint is not merely truncating; it is answering for one server, and no
parameter changes that. Measured: three consecutive bulk calls on `u14multi`
each returned the same 8 of 10 sealed segments, all from one server.
`?columns=pk` returned 8. `?segments=` naming all twelve segments returned
the same 8 — the filter is applied *after* a server is chosen. On `u14rep`
(replication 2) the bulk call returns 4 of 12. A `?segments=` list whose
names all live on one server does answer for all of them.

Two alternatives were measured and rejected:

- `GET /segments/{t}/zkmetadata` is one call and complete (12 of 12), and
  carries `segment.total.docs` and `segment.size.in.bytes` — but no
  per-column bytes, so a `SELECT pk` would be charged whole segments. That
  is a large over-charge on a wide table, and the column projection is most
  of what makes a Pinot quote useful.
- `GET /segments/{t}_REALTIME/{seg}/metadata?columns=pk` is complete and
  full-fidelity per segment (docs 100, bytes 7480, pk `indexSizeMap`
  `{dictionary: 708, forward_index: 808}`), but costs one call per segment
  and 500s on the untyped table name ("Ideal state does not exist for
  table").

`GET /segments/{t}/servers` says which server holds which segments — one
element per table type, `serverToSegmentsMap`, consuming names included — and
a `?segments=` filter whose names all live on one server comes back whole,
with the bulk endpoint's own fidelity.

It does not come back whole *every* time. The answer to any filtered ask is
one server's response rather than a union — over 20 bulk trials on
`u14multi` the 7-segment server answered 19 times and the 3-segment server
once — and a filter naming one holder's names comes back whole because only
that holder has work to do for it. When a server holding none of the names
answers last, the body is `{}`. Measured at 40 trials per ask: `u14rep`'s
three-name ask on 7053 came back `{}` once, the other five per-server asks
0/40, and a **single-name** ask was empty 6/30 on `u14multi` and 4/30 on
`u14rep` — about one in five. So a short answer is a random event, not a
verdict about the table, and an identical retry is an independent trial
(1/1 on the one that was caught). Pinot 1.5.1's
`TableMetadataReader.fetchAndAggregateMetadata` reads as a per-property union
over the server responses, which does not match what this cluster returns;
the mechanism is not established here, only the behaviour.

## Decision

When the bulk metadata response does not cover every sealed segment the size
report names, the adapter fetches the rest from the servers that hold them:

1. `GET /segments/{t}/servers` for the server-to-segments map.
2. The missing names are **grouped** by the first server listing them, in the
   response's own order. The adapter never addresses a server: the only thing
   it sends is a `segments=` filter. Grouping is what makes the *aggregated*
   answer complete — measured, a filter naming one server's segments comes
   back whole, while a mixed one comes back as one server's subset. A name
   already covered by another group is not asked for twice: measured on
   `u14rep`, the two replicas' entries for a segment are identical in `crc`,
   `totalDocs` and every column index size for all ten sealed segments, so a
   second ask buys nothing and costs a call.
3. Each group is sent as `GET /segments/{t}/metadata` with
   `segments=<the group's names>` and the bulk call's own `columns=` filter,
   so the projection is priced the same way it was before. A group whose
   names would not fit one URL is split into several; see the bounds below.

What the controller does with that filter is its own business, and Pinot
1.5.1's `TableMetadataReader.getSegmentsMetadataInternal` says what it is: the
controller takes the server-to-segments map itself, and `buildTableLevelUrls`
sends **our filter to every server hosting the table**, then aggregates the
responses. On a `RuntimeException` it falls back to `buildSegmentLevelUrls`,
one URL per segment the filter names. So the backend cost of one adapter GET
is one server request per server hosting the table — not one — and on the
fallback path one per named segment. Source: `TableMetadataReader.java`
(release-1.5.1) lines ~175–225.
4. An answer missing any name its ask named is **asked again for exactly the
   names still missing**, up to `_METADATA_RETRIES` (2) more times per batch.
   A short answer is one server's `{}`, not a fact about the table, and the
   same request is an independent trial. Two retries take the worst measured
   rate — 20%, a single-name ask — to under 1%. A batch still short after
   its retries stops the fan-out and keeps the bulk response, exactly as an
   unreadable answer does; later batches are not asked for. The merge happens
   only when every batch came back whole.
5. The answers are merged name-keyed into the bulk response, bulk entries
   winning, and the merged document goes to `table_facts` unchanged.

The completeness guard is untouched. It runs on the merged document exactly
as it ran on the bulk one, and it still decides. This widens what the guard
can see; it never widens what the guard accepts. `metadata_is_complete` is
now defined as `not missing_sealed_segments(...)` so the guard and the fetch
cannot disagree about which names are missing.

Every bound leaves the quote exactly where ADR 0009 left it — `low` — rather
than guessing:

- a group's names are split into asks whose request line — path plus the
  query httpx encodes — stays under `_MAX_METADATA_URL_BYTES` (6144).
  Measured on the live controller: an 8,059-byte URL answers `200` and 8,099
  answers `400` with an empty body, and httpx itself raises `InvalidURL`
  above 65,536 bytes per component, before any request is made. A single
  name too long to ask for alone: no fan-out at all;
- more than `_MAX_METADATA_REQUESTS` (64) asks in the **worst case**, counted
  before the first call: no fan-out at all. The worst case is
  `batches x (1 + _METADATA_RETRIES)`, so 21 batches is the most that is ever
  attempted (63 <= 64) and 22 makes no call — the retries are bounded by the
  same number as the asks they retry, never on top of it;
- any call returning `NotFound`, a non-dict, or raising
  `PinotTransportError` (or `InvalidURL`, which the byte cap should already
  have prevented): stop, keep what the bulk call gave;
- the whole fan-out, **discovery included**, under
  `anyio.fail_after(_METADATA_FANOUT_SECONDS)` (10.0s, the EXPLAIN
  deadline's sibling): on timeout, keep the bulk response. httpx's own
  timeout is per-operation and defaults to 30s, so it is not this bound.

Calls are sequential. 64 is a bound on the worst case, not a target.

## Consequences

A multi-server table is quoted instead of denied. Measured on the same
cluster, before and after:

| table | servers | before | after |
|---|---|---|---|
| `u14multi` (replication 1) | 2 | `low`, all `None` | `scanned_bytes=4548 row_estimate=300 confidence='high'` |
| `u14rep` (replication 2) | 4 | `low`, all `None` | `scanned_bytes=6064 row_estimate=400 confidence='high'` |
| `u12plain` (replica-group pinned) | 1 | `scanned_bytes=1582 row_estimate=400` | unchanged |

The retry changes how *reliably* that holds, not what it says. Forty live
`estimate_cost` trials per table, without the retry and with it — the quoted
numbers are identical in every `high` answer either way:

| table | `low` without the retry | `low` with it |
|---|---|---|
| `u14multi` | 1/40 | 0/40 |
| `u14rep` | 2/40 | 0/40 |
| `u12plain` | 0/40 | 0/40 |

The residual is not zero. At the worst rate measured on this cluster (a 20%
empty single-name ask) three independent asks leave under 1% of batches
short, and a table whose fan-out is several batches multiplies that by its
batch count. Such a quote is `low`, which is where it already was — the
failure is a denied query, never an under-quote. And the mechanism inside
the controller is not established (Context), so the rate is this cluster's
behaviour, not a property anything here proves.

A single-server table makes **no new request**. It is complete on the bulk
call, `missing_sealed_segments` is empty, and `/servers` is never asked —
which is every existing integration test and the batch quickstart.

A multi-server table costs `1 + R` extra **client-to-controller** GETs per
quotation, where `R` is the number of asks: one per group holding a segment
the bulk call missed, plus a split wherever a group's names do not fit one
URL — up to `1 + 3R` where every ask comes back short twice. On `u14multi`
that is two calls in the normal case. That count is the adapter's only
measured cost; it is not a count of backend requests. Each of those GETs
makes the controller ask every server hosting the table (Decision §3), so a
wide cluster pays more inside Pinot than the adapter's number shows, and
nothing here bounds that. A fan-out needing more than 64 asks quotes `low`,
which is where it already was.

**The completeness guard is now load-bearing in a second way.** Before, a
failed guard meant "this table spans servers". Now it means "this table spans
servers *and* the fan-out could not close the gap" — a server that 404s, a
name no server claims, a slow cluster, or an ask that came back short three
times running. The quote is the same (`low`) either
way, so nothing downstream changes, but an operator diagnosing a `low` quote
has one more place to look.

**The realtime profile no longer pins every table to one server.** ADR 0009
pinned `u12plain` and `u12upsert` with a single replica group specifically to
dodge this case. They stay pinned, as the control that proves a single-server
table pays nothing — and `u14multi` and `u14rep` are added unpinned, with two
partitions, precisely so the multi-server path is exercised live rather than
only over fixtures.

**Replica agreement is assumed from ten segments on one table.** All ten
sealed segments on `u14rep` had byte-identical entries on both holders, which
is what a Pinot segment is — an immutable file with a CRC. If two replicas of
one segment ever disagreed, the merge would keep whichever server was asked
first. The guard would not catch that, because both entries carry the same
name; nothing measured suggests it can happen, and no code defends against
it.
