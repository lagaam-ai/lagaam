# 0006 — Row generators remain a SQL-level check

**Status:** Accepted

## Context

Plan-based gating (ADR 0004) covers every shape where rows multiply
through joins, because the planner estimates them. It cannot see rows
manufactured from an argument: measured on Trino 476,
`UNNEST(sequence(1, 10000))` over a 60,175-row table plans as 60,175 rows
and produces 601 million. `UNNEST(repeat(col, 10000))` behaves the same.

## Decision

Generator detection stays in `core/scans.py` as a sqlglot AST check — the
only SQL-shape analysis that survived the move to plan-based gating. It
flags `UNNEST`/`explode` over value-expanding functions (`sequence`,
`repeat`, `split`, ...), with an allowlist of row-preserving array
functions; an unrecognized function feeding a generator is assumed to
invent rows, because a denylist would silently miss anything sqlglot parses
as `Anonymous`.

Bounded generators — a literal array or a sequence whose length is spelled
out — are exempt individually, but their sizes multiply where they meet:
three 1000-row sequences cross-joined are a billion rows, each inside the
per-generator cap. The cap therefore also binds the *product* of bounded
generator sizes, counted per UNION branch (branches add rows rather than
multiply them) and once per CTE *reference* rather than once per AST node.

A bounded generator joined to a table is priced rather than refused. The
planner sizes such a join as the table alone — measured on Trino 476,
`orders CROSS JOIN UNNEST(sequence(1, 1000))` plans as 1,500,000 rows and
produces 1.5 billion — so `generator_fanout()` reports the multiplier and
the adapter scales the plan's own row estimate by it. The budget then
decides with the table's real cardinality in hand, which a flat cap cannot
have: 744 hourly buckets is ordinary against a small table and ruinous
against a large one.

The flat cap therefore binds only where no table is read. Alone, a spine is
all the query is and nothing downstream prices it, so it must be small; the
same spine crossed with a table is a multiplier the budget applies to a real
cardinality, and seven years of days against a 25-row lookup is 63,950 rows.

The multiplier is charged where the generator's rows can actually meet the
branch's. Rows an aggregate collapses on the way *out* of a scope, and a
predicate subquery (`EXISTS`, `IN`), contribute none — but only where that
scope builds nothing of its own: an aggregate over a generator already
crossed with a table runs after those rows exist, and excusing it quoted 6
billion rows as six million.

A column feeding a generator is only as bounded as whatever bound it. A
scanned column's rows are already in the plan, but a projection can build
one — `repeat(k, 1000000) AS arr` is a column by the time `UNNEST` reads
it — so columns resolve through the statement's projections, and a cycle
among them is refused rather than followed.

## Consequences

- A flagged query gets no quote (confidence "low"), which the budget
  denies with what-to-change text. A *sized* generator keeps its quote and
  is denied only if the scaled row count exceeds the budget, so raising
  `LAGAAM_MAX_INTERMEDIATE_ROWS` is the lever for a wider spine.
- The row-preserving allowlist must track Trino's array functions; an
  unknown-but-harmless function is over-blocked — the fail-safe direction.
- A generator *equi-joined* to a table is charged its full size even though
  the join key may match one row apiece: `orders LEFT JOIN UNNEST(spine) ON
  s.d = o.orderdate` quotes 182x its real work. Reading the key's
  selectivity from the SQL is exactly what ADR 0004 forbids — a join
  predicate is free to write and would hand back the multiplier to anyone
  who spells one — so this stays an over-quote until the fanout can be taken
  from the plan itself. Raising `LAGAAM_MAX_INTERMEDIATE_ROWS` is the lever
  meanwhile.
- If a future Trino version starts pricing generators, this check becomes
  redundant, not wrong.
