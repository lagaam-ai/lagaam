# 0007 — Parser safety bounds sit ahead of the parse

**Status:** Accepted

## Context

Every check in `core/safety.py` runs on a parsed AST, so sqlglot parses
before the gate decides anything. That makes the parser itself the first
thing an agent can attack: parsing is superlinear in nesting depth, and a
gate an agent can make burn CPU is the unbounded work the gate exists to
refuse.

Measured on sqlglot 30.12, under pytest with a fresh process per depth:

- **Size.** 3 MB of nested subqueries cost 5s to parse; 12 MB cost 25s —
  before any check of ours runs.
- **Bracket depth.** The recursive-descent parser breaks well before a
  query gets big enough to trip a size cap, and the break point depends on
  grammar shape rather than bracket count alone. `RecursionError` shapes
  are the shallower worry: nested `CASE WHEN ... THEN (...)` broke at 27
  brackets, nested `abs()` at 44. The binding constraint turned out to be
  slowness, not a crash — nested `ARRAY[...]` parses in 0.03s at depth 10,
  0.76s at depth 15, and hangs past 5s by depth 18.
- **AST nesting.** sqlglot's generator recurses once per AST level, so a
  *small* query nested deeply enough blows the stack inside `tree.sql()`:
  rendering raised `RecursionError` at an AST depth of 263 (87 levels of
  `SELECT x FROM (...) t`). An ordinary analytical query — joins, a CASE
  aggregate, a CTE pipeline, a window function — measured 6-9 deep; 20
  levels of synthetic subquery nesting measured 62.

## Decision

Three bounds are checked before or during the parse, each set below where
the measured curve turns expensive and above what real SQL needs:

| Bound | Value | Set below |
|---|---|---|
| `_MAX_SQL_CHARS` | 200,000 | the 3 MB / 5s parse cost |
| `_MAX_BRACKET_DEPTH` | 12 | depth 15, where nested `ARRAY[...]` hits 0.76s |
| `_MAX_NESTING_DEPTH` | 100 | AST depth 263, where rendering raises |

Trino itself accepts more than `_MAX_SQL_CHARS`, so that ceiling is
deliberately generous: a 200k-character query is a machine padding the
input, not an analyst asking a question.

## Consequences

- The bounds are pinned by value in `tests/core/test_safety.py`, so a
  stale mutation or an accidental edit fails loudly rather than silently
  disabling a guard.
- `_MAX_BRACKET_DEPTH` is the tightest bound and the one most likely to
  over-block a legitimate generated query; it binds far sooner than
  `_MAX_NESTING_DEPTH`, which matters when reasoning about how deep a plan
  or AST can get in practice.
- These are sqlglot-version-specific measurements. A sqlglot upgrade that
  changes parser performance should re-measure rather than assume the
  numbers still hold.
