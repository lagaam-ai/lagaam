# 0010 — The Pinot dialect preserves the agent's spellings rather than transpiling them

**Status:** Accepted

## Context

ADR 0003 says the agent generates SQL in the target dialect and sqlglot is
used for parse-validation and AST safety checks. The Pinot adapter did the
first half — the dialect card teaches Pinot SQL — but named the *generic*
sqlglot dialect (`""`) for the second, and every statement is re-rendered
by sqlglot before it reaches the broker: `validate_query` injects the
LIMIT, `two_part_sql` strips the synthetic catalog, and both re-render the
tree. The string the broker runs is sqlglot's rendering, not the agent's
text.

The generic generator canonicalises spellings. Pinot's parser does not
accept the canonical ones. Measured on Pinot 1.5.1, both engines, raw SQL
against the re-rendered form:

| the agent wrote | the broker received | effect |
|---|---|---|
| `CAST(x AS STRING)` | `CAST(x AS TEXT)` | error — and STRING is the type the card teaches |
| `JSON_EXTRACT_SCALAR(c, '$.a', 'STRING', 'x')` | `JSON_EXTRACT_SCALAR(c, '$.a')` | arguments 3–4 dropped, error |
| `SUBSTR(c, 0, 1)` / `SUBSTR(c, 1)` | `SUBSTRING(...)` | runs, **returns different rows** |
| `STRPOS(c, 'A')` | `STR_POSITION(...)` | error |
| `LOG10(x)`, `LOG2(x)` | `LOG(10, x)`, `LOG(2, x)` | error — Pinot's LOG takes one argument |
| `TRUNCATE(x[, n])` | `TRUNC(...)` | error |
| `VAR_POP(x)`, `VAR_SAMP(x)` | `VARIANCE_POP`, `VARIANCE` | error |
| `BOOL_AND(p)`, `BOOL_OR(p)` | `LOGICAL_AND`, `LOGICAL_OR` | error |
| `ARRAY['a','b']` | `ARRAY('a','b')` | error on the multi-stage engine |
| `ARRAY_AGG(c, 'STRING')` | `ARRAY_AGG(c)` | argument dropped, error |
| `ARRAY_AGG(c, 'STRING', true)` | — | rejected before it was sent: "more than the max 2 arguments" |

Fourteen rewrites and one outright rejection of valid Pinot SQL. The
`SUBSTR` row is the one that does not announce itself: Pinot's `SUBSTR`
takes a 0-based start and an *end*, `SUBSTRING` takes a 1-based start and a
*length*, so the query runs, returns a plausible answer, and the answer is
wrong. An agent debugging that has no way to see it — the SQL it wrote is
not the SQL that ran.

Full before/after tables over 55 shapes, on both engines:
`docs/superpowers/specs/2026-09-20-pinot-dialect-measurements.md`.

## Decision

Pinot gets its own sqlglot dialect, `server/src/lagaam/adapters/pinot/dialect.py`,
and the card names it. The principle is **preserve, never transpile**: the
card already makes the agent write Pinot SQL, so sqlglot's job here is
validation and two small rewrites (LIMIT, catalog). Where a generic node
cannot say a Pinot call back, the call is kept as written.

A dialect is configuration of sqlglot's own parser and generator — the
class supplies a `FUNCTIONS` table and a `TRANSFORMS` table, and sqlglot
does the parsing. It is not a parser, and ADR 0003's "dialect gaps in
sqlglot are absorbed by generating in-dialect" is exactly this.

Two mechanisms, chosen per name by what the node can hold:

- **Six scalars are dropped from the parser's `FUNCTIONS` table**
  (`JSON_EXTRACT_SCALAR`, `SUBSTR`, `STRPOS`, `LOG10`, `LOG2`, `TRUNCATE`),
  so they parse as `exp.Anonymous`, which preserves the spelling and every
  argument at any arity.
- **The aggregates keep a typed node and are renamed at render time**
  (`VAR_POP`, `VAR_SAMP`, `BOOL_AND`, `BOOL_OR`), and `ARRAY_AGG` gets a
  custom `exp.AggFunc` subclass with variadic arguments, since sqlglot's
  own holds one.

That asymmetry is deliberate. `core/scans.py` decides "a bare aggregate
collapses this select to one row" from `find_all(exp.AggFunc)`. An
anonymous aggregate is invisible to that check, which over-quotes — the
safe direction, but a regression in a number the operator sees. The six
scalars are not aggregates and nothing in core keys on their names.

`CAST(x AS STRING)` is a type mapping, not a function: the generator maps
`TEXT` to `STRING`. `ARRAY[...]` renders inline with the keyword, because
Pinot's parser rejects both the generic `ARRAY(...)` call *and* the bare
`[...]` that sqlglot's own `inline_array_sql` emits.

The card gains one rule — `CAST to Pinot types: STRING, LONG, INT, FLOAT,
DOUBLE, BOOLEAN, TIMESTAMP, BIG_DECIMAL` — so the agent picks a type Pinot
has. `adapters/pinot/__init__.py` imports the module, because core parses
with the card's string and the class must be registered before any parse.

## Consequences

The SQL the broker runs is the SQL the agent wrote, apart from the LIMIT
and the catalog. Re-measured against Pinot 1.5.1 on both engines over the
same 55 shapes: 50 identical, 4 fixed, 0 broken. The two remaining
differences are `VAR_POP` and `VAR_SAMP`, whose last digits differ between
*any* two runs because Pinot sums floats in segment-arrival order —
confirmed by running the raw form three times.

The renames Pinot happens to accept are left to sqlglot rather than
preserved, because they are measured to run: `CEILING→CEIL`,
`DAYOFMONTH→DAY_OF_MONTH`, `ISNAN→IS_NAN`, `ENDSWITH→ENDS_WITH`,
`POW→POWER`, `MOD(a,b)→a % b`, and the four that turn SQL Pinot rejects
into SQL it runs — `IFNULL`/`NVL`→`COALESCE`, `IF(...)`→`CASE`,
`TIMESTAMP_DIFF`→`TIMESTAMPDIFF`. Preserving those would lose four working
shapes to gain nothing.

A parse failure now names the engine: `validate_query`'s message read "could
not be parsed as " and reads "as pinot".

The cost is a dialect to maintain. Each entry in it is a measured claim
about Pinot 1.5.1, and a Pinot release that changes one will not announce
itself — the integration test is what catches it, running every shape raw
and re-rendered on both engines and asserting the rows match.

An unmeasured Pinot function the generic dialect also rewrites remains
possible: the six were found by surveying 400 names in sqlglot's generic
`FUNCTIONS` table for every one that renders under a different spelling or
drops an argument, then filtering to names Pinot recognises. That survey is
a snapshot of sqlglot 30.12, not a proof.
