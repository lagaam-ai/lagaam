# Plan — Pinot gets its own sqlglot dialect (2026-09-20)

Branch `fix/pinot-dialect`, worktree `/Users/muditkapoor/Documents/code/lagaam-dialect`.
Measurements and the probe script: `docs/superpowers/specs/2026-09-20-pinot-dialect-measurements.md`,
`docs/superpowers/specs/2026-09-20-pinot-dialect-probe.py`.

## The bug

The Pinot dialect card says `sqlglot_dialect=""` (generic). Every statement is
re-rendered through sqlglot before it reaches the broker (`validate_query` in
`core/safety.py`, then `two_part_sql` in `adapters/pinot/names.py`). The generic
generator canonicalises spellings, and Pinot's parser does not accept the
canonical ones. Measured on Pinot 1.5.1, both engines, fourteen rewrites of
valid Pinot SQL break or silently change the answer, and one valid shape is
rejected before it is ever sent:

| agent wrote | generic dialect sent | effect |
|---|---|---|
| `CAST(x AS STRING)` | `CAST(x AS TEXT)` | error, both engines — and STRING is the type the card teaches |
| `JSON_EXTRACT_SCALAR(c, '$.a', 'STRING', 'x')` | `JSON_EXTRACT_SCALAR(c, '$.a')` | args 3–4 dropped, error |
| `SUBSTR(c, 0, 1)` / `SUBSTR(c, 1)` | `SUBSTRING(...)` | runs, **wrong rows** (Pinot SUBSTR is 0-based start/end; SUBSTRING is 1-based start/length) |
| `STRPOS(c, 'A')` | `STR_POSITION` | error |
| `LOG10(x)`, `LOG2(x)` | `LOG(10, x)`, `LOG(2, x)` | error (Pinot LOG takes one arg) |
| `TRUNCATE(x[, n])` | `TRUNC` | error |
| `VAR_POP(x)`, `VAR_SAMP(x)` | `VARIANCE_POP`, `VARIANCE` | error |
| `BOOL_AND(p)`, `BOOL_OR(p)` | `LOGICAL_AND`, `LOGICAL_OR` | error |
| `ARRAY['a','b']` | `ARRAY('a','b')` | error on MSE (the only engine that runs array literals) |
| `ARRAY_AGG(c, 'STRING')` | `ARRAY_AGG(c)` | arg dropped, error |
| `ARRAY_AGG(c, 'STRING', true)` | — | `validate_query` rejects it: "more than the max 2 arguments" |

Renames that Pinot happens to accept (measured `same` or `fixes`, keep them):
`CEILING→CEIL`, `DAYOFMONTH→DAY_OF_MONTH`, `DAYOFWEEK→DAY_OF_WEEK`,
`DAYOFYEAR→DAY_OF_YEAR`, `WEEKOFYEAR→WEEK_OF_YEAR`, `YEAROFWEEK→YEAR_OF_WEEK`,
`ISNAN→IS_NAN`, `ENDSWITH→ENDS_WITH`, `STARTSWITH→STARTS_WITH`, `POW→POWER`,
`MOD(a,b)→a % b`, `IFNULL/NVL→COALESCE`, `IF(...)→CASE`, `TIMESTAMP_DIFF→TIMESTAMPDIFF`.

## The design (prototyped, verified: 50 same / 4 fixes / 0 breaks on the same table)

A sqlglot **dialect** for Pinot — configuration of sqlglot's own parser and
generator, which is exactly what ADR 0003 permits; not a parser. Principle:
**preserve, never transpile.** The agent writes Pinot SQL (the card makes
sure of it); sqlglot's job here is validation and two small rewrites (LIMIT,
catalog), so where the generic node cannot hold a Pinot call faithfully, the
call is kept as written.

`server/src/lagaam/adapters/pinot/dialect.py`:

```python
from sqlglot import exp, generator, parser
from sqlglot.dialects.dialect import Dialect, inline_array_sql, rename_func

# Calls the generic parser folds into a node that cannot say them back:
# LOG10/LOG2 become LOG(base, x) and Pinot's LOG takes one argument; SUBSTR
# becomes SUBSTRING, which counts from 1, not 0; the rest are renamed to
# spellings Pinot has never had, or lose arguments on the way.
_KEPT_AS_WRITTEN = frozenset({"JSON_EXTRACT_SCALAR", "SUBSTR", "STRPOS", "LOG10", "LOG2", "TRUNCATE"})


class ArrayAgg(exp.Expression, exp.AggFunc):
    """Pinot's ARRAY_AGG(column[, 'TYPE'[, distinct]]); sqlglot's own holds one argument."""
    arg_types = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names = ["ARRAY_AGG", "ARRAYAGG"]


class Pinot(Dialect):   # sqlglot registers the class under its lowercased name: "pinot"
    class Parser(parser.Parser):
        FUNCTIONS = {
            **{name: builder for name, builder in parser.Parser.FUNCTIONS.items() if name not in _KEPT_AS_WRITTEN},
            "ARRAY_AGG": lambda args: ArrayAgg(this=args[0], expressions=args[1:]),
            "ARRAYAGG": lambda args: ArrayAgg(this=args[0], expressions=args[1:]),
        }

    class Generator(generator.Generator):
        TYPE_MAPPING = {**generator.Generator.TYPE_MAPPING, exp.DataType.Type.TEXT: "STRING"}
        TRANSFORMS = {
            **generator.Generator.TRANSFORMS,
            exp.Array: inline_array_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Variance: rename_func("VAR_SAMP"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            ArrayAgg: <render as ARRAY_AGG(this, *expressions)>,
        }
```

Why aggregates are renamed at render time rather than kept as `Anonymous`:
`core/scans.py` decides "a bare aggregate collapses this select to one row"
by `find_all(exp.AggFunc)`. An anonymous aggregate is invisible to that, which
over-quotes (safe direction, but a regression). `ARRAY_AGG` therefore gets its
own `AggFunc` node rather than falling back to `Anonymous`. The six scalar
names kept as written are not aggregates and nothing in core keys on them
(verify: grep `exp\.` in `core/scans.py`; `_INJECTIVE_FUNCS` /
`_ONE_VALUE_PER_QUERY` are name-based and do not list them).

`PINOT_DIALECT_CARD.sqlglot_dialect = "pinot"`. `adapters/pinot/__init__.py`
imports `dialect` so the registration side effect happens on package import
(core parses with the string, so the class must exist before any parse).
Side benefit: `validate_query`'s error text currently reads "could not be
parsed as " — it will now say "as pinot".

Add one card rule so the agent picks types Pinot has:
`"CAST to Pinot types: STRING, LONG, INT, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP, BIG_DECIMAL"`.

## Tasks (strict TDD: failing test first, then the smallest change, then run the suite)

1. **Unit — `tests/adapters/pinot/test_pinot_dialect.py`.** Replace the
   "generic dialect" docstring and assertions. Add:
   - registration: `Dialect.get_or_raise("pinot")` works after importing
     `lagaam.adapters.pinot`.
   - one parametrised test over the 15 shapes in the table above:
     `two_part_sql(validate_query(sql, "pinot", default_limit=5))` contains the
     spelling the agent wrote (the exact expected fragments are in the
     measurements file, "After" table, column 2).
   - aggregates stay typed: parse `SELECT ARRAY_AGG(c, 'STRING', TRUE) AS a, VAR_POP(x) AS v, BOOL_AND(x > 1) AS b FROM t`
     with `dialect="pinot"`; `find_all(exp.AggFunc)` yields three nodes.
   - a kept-as-written scalar is `exp.Anonymous` with its name intact and all
     arguments intact for arities 1–4 (`JSON_EXTRACT_SCALAR` with 4).
   - the accepted renames still render (`IFNULL→COALESCE`, `IF→CASE`, `POW→POWER`,
     `DAYOFWEEK→DAY_OF_WEEK`) — they are measured to run on Pinot.
   - core semantics unchanged under the dialect: `SELECT *` rejected, the
     measured `INSERT INTO ... FROM FILE` rejected, LIMIT injected,
     `referenced_columns("SELECT count(*) FROM pinot.default.t")` is not None
     (`exp.Count` still typed so `count(*)` is not read as a projected star).
   - the parse error names the dialect: message contains "as pinot".
   Update `tests/integration/test_pinot_engine.py::test_dialect_card_targets_pinot`
   to `"pinot"`.
2. **Implement `dialect.py`** as above; `__init__.py` import. Run
   `uv run pytest -q` (unit, ~980 tests; all must pass — `names.py`,
   `engine.py`, `scans` paths all parse with the card's string) and
   `uv run mypy` (strict on `lagaam.core`, `lagaam.adapters.pinot`; the
   lambda builders and the custom node must type-check — use a small named
   function if a lambda will not).
3. **Integration — `tests/integration/test_pinot_engine.py`.** One
   parametrised test over the 15 shapes plus 4 controls, on both engines:
   raw SQL via `PinotClient.broker_query` (read its signature; options string
   `useMultistageEngine=true|false`) versus the re-rendered form; rows equal,
   `VAR_POP`/`VAR_SAMP` under `pytest.approx(rel=1e-9)`. Uses `pinot_ready`.
   Run: `uv run pytest -q -m integration tests/integration/test_pinot_engine.py`
   (containers `lagaam-pinot` on :9000/:8000 are up; do **not** start or
   stop containers). Note the file already imports `two_part_sql` and
   `validate_query`.
4. **Docs.** `docs/adr/0010-pinot-sqlglot-dialect-preserves-spellings.md`
   in the house style of 0008/0009 (Status / Context / Decision /
   Consequences; developer-pain language; the table; the measurement
   pointer; why this is not a custom parser per ADR 0003; why aggregates
   stay typed). Add its row to `docs/adr/README.md`. Fix the docstring in
   `dialect.py` and the test module. Leave the 2026-09-11 plan/spec files as
   the historical record they are.
5. **Commits** (conventional, atomic, each ends with the line
   `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`):
   1. `fix(pinot): a sqlglot dialect that sends Pinot the SQL the agent wrote`
      (dialect + unit tests + card rule + `__init__`)
   2. `test(pinot): raw and re-rendered SQL answer alike on both engines`
   3. `docs(adr): 0010 — the Pinot dialect preserves spellings; measurements`
   Do not push. Do not open a PR.

## Report back with

- unit count before/after and the integration run output (pass counts);
  `mypy` output;
- the after-table verdict counts from re-running
  `docs/superpowers/specs/2026-09-20-pinot-dialect-probe.py` against the
  real implementation (adapt its imports: it must use the card's dialect,
  not a prototype) — expected 50 same / 4 fixes / 2 float-noise DIFF;
- anything the generic dialect did that you found and this plan does not
  cover.
