# Plan — close ADR 0009's null caveat (2026-09-27)

Branch `feat/pinot-keys-nulls`, worktree `/Users/muditkapoor/Documents/code/lagaam-debts`.
Two commits already sit on it (the private-helper rename and the empty-spelling
guard). This plan adds one behaviour change and its docs.

Measurements: `docs/superpowers/specs/2026-09-27-pinot-null-keys-measurements.md` — read it first.

## The pain

A single-segment table whose column is provably unique by segment
cardinality is refused its key whenever the column *might* hold a null.
On `u15nulls` (table null handling on) even `id`, which holds no nulls, is
charged the full product: a self-join quoted 120 instead of 30. Meanwhile the
gate reads only the table flag, so on `u15nullscol` (schema column-based null
handling) it lets columns through. It guards a collision that cannot occur.

## The measured rule

`cardinality` counts distinct stored values; a null is stored as the column's
default and counted once; so any null collision (two nulls, or a null and a
literal default) lowers cardinality below `totalDocs`. `cardinality ==
totalDocs` therefore means every stored value differs, and a join on that
column matches each row at most once under either query-time null mode.
Measured identically under table-level null handling on, off, and schema
column-based null handling; dictionary and raw columns; STRING and INT.

## Task (one commit): `feat(pinot): a unique column is a key whether or not it may hold a null`

In `server/src/lagaam/adapters/pinot/keys.py`:

1. `single_segment_unique_columns`: remove the nullability gate — the
   `nullable_off` / `schema_nullable` / `not_null` lines and the
   `schema_json is None` early return (its only justification was
   nullability). Every other gate stays exactly as it is: exactly one sealed
   segment in the size report, no `-1` entry, metadata complete against it,
   the one entry is that segment, `totalDocs` a positive int, single-valued
   (`_is_multi_valued`), `cardinality == docs`.
2. The function no longer reads `config_json` or `schema_json`: remove both
   parameters, so its signature is `(seg_metadata_json, size_json)`. Update
   `catalog_keys` (which keeps its own keyword-only signature — `upsert_keys`
   still needs config and schema) and every caller in `server/` (grep
   `single_segment_unique_columns(`; about 16 sites, mostly tests).
3. Delete `_null_handling_disabled` and `_schema_nullable_columns`, now
   unused. Drop any import that becomes unused (`FIELD_SPEC_KEYS` if nothing
   else in `keys.py` uses it; `metadata.py` keeps its own).
4. Docstrings: in `single_segment_unique_columns`, replace the two
   paragraphs beginning "Gated on nullability" and "A `schema_json` of None"
   with one paragraph stating the measured rule above in three or four
   sentences, citing the measurements file. In the module docstring, the
   phrase describing source (b) as a "unique non-null single-valued column"
   loses "non-null". Keep every other sentence verbatim.

### Tests (TDD: write/rewrite first, see them fail, then change the code)

`server/tests/adapters/pinot/test_pinot_keys.py`:

- New, fixture-driven, parametrised over the three captured tables
  (`seg-metadata-u15nulls.json`, `size-u15nulls.json`, and the same for
  `u15nullsoff` and `u15nullscol`, already in `server/tests/adapters/pinot/fixtures/`):
  `single_segment_unique_columns(seg, size)` is exactly
  `{{"id"}, {"s_one_null"}, {"s_nodict_distinct"}}` (as frozensets of
  frozensets, lowercase) on every table. That one assertion pins both
  directions: the unique columns are keys, and `s_two_nulls`,
  `s_null_and_default`, `i_two_nulls`, `i_null_and_min`, `s_nodict`,
  `s_dup` are not.
- Also through `catalog_keys(...)` with each table's `tableconfig-*.json`,
  `schema-*.json`, and `table_metadata_json=None`: same result (no upsert).
- The existing tests that asserted the old gate — at least
  `test_a_nullable_column_yields_nothing_however_unique_it_looks`,
  `test_null_handling_disabled_plus_a_non_nullable_schema_is_enough`,
  `test_an_unread_schema_establishes_no_nullability_f1`,
  `test_a_schema_that_says_the_column_cannot_be_null_is_evidence_f1`, and any
  other whose assertion depends on nullability or on an unread schema (grep
  `nullab`, `notNull`, `schema_json=None`, `nullHandlingEnabled` in the
  tests) — encode the rule this change retires. For each: if its scenario
  now yields a key, rewrite it to assert the key and rename it to say so; if
  it duplicates the new fixture test, delete it. List every such test in the
  report with old name → new name/deleted and the old vs new assertion. No
  other test's assertion may change.

`server/tests/adapters/pinot/test_pinot_engine.py`: only call-site updates
if any; no assertion changes expected (report if one is needed, and why).

`server/tests/integration/`: add a fixture `pinot_nulls_ready` in
`conftest.py` that makes the three tables exist on the batch instance
idempotently — for each of `u15nulls`, `u15nullsoff`, `u15nullscol`: if
`GET /tables/<t>` 404s, `POST /schemas` with
`fixtures/pinot-nulls/<t>-schema.json`, `POST /tables` with
`<t>-table.json`, then `POST /ingestFromFile?tableNameWithType=<t>_OFFLINE&batchConfigMapStr={"inputFormat":"json"}`
with `fixtures/pinot-nulls/rows.json` (multipart field `file`); then wait until
`SELECT count(*) FROM <t>` returns 10. (The tables already exist on the live
instance with exactly this data; the fixture must be a no-op there.) Then in
`test_pinot_engine.py` (integration), one parametrised test over
(table × column): quote
`SELECT a.id FROM pinot.default.<t> a JOIN pinot.default.<t> b ON a.<col> = b.<col> LIMIT 10`
with the batch engine and assert `max_intermediate_rows == 30` for `id`,
`s_one_null`, `s_nodict_distinct` and `== 120` for `s_two_nulls`,
`i_null_and_min`, on all three tables; and for every case execute
`SELECT count(*) FROM <t> a JOIN <t> b ON a.<col> = b.<col>` with the
engine's own `execute` and assert the quote is `>=` the pairs actually built
(zero under-quotes). Follow the file's existing patterns for engines and
helpers.

### Docs (same commit)

`docs/adr/0009-consuming-segments-and-proven-join-keys.md`: append an
amendment section `## Amendment 2026-09-27 — the null caveat is closed`
(3–5 sentences: what was measured, the rule, that the nullability gate and
the schema requirement on source (b) are removed, pointer to the
measurements file). In the Decision text (~lines 173–176) and the
Consequences bullet "The null caveat on source (b) is open", do not rewrite
history — add "(closed 2026-09-27, see amendment)" after the relevant
sentence.

## Global constraints

- `server/src/lagaam/core` untouched. mypy strict clean.
- Unit count: 1154 before; report exact arithmetic after (new tests, deleted
  tests).
- Suites before committing, from `server/`: `uv run pytest -q`,
  `uv run mypy`, `uv run pytest -q -m integration` (Trino, Pinot batch
  :9000/:8000, realtime :9001/:8001 are up; report, never start/stop, any
  unreachable engine).
- Commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
  Also commit this plan, the measurements file and the new fixtures (they are
  untracked in the worktree) — in the same commit or a preceding
  `docs: measure how Pinot counts nulls` commit.
- No push, no PR, no containers, no subagents.
