# Plan — the join-key proof lives in one module: `pinot/keys.py` (2026-09-27)

Branch `refactor/pinot-keys`, worktree `/Users/muditkapoor/Documents/code/lagaam-keys`,
base `origin/main` 1c898d5 (v0.2.1).

Spec: ADR 0009 (`docs/adr/0009-consuming-segments-and-proven-join-keys.md`) is
the authority for what the key rule *does*. This plan changes where it lives,
never what it does.

## Why

The rule that decides when a Pinot join is bounded by a proven key — rather
than charged the full product — is the most security-sensitive logic in the
adapter (it is what refuses the `SELECT Origin AS Carrier` alias spoof). It is
spread across three files today:

- `metadata.py`: which keys the catalog proves (upsert primary key without a
  TTL; a unique non-null single-valued column on a one-sealed-segment table).
- `plan.py`: learning each key column's scan ordinal, composing a join
  operand down its side to an ordinal, deciding whether a side is covered.
- `engine.py`: which tables get a key-ordinal EXPLAIN and how it is spelled.

After this change a reviewer reads one module, `keys.py`, to audit the rule.

## Target layering (no import cycles)

```
rels.py      generic Calcite-JSON graph reading          (imports nothing from pinot/)
metadata.py  controller documents -> facts               (imports nothing new)
keys.py      the key rule: catalog proof, ordinals, join coverage
             (imports rels.py and metadata.py)
plan.py      widest row count                            (imports rels.py, keys.py)
engine.py    I/O: fetches, EXPLAINs                      (imports keys.py, plan.py, metadata.py)
```

Two cycles would exist without this shape, and the tasks are built to avoid
them: `plan._join_rows` needs key coverage while key coverage needs plan's
graph helpers (solved by `rels.py`), and `metadata.table_facts` computed keys
while the key sources need metadata's document helpers (solved by
`table_facts` receiving `unique_keys` from its caller).

## Global Constraints

1. **No behaviour change.** Every existing test assertion stays byte-identical.
   A test may change only: its import lines, the file it lives in, and — in
   exactly the four `table_facts` cases Task 3 names — which function it calls
   to get the same expected value.
2. **Code, docstrings and comments move verbatim.** They are measured
   records. The only permitted edits inside moved text are renames listed in
   this plan and references to a module's new name.
3. **Test count is an invariant.** Baseline on 1c898d5:
   `uv run pytest --collect-only -q` → `1132/1325 tests collected (193 deselected)`.
   Per file: `test_pinot_plan.py` 65, `test_pinot_metadata.py` 202,
   `test_pinot_engine.py` 140. After each task the total unit count is 1132
   plus exactly the new tests that task adds, and moved tests are counted in
   their new file. Report the arithmetic.
4. `git diff --stat origin/main -- server/src/lagaam/core` stays empty.
5. `uv run mypy` (strict on `lagaam.core`, `lagaam.adapters.pinot`) clean.
6. Commits: conventional, `refactor(pinot): …` for moves, `docs: …` for Task 5;
   one commit per task; every message ends with the exact line
   `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
7. Do not push, open a PR, or start/stop/restart containers. Engines are up:
   Trino, Pinot batch (controller :9000, broker :8000), Pinot realtime
   (:9001/:8001).
8. Inline comments one line max, only for constraints code cannot show
   (CLAUDE.md). A new module gets a docstring; moved docstrings are not
   rewritten.

## Task 1: `rels.py` — the generic plan-graph helpers

Create `server/src/lagaam/adapters/pinot/rels.py` holding the Calcite JSON
graph primitives that both `plan.py` and the future `keys.py` need. Move from
`plan.py`, renaming private to public because they now cross a module line:

| in plan.py | in rels.py |
|---|---|
| `_MAX_DEPTH` (value 400, its comment line) | `MAX_DEPTH` |
| `_rels(plan_json)` | `parse_rels(plan_json)` |
| `_children(rel_id, rel, previous)` | `children(rel_id, rel, previous)` |
| `_suffix(rel)` | `suffix(rel)` |

Add one new function, extracted from the two identical loops in
`max_intermediate_rows` and `key_ordinals`:

```python
def index_rels(
    rels: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], dict[str, str | None], list[str]] | None:
    """by_id, each node's predecessor, and document order; None on a missing or repeated id."""
```

Its body is exactly the loop both call sites run today (`rel_id =
rel.get("id")`; not a str or already seen → None; `by_id[rel_id] = rel`;
`previous[rel_id] = last`; append to `order`; `last = rel_id`). Replace both
loops with a call to it; `key_ordinals` keeps its own `if last is None:
return None` check, reading the last element of `order` (empty `order` → None).

`plan.py` imports these from `rels.py`; every use site in `plan.py` switches
to the new names. Nothing else in `plan.py` changes in this task.

`rels.py` module docstring (new): two to four sentences — Pinot's multi-stage
`EXPLAIN … AS JSON` is a flat topological `rels[]` list, a node with no
`inputs` key consumes the node before it (move that sentence from plan.py's
docstring verbatim), and these are the readers every plan consumer shares.

Tests (TDD: write first, see them fail on the missing module):
new `server/tests/adapters/pinot/test_pinot_rels.py` with
- `index_rels` on a two-node list returns `by_id` with both ids, `previous`
  `{"0": None, "1": "0"}`, order `["0", "1"]`;
- a repeated id → None; a non-string id (int `0`) → None;
- `children` of a node with no `inputs` key is `[previous]`, and `[]` when it
  has no predecessor; a non-list `inputs` → None;
- `suffix` of `{"relOp": "org.apache.calcite.rel.logical.LogicalProject"}` is
  `"logicalproject"`, and `""` when `relOp` is missing.

Expected count: 1132 + the new rels tests. All existing tests pass unchanged.

## Task 2: `keys.py`, part one — ordinals and join coverage, out of `plan.py`

Create `server/src/lagaam/adapters/pinot/keys.py`. Move from `plan.py`:

| in plan.py | in keys.py |
|---|---|
| `_PASS_THROUGH`, `_SCAN`, `_PROJECT` (with their comments) | same private names |
| `_Evidence` | `Evidence` |
| `SideFields` | `SideFields` |
| `key_ordinals(plan_json)` | `key_ordinals(plan_json)` |
| `_covered_sides(...)` | `covered_sides(...)` |
| `_side_covered`, `_covers`, `_ordinal_at`, `_project_reads` | same private names |
| `join_key_pairs(...)` | `join_key_pairs(...)` |
| `side_fields(...)` | `side_fields(...)` |
| `_scan_table`, `_equalities`, `_op_kind`, `_operand_indexes` | same private names |

`keys.py` imports `MAX_DEPTH`, `parse_rels`, `index_rels`, `children`,
`suffix` from `rels.py`. `plan.py` imports `Evidence` and `covered_sides`
from `keys.py`; `_rows`, `_join_rows` and `max_intermediate_rows` keep their
signatures and bodies apart from the renamed calls. `max_intermediate_rows`'s
signature and docstring are unchanged.

Docstrings: `plan.py`'s module docstring has a final paragraph beginning "A
catalog key is the one thing that lowers a join below the product" — move it
verbatim into `keys.py`'s module docstring. In `plan.py`'s docstring, the
paragraph beginning "The product holds for an equi-join too. Nothing on 1.5.1
can prove a join key" is stale since U12; replace its last two sentences'
claim with one sentence: the plan alone proves no key, the catalog can, and
`keys.py` decides when it has (keep the measured 10,719,442-pairs sentence
and the ADR 0004 sentence verbatim). `keys.py`'s docstring opens with one
new sentence naming what the module is: the whole rule for when a Pinot join
may be charged less than the product, per ADR 0009.

`engine.py` imports `key_ordinals` from `keys.py` instead of `plan.py`.

Tests: move every test in `tests/adapters/pinot/test_pinot_plan.py` whose
subject is `key_ordinals` or `join_key_pairs` into a new
`tests/adapters/pinot/test_pinot_keys.py`, bodies verbatim. That is the four
`test_key_ordinals_*` tests (with their parametrisations) and every test that
calls the module helper `join_pairs(...)` (which calls `join_key_pairs`); the
`join_pairs` helper moves with them. Module-level helpers they need that
other plan tests still use (e.g. `plan_cell`, `FIXTURES`) are copied, not
removed. Tests that
exercise keys through `max_intermediate_rows` stay in `test_pinot_plan.py`.
TDD for a move: first create `test_pinot_keys.py` importing from
`lagaam.adapters.pinot.keys` and watch collection fail, then move the code.

Expected count: unchanged total (moved, not added). Report per-file counts.

## Task 3: `keys.py`, part two — the catalog's proof, out of `metadata.py`

Move from `metadata.py` into `keys.py`, names unchanged:
`upsert_keys`, `upsert_config_present`, `single_segment_unique_columns`,
`_is_multi_valued`, `_has_upsert_config`, `_UPSERT_TTL_KEYS` (with its
comment), `_ttl_is_set`, `_has_primary_key_counts`, `_null_handling_disabled`,
`_schema_nullable_columns`.

`keys.py` imports from `metadata.py` what those need: `metadata_is_complete`,
`_reported_sizes`, `_positive_int`, `_FIELD_SPEC_KEYS` (private names
imported as they are; do not rename them in `metadata.py`).

Add to `keys.py`:

```python
def catalog_keys(
    config_json: Any,
    seg_metadata_json: Any,
    schema_json: Any,
    size_json: Any,
    table_metadata_json: Any,
) -> frozenset[frozenset[str]]:
    """Every column set the catalog proves unique on this table."""
    return upsert_keys(config_json, schema_json, table_metadata_json) or (
        single_segment_unique_columns(
            seg_metadata_json, config_json, schema_json, size_json
        )
    )
```

This is exactly the expression `table_facts` evaluates today.

`metadata.table_facts`: remove the `unique_keys=` computation and add a
keyword-only parameter `unique_keys: frozenset[frozenset[str]] = frozenset()`
passed straight into `TableFacts`. Its `schema_json` and `table_metadata_json`
parameters stay (other callers pass them). Update its docstring's sentence
"no consuming segments, no threshold, complete, no key evidence" only if it
becomes untrue — it stays true, since the default is empty.

`engine._table_facts`: pass `unique_keys=catalog_keys(config,
seg_json, schema_json or table_schema_json, <size as table_facts receives
it>, table_metadata_json)` — the same five documents, with the same
`NotFound → None` conversions, that `table_facts` received. Import
`upsert_config_present` from `keys.py` instead of `metadata.py`.

Tests: move every test in `test_pinot_metadata.py` whose subject is one of
the moved functions into `test_pinot_keys.py`, bodies verbatim. The four
assertions on `table_facts(...).unique_keys` (currently near lines 501, 1116,
1162, 1177 of `test_pinot_metadata.py`) change to assert the same expected
value on `catalog_keys(...)` called with the same documents `table_facts`
received there, and move with the key tests. If any of those tests also
asserts non-key facts from the same `table_facts` call, split it: the
non-key assertions stay on `table_facts` in `test_pinot_metadata.py`, the key
assertion moves — say so in the report, since it adds a test. Measured
before dispatch: the tests at ~501 and ~1116 do assert other facts, so
expect exactly those two splits.

Add one engine test in `test_pinot_engine.py` pinning the wiring: the
existing upsert fixture path (find the test that quotes an upsert self-join
through the engine) already proves keys reach `TableFacts`; if none asserts
`facts.unique_keys` via `engine._table_facts`, add one that does, using the
fake transport and fixtures that test already uses.

Integration: `uv run pytest -q -m integration tests/integration/test_pinot_engine.py tests/integration/test_demo_pinot.py`.

## Task 4: `keys.py`, part three — what the key-ordinal EXPLAIN is spelled with

Move from `engine.py` into `keys.py`:

| in engine.py | in keys.py |
|---|---|
| `_Keycols` (frozen dataclass, docstring verbatim) | `KeyColumns` |
| `_is_bare_identifier` | `_is_bare_identifier` |

Extract the pure part of `PinotEngine._record_keycols` into:

```python
def key_columns(
    database: str,
    spelled: str,
    spellings: Mapping[str, str],
    unique_keys: frozenset[frozenset[str]],
) -> KeyColumns | None:
```

— from `names = sorted({name for key in unique_keys for name in key})`
through the construction of the record, with both early `return`s becoming
`return None` and their one-line comments kept. `_record_keycols` keeps the
schema-spellings selection and the fetch (it is I/O), then does
`subject = key_columns(database, spelled, spellings, facts.unique_keys)` and
stores it under the same key when not None. Its docstring stays.

Extract the SQL text `_key_ordinals` builds into:

```python
def key_ordinal_sql(subject: KeyColumns) -> str:
    """SELECT <key columns> FROM <database>.<table>, no LIMIT, no ORDER BY."""
```

returning exactly `f"SELECT {', '.join(subject.columns)} FROM {subject.database}.{subject.table}"`;
`_key_ordinals` prepends `_EXPLAIN_SHAPE` as it does today. Its docstring
stays on `_key_ordinals`.

Rename every `_Keycols` annotation in `engine.py` to `KeyColumns`.

Tests (new, pinning current behaviour, in `test_pinot_keys.py`):
- `key_columns` resolves lowercase key names through `spellings` to the
  catalog's spelling, sorted by lowercase name, and returns the database and
  table as given;
- a key column missing from `spellings` → None;
- empty `unique_keys` → None;
- a database, table, or column spelling that is not a bare identifier → None,
  one case each: `"a,b"`, `"x--"`, `"ünï"`, `""` (empty spelling);
- a composite key `frozenset({frozenset({"a", "b"})})` yields both columns;
- `key_ordinal_sql` of a two-column subject is exactly
  `"SELECT A, B FROM default.T"` for columns `("A", "B")`, database
  `"default"`, table `"T"`.

Integration as in Task 3.

## Task 5: docs

- `docs/architecture.md`'s Pinot module table (around line 169): add rows for
  `rels.py` and `keys.py` in the table's existing style, and adjust the
  `plan.py` and `metadata.py` rows only where they claimed key logic.
- `docs/adr/0009-consuming-segments-and-proven-join-keys.md`: add one line
  at the end of the amendments: "2026-09-27: the rule this ADR decides lives
  in `server/src/lagaam/adapters/pinot/keys.py`." No other ADR edits.
- grep `docs/`, `README.md`, `CLAUDE.md` and `server/src` docstrings for
  references to the moved functions by old module (`plan.key_ordinals`,
  `metadata.upsert_keys`, `_Keycols`, `_record_keycols`'s old contents) and
  fix any that are now wrong. Leave `docs/superpowers/` plans and specs as
  the historical record.
