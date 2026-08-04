# Plan-Based Cardinality Gate Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the SQL-shape proxies that decide whether a query explodes with Trino's own per-operator row estimates, and delete the ~16 helpers those proxies live in.

**Architecture:** A new pure module `adapters/trino/plan.py` walks `EXPLAIN (TYPE LOGICAL, FORMAT JSON)` and returns the maximum intermediate row count. `TrinoEngine._estimate_cost` makes one extra EXPLAIN call and puts that number on `CostEstimate`. `enforce_budget` gains a dimension for it. `core/scans.py` keeps only the row-generator check, which the planner provably cannot see.

**Tech Stack:** Python 3.12+, pydantic, sqlglot, trino dbapi client, pytest (`asyncio_mode = "auto"`), mypy strict on `lagaam.core`, uv.

## Global Constraints

- Branch: `feat/plan-cardinality-gate` (already created, off `review/all-fixes`).
- Working dir for all commands: `/Users/muditkapoor/Documents/code/lagaam-combined/server`.
- Conventional commits, atomic: one logical change + its tests per commit.
- **No AI watermark and no `Co-Authored-By` line in any commit.**
- Inline comments: one line max, only for a constraint the code cannot show.
- Type hints everywhere; `uv run mypy` must stay clean (scoped to `lagaam.core`).
- `uv run pytest -q` must pass at the end of every task (integration tests are deselected by default via `-m 'not integration'`).
- Core never imports an engine SDK. Plan JSON parsing lives in `adapters/trino/`.
- Fail-safe rule, unchanged: a number we cannot measure is a denial, never an admission.
- All numbers cited below were measured on live Trino 476 against `tpch.tiny`; see `docs/superpowers/specs/2026-08-03-plan-cardinality-measurements.md`.

---

## File Structure

**Create:**
- `src/lagaam/adapters/trino/plan.py` — walks logical-plan JSON, returns max intermediate rows. Pure, never raises.
- `tests/adapters/test_trino_plan.py` — unit tests over captured real plan JSON.

**Modify:**
- `src/lagaam/core/models.py` — add `CostEstimate.max_intermediate_rows`.
- `src/lagaam/core/budget.py` — add `QueryBudget.max_intermediate_rows` + enforcement.
- `src/lagaam/adapters/trino/engine.py` — second EXPLAIN call; fill the field.
- `src/lagaam/core/scans.py` — delete the join/product/correlation half.
- `src/lagaam/core/safety.py` — add a nesting-depth cap (F3).
- `src/lagaam/__main__.py` — document the new env var.
- `tests/core/test_scans.py` — delete tests for deleted behaviour.
- `tests/core/test_budget.py`, `tests/core/test_models.py`, `tests/core/test_safety.py` — new dimension, new field, depth cap.
- `tests/integration/test_trino_engine.py` — the 32-shape corpus.
- `docs/` — README/config docs for the new knob.

---

### Task 1: Plan reader — the maximum intermediate row count

**Files:**
- Create: `src/lagaam/adapters/trino/plan.py`
- Test: `tests/adapters/test_trino_plan.py`

**Interfaces:**
- Consumes: `lagaam.adapters.trino.numbers.finite_number(value: Any) -> float | None` (existing; already maps the JSON string `"NaN"` and `"Infinity"` to `None`).
- Produces: `max_intermediate_rows(plan_json: str) -> float | None`.

**Context the implementer needs:**

Trino's logical plan JSON is a tree: each node has `name` (str), `estimates` (a list of dicts, each possibly carrying `outputRowCount`), and `children` (a list of nodes). An unknown estimate arrives as the **JSON string `"NaN"`**, not a float. A node can carry several estimates (one per plan alternative); take the greatest finite one.

The rule, measured:
- A join node with **no finite estimate** is charged the **product of its children's known rows**. This is the whole point: `CROSS JOIN ... WHERE comment LIKE '%x%'` reports `NaN` at the `CrossJoin`, and 60,175 x 15,000 = 902,625,000 is the number that must be blocked. Without this, five different laundering shapes (LIMIT, count(*), DISTINCT, GROUP BY, LIKE) all read as cheap while doing 902M rows of work.
- A non-join node with no finite estimate takes the **max** of its children (it cannot manufacture rows).
- The query's number is the max over every node.

- [ ] **Step 1: Write the failing tests**

Create `tests/adapters/test_trino_plan.py`:

```python
"""The plan reader: what the widest operator in the plan would produce."""

import json

from lagaam.adapters.trino.plan import max_intermediate_rows


def _node(name: str, rows: object, children: list[dict] | None = None) -> dict:
    """One plan node. rows=None means the estimate list is empty."""
    estimates = [] if rows is None else [{"outputRowCount": rows}]
    return {"name": name, "estimates": estimates, "children": children or []}


def test_a_healthy_join_reports_its_own_row_count() -> None:
    # Measured on Trino 476: lineitem JOIN orders ON orderkey -> 60,175.
    plan = _node(
        "Output",
        60175.0,
        [
            _node(
                "InnerJoin",
                60175.0,
                [_node("ScanFilter", 60175.0), _node("TableScan", 15000.0)],
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0


def test_a_nan_join_is_charged_the_product_of_its_children() -> None:
    # The laundering shape: CROSS JOIN under a LIKE '%x%' filter reports NaN
    # at the join while still doing 60,175 x 15,000 rows of work.
    plan = _node(
        "Output",
        "NaN",
        [
            _node(
                "CrossJoin",
                "NaN",
                [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 60175.0 * 15000.0


def test_a_nan_non_join_takes_the_widest_child() -> None:
    # A filter cannot manufacture rows, so an unknown one is not a product.
    plan = _node(
        "Output",
        "NaN",
        [_node("ScanFilterProject", "NaN", [_node("TableScan", 15000.0)])],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 15000.0


def test_the_widest_operator_wins_over_a_narrow_output() -> None:
    # A LIMIT collapses the output to 10 while the join still built 902M.
    plan = _node(
        "Output",
        10.0,
        [
            _node(
                "Limit",
                10.0,
                [
                    _node(
                        "CrossJoin",
                        902625000.0,
                        [_node("TableScan", 60175.0), _node("TableScan", 15000.0)],
                    )
                ],
            )
        ],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 902625000.0


def test_several_estimates_take_the_greatest_finite_one() -> None:
    node = {
        "name": "ScanFilterProject",
        "estimates": [
            {"outputRowCount": 15000.0},
            {"outputRowCount": "NaN"},
            {"outputRowCount": 60175.0},
        ],
        "children": [],
    }
    assert max_intermediate_rows(json.dumps(node)) == 60175.0


def test_an_unknown_node_name_is_not_treated_as_a_join() -> None:
    # Conservative default: a name we do not know cannot invent a product.
    plan = _node(
        "SomeFutureTrinoNode",
        "NaN",
        [_node("TableScan", 100.0), _node("TableScan", 200.0)],
    )
    assert max_intermediate_rows(json.dumps(plan)) == 200.0


def test_a_plan_with_nothing_knowable_is_none() -> None:
    plan = _node("Output", "NaN", [_node("TableScan", "NaN")])
    assert max_intermediate_rows(json.dumps(plan)) is None


def test_malformed_input_is_none_not_a_crash() -> None:
    assert max_intermediate_rows("not json") is None
    assert max_intermediate_rows("null") is None
    assert max_intermediate_rows("[]") is None
    assert max_intermediate_rows(json.dumps({"name": "Output"})) is None


def test_junk_inside_a_well_formed_plan_is_survived() -> None:
    plan = {
        "name": "Output",
        "estimates": "not-a-list",
        "children": [
            {"name": "TableScan", "estimates": [{"outputRowCount": 42.0}]},
            "not-a-node",
            None,
        ],
    }
    assert max_intermediate_rows(json.dumps(plan)) == 42.0


def test_a_pathologically_deep_plan_is_refused_rather_than_recursed() -> None:
    node: dict = {"name": "TableScan", "estimates": [{"outputRowCount": 1.0}]}
    for _ in range(5000):
        node = {"name": "Project", "estimates": [], "children": [node]}
    assert max_intermediate_rows(json.dumps(node)) is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/adapters/test_trino_plan.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'lagaam.adapters.trino.plan'`

- [ ] **Step 3: Write the implementation**

Create `src/lagaam/adapters/trino/plan.py`:

```python
"""The widest row count anywhere in Trino's logical plan.

The IO plan prices bytes scanned, which is correct and irrelevant when the
row work is quadratic: a cross join reads both inputs exactly once and
produces their product. Trino's own optimizer already estimates that
product, per operator, and EXPLAIN (TYPE LOGICAL, FORMAT JSON) reports it.

The *maximum over operators* is the quantity, not the query's output count.
Measured on Trino 476, a 902,625,000-row cross join reports 10 output rows
under a LIMIT, 1 under count(*), and 15,000 under DISTINCT or GROUP BY —
while doing the full 902M rows of work either way. Only the widest
intermediate survives all four rewrites.

Unknown estimates arrive as the JSON string "NaN", and they propagate
upward: a filter Trino cannot size makes every operator above it unknown.
A join in that state is charged the product of what it joins, because the
alternative is pricing the laundered cross join at its children's size.
"""

import json
from typing import Any

from lagaam.adapters.trino.numbers import finite_number

# Nodes whose output can exceed their inputs. An unknown estimate on one of
# these is a product, not a passthrough.
_JOIN_NODES = frozenset(
    {
        "Join",
        "InnerJoin",
        "CrossJoin",
        "LeftJoin",
        "RightJoin",
        "FullJoin",
        "SemiJoin",
        "IndexJoin",
        "NestedLoopJoin",
        "SpatialJoin",
        "CorrelatedJoin",
        "Apply",
        "ApplyNode",
    }
)

# A plan this deep is a machine's, not an analyst's, and recursing it would
# raise where the caller expects a number.
_MAX_PLAN_DEPTH = 400


def max_intermediate_rows(plan_json: str) -> float | None:
    """The widest row count any operator in this plan would produce.

    None means no operator carried a usable estimate — no quote, which the
    budget treats as a denial rather than as a cheap query.
    """
    try:
        root = json.loads(plan_json)
    except (json.JSONDecodeError, TypeError, ValueError):
        return None
    if not isinstance(root, dict):
        return None
    widest: list[float] = []
    _visit(root, widest, 0)
    return max(widest) if widest else None


def _visit(node: dict[str, Any], widest: list[float], depth: int) -> float | None:
    """This node's rows, recording every knowable count into ``widest``."""
    if depth > _MAX_PLAN_DEPTH:
        return None
    children = [
        _visit(child, widest, depth + 1)
        for child in _children(node)
    ]
    known = [rows for rows in children if rows is not None]
    rows = _own_estimate(node)
    if rows is None and known:
        if node.get("name") in _JOIN_NODES:
            # A join whose size Trino could not estimate still pairs its
            # inputs; charging less than the product is how a laundered
            # cross join reads as the size of one of its tables.
            rows = 1.0
            for child_rows in known:
                rows *= child_rows
        else:
            rows = max(known)
    if rows is not None:
        widest.append(rows)
    return rows


def _children(node: dict[str, Any]) -> list[dict[str, Any]]:
    children = node.get("children")
    if not isinstance(children, list):
        return []
    return [child for child in children if isinstance(child, dict)]


def _own_estimate(node: dict[str, Any]) -> float | None:
    """The greatest finite outputRowCount this node reports, if any.

    A node carries one estimate per plan alternative; the greatest is the
    one to price, since we cannot know which alternative runs.
    """
    estimates = node.get("estimates")
    if not isinstance(estimates, list):
        return None
    best: float | None = None
    for estimate in estimates:
        if not isinstance(estimate, dict):
            continue
        rows = finite_number(estimate.get("outputRowCount"))
        if rows is None:
            continue
        best = rows if best is None else max(best, rows)
    return best
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/adapters/test_trino_plan.py -q`
Expected: PASS, 10 tests.

Note on the deep-plan test: `_MAX_PLAN_DEPTH` returns `None` for nodes past the cap, but shallower nodes still record. The 5000-deep fixture has its only estimate at the *bottom*, past the cap, so nothing is recorded and the result is `None`. If Python's own recursion limit trips before the cap, lower `_MAX_PLAN_DEPTH` until the test passes — the cap exists to keep this function total.

- [ ] **Step 5: Verify mypy is clean and commit**

```bash
uv run mypy
uv run pytest -q
git add src/lagaam/adapters/trino/plan.py tests/adapters/test_trino_plan.py
git commit -m "feat(trino): read the widest row count from the logical plan"
```

---

### Task 2: Carry the number on CostEstimate

**Files:**
- Modify: `src/lagaam/core/models.py:53-73`
- Test: `tests/core/test_models.py`

**Interfaces:**
- Consumes: nothing from Task 1 (the field is engine-neutral).
- Produces: `CostEstimate.max_intermediate_rows: int | None`, defaulting to `None`.

**Why a new field rather than reusing `row_estimate`:** `row_estimate` is the sum of rows *scanned* from the IO plan. The new number is the widest *intermediate*, a different and stronger quantity — a cross join scans 75,175 rows and builds 902,625,000. Overloading one field would make the existing scanned-row budget silently mean something else.

- [ ] **Step 1: Write the failing test**

Add to `tests/core/test_models.py`:

```python
def test_max_intermediate_rows_defaults_to_unknown() -> None:
    assert CostEstimate(scanned_bytes=10).max_intermediate_rows is None


def test_max_intermediate_rows_is_carried() -> None:
    estimate = CostEstimate(scanned_bytes=10, max_intermediate_rows=902_625_000)
    assert estimate.max_intermediate_rows == 902_625_000


def test_max_intermediate_rows_does_not_decide_confidence() -> None:
    # Confidence tracks the byte number; a plan estimate is a separate axis.
    assert CostEstimate(max_intermediate_rows=5).confidence == "low"
    assert (
        CostEstimate(scanned_bytes=10, max_intermediate_rows=5).confidence == "high"
    )
```

- [ ] **Step 2: Run to verify it fails**

Run: `uv run pytest tests/core/test_models.py -q -k max_intermediate`
Expected: FAIL — pydantic rejects the unexpected keyword, or the attribute is missing.

- [ ] **Step 3: Add the field**

In `src/lagaam/core/models.py`, inside `CostEstimate`, after `row_estimate`:

```python
    # The widest row count any operator would produce — the number a cross
    # join blows and a byte sum cannot see. None when the engine has no plan
    # estimates to offer (a non-Trino adapter, or an unreadable plan).
    max_intermediate_rows: int | None = None
```

- [ ] **Step 4: Run to verify it passes**

Run: `uv run pytest tests/core/test_models.py -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
uv run mypy
git add src/lagaam/core/models.py tests/core/test_models.py
git commit -m "feat(core): carry the widest intermediate row count on a quote"
```

---

### Task 3: Gate on it

**Files:**
- Modify: `src/lagaam/core/budget.py`
- Test: `tests/core/test_budget.py`

**Interfaces:**
- Consumes: `CostEstimate.max_intermediate_rows` from Task 2.
- Produces: `QueryBudget.max_intermediate_rows: int | None`, env `LAGAAM_MAX_INTERMEDIATE_ROWS`, and `DEFAULT_MAX_INTERMEDIATE_ROWS = 1_000_000_000`.

**Why 1e9:** measured bands are legitimate 15,000–240,700 and explosions 225,000,000–902,625,000. 1e9 sits above every legitimate shape by 4,100x and below the smallest explosion by 4.5x.

**Fail-safe subtlety:** unlike bytes, a missing `max_intermediate_rows` must **not** be treated as low confidence coming from an adapter that has no plan estimates at all (Pinot, v0.2) — but for the Trino adapter it means the plan was unreadable, which is exactly when to block. The gate therefore blocks on a missing number whenever the dimension is configured; an adapter that cannot produce the number must not be run with the dimension gated. This is documented in the denial text and in the config docs.

- [ ] **Step 1: Write the failing tests**

Add to `tests/core/test_budget.py`:

```python
def test_a_query_within_the_intermediate_row_budget_passes() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    estimate = CostEstimate(scanned_bytes=10, max_intermediate_rows=240_700)
    enforce_budget(estimate, budget)


def test_a_product_over_the_intermediate_row_budget_is_denied() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    estimate = CostEstimate(scanned_bytes=10, max_intermediate_rows=902_625_000_0)
    with pytest.raises(BudgetExceededError) as err:
        enforce_budget(estimate, budget)
    message = str(err.value)
    assert "9,026,250,000" in message
    assert "1,000,000,000" in message
    # The agent must learn that a LIMIT cannot fix a product.
    assert "LIMIT" in message


def test_a_missing_intermediate_row_count_is_denied() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    with pytest.raises(BudgetExceededError):
        enforce_budget(CostEstimate(scanned_bytes=10), budget)


def test_a_low_confidence_estimate_is_denied_on_intermediate_rows() -> None:
    budget = QueryBudget(max_intermediate_rows=1_000_000_000)
    estimate = CostEstimate(confidence="low", max_intermediate_rows=5)
    with pytest.raises(BudgetExceededError):
        enforce_budget(estimate, budget)


def test_an_ungated_intermediate_row_dimension_admits_anything() -> None:
    budget = QueryBudget(max_scan_bytes=100)
    enforce_budget(
        CostEstimate(scanned_bytes=10, max_intermediate_rows=10**15), budget
    )


def test_the_env_budget_gates_intermediate_rows_by_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "LAGAAM_MAX_SCAN_BYTES",
        "LAGAAM_QUERY_TIMEOUT",
        "LAGAAM_MAX_ROWS",
        "LAGAAM_MAX_RETURNED_ROWS",
        "LAGAAM_MAX_INTERMEDIATE_ROWS",
    ):
        monkeypatch.delenv(name, raising=False)
    assert QueryBudget.from_env().max_intermediate_rows == 1_000_000_000


def test_the_env_budget_reads_an_explicit_intermediate_row_cap(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("LAGAAM_MAX_INTERMEDIATE_ROWS", "5000")
    assert QueryBudget.from_env().max_intermediate_rows == 5000
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/core/test_budget.py -q -k intermediate`
Expected: FAIL — `QueryBudget` has no such field.

- [ ] **Step 3: Implement**

In `src/lagaam/core/budget.py`, add the default beside the others:

```python
# Measured on Trino 476: legitimate analytics peaks around 240,700 rows at
# its widest operator, while the cheapest product shape starts at
# 225,000,000. This sits between them with room on both sides.
DEFAULT_MAX_INTERMEDIATE_ROWS = 1_000_000_000
```

Add the field to `QueryBudget` after `max_rows`:

```python
    # Rows the engine would build at its widest operator, not rows returned:
    # what a product join blows and a byte estimate cannot see.
    max_intermediate_rows: int | None = Field(default=None, gt=0)
```

In `from_env`, read and default it:

```python
        intermediate_rows = _int_env("LAGAAM_MAX_INTERMEDIATE_ROWS")
```

and inside the `cls(...)` call:

```python
            max_intermediate_rows=(
                DEFAULT_MAX_INTERMEDIATE_ROWS
                if intermediate_rows is None
                else intermediate_rows
            ),
```

Append the enforcement block at the end of `enforce_budget`:

```python
    if budget.max_intermediate_rows is not None:
        if estimate.confidence == "low" or estimate.max_intermediate_rows is None:
            raise BudgetExceededError(
                "The row work could not be estimated, so this query cannot be "
                f"cleared against your row budget. {_UNESTIMABLE}"
            )
        if estimate.max_intermediate_rows > budget.max_intermediate_rows:
            raise BudgetExceededError(
                f"This query would build {estimate.max_intermediate_rows:,} "
                "rows at its widest step, over your budget of "
                f"{budget.max_intermediate_rows:,}. These are rows the engine "
                "materializes internally, not rows returned, so a LIMIT will "
                "not help — join on a column with more distinct values, or "
                "filter each side before the join."
            )
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest tests/core/test_budget.py -q`
Expected: PASS.

- [ ] **Step 5: Document the knob and commit**

In `src/lagaam/__main__.py`, beside the other `LAGAAM_*` lines, add:

```
  LAGAAM_MAX_INTERMEDIATE_ROWS  widest-operator row budget (default 1000000000)
```

```bash
uv run mypy
uv run pytest -q
git add src/lagaam/core/budget.py src/lagaam/__main__.py tests/core/test_budget.py
git commit -m "feat(core): budget the widest row count a query would build"
```

---

### Task 4: Wire the adapter

**Files:**
- Modify: `src/lagaam/adapters/trino/engine.py:274-293`
- Test: `tests/adapters/test_trino_execute.py`

**Interfaces:**
- Consumes: `max_intermediate_rows(plan_json: str) -> float | None` (Task 1), `CostEstimate.max_intermediate_rows` (Task 2).
- Produces: `_estimate_cost` returning a `CostEstimate` with the field populated.

**Important — do NOT remove `_collapse_factor`, `_scaled`, `table_scan_counts`, or `plan_entry_counts`.** They correct the *byte* quote, which this change does not supersede: measured on Trino 476, a 3-way self-join and a 4x-referenced CTE each report one IO entry totalling 135,000 bytes, identical to a single scan. Removing the scaling would under-price self-joins 3-4x. The logical plan's rows *do* account for repeated reads (both shapes measure 15,000 rows), so the new number is used unscaled while bytes stay scaled.

**Failure policy:** the plan call must not turn a working quote into a crash. Wrap it so an engine error there yields `None` for the field (which the budget then denies on) rather than propagating — the byte quote is still worth returning.

- [ ] **Step 1: Write the failing test**

Add to `tests/adapters/test_trino_execute.py` (follow the fake-cursor pattern already used in that file; read the top of the file first and reuse its existing fake connection helper rather than writing a new one):

```python
def test_estimate_cost_carries_the_widest_row_count(monkeypatch) -> None:
    """The quote reports what the plan says the widest operator builds."""
    io_json = json.dumps(
        {
            "inputTableColumnInfos": [
                {
                    "table": {
                        "catalog": "tpch",
                        "schemaTable": {"schema": "tiny", "table": "orders"},
                    },
                    "estimate": {
                        "outputRowCount": 15000.0,
                        "outputSizeInBytes": 135000.0,
                    },
                }
            ]
        }
    )
    plan_json = json.dumps(
        {
            "name": "Output",
            "estimates": [{"outputRowCount": "NaN"}],
            "children": [
                {
                    "name": "CrossJoin",
                    "estimates": [{"outputRowCount": "NaN"}],
                    "children": [
                        {"name": "TableScan", "estimates": [{"outputRowCount": 60175.0}]},
                        {"name": "TableScan", "estimates": [{"outputRowCount": 15000.0}]},
                    ],
                }
            ],
        }
    )
    engine = _engine_answering({"TYPE IO": io_json, "TYPE LOGICAL": plan_json})
    estimate = engine._estimate_cost("SELECT a FROM tpch.tiny.orders")
    assert estimate.max_intermediate_rows == 902_625_000


def test_a_failed_plan_call_still_returns_the_byte_quote() -> None:
    """A plan we cannot read is an unknown row count, not a lost quote."""
    io_json = json.dumps(
        {
            "inputTableColumnInfos": [
                {
                    "table": {
                        "catalog": "tpch",
                        "schemaTable": {"schema": "tiny", "table": "orders"},
                    },
                    "estimate": {
                        "outputRowCount": 15000.0,
                        "outputSizeInBytes": 135000.0,
                    },
                }
            ]
        }
    )
    engine = _engine_answering({"TYPE IO": io_json, "TYPE LOGICAL": RuntimeError("boom")})
    estimate = engine._estimate_cost("SELECT a FROM tpch.tiny.orders")
    assert estimate.scanned_bytes == 135000
    assert estimate.max_intermediate_rows is None
```

Write `_engine_answering(answers: dict[str, str | Exception])` as a local helper in that test file: a fake connection whose cursor matches the executed SQL against the dict keys by substring, returning `[(value,)]` or raising. Model it on the fakes already in the file.

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/adapters/test_trino_execute.py -q -k widest or plan`
Expected: FAIL — the field is never populated.

- [ ] **Step 3: Implement**

In `src/lagaam/adapters/trino/engine.py`, add the import:

```python
from lagaam.adapters.trino.plan import max_intermediate_rows
```

Replace the body of `_estimate_cost` after the `row is None` guard:

```python
        io_json = row[0]
        factor = _collapse_factor(
            table_scan_counts(sql, dialect), plan_entry_counts(io_json)
        )
        estimate = _scaled(parse_io_estimate(io_json), factor)
        widest = self._widest_rows(sql)
        if widest is None:
            return estimate
        return estimate.model_copy(update={"max_intermediate_rows": round(widest)})
```

Add the helper method:

```python
    def _widest_rows(self, sql: str) -> float | None:
        """Rows the plan's widest operator would build, or None if unreadable.

        A plan we cannot get is an unknown row count the budget denies on —
        never a reason to lose a byte quote we already have.
        """
        try:
            with self._connect() as conn:
                cur = conn.cursor()
                cur.execute(f"EXPLAIN (TYPE LOGICAL, FORMAT JSON) {sql}")
                row = cur.fetchone()
        except Exception:
            return None
        return max_intermediate_rows(row[0]) if row else None
```

Note: the broad `except Exception` is deliberate and is the one place it is
correct — this call is best-effort enrichment of a quote that already exists,
and any failure must degrade to "unknown" rather than replace the quote with
a crash. Leave a one-line comment saying so.

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest tests/adapters/ -q`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
uv run mypy
uv run pytest -q
git add src/lagaam/adapters/trino/engine.py tests/adapters/test_trino_execute.py
git commit -m "feat(trino): quote the widest row count alongside the byte estimate"
```

---

### Task 5: Delete the proxies

**Files:**
- Modify: `src/lagaam/core/scans.py`
- Modify: `tests/core/test_scans.py`

**Interfaces:**
- Consumes: nothing.
- Produces: `has_unpriceable_shape(sql: str, dialect: str) -> bool` (same signature, now the generator check alone) and `table_scan_counts(sql: str, dialect: str) -> dict[str, int]` (unchanged).

**This is the point of the whole plan.** Everything deleted here is superseded by Task 4, and five known defects go with it: F1 (`GROUP BY 1` ordinal defeating `_groups_on`), F2 (`_is_bound_lateral` accepting a constant correlation), the `linestatus` low-cardinality gap, the m13 mutation survivor (`_pins_to_one_row` ignoring its `on` argument), and five of six known false positives — all of which live inside the deleted functions.

**Delete:** `_conjuncts`, `_predicate_sources`, `_joined_sources`, `_equi_join_pairs`, `_source_parts`, `_source_alias`, `_has_ambiguous_alias`, `_has_product_join`, `_pins_to_one_row`, `_with_clause`, `_pinned_body`, `_groups_on`, `_is_bound_lateral`, `_reads_a_table`, `_inline_cardinality`, `_direct_selects`, `_bounded_source`, `_has_inline_row_product`, `_has_nested_loop_correlation`, and the constants `_MAX_INLINE_ROWS`, `_UNVISITED`, `_IN_PROGRESS`, `_READS_TABLE`.

**Keep:** `_GENERATORS`, `_ROW_PRESERVING_FUNCS`, `_NOT_A_ROW_SOURCE`, `_func_name`, `_func_arguments`, `_generates_rows`, `_expands_a_bounded_value`, `_is_bounded_input`, `_generator_rows`, `table_scan_counts`, `_without_cte_bodies`.

**Careful:** `_is_bounded_input` still references `_MAX_INLINE_ROWS` for the literal-array length cap — that constant **stays**. Only the *inline relation cardinality* machinery goes. Re-read `_is_bounded_input` before deleting anything and keep every name it uses.

- [ ] **Step 1: Delete the tests for deleted behaviour first**

In `tests/core/test_scans.py`, delete every test that asserts a product-join, ambiguous-alias, constant-pin, LATERAL-binding, nested-loop-correlation, or inline-cardinality decision. Keep every test about row generators (UNNEST, sequence, repeat, array literals, MAP, function allowlists) and about `table_scan_counts`.

Run: `uv run pytest tests/core/test_scans.py -q` — expect PASS (deleting tests cannot break the ones that remain).

- [ ] **Step 2: Delete the code**

Remove the functions and constants listed above from `src/lagaam/core/scans.py`.

Rewrite `has_unpriceable_shape` to:

```python
def has_unpriceable_shape(sql: str, dialect: str) -> bool:
    """True if the plan's estimates would miss this query's real row work.

    Only row generators qualify. Measured on Trino 476,
    UNNEST(sequence(1, 10000)) over a 60,175-row table plans as 60,175 rows
    and produces 601 million: a generator manufactures rows from an argument
    and contributes no operator the planner sizes. Every other row explosion
    — products, low-cardinality join keys, correlated nested loops — the
    planner does size, and the budget prices from the plan itself.

    Unparseable SQL counts as unpriceable: if we cannot prove the shape is
    safe, we assume the risky answer.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        return True
    return _generates_rows(tree)
```

Rewrite the module docstring — the current one explains a byte-sum-versus-rows theory that no longer describes the module:

```python
"""Spot queries whose row work no plan estimate can see.

The budget prices a query from Trino's own plan: the widest row count any
operator would build (see adapters/trino/plan.py). That covers every shape
where rows multiply through joins — products, low-cardinality keys,
correlated nested loops — because the planner estimates all of them.

One shape it cannot see is a row generator. UNNEST(sequence(1, 10000))
manufactures rows from an argument rather than from a table, and
contributes no operator the planner sizes: measured on Trino 476, such a
query over a 60,175-row table plans as 60,175 rows and produces 601
million. That is what this module detects, and all it detects.

table_scan_counts() serves a different gate: the IO plan reports one entry
per table however often the query reads it, so a self-join's *bytes* are
undercounted even though its rows are not.
"""
```

- [ ] **Step 3: Run the full suite**

Run: `uv run pytest -q && uv run mypy`
Expected: PASS. If an import of a deleted name remains anywhere, fix it now — `grep -rn "_has_product_join\|_bounded_source\|_has_nested_loop_correlation\|_has_inline_row_product" src tests`.

- [ ] **Step 4: Confirm the deletion is real**

Run: `uv run python -c "import lagaam.core.scans as s; print(len([n for n in dir(s) if not n.startswith('__')]))"`
and `wc -l src/lagaam/core/scans.py` — expect roughly 250 lines, down from 696.

- [ ] **Step 5: Commit**

```bash
git add src/lagaam/core/scans.py tests/core/test_scans.py
git commit -m "refactor(core): let the planner judge row work, not the SQL text"
```

Use this commit body:

```
Nineteen helpers inferred cardinality from syntax — whether an equality
existed, whether an inline relation was under 1000 rows, whether a GROUP BY
had one key. Cardinality is a property of the data, so each rule had a
counterexample and ten audit rounds found them one at a time:
`ON l.linestatus = o.orderstatus` breaks none of the rules and plans at
300,875,000 rows because the column has two distinct values.

The plan estimate added in the previous commit answers all of it directly.
Deleting these removes F1 (GROUP BY ordinal), F2 (LATERAL constant
correlation), the low-cardinality join-key gap, a mutation survivor in
_pins_to_one_row, and five known false positives by construction.

Row generators stay: measured on Trino 476, UNNEST(sequence(1, 10000))
plans as 60,175 rows and produces 601 million, so the planner genuinely
cannot see them.
```

---

### Task 6: Stop a nested query from crashing the validator (F3)

**Files:**
- Modify: `src/lagaam/core/safety.py`
- Test: `tests/core/test_safety.py`

**Interfaces:**
- Consumes: nothing.
- Produces: no signature change; `validate_query` raises `SqlValidationError` instead of `RecursionError`.

**Reproduced:** a query of ~1,813 characters nested ~100 deep raises an uncaught `RecursionError` from `validate_query`. Parsing succeeds; the crash is at `safety.py:145`, `tree.sql(dialect=...)` — sqlglot's *generator* recurses once per nesting level. The existing `_MAX_SQL_CHARS` ceiling does not help, because the payload is tiny.

The depth check must be **iterative**, or it becomes the recursion it prevents.

- [ ] **Step 1: Write the failing tests**

Add to `tests/core/test_safety.py`:

```python
def _nested(depth: int) -> str:
    sql = "SELECT 1 AS x"
    for _ in range(depth):
        sql = f"SELECT x FROM ({sql}) t"
    return sql


def test_an_ordinary_nesting_depth_is_accepted() -> None:
    # Deeper than any human query, well inside what sqlglot can render.
    validate_query(_nested(20), "trino")


def test_a_deeply_nested_query_is_refused_not_crashed() -> None:
    # Measured: ~100 levels (1,813 characters) blew the stack inside
    # tree.sql(). A tiny payload must not take the server down.
    with pytest.raises(SqlValidationError) as err:
        validate_query(_nested(300), "trino")
    assert "nested" in str(err.value).lower()


def test_a_very_deeply_nested_query_never_raises_recursionerror() -> None:
    for depth in (100, 500, 1000):
        with pytest.raises(SqlValidationError):
            validate_query(_nested(depth), "trino")
```

- [ ] **Step 2: Run to verify they fail**

Run: `uv run pytest tests/core/test_safety.py -q -k nested`
Expected: FAIL — `RecursionError` escapes instead of `SqlValidationError`.

- [ ] **Step 3: Implement**

In `src/lagaam/core/safety.py`, add the constant beside `_MAX_SQL_CHARS`:

```python
# sqlglot's generator recurses once per nesting level, so a *small* query
# nested deeply enough blows the stack inside tree.sql(): measured at ~100
# levels in 1,813 characters. Deeper than any analyst writes, and shallow
# enough that rendering stays safe.
_MAX_NESTING_DEPTH = 60
```

Add the iterative depth check:

```python
def _too_deeply_nested(tree: exp.Expr) -> bool:
    """True if the AST nests past what the SQL generator can render.

    Iterative by construction: a recursive depth check would raise the very
    RecursionError it exists to prevent.
    """
    stack = [(tree, 0)]
    while stack:
        node, depth = stack.pop()
        if depth > _MAX_NESTING_DEPTH:
            return True
        for child in node.args.values():
            for item in child if isinstance(child, list) else [child]:
                if isinstance(item, exp.Expr):
                    stack.append((item, depth + 1))
    return False
```

Call it in `validate_query` immediately after the `exp.Query` type check and before any rendering:

```python
    if _too_deeply_nested(tree):
        raise SqlValidationError(
            "The SQL is nested too deeply for this server to process safely. "
            "Flatten the subqueries — most nesting can be replaced by a CTE "
            "or a join — and retry."
        )
```

- [ ] **Step 4: Run to verify they pass**

Run: `uv run pytest tests/core/test_safety.py -q`
Expected: PASS. If `_MAX_NESTING_DEPTH = 60` rejects a legitimate query in the wider suite, raise it — but verify by measurement that rendering still survives at the new value, using `_nested(depth)` directly against `tree.sql()`.

- [ ] **Step 5: Commit**

```bash
uv run mypy
uv run pytest -q
git add src/lagaam/core/safety.py tests/core/test_safety.py
git commit -m "fix(core): refuse a query nested deeper than the renderer survives"
```

---

### Task 7: Prove it end-to-end against live Trino

**Files:**
- Modify: `tests/integration/test_trino_engine.py`

**Interfaces:**
- Consumes: everything above.
- Produces: the test that is this design's warrant.

**This test is not optional and must not be weakened to pass.** If a shape fails, the code is wrong, not the test.

The corpus is the 32 shapes measured during design. Read `docs/superpowers/specs/2026-08-03-plan-cardinality-measurements.md` for the expected magnitudes.

- [ ] **Step 1: Write the test**

Add to `tests/integration/test_trino_engine.py` (follow the existing integration-test conventions in that file — the `pytest.mark.integration` marker and the Trino-ready fixture it already uses):

```python
_LEGITIMATE = [
    ("healthy equi-join", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey"),
    ("3-way star join", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey = o.orderkey JOIN tpch.tiny.customer c ON o.custkey = c.custkey"),
    ("self join", "SELECT a.orderkey FROM tpch.tiny.orders a JOIN tpch.tiny.orders b ON a.orderkey = b.orderkey"),
    ("cte referenced four times", "WITH t AS (SELECT orderkey FROM tpch.tiny.orders) SELECT a.orderkey FROM t a JOIN t b ON a.orderkey = b.orderkey JOIN t c ON a.orderkey = c.orderkey JOIN t d ON a.orderkey = d.orderkey"),
    ("group by", "SELECT l.linestatus, count(*) AS c FROM tpch.tiny.lineitem l GROUP BY l.linestatus"),
    ("count star", "SELECT count(*) AS c FROM tpch.tiny.lineitem"),
    ("distinct", "SELECT DISTINCT l.linestatus FROM tpch.tiny.lineitem l"),
    ("date filter", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.orderdate > DATE '1995-01-01'"),
    ("like filter", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.comment LIKE '%special%'"),
    ("window function", "SELECT l.orderkey, row_number() OVER (PARTITION BY l.orderkey ORDER BY l.linenumber) AS r FROM tpch.tiny.lineitem l"),
    ("union all", "SELECT orderkey FROM tpch.tiny.orders UNION ALL SELECT orderkey FROM tpch.tiny.orders"),
    ("lateral aggregate", "SELECT o.orderkey, t.c FROM tpch.tiny.orders o LEFT JOIN LATERAL (SELECT count(*) AS c FROM tpch.tiny.lineitem l WHERE l.orderkey = o.orderkey) t ON true"),
    ("correlated equality subquery", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.totalprice > (SELECT avg(l.extendedprice) FROM tpch.tiny.lineitem l WHERE l.orderkey = o.orderkey)"),
    ("scalar subquery", "SELECT orderkey FROM tpch.tiny.orders WHERE totalprice > (SELECT avg(totalprice) FROM tpch.tiny.orders)"),
    ("unnest a literal array", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN UNNEST(ARRAY['a','b']) AS u(n)"),
    ("constant cross join", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN (SELECT 0.2 AS rate) r"),
    ("two small values relations", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN (VALUES (1),(2)) AS a(x) CROSS JOIN (VALUES (1),(2)) AS b(y)"),
    ("semi join", "SELECT orderkey FROM tpch.tiny.orders WHERE orderkey IN (SELECT orderkey FROM tpch.tiny.lineitem)"),
]

_EXPLOSIONS = [
    ("cross join", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o"),
    ("cross join laundered by a like filter", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o WHERE o.comment LIKE '%special%'"),
    ("cross join laundered by a limit", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o LIMIT 10"),
    ("cross join laundered by an aggregate", "WITH t AS (SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o) SELECT count(*) AS c FROM t"),
    ("cross join laundered by distinct", "SELECT DISTINCT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o"),
    ("cross join laundered by a group by", "SELECT l.orderkey, count(*) AS c FROM tpch.tiny.lineitem l CROSS JOIN tpch.tiny.orders o GROUP BY l.orderkey"),
    ("join on a constant", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON 1 = 1"),
    ("join on a two-valued column", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.linestatus = o.orderstatus"),
    ("inequality join", "SELECT l.orderkey FROM tpch.tiny.lineitem l JOIN tpch.tiny.orders o ON l.orderkey < o.orderkey"),
    ("group by ordinal constant pin", "SELECT a.orderkey FROM tpch.tiny.orders a JOIN (SELECT 1 AS m, count(*) AS c FROM tpch.tiny.lineitem l GROUP BY l.orderkey) t ON t.m = 1"),
    ("correlated inequality subquery", "SELECT o.orderkey FROM tpch.tiny.orders o WHERE o.totalprice > (SELECT avg(l.extendedprice) FROM tpch.tiny.lineitem l WHERE l.orderkey < o.orderkey)"),
]

_GENERATORS = [
    ("unnest a sequence", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN UNNEST(sequence(1, 10000)) AS u(n)"),
    ("unnest a repeat", "SELECT l.orderkey FROM tpch.tiny.lineitem l CROSS JOIN UNNEST(repeat(l.linestatus, 10000)) AS u(n)"),
]


@pytest.mark.integration
@pytest.mark.parametrize("label,sql", _LEGITIMATE, ids=[t[0] for t in _LEGITIMATE])
async def test_legitimate_shapes_clear_the_default_budget(label, sql) -> None:
    engine = TrinoEngine()
    estimate = await engine.estimate_cost(sql)
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    enforce_budget(estimate, budget)


@pytest.mark.integration
@pytest.mark.parametrize("label,sql", _EXPLOSIONS, ids=[t[0] for t in _EXPLOSIONS])
async def test_row_explosions_are_denied(label, sql) -> None:
    engine = TrinoEngine()
    estimate = await engine.estimate_cost(sql)
    budget = QueryBudget(
        max_scan_bytes=DEFAULT_MAX_SCAN_BYTES,
        max_intermediate_rows=DEFAULT_MAX_INTERMEDIATE_ROWS,
    )
    with pytest.raises(BudgetExceededError):
        enforce_budget(estimate, budget)


@pytest.mark.integration
@pytest.mark.parametrize("label,sql", _GENERATORS, ids=[t[0] for t in _GENERATORS])
async def test_row_generators_are_denied_by_the_shape_check(label, sql) -> None:
    """The planner cannot see these, so scans.py must still refuse them."""
    engine = TrinoEngine()
    estimate = await engine.estimate_cost(sql)
    assert estimate.confidence == "low"
```

- [ ] **Step 2: Run against live Trino**

```bash
docker compose -f ../examples/docker-compose.yml --profile trino up -d
uv run pytest tests/integration/test_trino_engine.py -q -m integration
```

Expected: all 31 pass. **A failure here means the implementation is wrong.** Investigate the actual plan (`EXPLAIN (TYPE LOGICAL, FORMAT JSON) <sql>`) before changing any threshold.

- [ ] **Step 3: Commit**

```bash
git add tests/integration/test_trino_engine.py
git commit -m "test(integration): pin the gate against thirty-one measured shapes"
```

---

### Task 8: Measure the false-block rate on ground the design has not seen

**Files:**
- Create: a throwaway script in the scratchpad (not committed).

**Why:** over-blocking is a product failure, and it has been missed twice before (62%, then 16.1%). The 18 legitimate shapes in Task 7 are the shapes the design was *built* from, so they prove nothing about generalization.

- [ ] **Step 1: Write a fresh corpus**

Write at least 40 legitimate analytical queries against `tpch.tiny` and `tpch.sf1` that were **not** used during design: window functions with frames, `GROUPING SETS`, `ROLLUP`, multi-CTE pipelines, `EXISTS`/`NOT EXISTS`, anti-joins, `UNION`, `INTERSECT`, `EXCEPT`, `CASE` aggregations, date arithmetic, `HAVING`, self-joins on keys, nested derived tables, `ORDER BY ... LIMIT`, `approx_percentile`, `filter (WHERE ...)` aggregates, string functions, joins of three or more tables, and a `LEFT JOIN` returning unmatched rows.

- [ ] **Step 2: Run them end-to-end through the real server**

Not through `estimate_cost` alone — through the in-process MCP server path, exactly as an agent would, with the default budget.

- [ ] **Step 3: Report the rate and fix what blocks**

Compute blocked/total. Investigate **every** block: for each, get `EXPLAIN (TYPE LOGICAL, FORMAT JSON)` and decide whether the query truly is expensive (a correct block) or the gate is wrong (a defect to fix). Record the outcome per query.

Target: below the 8.8% the previous design measured. If a legitimate shape blocks, fix the code — do not raise the threshold to hide it.

- [ ] **Step 4: Commit any fixes with their own tests**

Each fix gets a failing test first, then the fix, then the commit.

---

### Task 9: Mutation-test the new guards

**Files:** none created; this is a verification pass.

**Why:** a test that passes when the code is broken pins nothing. Round 8's lesson: a test asserting `max()` of a dict passed while the fix under it was fully reverted — it asserted a *proxy* for the behaviour, not the behaviour.

- [ ] **Step 1: Mutate and record**

For each mutation below: apply it, run `uv run pytest -q` (plus `-m integration` where the guard is only exercised there), record whether the suite fails, then revert.

1. In `plan.py`, make a NaN join take `max(known)` instead of the product.
2. In `plan.py`, return the root node's estimate instead of the maximum.
3. In `plan.py`, drop the `finite_number` call so `"NaN"` becomes `float("nan")`.
4. In `plan.py`, ignore extra estimates and read only `estimates[0]`.
5. In `budget.py`, treat a missing `max_intermediate_rows` as passing.
6. In `budget.py`, use `>=` instead of `>` (should survive — a boundary, not a behaviour; note it as such).
7. In `engine.py`, never call `_widest_rows`, leaving the field `None`.
8. In `engine.py`, let a plan-call failure propagate instead of returning `None`.
9. In `safety.py`, raise `_MAX_NESTING_DEPTH` to 10000.
10. In `scans.py`, make `has_unpriceable_shape` always return `False`.

- [ ] **Step 2: Fix every survivor**

A surviving mutation means a missing test. Write the test that kills it, and confirm the test fails against the mutation before reverting.

- [ ] **Step 3: Commit the new tests**

```bash
git add tests/
git commit -m "test: pin the guards the mutation pass found unpinned"
```

---

### Task 10: Documentation and the final gate

**Files:**
- Modify: `docs/` (the configuration page documenting `LAGAAM_*` vars), `README.md` if it describes what the gate catches.

- [ ] **Step 1: Document the new knob**

Add `LAGAAM_MAX_INTERMEDIATE_ROWS` to the config docs beside the other budget vars: what it means (rows built at the widest operator, not rows returned), its default (1,000,000,000), and the measured bands that justify it.

Also state the honest limitation: an adapter that cannot produce plan estimates leaves this unknown, and an unknown number is a denial while the dimension is gated.

- [ ] **Step 2: Correct any stale claim about how the gate works**

Search the docs for descriptions of product-join detection or SQL-shape analysis and rewrite them to describe the plan-based gate: `grep -rni "product join\|equi-join\|cross join" README.md docs/`.

- [ ] **Step 3: Full verification**

```bash
uv run pytest -q
uv run pytest -q -m integration
uv run mypy
```

All three must be clean. Report the actual counts; do not claim a pass without the output.

- [ ] **Step 4: Commit and open the PR**

```bash
git add -A
git commit -m "docs: document the widest-row budget and how the gate now works"
git push -u origin feat/plan-cardinality-gate
```

PR body format (project convention — no AI watermark):

```markdown
## What
Replace the SQL-shape proxies that decided whether a query explodes with
Trino's own per-operator row estimates.

## Why
Cardinality is a property of the data, not of syntax. `ON l.linestatus =
o.orderstatus` is an equi-join on bare columns that breaks none of the old
rules and plans at 300,875,000 rows, because the column has two distinct
values. Ten audit rounds found such counterexamples one at a time.

## Changes
- `adapters/trino/plan.py` reads the widest operator's row count from
  EXPLAIN (TYPE LOGICAL, FORMAT JSON).
- `QueryBudget.max_intermediate_rows` gates on it (default 1e9).
- `core/scans.py` loses nineteen helpers and keeps only the row-generator
  check, which the planner provably cannot see.
- `core/safety.py` refuses a query nested deeper than the SQL generator can
  render (previously an uncaught RecursionError at 1,813 characters).

## Testing
Measured on live Trino 476: 18 legitimate shapes admitted (15,000-240,700
rows at their widest), 11 explosions denied (225,000,000-902,625,000),
including five laundering variants that collapse the output count while
doing the full work, plus 2 generators denied by the shape check.
False-block rate measured on a fresh 40-query corpus. Mutation pass over
all new guards.

## Notes
The byte-quote scaling (`_collapse_factor`) is retained: the IO plan still
reports one entry per table, so a self-join's bytes are undercounted 3-4x
even though its rows are not.
```

---

## Self-Review

**Spec coverage:** `plan.py` (Task 1), `CostEstimate` field (Task 2), budget dimension + default + env (Task 3), adapter wiring and the retained byte scaling (Task 4), scans.py deletions (Task 5), F3 nesting depth (Task 6), the 32-shape integration corpus (Task 7), false-block measurement (Task 8), mutation testing (Task 9), docs and mypy (Task 10). All seven testing gates from the spec map to Tasks 7-10.

**Deliberately not implemented:** the spec's note that two round-10 claims did not reproduce — no task, because there is nothing to fix.

**Type consistency:** `max_intermediate_rows` is the name of the plan function (returning `float | None`), the `CostEstimate` field (`int | None`, rounded at the adapter boundary in Task 4), and the `QueryBudget` field (`int | None`). The name is shared deliberately; the rounding happens once, in `_estimate_cost`.
