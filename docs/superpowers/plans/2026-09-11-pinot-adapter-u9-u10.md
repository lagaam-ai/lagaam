# Pinot adapter U9–U10 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the Pinot adapter's skeleton, grounding and execution so `LAGAAM_ENGINE=pinot` starts a server that grounds an agent on a dockerized Pinot 1.5.1 and runs validated SQL on the multi-stage engine with pinned reins.
**Architecture:** A new `lagaam.adapters.pinot` package implements the `QueryEngine` port over plain `httpx`: `client.py` is the only module that knows an HTTP verb, `metadata.py` and `response.py` are pure JSON-to-domain parsers unit-tested on JSON captured from the live engine, `errors.py` maps Pinot's `errorCode` plus a message prefix onto core's engine-agnostic hint codes, and `engine.py` composes them. Names are three-part `pinot.<database>.<table>` to the agent and two-part `<database>.<table>` to Pinot, stripped on the sqlglot AST of already-validated SQL. `estimate_cost` returns `CostEstimate(confidence="low")` until U11 builds the quotation, which the default budget denies — the honest interim answer, pinned by a test.
**Tech Stack:** Python 3.12+, uv, pydantic 2.11+, sqlglot 30.x, httpx (becomes a runtime dependency), pytest asyncio_mode=auto, mypy strict on lagaam.core (this plan adds lagaam.adapters.pinot)
**Spec:** docs/superpowers/specs/2026-09-11-pinot-adapter-design.md

## Global Constraints
- Work only in the worktree `/Users/muditkapoor/Documents/code/lagaam-pinot` on branch `feat/pinot-adapter`; never push, never `git stash`.
- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`, `test:`), atomic: one logical change plus its tests per commit.
- Commit messages carry NO `Co-authored-by:` and NO `Claude-Session:` trailers.
- Inline comments: one line max, and only for a constraint the code cannot show.
- Type hints everywhere; `uv run mypy` clean. `pyproject.toml` scopes mypy to `packages = ["lagaam.core"]`; Task 1 adds `"lagaam.adapters.pinot"` to that list and every later task keeps it clean.
- `uv run pytest -q` is green at the end of every task (the default run deselects `-m integration`).
- Core never imports `httpx` and never learns a Pinot-specific shape; the only core edit in this plan is two new engine-agnostic hint codes in `core/query_errors._HINTS`.
- Fail-safe rule: a number that cannot be measured is `None` / `confidence="low"`, never a guess.
- Every Pinot query error is an HTTP 200 with a populated `exceptions[]`; never branch on HTTP status for a query response.
- Agent-facing error text is core's hint from `hint_for_engine_error`, never the broker's message — broker messages carry broker and server IPs, ports and request ids.
- The sqlglot dialect for Pinot is the generic one, the empty string `""`; that is the card's `sqlglot_dialect`.
- Names are three-part `pinot.<database>.<table>` to the agent and two-part `<database>.<table>` to Pinot; a third part is an HTTP 500 on the broker.

---

## File Structure

**Create**

| Path | Responsibility |
|---|---|
| `server/src/lagaam/adapters/pinot/__init__.py` | Package marker for the Pinot adapter. |
| `server/src/lagaam/adapters/pinot/dialect.py` | `PINOT_DIALECT_CARD` — the engine-quirks brief injected into SQL-generation prompts. |
| `server/src/lagaam/adapters/pinot/client.py` | The only module with an HTTP verb: controller GET and broker POST over `httpx.AsyncClient`, basic auth, timeouts, URL path-part validation, `PinotTransportError`. |
| `server/src/lagaam/adapters/pinot/metadata.py` | PURE. Controller JSON to `TableSchema` / table facts; never raises on shape. |
| `server/src/lagaam/adapters/pinot/names.py` | PURE. `two_part_sql` — strip the synthetic `pinot` catalog from validated SQL. |
| `server/src/lagaam/adapters/pinot/response.py` | PURE. Broker JSON to `QueryResult`, or the failure it carries. |
| `server/src/lagaam/adapters/pinot/errors.py` | `errorCode` plus message prefix to a core hint code. |
| `server/src/lagaam/adapters/pinot/engine.py` | `PinotEngine`: composes the above into the `QueryEngine` port. |
| `server/tests/adapters/pinot/__init__.py` | Test package marker. |
| `server/tests/adapters/pinot/fixtures/` | Raw JSON captured from live Pinot 1.5.1, copied from the measurement spike. |
| `server/tests/adapters/pinot/test_pinot_dialect.py` | The card, and `validate_query` under the generic dialect on measured Pinot shapes. |
| `server/tests/adapters/pinot/test_pinot_client.py` | `PinotClient` over `httpx.MockTransport`; no network. |
| `server/tests/adapters/pinot/test_pinot_metadata.py` | Pure metadata parsers on the fixtures. |
| `server/tests/adapters/pinot/test_pinot_names.py` | The catalog-strip rewrite. |
| `server/tests/adapters/pinot/test_pinot_response.py` | Result parsing and result-trust failure detection on the fixtures. |
| `server/tests/adapters/pinot/test_pinot_errors.py` | The error table row by row from `07-errors.json` and `04-query-options-matrix.json`. |
| `server/tests/adapters/pinot/test_pinot_engine.py` | `PinotEngine` grounding and execution over `httpx.MockTransport`. |
| `server/tests/integration/test_pinot_engine.py` | The adapter against the dockerized quickstart. |

`plan.py` and `quote.py` from the spec's module layout are U11 and are **not** created here.

**Modify**

| Path | Change |
|---|---|
| `server/pyproject.toml` | `httpx` becomes a runtime dependency; `lagaam.adapters.pinot` joins the mypy package list. |
| `server/src/lagaam/core/query_errors.py` | Three new engine-agnostic hint codes: `EXCEEDED_ROW_LIMIT`, `RESPONSE_TOO_LARGE`, `INCOMPLETE_RESULT`. |
| `server/src/lagaam/__main__.py` | `LAGAAM_ENGINE` selects the adapter (`trino` default, or `pinot`). |
| `examples/docker-compose.yml` | A `pinot` profile. |
| `server/tests/integration/conftest.py` | A `pinot_ready` fixture. |
| `server/tests/integration/test_e2e_mcp.py` | The grounding round-trip for `PinotEngine`, plus the interim budget denial. |

---

### Task 1: Package skeleton, dialect card, httpx runtime dependency, mypy scope

**Files:**
- Create: `server/src/lagaam/adapters/pinot/__init__.py`
- Create: `server/src/lagaam/adapters/pinot/dialect.py`
- Create: `server/tests/adapters/pinot/__init__.py`
- Create: `server/tests/adapters/pinot/test_pinot_dialect.py`
- Modify: `server/pyproject.toml` (lines 7–14 dependencies, lines 16–23 dev group, lines 44–46 mypy)

**Interfaces:**
- Consumes: `lagaam.core.models.DialectCard`, `lagaam.core.cards.render_dialect_card`, `lagaam.core.safety.validate_query`, `lagaam.core.errors.SqlValidationError`
- Produces: `lagaam.adapters.pinot.dialect.PINOT_DIALECT_CARD: DialectCard`

**Steps:**

- [ ] 1. Create the package markers.

```bash
mkdir -p server/src/lagaam/adapters/pinot server/tests/adapters/pinot
printf '"""Native Pinot adapter for the QueryEngine port."""\n' > server/src/lagaam/adapters/pinot/__init__.py
: > server/tests/adapters/pinot/__init__.py
```

- [ ] 2. Write the failing test at `server/tests/adapters/pinot/test_pinot_dialect.py`.

```python
"""The Pinot dialect card, and what the generic sqlglot dialect does to Pinot SQL.

The card's sqlglot_dialect is the generic dialect (""), measured in the spike:
15 Pinot shapes re-rendered through validate_query executed unchanged on both
engines, and `mysql` produced identical output.
"""

import pytest

from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.core.cards import render_dialect_card
from lagaam.core.errors import SqlValidationError
from lagaam.core.safety import validate_query

_DIALECT = PINOT_DIALECT_CARD.sqlglot_dialect


def test_card_names_pinot_and_the_generic_sqlglot_dialect() -> None:
    assert PINOT_DIALECT_CARD.engine == "Pinot"
    assert PINOT_DIALECT_CARD.sqlglot_dialect == ""
    assert PINOT_DIALECT_CARD.rules


def test_card_teaches_the_three_part_name_the_agent_must_write() -> None:
    text = render_dialect_card(PINOT_DIALECT_CARD)
    assert "Pinot" in text.splitlines()[0]
    assert any("pinot.default.table" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_card_warns_that_time_filters_are_what_prune_segments() -> None:
    assert any("time column" in rule for rule in PINOT_DIALECT_CARD.rules)


def test_measured_pinot_aggregation_survives_the_generic_dialect() -> None:
    # Measurements section 12: this exact shape re-rendered and executed on both engines.
    sql = validate_query(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier ORDER BY n DESC LIMIT 5",
        dialect=_DIALECT,
        default_limit=1000,
    )
    assert "pinot.default.airlineStats" in sql
    assert "LIMIT 5" in sql


def test_measured_pinot_time_filter_survives_the_generic_dialect() -> None:
    sql = validate_query(
        "SELECT Carrier FROM pinot.default.airlineStats "
        "WHERE DaysSinceEpoch BETWEEN 16071 AND 16073",
        dialect=_DIALECT,
        default_limit=5,
    )
    assert "BETWEEN 16071 AND 16073" in sql
    assert "LIMIT 5" in sql


def test_insert_from_file_is_rejected_under_the_generic_dialect() -> None:
    # Measurements section 5: the broker ACCEPTS this on both engines and
    # dispatches it as a Minion ingestion task. It must never reach the broker.
    with pytest.raises(SqlValidationError):
        validate_query(
            "INSERT INTO airlineStats FROM FILE 'file:///tmp/x.csv'",
            dialect=_DIALECT,
            default_limit=5,
        )
```

- [ ] 3. Run it and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_dialect.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.dialect'` — collection errors for the whole file.

- [ ] 4. Write `server/src/lagaam/adapters/pinot/dialect.py`.

```python
"""The Pinot dialect card: what an LLM must know to write Pinot SQL.

The sqlglot dialect is the generic one: measured against Pinot 1.5.1, 15
Pinot shapes re-rendered through validate_query executed unchanged on both
engines, and `mysql` produced byte-identical output.
"""

from lagaam.core.models import DialectCard

PINOT_DIALECT_CARD = DialectCard(
    engine="Pinot",
    sqlglot_dialect="",
    rules=[
        "Names have three levels: pinot.default.table — pinot is the only catalog",
        "Quote identifiers with double quotes; strings use single quotes",
        "Table and column names are case-insensitive",
        "Time columns are epoch numbers: convert with DATETIMECONVERT, DATETRUNC or ToDateTime",
        "Always filter on the table's time column — that is what prunes segments",
        "Prefer DISTINCTCOUNTHLL(x) over DISTINCTCOUNT(x) on large tables",
        "Name columns explicitly; SELECT * is rejected",
        "Every query needs a LIMIT; one is added if missing",
        "Joins run on the multi-stage engine and are bounded by a row limit",
    ],
)
```

- [ ] 5. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_dialect.py
```

Expected: `6 passed`.

- [ ] 6. Make `httpx` a runtime dependency and put the new package under mypy. Edit `server/pyproject.toml`: in `[project].dependencies`, after the `"sqlglot>=30,<31",` line, insert

```toml
    # The Pinot adapter's only transport: one POST and one JSON response.
    "httpx>=0.27",
```

then delete the `"httpx>=0.27",` line from `[dependency-groups].dev` (it is a runtime dependency now), and change the mypy packages line to

```toml
packages = ["lagaam.core", "lagaam.adapters.pinot"]
```

- [ ] 7. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: the full suite passes and mypy reports `Success: no issues found`.

- [ ] 8. Commit.

```bash
git add server/src/lagaam/adapters/pinot/__init__.py \
        server/src/lagaam/adapters/pinot/dialect.py \
        server/tests/adapters/pinot/__init__.py \
        server/tests/adapters/pinot/test_pinot_dialect.py \
        server/pyproject.toml
git commit -m "feat(pinot): the dialect card and the generic sqlglot dialect

Pinot's sqlglot dialect is the generic one: 15 measured Pinot shapes
re-rendered through validate_query executed unchanged on both engines, and
mysql bought nothing. The card teaches the three-part pinot.default.table
name the agent writes and the time filter that does the pruning. httpx
becomes a runtime dependency and the new package joins the mypy scope."
```

---

### Task 2: `PinotClient` — the only module that knows an HTTP verb

**Files:**
- Create: `server/src/lagaam/adapters/pinot/client.py`
- Create: `server/tests/adapters/pinot/test_pinot_client.py`

**Interfaces:**
- Consumes: `httpx`
- Produces:
  - `lagaam.adapters.pinot.client.PinotTransportError(Exception)`
  - `lagaam.adapters.pinot.client.PinotClient(controller_url: str, broker_url: str, user: str | None = None, password: str | None = None, timeout_seconds: float = 30.0)`
  - `PinotClient.controller_get(path: str, params: dict[str, str] | None = None, database: str | None = None) -> Any` (async)
  - `PinotClient.broker_query(sql: str, options: str, timeout_seconds: float | None = None) -> Any` (async)
  - `PinotClient.path_part(part: str) -> str` (staticmethod)
  - `PinotClient.NotFound` — sentinel class attribute; `controller_get` returns `PinotClient.NotFound` on HTTP 404

**Steps:**

- [ ] 1. Write the failing test at `server/tests/adapters/pinot/test_pinot_client.py`.

```python
"""PinotClient over httpx.MockTransport — no network, no container.

Every assertion here is about the request the adapter builds and the
failure it raises; the JSON parsers are tested separately.
"""

import base64
import json
from typing import Any

import httpx
import pytest

from lagaam.adapters.pinot.client import PinotClient, PinotTransportError


def make_client(
    handler: Any, user: str | None = None, password: str | None = None
) -> PinotClient:
    client = PinotClient(
        controller_url="http://controller:9000",
        broker_url="http://broker:8000",
        user=user,
        password=password,
    )
    client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


async def test_controller_get_returns_parsed_json() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://controller:9000/tables")
        return httpx.Response(200, json={"tables": ["airlineStats"]})

    body = await make_client(handler).controller_get("/tables")
    assert body == {"tables": ["airlineStats"]}


async def test_controller_get_sends_params_and_the_database_header() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["database"] = request.headers.get("database")
        return httpx.Response(200, json=[])

    await make_client(handler).controller_get(
        "/segments/airlineStats/metadata",
        params={"columns": "Carrier"},
        database="analytics",
    )
    assert seen["url"].endswith("/segments/airlineStats/metadata?columns=Carrier")
    assert seen["database"] == "analytics"


async def test_controller_404_returns_the_not_found_sentinel() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": 404, "error": "Table not found"})

    body = await make_client(handler).controller_get("/tables/nope/schema")
    assert body is PinotClient.NotFound


async def test_controller_500_is_a_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    with pytest.raises(PinotTransportError):
        await make_client(handler).controller_get("/tables")


async def test_broker_query_posts_sql_and_options() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"resultTable": None, "exceptions": []})

    await make_client(handler).broker_query(
        "SELECT a FROM default.t LIMIT 1", "useMultistageEngine=true;timeoutMs=1000"
    )
    assert seen["url"] == "http://broker:8000/query/sql"
    assert seen["body"] == {
        "sql": "SELECT a FROM default.t LIMIT 1",
        "queryOptions": "useMultistageEngine=true;timeoutMs=1000",
    }


async def test_basic_auth_is_sent_to_both_services() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, json={})

    client = make_client(handler, user="lagaam", password="secret")
    await client.controller_get("/tables")
    await client.broker_query("SELECT 1", "")
    expected = "Basic " + base64.b64encode(b"lagaam:secret").decode()
    assert seen == [expected, expected]


async def test_no_auth_header_without_credentials() -> None:
    seen: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("authorization"))
        return httpx.Response(200, json={})

    await make_client(handler).controller_get("/tables")
    assert seen == [None]


async def test_a_non_json_body_is_a_transport_error() -> None:
    # Measurements section 11: a three-part name is an HTTP 500 with a
    # non-JSON body, so a parse failure must fail closed, never crash.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="Table name containing more than one '.'")

    with pytest.raises(PinotTransportError):
        await make_client(handler).broker_query("SELECT 1", "")


async def test_a_connect_failure_is_a_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(PinotTransportError):
        await make_client(handler).controller_get("/tables")


@pytest.mark.parametrize(
    "part", ["a/b", "a.b", "a b", "a\tb", "", "..", "a\nb"]
)
def test_path_part_rejects_anything_that_could_reshape_the_url(part: str) -> None:
    with pytest.raises(ValueError):
        PinotClient.path_part(part)


def test_path_part_percent_encodes_a_legal_name() -> None:
    assert PinotClient.path_part("airlineStats") == "airlineStats"
    assert PinotClient.path_part("weird$name") == "weird%24name"
```

- [ ] 2. Run it and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_client.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.client'`.

- [ ] 3. Write `server/src/lagaam/adapters/pinot/client.py`.

```python
"""httpx wiring for Pinot: the only module in this adapter with an HTTP verb.

Two services with separate principal lists — the controller for metadata and
the broker for queries — so basic auth is threaded into both. Every failure
that is not a well-formed JSON body leaves as PinotTransportError, which the
engine turns into an EngineError; a controller 404 is not a failure but a
"no such thing", returned as the NotFound sentinel so the caller can decide.
"""

from typing import Any, Final
from urllib.parse import quote

import httpx


class PinotTransportError(Exception):
    """Pinot could not be reached, or answered with something unreadable.

    Adapter-private: never raised past engine.py, which translates it into
    core's EngineError so no transport detail reaches an agent.
    """


class _NotFound:
    """Sentinel: the controller answered 404 for this path."""


# Anything that could reshape a URL path, plus whitespace a name may not carry.
_ILLEGAL_PART_CHARS: Final = frozenset("/.\\?#%")


class PinotClient:
    NotFound: Final = _NotFound

    def __init__(
        self,
        controller_url: str,
        broker_url: str,
        user: str | None = None,
        password: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._controller_url = controller_url.rstrip("/")
        self._broker_url = broker_url.rstrip("/")
        auth = (user, password) if user is not None and password is not None else None
        self._http = httpx.AsyncClient(auth=auth, timeout=timeout_seconds)

    @staticmethod
    def path_part(part: str) -> str:
        """One agent-supplied name, safe to place in a URL path.

        Table names arrive from agents and land in controller paths, so this
        is a security boundary: a part carrying a separator is refused rather
        than encoded, because a name Pinot cannot hold is a name we never ask for.
        """
        if not part or part.strip() != part:
            raise ValueError(f"not a usable Pinot name: {part!r}")
        if any(char in _ILLEGAL_PART_CHARS for char in part):
            raise ValueError(f"not a usable Pinot name: {part!r}")
        if any(char.isspace() for char in part):
            raise ValueError(f"not a usable Pinot name: {part!r}")
        return quote(part, safe="")

    async def controller_get(
        self,
        path: str,
        params: dict[str, str] | None = None,
        database: str | None = None,
    ) -> Any:
        """GET one controller path. Returns parsed JSON, or NotFound on 404."""
        headers = {"database": database} if database else None
        try:
            response = await self._http.get(
                f"{self._controller_url}{path}", params=params, headers=headers
            )
        except httpx.HTTPError as exc:
            raise PinotTransportError(f"controller GET {path} failed") from exc
        if response.status_code == 404:
            return self.NotFound
        if response.status_code >= 400:
            raise PinotTransportError(
                f"controller GET {path} returned {response.status_code}"
            )
        return self._json(response, f"controller GET {path}")

    async def broker_query(
        self, sql: str, options: str, timeout_seconds: float | None = None
    ) -> Any:
        """POST one query to the broker. Returns parsed JSON.

        Never checks the status code for a query error: every Pinot query
        error is an HTTP 200 carrying exceptions[].
        """
        try:
            response = await self._http.post(
                f"{self._broker_url}/query/sql",
                json={"sql": sql, "queryOptions": options},
                timeout=timeout_seconds if timeout_seconds is not None else httpx.USE_CLIENT_DEFAULT,
            )
        except httpx.HTTPError as exc:
            raise PinotTransportError("broker query failed") from exc
        if response.status_code >= 400:
            raise PinotTransportError(
                f"broker query returned {response.status_code}"
            )
        return self._json(response, "broker query")

    async def aclose(self) -> None:
        await self._http.aclose()

    @staticmethod
    def _json(response: httpx.Response, what: str) -> Any:
        try:
            return response.json()
        except ValueError as exc:
            raise PinotTransportError(f"{what} returned a non-JSON body") from exc
```

- [ ] 4. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_client.py
```

Expected: `17 passed`.

- [ ] 5. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 6. Commit.

```bash
git add server/src/lagaam/adapters/pinot/client.py \
        server/tests/adapters/pinot/test_pinot_client.py
git commit -m "feat(pinot): the http client for the controller and the broker

One module holds every HTTP verb in this adapter, so basic auth, timeouts and
the non-JSON body all have exactly one place to be got right. Table names
reach controller URL paths from agents, so a name carrying a separator or
whitespace is refused rather than encoded. A controller 404 comes back as a
sentinel, since a missing table is an answer and not a transport failure."
```

---

### Task 3: `metadata.py` — pure controller-JSON parsers on real fixtures

**Files:**
- Create: `server/tests/adapters/pinot/fixtures/tables.json` (copy of `01-tables.json`)
- Create: `server/tests/adapters/pinot/fixtures/schema-airlineStats.json` (copy of `01-schema-airlineStats.json`)
- Create: `server/tests/adapters/pinot/fixtures/schema-baseballStats.json` (copy of `01-schema-baseballStats.json`)
- Create: `server/tests/adapters/pinot/fixtures/metadata-airlineStats.json` (copy of `01-metadata-airlineStats.json`)
- Create: `server/tests/adapters/pinot/fixtures/metadata-baseballStats.json` (copy of `01-metadata-baseballStats.json`)
- Create: `server/tests/adapters/pinot/fixtures/tableconfig-airlineStats.json` (copy of `01-tableconfig-airlineStats.json`)
- Create: `server/tests/adapters/pinot/fixtures/rt-metadata.json` (copy of `10-rt-metadata.json`)
- Create: `server/src/lagaam/adapters/pinot/metadata.py`
- Create: `server/tests/adapters/pinot/test_pinot_metadata.py`

**Interfaces:**
- Consumes: `lagaam.core.models.ColumnInfo`, `lagaam.core.models.TableSchema`
- Produces:
  - `lagaam.adapters.pinot.metadata.table_names(tables_json: Any) -> list[str]`
  - `lagaam.adapters.pinot.metadata.table_types(config_json: Any) -> frozenset[str]`
  - `lagaam.adapters.pinot.metadata.row_estimate(metadata_json: Any, types: frozenset[str]) -> int | None`
  - `lagaam.adapters.pinot.metadata.table_schema(catalog: str, schema: str, table: str, schema_json: Any, metadata_json: Any, config_json: Any) -> TableSchema`

**Steps:**

- [ ] 1. Copy the fixtures from the measurement spike.

```bash
SPIKE=/private/tmp/claude-501/-Users-muditkapoor-Downloads/77f40bb7-1cc1-48c1-87e9-c43459990ce5/scratchpad/pinot-spike
DEST=server/tests/adapters/pinot/fixtures
mkdir -p "$DEST"
cp "$SPIKE/01-tables.json"                  "$DEST/tables.json"
cp "$SPIKE/01-schema-airlineStats.json"     "$DEST/schema-airlineStats.json"
cp "$SPIKE/01-schema-baseballStats.json"    "$DEST/schema-baseballStats.json"
cp "$SPIKE/01-metadata-airlineStats.json"   "$DEST/metadata-airlineStats.json"
cp "$SPIKE/01-metadata-baseballStats.json"  "$DEST/metadata-baseballStats.json"
cp "$SPIKE/01-tableconfig-airlineStats.json" "$DEST/tableconfig-airlineStats.json"
cp "$SPIKE/10-rt-metadata.json"             "$DEST/rt-metadata.json"
```

- [ ] 2. Write the failing test at `server/tests/adapters/pinot/test_pinot_metadata.py`.

```python
"""Pure controller-JSON parsers, on JSON captured from live Pinot 1.5.1.

Every parser here must survive a shape it does not recognise: an unreadable
shape is "no fact" (empty list, None), which fails safe downstream, never an
exception that costs an agent its grounding.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.metadata import (
    row_estimate,
    table_names,
    table_schema,
    table_types,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def test_table_names_are_bare_and_sorted() -> None:
    names = table_names(load("tables.json"))
    assert names[0] == "airlineStats"
    assert "baseballStats" in names
    assert len(names) == 10
    assert names == sorted(names)
    assert not any(n.endswith("_OFFLINE") for n in names)


@pytest.mark.parametrize("body", [None, {}, {"tables": "nope"}, [], "junk"])
def test_table_names_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert table_names(body) == []


def test_table_types_reads_the_offline_key() -> None:
    assert table_types(load("tableconfig-airlineStats.json")) == frozenset({"OFFLINE"})


def test_table_types_reads_a_hybrid_config() -> None:
    both = {"OFFLINE": {"tableType": "OFFLINE"}, "REALTIME": {"tableType": "REALTIME"}}
    assert table_types(both) == frozenset({"OFFLINE", "REALTIME"})


@pytest.mark.parametrize("body", [None, {}, {"NONSENSE": {}}, "junk", []])
def test_table_types_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert table_types(body) == frozenset()


def test_row_estimate_is_numrows_for_an_offline_table() -> None:
    assert row_estimate(load("metadata-airlineStats.json"), frozenset({"OFFLINE"})) == 9746
    assert row_estimate(load("metadata-baseballStats.json"), frozenset({"OFFLINE"})) == 97889


def test_a_realtime_half_makes_the_row_count_unknown_rather_than_zero() -> None:
    # Measured: a REALTIME table serving 70 rows reported numRows 0, because
    # the controller only learns a segment's size once it is sealed. Zero is a
    # lie an agent would plan against; None says we do not know.
    body = load("rt-metadata.json")
    assert body["numRows"] == 0
    assert row_estimate(body, frozenset({"REALTIME"})) is None
    assert row_estimate(body, frozenset({"OFFLINE", "REALTIME"})) is None


@pytest.mark.parametrize("body", [None, {}, {"numRows": "many"}, "junk", []])
def test_row_estimate_never_raises_on_a_shape_it_cannot_read(body: Any) -> None:
    assert row_estimate(body, frozenset({"OFFLINE"})) is None


def test_table_schema_concatenates_all_three_field_specs() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    names = [c.name for c in card.columns]
    assert "Carrier" in names            # dimensionFieldSpecs
    assert "DaysSinceEpoch" in names     # dateTimeFieldSpecs
    assert len(names) == len(set(names))
    by_name = {c.name: c for c in card.columns}
    assert by_name["Carrier"].type == "STRING"
    assert by_name["ActualElapsedTime"].type == "INT"


def test_table_schema_echoes_lowercased_names_and_the_row_estimate() -> None:
    card = table_schema(
        "pinot",
        "DEFAULT",
        "airlineStats",
        load("schema-airlineStats.json"),
        load("metadata-airlineStats.json"),
        load("tableconfig-airlineStats.json"),
    )
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlinestats"
    assert card.row_estimate == 9746


def test_pinot_has_no_column_comments() -> None:
    card = table_schema(
        "pinot",
        "default",
        "baseballStats",
        load("schema-baseballStats.json"),
        load("metadata-baseballStats.json"),
        {"OFFLINE": {"tableType": "OFFLINE"}},
    )
    assert card.columns
    assert all(c.comment is None for c in card.columns)


def test_table_schema_survives_metadata_it_cannot_read() -> None:
    card = table_schema(
        "pinot",
        "default",
        "airlineStats",
        load("schema-airlineStats.json"),
        "not json at all",
        {"OFFLINE": {"tableType": "OFFLINE"}},
    )
    assert card.columns
    assert card.row_estimate is None


def test_table_schema_with_an_unreadable_schema_body_has_no_columns() -> None:
    card = table_schema("pinot", "default", "t", {"nonsense": 1}, {}, {})
    assert card.columns == []
    assert card.row_estimate is None
```

- [ ] 3. Run it and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_metadata.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.metadata'`.

- [ ] 4. Write `server/src/lagaam/adapters/pinot/metadata.py`.

```python
"""Controller JSON to domain values. PURE: no I/O, and it never raises.

Pinot has no catalog hierarchy and no column comments, so a grounding card is
assembled from three separate controller documents. Each parser treats a shape
it cannot read as "no fact" — an empty list or None — because a grounding call
that crashes on an unexpected key costs an agent the names it needs, and a
missing number fails safe at the budget gate anyway.
"""

from typing import Any

from lagaam.core.models import ColumnInfo, TableSchema

# Pinot's three field-spec lists, in the order a card presents them.
_FIELD_SPEC_KEYS = ("dimensionFieldSpecs", "metricFieldSpecs", "dateTimeFieldSpecs")


def table_names(tables_json: Any) -> list[str]:
    """Bare table names from GET /tables, sorted. Never the _OFFLINE spelling."""
    if not isinstance(tables_json, dict):
        return []
    tables = tables_json.get("tables")
    if not isinstance(tables, list):
        return []
    return sorted(t for t in tables if isinstance(t, str) and t)


def table_types(config_json: Any) -> frozenset[str]:
    """Which halves this table has, from GET /tables/{t}: OFFLINE, REALTIME, or both."""
    if not isinstance(config_json, dict):
        return frozenset()
    return frozenset(
        key for key in ("OFFLINE", "REALTIME") if isinstance(config_json.get(key), dict)
    )


def row_estimate(metadata_json: Any, types: frozenset[str]) -> int | None:
    """Total rows from GET /tables/{t}/metadata, or None when it cannot be trusted.

    Measured: a REALTIME table serving 70 rows reported numRows 0, because a
    consuming segment's size is unknown until it is sealed. Zero would ground
    an agent on a falsehood, so a REALTIME half means None.
    """
    if "REALTIME" in types:
        return None
    if not isinstance(metadata_json, dict):
        return None
    rows = metadata_json.get("numRows")
    if isinstance(rows, bool) or not isinstance(rows, int):
        return None
    return rows


def table_schema(
    catalog: str,
    schema: str,
    table: str,
    schema_json: Any,
    metadata_json: Any,
    config_json: Any,
) -> TableSchema:
    """One grounding card, from the schema, metadata and config documents."""
    return TableSchema(
        catalog=catalog.lower(),
        schema_name=schema.lower(),
        table=table.lower(),
        columns=_columns(schema_json),
        row_estimate=row_estimate(metadata_json, table_types(config_json)),
    )


def _columns(schema_json: Any) -> list[ColumnInfo]:
    """Dimension + metric + dateTime specs as columns; Pinot has no comments."""
    if not isinstance(schema_json, dict):
        return []
    columns: list[ColumnInfo] = []
    for key in _FIELD_SPEC_KEYS:
        specs = schema_json.get(key)
        if not isinstance(specs, list):
            continue
        for spec in specs:
            if not isinstance(spec, dict):
                continue
            name = spec.get("name")
            data_type = spec.get("dataType")
            if isinstance(name, str) and name and isinstance(data_type, str):
                columns.append(ColumnInfo(name=name, type=data_type))
    return columns
```

- [ ] 5. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_metadata.py
```

Expected: `25 passed`.

- [ ] 6. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 7. Commit.

```bash
git add server/tests/adapters/pinot/fixtures \
        server/src/lagaam/adapters/pinot/metadata.py \
        server/tests/adapters/pinot/test_pinot_metadata.py
git commit -m "feat(pinot): read a grounding card out of controller metadata

Pinot has no catalog hierarchy, no SHOW TABLES and no column comments, so a
card is assembled from three controller documents and tested against JSON
captured from a live 1.5.1. A REALTIME half reports numRows 0 while serving
rows, so it yields None: zero is a number an agent would plan against, and
None is the truth the budget gate already fails safe on."
```

---

### Task 4: `names.py` — the catalog strip between the agent and the broker

**Files:**
- Create: `server/src/lagaam/adapters/pinot/names.py`
- Create: `server/tests/adapters/pinot/test_pinot_names.py`

**Interfaces:**
- Consumes: `sqlglot`, `lagaam.core.errors.TableNotFoundError`
- Produces: `lagaam.adapters.pinot.names.two_part_sql(sql: str, catalog: str = "pinot") -> str`

**Steps:**

- [ ] 1. Write the failing test at `server/tests/adapters/pinot/test_pinot_names.py`.

```python
"""The one transformation between validated SQL and the broker.

Names are three-part to the agent and two-part to Pinot: a third part is an
HTTP 500 on the broker's own parser, so it is stripped here, on the AST of
SQL that has already been validated and allowlisted.
"""

import pytest

from lagaam.adapters.pinot.names import two_part_sql
from lagaam.core.errors import TableNotFoundError


def test_the_synthetic_catalog_is_dropped() -> None:
    assert two_part_sql(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    ) == "SELECT Carrier FROM default.airlineStats LIMIT 5"


def test_the_catalog_match_is_case_insensitive() -> None:
    assert two_part_sql(
        "SELECT Carrier FROM PINOT.Default.airlineStats LIMIT 5"
    ) == "SELECT Carrier FROM Default.airlineStats LIMIT 5"


def test_every_table_in_a_join_is_stripped() -> None:
    out = two_part_sql(
        "SELECT a.Carrier FROM pinot.default.airlineStats AS a "
        "JOIN pinot.default.baseballStats AS b ON a.Carrier = b.playerName LIMIT 5"
    )
    assert "pinot." not in out
    assert "default.airlineStats AS a" in out
    assert "default.baseballStats AS b" in out


def test_a_cte_reference_is_left_alone() -> None:
    # After validate_query and check_tables_allowed, a bare name can only be a
    # CTE — the allowlist refuses any base table that is not three parts.
    out = two_part_sql(
        "WITH recent AS (SELECT Carrier FROM pinot.default.airlineStats LIMIT 100) "
        "SELECT Carrier FROM recent LIMIT 5"
    )
    assert "FROM default.airlineStats" in out
    assert "FROM recent" in out


def test_a_subquery_table_is_stripped_too() -> None:
    out = two_part_sql(
        "SELECT n FROM (SELECT count(*) AS n FROM pinot.default.airlineStats) AS s "
        "LIMIT 5"
    )
    assert "pinot." not in out
    assert "default.airlineStats" in out


def test_another_catalog_is_refused_before_any_request() -> None:
    with pytest.raises(TableNotFoundError):
        two_part_sql("SELECT a FROM hive.default.airlineStats LIMIT 5")


def test_a_two_part_base_table_is_refused() -> None:
    # Validated SQL is always three-part, so a name with a schema but no
    # catalog never reached the allowlist as a base table; refuse it here too.
    with pytest.raises(TableNotFoundError):
        two_part_sql("SELECT a FROM default.airlineStats LIMIT 5")


def test_unparseable_sql_is_refused_rather_than_forwarded() -> None:
    with pytest.raises(TableNotFoundError):
        two_part_sql("SELECT FROM WHERE")


def test_comments_do_not_survive_the_rewrite() -> None:
    out = two_part_sql(
        "SELECT Carrier /* a note */ FROM pinot.default.airlineStats LIMIT 5"
    )
    assert "a note" not in out
```

- [ ] 2. Run it and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_names.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.names'`.

- [ ] 3. Write `server/src/lagaam/adapters/pinot/names.py`.

```python
"""Three-part names for the agent, two-part names for Pinot.

Pinot's namespace is exactly database.table; a third part is an HTTP 500 with
a non-JSON body on the broker's own parser. Core's grant format, allowlist,
cache key and describe_table are all three-part, so the adapter presents a
synthetic catalog named `pinot` and strips exactly that part here.

This runs after validate_query and check_tables_allowed, so a bare name can
only be a CTE the allowlist already vouched for — bare names are left alone,
and only a qualified name is stripped or refused. Refusing a qualified name
we do not recognise is the security boundary: no request is made at all.
"""

import sqlglot
from sqlglot import exp

from lagaam.core.errors import TableNotFoundError


def two_part_sql(sql: str, catalog: str = "pinot") -> str:
    """Validated SQL with the synthetic catalog dropped from every table."""
    try:
        tree = sqlglot.parse_one(sql, dialect="")
    except sqlglot.errors.SqlglotError as exc:
        raise TableNotFoundError(catalog=catalog, schema="?", table="?") from exc

    for table in tree.find_all(exp.Table):
        found = table.catalog
        if not found:
            # A bare name here is a CTE; a base table without a catalog never
            # got past the allowlist, so refuse a schema-qualified bare table.
            if table.db:
                raise TableNotFoundError(
                    catalog="", schema=table.db, table=table.name
                )
            continue
        if found.lower() != catalog.lower():
            raise TableNotFoundError(
                catalog=found, schema=table.db, table=table.name
            )
        table.set("catalog", None)

    return tree.sql(dialect="", comments=False)
```

- [ ] 4. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_names.py
```

Expected: `9 passed`.

- [ ] 5. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 6. Commit.

```bash
git add server/src/lagaam/adapters/pinot/names.py \
        server/tests/adapters/pinot/test_pinot_names.py
git commit -m "feat(pinot): strip the synthetic catalog on the way to the broker

Pinot's namespace is two levels and a third part is an HTTP 500, while core's
grants, allowlist and cache key are all three-part — so the adapter carries a
synthetic pinot catalog and drops it here, on the AST of already-validated
SQL. A catalog that is not pinot never reaches the network: it is a table that
does not exist, decided before any request is made."
```

---

### Task 5: `errors.py` and the two new core hint codes

**Files:**
- Create: `server/tests/adapters/pinot/fixtures/errors.json` (copy of `07-errors.json`)
- Create: `server/tests/adapters/pinot/fixtures/query-options-matrix.json` (copy of `04-query-options-matrix.json`)
- Create: `server/src/lagaam/adapters/pinot/errors.py`
- Create: `server/tests/adapters/pinot/test_pinot_errors.py`
- Modify: `server/src/lagaam/core/query_errors.py` (add three entries to `_HINTS`, after the `OPTIMIZER_TIMEOUT` entry ending line 54)

**Interfaces:**
- Consumes: `lagaam.core.query_errors.hint_for_engine_error`
- Produces:
  - `lagaam.adapters.pinot.errors.classify(error_code: int, message: str) -> str`
  - core hint codes `EXCEEDED_ROW_LIMIT`, `RESPONSE_TOO_LARGE` and `INCOMPLETE_RESULT`

**Steps:**

- [ ] 1. Copy the two error fixtures.

```bash
SPIKE=/private/tmp/claude-501/-Users-muditkapoor-Downloads/77f40bb7-1cc1-48c1-87e9-c43459990ce5/scratchpad/pinot-spike
DEST=server/tests/adapters/pinot/fixtures
cp "$SPIKE/07-errors.json"                "$DEST/errors.json"
cp "$SPIKE/04-query-options-matrix.json"  "$DEST/query-options-matrix.json"
```

- [ ] 2. Write the failing test at `server/tests/adapters/pinot/test_pinot_errors.py`.

```python
"""Pinot errorCode plus message prefix to a core hint code.

Every case here is a real exceptions[] entry captured from Pinot 1.5.1: the
same logical error carries different codes per engine (a bad column is 710 on
the single-stage engine and 700 on the multi-stage one), and the multi-stage
engine folds bad column, bad function and unsupported DML into 700 alike — so
classification needs the code AND a message prefix.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.errors import classify
from lagaam.core.query_errors import hint_for_engine_error, is_self_correctable

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def first_exception(case: dict[str, Any]) -> tuple[int, str]:
    exc = case["exceptions"][0]
    return exc["errorCode"], exc["message"]


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("v1:bad column", "COLUMN_NOT_FOUND"),
        ("v1:unknown table", "TABLE_NOT_FOUND"),
        ("v1:syntax error", "SYNTAX_ERROR"),
        ("v1:unknown function", "FUNCTION_NOT_FOUND"),
        ("MSE:bad column", "COLUMN_NOT_FOUND"),
        ("MSE:unknown table", "TABLE_NOT_FOUND"),
        ("MSE:syntax error", "SYNTAX_ERROR"),
        ("MSE:unknown function", "FUNCTION_NOT_FOUND"),
    ],
)
def test_every_measured_error_classifies(case: str, expected: str) -> None:
    code, message = first_exception(load("errors.json")[case])
    assert classify(code, message) == expected


@pytest.mark.parametrize(
    ("case", "expected"),
    [
        ("useMultistageEngine=true;maxRowsInJoin=5", "EXCEEDED_ROW_LIMIT"),
        ("useMultistageEngine=true;maxRowsInWindow=5", "EXCEEDED_ROW_LIMIT"),
        ("maxQueryResponseSizeBytes=100", "RESPONSE_TOO_LARGE"),
        ("maxServerResponseSizeBytes=100", "RESPONSE_TOO_LARGE"),
    ],
)
def test_every_measured_cap_classifies(case: str, expected: str) -> None:
    code, message = first_exception(load("query-options-matrix.json")[case])
    assert classify(code, message) == expected


def test_the_multistage_timeout_is_a_time_limit() -> None:
    assert classify(400, "BrokerTimeoutError: Timed out while planning query") == (
        "EXCEEDED_TIME_LIMIT"
    )


def test_the_single_stage_timeout_is_a_time_limit() -> None:
    assert classify(427, "1 servers [172.17.0.2_O] not responded") == (
        "EXCEEDED_TIME_LIMIT"
    )


def test_other_validation_errors_are_not_supported_rather_than_a_guess() -> None:
    assert classify(700, "QueryValidationError: something else entirely") == (
        "NOT_SUPPORTED"
    )


def test_an_unmapped_code_is_not_blamed_on_the_agent() -> None:
    # Unknown maps to no core hint, so the engine takes the blame, not the query.
    assert not is_self_correctable(classify(999, "who knows"))


def test_the_new_core_codes_carry_agent_facing_hints() -> None:
    row_limit = hint_for_engine_error("EXCEEDED_ROW_LIMIT")
    assert "join" in row_limit
    assert is_self_correctable("EXCEEDED_ROW_LIMIT")
    too_large = hint_for_engine_error("RESPONSE_TOO_LARGE")
    assert "LIMIT" in too_large
    assert is_self_correctable("RESPONSE_TOO_LARGE")
    incomplete = hint_for_engine_error("INCOMPLETE_RESULT")
    assert "incomplete" in incomplete
    assert is_self_correctable("INCOMPLETE_RESULT")


def test_a_broker_message_never_becomes_the_agent_facing_text() -> None:
    # Broker messages name broker and server IPs, ports and request ids.
    code, message = first_exception(
        load("query-options-matrix.json")["maxQueryResponseSizeBytes=100"]
    )
    assert "Broker_172.17.0.2_8000" in message
    assert "172.17.0.2" not in hint_for_engine_error(classify(code, message))
```

- [ ] 3. Run it and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_errors.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.errors'`.

- [ ] 4. Add the three engine-agnostic hint codes to core. In `server/src/lagaam/core/query_errors.py`, insert these three entries into `_HINTS` immediately after the `"OPTIMIZER_TIMEOUT"` entry (before the closing `}` on line 55).

```python
    # A hard backstop, not a truncation: the engine refused rather than
    # returning a partial answer, so the query has to build fewer rows.
    "EXCEEDED_ROW_LIMIT": (
        "The query built too many rows at one step and was stopped. Join on a "
        "column with more distinct values, filter each side before the join, "
        "or aggregate earlier, then retry."
    ),
    "RESPONSE_TOO_LARGE": (
        "The result was too large to send back. Return fewer columns, lower "
        "the LIMIT, or aggregate instead of returning raw rows, then retry."
    ),
    # The engine answered, but trimmed groups or servers on the way: the
    # numbers look complete and are not, so they are refused, not returned.
    "INCOMPLETE_RESULT": (
        "The engine returned an incomplete result — some groups or servers "
        "were dropped, so the numbers cannot be trusted. Add a WHERE filter "
        "to read less, or group by a column with fewer distinct values, then "
        "retry."
    ),
```

- [ ] 5. Write `server/src/lagaam/adapters/pinot/errors.py`.

```python
"""Pinot errorCode plus message prefix to a core hint code.

The code alone is not enough. The same logical error carries different codes
per engine — a bad column is 710 on the single-stage engine and 700 on the
multi-stage one — and 700 folds bad column, bad function and unsupported DML
together, so the message prefix decides between them. The message itself never
travels further than this function: it names broker and server IPs, ports and
request ids, and the agent gets core's curated hint instead.
"""

# The multi-stage engine reports an unknown column as a column that "depends
# on itself" — measured on 1.5.1; without this it would read as unsupported.
_MSE_UNKNOWN_COLUMN = "depends on itself"

_UNKNOWN = "PINOT_UNKNOWN"


def classify(error_code: int, message: str) -> str:
    """The core hint code for one Pinot exceptions[] entry.

    Returns a code core does not know when nothing matches, so
    is_self_correctable reads it as an engine fault rather than the
    agent's fault — a query is never blamed for a failure we cannot name.
    """
    if error_code == 150:
        return "SYNTAX_ERROR"
    if error_code == 190:
        return "TABLE_NOT_FOUND"
    if error_code == 710:
        return "COLUMN_NOT_FOUND"
    if error_code == 245:
        return "EXCEEDED_ROW_LIMIT"
    if error_code in (400, 427):
        return "EXCEEDED_TIME_LIMIT"
    if error_code == 503:
        return "RESPONSE_TOO_LARGE"
    if error_code == 700:
        if "UnknownColumnError" in message or _MSE_UNKNOWN_COLUMN in message:
            return "COLUMN_NOT_FOUND"
        if "Unsupported function" in message or "No match found for function" in message:
            return "FUNCTION_NOT_FOUND"
        return "NOT_SUPPORTED"
    return _UNKNOWN
```

- [ ] 6. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_errors.py
```

Expected: `18 passed`.

- [ ] 7. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 8. Commit.

```bash
git add server/tests/adapters/pinot/fixtures/errors.json \
        server/tests/adapters/pinot/fixtures/query-options-matrix.json \
        server/src/lagaam/adapters/pinot/errors.py \
        server/tests/adapters/pinot/test_pinot_errors.py \
        server/src/lagaam/core/query_errors.py
git commit -m "feat(pinot): classify broker errors into hints an agent can act on

Pinot's errorCode alone cannot name the failure: a bad column is 710 on the
single-stage engine and 700 on the multi-stage one, which also folds bad
function and unsupported DML into 700, and reports an unknown column as one
that depends on itself. Classification reads the code and the message prefix,
and the message goes no further — it carries broker and server IPs. Two new
engine-agnostic codes cover the row limit and the response cap."
```

---

### Task 6: `response.py` — broker JSON to a result, or the failure it carries

**Files:**
- Create: `server/tests/adapters/pinot/fixtures/agg-groupby.json` (copy of `02-agg-groupby.json`)
- Create: `server/tests/adapters/pinot/fixtures/numgroupslimit2.json` (copy of `04-numgroupslimit2.json`)
- Create: `server/tests/adapters/pinot/fixtures/timeout1-mse.json` (copy of `04-timeout1-mse.json`)
- Create: `server/tests/adapters/pinot/fixtures/insert-from-file-mse.json` (copy of `05-insert-from-file-MSE.json`)
- Create: `server/tests/adapters/pinot/fixtures/nolimit-mse.json` (copy of `04-nolimit-mse.json`)
- Create: `server/src/lagaam/adapters/pinot/response.py`
- Create: `server/tests/adapters/pinot/test_pinot_response.py`

**Interfaces:**
- Consumes: `lagaam.core.models.QueryResult`, `lagaam.adapters.pinot.errors.classify`
- Produces:
  - `lagaam.adapters.pinot.response.result_failure(body: Any) -> str | None` — a core hint code, or None when the body is a trustworthy result
  - `lagaam.adapters.pinot.response.parse_query_result(body: Any, max_rows: int) -> QueryResult`
  - `lagaam.adapters.pinot.response.INCOMPLETE_RESULT: str` — the hint code for a trimmed or partial result
  - `lagaam.adapters.pinot.response.ENGINE_FAULT: str` — the hint code for a body that carries no result at all

**Steps:**

- [ ] 1. Copy the five response fixtures.

```bash
SPIKE=/private/tmp/claude-501/-Users-muditkapoor-Downloads/77f40bb7-1cc1-48c1-87e9-c43459990ce5/scratchpad/pinot-spike
DEST=server/tests/adapters/pinot/fixtures
cp "$SPIKE/02-agg-groupby.json"         "$DEST/agg-groupby.json"
cp "$SPIKE/04-numgroupslimit2.json"     "$DEST/numgroupslimit2.json"
cp "$SPIKE/04-timeout1-mse.json"        "$DEST/timeout1-mse.json"
cp "$SPIKE/05-insert-from-file-MSE.json" "$DEST/insert-from-file-mse.json"
cp "$SPIKE/04-nolimit-mse.json"         "$DEST/nolimit-mse.json"
```

- [ ] 2. Write the failing test at `server/tests/adapters/pinot/test_pinot_response.py`.

```python
"""Broker JSON to a QueryResult, or the failure it carries.

Every fixture here is a real HTTP 200 from Pinot 1.5.1 — including the
failures, because every Pinot query error is an HTTP 200. An incomplete
result is a failure and not a warning: a trimmed GROUP BY returns plausible
wrong numbers, measured as 22 groups presented as complete.
"""

import json
from pathlib import Path
from typing import Any

import pytest

from lagaam.adapters.pinot.response import (
    ENGINE_FAULT,
    INCOMPLETE_RESULT,
    parse_query_result,
    result_failure,
)

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


def test_a_clean_aggregation_carries_no_failure() -> None:
    assert result_failure(load("agg-groupby.json")) is None


def test_a_clean_aggregation_parses_into_rows_and_columns() -> None:
    result = parse_query_result(load("agg-groupby.json"), max_rows=10)
    assert result.columns == ["Carrier", "n"]
    assert result.rows[0] == ["WN", 2008]
    assert result.row_count == 5
    assert result.truncated is False
    assert result.warnings == []


def test_rows_are_capped_and_truncation_is_flagged() -> None:
    # The server asks for max_rows + 1, so more rows than the cap means more exist.
    result = parse_query_result(load("agg-groupby.json"), max_rows=3)
    assert result.row_count == 3
    assert result.truncated is True
    assert result.rows[-1] == ["OO", 1159]


def test_a_trimmed_group_by_is_a_failure_not_a_warning() -> None:
    # Measured: numGroupsLimit=2 returned 22 groups with HTTP 200, partialResult
    # true and numGroupsLimitReached true — plausible numbers that are wrong.
    body = load("numgroupslimit2.json")
    assert body["numGroupsLimitReached"] is True
    assert result_failure(body) == INCOMPLETE_RESULT


def test_a_timeout_is_classified_from_its_exception() -> None:
    assert result_failure(load("timeout1-mse.json")) == "EXCEEDED_TIME_LIMIT"


def test_an_exception_outranks_the_partial_flag() -> None:
    # The timeout body sets partialResult too; the named cause is the better hint.
    assert load("timeout1-mse.json")["partialResult"] is True
    assert result_failure(load("timeout1-mse.json")) != INCOMPLETE_RESULT


def test_an_ingestion_shaped_response_is_an_engine_fault() -> None:
    # Measured: INSERT INTO ... FROM FILE returns HTTP 200 with empty
    # exceptions[], a null requestId and an ingestion task schema. Core's AST
    # allowlist already denies INSERT, so this can only mean the broker
    # answered something that is not a query result.
    body = load("insert-from-file-mse.json")
    assert body["exceptions"] == []
    assert body["requestId"] is None
    assert result_failure(body) == ENGINE_FAULT


def test_a_real_result_with_a_request_id_is_not_an_engine_fault() -> None:
    assert load("agg-groupby.json")["requestId"]
    assert result_failure(load("agg-groupby.json")) is None


def test_the_group_warning_limit_becomes_a_warning_on_the_result() -> None:
    body = load("agg-groupby.json")
    body["numGroupsWarningLimitReached"] = True
    assert result_failure(body) is None
    result = parse_query_result(body, max_rows=10)
    assert len(result.warnings) == 1
    assert "group" in result.warnings[0].lower()


def test_an_unlimited_multistage_selection_parses_and_caps() -> None:
    # Measured: the multi-stage engine returned all 9,746 rows for a query
    # with no LIMIT. The adapter caps what it hands back either way.
    result = parse_query_result(load("nolimit-mse.json"), max_rows=5)
    assert result.row_count == 5
    assert result.truncated is True


@pytest.mark.parametrize("body", [None, "junk", [], 7])
def test_a_body_that_is_not_an_object_is_an_engine_fault(body: Any) -> None:
    assert result_failure(body) == ENGINE_FAULT


def test_a_missing_result_table_parses_as_no_rows() -> None:
    result = parse_query_result({"resultTable": None}, max_rows=10)
    assert result.columns == []
    assert result.rows == []
    assert result.row_count == 0
    assert result.truncated is False


def test_a_malformed_row_does_not_crash_the_parse() -> None:
    body = {
        "resultTable": {
            "dataSchema": {"columnNames": ["a"]},
            "rows": [["ok"], "not a row", ["fine"]],
        }
    }
    result = parse_query_result(body, max_rows=10)
    assert result.rows == [["ok"], ["fine"]]
```

- [ ] 3. Run it and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_response.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.response'`.

- [ ] 4. Write `server/src/lagaam/adapters/pinot/response.py`.

```python
"""Broker JSON to a QueryResult, or the failure it carries. PURE, no I/O.

Every Pinot query error is an HTTP 200 with a populated exceptions[], so the
body is the only signal there is. An incomplete result is a failure and not a
warning: measured, numGroupsLimit=2 returned 22 groups as though they were all
of them, with HTTP 200 and plausible-looking aggregates.
"""

from typing import Any

from lagaam.adapters.pinot.errors import classify
from lagaam.core.models import QueryResult

INCOMPLETE_RESULT = "INCOMPLETE_RESULT"

# Not a hint code core knows, so it reads as an engine fault, not the query's.
ENGINE_FAULT = "PINOT_ENGINE_FAULT"

_GROUP_WARNING = (
    "The engine approached its group limit on this query. The numbers are "
    "complete, but a broader grouping may not be — narrow the GROUP BY or add "
    "a filter before relying on it."
)


def result_failure(body: Any) -> str | None:
    """The hint code this response carries, or None when it can be trusted.

    Precedence follows the spec: a named exception beats a bare trust flag,
    because the exception says what to change and the flag only says something
    is wrong.
    """
    if not isinstance(body, dict):
        return ENGINE_FAULT

    exceptions = body.get("exceptions")
    if isinstance(exceptions, list) and exceptions:
        first = exceptions[0]
        if isinstance(first, dict):
            code = first.get("errorCode")
            message = first.get("message")
            if isinstance(code, int) and not isinstance(code, bool):
                return classify(code, message if isinstance(message, str) else "")
        return ENGINE_FAULT

    for flag in ("partialResult", "numGroupsLimitReached", "groupsTrimmed"):
        if body.get(flag) is True:
            return INCOMPLETE_RESULT

    # A query the broker answered without a requestId is not a query it ran:
    # measured, INSERT INTO ... FROM FILE comes back exactly this way.
    if body.get("requestId") is None:
        return ENGINE_FAULT

    return None


def parse_query_result(body: Any, max_rows: int) -> QueryResult:
    """Rows and columns from a trusted response, capped at max_rows.

    The server asks the engine for max_rows + 1, so more rows than the cap is
    how truncation is detected without a second query.
    """
    warnings: list[str] = []
    if isinstance(body, dict) and body.get("numGroupsWarningLimitReached") is True:
        warnings.append(_GROUP_WARNING)

    table = body.get("resultTable") if isinstance(body, dict) else None
    if not isinstance(table, dict):
        return QueryResult(columns=[], rows=[], row_count=0, warnings=warnings)

    schema = table.get("dataSchema")
    names = schema.get("columnNames") if isinstance(schema, dict) else None
    columns = (
        [c for c in names if isinstance(c, str)] if isinstance(names, list) else []
    )

    raw = table.get("rows")
    rows = [list(r) for r in raw if isinstance(r, list)] if isinstance(raw, list) else []

    truncated = len(rows) > max_rows
    capped = rows[:max_rows]
    return QueryResult(
        columns=columns,
        rows=capped,
        row_count=len(capped),
        truncated=truncated,
        warnings=warnings,
    )
```

- [ ] 5. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_response.py
```

Expected: `16 passed`.

- [ ] 6. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 7. Commit.

```bash
git add server/tests/adapters/pinot/fixtures/agg-groupby.json \
        server/tests/adapters/pinot/fixtures/numgroupslimit2.json \
        server/tests/adapters/pinot/fixtures/timeout1-mse.json \
        server/tests/adapters/pinot/fixtures/insert-from-file-mse.json \
        server/tests/adapters/pinot/fixtures/nolimit-mse.json \
        server/src/lagaam/adapters/pinot/response.py \
        server/tests/adapters/pinot/test_pinot_response.py
git commit -m "feat(pinot): read the broker's answer, and refuse the incomplete ones

Every Pinot query error is an HTTP 200, so the body is the only signal. A
trimmed GROUP BY is treated as a failure rather than a note: measured, it
returned 22 groups as though they were all of them, with plausible aggregates
an agent would have believed. A response with no requestId is an engine fault
— that is exactly the shape an ingestion task comes back as."
```

---

### Task 7: `PinotEngine` grounding, `estimate_cost`, and `LAGAAM_ENGINE` selection

**Files:**
- Create: `server/src/lagaam/adapters/pinot/engine.py`
- Create: `server/tests/adapters/pinot/test_pinot_engine.py`
- Modify: `server/src/lagaam/__main__.py` (docstring lines 9–21, import line 26, `main()` body lines 35–40)

**Interfaces:**
- Consumes: `PinotClient`, `PinotTransportError`, `PINOT_DIALECT_CARD`, `lagaam.adapters.pinot.metadata.*`, `lagaam.core.models.*`, `lagaam.core.errors.*`
- Produces:
  - `lagaam.adapters.pinot.engine.PinotEngine(controller_url: str = "http://localhost:9000", broker_url: str = "http://localhost:8000", user: str | None = None, password: str | None = None, max_tables_per_catalog: int = 1000, max_intermediate_rows: int | None = None)`
  - `PinotEngine.from_env() -> PinotEngine` (classmethod)
  - `PinotEngine.CATALOG: str` — the synthetic catalog name, `"pinot"`
  - `PinotEngine.list_catalogs()`, `.describe_table()`, `.dialect()`, `.estimate_cost()` (async except `dialect`)

**Steps:**

- [ ] 1. Write the failing test at `server/tests/adapters/pinot/test_pinot_engine.py`.

```python
"""PinotEngine grounding over httpx.MockTransport, routing the real fixtures."""

import json
import os
from pathlib import Path
from typing import Any

import httpx
import pytest

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.core.errors import EngineError, TableNotFoundError
from lagaam.core.ports import QueryEngine

FIXTURES = Path(__file__).parent / "fixtures"


def load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text())


_ROUTES: dict[str, str] = {
    "/databases": "databases.json",
    "/tables": "tables.json",
    "/tables/airlineStats/schema": "schema-airlineStats.json",
    "/tables/airlineStats/metadata": "metadata-airlineStats.json",
    "/tables/airlineStats": "tableconfig-airlineStats.json",
}


def controller_handler(request: httpx.Request) -> httpx.Response:
    name = _ROUTES.get(request.url.path)
    if name is None:
        return httpx.Response(404, json={"code": 404, "error": "not found"})
    return httpx.Response(200, json=load(name))


def make_engine(handler: Any = controller_handler) -> PinotEngine:
    engine = PinotEngine(
        controller_url="http://controller:9000", broker_url="http://broker:8000"
    )
    engine._client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return engine


def test_pinot_engine_satisfies_the_port() -> None:
    assert isinstance(make_engine(), QueryEngine)


def test_the_dialect_is_the_pinot_card() -> None:
    card = make_engine().dialect()
    assert card.engine == "Pinot"
    assert card.sqlglot_dialect == ""


async def test_list_catalogs_synthesises_one_catalog_over_the_databases() -> None:
    meta = await make_engine().list_catalogs()
    assert [c.name for c in meta.catalogs] == ["pinot"]
    catalog = meta.catalogs[0]
    assert [s.name for s in catalog.schemas] == ["default"]
    assert "airlineStats" in catalog.schemas[0].tables
    assert catalog.truncated is False


async def test_the_table_listing_is_sorted() -> None:
    tables = (await make_engine().list_catalogs()).catalogs[0].schemas[0].tables
    assert tables == sorted(tables)


async def test_the_table_listing_is_capped_and_flagged() -> None:
    engine = make_engine()
    engine._max_tables = 3
    catalog = (await engine.list_catalogs()).catalogs[0]
    assert len(catalog.schemas[0].tables) == 3
    assert catalog.truncated is True


async def test_a_missing_databases_endpoint_falls_back_to_default() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/databases":
            return httpx.Response(404, json={"code": 404, "error": "no"})
        return controller_handler(request)

    meta = await make_engine(handler).list_catalogs()
    assert [s.name for s in meta.catalogs[0].schemas] == ["default"]


async def test_an_unreachable_controller_is_an_engine_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(EngineError):
        await make_engine(handler).list_catalogs()


async def test_describe_table_returns_the_grounding_card() -> None:
    card = await make_engine().describe_table("pinot", "default", "airlineStats")
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlinestats"
    assert card.row_estimate == 9746
    assert any(c.name == "Carrier" for c in card.columns)


async def test_describe_table_accepts_any_spelling_of_the_name() -> None:
    card = await make_engine().describe_table("PINOT", "DEFAULT", "airlineStats")
    assert card.table == "airlinestats"


async def test_a_catalog_that_is_not_pinot_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("hive", "default", "airlineStats")


async def test_a_controller_404_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("pinot", "default", "nosuchtable")


async def test_a_name_that_cannot_be_a_url_part_is_a_missing_table() -> None:
    with pytest.raises(TableNotFoundError):
        await make_engine().describe_table("pinot", "default", "../secrets")


async def test_estimate_cost_is_honest_that_it_cannot_price_yet() -> None:
    # U11 builds the quotation from segment metadata. Until then there is no
    # number, so confidence is low and the default budget denies the query.
    estimate = await make_engine().estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    assert estimate.confidence == "low"
    assert estimate.scanned_bytes is None
    assert estimate.row_estimate is None
    assert estimate.max_intermediate_rows is None


async def test_the_interim_estimate_is_denied_by_the_default_budget() -> None:
    from lagaam.core.budget import QueryBudget, enforce_budget
    from lagaam.core.errors import BudgetExceededError

    estimate = await make_engine().estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    with pytest.raises(BudgetExceededError, match="could not be estimated"):
        enforce_budget(estimate, QueryBudget.from_env())


def test_from_env_reads_the_pinot_variables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PINOT_CONTROLLER_URL", "http://c:19000")
    monkeypatch.setenv("PINOT_BROKER_URL", "http://b:18000")
    monkeypatch.setenv("PINOT_USER", "lagaam")
    monkeypatch.setenv("PINOT_PASSWORD", "secret")
    engine = PinotEngine.from_env()
    assert engine._controller_url == "http://c:19000"
    assert engine._broker_url == "http://b:18000"


def test_from_env_defaults_to_the_quickstart_ports(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in ("PINOT_CONTROLLER_URL", "PINOT_BROKER_URL", "PINOT_USER", "PINOT_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.delenv("LAGAAM_MAX_INTERMEDIATE_ROWS", raising=False)
    engine = PinotEngine.from_env()
    assert engine._controller_url == "http://localhost:9000"
    assert engine._broker_url == "http://localhost:8000"
    assert os.environ.get("PINOT_USER") is None


def test_from_env_takes_the_intermediate_row_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The port's execute() has no such parameter, so the engine learns the
    # budget at construction and spends it on the broker's own row limits.
    monkeypatch.setenv("LAGAAM_MAX_INTERMEDIATE_ROWS", "1234")
    assert PinotEngine.from_env()._max_intermediate_rows == 1234
```

- [ ] 2. Add the `databases.json` fixture the routing table needs.

```bash
printf '["default"]\n' > server/tests/adapters/pinot/fixtures/databases.json
```

- [ ] 3. Run the test and watch it fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_engine.py
```

Expected: `ModuleNotFoundError: No module named 'lagaam.adapters.pinot.engine'`.

- [ ] 4. Write `server/src/lagaam/adapters/pinot/engine.py`.

```python
"""Pinot adapter for the QueryEngine port.

Pinot has no catalog hierarchy, no SHOW TABLES and no information_schema, so
grounding is controller REST and the catalog is synthetic: exactly one, named
`pinot`, whose schemas are Pinot databases. Names are three-part to the agent
and two-part to Pinot. Every failure leaves this module as a LagaamError —
httpx exceptions and broker messages never escape.
"""

import os
from typing import Any

from lagaam.adapters.pinot.client import PinotClient, PinotTransportError
from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.adapters.pinot.metadata import table_names, table_schema
from lagaam.core.errors import EngineError, TableNotFoundError
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    SchemaInfo,
    TableSchema,
)

_UNREACHABLE = "the query engine is not reachable right now"

# Pinot's namespace is database.table and `default` always exists; 1.5.1 does
# expose GET /databases, but an older controller answering 404 still grounds.
_DEFAULT_DATABASE = "default"


class PinotEngine:
    CATALOG = "pinot"

    def __init__(
        self,
        controller_url: str = "http://localhost:9000",
        broker_url: str = "http://localhost:8000",
        user: str | None = None,
        password: str | None = None,
        max_tables_per_catalog: int = 1000,
        max_intermediate_rows: int | None = None,
    ) -> None:
        self._controller_url = controller_url
        self._broker_url = broker_url
        self._max_tables = max_tables_per_catalog
        # The port's execute() carries no row budget, so the engine is told
        # once and spends it on the broker's own maxRowsInJoin/InWindow caps.
        self._max_intermediate_rows = max_intermediate_rows
        self._client = PinotClient(
            controller_url=controller_url,
            broker_url=broker_url,
            user=user,
            password=password,
        )

    @classmethod
    def from_env(cls) -> "PinotEngine":
        rows = os.environ.get("LAGAAM_MAX_INTERMEDIATE_ROWS")
        return cls(
            controller_url=os.environ.get(
                "PINOT_CONTROLLER_URL", "http://localhost:9000"
            ),
            broker_url=os.environ.get("PINOT_BROKER_URL", "http://localhost:8000"),
            user=os.environ.get("PINOT_USER"),
            password=os.environ.get("PINOT_PASSWORD"),
            max_intermediate_rows=int(rows) if rows else None,
        )

    def dialect(self) -> DialectCard:
        return PINOT_DIALECT_CARD

    async def list_catalogs(self) -> CatalogMetadata:
        try:
            databases = await self._databases()
            schemas: list[SchemaInfo] = []
            truncated = False
            for database in databases:
                body = await self._client.controller_get("/tables", database=database)
                if body is PinotClient.NotFound:
                    continue
                names = table_names(body)
                if len(names) > self._max_tables:
                    truncated = True
                    names = names[: self._max_tables]
                schemas.append(SchemaInfo(name=database, tables=names))
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
        return CatalogMetadata(
            catalogs=[
                CatalogInfo(name=self.CATALOG, schemas=schemas, truncated=truncated)
            ]
        )

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        if catalog.lower() != self.CATALOG:
            raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
        try:
            part = PinotClient.path_part(table)
        except ValueError as exc:
            raise TableNotFoundError(
                catalog=catalog, schema=schema, table=table
            ) from exc
        try:
            schema_json = await self._client.controller_get(
                f"/tables/{part}/schema", database=schema
            )
            if schema_json is PinotClient.NotFound:
                raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
            metadata_json = await self._client.controller_get(
                f"/tables/{part}/metadata", database=schema
            )
            config_json = await self._client.controller_get(
                f"/tables/{part}", database=schema
            )
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
        return table_schema(
            catalog,
            schema,
            table,
            schema_json,
            None if metadata_json is PinotClient.NotFound else metadata_json,
            None if config_json is PinotClient.NotFound else config_json,
        )

    async def estimate_cost(self, sql: str) -> CostEstimate:
        """No quotation exists yet — U11 builds it from segment metadata.

        Pinot 1.5.1 reports no bytes anywhere and a constant rowcount of 100
        per table scan, so there is nothing honest to return but "unknown",
        which the default budget denies. Guessing here would admit a query
        the gate exists to stop.
        """
        return CostEstimate(confidence="low")

    async def _databases(self) -> list[str]:
        """Pinot databases, or just `default` on a controller without the endpoint."""
        body = await self._client.controller_get("/databases")
        if body is PinotClient.NotFound or not isinstance(body, list):
            return [_DEFAULT_DATABASE]
        names = sorted({d for d in body if isinstance(d, str) and d})
        return names or [_DEFAULT_DATABASE]

    @staticmethod
    def _unused(value: Any) -> None:
        return None
```

- [ ] 5. Remove the leftover helper: delete the `_unused` staticmethod and the now-unneeded `from typing import Any` import from `engine.py`.

```bash
cd server && python3 - <<'PY'
from pathlib import Path
p = Path("src/lagaam/adapters/pinot/engine.py")
text = p.read_text()
text = text.replace("""
    @staticmethod
    def _unused(value: Any) -> None:
        return None
""", "")
text = text.replace("import os\nfrom typing import Any\n", "import os\n")
p.write_text(text)
PY
```

- [ ] 6. Run the test and watch it pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_engine.py
```

Expected: `17 passed`.

- [ ] 7. Wire `LAGAAM_ENGINE` into `server/src/lagaam/__main__.py`. Replace the import on line 26

```python
from lagaam.adapters.trino.engine import TrinoEngine
```

with

```python
from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.ports import QueryEngine
```

Replace the docstring's `Env:` block (lines 9–21) with

```python
Env:
  LAGAAM_ENGINE          which adapter to run: trino (default) or pinot
  TRINO_HOST (localhost), TRINO_PORT (8080), TRINO_USER (lagaam)
  PINOT_CONTROLLER_URL (http://localhost:9000), PINOT_BROKER_URL
    (http://localhost:8000), PINOT_USER, PINOT_PASSWORD (unset = no auth)
  LAGAAM_ALLOWED_TABLES  comma list of catalog.schema.table — REQUIRED
  LAGAAM_ALLOW_ALL_TABLES=true  explicit opt-out: every table, no grant
  LAGAAM_AGENT_NAME      identity on the audit trail (anonymous)
  LAGAAM_MAX_SCAN_BYTES  pre-execution scan budget (50 GiB)
  LAGAAM_MAX_ROWS        pre-execution scanned-row budget (ungated)
  LAGAAM_MAX_INTERMEDIATE_ROWS  widest-operator row budget (default 50000000)
  LAGAAM_MAX_RETURNED_ROWS  rows handed back (1000, capped at 100000)
  LAGAAM_QUERY_TIMEOUT   wall-clock seconds per query (300)
  LAGAAM_METADATA_TTL    metadata cache TTL in seconds (300)
  LAGAAM_AUDIT_LOG       audit JSONL path (unset = stderr)
"""
```

and replace the `CachingQueryEngine(...)` construction inside `main()` (lines 37–40) with

```python
        engine = CachingQueryEngine(
            _select_engine(),
            ttl_seconds=float(os.environ.get("LAGAAM_METADATA_TTL", "300")),
        )
```

Then add this function directly above `def main() -> None:`.

```python
def _select_engine() -> QueryEngine:
    """The adapter named by LAGAAM_ENGINE; trino when unset.

    An unknown name is a configuration error rather than a silent default:
    a typo must not quietly point an agent at the wrong data platform.
    """
    name = os.environ.get("LAGAAM_ENGINE", "trino").strip().lower()
    if name == "trino":
        return TrinoEngine.from_env()
    if name == "pinot":
        return PinotEngine.from_env()
    raise ConfigurationError(
        f"LAGAAM_ENGINE={name!r} is not an engine this server has. "
        "Set it to 'trino' or 'pinot'."
    )
```

- [ ] 8. Verify both selections start and the unknown one refuses.

```bash
cd server && uv run python -c "
import os
os.environ['LAGAAM_ENGINE'] = 'pinot'
from lagaam.__main__ import _select_engine
print(type(_select_engine()).__name__)
os.environ['LAGAAM_ENGINE'] = 'trino'
print(type(_select_engine()).__name__)
os.environ['LAGAAM_ENGINE'] = 'duckdb'
try:
    _select_engine()
except Exception as exc:
    print('refused:', exc)
"
```

Expected: `PinotEngine`, `TrinoEngine`, then `refused: LAGAAM_ENGINE='duckdb' is not an engine this server has. Set it to 'trino' or 'pinot'.`

- [ ] 9. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 10. Commit.

```bash
git add server/src/lagaam/adapters/pinot/engine.py \
        server/tests/adapters/pinot/test_pinot_engine.py \
        server/tests/adapters/pinot/fixtures/databases.json \
        server/src/lagaam/__main__.py
git commit -m "feat(pinot): ground an agent on Pinot through the MCP tools

Pinot is flat and has no SQL catalog path at all, so list_catalogs
synthesises one catalog named pinot whose schemas are the controller's
databases — GET /databases exists on 1.5.1, and a controller without it still
grounds on default. estimate_cost answers low confidence and nothing else:
Pinot reports no bytes and a constant rowcount of 100 per scan, so the honest
interim answer is that the query cannot be priced, which the default budget
denies. U11 replaces it. LAGAAM_ENGINE picks the adapter, trino by default."
```

---

### Task 8: `execute()` — the multi-stage engine with pinned reins

**Files:**
- Modify: `server/src/lagaam/adapters/pinot/engine.py` (add imports, `_query_options`, `execute`)
- Modify: `server/tests/adapters/pinot/test_pinot_engine.py` (append the execution tests)

**Interfaces:**
- Consumes: `lagaam.adapters.pinot.names.two_part_sql`, `lagaam.adapters.pinot.response.parse_query_result`, `lagaam.adapters.pinot.response.result_failure`, `lagaam.core.query_errors.hint_for_engine_error`, `lagaam.core.query_errors.is_self_correctable`
- Produces:
  - `PinotEngine.execute(sql: str, max_rows: int, timeout_seconds: float | None = None) -> QueryResult` (async)
  - `PinotEngine.MAX_QUERY_RESPONSE_BYTES: int` — the fixed 64 MiB response cap

**Steps:**

- [ ] 1. Append the failing execution tests to `server/tests/adapters/pinot/test_pinot_engine.py`.

```python
def broker_engine(handler: Any, max_intermediate_rows: int | None = None) -> PinotEngine:
    engine = PinotEngine(
        controller_url="http://controller:9000",
        broker_url="http://broker:8000",
        max_intermediate_rows=max_intermediate_rows,
    )
    engine._client._http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return engine


def replying(body: Any) -> Any:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json=body)

    handler.seen = seen  # type: ignore[attr-defined]
    return handler


async def test_execute_sends_two_part_sql_to_the_broker() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 5",
        max_rows=10,
    )
    assert handler.seen["url"] == "http://broker:8000/query/sql"
    assert handler.seen["body"]["sql"] == (
        "SELECT Carrier, COUNT(*) AS n FROM default.airlineStats "
        "GROUP BY Carrier LIMIT 5"
    )


async def test_execute_pins_the_multistage_engine_and_the_response_cap() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
        max_rows=10,
        timeout_seconds=30.0,
    )
    options = handler.seen["body"]["queryOptions"].split(";")
    assert "useMultistageEngine=true" in options
    assert "timeoutMs=30000" in options
    assert "maxQueryResponseSizeBytes=67108864" in options


async def test_a_sub_millisecond_timeout_rounds_up_rather_than_to_zero() -> None:
    # timeoutMs=0 would be no cap at all, which is the opposite of a budget.
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5",
        max_rows=10,
        timeout_seconds=0.0004,
    )
    assert "timeoutMs=1" in handler.seen["body"]["queryOptions"].split(";")


async def test_no_timeout_means_no_timeout_option() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    assert "timeoutMs" not in handler.seen["body"]["queryOptions"]


async def test_the_row_budget_becomes_the_engines_own_row_limits() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler, max_intermediate_rows=50_000).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    options = handler.seen["body"]["queryOptions"].split(";")
    assert "maxRowsInJoin=50000" in options
    assert "maxRowsInWindow=50000" in options


async def test_no_row_budget_omits_the_row_limits() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
    )
    options = handler.seen["body"]["queryOptions"]
    assert "maxRowsInJoin" not in options
    assert "maxRowsInWindow" not in options


async def test_the_adapter_neither_adds_nor_removes_a_limit() -> None:
    handler = replying(load("agg-groupby.json"))
    await broker_engine(handler).execute(
        "SELECT Carrier FROM pinot.default.airlineStats", max_rows=10
    )
    assert "LIMIT" not in handler.seen["body"]["sql"].upper()


async def test_execute_returns_capped_rows() -> None:
    result = await broker_engine(replying(load("agg-groupby.json"))).execute(
        "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
        "GROUP BY Carrier LIMIT 5",
        max_rows=3,
    )
    assert result.row_count == 3
    assert result.truncated is True
    assert result.columns == ["Carrier", "n"]


async def test_a_row_limit_failure_becomes_a_teachable_error() -> None:
    from lagaam.core.errors import QueryFailedError

    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [
        {"message": "Cannot build in memory hash table for join operator", "errorCode": 245}
    ]
    with pytest.raises(QueryFailedError, match="distinct values"):
        await broker_engine(replying(body)).execute(
            "SELECT a.Carrier FROM pinot.default.airlineStats AS a "
            "JOIN pinot.default.baseballStats AS b ON a.Carrier = b.playerName LIMIT 5",
            max_rows=10,
        )


async def test_a_timeout_becomes_a_teachable_error() -> None:
    from lagaam.core.errors import QueryFailedError

    with pytest.raises(QueryFailedError, match="took too long"):
        await broker_engine(replying(load("timeout1-mse.json"))).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_a_trimmed_group_by_is_refused_rather_than_returned() -> None:
    from lagaam.core.errors import QueryFailedError

    with pytest.raises(QueryFailedError):
        await broker_engine(replying(load("numgroupslimit2.json"))).execute(
            "SELECT Origin, count(*) AS n FROM pinot.default.airlineStats "
            "GROUP BY Origin LIMIT 100",
            max_rows=100,
        )


async def test_a_broker_message_never_reaches_the_agent() -> None:
    from lagaam.core.errors import QueryFailedError

    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [
        {
            "message": "Serialized query response size 5190 exceeds threshold 100 "
            "for requestId 786551596000000039 from broker Broker_172.17.0.2_8000",
            "errorCode": 503,
        }
    ]
    with pytest.raises(QueryFailedError) as caught:
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )
    assert "172.17.0.2" not in str(caught.value)
    assert "786551596000000039" not in str(caught.value)


async def test_an_unmapped_failure_is_the_engines_fault_not_the_querys() -> None:
    body = dict(load("agg-groupby.json"))
    body["exceptions"] = [{"message": "who knows", "errorCode": 999}]
    with pytest.raises(EngineError):
        await broker_engine(replying(body)).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_an_unreachable_broker_is_an_engine_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with pytest.raises(EngineError):
        await broker_engine(handler).execute(
            "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5", max_rows=10
        )


async def test_another_catalog_is_refused_before_the_broker_is_called() -> None:
    called: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        called.append(str(request.url))
        return httpx.Response(200, json=load("agg-groupby.json"))

    with pytest.raises(TableNotFoundError):
        await broker_engine(handler).execute(
            "SELECT a FROM hive.default.airlineStats LIMIT 5", max_rows=10
        )
    assert called == []
```

- [ ] 2. Run the tests and watch them fail.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_engine.py
```

Expected: `AttributeError: 'PinotEngine' object has no attribute 'execute'` on each new test.

- [ ] 3. Extend the imports at the top of `server/src/lagaam/adapters/pinot/engine.py`. Replace `import os` with

```python
import math
import os
```

and add these imports after the `from lagaam.adapters.pinot.metadata import ...` line.

```python
from lagaam.adapters.pinot.names import two_part_sql
from lagaam.adapters.pinot.response import parse_query_result, result_failure
```

Extend the core imports to bring in `QueryFailedError` and `QueryResult`, and add the hint lookup.

```python
from lagaam.core.errors import EngineError, QueryFailedError, TableNotFoundError
from lagaam.core.query_errors import hint_for_engine_error, is_self_correctable
```

Add `QueryResult` to the `from lagaam.core.models import (...)` list, keeping it alphabetical after `DialectCard`.

- [ ] 4. Add the option builder and `execute` to `PinotEngine`, directly after `estimate_cost`. Also add the class constant next to `CATALOG`.

```python
    # A fixed ceiling on what one answer may weigh; the only byte-denominated
    # control Pinot offers, and the agent's row cap is not one.
    MAX_QUERY_RESPONSE_BYTES = 64 * 1024 * 1024
```

```python
    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        two_part = two_part_sql(sql, self.CATALOG)
        options = self._query_options(timeout_seconds)
        try:
            body = await self._client.broker_query(
                two_part, options, timeout_seconds=timeout_seconds
            )
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc

        failure = result_failure(body)
        if failure is not None:
            if is_self_correctable(failure):
                raise QueryFailedError(hint_for_engine_error(failure))
            raise EngineError(_UNREACHABLE)
        return parse_query_result(body, max_rows)

    def _query_options(self, timeout_seconds: float | None) -> str:
        """The reins, as Pinot's semicolon-separated option string.

        Every name here is pinned by an integration test that sets it low
        enough to trip: an unrecognised option is silently ignored, so a typo
        would disable a cap with no signal at all.
        """
        options = ["useMultistageEngine=true"]
        if timeout_seconds is not None:
            # Round up: a sub-millisecond budget must never render as 0, which
            # Pinot reads as no cap rather than as no time.
            options.append(f"timeoutMs={max(1, math.ceil(timeout_seconds * 1000))}")
        if self._max_intermediate_rows is not None:
            options.append(f"maxRowsInJoin={self._max_intermediate_rows}")
            options.append(f"maxRowsInWindow={self._max_intermediate_rows}")
        options.append(f"maxQueryResponseSizeBytes={self.MAX_QUERY_RESPONSE_BYTES}")
        return ";".join(options)
```

- [ ] 5. Run the tests and watch them pass.

```bash
cd server && uv run pytest -q tests/adapters/pinot/test_pinot_engine.py
```

Expected: `32 passed`.

- [ ] 6. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 7. Commit.

```bash
git add server/src/lagaam/adapters/pinot/engine.py \
        server/tests/adapters/pinot/test_pinot_engine.py
git commit -m "feat(pinot): run validated SQL on the multi-stage engine with reins on

Every query goes to the multi-stage engine so joins work and the row limits
fail hard rather than truncating — the single-stage engine caps silently at
ten rows with no flag, which is the worse failure. The timeout rounds up
because timeoutMs=0 is no cap at all, the budget's intermediate-row ceiling
becomes maxRowsInJoin and maxRowsInWindow, and the response is capped at
64 MiB. A failure the classifier cannot name is the engine's fault, and no
broker message reaches the agent."
```

---

### Task 9: the `pinot` compose profile and the `pinot_ready` fixture

**Files:**
- Modify: `examples/docker-compose.yml` (append after line 14)
- Modify: `server/tests/integration/conftest.py` (append after line 35)
- Modify: `server/pyproject.toml` (line 36, the `integration` marker text)

**Interfaces:**
- Consumes: `httpx`
- Produces: pytest fixture `pinot_ready` (returns `None`), available to every test under `server/tests/integration/`

**Steps:**

- [ ] 1. Add the `pinot` profile to `examples/docker-compose.yml`, appended after the trino service.

```yaml
  pinot:
    image: apachepinot/pinot:release-1.5.1
    container_name: lagaam-pinot
    profiles: ["pinot"]
    command: ["QuickStart", "-type", "BATCH"]
    environment:
      # The quickstart honours JAVA_OPTS and stays inside 3 GiB with this.
      JAVA_OPTS: "-Xms1G -Xmx3G -XX:+UseG1GC -Dlog4j2.configurationFile=/opt/pinot/conf/quickstart-log4j2.xml"
    ports:
      - "9000:9000"
      - "8000:8000"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/health"]
      interval: 5s
      timeout: 3s
      # First query answers at ~42s; the sample tables keep loading for ~20min.
      retries: 60
```

- [ ] 2. Update the marker text in `server/pyproject.toml` line 36 so it names both profiles.

```toml
    "integration: needs a running engine (docker compose --profile trino|pinot up)",
```

- [ ] 3. Add the `pinot_ready` fixture to `server/tests/integration/conftest.py`, appended after the `trino_ready` fixture.

```python
_PINOT_TABLES = ("airlineStats", "baseballStats")


@pytest.fixture
def pinot_ready() -> None:
    """Skip (don't fail) when Pinot isn't up, or hasn't loaded what we query.

    The quickstart answers its first query at ~42s but keeps bootstrapping
    tables for ~20 minutes, so a controller that is merely healthy can still
    show a partial catalog. Waiting on the two tables the tests actually
    query is the only honest readiness signal.
    """
    try:
        httpx.get("http://localhost:9000/health", timeout=2.0).raise_for_status()
    except httpx.HTTPError:
        pytest.skip("Pinot not reachable — docker compose --profile pinot up -d")

    deadline = time.monotonic() + 300
    while True:
        try:
            answered = all(_pinot_answers(table) for table in _PINOT_TABLES)
        except httpx.HTTPError:
            answered = False
        if answered:
            return
        if time.monotonic() > deadline:
            pytest.skip(
                "Pinot never had airlineStats and baseballStats queryable "
                "within 300s — the quickstart loads tables for ~20 minutes"
            )
        time.sleep(2)


def _pinot_answers(table: str) -> bool:
    """Is this table queryable through the broker right now?"""
    response = httpx.post(
        "http://localhost:8000/query/sql",
        json={
            "sql": f"SELECT count(*) AS n FROM {table} LIMIT 1",
            "queryOptions": "useMultistageEngine=true",
        },
        timeout=10.0,
    )
    response.raise_for_status()
    body = response.json()
    # Every Pinot query error is an HTTP 200, so the body is the only signal.
    return not body.get("exceptions") and bool(
        (body.get("resultTable") or {}).get("rows")
    )
```

- [ ] 4. Start Pinot and confirm the fixture's readiness probe answers.

```bash
docker compose -f examples/docker-compose.yml --profile pinot up -d
cd server && uv run python -c "
import sys
sys.path.insert(0, 'tests')
from integration.conftest import _pinot_answers
print('airlineStats', _pinot_answers('airlineStats'))
print('baseballStats', _pinot_answers('baseballStats'))
"
```

Expected: `airlineStats True` and `baseballStats True` (retry after a minute if the container is still bootstrapping).

- [ ] 5. Run the whole suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 6. Commit.

```bash
git add examples/docker-compose.yml \
        server/tests/integration/conftest.py \
        server/pyproject.toml
git commit -m "test(integration): a pinot profile and a fixture that waits for data

The quickstart answers its first query at ~42s but keeps loading sample tables
for around twenty minutes, so a healthy controller can still show a partial
catalog. The fixture waits on airlineStats and baseballStats specifically,
through the broker, because that is the only signal that says the tests can
run rather than that the process is up."
```

---

### Task 10: integration tests against the dockerized Pinot

**Files:**
- Create: `server/tests/integration/test_pinot_engine.py`

**Interfaces:**
- Consumes: `lagaam.adapters.pinot.engine.PinotEngine`, fixture `pinot_ready`, `lagaam.core.safety.validate_query`, `lagaam.core.ports.QueryEngine`, `lagaam.core.errors.*`
- Produces: nothing importable

**Steps:**

- [ ] 1. Write the failing test at `server/tests/integration/test_pinot_engine.py`.

```python
"""Pinot adapter against a real dockerized Pinot 1.5.1 batch quickstart.

Run: docker compose --profile pinot up -d   (from examples/)
Then: uv run pytest -m integration
"""

import pytest

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.core.budget import QueryBudget, enforce_budget
from lagaam.core.errors import (
    BudgetExceededError,
    QueryFailedError,
    TableNotFoundError,
)
from lagaam.core.ports import QueryEngine
from lagaam.core.safety import validate_query

pytestmark = pytest.mark.integration


@pytest.fixture
def engine(pinot_ready: None) -> PinotEngine:
    return PinotEngine(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )


def test_pinot_engine_satisfies_the_port(engine: PinotEngine) -> None:
    assert isinstance(engine, QueryEngine)


def test_dialect_card_targets_pinot(engine: PinotEngine) -> None:
    card = engine.dialect()
    assert card.engine == "Pinot"
    assert card.sqlglot_dialect == ""
    assert card.rules


async def test_list_catalogs_grounds_on_the_quickstart(engine: PinotEngine) -> None:
    meta = await engine.list_catalogs()
    assert [c.name for c in meta.catalogs] == ["pinot"]
    schemas = {s.name: s.tables for s in meta.catalogs[0].schemas}
    assert "default" in schemas
    tables = [t.lower() for t in schemas["default"]]
    assert "airlinestats" in tables
    assert "baseballstats" in tables


async def test_describe_table_reads_columns_and_the_row_count(
    engine: PinotEngine,
) -> None:
    card = await engine.describe_table("pinot", "default", "airlineStats")
    assert card.catalog == "pinot"
    assert card.schema_name == "default"
    assert card.table == "airlinestats"
    # The controller's numRows matched count(*) exactly on this OFFLINE table.
    assert card.row_estimate == 9746
    names = {c.name for c in card.columns}
    assert "Carrier" in names
    assert "DaysSinceEpoch" in names


async def test_a_table_that_does_not_exist_says_so(engine: PinotEngine) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("pinot", "default", "nosuchtable")


async def test_a_catalog_that_is_not_pinot_says_so(engine: PinotEngine) -> None:
    with pytest.raises(TableNotFoundError):
        await engine.describe_table("hive", "default", "airlineStats")


async def test_validated_sql_executes_on_the_multistage_engine(
    engine: PinotEngine,
) -> None:
    sql = validate_query(
        "select Carrier, count(*) as n from pinot.default.airlineStats "
        "group by Carrier order by n desc",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    assert "LIMIT 5" in sql
    result = await engine.execute(sql, max_rows=5, timeout_seconds=30.0)
    assert result.columns == ["Carrier", "n"]
    assert result.row_count == 5
    assert result.rows[0][0] == "WN"


async def test_a_join_runs_because_every_query_is_multistage(
    engine: PinotEngine,
) -> None:
    # The single-stage engine rejects any join outright with errorCode 150.
    sql = validate_query(
        "select a.Carrier as c from pinot.default.airlineStats a "
        "join pinot.default.airlineStats b on a.Carrier = b.Carrier",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    result = await engine.execute(sql, max_rows=5, timeout_seconds=60.0)
    assert result.row_count > 0


async def test_the_join_row_limit_is_a_real_backstop(pinot_ready: None) -> None:
    # maxRowsInJoin is only a cap if Pinot recognises the name — an unknown
    # option is silently ignored. Setting it low enough to trip proves it.
    engine = PinotEngine(
        controller_url="http://localhost:9000",
        broker_url="http://localhost:8000",
        max_intermediate_rows=5,
    )
    sql = validate_query(
        "select a.Carrier as c from pinot.default.airlineStats a "
        "join pinot.default.airlineStats b on a.Carrier = b.Carrier",
        dialect=engine.dialect().sqlglot_dialect,
        default_limit=5,
    )
    with pytest.raises(QueryFailedError, match="distinct values"):
        await engine.execute(sql, max_rows=5, timeout_seconds=60.0)


async def test_a_bad_column_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="describe_table"):
        await engine.execute(
            "SELECT nosuchcolumn FROM pinot.default.airlineStats LIMIT 5",
            max_rows=5,
            timeout_seconds=30.0,
        )


async def test_a_bad_table_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="list_catalogs"):
        await engine.execute(
            "SELECT Carrier FROM pinot.default.nosuchtable LIMIT 5",
            max_rows=5,
            timeout_seconds=30.0,
        )


async def test_a_bad_function_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="dialect card"):
        await engine.execute(
            "SELECT nosuchfunc(Carrier) AS x FROM pinot.default.airlineStats LIMIT 5",
            max_rows=5,
            timeout_seconds=30.0,
        )


async def test_a_timeout_maps_to_its_hint(engine: PinotEngine) -> None:
    with pytest.raises(QueryFailedError, match="took too long"):
        await engine.execute(
            "SELECT Carrier, count(*) AS n FROM pinot.default.airlineStats "
            "GROUP BY Carrier LIMIT 5",
            max_rows=5,
            timeout_seconds=0.001,
        )


async def test_the_adapter_neither_adds_nor_removes_a_limit(
    engine: PinotEngine,
) -> None:
    # The multi-stage engine has no auto-limit — measured, a LIMIT-less
    # selection returned all 9,746 rows. The LIMIT is core's job, not ours,
    # and the adapter must not paper over its absence.
    result = await engine.execute(
        "SELECT Carrier FROM pinot.default.airlineStats",
        max_rows=20,
        timeout_seconds=60.0,
    )
    assert result.row_count == 20
    assert result.truncated is True


async def test_estimate_cost_denies_until_the_quotation_lands(
    engine: PinotEngine,
) -> None:
    # U11 builds the quotation. Until then Pinot cannot be priced, so the
    # default budget denies every query — and says so, rather than admitting it.
    estimate = await engine.estimate_cost(
        "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
    )
    assert estimate.confidence == "low"
    with pytest.raises(BudgetExceededError, match="could not be estimated"):
        enforce_budget(estimate, QueryBudget.from_env())
```

- [ ] 2. Run the integration tests and watch them pass against the running container.

```bash
cd server && uv run pytest -q -m integration tests/integration/test_pinot_engine.py
```

Expected: `15 passed` (or `15 skipped` if Pinot is not up — start it with `docker compose -f examples/docker-compose.yml --profile pinot up -d` first).

- [ ] 3. Run the default suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green (integration deselected), `Success: no issues found`.

- [ ] 4. Commit.

```bash
git add server/tests/integration/test_pinot_engine.py
git commit -m "test(integration): the Pinot adapter against a real 1.5.1

Grounding reads the quickstart's real column list and its 9,746-row count,
and every error path is exercised against the engine that produces it rather
than a fixture. The join-limit test sets maxRowsInJoin low enough to trip on
purpose: an unrecognised query option is silently ignored, so the only proof
a cap is a cap is watching it fire."
```

---

### Task 11: end-to-end through MCP, and the honest interim denial

**Files:**
- Modify: `server/tests/integration/test_e2e_mcp.py` (rewrite: parameterize the grounding test over both engines, add the Pinot round-trip)

**Interfaces:**
- Consumes: `lagaam.adapters.pinot.engine.PinotEngine`, `lagaam.adapters.trino.engine.TrinoEngine`, `tests.helpers.lagaam_client`, `lagaam.core.identity.AgentIdentity`
- Produces: nothing importable

**Steps:**

- [ ] 1. Rewrite `server/tests/integration/test_e2e_mcp.py` with the failing Pinot cases.

```python
"""End to end: MCP protocol -> Lagaam server -> an adapter -> a real engine."""

import pytest

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.identity import AgentIdentity
from tests.helpers import lagaam_client

pytestmark = pytest.mark.integration


async def test_agent_can_ground_itself_end_to_end(trino_ready: None) -> None:
    engine = TrinoEngine(host="localhost", port=8080, user="lagaam-e2e")
    async with lagaam_client(engine) as client:
        catalogs = await client.call_tool("list_catalogs", {})
        assert not catalogs.isError
        assert catalogs.structuredContent is not None
        names = [c["name"] for c in catalogs.structuredContent["catalogs"]]
        assert "tpch" in names

        card = await client.call_tool(
            "describe_table",
            {"catalog": "tpch", "schema": "tiny", "table": "orders"},
        )
        assert not card.isError
        assert card.structuredContent is not None
        assert card.structuredContent["schema"] == "tiny"
        assert any(
            c["name"] == "orderkey" for c in card.structuredContent["columns"]
        )


async def test_agent_can_ground_itself_on_pinot_end_to_end(pinot_ready: None) -> None:
    engine = PinotEngine(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )
    identity = AgentIdentity(
        name="lagaam-e2e", allowed_tables=["pinot.default.airlinestats"]
    )
    async with lagaam_client(engine, identity=identity) as client:
        catalogs = await client.call_tool("list_catalogs", {})
        assert not catalogs.isError
        assert catalogs.structuredContent is not None
        names = [c["name"] for c in catalogs.structuredContent["catalogs"]]
        assert names == ["pinot"]

        card = await client.call_tool(
            "describe_table",
            {"catalog": "pinot", "schema": "default", "table": "airlineStats"},
        )
        assert not card.isError
        assert card.structuredContent is not None
        assert card.structuredContent["schema"] == "default"
        assert card.structuredContent["row_estimate"] == 9746
        assert any(
            c["name"] == "Carrier" for c in card.structuredContent["columns"]
        )


async def test_the_grant_hides_every_pinot_table_it_does_not_name(
    pinot_ready: None,
) -> None:
    engine = PinotEngine(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )
    identity = AgentIdentity(
        name="lagaam-e2e", allowed_tables=["pinot.default.airlinestats"]
    )
    async with lagaam_client(engine, identity=identity) as client:
        catalogs = await client.call_tool("list_catalogs", {})
        assert catalogs.structuredContent is not None
        tables = catalogs.structuredContent["catalogs"][0]["schemas"][0]["tables"]
        assert [t.lower() for t in tables] == ["airlinestats"]


async def test_query_data_on_pinot_is_denied_until_the_quotation_lands(
    pinot_ready: None,
) -> None:
    # U11 builds the Pinot quotation. Until then estimate_cost cannot price a
    # query, so the default budget denies it — and the denial says exactly
    # that, rather than an agent quietly getting rows it was never cleared for.
    engine = PinotEngine(
        controller_url="http://localhost:9000", broker_url="http://localhost:8000"
    )
    identity = AgentIdentity(
        name="lagaam-e2e", allowed_tables=["pinot.default.airlinestats"]
    )
    async with lagaam_client(engine, identity=identity) as client:
        answer = await client.call_tool(
            "query_data",
            {
                "sql": "SELECT Carrier FROM pinot.default.airlineStats LIMIT 5"
            },
        )
        assert answer.isError
        text = " ".join(
            block.text for block in answer.content if hasattr(block, "text")
        )
        assert "could not be estimated" in text
```

- [ ] 2. Run the e2e tests and watch them pass against both running engines.

```bash
cd server && uv run pytest -q -m integration tests/integration/test_e2e_mcp.py
```

Expected: `4 passed` when both containers are up; the Trino case skips on its own if only Pinot is running, and vice versa.

- [ ] 3. Run the default suite and the type checker.

```bash
cd server && uv run pytest -q && uv run mypy
```

Expected: green, `Success: no issues found`.

- [ ] 4. Run the full integration suite once, both engines up, as the closing check.

```bash
docker compose -f examples/docker-compose.yml --profile trino --profile pinot up -d
cd server && uv run pytest -q -m integration
```

Expected: every Trino and Pinot integration test passes; nothing skips.

- [ ] 5. Commit.

```bash
git add server/tests/integration/test_e2e_mcp.py
git commit -m "test(integration): an agent grounds itself on Pinot over MCP

The grounding round-trip runs against both adapters now, and the grant hides
every Pinot table it does not name. query_data is pinned to the denial it
currently gives: no Pinot quotation exists until U11, so the default budget
refuses the query and says the scan could not be estimated — the honest
interim answer, and a test that will fail loudly when U11 changes it."
```

---

## Notes on decisions this plan makes

**`GET /databases` exists on Pinot 1.5.1.** Confirmed by `curl` against the live container: it returns HTTP 200 with the bare JSON array `["default"]`. `list_catalogs` therefore uses it, and falls back to `default` alone when a controller answers 404 — Task 7 tests both paths.

**Bare names are left alone in `two_part_sql`.** By the time the adapter sees the SQL, `validate_query` and `check_tables_allowed` have both run, and the allowlist rejects any base table that is not three resolvable parts — so a bare name at this point can only be a CTE the allowlist already vouched for. Stripping and denying only qualified names keeps `names.py` free of scope tracking without weakening the boundary.

**The engine learns the intermediate-row budget at construction.** The port's `execute(sql, max_rows, timeout_seconds)` has no `max_intermediate_rows` parameter, and adding one would change core for one engine. `PinotEngine.from_env()` reads `LAGAAM_MAX_INTERMEDIATE_ROWS` — the same variable `QueryBudget.from_env()` reads — and spends it on `maxRowsInJoin` and `maxRowsInWindow`. Zero core change.

**`05-insert-from-file-mse.json` proves a rejection that happens in core.** `INSERT INTO ... FROM FILE` never reaches the broker: `validate_query` refuses it under the generic dialect, pinned in Task 1. The fixture earns its place in `response.py` for a different reason — it is the measured shape of a broker response that carries no result at all (empty `exceptions[]`, `requestId: null`), and `result_failure` reads exactly that as an engine fault.

**The multi-stage engine's unknown-column error needed a message rule the spec's table does not have.** Measured on 1.5.1: `SELECT nosuchcolumn FROM airlineStats` returns errorCode 700 with `QueryValidationError: ... The definition of column 'nosuchcolumn' depends on itself ...` — no `UnknownColumnError` prefix anywhere. Under the spec's table as written that classifies as `NOT_SUPPORTED`, which tells an agent to rewrite a query whose real problem is a typo'd column name. `errors.py` adds a `depends on itself` rule so 700 maps to `COLUMN_NOT_FOUND`, and Task 5 pins it against the captured `MSE:bad column` fixture.
