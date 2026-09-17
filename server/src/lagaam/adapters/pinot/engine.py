"""Pinot adapter for the QueryEngine port.

Pinot has no catalog hierarchy, no SHOW TABLES and no information_schema, so
grounding is controller REST and the catalog is synthetic: exactly one, named
`pinot`, whose schemas are Pinot databases. Names are three-part to the agent
and two-part to Pinot. Every failure leaves this module as a LagaamError —
httpx exceptions and broker messages never escape.
"""

import math
import os
from dataclasses import dataclass
from typing import Any

import anyio
import httpx

from lagaam.adapters.pinot.client import (
    PinotClient,
    PinotForbidden,
    PinotResponseTooLarge,
    PinotTransportError,
)
from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.adapters.pinot.metadata import (
    TableFacts,
    schema_columns,
    table_facts,
    table_names,
    table_schema,
    upsert_config_present,
)
from lagaam.adapters.pinot.names import (
    has_offset,
    referenced_columns,
    referenced_tables,
    two_part_sql,
)
from lagaam.adapters.pinot.plan import key_ordinals, max_intermediate_rows
from lagaam.adapters.pinot.quote import quote, surviving_docs
from lagaam.adapters.pinot.response import (
    consuming_segments_queried,
    parse_query_result,
    result_failure,
    surviving_segments,
)
from lagaam.core.errors import EngineError, QueryFailedError, TableNotFoundError
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    CostEstimate,
    DialectCard,
    QueryResult,
    SchemaInfo,
    TableSchema,
)
from lagaam.core.query_errors import hint_for_engine_error, is_self_correctable
from lagaam.core.scans import (
    generator_fanout,
    has_unpriceable_shape,
    scan_counts_saturated,
    table_scan_counts,
)

_UNREACHABLE = "the query engine is not reachable right now"

# Our own words: the refusal body names tables the agent may not be told about.
_CREDENTIALS_REFUSED = "Lagaam's Pinot credentials were refused"

# Pinot's namespace is database.table and `default` always exists; 1.5.1 does
# expose GET /databases, but an older controller answering 404 still grounds.
_DEFAULT_DATABASE = "default"

# Pinot silently ignores an option name it does not know, so a typo here would
# disable a cap with no signal at all; the integration tests trip each one.
# _OPT_MAX_RESPONSE_BYTES is the exception: measured on 1.5.1, the multi-stage
# engine accepts the name and enforces nothing, so the client holds that ceiling.
_OPT_MULTISTAGE = "useMultistageEngine"
_OPT_TIMEOUT_MS = "timeoutMs"
_OPT_MAX_ROWS_IN_JOIN = "maxRowsInJoin"
_OPT_MAX_ROWS_IN_WINDOW = "maxRowsInWindow"
_OPT_MAX_RESPONSE_BYTES = "maxQueryResponseSizeBytes"

# The pruning oracle. Single-stage only: the multi-stage engine reports no
# pruning counters, and it refuses nothing that would reveal them.
_EXPLAIN_PRUNING = "EXPLAIN PLAN FOR "
_EXPLAIN_SHAPE = "EXPLAIN PLAN INCLUDING ALL ATTRIBUTES AS JSON FOR "

# Backstop only: the broker must hit its own timeoutMs and answer first. Now
# also the total deadline, so a body arriving in slow chunks cannot outlast it.
_TIMEOUT_GRACE_SECONDS = 5.0

# Advisory, so a wedged broker must not hold the gate for execute()'s default.
_EXPLAIN_TIMEOUT_SECONDS = 10.0
_EXPLAIN_TIMEOUT_MS = int(_EXPLAIN_TIMEOUT_SECONDS * 1000)


def _spelled(catalog: str, schema: str, table: str, listed: list[str]) -> str:
    """The controller's own spelling of a table, from its listing."""
    if table in listed:
        return table
    matches = [name for name in listed if name.lower() == table.lower()]
    if len(matches) != 1:
        # None is a missing table; more than one, and nothing says which.
        raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
    return matches[0]


def _plan_cell(body: object) -> str | None:
    """The PLAN column of a multi-stage EXPLAIN answer: one row, one string."""
    if not isinstance(body, dict) or body.get("exceptions"):
        return None
    result = body.get("resultTable")
    if not isinstance(result, dict):
        return None
    rows = result.get("rows")
    if not isinstance(rows, list) or not rows:
        return None
    first = rows[0]
    if not isinstance(first, list) or len(first) < 2:
        return None
    return first[1] if isinstance(first[1], str) else None


@dataclass(frozen=True)
class _Keycols:
    """What the key-ordinal EXPLAIN of one table has to be spelled with.

    Both spellings come from the controller — the listing for the table, the
    schema for the columns — and never from the agent's SQL, which reaches
    the broker case-insensitively and would name a column the plan does not.
    """

    database: str
    table: str
    columns: tuple[str, ...]


def _is_bare_identifier(name: str) -> bool:
    """Is this a name the ordinals EXPLAIN can carry as it stands?

    A key column's spelling comes from the schema document and is
    interpolated into a SELECT list, so it is checked like any other name
    this module puts in a statement: a controller is trusted for facts, not
    for syntax, and a name carrying a comma or a comment marker would be a
    second clause rather than a column.
    """
    return bool(name) and name.isascii() and name.replace("_", "").isalnum()


def _schema_name(config_json: Any, resolved: str) -> str:
    """The schema document's own name for this table.

    segmentsConfig.schemaName was absent on all four configs captured, so the
    fallback is the path that normally runs — and it has to be the resolved
    listing spelling, never the config's own tableName, which comes back
    suffixed ("u12upsert_REALTIME") and would 404 as a schema path.
    """
    if isinstance(config_json, dict):
        for key in ("REALTIME", "OFFLINE"):
            half = config_json.get(key)
            if not isinstance(half, dict):
                continue
            segments_config = half.get("segmentsConfig")
            if not isinstance(segments_config, dict):
                continue
            name = segments_config.get("schemaName")
            if isinstance(name, str) and name:
                return name
    return resolved


class PinotEngine:
    CATALOG = "pinot"

    # The default ceiling on what one answer may weigh; the client holds it,
    # and the same number is sent as the option so the two never disagree.
    MAX_QUERY_RESPONSE_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        controller_url: str = "http://localhost:9000",
        broker_url: str = "http://localhost:8000",
        user: str | None = None,
        password: str | None = None,
        max_tables_per_catalog: int = 1000,
        max_intermediate_rows: int | None = None,
        max_response_bytes: int = MAX_QUERY_RESPONSE_BYTES,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._controller_url = controller_url
        self._broker_url = broker_url
        self._max_tables = max_tables_per_catalog
        self._max_response_bytes = max_response_bytes
        # The port's execute() carries no row budget, so the engine is told
        # once and spends it on the broker's own maxRowsInJoin/InWindow caps.
        self._max_intermediate_rows = max_intermediate_rows
        self._client = PinotClient(
            controller_url=controller_url,
            broker_url=broker_url,
            user=user,
            password=password,
            max_response_bytes=max_response_bytes,
            transport=transport,
        )

    @classmethod
    def from_env(
        cls, transport: httpx.AsyncBaseTransport | None = None
    ) -> "PinotEngine":
        rows = os.environ.get("LAGAAM_MAX_INTERMEDIATE_ROWS")
        return cls(
            controller_url=os.environ.get(
                "PINOT_CONTROLLER_URL", "http://localhost:9000"
            ),
            broker_url=os.environ.get("PINOT_BROKER_URL", "http://localhost:8000"),
            user=os.environ.get("PINOT_USER"),
            password=os.environ.get("PINOT_PASSWORD"),
            max_intermediate_rows=int(rows) if rows else None,
            transport=transport,
        )

    def dialect(self) -> DialectCard:
        return PINOT_DIALECT_CARD

    async def list_catalogs(self) -> CatalogMetadata:
        try:
            databases = await self._databases()
        except PinotForbidden as exc:
            raise EngineError(_CREDENTIALS_REFUSED) from exc
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
        schemas: list[SchemaInfo] = []
        truncated = False
        attempted = 0
        for database in databases:
            try:
                PinotClient.path_part(database)
            except ValueError:
                # Non-ASCII cannot be granted either — see core.normalize_grant.
                continue
            attempted += 1
            try:
                body = await self._client.controller_get("/tables", database=database)
            except PinotForbidden as exc:
                # A refusal is the same for every database: never a skip.
                raise EngineError(_CREDENTIALS_REFUSED) from exc
            except PinotTransportError:
                # One broken database must not cost the grounding for healthy ones.
                continue
            if body is PinotClient.NotFound:
                continue
            names = table_names(body)
            budget = self._max_tables - sum(len(s.tables) for s in schemas)
            if len(names) > budget:
                truncated = True
                names = names[:budget]
            schemas.append(SchemaInfo(name=database, tables=names))
        if attempted and not schemas:
            raise EngineError(_UNREACHABLE)
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
            PinotClient.path_part(table)
            PinotClient.path_part(schema)
        except ValueError as exc:
            raise TableNotFoundError(
                catalog=catalog, schema=schema, table=table
            ) from exc
        try:
            resolved = await self._resolve_table(catalog, schema, table)
            part = PinotClient.path_part(resolved)
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
        except PinotForbidden as exc:
            raise EngineError(_CREDENTIALS_REFUSED) from exc
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc
        return table_schema(
            catalog,
            schema,
            resolved,
            schema_json,
            None if metadata_json is PinotClient.NotFound else metadata_json,
            None if config_json is PinotClient.NotFound else config_json,
        )

    async def _resolve_table(self, catalog: str, schema: str, table: str) -> str:
        """The controller's own spelling of the table the agent asked for.

        Controller REST paths are case-sensitive while broker SQL and grants
        are not, so the agent's spelling is a request, not an address: an
        unresolved name would 404 on a table that plainly exists.
        """
        body = await self._client.controller_get("/tables", database=schema)
        if body is PinotClient.NotFound:
            raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
        return _spelled(catalog, schema, table, table_names(body))

    async def estimate_cost(self, sql: str) -> CostEstimate:
        """An upper bound on what this SQL would scan, synthesised here.

        Pinot reports no bytes and a constant rowcount of 100 per scan, so
        every number below comes from segment metadata plus one honest
        engine signal: how many segments survive the predicate. A number
        that cannot be bounded is withheld, and the gate denies on that.
        """
        dialect = PINOT_DIALECT_CARD.sqlglot_dialect
        # Generators and a saturated read count both break the byte sum in
        # ways no scaling repairs — don't vouch for a quote at all.
        if has_unpriceable_shape(sql, dialect) or scan_counts_saturated(sql, dialect):
            return CostEstimate(confidence="low")
        two_part = two_part_sql(sql, self.CATALOG)
        tables = referenced_tables(sql, self.CATALOG)
        if not tables:
            return CostEstimate(confidence="low")
        columns = referenced_columns(sql)
        # One /tables listing per quotation, not one per table: every table
        # here shares a database, and a self-join reads the same name twice.
        listings: dict[str, list[str]] = {}
        # Lowercase database.table to the keycols EXPLAIN's own subject, filled
        # in only for a table whose keys were proven; see _key_ordinals.
        keycols: dict[str, _Keycols] = {}
        try:
            facts = [
                (
                    database,
                    await self._table_facts(
                        database, table, columns, listings, keycols
                    ),
                )
                for database, table in tables
            ]
        except PinotForbidden as exc:
            raise EngineError(_CREDENTIALS_REFUSED) from exc
        except PinotTransportError:
            # A quotation nobody could build is a denial at the gate, which
            # is the safe answer; an EngineError would read as an outage.
            return CostEstimate(confidence="low")
        surviving = await self._surviving(
            two_part, len(tables), trust_limit_prune=not has_offset(sql)
        )
        located = [(database, fact, surviving) for database, fact in facts]
        widest = await self._widest_rows(two_part, located, keycols)
        # The plan folds a repeated scan into one node and referenced_tables
        # dedupes, so a table read N times would be charged once: measured,
        # a self-join quoted 9,746 rows against 19,492 scanned, and UNION ALL
        # of 60 identical arms quoted 1/60th of the bytes at high confidence.
        # Charging the table's facts once per read scales both dimensions.
        reads = table_scan_counts(sql, dialect)
        charged = [
            pair
            for database, fact, k in located
            for pair in [(fact, k)]
            * max(1, reads.get(f"{self.CATALOG}.{database}.{fact.table}".lower(), 1))
        ]
        estimate = quote(charged, columns, widest)
        if estimate.max_intermediate_rows is None:
            return estimate
        fanout = generator_fanout(sql, dialect)
        return estimate.model_copy(
            update={"max_intermediate_rows": estimate.max_intermediate_rows * fanout}
        )

    async def _table_facts(
        self,
        database: str,
        table: str,
        columns: frozenset[str] | None,
        listings: dict[str, list[str]],
        keycols: dict[str, _Keycols],
    ) -> TableFacts:
        """One table's config, segment metadata and size, from the controller.

        The agent's spelling is a request, not an address, exactly as in
        describe_table: broker SQL is case-insensitive and controller REST
        paths are not, so an unresolved name 404s on a table that plainly
        exists and quotes low — denying a query the broker would run.
        """
        try:
            PinotClient.path_part(table)
        except ValueError as exc:
            # No URL path can carry this name, so the table it would name
            # cannot be reached either — decided before any request.
            raise TableNotFoundError(
                catalog=self.CATALOG, schema=database, table=table
            ) from exc
        if database not in listings:
            body = await self._client.controller_get("/tables", database=database)
            if body is PinotClient.NotFound:
                raise TableNotFoundError(
                    catalog=self.CATALOG, schema=database, table=table
                )
            listings[database] = table_names(body)
        spelled = _spelled(self.CATALOG, database, table, listings[database])
        part = PinotClient.path_part(spelled)
        resolved = await self._table_columns(part, database, columns)
        params: dict[str, str | list[str]] | None = (
            {"columns": sorted(resolved.values())} if resolved else None
        )
        config_json = await self._client.controller_get(
            f"/tables/{part}", database=database
        )
        config = None if config_json is PinotClient.NotFound else config_json
        seg_json = await self._client.controller_get(
            f"/segments/{part}/metadata", params=params, database=database
        )
        size_json = await self._client.controller_get(
            f"/tables/{part}/size", database=database
        )
        externalview_json = await self._client.controller_get(
            f"/tables/{part}/externalview", database=database
        )
        schema_json: Any = None
        table_metadata_json: Any = None
        if upsert_config_present(config):
            # Two more documents, only where the config says they say
            # something: primaryKeyColumns lives on the schema, and the PK
            # count map on the table metadata has to agree with it.
            try:
                schema_part = PinotClient.path_part(_schema_name(config, spelled))
            except ValueError as exc:
                raise TableNotFoundError(
                    catalog=self.CATALOG, schema=database, table=table
                ) from exc
            schema_body = await self._client.controller_get(
                f"/schemas/{schema_part}", database=database
            )
            schema_json = None if schema_body is PinotClient.NotFound else schema_body
            metadata_body = await self._client.controller_get(
                f"/tables/{part}/metadata", database=database
            )
            table_metadata_json = (
                None if metadata_body is PinotClient.NotFound else metadata_body
            )
        facts = table_facts(
            table,
            config,
            None if seg_json is PinotClient.NotFound else seg_json,
            None if size_json is PinotClient.NotFound else size_json,
            frozenset(resolved),
            externalview_json=(
                None if externalview_json is PinotClient.NotFound else externalview_json
            ),
            schema_json=schema_json,
            table_metadata_json=table_metadata_json,
        )
        if facts.unique_keys:
            await self._record_keycols(
                keycols, database, table, part, spelled, schema_json, facts
            )
        return facts

    async def _record_keycols(
        self,
        keycols: dict[str, _Keycols],
        database: str,
        table: str,
        part: str,
        spelled: str,
        schema_json: Any,
        facts: TableFacts,
    ) -> None:
        """Note what this table's key-ordinal EXPLAIN must be spelled with.

        Only a table with a proven key is recorded, so the extra EXPLAIN is
        never paid for by a table it could tell nothing about. The column
        spellings come from the schema document — the plan names columns as
        the catalog does, and the agent's SQL reaches the broker in any case.
        """
        spellings = schema_columns(schema_json) if schema_json is not None else {}
        if not spellings:
            body = await self._client.controller_get(
                f"/tables/{part}/schema", database=database
            )
            if body is PinotClient.NotFound:
                return
            spellings = schema_columns(body)
        names = sorted({name for key in facts.unique_keys for name in key})
        resolved = [spellings.get(name) for name in names]
        if not resolved or any(name is None for name in resolved):
            # A key column the schema does not name cannot be selected at all.
            return
        if not all(_is_bare_identifier(name) for name in resolved if name):
            # This name is interpolated into the ordinals EXPLAIN, so anything
            # but a plain identifier forfeits the evidence rather than be sent.
            return
        keycols[f"{database}.{table}".lower()] = _Keycols(
            database=database,
            table=spelled,
            columns=tuple(name for name in resolved if name is not None),
        )

    async def _table_columns(
        self, part: str, database: str, columns: frozenset[str] | None
    ) -> dict[str, str]:
        """The referenced columns this table carries, lowercase to its spelling.

        The controller's `?columns=` filter is case-sensitive and SQL is not,
        so asking for `playerid` returns nothing at all and the segment reads
        as though the column were free. A name the schema does not carry
        belongs to another table (or to no table) and is not asked for.

        Empty when there is nothing to resolve or no schema to resolve
        against, which charges whole segments — the fail-safe side.
        """
        if not columns:
            return {}
        schema_json = await self._client.controller_get(
            f"/tables/{part}/schema", database=database
        )
        if schema_json is PinotClient.NotFound:
            return {}
        spellings = schema_columns(schema_json)
        return {
            lowered: spelling
            for lowered, spelling in spellings.items()
            if lowered in columns
        }

    async def _surviving(
        self, two_part: str, table_count: int, *, trust_limit_prune: bool
    ) -> int | None:
        """Segments surviving the predicate, or None meaning "charge them all".

        Only ever asked for a single-table query: the single-stage engine
        refuses a join outright, and it is the only engine that prunes.

        The SQL is known here and not in the parser, so whether the limit
        prune may be believed is decided here and passed down.
        """
        if table_count != 1:
            return None
        try:
            body = await self._explain(
                f"{_EXPLAIN_PRUNING}{two_part}",
                f"{_OPT_TIMEOUT_MS}={_EXPLAIN_TIMEOUT_MS}",
            )
        except PinotForbidden as exc:
            raise EngineError(_CREDENTIALS_REFUSED) from exc
        except (PinotTransportError, PinotResponseTooLarge, TimeoutError):
            return None
        sealed = surviving_segments(body, trust_limit_prune=trust_limit_prune)
        if sealed is None:
            return None
        # numSegmentsQueried includes the consuming segments, so the k that
        # applies to sealed ones is what is left after subtracting them. The
        # consuming charge is added by quote.py, outside this k entirely.
        return max(0, sealed - consuming_segments_queried(body))

    async def _explain(self, sql: str, options: str) -> Any:
        """One quotation EXPLAIN, bounded end to end rather than per operation.

        The same deadline execute() carries, for the same reason: httpx's
        timeout resets on every read, so a body trickling in chunks outlives
        it — measured, a 1s timeout returned a result after 3.51s. The
        quotation is advisory, so a wedged broker must not hold the gate.
        """
        deadline = _EXPLAIN_TIMEOUT_SECONDS + _TIMEOUT_GRACE_SECONDS
        with anyio.fail_after(deadline):
            return await self._client.broker_query(
                sql, options, timeout_seconds=deadline
            )

    async def _widest_rows(
        self,
        two_part: str,
        tables: list[tuple[str, TableFacts, int | None]],
        keycols: dict[str, _Keycols],
    ) -> int | None:
        """Rows the widest plan node would build, or None if unreadable.

        Keyed as the plan spells a scan's table: [database, table], lowered.
        """
        leaves: dict[str, int | None] = {}
        keys: dict[str, frozenset[frozenset[str]]] = {}
        for database, facts, surviving in tables:
            name = f"{database}.{facts.table}".lower()
            leaves[name] = surviving_docs(facts, surviving)
            if facts.unique_keys:
                keys[name] = facts.unique_keys
        try:
            body = await self._explain(
                f"{_EXPLAIN_SHAPE}{two_part}",
                f"{_OPT_MULTISTAGE}=true;{_OPT_TIMEOUT_MS}={_EXPLAIN_TIMEOUT_MS}",
            )
        except PinotForbidden as exc:
            raise EngineError(_CREDENTIALS_REFUSED) from exc
        except (PinotTransportError, PinotResponseTooLarge, TimeoutError):
            return None
        cell = _plan_cell(body)
        if cell is None:
            # A plan nobody can read spends no EXPLAIN on the ordinals either.
            return None
        return max_intermediate_rows(
            cell, leaves, keys, await self._key_ordinals(keycols)
        )

    async def _key_ordinals(
        self, keycols: dict[str, _Keycols]
    ) -> dict[str, dict[str, int]]:
        """Each keyed table's key-column scan ordinals, one EXPLAIN apiece.

        A key column is proven by the position the scan reads it from, never
        by the name a projection gives it: `SELECT Origin AS Carrier` is a
        field called Carrier over another column's ordinal. So the ordinals
        are learned from the catalog's own spelling of the key columns,
        against a statement carrying no LIMIT and no ORDER BY — either would
        put a Sort above the project and leave nothing to read.

        A table whose EXPLAIN fails or parses to None simply gets no entry,
        which charges its joins the product exactly as before.
        """
        ordinals: dict[str, dict[str, int]] = {}
        for name, subject in keycols.items():
            columns = ", ".join(subject.columns)
            sql = (
                f"{_EXPLAIN_SHAPE}SELECT {columns} "
                f"FROM {subject.database}.{subject.table}"
            )
            try:
                body = await self._explain(
                    sql,
                    f"{_OPT_MULTISTAGE}=true;{_OPT_TIMEOUT_MS}={_EXPLAIN_TIMEOUT_MS}",
                )
            except PinotForbidden as exc:
                raise EngineError(_CREDENTIALS_REFUSED) from exc
            except (PinotTransportError, PinotResponseTooLarge, TimeoutError):
                continue
            cell = _plan_cell(body)
            if cell is None:
                continue
            learned = key_ordinals(cell)
            if learned:
                ordinals[name] = learned
        return ordinals

    async def execute(
        self, sql: str, max_rows: int, timeout_seconds: float | None = None
    ) -> QueryResult:
        two_part = two_part_sql(sql, self.CATALOG)
        options = self._query_options(timeout_seconds)
        client_timeout = (
            None
            if timeout_seconds is None
            else timeout_seconds + _TIMEOUT_GRACE_SECONDS
        )
        try:
            body = await self._with_deadline(
                two_part, options, client_timeout
            )
        except TimeoutError as exc:
            raise QueryFailedError(
                hint_for_engine_error("EXCEEDED_TIME_LIMIT")
            ) from exc
        except PinotResponseTooLarge as exc:
            raise QueryFailedError(
                hint_for_engine_error("RESPONSE_TOO_LARGE")
            ) from exc
        except PinotForbidden as exc:
            raise QueryFailedError(
                hint_for_engine_error("PERMISSION_DENIED")
            ) from exc
        except PinotTransportError as exc:
            raise EngineError(_UNREACHABLE) from exc

        failure = result_failure(body)
        if failure is not None:
            if is_self_correctable(failure):
                raise QueryFailedError(hint_for_engine_error(failure))
            raise EngineError(_UNREACHABLE)
        return parse_query_result(body, max_rows)

    async def _with_deadline(
        self, sql: str, options: str, deadline: float | None
    ) -> Any:
        """The broker call, bounded end to end rather than per operation.

        httpx's timeout resets on every read, so a body trickling in chunks
        outlives it: measured, a 0.2s budget returned a result after 6.01s.
        """
        if deadline is None:
            return await self._client.broker_query(sql, options)
        with anyio.fail_after(deadline):
            return await self._client.broker_query(
                sql, options, timeout_seconds=deadline
            )

    def _query_options(self, timeout_seconds: float | None) -> str:
        """The reins, as Pinot's semicolon-separated option string."""
        options = [f"{_OPT_MULTISTAGE}=true"]
        if timeout_seconds is not None:
            # Round up: a sub-millisecond budget must never render as 0, which
            # Pinot reads as no cap rather than as no time.
            milliseconds = max(1, math.ceil(timeout_seconds * 1000))
            options.append(f"{_OPT_TIMEOUT_MS}={milliseconds}")
        if self._max_intermediate_rows is not None:
            options.append(f"{_OPT_MAX_ROWS_IN_JOIN}={self._max_intermediate_rows}")
            options.append(f"{_OPT_MAX_ROWS_IN_WINDOW}={self._max_intermediate_rows}")
        options.append(f"{_OPT_MAX_RESPONSE_BYTES}={self._max_response_bytes}")
        return ";".join(options)

    async def _databases(self) -> list[str]:
        """Pinot databases, or just `default` on a controller without the endpoint."""
        body = await self._client.controller_get("/databases")
        if body is PinotClient.NotFound or not isinstance(body, list):
            return [_DEFAULT_DATABASE]
        names = sorted({d for d in body if isinstance(d, str) and d})
        return names or [_DEFAULT_DATABASE]
