"""Trino adapter for the QueryEngine port.

The trino dbapi client is blocking, so every call runs in a worker thread;
the async surface is what the MCP server needs. All engine failures leave
this module as LagaamError subclasses — raw trino exceptions never escape.
"""

import os

import anyio.to_thread
import trino.dbapi
import trino.exceptions

from lagaam.core.errors import EngineError, TableNotFoundError
from lagaam.core.identifiers import quote_identifier
from lagaam.core.models import (
    CatalogInfo,
    CatalogMetadata,
    ColumnInfo,
    SchemaInfo,
    TableSchema,
)

_NOT_FOUND_ERRORS = {"CATALOG_NOT_FOUND", "SCHEMA_NOT_FOUND", "TABLE_NOT_FOUND"}

# Present in every catalog; protocol plumbing, not grounding material.
_HIDDEN_SCHEMAS = {"information_schema"}


class TrinoEngine:
    def __init__(
        self, host: str = "localhost", port: int = 8080, user: str = "lagaam"
    ) -> None:
        self._host = host
        self._port = port
        self._user = user

    @classmethod
    def from_env(cls) -> "TrinoEngine":
        kwargs: dict[str, str | int] = {}
        if "TRINO_HOST" in os.environ:
            kwargs["host"] = os.environ["TRINO_HOST"]
        if "TRINO_PORT" in os.environ:
            kwargs["port"] = int(os.environ["TRINO_PORT"])
        if "TRINO_USER" in os.environ:
            kwargs["user"] = os.environ["TRINO_USER"]
        return cls(**kwargs)  # type: ignore[arg-type]

    async def list_catalogs(self) -> CatalogMetadata:
        try:
            return await anyio.to_thread.run_sync(self._list_catalogs)
        except (trino.exceptions.Error, OSError) as exc:
            raise EngineError(str(exc)) from exc

    async def describe_table(
        self, catalog: str, schema: str, table: str
    ) -> TableSchema:
        try:
            return await anyio.to_thread.run_sync(
                self._describe_table, catalog, schema, table
            )
        except (trino.exceptions.Error, OSError) as exc:
            raise EngineError(str(exc)) from exc

    def _connect(self) -> trino.dbapi.Connection:
        return trino.dbapi.connect(
            host=self._host, port=self._port, user=self._user
        )

    def _list_catalogs(self) -> CatalogMetadata:
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute("SHOW CATALOGS")
            catalog_names = [row[0] for row in cur.fetchall()]

            # U2 caches and bounds this listing; serial + unbounded is U1-only.
            catalogs: list[CatalogInfo] = []
            for name in catalog_names:
                try:
                    cur.execute(
                        "SELECT table_schema, table_name "
                        f"FROM {quote_identifier(name)}.information_schema.tables "
                        "ORDER BY table_schema, table_name"
                    )
                    rows = cur.fetchall()
                except (trino.exceptions.TrinoQueryError, ValueError):
                    # One broken catalog must not cost the grounding for healthy ones.
                    continue
                schemas: dict[str, list[str]] = {}
                for table_schema, table_name in rows:
                    if table_schema in _HIDDEN_SCHEMAS:
                        continue
                    schemas.setdefault(table_schema, []).append(table_name)
                catalogs.append(
                    CatalogInfo(
                        name=name,
                        schemas=[
                            SchemaInfo(name=s, tables=t) for s, t in schemas.items()
                        ],
                    )
                )
            return CatalogMetadata(catalogs=catalogs)

    def _describe_table(self, catalog: str, schema: str, table: str) -> TableSchema:
        try:
            quoted = ".".join(quote_identifier(p) for p in (catalog, schema, table))
        except ValueError:
            raise TableNotFoundError(catalog=catalog, schema=schema, table=table)
        with self._connect() as conn:
            cur = conn.cursor()
            try:
                cur.execute(f"SHOW COLUMNS FROM {quoted}")
                rows = cur.fetchall()
            except trino.exceptions.TrinoUserError as exc:
                if exc.error_name in _NOT_FOUND_ERRORS:
                    raise TableNotFoundError(
                        catalog=catalog, schema=schema, table=table
                    ) from exc
                # exc.message, not str(exc): repr leaks query ids and noise.
                raise EngineError(exc.message or type(exc).__name__) from exc
            # SHOW COLUMNS rows: (column, type, extra, comment)
            columns = [
                ColumnInfo(name=row[0], type=row[1], comment=row[3] or None)
                for row in rows
            ]
            # Echo canonical names so the card matches list_catalogs output.
            return TableSchema(
                catalog=catalog.lower(),
                schema_name=schema.lower(),
                table=table.lower(),
                columns=columns,
            )
