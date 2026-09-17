"""Three-part names for the agent, two-part names for Pinot.

Pinot's namespace is exactly database.table; a third part is an HTTP 500 with
a non-JSON body on the broker's own parser. Core's grant format, allowlist,
cache key and describe_table are all three-part, so the adapter presents a
synthetic catalog named `pinot` and strips exactly that part here — from both
table references and fully qualified column references.

This runs after validate_query and check_tables_allowed, so a bare name can
only be a CTE the allowlist already vouched for — bare names are left alone,
and only a qualified name is stripped or refused. Refusing a qualified name
we do not recognise is the security boundary: no request is made at all.
"""

import sqlglot
from sqlglot import exp

from lagaam.adapters.pinot.dialect import PINOT_DIALECT_CARD
from lagaam.core.errors import SqlValidationError, TableNotFoundError

_DIALECT = PINOT_DIALECT_CARD.sqlglot_dialect


def two_part_sql(sql: str, catalog: str = "pinot") -> str:
    """Validated SQL with the synthetic catalog dropped from every table."""
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except sqlglot.errors.SqlglotError as exc:
        raise SqlValidationError(
            "The SQL uses a construct this server cannot re-read safely. "
            "Rewrite it more simply and retry."
        ) from exc

    for table in tree.find_all(exp.Table):
        found = table.catalog
        if not found:
            # Bare name: a CTE, unless LAGAAM_ALLOW_ALL_TABLES let a schema-only table through.
            if table.db:
                rendered = table.sql(dialect=_DIALECT)
                raise SqlValidationError(
                    f"Table {rendered} is not a {catalog}.<database>.<table> "
                    f"name. Name every table as {catalog}.<database>.<table>, "
                    "then retry."
                )
            continue
        if found.lower() != catalog.lower():
            raise TableNotFoundError(catalog=found, schema=table.db, table=table.name)
        table.set("catalog", None)

    for column in tree.find_all(exp.Column):
        col_catalog = column.args.get("catalog")
        if not isinstance(col_catalog, exp.Identifier):
            continue
        col_catalog_name = col_catalog.this
        if col_catalog_name.lower() != catalog.lower():
            raise TableNotFoundError(
                catalog=col_catalog_name, schema=column.db, table=column.table
            )
        column.set("catalog", None)

    return tree.sql(dialect=_DIALECT, comments=False)


def referenced_tables(sql: str, catalog: str = "pinot") -> list[tuple[str, str]] | None:
    """Every (database, table) this SQL reads, folded case-insensitively and sorted.

    Folding keeps the first spelling encountered: the controller listing and
    core's scan-count keys are already case-folded, so two spellings of the
    same table must count as one entry, not two.

    None means the SQL did not re-parse, which charges the whole table rather
    than quoting a query nobody read.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return None
    found: dict[tuple[str, str], tuple[str, str]] = {}
    for table in tree.find_all(exp.Table):
        if not table.name:
            continue
        table_catalog = table.catalog
        if table_catalog and table_catalog.lower() != catalog.lower():
            raise TableNotFoundError(
                catalog=table_catalog, schema=table.db, table=table.name
            )
        # A bare name is a CTE the allowlist already vouched for, not a table.
        if not table.db:
            continue
        key = (table.db.lower(), table.name.lower())
        found.setdefault(key, (table.db, table.name))
    return sorted(found.values(), key=lambda pair: (pair[0].lower(), pair[1].lower()))


def has_offset(sql: str) -> bool:
    """Does this statement carry an OFFSET anywhere?

    True is the safe answer: a statement nobody could re-parse is treated as
    though it had one, which only ever charges more segments.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return True
    return any(True for _ in tree.find_all(exp.Offset))


def has_limit(sql: str) -> bool:
    """Does the outermost query carry an explicit integer row bound?

    The single-stage EXPLAIN plans a statement with no LIMIT under Pinot's
    own implicit default (`BROKER_REDUCE(limit:10)` appears in the plan) and
    reports `numSegmentsPrunedByLimit` for it, but the multi-stage engine
    that runs the query has no such default and scans everything. Measured:
    a bare `SELECT Carrier FROM airlineStats` quoted 200 against 600 docs
    scanned on the realtime instance and 422 against 9,746 on the batch one,
    both at high confidence. So the prune may only be believed when the
    statement itself carries the bound.

    `validate_query` injects a LIMIT before the port is ever called, but the
    port must not depend on its caller for a bound.

    `FETCH FIRST n ROWS ONLY` counts: sqlglot parses it as `exp.Fetch`
    rather than `exp.Limit`, and `validate_query` leaves it spelled that
    way, so it reaches the broker as a FETCH and bounds the execution just
    as a LIMIT does. Both spellings park the node on the outermost query's
    own `limit` argument, which is why a subquery's LIMIT — no bound on the
    rows the outer query walks — does not answer True.

    False is the safe answer: a statement nobody could re-parse, or a bound
    nobody can read as an integer (`LIMIT ALL`), charges every segment.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return False
    if not isinstance(tree, exp.Query):
        return False
    bound = tree.args.get("limit")
    # Fetch keeps its count under a different key than Limit's expression.
    if isinstance(bound, exp.Fetch):
        rows = bound.args.get("count")
    elif isinstance(bound, exp.Limit):
        rows = bound.args.get("expression")
    else:
        return False
    return isinstance(rows, exp.Literal) and not rows.is_string and rows.is_int


def referenced_columns(sql: str) -> frozenset[str] | None:
    """Lowercase bare names of every column this SQL mentions.

    None means a reference nobody can resolve to a column list — a star, or
    SQL that did not re-parse — and the caller charges whole segments for it.
    """
    try:
        tree = sqlglot.parse_one(sql, dialect=_DIALECT)
    except (sqlglot.errors.SqlglotError, RecursionError):
        return None
    for star in tree.find_all(exp.Star):
        # count(*) names no column; a projected star names all of them.
        if not isinstance(star.parent, exp.Count):
            return None
    for column in tree.find_all(exp.Column):
        if isinstance(column.this, exp.Star):
            return None
    return frozenset(
        column.name.lower()
        for column in tree.find_all(exp.Column)
        if column.name
    )
