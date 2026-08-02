"""SQL safety validation: parse, judge, constrain.

Allowlist design: only a single read-only query survives; anything sqlglot
parsed loosely (exp.Command) is rejected too. The validated AST is
re-rendered, so the SQL that executes is exactly the SQL that was judged.

This is parse-level safety, not authorization — table permissions (U7) and
engine-side access control remain the real enforcement layers.
"""

import re

import sqlglot
from sqlglot import exp

from lagaam.core.errors import SqlValidationError

# exp.DDL covers only Create+Insert in sqlglot 30.x — enumerate explicitly.
_DENY_NODES = (
    exp.Create,
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Alter,
    exp.Merge,
    exp.TruncateTable,
    exp.Grant,
    exp.Command,
)


# Parsing is superlinear in nesting depth: measured on sqlglot 30.12, 3 MB of
# nested subqueries cost 5s and 12 MB cost 25s, before any check of ours runs.
# The bound belongs here, ahead of the parse — a gate an agent can make burn
# CPU is the unbounded work it exists to refuse. Trino itself accepts more
# than this, so the ceiling is generous: a 200k-character query is a machine
# padding the input, not an analyst asking a question.
_MAX_SQL_CHARS = 200_000

# sqlglot's own recursive-descent parser blows the stack on deep bracket
# nesting well before a query gets big enough to trip _MAX_SQL_CHARS —
# measured under pytest: parsing itself raised RecursionError at 118 levels
# of "SELECT x FROM (...) t" (1,993 characters). This ceiling is a cheap,
# text-level proxy checked before parsing is attempted at all, generous
# enough that no ordinary query's bracket nesting comes close.
_MAX_BRACKET_DEPTH = 60

# sqlglot's generator recurses once per AST level, so a *small* query nested
# deeply enough blows the stack inside tree.sql(): measured under pytest,
# rendering raised RecursionError at an AST depth of 263 (87 levels of
# "SELECT x FROM (...) t"). An ordinary analytical query — joins, a CASE
# aggregate, a CTE pipeline, a window function — measured 6-9 deep; 20 levels
# of synthetic subquery nesting measured 62. This cap sits well above real
# SQL and well below where rendering breaks.
_MAX_NESTING_DEPTH = 100

_GROUPING_LIMIT = re.compile(
    r"(GROUP\s+BY\s+(?:ROLLUP|CUBE|GROUPING\s+SETS)\b.*?)\s+(LIMIT\s+\d+)\s*$",
    re.IGNORECASE | re.DOTALL,
)


def _bracket_depth(sql: str) -> int:
    """Deepest ``(``/``)`` nesting in the raw text, scanned iteratively.

    Runs before parsing, since sqlglot's recursive-descent parser can blow
    the stack on bracket nesting alone — a RecursionError the parser raises
    itself, which no post-parse check can catch.
    """
    depth = 0
    peak = 0
    for char in sql:
        if char == "(":
            depth += 1
            peak = max(peak, depth)
        elif char == ")":
            depth -= 1
    return peak


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


def _parse_statements(sql: str, dialect: str) -> list[exp.Expr | None]:
    """Parse the agent's SQL, working around one parser gap.

    sqlglot 30.12 and 30.13 cannot read a LIMIT directly after GROUP BY
    ROLLUP/CUBE/GROUPING SETS, though Trino runs it. Rejecting it would deny
    a core BI shape over a parser bug, so the LIMIT is re-attached to the
    tree after parsing the query without it. Only that exact tail is touched,
    and only after the unmodified SQL has already failed.
    """
    try:
        return list(sqlglot.parse(sql, dialect=dialect))
    except sqlglot.errors.ParseError:
        match = _GROUPING_LIMIT.search(sql)
        if match is None:
            raise
        statements = list(sqlglot.parse(sql[: match.end(1)], dialect=dialect))
        rows = int(re.sub(r"\D", "", match.group(2)))
        tail = statements[-1] if statements else None
        if not isinstance(tail, exp.Query):
            raise
        statements[-1] = tail.limit(rows)
        return statements


def validate_query(sql: str, dialect: str, default_limit: int = 1000) -> str:
    """Validate one read-only SELECT and return the canonical SQL to execute.

    Rejects (with what-to-change text): unparseable input, multiple
    statements, anything but SELECT/UNION/CTE, write/DDL nodes anywhere in
    the tree, and ``*`` projections. Injects ``LIMIT default_limit`` when
    the outer query has none.
    """
    if len(sql) > _MAX_SQL_CHARS:
        raise SqlValidationError(
            f"The SQL is {len(sql):,} characters, over the "
            f"{_MAX_SQL_CHARS:,} this server parses. Select fewer columns, "
            "shorten any IN list, or split the query."
        )
    if _bracket_depth(sql) > _MAX_BRACKET_DEPTH:
        raise SqlValidationError(
            "The SQL is nested too deeply for this server to process safely. "
            "Flatten the subqueries — most nesting can be replaced by a CTE "
            "or a join — and retry."
        )
    try:
        statements = _parse_statements(sql, dialect)
    except sqlglot.errors.ParseError as errs:
        first = errs.errors[0] if errs.errors else {}
        where = f" (line {first.get('line')}, col {first.get('col')})" if first else ""
        raise SqlValidationError(
            f"The SQL could not be parsed as {dialect}{where}: "
            f"{first.get('description', str(errs))}. Fix the syntax and retry."
        ) from errs
    except sqlglot.errors.SqlglotError as exc:
        # TokenError and friends fail closed, not as a raw crash.
        raise SqlValidationError(
            f"The SQL could not be parsed as {dialect}. Fix the syntax and retry."
        ) from exc

    statements = [s for s in statements if s is not None]
    if len(statements) != 1:
        raise SqlValidationError(
            f"Exactly one statement is required, got {len(statements)}. "
            "Send a single SELECT query."
        )
    tree = statements[0]

    if not isinstance(tree, exp.Query):
        raise SqlValidationError(
            "This tool is read-only: only SELECT queries are allowed. "
            "Use the metadata tools for catalogs and schemas."
        )
    if _too_deeply_nested(tree):
        raise SqlValidationError(
            "The SQL is nested too deeply for this server to process safely. "
            "Flatten the subqueries — most nesting can be replaced by a CTE "
            "or a join — and retry."
        )
    denied = tree.find(*_DENY_NODES)
    if denied is not None:
        raise SqlValidationError(
            "This tool is read-only: write/DDL constructs are not allowed "
            f"(found {denied.key.upper()}). Send a plain SELECT query."
        )

    for table in tree.find_all(exp.Table):
        # A base table parses with an Identifier here; a table function (e.g.
        # a system.query passthrough) parses as a Func and can smuggle
        # arbitrary SQL — including writes — past every check above.
        if isinstance(table.this, exp.Func):
            raise SqlValidationError(
                "This tool is read-only: table functions are not allowed. "
                "Query base tables directly by catalog.schema.table name."
            )

    for star in tree.find_all(exp.Star):
        # count(*) is fine — a star only offends as a projection. Projection
        # stars parent under Select, Column, or (for 4+ part names) Dot.
        if isinstance(star.parent, (exp.Select, exp.Column, exp.Dot)):
            raise SqlValidationError(
                "SELECT * is not allowed — name the columns you need. "
                "Use describe_table to see them."
            )

    # FETCH FIRST N ROWS parses under the "limit" key too, so this covers it.
    if tree.args.get("limit") is None:
        tree = tree.limit(default_limit)

    # Comments carry nothing the engine needs, and kilobytes of them are how
    # an agent pushes the real query out of a truncated audit line.
    rendered = tree.sql(dialect=dialect, comments=False)
    return _reparseable(rendered, tree, dialect)


def _reparseable(rendered: str, tree: exp.Expr, dialect: str) -> str:
    """The validated SQL in a form the parser can read back.

    Every downstream gate re-parses this string and denies what it cannot
    read, so a shape sqlglot emits but cannot re-read is a shape the server
    refuses outright. sqlglot 30.12 and 30.13 cannot parse a LIMIT directly
    after GROUP BY ROLLUP/CUBE/GROUPING SETS — which is ordinary BI SQL, not
    an attack — so the LIMIT moves outside a wrapper the parser accepts.
    """
    try:
        sqlglot.parse_one(rendered, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        pass
    else:
        return rendered

    limit = tree.args.get("limit")
    if limit is None or not isinstance(tree, exp.Query):
        raise SqlValidationError(
            "The SQL uses a construct this server cannot re-read safely. "
            "Rewrite it more simply and retry."
        )
    inner = tree.copy()
    inner.set("limit", None)
    wrapped = (
        exp.select("*").from_(inner.subquery(alias="_lagaam")).limit(limit.expression)
    )
    rendered = wrapped.sql(dialect=dialect, comments=False)
    try:
        sqlglot.parse_one(rendered, dialect=dialect)
    except sqlglot.errors.SqlglotError:
        raise SqlValidationError(
            "The SQL uses a construct this server cannot re-read safely. "
            "Rewrite it more simply and retry."
        ) from None
    return rendered
