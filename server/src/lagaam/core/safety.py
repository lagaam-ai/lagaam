"""SQL safety validation: parse, judge, constrain.

Allowlist design: only a single read-only query survives; anything sqlglot
parsed loosely (exp.Command) is rejected too. The validated AST is
re-rendered, so the SQL that executes is exactly the SQL that was judged.

This is parse-level safety, not authorization — table permissions (U7) and
engine-side access control remain the real enforcement layers.
"""

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


def validate_query(sql: str, dialect: str, default_limit: int = 1000) -> str:
    """Validate one read-only SELECT and return the canonical SQL to execute.

    Rejects (with what-to-change text): unparseable input, multiple
    statements, anything but SELECT/UNION/CTE, write/DDL nodes anywhere in
    the tree, and ``*`` projections. Injects ``LIMIT default_limit`` when
    the outer query has none.
    """
    try:
        statements = sqlglot.parse(sql, dialect=dialect)
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
    denied = tree.find(*_DENY_NODES)
    if denied is not None:
        raise SqlValidationError(
            "This tool is read-only: write/DDL constructs are not allowed "
            f"(found {denied.key.upper()}). Send a plain SELECT query."
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

    return tree.sql(dialect=dialect)
