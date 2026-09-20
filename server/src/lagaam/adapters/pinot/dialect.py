"""The Pinot dialect card, and a sqlglot dialect that sends Pinot what was written.

Every statement is re-rendered by sqlglot before it reaches the broker
(`validate_query`, then `two_part_sql`). The generic dialect canonicalises
spellings, and Pinot's parser does not accept the canonical ones: measured on
Pinot 1.5.1 on both engines, fourteen valid Pinot shapes broke or changed
their answer on the way out, and `ARRAY_AGG(c, 'TYPE', distinct)` was rejected
before it was ever sent. See
docs/superpowers/specs/2026-09-20-pinot-dialect-measurements.md and ADR 0010.

This is configuration of sqlglot's own parser and generator — what ADR 0003
permits — not a parser. Principle: preserve, never transpile. The card makes
the agent write Pinot SQL; sqlglot's job here is validation and two small
rewrites (LIMIT, catalog), so where a generic node cannot say a Pinot call
back, the call is kept as written.
"""

from collections.abc import Callable
from typing import ClassVar

from sqlglot import exp, generator, parser
from sqlglot.dialects.dialect import Dialect, inline_array_sql, rename_func

from lagaam.core.models import DialectCard

# Calls the generic parser folds into a node that cannot say them back:
# LOG10/LOG2 become LOG(base, x) and Pinot's LOG takes one argument; SUBSTR
# becomes SUBSTRING, which counts from 1, not 0; the rest are renamed to
# spellings Pinot has never had, or lose arguments on the way.
_KEPT_AS_WRITTEN = frozenset(
    {"JSON_EXTRACT_SCALAR", "SUBSTR", "STRPOS", "LOG10", "LOG2", "TRUNCATE"}
)


class ArrayAgg(exp.Expression, exp.AggFunc):
    """Pinot's ARRAY_AGG(column[, 'TYPE'[, distinct]]); sqlglot's own holds one argument."""

    arg_types: ClassVar[dict[str, bool]] = {"this": True, "expressions": False}
    is_var_len_args = True
    _sql_names: ClassVar[list[str]] = ["ARRAY_AGG", "ARRAYAGG"]


def _parse_array_agg(args: list[exp.Expr]) -> ArrayAgg:
    return ArrayAgg(this=args[0], expressions=args[1:])


def _array_agg_sql(self: generator.Generator, expression: ArrayAgg) -> str:
    return self.func("ARRAY_AGG", expression.this, *expression.expressions)


def _array_sql(self: generator.Generator, expression: exp.Array) -> str:
    """ARRAY['a', 'b'] — measured: Pinot's parser rejects the bare `['a', 'b']`
    that sqlglot's own inline_array_sql emits, on both engines."""
    return f"ARRAY{inline_array_sql(self, expression)}"


class Pinot(Dialect):
    """Registered by sqlglot under its lowercased class name: "pinot"."""

    class Parser(parser.Parser):
        FUNCTIONS: ClassVar[dict[str, Callable[..., exp.Expr]]] = {
            **{
                name: builder
                for name, builder in parser.Parser.FUNCTIONS.items()
                if name not in _KEPT_AS_WRITTEN
            },
            "ARRAY_AGG": _parse_array_agg,
            "ARRAYAGG": _parse_array_agg,
        }

    class Generator(generator.Generator):
        # Bare ClassVar, as in sqlglot: exp.DataType.Type is not valid as a type.
        TYPE_MAPPING: ClassVar = {
            **generator.Generator.TYPE_MAPPING,
            exp.DataType.Type.TEXT: "STRING",
        }
        TRANSFORMS: ClassVar[dict[type[exp.Expr], Callable[..., str]]] = {
            **generator.Generator.TRANSFORMS,
            exp.Array: _array_sql,
            exp.VariancePop: rename_func("VAR_POP"),
            exp.Variance: rename_func("VAR_SAMP"),
            exp.LogicalAnd: rename_func("BOOL_AND"),
            exp.LogicalOr: rename_func("BOOL_OR"),
            ArrayAgg: _array_agg_sql,
        }


PINOT_DIALECT_CARD = DialectCard(
    engine="Pinot",
    sqlglot_dialect="pinot",
    rules=[
        "Names have three levels: pinot.default.table — pinot is the only catalog",
        "Quote identifiers with double quotes; strings use single quotes",
        "Table and column names are case-insensitive",
        "Time columns are epoch numbers: convert with DATETIMECONVERT, DATETRUNC or ToDateTime",
        "Always filter on the table's time column — that is what prunes segments",
        "Prefer DISTINCTCOUNTHLL(x) over DISTINCTCOUNT(x) on large tables",
        "A type ending in [] is a multi-value column: use ARRAYLENGTH/ARRAY functions, not scalar comparisons",
        "CAST to Pinot types: STRING, LONG, INT, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP, BIG_DECIMAL",
        "Name columns explicitly; SELECT * is rejected",
        "Every query needs a LIMIT; one is added if missing",
        "Joins run on the multi-stage engine and are bounded by a row limit",
    ],
)
