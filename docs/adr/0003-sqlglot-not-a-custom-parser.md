# 0003 — sqlglot for validation and AST checks; never a custom SQL parser

**Status:** Accepted

## Context

The server must reject unsafe SQL — DDL/DML, `SELECT *`, missing `LIMIT` —
before spending an engine round-trip, and must handle real dialects.
Writing a SQL parser is a project-sized tarpit, and regex checks on SQL
text are wrong the day they ship.

## Decision

The LLM generates SQL in the target dialect (a dialect card is injected
into the prompt). sqlglot is used for parse-validation, AST safety checks,
and best-effort transpile only. Never write a custom SQL parser.

## Consequences

- Safety checks are structural, not textual: they operate on the AST, so a
  creative spelling of a forbidden construct still trips them.
- Dialect gaps in sqlglot are absorbed by generating in-dialect;
  transpilation is fallback, never the primary path.
- The gate inherits sqlglot's parsing limits: nesting depth is capped
  before rendering, and anything sqlglot cannot parse is refused rather
  than forwarded to the engine.
- Cardinality is deliberately *not* judged from the AST — that experiment
  failed and is recorded in ADR 0004.
