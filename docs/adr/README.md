# Architecture Decision Records

Decisions that shaped Lagaam, in the order they were locked. Format:
Title / Status / Context / Decision / Consequences.

| # | Decision |
|---|---|
| [0001](0001-hexagonal-core-queryengine-port.md) | Hexagonal core with a `QueryEngine` port |
| [0002](0002-token-meter-query-quotation.md) | Token cost is a meter; query cost is a quotation |
| [0003](0003-sqlglot-not-a-custom-parser.md) | sqlglot for validation and AST checks; never a custom SQL parser |
| [0004](0004-plan-based-cardinality-gating.md) | Plan-based cardinality gating over SQL-shape heuristics |
| [0005](0005-gate-on-max-intermediate-rows.md) | Gate on maximum intermediate rows, not output rows |
| [0006](0006-row-generators-stay-sql-level.md) | Row generators remain a SQL-level check |
