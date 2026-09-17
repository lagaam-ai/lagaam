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
| [0007](0007-parser-safety-bounds.md) | Parser safety bounds sit ahead of the parse |
| [0008](0008-pinot-quotation-is-adapter-synthesised.md) | A Pinot quotation is synthesised by the adapter, not read from the engine |
| [0009](0009-consuming-segments-and-proven-join-keys.md) | A consuming segment is charged at its flush threshold, and a key is proven by the catalog and the scan ordinal |
