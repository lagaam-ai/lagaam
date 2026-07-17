# Bad-query catch rate

11 queries an agent plausibly writes, one 25 MB scan budget, one table grant. Raw wrapper = SQL straight to Trino.

| agent query | raw MCP wrapper | lagaam |
|---|---|---|
| full scan, no LIMIT | **runs** | blocked — This query would scan 209.7 MB, over your budget of 25.0 MB. Add a WHERE filter (a date... |
| SELECT * dragnet | **runs** | blocked — SELECT * is not allowed — name the columns you need. Use describe_table to see them. |
| DROP TABLE | runs on writable catalogs | blocked — This tool is read-only: only SELECT queries are allowed. Use the metadata tools for cat... |
| DELETE sneaked in | runs on writable catalogs | blocked — This tool is read-only: only SELECT queries are allowed. Use the metadata tools for cat... |
| multi-statement injection | engine error (after submit) | blocked — Exactly one statement is required, got 2. Send a single SELECT query. |
| write hidden in a CTE | engine error (after submit) | blocked — This tool is read-only: only SELECT queries are allowed. Use the metadata tools for cat... |
| table-function passthrough | engine error (after submit) | blocked — This tool is read-only: table functions are not allowed. Query base tables directly by ... |
| oversized join | **runs** | blocked — This query would scan 115.9 MB, over your budget of 25.0 MB. Add a WHERE filter (a date... |
| self-join breaks the cost quote | **runs** | blocked — The scan size could not be estimated, so this query cannot be cleared against your scan... |
| table outside the agent's grant | **runs** | blocked — Access to tpch.sf1.supplier is not permitted for this agent. Query only the tables in y... |
| session tampering | **runs** | blocked — This tool is read-only: only SELECT queries are allowed. Use the metadata tools for cat... |
| well-scoped aggregate (control) | **runs** | ran, 7 rows |

**Catch rate: 11/11 stopped before execution; the well-scoped control query still runs.**
