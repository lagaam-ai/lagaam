# Lagaam

**Stop your agent's query from taking down a shared Trino or Pinot cluster.**
Lagaam is an MCP server that sits between your AI agents and your lakehouse
(Trino and Apache Pinot). Every query is schema-grounded, priced *before* it
runs, checked against a budget, and audited. Over budget, it is refused before
it runs — and the rejection tells the agent exactly how to fix its SQL.

## Run it

Against the Trino you already have:

```bash
TRINO_HOST=trino.internal LAGAAM_ALLOWED_TABLES=hive.sales.orders uvx lagaam
```

Wire it into any MCP client (Claude Code, Claude Desktop, or your own agent):

```json
{
  "mcpServers": {
    "lagaam": {
      "command": "uvx",
      "args": ["lagaam"],
      "env": {
        "TRINO_HOST": "localhost",
        "LAGAAM_MAX_SCAN_BYTES": "5368709120",
        "LAGAAM_ALLOWED_TABLES": "hive.sales.orders,hive.sales.customers"
      }
    }
  }
}
```

The agent gets three tools — `list_catalogs`, `describe_table`,
`query_data` — and cannot reach the engine any other way.

## Configure it

| Env var | Meaning | Default |
|---|---|---|
| `LAGAAM_ALLOWED_TABLES` | Comma list of `catalog.schema.table` grants | **required** |
| `TRINO_HOST` / `TRINO_PORT` / `TRINO_USER` | Trino coordinator | `localhost` / `8080` / `lagaam` |
| `LAGAAM_ENGINE` | `pinot` to run the native Pinot adapter | `trino` |
| `PINOT_CONTROLLER_URL` / `PINOT_BROKER_URL` | Pinot controller and broker | `http://localhost:9000` / `http://localhost:8000` |
| `LAGAAM_MAX_SCAN_BYTES` | Scan-bytes budget per query, pre-execution | 50 GiB |

**The server will not start without `LAGAAM_ALLOWED_TABLES`.** An agent that
can reach every table in every catalog is the thing this exists to prevent, so
that has to be asked for — set `LAGAAM_ALLOW_ALL_TABLES=true` if you mean it.

Every budget and setting: [Configuration](https://github.com/lagaam-ai/lagaam#configuration).

Full docs: [github.com/lagaam-ai/lagaam](https://github.com/lagaam-ai/lagaam)
