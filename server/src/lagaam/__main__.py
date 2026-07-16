"""Run the Lagaam MCP server over stdio, wired to Trino from env vars.

Usage: uv run python -m lagaam
Env: TRINO_HOST (localhost), TRINO_PORT (8080), TRINO_USER (lagaam),
     LAGAAM_METADATA_TTL (seconds, 300), LAGAAM_MAX_SCAN_BYTES,
     LAGAAM_MAX_ROWS, LAGAAM_QUERY_TIMEOUT (all unset = no limit),
     LAGAAM_AGENT_NAME, LAGAAM_ALLOWED_TABLES (comma list, unset = all),
     LAGAAM_AUDIT_LOG (file path, unset = stderr)
"""

import os

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.audit import AuditLog
from lagaam.core.budget import QueryBudget
from lagaam.core.caching import CachingQueryEngine
from lagaam.core.identity import AgentIdentity
from lagaam.server import create_server


def main() -> None:
    engine = CachingQueryEngine(
        TrinoEngine.from_env(),
        ttl_seconds=float(os.environ.get("LAGAAM_METADATA_TTL", "300")),
    )
    create_server(
        engine,
        budget=QueryBudget.from_env(),
        identity=AgentIdentity.from_env(),
        audit=AuditLog.from_env(),
    ).run()


if __name__ == "__main__":
    main()
