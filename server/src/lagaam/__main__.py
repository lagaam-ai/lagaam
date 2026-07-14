"""Run the Lagaam MCP server over stdio, wired to Trino from env vars.

Usage: uv run python -m lagaam
Env: TRINO_HOST (localhost), TRINO_PORT (8080), TRINO_USER (lagaam),
     LAGAAM_METADATA_TTL (seconds, 300)
"""

import os

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.caching import CachingQueryEngine
from lagaam.server import create_server


def main() -> None:
    engine = CachingQueryEngine(
        TrinoEngine.from_env(),
        ttl_seconds=float(os.environ.get("LAGAAM_METADATA_TTL", "300")),
    )
    create_server(engine).run()


if __name__ == "__main__":
    main()
