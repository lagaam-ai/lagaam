"""Run the Lagaam MCP server over stdio, wired to Trino from env vars.

Usage: uv run python -m lagaam
Env: TRINO_HOST (localhost), TRINO_PORT (8080), TRINO_USER (lagaam)
"""

from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.server import create_server


def main() -> None:
    create_server(TrinoEngine.from_env()).run()


if __name__ == "__main__":
    main()
