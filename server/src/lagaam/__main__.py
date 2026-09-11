"""Run the Lagaam MCP server over stdio, wired to one engine from env vars.

Usage: uv run python -m lagaam

The server refuses to start without a table grant: an agent that can reach
every table in every catalog is the thing this exists to prevent, so that
has to be asked for rather than inherited from an empty environment.

Env:
  LAGAAM_ENGINE          which adapter to run: trino (default) or pinot
  TRINO_HOST (localhost), TRINO_PORT (8080), TRINO_USER (lagaam)
  PINOT_CONTROLLER_URL (http://localhost:9000), PINOT_BROKER_URL
    (http://localhost:8000), PINOT_USER, PINOT_PASSWORD (unset = no auth)
  LAGAAM_ALLOWED_TABLES  comma list of catalog.schema.table — REQUIRED
  LAGAAM_ALLOW_ALL_TABLES=true  explicit opt-out: every table, no grant
  LAGAAM_AGENT_NAME      identity on the audit trail (anonymous)
  LAGAAM_MAX_SCAN_BYTES  pre-execution scan budget (50 GiB)
  LAGAAM_MAX_ROWS        pre-execution scanned-row budget (ungated)
  LAGAAM_MAX_INTERMEDIATE_ROWS  widest-operator row budget (default 50000000)
  LAGAAM_MAX_RETURNED_ROWS  rows handed back (1000, capped at 100000)
  LAGAAM_QUERY_TIMEOUT   wall-clock seconds per query (300)
  LAGAAM_METADATA_TTL    metadata cache TTL in seconds (300)
  LAGAAM_AUDIT_LOG       audit JSONL path (unset = stderr)
"""

import os
import sys

from lagaam.adapters.pinot.engine import PinotEngine
from lagaam.adapters.trino.engine import TrinoEngine
from lagaam.core.audit import AuditLog
from lagaam.core.budget import QueryBudget
from lagaam.core.caching import CachingQueryEngine
from lagaam.core.errors import ConfigurationError
from lagaam.core.identity import AgentIdentity
from lagaam.core.ports import QueryEngine
from lagaam.server import create_server


def _select_engine() -> QueryEngine:
    """The adapter named by LAGAAM_ENGINE; trino when unset.

    An unknown name is a configuration error rather than a silent default:
    a typo must not quietly point an agent at the wrong data platform.
    """
    name = os.environ.get("LAGAAM_ENGINE", "trino").strip().lower()
    if name == "trino":
        return TrinoEngine.from_env()
    if name == "pinot":
        return PinotEngine.from_env()
    raise ConfigurationError(
        f"LAGAAM_ENGINE={name!r} is not an engine this server has. "
        "Set it to 'trino' or 'pinot'."
    )


def main() -> None:
    try:
        engine = CachingQueryEngine(
            _select_engine(),
            ttl_seconds=float(os.environ.get("LAGAAM_METADATA_TTL", "300")),
        )
        server = create_server(
            engine,
            budget=QueryBudget.from_env(),
            identity=AgentIdentity.from_env(),
            audit=AuditLog.from_env(),
        )
    except (ConfigurationError, ValueError) as exc:
        # A misconfigured server is an operator's problem: say what to set,
        # on stderr, and exit — never a traceback into an agent's stdio.
        print(f"lagaam: {exc}", file=sys.stderr)
        raise SystemExit(2)
    server.run()


if __name__ == "__main__":
    main()
