"""Agent identity: who is asking, and what they may touch.

The MCP server resolves a bearer token (or, over stdio, an env var) to one of
these, then enforces the allowlist and stamps it on every audit event. In
v0.1 the control plane isn't built yet, so identity is injected at startup —
one agent per server process, which matches the pod-per-agent model.
"""

import os

from pydantic import BaseModel


class AgentIdentity(BaseModel):
    """An agent principal. ``allowed_tables`` is a set of fully-qualified
    ``catalog.schema.table`` names; None means unrestricted (single-tenant
    default), an empty set means no tables at all."""

    name: str
    allowed_tables: set[str] | None = None

    def normalized_allowlist(self) -> set[str] | None:
        """Case-folded allowlist for matching; None stays None."""
        if self.allowed_tables is None:
            return None
        return {t.lower() for t in self.allowed_tables}

    @classmethod
    def from_env(cls) -> "AgentIdentity":
        """Identity for the process. LAGAAM_ALLOWED_TABLES is a comma list of
        catalog.schema.table; unset means unrestricted."""
        raw = os.environ.get("LAGAAM_ALLOWED_TABLES")
        allowed = (
            {t.strip() for t in raw.split(",") if t.strip()}
            if raw is not None
            else None
        )
        return cls(
            name=os.environ.get("LAGAAM_AGENT_NAME", "anonymous"),
            allowed_tables=allowed,
        )
