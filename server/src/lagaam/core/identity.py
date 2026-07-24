"""Agent identity: who is asking, and what they may touch.

The MCP server resolves a bearer token (or, over stdio, an env var) to one of
these, then enforces the allowlist and stamps it on every audit event. In
v0.1 the control plane isn't built yet, so identity is injected at startup —
one agent per server process, which matches the pod-per-agent model.
"""

import os

from pydantic import BaseModel, field_validator

from lagaam.core.identifiers import IdentifierError, normalize_grant

_UNRESTRICTED_ENV = "LAGAAM_ALLOW_ALL_TABLES"

_NO_GRANT = (
    f"No table grant configured. Set LAGAAM_ALLOWED_TABLES to a comma list of "
    f"catalog.schema.table names, or set {_UNRESTRICTED_ENV}=true to let this "
    "agent read every table in every catalog."
)


class AgentIdentity(BaseModel):
    """An agent principal. ``allowed_tables`` is a set of fully-qualified
    ``catalog.schema.table`` names; None means unrestricted, an empty set
    means no tables at all. Unrestricted is never the default from env — a
    gate that opens when unconfigured is not a gate."""

    name: str
    allowed_tables: set[str] | None = None

    @field_validator("allowed_tables")
    @classmethod
    def _grants_are_three_ascii_parts(
        cls, value: set[str] | None
    ) -> set[str] | None:
        """Reject grants that could never match, at construction not at query."""
        if value is None:
            return None
        for grant in value:
            normalize_grant(grant)
        return value

    def normalized_allowlist(self) -> set[str] | None:
        """Case-folded allowlist for matching; None stays None."""
        if self.allowed_tables is None:
            return None
        return {normalize_grant(t) for t in self.allowed_tables}

    @classmethod
    def from_env(cls) -> "AgentIdentity":
        """Identity for the process. LAGAAM_ALLOWED_TABLES is a comma list of
        catalog.schema.table; unset is an error unless LAGAAM_ALLOW_ALL_TABLES
        opts out explicitly."""
        raw = os.environ.get("LAGAAM_ALLOWED_TABLES")
        unrestricted = os.environ.get(_UNRESTRICTED_ENV, "").lower() in (
            "1",
            "true",
            "yes",
        )
        if raw is None:
            if not unrestricted:
                raise IdentifierError(_NO_GRANT)
            allowed = None
        else:
            allowed = {t.strip() for t in raw.split(",") if t.strip()}
        return cls(
            name=os.environ.get("LAGAAM_AGENT_NAME", "anonymous"),
            allowed_tables=allowed,
        )
