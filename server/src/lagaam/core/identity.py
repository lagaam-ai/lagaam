"""Agent identity: who is asking, and what they may touch.

The MCP server resolves a bearer token (or, over stdio, an env var) to one of
these, then enforces the allowlist and stamps it on every audit event. In
v0.1 the control plane isn't built yet, so identity is injected at startup —
one agent per server process, which matches the pod-per-agent model.
"""

import os

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from lagaam.core.errors import ConfigurationError
from lagaam.core.identifiers import normalize_grant


def _first_message(exc: ValidationError) -> str:
    """The rule that failed, without pydantic's location and URL noise."""
    errors = exc.errors()
    detail = errors[0]["msg"] if errors else str(exc)
    return detail.removeprefix("Value error, ")

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

    # Frozen: the allowlist is an authorization input, so it must not be
    # mutable after the validator has vouched for it.
    model_config = ConfigDict(frozen=True, validate_assignment=True)

    name: str
    allowed_tables: frozenset[str] | None = None

    @field_validator("allowed_tables")
    @classmethod
    def _grants_are_three_ascii_parts(
        cls, value: frozenset[str] | None
    ) -> frozenset[str] | None:
        """Normalize at construction, so no query pays for it or trips on it."""
        if value is None:
            return None
        return frozenset(normalize_grant(grant) for grant in value)

    def normalized_allowlist(self) -> frozenset[str] | None:
        """Case-folded allowlist for matching; None stays None."""
        return self.allowed_tables

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
                raise ConfigurationError(_NO_GRANT)
            allowed = None
        else:
            allowed = frozenset(t.strip() for t in raw.split(",") if t.strip())
        try:
            return cls(
                name=os.environ.get("LAGAAM_AGENT_NAME", "anonymous"),
                allowed_tables=allowed,
            )
        except ValidationError as exc:
            # An operator reading stderr needs the rule, not a pydantic dump.
            raise ConfigurationError(
                f"LAGAAM_ALLOWED_TABLES is not usable: {_first_message(exc)}"
            ) from exc
