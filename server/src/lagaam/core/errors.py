"""Domain errors.

Every error an agent can hit must say what to change — agents self-correct
on error text, so the message is part of the API. Core states the fact;
recovery hints that name MCP tools are attached at the server layer.
"""


class LagaamError(Exception):
    """Base for all domain errors."""


class TableNotFoundError(LagaamError):
    def __init__(self, catalog: str, schema: str, table: str) -> None:
        super().__init__(f"Table {catalog}.{schema}.{table} does not exist.")


class SqlValidationError(LagaamError):
    """The SQL failed a safety check; the message says what to change."""


class TableAccessDeniedError(LagaamError):
    """The query touches a table outside the agent's allowlist."""


class BudgetExceededError(LagaamError):
    """The query's estimated cost exceeds the agent's budget; message says
    what to change to make it fit."""


class EngineError(LagaamError):
    """The query engine failed or is unreachable — not the agent's fault."""

    def __init__(self, detail: str) -> None:
        super().__init__(
            f"The query engine could not complete the request: {detail}. "
            "This is not a problem with your input — retry, and report it "
            "if it persists."
        )
