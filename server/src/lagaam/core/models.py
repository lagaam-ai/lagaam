"""Engine-agnostic domain models.

These are the shapes the MCP tool surface returns to agents; adapters
translate engine-specific metadata into them. Core never imports engine SDKs.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic.json_schema import SkipJsonSchema


class ColumnInfo(BaseModel):
    name: str
    type: str
    comment: str | None = None


class TableSchema(BaseModel):
    """Grounding card for one table: identity + columns."""

    # Agents see "schema"; the Python name avoids shadowing BaseModel.schema.
    model_config = ConfigDict(populate_by_name=True, serialize_by_alias=True)

    catalog: str
    schema_name: str = Field(alias="schema")
    table: str
    columns: list[ColumnInfo]
    comment: str | None = None
    # From engine statistics when available; grounds LIMIT/filter decisions.
    row_estimate: int | None = None

    @property
    def fqn(self) -> str:
        return f"{self.catalog}.{self.schema_name}.{self.table}"


class SchemaInfo(BaseModel):
    name: str
    tables: list[str]


class CatalogInfo(BaseModel):
    name: str
    schemas: list[SchemaInfo]
    # Listing was capped; unseen tables may still exist.
    truncated: bool = False


class CatalogMetadata(BaseModel):
    catalogs: list[CatalogInfo]


class CostEstimate(BaseModel):
    """Pre-execution QUOTATION for a query: what it would scan, how sure we are.

    Two different row numbers, because they answer different questions.
    ``row_estimate`` is rows *scanned*, summed from the IO plan.
    ``max_intermediate_rows`` is the widest row count any single operator
    would build — a cross join scans 75,175 rows and builds 902,625,000, and
    only the second number says so.

    ``confidence`` is "low" whenever the byte number is missing — the engine
    had no statistics, so the budget gate must fail safe rather than admit a
    query on an absent estimate.
    """

    scanned_bytes: int | None = None
    row_estimate: int | None = None
    # Rows the widest operator would build; None when the plan cannot say.
    max_intermediate_rows: int | None = None
    # None = "infer from the evidence"; an explicit value is checked for sanity.
    confidence: Literal["high", "low"] | None = None

    @model_validator(mode="after")
    def _confidence_follows_the_evidence(self) -> "CostEstimate":
        if self.confidence == "high" and self.scanned_bytes is None:
            raise ValueError("high confidence needs a scanned_bytes number")
        if self.confidence is None:
            resolved = "low" if self.scanned_bytes is None else "high"
            object.__setattr__(self, "confidence", resolved)
        return self


class QueryActuals(BaseModel):
    """What the engine says a query actually cost, read after it ran.

    The receipt to CostEstimate's quotation, for the audit line: an operator
    can only tune a budget by seeing how far the quote was off. Observation
    only — nothing is ever gated on these numbers.

    Every field is optional because engines report different things: Pinot
    reports no bytes at all, so ``bytes_scanned`` is None there rather than 0.
    ``rows_scanned`` is rows the scan read (Trino ``processedRows``, Pinot
    ``numDocsScanned``), the same sense as ``CostEstimate.row_estimate``.
    """

    rows_scanned: int | None = None
    bytes_scanned: int | None = None
    elapsed_ms: int | None = None
    # The engine was still running when read (a capped fetch), so every
    # count is a floor rather than the total.
    partial: bool = False
    # Engine-native counters under their engine's own names, for forensics.
    engine_counters: dict[str, int] = Field(default_factory=dict)


class QueryResult(BaseModel):
    """Rows returned to the agent, capped to a row budget.

    ``truncated`` says the cap hit — more rows exist than were returned, so a
    conclusion drawn from these alone may be wrong.
    """

    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    truncated: bool = False
    # Non-blocking notes the agent should weigh before trusting the result.
    warnings: list[str] = Field(default_factory=list)
    # For the audit line only: kept out of the agent's answer and tool schema.
    actuals: SkipJsonSchema[QueryActuals | None] = Field(default=None, exclude=True)


class DialectCard(BaseModel):
    """Engine-quirks brief injected into SQL-generation prompts."""

    engine: str
    # The sqlglot dialect id used for parse-validation of generated SQL.
    sqlglot_dialect: str
    rules: list[str]
