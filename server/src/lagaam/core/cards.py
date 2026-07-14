"""Cards: compact prompt-ready grounding text.

Cards go inside LLM prompts, so size is a feature: one header line, one
line per item, nothing else.
"""

from lagaam.core.models import DialectCard, TableSchema


def render_card(schema: TableSchema) -> str:
    header = schema.fqn
    if schema.row_estimate is not None:
        header += f" (~{schema.row_estimate:,} rows)"
    lines = [header]
    for column in schema.columns:
        line = f"- {column.name} {column.type}"
        if column.comment:
            line += f" — {column.comment}"
        lines.append(line)
    return "\n".join(lines)


def render_dialect_card(card: DialectCard) -> str:
    lines = [f"{card.engine} SQL dialect:"]
    lines += [f"- {rule}" for rule in card.rules]
    return "\n".join(lines)
