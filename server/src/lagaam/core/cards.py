"""Table cards: compact prompt-ready grounding for one table.

Cards go inside LLM prompts (SQL generation lands in U3), so size is a
feature: one header line, one line per column, nothing else.
"""

from lagaam.core.models import TableSchema


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
