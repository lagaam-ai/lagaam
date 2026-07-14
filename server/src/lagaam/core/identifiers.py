"""Identifier quoting: the one injection guard every adapter must use.

Agent-supplied names are interpolated into engine SQL (metadata statements
take no bind parameters), so quoting here is a security boundary, not a
convenience. Adapters never build their own identifier strings.
"""


def quote_identifier(part: str) -> str:
    """Return one identifier part, lowercased and double-quoted.

    Lowercasing mirrors how engines resolve unquoted identifiers, so agents
    may spell names in any case. Quoting makes reserved words and characters
    like ``$`` legal names instead of SQL syntax errors.

    Raises ValueError for parts that cannot be a single safe identifier
    (empty, or containing a double quote).
    """
    if not part or '"' in part:
        raise ValueError(f"not a valid identifier: {part!r}")
    return f'"{part.lower()}"'
