"""Identifier handling: the one injection guard every adapter must use, and
the one normalizer every authorization check must use.

Agent-supplied names are interpolated into engine SQL (metadata statements
take no bind parameters), so quoting here is a security boundary, not a
convenience. Adapters never build their own identifier strings.

Normalization is the other half of the boundary. An allowlist that compares a
lossy string projection of a parsed name — dropping parts, folding case a
quoted identifier preserves, or applying Python's Unicode folding where the
engine applies none — decides access for a different table than the one that
executes. Every comparison goes through ``table_fqn``.
"""

import unicodedata

from sqlglot import exp


class IdentifierError(ValueError):
    """A name that cannot be resolved to one comparable identifier."""


def _fold(part: exp.Identifier) -> str:
    """Normalize one name part the way the engine resolves it.

    Trino folds unquoted identifiers to lowercase and treats quoted ones as
    literal, so folding a quoted part would authorize a different table than
    the one that runs. Non-ASCII is rejected rather than folded: Python maps
    characters the engine does not (U+212A KELVIN SIGN lowercases to ``k``),
    so a folded match can name a different object than the rendered SQL.
    """
    text = part.name
    if not text:
        raise IdentifierError("empty identifier part")
    if not text.isascii():
        # NFKC first, so a name that is merely a compatibility spelling of an
        # ASCII one is reported as such instead of looking arbitrary.
        normalized = unicodedata.normalize("NFKC", text)
        hint = f" (did you mean {normalized!r}?)" if normalized.isascii() else ""
        raise IdentifierError(
            f"identifier {text!r} contains non-ASCII characters{hint}"
        )
    return text if part.quoted else text.lower()


def table_fqn(table: exp.Table) -> str:
    """Return the comparable ``catalog.schema.table`` name for one table node.

    Raises IdentifierError unless the node is exactly three resolvable parts.
    Trino accepts longer names for connectors with nested namespaces, and
    sqlglot maps those onto catalog/db/name lossily — the middle parts vanish
    from the comparison but survive into the rendered SQL.
    """
    parts = table.parts
    if len(parts) != 3:
        raise IdentifierError(
            f"expected catalog.schema.table, got {len(parts)} name parts"
        )
    folded: list[str] = []
    for part in parts:
        if not isinstance(part, exp.Identifier):
            raise IdentifierError("name parts must be plain identifiers")
        folded.append(_fold(part))
    return ".".join(folded)


def normalize_grant(grant: str) -> str:
    """Normalize one allowlist entry the same way ``table_fqn`` normalizes SQL.

    Grants are written unquoted, so every part folds to lowercase; a grant that
    is not three ASCII parts can never match and is rejected at startup rather
    than silently never matching.
    """
    parts = grant.split(".")
    if len(parts) != 3 or not all(parts):
        raise IdentifierError(
            f"grant {grant!r} must be catalog.schema.table"
        )
    for part in parts:
        if not part.isascii():
            raise IdentifierError(
                f"grant {grant!r} contains non-ASCII characters"
            )
    return grant.lower()


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
