"""Identifier quoting: the shared injection guard every adapter uses."""

import pytest

from lagaam.core.identifiers import quote_identifier


def test_quotes_and_lowercases() -> None:
    # Unquoted SQL identifiers resolve lowercase; agents type freely.
    assert quote_identifier("ORDERS") == '"orders"'
    assert quote_identifier("orders") == '"orders"'


def test_reserved_words_and_dollar_names_become_plain_identifiers() -> None:
    # Quoted, these are valid names instead of SQL syntax errors.
    assert quote_identifier("order") == '"order"'
    assert quote_identifier("orders$partitions") == '"orders$partitions"'


@pytest.mark.parametrize("bad", ["", 'tab"le', 'x" --'])
def test_rejects_empty_and_quote_bearing_parts(bad: str) -> None:
    with pytest.raises(ValueError):
        quote_identifier(bad)
