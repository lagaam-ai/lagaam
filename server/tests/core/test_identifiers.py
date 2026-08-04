"""Identifier handling: the shared injection guard every adapter uses, and
the normalizer every authorization comparison goes through."""

import pytest
import sqlglot
from sqlglot import exp

from lagaam.core.identifiers import (
    IdentifierError,
    normalize_grant,
    quote_identifier,
    table_fqn,
)


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


# --- normalization: the one comparison authorization depends on -----------


def _fqn(sql: str) -> str:
    tree = sqlglot.parse_one(sql, dialect="trino")
    table = next(iter(tree.find_all(exp.Table)))
    return table_fqn(table)


def test_three_part_name_folds_to_lowercase() -> None:
    assert _fqn("SELECT x FROM TPCH.Tiny.ORDERS") == "tpch.tiny.orders"


def test_quoted_parts_fold_too() -> None:
    # Trino lowercases identifiers in table positions whether quoted or not,
    # and cannot hold two tables differing only by case.
    assert _fqn('SELECT x FROM "TPCH"."TINY"."ORDERS"') == "tpch.tiny.orders"
    assert _fqn('SELECT x FROM tpch.tiny."Orders"') == "tpch.tiny.orders"


def test_two_part_name_is_rejected() -> None:
    # Session-catalog resolution is not visible here, so it cannot be checked.
    with pytest.raises(IdentifierError, match="2 name parts"):
        _fqn("SELECT x FROM tiny.orders")


def test_four_part_name_is_rejected() -> None:
    with pytest.raises(IdentifierError, match="4 name parts"):
        _fqn("SELECT x FROM tpch.tiny.secret.orders")


def test_non_ascii_part_names_its_codepoint() -> None:
    # The NFKC suggestion alone renders identically to the input, so the
    # message has to say which character is the problem.
    with pytest.raises(IdentifierError) as caught:
        _fqn("SELECT x FROM tpch.tiny.orderK")
    assert "U+212A" in str(caught.value)
    assert "KELVIN SIGN" in str(caught.value)


def test_empty_quoted_part_is_rejected() -> None:
    with pytest.raises(IdentifierError, match="empty"):
        _fqn('SELECT x FROM tpch.tiny.""')


def test_normalize_grant_folds_and_validates() -> None:
    assert normalize_grant("TPCH.Tiny.ORDERS") == "tpch.tiny.orders"
    for bad in ("tiny.orders", "a.b.c.d", "a..c", "tpch.tiny.orderK"):
        with pytest.raises(IdentifierError):
            normalize_grant(bad)
