"""finite_number: the guard between Trino's text-shaped numbers and round().

Trino renders unknowns as "NaN" and unbounded costs as "Infinity". Letting
either through would crash round() or forge a huge high-confidence quote.
"""

import pytest

from lagaam.adapters.trino.numbers import finite_number


@pytest.mark.parametrize(
    "value, expected",
    [
        (1.5, 1.5),
        (0, 0.0),
        (10, 10.0),
        ("1.5", 1.5),
        ("  3 ", 3.0),
        ("1e6", 1_000_000.0),
    ],
)
def test_finite_values_pass_through(value: object, expected: float) -> None:
    assert finite_number(value) == expected


@pytest.mark.parametrize(
    "value",
    [
        "NaN",
        "Infinity",
        "-Infinity",
        float("nan"),
        float("inf"),
        -1.0,
        "-2",
        True,  # a bool is an int in Python; it is never a byte count
        False,
        None,
        "junk",
        "0x10",
        [1],
        {"a": 1},
    ],
)
def test_unusable_values_are_none(value: object) -> None:
    assert finite_number(value) is None


def test_extremely_large_integers_that_overflow_float_are_none() -> None:
    # A JSON integer like 10**400 can be parsed by json.loads but raises
    # OverflowError when converted to float. This must be caught and return None.
    assert finite_number(10**400) is None
    assert finite_number("10" + "0" * 400) is None
