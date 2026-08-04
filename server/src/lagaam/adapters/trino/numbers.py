"""Numeric guards for values Trino reports as text.

Trino renders an unknown estimate as the JSON string "NaN" and an unbounded
one as "Infinity", and SHOW STATS reports counts as DOUBLEs that can be NaN.
Both would crash round() or forge a huge high-confidence number, so every
number crossing out of the adapter goes through here first.
"""

import math
from typing import Any


def finite_number(value: Any) -> float | None:
    """A finite, non-negative float, or None for NaN/Infinity/missing/junk.

    Infinity must be rejected too: Trino renders an unbounded cost as
    "Infinity", and letting it through would either crash round() or forge a
    huge high-confidence number. A negative size is nonsense — also None.
    """
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        f = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return f if math.isfinite(f) and f >= 0 else None


