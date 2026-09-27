"""Wall-clock bounds for the guards against runaway parsing work."""

import os

# Bounds are laptop seconds; CI sets this to how much slower its runners are.
_SLOWDOWN = float(os.environ.get("LAGAAM_TEST_SLOWDOWN", "1"))


def cpu_seconds(bound: float) -> float:
    return bound * _SLOWDOWN
