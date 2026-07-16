"""CostEstimate: the QUOTATION the budget gate (U5) judges against.

The estimate must degrade honestly — when the engine has no statistics,
confidence drops to "low" so the budget layer can fail safe rather than
wave a query through on a missing number.
"""

import pytest

from lagaam.core.cost import human_bytes
from lagaam.core.models import CostEstimate


def test_high_confidence_estimate_carries_numbers() -> None:
    est = CostEstimate(scanned_bytes=25_700_000, row_estimate=1_500_000)
    assert est.confidence == "high"
    assert est.scanned_bytes == 25_700_000
    assert est.row_estimate == 1_500_000


def test_missing_bytes_forces_low_confidence() -> None:
    # No statistics (Trino NaN) means we cannot vouch for the number.
    est = CostEstimate(scanned_bytes=None, row_estimate=None)
    assert est.confidence == "low"


def test_explicit_low_confidence_is_respected() -> None:
    # Adapter may know the number but distrust it (e.g. partial stats).
    est = CostEstimate(scanned_bytes=1024, row_estimate=10, confidence="low")
    assert est.confidence == "low"


def test_high_confidence_requires_a_byte_number() -> None:
    # You cannot claim high confidence with nothing to back it.
    with pytest.raises(ValueError):
        CostEstimate(scanned_bytes=None, row_estimate=5, confidence="high")


def test_summary_reads_as_agent_facing_prose() -> None:
    est = CostEstimate(scanned_bytes=48 * 1024**3, row_estimate=2_000_000)
    text = est.summary()
    assert "48" in text and "GB" in text
    assert "2,000,000" in text or "2000000" in text


def test_summary_flags_unknown_when_low_confidence() -> None:
    est = CostEstimate(scanned_bytes=None, row_estimate=None)
    text = est.summary().lower()
    assert "unknown" in text or "no estimate" in text


@pytest.mark.parametrize(
    "raw, expected",
    [
        (0, "0 B"),
        (512, "512 B"),
        (1024, "1.0 KB"),
        (25_700_000, "24.5 MB"),
        (48 * 1024**3, "48.0 GB"),
        (3 * 1024**4, "3.0 TB"),
    ],
)
def test_human_bytes_is_readable(raw: int, expected: str) -> None:
    assert human_bytes(raw) == expected
