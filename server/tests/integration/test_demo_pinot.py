"""The shipped Pinot demo, run as a test, so it cannot rot silently.

`examples/demo_pinot.py` is a recorded artefact: the GIF in the README is
rendered from one run of it. A demo nobody executes is a screenshot of a
claim, so the beats are importable and this module runs all three against
the live realtime instance and asserts the outcomes the README promises.
"""

import sys
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

# The demo lives in examples/, which is not a package and not on the path:
# it is a script users run, and it stays one.
_EXAMPLES = Path(__file__).resolve().parents[3] / "examples"
if str(_EXAMPLES) not in sys.path:
    sys.path.insert(0, str(_EXAMPLES))

from demo_pinot import run_beats  # noqa: E402


async def test_the_demo_still_shows_what_the_readme_says_it_shows(
    pinot_realtime_ready: None,
) -> None:
    """All three beats, one session, exactly as the recorded run made them."""
    fresh, blocked, admitted = await run_beats()

    # Beat 1: rows that landed seconds ago are priced and returned.
    assert not fresh.isError
    assert fresh.structuredContent is not None
    assert fresh.structuredContent["row_count"] > 0

    # Beat 2: the join with no proven key is denied on the row work, with
    # both halves of the teaching text the agent self-corrects on.
    assert blocked.isError
    text = " ".join(
        block.text for block in blocked.content if hasattr(block, "text")
    )
    assert "rows at its widest step" in text
    assert "LIMIT will not help" in text

    # Beat 3: the same join on the upsert twin clears the same ceiling.
    assert not admitted.isError
    assert admitted.structuredContent is not None
    assert admitted.structuredContent["row_count"] > 0
