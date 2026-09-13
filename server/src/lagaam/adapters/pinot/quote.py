"""Table facts to a CostEstimate. PURE: no I/O, and it never guesses.

Pinot reports no bytes and no trustworthy rows before execution, so the
quotation is synthesised here from static segment metadata plus one
engine-authoritative number: how many segments survive the predicate.

The oracle says how many survive, not which. Charging the k largest is
therefore an upper bound on any k that could survive — and docs and bytes
take their own k largest, because the segment with the most rows is not
always the one with the most bytes.

A number that cannot be bounded is None, which the budget gate denies. That
is the whole contract: this module never returns a figure it cannot defend.
"""

from collections.abc import Iterable, Sequence
from typing import Any

from lagaam.adapters.pinot.metadata import TableFacts
from lagaam.core.models import CostEstimate


def surviving_docs(facts: TableFacts, surviving: int | None) -> int | None:
    """Docs in the k largest segments by docs, or None if any is unknown.

    One unknown segment poisons the sum: the others do not bound it.
    """
    counts = [segment.docs for segment in facts.segments]
    if not counts or any(count is None for count in counts):
        return None
    known = sorted((count for count in counts if count is not None), reverse=True)
    return sum(known[: _k(surviving, len(known))])


def surviving_bytes(
    facts: TableFacts, surviving: int | None, columns: frozenset[str] | None
) -> int | None:
    """Bytes in the k largest segments, charging only referenced columns.

    A segment matching none of the referenced columns is charged whole,
    and unresolvable columns fall back to whole segments table-wide.
    """
    sizes = _column_sizes(facts, columns)
    if sizes is None:
        sizes = [segment.total_bytes for segment in facts.segments]
    if not sizes or any(size is None for size in sizes):
        return None
    known = sorted((size for size in sizes if size is not None), reverse=True)
    return sum(known[: _k(surviving, len(known))])


def quote(
    tables: Sequence[tuple[TableFacts, int | None]],
    columns: frozenset[str] | None,
    max_intermediate_rows: int | None,
) -> CostEstimate:
    """One CostEstimate over every table the query reads.

    Rows and bytes each fail independently, and both fail whole: one table
    nobody could size makes the sum a bound on nothing.
    """
    if not tables:
        return CostEstimate(
            max_intermediate_rows=max_intermediate_rows, confidence="low"
        )
    # A consuming segment reports zero docs and -1 bytes, so a REALTIME half
    # is an unbounded unknown until U12 charges it at its flush threshold.
    realtime = any("REALTIME" in facts.types for facts, _ in tables)
    rows = _total(surviving_docs(facts, k) for facts, k in tables)
    total_bytes = (
        None
        if realtime
        else _total(surviving_bytes(facts, k, columns) for facts, k in tables)
    )
    return CostEstimate(
        scanned_bytes=total_bytes,
        row_estimate=rows,
        max_intermediate_rows=max_intermediate_rows,
        confidence="low" if total_bytes is None else "high",
    )


def _k(surviving: int | None, available: int) -> int:
    """How many segments to charge: all of them unless the oracle said fewer."""
    if surviving is None or surviving >= available:
        return available
    # The oracle has been seen to prune every segment; something is always read.
    return max(1, surviving)


def _column_sizes(
    facts: TableFacts, columns: frozenset[str] | None
) -> list[int | None] | None:
    """Per-segment bytes for the referenced columns; None only if columns is None.

    A segment whose columns match none of `columns` falls back to its own
    total_bytes, so the fallback is decided per segment, not per table.
    """
    if columns is None:
        return None
    sizes: list[int | None] = []
    for segment in facts.segments:
        total = 0
        matched = False
        for name, size in segment.column_bytes.items():
            if name.lower() in columns:
                total += size
                matched = True
        sizes.append(total if matched else segment.total_bytes)
    return sizes


def _total(values: Iterable[int | None]) -> int | None:
    """Sum, unless any part is unknown — then the whole sum is unknown."""
    total = 0
    for value in values:
        if value is None:
            return None
        total += value
    return total


# Only the counters measured to nest with numSegmentsQueried on 1.5.1. They
# are not additive: the server-side total and its by-value / by-limit
# breakdowns all appear at once, so the largest is read, not the sum.
_PRUNED_COUNTERS = (
    "numSegmentsPrunedByServer",
    "numSegmentsPrunedByValue",
    "numSegmentsPrunedByLimit",
)


def surviving_segments(explain_json: Any) -> int | None:
    """How many segments survive the predicate, from a single-stage EXPLAIN.

    Measured on 1.5.1, the pruning counters nest rather than add: a time
    filter reported ByServer 28 with ByValue 28 of 31 segments, so summing
    would claim 56 pruned and quote a negative scan. The largest single
    counter is exact where they nest and conservative where they do not.

    Only ByServer, ByValue and ByLimit are read: those are the three the
    time filter, the limit prune and the bare scan actually measured
    nesting against. numSegmentsPrunedByBroker and numSegmentsPrunedInvalid
    are excluded — every fixture reports them 0, so neither is known to
    behave as a breakdown of numSegmentsQueried, and if a broker reports
    numSegmentsQueried already net of its own pruning, subtracting
    ByBroker again would under-count survivors (e.g. queried 10, ByBroker
    21 quotes 1 where 10 are actually read). Not reading a counter can
    only leave more segments charged, never fewer, which keeps this the
    fail-safe side. Re-add either only with a fixture from a table that
    actually trips it.

    None means "no oracle" — the caller then charges every segment.
    """
    if not isinstance(explain_json, dict):
        return None
    if explain_json.get("exceptions"):
        return None
    # EXPLAIN must plan without running; anything scanned means we misread it.
    scanned = explain_json.get("numDocsScanned")
    if isinstance(scanned, bool) or not isinstance(scanned, int) or scanned != 0:
        return None
    queried = explain_json.get("numSegmentsQueried")
    if isinstance(queried, bool) or not isinstance(queried, int) or queried <= 0:
        return None
    pruned = 0
    for key in _PRUNED_COUNTERS:
        value = explain_json.get(key)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            continue
        pruned = max(pruned, value)
    return max(1, queried - min(pruned, queried))
