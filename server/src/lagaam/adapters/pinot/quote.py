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

    A segment is priced per column only when it carries every one of the
    table's referenced columns. One matching column used to be enough, which
    is how a segment answering `league` alone was quoted 36,739 bytes while
    `playerID` added 542,226 more: a partial answer looks exactly like a
    cheap one. Missing any of them falls back to the segment's own
    total_bytes, decided per segment rather than per table.

    `facts.columns` empty means no referenced column was resolved to this
    table — no schema, or none of them belongs here — and then a match is
    all there is to go on, as before.
    """
    if columns is None:
        return None
    sizes: list[int | None] = []
    for segment in facts.segments:
        carried = {name.lower() for name in segment.column_bytes}
        if facts.columns and not facts.columns <= carried:
            sizes.append(segment.total_bytes)
            continue
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
