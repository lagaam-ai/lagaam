# Plan — PR #38 Astra round 1 fixes (2026-09-21)

Review: `/private/tmp/lagaam-pr38-review/review/REVIEW.md`; its tests
`review/test_adversarial.py` (6 failing on HEAD, 9 passing). All four
findings verified by the driver. Fix all four; port the reviewer's scenarios
as regression tests in our own style (fixtures/fake client, names in house
style), keep the 9 controls passing too.

## F1 (P2) — a non-dict body counts as present, so a missing sealed segment quotes free at high

`missing_sealed_segments` counts a *key* as present; `segment_facts` skips a
non-dict body. `{name: null}` (or a list, a string, an int) merged in makes
the guard say complete while the facts omit the segment → confident
under-quote. Measured by the reviewer with the PR's own u14multi fixtures:
10 sealed / 17,692 bytes / 1,200 rows / denied under a 1,000-row budget
becomes 7 sealed / 13,144 / 900 / **allowed**. Verified by the driver that
the same hole exists on main's `metadata_is_complete` (`{'b': None}` → True,
facts → `['a']`); the fan-out just made it reachable.

Fix in `metadata.py`: a segment is present only when its entry is a dict
(the shape `segment_facts` can read). Non-dict bodies are missing — and
therefore fetched by the fan-out, which is the right outcome. Unit tests:
None / list / str / int bodies for a sealed name → in `missing_sealed_segments`,
`metadata_is_complete` False; and the reviewer's end-to-end shape: truncated
bulk + per-server answer where one missing name maps to null → `complete`
False, quote low.

## F2 (P2) — every name for a server in one GET URL

Measured by the driver on the live controller (repeating a real 30-char
segment name): URL length 8,059 → 200, 8,099 → 400 (empty body); the
reviewer saw 20 KB and 60 KB → 400 too, and 2,000 distinct names → httpx
`InvalidURL: URL component 'query' too long` raised *before* any request,
which is not a `PinotTransportError` and escapes the fallback as an
internal error.

Fix in `engine.py`: batch each server's names so the encoded request line
stays under `_MAX_METADATA_URL_BYTES = 6144` (path + query, computed with
the same encoding httpx will use; 8,059 measured OK, this is the margin).
A name that alone exceeds the cap → no fan-out (low). Replace the
server-count cap with a total-request cap `_MAX_METADATA_REQUESTS = 64`
across all servers and batches — count the batches before making any call;
over the cap → no fan-out (low). Catch `httpx.InvalidURL` alongside
`PinotTransportError` as defence in depth (should be unreachable with the
batching; test that batching makes it unreachable for 2,000 names: the
fake client must see ≤ 64 requests each under the byte cap, and the quote
is high when every batch answers). Merge only once every batch answered
(a later failure discards the partial answers — the reviewer's control
already checks this; keep it).

## F3 (P2) — `/segments/{t}/servers` runs before the deadline

Move the discovery GET inside the `anyio.fail_after` scope and the same
`except`. Regression: fake client delays `/servers` by more than the
budget → returns the bulk response within the budget, `complete` False.
Keep `PinotForbidden` propagating as before (reviewer's control).

## F4 (P3) — the ADR and docstring misstate routing

Pinot 1.5.1 `TableMetadataReader.getSegmentsMetadataInternal`
(`/private/tmp/lagaam-pr38-review/review/TableMetadataReader.java` lines
~195–225): the controller takes the server→segments map itself, sends
**our `segments` filter to every server hosting the table**
(`buildTableLevelUrls`), aggregates, and on a RuntimeException falls back
to per-segment URLs. So the adapter never addresses a server; grouping by
holder is what makes the *aggregated* answer complete (measured: a filter
naming one server's segments comes back whole; a mixed one comes back as
one server's subset). Backend cost per adapter GET is one server request
per server hosting the table, not one. Rewrite ADR 0011's Decision points
2–3 and the Consequences cost line, and the `_complete_segment_metadata`
docstring, to say exactly that: what is measured, what the source says,
and that `1 + requests` is the client-to-controller count only. Do not
claim anything about backend cost that is not in the source.

## Verify

`uv run pytest -q` (expect ≥ 1104 + new), `uv run mypy`, the reviewer's
suite from `/private/tmp/lagaam-pr38-review`:
`PYTHONPATH=<worktree>/server/src:<worktree>/server <worktree>/server/.venv/bin/python -m pytest review/test_adversarial.py -q`
→ 15 passed; `uv run pytest -q -m integration` (193 + any new); live
`estimate_cost` for u14multi/u14rep/u12plain unchanged from the PR
description (4548/300, 6064/400, 1582/400, all high).

Commits (each ending `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`):
1. `fix(pinot): a segment whose metadata is unreadable is missing, not present`
2. `fix(pinot): batch the per-server metadata asks by URL size, under one deadline with discovery`
3. `docs(adr): 0011 says what the controller does with the filter`
Do not push, do not open a PR, do not touch containers.
