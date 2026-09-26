# Plan — PR #38 round 3: a short answer is retried, not believed (2026-09-21)

Measurements §5 of `docs/superpowers/specs/2026-09-21-pinot-multiserver-measurements.md`
(already appended). The implementer of round 2 saw `u14rep` quote `low`
about 1 run in 10 (7/40 on the original HEAD, 3/40 after round 2), and the
integration test flaked with it. Cause, measured: the controller's answer
to a filtered ask is one server's response, and occasionally that server is
one holding none of the names, so the answer is `{}`. An identical retry
is an independent trial and fixed it 1/1; single-name asks are empty ~20%.

## The rule

In `_complete_segment_metadata`, after each batch ask: if the answer lacks
any of the names asked, ask again for **exactly the names still missing**,
up to `_METADATA_RETRIES = 2` more times per batch. Every retry counts
against `_MAX_METADATA_REQUESTS` (count the worst case before the first
call: `len(batches) * (1 + _METADATA_RETRIES) <= _MAX_METADATA_REQUESTS`,
else no fan-out — pin the new constant by value in the caps test) and
runs under the same deadline. A batch still short after its retries: stop,
keep the bulk response (low), exactly as an unreadable answer does today.
Merge only when every batch is whole.

Engine tests (fake client): a batch answered `{}` then whole on retry →
one extra request whose `segments` param is exactly the missing names,
quote high; answered short three times → three requests for that batch,
then no further batches asked, `complete` False; retries counted toward the
request cap (a fan-out whose `batches * 3` exceeds 64 makes no call). Keep
the reviewer's 15 adversarial tests passing unmodified
(`/private/tmp/lagaam-pr38-review/review/test_adversarial.py`, run as in
round 2).

Integration: run the u14rep/u14multi quote tests 20 times each
(`--count` is not installed; loop `uv run pytest -q -m integration -k "u14" ` 20×
in shell and count failures): 0 failures expected. Also re-run the live
`estimate_cost` lines 40× per table and report the low count before
(current HEAD) and after.

Docs: ADR 0011 — Context: replace "answered ... every time" with §5's
numbers; Decision: add the retry rule and its bound; Consequences: the
residual (<1% at the worst measured rate) stays `low`, and the mechanism
inside the controller is unestablished. Docstring of
`_complete_segment_metadata` likewise (one short paragraph). Commit the two
plan files and the measurements append in the docs commit.

Commits (each ending `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`):
1. `fix(pinot): a short per-server answer is asked again, twice, before the quote stays low`
2. `docs(adr): 0011 — the answer is one server's, so a short one is retried`
Do not push, no PR, no containers.
