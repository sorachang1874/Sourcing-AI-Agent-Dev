# Decision

> Status: Reference (consult transcript, 2026-07-14 era). Decisions were promoted into their owning docs; do not treat dated numbers here as current.

## Authority

ADVISORY_ONLY — not an independent-review artifact or formal GO.

## Local disposition

P0 disposition: accepted — use one process-local owner lifecycle lock so consumer attachment and owner retirement are one atomic transition; keep durable ownership out of C1b.
P1 disposition: accepted — generation-fence frontend history publication, while recording review and criteria side effects as an unresolved D-C1-3/C1d compute/publish blocker.
P2 disposition: accepted — require server-authored provenance for authenticated history replacement, reject empty artifact handles, and retain the expanded callable-provenance source ratchet.
Validation: request preflight passed; the fixed-forward C1b contract, history, identity, and async-task set passed 93 tests, Ruff and Python compilation passed, and the author contract lane recorded 349+2+11+1+2 plus dry_run_ready with mypy unchanged at 81 errors / 4 files.

## Follow-up

Keep C1c-e, durable/live/W6/manual/product signoff, and the D-C1-3 publication boundary blocked pending owner decisions and scoped independent review; continue unrelated D0a and offline X-first work asynchronously.
