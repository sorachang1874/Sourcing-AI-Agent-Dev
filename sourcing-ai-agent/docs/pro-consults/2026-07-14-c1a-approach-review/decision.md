# Decision

## Authority

ADVISORY_ONLY — not an independent-review artifact or formal GO.

## Local disposition

### Evidence classification

The Pro response is preserved as historical advisory input, but this bundle is **not Connector-grounded decision
evidence** under consultation contract v2. The response has no durable Connector diff citation and the request was sent
before the hash-bound redaction preflight existed. The local decision below therefore rests on the pinned commit,
targeted tests, contract lane, and non-author read-only audits—not on Connector authority.

### Decision

Normalized local disposition: `keep`.

- Keep C1a at commit `bc9037b7c3e962ed542031eab1d11c5a55a391e6`; no P0/P1 correction is required.
- Preserve the P2 `async_task_contract.py` unknown-to-running mismatch as explicit C1b debt. Do not silently expand C1a.
- Preserve the no-provider/no-model/no-live boundary.
- Preserve the formal independent-review gate as pending. Pro output cannot produce `GO`.

### Local evidence

- Combined targeted regression: `38 passed + 17 subtests`.
- Exact legacy classifier node: `1 passed`.
- Frontend TypeScript/Vite build: passed, 81 modules; existing bundle-size warning only.
- `make lint`: passed.
- Mypy ratchet: unchanged at `81 errors / 4 files`.
- Final contract lane: `349+2+11+1+2`, `dry_run_ready`.
- Two non-author read-only audits found the empty-terminal-timeline and history-error-race P1s before commit; both were
  fixed, and the final re-audit reported no remaining P0/P1.

### Deferred

- Backend/public task semantic convergence belongs to C1b.
- Durable repository, provider/model work, live validation, W6/manual validation, and milestone signoff remain outside
  this advisory artifact.

## Follow-up

Retain the response only as historical advisory input. Carry the P2 into C1b planning and obtain the repository's
canonical non-author review artifact before any live, W6, manual, product, or milestone signoff.
