# Track C C2.8 criteria owner-preflight totality

Status: author implementation; fresh pinned independent review required before
live/manual/product/milestone signoff.

## Goal and root cause

C2.8 fixes the sole P1 finding from the pinned `4b2370a` review. The shared
criteria owner preflight was incorrectly gated by `rerun_retrieval`, allowing an
authenticated feedback, explicit recompile, or suggestion-review request with
the flag missing/false to cross the first criteria-domain write while referring
to a missing or foreign job.

Authorization and rerun policy are now separate contracts:

- every nonblank caller-supplied `job_id` and `baseline_job_id`, plus every
  suggestion-derived source job, is exact-owner checked before feedback,
  review, version, compiler, result, or derived-job writes;
- `rerun_retrieval` controls only the post-mutation rerun decision;
- missing and foreign jobs return the same `job_not_found` projection;
- requests with no job/source ref retain the criteria-only path;
- blank expected owner values retain open-mode operator compatibility for an
  existing referenced job;
- automatic baseline selection remains requester+tenant scoped and the selected
  owner is re-read before retrieval execution.

## Implementation boundary

The shared helper is renamed from rerun-specific
`_preflight_criteria_rerun_job_ownership` to
`_preflight_criteria_job_ownership`. Its three canonical callers remain:

1. `record_criteria_feedback` before feedback and suggestion writes;
2. `recompile_criteria` before criteria version/compiler writes;
3. `review_pattern_suggestion` after read-only suggestion/source discovery but
   before review, recompile, result, or derived-job writes.

No schema, provider/model behavior, promotion, typed-CAS, CRM, daemon allowlist,
or live-provider gate changes are included.

## Regression contract

The fast owner-fencing lane covers `rerun_retrieval` missing/false crossed with
foreign/missing jobs for feedback, both explicit recompile id spellings, and
suggestion source jobs. Every cell requires the job read and prohibits all
criteria-domain writes. Positive coverage preserves same-owner, no-ref, and
open-mode behavior. API request-scope coverage repeats missing/false
non-enumeration for all three public routes and verifies bearer-derived owner
propagation.

## Author validation

- fast owner/request-scope/transport lane: `74 passed + 61 subtests`;
- PostgreSQL owner-fencing adjacency: `9 passed`;
- `make lint`: `58 files already formatted`, all checks passed;
- `make typecheck`: expected nonzero, unchanged at `81 errors / 4 files`;
- touched orchestrator `py_compile` and exact-file `git diff --check`: clean;
- no full `tests/test_pipeline.py`, provider/model/live, W6, nightly, or manual
  execution.

The implementation is the commit containing this document. A fresh pinned
non-author review is required; author evidence is not a formal review verdict.
