# Track C C2.8 criteria owner-preflight totality

Status: author fixed-forward after an invalid fresh review run surfaced two
substantive findings; a new pinned independent review is required before
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

The first fresh review attempt after commit `254b7c9` was not valid formal
evidence because its durable artifact failed causal binding, despite a zero
reviewer process exit. Its substantive output was still treated as engineering
input and fixed forward:

- suggestion review now locks the suggestion, linked feedback, and every
  distinct direct/feedback/caller job reference, exact-owner checks them in the
  same PostgreSQL transaction as pattern + review writes, and returns the
  authorized frozen suggestion/feedback snapshot to the compiler;
- `job_id` and `baseline_job_id` are normalized independently before rerun
  precedence, so whitespace in the first alias cannot mask the second.

## Implementation boundary

The shared helper is renamed from rerun-specific
`_preflight_criteria_rerun_job_ownership` to
`_preflight_criteria_job_ownership`. Its three canonical callers remain:

1. `record_criteria_feedback` before feedback and suggestion writes;
2. `recompile_criteria` before criteria version/compiler writes;
3. `review_pattern_suggestion` after read-only suggestion/source discovery and
   again inside the locked PostgreSQL review UoW before pattern/review writes;
   compiler and rerun consume only the UoW's frozen authorized snapshot.

No schema, provider/model behavior, promotion, typed-CAS, CRM, daemon allowlist,
or live-provider gate changes are included.

## Regression contract

The fast owner-fencing lane covers `rerun_retrieval` missing/false crossed with
foreign/missing jobs for feedback, both explicit recompile id spellings, and
suggestion source jobs. Every cell requires the job read and prohibits all
criteria-domain writes. Positive coverage preserves same-owner, no-ref, and
open-mode behavior. It also covers whitespace alias precedence, distinct direct
and feedback suggestion sources, and a locked recheck owner miss before compiler
writes. API request-scope coverage repeats missing/false/true non-enumeration,
checks both explicit aliases independently, and verifies bearer-derived owner
propagation. PostgreSQL regressions prove dual-source and concurrent source-swap
owner misses leave pattern/review at zero writes, while whitespace direct source
plus same-owner feedback source applies from a frozen snapshot.

## Author validation

- stable fast owner/request-scope/transport + native-write adjacency:
  `78 passed + 61 subtests`;
- PostgreSQL owner-fencing adjacency: `12 passed`;
- `make lint`: `58 files already formatted`, all checks passed;
- `make typecheck`: expected nonzero, unchanged at `81 errors / 4 files`;
- five touched source modules `py_compile` and scoped `git diff --check`: clean;
- `make lint`: `58 files already formatted`, all checks passed;
- `make typecheck`: expected nonzero, unchanged at `81 errors / 4 files`;
- touched orchestrator `py_compile` and exact-file `git diff --check`: clean;
- no full `tests/test_pipeline.py`, provider/model/live, W6, nightly, or manual
  execution.

The implementation is the commit containing this document. A fresh pinned
non-author review is required; author evidence is not a formal review verdict.
