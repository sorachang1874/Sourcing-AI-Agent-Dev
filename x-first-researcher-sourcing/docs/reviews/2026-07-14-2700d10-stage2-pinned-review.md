# Pinned independent adversarial review — Stage 2A field capability

## Evidence header

- Verdict: `NO-GO`
- Reviewer: non-author, adversarial, read-only.
- Base: `8087465523bbfe0dbbc27d1f4442fa1f79ef441b`
- Reviewed: `2700d10833463e822ff5b7d80debd9991d1fa244`
- Immediate parent verified as the stated base.
- Reviewed tree: `c725834ce837ae68969f8b4ea9af197e221d1a14`
- Binary diff SHA-256: `1f3dc67422d289ec800df2db7ed932eca3b70b47f62c03857576da6fc56a65d3`
- Scope: 13 files, 6,408 insertions, 20 deletions.
- Detached clean worktree: `/private/tmp/x-first-stage2-review-2700d108`
- Fixture file SHA-256: `f369128352b0f831f531b6d14de9a1de626e46217d09bf0e1e65fae6bedb02af`
- Canonical registry/request/collection/expectation hash prefixes: `5dce4661`, `3e640e66`, `7a242b26`, `77989b3b`.
- No live call, provider/model invocation, network access, credentials, repository edit, staging, or commit occurred.

## Validation

- Python 3.12.13 focused Stage 2: 37/37 passed.
- Python 3.14.4 focused Stage 2: 37/37 passed.
- Python 3.12.13 full offline suite: 302/302 passed.
- Python 3.14.4 full offline suite: 302/302 passed.
- Deterministic generator check passed on both Python versions.
- Ruff 0.15.11 lint passed; three changed Python files were already formatted.
- Compileall passed on both Python versions.
- Six added JSON artifacts parsed successfully.
- `git diff --check` passed.
- Canonical fixture validated with zero errors.

These green checks do not override the blocking findings below. The reviewer reproduced the findings with coherent, fully rehashed adversarial mutations rather than ordinary malformed inputs.

## Findings

Severity totals: P0=0, P1=7, P2=2. All are `new`; no prior formal finding is being re-raised and no accepted residual is being promoted into this verdict.

### P1-1 — Metadata-only raw payloads have no closed shape

`_source_record_errors` validates only the source envelope, raw-record hash, visibility, kind, and replayability. It does not enforce the documented four-key metadata shape or exclude credentials, profile fields, Bios, live URLs, or private trace values.

A coherently rehashed metadata source containing synthetic `api_key`, platform IDs, handle, live-looking profile URL, and Bio produced a conformant bundle and all field states remained `unverified`. Fail-closed trust state was preserved, but privacy and data minimization were not. Exact raw schemas are required per record kind, and credential-like/private fields must be rejected before persistence.

### P1-2 — Quarantined raw records bypass deep safety validation

Field-state derivation treats any scalar profile URL as `present_exact`, while canonical reserved URL and exact raw-key validation only run when a normalized profile exists. Quarantined rows are forbidden from retaining normalized profiles, so their raw records bypass this validation.

A conflicting-ID source with `https://x.com/not_synthetic` remained bundle-conformant and reported `profile_url=present_exact`. Per-kind URL, raw-shape, full-body, and unsafe-field validation must run for every replayable source before state derivation, including quarantined and failed sources.

### P1-3 — Native-X provenance is self-labelled

`provider_path` is arbitrary bounded text. Receipt validation checks only an allowed `tool_name`; it does not bind provider transport, result type, provider path, and tool invocation.

After relabelling a completed source path to `generic_web_search.results[0]` and rebuilding all identities, the bundle remained conformant, the receipt still claimed `x_user_search`, and the profile remained `replay_bound_exact`. Free-text provenance must be replaced with a closed transport/result descriptor mechanically bound to the receipt and request tool policy.

### P1-4 — A valid Post-only result has no legal terminal state

A replayable Post source derives exact/bounded Post states, while absence of a profile derives `source_payload_unavailable`. Failed rows must simultaneously keep all 14 states `unverified`. A structurally valid Post-only mutation therefore cannot satisfy both rules.

An explicit Post-only terminal outcome is required. Derivation, normalization, incidents, expectations, and documentation must agree on whether proven Post fields remain source-bound under a typed non-profile outcome.

### P1-5 — Handle-to-stable-ID conflicts are not closed

The identity reducer detects only one platform ID under multiple handles. It has no inverse mapping for one simultaneous handle under multiple stable IDs.

A same-time mutation produced `fixture_a -> ID1` and `fixture_a -> ID3`; one row remained completed and the other was quarantined only as a rename. An observation-time-aware reverse owner must quarantine every affected mapping. Legitimate historical reassignment requires explicit handle-history intervals.

### P1-6 — Retention is not temporally bound to retained evidence

Retention validation checks timestamp syntax and a 24-hour delta but never relates creation or expiry to receipt, source, profile, or Post timestamps. The evaluator hardcodes `retention_contract_violation=0`.

A 2020 retention interval around evidence observed in 2026 remained conformant with zero violations. Retention creation and deletion must bound every retained observation, and the evaluator guardrail must be derived from those checks.

### P1-7 — Task and child identities collide across experiments

Task identity hashes only the task-local object; lab and frozen window enter only the experiment identity. Changing the target lab and window produced a separately valid experiment and collection while every task, receipt, source, profile, and Post ID remained byte-identical.

Target, window, and every interpretation-changing contract digest must enter task identity, or every child identity must be explicitly namespaced by an equivalent experiment scope.

### P2-1 — Python equality weakens JSON type strictness

Several nested schemas are only `{type: object}` and runtime comparisons use Python equality. As a result, booleans can satisfy integer counts and integer zero can satisfy expected `false` authority values.

Use deep typed schemas and explicit `type(value) is int` / `type(value) is bool` checks for counts, authority, and receipt facts.

### P2-2 — Exported profile-binding helper accepts stale source identity

`validate_profile_source_binding` validates only profile/raw comparisons. It does not verify `raw_record_sha256` or recompute `source_record_id`.

After changing the Bio and recomputing only its raw hash and normalized profile while leaving the source ID stale, the helper returned no errors. It must validate source identity itself or become private so callers use full collection validation.

## Verified non-findings

- External selection tuple and denominator bindings are closed.
- Scenario semantics are request-frozen and expectation-bound.
- Collection identity covers all material top-level fields except itself.
- Completed normalized profile/Post objects enforce reserved URLs, Bio hash, same-account author binding, and receipt timing.
- ID-to-multiple-handle conflict, multi-ID precedence, multiple-profile-source handling, Post-ID uniqueness, full-source consumption, and row/receipt/source closure are implemented.
- Authority, canonical-write, and outreach arrays fail closed.
- No target-lab literal is embedded in the validator; the remaining generalization problem is identity scope, not a hardcoded company.
- The canonical committed fixture itself contains synthetic handles and reserved `.invalid` URLs and makes no live/search-quality claim.

## Promotion boundary

Commit `2700d10833463e822ff5b7d80debd9991d1fa244` must not become the accepted Stage 2A capability contract, support a live canary, or receive milestone/manual signoff until the seven P1 findings are remediated and independently re-reviewed on a new pinned commit. Unrelated offline development is not blocked.

Even after remediation, approval of this slice would establish only deterministic offline fixture semantics. A live X transport, execution grant, provider provenance, real purge owner, costing envelope, and live-quality evaluation require their own reviewed contracts.

`NO-GO`
