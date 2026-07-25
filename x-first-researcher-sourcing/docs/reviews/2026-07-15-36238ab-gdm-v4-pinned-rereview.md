# Google DeepMind wave2-v4 discovery-only pinned rereview

## Evidence header

- Reviewer: independent non-author subagent `/root/gdm_v4_fix_pinned_rereview`; adversarial rereview against pinned Git
  objects only.
- Reviewed head: `36238ab36b59ca5fea52e5d926b6cb2c9b139c56`.
- First parent: `d7cb306c1b071aed896e89b832da5e8e6a944c85`.
- Reviewed scope: exactly the eight `x-first-researcher-sourcing/` files in the parent-to-head diff (`730 insertions`,
  `40 deletions`), with binary-diff SHA-256
  `a570bbbd3851b43218cf5a66c31689122519b479e9eac7d32858f1d36d0f4a3f`.
- Prior finding source: `docs/reviews/2026-07-15-bbe36b8-gdm-v4-pinned-review.md`; this rereview specifically
  re-tested P1-1, P1-2, and P2-1 rather than relying on the author summary.
- Reproduction environment: detached clean worktree `/private/tmp/xfirst-gdm-v4-rereview.K7FMle` at the exact full
  SHA. The ambient working tree and any later remediation were not read as review evidence.
- No Grok/X/model/network call, real OAuth or credential read, real grant issuance, provider execution, repository
  promotion, staging, commit, or product write occurred. All mutation probes used synthetic binaries, auth bytes,
  grants, session transcripts, and results in automatically deleted temporary directories. This artifact is the only
  shared-working-tree write.

## Pinned validation

- `PYTHONPATH=src .../.venv/bin/python -m unittest tests.test_adaptive_grok_wave_runner -v` — exit `0`, `62/62`
  tests passed in `11.528s`.
- `PYTHONPATH=src .../.venv/bin/python -m unittest discover -s tests -v` — exit `0`, `351/351` tests passed in
  `35.979s`.
- `PYTHONPATH=src .../.venv/bin/python -m x_first.contracts` — exit `0`, status `valid`, precision `1.0`, recall
  `1.0`, predicted/relevant `20/20`, false merges `0`.
- `.../.venv/bin/ruff check src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py` — exit
  `0`, `All checks passed!`.
- The tracked v4 prompt hashes to
  `9b9bf931bb3cc27d12c10eda65a98b5d070ef136a4d7d8735dd61a48580c85f2`, exactly matching its selected effective-
  prompt entry. The entry's session-query semantic digest is
  `db36a3566b424388ba968b566f7f020eb5ba23ae1dacbfb310bdb903fba0c7a2`, exactly matching the runtime recomputation.

Severity totals: `P0=0`, `P1=2`, `P2=1`.

## Findings

### P1-1 `[residual]` — discovery-only person hydration still passes through punctuation and non-ASCII token gaps

The selected-entry semantic binding itself is now real: the ID plus semantic digest are part of the effective-prompt
entry and therefore flow through the existing grant, intent, receipt, and replay binding. The parser also rejects
ordinary positive/negated `from:`, an unquoted bare handle, and the tested ASCII exact-name/user-search forms.

The bound implementation, however, does not enforce the semantic it claims. A handle-like query is rejected only when
the complete stripped string matches `@?[A-Za-z0-9_]{1,15}`; quotes, parentheses, or terminal punctuation make the
same single lexical subject pass (`src/x_first/adaptive_grok_wave_runner.py:3582-3589`). A pinned fake-live probe
changed the sole keyword call to `{"query":"\"TargetPerson\"","limit":"100","mode":"Latest"}`. It sealed
`receipt.status=completed`, recorded `session_proof.status=verified`, published the expected partial projection, and
replayed with `[]`. Direct probes likewise admitted `(@TargetPerson)` and `TargetPerson.` in keyword/semantic query
positions. These are ordinary exact-person hydration spellings, not broad discovery queries.

The `x_user_search` closed grammar has a second fail-open: it extracts only ASCII `[a-z0-9]+` tokens and never proves
that those tokens consume the complete query (`:3592-3607`). Consequently `李飞飞 Google DeepMind researcher` and
`デミス Google DeepMind researcher` both pass the base argument policy and this discovery-only policy because the
person-name characters disappear before the allowlist comparison. This directly contradicts the contract claim that
every token belongs to the closed grammar (`docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md:88-91`) and leaves a realistic
multiword-person user-search path in the target domain.

Required remediation: use one versioned, full-consumption query lexer after Unicode normalization. Reject a quoted or
punctuation-wrapped single handle/name token in keyword/semantic/user query positions, and fail closed on every
unconsumed Unicode scalar or token in the closed user-search grammar. Preserve typed `post_id`/URL thread expansion.
Add end-to-end fake-live regressions for quoted bare handles and a non-ASCII person name plus otherwise allowed lab/
professional terms.

### P1-2 `[residual]` — an already-partial model result can retain a false discovery-convergence status reason

The hydration-status conflict is substantially improved: discovery-only projection skips the per-handle Post/Reply
gate, forces `X_SEARCH_OK` to `X_SEARCH_PARTIAL`, and appends the operator-owned convergence-unproven limitation.
However, it replaces `status_reason` only inside the `X_SEARCH_OK` branch
(`src/x_first/adaptive_grok_wave_runner.py:1502-1513`). An input already labelled `X_SEARCH_PARTIAL` keeps arbitrary
model wording.

A pinned projection probe supplied a valid discovery-only result with
`status=X_SEARCH_PARTIAL` and `status_reason="Discovery converged after three zero-yield expansions."`. The sanitized
result kept that exact reason, appended the contradictory operator limitation saying convergence had not been
mechanically proven, and passed `validate_model_result(..., live_mode=True)` with `[]`. Thus one operator-owned artifact
simultaneously says the population converged and that convergence is unproven, contrary to the documented guarantee
that the projector prohibits a `discovery_converged` claim
(`docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md:93-101`).

Required remediation: for every nonblocked discovery-only `X_SEARCH_OK|X_SEARCH_PARTIAL` result, set both the terminal
status and `status_reason` to the operator-owned partial/unproven values. Preserve the raw model reason only in raw
diagnostics if needed. Add a paired regression whose model input starts partial and explicitly claims convergence.

### P2-1 `[residual]` — transcript-recovery technical-limit kind is not recomputed on bundle replay

The shared post-transform serializer closes the original direct-normalization overflow: direct normalization and phase
projection now reapply byte/depth/node ceilings, suppress oversized `sanitized.json`, and produce a technical terminal.
Crash recovery also records a direct transform's exact limit kind. The remaining replay path is asymmetric.

Bundle replay calls transcript-terminal recovery only when the recorded receipt status is `completed`
(`src/x_first/adaptive_grok_wave_runner.py:5964-5976`). A legitimate terminal transform that crosses a ceiling is
recorded as `technical_limit_exceeded`, so replay skips the operation that derived its limit. It then compares the
recorded kind only if replay independently found a non-null kind (`:6001-6006`).

A pinned fake-live probe used concatenated progress plus terminal assistant JSON, with the raw terminal at exactly
`10,000` nodes and relationship normalization adding two nodes. Execution correctly sealed
`technical_limit_exceeded/json_structure`, published no `sanitized.json`, and replayed with `[]`. Replacing only the
receipt's limit kind with `json_bytes` still made both `validate_operator_receipt` and full bundle replay return `[]`.
The exact technical cause is therefore receipt-authored rather than replay-owned, despite the contract statement that
replay derives the same post-transform outcome (`docs/ADAPTIVE_GROK_WAVE_RUNNER_CONTRACT.md:103-108`).

Required remediation: replay transcript-terminal recovery for the terminal states that can truthfully result from that
operation, including `technical_limit_exceeded`, while preserving the explicitly versioned compatibility exception for
old rejected bundles. Require equality in both directions between recorded and recomputed technical-limit kind. Add an
end-to-end concatenated-terminal boundary regression and a forged-kind receipt mutation.

## Closure ledger and verified non-findings

| Prior finding | Rereview state | Evidence |
|---|---|---|
| P1-1 phase/query/convergence ownership | `residual` | Semantic digest and normal mutations are bound, but quoted/punctuated bare subjects and non-ASCII person-name user searches still seal verified sessions; convergence wording remains separately inconsistent in P1-2 |
| P1-2 phase-neutral hydration downgrade | `residual` | Discovery-only correctly skips the hydration surface gate and remains partial, but an input already partial retains a false convergence reason |
| P2-1 post-transform technical envelope | `residual` | Direct normalization/projection and crash-recovery limit publication are bounded; transcript-recovery limit kind is not replay-derived |

- A forged `session_query_policy_sha256` fails policy loading, while the reviewed production entry's prompt and semantic
  hashes match. Ordinary `from:`, bare ASCII user, exact ASCII name, and disguised ASCII name plus extra user-search
  tokens fail the new parser as intended.
- A discovery-only `X_SEARCH_OK` model result is projected to partial with the operator reason; the hydration-specific
  authored Post/Reply limitation is absent. `X_SEARCH_BLOCKED` remains blocked and does not receive a false partial
  readiness claim.
- The operator serializer rejects exact byte, node, and depth expansion at direct normalization and projection sites;
  the generated direct-limit bundles self-replay and never publish an over-ceiling sanitized artifact. P2-1 is limited
  to the transcript-terminal-recovery replay branch and exact cause integrity.
- Raw provider output remains unchanged; the mechanical `self` to `third_party` normalization is monotonic and does not
  upgrade support, evidence, state, or confidence. Existing result-v3/v2 compatibility lanes remained green in the
  full suite.
- Strategy-matrix completion, per-query yield, zero-yield sequences, and population convergence are now truthfully
  documented as unproven from this transport. No canonical identity, product, CRM, ranking, export, outreach, provider
  fallback, or protected-identity authority is added.

## Gate consequence

The two P1 residuals permit person-scoped hydration and a convergence claim inside an ostensibly discovery-only,
operator-owned artifact. The P2 residual leaves the exact technical-limit cause forgeable for transcript-recovered
terminal results. They block the provider-costing Google DeepMind wave2-v4 live execution, performance/convergence
claims, and milestone promotion at this pinned commit. Fixes require a new pinned commit, targeted/full validation, and
a fresh non-author rereview. Unrelated fixture-only work remains outside this verdict.

NO-GO
