# OpenAI zero-prior discovery v5 remediation pinned rereview

## Evidence header

- Reviewer: independent non-author subagent
  `/root/oauth_lifecycle_d2_precommit_audit/auth_taint_concurrency_audit`.
- Reviewed head: `a86513fdb1147bb9af303829fdf07cd9398ccdd5`.
- Reviewed base and literal first parent: `502155ba51bbc9a4454b5a26d7f469b7e3ad0c09`.
- The base is the parent D2 commit `fix(x-first): bind Grok OAuth lifecycle`; this review therefore tested the v5
  remediation on top of, rather than instead of, D2.
- Controlling findings artifact:
  `docs/reviews/2026-07-16-66a894a-openai-v5-transfer-pinned-review.md`.
- Reviewed scope: exactly seven X-First files, `379 insertions`, `65 deletions`: the effective-prompt policy registry
  and schema, adaptive-runner contract, OpenAI transfer note and prompt, adaptive runner, and runner tests.
- Exact binary-diff SHA-256:
  `3e1354a8fdcd761e396fe8839eeedfde7942a440027814ffbf727f9dd85cd627`.
- Reproduction tree: clean detached worktree
  `/private/tmp/x-first-openai-v5-rereview-a86513f` at the reviewed head. Review reads, tests, and probes imported only
  that pinned source. The ambient working tree was not a source input.
- No network, Grok/X, model, connector, live provider, OAuth, API key, credential file, grant issuance, product write,
  push, or live execution was used. Synthetic adversarial inputs lived only in a temporary owner-only directory. Two
  existing private operator bundles and their approval metadata were read only for offline replay. This artifact is
  the reviewer's only repository write.

## Exact validation evidence

1. Object, ancestry, scope, and digest

   ```text
   git rev-parse HEAD
   # a86513fdb1147bb9af303829fdf07cd9398ccdd5

   git rev-parse HEAD^
   # 502155ba51bbc9a4454b5a26d7f469b7e3ad0c09

   git diff --binary 502155ba51bbc9a4454b5a26d7f469b7e3ad0c09 \
     a86513fdb1147bb9af303829fdf07cd9398ccdd5 | shasum -a 256
   # 3e1354a8fdcd761e396fe8839eeedfde7942a440027814ffbf727f9dd85cd627

   git diff --shortstat 502155ba51bbc9a4454b5a26d7f469b7e3ad0c09 \
     a86513fdb1147bb9af303829fdf07cd9398ccdd5
   # 7 files changed, 379 insertions(+), 65 deletions(-)

   git diff --check 502155ba51bbc9a4454b5a26d7f469b7e3ad0c09 \
     a86513fdb1147bb9af303829fdf07cd9398ccdd5
   # exit 0, no output
   ```

2. Targeted adaptive-runner suite

   ```text
   PYTHONPATH=src .../sourcing-ai-agent/.venv/bin/python \
     -m unittest tests.test_adaptive_grok_wave_runner -v
   # Ran 92 tests in 36.946s — OK
   ```

3. Full standard-library suite

   ```text
   PYTHONPATH=src .../sourcing-ai-agent/.venv/bin/python \
     -m unittest discover -s tests -v
   # Ran 381 tests in 72.501s — OK
   ```

4. Contract and static checks

   ```text
   PYTHONPATH=src .../sourcing-ai-agent/.venv/bin/python -m x_first.contracts
   # status=valid, errors=[]
   # precision=1.0, recall=1.0, predicted/relevant=20/20, false_merge_count=0

   .../sourcing-ai-agent/.venv/bin/ruff check src tests scripts
   # All checks passed!

   .../sourcing-ai-agent/.venv/bin/python -m py_compile \
     src/x_first/adaptive_grok_wave_runner.py tests/test_adaptive_grok_wave_runner.py
   # exit 0, no output

   .../sourcing-ai-agent/.venv/bin/python -m json.tool \
     configs/adaptive_grok_wave_effective_prompt_policy.v1.json
   .../sourcing-ai-agent/.venv/bin/python -m json.tool \
     contracts/x.grok.adaptive_recall_wave.effective_prompt_policy.v1.schema.json
   # both exit 0
   ```

5. Production OpenAI binding

   - Prompt bytes SHA-256:
     `47c2e97a381e19897aa5327ec57baf590fe8e0bf8765a027ea1a87ed79387f5e`; it exactly equals the registry row.
   - Selected entry: `openai_pretraining_zero_prior_official_discovery.v5`.
   - Selected-entry semantic digest:
     `3edef6bdf4704027d832b02ded32e3c26233fa34bc5a7055107ad33bcba3d4fe`.
   - Prior-input policy: `require_empty_prior_waves_v1`; recomputed and registered semantic digest:
     `8b2e6ed95ad3574903a2d459c0bcafb92cfa293020fd716ad311e115144ec47c`.
   - Session policy: `discovery_only_official_accounts_no_person_hydration_v2`; recomputed and registered semantic
     digest: `26305c1bd1529e51bb35e9ce1533b64f09b2ed87f99981f14be5dde0718101fc`.
   - Entry-bound official handles: exactly `OpenAI`.

6. Synthetic non-empty-prior ordering probe

   A production-policy OpenAI request with one SHA-bound `KnownPrior` row was evaluated after its synthetic auth file
   had been deleted. Both entrypoints returned the policy error, not an auth or grant error, and neither authority root
   was created:

   ```text
   issue_error PermissionError effective_prompt_prior_input_not_approved
   run_error PermissionError effective_prompt_prior_input_not_approved
   grant_root_exists False
   run_root_exists False
   ```

   The selected-entry digest includes the optional paired prior policy and its semantic digest. The targeted tests
   additionally remove the pair after a completed synthetic run: grant/bundle replay returns
   `command_binding_request_replay_mismatch` plus `grant_replay_invalid`, and recovery fails before terminal
   publication.

7. Query-policy probes

   - All `13/13` prompt-enumerated `x_user_search` terms pass with target `OpenAI`.
   - Bare user queries `OpenAI`, `@OpenAI`, and `OpenAI data` all fail.
   - `OpenAI data` through keyword search and `OpenAI training data` through semantic search both pass.
   - The remaining blocking matrix mismatch reproduced independently:

     ```text
     tool                 query                  allowed
     x_keyword_search     OpenAI                false
     x_keyword_search     @OpenAI               false
     x_keyword_search     OpenAI pretraining    true
     x_keyword_search     @OpenAI pretraining   true
     x_semantic_search    OpenAI                false
     x_semantic_search    @OpenAI               false
     x_semantic_search    OpenAI pretraining    true
     x_semantic_search    @OpenAI pretraining   true
     ```

8. Retained replay and D2 coexistence

   ```text
   validate_operator_bundle(grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924)
   # []

   validate_operator_bundle(grok_wave_live_c4ec7140840942b2b1e62b5d078562f1)
   # []
   ```

   The target changes no request, grant, intent, receipt, result, or campaign wire schema. The only schema diff is the
   backward-compatible, optional paired field in the module-owned effective-prompt policy configuration schema. The
   target's runner diff is confined to prior-input policy loading, selection, and binding; it does not edit D2 auth
   lifecycle or taint/active-use semantics. Full tests and both retained replays remained green.

## Controlling-finding closure matrix

| Controlling item | Status | Rereview evidence |
|---|---|---|
| P1 zero-prior was advisory | closed | Optional paired prior policy is schema/loader validated, selected-entry bound, enforced before auth/grant/run-root, and reselected by grant validation, bundle replay, and recovery; positive/negative tests and the independent ordering probe pass |
| P1 prompt/runtime query vocabulary | partial; blocking condition remains | The exact 13 user terms and `data` routing now reconcile, but the controlling requirement that bare organization literals be combined with professional terms is not implemented; see P1 below |
| P2 model prompt disclosed denominator `98` | closed | Prompt contains neither `98` nor frozen-union wording; the denominator remains only in the operator-side post-hoc note |
| P2 registry tests hard-coded two-lab totals | closed | An explicit lab-to-prompt-family table derives family paths, target equality, per-family counts, digest closure, and total live count; no OpenAI/GDM/global count literal remains |
| P2 thread-fetch causal origin | accepted residual | Transport still proves typed call arguments rather than native result-body causality; output remains partial/model-mediated and claims no source-bound origin or per-thread yield |

## Findings

Severity totals: `P0=0`, `P1=1`, `P2=1` (accepted residual).

### [P1][re-raise] Required bare-organization coverage still contradicts the selected runtime policy

The controlling review explicitly required coverage literals `OpenAI` and `@OpenAI` to be combined with professional
terms because both bare forms are mechanically rejected. The remediation prompt does not say that. It calls bare
`OpenAI` and `@OpenAI` keyword/semantic organization literals at lines 36–38, then lists those exact bare literals as
required organization/time coverage at lines 49–51. The adjacent single-token prohibition makes the instructions
internally contradictory rather than closing the mismatch.

Runtime strips wrappers and rejects any remaining handle-like single-token subject before branching by native-X tool
(`adaptive_grok_wave_runner.py:4426-4461`). The independent matrix above proves both bare forms fail for keyword and
semantic search while the corresponding `... pretraining` queries pass. The new tests cover bare rejection only for
`x_user_search`; their keyword/semantic positives use multi-token `data` queries and therefore do not catch this
required-matrix failure (`test_adaptive_grok_wave_runner.py:1182-1207`). A model following the literal matrix can spend
provider work on a permitted-looking call and have the operator reject the session during post-run transcript replay.

Required closure: make the prompt and its matrix unambiguous that `OpenAI` and `@OpenAI` must be combined with at least
one professional or technical topic for keyword/semantic search, with examples such as `OpenAI pretraining` and
`@OpenAI pretraining`. Add table-driven keyword and semantic assertions for both bare-negative and combined-positive
forms, update the prompt/registry digests, and obtain a new pinned rereview. Versioning the runtime to permit bare
handle-like subjects would instead widen the shared anti-person-query policy and would require a separate contract and
replay review.

### [P2][residual] Thread-fetch causal origin remains unproven

This is unchanged from the controlling review. The retained transport proves typed `x_thread_fetch` arguments but not
native-X result bodies that bind a fetched thread ID to a preceding official/project discovery result. It remains
acceptable only while the result is `X_SEARCH_PARTIAL`, evidence is model-mediated/unverified, and no source-bound
origin, convergence, or per-thread yield is claimed.

## Confirmed non-findings

- Empty-prior authority is now an entry-owned optional pair whose semantic digest is part of the selected-entry digest;
  non-empty input fails before auth, grant access/creation, or run-root creation and also fails replay/recovery.
- The model prompt no longer receives the historical denominator or frozen-union comparison language.
- The exact 13 user-search terms reconcile, and `data` is consistently restricted to keyword/semantic discovery.
- Registry coverage is derived from an explicit extensible lab-family table instead of fixed `8 + 7 = 15` assertions.
- No runtime wire schema changed, and the remediation composes with the parent D2 OAuth lifecycle implementation.
- Retained v4 and v5 bundles replay with zero integrity errors under the current target.

The P1 blocks another provider-costing OpenAI discovery v5 grant, live run, performance comparison, or milestone
promotion for this scope. Green offline suites and closed zero-prior authority do not override the prompt/runtime
contradiction.

NO-GO
