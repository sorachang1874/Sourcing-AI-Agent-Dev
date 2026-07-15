# OpenAI discovery v5 query-alignment final pinned rereview

## Evidence header

- Reviewer: original non-author query-mismatch reviewer
  `/root/oauth_lifecycle_d2_precommit_audit/auth_taint_concurrency_audit`.
- Reviewed head: `7ebbe857906aa72a0855501d0e71924238ff7bda`.
- Reviewed base and literal first parent: `9f5db006d6bdd1975ac0e40e64b6a73429e0c76b`.
- The base is the durable prior rejection artifact for the OpenAI v5 query mismatch. Its ancestry includes the D2 OAuth
  lifecycle implementation and its independent audit.
- Reviewed scope: exactly four X-First files, `53 insertions`, `11 deletions`: the effective-prompt registry, OpenAI
  transfer note, OpenAI v5 prompt, and adaptive-runner tests.
- Exact binary-diff SHA-256:
  `c60490644a5e5428c4d75f08bcfde4234c0c46141af3e828bf79bb6f9ba270ac`.
- Reproduction tree: clean detached worktree
  `/private/tmp/x-first-openai-query-fix-review-7ebbe85` at the reviewed head. Review reads, tests, and probes imported
  only that pinned source. The ambient working tree was not a review input.
- No network, Grok/X, model, connector, live provider, OAuth, API key, credential file, grant issuance, product write,
  push, or live execution was used. Synthetic adversarial input lived only in a temporary owner-only directory. Two
  existing private operator bundles and their approval metadata were read only for offline replay. This artifact is
  the reviewer's only repository write.

## Exact validation evidence

1. Object, ancestry, scope, and digest

   ```text
   git rev-parse HEAD
   # 7ebbe857906aa72a0855501d0e71924238ff7bda

   git rev-parse HEAD^
   # 9f5db006d6bdd1975ac0e40e64b6a73429e0c76b

   git diff --binary 9f5db006d6bdd1975ac0e40e64b6a73429e0c76b \
     7ebbe857906aa72a0855501d0e71924238ff7bda | shasum -a 256
   # c60490644a5e5428c4d75f08bcfde4234c0c46141af3e828bf79bb6f9ba270ac

   git diff --shortstat 9f5db006d6bdd1975ac0e40e64b6a73429e0c76b \
     7ebbe857906aa72a0855501d0e71924238ff7bda
   # 4 files changed, 53 insertions(+), 11 deletions(-)

   git diff --check 9f5db006d6bdd1975ac0e40e64b6a73429e0c76b \
     7ebbe857906aa72a0855501d0e71924238ff7bda
   # exit 0, no output
   ```

2. Exact query-registry regression test

   ```text
   PYTHONPATH=src .../sourcing-ai-agent/.venv/bin/python -m unittest -v \
     tests.test_adaptive_grok_wave_runner.AdaptiveGrokWaveRunnerTests.test_production_effective_prompt_policy_reconciles_registered_lab_prompt_families
   # Ran 1 test in 0.958s — OK
   ```

3. Targeted adaptive-runner suite

   ```text
   PYTHONPATH=src .../sourcing-ai-agent/.venv/bin/python \
     -m unittest tests.test_adaptive_grok_wave_runner -v
   # Ran 92 tests in 32.085s — OK
   ```

4. Full standard-library suite

   ```text
   PYTHONPATH=src .../sourcing-ai-agent/.venv/bin/python \
     -m unittest discover -s tests -v
   # Ran 381 tests in 61.554s — OK
   ```

5. Contract and static checks

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
   # exit 0
   ```

6. Production binding

   - Prompt bytes SHA-256:
     `5318d0baedd1b7fe6ec0057c01dfceccff5296d0e1f85dbf5642a094ec85164b`; it exactly equals the selected registry row.
   - Selected entry: `openai_pretraining_zero_prior_official_discovery.v5`.
   - Recomputed selected-entry semantic digest:
     `9779d588d2e69582112334e39670c9c05dc93d93a43324f0848ee80a6907efc2`.
   - Prior-input policy: `require_empty_prior_waves_v1`; recomputed and registered semantic digest:
     `8b2e6ed95ad3574903a2d459c0bcafb92cfa293020fd716ad311e115144ec47c`.
   - Session policy: `discovery_only_official_accounts_no_person_hydration_v2`; recomputed and registered semantic
     digest: `26305c1bd1529e51bb35e9ce1533b64f09b2ed87f99981f14be5dde0718101fc`.
   - Entry-bound official handles: exactly `OpenAI`.

7. Independent query truth table

   ```text
   tool                 query                    allowed
   x_keyword_search     OpenAI                  false
   x_keyword_search     @OpenAI                 false
   x_keyword_search     OpenAI pretraining      true
   x_keyword_search     @OpenAI pretraining     true
   x_semantic_search    OpenAI                  false
   x_semantic_search    @OpenAI                 false
   x_semantic_search    OpenAI pretraining      true
   x_semantic_search    @OpenAI pretraining     true
   ```

   All `13/13` prompt-enumerated `x_user_search` terms pass when combined with `OpenAI`. The three closed negative
   controls `OpenAI`, `@OpenAI`, and `OpenAI data` all fail user search. The prompt now says the bare organization forms
   are invalid across keyword, semantic, user, and thread search, supplies the two multi-term positive examples, and
   replaces the coverage-matrix bare literals with those multi-term forms. The new table-driven test exercises the
   exact four keyword and four semantic outcomes above.

8. Zero-prior ordering probe

   A production-policy OpenAI request containing one SHA-bound prior wave was evaluated after its synthetic auth file
   had been deleted. Policy rejection remained earlier than auth/grant/run-root work:

   ```text
   prior_issue PermissionError effective_prompt_prior_input_not_approved
   prior_run PermissionError effective_prompt_prior_input_not_approved
   prior_roots False False
   ```

9. Retained replay and D2 regression check

   ```text
   validate_operator_bundle(grok_wave_live_f41ad0d4206f4435baceb4bfe0b0e924)
   # []

   validate_operator_bundle(grok_wave_live_c4ec7140840942b2b1e62b5d078562f1)
   # []
   ```

   The four-file target changes no runtime source, contract schema, request/grant/intent/receipt wire shape, auth
   lifecycle, taint registry, active-use claim, or recovery path. It changes only the reviewed prompt text, its registry
   digest, the operator note, and the query-alignment regression assertions. The full D2-bearing suite and both retained
   bundles remain replay-clean.

## Prior-finding closure

The prior P1 is closed. The prompt, coverage matrix, tests, and unchanged selected runtime policy now agree on all
required query forms:

- bare `OpenAI` and `@OpenAI` fail keyword and semantic search;
- `OpenAI pretraining` and `@OpenAI pretraining` pass both tools;
- all 13 enumerated organization-plus-professional user searches pass;
- bare organization and topic-only `OpenAI data` user searches fail;
- data-topic discovery remains available as a multi-term keyword/semantic path.

The prompt no longer contains a contradictory instruction that could cause a model-compliant call to fail post-run
query-policy replay. Prompt bytes, registry row, selected-entry digest, prior-input semantic digest, and session-policy
semantic digest reconcile.

## Findings

Severity totals: `P0=0`, `P1=0`, `P2=1` accepted residual.

No new or re-raised blocking finding was identified.

### [P2][residual] Thread-fetch causal origin remains unproven

This is unchanged from the earlier v5 reviews. The retained transport proves typed `x_thread_fetch` arguments but not
native-X result bodies that causally bind a fetched thread ID to a preceding official/project discovery result. It
remains acceptable only while the result is `X_SEARCH_PARTIAL`, evidence is model-mediated/unverified, and no source-
bound origin, convergence, or per-thread yield is claimed.

## Scoped approval consequence

The prior query mismatch is closed for the exact pinned target. This approval is limited to the OpenAI zero-prior v5
prompt/registry/query contract on top of the inherited D2 lifecycle. It does not claim live execution success,
population convergence, complete recall, source-replayable X result bodies, or permission to bypass a fresh
operator-triggered grant, OAuth freshness, private retention, budget, one-shot authority, and kill boundaries.

GO
