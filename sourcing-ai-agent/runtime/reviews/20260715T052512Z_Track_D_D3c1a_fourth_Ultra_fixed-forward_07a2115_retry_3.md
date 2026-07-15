## Review Metadata

- title: Track D D3c1a fourth Ultra fixed-forward 07a2115 retry 3
- base/ref: 74df4b49c6cdec362f370504d6c77a3f3ff3f325
- reviewer_model: gpt-5.6-sol
- reviewer_reasoning_effort: ultra
- reviewer_service_tier: priority
- reviewer_configuration_mode: operator config values requested explicitly; active values verified from thread/start response
- reviewer_transport: app_server_stdio
- reviewer_config_path: `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/runtime/reviewer_codex_home_v2/config.toml`
- reviewer_config_sha256: 090c5268b83fdadb8c524c15cfcbb2506aabc4f16d5ea50c02625f336a1782bd
- reviewer_exit_code: 0
- reviewer_codex_executable: `/Users/changyuyi/.local/bin/codex`
- reviewer_codex_cli_version: 0.144.3
- reviewer_session_id: 019f643c-80ee-75f2-b945-b40921f3e024
- reviewer_session_source: vscode
- reviewer_thread_id: 019f643c-80ee-75f2-b945-b40921f3e024
- reviewer_turn_id: 019f643c-962e-7be2-851b-14b99bd0fc1b
- reviewer_rollout_path: `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/runtime/reviews/20260715T052512Z_Track_D_D3c1a_fourth_Ultra_fixed-forward_07a2115_retry_3.rollout-019f643c-80ee-75f2-b945-b40921f3e024.jsonl`
- reviewer_rollout_sha256: 0235ae1fdc113c55d64e6723ca1673a1de676a154313a77a6d21730cfbaa6f2c
- reviewer_effective_config_path: `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/runtime/reviews/20260715T052512Z_Track_D_D3c1a_fourth_Ultra_fixed-forward_07a2115_retry_3.effective-config.json`
- reviewer_effective_config_sha256: 4a64c4eee39cdc85de7c004dc0dfb169af7a5003770d3fe04ace4b6bfc6bfd8e
- review_scope_mode: pinned_commit_diff
- review_base_ref: 74df4b49c6cdec362f370504d6c77a3f3ff3f325
- review_resolved_base_commit: 74df4b49c6cdec362f370504d6c77a3f3ff3f325
- review_resolved_head_commit: 07a2115c20557e8867e7a33aa07832d4fe63d786
- review_git_diff_sha256: 9a8628a80f45dbd597cffbd1024b1b334780087a8e809301e90b1f83e181dceb
- review_git_tree_sha256: 4625c040be12b12f82b9183d13c6c0ba02b75282b9f871d200f6bc1b67ed64b2
- review_extra_context_sha256: 3aecb4268128028e079d66da7f40aa114598720705abbcfd88c20ccddb884237
- review_scope_digest_sha256: 6774a07db628ec8d5e4a39517ada8246b39a78bef651a24b9532b87c6e5069c6
- timeout_seconds: 1800
- prompt_path: `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/runtime/reviews/20260715T052512Z_Track_D_D3c1a_fourth_Ultra_fixed-forward_07a2115_retry_3.prompt.md`
- prompt_sha256: dc46daddb23445918d8671e434057df5dd93f2d151648a48bf1fd13b702ff975
- events_path: `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/runtime/reviews/20260715T052512Z_Track_D_D3c1a_fourth_Ultra_fixed-forward_07a2115_retry_3.events.jsonl`
- events_sha256: e376db5da39cffb24672961e98f0ec4a82ff2ddf4f19f7c3ce91575a4504cb80
- raw_output_path: `/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent/runtime/reviews/20260715T052512Z_Track_D_D3c1a_fourth_Ultra_fixed-forward_07a2115_retry_3.raw-output.md`
- raw_output_sha256: ff768ffad43b9824699812b55f3ca403fa5c83ebb8563327dbe1491f7ca39382
- command: `/Users/changyuyi/.local/bin/codex --sandbox read-only app-server --strict-config --stdio`
- contract_docs_considered: `AGENTS.md`, `docs/PRE_AGENT_CONTRACT_REVIEW.md`, `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `docs/INDEPENDENT_REVIEW_GATE.md`
- author_validation: see latest assistant handoff/progress note and targeted test output for this run
- accepted_exceptions: none recorded by this runner

Reviewed scope:
- `docs/TRACK_D_D3C1A_WORKFLOW_COMMAND_PUBLIC_PROJECTION_FIXED_FORWARD.md`
- `frontend-demo/src/lib/api.readonly-contract.ts`
- `frontend-demo/src/lib/api.ts`

## Reviewer Output

Evidence header

- Reviewed scope: exact pinned three-file diff `74df4b49c6cdec362f370504d6c77a3f3ff3f325..07a2115c20557e8867e7a33aa07832d4fe63d786`; runner-supplied scope digest `6774a07db628ec8d5e4a39517ada8246b39a78bef651a24b9532b87c6e5069c6`.
- Contracts considered: targeted sections of `AGENTS.md`, `docs/PRE_AGENT_CONTRACT_REVIEW.md`, `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `docs/INDEPENDENT_REVIEW_GATE.md`, plus residual rows `R-019` and `R-030`.
- Prior artifact considered: `runtime/reviews/20260715T044618Z_Track_D_D3c1a_third_Ultra_fixed-forward_74df4b4.md`.
- Author validation considered: frontend build 82 modules, projection 19 passed, combined lane 119 passed, scoped diff clean.
- Independent validation: scoped `git diff --check` passed; 55 exact head Git blobs compiled in memory with lockfile TypeScript 5.9.3 at zero diagnostics. An additional generic probe confirmed readable frozen indices/length, blocked index/length/`push`/`splice` mutations, and unchanged mutable `string[]` narrowing.
- Runtime-wire check: base/head `api.ts` transpilation was byte-identical at 379,103 bytes, SHA-256 `bcd90aba58d6957f74113dd9828bb6ca766be6ae91d35b3d3d17ed9594ba629d`.
- Reviewer model/effort/tier: not exposed in this turn.
- Accepted exceptions: `R-019`, `R-030`.
- Counts: P0/P1/P2/P3 = `0/0/2/2`; classifications are `0 new / 2 re-raise / 2 residual`.

Findings

1. `P2` / `frontend-demo/src/lib/api.ts:329` / the prior `Array.isArray` overload-domain gap is closed.

   The constrained generic preserves optional `WorkflowPublicJsonValue` arrays and arbitrary `WorkflowPublicFrozenArray<T>` element types after narrowing. The checked-in assertions at `api.readonly-contract.ts:51-83` and the independent generic probe reject index and length writes plus `push` and `splice`, while retaining readable typed indices and length.

   Concrete fix: none further; retain the compile oracle.

   **Classification: `re-raise`** — raised in the `20260715T044618Z` artifact finding #1 and addressed by `07a2115c20557e8867e7a33aa07832d4fe63d786`.

2. `P2` / `frontend-demo/src/lib/api.ts:215,218,2477` / the three previously omitted nested lists now use the frozen-array contract.

   `providerAfterStartControlUpgradeRequirements`, `runningControlCategories`, and `allowedActions` are all `WorkflowPublicFrozenArray<string>`. Assertions at `api.readonly-contract.ts:89-120` cover index, length, `push`, and `splice`. Ordinary mutable-array behavior remains available at lines 123-128.

   Concrete fix: none further.

   **Classification: `re-raise`** — raised in the `20260715T044618Z` artifact finding #2 and addressed by `07a2115c20557e8867e7a33aa07832d4fe63d786`.

3. `P3` / `frontend-demo/src/lib/api.ts:2914` / without trustworthy `Content-Length`, `response.text()` allocates the complete body before the 4 MiB fallback check.

   An oversized response can therefore be materialized before rejection.

   Concrete fix: enforce the limit incrementally through `Response.body` and cancel the reader on overflow.

   **Classification: `residual`** — `docs/RESIDUAL_LEDGER.md` row `R-019`.

4. `P3` / `frontend-demo/src/lib/api.ts:3194` / the non-enumerable `raw` compatibility view remains outside the measured enumerable snapshot.

   Direct access or separate serialization of `dto.raw` is not covered by the final JSON footprint.

   Concrete fix: complete the bounded consumer migration, remove `attachDemoRaw` and exported `raw` members, append exit evidence, and transition the retained ledger row to closed.

   **Classification: `residual`** — `docs/RESIDUAL_LEDGER.md` row `R-030`.

The scoped fixed-forward document accurately records the prior artifact, its `0/0/2/2` counts, both corrections, unchanged runtime wire, and pending fresh review. No new documentation, fallback, provider-cost, migration, or runtime issue was found.

**GO**

