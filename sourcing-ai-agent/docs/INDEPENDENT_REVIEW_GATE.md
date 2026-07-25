# Independent Review Gate

> Status: active engineering gate. Use this together with `AGENTS.md`, `docs/PRE_AGENT_CONTRACT_REVIEW.md`, and `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`.

## Purpose

Fast contract preflights prove known invariants. W6/nightly proves long-chain pressure behavior. Neither replaces independent review. Significant design notes, contract changes, and implementation slices need a separate adversarial review by a reviewer that did not author the change, ideally using a different model or coding agent.

This gate is adapted from the `genius-x/docs/agents` independent-review practice, but tailored to this repository's durable workflow, provider-cost, Public Web, CRM, projection, and Agent-callable runtime risks.

## Mandatory Triggers

Run independent review before live provider validation, W6/nightly, founder/manual signoff, milestone closeout, or Phase 13 product work when a change touches any of:

- Contract docs, schema, owner matrix, source of truth, allowed values, readiness/filter/export/status/projection/permission field semantics, or deletion conditions for an old source.
- Frontend/backend public API fields, adapters, generated schemas, response wording that encodes state, user-visible counts, promotion/export controls, or retry/cancel/resume actions.
- Durable runtime, workflow event/command/activity/attempt/entity-delta ownership, reducer output, recovery owner, retry/idempotency/outbox/causality, or current-state materialization.
- Provider scheduling, after-start control, retry/circuit/rate-limit policy, webhook/poll handling, model-provider routing, fallback behavior, or live-provider cost attribution.
- Public Web search, document fetch, model adjudication, evidence/signal materialization, promotion, suppression, manual override, or export policy.
- CRM, PersonAsset, CompanyAsset, assertion promotion, CRM task/current-state, or company/person media asset semantics.
- Projection/public-reader contracts, canonical reader ownership, field visibility, filter/facet/readiness scope, board runtime state, or frontend API adapters.
- Migration bridge deletion, legacy endpoint retirement, PG-only storage cutover, env/toggle hard-disable, or any temporary compatibility path that could become a hidden fallback.
- Agent/Operation/action registry/control surfaces, approval gates, user/Agent callable command APIs, or Phase 13-adjacent UX/API work.
- Any implementation slice being claimed as a completed milestone or ready for manual/live/product signoff.

If the author is unsure whether a change affects shared semantics, run the gate. The default for Contract-heavy work is review, not skip.

## Allowed Skips

Small localized changes may skip this only when they do not change shared semantics, provider/model behavior, storage schema, read/write ownership, frontend/backend contract fields, runtime gates, or user-visible action semantics. Examples:

- Typo-only docs updates that do not change a Contract, gate, runbook, or signoff requirement.
- Purely local CSS polish or copy that does not encode state, readiness, risk, exportability, retry/cancel/resume behavior, or API semantics.
- Isolated test fixture cleanup that does not weaken expectations or alter a shared gate.
- Refactors proven by targeted tests to preserve existing public/runtime contracts exactly.

## Async Reference Review Lane (owner-approved 2026-06-12)

For changes inside the narrowed gate scope that are already covered by independent adversarial subagent verification (the post-handoff standard for extractions, transport ports, and storage-contract fixes), the Codex review runs as a NON-BLOCKING parallel reference channel instead of a blocking gate:

- Fire `scripts/run_independent_review_gate.py --execute` in the background once the implementation is settled (anchor `--base` to a pinned commit), while the primary subagent verification runs — both channels are read-only and review the same diff. The reviewer must be a separate non-author session; a different model family is preferred but not required.
- Once the request and pinned scope are recorded, the next repository/module batch and unrelated work proceed immediately. The Codex result is read when it lands and triaged under the standard finding discipline: real findings are fixed forward in a follow-up commit, false positives are recorded. Never roll back landed work solely because the reference review is pending.
- A `NO-GO` creates scope-local review debt: it blocks live/W6/manual validation and milestone signoff for that reviewed scope, but does not freeze unrelated development. Track the debt until fixed or explicitly accepted.
- Destructive operations on non-rebuildable assets keep the BLOCKING gate below unchanged — the async lane never applies to them.
- First instance: `runtime/reviews/20260612T080442Z_async-reference-phase3d-acquisition-extraction.md` (Phase 3d extraction, GO; independently cross-confirmed the Claude verifier's AST findings).

## Gate Order

1. Freeze or update the relevant contract first, including owner/source-of-truth/fallback/deletion semantics.
2. Run targeted tests and fast preflight. Expensive W6/nightly must not be the first detector for basic Contract drift.
3. Generate the independent review prompt from `docs/INDEPENDENT_REVIEW_BRIEF.md`.
4. Run reviewer in read-only mode with no stdin blocking.
5. Address blocking findings or record an explicit user/founder-accepted exception.
6. Only then run expensive W6/nightly, live provider validation, manual product signoff, or milestone closeout.

## Codex Reviewer Command

Use the canonical runner's Codex app-server JSON-RPC transport in read-only mode. The standing policy is the newest available model at its highest supported reasoning effort. The operator maintains the reviewer-only model, effort, and service tier in `configs/reviewer-codex/reviewer.toml`. `make independent-review-gate` first builds `runtime/reviewer_codex_home_v2/`: its private `config.toml` is derived from the current source Codex config with that checked-in top-level policy trio forced. Login, session, and model-catalog state remain symlinked to the source home, but the version-sensitive `models_cache.json` is reviewer-private and is populated lazily by the reviewer runtime. This prevents a Desktop/CLI cache with an older schema from aborting app-server initialization before a review can start. Bootstrap never copies the source cache or writes through to it; it removes a legacy target-side cache symlink without following it and preserves an existing real private target cache byte-for-byte across refreshes. The Make target supplies this directory as `CODEX_HOME`, so ChatGPT Desktop may continue changing `~/.codex/config.toml` or its own model cache without changing reviewer routing or cache compatibility. The bootstrap reads but never writes the source home, uses private directory/file permissions, and fails without replacing any other same-name real target entry. This is policy plus version-sensitive cache isolation, not full Codex-state isolation.

`runtime/reviewer_codex_home_v2/` is a one-time clean cutover made on 2026-07-15. The pre-hardening `runtime/reviewer_codex_home/` had already acquired a real `mcp-oauth-locks/` directory containing a nested same-name symlink under the old `ln -sfn` behavior. It is legacy runtime state: bootstrap and Make do not delete, rename, repair, or reuse it. Operators may inspect or retire it separately after active-review retention needs are understood; canonical reviews use v2. Explicit `REVIEWER_CODEX_HOME` overrides remain supported but must name a clean or already hardening-compatible target.

The runner reads the explicit trio from the isolated reviewer config, mirrors the same values into the isolated `thread/start` and `turn/start` requests, and does not add model/effort/tier CLI pins. It writes the prompt to `runtime/reviews/*.prompt.md`, captures the bidirectional transcript, and treats the active model, reasoning effort, and service tier returned by `thread/start` as authoritative. The persisted rollout must corroborate the same thread/source/model/effort; any rollout tier observation must also match, but a rollout that omits tier no longer hides the active tier response. Missing active settings, conflicting evidence, settings drift, or model reroute fails closed. The Make default timeout is 1800 seconds; `REVIEW_TIMEOUT_SECONDS` remains an explicit per-run override.

```sh
make independent-review-gate \
  REVIEW_TITLE="W7g CRM Public Web live product validation" \
  REVIEW_BASE="<pinned-base-SHA>" \
  REVIEW_FILES="Makefile scripts/run_crm_public_web_live_product_validation.py scripts/run_independent_review_gate.py src/sourcing_agent/model_provider.py src/sourcing_agent/public_web_runtime_core.py src/sourcing_agent/runtime_asset_retention_prune.py tests/test_model_provider.py tests/test_crm_public_web_runtime_boundary.py tests/test_independent_review_gate_runner.py tests/test_pre_agent_contract_review.py docs/PRE_AGENT_CONTRACT_REVIEW.md docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md docs/INDEPENDENT_REVIEW_GATE.md docs/TESTING_PLAYBOOK.md" \
  REVIEW_EXTRA_CONTEXT="Review the W7g live gate, its review-runner/verifier trust root, mandatory force-refresh-before-request guard, invocation-bound batch/run evidence, and immutable gpt-5.6-sol provider-response model identity. Require exact title/scope digest and co-located latest-run phase metrics." \
  REVIEW_EXECUTE=1
```

`REVIEW_FILES` remains a whitespace-separated Make variable, but Make expands every word into one exact `--file PATH` argument. Direct script callers should likewise prefer repeatable `--file PATH`; one argument is one project-relative path and is never shell-split by the runner. The legacy `--files "PATH ..."` form remains compatible with shell-style space-separated lists, rejects comma-separated lists explicitly, and is mutually exclusive with `--file`. A path containing whitespace should therefore use direct repeated `--file` invocation rather than `REVIEW_FILES`.

For this W7g command, replace `REVIEW_BASE="<pinned-base-SHA>"` with the exact base commit before launch; never omit it or substitute a moving branch. The immutable file scope includes `scripts/run_independent_review_gate.py`, the shared verifier in `src/sourcing_agent/runtime_asset_retention_prune.py`, and `tests/test_independent_review_gate_runner.py` because they create or validate the `GO` artifact consumed by W7g. The review must also verify that real execution requires exact `CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1` / `--force-refresh` and rejects the omission before provider health or any other backend request. The runner binds the resolved base and head commits, normalized file list, Git diff/tree content, title, scope mode, and extra-context digest into `review_scope_digest_sha256`. Project-relative scope paths are explicitly mapped to the containing Git top-level, so this remains signoff-capable when the application root is a subdirectory of the worktree. A file path is signoff-capable only for one of three exact Git object transitions: `blob -> blob`, `absent -> blob`, or `blob -> absent`. The builder and verifier share that closed allowlist; missing at both revisions, trees, submodules/gitlinks, and mixed blob/tree transitions fail closed instead of becoming a pinned scope merely because one side is a blob. Record the emitted digest with the review request, then pass the exact value as `CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256` when the resulting artifact is used for live validation. An unpinned/current-working-tree request is reference-only. A pinned commit-object review remains signoff-capable while unrelated work continues, but any later scoped commit, staged change, unstaged change, or untracked replacement invalidates the artifact at consumption; index and worktree are checked separately so opposing changes cannot cancel. Commit the intended scope and launch a new pinned review for those gates.

Transport process shape. This command alone is not a review request and cannot create valid evidence; the Make/script path owns initialization, thread/turn requests, transcript capture, timeout, and artifact binding through Python so it works on macOS without GNU `timeout`.

```sh
codex --sandbox read-only app-server --strict-config --stdio
```

The canonical process has no model/effort/tier CLI overrides. Before execution, the runner requires explicit values from the process-global config under the dedicated reviewer `CODEX_HOME` (not the Desktop-owned `~/.codex/config.toml`) and sends those exact values in the app-server thread/turn protocol; after execution, it requires the `thread/start` active response to match. It writes a redacted `*.effective-config.json` using `independent_review_effective_config_v4`; this is an evidence-schema version, while the unchanged wire transport remains `codex_app_server_jsonrpc_v2`. The evidence binds prompt, bidirectional transcript, raw reviewer output, reviewer config, source rollout, active settings, thread/session/turn identity, dedicated thread source, and process status by SHA-256, plus a canonical pinned-Git scope digest over title/base/head/files/diff/tree/extra-context. The shared verifier replays the transcript and rollout instead of trusting the JSON copy. It binds the requested root turn by exact root thread/turn identity, requires the root `turn/start` RPC response and root start notification to agree on `status=inProgress` with no error, and forbids any other observed turn from using the root thread id. Every child turn must use its own distinct nonempty thread id. Every turn observed in this app-server process must have one ordered start/completion pair, `status=completed`, no error, and exactly one durable final `item/completed`; final ids and text must be nonempty strings, and final ids must be globally unique. Legacy completions may inline the same final item; Codex 0.144+ may instead report `itemsView=notLoaded` with an empty `items` list. In either protocol the root raw output is taken from the durable root final item, and an inline copy, when present, must match its id and text. Orphan, duplicate, incomplete, errored, or final-less turns fail closed. Arbitrary status/telemetry notifications and non-final items that do not participate in identity, settings, turn, or final-output evidence may be ignored. Additional `thread/started` notifications, any `thread/settings/updated`, all turn notifications, and all final items remain subject to their strict root identity, settings-consistency, pairing, and ownership checks even when they name another thread. The verifier also proves the exact prompt and final raw output occur in both evidence paths before accepting `reviewer_exit_code=0`. Only a dedicated Codex Desktop app-server rollout with `source=vscode` and the canonical independent-review thread source may append one strictly shaped terminal `<oai-mem-citation>` transport annotation to its single persisted assistant `response_item`; the verifier separates that suffix and removes it solely for the redundant response-item comparison, while the durable app-server final item, rollout `agent_message`, and `task_complete.last_agent_message` must still match raw output exactly. The root turn window must contain exactly one final assistant response item. Arbitrary, malformed, duplicate, nonterminal, non-Desktop, or noncanonical-thread annotations and response items fail closed. Complete v2 artifacts remain verifier-compatible only under their original exact-output contract and cannot use this Desktop annotation exception; complete v3 artifacts retain their old exact transcript field set and are replay-compared only over those legacy fields. New runs emit v4. A requested alias therefore cannot be misreported as the active tier, old real-session evidence cannot be repacked under another prompt/scope, and a nonzero Codex process cannot leave a valid-looking `GO`.

This is a local integrity and anti-replay contract, not an external signature scheme. It does not claim cryptographic authenticity or remote attestation against a malicious author who can rewrite the repository and every local evidence file together; human/session independence and protected CI/PR storage remain the trust boundary for that threat.

If the configured newest model is temporarily unavailable, follow the Model Routing Table: update the operator-owned `configs/reviewer-codex/reviewer.toml` to the designated fallback at that model's highest supported effort, record the fallback, and retry only the unfinished review. A same-family Codex/GPT fallback is allowed only through a separate non-author read-only session; the implementation author still cannot self-certify. If no reviewer session is reachable, defer the gate visibly in `docs/RESIDUAL_LEDGER.md` while unrelated work continues.

## Model Routing Table (checked-in 2026-07-09; owner-updated 2026-07-10)

Routing is a checked-in artifact, not a per-session improvisation — the ②.1 recon batch was wiped by a
primary-model quota wall and the fallback was improvised in-chat; this table makes the fallback a
pre-declared decision. Both `AGENTS.md` files point here; do not fork per-file copies of these defaults.

| Lane | Nature | Primary | Fallback | On quota exhaustion |
| --- | --- | --- | --- | --- |
| Recon / scout (read-only fan-out) | execution-dense | author-session model | author family, lighter tier (e.g. Sonnet) | degrade to fallback; resume, don't rerun survivors |
| Mechanical edit fan-out | execution-dense | author family, lighter tier | author-session model | degrade |
| Adversarial verification / synthesis | reasoning-dense | author-session model | author family, deepest available | degrade |
| **Independent review (this gate)** | reasoning-dense | newest available Codex reviewer model at its highest supported effort, loaded from the dedicated reviewer config in a separate read-only session | operator-selected next newest/deepest available Codex reviewer in a separate read-only session | record the fallback or defer the review; continue unrelated work |

Constraints:

- Review independence is session/author separation: the implementation author cannot certify their own
  change, but a distinct read-only Codex reviewer session may use the same model family. Cross-model
  review remains preferred when its transport is operational because it adds heterogeneous blind-spot coverage.
- Every fired fallback or deferred gate is recorded in the review artifact metadata (and the
  residual ledger for deferrals), so a silent downgrade cannot become a hidden fallback.
- Interrupted fan-outs resume from their run id (re-dispatch only unfinished lanes); a completed
  agent's output is never re-rolled by a full rerun.
- Initial assignments dated 2026-07-09; owner changed the independent lane to Codex-first and explicitly
  non-blocking for unrelated development on 2026-07-10. The reviewer-home implementation keeps routing in
  one operator-owned policy file without coupling it to Desktop settings. Revisit
  `configs/reviewer-codex/reviewer.toml` when the model lineup changes; do not edit the runner or active docs
  to chase model versions.

## Required Evidence

The review output must record:

- Reviewed files or diff scope.
- Contract docs considered.
- Validation already run by the author.
- Command, `reviewer_exit_code=0`, Codex CLI/session/thread/turn identity, isolated-reviewer-config digest (named `global_config` in the evidence schema), transcript and rollout paths/digests, and active model, reasoning effort, and service tier used for the independent review.
- A hash-bound `independent_review_effective_config_v4` JSON artifact with prompt/transcript/raw-output/rollout provenance, replayable root and observed-turn app-server bindings (including `observed_turn_count`), rollout causal bindings, a recomputable pinned-Git scope digest, and an empty model-reroute chain. The transport protocol inside that artifact remains `codex_app_server_jsonrpc_v2`.
- Blocking findings or `GO` / `NO-GO`.
- Accepted exceptions, if any, with user/founder approval context.
- Residual risks that should be checked in W6/nightly, live validation, or manual product review.

Store outputs under `runtime/reviews/<stamp>_<title>.md` or attach them to the PR/review thread. For milestone signoff, reference the artifact path from `docs/NEXT_TODO.md`, the relevant Contract doc, or the PR checklist. Do not treat an unrecorded chat answer, author summary, or normal implementation note as review evidence.
A bare `NO-GO` without at least one prioritized finding is an invalid review artifact; rerun with a narrower scope or clearer prompt instead of treating it as a valid blocked review.
Provider-costing live gates and milestone signoff runners must call the shared artifact verifier, not just check file existence or parse the final word: valid evidence is a runner-produced review artifact under `runtime/reviews/` with a zero reviewer exit, hash-verified effective-config/rollout evidence, complete metadata, matching scope/title tokens and required files/contracts, and a final `GO` verdict. `NO-GO`, timeout, no-output, nonzero-exit, rerouted, invalid, prompt-file, missing/tampered evidence, missing-scope-file, or unrelated artifacts fail closed.

## Independence Rule

The implementation author cannot self-certify. A valid reviewer must be a separate read-only Codex app-server thread/session, another coding agent, or another model/tool configured to inspect the diff without mutating code. The reviewer may use local read-only commands and tests already produced by the author as evidence, but must independently judge GO/NO-GO.

## Scope Discipline

Independent review is scoped review, not a full development-session resume. The reviewer should inspect the listed files/diff and targeted Contract sections with `git diff -- <files>`, `rg`, and line-range reads. It should not read full `PROGRESS.md`, full `docs/NEXT_TODO.md`, or full long Contract docs unless the review scope explicitly requires full context. This keeps the gate fast enough to run before live/W6/manual signoff instead of becoming another expensive matrix.

## Non-Goals

- This gate does not replace tests.
- This gate does not authorize live provider calls.
- This gate does not approve contract direction changes that need user/founder confirmation.
- This gate must not mutate code or run broad live workflows; it is read-only review.

## Failure Policy

`NO-GO` findings must be resolved or explicitly accepted by the user/founder before the affected scope is promoted, run through live/W6/manual validation, or signed off. Unrelated modules and later pinned batches may continue while that debt is fixed asynchronously. A green targeted test, W6/nightly, or browser/manual pass does not override the scope-local block. If an exception is accepted, it must be named in the review artifact and carried in the next milestone TODO until retired.
