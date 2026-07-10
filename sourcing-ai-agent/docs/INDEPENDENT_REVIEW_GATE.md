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

Use `codex exec` in read-only mode and write the review to a file. The standing policy is the newest available model at its highest supported reasoning effort. The operator maintains model, effort, and service tier in `~/.codex/config.toml`; project scripts and docs must not pin or override those values. The runner writes the prompt to `runtime/reviews/*.prompt.md`, feeds it non-interactively, captures JSON events, and reconciles persisted `thread_settings_applied`, `session_configured`, and `turn_context` observations. Missing effective tier evidence, conflicting observations, or model reroute fails closed. Do not pipe through `head` or `tail`.

```sh
make independent-review-gate \
  REVIEW_TITLE="W7g CRM Public Web live product validation" \
  REVIEW_BASE="<pinned-base-SHA>" \
  REVIEW_FILES="Makefile scripts/run_crm_public_web_live_product_validation.py scripts/run_independent_review_gate.py src/sourcing_agent/model_provider.py src/sourcing_agent/public_web_runtime_core.py src/sourcing_agent/runtime_asset_retention_prune.py tests/test_model_provider.py tests/test_crm_public_web_runtime_boundary.py tests/test_independent_review_gate_runner.py tests/test_pre_agent_contract_review.py docs/PRE_AGENT_CONTRACT_REVIEW.md docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md docs/INDEPENDENT_REVIEW_GATE.md docs/TESTING_PLAYBOOK.md" \
  REVIEW_EXTRA_CONTEXT="Review the W7g live gate, its review-runner/verifier trust root, mandatory force-refresh-before-request guard, invocation-bound batch/run evidence, and immutable gpt-5.6-sol provider-response model identity. Require exact title/scope digest and co-located latest-run phase metrics." \
  REVIEW_EXECUTE=1
```

For this W7g command, replace `REVIEW_BASE="<pinned-base-SHA>"` with the exact base commit before launch; never omit it or substitute a moving branch. The immutable file scope includes `scripts/run_independent_review_gate.py`, the shared verifier in `src/sourcing_agent/runtime_asset_retention_prune.py`, and `tests/test_independent_review_gate_runner.py` because they create or validate the `GO` artifact consumed by W7g. The review must also verify that real execution requires exact `CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1` / `--force-refresh` and rejects the omission before provider health or any other backend request. The runner binds the resolved base and head commits, normalized file list, Git diff/tree content, title, scope mode, and extra-context digest into `review_scope_digest_sha256`. Project-relative scope paths are explicitly mapped to the containing Git top-level, so this remains signoff-capable when the application root is a subdirectory of the worktree. Record the emitted digest with the review request, then pass the exact value as `CRM_PUBLIC_WEB_LIVE_INDEPENDENT_REVIEW_SCOPE_DIGEST_SHA256` when the resulting artifact is used for live validation. An unpinned/current-working-tree request is reference-only. A pinned commit-object review remains signoff-capable while unrelated work continues, but any later scoped commit, staged change, unstaged change, or untracked replacement invalidates the artifact at consumption; index and worktree are checked separately so opposing changes cannot cancel. Commit the intended scope and launch a new pinned review for those gates.

Equivalent direct shape. The Make/script path enforces the timeout through Python so it works on macOS without GNU `timeout`; add an external `timeout`/`gtimeout` only if your shell has it.

```sh
codex exec \
  --strict-config \
  --cd . \
  --sandbox read-only \
  --json \
  --output-last-message runtime/reviews/<review-id>.md \
  - \
  < runtime/reviews/<review-id>.prompt.md
```

The canonical command has no model/effort/tier CLI overrides. Before execution, the runner requires explicit global values; after execution, it extracts the actual model, reasoning effort, and service tier from the persisted rollout and fails closed on missing metadata or a mismatch. It writes a redacted `*.effective-config.json` using `independent_review_effective_config_v2`; the evidence binds prompt, captured JSON events, raw reviewer output, global config, and source rollout by SHA-256, plus a canonical pinned-Git scope digest over title/base/head/files/diff/tree/extra-context. The shared verifier reparses the rollout instead of trusting the JSON copy, records Codex session/thread and CLI version, requires one completed root `source=exec` turn, and proves the exact prompt and final raw output appear in the independent rollout event shapes before accepting `reviewer_exit_code=0`. A requested alias therefore cannot be misreported as the applied tier, old real-session evidence cannot be repacked under another prompt/scope, and a nonzero Codex process cannot leave a valid-looking `GO`.

This is a local integrity and anti-replay contract, not an external signature scheme. It does not claim cryptographic authenticity or remote attestation against a malicious author who can rewrite the repository and every local evidence file together; human/session independence and protected CI/PR storage remain the trust boundary for that threat.

If the configured newest model is temporarily unavailable, follow the Model Routing Table: update the operator-owned global config to the designated fallback at that model's highest supported effort, record the fallback, and retry only the unfinished review. A same-family Codex/GPT fallback is allowed only through a separate non-author read-only session; the implementation author still cannot self-certify. If no reviewer session is reachable, defer the gate visibly in `docs/RESIDUAL_LEDGER.md` while unrelated work continues.

## Model Routing Table (checked-in 2026-07-09; owner-updated 2026-07-10)

Routing is a checked-in artifact, not a per-session improvisation — the ②.1 recon batch was wiped by a
primary-model quota wall and the fallback was improvised in-chat; this table makes the fallback a
pre-declared decision. Both `AGENTS.md` files point here; do not fork per-file copies of these defaults.

| Lane | Nature | Primary | Fallback | On quota exhaustion |
| --- | --- | --- | --- | --- |
| Recon / scout (read-only fan-out) | execution-dense | author-session model | author family, lighter tier (e.g. Sonnet) | degrade to fallback; resume, don't rerun survivors |
| Mechanical edit fan-out | execution-dense | author family, lighter tier | author-session model | degrade |
| Adversarial verification / synthesis | reasoning-dense | author-session model | author family, deepest available | degrade |
| **Independent review (this gate)** | reasoning-dense | newest available Codex reviewer model at its highest supported effort, inherited from global config in a separate read-only session | operator-selected next newest/deepest available Codex reviewer in a separate read-only session | record the fallback or defer the review; continue unrelated work |

Constraints:

- Review independence is session/author separation: the implementation author cannot certify their own
  change, but a distinct read-only Codex reviewer session may use the same model family. Cross-model
  review remains preferred when its transport is operational because it adds heterogeneous blind-spot coverage.
- Every fired fallback or deferred gate is recorded in the review artifact metadata (and the
  residual ledger for deferrals), so a silent downgrade cannot become a hidden fallback.
- Interrupted fan-outs resume from their run id (re-dispatch only unfinished lanes); a completed
  agent's output is never re-rolled by a full rerun.
- Initial assignments dated 2026-07-09; owner changed the independent lane to Codex-first and explicitly
  non-blocking for unrelated development on 2026-07-10. The same playbook sync retired project-level
  model/effort pins in favor of global latest+max inheritance. Revisit global config when the model lineup
  changes; do not edit the runner or active docs to chase model versions.

## Required Evidence

The review output must record:

- Reviewed files or diff scope.
- Contract docs considered.
- Validation already run by the author.
- Command, `reviewer_exit_code=0`, Codex CLI/session/thread identity, global-config digest, rollout path/digest, and effective model, reasoning effort, and service tier used for the independent review.
- A hash-bound `independent_review_effective_config_v2` JSON artifact with prompt/events/raw-output/rollout provenance, a recomputable pinned-Git scope digest, and an empty model-reroute chain.
- Blocking findings or `GO` / `NO-GO`.
- Accepted exceptions, if any, with user/founder approval context.
- Residual risks that should be checked in W6/nightly, live validation, or manual product review.

Store outputs under `runtime/reviews/<stamp>_<title>.md` or attach them to the PR/review thread. For milestone signoff, reference the artifact path from `docs/NEXT_TODO.md`, the relevant Contract doc, or the PR checklist. Do not treat an unrecorded chat answer, author summary, or normal implementation note as review evidence.
A bare `NO-GO` without at least one prioritized finding is an invalid review artifact; rerun with a narrower scope or clearer prompt instead of treating it as a valid blocked review.
Provider-costing live gates and milestone signoff runners must call the shared artifact verifier, not just check file existence or parse the final word: valid evidence is a runner-produced review artifact under `runtime/reviews/` with a zero reviewer exit, hash-verified effective-config/rollout evidence, complete metadata, matching scope/title tokens and required files/contracts, and a final `GO` verdict. `NO-GO`, timeout, no-output, nonzero-exit, rerouted, invalid, prompt-file, missing/tampered evidence, missing-scope-file, or unrelated artifacts fail closed.

## Independence Rule

The implementation author cannot self-certify. A valid reviewer must be a separate read-only Codex exec session, another coding agent, or another model/tool configured to inspect the diff without mutating code. The reviewer may use local read-only commands and tests already produced by the author as evidence, but must independently judge GO/NO-GO.

## Scope Discipline

Independent review is scoped review, not a full development-session resume. The reviewer should inspect the listed files/diff and targeted Contract sections with `git diff -- <files>`, `rg`, and line-range reads. It should not read full `PROGRESS.md`, full `docs/NEXT_TODO.md`, or full long Contract docs unless the review scope explicitly requires full context. This keeps the gate fast enough to run before live/W6/manual signoff instead of becoming another expensive matrix.

## Non-Goals

- This gate does not replace tests.
- This gate does not authorize live provider calls.
- This gate does not approve contract direction changes that need user/founder confirmation.
- This gate must not mutate code or run broad live workflows; it is read-only review.

## Failure Policy

`NO-GO` findings must be resolved or explicitly accepted by the user/founder before the affected scope is promoted, run through live/W6/manual validation, or signed off. Unrelated modules and later pinned batches may continue while that debt is fixed asynchronously. A green targeted test, W6/nightly, or browser/manual pass does not override the scope-local block. If an exception is accepted, it must be named in the review artifact and carried in the next milestone TODO until retired.
