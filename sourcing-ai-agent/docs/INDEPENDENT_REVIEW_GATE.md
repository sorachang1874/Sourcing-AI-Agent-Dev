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

## Gate Order

1. Freeze or update the relevant contract first, including owner/source-of-truth/fallback/deletion semantics.
2. Run targeted tests and fast preflight. Expensive W6/nightly must not be the first detector for basic Contract drift.
3. Generate the independent review prompt from `docs/INDEPENDENT_REVIEW_BRIEF.md`.
4. Run reviewer in read-only mode with no stdin blocking.
5. Address blocking findings or record an explicit user/founder-accepted exception.
6. Only then run expensive W6/nightly, live provider validation, manual product signoff, or milestone closeout.

## Codex Reviewer Command

Use `codex exec` in read-only mode and write the review to a file. The runner writes the prompt to `runtime/reviews/*.prompt.md` and feeds that prompt file to Codex, so it does not wait for interactive stdin. Do not pipe through `head` or `tail`.

```sh
make independent-review-gate \
  REVIEW_TITLE="W7g Public Web model-provider hardening" \
  REVIEW_FILES="src/sourcing_agent/model_provider.py tests/test_model_provider.py docs/PRE_AGENT_CONTRACT_REVIEW.md" \
  REVIEW_EXECUTE=1
```

Equivalent direct shape. The Make/script path enforces the timeout through Python so it works on macOS without GNU `timeout`; add an external `timeout`/`gtimeout` only if your shell has it.

```sh
codex exec \
  --cd . \
  --sandbox read-only \
  --model gpt-5.5 \
  -c model_reasoning_effort='"xhigh"' \
  --output-last-message runtime/reviews/<review-id>.md \
  - \
  < runtime/reviews/<review-id>.prompt.md
```

If `gpt-5.5` is unavailable, use another independent reviewer model and record that substitution in the review output. The author still cannot self-certify.
The Make target defaults `REVIEW_MODEL=default`, which lets the installed Codex CLI choose its currently available default model. Set `REVIEW_MODEL=gpt-5.5` or another explicit model only when that model is supported in the local reviewer environment.

## Required Evidence

The review output must record:

- Reviewed files or diff scope.
- Contract docs considered.
- Validation already run by the author.
- Command/model used for the independent review.
- Blocking findings or `GO` / `NO-GO`.
- Accepted exceptions, if any, with user/founder approval context.
- Residual risks that should be checked in W6/nightly, live validation, or manual product review.

Store outputs under `runtime/reviews/<stamp>_<title>.md` or attach them to the PR/review thread. For milestone signoff, reference the artifact path from `docs/NEXT_TODO.md`, the relevant Contract doc, or the PR checklist. Do not treat an unrecorded chat answer, author summary, or normal implementation note as review evidence.
A bare `NO-GO` without at least one prioritized finding is an invalid review artifact; rerun with a narrower scope or clearer prompt instead of treating it as a valid blocked review.
Provider-costing live gates and milestone signoff runners must validate the artifact verdict, not just file existence: valid evidence is a runner-produced review artifact under `runtime/reviews/` with complete metadata, matching scope/title tokens and required files/contracts for the gate being unlocked, and a final `GO` verdict. `NO-GO`, timeout, no-output, invalid, prompt-file, missing-metadata, missing-scope-file, or unrelated artifacts must fail closed.

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

`NO-GO` findings must be resolved or explicitly accepted by the user/founder before proceeding. A green targeted test, W6/nightly, or browser/manual pass does not override a blocking independent review. If an exception is accepted, it must be named in the review artifact and carried in the next milestone TODO until retired.
