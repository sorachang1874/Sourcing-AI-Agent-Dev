# Track D D1n S1d — result link-policy carrier

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Date: 2026-07-17

Status: the correctly scoped pinned Ultra review of `bacae9e..3b235fd` was a valid `NO-GO 0/1/4/0`. The bounded
fixed-forward commit `4dddd0e` received a second valid pinned Ultra `NO-GO 0/3/3/0`; its three P1 and three P2 new
findings are addressed author-side by the next bounded response, whose fresh pinned review remains pending. Neither
artifact is a formal `GO`. This batch separates physical result-link semantics in the immutable tool contract and
durable result aggregate. It does not implement the
`start_acquisition_run` approval/start UoW, create an Activity, serve a tool, or authorize provider/model/live
execution.

## Engineering goal

S1a originally inferred one link shape from `effect_class`: every command-backed result required a terminal
ActivityRun/ActivityAttempt. That is wrong for `start_acquisition_run`: its model-safe success result acknowledges an
exact queued `acquisition.run.create` command before downstream orchestration creates an Activity.

S1d makes the link interpretation explicit and registry-owned:

| Policy | Exact terminal link shape |
| --- | --- |
| `no_command_v1` | no command or Activity refs; all command fences are zero |
| `workflow_command_acceptance_v1` | Action, Operation, and exact WorkflowCommand present; Activity refs absent; command attempt, claim generation, and control epoch are zero |
| `activity_attempt_terminal_v1` | Action, Operation, WorkflowCommand, ActivityRun, and ActivityAttempt present with the existing positive command fences |

For `no_command_v1`, a commandless action still requires its Action/Operation pair; a read-only query permits either
both absent or both present. No other combination is valid.

## Ownership and derivation

- source of truth: immutable `AgentToolBehavior` in one exact historical `AgentToolSpec`;
- allowed values: the three closed policies above;
- caller authority: none; `AgentToolTerminalResult` cannot supply or override policy;
- derivation: an `AgentToolOccurrence` exact-copies the policy from the pinned historical tool spec;
- persistence trust root: reserve, read-only prepare, and shared terminal acceptance resolve the exact
  `(tool_name, tool_spec_version, tool_spec_digest)` from the server-owned historical registry, rebuild the occurrence,
  and exact-compare every spec-derived pin before schema bootstrap, owner reads, or result writes;
- persisted consumers: result slot, every accepted/quarantined attempt, and immutable journal;
- enforcement: occurrence validation, exact replay/collision checks, PostgreSQL row checks, slot mutation guard, and
  deferred accepted-aggregate validation;
- fallback: unknown, absent after cutover, or shape mismatch fails closed before a terminal aggregate commits;
- deletion condition: a policy version remains lookup-capable while any retained historical tool spec, occurrence,
  attempt, or journal references it.

`result_link_policy` is not added directly to `logical_occurrence_digest`: the already-pinned `tool_spec_digest`
binds it for new spec-schema v2 records, while existing v1 digests must remain byte-identical.

## Historical fingerprint strategy

- Existing `agent_tool_spec_v1` fingerprints stay byte-identical. Their effective policy is a frozen mapping:
  read-only/commandless to `no_command_v1`, command-backed to `activity_attempt_terminal_v1`.
- `agent_tool_spec_v2` includes the explicit policy in the behavior fingerprint.
- Historical `start_acquisition_run_tool_v2` remains lookup-capable with its original digest and legacy Activity
  policy.
- A new `start_acquisition_run_tool_v3` carries `workflow_command_acceptance_v1`; its policy-bearing simulate fixture
  uses `local_agent_simulate_fixture_v2`, while the v2 fixture remains byte-identical under fixture schema v1.
- At the S1d checkpoint the isolated registry had four names and five historical specs; the later S1b versioned
  inspect response retains those identities and adds inspect v1/v3 history, producing four names/seven specs. The
  public/default served population remains zero.

This avoids publishing the same `(tool_name, tool_spec_version)` with a different digest and avoids rotating the
unchanged plan/filter/inspect historical identities.

## Migration and rollout contract

Migration `0013_agent_tool_result_link_policy.sql` performs a quiesced fixed-forward cutover:

1. add nullable, no-default policy columns to slot/attempt/journal;
2. backfill existing slots deterministically from their checked effect class;
3. exact-copy the slot policy to attempts and journal;
4. set all three columns `NOT NULL` without installing a universal default;
5. replace link-shape, immutable-slot, and deferred aggregate checks with policy-aware exact checks.

The migration also fixed-forwards the S1c payload-family invariant: attempt/journal v2 requires a nonempty opaque
revision token, while numeric-only payloads remain v1. A preexisting noncanonical numeric-only v2 row fails the
quiesced migration atomically. A separate deferred constraint trigger requires every accepted or quarantined attempt
to exact-match its owning slot policy; quarantined evidence cannot select a different policy merely because it is
outside the accepted aggregate.

Migration `0014_agent_tool_result_attempt_effect_contract.sql` fixed-forwards the remaining all-attempt database
shape gap. Under a bounded table lock it first rejects any retained attempt whose link shape is inconsistent with its
owning slot's immutable effect class and policy, then replaces the existing deferred validator. This applies to both
accepted and quarantined attempts: commandless actions require their Action/Operation pair, read-only rows permit
only the documented both-empty or both-present pair, and both command-backed policies retain their exact closed
shapes. It adds no default, serving state, or live authority.

No universal default is semantically correct. Old binaries that omit the required column fail closed after `0013`;
rolling overlap would require a separate reviewed trigger/sentinel bridge. This repository can use the quiesced path
because no Agent tool is served.

The operator procedure is exact and non-rolling: quiesce all result-aggregate writes, drain old replicas, apply
`0012 -> 0013 -> 0014`, recycle every pool/session/prepared statement, start only the compatible release, verify the
same registry/retained-set digest on every replica, and only then CAS-activate action-contract v2 with current tool-spec
v3. Historical `start_acquisition_run_tool_v2` is retained for exact lookup/replay only; it is never reactivated for new
submission. Activation does not imply serving, and the public/default served population remains zero.

The existing slot and attempt/journal schema carriers keep their scoped meanings: slot v1 binds logical occurrence,
while attempt/journal v1/v2 distinguish numeric-only versus opaque-token terminal owner payloads. Link-policy schema
identity is carried by the versioned policy value and, for new tools, by `agent_tool_spec_v2`; `0013` does not
reinterpret the S1c terminal-payload families.

## Deliberate S1e boundary

S1d does not guess the missing start authorities. Before the start PG UoW can land, a bounded contract batch must
ratify:

- the exact physical `ActionApproved` event encoding for `acquisition_confirmation_receipt.v1`;
- the exact `OperationCommandPlanned` event/sequence mapping that wins command acceptance;
- the durable parent-budget reservation owner and identity.

The existing generic approve/dispatch path is multi-transaction and builds the historical acquisition-root payload;
it is not the V2 start authority.

## Required evidence before commit

- all existing v1 tool digests remain exact;
- new start v3 fingerprint and the S1d-checkpoint registry 4-name/5-history shape;
- positive and negative Python matrices for all three policies;
- policy exact-copy through pending slot, accepted/quarantined attempt, accepted slot, journal, and replay;
- quiesced `0012 -> 0013 -> 0014` deterministic backfill and all-attempt enforcement for every historical effect class;
- old-writer omission, unknown policy, link mismatch, immutable collision, and deferred aggregate mismatch fail closed;
- S1a/S1b/S1c regression, scoped mypy/lint, global `81 errors / 4 files` ceiling, Python compilation, and clean diff.

## Pinned review records and next request

### Historical first request (do not reuse)

The former reusable "Fresh pinned review scope" based at
`bacae9e80c8c0593b6c8c436b06e68777ade6d54` is historical. It described the predecessor 15-file S1d request before
the S1a wording correction, canonical pre-Agent owner matrix, migration `0014`, and governing preflight existed. It
must not be copied into a later review request. Its completed independent artifact is
`runtime/reviews/20260717T130538Z_Track-D-D1n-S1d-result-link-policy.md`, with valid verdict
`NO-GO 0/1/4/0`; it is not a formal `GO`.

### Completed fixed-forward re-review

The completed independent Ultra re-review is exact-bound as follows:

- artifact: `runtime/reviews/20260717T135248Z_Track-D-D1n-S1d-fixed-forward-re-review.md`;
- base: `5aa393699075df41182101e20ea0c9cad81b2762`;
- head: `4dddd0e93634fcb4920f3f15a8c1984ca239a6f3`;
- model / effort / tier: `gpt-5.6-sol` / `ultra` / `priority`;
- reviewer exit: `0`;
- diff SHA-256: `3886c965622cffe7d7d3b1f2627342fba30437ced6714f0fcb0bdc457a12ad6a`;
- tree SHA-256: `27a792df738ec655fd61664f7d5d7d49a59807943b33dfbb85eac4bf1b4cbf12`;
- extra-context SHA-256: `f2a40036d68139e1b84ab0a982d442e7601b779d1d023dbe18299ee0846696c4`;
- scope digest SHA-256: `013c8d1e2d062fb778f692c129bc3d320e248c9d07a17ca0f202fbb9a5f32c08`;
- valid verdict: `NO-GO 0/3/3/0`, with five re-raises and residuals R-019/R-029; this is not a formal `GO`.

Its exact 12-file reviewed scope was:

- `docs/NEXT_TODO.md`
- `docs/PRE_AGENT_CONTRACT_REVIEW.md`
- `docs/TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md`
- `docs/TRACK_D_D1N_S1A_AGENT_TOOL_RESULT_SLOT_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_S1D_RESULT_LINK_POLICY_IMPLEMENTATION.md`
- `src/sourcing_agent/agent_tool_result_postgres.py`
- `src/sourcing_agent/agent_tool_result_slot.py`
- `src/sourcing_agent/migrations/0014_agent_tool_result_attempt_effect_contract.sql`
- `tests/test_d1n_agent_tool_result_slot.py`
- `tests/test_d1n_agent_tool_result_slot_uow.py`
- `tests/test_migration_runner.py`
- `tests/test_pre_agent_contract_review.py`

This scope explicitly reviewed the `0014` brownfield/all-attempt enforcement, S1a historical/current start semantics,
the canonical owner matrix, and its executable preflight. Those artifacts are evidence for that exact range only.

### Completed canonical-owner response review

The next independent Ultra review exact-bound the complete author response:

- artifact: `runtime/reviews/20260717T150637Z_Track-D-D1n-S1d-canonical-owner-response.md`;
- base: `28c2b2eb6282462fa5e1a7003590473119e4a30e`;
- head: `f4f3e58ddce3342f8f7e2e0b88c30898f510fcb1`;
- model / effort / tier: `gpt-5.6-sol` / `ultra` / `priority`;
- reviewer exit: `0`;
- scope digest SHA-256: `7fd287ab4d9cfdd4ec5ffa88e523b3cfebd4482b98c6deed42b5651b5dad71ba`;
- valid verdict: `NO-GO 0/1/1/0`, plus residuals R-019/R-029; this is not a formal `GO`.

Its P1 found that plan occurrence canonicalization discarded surplus request-root fields before exact Action binding,
including the stale-generation quarantine path. Its P2 found that the serving runbook falsely described the existing
`0012 -> 0013 -> 0014` sequence as deferred `NOT VALID` validation even though those migrations synchronously
validate, backfill, and scan during quiescence.

### Next fixed-forward response request

- review base: `f8a72c75b299825d3c14584751a1586adb51dff7` (the intervening S1b response commit is the direct
  parent; the valid `f4f3e58` S1d artifact remains required finding context, not part of this response diff);
- review head: `<commit-containing-the-complete-S1d-P1-P2-response>`.

Replace the head placeholder with the exact committed SHA, derive the file list and scope digest from that pinned range,
and include every response file. Do not reuse either historical file list. Until a fresh pinned non-author artifact for
that exact range returns `GO`, all new validation remains author evidence, the current verdict remains `NO-GO`, and
served population remains zero.

## Review-driven fixed-forward work

The bundled Ultra S1c run completed with valid model/effort/tier evidence but an incorrectly comma-joined file scope,
so its artifact is reference-only and cannot be a formal verdict. Its concrete code/doc findings were nevertheless
reproduced and fixed in this successor scope:

- numeric-only attempt/journal payloads are canonical v1; v2 now requires a nonempty opaque token;
- `0012` and token-only activation are documented as quiesced/pool-recycled, not rolling-overlap safe;
- membership/publication value ownership, adapter exact-copy responsibility, result-writer ownership, forbidden
  consumers, and the missing physical-owner preflight gate are separated explicitly;
- optional identifiers require exact string type before the empty sentinel;
- forged plan-owner token/generation diagnostics name the mismatched carrier and remain zero-write.

The earlier local S1d adversarial P1 for mismatched quarantined-attempt policy was also reproduced against PostgreSQL
and closed by the all-attempt constraint trigger plus fresh-connection zero-write assertions. The resolved dirty-tree
prereview reported `P0/P1/P2/P3=0/0/0/0`, but it includes subdiff authors and is not independent or formal.

The fresh correctly scoped pinned review at
`runtime/reviews/20260717T130538Z_Track-D-D1n-S1d-result-link-policy.md` then returned valid
`NO-GO 0/1/4/0`. Its bounded fixed-forward response is:

- P1 historical-spec trust root: persistence uses the server-owned exact historical registry at reserve, prepare,
  and acceptance; compatible v2/v3 policy swaps and forged request/result/serializer pins fail closed before effects;
- P2 quarantined commandless shape: `0014` validates brownfield rows and extends the deferred all-attempt trigger to
  the slot effect-class/policy matrix;
- P2 stale S1a start wording: historical start v2 remains Activity-terminal, while current v3 terminates at exact
  WorkflowCommand acceptance;
- P2 stale S1a rollout wording: `0012` is explicitly quiesced/pool-recycled, with old replicas drained before
  token-only activation;
- P2 owner/preflight omission: the canonical pre-Agent matrix now assigns the PG-only result aggregate, link policy,
  and inspect readiness owners and names a fast executable preflight.

The fresh re-review at
`runtime/reviews/20260717T135248Z_Track-D-D1n-S1d-fixed-forward-re-review.md` exact-bound `5aa3936..4dddd0e` and
returned valid `NO-GO 0/3/3/0`: equality-alias carriers, plan occurrence/Action binding, the contradictory migration
procedure, governing durable inventory, and stale review-scope instructions require fixed-forward work. R-019 and
R-029 remain residual; no tool is served and no provider/model/live call is authorized.

The current author-side response closes those six findings without claiming review success:

- closed literals and all three JSON carriers require exact plain strings; registry revalidation returns a
  server-reconstructed canonical occurrence, while terminal revalidation regenerates canonical JSON/digest bytes;
- reserve, plan acceptance, inspect prepare, and inspect acceptance reject equality-alias carriers before schema
  bootstrap, owner reads, or result effects, with pending-slot/zero-attempt/zero-journal PostgreSQL evidence;
- one pure acquisition-plan request canonicalizer is shared by preview construction and result acceptance, so legal
  order-insensitive input remains accepted while the canonical occurrence is exact-bound to strict-decoded locked
  Action input/target plus requester and workspace before any terminal or quarantine write;
- the rollout procedure is exact quiesce, old-replica drain, `0012 -> 0013 -> 0014`, pool/session recycle,
  compatible-release start, registry-digest verification, then per-action CAS activation; historical tool v2 remains
  lookup/replay-only;
- both governing PG-only inventories name slot/attempt/journal, executable preflight enforces them, and obsolete review
  instructions are historical rather than reusable.

The subsequent `f4f3e58` review response additionally rejects any plan occurrence whose root is not exactly the two
plain-string keys `input_payload` and `target_ref` before schema bootstrap, connection acquisition, owner access, or
normal/stale terminal effects. The runbook now states the real synchronous validation/backfill/scan and lock-budget
work performed by `0012`, `0013`, and `0014`; a future `NOT VALID` strategy would require a separately named review.

## Fixed-forward author validation evidence

Run from the repository root with local PostgreSQL and no live provider/model variables:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_pre_agent_contract_review.py
=> 179 passed

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py
=> 54 passed + 44 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q --tb=short \
  tests/test_d1n_action_contract_activation.py \
  tests/test_d1n_action_contract_identity.py \
  tests/test_d1n_action_result_schema.py \
  tests/test_d1n_acquisition_plan_preview.py \
  tests/test_d1n_acquisition_plan_preview_uow.py \
  tests/test_d1n_start_acquisition_v2.py \
  tests/test_d1n_projection_query_contracts.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_migration_runner.py \
  tests/test_pre_agent_contract_review.py
=> 807 passed + 119 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/acquisition_plan_preview.py \
  src/sourcing_agent/agent_tool_result_slot.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  tests/test_d1n_acquisition_plan_preview.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py
=> success, 0 issues in 7 files

make lint
=> 58 files already formatted; all checks passed

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

py_compile on all 8 changed Python files
git diff --check
=> clean
```

The latest direct-child response additionally ran:

```text
full tests/test_operation_runtime.py
=> 193 passed + 592 subtests

full result-slot + inspect-result PG files plus historical fixtures
=> 70 passed + 52 subtests

full tests/test_pre_agent_contract_review.py
=> 62 passed

exact normal/stale extra-root rejection plus legal order-insensitive request
=> 3 passed

historical v1 reason-bearing probe and frozen v1/v2 bytes
=> 2 passed

make lint
=> 58 files already formatted; all checks passed

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

py_compile on all 9 changed Python files; git diff --check
=> clean
```

These remain author results. Reviewer transport/cache diagnostics produced no valid verdict, and the later
filtered-cache execution stopped on server-side `usageLimitExceeded`; S1d therefore remains `NO-GO` pending a fresh
pinned non-author review of the committed response.

## Earlier predecessor author validation evidence

Run from the repository root with local PostgreSQL and no live provider/model variables:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q --tb=short \
  tests/test_migration_runner.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py
=> 169 passed + 74 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q --tb=short \
  tests/test_d1n_action_contract_activation.py \
  tests/test_d1n_action_contract_identity.py \
  tests/test_d1n_action_result_schema.py \
  tests/test_d1n_acquisition_plan_preview.py \
  tests/test_d1n_acquisition_plan_preview_uow.py \
  tests/test_d1n_start_acquisition_v2.py \
  tests/test_d1n_projection_query_contracts.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_migration_runner.py
=> 677 passed + 78 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/agent_tool_registry.py \
  src/sourcing_agent/agent_canary_registry.py \
  src/sourcing_agent/agent_tool_result_slot.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py
=> success, 0 issues in 8 files

make lint
=> 58 files already formatted; all checks passed

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m py_compile \
  src/sourcing_agent/agent_tool_registry.py \
  src/sourcing_agent/agent_canary_registry.py \
  src/sourcing_agent/agent_tool_result_slot.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  src/sourcing_agent/repositories/workflow_runtime.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_migration_runner.py
git diff --check
=> clean
```
