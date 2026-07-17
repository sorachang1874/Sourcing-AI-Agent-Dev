# Track D D1n S1d — result link-policy carrier

Date: 2026-07-17

Status: validated fixed-forward author candidate, ready for a standalone commit and fresh pinned review. This batch
separates physical result-link semantics in the immutable tool contract and durable result aggregate. It does not
implement the `start_acquisition_run` approval/start UoW, create an Activity, serve a tool, or authorize
provider/model/live execution.

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
- The isolated registry therefore has four names and five historical specs; the public/default served population
  remains zero.

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

No universal default is semantically correct. Old binaries that omit the required column fail closed after `0013`;
rolling overlap would require a separate reviewed trigger/sentinel bridge. This repository can use the quiesced path
because no Agent tool is served.

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
- new start v3 fingerprint and registry 4-name/5-history shape;
- positive and negative Python matrices for all three policies;
- policy exact-copy through pending slot, accepted/quarantined attempt, accepted slot, journal, and replay;
- `0012 -> 0013` deterministic backfill for all historical effect classes;
- old-writer omission, unknown policy, link mismatch, immutable collision, and deferred aggregate mismatch fail closed;
- S1a/S1b/S1c regression, scoped mypy/lint, global `81 errors / 4 files` ceiling, Python compilation, and clean diff.

## Fresh pinned review scope

Review base: `bacae9e80c8c0593b6c8c436b06e68777ade6d54`. The head is the eventual standalone S1d commit. Intended scope:

- `docs/NEXT_TODO.md`
- `docs/TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md`
- `docs/TRACK_D_D1N_S1C_OPAQUE_OWNER_REVISION_CARRIER_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_S1D_RESULT_LINK_POLICY_IMPLEMENTATION.md`
- `src/sourcing_agent/agent_canary_registry.py`
- `src/sourcing_agent/agent_tool_registry.py`
- `src/sourcing_agent/agent_tool_result_postgres.py`
- `src/sourcing_agent/agent_tool_result_slot.py`
- `src/sourcing_agent/migrations/0013_agent_tool_result_link_policy.sql`
- `src/sourcing_agent/repositories/workflow_runtime.py`
- `tests/test_d1n_agent_tool_registry.py`
- `tests/test_d1n_agent_tool_result_slot.py`
- `tests/test_d1n_agent_tool_result_slot_uow.py`
- `tests/test_d1n_canary_agent_tool_population.py`
- `tests/test_migration_runner.py`

All results in this document are author evidence until a fresh pinned non-author artifact says otherwise.

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

## Author validation evidence

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
