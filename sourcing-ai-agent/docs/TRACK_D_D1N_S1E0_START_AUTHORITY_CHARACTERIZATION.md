# Track D D1n S1e0 — acquisition-start authority characterization

> Status: Current non-live decision-lock characterization (2026-07-17). This batch adds no product writer,
> migration, registry activation, provider/model call, or serving authority. It is author evidence only; it is not an
> independent-review `GO` and does not authorize S1e1 implementation to guess a physical schema.

## Outcome

S1e0 freezes the current physical facts that S1e1 must replace for the isolated local Agent
`start_acquisition_run` path. It deliberately does not choose a storage schema or reinterpret the existing W11
approval/dispatch implementation as the V2 authority.

The characterized statuses are:

```text
approval_receipt_physical_owner_status=characterized_not_ratified
command_acceptance_winner_status=characterized_not_ratified
parent_budget_physical_owner_status=declared_policy_pin_only
cost_reservation_status=decision_locked_not_implemented
default_public_agent_served_population=0
```

The existing pure V2 contract remains useful input: it defines a closed preview reference,
`acquisition_confirmation_receipt.v1`, the exact root-command payload, and the revisioned model-safe result. None of
those values currently has a complete specialized PostgreSQL start UoW.

## Scope and non-goals

This batch does exactly two things:

1. record the current approval, command-planning, budget, and serving facts;
2. install a fast characterization test that fails when any of those facts changes, forcing the S1e1 owner decision
   and test to change in the same commit.

It does **not**:

- edit `operation_runtime.py`, `orchestrator.py`, a repository, a registry, or a migration;
- select `agent_actions.budget_json`, `operation_runs.cost_budget_json`, or a JSON payload as a reservation owner;
- select the future D3 `cost_reservations` decision surface as the acquisition parent-budget owner;
- create or accept an Agent result slot;
- change historical `acquisition_root_request_v1` or activate `acquisition_root_request_v2` in the default action
  registry;
- populate the default Agent registry, run a provider/model, or authorize simulate/scripted/live execution.

## Current fact A — generic `ActionApproved` is not the V2 receipt authority

The pure contract defines `AcquisitionConfirmationReceipt`, including the complete preview, requester, Cohort,
manifest, budget, schema/tool pins, actor, policy, timestamp, and digest. The current generic
`OperationRuntimeWriter.approve_action` path does not persist that record.

Its normalized AST digest at this checkpoint is:

```text
OperationRuntimeWriter.approve_action
sha256=06c52f165740401ad7675b9e99e75d60461e50c041fbdc86c087be208f8e6f48
```

The current write order is four separate repository calls:

```text
update_action_state
-> append_operation_event(ActionApproved)
-> upsert_operation
-> append_operation_event(OperationRunQueued)
```

The `ActionApproved` row uses the Action stream, omits `operation_run_id`, relies on default
`operation_event_v1`, and its payload contains exactly:

```text
action_type, owner_module
```

It does not carry `acquisition_confirmation_receipt.v1`, receipt id/digest, requester, preview identity, full budget,
request/result/tool pins, or the approved start snapshot. It therefore cannot be relabeled as the V2 receipt, and its
multi-call sequence cannot satisfy the required exact-preview approval plus OperationRun/command/budget atomicity.

## Current fact B — generic planned event is not a ratified command-acceptance winner

The production action registry still maps historical `start_acquisition_run` to `acquisition.run.create`, owned by
`acquisition_run_writer` at stage `acquisition_run_create`. The isolated local canary separately declares current
`start_acquisition_run_tool_v3` with `workflow_command_acceptance_v1`; that declaration is not a physical adapter.

The normalized current AST digests are:

```text
SourcingOrchestrator._dispatch_agent_callable_workflow_command_operation
sha256=0942086c6b68218f2c002bb2f1c7ffe1c1a243cc5f4ad8eec3fe7edc923c5b60

SourcingOrchestrator._plan_agent_callable_workflow_command
sha256=6e992fdfdb908288e72005aca09c5d4d352420f443bcdc15cf362ac82a3634d5
```

The generic planner first appends and reduces `WorkflowStarted`, then separately appends and reduces
`CommandPlanRequested`; that reducer path creates or reloads the WorkflowCommand. Only after it returns does generic
dispatch perform:

```text
update_operation_state
-> update_action_state
-> append_operation_event(OperationCommandPlanned)
```

The `OperationCommandPlanned` caller supplies no exact sequence number, so the operation-event writer allocates it.
Its payload expands the four-field workflow ref and adds only `module_state_mutated` plus `migration_phase`. It has no
confirmation receipt ref, parent-budget reservation ref, start-snapshot digest, or result-occurrence identity.

The WorkflowCommand is therefore already produced from a different workflow-event boundary before the
`OperationCommandPlanned` event exists. S1e0 does not choose which row/event becomes the S1e1 acceptance winner, how
its sequence is allocated and exact-replayed, or how command/event/source causality avoids a cycle. Those are
`characterized_not_ratified` decisions.

## Current fact C — parent-budget physical ownership is unresolved

The current Agent tool fingerprint declares:

```text
mode=parent_reservation_required
owner_id=acquisition.parent_budget_reservation
owner_revision=acquisition_parent_budget_v1
owner_contract_digest=50c72166a683f8a49826bab1af82fdc0922026d5993a3725403582dff24c5670
```

That is a policy/owner pin, not durable reservation evidence. Current physical inventory has:

- `agent_actions.budget_json` and `operation_runs.cost_budget_json` projections;
- no `cost_reservations`, `parent_budget_reservations`, or `acquisition_budget_reservations` table descriptor;
- no migration that creates those tables or a `budget_reservation_ref` column;
- no budget/reservation method on `WorkflowRuntimeRepository` or `LiveControlPlanePostgresAdapter`;
- no physical reservation ref on the current WorkflowCommand descriptor.

The D3 cost-ledger contract has separately decision-locked future `cost_reservations` and `dispatch_exposures`, but
its implementation status remains `decision_locked_not_implemented` under OB-2.2/OB-10.3. S1e0 does not assert that
the acquisition parent budget is the same aggregate, nor invent a second aggregate. S1e1 must first decide their
relationship, including whether one is a parent policy envelope and the other a per-transport cost ledger, and must
name one physical source of truth for every shared field.

## Owner/source-of-truth matrix at this checkpoint

| Field/object | Single owner | Physical SOT | Allowed values | Derivation rule | Consumers | Forbidden consumers | Fallback/brownfield status | Migration status | Deletion condition |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `acquisition_confirmation_receipt.v1` | pure builder exists; physical writer unratified | none accepted; generic `ActionApproved` is insufficient | exact closed receipt record only | rebuild from locked preview + pending Action + authenticated human/open operator | future start UoW, root command, result serializer | model, caller-supplied receipt, generic event relabeling | fail closed; `characterized_not_ratified` | none | N/A until physical owner ratified |
| command-acceptance winner | physical writer unratified | current command and planned event are split across workflow/operation calls | future exact queued root command plus one exact winner mapping | must derive under one bounded lock/UoW from accepted receipt and start snapshot | future result owner/serializer, Operation projection | mutable latest command, caller event id/sequence, old multi-call dispatch | fail closed; `characterized_not_ratified` | none | retain while any occurrence/result/audit refers to it |
| parent-budget reservation | registry policy owner pin only | none | future closed reservation identity and lifecycle | must derive from approved immutable start snapshot; no caller money/ref | future start dispatch, live predecessor, audit/cost reconciliation | Action/Operation JSON projection as authority; model/caller; guessed D3 reuse | fail closed; `declared_policy_pin_only` | D3 ledger is separately `decision_locked_not_implemented` | retain through all child exposure/audit references |
| default/public Agent serving | default registry | empty default registry | `served=0` only | no local harness declaration derives hosted serving | public/hosted release gate | S1e0, fixture presence, author tests | disabled | no activation | separate hosted gate only |

## Mechanism × design-invariant matrix

| Mechanism | 1 single writer | 2 tenant key | 3 generation/fence | 4 lifecycle | 5 late/partial | 6 cost honesty | 7 physical identity | 8 provenance | 9 consistency | 10 runtime/mode isolation |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| V2 approval receipt | open for S1e1 | workspace present in generic event; requester absent | no accepted receipt fence | generic approval only; V2 lifecycle open | conflict/replay must be specified | binds proposed budget but reserves nothing | full receipt identity is pure-only | generic actor/source insufficient for V2 record | this doc matches D1n/S1d boundary | runtime/mode absent from generic receipt event |
| command acceptance | old generic writers observed; final owner open | Operation workspace is indirect | current queued command has no S1e result winner fence | command and planned event can split | cancel/retry/late winner remains R-019-sensitive | no reservation ref | workflow source event and later operation event are distinct | current planned event carries only workflow ref | exact split is characterized, not accepted | dormant D3 columns do not authorize this path |
| parent budget | owner pin only | physical tenant key absent | no reservation generation/CAS | no physical lifecycle | no release/reconcile owner | unresolved; cannot claim cost honesty | no durable reservation identity | approved snapshot is future input, not evidence | relation to D3 ledger explicitly open | OB-2.2/OB-10.3 remain open |
| S1e0 characterization | test file is sole writer | N/A | AST/inventory digests detect drift | replaced by S1e1 evidence when implemented | N/A | no cost/provider call | exact current source objects | author evidence only | two-file scope is explicit | asserts default served population zero |

## S1e1 start gate

S1e1 may not add a PG writer until a follow-up owner decision specifies all of the following without placeholders:

1. exact receipt row/event schema, stream, event id, sequence, digest, timestamp owner, replay key, and immutable
   comparison fields;
2. exact pending Action physical mapping, requester ownership carrier, and approval-state CAS;
3. exact WorkflowCommand creation/source-event relation and the `OperationCommandPlanned` winner id/sequence mapping;
4. one durable parent-budget table/owner/identity/lifecycle, or an explicit reviewed reuse of an already implemented
   owner—not a future decision document;
5. one overall PG deadline and D1n lock order, fault points, lost-ACK replay, concurrency winner, outbox/wakeup rule,
   and zero-write foreign/stale/conflict matrices;
6. exact `workflow_command_acceptance_v1` result owner ref/digest/revision and shared result-slot acceptance mapping;
7. runtime namespace/provider-mode behavior and explicit non-closure of the Plan §6#6 action-root durable-scope gate,
   R-019, R-029, OB-2.2, OB-10.3, and OB-10.4 where they remain outside this slice.

No generic `approve_action()`/`_dispatch_agent_callable_workflow_command_operation()` call chain is an acceptable
implementation of that gate.

## Served and review boundary

S1e0 leaves:

- production action partition unchanged at `10 schema-defined / 5 schema-less`;
- default/public Agent tool population at `served=0`;
- the isolated local canary registry declaration-only;
- `start_acquisition_run` and `filter_projection` physical terminal-success work open;
- R-019, R-029, the Plan §6#6 action-root durable-scope gate, OB-2.2/10.3/10.4, S2/S3, L1/L2, and paid TML open;
- provider/model/live invocation count at zero.

This contract-heavy two-file scope requires a fresh pinned non-author review before it can be treated as accepted
design evidence. Its author test is not a formal `GO`. After the review request is recorded, S1e1 non-live work may
continue asynchronously, but live/served/milestone signoff remains fail closed.

## Author validation command

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
  .venv/bin/python -m pytest -q \
  tests/test_d1n_s1e0_start_authority_characterization.py
```

The intended review scope is exactly:

```text
docs/TRACK_D_D1N_S1E0_START_AUTHORITY_CHARACTERIZATION.md
tests/test_d1n_s1e0_start_authority_characterization.py
```
