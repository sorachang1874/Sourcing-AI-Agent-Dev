# Track D D1n S1e2b — acquisition-start confirmation and root-command PostgreSQL UoW

> Status: Current non-live implementation candidate (2026-07-18). Author evidence only; fresh pinned non-author
> review is pending. This batch keeps the default/public Agent population at `served=0`, starts no daemon, and
> authorizes no provider, model, network, replay-mode, or live execution.

## Outcome

S1e2b implements the second of the three S1e1 start-authority write transactions. From the exact S1e2a pending
aggregate, one specialized PostgreSQL transaction creates or exact-reloads:

1. Action-stream `ActionApproved` sequence `2`, whose exact
   `acquisition_confirmation_receipt.v1` payload is the approval and five-field admission-envelope source of truth;
2. the existing Action advanced to `queued/approved` and one linked queued OperationRun;
3. Workflow-stream `WorkflowStarted` sequence `1` and `CommandPlanRequested` sequence `2`;
4. one dormant queued `acquisition.run.create` command owned by `acquisition_run_writer`;
5. the reducer-owned canonical `workflow_current_state` projection at sequence `2`;
6. Operation-stream `OperationCommandPlanned` sequence `1`, whose exact owner-result ref/digest is the command-
   acceptance winner.

The Action and Operation exact-copy the same 18-field owner-result ref. The transaction creates no result
attempt/journal, runtime-outbox, Activity, AcquisitionRun, provider, model, CRM, projection, or other domain row.

## Typed budget reference

`build_acquisition_parent_budget_envelope_ref(receipt, registered_owner_pin)` is the only production builder for the
receipt-backed admission reference. It accepts exact typed current-owner values, revalidates the complete receipt and
its digest, canonicalizes the five integer budget fields, and returns exactly:

```text
owner_id
owner_revision
owner_contract_digest
confirmation_receipt_id
confirmation_receipt_digest
budget_digest
```

Raw dictionaries, subclasses, a stale owner revision/digest/id, tampered receipt bytes, and self-consistent budget
drift fail closed. `agent_actions.budget_json` and `operation_runs.cost_budget_json` exact-copy the receipt budget but
remain projections. This is not a D3 cost reservation, dispatch exposure, consume/release/refund balance, or paid-live
checkpoint.

## Pre-connection and authority fence

Before any adapter dependency check or connection, create reuses the S1e2a historical occurrence revalidator. It
requires the current `start_acquisition_run_tool_v3`, exact request/result/serializer/link pins, the complete canonical
`{input_payload,target_ref}` root, `runtime_namespace=isolated_local_canary`, and
`provider_mode=simulate|scripted`. It separately requires a nonblank human actor, actor kind
`authenticated_user|open_operator`, a versioned approval policy, a positive finite total deadline, and a closed fault
surface.

All eight physical dependencies must be both read-preferred and PostgreSQL-authoritative. The UoW never bootstraps or
migrates a schema and never invokes the older generic approval or dispatch writers.

## One transaction, identities, and physical projections

All action, operation, workflow, command, receipt, and winner identities are derived from the server-revalidated
occurrence using the S1e1 formulas. Caller/model IDs, receipt bytes, owner-result fields, command causality, timestamps,
and budgets are not accepted.

One DB transaction timestamp owns a new receipt's `approved_at`, the Action update, and every created row/event time.
The exact physical implementation shapes are:

- receipt event: human actor, `source=agent_start_v2_create_uow`;
- workflow events: `actor=operation_workflow_command_planner`, `source=operation_run_dispatch`;
- Action: existing submit identity and occurrence metadata, `queued/approved`, exact owner-result ref;
- Operation: `queued`, `progress={phase: workflow_command_planned}`, exact four-field workflow ref, receipt budget,
  exact owner-result ref, empty metadata;
- queued command: standard `command_causality_v1` columns derived from the sequence-2 source event, five attempts, the
  closed acquisition retry policy, and a fixed result-acceptance hold in `not_before_at`;
- current state: pure reducer result, `running`, stage `acquisition_run_create`, one active command, zero terminal
  commands, sequence `2`, empty pointers/migration/metadata;
- planned winner: planner actor, `source=agent_start_v2_create_uow`, schema
  `acquisition_start_command_acceptance.v1`, and payload containing only `owner_result_ref` plus its canonical digest.

Every returned physical insert/update is exact-compared before commit.

## Lock discovery, row probes, and replay

The overall monotonic deadline includes connection acquisition, every advisory/row lock, statement, write, and the
pre-commit boundary. Transaction-local lock and statement timeouts are refreshed from the remaining budget.

The root command lock key contains the receipt digest, while that digest contains the persisted approval timestamp.
To avoid a replay hash cycle, create:

1. acquires all action/operation/workflow event-stream advisory locks in bytewise order;
2. performs one non-locking deterministic receipt-id discovery only to select a new DB timestamp or recover the exact
   persisted replay timestamp and derive the command keys;
3. acquires Operation, Action, result-slot, preview, command, and current-state advisory groups in S1e1 order;
4. performs the official `FOR UPDATE` probes in exact order: Operation, Action, slot, preview, command, current state,
   operation events, workflow events;
5. rebuilds the receipt from the locked Action/preview and full approval identity, then exact-compares every existing
   row/event.

Discovery is never an authority read. Fresh creation requires exactly the submit Action/sequence-1 event, one pending
slot, one exact preview, and zero create-owned rows. Replay requires the complete create bundle. Alternate identities,
partial bundles, a different approver/policy, corrupted Action/Operation/receipt/source/command/state/winner, or an
unexpected lifecycle all roll back with zero partial effects.

A new create writes in the S1e1 order. Fault injection after each of the eight write boundaries rolls back the complete
effect surface. A fault after commit is recovered by exact replay using persisted `approved_at`; replay never evaluates
or compares a new transaction timestamp. Eight identical contenders produce one create and seven byte-identical
replays. Different approvers produce one winner and one zero-write collision.

## Result-acceptance hold and delegation

`LiveControlPlanePostgresAdapter.create_acquisition_start_v2_uow(...)` is the sole native writer. The repository
requires all eight authoritative tables, delegates once, and maps the complete physical bundle. The root command and
its `CommandPlanRequested` event both carry `not_before_at=9999-12-31 23:59:59`, so normal ready-list polling cannot
claim it before S1e2c accepts and persists the start result. Create intentionally does not call
`DurableRuntimeWriter.signal_recovery_for_committed_commands`; result acceptance owns clearing the hold and waking the
owner. No `runtime_outbox` row is created.

## Explicit non-closure

- S1e2c has a separate author candidate for read-only owner reconstruction, start-specific shared result acceptance,
  result-acceptance hold release, post-accept recovery wake, result attempt/slot/journal terminal success, and
  post-accept exact replay/quarantine/rollback proof. Its formal independent review remains pending.
- The dormant queued root command is visible only in an isolated test database. This batch does not start the shared
  recovery daemon or prove the root consumer against the v2 payload.
- Generic approve/reject/cancel/retry/resume/dispatch API controls reject this v2 action before any runtime writer
  mutation. A future owner-specific start control must replace that shadow-only fence rather than using generic
  operation control.
- The production partition remains `10 schema-defined / 5 schema-less`; R-029 stays open and no observation window
  starts.
- R-019, Plan §6#6, OB-2.2, OB-10.3, and OB-10.4 stay open. Dormant D3 columns/defaults and the future cost/exposure
  ledger are not closure evidence.
- The default/public registry remains empty and `served=0`; provider/model/network/live invocation count remains zero.

## Author validation

- start-v2/request/result, budget builder, submit/create delegates, submit/create UoW, S1e0/S1e1 executable
  contracts, and canary tool-population adjacency:
  `120 passed`;
- real PostgreSQL complete-bundle, dormant result-acceptance hold, generic control zero-write matrix, eight write-fault
  boundaries, lost-ACK, eight-way identical concurrency, distinct approver contention, alternate Operation identity,
  and seven owner-corruption cases:
  `9 passed + 21 subtests`;
- adjacent PostgreSQL preview/result-slot/start-submit/start-create UoWs:
  `49 passed + 36 subtests`;
- new create owner module scoped mypy: `0 errors / 1 file`;
- canonical `make typecheck`: existing ceiling unchanged at `81 errors / 4 files`;
- scoped Ruff plus canonical `make lint`: green (`58 files already formatted`).

These are author results, not a formal review verdict. A fresh pinned non-author review must bind the exact eventual
implementation commit and scope digest before any live/manual/product/milestone gate.
