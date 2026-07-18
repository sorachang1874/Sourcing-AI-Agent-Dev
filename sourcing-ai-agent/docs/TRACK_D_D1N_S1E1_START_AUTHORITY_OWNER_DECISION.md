# Track D D1n S1e1 — acquisition-start authority owner decision

> Status: non-live, zero-migration owner decision lock (2026-07-18). This batch changes no product writer, schema,
> migration, registry population, provider/model route, or serving state. It is author evidence, not an independent
> review `GO`.

## 1. Outcome

S1e1 replaces the three `characterized_not_ratified` decisions from S1e0 with one bounded implementation target:

```text
implementation_status=decision_locked_not_implemented
migration_delta=0
pending_action_owner=submit_acquisition_start_v2_action_uow
confirmation_receipt_physical_sot=operation_events.ActionApproved.sequence_2
command_acceptance_winner=operation_events.OperationCommandPlanned.sequence_1
parent_budget_owner=ActionApproved.acquisition_confirmation_receipt.v1.budget
allowed_runtime_namespace=isolated_local_canary
allowed_provider_modes=simulate,scripted
runtime_outbox_delta=0
default_public_agent_served_population=0
```

The decision deliberately reuses the existing PostgreSQL `agent_actions`, `operation_runs`, `operation_events`,
`workflow_events`, `workflow_commands`, `workflow_current_state`, and Agent result-slot tables. `schema_version` and
closed JSON payloads provide the new record contracts; no column or relation is required. The implementation is split
into exactly three write transactions: submit, create, and result accept. Result preparation is read-only.

## 2. Deterministic identities

The future adapter must use the existing `operation_action_id`, `operation_run_id_for`, and `command_id_for` helpers.
All SHA inputs are UTF-8 and all truncation is the first 24 lowercase hex characters.

| identity | exact derivation |
| --- | --- |
| start idempotency | `agent-start-v2:{logical_occurrence_digest}` |
| Action id | `operation_action_id(workspace_id, "start_acquisition_run", start_idempotency)` |
| OperationRun id | `operation_run_id_for(action_id, "acquisition_run", start_idempotency)` |
| workflow run id | `wf_operation_{sha1(operation_run_id)[:24]}` |
| operation event id | `opevt_{sha1(event_stream_id + ":" + sequence + ":" + idempotency_key)[:24]}` |
| workflow event id | `evt_{sha1(workflow_run_id + ":" + sequence + ":" + idempotency_key)[:24]}` |
| approval-required event key | `{start_idempotency}:ActionApprovalRequired` |
| receipt event key | `{start_idempotency}:ActionApproved` |
| receipt id | Action-stream `ActionApproved` operation-event id at exact sequence `2` |
| root command key | `acquisition.run.create:start-v2:{receipt_digest}` |
| root command id | `command_id_for(workflow_run_id, root_command_key)` |
| workflow-start event key | `{workflow_run_id}:operation_command_started:{operation_run_id}` |
| command-plan event key | `{root_command_key}:plan` |
| planned event key | `{start_idempotency}:OperationCommandPlanned:{workflow_command_id}` |

`SELECT date_trunc('second', transaction_timestamp())` is evaluated once by the first create attempt. That DB-minted
value owns
`approved_at` and every created/updated/event timestamp in that transaction. The event id is known before receipt
construction because it depends only on stream, sequence, and idempotency key; the receipt digest can therefore include
its own physical receipt id without a hash cycle. On lost-ack replay the UoW first discovers the complete existing
aggregate, reloads its persisted `approved_at`, and rebuilds/compares the receipt with that value; it never compares a
fresh transaction timestamp or accepts a caller timestamp.

## 3. Submit UoW — pending Action authority

Before any occurrence-slot reserve/write, the invocation owner performs the initial read-only
`AcquisitionStartV2OwnerBinder.bind(raw_preview_ref, server_context, current_tool_pins)` and uses that exact bound root
to create the historical `start_acquisition_run_tool_v3` occurrence. `submit_acquisition_start_v2_action_uow` then
accepts that reserved occurrence. Before opening a connection it rejects any namespace other than
`isolated_local_canary`, any mode outside `simulate|scripted`, any non-current tool/result/request pin, and canonical
args other than the exact full request root `{input_payload,target_ref}`: `input_payload` is the closed preview
reference and `target_ref` is the server-bound workspace/requester/complete start snapshot. Inside one overall
five-second deadline it exact-locks/reloads the result slot and preview, invokes `AcquisitionStartV2OwnerBinder`,
requires the rebuilt bound request to equal the occurrence root byte-for-byte, and only then inserts or exact-replays:

- one `agent_actions` row with physical `status=approval_required`, `approval_status=required`, and
  `approval_policy=required`; the pure binder adapter maps that exact state to its closed `state=pending_approval` view;
- complete v2 request, current v3 tool, and revisioned result/serializer pins from the occurrence;
- `input_json` equal to the closed preview reference and `target_ref_json` equal to the owner-minted bound target,
  including `requester_id` and the complete immutable start snapshot;
- `budget_json` equal to the snapshot's exact five-field budget as a projection only;
- result-occurrence identity in metadata: slot id/generation and logical occurrence digest;
- one Action-stream `ActionApprovalRequired` event at exact sequence `1`, schema
  `acquisition_start_approval_required.v1`, binding the same request/tool/result/occurrence/snapshot identities.

No OperationRun, workflow event, WorkflowCommand, result attempt/journal, outbox, provider, model, or domain row is
created by submit. Existing generic `submit_action` is not extended to this v2 local-canary path.

The first submit attempt evaluates one `SELECT date_trunc('second', transaction_timestamp())` and owns the Action
`created_at/updated_at` plus the sequence-1 event time fields. An exact lost-ack replay reloads those persisted values
before immutable comparison; it never compares a new transaction timestamp or accepts a caller timestamp.

### 3.1 Exact approval-required event contract

The sequence-1 row has `event_family=operation_event`, `event_type=ActionApprovalRequired`, empty
`operation_run_id`, exact `action_id/workspace_id`, `actor=requester_id`, `source=agent_start_v2_submit_uow`, and
`schema_version=acquisition_start_approval_required.v1`. Its closed payload has exactly these fields, in order:
`schema_version`, `action_id`, `workspace_id`, `requester_id`, `action_type`, `request`, `request_schema_ref`,
`tool_spec_ref`, `result_contract_ref`, `result_occurrence_ref`, and `start_snapshot_digest`. `request` is the complete
bound `{input_payload,target_ref}` root. The three pin refs and occurrence ref are exact projections of that root plus
the server-revalidated occurrence; callers cannot mint or override them. The event id is the deterministic sequence-1
operation-event id from the approval-required key in section 2.

## 4. Create UoW — receipt, budget envelope, command, and winner

`create_acquisition_start_v2_uow` is the only approval/start writer. Generic `approve_action` and generic Agent
dispatch must reject this v2 adapter before writes; neither may call through or repair it.

The UoW exact-reloads the pending Action, occurrence, and immutable preview; calls
`AcquisitionStartV2OwnerBinder.confirm_exact_action`; requires a human `authenticated_user|open_operator`; and performs
the following writes atomically:

1. append the Action-stream `ActionApproved` event at exact sequence `2`, link both `action_id` and the precomputed
   `operation_run_id`, set row and payload schema to `acquisition_confirmation_receipt.v1`, and persist the **exact**
   `AcquisitionConfirmationReceipt.to_record()` as its payload;
2. CAS the Action to `queued/approved` and create the exact linked OperationRun with the same request/tool/result pins,
   workflow ref, budget projection, and future owner-result ref;
3. append `WorkflowStarted` sequence `1` and `CommandPlanRequested` sequence `2` to the new workflow stream;
4. create one queued `acquisition.run.create` command owned by `acquisition_run_writer`; its source is the exact
   `CommandPlanRequested` event and its payload contains the closed `AcquisitionStartV2RootCommandPayload` plus the
   standard typed causality envelope;
5. reduce both workflow events into the canonical `workflow_current_state` row (`running`, exact stage, last processed
   sequence `2`) in the same transaction; the reducer produces exactly one command and zero outbox rows;
6. append Operation-stream `OperationCommandPlanned` at exact sequence `1`, schema
   `acquisition_start_command_acceptance.v1`; this event is the sole command-acceptance terminal winner.

Every pre-existing row/event is accepted only after full immutable equality. An exact committed bundle replays using
its persisted DB-minted timestamp; a different approval actor/kind/policy, receipt, command, or event is a collision
and the transaction writes nothing.

Only after the create transaction commits (including an exact lost-ack replay) the adapter passes the exact queued
command to `DurableRuntimeWriter.signal_recovery_for_committed_commands`. That wake is best-effort, coalesced, and
outside the transaction; its failure does not change the committed result, and the recovery daemon poll remains the
backstop. No `runtime_outbox` row is created.

### 4.0 Exact workflow event and reducer contract

Both workflow rows use `event_family=workflow_event`, `schema_version=workflow_event_v1`, empty `command_id` and
`activity_attempt_id`, `actor=operation_workflow_command_planner`, `source=operation_run_dispatch`, and empty event
artifact refs. Their physical event ids use the derivation in section 2. The immutable payloads are:

| sequence/type | exact payload |
| --- | --- |
| `1 / WorkflowStarted` | `workflow_type=agent_callable_workflow_command`; `stage_key=acquisition_run_create`; exact `operation_run_id`, `action_id`, `action_type=start_acquisition_run`; `migration_phase=W11_agent_callable_workflow_command` |
| `2 / CommandPlanRequested` | the same workflow type and stage; `command_type=acquisition.run.create`; exact root command key; command payload equal to the closed root plus `operation_id` and the complete `command_causality_v1` envelope; `artifact_refs=[]`; `max_attempts=5`; `retry_policy={kind: operation_acquisition_run_create, retry_delay_seconds: 30}` |

The causality envelope is produced by the existing `command_causality_for` owner using the precomputable sequence-2
event id. It pins `owner=acquisition_run_writer`, `stage_id=acquisition_run_create`, the root command key,
`source_event_id`, `source_event_type=CommandPlanRequested`, and the existing readiness effect; caller/model data may
not mint or patch it. `command_source_event_contract_digest` hashes exactly these stable physical fields, in canonical
JSON: `event_id`, `workflow_run_id`, `operation_id`, `command_id`, `activity_attempt_id`, `event_family`, `event_type`,
`sequence_number`, `idempotency_key`, `actor`, `source`, `payload`, `artifact_refs`, and `schema_version`. Mutable time
fields are excluded.

Reducing sequences 1 and 2 through the existing pure `reduce_workflow_events` contract must yield one queued command,
zero outbox specs, and this exact timestamp-free current-state projection:

| field | exact value |
| --- | --- |
| `schema_version` | `workflow_current_state_v1` |
| `workflow_run_id` | deterministic workflow id from section 2 |
| `operation_id` | exact OperationRun id |
| `workflow_type` | `agent_callable_workflow_command` |
| `status` | `running` |
| `current_stage_key` | `acquisition_run_create` |
| `completion_proofs` | `{}` |
| `active_command_counts` | `{acquisition_run_writer: {acquisition.run.create: 1}}` |
| `terminal_command_counts` | `{}` |
| `read_model_pointers` | `{}` |
| `migration_status` | `{}` |
| `last_processed_sequence_number` | `2` |
| `reducer_version` | `durable_runtime_reducer_v1` |
| `metadata` | `{}` |

### 4.1 Receipt-backed parent-budget envelope

The exact five budget dimensions are:

| position | field |
| --- | --- |
| 1 | `max_provider_calls` |
| 2 | `max_provider_items` |
| 3 | `max_output_candidates` |
| 4 | `max_cost_micro_usd` |
| 5 | `max_elapsed_seconds` |

The immutable `ActionApproved` receipt payload and `receipt_digest` are the physical SOT for this acquisition parent
budget envelope. Its owner pin remains `acquisition.parent_budget_reservation / acquisition_parent_budget_v1 /
50c72166a683f8a49826bab1af82fdc0922026d5993a3725403582dff24c5670`. `agent_actions.budget_json` and
`operation_runs.cost_budget_json` must exact-equal the receipt budget but are projections, never independent authority.

This is admission-envelope reuse for `simulate|scripted`; it has no consume, release, refund, generation, or balance
CAS. It is explicitly **not** D3 `cost_reservations`/`dispatch_exposures`, does not claim money/exposure accounting, and
cannot satisfy the live L1 predecessor. OB-2.2 and OB-10.3 remain open.

S1e2 must add a typed pure `build_acquisition_parent_budget_envelope_ref(receipt, registered_owner_pin)` builder. It
accepts an already validated `AcquisitionConfirmationReceipt`, extracts the five fields, and recomputes the receipt and
budget digests. Caller/model/raw dictionaries cannot mint or override the envelope or either budget projection.

### 4.2 Exact command-acceptance owner result

The `OperationCommandPlanned` payload has exactly `owner_result_ref` and `owner_result_digest`. The ref has these exact
fields:

| position | field |
| --- | --- |
| 1 | `schema_version` |
| 2 | `runtime_namespace` |
| 3 | `provider_mode` |
| 4 | `workspace_id` |
| 5 | `action_id` |
| 6 | `operation_run_id` |
| 7 | `workflow_run_id` |
| 8 | `workflow_command_id` |
| 9 | `terminal_winner_id` |
| 10 | `terminal_winner_sequence_number` |
| 11 | `command_source_event_id` |
| 12 | `command_source_event_sequence_number` |
| 13 | `command_source_event_contract_digest` |
| 14 | `confirmation_receipt_ref` |
| 15 | `parent_budget_envelope_ref` |
| 16 | `start_snapshot_digest` |
| 17 | `root_command_payload_digest` |
| 18 | `result_occurrence_ref` |

`schema_version=acquisition_start_command_acceptance_owner_result_ref.v1`; winner sequence is `1`; source sequence is
`2`. The receipt ref is exact id+digest. The budget ref exact-copies the registry owner pin, receipt id/digest, and the
SHA-256 digest of the canonical five-field budget. The occurrence ref is exact slot id, slot generation, and logical
occurrence digest. `command_source_event_contract_digest` hashes the closed source-event identity and payload without
mutable timestamps. `owner_result_digest=sha256(canonical_json(owner_result_ref))`.

The Action and Operation `result_ref_json` exact-copy this ref. Result preparation uses
`terminal_winner_id=OperationCommandPlanned.event_id`, `owner_target_kind=acquisition_start_command_acceptance_v1`,
`owner_target_id=workflow_command_id`, `owner_target_revision=1` (the planned-event sequence),
`owner_target_generation=0`, blank `owner_target_revision_token`, and zero command attempt/generation/control epoch.
The complete owner-result digest remains in `owner_result_digest`, not in the revision-token carrier. This is the exact
`workflow_command_acceptance_v1` shape. The planned event id and complete owner ref/digest are precomputable before
either Action or Operation `result_ref_json` is inserted because the winner id depends only on stable identities.

## 5. Result prepare and accept

`prepare_start_acquisition_tool_result` is read-only. It exact-resolves the historical occurrence before owner reads,
then reloads Action, Operation, result slot, preview, receipt event, both workflow events, command, canonical
`workflow_current_state`, and planned event.
It rebuilds the receipt, budget ref, root payload, source-event digest, owner ref/digest, and the existing bounded
`acquisition_start_result_v2` success serialization. Any mismatch returns a typed conflict and writes nothing.

`accept_start_acquisition_tool_result_uow` reuses the shared pending-to-accepted result-slot state machine with the
start-specific owner assertion. One transaction accepts attempt + slot + journal, or exact-replays it. A losing later
attempt may append only the existing quarantined attempt evidence; it writes zero Action, Operation, command, event,
outbox, provider, model, or domain state. `runtime_outbox` delta is always zero in all three UoWs.

## 6. Lock order, concurrency, and lost acknowledgement

Every UoW uses one monotonic five-second deadline, including connection acquisition. Skipped groups do not change the
order:

| order | lock group |
| --- | --- |
| 1 | all action/operation/workflow event-stream advisory keys, bytewise sorted |
| 2 | `operation_runs` identity |
| 3 | `agent_actions` identity |
| 4 | `agent_tool_result_slots` identity |
| 5 | `acquisition_plan_previews` identity |
| 6 | `workflow_commands` command-id and workflow/idempotency identities |
| 7 | `workflow_current_state` workflow-run identity |
| 8 | exact receipt/source/winner event rows in stream+sequence order |
| 9 | result attempt and journal identities |

### 6.1 Exact advisory-lock and row-probe contract

Within each applicable group, the adapter sorts UTF-8 lock-key bytes before calling
`pg_try_advisory_xact_lock(hashtext(key))`. These are the only allowed key templates:

| group | exact key templates |
| --- | --- |
| event streams | `operation_events:{action_id}`; `operation_events:{operation_run_id}`; `workflow_events:{workflow_run_id}` |
| OperationRun | `operation_runs:id:{operation_run_id}`; `operation_runs:idempotency:{workspace_id}:{start_idempotency}` |
| Action | `agent_actions:id:{action_id}`; `agent_actions:idempotency:{workspace_id}:{start_idempotency}` |
| result slot | `agent_tool_result_slots:id:{result_slot_id}`; `agent_tool_result_slots:occurrence:{logical_occurrence_digest}` |
| preview | `acquisition_plan_previews:id:{preview_id}`; `acquisition_plan_previews:revision:{preview_revision}` |
| command | `workflow_commands:id:{workflow_command_id}`; `workflow_commands:idempotency:{workflow_run_id}:{root_command_key}` |
| current state | `workflow_current_state:{workflow_run_id}` |

After those locks, each start-authority `SELECT ... FOR UPDATE` covers its physical alternate identities, rejects more
than one candidate, and processes candidate rows in this exact table/order. Result acceptance keeps the existing
result-slot lock as its serialization owner and the shared state machine's exact probes:

| order | table | exact alternate identity probe |
| --- | --- | --- |
| 1 | `operation_runs` | `operation_run_id OR (workspace_id, start_idempotency)` |
| 2 | `agent_actions` | `action_id OR (workspace_id, start_idempotency)` |
| 3 | `agent_tool_result_slots` | `result_slot_id OR logical_occurrence_digest`; the server-revalidated digest binds the complete occurrence tuple |
| 4 | `acquisition_plan_previews` | exact `preview_id + workspace_id + requester_id + preview_revision + preview_digest` |
| 5 | `workflow_commands` | `workflow_command_id OR (workflow_run_id, root_command_key)` |
| 6 | `workflow_current_state` | exact `workflow_run_id` |
| 7 | operation events | `event_id OR (event_stream_id, sequence_number) OR (event_stream_id, idempotency_key)`, ordered by stream/sequence/event id |
| 8 | workflow events | `event_id OR (workflow_run_id, sequence_number) OR (workflow_run_id, idempotency_key)`, ordered by run/sequence/event id |
| 9 | result attempt/journal | attempt by exact `result_attempt_id`; journal by exact `result_slot_id`; deterministic `journal_id=tooljournal_{sha1(result_slot_id + ":" + result_attempt_id)[:24]}`; accepted-attempt uniqueness remains serialized by the locked result slot |

Concurrent exact submit/create/accept calls return the same rows and bytes. Two nonidentical approvers contend on the
same Action stream and Action row; one bundle commits and the other observes a digest collision with zero writes.
Cancel/retry/resume/dispatch are not enabled for this shadow-only adapter. A crash before commit rolls back every row.
After a lost commit acknowledgement, retry first exact-reloads the deterministic identities and returns the committed
bundle; it never creates a second receipt, command, event, attempt, or journal.

## 7. Fault and zero-effect matrix

| boundary | required outcome |
| --- | --- |
| namespace not `isolated_local_canary`; mode `live|replay` | reject before any PG write or provider/model call |
| historical tool/request/result pin or canonical-args mismatch | reject before schema bootstrap/owner read/write |
| preview missing/foreign/stale/expired or Action requester/workspace/snapshot drift | one conflict class; zero rows |
| Action/Operation/receipt/source event/command/current-state/winner collision | rollback complete transaction; zero partial rows |
| fault after each submit/create write | rollback complete transaction |
| fault after submit/create commit | exact lost-ACK replay |
| prepare owner/event/budget/occurrence mismatch | read-only conflict; zero rows |
| accept fault before commit | attempt/slot/journal all rollback |
| accept fault after commit | exact accepted replay |
| late non-winning terminal proposal | append-only quarantine only; all domain/runtime owners unchanged |

No transaction spans provider, model, network, or callback I/O.

## 8. Owner matrix and design invariants

| mechanism | single owner / physical SOT | tenant and identity | lifecycle / late behavior | cost honesty | mode isolation |
| --- | --- | --- | --- | --- | --- |
| pending start | specialized submit UoW / Action + seq1 event | occurrence workspace+actor and bound requester | create-or-exact-replay only | five fields are projections | isolated simulate/scripted only |
| confirmation | specialized create UoW / seq2 receipt event | action+operation+workspace+requester | immutable; conflicting approver loses | receipt budget is envelope SOT | live/replay pre-write reject |
| root command | specialized create UoW / source event+command+current-state | exact operation/action/receipt/snapshot | queued once; reducer projection exact-replays | no exposure or spend | no provider/model I/O |
| acceptance winner | specialized create UoW / planned event seq1 | source/receipt/budget/snapshot/occurrence bound | immutable one winner | references envelope only | local namespace/mode bound |
| Agent result | shared accept UoW / slot+attempt+journal | exact physical owner ref/digest | one CAS; loser quarantined | no cost mutation | occurrence mode exact |

This covers the ten checklist dimensions: single writer, tenant key, equality fence, lifecycle, late/partial behavior,
cost truth, physical identity, provenance, cross-document consistency, and runtime/mode isolation. It creates no new
OB-ID.

## 9. Explicit non-closure and next batch

S1e1 leaves the production partition at `10 schema-defined / 5 schema-less`, default/public Agent population at
`served=0`, and provider/model/live invocation count at zero. R-019 and R-029 remain open. The Plan §6#6 action-root
durable-scope gate remains open because Action/Operation do not yet have the required typed namespace/mode/issuer/scope
columns. OB-2.2, OB-10.3, and OB-10.4 remain open. The D3 money/exposure ledger, release/consume CAS, S2/S3, L1/L2,
paid TML canary, hosted activation, and all five remaining action migrations remain outside this decision.

The next bounded batch is S1e2 product implementation of the two specialized start UoWs plus read-only prepare and
start-specific shared acceptance, followed by PG fault/concurrency/terminal-success tests. S1e0 must then be updated by
that implementation batch; this decision-only batch does not rewrite its characterization oracle.

Fresh pinned non-author review is required. Author tests do not constitute formal `GO`, and this decision authorizes
neither serving nor live validation.

## 10. Author validation

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src \
  .venv/bin/python -m pytest -q tests/test_d1n_s1e1_start_authority_owner_decision.py
```

The intended scope is exactly this document, its executable decision-lock test, and scoped tracker annotations in
`TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md`, `NEXT_TODO.md`, `RESIDUAL_LEDGER.md`, and `INDEX.md`.
