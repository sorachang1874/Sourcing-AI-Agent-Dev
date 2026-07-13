# Agent Operation Contract

> Status: Proposed product/operation contract. Drafted 2026-05-19 to define the future multi-turn Agent layer after canonical projection, CRM, and person asset boundaries. Read with `CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `CRM_STATE_CONTRACT.md`, `PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md`, `WORKFLOW_PROGRESS_CONTRACT.md`, and `TEST_ENVIRONMENT.md` before adding natural-language operations, approval flows, staged acquisition, or Agent-triggered mutations.

## Purpose

Agent is the product-level operation layer. It can help users plan, inspect, search, enrich, add to CRM, export, and run acquisitions across modules.

Agent is not a data owner.

The durable model is:

```text
AgentConversation stores multi-turn context.
AgentAction stores a typed requested operation.
OperationRun stores long-running execution state.
Module owners execute actions and write their own state.
```

## Reference Patterns

These are references, not mandatory dependencies:

- Temporal is a durable execution platform that resumes workflows after crashes and outages. Its workflow/activity/signal/query model is relevant to long-running acquisition and provider waits.
- LangGraph is a low-level agent orchestration runtime focused on durable execution, streaming, human-in-the-loop, and persistence.

The contract boundary must let us adopt either later without changing data ownership. A framework may run Agent or workflow orchestration, but it must not become the source of truth for projections, CRM, person assets, provider registry rows, or public reader state.

## Long-Term General Agent Direction

The long-term target is an OpenClaw-like general Agent constrained by the sourcing domain: it should plan across public web research, company/candidate assets, CRM, export, and workflow recovery by calling typed tools, not by embedding one narrow workflow.

The Agent must be able to:

- Call model providers such as GPT-class and Claude-class models through bounded provider contracts, including optional reviewed model-native Search experiments when a future `model_native_search` owner supplies cost, retry, provenance, export, and circuit-breaker semantics.
- Inspect staged results, Activity/Attempt/EntityDelta evidence, Public Web candidates, and human promotion/export decisions, then plan the next typed action instead of directly writing module tables.
- Use approval and budget policies before paid provider calls, bulk export, CRM mutation, or broad refresh actions.
- Compare multiple evidence sources and model judgments only through auditable artifacts, so a later reviewer can see why a link, email, company page, or Scholar/GitHub profile was accepted or rejected.

Current gap to that target: the module owner and Operation/Command contracts are now partially in place, but the natural-language planner loop, tool registry UX, model-native Search provider, multi-model adjudication harness, durable Agent memory, and end-to-end browser/product validation are still future Phase 13 work. Until those are implemented, Agent-like behavior should submit typed `AgentAction` / `workflow_commands` and wait on owner-owned results rather than running an unconstrained autonomous loop.

2026-06-07 status note: the Operation/Command control surface is now product-visible, CRM Public Web exposes backend-owned workspace/run display/control contracts, and the target-candidate UI can review durable Public Web promotions without clearing them on retry. This is still an operation workbench, not the OpenClaw-like Agent itself. Model-native Search remains default-off and must become a reviewed typed provider/owner before it can supplement DataForSEO in normal Public Web flows.

2026-06-08 architecture note: OpenClaw/Codex/LangGraph-style runtimes may be adopted as the outer planner/browser/search/model orchestration layer, but only after `SERVICE_GRADE_ARCHITECTURE_PLAN.md` milestones close the bottom-layer service boundaries. The adapter target is not "let a general Agent call current internals"; it is "let a general Agent submit/query/approve typed actions whose owners already provide provider task identity, budget, retry, cancel/resume, Activity/EntityDelta evidence, and export/CRM permission contracts." Until those service-grade contracts exist, Candidate Acquisition, Profile Fetch, Public Web, CRM/Export, provider tasks, and serving projections must not be exposed as raw OpenClaw/Codex tools.

2026-06-08 Agent-native planning update: service closure must itself be designed for future Agent operation, not as a traditional backend that receives an adapter later. Candidate Acquisition and Public Web should support dual evidence sources: deterministic/API providers such as DataForSEO plus reviewed Agent Search/fetch/browser sources. Both sources must enter the same ProviderTask/Evidence/EntityDelta adjudication path, and only owner-owned promotion/export/CRM actions may convert evidence into product state. Progress must be event-visible so Codex/OpenClaw can report current stage, pending items, retries, costs, blockers, and available control actions without reading private tables.

## Non-Negotiable Rules

1. Agent writes `AgentAction`; module owners write module state.
2. Every action has an `action_type`, target references, idempotency key, approval policy, budget policy, and owner.
3. Agent cannot directly mutate CRM tables, projection membership, person assets/evidence/assertions, provider registry rows, or workflow durable queues.
4. Paid provider calls require explicit budget and approval policy.
5. Human approval is a first-class state, not a final UI button.
6. Long-running work uses `OperationRun` or the existing workflow queue with durable progress, cancellation, retry, and audit.
7. Agent state can be implemented by LangGraph or another graph runtime later, but the persisted contract remains `AgentConversation` / `AgentAction` / `OperationRun`.

## Implementation Phase Mapping

Agent-facing capability is intentionally split from module-owner convergence:

| phase | scope | exit condition |
| --- | --- | --- |
| `W7` | Module durable-runtime reuse before Agent. CRM Public Web, Excel, export, and other callable modules must expose typed command owners or explicitly documented short synchronous owner paths with fail-closed normal-path contracts. | Module can be called by a future operation layer without hidden fallback, route-local side effects, or legacy owner repair. |
| `W8` | OperationRun and action registry foundation. | User/Agent intent is represented as `AgentAction` / `OperationRun` / operation events in PG-only durable storage; every action maps to one owner; unknown action types fail closed; operation state never mutates module tables directly. |
| `W9` | Recoverable operation API/UI and approval gates. | Each action/command can be queried, retried, cancelled, resumed, and inspected for causality/provenance; approval-required operations cannot execute before approval. The command surface is active through `/api/workflow/command-registry`, `/api/workflow/commands`, `/api/workflow/commands/{command_id}`, safe queued-state controls `/cancel`, `/retry`, `/resume`, owner-specific running cancel/resume for CRM Public Web phase commands, owner-specific running cancel/resume for projection/CRM Public Web export commands, owner-specific running resume for orchestration commands, provider-attempt commands, domain-mutation commands, `company.public_web.assets.materialize`, CRM writer commands, and `media.asset.cache`, and cooperative owner-specific running cancel/resume for Excel intake commands. These query surfaces expose a shared `control_policy` contract, including `running_control_maturity`, `running_control_gap_status`, and `running_control_surface`, and Activity/Attempt/EntityDelta query rows expose `control_target` back to the owning command, so Agent/UI code reads whether a command supports generic or owner-specific control instead of hard-coding heuristics. Running command cancellation remains fail-closed unless an owner implements explicit interrupt/compensation semantics. |
| `W10` | Pre-Agent full contract review. | Projection, workflow readers, CRM, person assets, Public Web, Excel, export, and operation APIs have owner/source-of-truth matrices, field visibility, fallback status, migration bridges, deletion conditions, and fast contract preflights. |
| `Phase 13` | Product Agent interaction layer. | Natural-language Agent UI/graph can emit `AgentAction` and wait for module-owned operation results. It does not own domain state. |

This means the Agent UI/graph is not the next step after a single module cutover. The system first needs W7 module reuse, W8 operation persistence/action routing, W9 recovery/approval surfaces, and W10 contract signoff.

## Core Objects

### `AgentConversation`

| field | meaning |
| --- | --- |
| `conversation_id` | stable random id |
| `workspace_id` | tenant/operator workspace |
| `user_id` | initiating user |
| `scope_ref` | optional projection/run/collection/CRM context |
| `status` | `active`, `waiting_for_user`, `running_operation`, `completed`, `archived` |
| `summary_json` | bounded conversation memory |
| `created_at`, `updated_at` | timestamps |

### `AgentAction`

| field | meaning |
| --- | --- |
| `action_id` | stable random id |
| `workspace_id` | tenant/operator workspace |
| `conversation_id` | parent conversation |
| `action_type` | typed operation |
| `operation_type` | durable operation class selected by the registry |
| `target_ref_json` | projection/person/CRM/run/collection refs |
| `input_json` | bounded structured input |
| `owner_module` | module responsible for execution |
| `approval_status` | `not_required`, `required`, `approved`, `rejected`, `expired` |
| `approval_policy` | default approval rule selected by the registry |
| `budget_json` | provider/model/runtime budget |
| `idempotency_key` | required |
| `status` | `planned`, `approval_required`, `queued`, `running`, `completed`, `failed`, `cancelled` |
| `result_ref_json` | output references |
| `metadata_json` | contract/provenance metadata |
| `created_at`, `updated_at` | timestamps |

### `OperationRun`

| field | meaning |
| --- | --- |
| `operation_run_id` | stable random id |
| `workspace_id` | tenant/operator workspace |
| `action_id` | source Agent action |
| `owner_module` | execution owner |
| `operation_type` | `acquisition_run`, `profile_sample`, `crm_update`, `person_enrichment`, `export`, etc. |
| `status` | durable status |
| `progress_json` | bounded progress |
| `workflow_ref_json` | existing job/worker refs when applicable |
| `cost_budget_json` | model/provider budgets |
| `idempotency_key` | required; duplicate operation submissions return the original run |
| `result_ref_json` | owner-produced output references |
| `metadata_json` | contract/provenance metadata |
| `started_at`, `completed_at` | timestamps |

### `OperationEvent`

Operation events are append-only and idempotent per `event_stream_id`.

| field | meaning |
| --- | --- |
| `event_id` | stable event id |
| `workspace_id` | tenant/operator workspace |
| `event_stream_id` | usually `action_id` or `operation_run_id` |
| `operation_run_id` | optional linked operation |
| `action_id` | optional linked action |
| `event_family` | event namespace |
| `event_type` | typed event |
| `sequence_number` | monotonic per stream |
| `idempotency_key` | required; duplicate event appends return the original event |
| `payload_json` | bounded event payload |
| `actor`, `source` | caller provenance |
| `schema_version` | event schema version |

## W8 Implementation Status

W8 foundation is active.

- `src/sourcing_agent/operation_runtime.py` owns the module action registry and `OperationRuntimeWriter`.
- `agent_actions`, `operation_runs`, `acquisition_runs`, `workflow_activity_runs`, `workflow_activity_attempts`, `workflow_entity_deltas`, `acquisition_discovery_lanes`, `operation_events`, and CRM task current-state (`crm_tasks`) are PG-only durable/current-state tables. SQLite is not a normal operation runtime path, and new Operation/W11/CRM task state must not add SQLite DDL/fallback as a compatibility shortcut.
- Unknown action types fail closed in the registry.
- Approval-required actions are persisted as `AgentAction(status='approval_required')` and `OperationEvent(ActionApprovalRequired)` but do not create `OperationRun` before approval.
- Budget-required actions fail closed unless an explicit budget is supplied.
- Read-only/non-sensitive actions can create an idempotent queued `OperationRun`, but W8 still does not execute module side effects.
- Operation persistence must not create `workflow_commands` or mutate CRM/projection/person asset/provider tables. Module execution remains owner-owned and belongs to W9+ action execution surfaces.

## W9 Implementation Status

W9 backend control foundation is active; product Agent UI remains deferred.

- `GET /api/operations/action-registry` exposes the typed action registry.
- `GET /api/operations/actions` lists bounded `AgentAction` rows by workspace/conversation/status/type/owner.
- `POST /api/operations/actions` persists an `AgentAction` and, when no approval is required, a queued `OperationRun`.
- `GET /api/operations/actions/{action_id}` and `GET /api/operations/runs/{operation_run_id}` expose bounded action/run state and append-only operation events.
- `GET /api/operations/runs` lists bounded `OperationRun` rows by workspace/action/status/type/owner.
- `GET /api/operations/runs/{operation_run_id}/provenance` returns the action, run, action/run events, action event timeline, and linked workflow commands without repairing or executing module state.
- OperationRun records and control responses expose `control_state` from `operation_runtime.operation_run_control_state`; Agent/UI code must use its `allowed_actions` and `disabled_reasons` for dispatch/resume/retry/cancel buttons instead of local terminal-status sets. If retry returns a child OperationRun, the caller should continue with that child run id rather than mutating or re-dispatching the terminal parent.
  All controls fail closed when the linked action is missing, and the API/repository boundary rejects a linked action
  from another workspace rather than treating it as eligible control state.
- `POST /api/operations/actions/{action_id}/approve` records approval and creates the idempotent queued `OperationRun` for approval-required actions.
- `POST /api/operations/actions/{action_id}/reject` atomically records `status=cancelled`,
  `approval_status=rejected`, and `ActionRejected` in one PG UoW; it keeps the action non-executable.
- `POST /api/operations/runs/{operation_run_id}/cancel` atomically marks the operation and eligible linked action
  cancelled and appends `OperationCancelled` in one PG UoW. Event failure or workspace/entity/idempotency mismatch
  rolls back the entire transition; repeated calls reuse the event and repair legacy target-state-without-event rows.
- Reject/cancel return success only from the committed target state. If another terminal transition wins the CAS,
  the API returns `status=conflict` / HTTP 409 and does not append the losing success event. Approve/resume/retry also
  validate their committed target before creating downstream events or child runs. The wider command-plan and owner
  completion UoWs remain tracked by `RESIDUAL_LEDGER.md` R-019.
- `POST /api/operations/runs/{operation_run_id}/resume` appends `OperationResumeRequested` and moves a non-terminal run back to queued control state; it does not execute the owner.
- `POST /api/operations/runs/{operation_run_id}/retry` creates an idempotent queued child `OperationRun` for failed/cancelled runs; it does not mutate the terminal parent or execute the owner.
  A linked `failed` action, or a normally cancelled action whose approval was not rejected, is requeued through the
  fixed `requeue_agent_action_for_operation_retry` PG primitive before child/event creation. Completed and rejected
  actions remain fail-closed. The action's `retry_operation_run_id` is a single-chain pointer: only the current chain
  tip may create its child, while an exact requested-child replay is idempotent; retrying a stale ancestor after the
  pointer advances is rejected. Persisted child and event idempotency identities are parent-bound, so the same caller
  key on different parents, including a key equal to a parent key, cannot alias another run. Before reserving the
  action, retry looks up the deterministic child id and validates its persisted idempotency key, workspace, action,
  owner, operation type, and parent identity. An exact replay returns that child's current state, including terminal
  or other non-queued state, plus only the matching creation events already queryable from PG; it does not requeue the
  action, rewrite the child, or recreate missing events. Any child or existing event identity mismatch fails closed.
  The action retry reservation, child creation, and both retry events are not yet one transaction. A crash between
  those steps can leave a reserved pointer without its child/events, or a child with only a subset of its events;
  exact replay exposes but does not repair that partial state. That remaining UoW is tracked by
  `RESIDUAL_LEDGER.md` R-019.
- `POST /api/operations/runs/{operation_run_id}/dispatch` is the owner-adapter handoff. W9b.2 currently supports `export_candidates`: after approval, it plans `workflow_commands(command_type='export.projection.generate', owner='projection_exporter')` with `operation_id=<operation_run_id>` and appends `OperationCommandPlanned`. It does not run the export owner synchronously.
- `POST /api/operations/runs/{operation_run_id}/dispatch` also supports read-only projection actions `filter_projection` and `search_projection`. These actions call the canonical projection reader, persist the bounded result in `OperationRun.result_ref`, and append `OperationReadCompleted` / `OperationReadFailed`; they do not create workflow commands or mutate projection/CRM/person-asset state.
- `POST /api/operations/runs/{operation_run_id}/dispatch` supports `enrich_person_public_web` after approval/budget. Dispatch only plans `workflow_commands(command_type='crm.public_web.queue_batch', owner='crm_public_web_owner')` with `operation_id=<operation_run_id>`; the command owner creates CRM Public Web batch/run rows and queues workers. Operation dispatch must not call the synchronous CRM Public Web start route.
- `POST /api/operations/runs/{operation_run_id}/dispatch` supports company Public Web refresh action `refresh_company_public_web_assets` after approval/budget. Dispatch only plans `workflow_commands(command_type='company.public_web.refresh', owner='company_public_web_owner')`; that root command only orchestrates phase commands. `company.public_web.source.collect` is the only Agent normal path that calls the company Public Web refresh service and writes source-specific rows/artifacts with canonical asset sync deferred. `company.public_web.assets.materialize` is the only Agent normal path that syncs canonical `CompanyAsset` / `CompanyEvidence` and records ActivityRun/Attempt/EntityDelta evidence for company asset effects.
- `POST /api/operations/runs/{operation_run_id}/dispatch` supports CRM writer actions `add_to_crm`, `set_crm_stage`, `add_crm_note`, and `create_crm_task`. Dispatch only plans `crm.record.add_from_projection`, `crm.record.update`, `crm.note.add`, or `crm.task.create`; the `crm_writer` command owner is the only normal path that writes `crm_records`, `crm_engagements`, PG-only `crm_tasks`, and `crm_events`. Sensitive stage changes such as `do_not_contact` / `archived` and bulk stage changes require approval before command planning. Stale running CRM writer commands may be resumed only through `crm_writer.resume_crm_writer_command`; resume records control evidence and requeues the command, but does not write CRM state from the Operation/API request path.
- `POST /api/operations/runs/{operation_run_id}/dispatch` supports first W11 acquisition/profile command adapters. `start_acquisition_run` now plans `acquisition.run.create` as the acquisition root command; it does not plan a discovery query directly and does not call `queue_workflow` inline. `fetch_profile_sample` plans `linkedin.profile_fetch.activity.run`; `continue_acquisition_run` may plan one reviewed command type from the action registry's `allowed_workflow_command_types` when the caller supplies `command_type` and `command_payload`. These adapters only create `workflow_commands`; they do not create jobs, call providers, write registry rows, or publish projections inline. A command type that exists in the durable registry is still invalid for an action unless the action registry explicitly allows it.
- Owner command completion is propagated back into `OperationRun` by the command owner, not by a read repair. `export.projection.generate`, `crm.public_web.queue_batch`, and CRM writer commands update the linked operation/action to `completed`, `failed`, or retry-waiting planned state and append `OperationCommandSucceeded` / `OperationCommandFailed` / `OperationCommandRetryWaiting` with the command id as provenance.
- `POST /api/workflow/commands/{command_id}/cancel` is a safe generic control only for `queued` or `retry_wait` commands. It marks the command `cancelled`; if linked to a non-terminal `OperationRun`, it also marks the operation/action `cancelled` and appends `OperationCommandCancelled`.
- `claimed` / `running` commands require owner-specific control. CRM Public Web phase commands delegate cancel and resume to `crm_public_web_owner`; cancel marks the CRM Public Web run interrupted/cancelled where applicable, and resume records Activity/Attempt/EntityDelta control evidence before returning the command to `queued`. Projection/CRM Public Web export commands now delegate to their export owners, record cancelled/resumed Activity/Attempt/EntityDelta state, and use a temp-file publish checkpoint so a cancelled command does not intentionally publish a normal ZIP artifact. Excel intake commands delegate cancel/resume to `excel_intake_owner`; cancel writes a durable cancel marker and relies on thread/materialization checkpoints before terminalization, while resume only records control evidence and requeues without starting a local thread or writing intake materializations from the request path. Orchestration, provider-attempt, and domain-mutation commands expose owner-specific running cancel/resume at safe checkpoints: orchestration before downstream planning, provider-attempt before provider EntityDelta/downstream evidence, and domain-mutation before ActivityAttempt/EntityDelta/downstream evidence. Provider-attempt command policies expose v1 after-start control: Harvest/Apify/DataForSEO use `provider_after_start_control_status=active` and `provider_after_start_control_mode=poll_cancel_late_result_quarantine`, so after a provider ActivityAttempt starts but before EntityDelta/downstream effects the owner may stop local polling and quarantine late results; Document fetch and Qwen use `provider_after_start_control_mode=fail_closed_until_terminal`. After those boundaries, cancellation remains fail-closed until the owner has a stronger remote interrupt or compensating command; any unsupported future running-control surface must return `running_command_requires_owner_specific_cancel` and expose `running_cancel_upgrade_requirements` instead of relying on generic interruption. Agent code must not mutate module tables to simulate cancellation/resume or treat the generic API as a remote interrupt.
- `POST /api/workflow/commands/{command_id}/retry` requeues `failed_terminal` or `cancelled` commands. If the linked `OperationRun` is already terminal, the command may be requeued as current-state repair, but the operation is not revived; terminal operation retries must use `POST /api/operations/runs/{operation_run_id}/retry` to create a child run.
- `POST /api/workflow/commands/{command_id}/resume` requeues `retry_wait` commands by clearing `not_before_at`. Running resume is not generic; owners that can safely resume or interrupt in-flight provider/artifact/domain work must expose explicit semantics before Agent can call it. Orchestration commands use `workflow_orchestrator.resume_orchestration_command`, which only records control evidence and requeues; it does not run reducers or create downstream commands from the request path. Provider-attempt commands use `workflow_provider_owner.resume_provider_attempt_command`, which only records control evidence and requeues; it does not call providers or write provider-result read models from the request path.
- `GET /api/operations/runs/{operation_run_id}` returns `operation_run.status_summary`, and `GET /api/operations/runs?include_status_summary=true` can include the same bounded queue card in lists. It summarizes operation status, phase, event count, command count/statuses, and latest command through the command API record. It reads only `operation_runs`, `operation_events`, and `workflow_commands`; domain rows remain materialized effects, not queue status truth.
- `GET /api/workflow/commands/{command_id}` returns `workflow_command.execution_summary`, and `GET /api/workflow/commands?include_execution_summary=true` can include the same bounded summary in lists. `GET /api/operations/runs/{operation_run_id}/provenance` returns workflow commands through the same API record shape. This summary is read-only and owner-neutral: it samples ActivityRun/Attempt/EntityDelta evidence for the command, exposes status/kind counts plus latest records, reports `fallback_status=fail_closed`, and does not repair or infer state from CRM, Public Web, projection, Excel, export, registry, or lane tables. Agent UI should use this as the default command status/provenance card before drilling into the dedicated Activity/Attempt/Delta endpoints.
- Agent/UI action and operation cards must render title/category from `operation_action.display_contract` / `operation_run.display_contract`, generated by `operation_runtime.ActionRegistry.display_contract_for`. They may show `action_type` and `operation_type` as technical ids, but must not derive product labels or grouping from action type strings, operation type strings, owner module names, or local frontend maps.
- Agent/UI command cards must render command title/category from `workflow_command.display_contract`, generated by `durable_runtime.workflow_command_display_contract`. They may show `command_type` as a technical id, but must not derive product labels or grouping from command type strings, owners, stage ids, or local frontend maps.
- Agent/UI command cards must render `activity_status_counts`, `attempt_status_counts`, `entity_delta_status_counts`, `entity_delta_kind_counts`, `latest_effect_status`, latest Activity/Attempt/EntityDelta reason/entity, and `sample_truncated` from `workflow_command.execution_summary`. They must not infer effect status from command type strings, timestamps, domain rows, or owner-private result shapes.
- Agent/UI drill-down may load ActivityRun, ActivityAttempt, and EntityDelta rows through the read-only workflow runtime APIs by command id. Those rows are inspection/provenance only; retry/cancel/resume must target the owning OperationRun or WorkflowCommand control API, never the Activity/Attempt/Delta rows directly.
- Agent/UI command-level controls must use the WorkflowCommand control API and render button availability from `workflow_command.control_state`. A command can be visible in an Activity/Attempt/Delta drill-down without being directly controllable through those rows.
- `GET /api/workflow/activities`, `GET /api/workflow/activities/{activity_run_id}`, `GET /api/workflow/activity-attempts`, `GET /api/workflow/activity-attempts/{attempt_id}`, `GET /api/workflow/entity-deltas`, and `GET /api/workflow/entity-deltas/{delta_id}` expose read-only Activity/Attempt/Entity Delta state for Agent debug, retry planning, and provenance inspection. Rows include `control_target` back to the owning workflow command plus its `display_contract`, `control_policy`, and `activity_spine_policy`; they do not execute owner work or mutate lane/domain state. Discovery-lane read models expose the same command-owned `control_target` shape, so Agent/UI code does not derive titles, categories, control behavior, or Activity-spine requirements from lane fields.
- Control/query APIs still do not execute module side effects. W9b must still add more owner-specific execution adapters and user-facing UI before Phase 13.

## Action Registry

| action | owner |
| --- | --- |
| `plan_acquisition` | Planner |
| `start_acquisition_run` | AcquisitionRunWriter |
| `fetch_profile_sample` | ProfileScheduler / AcquisitionRunWriter |
| `continue_acquisition_run` | AcquisitionRunWriter |
| `search_projection` | ProjectionSearchService |
| `filter_projection` | ProjectionSearchService |
| `add_to_crm` | CRMWriter |
| `set_crm_stage` | CRMWriter |
| `add_crm_note` | CRMWriter |
| `create_crm_task` | CRMWriter |
| `enrich_person_public_web` | PersonEvidenceIngestion |
| `refresh_company_public_web_assets` | CompanyPublicWebOwner |
| `promote_person_assertion` | PersonAssertionWriter |
| `export_candidates` | ExportService |
| `external_intake` | IntakeService |

Unknown action types fail closed. Agent-callable actions that plan workflow commands must expose `display_contract`, `allowed_workflow_command_types`, `allowed_workflow_command_contracts`, `workflow_command_control_summary`, and, when applicable, `default_workflow_command_type` / `default_workflow_command_contract` from the action registry so the UI/Agent layer does not infer action labels, command owner, readiness, control capabilities, or Activity spine requirements from private orchestrator code. `operation_runtime.ActionRegistry` is the registration-time gate: it rejects missing display labels/categories/descriptions, unregistered command types, duplicate command types, default commands outside the allowed set, and commands whose `activity_spine_policy` is not Agent-callable. `activity_spine_policy.agent_callable` is necessary but not sufficient for exposure: the ActionRegistry allowlist is the only normal Agent exposure gate. Registry records must expose `workflow_command_exposure_gate=operation_runtime.ActionRegistry.allowed_workflow_command_types`, `workflow_command_exposure_status`, per-command `agent_exposure_gate` / `agent_exposure_status`, and action-level `workflow_command_control_summary`, so UI/Agent code cannot treat the durable owner registry as a product allowlist or recompute running-control maturity from command type strings. `operation_runtime.ActionRegistry.to_record()` is the owner for enriching action specs with workflow command contracts and control summaries; `operation_runtime.ActionRegistry.display_contract_for` is the owner for action/run display copy. HTTP registry handlers must not rederive those contracts independently. Each command contract includes `activity_spine_policy`; commands marked `legacy_internal_pending_activity_spine` are not valid normal Agent action commands. Implemented W10 registry coverage includes acquisition start/continue, profile sample fetch, CRM writer actions, CRM Public Web enrichment, company Public Web refresh, projection/CRM Public Web export, and Excel intake command defaults.

## Approval And Budget Policy

| operation class | default policy |
| --- | --- |
| read-only search/filter/summarize current projection | no extra approval |
| add selected visible candidates to CRM | no extra approval |
| single-record CRM note/task/stage update | no extra approval unless sensitive |
| bulk CRM stage update | approval required |
| do-not-contact/manual-exclude/archive | approval required |
| export contact fields/CRM notes | approval required |
| paid provider acquisition/enrichment | approval and budget required |
| company Public Web refresh | approval and budget required; seed-url-only refresh may use zero-provider budget but still must be explicit |
| destructive migration/backfill/cleanup | explicit operator approval required |

Approval events must be auditable and replayable.

## Staged Acquisition Contract

Agent should support acquisition as a multi-step operation:

```text
plan -> discover candidate list -> publish run_scope_projection row shell -> fetch sample profiles -> user/Agent review -> continue or stop
```

Rules:

- Candidate-list discovery and profile fetch are separate steps.
- Sample fetch size such as 50/100 is explicit in the action input.
- Continuing a run reuses registry state and local profile assets; it must not refetch profiles already fetched.
- The run projection remains scoped to the run unless planner/user explicitly requests baseline merge.
- W11a implementation status: `start_acquisition_run` is a command-planning adapter for `acquisition.run.create`. This command is the causal root for a staged acquisition operation. It records the bounded workflow payload and decomposition contract, but it is not allowed to execute `queue_workflow` inline from operation dispatch. The `acquisition_run_writer` owner now claims `acquisition.run.create`, records that the root is ready for downstream typed commands, plans the first downstream `acquisition.intent.resolve` command, and keeps the linked `OperationRun` in `running` with `operation_completion_deferred=true`; it does not create a legacy job shell or call providers.
- W11b/W11c/W11d/W11e/W11f/W11g/W11h foundation implementation status: `acquisition.intent.resolve` is registered under `acquisition_planner`, claimed by the intent owner, deterministically normalizes the bounded operation payload without model/provider calls, and plans `acquisition.plan.build` with parent-command causality. `acquisition.plan.build` is claimed by the plan owner, creates a bounded typed plan result with explicit downstream stage/command ownership, and plans `acquisition.plan_review.request`; it does not call a model/provider, create a legacy job shell, or call `queue_workflow`. `acquisition.plan_review.request` creates or reuses a pending `plan_review_session`, links it back to the command/operation, and leaves the linked `OperationRun` running with `phase=acquisition_plan_review_requested`. When a plan review is approved, the review API plans `acquisition.plan.commit`; the commit owner verifies the approved review, materializes a canonical PG-only `acquisition_runs` row with `status=committed_pending_probe`, and moves the linked operation to `acquisition_plan_committed_pending_probe` without provider calls or legacy job shell creation. W11c then advances via typed `acquisition.probe.submit`, `acquisition.probe.collect`, and `acquisition.scale.plan` commands from `acquisition_run_id`. W11d adds PG-only `workflow_activity_runs` / `workflow_activity_attempts` as the generic activity spine, `workflow_entity_deltas` as the entity-effect explanation layer, and `acquisition_discovery_lanes` as the acquisition read model. `acquisition.scale.plan` materializes a planned discovery activity/lane with `planned_pending_owner` and does not invoke the legacy job-shell discovery worker. `continue_acquisition_run` can plan `linkedin.discovery_query.run` only from an existing acquisition lane/activity; legacy job/snapshot discovery payloads are rejected as Agent input. `operation_native_discovery_activity_owner` now claims operation-native discovery commands, records activity attempts, runs provider-backed discovery in operation-native mode, writes model-safe artifacts, records candidate entity deltas, and plans downstream `linkedin.profile_fetch.activity.run`. W11e makes `fetch_profile_sample` default to that operation-native profile activity command, whose owner records cache-hit/fetch-required Activity/Attempt/EntityDelta facts without calling `queue_workflow` or creating a legacy job shell. W11f adds `linkedin.profile_fetch.provider.fetch` and `linkedin.profile_terminal.admit`: provider fetch consumes `profile_fetch_required` deltas, records provider attempts, marks fetched raw paths in `linkedin_profile_registry`, and emits `profile_provider_fetched` deltas without direct Agent registry mutation; terminal admit consumes cache-hit/provider-fetched deltas and emits `profile_terminal_recorded` deltas without mutating projection membership. W11g adds `projection.profile_admission.apply`, owned by `serving_projection_owner`, to consume terminal deltas, write canonical run-scope projection membership, and emit `projection_member_admitted` deltas. W11h plans `projection.person_search_index.build` and `collection.authoritative.merge` commands after operation-native run-scope projection publication on the same Operation workflow id, without creating a legacy job shell or normal-path materialization item; those owners now record ActivityRun/Attempt/EntityDelta evidence for index and collection effects. W11i starts extending the same spine to CRM Public Web phase commands: each per-run phase command records ActivityRun/Attempt evidence and a run-level EntityDelta, so Agent/debug queries no longer infer Public Web progress only from command result JSON. W11i follow-up extends CRM writer commands onto the same spine: `crm.record.add_from_projection`, `crm.record.update`, `crm.note.add`, and `crm.task.create` record ActivityRun/Attempt evidence plus EntityDeltas for CRM record/event/note/task effects. Retryable discovery/provider/Public Web waits stay in retry-wait attempts; legacy owners skip operation-native commands instead of claiming them.
- W11c target: split candidate-list acquisition into typed probe/scale/discovery commands such as `acquisition.probe.submit`, `acquisition.probe.collect`, `acquisition.scale.plan`, and `linkedin.discovery_query.run`.
- W11d target: provider-backed discovery uses Command -> ActivityRun -> ActivityAttempt -> EntityDelta, and domain lane rows are read models only.
- W11e target: profile fetch uses `linkedin.profile_fetch.activity.run` as the Agent-facing cache/planning boundary; legacy `linkedin.profile_refill.submit_batch` remains workflow-internal.
- W11f target: provider fetch uses `linkedin.profile_fetch.provider.fetch`; profile registry terminalization and post-profile materialization remain behind `linkedin.profile_url_terminal.record`, `linkedin.local_profile_delta.apply`, `projection.board_visible_patch.publish`, `projection.run_scope.finalize`, projection index/facet builders, collection merge, and snapshot compaction.

`acquisition.run.create` must not become a new monolith. The owner may create/reuse canonical acquisition current state only after the acquisition root/plan-commit command chain is claimed; any provider calls, registry writes, profile fetches, projection writes, or collection merges must be planned as downstream typed commands with causal links to the root command and `acquisition_run_id`.

## Framework Adoption Boundary

Temporal could replace parts of the homegrown long-running workflow/recovery runtime only if workflows are thin deterministic orchestration and provider/local side effects are activities.

LangGraph could implement multi-turn Agent orchestration only if graph nodes emit `AgentAction` and wait for module-owned operation results.

Required adapter boundary:

```text
Framework runtime <-> AgentAction / OperationRun <-> module owner writer
```

Not allowed:

- Framework checkpoint as the only CRM/projection/workflow source of truth.
- Provider I/O inside deterministic workflow/graph code without an activity/tool boundary.
- Agent graph directly writing module tables.
- Losing existing PG/scripted smoke reportability.

## Tests And Gates

Implementation is incomplete until tests prove:

- Unknown action types fail closed.
- Idempotency keys prevent duplicate CRM/workflow/export operations.
- Approval-required actions cannot execute before approval.
- Paid provider actions cannot execute without budget and provider-mode checks.
- Agent-triggered CRM writes produce the same `CRMEvent` shape as direct frontend writes.
- Agent-triggered acquisition produces a linked run/projection and obeys profile scheduler gates.
- Staged sample fetch can stop after sample completion and continue without refetching existing profiles.
