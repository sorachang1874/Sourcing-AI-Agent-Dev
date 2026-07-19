# Durable Execution Runtime Contract

> Status: Current architecture contract drafted 2026-05-21. Use with `EVENT_LEVEL_WORKFLOW_RESPONSE.md`, `WORKFLOW_PROGRESS_CONTRACT.md`, `CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `AGENT_OPERATION_CONTRACT.md`, `CRM_STATE_CONTRACT.md`, `PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, and `NEXT_TODO.md` before changing workflow orchestration, recovery, provider scheduling, durable queues, Agent operations, or legacy job/materialization paths.

## Purpose

The target architecture is a durable execution runtime, not only a workflow engine.

The runtime must support acquisition workflows, LinkedIn profile refill, post-profile local apply, board-visible publication, projection/index builders, CRM operations, Public Web enrichment, export, future Agent operations, approval gates, retries, recovery, and audit. The first migration target is the LinkedIn acquisition/profile pipeline because it currently carries the most provider scheduling, progress, recovery, and materialization complexity.

The goal is to make execution reliable by construction:

- state transitions are event-driven and auditable
- reducers, not workers, decide the next commands
- side effects are command-owned and idempotent
- recovery resumes from the typed owner of the failed state
- public readers consume read models and never repair execution state
- legacy durable queues are retired instead of expanded into another fallback layer

## System Layers

### Operation Layer

`OperationRun` represents a user or Agent intent.

Examples:

- start acquisition for a company and scope
- fetch a profile sample
- continue a staged run
- add selected candidates to CRM
- promote Public Web evidence
- export selected candidates

Operation state captures intent, approval, budget, user/Agent provenance, and high-level status. It does not own provider calls, profile registry mutation, projection writes, or CRM writes directly.

W8 operation persistence contract:

- `agent_actions`, `operation_runs`, `acquisition_runs`, `workflow_activity_runs`, `workflow_activity_attempts`, `workflow_entity_deltas`, `acquisition_discovery_lanes`, `operation_events`, Agent tool result aggregate (`agent_tool_result_slots`, `agent_tool_result_attempts`, `agent_tool_result_journal`), CRM task current-state (`crm_tasks`), and canonical company fact/media current-state (`company_assets`, `company_evidence`, `company_assertions`) are PG-only durable/current-state tables.
- New Operation/W11/CRM current-state tables must be introduced through Postgres live DDL first; adding a SQLite normal-path schema or fallback for these tables is forbidden.
- `OperationRuntimeWriter` may persist user/Agent intent and operation events only.
- Unknown action types fail closed through the action registry.
- Approval-required actions stop at `AgentAction(status='approval_required')` plus an append-only approval-required event until W9 approval execution APIs approve them.
- Budget-required actions require explicit budget before persistence.
- A schema-defined action request is owned by checked-in `ActionRequestSpec`; its single strict schema covers closed
  `input_payload` and `target_ref` segments and is validated/digested by the same D0 `ToolSpec` owner used for model
  tool parsing. Caller/model values may populate only `input_payload`; the action owner mints `OwnerBoundTargetRef`.
  Raw caller targets, cross-owner targets, duplicate/alias owner fields, and caller-supplied pin fields fail before the
  first action write.
- `agent_actions` and `operation_runs` carry physical `request_schema_version` and `request_schema_digest` columns.
  The action writer derives the pair from the checked-in registry; each immediate, approval-created, or retry-child run
  copies the exact action pair. Native upsert replay, approval, retry, and dispatch compare the persisted action/run
  pair and current registry before accepting work. Dispatch also revalidates a schema-defined persisted request before
  invoking an adapter; drift returns a zero-module-mutation conflict.
- Migration `0002_action_request_schema_pins.sql` permits only empty/empty or a normalized non-empty version paired with
  a lowercase 64-hex digest. It uses a five-second local lock budget and `NOT VALID` checks so installation does not
  scan populated tables; validation of existing rows is a later, separately deployed transaction. The database CHECK
  owns pair shape; repository/upsert and runtime preflight own immutable replay identity. No direct-SQL
  immutability-trigger guarantee is claimed.
- In the D1m candidate registry, ten production actions have implemented explicit closed schemas and owner-minted
  targets; the remaining **5/15**
  stay on the R-029 schema-less compatibility bridge with empty/empty physical pins. Schema-less submissions record
  `request_schema_status=schema_less_compatibility` and `request_schema_compatibility_hit=true`, while their
  replay/approve/retry/dispatch continuations record a pre-mutation, release-epoch-scoped
  `ActionRequestSchemaCompatibilityObserved` event, including brownfield origin. Served Agent tool population remains
  zero. The bridge closes only after all API-submittable actions have reviewed schemas/owner binders and the complete
  population records zero hits for one release window; measuring only a future served subset is insufficient.
- The D1m candidate activates only `refresh_company_public_web_assets` with deterministic `seed_url_only` input. The
  request requires canonical company, source-family, and normalized seed-URL fields; the owner mints exact
  `workspace_id + company_key` through the canonical alias resolver. Authenticated transport uses server-derived scope
  and open mode preserves explicit operator workspace. Persisted action/run, dispatch, root refresh, source collection,
  and asset materialization each reload and exact-compare the canonical request/target plus OperationRun/AgentAction
  and command causality before their new effect. Source collection uses one normalized idempotency-key owner: a partial
  unique index plus sorted key/run advisory locks prevent split identity; current command attempt and lease ownership
  fence reclaim and terminalization; stale attempts return `owner_lost` without changing the source run. Completed
  D1m root, source, and materialize drains opt into bounded expired-`claimed` recovery while the shared default remains
  unchanged. Activity start and exhausted-final-source closure use the PostgreSQL clock, deterministic ActivityRun plus
  all deterministic ActivityAttempt identities through the current attempt, and full immutable validation of primary
  ids/keys, workspace, command/activity/workflow/operation/type/owner/provider links, attempt, lease, and owner metadata.
  Split identities, alternate nonterminal rows, current/future terminal execution Activity/Attempt rows, future/malformed resume
  evidence, semantic drift, and still-active database leases fail closed before writes; fully exact prior terminal
  execution evidence may remain. A succeeded owner-specific resume terminal may coexist only under its deterministic
  resume id/key, at generation `<=` current, with complete workspace/activity/workflow/command/provider/request-ref/
  lease and target/company/boolean-force/nonblank-output-reason semantics. Successful takeover first closes exact
  superseded prior running execution attempts; returned Activity/Attempt rows are accepted only as the exact current
  `running` spine. Exhausted closure atomically fails the exact Command/Activity/Attempts, retains a valid resume-control
  Attempt, and converges an exact current failed owner-loss partial only when error/metadata/output share one nonblank
  reason, `output.status=skipped`, `error.owner_lost=true`, and
  `error.deterministic_terminal_failure=false`. Completed materialization consumes
  `company_public_web_run_snapshot_v3`, whose digest binds assets, summary, artifact paths,
  artifact-publication digest, positive source revision/completion time, started_at, and completed_at, rather than
  rereading mutable source rows or a later clock. Positive logical revision
  owns latest ordering, with explicit brownfield fallback; exact-claim canonical rows share one PG transaction. These
  bounded owner/effect fences do not claim a global
  submit/command/effect/terminal/linked-Operation UoW or close R-019. The current direct state-sync caller ratchet is
  **24**. Served population remains zero because this action still has no populated action-specific revisioned result
  spec, result/simulate serializer mapping, or complete served predicate.
- D1i's `start_acquisition_run` root validates exact OperationRun/AgentAction/request/target plus canonical source-event/
  causality and current claim-owner/attempt/lease authority. Lease validity uses the PG repository clock with persisted
  naive timestamps interpreted as UTC, and persisted root/event/child/result contracts use strict JSON container and
  scalar-type checks. The specialized completion locks root + stream and commits the root-plan event, deterministic
  intent child, physical downstream edge, and root terminal in one PG transaction. An actual-root-scoped trigger plus
  parent identity advisory locking prevents any producer from attaching a second child or wrong-type child to that
  root, while preserving generic workflow fan-out; conflict reread accepts only the exact canonical winner. Succeeded
  replay exact-matches that complete persisted tuple while treating scheduler
  fields as mutable lifecycle state. A driver exception after successful commit is reconciled only by a fresh
  authoritative succeeded read plus exact replay; pre-commit failures are re-raised. R-019 remains open because this is
  not a global command-generation fence, OperationRun/AgentAction preflight is outside this UoW,
  state/wakeup/linked-Operation sync remains post-commit, and a committed failure CAS can still lose its acknowledgement.
  D1i's **26-call** ratchet is a historical checkpoint; the current D1m state-sync ratchet is **24**. No
  provider/model/live authorization follows.
- Operation-layer persistence must not create `workflow_commands`, CRM rows, projection rows, person assets/evidence/assertions, provider registry rows, or export artifacts. Those remain module-owner effects.
- W9 backend operation controls may approve, reject, query, and cancel operation state through operation runtime tables and append-only events. They must still not execute module side effects or bypass workflow command owners.
- `store.repos.workflow_runtime` is the public storage owner for `agent_actions`, `operation_runs`, `operation_events`,
  `acquisition_runs`, `acquisition_discovery_lanes`, `workflow_activity_runs`, `workflow_activity_attempts`, and
  `workflow_entity_deltas`; the retired `ControlPlaneStore` operation/acquisition/activity facades must not be restored.
- Acquisition run/lane and ActivityRun/ActivityAttempt writes lock both primary-key and
  `(workspace_id, idempotency_key)` identity scopes in a deterministic order. Primary/idempotency identity collisions
  fail closed; immutable workflow/operation/command/activity ownership cannot drift; JSON object patches merge under
  the row lock; and terminal rows cannot be reopened or rewritten by a stale writer. EntityDelta rows are write-once
  effect evidence: exact replay returns the committed row and a later payload cannot rewrite the recorded fact.
  `acquisition_discovery_lanes` remains a domain read model and never owns retry/cancel/resume semantics.
- Ordinary action/run state updates use expected-status compare-and-set and return the committed row. Callers
  must validate the committed target before emitting a success event or reporting success. A stale writer must
  not reopen `completed`, `failed`, `cancelled`, or `rejected` action state, or a terminal operation run.
- Reject and cancel are fixed PG UoWs. `ActionRejected` commits atomically with action
  `status=cancelled, approval_status=rejected`; `OperationCancelled` commits atomically with operation
  `status=cancelled` and the eligible linked-action cancellation. Event failure, identity mismatch, or CAS conflict
  rolls back the whole control transition. Duplicate target-state requests reuse the idempotent event; a legacy
  target state with a missing event is repaired in the same transaction.
- Operation control APIs report `rejected` / `cancelled` only from the committed row. A concurrent terminal winner
  returns `status=conflict` and HTTP 409 and must not create a loser success event. This guarantee is bounded to
  the implemented reject/cancel UoWs plus committed-target guards for approve/resume/retry. Command planning and
  generic operation+action+event synchronization still require the UoWs tracked by `RESIDUAL_LEDGER.md` R-019.
- Owner-specific acquisition plan-commit, scale-plan, and profile-fetch pre-effect cancellation is one fixed PG UoW.
  It locks the command, acquisition run, relevant activity/lane rows, and attempt/delta/downstream blockers; validates
  command type/owner, causal identity, lease or explicit force, terminal winner, and the no-effect boundary; then
  commits every module row and the workflow-command cancellation together. It returns only structured committed
  outcomes (`applied`, `repaired`, `already_applied`, `blocked`, `conflict`, `not_found`). Exact replay preserves
  timestamps, matching legacy partial state can be repaired, and faults roll back the entire transition. This closes
  `RESIDUAL_LEDGER.md` R-020's existing-row partial-commit boundary.
- This UoW does not fence a stale owner from first creating a new downstream child or ActivityAttempt after the cancel
  transaction commits, and it does not include linked OperationRun/AgentAction post-commit synchronization. Those
  guarantees require parent-command generation/lease-token ownership fencing and operation synchronization under
  `RESIDUAL_LEDGER.md` R-019; R-020 closure must not be used as evidence that the wider acquisition chain is atomic.
- D1 request-pin preflights occur before approve/retry writes on a drift path, but the existing action/event/run and
  retry reservation/child/event sequences are still multi-step. They do not close R-019's UoW, command-generation /
  lease-token, post-sync, or total transaction-lock acquisition-budget boundaries. Concurrent identity/state changes
  after the read preflight remain an R-019 race; the zero-write guarantee is limited to preflight-observed drift.

### Workflow Layer

`WorkflowRun` represents an execution plan for one operation or system maintenance task.

Examples:

- LinkedIn acquisition/profile workflow
- collection authoritative merge
- CRM Public Web enrichment
- projection search-index build
- export materialization

Workflow state is advanced by reducers from durable events and current state. A workflow can create commands for domain owners, but it does not execute side effects inline.

### Command Layer

`WorkflowCommand` is a claimable, retryable, idempotent unit of side-effect work.

Examples:

- `profile.refill.submit_batch`
- `profile.registry.record_terminal_urls`
- `local_apply.apply_profile_delta`
- `board_visible.publish_delta_patch`
- `projection.index.build`
- `collection.merge.authoritative`
- `crm.record.add_from_projection`
- `crm.record.update`
- `crm.note.add`
- `crm.task.create`
- `public_web.crm.search`
- `export.projection.generate`

Each command type maps to exactly one owner in the command owner registry.

Command control surface:

- `GET /api/workflow/command-registry`, `GET /api/workflow/commands`, and `GET /api/workflow/commands/{command_id}` are bounded query surfaces. They never execute owner work or repair module state. The command registry is not the Agent action allowlist: each entry must expose `agent_exposure_gate=operation_runtime.ActionRegistry.allowed_workflow_command_types` and an `agent_exposure_status` such as `action_registry_allowlisted` or `not_action_registry_allowlisted`.
- The same query surfaces must expose `control_policy` from `durable_runtime.workflow_command_control_policy`: generic cancel/retry/resume status sets, owner-specific running-cancel support, explicit running-resume block/support policy, delegate owner, prerequisites, blocked reason, upgrade requirements, unsupported-running reason, fallback status, and module-state mutation expectation. Agent/UI code must read this policy instead of hard-coding which commands can be interrupted or resumed.
- The same query surfaces must expose `control_state` from `durable_runtime.workflow_command_control_state`: current command status, allowed `cancel` / `retry` / `resume` actions, cancel/resume mode (`generic` vs `owner_specific`), disabled reasons, and whether cancel/resume mutates module state. Agent/UI code must render buttons from this status-specific state, not from local status-string heuristics.
- The same query surfaces must expose `activity_spine_policy` from `durable_runtime.workflow_command_activity_spine_policy`: whether the command must write ActivityRun/Attempt/EntityDelta evidence, only creates a planned downstream activity boundary, or is orchestration-only. The normal owner registry must not contain `legacy_internal_pending_activity_spine`; if a migration accidentally reintroduces that policy, Agent/UI code must fail closed and never expose it as a normal action.
- The same query surfaces must expose `display_contract` from `durable_runtime.workflow_command_display_contract`: display label, display category, and description. Agent/UI code may render raw `command_type` only as a debug identifier; product labels must come from the display contract so command presentation does not become another frontend heuristic.
- Operation action/run query surfaces and operation control responses must expose `display_contract` from `operation_runtime.ActionRegistry.display_contract_for`: action display label, product category, and description. Operation control responses expose the same contract at the top level beside `control_state`. Agent/UI code may render raw `action_type` / `operation_type` only as debug identifiers; product labels must come from the registry-owned display contract.
- `POST /api/workflow/commands/{command_id}/cancel` is generic only before owner execution starts: accepted statuses are `queued` and `retry_wait`; `claimed` and `running` require owner-specific cancellation because provider calls, artifact writes, or domain mutations may already be in flight. Command control responses must expose top-level `display_contract`, `control_policy`, and `activity_spine_policy` in addition to the nested `workflow_command` copy, so Agent/UI callers can render labels, capability, and provenance requirements without deriving semantics from command names.
- W11 owner-specific control first slice: `crm_public_web_owner` phase commands in `claimed` / `running` may be cancelled through the same command API only by delegating to the CRM Public Web owner. The owner cancels the CRM Public Web run, interrupts owned workers where applicable, marks the `workflow_command` cancelled from `claimed/running`, and records cancelled ActivityRun/ActivityAttempt plus a `crm_public_web_run` EntityDelta. Projection export and CRM Public Web export commands also support owner-specific running cancel through `projection_exporter.cancel_export_command` / `crm_public_web_exporter.cancel_export_command`; the owners record cancelled Activity/Attempt/EntityDelta evidence and publish artifacts through a temp-file checkpoint that checks command cancellation before normal ZIP publication. Provider-attempt commands support owner-specific running cancel before provider EntityDelta or downstream command evidence exists. Before an ActivityAttempt exists this is a local pre-attempt cancel; after a Harvest/Apify/DataForSEO ActivityAttempt exists this is local `poll_cancel_late_result_quarantine` that stops polling and quarantines late results without killing remote provider work. `linkedin.profile_fetch.activity.run` supports owner-specific running cancel only at the pre-cache-lookup checkpoint through `linkedin_profile_activity_owner.cancel_before_cache_lookup_attempt`. Unsupported running commands still fail closed with `running_command_requires_owner_specific_cancel`; the generic API must not invent module cancellation semantics.
- Excel commands support owner-specific cooperative running cancel through `excel_intake_owner.cancel_excel_intake_run_command` and owner-specific running resume through `excel_intake_owner.resume_excel_intake_run_command`. The cancel owner writes a durable command-cancel marker, marks the Excel job read model cancelled, records cancelled Activity/Attempt/EntityDelta evidence, and the async thread checks the command before terminalization and at major materialization checkpoints so a cancelled command cannot later be terminalized as succeeded. The resume owner records Activity/Attempt/EntityDelta control evidence and requeues stale/forced commands without starting a local thread or writing intake results from the request path. Generic cancellation still does not interrupt local threads; all claimed/running Excel cancellation or resume must go through the owner delegate.
- Every owner-registry command must expose a non-placeholder running-control reason when running cancel is unsupported. Provider/probe commands expose owner-specific pre-attempt cancel and therefore must fail closed only after provider ActivityAttempt, EntityDelta, or downstream-command evidence exists; later unsupported orchestration commands use `orchestration_command_must_finish_or_plan_compensating_command`; domain mutation/materialization commands expose owner-specific pre-attempt cancel and therefore must fail closed only after ActivityAttempt, EntityDelta, or downstream-command evidence exists; `owner_has_no_safe_inflight_interrupt` remains the reserved reason for future artifact/thread owners that have no safe owner checkpoint. Unsupported policies must also expose `running_cancel_upgrade_requirements` so future work cannot enable running cancel by flipping a boolean. `owner_specific_interrupt_not_implemented` is a development placeholder only and is blocked by W10 preflight for normal owner-registry commands.
- Running-control category membership is itself a contract. `durable_runtime.workflow_command_running_control_categories(command_type)` must return exactly one category for every owner-registry command, and the public `control_policy` record must expose that value as `running_control_category` plus the audited `running_control_categories` list. Commands that combine provider work and domain materialization must either be split into smaller typed activities or deliberately pick one control category until the split exists; branch order in `workflow_command_control_policy(...)` must never decide the public blocked reason.
- Running-control maturity is a contract, not a UI inference. The public `control_policy` record must expose `running_control_maturity`, `running_control_gap_status`, and `running_control_surface=workflow_command_control_api_only`. Supported owner-specific cancel+resume commands use `owner_specific_cancel_resume` / `closed`; owner-specific cancel without running resume uses `owner_specific_cancel_only` / `partial_resume_gap_reported`; owner-specific resume without safe running cancel uses `owner_specific_resume_only` / `partial_cancel_gap_reported`; unsupported running control uses `fail_closed_with_upgrade_requirements` / `accepted_fail_closed_pending_owner_specific_control`. Agent/UI code must not derive these values from command type strings or blocked reasons.
- `POST /api/workflow/commands/{command_id}/retry` requeues `failed_terminal` and `cancelled` command rows as command current-state repair. It does not revive a terminal `OperationRun`; terminal user intent retry creates a child run through the operation API.
- `POST /api/workflow/commands/{command_id}/resume` requeues `retry_wait` command rows and clears `not_before_at` so the owner can claim them immediately.
- `claimed` / `running` command resume is fail-closed unless a command owner explicitly registers owner-specific resume support. Supported owner-specific resume cases are deliberately requeue-only unless the owner has an explicit stronger contract. CRM Public Web phase commands delegate to `crm_public_web_owner.resume_crm_public_web_phase_command`, require `payload.run_id`, require an expired command lease unless the caller uses explicit force, record Activity/Attempt/EntityDelta evidence for the resume/requeue decision, and return the command to queued for normal owner drain. Orchestration commands delegate to `workflow_orchestrator.resume_orchestration_command`, require an expired command lease unless forced, record Activity/Attempt/EntityDelta control evidence, and requeue without executing reducer/owner side effects in the request path; idempotent downstream command keys remain the owner retry safety boundary. Provider-attempt commands can be cancelled before provider EntityDelta/downstream evidence through `workflow_provider_owner.cancel_or_poll_stop_provider_attempt` or the company Public Web source owner, and can be resumed through `workflow_provider_owner.resume_provider_attempt_command`; both delegates require expired lease unless forced, record Activity/Attempt/EntityDelta control evidence, and must not call providers, kill remote work, write registry rows, or publish artifacts from the request path. Domain-mutation commands can be cancelled only before ActivityAttempt/EntityDelta/downstream evidence through `workflow_domain_owner.cancel_before_domain_mutation_attempt`, and can be resumed through `workflow_domain_owner.resume_domain_mutation_command`; both delegates require expired lease unless forced, record control evidence, and must not write registry, projection, collection, snapshot, CRM, Public Web, media, or asset read models from the request path. `export.projection.generate` and `export.crm_public_web.generate` delegate to their export owners, require an unpublished artifact plus expired command lease unless forced, record export Activity/Attempt/EntityDelta resume evidence, and requeue without publishing an artifact. Unsupported running resume returns `running_command_requires_owner_specific_resume` in `control_state.disabled_reasons.resume` plus a typed `running_resume_blocked_reason` and `running_resume_upgrade_requirements` in `control_policy`. Agent/UI code must not use resume as a generic repair for in-flight provider calls, artifact writes, local threads, or domain mutations.
- Command controls update command current-state only. Linked `OperationRun` / `AgentAction` status is synchronized through operation events when doing so does not violate terminal-state monotonicity.

### Activity Layer

`ActivityAttempt` is one real execution attempt of a command.

Activities may call providers, read/write artifacts, update domain stores through their owner, or publish compact read-model events. They are not replayed deterministically. They must be idempotent and must record result events that allow recovery to continue safely.

W11d activity spine:

- `workflow_activity_runs` is the generic bounded side-effect current-state table. It is owned by the command owner that created or claimed the work, not by public readers or domain read models.
- `workflow_activity_attempts` records per-attempt provider refs, rate-limit/retry state, errors, input/output envelopes, and artifact refs. An activity may have zero attempts while it is only planned, and multiple attempts after retry.
- `workflow_entity_deltas` records entity-level effects produced by activities. It explains candidate/evidence/asset/assertion/projection membership creation, non-creation, suppression, no-op, and not-applied reasons with source refs, artifact refs, and projection effects.
- Domain-specific tables such as `acquisition_discovery_lanes` may reference an activity by `activity_run_id`, but they must not own retry/cancel/resume semantics.
- Agent-facing retry/cancel/resume/debug operations target Operation/Command/Activity APIs. They must not mutate lane, registry, projection, CRM, Public Web, or export tables directly.
- `workflow_command_activity_spine_policy` is the policy owner for which command types must leave Activity/Attempt/EntityDelta evidence. Adding a new Agent-callable side-effect command requires updating that policy and its fast preflight before wiring UI/Agent access.
- `linkedin.discovery_query.run` operation-native commands must carry `acquisition_run_id`, `lane_id`, and `activity_run_id`. Normal Agent/Operation dispatch must reject legacy `job_id`/`snapshot_id` discovery payloads. `operation_native_discovery_activity_owner` is the only normal owner for these commands: it records `workflow_activity_attempts`, executes provider-backed discovery with `runtime_mode=operation_native_discovery`, writes activity artifacts, and records candidate `workflow_entity_deltas` before any downstream profile/projection command may consume the result.
- Retryable provider errors are represented as retry-wait activity attempts. Zero-result discovery is represented as a no-op `workflow_entity_deltas` row so Agent/debug queries can explain why the lane did not produce candidates or projection membership.
- The legacy search-seed queue owner must report-visibly skip operation-native discovery commands and leave them claimable for `operation_native_discovery_activity_owner`.
- `linkedin.profile_fetch.activity.run` is the Agent-facing profile-fetch boundary. It consumes profile URLs or candidate entity deltas, records a profile-fetch `workflow_activity_run`, records a cache/planning `workflow_activity_attempt`, and emits profile `workflow_entity_deltas` such as `profile_cache_hit` or `profile_fetch_required`. It must not create a legacy job shell, call `queue_workflow`, or invoke `linkedin.profile_refill.submit_batch` as an Agent normal path. A claimed/running command may be cancelled only before the cache/planning attempt starts, before any profile entity delta is recorded, before downstream commands are planned, and after lease expiry or explicit force; the owner must mark linked activity/run state `cancelled_before_cache_lookup` instead of deleting evidence.
- `linkedin.profile_fetch.provider.fetch` consumes `profile_fetch_required` deltas from the same activity, records a provider `workflow_activity_attempt`, writes fetched raw-path evidence to `linkedin_profile_registry`, and emits `profile_provider_fetched` deltas. It is retryable as a command/activity boundary; Agent or recovery must not mutate registry rows directly to simulate a provider fetch.
- Provider-attempt retry never overrides profile URL item semantics. For LinkedIn profile fetch, partial provider success is item-level: fetched URLs must immediately emit `profile_provider_fetched` deltas and plan `linkedin.profile_terminal.admit`; only failed/unresolved URLs may emit `profile_provider_retry_bucketed` and plan a retry-wave `linkedin.profile_fetch.provider.fetch` command. The retry-wave command payload must carry `provider_attempt_scope='retry_wave'`, `retry_wave_index`, `retry_unit='linkedin_url_key'`, and `retry_strategy='bucketed_entity_retry_after_normal_wave'`, must not include successful normal-wave URLs, and must not call `linkedin.profile_refill.submit_batch` or mutate registry/projection rows from the control endpoint. After the retry budget is exhausted, failed URLs emit `profile_provider_retry_exhausted` and late/old provider results remain quarantined evidence unless an explicit owner command adopts them.
- Provider-attempt after-start control is a separate machine-readable contract, not implied by pre-attempt cancel/resume. Provider-attempt command `control_policy` must expose `provider_after_start_control_contract='w11_provider_after_start_control_v1'`. Harvest/Apify/DataForSEO-family provider attempts expose `provider_after_start_control_status='active'` and `provider_after_start_control_mode='poll_cancel_late_result_quarantine'`: after ActivityAttempt creation but before any EntityDelta or downstream command, cancel stops only local polling/waiting, marks the attempt `cancelled_remote_ignored`, and requires any late remote result to be recorded as ignored/quarantined evidence unless a later explicit owner adoption command accepts it. Document fetch and Qwen adjudication expose `provider_after_start_control_status='active'` and `provider_after_start_control_mode='fail_closed_until_terminal'`: no mid-attempt cancel is exposed, and retry/control starts only from terminal evidence. Generic control must not kill remote work, write registry/projection/CRM/Public Web/asset rows, or mark late provider results accepted from the control API.
- DataForSEO Standard Queue batch envelopes are transport batches, not retry/progress units. `task_post` may submit many query tasks in one HTTP request, but each query item must have a caller-assigned stable `task_key` carried through the provider request tag and returned checkpoint metadata as `query_identity_key`. Downstream joins, retries, manifests, and Public Web outcomes must use this key, not request/response array order or candidate display ordinal. Public Web query task keys must be scoped to stable candidate/query identity, not the candidate's position in the current batch. Search seed discovery worker keys must be scoped to stable search query signature, bundle, and employment scope, not the query's ordinal in the current query list. Exploratory enrichment query keys must be scoped to stable candidate/query identity, not `candidate_id::index`. The provider layer must fail closed when a batch spec lacks `task_key`; it must not generate identity from query text, task id, candidate ordinal, or array position. The normal path may map a provider response task only by provider-echoed `tag` or unambiguous provider-echoed `keyword`; if the provider omits query identity, the item must fail closed to query-level retry/failure instead of using request-order fallback. Per-task failures must be retried at `retry_unit='search_query'` with `retry_strategy='dataforseo_batch_failed_query_retry_only'`; successful task ids must stay submitted and must not be resent with failed queries. `task_get` batch fetch is likewise task-id level: one `task_get` failure must produce failed/retryable evidence for that `dataforseo_task_id` while preserving successful fetched responses from sibling tasks. Public Web, seed-discovery, and exploratory-enrichment consumers must treat failed query/task checkpoints as item-level outcomes, not as proof that the whole batch envelope failed.
- DataForSEO readiness is also item-level. `tasks_ready` is a readiness hint, not the source of truth; owners may run a bounded `task_get` direct probe for each waiting task id and must record `provider_status_code`, `provider_status_message`, `provider_wait_state`, and `readiness_strategy` on that query's checkpoint. Status codes `40601` (`Task Handed`) and `40602` (`Task In Queue`) are provider-pending states, not terminal failures. After a query has exhausted its local ready-poll budget while still provider-pending, the owner may reset only that query by submitting a replacement task with the same `task_key` / `query_identity_key`, preserving `previous_task_ids`, incrementing `provider_task_reset_attempt_count`, and reporting `provider_task_reset_reason='provider_pending_after_poll_budget'`. This reset is bounded by `max_provider_task_reset_attempts` and must not resubmit sibling queries or recreate the CRM Public Web batch/run. Provider-pending age is governed by the independent `max_provider_pending_wait_seconds` budget; exceeding `max_remote_search_wait_seconds` while the task is still explicitly `40601` / `40602` must keep the run in `waiting_remote_search` and expose `provider_pending_wait_deferred` rather than terminalizing. Only hard provider errors, unknown never-ready states past `max_remote_search_wait_seconds`, or provider-pending states past `max_provider_pending_wait_seconds` may become query-level terminal timeout evidence. A timed-out query is terminal evidence for that query only, not permission for whole-batch retry.
- Model adjudication is a bounded provider activity with fail-visible fallback. OpenAI-compatible relay health must verify the configured model through the configured generation API (`/responses` for `api_style=openai_responses`, otherwise `/chat/completions`); `/models` alone is inventory evidence, not readiness proof. Health probes are not allowed to become recurring LLM workload: normal healthcheck responses must be cached, and any failed generation must open a provider/base-url/model circuit breaker so repeated health probes, recovery ticks, or AI adjudication retries fail fast locally during cooldown. Each OpenAI-compatible generation records `requested_model` from configuration separately from provider-authored `response_model`/`effective_model`; `model_identity_provenance=provider_response` is emitted only when that response identity exists. Normal CRM Public Web real-provider adjudication has the additional pre-transport invariant `OpenAICompatibleChatModelClient.settings.model == CRM_PUBLIC_WEB_PRODUCT_MODEL == 'gpt-5.6-sol'`. A missing or different configured model sends no provider request and returns deterministic fail-closed evidence with `fallback_reason=model_configuration_mismatch` plus `model_error`; this local product-contract failure does not open the shared model circuit or constrain non-Public-Web calls on the same client. `QwenResponsesModelClient` is not a product-model fallback: its Public Web method returns the same configuration evidence before any Qwen prompt, while Qwen's other methods remain available. Deterministic, offline/scripted, and fake clients remain outside that real-provider invariant. Health and W7g product validation fail closed when the provider response model is missing or does not exactly match the requested/expected model. A bounded `model_usage` record may carry only input/output/total/cached-input/reasoning-output token counts; it must not retain raw provider payloads. When a model call fails, returns empty text, returns non-JSON, or is blocked by the circuit breaker, the owner may use deterministic fail-closed fallback to avoid unsafe promotion, but the result and phase metrics must carry `provider`, requested `model`/`model_version`, `requested_model`, `fallback_used=true`, `fallback_reason`, and `model_error` or response preview, without fabricating `effective_model` or provider-response provenance. Readers, live validation, and Agent surfaces must treat fallback or missing effective-model proof as incomplete AI adjudication evidence, not as successful model review.

#### Model Identity Field Ownership

| Field | Owner / source of truth | Allowed values and derivation | Consumers / fallback / migration |
| --- | --- | --- | --- |
| `requested_model` | `OpenAICompatibleChatModelClient`, copied from startup-scoped `ModelProviderSettings.model` at request construction; a rejected Qwen Public Web configuration copies `QwenSettings.model` into the same diagnostic field | Non-empty configured model string; must equal the outbound OpenAI-compatible request `model`. CRM Public Web real-provider adjudication additionally requires the OpenAI-compatible client and exact `gpt-5.6-sol` before transport. | Health, Public Web phase metrics, detail APIs, and W7g. It remains visible on failed/fallback calls; it is request intent, never proof of execution. A CRM Public Web configuration mismatch is exposed without fabricating response/effective identity. |
| `response_model` | Same client, copied only from the provider generation response body `model` | Non-empty provider string or absent; no alias/sub-string inference | Audit/diagnostics and exact-match gates. Missing is fail-closed and must not be filled from configuration or `/models`. |
| `effective_model` / `model_identity_provenance` | Same client; `effective_model=response_model` and provenance is exactly `provider_response` | Both fields are emitted together only when `response_model` exists | Health, phase metrics, detail APIs, W7g. Missing or `effective_model != requested_model` opens the existing provider/model circuit and forces deterministic fallback; no alternate source is allowed. |
| `model_usage` | Same client, then revalidated by the CRM Public Web phase-metrics owner | Optional keys only: `input_tokens`, `output_tokens`, `total_tokens`, `cached_input_tokens`, `reasoning_output_tokens`; integer-coercible provider values are clamped to `0..1_000_000_000` | Cost/audit diagnostics only; never readiness or routing input. Unknown keys, non-numeric values, booleans, and raw response payloads are dropped at both boundaries. |
| `model` / `model_version` | Compatibility aliases emitted by the model client/phase owner | Successful proven call: exact `effective_model`; failed/unproven call: requested configured model | Existing readers only. These aliases are not effective-model proof. Delete after all normal readers and exported schemas consume `requested_model` + `effective_model` + provenance directly and the W7g compatibility assertions are retired in a reviewed contract change. |

- Product runtime model calls currently do not use model-native web search or tool calls. Public Web and intent/planning model calls receive evidence from repository-owned provider/search/fetch pipelines such as DataForSEO, Serper, browser search, document fetch, and model-safe evidence selection, then call Qwen `/responses` or OpenAI-compatible `/chat/completions` with strict JSON prompts. `model_native_search` is a reserved provider id and is fail-closed in the normal search-provider chain until `docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md` is implemented. If a future implementation enables model-native search/tools, it must be a new provider contract with explicit owner, typed command/activity evidence, cost budget, source provenance, retry/circuit-breaker policy, and export/audit treatment; it must not silently replace DataForSEO evidence, bypass DataForSEO item-level retry semantics, or make the model an untracked search provider.
- `linkedin.profile_terminal.admit` consumes `profile_cache_hit` / `profile_provider_fetched` deltas, records a local terminal-admission activity, and emits `profile_terminal_recorded` deltas. It does not mutate projection membership; projection admission remains a downstream owner boundary that must consume terminal deltas.
- `linkedin.local_profile_delta.apply` is Activity-spine-backed for the legacy workflow post-profile path. It records `local_profile_delta` EntityDeltas for completed, partial, waiting-prerequisite, retry-wait, failed, or already-completed effects. Board-visible publication must consume the command-owned outcome; metrics must not infer local-apply completion only from legacy item status or worker timestamps.
- `projection.profile_admission.apply` consumes `profile_terminal_recorded` deltas, records a projection-admission activity, writes canonical `serving_projection_members` through `serving_projection_owner`, and emits `projection_member_admitted` deltas. It is the first normal Agent-callable bridge from profile terminal facts into public run-scope projection membership.
- `projection.board_visible_patch.publish` is Activity-spine-backed. It records `board_visible_patch` EntityDeltas for completed, waiting-prerequisite, retry-wait, or failed board publication effects. SLO/metrics reducers must follow this command/activity/delta causality rather than pairing local apply and board-patch timestamps heuristically.
- `projection.run_scope.finalize` is also Activity-spine-backed even when invoked by legacy job result-view publication. The `serving_projection_owner` must record a `workflow_activity_runs` row, a `workflow_activity_attempts` row, and a `run_scope_projection` EntityDelta for published, obsolete, retry-wait, or failed finalization effects. Agent/debug queries must not infer serving-finalized state only from `job_result_view.metadata` or command result JSON.
- After operation-native projection admission publishes the run-scope projection, it may plan existing `projection.person_search_index.build` and `collection.authoritative.merge` commands on the same Operation workflow id. These commands update search/index and collection-authoritative assets in the background; they do not block current run projection visibility and must not create a legacy job shell or normal-path `job_materialization_items` row. Their owners must also write ActivityRun/ActivityAttempt/EntityDelta evidence, so index pages, collection pointer updates, retry waits, and not-applied reasons are queryable through the W11 activity/entity APIs rather than inferred from command result JSON alone.
- `projection.facet_layering.build` is Activity-spine-backed. It may update projection member summaries/metrics and layering artifacts, but the execution truth is the command/activity/attempt/delta chain. The owner must record `facet_layering` EntityDeltas for completed, partial, retry-wait, or failed effects; candidate board/read APIs must not infer facet-layering readiness from artifact paths or job summary text alone.
- CRM Public Web per-run phase commands (`crm.public_web.search.submit`, `crm.public_web.search.poll_fetch`, `crm.public_web.documents.fetch`, `crm.public_web.evidence.adjudicate`, `crm.public_web.model_safe.finalize`, `crm.public_web.signals.materialize`) are bounded activities. Each claimed phase command must write a `workflow_activity_runs` row, a `workflow_activity_attempts` row, and at least one run-level `crm_public_web_run` `workflow_entity_deltas` row. Remote-search waiting is a `retry_wait` attempt plus a `not_applied` delta, not a worker-private wait state. `crm.public_web.evidence.adjudicate` must persist `adjudication_input_payload.json` before applying model output, so audit can separate provider returned links, selected model window, model judgement, and final materialized signal. The phase owner may drain multiple independent `evidence.adjudicate` commands with bounded concurrency (`CRM_PUBLIC_WEB_ADJUDICATION_COMMAND_CONCURRENCY`, default 2), but signal materialization remains command-owned and idempotent. `/api/crm/records/public-web-search/poll` and `/api/crm/records/{crm_record_id}/public-web-search` must expose `crm_public_web_phase_command_status_v1` from `workflow_commands` for each run, including command ids, status, phase order, fixed `phase_count`, `completed_phase_count`, `materialized_command_count`, current command, and generic command-control API paths; frontend and Agent code must not infer per-person Public Web progress from run phase strings alone or query a global command list as a hidden fallback. Product-facing progress denominators must use the fixed `phase_order`; `materialized_command_count` is diagnostic only because commands may be planned lazily. DataForSEO remote-search progress is query-task level, not batch-envelope level: one timeout/provider-pending task is terminal or waiting evidence for that query only. `crm.public_web.documents.fetch` must also record document-level `public_web_document` deltas from `document_fetch_payload.json`, and `crm.public_web.signals.materialize` must record signal-level `public_web_signal` deltas from materialized `person_public_web_signals` plus canonical `person_asset` / `person_evidence` sync deltas under the same activity id, so Agent/debug queries can explain individual Public Web documents, signals, and PersonAsset-layer evidence without mutating document/signal read models outside the owner. They must not create another Public Web workflow state table.
- CRM Public Web manual promotions are durable human decisions, not latest-run transient signals. `public_web_promotions` / linked assertions remain visible and default-exportable across retries, provider timeouts, model fail-closed results, latest runs with zero materialized signals, and latest runs that are still queued/running/retry-wait. Latest-run `person_public_web_signals` gates only new promote/reject actions. Export owners must merge manually promoted assets into export input snapshots even when the current latest run has no matching signal row or is not terminal, and must never export rejected/suppressed promotions. Non-terminal latest-run signals are not exportable transient facts unless they have already been converted into a durable manual promotion.
- CRM Public Web retry is a stable control intent. The public retry API must not create a random force-refresh nonce when the caller omits one; it must derive a deterministic retry idempotency key from source run ids, retry reason, workspace, and requester, then use force-refresh with that explicit nonce. Repeated retry requests for the same terminal source run must join the same child batch/run and queue command. Ordinary manual force-refresh without a caller nonce may still create a distinct run, but retry-owner paths cannot duplicate provider/model work because of HTTP retries, lost responses, or accidental double-clicks. This is a storage-level contract, not only an application-level lookup: normal PG schemas and SQLite-to-PG sync must enforce unique `crm_public_web_batches.idempotency_key` and `crm_public_web_runs.idempotency_key` for non-empty keys.
- CRM writer commands (`crm.record.add_from_projection`, `crm.record.update`, `crm.note.add`, `crm.task.create`) are bounded activities. Operation dispatch may plan these commands only; the `crm_writer` owner must record a `workflow_activity_runs` row, a `workflow_activity_attempts` row, and EntityDeltas for CRM record plus event/note/task effects when it mutates `crm_records`, `crm_engagements`, `crm_tasks`, or `crm_events`. Agent/debug queries must follow command/activity/entity-delta causality instead of inferring mutation provenance only from CRM table timestamps. Running CRM writer command cancel is owner-specific only before mutation attempt creation through `crm_writer.cancel_before_mutation_attempt`; it is blocked after ActivityAttempt, EntityDelta, or downstream command creation. Running resume is owner-specific through `crm_writer.resume_crm_writer_command`, which only records control evidence and requeues the command after expired lease or explicit force. Control APIs must not write CRM tables directly.
- CRM Public Web queue-batch is a root orchestration command. `crm.public_web.queue_batch` may be cancelled while running only before per-run phase commands exist; the owner-specific cancel marks the batch/run read models cancelled and then cancels the command. After phase command planning, cancellation must target the per-run phase command owner or fail closed.
- Company Public Web refresh is split into a root orchestration command and bounded phase commands. D1m's Operation
  request is canonical deterministic `seed_url_only`, with required company/source families/seed URLs and owner-minted
  exact `workspace_id + company_key`; provider-search or collector-bundle fallbacks are not part of this action.
  Operation dispatch may plan `company.public_web.refresh` only; the root owner does not call the refresh service
  directly and instead plans `company.public_web.source.collect`. Persisted action/run, dispatch, root, source
  collection, and materialization each revalidate the same canonical request/target plus exact OperationRun/AgentAction
  and command causality before their new effect. The root command may be cancelled while running only before downstream
  source/materialize commands exist and only after lease expiry or explicit force. The source collection phase writes
  source-specific company Public Web rows and artifacts with canonical asset sync deferred. It then derives a pure
  materialize plan and commits the `CommandPlanRequested` event, deterministic
  `company.public_web.assets.materialize` child, one source-run EntityDelta, physical downstream edge, and exact parent
  success CAS in one PostgreSQL transaction. Its normalized effective idempotency key is enforced by a partial unique
  index; sorted effective-key/run-id advisory locks reject split identity, and physical command id/current attempt/lease
  ownership fences reclaim and terminal writes. A newer current retry attempt may reclaim `running` or `failed` work;
  an older attempt receives `owner_lost` and cannot overwrite source-run terminal state. The retry ActivityRun remains
  `retry_wait` between attempts and converges to `succeeded` only with the successful attempt. A plan-spec failure
  writes no child or EntityDelta; a stale claim or final-CAS failure rolls the entire completion bundle back. Exact
  bundle validation repairs a post-commit acknowledgement loss before Activity/Operation synchronization. The defer rule also
  applies when source collection joins an existing source run; joined source rows must not opportunistically sync
  canonical company facts. The completed source owner freezes `company_public_web_run_snapshot_v3`; its digest binds
  discovered assets, summary, artifact paths, artifact-publication digest, source revision/completion time, started_at,
  and completed_at, and materialization reconstructs that exact immutable snapshot instead
  of rereading mutable source rows. The materialization phase validates source-run identity, syncs model-safe rows into
  PG-only `CompanyAsset` / `CompanyEvidence`, and records ActivityRun/Attempt plus `company_public_web_run`,
  `company_asset`, and `company_evidence` EntityDeltas. Asset upserts return the just-written current-run payload so a
  concurrent writer cannot contaminate the frozen snapshot through a post-upsert reread. Stale running
  source-collection commands may be resumed only through the source owner; stale running asset-materialization commands
  may be cancelled only before sync attempt or resumed through owner-specific delegates. Both delegates record control
  evidence and must not perform request-path canonical sync. Agent/debug queries must follow
  command/activity/entity-delta causality instead of inferring refresh provenance from source-specific
  `company_public_web_assets` timestamps. The synchronous refresh service remains a manual API/CLI service entry and is
  not the Agent normal path. The source row/artifact effect remains before the completion bundle, while Activity and
  linked Operation/action synchronization remain after it. These bounded owner/effect transactions therefore do not make
  submit/command/effect/terminal/linked-Operation state one global exactly-once transaction.
- Profile-experience company logo discovery (`company.logo.profile_experience.discover`) is a non-blocking company asset command owned by `company_asset_owner`. Local profile apply may only plan this command after a fresh Harvest/profile delta; it must not scan profile JSON, fetch media, or write `CompanyEvidence` in the local-apply/materialization path. The owner reads at most one profile payload, extracts unexpired target-company `companyLogo` URL evidence, writes `CompanyEvidence(evidence_type='logo_url')`, records ActivityRun/Attempt plus `company_evidence` or no-op discovery EntityDelta, and then plans `media.asset.cache`. If a stable `CompanyAsset.logo_media` already exists, the planner no-ops. If the selected profile has no eligible or unexpired logo URL, the owner completes with `source_discovery_required`; explicit logo source discovery remains a separate path. This stage must not block profile fetch, board-visible publication, projection finalization, or collection-authoritative merge.
- Media cache (`media.asset.cache`) is a bounded activity owned by `media_asset_owner`. It imports or fetches bounded media inputs, uploads normalized bytes to object storage, writes stable `PersonAsset(asset_type='avatar_media')` or `CompanyAsset(asset_type='logo_media')`, and records ActivityRun/Attempt plus `person_asset` or `company_asset` EntityDeltas. `/api/media/assets/{asset_id}` is the fail-closed read API for object-backed cached media and must not fetch provider URLs or repair missing assets in the request path. Provider avatar/logo URLs are metadata until this command owner writes a stable asset; readers must not hotlink provider media as a normal fallback. Running cancel is owner-specific only before fetch/upload attempt creation through `media_asset_owner.cancel_before_fetch_upload_attempt`; it is blocked after ActivityAttempt, EntityDelta, or downstream command creation. Stale running resume is owner-specific through `media_asset_owner.resume_media_asset_cache`; it only records ActivityRun/Attempt/EntityDelta control evidence and requeues the command, and must not fetch, upload, or write PersonAsset/CompanyAsset rows in the resume API. Agent/UI callers must not expose broader media cancel semantics than the control policy reports.
- Export commands are bounded activities. `export.projection.generate` and `export.crm_public_web.generate` must record ActivityRun/ActivityAttempt evidence and an export EntityDelta with artifact refs when an archive is generated or a failed/not-ok reason when generation fails. The ZIP artifact is an output ref, not the source of execution truth.
- `excel.intake.run` is a bounded activity even though its existing owner starts a local async thread. The command owner must create ActivityRun/ActivityAttempt evidence before starting the thread, and the thread must update the same activity plus an `excel_intake_job` EntityDelta when the job reaches terminal success/failure/cancelled. This keeps Excel retry/debug surfaces on the same Activity API without making Excel job rows the execution owner.
- `linkedin.profile_refill.submit_batch` remains a workflow-internal/provider owner while W11f implements direct profile-fetch activities and terminal registry/projection admission. It is not a normal Operation action command type.

Activity query surface:

- `GET /api/workflow/activities` lists bounded activity current-state by workflow, operation, acquisition run, command, activity type, or status.
- `GET /api/workflow/activities/{activity_run_id}` returns one activity plus its attempts.
- `GET /api/workflow/activity-attempts` lists attempt envelopes by activity, workflow, command, or status.
- `GET /api/workflow/activity-attempts/{attempt_id}` returns one attempt envelope.
- `GET /api/workflow/entity-deltas` lists entity effects by workflow, operation, command, activity, attempt, acquisition run, entity type/key, or status.
- `GET /api/workflow/entity-deltas/{delta_id}` returns one entity effect.
- `GET /api/workflow/discovery-lanes` lists acquisition discovery lane read-model rows by workflow, operation, acquisition run, command, activity, provider, company, query, or status.
- `GET /api/workflow/discovery-lanes/{lane_id}` returns one acquisition discovery lane read-model row.
- Activity, attempt, and entity-delta read rows expose a read-only `control_target` pointing back to the owning `workflow_command` and its `control_policy`. Agent/debug tooling must use that command control surface for retry/cancel/resume; it must not mutate ActivityRun, Attempt, EntityDelta, lane, registry, projection, or module tables directly.
- Discovery lane rows also expose a read-only `control_target`; lane rows remain acquisition read models and must never become retry/cancel/resume owners.
- These APIs are read-only. They never execute providers, repair domain state, or mutate lane/read-model rows.

### Read Model Layer

Public views such as job status, timeline, progress, result page, projection summary, CRM overlay, and collection overview are read models. They may lag execution, but they must not drive execution or repair missing runtime state.

## Identifiers

The runtime uses separate identifiers for separate responsibilities:

- `operation_id`: user or Agent intent.
- `workflow_run_id`: one durable execution run.
- `command_id`: one claimable atomic command.
- `activity_attempt_id`: one real execution attempt for a command.
- `projection_id`: public result/read model resource.
- `person_identity_key`: global person identity key.
- `collection_id`: local asset namespace.

Legacy `job_id` remains a compatibility/read-model field during migration. It is not the long-term source of execution truth.

Command attribution rule: a `workflow_run_id` may contain commands planned by multiple `OperationRun`s, especially after Agent/user continuation. `workflow_commands.operation_id` must identify the Operation that planned the command, not merely the first operation recorded in `workflow_current_state`. Reducers must therefore resolve command attribution from the command payload/causality envelope (`operation_id` or `operation_run_id`) before falling back to workflow current-state. Operation sync must use the same rule so completion, retry, and failure events update the correct Agent action/run.

## Event Contract

Runtime events are append-only. Updates, deletes, corrections, cancellations, manual overrides, and supersession must be represented as new events.

Recommended physical storage is one append-only `workflow_events` table with typed fields:

- `event_id`
- `workflow_run_id`
- `operation_id`
- `command_id`
- `activity_attempt_id`
- `event_family`
- `event_type`
- `sequence_number`
- `idempotency_key`
- `occurred_at`
- `recorded_at`
- `actor`
- `source`
- `payload`
- `artifact_refs`
- `schema_version`

Storage authority:

- Durable/current-state tables are PG-only for normal execution: `workflow_events`, `workflow_current_state`, `workflow_commands`, `runtime_outbox`, `agent_actions`, `operation_runs`, `acquisition_runs`, `workflow_activity_runs`, `workflow_activity_attempts`, `workflow_entity_deltas`, `acquisition_discovery_lanes`, `operation_events`, `agent_tool_result_slots`, `agent_tool_result_attempts`, `agent_tool_result_journal`, `crm_tasks`, `company_assets`, `company_evidence`, and `company_assertions`.
- SQLite is not an accepted durable/current-state backend for these tables, including unit tests. Typed durable-runtime and CRM task tests must use a PG-backed fixture with `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`; missing PG is a test-environment failure or skip in optional local mode, not permission to exercise SQLite. New durable runtime, Operation, W11, or CRM task current-state storage must not add SQLite DDL/fallback as a normal path.
- Hosted/local workflow confidence must use `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only` with a resolved Postgres DSN.

Event families:

- `operation_event`: user/Agent intent, approval, cancellation, budget update.
- `workflow_event`: reducer state transition, completion-policy proof, stage readiness.
- `command_event`: command created, claimed, heartbeat, succeeded, retry-wait, terminal failure, cancelled.
- `domain_event`: durable business fact such as `ProfileUrlsTerminalRecorded`, `BoardPatchPublished`, `ProjectionIndexed`, `CRMRecordAdded`.

Hard rules:

- Events are append-only.
- Event payloads must be compact.
- Large raw profile data, raw HTML, provider datasets, candidate arrays, and full export files belong in domain asset stores or object storage and are referenced by artifact refs.
- Every event that changes state must have an idempotency key.
- Events must be ordered per workflow by `sequence_number`.
- An operation control transition and its corresponding `operation_event` must share one Postgres connection,
  cursor, transaction, and commit. The lock order is event stream advisory lock, operation row when present, then
  linked action row. Existing idempotent events must match workspace, stream, operation, action, family, type, and
  idempotency identity; a collision is a rollback condition, not proof that the transition was audited.

## Current State Contract

`workflow_current_state` is a materialized projection of the append-only event log.

It may be updated for fast reads and reducer checkpoints, but it must be reproducible or at least auditable from event history plus domain owner outputs.

Current state should include:

- workflow status
- current completion-policy proofs
- active command counts by owner/type
- terminal command counts by owner/type
- last processed event sequence
- last reducer version
- read-model readiness pointers
- migration/legacy bridge status

Current state is not a substitute for event history.

## Reducer Contract

Reducers decide state transitions.

Input:

- current workflow state
- new durable events
- existing pending commands for idempotency checks

Output, written in one transaction:

- new `workflow_events`
- updated `workflow_current_state`
- new or updated `workflow_commands`
- new `runtime_outbox` events when an external dispatcher must be notified

Reducers must not:

- call external providers
- read raw provider files as decision inputs
- scan large candidate artifacts
- generate non-persisted random IDs
- depend on wall-clock time except through persisted timer events or `not_before_at`
- write domain stores directly
- publish public read models directly unless the write is represented as a command/domain owner output

Workers and callbacks write events/results. They do not decide arbitrary next commands.

## Command Contract

`workflow_commands` is the normal durable work queue.

Required fields:

- `command_id`
- `workflow_run_id`
- `operation_id`
- `command_type`
- `owner`
- `status`
- `idempotency_key`
- `payload`
- `artifact_refs`
- `not_before_at`
- `attempt`
- `max_attempts`
- `retry_policy`
- `lease_owner`
- `lease_expires_at`
- `heartbeat_at`
- `created_at`
- `updated_at`
- `schema_version`

Every normal-path command must carry typed causality as physical `workflow_commands` columns. This is not optional logging and it is not a long-term `payload_json` convention; it is the contract that lets metrics, recovery, debugging, and future Agent operations query why a command exists and which downstream readiness it owns.

Required physical causality fields:

- `workflow_run_id`
- `operation_id`
- `stage_id`
- `command_type`
- `owner`
- `causal_group_id`
- `parent_command_id`
- `source_event_id`
- `source_event_type`
- `idempotency_key`
- `input_artifact_refs`
- `output_artifact_refs`
- `produced_entity_counts`
- `no_op_reason`
- `readiness_effect`
- `downstream_command_ids`
- `schema_version=command_causality_v1`

Rules:

- Reducers attach causality when handling `CommandPlanRequested`; `workflow_commands` persists that causality in physical columns. A `payload.causality` debug mirror may exist during migration, but metrics/preflight/readiness must treat the physical columns as source of truth.
- `source_event_id` points to the durable event that caused the command. If an owner emits a downstream command, the upstream command id is recorded as `parent_command_id` and shared work is grouped by `causal_group_id`.
- `produced_entity_counts` records the bounded count claims used by SLOs and readiness checks. If all declared counts are zero, `no_op_reason` is required so downstream SLOs do not pair unrelated events.
- `readiness_effect` states what public readiness this command can change, for example `profile_registry_terminal`, `local_profile_delta_applied`, `board_visible_patch_published`, `projection_index_ready`, or `collection_authoritative_pointer_switched`.
- Migration-adapter commands converted from `job_materialization_items` may carry legacy references, but adapter use must remain report-visible and must not be counted as the normal-path causality standard.
- High-cardinality raw entities remain in artifacts/domain stores. The envelope carries refs and counts, not full candidate/profile arrays.

Command statuses:

- `queued`
- `claimed`
- `running`
- `retry_wait`
- `succeeded`
- `failed_terminal`
- `cancelled`
- `superseded`

Allowed transitions:

- `queued -> claimed -> running -> succeeded`
- `queued -> claimed -> running -> retry_wait -> queued`
- `queued -> claimed -> running -> failed_terminal`
- `queued -> claimed -> running -> cancelled`
- any non-terminal command -> `superseded` only through a reducer-owned supersession event

Database constraints must enforce:

- unique `command_id`
- unique active command by `idempotency_key`
- no duplicate terminal command result for the same command
- command lease claim atomicity

Write-path and restore boundaries:

- `workflow_commands` writes must enter through dedicated command-writer methods. The public raw SQL helpers on the live PG adapter are a strict single-statement, read-only allowlist backed by recursive PostgreSQL statement decomposition over tokens: only plainly read-only statements pass (`SELECT`/`VALUES`/`TABLE`/`WITH`-of-read-only-queries, `EXPLAIN` of a provably read-only statement including `ANALYZE`, and `SHOW`), `SELECT ... INTO` and locking reads (`FOR UPDATE`/`FOR SHARE` forms) fail, and function calls pass only from an explicit read-only builtin allowlist so a side-effecting `SELECT existing_mutator()` can never execute. DML, DDL, utility execution, opaque bodies (`DO`, `CALL`, `EXECUTE`, `PREPARE`), multi-statement input, and target-smuggling forms (`MERGE INTO ONLY`, cross-table `CREATE RULE`, `DROP SCHEMA ... CASCADE`, `DROP OWNED`) all fail by default rather than by enumerating dangerous shapes; DDL and utility execution belong to the separate private migration/test interface (`_execute_returning_one`/`_execute_non_query`). The lexical allowlist is defense in depth, not the sole mutation boundary: it cannot inspect database-resolved objects (view definitions, RLS policies, user operators/casts, allowlist-shadowing function overloads), so every admitted public probe executes inside a database-enforced read-only transaction (`SET TRANSACTION READ ONLY` as the probe transaction's first statement) on a fresh disposable connection, where PostgreSQL itself rejects any catalog-hidden write with sqlstate `25006`. The probe is then rolled back — never committed, so notifications are never delivered and no accidental commit is possible — and the connection is closed, never returned to the pool, so session-level side effects the read-only transaction cannot see (`set_config` GUC poisoning, session advisory locks, temporary state) die with it. Real-PG regressions prove the bypass families — a mutating view, an `EXPLAIN ANALYZE` of it, an allowlisted-shadowing `lower(integer)` overload, a user operator hiding a proven mutator, a hidden `set_config`, a hidden advisory lock, a hidden notification, and hidden temporary-state DDL — cannot mutate the held command table or poison later sessions.
- The complete PG-only durable runtime causal aggregate (`workflow_commands`, `workflow_events`, `workflow_current_state`, `runtime_outbox`, `agent_actions`, `operation_runs`, `agent_tool_result_slots`/`attempts`/`journal`, `workflow_activity_runs`, `workflow_activity_attempts`, `workflow_entity_deltas`, `operation_events`, the acquisition runtime rows `acquisition_runs` and `acquisition_discovery_lanes` whose Action/Operation/command/event owners are in the same aggregate, and the model invocation envelopes `model_invocation_envelopes` with immutable causal/cost references and non-revivable purge tombstones) and the nonportable live execution/recovery/lease/cost-control coordination tables (`workflow_job_leases`, `workflow_recovery_intents`, `runtime_provider_limiter_leases`, the active worker rows `agent_worker_runs`, the profile-URL scheduler leases `linkedin_profile_registry_leases`, and the complete profile-scheduler owner aggregate `linkedin_profile_registry`/`linkedin_profile_registry_aliases`/`linkedin_profile_registry_events`/`linkedin_profile_registry_backfill_runs` carrying scheduler retry waits, coalescing timers, terminal state, and dispatch identity) are excluded from generic control-plane snapshot export/import through the canonical per-table portability registry (`CONTROL_PLANE_TABLE_PORTABILITY_REGISTRY`; `DEFAULT_CONTROL_PLANE_TABLES`, `PG_ONLY_DURABLE_RUNTIME_CAUSAL_AGGREGATE_TABLES`, `NONPORTABLE_RUNTIME_COORDINATION_TABLES`, and `GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES` are all derived from it, so no second hand-maintained set can drift). Generic snapshots are projection/domain-only and record the exclusion as a typed `excluded_pg_only_durable_runtime_tables` gap; every generic restore boundary (snapshot→PG sync, runtime→PG mirror, snapshot→SQLite restore) requires that exact schema-versioned exclusion declaration before import, rejects missing or mismatched declarations, rejects excluded tables, and copies the verified gap into every restore/sync/cloud-import summary, so no partial aggregate or live coordination slice can pass as complete. Durable-runtime backup/restore must use a quiesced PG logical backup outside the generic snapshot path.

## Owner Registry

Every command type has one owner.

Example owner mapping:

| Command Type | Owner |
| --- | --- |
| `discovery.search_seed.dispatch` | `discovery_provider_queue` |
| `profile.refill.submit_batch` | `profile_refill` |
| `profile.registry.record_terminal_urls` | `profile_registry` |
| `local_apply.apply_profile_delta` | `local_apply` |
| `board_visible.publish_delta_patch` | `board_visible` |
| `projection.index.build` | `projection_index` |
| `collection.merge.authoritative` | `collection_writer` |
| `snapshot.compact.full` | `snapshot_compaction` |
| `crm.record.add_from_projection` | `crm_writer` |
| `crm.record.update` | `crm_writer` |
| `crm.note.add` | `crm_writer` |
| `crm.task.create` | `crm_writer` |
| `company.public_web.refresh` | `company_public_web_owner` |
| `company.public_web.source.collect` | `company_public_web_owner` |
| `company.public_web.assets.materialize` | `company_public_web_owner` |
| `company.logo.profile_experience.discover` | `company_asset_owner` |
| `media.asset.cache` | `media_asset_owner` |
| `crm.public_web.queue_batch` | `crm_public_web_owner` |
| `excel.intake.run` | `excel_intake_owner` |
| `export.projection.generate` | `projection_exporter` |
| `export.crm_public_web.generate` | `crm_public_web_exporter` |

Owner rules:

- A worker may claim only commands for its owner.
- A command owner may write only its owned domain outputs.
- Public readers may not claim commands.
- Provider callbacks may record events and wake the runtime, but may not claim downstream commands inline.
- Agent operations may request commands only by writing operation events; reducer policy decides actual commands.
- Export commands that publish reusable artifacts must include an input watermark in the command idempotency scope, not only a contract version. `export.crm_public_web.generate` watermarks the exact model-safe export input snapshot, including CRM record export fields, `crm_public_web_runs` latest-run detail, CRM Public Web phase-command summary, `person_public_web_asset` summary, export-mode-filtered `person_public_web_signals` / evidence links, `crm_public_web_promotions`, and relevant `PersonAssertion` rows. The `crm_public_web_runs` latest-run detail is record-owned: it must match the requested `crm_record_id` and `workspace_id`; `person_public_web_asset.latest_run_id` is provenance only and must not be used as a fallback latest-run source for CRM detail/export. Signal reads used by CRM detail/export must include both `run_id` and `record_id`. Promotion uses the same owner guard: the signal run must match the current CRM record and workspace, and the current record/workspace latest run must be nonempty and equal to `signal.run_id` before writing promotion, assertion, CRM event, or exportable state. A succeeded artifact may be replayed only when the stored `export_input_watermark_hash` equals the current owner-computed watermark. The owner must also recheck the same watermark after claiming a queued command and before publishing any ZIP/artifact side effect; stale queued commands fail closed and do not publish. Before opening a ZIP writer, the owner must materialize an ordered per-record export-input snapshot list from the same watermark computation. Missing snapshots are a contract failure (`crm_public_web_export_input_snapshot_missing`); archive generation must not fallback to fresh detail/asset/promotion/assertion reads after watermark validation. Public HTTP export endpoints must propagate those owner failures as JSON errors, never as empty successful binary downloads.

## Timer And Retry Contract

Timer and retry are runtime concepts, not module-local inventions.

Every retryable command must use:

- `not_before_at`
- `attempt`
- `max_attempts`
- `retry_policy`
- retry reason
- terminal failure reason when exhausted

Retry policy must be explicit:

- fixed delay
- exponential backoff
- provider rate-limit backoff
- no retry
- manual retry required

Timer wakeups should create or requeue commands through reducer-owned events. They must not be hidden inside owner-specific loops.

## Completion Policy Contract

Workflow completion is not a single `job.status=completed` flag.

The LinkedIn acquisition/profile pipeline must expose separate completion policies:

- `stage1_candidate_set_terminal`: discovery lanes produced a coherent candidate-set projection or terminal empty/degraded proof.
- `stage1_preview_allowed`: candidate rows are stable enough for preview/result shell publication.
- `profile_fetch_terminal`: required profile URL items reached terminal fetched/unrecoverable states.
- `local_apply_terminal`: provider outputs have been applied or explicitly skipped with proof.
- `board_visible_terminal`: board-visible patches/projection membership reflect the expected visible candidate/card set.
- `serving_finalized`: canonical projection/lifecycle can serve public result pages with stable counters.
- `post_result_layering_ready`: heavier facets/layering are built, unavailable, or explicitly deferred.
- `collection_merge_terminal`: background authoritative collection merge succeeded or failed independently of run result serving.

Public result pages may become usable before all background policies are terminal, but the API must expose readiness precisely. It must not imply that post-result layering or collection merge is complete merely because the result board is usable.

## LinkedIn Pipeline First Migration Scope

The first runtime migration covers:

- discovery lane candidate-set events
- Stage 1 progress coherence
- profile URL registry scheduling and refill
- provider envelope submit and terminal observation
- URL-level terminal state recording
- local profile delta apply
- board-visible patch publication
- run-scope projection finalization
- projection search-index build command enqueue
- collection-authoritative merge command enqueue

Out of first scope:

- Excel intake rewrite
- CRM state rewrite
- Public Web rewrite
- export rewrite
- full Agent graph implementation

Those modules should migrate later by reusing the same runtime/event/command/owner primitives.

## Agent And Operation Boundary

Future Agentic graph or LangGraph-style execution is an operation-layer caller, not a domain state owner.

Allowed Agent actions:

- write `OperationRequested`
- write `UserApprovalGranted`
- write `UserApprovalDenied`
- request plan/update/cancel events
- explain state from read models and runtime events

Forbidden Agent actions:

- directly mutate projection membership
- directly update CRM records without CRM writer command
- directly edit profile registry rows
- directly dispatch providers
- directly mark workflow completion
- directly repair public readers

Approval gates are represented as operation events. Reducers convert approved operations into workflow commands according to policy.

## Public Reader Boundary

Public readers consume read models only:

- serving projections
- lifecycle/current-state projections
- board runtime state
- CRM overlay summaries
- asset/index summaries

Public readers must not:

- claim commands
- mutate runtime events
- synthesize missing commands
- repair lifecycle rows
- rebuild projection indexes
- scan raw provider artifacts to compute normal response fields
- fall back to legacy job artifacts without a report-visible migration bridge

Missing canonical read models fail closed with a typed readiness/migration-required response.

## Legacy Migration Contract

`job_materialization_items` is a legacy durable work queue. It currently records materialization/recovery items such as local apply, board-visible apply, projection index build, collection merge, and snapshot compaction. It does not cover the full durable runtime control plane and must not be expanded into the long-term command system.

Long-term target:

- New normal execution writes `workflow_events` and `workflow_commands`.
- `job_materialization_items` becomes migration input only.
- A migration adapter may convert existing item rows into canonical events/commands.
- Adapter usage must be report-visible in service metrics and signoff.
- Contract gates must fail if new normal-path code writes new legacy item kinds after cutover.
- Old recovery branches are deleted after migration evidence and data-asset cleanup gates pass.

Legacy cutover phases:

1. Add runtime tables and reducer skeleton.
2. Add command owner registry and typed command leases.
3. Route new LinkedIn profile pipeline commands through `workflow_commands`.
4. Add adapter from selected existing `job_materialization_items` to canonical commands for in-flight/historical rows.
5. Add metrics: legacy item read/write counts, adapter conversion counts, unconverted item counts, normal-path legacy write count.
6. Block normal signoff on normal-path legacy writes.
7. Backfill or retire historical rows.
8. Delete old recovery branches and legacy write helpers.

### W2b Profile Refill Submit Migration Contract

During W2b, LinkedIn profile-refill provider submit chunks must be represented by `workflow_commands`. The registry scheduler may discover ready profile URLs and plan commands, but provider submit execution belongs to the typed command owner.

Required behavior:

- Each provider-submit chunk writes a reducer-owned `CommandPlanRequested` event for `linkedin.profile_refill.submit_batch`.
- The command idempotency key includes `job_id`, `snapshot_dir`, normalized URL set, and submit scope. Submit scope must distinguish normal wave from retry wave so a prior normal-wave terminal command cannot suppress a later legitimate retry.
- The scheduler must not execute provider submit inline on the normal recovery path. It plans commands with `execute_profile_refill_submit_commands=false` and reports `planned_command_count`, `planned_worker_count`, and `planned_url_count`.
- The `linkedin_profile_refill_command_owner` recovery/service phase claims ready `linkedin.profile_refill.submit_batch` commands, marks them `running`, executes the provider-envelope primitive, and marks `succeeded` / `retry_wait` / `failed_terminal`.
- The owner must write `workflow_activity_runs`, `workflow_activity_attempts`, and per-profile-url `workflow_entity_deltas(entity_type='profile_refill_submit')` for provider submit queued/completed/deferred/failed outcomes. Provider worker rows and command result JSON are not sufficient as the long-term Agent/debug evidence surface.
- If a matching command is already `claimed`, `running`, `retry_wait`, `succeeded`, `failed_terminal`, `cancelled`, or `superseded`, the scheduler must not submit the provider again for that chunk. It returns queued/owned evidence with `runtime_command_contention=true`.
- Command lifecycle evidence must be returned in profile-prefetch summaries as `workflow_commands` plus status counts. Recovery/service summaries must also expose the owner phase as `profile_refill_command_owner` with `command_count`, `executed_command_count`, `dispatched_url_count`, `queued_worker_count`, and `deferred_url_count`.
- If one recovery tick already planned profile-refill commands, later refill scans in that same tick must yield with `profile_refill_command_handoff_to_owner` instead of re-scanning the same registry rows. The owner phase is the only same-tick provider submit path.

W2b is still not the full durable execution model: the owner phase runs inside the existing recovery/service tick. W2c must apply the same event -> command -> owner split to URL terminal-state recording, local profile delta apply, board-visible patch publication, run-scope projection finalization, projection index build, and collection merge.

### W2c.1 Profile URL Terminal-State Recording Contract

Profile URL terminal-state recording is the first post-profile W2c command owner.

Required behavior:

- Profile workers may persist provider payload files, but they must not own durable URL terminal state directly.
- After bounded terminal payload persistence, the worker writes a reducer-owned `CommandPlanRequested` event for `linkedin.profile_url_terminal.record`.
- The command payload contains `job_id`, `snapshot_dir`, `snapshot_id`, `terminal_scope`, and terminal `entries`. Each entry carries the normalized profile URL, terminal status, raw path or error, retryability, source jobs/shards, provider run/dataset ids, and snapshot dir.
- The command idempotency key includes job, snapshot, terminal scope, and normalized terminal entries. Replay of the same worker/chunk must not duplicate registry writes or advance progress twice.
- The `linkedin_profile_url_terminal_record_command_owner` recovery/service phase claims ready `linkedin.profile_url_terminal.record` commands and is the owner that mutates `linkedin_profile_registry`.
- The command owner must also write a `workflow_activity_runs` row, a `workflow_activity_attempts` row, and per-profile-url `workflow_entity_deltas(entity_type='profile_url_terminal_record')`. `linkedin_profile_registry` is the profile-terminal read model; command result JSON alone is not accepted as Agent/debug evidence.
- If terminal-record command planning fails on a normal `ControlPlaneStore` path, the profile worker must remain `running` with report-visible failure/pending evidence. It must not mark the chunk processed and then let local apply or board-visible publication run from an unproven URL terminal state.
- If a terminal-record command is already `claimed` or `running`, the worker reports `profile_url_terminal_record_command_pending` and leaves its `terminal_persist_progress` recoverable instead of treating the URL chunk as processed.
- If a terminal-record command is already `succeeded`, replay is idempotent and may treat the entries as recorded.
- Recovery/service summaries must expose `profile_url_terminal_record_command_owner` with `command_count`, `executed_command_count`, `recorded_count`, `fetched_count`, and `failed_count`.
- Public readers, local apply, board-visible publication, projection builders, and completed-workflow reconcile must not repair missing URL terminal state. They can report not-ready, but the command owner must converge the registry.

### W2c.2 Local Profile Delta Apply Command Owner Contract

Local profile delta apply is the owner boundary that turns provider-completed workers into local candidate/profile/materialization state. It must not be repaired by public readers or completed-workflow scans.

Required behavior:

- Worker completion callbacks and historical backfill must plan a reducer-owned `CommandPlanRequested` event for `linkedin.local_profile_delta.apply`.
- The normal-path typed command payload is self-contained in `workflow_commands`: `job_id`, `snapshot_id`, `item_id`, `worker_kind`, `source_worker_ids`, bounded candidate/profile count hints, request payload, and materialization metadata.
- `legacy_materialization_item` is allowed only on commands produced by the report-visible migration adapter for historical `job_materialization_items` rows. Normal-path planners must not include it.
- The command idempotency key includes job, snapshot, item id, worker kind, source worker ids, and apply scope. Replaying the same worker completion must not create a second apply command or process the item twice.
- The `profile_local_apply_command_owner` recovery/service phase claims ready `linkedin.local_profile_delta.apply` commands, marks them `running`, synthesizes the former item envelope from command-owned payload, executes the existing bounded local apply primitive, and then marks the command `succeeded`, `retry_wait`, or `failed_terminal`. It may claim/read `local_apply_closure` only when the command carries an explicit migration-adapter legacy reference.
- The owner must also write ActivityRun/Attempt/EntityDelta evidence under `activity_type=linkedin.local_profile_delta.apply` and `entity_type=local_profile_delta`. `local_apply_closure` item rows remain migration evidence only and are not sufficient execution truth.
- `waiting_prerequisite` remains a non-failure state. Missing `candidate_documents` or company identity evidence must not consume retry budget; the migration payload row keeps its zero-cost waiting-prerequisite behavior and the command is released to `retry_wait` with bounded `not_before_at`. Event-time prerequisite reawaken must clear both the migration payload row and the typed command.
- Partial profile URL chunks must release the command back to queued without burning retry attempts, preserving the worker/item progress marker.
- Recovery/service summaries must expose local apply command-owner evidence through `local_apply_backlog`: `command_count`, `executed_command_count`, `claimed_count`, `completed_count`, `partial_count`, `waiting_prerequisite_count`, `failed_count`, `skipped_count`, `runtime_namespace_skipped_count`, `candidate_count`, `legacy_bridge_used=false`, and `migration_phase`.
- The old item queue must not execute, even with explicit migration/emergency allow flags. The compatibility entrypoint may convert supported `job_materialization_items` rows into `workflow_commands`, then the typed owner executes those commands. Disabling the typed command owner returns `legacy_job_materialization_recovery_bridge_disabled`.
- Same-scope coalesced batching, per-job single-flight, candidate/profile budgets, and runtime-namespace checks belong to the typed command owner, not the old item queue.

W6 decision A is active for W2c.2: new local-apply normal-path payloads live in `workflow_commands`; `local_apply_closure` rows are migration input only.

### W2c.3 Board-Visible Patch Publication Command Owner Contract

Board-visible patch publication is the owner boundary that turns locally applied profile/candidate deltas into projection-visible candidate board state. It must not be repaired by public readers or worker completion callbacks.

Required behavior:

- Local apply must plan a reducer-owned `CommandPlanRequested` event for `projection.board_visible_patch.publish`.
- The normal-path typed command payload is self-contained in `workflow_commands`: `job_id`, `snapshot_id`, `item_id`, candidate ids, request payload, source worker ids, delta control-plane sync evidence, and materialization metadata.
- `legacy_materialization_item` is allowed only on commands produced by the report-visible migration adapter for historical `job_materialization_items` rows. Normal-path planners must not include it.
- The command idempotency key includes job, snapshot, item id, candidate ids, and publish scope. Replaying the same local apply result must not create a second board-visible publication command or publish the same patch twice.
- Worker completion callbacks must not publish board-visible overlays. Harvest profile completion callbacks enqueue local apply only; the `profile_local_apply_command_owner` may plan board-visible publish commands after local apply succeeds.
- The `board_visible_projection_owner` recovery/service phase claims ready `projection.board_visible_patch.publish` commands, marks them `running`, synthesizes the former item envelope from command-owned payload, executes the bounded patch-publication primitive, and then marks the command `succeeded`, `retry_wait`, or `failed_terminal`. It may claim/read `board_visible_delta_apply` only when the command carries an explicit migration-adapter legacy reference.
- Transient overlay/projection writer failures are retryable command-owner failures. The legacy item may move to `failed_retryable` as a migration payload state, but retry timing and terminal evidence belong to `workflow_commands`.
- `waiting_prerequisite` remains a non-failure state. Missing candidate documents or delta control-plane evidence must release the command to bounded `retry_wait` without consuming retry budget semantics from prerequisite waits. Event-time prerequisite reawaken must clear both the migration payload row and the typed command.
- Recovery/service summaries must expose board-visible command-owner evidence through `board_visible_apply`: `command_count`, `executed_command_count`, `claimed_count`, `completed_count`, `waiting_prerequisite_count`, `failed_count`, `skipped_count`, `runtime_namespace_skipped_count`, `candidate_count`, `legacy_bridge_used=false`, and `migration_phase=W2c_board_visible_patch_publish`.
- The old item queue must not execute, even with explicit migration/emergency allow flags. The compatibility entrypoint may convert supported `job_materialization_items` rows into `workflow_commands`, then the typed owner executes those commands. Disabling the typed command owner returns `legacy_job_materialization_recovery_bridge_disabled`.
- Same-scope coalesced grouping, candidate-budget splitting, and runtime-namespace checks belong to the typed command owner, not the old item queue.

W6 decision A is active for W2c.3: new board-visible normal-path payloads live in `workflow_commands`; `board_visible_delta_apply` rows are migration input only.

### W2c.4a Projection Person Search Index Build Command Owner Contract

Projection person search index build is the owner boundary that turns persisted `serving_projection_members` plus person asset indexes into public projection search/filter records and exact public facet counts. It must not be repaired by public readers, result-view readers, or frontend request paths.

Required behavior:

- Projection publication, layer assignment publication, and person assertion promotion must plan a reducer-owned `CommandPlanRequested` event for `projection.person_search_index.build`.
- The normal-path typed command payload is self-contained in `workflow_commands`: `job_id`, `projection_id`, `item_id`, projection type, collection id, source run id, member count, count scope, page size, input version, and materialization metadata.
- `legacy_materialization_item` is allowed only on commands produced by the report-visible migration adapter for historical `job_materialization_items` rows. Normal-path planners must not include it.
- The command idempotency key includes projection id, item id, semantic projection-index input version, and build scope.
  The semantic input version includes the storage-owned `projection_person_search_index_input_revision`, not
  `serving_projections.updated_at`. An identical publication replay preserves that revision and therefore reuses the
  durable item/command identity; a semantic member change, including a same-count replacement, advances the revision
  and produces new durable build identity. Builder-owned raw/evidence index watermarks are output freshness evidence,
  not command input identity; finalization must not make its own command obsolete or create a new command on replay.
- The `projection_index_owner` recovery/service phase claims ready `projection.person_search_index.build` commands, marks them `running`, synthesizes the former item envelope from command-owned payload, executes one bounded index page, and then marks the command `succeeded`, re-queues it for partial progress, or records retry/terminal failure. It may claim/read `projection_person_search_index_build` only when the command carries an explicit migration-adapter legacy reference.
- Partial progress is not a failure and must not burn retry attempts. The command and legacy item both retain page progress so large projections can finish across bounded background ticks.
- Obsolete projection input/version checks are terminal success with `candidate_count=0`; they prevent stale index builds from overwriting newer projection semantics.
- Each paged build is fenced by three reserved projection metadata keys:
  `projection_person_search_index_input_revision` is the storage-owned semantic member-input revision,
  `projection_person_search_index_build_input_revision` is the revision captured by the current build, and
  `projection_person_search_index_build_generation` is the build identity. Member publication advances the input
  revision only for an index-relevant semantic change and preserves it for an identical replay.
- A reset compares the previously observed generation and input revision, then binds the new generation to the current
  input revision and performs the scoped replace in one advisory-lock transaction. `updated_at` is deliberately not a
  build fence. Continuation, partial progress, finalization, and facet/state publication must compare the generation
  and require build-bound revision to equal current input revision in their write transaction. A delayed reset,
  continuation, or state write returns obsolete without mutating the newer index, and a completed generation cannot be
  downgraded to `building` or `partial`.
- Public index reads validate a non-empty generation and equality of build-bound/current input revision before and
  after the read. Stale or changing input fails closed. Projection-row upsert, including bulk conflict metadata merge,
  preserves all three reserved keys; only the fixed member-publication UoW may advance the storage revision, and only
  the index writer may claim build ownership.
- Public facet counts and index-readiness mirrors carry the same three-key binding and become visible only after the
  generation-fenced finalization UoW marks that build `completed`. Semantic membership mutation and reset both
  invalidate the prior product in their own transaction, so an unfiltered projection page cannot expose the previous
  exact facet/readiness state between reset and finalization. A completed empty projection remains an exact-zero
  product under this same contract.
- Recovery/service summaries must expose index command-owner evidence through `projection_person_search_index`: `command_count`, `executed_command_count`, `claimed_count`, `completed_count`, `partial_count`, `waiting_prerequisite_count`, `failed_count`, `skipped_count`, `runtime_namespace_skipped_count`, `candidate_count`, `indexed_count`, `legacy_bridge_used=false`, and `migration_phase=W2c_projection_person_search_index_build`.
- The old item queue must not execute, even with explicit migration/emergency allow flags. The compatibility entrypoint may convert supported `job_materialization_items` rows into `workflow_commands`, then the typed owner executes those commands. Disabling the typed command owner returns `legacy_job_materialization_recovery_bridge_disabled`.

W6 decision A is active for W2c.4a: new projection-index normal-path payloads live in `workflow_commands`; `projection_person_search_index_build` rows are migration input only.

### W2c.4b Collection Authoritative Merge Command Owner Contract

Collection authoritative merge is the owner boundary that folds a completed run-scope projection into the collection-level local asset projection and atomically switches `collection_authoritative_pointer`. It must not be repaired by public readers, collection overview readers, or run result readers.

Required behavior:

- Run-scope projection publication must plan a reducer-owned `CommandPlanRequested` event for `collection.authoritative.merge`.
- The normal-path typed command payload is self-contained in `workflow_commands`: `job_id`, `collection_id`, `source_projection_id`, `item_id`, source run id, publication fingerprint, projection input version, member count, and materialization metadata.
- `legacy_materialization_item` is allowed only on commands produced by the report-visible migration adapter for historical `job_materialization_items` rows. Normal-path planners must not include it.
- The command idempotency key includes collection id, source projection id, item id, publication fingerprint, and merge scope. Replaying the same run projection publication must not create a second merge command or switch the pointer twice.
- The `collection_writer_owner` recovery/service phase claims ready `collection.authoritative.merge` commands, marks them `running`, synthesizes the former item envelope from command-owned payload, executes the collection merge writer primitive, and then marks the command `succeeded`, `retry_wait`, or `failed_terminal`. It may claim/read `collection_authoritative_merge` only when the command carries an explicit migration-adapter legacy reference.
- Pointer switching remains owned by `ServingProjectionWriter.publish_collection_authoritative_projection(...)`; the command owner orchestrates the merge but does not let public readers or legacy result endpoints synthesize collection state.
- Recovery/service summaries must expose collection merge command-owner evidence through `collection_authoritative_merge`: `command_count`, `executed_command_count`, `claimed_count`, `completed_count`, `failed_count`, `skipped_count`, `runtime_namespace_skipped_count`, `candidate_count`, `legacy_bridge_used=false`, and `migration_phase=W2c_collection_authoritative_merge`.
- The old item queue must not execute, even with explicit migration/emergency allow flags. The compatibility entrypoint may convert supported `job_materialization_items` rows into `workflow_commands`, then the typed owner executes those commands. Disabling the typed command owner returns `legacy_job_materialization_recovery_bridge_disabled`.

W6 decision A is active for W2c.4b: new collection-merge normal-path payloads live in `workflow_commands`; `collection_authoritative_merge` rows are migration input only.

### W2c.4c Run-Scope Projection Finalization Command Owner Contract

Run-scope projection finalization is the owner boundary that turns a persisted asset-population result view into the canonical public result projection for that run. It must not be repaired by public readers, legacy result endpoints, or frontend request paths.

Required behavior:

- Job result-view writers may request run-scope projection finalization, but they must plan a reducer-owned `CommandPlanRequested` event for `projection.run_scope.finalize`.
- The typed command payload contains `job_id`, `view_id`, `snapshot_id`, `candidate_source`, request payload, replace-members policy, and finalization reason.
- The command idempotency key includes job id, result view id, snapshot id, source path, and finalize scope. Replaying the same result-view publication must not create a second finalize command or publish duplicate projection members.
- The `serving_projection_owner` recovery/service phase claims ready `projection.run_scope.finalize` commands, marks them `running`, publishes the run-scope projection through `ServingProjectionWriter`, writes the run/projection link, updates `job_result_view.metadata.run_scope_projection`, and then marks the command `succeeded`, `retry_wait`, or `failed_terminal`.
- The owner must write ActivityRun/Attempt/EntityDelta evidence under `activity_type=projection.run_scope.finalize` and `entity_type=run_scope_projection`. Successful publication records `delta_kind=run_scope_projection_finalized`; obsolete, retry-wait, or failed publication records `run_scope_projection_finalize_not_applied` with a not-applied reason. Command result JSON may carry the activity and delta ids, but it is not the only audit source.
- Synchronous result-view publication may drain only the command it just planned to preserve existing result-page readiness. Background recovery handles other queued finalize commands.
- Successful finalization must record an explicit durable `CompletionProofRecorded(proof_key='serving_finalized')` event. `workflow_current_state.completion_proofs.serving_finalized` is the typed proof that canonical projection/lifecycle can serve public result pages with stable counters.
- Finalization may enqueue downstream projection index and collection merge commands, but those remain separate owners. It must not run indexing, collection merge, snapshot compaction, or facet layering inline.
- Recovery/service summaries must expose finalization owner evidence through `run_scope_projection_finalize`: `command_count`, `executed_command_count`, `claimed_count`, `completed_count`, `failed_count`, `candidate_count`, `legacy_bridge_used=false`, and `migration_phase=W2c_run_scope_projection_finalize`.

W2c.4c intentionally keeps downstream projection-index and collection-merge as separate command owners. W3 consumes `serving_finalized` as a hard completion policy proof instead of overloading `job.status=completed/results`.

## Data Asset Cleanup Gate Before Legacy Deletion

Before deleting old recovery branches, local data assets must be reviewed and consolidated.

Reason: early production and test environments were not fully separated, and companies such as Google and Anthropic may have multiple large, nearly duplicate snapshots created by testing or mixed runtime states. Keeping all of them as active source candidates raises storage, migration, and serving costs.

Required cleanup evidence:

- list of large company snapshots by company, snapshot id, candidate count, profile count, source runtime, creation time, and serving/projection references
- overlap/subsumption report for large companies such as Google and Anthropic
- authoritative projection/pointer recommendation per company and scope
- scoped shard preservation plan for valuable assets such as Gemini, Veo, Infra, Agent, Lovable, OpenAI scoped runs
- archive manifest for no-increment duplicate snapshots
- cold-backup manifest for archived snapshots before deletion or exclusion from default source selection
- proof that CRM records, PersonIdentity, PersonAsset, assertions, exports, and run projections no longer depend on archived snapshots as the only source

Cleanup outputs should update `DATA_ASSET_GOVERNANCE.md` and authoritative collection metadata. Normal local asset consumption should use clean authoritative projections, not a union of every historical large snapshot.

## Testing And Preflight

Fast contract preflight must run before long Nightly matrices.

Required preflight gates:

- Stage 1 public progress uses one coherent semantic version: lane counts, deduped count, and profile denominator cannot mix sources.
- Scheduler slot-refill evaluator distinguishes true open-slot underuse from reserved, provider-owned, dispatch-claimed, or lease-deferred legal states.
- Provider dispatch idempotency includes process-local and artifact-level guards. `harvest_profile_search` recovery/resume must consume the existing raw artifact when another process owns the same dispatch lock; duplicate provider calls for the same payload are a preflight failure.
- Remote-wait ownership is typed. A submitted remote worker with `run_id/dataset_id` and no `remote_provider_terminal_event` marker belongs to provider webhook/watcher event handling, not generic worker recovery. Generic recovery may recover only explicit terminal-event wakeups or local post-terminal phases.
- Reducer tests prove `state + event -> state + commands` without provider calls.
- Workflow causality contract proves normal-path `workflow_commands` carry physical causality columns with source event, owner, causal group, produced counts/no-op reason, and readiness effect. Metrics must not infer command causality from timestamp or snapshot proximity.
- CRM Public Web start must plan `workflow_commands(command_type='crm.public_web.queue_batch')` before batch/run creation or worker creation. The route may drain that one command synchronously for current UX, but the batch/run write, job shell update, and worker/phase enqueue side effects belong to `crm_public_web_owner`; disabling the owner must fail closed/report-visible and must not fall back to route-local batch/run writes, worker creation, or target-candidate bridges. A visible CRM Public Web batch/run without a succeeded or pending queue-batch command proof is an orphan-run contract failure.
- CRM Public Web W6/W7 preflight must prove the post-workflow action report includes a succeeded `crm.public_web.queue_batch` command with owner `crm_public_web_owner`, physical causality columns, produced counts/no-op reason, and status counts. A CRM-owned batch without this command proof is a contract failure, even if storage owner and execution backend are already `crm_public_web_v1`. Current-run selection for a CRM record is ordered by run creation/planning time, not `updated_at`; progress updates from an older run must not make that older run current again.
- Cross-module durable command owner preflight must report `durable_command_owner_contracts` for the acquisition root/planning commands (`acquisition.run.create`, `acquisition.intent.resolve`, `acquisition.plan.build`, `acquisition.plan_review.request`, `acquisition.plan.commit`), LinkedIn acquisition/profile commands (`linkedin.discovery_query.run`, `linkedin.profile_refill.submit_batch`, `linkedin.profile_url_terminal.record`, `linkedin.local_profile_delta.apply`), projection/collection/snapshot commands, CRM Public Web queue/phase commands, CRM writer commands (`crm.record.add_from_projection`, `crm.record.update`, `crm.note.add`, `crm.task.create`), company Public Web root/phase commands (`company.public_web.refresh`, `company.public_web.source.collect`, `company.public_web.assets.materialize`), profile-experience company logo discovery (`company.logo.profile_experience.discover`), media cache command (`media.asset.cache`), `excel.intake.run`, `export.projection.generate`, and `export.crm_public_web.generate`. Each report must expose command count, expected owner count, invalid owner count, pending/terminal status counts, and incomplete causality samples. W6/pre-release signoff may require this through `require_durable_command_owner_contracts`; a missing or dirty report is a contract failure before long-chain matrix validation.
- Post-profile SLO metrics must expose whether they used typed causal-group facts or legacy snapshot/timestamp heuristic pairing. W6/nightly normal-path signoff must require `heuristic_pairing_used=false`; legacy snapshot pairing is migration evidence only and cannot satisfy normal durable-runtime SLO proof.
- Public results runtime diagnostics must stay bounded. `/api/jobs/{job_id}/results` on the public API path may expose compact events, compact Agent runtime session/trace/worker summaries, and compact workflow stage summaries only when it also returns `runtime_details_contract.schema_version=public_results_runtime_details_v1`. Raw job summaries, large runtime blobs, and artifact payloads belong to internal compatibility APIs or explicit artifact/debug endpoints, not the public results reader.
- Finalization-start SLO gating must prefer typed terminal-record command evidence. For provider-backed profile work, `post_preview_finalization.profile_terminal_at` is sourced from succeeded `workflow_commands(command_type='linkedin.profile_url_terminal.record')` before falling back to remote worker terminal timestamps. Remote worker completion proves provider activity; terminal-record command completion proves the profile registry owner committed the durable terminal state that can unblock local apply, board visibility, and projection finalization.
- Command lease tests prove atomic claim, heartbeat, retry-wait, terminal failure, and idempotent success.
- Public reader tests prove missing canonical read models fail closed and do not repair.
- Legacy bridge tests prove adapter usage is report-visible and normal-path legacy writes fail signoff.

Nightly and containerized gates validate end-to-end behavior after these preflights pass. Nightly should not be used to discover basic contract/source-of-truth violations.

## Implementation Order

### Phase W0: Current Failure Preflight

Close the two current Nightly failure classes before runtime migration:

- Stage 1 progress single-version contract.
- Scheduler slot-refill legal-state contract.

Then run targeted OpenAI baseline+delta and Lovable live-roster.

### Phase W1: Runtime Schema And Reducer Skeleton

Add `workflow_events`, `workflow_current_state`, `workflow_commands`, `runtime_outbox`, reducer interfaces, command owner registry, and tests for append-only events, deterministic reducer output, and command idempotency.

### Phase W2: LinkedIn Pipeline Command Cutover

Move new LinkedIn acquisition/profile normal path to typed commands for profile refill, URL terminal-state recording, local apply, board-visible apply, run projection finalization, projection index build, and collection merge.

### Phase W3: Completion Policy Cutover

Make Stage 1 preview, profile fetch terminal, local apply terminal, board visible terminal, serving finalized, and post-result layering readiness explicit runtime policies. `job.status/results/completed` becomes a UI/read-model projection only.

W3a establishes the central policy evaluator:

- `workflow_completion_policy.evaluate_linkedin_completion_policies(...)` is side-effect free. It consumes durable observations supplied by callers and must not read DBs, repair state, or publish projections.
- The evaluator returns one record per typed policy plus a legacy-compatible `first_blocker` adapter while call sites migrate.
- `_workflow_completion_promotion_blockers(...)` may collect observations from workflow lease, workers, lifecycle, board-visible patch ledger, materialization items, Stage 1 progress, and workflow current state, but blocker semantics come from the evaluator.
- Completion gates should attach the full `completion_policy` payload to blockers while migration is active. This makes hidden fallback or ambiguous completion semantics report-visible.
- `serving_finalized` is sourced from `workflow_current_state.completion_proofs.serving_finalized`, not from public result endpoints or `job.status`.

W3b cuts over workflow terminal consumers:

- Workflow terminal checks must use `_job_is_terminal(...)` or an equivalent typed policy helper. `job.status in {"completed", "results"}` is a UI/read-model state and is not sufficient for LinkedIn workflow truth.
- Supervisor exit, terminal wait loops, progress auto-recovery suppression, final-results phase labeling, and deferred terminal promotion must require typed policy proof for workflow jobs.
- If typed durable current state is unavailable, completion evaluation fails closed. The reported reason is `serving_finalized_proof_unavailable` only when no earlier concrete blocker exists; active workers, active materialization commands/items, incomplete profile fetch, and incomplete board visibility remain the first diagnostic blocker.
- Tests that need to write `workflow_events`, `workflow_current_state`, `workflow_commands`, or `runtime_outbox` must use the PG-only fixture/harness. Unit tests may mock typed proof when they are testing policy consumers rather than durable storage.

### Phase W4: Legacy Adapter And Retirement Gates

Convert in-flight/historical `job_materialization_items` into canonical commands through a report-visible adapter. Add gates that block normal-path legacy writes and legacy recovery branch usage.

W4a establishes the adapter foundation:

- `legacy_materialization_adapter` is a recovery/service phase owned by `durable_runtime_migration_adapter`.
- The adapter may inspect `job_materialization_items` and plan reducer-owned `workflow_commands` for supported migration-era kinds only: `local_apply_closure`, `board_visible_delta_apply`, `search_seed_discovery_query`, `projection_facet_layering_build`, `projection_person_search_index_build`, `collection_authoritative_merge`, and `snapshot_full_materialization`.
- The adapter must not execute legacy items. Typed owners execute commands later: `linkedin_profile_owner`, `linkedin_acquisition_owner`, `board_visible_projection_owner`, `projection_facet_layering_owner`, `projection_index_owner`, `collection_writer_owner`, and `snapshot_materialization_owner`.
- Unsupported kinds, invalid scopes, open unconverted rows, already-converted rows, and command observations are report-visible. They must not be silently ignored.
- `already_converted_count` is audit evidence only. It must not block downstream typed owners from executing queued commands.
- Same-tick legacy drains must yield when the adapter planned commands for that scope, so the old recovery path cannot consume a row that has just been transferred to the typed command owner.
- Projection facet/layering command planning must fail closed and report the concrete runtime error if `workflow_events` / `workflow_commands` cannot be written. It must not silently return an empty command and let callers infer an overlay or reader fallback. Tests for this planner use the PG-only durable-runtime fixture because SQLite durable runtime is not a normal execution path.

Command selection invariant:

- A planner must select the command by its exact `idempotency_key`, never by `apply_result.commands[0]` or list order.
- This applies to local apply, board-visible publish, run-scope finalize, projection index build, and collection merge.
- Reusing the first command in a workflow can route later chunks to an earlier succeeded command, producing false completion, stale patch payloads, or missing cumulative counts.

W4b establishes the normal-write gate:

- Service metrics must expose `legacy_materialization_write_contract` over all observed `job_materialization_items`.
- The report must split `normal_path_write_count`, `migration_adapter_write_count`, and `missing_contract_count`, with item-kind counts and samples.
- Pre-Manual Signoff blocks `legacy_materialization_normal_write_used` when a cutover case declares `require_no_legacy_materialization_normal_writes=true`.
- Pre-Manual Signoff blocks missing legacy write contract metadata when a case declares `require_legacy_materialization_write_contract_report=true`.
- The strict storage env gate `SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES=1` remains the hard local/CI preflight for cutover slices. Explicit migration adapter/backfill writes must carry `migration_adapter=true` or `write_owner=durable_runtime_migration_adapter`.
- Full scripted/nightly signoff must not be rerun as the first detector for legacy normal writes. Promote the `durable_runtime_legacy_write_cutover` service coverage tag to `required_now` before W6 full runtime signoff, after W4c/W5 reduce false positives from still-open migration payload/reference rows.

W4c establishes the pre-deletion recovery-bridge guard:

- Recovery/service metrics must aggregate `legacy_bridge_used` from phase-level owner reports into `recovery_phase_metrics.legacy_bridge_used_count`, `legacy_bridge_used_present`, and `legacy_bridge_used_phases`.
- `legacy_bridge_used=true` is high-severity debt in service metrics because it means recovery executed a legacy branch instead of a typed command owner.
- Pre-Manual Signoff blocks `legacy_materialization_recovery_bridge_used` when a cutover case declares `require_no_legacy_materialization_recovery_bridge=true`.
- Normal recovery must hard-disable implicit legacy fallback. Closing a typed command owner toggle must return `legacy_job_materialization_recovery_bridge_disabled`, not drain the old queue.
- Legacy recovery branches must not execute. Migration-era rows may be converted by the report-visible adapter, but execution belongs to typed command owners only.
- Explicit bridge execution allow flags are retired for normal recovery. They must not bypass typed owner disablement or runtime-namespace checks.
- Fast preflight must catch legacy bridge usage before full PG-backed Nightly. Long matrices are for timing/stability validation after contract guards pass, not for discovering unclassified legacy execution.

### Phase W5: Asset Consolidation Before Deletion

Run large-snapshot asset audit and consolidation for mixed historical production/test assets. Publish clean authoritative projections and archive duplicate large snapshots before deleting old recovery branches that might still be needed for historical rows.

W5a establishes the read-only dependency preflight:

- `asset_consolidation_audit_v1` is the metadata-first deletion/archival preflight. It reads local snapshot directories plus `organization_asset_registry`, `acquisition_shard_registry`, `serving_projections`, `collection_authoritative_pointers`, `crm_records`, and `person_assets`.
- It must not mutate registry rows, projection rows, CRM/person assets, or local files. It classifies snapshots as `keep_authoritative_serving`, `keep_reusable_shard_source`, `keep_projection_dependency`, `archive_candidate_no_increment_duplicate`, or `review_*`.
- Deletion blockers are explicit and reportable: authoritative registry pointer, selected source snapshot, latest local pointer, reusable acquisition shard, active serving projection dependency, CRM source projection dependency, and person-asset source projection dependency.
- Normal cleanup may only archive/delete snapshots that are `archive_candidate_no_increment_duplicate`, `archive_ready=true`, and have no deletion blockers.
- Local-only snapshots without registry proof are `review_local_only_snapshot`, not silent archive candidates.
- The W5a audit does not compute member-level overlap/subsumption. Production-scale W5b overlap jobs may read large candidate manifests after W5a identifies the candidate snapshot set and blockers.
- Operator entrypoint: `scripts/audit_asset_consolidation.py --company Google --output-json ... --output-md ...`. Full PG-backed Nightly remains deferred until W5 evidence and W6 preflight gates are clean.

W5b establishes bounded archive-candidate overlap evidence:

- `--include-overlap` upgrades the report to `asset_consolidation_audit_v2` and reads candidate identity sets only for current authoritative/source snapshots plus W5a archive candidates.
- Overlap is offline maintenance evidence. It must never run inside public readers, workflow recovery, collection pages, or normal acquisition callbacks.
- Identity comparison must prefer LinkedIn sanity/profile URL identity (`linkedin:{profile_url_key}`) over unstable historical candidate ids. Candidate ids are fallback only when no profile/person key exists.
- Archive candidates remain `archive_ready=false` unless their identity set is loaded, not truncated, and fully subsumed by the authoritative/source reference identity set.
- Bounded controls are mandatory: `--overlap-candidate-limit` caps candidate reads per snapshot and `--overlap-snapshot-limit` caps archive-candidate snapshots per company.
- Missing local payloads, truncated reads, missing reference identity, or unique identities are review states, not archive approval.

W5c establishes reviewed repair and cold-archive manifest boundaries:

- Authoritative-source repair may publish a clean `collection_authoritative_projection` and pointer only through a reviewed maintenance operation. It must not mutate historical snapshot files or registry rows.
- Cold archive manifests are evidence, not execution. `asset_consolidation_cold_archive_manifest_v1` must be read-only and carry `deletion_allowed=false`.
- A snapshot can enter the manifest only when the W5 plan already says `archive_ready_for_cold_backup_review`, overlap status is `subsumed_by_reference`, unique identity count is zero, deletion blockers are absent, the local snapshot directory exists, and the file listing is complete.
- Manifest rows may recommend normal-reuse exclusion after cold-copy verification, but actual exclusion, movement, or deletion must be a separate reviewed operation with its own evidence.

### Phase W6: Full Signoff And Legacy Deletion

Run targeted OpenAI/Lovable/Google, full PG-backed scripted matrix, fake-provider containerized gates, browser gates, and signoff. Delete or hard-disable old recovery branches and legacy normal-path job-materialization writes after evidence is clean.

W6 decision A:

- The long-term target is command-owned payload storage, not tolerated normal-path migration rows.
- New W2c/W6 normal-path planners for local apply, board-visible publish, search-seed discovery, projection facet/layering, projection index build, collection authoritative merge, and snapshot compaction must write the full execution payload into `workflow_commands`.
- Normal-path command payloads must not contain `legacy_materialization_item`.
- `job_materialization_items` may be read only by the report-visible migration adapter for historical/in-flight rows and by command owners only when a command carries an explicit `legacy_materialization_item` reference with `migration_adapter=true`.
- The strict preflight is `SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES=1` plus service/signoff checks for `normal_path_write_count=0` and `legacy_bridge_used=false`.
- Public diagnostics and smoke reports must include `workflow_commands` alongside any remaining migration-era `job_materialization_items`, so command-owned backlog/status is visible without reviving a legacy queue.
- CRM Public Web is part of the typed-command normal path. `crm.public_web.queue_batch` owns CRM Public Web batch/run planning for `crm_public_web_v1` and then plans per-run phase commands: `crm.public_web.search.submit`, `crm.public_web.search.poll_fetch`, `crm.public_web.documents.fetch`, `crm.public_web.evidence.adjudicate`, `crm.public_web.model_safe.finalize`, and `crm.public_web.signals.materialize`. Normal API start may synchronously drain only the command it planned; it must not create CRM Public Web agent workers or write Public Web phase state directly. route-local worker creation is not a normal path for CRM Public Web. Legacy target-candidate Public Web endpoints and target-to-CRM sync are migration/test-only and cannot satisfy this command-owner proof. Worker-daemon recovery must not execute CRM or legacy target-candidate Public Web runtime functions for historical `agent_worker` rows; those rows may be quarantined as migration evidence only. `workflow_service_metrics.target_candidate_public_web` and `durable_command_owner_contracts` are the signoff owners for queue-batch and phase command counts and must block missing, pending, failed, invalid-owner, or incomplete-causality command evidence before W6/nightly is accepted.
- W6 is not fully signed off until targeted OpenAI/Lovable/Google, full PG-backed scripted matrix, fake-provider containerized gates, browser gates, and Pre-Manual Signoff all pass with no normal-path legacy writes.
- After W6 signoff, normal recovery must not run the legacy materialization adapter by default. `legacy_materialization_adapter` is an explicit historical migration operation only: it requires `legacy_materialization_adapter_enabled=true` in the recovery payload or `LEGACY_JOB_MATERIALIZATION_ADAPTER_ENABLED=1`. Default skipped reason is `legacy_materialization_adapter_disabled_after_w6_signoff`. Typed command owners may execute command-owned payloads or explicit migration-adapter commands, but they must not scan `job_materialization_items` as a normal backlog source.
- W6b PG-only cutover deleted the pytest SQLite durable-runtime escape hatch. `workflow_events` and `workflow_commands` must keep physical PG idempotency constraints, including unique `(workflow_run_id, sequence_number)` / `(workflow_run_id, idempotency_key)` for events and unique `(workflow_run_id, idempotency_key)` for commands, even when PG tables are bootstrapped from an older SQLite shadow schema.
- `snapshot.compaction.run` is background maintenance after canonical board-visible projection and post-profile SLO proof are clean. Healthy queued/running backlog must be reported through `service_metrics.snapshot_full_materialization_queue` with owner `snapshot_materialization_owner`, scope `background_snapshot_compaction`, and `manual_handoff_blocking=false`; Pre-Manual Signoff records it as a passed background-maintenance gate. Retry, stale-running, or terminal-failed compaction is not healthy background backlog and must block signoff unless the case is explicitly a historical migration review.
- The `snapshot_materialization_owner` must write ActivityRun/Attempt evidence and a `workflow_entity_deltas(entity_type='snapshot_compaction')` row for completed, waiting-prerequisite, deferred, and failed effects. Snapshot compaction remains background maintenance, but it must not be command-result-only.
- `durable_work_handoff_yield` is cooperative scheduling, not a warning, when the recovery phase contract is otherwise clean. Service metrics must expose `cooperative_handoff_yield_count`, `handoff_yield_contract=cooperative_scheduling_yield`, and `handoff_yield_manual_handoff_blocking=false`; Pre-Manual Signoff records `recovery_cooperative_handoff_yield` as a passed gate. If the same report has missing/failed/slow/unexpected phases, legacy bridge usage, or tick-budget exhaustion, the yield is only diagnostic and the underlying dirty recovery signal remains blocking or warning by its own gate.
- `recovery_tick_budget_exhausted` is cooperative scheduling only when the same report proves `next_tick_requested=true`, no missing/failed/slow/unexpected phases, and no legacy bridge. Service metrics must expose `cooperative_budget_yield_count`, `budget_yield_next_tick_requested_count`, `budget_yield_contract=cooperative_recovery_budget_yield`, and `budget_yield_manual_handoff_blocking=false`; Pre-Manual Signoff records `recovery_cooperative_budget_yield` as a passed gate. Missing next-tick proof or dirty owner evidence keeps the budget yield report-visible as attention/warning rather than a passed gate.

W6 preflight rule:

- W6 must validate long-chain stability, latency, recovery, command-owner pressure behavior, and migration deletion evidence. It must not be used as the first detector for field-owner drift, hidden fallback, or basic source-of-truth ambiguity.
- Before W6, fast preflight must prove public endpoint parity for shared board/projection fields, typed command owner readiness, strict legacy-write metrics, and completed-reconcile idempotency.
- Before W6, provider dispatch preflight must prove same-payload `harvest_profile_search` cannot double-submit across recovery/resume, and remote-wait owner preflight must prove generic recovery skips provider-owned workers until a durable terminal marker exists.
- Runtime limiter calls with no explicit `lease_token` represent a new acquire attempt and must generate a unique token. Only callers that pass an explicit token may refresh an existing lease. A budget-1 single-flight slot must reject a second same-owner attempt while the first lease is active.
- `completed_workflow_reconcile_inflight` is a coalesced prerequisite, not a retryable failure. Event-time owners should emit a `phase=coalesced` skip; command queue wrappers may map it to `waiting_prerequisite`/`retry_wait`, but must not create legacy `failed_retryable` backlog or a second materialize/deferred/completed event for the same worker set.
- Completed-worker reconcile owners must re-read current worker state before executing from any pending-worker list captured by another recovery phase. A worker that already has an `inline_incremental_ingest` marker is consumed; stale pending lists must skip without publishing another materialize/deferred/completed event for that worker.
- `same_worker_reconcile_repeat_count=0` is a hard contract for normal LinkedIn profile/materialization paths. Repeated deferred observations are allowed only if they are for different workers or different command owners with explicit idempotency keys; a second event for the same worker/snapshot/kind means owner-boundary revalidation failed.
- Results API runtime diagnostics are a read-model contract, not a hidden recovery or artifact channel. Public readers must keep `runtime_details_contract.payload_shape=bounded_public_diagnostics` and must not reintroduce raw `job.summary`, full worker payloads, full trace payloads, or large artifact summaries to make smoke reports pass.
- Post-preview finalization pressure cases must report `finalization_start_gate_source=profile_url_terminal_record_command_completed_at` when typed terminal-record command evidence exists. `profile_terminal_at` fallback from remote workers is tolerated only for migration/historical reports where command evidence is absent and must remain visible through `profile_terminal_source`.

### Phase W7: Module Reuse

Migrate CRM/Public Web, Excel, export, and future Agent operation execution onto the same durable runtime primitives. Do not start Phase 13 Agent graph implementation until the runtime contract, LinkedIn pipeline migration, and non-LinkedIn module reuse are stable.

W7 status and rules:

- CRM Public Web is the first W7 module cutover. Normal start uses `workflow_commands(command_type='crm.public_web.queue_batch', owner='crm_public_web_owner')` to create/validate CRM-owned batch/run rows under `crm_public_web_v1`, then queues one typed per-run phase command. Route-local synchronous worker creation is not a normal path. The typed phase owner drains `crm.public_web.search.submit`, `crm.public_web.search.poll_fetch`, `crm.public_web.documents.fetch`, `crm.public_web.evidence.adjudicate`, `crm.public_web.model_safe.finalize`, and `crm.public_web.signals.materialize`.
- Normal CRM Public Web runtime imports must go through `src/sourcing_agent/crm_public_web_runtime.py`, which delegates to owner-neutral `src/sourcing_agent/public_web_runtime_core.py`. The retired `src/sourcing_agent/target_candidate_public_web.py` facade has been physically deleted, so explicit legacy target-candidate execution references must go through `src/sourcing_agent/legacy_target_candidate_public_web_runtime.py`, not arbitrary direct imports from normal service modules. `tests/test_crm_public_web_runtime_boundary.py` owns this source/runtime contract: the deleted target facade must not reappear, `crm_public_web_runtime.py` and `legacy_target_candidate_public_web_runtime.py` must import the physical core rather than a target facade, worker recovery must keep `crm_public_web_search` separate from legacy `target_candidate_public_web_search`, normal source files may not call `target_candidate_public_web_*` storage helpers, and non-migration tests may not seed old rows through direct store writes. `public_web_runtime_core.py` no longer keeps an unreachable legacy target-candidate execution body that can write target-candidate Public Web rows and no longer directly calls legacy storage helpers. The only allowed legacy table reader/writer outside storage/PG schema is `src/sourcing_agent/legacy_public_web_storage.py`, which is migration-only and returns empty rows when the old tables are physically absent. `ControlPlaneStore.upsert/update_target_candidate_public_web_*` fails closed unless a reviewed `legacy_target_public_web_migration_write_context(...)` is active. New SQLite compatibility shadows no longer bootstrap `target_candidate_public_web_*` tables in normal schema setup; normal PG live/bootstrap defaults no longer list retired tables; explicit migration seeding recreates them only inside the migration context, enables a migration-only PG table context, and PG writer-schema repair may index legacy tables only if historical tables already exist. W7e deletion now focuses on final physical PG/table-helper retirement; the shared execution implementation is no longer physically owned by a target-candidate module.
- The remaining shared Public Web execution implementation must stay owner-neutral. The historical file may temporarily hold `_execute_public_web_run_once(...)` / `_execute_public_web_run_to_local_idle(...)`, but CRM and target-candidate entrypoints must be thin owner-specific wrappers around that neutral core. New behavior must not be added to a target-candidate-named execution wrapper and then reused by CRM.
- CRM Public Web owner evidence is mandatory in service metrics and signoff: `storage_owner_counts.crm_public_web_v1 > 0`, `execution_backend_counts.crm_public_web_v1 > 0`, `queue_batch_command_succeeded_count > 0`, phase command owner contracts are complete when phase commands are present, and no normal-path `target_candidate_public_web_v1` execution backend.
- W7e physical deletion requires `legacy_public_web_retirement_audit_v1` evidence from `scripts/audit_legacy_public_web_retirement.py --strict`. The deletion blocker is remaining legacy target-candidate Public Web batches/runs/promotions or a row-limit-truncated audit. CRM-owned Public Web rows and collection authoritative pointers are context only; incomplete local asset Overview coverage must not block deleting the retired target-candidate Public Web runtime.
- W7e archive/drop is a separate migration-only side effect, not part of the read-only audit. Run it with the repository virtualenv Python, for example `.venv/bin/python scripts/archive_drop_legacy_public_web_tables.py ...`, so the PG adapter uses the same `psycopg` dependency as the service runtime. The script writes `legacy_public_web_archive_v1` cold manifests and executes `legacy_public_web_drop_v1` table drops. Non-empty legacy tables must be archived before drop unless a dangerous explicit override is supplied. Normal runtime, CRM Public Web owners, worker recovery, and public readers must never call this utility or use its manifest as serving state.
- Latest W7e active-runtime closeout on 2026-05-26: `runtime/audits/legacy_public_web_retirement_w7e_strict_latest.json` stayed `ready_for_physical_deletion`; `runtime/audits/legacy_public_web_archive_w7e_latest.json` archived zero legacy rows; `runtime/audits/legacy_public_web_drop_w7e_latest.json` dropped the three retired PG tables under `legacy_public_web_drop_v1`. New deployments should not create these tables through normal bootstrap.
- W7e targeted signoff must also run the CRM Public Web fake-provider/action matrix with `require_legacy_public_web_retirement_ready=true`. Latest local evidence on 2026-05-25: `output/w7e_crm_public_web/target_public_web_service_20260525T153316Z_report.json` completed with no expectation failures, `crm.public_web.queue_batch` succeeded under owner `crm_public_web_owner`, storage/execution owner stayed `crm_public_web_v1`, legacy storage/bridge counts were `0`, and `legacy_public_web_retirement.status=ready_for_physical_deletion`.
- Legacy target-candidate Public Web APIs, orchestrator methods, and lower-level helper calls are permanently retired. HTTP aliases return unconditional `410` canonical endpoint payloads; API code must not keep dead override branches that call legacy orchestrator methods. Orchestrator/lower-level helpers must return report-visible retired envelopes with `migration_override_status=removed`; `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS` is ignored and cannot re-enable execution. Historical target-to-CRM sync is a separate reviewed migration path and must not serve as a normal read/write/recovery fallback.
- Fake-provider/action signoff can prove owner semantics, command causality, metrics, and fail-closed behavior. Physical deletion of legacy target-candidate Public Web tables/helpers still requires a small live-provider product validation because result quality, promotion UX, and export payload quality depend on real DataForSEO/Qwen behavior.
- Excel intake first-owner slice is active: upload planning writes `excel.intake.run` with physical causality, produced counts, and owner `excel_intake_owner`. The route may synchronously drain the just-planned command to start the async workflow for current UX, but stale recovery must requeue/plan and drain `excel.intake.run`; it must not call `_run_excel_intake_workflow(...)` directly. This command currently owns durable start/recovery and cooperative running cancel of the intake run. Remaining Agent-grade operation control work is richer status/resume/progress UX, not a second Excel execution owner.
- Projection export and CRM Public Web export first-owner slices are active: `export.projection.generate` and `export.crm_public_web.generate` own ZIP generation, artifact persistence, idempotency, and replay-from-artifact behavior. Current HTTP routes may synchronously drain the command for small exports. Large/bulk exports must move to explicit query/retry/cancel/status APIs before Agent can call them as long-running operations.
- CRM writer operation slices are active for Agent/Operation-driven `add_to_crm`, `set_crm_stage`, `add_crm_note`, and `create_crm_task`. Operation dispatch plans typed commands only; `crm_writer` command owner calls `CRMWriter` and writes `crm_records`, `crm_engagements`, PG-only `crm_tasks`, and `crm_events`. `add_to_crm` uses an owner-bound projection-selection target and revalidates the exact projection membership snapshot at submit, dispatch, and command-owner time before CRM effects. Sensitive or bulk stage mutations are approval-gated before command planning. Stale `claimed/running` CRM writer commands may be resumed only through the owner-specific command control delegate; the delegate records Activity/Attempt/EntityDelta resume evidence and returns the command to `queued` without applying CRM mutations from the request path.
- Company Public Web refresh is active for Agent/Operation-driven company enrichment. The D1m candidate request is
  closed, canonical, and deterministic `seed_url_only`; its owner target is exact `workspace_id + company_key`, and
  persisted action/run plus every dispatch/root/source/materialize boundary revalidates that request/target and its
  Operation/command links before a new effect. Operation dispatch plans `company.public_web.refresh` only; the root
  command plans `company.public_web.source.collect`. The source owner uses one normalized idempotency-key identity,
  sorted key/run advisory locks, and physical current-attempt/lease ownership to create, reclaim, join, and terminalize
  source work without stale-attempt writes. It writes or joins source-specific company Public Web rows/artifacts with
  canonical asset sync deferred, then freezes the full digest-bound `company_public_web_run_snapshot_v3` (assets,
  summary, artifact paths/publication digest, revision/completion time, and run timestamps). A closed source-completion
  PG UoW then exact-creates/reuses the plan event, deterministic `company.public_web.assets.materialize` child, one
  source-run EntityDelta, and parent success CAS together; stale/taken-over claims and any final-CAS failure leave that
  bundle at zero writes, while an acknowledgement loss must exact-validate the committed bundle before continuing.
  Materialization validates source-run identity, consumes only that immutable
  snapshot, syncs canonical `CompanyAsset` / `CompanyEvidence`, and records ActivityRun/Attempt/EntityDelta evidence.
  Running root refresh commands may be cancelled through `workflow_orchestrator.cancel_orchestration_before_downstream`
  only when no downstream source/materialize command exists and the lease is expired or force is explicit. Stale running
  source-collection commands may be resumed only through `company_public_web_owner.resume_source_collect`, which records
  ActivityRun/Attempt/EntityDelta control evidence and requeues the command without syncing canonical company facts.
  Stale running asset-materialization commands may be cancelled before sync through
  `company_public_web_owner.cancel_assets_materialize_before_sync` or resumed through
  `company_public_web_owner.resume_assets_materialize`; neither control delegate may write CompanyAsset/CompanyEvidence
  from the request path. The normal Agent path must not call the synchronous refresh service inline, treat mutable
  source-specific rows as execution truth, or sync canonical company facts from the source-collection/join branch. This
  does not close R-019 or claim one global command/effect/terminal/Operation-sync UoW.
- Stable media cache is active for bounded avatar/logo imports through `media.asset.cache`. The owner writes canonical `PersonAsset.avatar_media` / `CompanyAsset.logo_media` and Activity/EntityDelta evidence, and the read side serves object-backed cached media through `/api/media/assets/{asset_id}`; provider media URLs remain metadata until cached. Stale running media cache commands can be cancelled only before fetch/upload attempt creation or resumed only through the owner-specific command API; both delegates update command/activity evidence and never fetch, upload, or materialize assets from the request path.

After W7, the next pre-Agent layers are:

- `W8`: `OperationRun` persistence and module action registry. Foundation is active: `agent_actions`, `operation_runs`, and `operation_events` are PG-only durable runtime tables; `OperationRuntimeWriter` persists idempotent actions/runs/events without module side effects; unknown actions and missing required budgets fail closed; approval-required actions do not create executable operation runs before approval.
- `W9`: recoverable operation API/UI and approval gates. Backend control/provenance foundation is active: action registry/query/list, action submission, approval, rejection, operation cancellation, retry, resume, and provenance views are exposed through `/api/operations/*` and remain module-side-effect-free. Retry creates an idempotent queued child `OperationRun` instead of mutating a terminal parent. Owner-adapter slices are active for `export_candidates`, `filter_projection`, `search_projection`, `enrich_person_public_web`, `refresh_company_public_web_assets`, and CRM writer mutations: export dispatch plans `export.projection.generate` with `operation_id=<operation_run_id>` and does not synchronously run the export owner; projection read dispatch executes only canonical reader queries and stores bounded read results in `OperationRun.result_ref`; Public Web enrichment dispatch plans `crm.public_web.queue_batch`, while the command owner creates CRM Public Web batch/run rows and downstream per-run phase commands; company Public Web refresh dispatch plans `company.public_web.refresh`, while phase command owners collect source rows and then sync canonical CompanyAsset/CompanyEvidence rows; CRM mutations plan `crm_writer` commands and never write CRM tables from Operation dispatch. Command owners now propagate terminal/retry-wait/downstream-queued status back to the linked `OperationRun` and `AgentAction`; operation reads must not repair this state. `/operations` is the minimal read/control UI for this surface and calls only Operation APIs, including approve/reject action APIs for approval-required actions. Remaining W9 work is richer per-owner status copy and product polish.
- `W11`: Agent-callable workflow atomization. `start_acquisition_run` must plan `acquisition.run.create` as a causal root command, not `linkedin.discovery_query.run`. The root command is claimed by `acquisition_run_writer`, records bounded run intent, plans `acquisition.intent.resolve`, and defers operation completion until downstream typed commands finish. `acquisition.intent.resolve` normalizes operation payloads deterministically and plans `acquisition.plan.build`; `acquisition.plan.build` creates a typed plan result and plans `acquisition.plan_review.request` without calling providers or `queue_workflow`; `acquisition.plan_review.request` creates or reuses a pending `plan_review_session` and moves the linked operation to `acquisition_plan_review_requested`; approved review calls plan `acquisition.plan.commit`, whose owner verifies the approved review and materializes canonical PG-only `acquisition_runs` state (`committed_pending_probe`) before moving the linked operation to `acquisition_plan_committed_pending_probe`. A running `acquisition.plan.commit` can be cancelled only before probe command planning; the owner-specific cancel marks the acquisition run `cancelled_before_probe` so approval/commit state is not left as an ambiguous active run. A running `acquisition.scale.plan` can be cancelled only before discovery command planning and before any `workflow_activity_attempts` row exists; the owner-specific cancel marks the acquisition run, `workflow_activity_runs`, and `acquisition_discovery_lanes` rows `cancelled_before_discovery`. A running `linkedin.profile_fetch.activity.run` can be cancelled only before cache lookup starts, before profile EntityDelta creation, and before downstream profile/provider/terminal commands exist; the owner-specific cancel marks the profile activity `cancelled_before_cache_lookup`. A running `crm.public_web.queue_batch` can be cancelled only before phase command planning; the owner-specific cancel marks queued CRM Public Web batch/run rows cancelled. Later commands must cover provider-backed discovery, profile refill/terminal record, local apply, board-visible publication, run-scope projection finalization, index/facet builds, collection merge, and snapshot compaction. `queue_workflow(...)` may remain a report-visible migration bridge during this cutover, but it must not become the long-term execution body of `acquisition.run.create` or `acquisition.plan.commit`.
- `W10`: full contract review and fail-closed signoff across projection, workflow readers, CRM, person assets, Public Web, Excel, export, and operation APIs.
