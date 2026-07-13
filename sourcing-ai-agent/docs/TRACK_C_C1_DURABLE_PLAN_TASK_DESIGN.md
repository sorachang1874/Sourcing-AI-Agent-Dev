# Track C C1 Durable Plan Task Design

> Status: Author-complete executable design for owner review (2026-07-13). Scout evidence was reconciled against
> the D-3 implementation commit `82d69a1`; C1a reached author-complete implementation on 2026-07-14 and its scoped
> independent review remains pending. D-C1-1 through D-C1-4 remain owner-pending.
> Symbol names are authoritative where line numbers drift. This document authorizes no schema migration, production
> rollout, or live provider/model call.
> Implementation and validation are **provider simulate-only** until targeted gates and a scope-matched independent
> review return `GO`.

## 0. Decision Summary

The service-level goal is not merely to return from `POST /api/plan/submit` quickly. A plan compile must be a
durable, identity-scoped task that survives API/worker restart, has non-overlapping canonical owners for the shared
compile lifecycle and each consumer's public lifecycle, coalesces equivalent fresh submissions without duplicate
model cost, supports cancel/supersede without late publication, and gives the frontend a bounded terminal observation
path.

The next implementation order is:

1. **C1a, small transport/client correction**: make async submit/status reads use the light lane, terminate workflow
   polling for `cancelled`/`canceled`/`detached`/`superseded` or missing/unknown status, and consume the server-provided
   export `artifact.handle` fail-closed.
   This batch changes no storage and calls no provider/model.
2. **C1b, characterize and ratify**: pin the current plan response/side effects, approve the four
   storage/API/freshness decision cards in section 15, and add fast contract preflights before changing the writer.
3. **C1c-e, durable plan slice**: persist a consumer-generation-specific public task handle, register and drain
   `plan.compile.generate` on an isolated bounded worker lane, share only an immutable fresh compile checkpoint, and
   publish independently owned review/history state for each consumer generation. Cut over only after generic
   poll/cancel, ownership, freshness, repair, and rollback gates are green.
4. **C1f, deletion**: after durable cutover and recovery evidence, delete process-local hydration execution state;
   retain only explicitly measured compatibility projections until their deletion conditions mature.

`POST /api/jobs` is not a candidate first slice: its serving route is already deleted. Projection and CRM exports
already use durable commands. The active user-facing correctness hole is plan hydration.

## 1. Ground Truth And Deleted Assumptions

### 1.1 Current ground truth

| Surface | Current implementation | Service consequence |
|---|---|---|
| `POST /api/jobs` | Route deleted; `run_job` remains CLI/test-only | No remaining serving work in this route |
| `POST /api/plan` | Route deleted | No synchronous legacy plan route to migrate |
| `POST /api/plan/submit` | HTTP 200; persists history metadata, then starts `_queue_plan_hydration` | Fast response, but not a durable task contract |
| Plan execution | `_plan_hydration_inflight`, `_plan_hydration_signature_inflight`, lock, bounded semaphore, daemon thread | API restart loses ownership and execution |
| Plan observation | Frontend polls `/api/frontend-history/{history_id}` | A successful response that stays pending can poll forever |
| Workflow submit | HTTP 202 and durable job for the normal reviewed-plan UI path | Raw public payloads can still compile/rebuild a plan inline |
| Projection/CRM export | Durable `workflow_commands`, 202/poll/artifact | Core async execution is present |
| Legacy target export | Default 410; synchronous only under a migration-only environment flag | Not normal-path C1 work |
| Refine | Two compile endpoints and `POST /api/results/refine` remain synchronous | Three heavy public routes, currently no live frontend caller |
| Explain | `POST /api/workflows/explain` may call the model inline | Remaining public heavy-route debt |
| HTTP transport | `create_server` wraps the same FastAPI `create_app` | Parity means wire/route parity, not two independent adapters |

### 1.2 Assumptions this design deletes

1. **"Plan is complete because it is off the HTTP thread" is false.** The current thread is process-local and has no
   durable claim, lease, recovery, or terminal repair.
2. **"`/api/plan/submit` already returns 202" is false.** The handler currently returns 200 and top-level
   `status="pending"`.
3. **"History metadata is a task queue" is false.** `metadata.plan_generation` is a UI projection with no worker claim
   semantics; it cannot remain the lifecycle source of truth.
4. **"Current request-signature coalescing is durable" is false.** Coalesced history membership lives in an in-memory
   dictionary and disappears on restart.
5. **"The next low-risk C1 cut is `post_jobs`" is false.** The route is already absent and transport tests pin 404.
6. **"Three exports still need async conversion" is false.** The two canonical exports are durable; the third is a
   default-off migration bridge.
7. **"Only two refine-compile handlers remain" is incomplete.** `POST /api/results/refine` also compiles and then runs
   retrieval synchronously.
8. **"Async task status reads are protected from head-of-line blocking" is false.** Export submit and
   `/api/exports/{id}` status currently fall into the shared lane.
9. **"Frontend terminal mapping is total" is false.** Workflow `cancelled` and `superseded` are currently mapped to
   `running` and continue polling.

The main Track C plan and the earlier C1 design remain historical inputs, not the authority for these corrected facts.
In particular, the RATIFIED `TRACK_C_C1_HEAVY_OPS_DESIGN.md` predates the current route inventory and the distinction
between a public consumer task handle and an internal command id. Before any C1 durable cutover, that document must be
marked superseded for Plan or receive a scoped erratum pointing here. Its Plan claims that `task_id` is a command/job
id, succeeded results replay indefinitely by idempotency key, and cancel transitions the command are all replaced by
this consumer-handle/freshness/detach contract; its current Export contract is not revoked. The implementation batch
must also update the authority and module contract in `src/sourcing_agent/async_task_contract.py`: its public `task_id`
becomes an owner-supplied task handle, not an assumed command id, and unknown domain states fail closed. Leaving either older
authority unchanged blocks normal-path signoff even if tests are green.

## 2. Engineering Goal And Invariants

### 2.1 Goal

Convert plan generation into one typed durable command and remove its process-local execution model without creating
a second long-term task runtime.

### 2.2 Required invariants

1. The API request path performs validation, trusted identity injection, durable enqueue/attach, and a wake signal
   only. It never invokes plan/model compilation or starts a plan thread.
2. `workflow_commands` is the canonical shared compile/checkpoint lifecycle source. `plan_task_consumers` is the
   canonical per-handle publication lifecycle source. `async_task_contract` is only the consumer-first public
   projection of those two owners; neither history metadata nor the adapter owns lifecycle.
3. Normal causality is `WorkflowStarted/CommandPlanRequested event -> reducer -> workflow_command -> owner activity ->
   review/history projections`; code must not insert an uncaused command directly.
4. One semantic request within one trusted `(tenant_id, requester_id, compiler_contract_version)` scope invokes the
   model at most once in the uncontended success case for concurrent consumers and consumers attached to the same
   non-expired checkpoint. Completed checkpoints are never replayed outside the bounded freshness policy in section
   8.3.
5. A command never coalesces across requester or tenant boundaries. A client-supplied body identity is never trusted.
6. `history_id`, public `task_id`, internal `command_id`, `workflow_run_id`, and `review_id` are distinct identifiers
   and are never aliases. For Plan, one public task handle identifies exactly one consumer generation; many such
   handles may reference one internal compile command.
7. Cancelled, superseded, detached, stale-generation, or lease-lost work cannot publish a review session, criteria
   artifacts, a completed history plan, or a success transition.
8. A known task reaches one canonical terminal state. An unknown task/consumer/command state maps to terminal public
   `failed` with `error.reason="unknown_domain_status"`, emits report-visible diagnostics, and never falls back to
   `running`, success, publication, or retry.
9. Legacy queued/running history is repaired by a reusable offline/daemon flow, never by a public read endpoint.
10. The design works with the current combined deployment and remains valid after C3 separates API and worker.

### 2.3 Explicit non-goals

- No `agent_session`, `agent_turn`, Agent SSE, or Track D tool/loop work.
- No C3 process split, runtime outbox claim, new global advisory-lock mechanism, or LISTEN/NOTIFY. This does not waive
  the existing pool-safe transaction-lock contract: a contender must never hold the only pooled connection while
  waiting for an advisory lock.
- No C4 Pydantic/OpenAPI generation or SSE. C1 ships poll-first.
- No refine/explain async conversion and no raw `/api/workflows` plan-task chaining in this slice. These remain visible
  residuals; C1 must not be declared globally complete while they remain.
- No legacy target-export cutover and no object-storage migration.
- No Track B repository migration, jsonb/timestamptz conversion, or speculative storage facade expansion.
- No live provider/model, W6, nightly, or manual signoff before the independent review gate.

## 3. Target Execution Flow

```text
POST /api/plan/submit
  -> validate request and derive identity from request.state
  -> normalize request and compute semantic fingerprint/idempotency key
  -> append/reduce WorkflowStarted + CommandPlanRequested
  -> get the existing-or-new fresh plan.compile.generate command
  -> create/reuse one public task handle for the current history consumer generation
  -> persist pending history projection
  -> signal the isolated bounded plan-consumer lane
  -> 202 canonical async-task envelope

worker recovery tick
  -> registry computes free capacity for the isolated plan lane and returns without waiting
  -> claim at most that capacity and dispatch to the bounded plan executor
  -> verify at least one active, same-scope consumer
  -> create ActivityRun/Attempt
  -> compute plan envelope once with the configured model client (scripted/simulated for all C1 validation)
  -> checkpoint immutable compiled result + hash + freshness expiry
  -> re-read command lease/status, record compile ActivityRun/Attempt, and mark command succeeded

consumer publication drain on the isolated bounded Plan publisher lane
  -> claim bounded attached consumers whose command has a valid checkpoint
  -> re-read current generation, consumer status, publication lease, command, and checkpoint hash
  -> idempotently create that generation's mutable review + criteria set + history projection
  -> record EntityDelta/audit refs and CAS that consumer completed
  -> reclaim retryable publication failures without invoking the model

GET /api/tasks/{task_id}
  -> resolve the public task handle to exactly one consumer generation under trusted owner scope
  -> command-type mapper registry
  -> compose consumer-first state with its internal command; never expose or accept command_id as the Plan handle
  -> async_task_contract normalized envelope
  -> frontend stops on terminal; on success reads linked history/review

POST /api/tasks/{task_id}/cancel
  -> resolve the same owned consumer generation or return indistinguishable 404
  -> terminalize only that consumer; cancel the shared command only when no active consumers remain
  -> fence all late publication for the cancelled generation
```

The worker must query durable consumers before model invocation. This makes a partial submit that created a command
but failed to attach its first consumer cost-safe: the command performs no model call and is reconciled or terminalized.

## 4. Contract Owner / Source / Consumer Matrix

| Field or state | Owner | Source of truth and derivation | Allowed values | Normal consumers | Forbidden consumers | Fallback, deletion, fast preflight |
|---|---|---|---|---|---|---|
| Public `task_id` | plan task-consumer owner; projected by async-task adapter | Opaque handle stored on exactly one durable `(command_id, history_id, consumer_generation)` relation; generated server-side | Non-empty opaque id, distinct from every Plan `command_id` | submit response, generic poll/control, history compatibility projection, logs | Command repository, request body, history id, or route prefix must not derive it | No fallback; exact submit/poll/cancel/history cross-link preflight and mutation making it equal to `command_id` must fail |
| Internal `command_id` | durable runtime | `workflow_commands.command_id`; referenced by the consumer relation and never used as the Plan public handle | Non-empty internal id | plan compiler/publisher, recovery, audit | Plan public routes and frontend | Not present in normal Plan response; direct poll/cancel by command id returns 404 even to an otherwise authorized consumer |
| `task_type` | `CommandTypeSpec` registry | `workflow_commands.command_type` | Registered types only; C1 adds `plan.compile.generate` | mapper registry, frontend, audit | Route-name or string-prefix inference | Unknown type fails closed; registry/preflight totality test |
| `status` | `async_task_contract` | Plan mapper composes the consumer generation and referenced command, with consumer terminal state taking precedence | `queued`, `running`, `succeeded`, `failed`, `cancelled`, `expired` | frontend terminal logic, future SSE | Domain owners must not invent public aliases | Unknown/missing/impossible state maps to terminal `failed`; there is no fallback-to-running path; terminal-totality test |
| `domain_status` | plan task-consumer mapper | Stable consumer-first projection: active consumers reflect queued/running/publishing command work; terminal consumers reflect their own state | `queued`, `running`, `publishing`, `succeeded`, `failed_terminal`, `cancelled`, `detached`, `superseded`, `expired`, `unknown_domain_status` | audit, retry/resume/control diagnostics | Frontend must not rederive terminality independently; raw command status is internal | `unknown_domain_status` is terminal public `failed` with sanitized error and diagnostic containing internal ids only in operator logs |
| `idempotency_key` | plan compiler owner | Hash of trusted scope + canonical request + compiler contract version + server freshness generation | Non-empty, deterministic within one freshness generation, scope-namespaced | enqueue dedupe, fresh replay, audit | Client body cannot replace the server key or freshness generation | Collision with different fingerprint is 409; expired checkpoint cannot win ensure-command; mutation tests remove one input and bypass expiry |
| `request_fingerprint` | plan compiler owner | Hash of canonical semantic request before history-specific fields | Stable hash | collision guard, A/B evidence | UI display and permission checks | No heuristic fallback; stored with command payload/result and compared on replay |
| `compiler_contract_version` | plan compiler owner | Explicit version of request normalization + compile/output semantics | Registered non-empty version | idempotency derivation, replay guard, audit | UI and timestamp-based inference | Version bump required for semantic change; preflight pins key inputs |
| `history_id` | frontend-history owner | Client correlation id after validation or server-generated id | Non-empty sourcing id | history read, route state, task result link | Command id, workflow id, idempotency key | Preserved through migration; submit/poll/history cross-link preflight |
| `consumer_generation` / `plan_request_id` | plan task consumer owner | Monotonic/current token for one history submission; exact client retry reuses it, a new submission allocates another | Non-empty opaque token or monotonic generation | supersede CAS, late-result fence, per-generation publication key | Model compiler cannot infer from timestamps | Physical representation requires D-C1-1; stale publish mutation test |
| Consumer `status` | plan task consumer owner | Durable consumer relation transition | `attached`, `publishing`, `completed`, `failed`, `cancelled`, `detached`, `superseded` | public task mapper, publish fence, detach/cancel, repair | History metadata cannot schedule from it; command owner cannot overwrite terminal consumer state | No implicit timestamp transition; impossible consumer/command pairs fail closed; consumer-state preflight |
| `requester_id`, `tenant_id` | auth/API boundary | Server identity in request scope; copied to durable task-consumer scope | Auth-resolved values; explicit open-mode sentinel only | enqueue scope, read/cancel gate, idempotency namespace | Request body, history metadata fallback | Legacy unscoped rows quarantined; cross-user preflight returns 404 |
| `review_id` | plan review owner, linked by plan task-consumer owner | Idempotent mutable review publication for one consumer generation from an immutable compiled checkpoint | Positive integer after that consumer publishes; absent before | owned history, review UI, compile/refine, workflow start | Command result, another consumer, and task transport cannot manufacture/share it | D-C1-3 enforces one publication identity per consumer generation; exact handle replay reuses it, a different handle/generation gets a different review |
| `phase` | frontend-history/plan transport projection | Fixed plan-stage projection until workflow starts | `plan` for this task contract | Search route and history UI | Worker scheduling and command lifecycle | Preserved additively during bridge; endpoint parity test |
| `error` | plan task-consumer mapper | Consumer-specific terminal reason, otherwise sanitized command `last_error`; impossible/unknown states synthesize stable fail-closed reason | `null` or stable `{reason,message?,retryable?}` | task poll, UI | Raw exception/credentials | Legacy unrecoverable and `unknown_domain_status` get explicit non-retryable reasons; error-shape preflight |
| `compiled_checkpoint` freshness | plan compiler owner | Immutable checkpoint has `compiled_at`, `replay_expires_at`, compiler version, fingerprint, freshness generation, and content hash | DB-server timestamps plus non-empty hashes/version/generation | ensure-command, publisher, recovery, audit | Frontend, client timestamps, history metadata | Completed reuse only while unexpired under D-C1-4; exact existing task handle remains readable after expiry but new consumers refresh |
| `plan_task_lock_acquire_budget_ms` | Plan registry/runtime owner | Checked-in monotonic try-lock budget; initial 250 ms, upward change beyond 500 ms requires owner review | Positive integer <= 500 by normal contract | attach/publish/cancel/refresh repositories, service metrics | Request bodies, clients, or individual callers cannot override it | Exhaustion is typed `plan_task_lock_busy`, never an infinite loop; config/preflight and permanent-holder PG test |
| `artifact` | result owner via async-task adapter | Owner-produced reference after success | `null` or canonical artifact object | export download client; optional for plan | Client URL construction | Plan may use history/review refs instead; export alias parity test |
| `plan_generation` | frontend-history projection owner | Derived from command + consumer state during bridge | Legacy `pending/queued/running/completed/failed/cancelled` only | old history recovery UI | Worker scheduling, retry, permission, cost decisions | Transitional; delete independent lifecycle meaning after section 13 conditions |
| `links` | async-task transport adapter | Route builders from `task_id` and authorized resource ids | Known relative API paths | frontend poll/cancel/history navigation | Domain owners must not persist URL strings as truth | Generated, never stored; route inventory preflight |

No field enters the normal public contract until this matrix and its fast preflight are updated in the implementation
diff. Similar names do not permit derivation across rows.

## 5. `plan.compile.generate` Command Contract

### 5.1 Registry proposal

| Property | Proposed contract |
|---|---|
| Command type | `plan.compile.generate` |
| Owner | `plan_compiler` |
| Stage | `plan_compile` |
| Readiness effect | `plan_compiled` |
| Display category | `planning` |
| Activity spine | ActivityRun + ActivityAttempt + EntityDelta required |
| Running cancel | Cooperative cancellation plus late-result quarantine |
| Resume | Requeue only after expired/lost lease or an explicit retryable failure; never resume a live lease |
| Provider-after-start policy | Poll/cancel/late-result quarantine semantics; model credentials never enter payload/result |

The implementation must add the spec, its cancel/resume dispatch entries, manifest/golden coverage, and one recovery
binding. It must not add another manual command-type table.

### 5.2 Durable identity and causality

- Derive a stable opaque workflow/operation identity from trusted scope, semantic request fingerprint, compiler
  contract version, and server-allocated freshness generation. The exact prefix/hash length is an implementation
  detail; collision behavior is contractual.
- Append `WorkflowStarted` and `CommandPlanRequested` through `DurableRuntimeWriter.append_event_and_reduce` with
  deterministic event idempotency keys. Do not insert `workflow_commands` directly.
- `CommandPlanRequested.payload` names the registered command type, canonical idempotency key, bounded retry policy,
  and a payload containing only normalized plan input, trusted scope, fingerprint, and compiler contract version.
- `history_id` and consumer generation do not participate in the semantic command key. They live in the consumer
  relation so multiple histories can share one compile.
- Exact duplicate event/command planning within the active freshness generation returns the winning command. The
  submit path then idempotently creates or reuses the requested consumer task handle. An exact retry of an existing
  handle never silently refreshes or moves it to another command.
- If the same idempotency key resolves to a different fingerprint, tenant, requester, planning mode, or compiler
  version, fail with 409 and record a collision metric. Never reuse the old result.
- A new consumer may attach to completed work only while its checkpoint is unexpired. Once expired, ensure-command
  atomically advances the server freshness generation and creates a new command; the old command and existing task
  handles remain immutable/readable.

### 5.3 Payload and result

Command payload, logically:

```json
{
  "request": "<canonical normalized plan request>",
  "request_fingerprint": "<hash>",
  "planning_mode": "model_assisted",
  "compiler_contract_version": "<explicit version>",
  "freshness_generation": "<server-allocated monotonic generation>",
  "requester_id": "<trusted scope>",
  "tenant_id": "<trusted scope>"
}
```

Command result, logically:

```json
{
  "checkpoint": {
    "compiled_envelope_ref": "<durable checkpoint or inline bounded result>",
    "compiled_envelope_hash": "<hash>",
    "compiler_contract_version": "<explicit version>",
    "compiled_at": "<DB server timestamp>",
    "replay_expires_at": "<DB server timestamp>"
  },
  "model_invocation_count": "<bounded integer>"
}
```

Whether the bounded compiled envelope is stored directly in `result_json` or through an artifact reference must be
decided from measured payload size and current row limits. It must be durable before consumer publication so lease
reclaim can resume without a second model call. It must not contain secrets or provider credentials. The command
result deliberately contains no `review_id`, criteria-artifact ids, editable review state, or `history_id`: those are
consumer-generation-owned mutable publications and sharing them would cross ownership and supersede fences.
Consumer counts and late-publication counters are derived from the consumer repository/metrics; they are not mutable
fields inside the hashed checkpoint.

### 5.4 Compute/publish split

The current `plan_workflow` combines computation with three side effects: review creation, frontend-history publish,
and criteria-artifact persistence. The command owner must split that behavior:

1. **Compute**: produce the explained/compiled envelope without publishing review, criteria, or history rows.
2. **Checkpoint**: durably store the compiled result and hash while the command lease is current.
3. **Compile terminalize**: re-read command status/lease, record compile activity evidence, and mark the command
   succeeded. That means only that the immutable checkpoint exists; it is not public task success.
4. **Consumer claim/fence**: the durable publication drain separately claims one attached generation, then re-reads
   its status/current-generation CAS, publication lease, referenced command, and checkpoint hash.
5. **Publish/terminalize consumer**: use D-C1-3 publication identity to create/reuse a distinct review and criteria set,
   CAS-publish that generation's history, record effect evidence, and only then mark its public task completed.

A cancellation or command-lease loss before step 3 may retain a private compile checkpoint for audit, but must not
create user-visible review, criteria, or history state. Publication lease loss or consumer cancellation before step 5
blocks that generation's public writes. A later consumer may reuse only an unexpired completed checkpoint and still
receives its own mutable publications. Calling the current side-effecting `plan_workflow` wholesale and checking
cancellation only afterward is explicitly rejected.

Step 5 must be visibility-atomic. The approved D-C1-3 implementation either writes review/criteria/history linkage in
one PG unit of work under the consumer CAS, or stages rows as non-public and activates all links with one final CAS.
Independent public inserts followed by a consumer-status update are not acceptable: a cancel/crash between them would
leak a review that the task contract says was never published. Staged rows left by a lost lease are quarantined and
reused or retired only by the exact publication key; list/read/compile/start cannot see them before activation.

The repository lock order is command/freshness generation -> consumer generation -> publication identity ->
review/criteria/history activation; every attach, publish, cancel, and refresh UoW uses that same order. Pooled
advisory-lock contention uses `pg_try_advisory_xact_lock`: on busy, rollback and return the
connection before capped backoff; on success, the same connection retains every acquired lock through commit or
rollback. A blocking lock wait while retaining a pooled connection is forbidden, including with pool max greater
than one, because it recreates the pool-max=1 circular wait under saturation.

Try-lock retry is also time-bounded. The Plan registry owns `plan_task_lock_acquire_budget_ms`, initially **250 ms**
and never above the 500 ms submit-latency gate without owner review. Every attach, publish, cancel, and freshness
refresh receives one monotonic deadline; backoff is clipped to the remaining budget. Budget exhaustion rolls back,
returns the connection, and emits `plan_task_lock_busy_total{operation}` plus wait duration. Public attach/cancel
returns retryable HTTP 503 `plan_task_lock_busy` with no partial mutation; background publish/refresh returns a typed
retryable outcome and relinquishes its executor slot. If claim and lock acquisition share one transaction, rollback
also removes the uncommitted claim. If the publication claim was already durably committed, the busy path must not
mutate status/lease without the canonical locks; it leaves the lease intact for expiry/recovery reclaim and records
the busy outcome in process/service metrics. It never loops inside one API/worker call or performs an unlocked
"release claim" write.

## 6. Durable Consumer And History Model

### 6.1 Logical consumer record

The physical schema is deliberately not approved here. Whichever option D-C1-1 selects must expose this logical
contract through one repository:

| Logical field | Purpose |
|---|---|
| `task_id` | Unique opaque public handle for this consumer generation; never equals `command_id` |
| `command_id` | Internal FK/logical reference to the shared immutable compile command |
| `history_id` | UI/read-model correlation |
| `consumer_generation` / `plan_request_id` | Current-writer fence for the history |
| `requester_id`, `tenant_id` | Trusted ownership and coalescing boundary |
| `status` | `attached`, `publishing`, `completed`, `failed`, `cancelled`, `detached`, or `superseded` |
| `request_fingerprint` | Confirms attachment semantic equality |
| `attached_at`, `updated_at`, `detached_at` | Audit and repair ordering; not lifecycle derivation inputs |
| `review_id` | Published result pointer after success |
| `criteria_artifact_refs` | This generation's mutable-publication artifact ids, never command-shared |
| `publication_key` | Stable unique key from task handle + generation + checkpoint hash for crash-safe publication |
| `publish_lease_owner`, `publish_lease_expires_at`, `publish_attempt` | Durable per-consumer publication claim/reclaim; no in-memory-only publisher ownership |
| `error_reason` | Consumer-specific terminal reason |

Required uniqueness/CAS semantics:

- At most one current active generation per `history_id` and trusted owner scope.
- `task_id` is globally unique, indexed with trusted scope, and has a constraint proving `task_id <> command_id` for
  Plan. Exact `(task_id, history_id, generation)` replay is idempotent.
- Many consumer handles may attach to one command only when identity scope and request fingerprint match.
- Many consumer handles may reference one command, but no two handles share `review_id`, criteria publication rows, or
  mutable history state. Exact retry of the same handle reuses those publications.
- A newer generation supersedes the older generation atomically. Supersede/detach/cancel, active-consumer recount,
  and a last-consumer command-cancel request execute in one repository UoW/CAS under the published lock order.
  Concurrent attach must either serialize before the recount and keep the command active, or serialize after the
  committed cancellation and allocate/join an eligible command; it cannot attach to a command cancelled from a stale
  zero-consumer observation. A repairable partial state is not an acceptable normal outcome.
- Owner reads by task and owner scope must be indexed; JSON metadata scans are forbidden.

### 6.2 History role after cutover

`frontend_history_links` remains the UI recovery/read model for query text, plan, review id, job id, phase, and
presentation metadata. It does not own task status, retries, leases, idempotency, or permission.

During the compatibility window, `metadata.plan_generation` mirrors the command/consumer projection and includes the
task id. Readers may display it, but scheduling and terminal decisions use the generic task endpoint. After the
deletion conditions in section 13 are met, history no longer needs an independent `plan_generation.status` contract.

### 6.3 Partial submit convergence

Because current durable event reduction and history writes are not proven to share one transaction, the first slice
must handle partial writes explicitly rather than pretending atomicity:

1. A command with zero active consumers is never allowed to invoke the model.
2. The API returns 202 only after command, consumer, and pending history projection are all readable.
3. If attach/history persistence fails, return a stable 5xx reason and leave a repairable command; the drain moves it
   to a bounded retry/no-consumer state without model work.
4. A reconciliation pass identifies commands with zero consumers, consumers with missing commands, and pending
   histories with missing consumers. It repairs exact matches and terminalizes ambiguity fail-closed.
5. Do not add a timing sleep or request-path scan as a substitute for a transactional/reconciliation contract.

### 6.4 Ownership and anti-enumeration matrix

All checks use trusted request identity and the durable consumer relation. A body/query `requester_id`, `history_id`,
`review_id`, or internal command id is a selector only, never authority. Missing and wrong-owner resources use the
same 404 body; authorization cannot be inferred from timing or a different status code.

| Surface | Required durable relation | Authorized behavior | Missing/wrong owner | Owned but stale/non-current generation |
|---|---|---|---|---|
| Plan/history list | History row joined to a consumer in the trusted scope | Return only owned rows; never list orphan/internal commands | Omit row | Include historical terminal row only when product history policy allows it; never present it as current |
| Task/history/review read | Exact task handle or history/review id resolves through the same owned consumer generation | Return that generation's projection; task poll may return its terminal superseded/cancelled state | 404 | Read-only historical result is allowed; links must not point to a newer generation as though it were this task |
| Review edit/approve | Review id equals the per-generation `review_id`, consumer is owned/current, and publication is complete | Apply mutation with consumer-generation CAS and audit identity | 404 | 409 `stale_plan_generation`; no mutation |
| Review compile/refine | Same owned current review link plus permitted review state | Compile against that review only; resulting artifact remains linked to the same generation | 404 | 409 `stale_plan_generation`; no model call or artifact write |
| Workflow start | Approved review and history both resolve to the same owned current consumer generation | Append the workflow-start event with task/history/review causality | 404 | 409 `stale_plan_generation`; no workflow/event/command write |
| Task cancel | Exact owned public task handle | Apply section 7.2 to that consumer only | 404, including an internal `command_id` used as the path id | Terminal stale handles are idempotently observed; they cannot cancel the current generation |

List/read/review/compile/start tests must cover same-tenant different-requester, different-tenant, guessed command id,
orphan history/review, and a superseded generation. Open mode uses its one explicit trusted sentinel; it does not skip
the relation checks.

## 7. HTTP 202 And Generic Task Poll

### 7.1 Submit

For a new or in-flight exact submission:

```http
POST /api/plan/submit
HTTP/1.1 202 Accepted
```

```json
{
  "task_id": "ptask_...",
  "task_type": "plan.compile.generate",
  "status": "queued",
  "domain_status": "queued",
  "idempotency_key": "plan.compile.generate:...",
  "history_id": "history-...",
  "phase": "plan",
  "links": {
    "status": "/api/tasks/ptask_...",
    "cancel": "/api/tasks/ptask_.../cancel",
    "history": "/api/frontend-history/history-..."
  }
}
```

The example `task_id` is a public consumer handle such as `ptask_...`, never the internal `cmd_...`. An exact retry
that supplies the same accepted `plan_request_id` returns the same handle and may return HTTP 200 with its current
terminal state. A new consumer submission receives a new handle even when it reuses an unexpired checkpoint. Invalid
input is 400. A semantic idempotency collision or stale history-generation conflict is 409. Missing or unauthorized
resources return the same 404 shape to prevent enumeration. Exhausting the server-owned lock-acquisition budget
returns retryable 503 `plan_task_lock_busy` with no partial task/history mutation.

### 7.2 Generic poll and control

`GET /api/tasks/{task_id}` first resolves an owner-registered public task handle, then returns the
`async_task_status` fields plus task-type-owned domain references. The endpoint must not look up a Plan command by the
path id, infer an owner from an id/command-type prefix, or expose arbitrary internal workflow commands.

For Plan, mapper precedence is exact: a terminal consumer wins over command state; `attached + queued/retry_wait`
maps queued; `attached + claimed/running` maps running; `attached|publishing + succeeded checkpoint` maps running with
`domain_status="publishing"`; only consumer `completed` maps succeeded. Consumer failed maps failed and
cancelled/detached/superseded maps cancelled. An active consumer whose command is `failed_terminal` maps failed with
the sanitized command error, and one whose command is cancelled maps cancelled; reconciliation durably copies that
terminal state to the consumer without waiting for another client poll. Any missing command, unknown value, or
impossible pair maps terminal failed with `unknown_domain_status`; it is never guessed as running. Success includes
the consumer's own `history_id` and `review_id`. `artifact` may be null because the result is a JSON resource, not a
download.

`links.history` is emitted only when that endpoint can prove it will return this exact generation's projection. A
superseded/detached handle may retain `history_id` as audit correlation, but omits the unversioned history link rather
than pointing at the newer generation. Historical `review_id` may remain readable under section 6.4, but its mutation,
compile, and workflow-start controls are absent.

For exports, the same generic adapter returns the existing artifact object. Existing export `task_id == command_id`
remains an owner-specific compatibility fact, not a generic contract assumption; `GET /api/exports/{id}` remains an
alias to the same mapper until C4 client generation is complete.

`POST /api/tasks/{task_id}/cancel` has these exact Plan semantics:

- Resolve the public handle under trusted scope. Missing, wrong-owner, and an internal command id all return the same
  404.
- `attached` or `publishing` CAS-transitions only this consumer to `cancelled` and immediately makes its public task
  terminal. In the same repository UoW it re-counts locked active consumers and, only if this was the last one,
  requests queued cancellation or cooperative running cancellation of the command; otherwise the shared command
  continues. Attach uses the same lock order and cannot race the zero-consumer decision. No other consumer changes.
- Repeating cancel on `cancelled`, `detached`, or `superseded` is idempotent HTTP 200 with canonical cancelled state.
- `completed`, `failed`, or expired task retention returns 409 `task_not_cancellable` without mutation.
- A cancellation accepted during provider work may not stop provider cost, but its generation fence blocks every
  later review, criteria, history, activity-effect, and success publication for that consumer.

Internal `/api/workflow/commands/{id}/...` control remains an operator surface and is not the frontend contract.

### 7.3 Compatibility sequence

1. Deploy frontend parsing that accepts `pending|queued`, stores the opaque `task_id` without interpreting it as a
   command id, understands canonical and unknown-terminal states, and can fall back to history poll only while the
   server omits a task handle.
2. Deploy the generic task endpoint and durable owner dark, with no live submit route.
3. Cut `/api/plan/submit` to 202 durable mode. One request must select exactly one execution path; there is no shadow
   model call and no legacy-thread fallback.
4. Preserve current domain fields (`history_id`, `phase`, request preview, empty plan/review containers, intent rewrite,
   and `metadata.plan_generation`) only for the explicitly approved bridge duration.
5. Remove the bridge when section 13 conditions are met. Full OpenAPI/client generation remains C4.

The exact top-level-vs-nested transition is D-C1-2. Any bridge must be report-visible, preflighted, and time-bounded.

## 8. State, Idempotency, Cancellation, And Recovery

### 8.1 State projection

| Command domain state | Public task state | Worker/client behavior |
|---|---|---|
| `queued`, `retry_wait` | `queued` | Eligible later; client polls with bounded backoff |
| `claimed`, `running` with active consumer | `running` | Lease/heartbeat visible; cancel remains cooperative |
| Command `succeeded`, consumer `attached`/`publishing` | `running` | Per-consumer review/history publication is pending; never report premature success |
| Consumer `completed` | `succeeded` | No rerun for this handle; return its own result refs |
| `failed_terminal` | `failed` | Expose sanitized error; explicit retry may create/requeue per owner contract |
| `cancelled`, `canceled`, `detached`, `superseded` | `cancelled` | Terminal; frontend stops; no publish |
| Expired task policy | `expired` | Terminal transport projection where retention has elapsed |
| Unknown/missing/impossible consumer-command pair | `failed` | Terminal `unknown_domain_status`; alert/report; no publish, retry, or success inference |

`retry_wait -> queued` is an intentional explicit mapping. The current generic fallback that maps unrecognized
statuses to `running` must be deleted from the Plan mapper and from the shared `async_task_contract` authority. A
mutation restoring that fallback must fail both backend and frontend terminal-totality tests.

Frontend workflow progress must separately map raw workflow `cancelled`, `canceled`, `detached`, and `superseded` to
a terminal UI state in C1a. Missing or unknown status maps terminal failed. None may silently map to `running`.

### 8.2 Idempotency and fanout

- Canonicalize list ordering, defaults, recall limits, planning mode, and compiler contract version before hashing.
- Exclude `history_id`, consumer generation, timestamps, UI-only metadata, and request ids from the semantic fingerprint.
- Include trusted tenant and requester in the idempotency namespace. Cross-user fanout is prohibited in C1.
- Concurrent identical submitters within the same server freshness generation converge on one command and
  independently attach consumer handles.
- An exact retry of an existing `plan_request_id` reuses its existing handle and publication. A new consumer may reuse
  an already-completed command only while the immutable checkpoint passes section 8.3 freshness checks, and still
  receives a distinct review/history publication.
- A new submission for an existing history creates a new generation and supersedes the old consumer. A late older
  result fails the generation CAS and increments `late_publish_blocked_count`.
- Losing bounded coalescing and silently running one command per concurrently attached history is a regression even
  if functional output matches. Reusing a completed checkpoint indefinitely is also a regression.

### 8.3 Bounded replay freshness

C1 deliberately chooses **bounded TTL plus automatic refresh**, not an invented planning-input watermark. The current
planner has no single authoritative revision spanning every mutable prompt/configuration/context dependency; hashing
only the fields currently visible to the route would create a false freshness guarantee. A bounded TTL gives a
measurable maximum reuse window now. A future explicit watermark may replace it only when every planning input owner
contributes a revision and a contract preflight proves totality.

- The plan registry owns `completed_checkpoint_reuse_ttl_seconds`. D-C1-4 recommends an initial maximum of 300 seconds.
  Lowering it, including to zero, is safe; increasing it is a reviewed contract change.
- The repository allocates a monotonic `freshness_generation` under DB CAS. `idempotency_key` includes that generation;
  timestamps are eligibility inputs, never identity or a CAS fence.
- A checkpoint records DB-clock `compiled_at` and `replay_expires_at = compiled_at + TTL`. A new consumer can reuse a
  **completed** command only before that instant and only when scope, fingerprint, compiler version, freshness
  generation, and checkpoint hash contract match. Existing handles remain valid and may finish/read after expiry.
- Concurrent consumers may join the same still-active command generation before a checkpoint exists; its input is the
  immutable canonical request already captured by that generation. A terminal failed/cancelled command is never a
  replay candidate.
- For a **new** consumer submission, a current generation whose command is terminal cancelled/failed, or completed
  without a valid reusable checkpoint, is an explicit refresh loser: under the same command/freshness lock the
  repository atomically advances `freshness_generation` and creates/joins the successor command. Exact retry of an
  existing public handle remains bound to its old terminal command and never advances or rebinds it. Concurrent
  post-cancel submitters converge on one successor generation.
- On or after expiry, ensure-command atomically advances freshness generation and creates/joins the new command. It
  never mutates the old checkpoint or moves an existing handle. Concurrent refresh submitters converge through the
  new generation CAS.
- If expiry, DB time, version, fingerprint, or checkpoint integrity cannot be proved, fail closed to **no reuse** and
  create/join a new freshness generation. This may spend another simulated/model call after cutover but cannot serve
  silently stale planning output.
- Metrics distinguish `inflight_coalesced`, `checkpoint_replayed_fresh`, `checkpoint_refresh_expired`,
  `checkpoint_refresh_terminal`, `checkpoint_refresh_forced_by_validation`, and exact-handle retry. Tests use an
  injected DB clock; sleeps and client timestamps are forbidden.

The timeout default if D-C1-4 is not approved is stricter: in-flight consumers already attached may converge, but a
new consumer never reuses a completed checkpoint. That default permits dark tests but blocks public cost/SLO signoff.

### 8.4 Cancel, detach, and late results

- **Queued**: cancel/detach the addressed consumer first; transition the command to cancelled only when no active
  consumers remain, otherwise keep the shared command alive.
- **Running before model call**: active-consumer and command-status checks prevent invocation.
- **Running during model call**: request cancellation through the client if supported. Regardless, re-check command
  status, lease ownership, and consumer generation after return. A late result is quarantined from all public writes.
- **Delete history**: detach that consumer. It must not delete a shared command or allow the worker to recreate the
  deleted history row.
- **Supersede same history**: mark the prior generation superseded; it cannot publish even if its command succeeds for
  another consumer.
- **Publish fence**: review creation, criteria persistence, history update, activity effect, and success transition
  each carry or verify the current command/consumer identity. Timestamp ordering is not a fence.

### 8.5 Isolated bounded consumer, lease, and retry

- Plan model execution runs on a dedicated `plan_compile` worker lane with its own fixed concurrency and bounded
  admission capacity. Per-consumer publication uses a separate bounded `plan_publish` batch/worker lane with no model
  calls, so full model slots cannot delay an already-compiled consumer. Neither lane occupies the API shared/light
  lanes or executes serially inside the registry recovery tick.
- Each recovery tick reads free Plan capacity, reserves at most that many slots, claims no more than the reservation,
  dispatches the claims, and returns without waiting for model completion. When capacity is zero it claims zero Plan
  commands and continues other owner drains. There is no unbounded executor queue.
- Dispatch failure after claim immediately records a stable retryable reason and releases/requeues that lease; a claim
  must not wait unseen in a process-local queue. Process-local slots are performance controls only: command lease,
  checkpoint, consumer state, and recovery remain the correctness owners after restart.
- The publisher claims only as many generations as its bounded capacity, reclaims expired publication leases, and
  terminalizes active consumers when their command is failed/cancelled. It never reinvokes the model or treats a
  command-success checkpoint as consumer success before its own publication CAS.
- Saturation tests hold every Plan slot with deterministic delayed fixtures while export/manual/recovery drains and
  task status remain responsive, then prove queued Plan work advances fairly when capacity returns.

- Claim uses the workflow command lease and a unique owner identity. Reclaim of expired `claimed/running` Plan work is
  opt-in for this owner and covered by the same tests that protect export reclaim.
- The lease duration must exceed the configured model timeout plus checkpoint margin, or the model adapter must
  heartbeat during the call. A second owner cannot publish with an expired/stale lease.
- Transient model/backend failures enter bounded `retry_wait` with explicit backoff and attempt count. Schema-invalid,
  permission, collision, or unrecoverable input failures are terminal.
- A persisted compiled checkpoint is reused after reclaim. Retrying publication for the same consumer generation must
  not invoke the model again or create a second review/criteria artifact set; another consumer generation creates its
  own publication set from the same fresh checkpoint.
- Exhausted **compile** attempts mark the command failed and reconciliation marks each still-active consumer failed
  with the sanitized compile reason. Exhausted **publication** attempts mark only that consumer failed with a stable
  publication reason; the successful checkpoint, command, and other consumers remain unchanged.

### 8.6 Legacy pending repair

The repair flow is a reusable operator/daemon entrypoint with dry-run and idempotent apply. It never compiles inline.

Inventory rows where history has no plan/review/job and `plan_generation.status` is `pending`, `queued`, or `running`.
Classify each row:

| Class | Action |
|---|---|
| Already materialized plan/review | Reconcile projection to completed; do not enqueue |
| Scoped, complete, recoverable request | Derive canonical fingerprint, ensure command, attach consumer, signal worker |
| Superseded by a newer history generation | Persist superseded terminal projection; do not enqueue |
| Missing/invalid request | Persist `legacy_plan_request_unrecoverable` |
| Missing trusted owner under authenticated mode | Quarantine as `legacy_plan_owner_unresolved`; never trust body identity |
| Command/consumer mismatch | Fail closed, report both ids, require operator resolution |

The report includes scanned/repaired/quarantined/failed counts, task ids, history ids, reason codes, fallback-used=false,
and provider/model invocation count=0. Read endpoints may expose this state but must not run the repair.

## 9. Implementation Batches

### C1a: Light lane, terminal mapping, artifact handle

Scope:

- Classify exactly the projection/CRM export submit methods and `GET /api/exports/{id}` status poll as light. Keep
  binary artifact download shared; near-miss methods/paths must not acquire the light lane.
- Add the future `/api/tasks/{id}` status and cancel patterns to the light-lane contract when the route lands.
- Extend frontend workflow terminal handling so cancelled/canceled/detached/superseded jobs stop polling as cancelled,
  while missing/unknown status stops as failed; neither class may map to running.
- Make the export client accept `artifact.handle` only when it exactly equals the server contract route
  `/api/exports/${encodeURIComponent(task_id)}/artifact`. It must be a root-relative path with no scheme, authority,
  query, fragment, dot segment, encoded path separator, or different task id. `succeeded` with a missing or non-exact
  handle fails closed; the client never constructs a fallback URL or fetches the supplied value.
- Preserve `task_id` outside the helper so reload/resume work can be added without changing submit semantics.
- Make the Plan submit client tolerate the current top-level `pending` and future `queued` states without changing the
  server's current HTTP 200 path; the top-level 202 cutover remains C1e and D-C1-2-gated.

Fast tests include an exact route-method classifier table, near-miss routes, shared-lane saturation with export submit
and poll still responsive, binary download remaining shared, frontend valid/missing/absolute/protocol-relative/
wrong-id/query/fragment/dot-segment/encoded-separator handle cases, terminal-total workflow status mapping, and
`pending|queued` Plan submit parsing. Exit: no schema/code outside API/frontend/tests; no provider/model; targeted
independent review because public status and download semantics change.

#### C1a author batch record (2026-07-14; independent review pending)

- Backend lane classification is exact: the two export submits and single-segment export status poll are light;
  binary download, wrong methods, empty/trailing/extra segments remain shared. A deterministic saturation test holds
  the shared slot, proves submit/poll use the reserved lane, and proves `/artifact` waits for shared capacity.
  The pre-existing all-route CORS `OPTIONS` preflight remains an explicit light-lane transport exception.
- Export transport keeps `task_id` in each public wrapper across separate submit/wait/download calls. The client rejects
  missing, absolute, protocol-relative, wrong-id, query, fragment, dot-segment, encoded-separator, percent-alias, and
  double-decoding task/handle shapes before any poll or binary fetch.
- `workflowStatus.ts` is the only frontend workflow-status registry. Cancel aliases are terminal cancelled;
  missing/unknown are terminal failed. Timeline, history recovery, Excel launch, dashboard caching, and launch-reuse
  consumers use the same semantics. The intentional completed-with-active-background-work projection stays running.
- Initial and revision Plan clients accept top-level `pending|queued`; server HTTP `200` remains unchanged.
- Scope contains no schema, durable owner, provider, model, live environment, or credential change. The existing Python
  `async_task_contract.py` unknown-to-running behavior is explicitly deferred to C1b and must not be treated as closed.
- Targeted evidence before review: C1a transport/frontend tests `11 passed + 17 subtests`; adjacent frontend contract
  tests `26 passed`; combined targeted regression `38 passed + 17 subtests`; existing two-lane middleware test passed;
  the exact legacy pipeline classifier node passed after starting the project-local PG; frontend production build and
  `make lint` passed. The mypy ratchet stayed at `81 errors / 4 files`; the contract lane passed
  `349+2+11+1+2` plus `dry_run_ready`. A formal `GO` must come from the repository review gate, not this author record.

#### C1b author batch record (2026-07-14; independent review pending)

- `plan_submit_contract.py` is the single compatibility authority for current HTTP `200` + top-level `pending`,
  `plan_generation` states, compiler contract version, and the semantic coalescing signature. History identity,
  request ids, idempotency fields, and transport timestamps remain excluded from that signature; trusted scope and
  semantic request fields remain included. List order is intentionally preserved in this characterization slice.
- The serving chain has one owner: `submit_plan_workflow -> _queue_plan_hydration ->
  Thread(target=_run_plan_hydration) -> plan_workflow`. The API no longer calls synchronous `plan_workflow` when the
  submit owner is missing; it fails closed as retryable HTTP `503 plan_submit_owner_unavailable` with
  `fallback_used=false`. The CLI `plan` call remains explicitly classified as a non-serving one-shot helper.
- A repository-wide AST/source ratchet pins exactly one hydration queue caller, exactly one hydration thread creator,
  one serving compile caller, and the one classified CLI compile caller. At C1e the same guard is tightened to zero
  legacy serving thread/compile callers after cutover.
- Current history `queued -> running -> completed|failed`, same-signature coalescing, same-history current-request
  selection, review creation, and criteria version/compiler-run side effects are characterized rather than migrated.
  The shared async-task adapter now treats missing/unknown states as terminal failed with stable reasons, preserves
  explicit retry/publish/cancel mappings, and treats public task/artifact handles as owner-supplied values.
- Scope contains no schema, durable consumer/repository, provider/model invocation, live environment, HTTP 202 switch,
  TTL, replay, or compute/publication split. D-C1-1..4 remain required before C1c-e. The post-change targeted contract
  lane passed `19`, history recovery passed `28`, adjacent export/transport and CRM boundary lanes passed `15` and
  `34`, `make lint` passed, and mypy remained at its `81 errors / 4 files` ratchet. The full fast contract lane passed
  `349+2+11+1+2` plus `dry_run_ready`; this author record is not a formal `GO`.

#### C1b pinned advisory fixed-forward record (2026-07-14; formal gate still pending)

- The legacy in-process owner now uses one re-entrant lock and an owner token for submit publication, current-generation
  publication, late-consumer drain, and retirement. A late same-signature submit before the retirement barrier joins
  the current compiled result; one arriving after retirement creates a successor. The regression injects the submit
  inside terminal publication, the former snapshot/cleanup gap.
- Same-history Plan history publication is now guarded by the same lock and the compiler is invoked without a
  `history_id`, so an obsolete generation cannot overwrite the successor history row through `plan_workflow`.
  This is not full closure: a deterministic real-compiler race proves the obsolete generation can still create an
  orphan review session and criteria versions before the post-call fence. D-C1-3/C1d must split pure compute from
  owner-gated publication; no durable/live/manual/product signoff may treat this C1b fence as that split.
- The AST ratchet now follows callable aliases and `partial`/`getattr`, asserts there are no direct hydration-run
  callers, and inspects thread plus executor targets. Mutation fixtures prove aliased direct compile/hydration,
  aliased hydration thread, `submit`, `asyncio.to_thread`, `run_in_executor`, and pool `starmap` bypasses are detected.
- Authenticated unlinked Plan rows carry API-authored requester/tenant provenance. First-create history IDs are always
  server-generated; an authenticated caller-provided absent `history_id` is rejected rather than claimed, which closes
  the cross-process check/claim gap without inventing a pre-C1c durable CAS row. Within one API process, owner recheck,
  queued projection, and registration also share the hydration lock. The frontend omits its provisional local ID on
  initial submit, atomically rekeys the local snapshot and route to the returned server ID, and sends that server ID
  only for later revision. Submit, exact read, list, and resubmit use one
  fail-closed matrix: missing,
  partial, mismatched, or body-authored ownership is 404/excluded. Non-Plan legacy rows retain read compatibility but
  never gain Plan replacement authority; open mode retains its explicit compatibility behavior. This bridge adds no
  schema and is deleted after durable consumer identity and legacy-owner repair are authoritative.
- Empty owner-supplied artifact handles are rejected at construction and malformed succeeded adapter payloads omit
  the artifact. The current Plan bridge remains HTTP `200`/`pending`; only C1e may switch it to durable HTTP `202`.
- Deterministic tests use event/barrier or publication injection, not timing sleeps: late-consumer ordering is tested
  inside terminal publication, and a compiler-owner exception terminalizes every coalesced consumer as `failed` and
  clears both process-local registries rather than leaving silent `pending` work. A thread-start failure now retires
  the registered owner, best-effort terminalizes its queued projection, and returns retryable `503` instead of leaving
  later consumers attached to a dead owner. A permanently unavailable history store can still prevent that terminal
  projection; this process-local bridge cannot prove durable terminalization and remains blocked on the C1c owner.
  Fixed-forward evidence before this follow-up: contract/identity `60 passed`, history recovery `32 passed`,
  export/transport `15 passed`, CRM `26 passed + 4 subtests`, frontend contracts `13 passed`, frontend production
  build and lint green, mypy unchanged at `81 errors / 4 files`, and `349+2+11+1+2` + `dry_run_ready` green.

### C1b: Characterize and contract preflight

- Pin current HTTP 200/pending shape, history metadata transitions, plan/review/criteria side effects, same-signature
  coalescing, same-history supersede behavior, and current identity behavior.
- Add a two-stage source/AST ratchet. In C1b, pin exactly one legacy hydration-thread owner reachable only from the
  current plan-submit path and forbid any new direct `plan_workflow`/model-compile/thread caller. Do not claim zero
  callers while C1d intentionally leaves legacy submit active. In C1e, after durable cutover and synchronous fallback
  deletion, tighten the same guard to zero public submit paths that compile or start a thread.
- Ratify D-C1-1 through D-C1-4 before migration or high-risk field work.
- Define the compiler contract version and canonical request normalizer in one module.
- Amend `TRACK_C_C1_HEAVY_OPS_DESIGN.md` with a scoped supersession/erratum, and update
  `async_task_contract.py` plus its owner/preflight so public handles are owner supplied and unknown states fail
  terminal. These authority changes land before Plan route implementation, not as cleanup after cutover.

### C1c: Additive identity and public-task substrate

- Add the approved consumer/public-handle and per-generation publication-key schema/repositories in a separately
  reviewed migration. Enforce distinct Plan task/command ids and indexed trusted-owner lookup.
- Add freshness generation/expiry ownership with injected DB clock and atomic ensure/refresh behavior.
- Add the generic task-handle resolver, Plan consumer-first mapper, poll/cancel, and section 6.4 ownership gates; keep
  submit and model execution unchanged.
- Run repository concurrency, 404/503 matrix, unknown/impossible state, exact retry, cancel-one-of-many,
  cancel-last-then-new-submit, terminal-generation refresh, permanent-lock-holder timeout, and TTL-boundary tests
  before introducing the worker.

### C1d: Dark isolated Plan owner and publication

- Register `plan.compile.generate`, cancel/resume handlers, manifest/golden entries, activity requirements, and the
  isolated bounded `plan_compile` recovery binding.
- Add compute/immutable-checkpoint/per-consumer publish functions, lease heartbeat/reclaim, and simulate-only drain
  tests. Prove the recovery tick never waits on model work and other owners advance under Plan saturation.
- Add legacy repair dry-run only. Keep `/api/plan/submit` on the legacy path and do not attach public traffic.
- Run A/B, mutation, ownership, freshness, crash-boundary, PG service metrics, and pinned independent review.

### C1e: Consumer-first frontend and API cutover

- Deploy tolerant frontend/client contract first.
- Switch `/api/plan/submit` to durable ensure-command + attach + 202.
- Remove the synchronous fallback immediately. No feature path may start the old plan thread for the same request.
- Run repair dry-run, review its report, then apply scoped recoverable rows.

### C1f: Delete legacy execution state

- Delete `_queue_plan_hydration`, `_run_plan_hydration`, current-consumer helpers, both inflight dictionaries, lock, and
  hydration semaphore once the route and tests use only the command owner.
- Port tests from private in-memory structures to submit/drain/poll behavior.
- Retain only the explicitly approved response/history compatibility projection until section 13 deletion gates pass.

## 10. Expected Implementation Files

This is an impact inventory, not permission to edit every file in one commit.

| Area | Expected files |
|---|---|
| Command registry/control | `src/sourcing_agent/durable_runtime.py` |
| Recovery binding/lane | `src/sourcing_agent/recovery_drain_registry.py` plus the existing worker scheduler/executor owner; no API-owned executor |
| Plan owner | New bounded owner module such as `src/sourcing_agent/plan_compile_owner.py` |
| Facade/compile split | `src/sourcing_agent/orchestrator.py` and existing planning helpers |
| Public envelope | `src/sourcing_agent/async_task_contract.py` |
| Contract authority correction | `docs/TRACK_C_C1_HEAVY_OPS_DESIGN.md` Plan erratum/supersession plus async-task owner/preflight docs discovered by usage search |
| Routes/light lane/identity gate | `src/sourcing_agent/api.py` |
| Consumer/review storage | Approved migration plus the matching domain repository/adapter; exact files depend on D-C1-1/D-C1-3 and include public-handle/freshness indexes |
| Live frontend API | `frontend-demo/src/lib/api.ts`, `sourcingBackend.ts` |
| UI polling/recovery | `frontend-demo/src/pages/SearchPage.tsx`, `frontend-demo/src/lib/historyRecovery.ts`, `types.ts` |
| Hand-written reference client | `contracts/frontend_api_adapter.ts` and directly coupled contract types/examples; full generation stays C4 |
| Core tests | `tests/test_async_task_contract.py`, `test_api_transport_parity.py`, `test_frontend_history_recovery.py` |
| Runtime tests | `tests/test_command_type_specs.py`, `test_cancel_resume_dispatch_contract.py`, `test_recovery_drain_registry.py` |
| Identity/preflight | `tests/test_user_private_reads.py`, `test_pre_agent_contract_review.py` |
| Frontend contract tests | `tests/test_frontend_plan_contract.py` plus source-contract tests for export/workflow polling |
| C1a lane/export tests | `tests/test_runtime_tuning.py`, `tests/test_export_async_task.py`, transport parity, and frontend source-contract tests |
| Operational repair | A bounded script/CLI and its report-schema test; no request-path repair |

The implementation must re-run an all-usage search before editing because Track B is concurrently moving workflow
runtime repository methods. It must use the current repository owner rather than adding a new Store facade.

## 11. Verification Plan

### 11.1 Characterize-first

Pin, with deterministic model fixtures:

- Submit HTTP status/body and the frontend fields consumed today.
- Plan request, preview, plan, review gate/session, intent rewrite, execution semantics, and criteria artifact refs.
- History pending/running/completed/failed projection and same-history revision behavior.
- Two histories with one semantic signature: one compile invocation/checkpoint, two consumer handles, two independently
  owned review/criteria publications, and two history projections.
- Delete during run and current legacy behavior, even where the characterization documents a bug.
- Owner gating for linked and unlinked plan-stage history.

Characterization is evidence, not a requirement to preserve broken 200/infinite-poll/security behavior.

### 11.2 A/B equivalence

Run legacy and durable owners against isolated PG schemas with the same canonical request and scripted model output.
Normalize only declared nondeterministic ids/timestamps, then compare:

- Effective request, plan, gate, execution bundle, preview, intent rewrite, organization/lane semantics.
- Per-consumer review status/risk/required-before-execution and history-visible plan fields; mutable ids are normalized
  but both reviews must derive from the same immutable checkpoint hash.
- Per-consumer criteria artifact semantic content and count, with no cross-consumer row sharing.
- Model call count and input payload hash.
- Success, deterministic validation failure, transient failure, and explicit patch/planning-mode variants.
- Single consumer, two-consumer fanout, exact-handle retry, fresh checkpoint replay, expired checkpoint refresh, and
  same-history supersede.

The A/B harness must compare against a pinned pre-cutover implementation or frozen expected artifact, not two adapters
that share the newly refactored helper.

### 11.3 Mutation sensitivity

After the green battery, apply and revert controlled mutations proving the tests fail when:

1. Tenant/requester is removed from the idempotency namespace.
2. Plan public `task_id` is aliased to/accepted as internal `command_id`, or ownership resolution is skipped.
3. `history_id` is incorrectly added to the semantic fingerprint and breaks coalescing.
4. A same key/different fingerprint replay is accepted, or expiry is ignored and a stale checkpoint is reused.
5. Two consumer generations share one mutable review/criteria publication.
6. The post-model active-consumer or lease check is removed, allowing late publication.
7. A stale consumer generation overwrites the latest history or starts a workflow.
8. Unknown/impossible status falls back to running, or a poll reports success before consumer publication completes.
9. Plan model work blocks the serial recovery tick, queues beyond reserved capacity, or starves another owner drain.
10. Expired lease reclaim or same-consumer compiled-checkpoint publication retry is disabled.
11. Legacy repair trusts body identity or runs model work inline.
12. Export poll is moved back to shared, export client accepts a missing/non-exact `artifact.handle`, or cancelled/
    canceled/detached/superseded/missing/unknown workflow maps to running.
13. Attach, publication, cancellation, or freshness refresh returns to blocking advisory-lock acquisition while
    retaining a pooled connection, or last-consumer cancellation is split from the locked active-consumer decision.
14. Try-lock retry loses its monotonic total budget/typed `plan_task_lock_busy` outcome, or a new submission reattaches
    to a terminal cancelled/failed command instead of advancing one successor freshness generation.
15. The C1b legacy-caller ratchet is weakened to allow a second thread/compile entrypoint, or C1e fails to tighten it
    to zero after durable cutover.

Mutation evidence is stored with the implementation review packet; no mutation remains in the worktree.

### 11.4 Targeted test nodes

- `test_async_task_contract.py`: owner-supplied handle, type/status/error/ref mapping, and unknown-state terminal totality.
- `test_api_transport_parity.py`: 202/200 retry, 400/404/409, distinct handle/command identity, generic poll/control,
  exact cancel semantics, route/header parity.
- `test_frontend_history_recovery.py`: enqueue/drain/poll, fanout, restart, cancel, delete, supersede, late-result fence,
  expired lease reclaim, fresh/expired checkpoint replay, per-generation review identity, and legacy repair.
- `test_command_type_specs.py`, cancel/resume golden tests, and recovery registry coverage including bounded dispatch,
  zero-capacity no-claim, dispatch failure, and another owner advancing under Plan saturation.
- `test_user_private_reads.py`: the full section 6.4 list/read/review/compile/start/poll/cancel matrix across tenant,
  requester, guessed command id, orphan link, and stale generation.
- `test_frontend_plan_contract.py`: pending/queued compatibility, unknown-state fail-closed, bounded terminal/error behavior,
  opaque reload/resume task handle, and no command-id inference.
- Export async/frontend tests remain green, including immediate succeeded replay, stale watermark artifact-null behavior,
  exact-relative handle validation, route-method lane classification, and shared-lane saturation.
- Real PG tests run with `SOURCING_CONTROL_PLANE_PG_POOL_MAX=1` and force concurrent attach/publish, attach/cancel,
  publish/cancel, cancel-last/new-submit, terminal/expired-checkpoint refresh, and a permanently held lock. Every
  lock-acquisition attempt must return within the configured budget plus scheduler tolerance, leave one authoritative
  command/publication outcome, prove a waiting contender returned its connection before backoff, and return the typed
  busy outcome when the holder remains. The permanent-holder case also proves there is no unlocked status/lease
  mutation and that an already-committed publication lease is reclaimable only after normal expiry. Full UoW latency
  remains the separate 500 ms service gate below.

### 11.5 Service-level metrics and gates

Use a deterministic delayed model and PG-backed isolated runtime. Provider/model invocation mode must report
`simulate`/`scripted` for every invocation.

| Metric | Gate before promotion |
|---|---|
| `plan_task_submit_latency_ms` | Under eight saturated shared slots, p95 <= 500 ms and <= 2x idle p95 |
| `plan_task_poll_latency_ms` | Under the same load, p95 <= 250 ms and zero timeout for 20 concurrent clients |
| Request-thread model calls | Exactly zero from plan submit, task poll, history read, cancel, and repair endpoints |
| Model calls per freshness generation | One on uncontended success; retries separately labeled and bounded; expired generation refreshes explicitly |
| Fanout efficiency | Two or more fresh consumers share one command/model call, receive distinct publications, and all current consumers converge |
| Freshness | Zero attachment to an expired/mismatched/terminal command; exact metrics for fresh replay vs expiry/terminal/validation refresh |
| Restart recovery | Queued task resumes after worker restart; expired running lease reclaims within lease + backstop budget |
| State coherence | Each task handle, consumer terminal, its review/history projection, and immutable command checkpoint agree in one poll interval |
| Orphan/legacy counts | Zero unexplained orphan command/consumer; legacy pending count decreases monotonically to zero |
| Late publish | Injected cancel/supersede produces blocked evidence and zero public review/history/criteria mutation |
| Lane isolation | Export/task status and non-Plan recovery stay reachable while shared and all Plan slots are occupied; Plan queue never exceeds configured capacity |
| Pool-lock liveness | Pool max=1 attach/publish/cancel/refresh lock acquisition returns within 250 ms plus scheduler tolerance, with no duplicate command/publication or pooled connection retained during backoff; permanent holder yields typed busy, no unlocked claim mutation, then expiry reclaim |

Thresholds are local service gates, not live-provider latency claims. If the existing test host cannot meet them, the
review packet must include idle/saturated baselines and an owner-approved adjustment before weakening assertions.

## 12. Rollout And Rollback

### 12.1 Rollout

1. Land C1a independently and verify no storage change.
2. Land characterization/preflight tests, correct the older RATIFIED C1 authority and `async_task_contract` authority,
   and obtain D-C1-1 through D-C1-4 decisions. Do not implement a guessed migration.
3. Apply the approved public-handle/consumer/publication/freshness schema and repository dark; verify old binaries
   ignore it safely and repository concurrency/ownership/TTL tests pass.
4. Land generic task resolution/poll/cancel dark. Prove Plan command ids are rejected publicly, the 404 matrix is
   total, and unknown/impossible states fail terminal before adding model work.
5. Land command owner and the isolated bounded Plan consumer/publisher with repair dry-run while submit remains legacy.
   Verify zero-capacity no-claim, nonblocking recovery tick, other-owner progress, checkpoint immutability, and
   per-generation publication across crash boundaries.
6. Run targeted tests, A/B, mutation, PG service metrics, and scope-matched independent review against a pinned commit.
7. Deploy the tolerant frontend/client and confirm opaque handle persistence plus terminal-total polling.
8. Cut submit to durable 202 and remove its sync/thread fallback in the same application release.
9. Observe task/consumer/freshness/lane/recovery metrics; run legacy repair dry-run, approve, then apply.
10. Delete process-local execution state and later the response/history bridge as their gates mature.

Every migration bridge must expose `bridge_used`, remaining-row counts, and its deletion gate in the review report.
There is no silent automatic fallback from durable command to legacy thread.

### 12.2 Rollback

- C1a is independently revertible because it changes no durable state.
- Before API cutover, additive schema, task-handle resolver, and dark owner code can remain unused; rollback does not
  drop columns/tables or mutate immutable checkpoints.
- After API cutover, **blind rollback to a binary without the Plan consumer/publisher and public-handle poll is
  prohibited** while nonterminal consumers exist. Freeze new attaches, keep compatible poll/cancel plus isolated drain
  running, and either finish each owned publication or explicitly terminalize affected consumer handles before
  rolling API code back. Draining the shared command alone is insufficient.
- Frontend tolerant parsing is backward compatible and should remain during rollback.
- Rollback must not widen freshness TTL, attach a new consumer to an expired checkpoint, alias a task handle to its
  command id, or reuse one consumer's mutable review/history publication for another. Old checkpoints may remain for
  audit and exact existing handles remain readable.
- Restoring the legacy thread is an emergency, owner-approved stopgap only after durable tasks are drained and the
  consumer inventory is terminalized, and the temporary path is report-visible. It is not the normal rollback
  strategy; forward-fix is preferred.
- No destructive schema rollback occurs in the incident window. Schema removal is a later reviewed migration.

## 13. Deletion Conditions

| Legacy element | Delete when |
|---|---|
| Sync `plan_workflow` fallback in API | At durable submit cutover; no grace period |
| `_queue_plan_hydration`, `_run_plan_hydration`, inflight dictionaries, lock, semaphore, helper tests | Route has no caller and durable owner submit/isolated-drain/poll/restart tests pass |
| Frontend exact `status === "pending"` branch | All supported servers return canonical task handle and tolerant client has shipped |
| History `plan_generation` as lifecycle source | Generic task poll is normal path, legacy pending inventory is zero, and no reader schedules/retries from metadata |
| History-poll compatibility after plan submit | Two release windows with zero fallback use and task polling service gate green |
| Nested/top-level response bridge | D-C1-2 condition, generated/handwritten clients updated, bridge-used metric zero |
| Legacy pending repair apply mode | Two release windows with zero eligible rows; retain report reader if audit requires |
| Export-specific status alias | C4-generated client consumes `/api/tasks`, parity and artifact tests green |
| Additive consumer/public-handle/publication schema | Only through a separate reviewed destructive migration after all references are zero and retained task handles no longer require reads |

## 14. Track Boundaries And Residual Risks

### C2 identity boundary

C2 owns authentication/token resolution. C1 consumes trusted identity and closes the plan-stage history gap; it does
not redesign auth. Open mode must have one explicit sentinel policy. Authenticated mode cannot auto-claim legacy rows
whose trusted owner was never persisted.

### C3 runtime boundary

C1 uses the existing recovery daemon, wake signal, poll backstop, and command lease, but Plan execution is dispatched
onto its isolated bounded lane rather than blocking the serial recovery tick. C3 later makes the worker the only
process runner and may move that lane to its own container. No C1 correctness may depend on a shared Python process,
local slot reservation, local lock, or in-memory consumer map.

### C4 contract boundary

C1 defines 202/poll/control semantics and minimally updates hand-written consumers and the
`async_task_contract.py` authority. C4 generates OpenAPI clients and adds SSE over the same event/command spine. C4
must preserve owner-supplied public handles and must not invent a second Agent/task status vocabulary.

### Track D boundary

Track D may later reuse generic task envelopes and event tailing for Agent turns, but C1 adds no session/turn tables,
Agent commands, tool manifests, or Agent-specific fields. The generic task resolver/mapper remains owner registered so
future types can supply their own public handles without Plan heuristics or a universal `task_id == command_id` rule.

### Residual risks

- Current `plan_workflow` side effects make late-result quarantine impossible without the compute/publish split.
- Existing durable writer operations and history attachment are not proven one transaction; reconciliation and the
  zero-consumer cost fence are mandatory unless a reviewed UoW is added.
- Model call duration can exceed a lease and cause duplicate cost unless timeout/heartbeat/reclaim policy is tested.
- Review and criteria publication lack an explicit per-consumer-generation uniqueness contract today; D-C1-3 blocks
  coding around that ambiguity.
- No complete planning-input watermark exists today. D-C1-4 bounds checkpoint replay with TTL; an unreviewed TTL
  increase or partial pseudo-watermark would reintroduce stale-plan risk.
- Raw `/api/workflows`, `/api/workflows/explain`, and refine remain possible request-thread model/retrieval paths.
- Contracts SDK still names deleted plan/jobs routes; minimal direct callers must be repaired in C1, full generation
  remains C4.
- Local export artifact storage remains a C6 concern but does not block the plan task.

## 15. Owner Decision Cards

These cards follow `TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md` section 7. A recommendation is not approval. Safe timeout
defaults allow C1a and characterize work to continue but block migration/public cutover.

### D-C1-1 Durable plan-consumer physical model

- **Single question**: Should durable Plan public handles, fanout, and ownership use a dedicated
  `plan_task_consumers` relation or explicit columns on `frontend_history_links`?
- **Options**: (a) Dedicated relation with opaque public `task_id`, internal `command_id`, history/generation,
  publication key, indexed owner scope, and history remaining a projection. This cleanly owns handle resolution,
  fanout/detach/CAS but adds one table/repository; (b) add public task handle, command id, generation, requester,
  tenant, publication key, and consumer status columns to `frontend_history_links`. This is a smaller schema surface
  but makes a UI read model also own execution/permission state; (c) store membership only in metadata JSON. This
  avoids DDL but cannot enforce distinct ids, uniqueness/CAS, or indexed owner queries and is not acceptable.
- **Recommendation**: (a), because fanout, detach, ownership, and generation fencing are durable execution semantics,
  not presentation metadata. Keep it plan-specific for this slice rather than prematurely creating a universal
  consumer table.
- **Deadline**: Before C1c migration design; **decider**: owner.
- **Timeout default**: No schema or durable submit cutover. C1a/C1b continue; legacy plan execution remains unchanged
  and explicitly not signed off.

### D-C1-2 Public response compatibility shape

- **Single question**: After the tolerant frontend ships, should `/api/plan/submit` switch directly to the canonical
  top-level 202 envelope, or serve one release of a nested canonical `task` bridge while top-level status stays
  `pending`?
- **Options**: (a) Direct top-level 202 with additive legacy domain fields and the opaque consumer task handle in the
  one canonical response location, but
  unknown external clients that require 200/`pending` may break; (b) one-release nested `task` bridge plus legacy
  top-level fields, with bridge-use telemetry and a fixed deletion release. Safer staggered deployment, but temporarily
  exposes two status locations and must block normal-path signoff until removed.
- **Recommendation**: (a) after consumer-first deployment and consumer inventory, because the live frontend already
  accepts any 2xx and can be made `pending|queued` tolerant in C1a. Choose (b) only with evidence of independently
  deployed clients that cannot upgrade atomically.
- **Deadline**: Before C1e API cutover; **decider**: owner/product API owner.
- **Timeout default**: Keep current 200/pending public response and do not route live submit to the new owner. Dark
  owner, generic poll, and simulate-only validation may continue.

### D-C1-3 Review/criteria publication idempotency key

- **Single question**: What physical unique key binds each consumer generation to its own review session and
  criteria-artifact set across crash/reclaim without making those mutable rows command-shared?
- **Options**: (a) Add an explicit per-consumer publication key/constraint to the review/criteria owner or a small
  reviewed result-link relation keyed by `(public_task_id, consumer_generation, checkpoint_hash)`, enabling exact
  lookup and transactional/CAS publication; (b) key only by internal command id. This prevents duplicates but wrongly
  shares mutable review/ownership across consumers; (c) reuse request signatures/find-pending or scan JSON metadata.
  This can select unrelated/stale reviews and lacks physical uniqueness/indexed recovery.
- **Recommendation**: (a). The immutable checkpoint may be shared, while review, criteria, and history publication are
  generation-owned. Exact retry of one handle must reuse its set; another handle must get a distinct set. The team
  must characterize current review reuse before choosing the exact table/column/UoW shape.
- **Deadline**: Before C1c publication-schema design and C1d owner publish code; **decider**: owner with
  plan-review/storage owners.
- **Timeout default**: Do not publish durable Plan results. Command/compute prototypes remain simulate-only and dark;
  C1a/C1b continue.

### D-C1-4 Completed-checkpoint replay freshness

- **Single question**: Until all mutable planner inputs have authoritative revisions, what bounded policy controls a
  new consumer reusing a completed compile checkpoint?
- **Options**: (a) Registry-owned TTL with DB-clock expiry, monotonic freshness-generation CAS, an initial maximum of
  300 seconds, and automatic refresh after expiry. This gives bounded staleness and preserves short retry/fanout cost
  savings; (b) require an explicit planning-input watermark now. This is stronger only if total, but current prompt,
  configuration, and context owners do not expose one complete revision, so implementation would either block all
  reuse or create false confidence; (c) indefinite replay by fingerprint/compiler version. This is cheapest and is
  rejected because mutable planning context can change without either value changing.
- **Recommendation**: (a), with metrics from section 8.3 and any later watermark introduced as a separately reviewed
  replacement. Increasing the 300-second maximum requires owner review; lowering it is safe.
- **Deadline**: Before C1c freshness schema/repository work; **decider**: owner with plan/product cost owner.
- **Timeout default**: Allow already-attached in-flight consumers to converge, but do not attach a new consumer to a
  completed checkpoint. Dark scripted tests continue; public cost/SLO signoff remains blocked.

## 16. Definition Of Done

C1 durable Plan is complete only when all are true:

1. C1a transport/frontend fixes are green and independently reviewed.
2. All four owner cards are resolved and schema/API/freshness decisions are recorded before C1c schema work, durable
   owner implementation, or public cutover; C1a and C1b characterization may proceed first.
3. `plan.compile.generate` is the only normal-path plan execution owner and has full registry/control/activity/drain
   coverage.
4. Submit performs no model call/thread start and returns canonical 202 with a consumer-specific handle distinct from
   command id; generic poll/control and list/read/review/compile/start are identity-gated.
5. Fanout, exact-handle retry, fresh replay, expired refresh, distinct mutable publication, cancel-one-of-many,
   supersede, delete, late result, restart, expired lease, lane saturation, and legacy repair tests pass.
6. A/B semantic output and model-call counts match the characterized contract.
7. Mutation tests prove the key safety gates can fail red.
8. PG service metrics meet section 11 using only scripted/simulated invocation.
9. Fast owner/source/consumer preflight is green, then a non-author pinned-commit independent review returns `GO`.
10. The older RATIFIED C1 design and `async_task_contract.py` authority are corrected, process-local hydration state is
    deleted, compatibility bridges are visible with deletion conditions, and remaining raw-workflow/explain/refine
    debt is not mislabeled as complete.
