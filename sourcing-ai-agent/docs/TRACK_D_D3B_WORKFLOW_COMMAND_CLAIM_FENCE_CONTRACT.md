# Track D D3b — Workflow-command claim fence decision lock

> Status: Owner-directed decision-lock document (2026-07-14), based on
> `60b10a29d3609058638ae53938f1a9a2ae1f0f44`. This batch is documentation only: zero product code, zero migration,
> zero provider/model call, and zero live behavior. A fresh pinned non-author review is required after the enclosing
> commit; this document is not a formal `GO` and does not activate D3. Two round9 local advisory attempts ended at the
> Codex operator usage limit before producing a final response or artifact, so they have **no verdict**; their direct
> partial findings were fixed forward, while fresh non-author/formal review remains pending and fail-closed for Live or
> signoff only.

## 1. Outcome and scope

D3b locks the physical authorization contract that must exist before a D3 company-identity verification command may
create an `ActivityAttempt`, authorize a provider/model dispatch, record a terminal result, mutate domain state, or plan
an effectful child. The core invariant is:

> A process that no longer owns the current workflow-command claim cannot authorize a new external dispatch or produce
> a new command/domain effect. A dispatch already linearized while the claim was current is authorized in-flight work;
> later invalidation cannot unsend it, but it prevents its result from being applied.

The contract uses three deliberately separate identities:

1. `claim_generation`: monotonic, audit-visible claim lineage;
2. `claim_token`: private, opaque authorization capability for one successful claim;
3. `control_epoch`: monotonic command-lifecycle fence that changes before a replacement claim exists.

`attempt` remains retry-budget accounting and is exact-bound as a freshness input; `lease_owner` remains diagnostic
ownership text. Neither is authorization by itself.

This document decides ownership, storage, transition, transaction, migration, projection, and deletion semantics. It
does not add the columns, change the current 29 production claim callers, add D3 command/action registrations, or close
the whole of R-019.

## 2. Controlling authority and obligation identity

The implementation authority for this contract is exactly:

- `TRACK_D_AGENT_RUNTIME_PLAN.md` **Plan section 6 item 6**: add a never-reset workflow-command claim
  generation/token as a D3 migration prerequisite;
- `RESIDUAL_LEDGER.md` **R-019**: prevent a stale command owner from first inserting a phantom child or
  `ActivityAttempt`, require generation/token validation before an owner effect, and make a failed authorization CAS
  write no action/event/command state;
- `TRACK_D_D3_COMPANY_IDENTITY_SELF_VERIFICATION_DESIGN.md` section 4c: bind a verification intent to the exact
  command claim, `ActivityAttempt`, and a control epoch distinct from claim generation;
- that design's section 4b: persist immutable `runtime_namespace + provider_mode` across the
  operation/command/attempt chain and reject cross-mode acceptance; command scope cannot be inferred from mutable
  process environment at claim time;
- `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`: PG-only workflow/activity state, one command owner, command-owned effects,
  terminal monotonicity, and the still-open R-019 boundary;
- `DESIGN_INVARIANT_CHECKLIST.md`: single writer, tenant equality, stored physical generation, synchronous control
  fencing, late-result handling, physical identity, and runtime-mode isolation.

There is **no claim-fence `OB-*` identifier** in the current invariant matrix. Plan section 6 item 6 is a numbered Plan
obligation, not an OB-ID. Implementations and reviews must cite `Plan section 6 item 6 + R-019`; they must not invent or
borrow an unrelated OB-ID.

One existing OB-ID does apply when the future D3 `verification_intent` row is created: **OB-10.1** requires immutable
`runtime_namespace` and `provider_mode` columns on that row and equality in every acceptance CAS. OB-10.1 does not
authorize or rename the workflow-command claim-fence migration.

Three more existing matrix obligations remain carried, not closed, by this decision lock:

- **OB-10.2**: `identity_search_budget_grant` must persist namespace/mode and include both in grant identity and every
  consume/revoke/exhaust/supersede CAS;
- **OB-10.3**: cost reservations and physical-call exposure rows must persist namespace/mode and include them in
  reservation, dispatch, and reconciliation CAS;
- **OB-10.4**: `ModelTurnExecutionContext` must carry namespace/mode and include them in the canonical request and
  idempotency digests.

All three block D3 transport activation. This document can specify where the claim fence consumes them, but a future
implementation and its own evidence must close them; D3b does not relabel them as satisfied.

Action-backed OperationRun creation has a separate, locally named **Plan section 6 item 6 action-root durable-scope
gate**. Before an action can seed a strict D3 scoped review session, the action owner must persist immutable namespace,
provider mode, workspace, and canonical scope-input/digest pins on the action root; the scoped review-session repository
must exact-compare them before it alone issues the session scope, and OperationRun creation may only copy that scope.
This prerequisite has no OB-ID and is not OB-10.4: OB-10.4 governs `ModelTurnExecutionContext`, not
`agent_actions` scope. D3b carries both gates and closes neither.

## 3. Characterized baseline

The D3a characterization at this base proves:

- `workflow_commands` has `attempt`, `lease_owner`, and `lease_expires_at`, but no physical claim generation, claim
  token verifier, or control epoch;
- `workflow_commands` also has no physical `runtime_namespace`, `provider_mode`, or `workspace_id`; those names in a
  claim predicate therefore cannot be treated as existing command columns or silently read from JSON;
- `attempt` increments on claim but is decremented by partial-progress, prerequisite-wait, and resume paths, and reset
  to zero by retry;
- one native PG adapter method owns the physical claim update behind one PG-only delegating facade;
- there are 29 production calls to `claim_workflow_command` across six modules;
- the current PG adapter contains 13 physical `workflow_commands` mutation statements: insert/upsert, queued payload
  update, prerequisite reawaken, claim, running, success, failure, partial progress, prerequisite wait, owner-specific
  cancel, generic cancel, retry, and resume;
- the current production call graph contains 29 running calls, 39 success calls, 71 failure calls, 15 partial-progress
  calls, and 3 prerequisite-wait calls;
- workflow event/reducer entry has 62 production calls, including 34 `CommandPlanRequested` planners; event append and
  child-command insert are separate durable writes;
- a new descriptor field currently flows through backend API dict construction and frontend raw-record spreading unless
  the server installs an explicit public projection boundary.

These facts mean `attempt + lease_owner` cannot be strengthened in place. The three-part identity is additive and the
existing retry counter keeps its current product meaning.

## 4. Owner and source-of-truth matrix

| Field/object | Single owner | Physical SOT | Allowed values | Derivation rule | Consumers | Forbidden consumers | Fallback/brownfield status | Migration status | Deletion condition |
|---|---|---|---|---|---|---|---|---|---|
| command authority policy | `DEFAULT_COMMAND_TYPE_SPECS` / `CommandTypeSpec` in `durable_runtime.py`; no second command-owner registry | checked-in canonical registry entry plus its canonical digest; existing immutable `workflow_commands.command_type` is the physical population discriminator | `claim_fence_policy in {legacy_unfenced, scoped_session_bootstrap_v1, d3_v1}` plus an explicit stage policy and non-empty `allowed_stage_ids` where membership is required; source session-create commands alone use `scoped_session_bootstrap_v1`, while positive-review strict D3 commands use `d3_v1` | canonical serialization and digest of every checked-in authority field, including `terminal_provenance_policy_digest` derived from the unique applicable `TERMINAL_PROVENANCE_SPECS` manifest; command `claim_authority_spec_digest` and the existing exposure policy pin preserve the historical digest without a new command column; the migration generator materializes separate sorted `scoped_session_bootstrap_command_types_v1` and `strict_d3_command_types_v1` literal manifests plus digests before either fenced population activates | bootstrap session-create predicate, claim repository, Stage B, terminal UoW, transport owner, Migration-C population guards | caller-supplied owner/type/stage text; row self-assertion; digest/sentinel presence as a population discriminator; handwritten DDL command-type list; public capability projection | existing entries are `legacy_unfenced` and cannot enter either fenced population; bootstrap commands cannot enter normal `d3_v1` dispatch/claim | future D3 registry extension plus two registry-derived migration manifests; D3b is decision-only | scope-local D3/bootstrap bridges may retire only for their manifest populations after §13; the global legacy bridge remains while any `legacy_unfenced` entry/caller exists. Retain every historical authority/provenance spec until no durable reference of any lifecycle state remains |
| pre-claim `ClaimAuthority` | process composition installs one private registry-backed factory; only the trusted scheduler selection path invokes it, once per exact selected command | sealed one-use in-memory capability | one current, unexpired exact-selection capability or not issued | private factory seals the selected row, registry entry, stage, scope, coordination lineage, business-fence digest, selection reservation, **pre-claim command attempt**, epoch, worker, and lease identity after repository selection wins | Stage-A claim API and the registered owner invocation selected for that command only | wrapper-startup mint, wrapper-held standing credential, strings/dicts/deserialization, arbitrary internal callers, API/frontend/model/logs | not issued | future D3 factory and selection repository; D3b is decision-only | delete the strict-population mint bridge only after every wrapper eligible for either fenced manifest uses exact-selection issuance; global legacy issuance remains out of this D3b subclosure |
| current `ClaimReceipt` / `ClaimIdentity` | workflow-runtime claim repository returns it only after Stage A validates the pre-claim authority and wins the row CAS | immutable private receipt containing raw token/current row identity plus authority id/digest | one current receipt for one active claim, or not issued | assembled from the committed Stage-A row plus its fresh raw token and sealed authority identity, including exact stage/coordination/business pins and the **post-claim `workflow_commands.attempt`** produced by that CAS | Stage B, heartbeat, terminal, and registered transport/effect repositories | generic command dicts; another wrapper; public projection | not issued | future D3 Stage-A repository; D3b is decision-only | delete only the scope-local strict receipt bridge after every fenced-population effect consumer enforces the central predicate; legacy/non-D3 consumers do not count as D3 closure |
| strict D3 plan-review/operation scope and coordination lineage | scoped review-session repository is the sole scope/lineage issuer; its private `ScopedReviewSessionBootstrapAuthority` factory, one-use bootstrap Stage A, specialized bootstrap Stage B/session UoW, and bootstrap predicate alone authorize initial issuance; OperationRun creation is exact-copy only | future `plan_review_sessions.runtime_namespace/provider_mode/workspace_id/scope_issuer/scope_digest` plus canonical BIGINT `review_id`, typed `creation_source_workflow_command_id/creation_source_event_id/creation_plan_id/creation_plan_revision/creation_plan_bundle_digest`, and `creation_idempotency_key`; sealed one-use authority plus pre-session private current-claim `ScopedReviewSessionBootstrapReceipt`; strict `operation_runs` exact-copies committed session scope and review id | canonical non-empty namespace/workspace/causal ids/plan id, normalized mode, registered `plan_review_session` issuer, lowercase 64-hex scope/plan-bundle/idempotency digests, non-negative plan revision, current source claim/generation/epoch/post-claim attempt/lease, and positive BIGINT review id only in the create result | create mode locks `scoped-session-bootstrap-lock-v1(scope tuple, creation_idempotency_key)`, exact-validates authority+Stage-A receipt against the current source command, authenticated workspace, immutable source event, and plan pins, then **creates the claim-bound ActivityRun/ActivityAttempt in that same UoW** before session/event/source terminal commit; no ActivityAttempt is required or bound before Stage A. A separate server-constructed committed-replay context may only read/exact-compare an already committed aggregate and return the stored `ScopedReviewSessionCreateResult` after token loss; it cannot create, mint, or mutate. Afterward OperationRun only copies and normal positive-review `d3_v1` applies | bootstrap Stage A and specialized Stage-B/session-create UoW, committed exact replay, strict OperationRun/command creation, same-scope review FK, normal coordination-lock key derivation after issuance, and all downstream copies | putting review id/session-created event/terminal digest in the pre-session receipt; binding a not-yet-created ActivityAttempt in the authority/receipt; using replay context to create; normal `ClaimAuthority` or `d3_business` predicate requiring review id during bootstrap; legacy JSON-scan creator; caller review id/key; request/plan/gate JSON; ambient/provider/model scope remint | legacy sessions keep empty scope/causal/idempotency sentinels and are ineligible for strict D3; bootstrap receipt is private/current-claim only and has no transport/domain branch | future D3 bootstrap factory/Stage-A receipt/predicate, specialized Stage B, committed-replay reader, scoped-session + operation migration/backfill; D3b is decision-only | delete sentinel compatibility only after §11 bootstrap claim/session identity, both crash windows/exact-replay, backfill validation and the action-root gate close |
| `runtime_namespace` | workflow-runtime command-creation repository copies the trusted runtime scope once | future `workflow_commands.runtime_namespace` | canonical non-blank namespace | exact-copy from the durable operation scope in the command-creation UoW | command identity, claim mint, central predicate, D3 activity/intent binding | mutable process environment at claim/effect time; caller payload; model output | empty string | future D3 additive migration/backfill; D3b is decision-only | delete empty-string bridge after every strict row validates against its operation |
| `provider_mode` | workflow-runtime command-creation repository copies the trusted normalized mode once | future `workflow_commands.provider_mode` | `live`, `simulate`, `scripted`, or `replay` | normalize once at operation creation, then exact-copy | command identity, claim mint, central predicate, D3 activity/intent binding | claim-time environment fallback; caller/model mutation | empty string | future D3 additive migration/backfill; D3b is decision-only | delete empty-string bridge after strict-mode validation reaches the full denominator |
| `workspace_id` | workflow-runtime command-creation repository copies the canonical operation workspace once | future `workflow_commands.workspace_id` | canonical non-blank authenticated workspace | exact-copy from the durable operation root | tenant equality at every claim/effect boundary | command payload, artifact path, implicit `default` fallback for an existing row | empty string | future D3 additive migration/backfill; D3b is decision-only | delete empty/default bridge after full workspace backfill validation |
| `claim_selection_generation` | workflow-runtime selection repository; exact-command selection is its only increment writer | future `workflow_commands.claim_selection_generation` | non-negative integer | increment exactly once for every new or expired-reservation reselection | one-use authority mint and Stage-A consume CAS | wrapper state, caller payload, `attempt`, claim-generation derivation | `0` | future D3 additive migration; D3b is decision-only | retain permanently as canonical selection lineage |
| consumed authority identity | Stage-A claim repository | future `workflow_commands.consumed_claim_authority_id` | empty or the one opaque authority id consumed for the current selection generation | a new/reselection UoW first advances `claim_selection_generation` and then clears this slot atomically; Stage A installs the winning sealed authority id with claim mint | current-selection duplicate/parallel authority rejection only | historical-consumption audit, public projection, logs, model context, effect authorization by itself | empty string | future D3 additive migration; D3b is decision-only | retain as the current-selection one-use enforcement slot; reselection reuses it only after generation advances |
| `claim_generation` | workflow-runtime repository; its native PG claim primitive is the only mint writer | future `workflow_commands.claim_generation` | non-negative integer without wrap | increment exactly once when Stage A wins; never decrement or reset | central claim predicate; activity/intent provenance; authenticated command diagnostics | caller payloads; provider/model output; readers attempting authorization | `0` | future D3 additive migration/backfill; D3b is decision-only | retain permanently as canonical claim lineage |
| raw `claim_token` | native PG claim primitive generates it and returns it once in a private typed receipt | claimant memory only; never persisted | 64 lowercase hex characters while the process holds the current receipt, otherwise absent | CSPRNG-generated at each winning Stage A | central claim predicate input in the owning process | database generic rows, logs, events, artifacts, public APIs, frontend, model context | absent | future D3 private receipt; D3b is decision-only | destroy from memory when the claim terminates or is invalidated |
| `claim_token_digest` | workflow-runtime repository | future `workflow_commands.claim_token_digest` | empty or lowercase 64-hex SHA-256 | hash the fresh raw token in Stage A; clear on every invalidating or terminal transition | central claim predicate only | public projections; domain rows; activity/intent mirrors; application logs; terminal replay | empty string | future D3 additive migration; D3b is decision-only | clear on invalidation/terminal; retain the column as the active verifier |
| `control_epoch` | workflow-command control repository; owner-specific delegates call this owner rather than minting locally | future `workflow_commands.control_epoch` | non-negative integer without wrap | increment exactly once for every accepted dispatch-invalidating executable-input, control, requeue, reopen, or recovery transition; first result-terminalization retains it | central claim predicate; activity/intent provenance; authenticated command diagnostics | caller/model mutation; review/gate aggregates using it as their own epoch | `0` | future D3 additive migration/backfill; D3b is decision-only | retain permanently as canonical control lineage |
| D3 expected-predecessor command pins | strict D3 command-creation repository | future nullable typed `workflow_commands.expected_predecessor_intent_id/expected_predecessor_phase_generation/expected_predecessor_source_control_epoch/expected_predecessor_decision_source_event_id` | exactly all SQL NULL for a candidate initial phase or a complete typed tuple with non-blank ids, positive generation, and non-negative epoch | in one UoW, reserve/create the non-claimable command identity in the all-command segment, then enter the intent segment, lock the typed current lineage, and fill the exact tuple before commit; Stage B still proves the semantic role | sealed authority/receipt, `d3_business_fence_v1`, Stage B, supersession, and central business evaluator | payload/JSON/model text, caller-supplied predecessor, half-null tuple, treating all-null as a wildcard, or returning to command locks after intent | all SQL NULL | future D3 additive migration/typed owner cutover; D3b is decision-only | retain as immutable command authorization pins; only a new command identity may change them |
| D3 business-fence digest | strict D3 command-creation repository | future immutable `workflow_commands.d3_business_fence_digest` | lowercase 64-hex SHA-256 for a strict D3 command; empty sentinel only for brownfield/non-strict rows | canonical `d3_business_fence_v1` over typed plan/review/gate/base-intent/predecessor pins at command creation; no JSON or ambient input | sealed authority/receipt and all six central phases: Stage B, terminal, record, dispatch, `resume_after_grant`, control | payload/model text, caller-computed digests, business authorization by digest alone | empty string | future D3 additive migration/typed-pin owner cutover; D3b is decision-only | delete the empty sentinel branch only after every strict row recomputes from typed owner rows |
| `attempt` | existing workflow-runtime retry accounting | existing `workflow_commands.attempt` | existing non-negative retry counter semantics | existing Stage-A claim/retry/partial/wait/resume rules; a sealed authority pins the pre-claim value and its winning receipt exact-copies the post-claim value | retry budget/diagnostics plus exact current-claim/ActivityAttempt equality | generation derivation; authorization by `attempt` alone | existing value | already physical; no D3 semantic migration | retain for product retry accounting; never substitute it for claim generation |
| `ClaimIdentity` | workflow-runtime repository | immutable private return object assembled from the committed claim row plus raw token | sealed fields exactly matching one current active claim, including post-claim command `attempt` | combine the committed row values with the fresh raw token after Stage A commits | current command owner only | generic command dictionaries and public serialization | not issued | future D3 private type; D3b is decision-only | destroy when the claim terminates or is invalidated |
| D3 attempt binding | `company_identity_verification` command owner through the workflow repository | future strict `workflow_activity_attempts` row | exactly one scope/coordination/business/operation/command/generation/epoch/post-claim-command-attempt identity | exact-copy from the current ClaimIdentity when normal or specialized bootstrap Stage B creates the ActivityAttempt | D3 effect and result owners | public ingress and other command owners | no row | future D3 migration/repository; D3b is decision-only | retain with terminal audit history under the registered retention policy |
| D3 intent binding | company-identity verification owner | future PG `verification_intent` row | immutable scope/coordination/business/core source tuple plus null-or-complete append-once terminal tuple | copy core identity at intent creation; append terminal event/digest once when the source becomes result-terminal | D3 record CAS | generic workflow controls writing the domain row directly | no row | future D3 migration/repository; D3b is decision-only | retain until the registered intent/audit retention owner tombstones it |
| heartbeat occurrence | workflow-runtime claim repository | future command-row sequence plus last occurrence id, scoped by claim generation | non-negative sequence plus empty-or-repository-issued occurrence id | increment once per accepted heartbeat; reset only when a new claim generation is minted | heartbeat/lease-renewal CAS only | effect authorization; caller-supplied expiry | `(0, '')` | future D3 additive migration; D3b is decision-only | retain columns; clear occurrence identity on terminal/invalidation |
| transport terminal provenance registry | checked-in `TERMINAL_PROVENANCE_SPECS` / closed `TerminalProvenanceSpec` union is the sole semantic owner | immutable checked-in `TransportResponseSpec`, `TransportAttemptFailureSpec`, and `NoExposureTerminalSpec` entries plus their canonical manifest/digests | exactly one applicable entry for each `(command_type, stage_id, terminal_transport_variant, terminal_status, terminal_event_type, terminal_outcome, terminal_reason if exposure, failure_code if attempt_failure)`; response/failure/no-exposure entries close their allowed membership, failure entries additionally close failure codes, retry revision, and `retry_disposition in {retryable, terminal}` | canonical serialization computes each spec digest and the unique applicable-spec manifest; `CommandTypeSpec.terminal_provenance_policy_digest` derives from that manifest, and commands/exposures preserve their historical policy digest | bootstrap/normal command policy validation, response/failure receipt creators, retry/terminal predicates, Migration-C parity guards | ambiguous/duplicate/missing applicable entry; omitting variant/reason/code from applicability; caller outcome/retry/code/digest; payload/model/exception inference; deleting a spec still referenced by durable history | legacy policy has no strict terminal authority; registered no-exposure is the sole complete-none terminal policy | future checked-in registry + preflight/migration manifest; D3b is decision-only | historical specs/manifests are append-only and remain until **no active or retained command, exposure, response/failure receipt, terminal event, source-intent tuple, quarantine/tombstone, or cost/audit row** references them; replacement never reinterprets any retained record |
| transport terminal evidence receipts | transport evidence repository is the sole receipt writer; D0 `model_tool_runtime` owns only valid terminal-response envelope classification | future PG `transport_response_receipts` and `transport_attempt_failure_receipts`, each carrying exact scope/exposure/attempt/generation/epoch/business and applicable response/failure spec pins | a response receipt exact-copies one `TransportResponseSpec` plus a D0-valid response envelope; a failure receipt exact-copies one `TransportAttemptFailureSpec`, registered code/retry pins, and envelope-free failure evidence | both creators lock their committed exposure `FOR UPDATE`; response validates `ModelInvocationEnvelopeV1` including valid `length`/`content_filter` terminal reasons, while only incomplete/truncated wire or protocol parse failure creates attempt-failure evidence; stable occurrence identities enforce exact replay | terminal/retry predicate, source provenance, cost audit; only a valid response receipt may back late quarantine | fabricated D0 envelope/result for failure, protocol-failure quarantine, another exposure/attempt/claim, reducer/domain/public projection | no receipt for proven no-call; retryable attempt failure can only move to `retry_wait` with zero terminal write | future D0/D3 durable receipt migrations; D3b is decision-only | retain immutable receipts/spec pins; no terminal/quarantine path may replace evidence or reinterpret retry disposition |
| terminal result/event identity | command owner through one workflow-runtime PG UoW | future nullable-as-a-pair command terminal event id/outcome digest plus immutable scope/coordination/business/claim/attempt and typed response-receipt, attempt-failure-receipt, or no-exposure provenance columns on `workflow_events` | null pair when not result-terminal; complete exact pair while result-terminal; `terminal_transport_variant in {exposure, attempt_failure, no_exposure}` has exactly one complete registered shape | server computes `terminal_outcome_v1` digest and writes exact variant provenance in the terminal UoW; registered reopen alone clears the command pair | reducer planning, record-command source verification, exact replay | raw token/digest; payload-only/alternate provenance; fabricated D0 failure envelope; cross-table `CHECK` assertions | null pair / no event | future D3 migration/UoW and real-PG receipt/event DDL acceptance; D3b is decision-only | retain terminal history; only registered reopen may clear command pair before requeue |
| late-result quarantine immutable insert identity | shared quarantine repository; typed result-acceptance insert entrypoint is the only insert writer | future PG `workflow_late_result_quarantine` immutable identity/digest columns keyed to an authorized exposure and response occurrence | one insert-once identity with `authorizable=false`, initial `cost_state=pending_reconciliation`, and `retention_state=retained` | insert only from a committed authorized exposure whose stale transport response cannot pass the current business/claim fence | authenticated audit plus the typed cost/retention entrypoints | reducer, command/domain apply, public APIs, future authorization, identity rebind | no row | future D3 table/repository; D3b is decision-only | identity/digest tombstone is permanent; payload refs may be purged only by the retention entrypoint |
| late-result quarantine `cost_state` | shared quarantine repository; typed cost-reconciliation CAS entrypoint is the only state writer | future PG `workflow_late_result_quarantine.cost_state` | `pending_reconciliation` advances to one of `reconciled_confirmed`, `reconciled_uncertain`, or `reconciled_no_call` | monotonic CAS from the exact immutable exposure/response identity | cost ledger and authenticated audit | result insert after creation, retention entrypoint, reducer/domain/public authorization | `pending_reconciliation` at insert | future D3 repository; D3b is decision-only | retain final state in the tombstone permanently |
| late-result quarantine `retention_state` | shared quarantine repository; typed retention CAS entrypoint is the only state writer | future PG `workflow_late_result_quarantine.retention_state` plus purge timestamp/payload-ref clearing owned by the same entrypoint | `retained -> purged_tombstone` | monotonic CAS at fixed `retention_until`, preserving immutable digests/identity | retention worker and authenticated audit | result insert after creation, cost entrypoint, reducer/domain/public authorization | `retained` at insert | future D3 repository; D3b is decision-only | final tombstone and immutable digests remain permanently |

This ten-column, **26-data-row** matrix is decision-complete and every cell is intentionally non-empty. The previous
single quarantine row is deliberately split into its insert identity and two monotonic mutable axes; the shared
quarantine repository remains the only SQL owner while its typed entrypoints have disjoint write sets. It does not claim any physical
owner, migration, repository, or runtime implementation; those remain future D3 work and must be verified mechanically
before Plan §6 item 7 can close at the implementation level.

`ControlPlaneStore` may remain a delegating compatibility surface during the repository cutover, but it is not another
semantic owner and may not implement a second claim predicate.

## 5. Exact physical command identity contract

The future additive migration installs both immutable scope and the three-part claim fence on `workflow_commands`.
This is required because the current table has none of the scope columns; a join guessed at claim time or an ambient
environment read would not meet the D3 section-4b physical-isolation contract.

| Column | Exact contract |
|---|---|
| existing `operation_id TEXT NOT NULL DEFAULT ''` | sole command-to-operation link; a strict command requires exact equality to `operation_runs.operation_run_id`; no `operation_run_id` alias is added to `workflow_commands` |
| `runtime_namespace TEXT NOT NULL DEFAULT ''` | immutable after insert; a new strict command requires the non-empty canonical namespace from the trusted runtime execution context |
| `provider_mode TEXT NOT NULL DEFAULT ''` | immutable after insert; a new strict command requires one normalized value in `live`, `simulate`, `scripted`, or `replay` |
| `workspace_id TEXT NOT NULL DEFAULT ''` | immutable after insert; a new strict command requires the non-empty canonical workspace from its owning operation |
| `scope_digest TEXT NOT NULL DEFAULT ''` | immutable lowercase 64-hex SHA-256 of canonical `scope_v1` namespace/mode/workspace tuple copied from the operation |
| `coordination_plan_review_id BIGINT NULL` | immutable exact-copy of `operation_runs.coordination_plan_review_id`, type-compatible with canonical BIGINT `plan_review_sessions.review_id`; NULL/non-positive fails strict creation and every coordinated owner predicate |
| `claim_authority_spec_digest TEXT NOT NULL DEFAULT ''` | immutable lowercase 64-hex digest of the exact canonical `CommandTypeSpec` authority fields at command creation |
| `expected_predecessor_intent_id TEXT NULL` | immutable; all-null only for a candidate initial phase, otherwise the exact non-blank current predecessor intent id locked by the command-creation owner |
| `expected_predecessor_phase_generation BIGINT NULL` | immutable; null with the other predecessor fields or positive in a complete tuple |
| `expected_predecessor_source_control_epoch BIGINT NULL` | immutable; null with the other predecessor fields or non-negative in a complete tuple |
| `expected_predecessor_decision_source_event_id TEXT NULL` | immutable; null with the other predecessor fields or the exact non-blank typed decision-source event id in a complete tuple |
| `d3_business_fence_digest TEXT NOT NULL DEFAULT ''` | immutable lowercase 64-hex digest of canonical `d3_business_fence_v1` over typed current plan/review/gate/base-intent pins and the four typed command predecessor columns at command creation; JSON, model text, and ambient context are forbidden inputs |
| `claim_selection_generation BIGINT NOT NULL DEFAULT 0` | non-negative exact-command scheduler selection lineage; each new/reselected authority increments it once before factory mint |
| `consumed_claim_authority_id TEXT NOT NULL DEFAULT ''` | empty for a fresh selection; Stage A atomically installs the winning opaque authority id and never accepts it again |
| `claim_generation BIGINT NOT NULL DEFAULT 0` | non-negative; successful claim increments exactly once; no wrap; overflow fails closed before mutation |
| `claim_token_digest TEXT NOT NULL DEFAULT ''` | empty or lowercase 64-hex SHA-256; only an active fenced claim may retain a non-empty value |
| `control_epoch BIGINT NOT NULL DEFAULT 0` | non-negative; an accepted invalidating executable-input/control/requeue/reopen/recovery transition increments exactly once; first result-terminalization retains it; no wrap |
| `heartbeat_sequence BIGINT NOT NULL DEFAULT 0` | non-negative occurrence number within one claim generation; reset to zero only when a new generation is minted |
| `last_heartbeat_id TEXT NOT NULL DEFAULT ''` | empty or the last repository-issued occurrence id; cleared on new claim and terminal/invalidation |
| `terminal_event_id TEXT NULL` | null until a `succeeded`/`failed_terminal` result UoW; immutable while the command is result-terminal; only a registered reopen may clear it before requeue, and no transition may overwrite a present value |
| `terminal_outcome_digest TEXT NULL` | null until a `succeeded`/`failed_terminal` result UoW; immutable lowercase 64-hex digest while result-terminal; only the same registered reopen may clear the pair before requeue, and no transition may overwrite a present value; a local pair check requires both terminal fields null or both non-null |

The inventory above contains exactly **20 additive strict command columns**, including the four nullable
expected-predecessor columns. At command creation, the repository first locks the operation root and every applicable
typed plan/review/gate row in the section-6.5 global order. It validates the three scope values against the trusted
operation/runtime context, requires the existing `workflow_commands.operation_id` to equal the parent
`operation_runs.operation_run_id`, and requires the operation's positive coordination pin to exact-reference the typed
plan-review session. It then reserves/create-or-locks the non-claimable command identity with scope plus coordination
lineage in the all-participating-command segment. Only after that complete command segment does it enter the intent
segment, lock the typed base-intent/current verification lineage, and fill either the complete four-column predecessor
tuple or the all-null candidate-initial shape before commit; it never reads those values from command payload, and after
entering intent it may not insert or lock another command row.
The all-null shape remains only a candidate until Stage B proves no current phase, and no local `CHECK` can prove that
cross-row semantic fact. The repository uses those already-locked operation/plan/review/gate/base-intent pins to
canonicalize the exact `d3_business_fence_v1` contract in
section 8.1, and persists its digest; neither payload JSON nor ambient state participates. It does not add or dual-write
a second operation-link column. Idempotent upsert replay requires exact operation/scope/coordination/business-digest equality; scope never
changes on retry, resume, reclaim, recovery, or deployment. If a canonical operation scope is absent, strict D3 command
creation fails closed rather than inventing `default` or using the current worker environment. Missing/NULL/non-positive
coordination lineage, missing typed business pins, or a digest mismatch also fails closed. After insertion, the command
row is the claim-authority source of truth for scope, coordination, and the frozen business-fence digest; downstream rows
copy and compare them rather than re-derive from ambient or JSON. This decision does not claim that those columns already
exist on `operation_runs`; the future command creator must receive trusted, owner-issued pins and cannot serve D3 until
their sources are durable and equality-checked.

### 5.1 Canonical registry-issued pre-claim `ClaimAuthority`

`DEFAULT_COMMAND_TYPE_SPECS` / `CommandTypeSpec` is the existing canonical command-type-to-owner registry and
`command_type_manifest()` is derived from it. D3b extends that same checked-in entry; it does not add a parallel D3
registry. Each strict entry must pin these non-secret authority fields:

- exact `command_type` and expected `owner`;
- `claim_fence_policy in {legacy_unfenced, scoped_session_bootstrap_v1, d3_v1}`; only the source session-create
  command uses `scoped_session_bootstrap_v1`, positive-review strict D3 entries use `d3_v1`, and the command row's
  `command_type` is immutable;
- `stage_policy in {required_membership, optional_membership, forbidden}` plus canonical sorted `allowed_stage_ids`;
  every strict D3 executable entry uses `required_membership` with a non-empty set;
- allowed `activity_types`, terminal event family/types, transport kinds, and effect kinds;
- activity-spine requirement and provider-after-start mode;
- repository-owned lease duration, heartbeat interval, and maximum renewal horizon;
- authority schema version, `terminal_provenance_policy_digest`, and the canonical digest of every field above,
  including claim-fence policy, stage policy, stage ids, and the digest derived from the unique applicable
  `TERMINAL_PROVENANCE_SPECS` manifest. The existing command `claim_authority_spec_digest` and exposure policy pin
  preserve that historical digest; no additive command column is introduced for it.

The source bootstrap policy is not a weak `d3_v1` entry: it has its own factory/context/predicate in §5.2 and cannot
claim, dispatch, or terminalize through the normal positive-review path. The migration generator derives two disjoint,
hash-bound population manifests from the same registry: `scoped_session_bootstrap_command_types_v1` is the sorted exact
set whose `claim_fence_policy=scoped_session_bootstrap_v1`, and `strict_d3_command_types_v1` is the sorted exact set whose
`claim_fence_policy=d3_v1`. Migration C materializes both literal sets plus their registry-manifest digests and the fast
preflight verifies registry/DDL parity. Existing immutable `workflow_commands.command_type` is therefore the physical
population discriminator for both fenced branches. Empty/non-empty scope or digest sentinels are never used as that
discriminator, because a malformed fenced row must not escape a constraint by clearing a digest. A new or changed
bootstrap or `d3_v1` registry entry remains disabled until a migration updates its matching materialized manifest;
handwritten or runtime table lookups inside a PostgreSQL `CHECK` are forbidden. A new or changed terminal provenance
policy remains inactive until its preflight proves exactly one
applicable spec entry and both the command-policy and terminal-provenance manifest digests are installed. Old policy/
spec entries remain immutable and retained until no active **or retained** command, exposure, receipt, event,
source-intent tuple, quarantine/tombstone, or cost/audit row pins their historical digest.

At process composition, the private registry/scheduler owner installs an unexported factory used by the trusted
exact-command selection path; owner wrappers receive no startup-minted or standing authority. The selection repository
locks one claimable command and first proves that any prior execution lease or exact-selection reservation is absent or
expired at repository time. Only that selection UoW increments `claim_selection_generation`, then clears
`consumed_claim_authority_id` in the same atomic update, and uses the existing command
`lease_owner/lease_expires_at` fields to persist a short
exact-selection reservation. A second selection cannot replace it before repository-time expiry; after expiry it must
increment the selection generation.
The returned authority expiry exactly equals that persisted reservation expiry. Only then, after proving the operation scope,
does the factory server-mint one fresh sealed immutable `ClaimAuthority` and pass it to the registered owner invocation
for that selected command. A given `(command_id, claim_selection_generation)` can be issued at most once by that
factory. It is a pre-claim capability, not a structure returned after claim or a long-lived wrapper credential. Its constructor,
deserializer, and issuer are not exported; strings, dicts, row contents, caller text, and public manifests cannot create
it. The issuer binds a private authority id/seal to the exact selected command id/type, checked-in registry revision/
digest, expected owner, the exact selected row `stage_id` admitted by the registry stage policy, allowed
activity/event/transport/effect types, trusted durable operation scope, exact coordination review lineage, immutable
typed expected-predecessor columns and business-fence digest, worker identity, lease identity, selection
generation/expiry, expected pre-claim `workflow_commands.attempt`, expected current control epoch, and
lease/heartbeat policy. A wrapper does
not request an authority at all; the scheduler passes the exact selection's one-use authority into the registered
invocation. Knowing a `command_id` or holding the wrapper across operations is insufficient to call Stage A.

```text
ClaimAuthority {
  authority_id                 # opaque process-private capability identity
  issuer_revision
  issuer_digest
  expected_operation_id        # equals operation_runs.operation_run_id
  expected_command_id
  expected_command_type
  expected_command_owner
  expected_stage_id             # exact selected row stage; strict D3 is non-blank
  stage_policy
  allowed_stage_ids
  allowed_activity_types
  allowed_terminal_event_types
  allowed_transport_kinds
  allowed_effect_kinds
  trusted_runtime_namespace
  trusted_provider_mode
  trusted_workspace_id
  trusted_scope_digest
  expected_coordination_plan_review_id
  expected_predecessor_intent_id
  expected_predecessor_phase_generation
  expected_predecessor_source_control_epoch
  expected_predecessor_decision_source_event_id
  expected_d3_business_fence_digest
  worker_identity
  lease_identity
  expected_claim_selection_generation
  expected_command_attempt       # exact pre-claim value; Stage A must commit +1
  expected_control_epoch
  authority_expires_at         # repository-time deadline, seal-bound and caller-immutable
  activity_spine_requirement
  provider_after_start_mode
  claim_authority_spec_digest
  lease_duration
  heartbeat_interval
  maximum_renewal_horizon
}
```

Stage A accepts only `(sealed ClaimAuthority, command_id)`; the argument must equal
`authority.expected_command_id`, and claimant/owner/type/scope/lease fields are read from the authority, never separate
arguments. The workflow repository validates the private seal with the process issuer, then in one transaction proves:
row id/type equal the selected authority; row owner, exact `stage_id`, registry stage policy/membership, activity policy,
and pinned spec digest equal the authority and
canonical registry entry; row operation/scope equals the trusted authority scope; worker/lease identity equals the
issuer binding; row and operation coordination lineage equal the sealed positive BIGINT review id; row business-fence digest
and all four typed expected-predecessor columns equal the sealed expected values; row selection generation,
pre-claim `attempt`, and control epoch equal the sealed expected values; the selection reservation lease
owner/expiry equal the authority and repository time is strictly before `authority_expires_at`;
`consumed_claim_authority_id=''`; and the row is claimable by that exact reservation. The winning CAS increments
`attempt` exactly once together with `claim_generation`, token/lease install, and consumed-authority id; its receipt
carries that post-claim attempt. Any attempt drift is stale before writes.
The same conditional update that mints the claim installs `consumed_claim_authority_id=authority.authority_id`; a lost,
expired, previously consumed, or concurrently duplicated authority returns
`not_applied(reason=stale_claim, detail_code=claim_authority_rejected)` with zero write
and can never be refreshed in place. Only after the CAS wins does Stage A return a private `ClaimReceipt` containing
the current `ClaimIdentity` (including raw token) plus authority id/issuer/spec digests. It does not mutate the pre-claim
authority into a current claim.

`consumed_claim_authority_id` is deliberately not a permanent, multi-generation audit log. It is the one-use slot for
the current `claim_selection_generation`; a reselection can clear it only in the same atomic update that first advances
that generation. Historical authority-consumption audit, if activation policy later requires it, is an explicit future
residual that must name its own durable owner, idempotency, privacy, and retention contract. D3b neither invents nor
claims such a historical SOT.

Every Stage-B, heartbeat, transport, terminal, and effect repository accepts the sealed authority plus its matching
current receipt and checks the requested type. Merely comparing a row to caller text, comparing `row.owner` to itself,
or constructing a lookalike dataclass is not authorization. Registry absence, seal/digest drift, type mismatch, owner or
stage mismatch, forbidden activity/transport/effect, worker mismatch, coordination/business-fence mismatch, selection mismatch/expiry/consumption, or scope mismatch returns
`not_applied(reason=stale_claim, detail_code=claim_authority_rejected)` before claim/effect writes. A queued row pinned to an older registry digest is not silently
reinterpreted after deployment; an owner migration/control transition must create a new command identity under the
current entry.

The public `command_type_manifest()` may continue to expose non-secret static semantics, but it never contains a raw
token, token digest, live `ClaimAuthority`, or enough material to construct one. D3 command entries remain absent until
their later implementation; therefore this decision does not increase the served population.

### 5.2 Durable scoped review-session root and propagation chain

The scoped review-session repository is the sole strict-D3 scope/coordination issuer. The existing baseline
`plan_review_sessions` table has no workspace, causal, or idempotency columns, so its legacy
`create_plan_review_session(...)` and JSON-scan lookup/reuse paths cannot seed a strict D3 operation. The future private
`create_or_exact_replay_scoped_plan_review_session(...)` accepts only the closed
`ScopedSessionBootstrapContext = ScopedSessionCreateContext | ScopedSessionCommittedReplayContext` union below.
Create mode carries both the sealed bootstrap authority and matching Stage-A receipt; committed-replay mode carries
only server-reconstructed, non-secret immutable identity and can never create. Neither mode accepts or manufactures a
normal `ClaimAuthority`, normal `ClaimReceipt`, caller review id, review/gate row, or `d3_business` context.
The source review-request command has the separate checked-in
`claim_fence_policy=scoped_session_bootstrap_v1`. After the trusted source-session-create selector locks and validates
the exact source OperationRun, command, immutable creation-source event, and canonical plan—but **no ActivityAttempt,
which does not exist yet**—the
private composition-installed `mint_scoped_review_session_bootstrap_authority(...)` factory seals one non-serializable
`ScopedReviewSessionBootstrapAuthority`. It reads `runtime_namespace + provider_mode` from the trusted runtime owner,
takes `workspace_id` from the authenticated owner context, and binds the source command's expected next claim
generation, current control epoch, **pre-claim command attempt**, source event, and immutable plan pins. The OperationRun,
command, event, plan, and
authenticated workspace must exact-equal; foreign tenant/scope, stale/cancelled source state, or attempt/plan drift
fails before authority mint or storage. The bootstrap selection UoW increments the source command's
`claim_selection_generation`, clears its current-selection consumed-authority slot, and persists the exact short
selection reservation before mint. The bootstrap Stage-A CAS then consumes that authority exactly once, mints the
sealed authority's expected next `claim_generation` and raw token, and returns the private immutable
`ScopedReviewSessionBootstrapReceipt` **before** session creation. That receipt exact-copies the post-claim command
attempt and is current-claim authorization, not an ActivityAttempt or session creation result.

```text
ScopedReviewSessionBootstrapAuthority {
  authority_id, issuer_revision, issuer_digest
  claim_fence_policy = scoped_session_bootstrap_v1
  claim_authority_spec_digest
  trusted_runtime_namespace, trusted_provider_mode, trusted_authenticated_workspace_id, trusted_scope_digest
  expected_source_operation_run_id
  expected_source_workflow_command_id, expected_source_command_type, expected_source_command_owner, expected_source_stage_id
  expected_claim_selection_generation
  expected_source_claim_generation             # exact next generation Stage A must mint
  expected_source_control_epoch
  expected_source_command_attempt_before_claim
  worker_identity, lease_identity
  creation_source_event_id
  creation_plan_id, creation_plan_revision, creation_plan_bundle_digest
  creation_idempotency_key
  allowed_effect = scoped_review_session_create
  authority_expires_at
  lease_duration, heartbeat_interval, maximum_renewal_horizon
}

ScopedReviewSessionBootstrapReceipt {
  bootstrap_authority_id, bootstrap_authority_digest
  issuer_revision, issuer_digest, claim_authority_spec_digest
  runtime_namespace, provider_mode, authenticated_workspace_id, scope_digest
  source_operation_run_id
  source_workflow_command_id, source_command_type, source_command_owner, source_stage_id, source_command_status
  claim_selection_generation
  claim_generation, control_epoch, source_command_attempt
  claim_token                                  # private raw capability; never persisted/serialized/logged
  lease_owner, lease_identity, lease_expires_at, heartbeat_sequence
  creation_source_event_id
  creation_plan_id, creation_plan_revision, creation_plan_bundle_digest
  creation_idempotency_key
}

ScopedSessionCreateContext {
  mode = create
  authority: ScopedReviewSessionBootstrapAuthority
  receipt: ScopedReviewSessionBootstrapReceipt
  authenticated_workspace_id
  expected_source_event_id
  expected_plan_id, expected_plan_revision, expected_plan_bundle_digest
  expected_creation_idempotency_key
}

ScopedSessionCommittedReplayContext {
  mode = committed_exact_replay
  trusted_runtime_namespace, trusted_provider_mode, authenticated_workspace_id, trusted_scope_digest
  expected_source_operation_run_id, expected_source_workflow_command_id
  expected_source_event_id
  expected_plan_id, expected_plan_revision, expected_plan_bundle_digest
  expected_creation_idempotency_key
}

ScopedSessionBootstrapContext = ScopedSessionCreateContext | ScopedSessionCommittedReplayContext

ScopedReviewSessionCreateResult {
  runtime_namespace, provider_mode, workspace_id, scope_digest
  creation_idempotency_key
  review_id
  session_created_event_id, session_created_terminal_outcome_digest
  creation_source_workflow_command_id
  source_activity_run_id, source_activity_attempt_id
  source_terminal_event_id, source_terminal_outcome_digest
}
```

The authority/receipt constructors and deserializers are private; the authority is one-use and the receipt can authorize
only the specialized bootstrap Stage-B/session-created aggregate. `verify_scoped_session_bootstrap(context, locked_rows)`
is the only bootstrap predicate and dispatches exhaustively on the closed context variant. For `mode=create` it
exact-checks both sealed values against each other and the locked current row:
issuer/spec/authority identity, authenticated workspace, runtime scope, source operation/command type-owner-stage,
selection generation/consumed authority id, raw-token digest, current claim generation/control epoch/**post-claim
command attempt**, lease owner/identity/expiry/heartbeat, immutable source event, plan revision/bundle digest,
idempotency key, and authority expiry. It has no transport, provider/model,
grant/cost, domain, record, child-command, or generic terminal permission. A bootstrap authority/receipt cannot be
passed to normal Stage A, `verify_d3_business_fence(...)`, or `d3-dispatch-v2`.

The bootstrap receipt contains no ActivityRun/ActivityAttempt, `review_id`, session-created event id, or session terminal
digest. Those identities do not exist when Stage A returns it and belong only to the later specialized Stage-B/
`ScopedReviewSessionCreateResult` committed aggregate.

The additive physical session/root shape is:

```text
plan_review_sessions.runtime_namespace TEXT NOT NULL DEFAULT ''
plan_review_sessions.provider_mode TEXT NOT NULL DEFAULT ''
plan_review_sessions.workspace_id TEXT NOT NULL DEFAULT ''
plan_review_sessions.scope_issuer TEXT NOT NULL DEFAULT ''       # strict D3: plan_review_session
plan_review_sessions.scope_digest TEXT NOT NULL DEFAULT ''       # SHA-256(scope_v1 canonical tuple)
plan_review_sessions.creation_source_workflow_command_id TEXT NOT NULL DEFAULT ''
plan_review_sessions.creation_source_event_id TEXT NOT NULL DEFAULT ''
plan_review_sessions.creation_plan_id TEXT NOT NULL DEFAULT ''
plan_review_sessions.creation_plan_revision BIGINT NOT NULL DEFAULT 0
plan_review_sessions.creation_plan_bundle_digest TEXT NOT NULL DEFAULT ''
plan_review_sessions.creation_idempotency_key TEXT NOT NULL DEFAULT ''

operation_runs.runtime_namespace TEXT NOT NULL DEFAULT ''
operation_runs.provider_mode TEXT NOT NULL DEFAULT ''
operation_runs.scope_issuer TEXT NOT NULL DEFAULT ''              # exact-copy from scoped review
operation_runs.scope_digest TEXT NOT NULL DEFAULT ''              # exact-copy from scoped review
operation_runs.coordination_plan_review_id BIGINT NULL
                                                                    # strict D3: same positive review_id
```

The repository derives the last field as lowercase SHA-256 over this fixed canonical identity:

```text
scoped_plan_review_session_v1 {
  schema = "scoped-plan-review-session-v1"
  runtime_namespace, provider_mode, workspace_id, scope_digest
  creation_source_workflow_command_id
  creation_source_event_id
  creation_plan_id, creation_plan_revision, creation_plan_bundle_digest
}
```

`(scope_digest, creation_idempotency_key)` is the scope-aware unique key. Before a review id exists, the private
repository acquires the dedicated advisory key whose canonical schema is
`scoped-session-bootstrap-lock-v1(runtime_namespace, provider_mode, workspace_id, scope_digest,
creation_idempotency_key)`. The key is the signed first 64 bits of SHA-256 over those length-delimited components; it
contains no review id or gate identity and is distinct from `d3-dispatch-v2`. For `mode=create`, under that lock the UoW
locks the existing `source OperationRun -> source command -> immutable creation-source event -> canonical plan` rows in
the fixed bootstrap order, calls `verify_scoped_session_bootstrap(...)` **before any write**, then creates one exact
claim-bound ActivityRun/ActivityAttempt from the receipt's generation/epoch/post-claim command attempt and atomically
insert-or-selects the session, appends the sole immutable session-created event, and installs that exact event id/
outcome-digest pair as the source review-request command's terminal pair. ActivityRun/Attempt, session, event, and source
terminal pair all commit or roll back together. It clears the source claim token/lease as part of that terminal aggregate and returns the
immutable `ScopedReviewSessionCreateResult`; the preexisting `ScopedReviewSessionBootstrapReceipt` is consumed as
authorization and is never rewritten into the result. No transport, domain,
grant/cost, child command, or unrelated event is writable in this UoW. On conflict it requires exact equality of every
session, created-event, source ActivityRun/Attempt, source-command-terminal, scope, causal, plan, status-initialization, authority/receipt non-secret
identity, and digest field. Exact replay returns the same positive BIGINT `review_id`, created event, terminal source-
command aggregate, and `ScopedReviewSessionCreateResult`; it does not return a newly minted receipt. Because the
successful commit cleared the source token/lease and a process crash destroys raw capability memory, post-commit replay
uses only `ScopedSessionCommittedReplayContext`: under the same bootstrap key it reads and exact-compares the already
committed session, ActivityRun/Attempt, events, terminal pair, authenticated workspace/scope, source ids, plan pins, and
idempotency identity. It never calls the create branch, never reconstructs authority/receipt/token, never revives the
claim, and writes nothing. Any same-key field mismatch returns
`scoped_review_session_identity_collision` with zero session/event/command write. If committed replay finds no aggregate,
it returns `scoped_review_session_not_found` with zero writes; a new create must wait for current-claim recovery/reselection
and present a newly minted authority/receipt. A stale/cancelled authority, expired bootstrap authority,
non-current/mismatched bootstrap receipt, generation/epoch/post-claim-command-attempt/lease drift,
cross-tenant/scope mismatch, failed bootstrap predicate, or injected fault on the new-create path writes zero session/
event/command rows. An exact post-commit replay remains readable after authority/lease expiry only under the complete
immutable collision check above.
A crash after commit and before reply therefore replays from durable identity to the same aggregate without any raw
capability, never a second session or terminal event. A crash after Stage A but before the create UoW waits for lease
expiry/reselection and a new authority/receipt; since no ActivityRun/Attempt/session/event was committed, retry uses the
same scope/idempotency key and creates at most one aggregate. Caller
review ids/keys, mutable plan JSON, request payloads, and a scan for a similar pending JSON session are not identity and
cannot enter strict D3.

The scoped review session is created first and is the physical scope root. Only after the committed create result's
positive review id, session-created event, and source terminal pair exist may strict D3 OperationRun creation enter normal positive-review `d3-dispatch-v2` coordination
and `d3_v1`/`d3_business` authorization. OperationRun creation locks that
session and requires its scope, causal/idempotency identity, and plan pins to be complete/canonical; it then exact-copies
the scope values into the operation's namespace/mode/existing workspace/issuer/digest
columns and pins the same `plan_review_sessions.review_id` as `coordination_plan_review_id`. It may not independently
remint scope from an operation payload or ambient environment. Internal poll-mode D3 creation uses this typed scoped
session path even while served Agent tools remain zero. A retry/recovery child OperationRun copies and equality-checks
the exact root/parent namespace, mode, workspace, issuer, scope digest, and coordination review lineage. It never
remints scope or coordination from the child's ambient deployment mode, root-intent id, payload, JSON review fields, or
authenticated request defaults. An unscoped/legacy-creator review session, missing causal/idempotency pins,
missing/NULL/non-positive review lineage, or any
review/operation scope mismatch makes strict D3 operation creation ineligible.

An action-backed scoped review/root is a separate fail-closed case: current `agent_actions` rows have no physical namespace/mode pins,
so their workspace/linkage alone cannot be compared as a complete scope. Action-backed strict D3 remains disabled until
the action owner satisfies the **Plan section 6 item 6 action-root durable-scope gate** by supplying durable namespace,
provider mode, workspace, and canonical scope-input/digest pins that the scoped review-session repository can
equality-check before it issues the session scope; the OperationRun UoW then only exact-copies that issued scope. This is a
separate scoped prerequisite with no OB-ID; it must not be credited to or conflated with OB-10.4. D3b adds no action-owned
columns and closes neither gate. Current rows with a missing scoped-session/operation scope, and every action-backed row before
that owner migration, remain legacy-unscoped and cannot seed a strict D3 command.

Every downstream row physically copies the scoped review/operation root at its own creation point:

| Table | Additive strict columns | Required parent/equality |
|---|---|---|
| `workflow_commands` | reuse existing `operation_id`; add namespace, mode, workspace, `scope_digest`, coordination review id, authority-spec digest, four typed expected-predecessor pins, D3 business-fence digest, selection/consumed-authority, and claim/epoch/heartbeat columns from this section | `workflow_commands.operation_id = operation_runs.operation_run_id` plus exact operation scope/coordination lineage; command creation exact-copies the typed predecessor shape before hashing it; canonical registry type/owner/stage/digest; no operation-link alias |
| `workflow_activity_runs` | namespace, mode, existing workspace, `scope_digest`, coordination review id, authority-spec and business-fence digests | exact command operation/scope/type/coordination/business fence; `command_id` already physical |
| `workflow_activity_attempts` | `operation_run_id`, namespace, mode, existing workspace, `scope_digest`, coordination review id, authority-spec and business-fence digests, `claim_generation`, `command_attempt`, `control_epoch` | exact ActivityRun + command + current `ClaimIdentity`; `command_attempt` exact-copies the post-claim `workflow_commands.attempt`; token/digest never copied |
| `workflow_events` terminal rows | reuse existing `operation_id`; add namespace, mode, workspace, `activity_run_id`, generation, epoch, authority/business digests, terminal outcome digest, and typed response-receipt/attempt-failure-receipt/no-exposure provenance | `workflow_events.operation_id = workflow_commands.operation_id = operation_runs.operation_run_id`; exact command/attempt plus exactly one registered terminal provenance variant in the terminal UoW; event id/digest atomically copied to command |
| `verification_intent` | namespace, mode, workspace, operation/review/source-command/source-attempt binding, source generation/epoch, and typed business/predecessor pins | exact source verification Stage B and same coordination review lineage; OB-10.1 remains carried until implemented |

Creation UoWs compare every copied field; no downstream repository defaults a missing workspace to `default`, reads
ambient environment, or trusts payload JSON. Composite scope indexes and foreign keys described in section 11 make the
operation -> command -> ActivityRun -> ActivityAttempt chain mechanically auditable instead of join-by-convention.

### 5.3 Heartbeat occurrence identity

A live claim can renew only through a repository-issued `HeartbeatOccurrence`:

```text
HeartbeatOccurrence {
  command_id
  claim_generation
  expected_heartbeat_sequence
  heartbeat_id             # repository-issued UUIDv7
}
```

The heartbeat UoW validates the complete `ClaimAuthority`, exact current sequence, and registry-owned renewal horizon;
then it increments `heartbeat_sequence` once, stores `heartbeat_id`, and computes expiry from repository time. The caller
cannot provide expiry. An exact retry with the same heartbeat id and expected sequence returns the already committed row
without extending again; a different id at an old sequence, an occurrence from another generation, an expired claim, or
an occurrence beyond the maximum renewal horizon is rejected with zero write. The UoW returns a refreshed private
`ClaimReceipt`/`ClaimIdentity` snapshot with the new sequence/expiry; the sealed pre-claim authority remains immutable.
This prevents delayed/replayed heartbeat messages from silently lengthening a lease.

The raw token algorithm is also locked:

1. the native claim primitive creates 32 bytes with a cryptographically secure random generator;
2. the private `claim_token` is the lowercase 64-hex encoding of those bytes;
3. the stored verifier is `sha256(raw_token_bytes).hexdigest()`;
4. the raw token is returned only in the private `ClaimIdentity` object;
5. a process crash that loses the raw token cannot recover it from PG and must wait for lease expiry/reclaim.

This design does not require a new claim-receipt table. The command row is the single current-state authority, while the
raw capability remains non-durable. Recovery never adopts an old claim by reading a secret back from storage; it mints a
new generation and token after the old lease expires.

The private receipt is logically:

```text
ClaimIdentity {
  runtime_namespace
  provider_mode
  workspace_id
  scope_digest
  operation_id                # exact operation_runs.operation_run_id
  command_id
  command_type
  command_owner
  stage_id
  coordination_plan_review_id
  claim_authority_spec_digest
  expected_predecessor_intent_id
  expected_predecessor_phase_generation
  expected_predecessor_source_control_epoch
  expected_predecessor_decision_source_event_id
  d3_business_fence_digest
  lease_owner
  claim_generation
  attempt                     # exact post-claim workflow_commands.attempt
  claim_token
  control_epoch
  heartbeat_sequence
  last_heartbeat_id
  lease_expires_at
}
```

All receipt fields are returned from the committed command row except the raw token. The command-creation owner supplies
and freezes scope before any claim exists; claim callers cannot supply or override generation, attempt, token, epoch, owner,
namespace, mode, workspace, scope/coordination/business pins, stage, or expiry.

## 6. Two-stage safe transaction boundary

### 6.1 Stage A — atomic claim mint

The single native PG claim primitive accepts only a sealed pre-claim `ClaimAuthority` plus `command_id` and performs one
conditional `UPDATE ... RETURNING` transaction. Authority issuance is not Stage A: it already occurred once after the
repository's exact-command selection UoW. Stage A:

1. validate the authority seal, issuer/registry/spec digests, allowed command type, expected owner, exact selected
   `stage_id` against the registry `stage_policy/allowed_stage_ids`, trusted operation scope, positive exact coordination
   review lineage, immutable D3 business-fence digest, bound worker/lease identity, expected selection generation,
   pre-claim command attempt, control epoch, unconsumed authority id, and
   repository-time expiry; then lock/qualify the exact matching command row by its physical namespace, provider mode,
   workspace, command id, actual `owner`, claimable status, and `not_before_at`; require its exact persisted selection
   reservation owner/expiry to equal the authority and to remain strictly unexpired at repository time;
2. atomically overwrite that exact current selection reservation with the execution lease while setting
   `status='claimed'`, `lease_owner`, `lease_expires_at`, heartbeat, and updated timestamp; Stage A never requires the
   reservation lease to be absent or expired;
3. increment existing `attempt` for retry accounting;
4. increment `claim_generation` exactly once;
5. generate a new raw token and store only its digest;
6. atomically set `consumed_claim_authority_id` to the exact sealed authority id, making the authority one-use;
7. preserve the authority-bound current `control_epoch`;
8. reset heartbeat occurrence to `(0, '')` for the new generation;
9. return the committed row, private raw token, and matching authority identity as a sealed current `ClaimReceipt` whose
   identity payload is `ClaimIdentity`, including the exact post-increment command attempt.

No ActivityRun, ActivityAttempt, verification intent, provider call, event, child command, artifact, EntityDelta, or
domain row is created in Stage A.

### 6.2 Stage B — claim-bound execution start and owner convergence

The D3 command owner then performs one specialized PG UoW using the matching sealed authority + current receipt:

1. acquire the section-6.5 common advisory coordination lock, then lock the operation root, any operation-touched
   plan/review/gate aggregates, and **all participating `workflow_commands` rows** in deterministic ascending
   `(scope_digest, operation_id, command_id)` order. That command segment includes the current owner command plus every
   source, record, resume, supersession, and current idempotency-target command row touched by the transaction. Any
   missing command identity is reserved/create-or-locked in that segment before the transaction enters intent rows;
   Stage B skips untouched optional aggregates but never starts later, returns to this segment, or inserts/locks another
   command after intent;
2. validate registry authority, exact stage membership, complete `ClaimIdentity`, coordination/business pins, current
   post-claim command attempt, heartbeat/lease, and requested activity type on the exact command row;
3. lock the root verification-intent lineage and its exact current predecessor after the command rows; both Stage B and
   asynchronous owner supersession call the same repository primitive with the physical expected predecessor tuple
   `expected_predecessor_intent_id` + `expected_predecessor_phase_generation` +
   `expected_predecessor_source_control_epoch` + `expected_predecessor_decision_source_event_id`;
   after all phase-required rows are locked, invoke the section-8.1 centralized predicate with `phase=stage_b` before
   any ActivityRun/Attempt, intent, event, claimable/terminal command, or source-tuple write. The only earlier SQL
   mutation allowed is a deterministic non-claimable command reservation under the section-8.1 rollback rule;
4. validate or create the exact owner-scoped `workflow_activity_run` identity and one claim-bound
   `workflow_activity_attempt` whose scope/generation/epoch/**post-claim command attempt**/spec digest equal the command;
5. for a new/reclaimed source verification phase, exact-CAS the current predecessor against that stored tuple, atomically
   supersede it, and create the successor intent/source binding for this command/generation/attempt; the successor
   physically stores the same predecessor tuple as immutable lineage, and a missing/different predecessor generation,
   control epoch, or source event is
   `not_applied(reason=business_precondition_conflict, detail_code=predecessor_mismatch)` with zero write;
6. for an independent record command, leave the source binding immutable and create only the record command's own
   ActivityRun/Attempt execution identity;
7. optionally move `claimed -> running` in the same transaction;
8. commit ActivityRun, ActivityAttempt, old-intent supersession, successor intent or record execution, and running state
   together.

The physical predecessor tuple has exactly two legal shapes. The initial `stage_b` command stores an **all-SQL-NULL**
tuple and, under the coordination lock and lineage row lock, must prove that no current phase exists for the exact
`(scope_digest, coordination_plan_review_id, base_intent_lineage)` before it creates the first phase. Every successor or
no-successor convergence command stores a **complete** tuple: all four columns non-null, both ids non-blank,
`phase_generation > 0`, and `source_control_epoch >= 0`. Any half-sentinel tuple, an all-sentinel tuple when a current
phase exists, or a complete tuple when no exact predecessor exists is
`not_applied(reason=business_precondition_conflict, detail_code=predecessor_shape_or_role)` with zero writes.
The all-null shape is not a wildcard and is legal only for initial Stage B.

Any failure rolls back the entire Stage B UoW. A control transition between Stage A and Stage B changes the stored epoch,
token verifier, status, or lease; Stage B then returns
`not_applied(reason=stale_claim, detail_code=stage_b_claim_invalidated)` with zero durable writes. Exact Stage-B replay uses
`scope_digest + coordination_plan_review_id + command_id + claim_generation + activity_type + expected predecessor tuple + d3_business_fence_digest` and returns the committed aggregate without a second
attempt, supersession, successor intent, or running transition.

#### 6.2.1 Source verification binding

The verification intent's immutable core source binding is:

```text
source_verification_command_id
source_claim_generation
source_control_epoch
source_command_attempt
source_activity_run_id
source_activity_attempt_id
source_claim_authority_spec_digest
```

Its separately append-once terminal tuple is:

```text
expected_source_terminal_status
expected_source_terminal_event_id
expected_source_terminal_outcome_digest
expected_source_terminal_transport_variant       # exposure | attempt_failure | no_exposure
expected_source_terminal_provenance_policy_digest
expected_source_response_spec_digest
expected_source_transport_response_receipt_id
expected_source_transport_attempt_failure_receipt_id
expected_source_dispatch_exposure_id
expected_source_physical_call_index
expected_source_provider_call_id_state            # response or failure variant's registered enum
expected_source_provider_call_id
expected_source_model_invocation_envelope_ref
expected_source_model_invocation_envelope_digest
expected_source_terminal_reason
expected_source_response_occurrence_id
expected_source_canonical_response_digest
expected_source_canonical_result_digest
expected_source_result_artifact_ref
expected_source_result_artifact_digest
expected_source_failure_occurrence_id
expected_source_failure_code
expected_source_failure_spec_digest
expected_source_canonical_failure_digest
expected_source_retry_policy_revision
expected_source_retry_disposition
expected_source_failure_artifact_ref
expected_source_failure_artifact_digest
expected_source_no_exposure_spec_digest
```

The core tuple never changes. The terminal tuple is initially all-null and is appended only by the source command's
section-6.6 terminal UoW under an exact expected-null CAS; once present it is immutable. For `exposure`, every receipt,
exposure/call, envelope, response occurrence/digest, result digest, and complete-or-absent result artifact pair is copied
from one exact locked `TransportResponseReceipt`, including the historical policy/response-spec digests and terminal
reason;
`provider_call_id` may be null only with the receipt's registry-admitted `missing_by_registered_transport` state. For
`attempt_failure`, the tuple exact-copies one locked `TransportAttemptFailureReceipt`: committed exposure/call identity,
provider-call state/id, failure occurrence/code/spec/canonical digest, `retry_policy_revision`, terminal
`retry_disposition`, and required failure artifact pair; response receipt, ModelInvocationEnvelope, response
occurrence/digests, result digest, and result artifact fields are SQL NULL. For `no_exposure`, all response/failure
receipt, exposure/provider/envelope/response/result/failure/retry fields are SQL NULL, and
`expected_source_response_spec_digest` and `expected_source_terminal_reason` are SQL NULL, while
`expected_source_no_exposure_spec_digest` exact-copies the admitted `NoExposureTerminalSpec` under the historical
terminal-provenance policy digest; the command registry must
explicitly admit that outcome. A half-populated variant fails closed. The tuple binds provenance, does not
keep the terminal source claim alive, and never stores token material. A registered source-command reopen may clear the
command row's terminal pair before requeue, but it cannot rewrite an intent's already appended historical source tuple;
the successor phase receives a new intent/source binding. The canonical source core remains exactly the seven fields above.
Its surrounding typed intent scope envelope separately carries operation/scope,
`coordination_plan_review_id = plan_review_sessions.review_id` as one positive BIGINT, and the immutable command
business-fence digest; record exact-compares
that envelope without expanding or aliasing the source core. The command link is always the existing `workflow_commands.operation_id`, which equals the root
`operation_runs.operation_run_id`.

Only a terminal `final_adjudication` source event may cause the reducer to plan the one
`company.identity.verification.record` command type. Its server-owned typed `record_outcome` discriminant is
`authorizable | awaiting_budget | needs_human | failed | timed_out`. An intermediate `evidence_insufficient` event with
an available grant plans only `search.expand`; it never plans record. When no grant is available, or execution ends in
failure, timeout, or needs-human, the verification owner first normalizes that result into `final_adjudication` with the
matching non-authorizable `record_outcome`; the reducer then maps it into the typed input of the independent record
command. Thus there is one record planner and one record command type, while every non-authorizable outcome remains
explicit rather than reaching record through an untyped catch-all route.

The record owner uses this exact closed mapping; every applied row emits the single typed domain event
`company_identity_verification_recorded` with the `record_outcome` discriminant and exact source terminal identity.
`not_applied` is not a table row because it is the typed zero-durable-write return for a stale/business predicate miss.

| `record_outcome` | Exact source condition | `verification_state` after record | `intent_state` after record | Owner event | Gate / resume effect |
|---|---|---|---|---|---|
| `authorizable` | final adjudication satisfies the complete manifest and every authorization predicate | `shadow_would_verify` | `applied` | `company_identity_verification_recorded(record_outcome=authorizable)` | gate remains blocking for the explicit Phase-2 human/promotion rule; no automatic resume |
| `awaiting_budget` | evidence remains insufficient; no usable grant was observed by the adjudication, and policy permits a later grant | exactly `pending`; never `needs_human` | `awaiting_budget` | `company_identity_verification_recorded(record_outcome=awaiting_budget)` | gate remains blocking and exposes only the recoverable grant action; post-apply gate watermark plus durable grant converge through §6.2.3 |
| `needs_human` | hard no-more-grant envelope/policy exhaustion, semantic ambiguity, protocol-invalid output, model fallback, policy-invalid result, or incomplete/non-authorizable manifest | `needs_human` | `applied` | `company_identity_verification_recorded(record_outcome=needs_human)` | gate remains blocking with a non-resumable human reason; no false grant affordance |
| `failed` | non-retryable verification execution failure | `failed` | `applied` | `company_identity_verification_recorded(record_outcome=failed)` | gate remains blocking; a later retry requires a new owner intent/command, not grant resume |
| `timed_out` | the **recorded verification outcome** reached its declared deadline/`max_wall` while the record claim and business fence remained current | `timed_out` | `applied` | `company_identity_verification_recorded(record_outcome=timed_out)` | gate remains blocking; later re-verification requires a new owner intent/command |

A control-plane timeout is deliberately not the `record_outcome=timed_out` row. The typed verification control owner,
under the same coordination lock and centralized predicate, transitions the current intent to `timed_out`, transitions
the current verification row to fail-closed `needs_human`, and emits the distinct
`company_identity_verification_control_timed_out` control event. It does not forge a source `final_adjudication`, plan a
record command, or emit `company_identity_verification_recorded`.

#### 6.2.2 Independent record-command claim

`company.identity.verification.record` is a distinct workflow command with its own registry authority, Stage-A claim,
generation/token/control epoch, ActivityRun, and ActivityAttempt. Its record/apply UoW must simultaneously validate:

- the record command's current sealed authority + receipt and still-live lease;
- the immutable source verification binding and exact committed source terminal status/event/outcome digest plus its
  complete response-receipt, attempt-failure-receipt, or registry `no_exposure` provenance variant.

The record UoW locks the source verification and record command rows plus every current idempotency-target command row
it actually touches as one complete participating-command set in ascending `(scope_digest, operation_id, command_id)`
order, the mandatory order for every multi-command UoW. Record does not create or lock the later resume command; it
never returns from intent to acquire a command row. It requires the
current source row's `operation_id`, scope, coordination review id, business-fence digest, `claim_generation`,
`control_epoch`, post-claim `attempt`, terminal status, `terminal_event_id`, and `terminal_outcome_digest` to exact-equal
the frozen intent binding (including `source_command_attempt`), and then
requires the referenced immutable event and exact response-receipt, attempt-failure-receipt, or no-exposure identity to
match every field of the append-once source tuple from section 6.6. The immutable event/evidence is read or
locked only in its documented final evidence segment; it does not add an earlier mutable lock. Reopened/retried source
status, an advanced source epoch, or a replaced/missing terminal pair therefore fails before any record/domain write;
the event pair alone is not accepted as proof of current source state.

After those two independent claim/provenance checks, the same UoW invokes the single section-8.1 centralized predicate
with `phase=record`; it does not repeat or weaken that predicate in a second branch. Record apply is not transport
dispatch, so the phase manifest does not require, lock, or consume a Tier-2 grant, cost reservation, physical-call
exposure, transport kind, or physical-call index. The durable intent/event/gate-watermark convergence in §6.2.3 closes
the grant race after record commits.

The record command never reuses the terminal source command's raw token, token digest, lease, or claim generation as its
own authorization. A valid source binding with a stale record claim writes nothing; a current record claim with a missing
or mismatched source terminal identity returns
`not_applied(reason=business_precondition_conflict, detail_code=source_terminal_mismatch)` and also writes nothing.

Record apply is not a transaction followed by terminalization. It is exactly the record-command specialization of the
section-6.6 terminal UoW, in **one PG transaction**. After the complete locks and claim predicate, that transaction first
calls `verify_d3_business_fence` with the closed `record` context to validate source/outcome/domain rules, then calls the
same evaluator with the closed `terminal` context to validate record-command terminal rules. Only then does one commit
write the declared `verification_state`, declared `intent_state`, the single typed
`company_identity_verification_recorded` physical workflow event, ActivityAttempt terminal state, command terminal
status/event pair, and all source-terminal validation plus idempotency evidence. That one physical event is both the
domain-owner event and the record command's workflow terminal event; its canonical typed payload contains
`record_outcome` and the exact source terminal identity, so there is no second event identity to reconcile. Its
scope-aware idempotency tuple is
`(scope_digest, coordination_plan_review_id, record_command_id, record_claim_generation,
source_terminal_event_id, source_terminal_outcome_digest, record_outcome)`.

A crash rolls every listed write back. Exact replay of the same tuple returns the full committed
verification/intent/event/attempt/command aggregate without a write; any tuple or canonical-payload collision returns
`not_applied(reason=business_precondition_conflict, detail_code=terminal_replay_conflict)` with zero writes. In
particular, `intent_state=applied` cannot coexist with a running record
command as an intermediate commit.

#### 6.2.3 Awaiting-budget grant convergence (no lost wakeup)

Grant issue and `record_outcome=awaiting_budget` converge through durable owner state, not a pre-gate command frozen to
old pins. The section-6.2.2 record/terminal UoW atomically writes `intent_state=awaiting_budget`, the single
`company_identity_verification_recorded` event, and `verification_intent.recorded_event_id`; it neither locks a grant nor
creates a resume command. An active `identity_search_budget_grant`, the awaiting intent plus recorded event id, and the
gate owner's applied-event watermark are the three durable convergence facts.

Exactly one registered command type performs the later work:
`company.identity.verification.resume_after_grant`. Its deterministic identity is
`resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>`.
The numeric component uses the section-6.5 no-sign/no-leading-zero BIGINT encoding. Its command/outbox idempotency tuple
contains no grant id or transient delivery id. One recorded event therefore has one stable identity; later gate/pin
drift is a conflict requiring a new intent/phase and cannot rebind the old key or rewrite its business digest.

The gate-event owner exposes one typed `maybe_plan_resume` entrypoint backed by the workflow-command repository:

1. `plan_review.identity_result.apply` acquires the common lock, locks operation/plan/review/gate, then reserves or
   exact-locks the deterministic resume-v2 row in the **all-participating-command** segment before locking intent/grant.
   A newly reserved row is non-claimable inside the uncommitted transaction and must be fully populated or rolled back;
   no partial/sentinel row can commit.
2. It then locks the exact intent, reads/validates the immutable FK-protected recorded event without adding a mutable
   event-lock segment, and locks the optional grant at its normal later position; validates through `phase=control`; atomically
   applies the recorded event to the gate watermark; recomputes `d3_business_fence_v1` from the post-apply typed pins;
   and fills the resume command's scope, positive review id, typed expected-predecessor columns, recorded-event input,
   and immutable post-apply digest. With an exact active grant it commits the command `queued` plus one outbox occurrence;
   without one it commits the same command in `retry_wait` with no dispatch outbox.
3. Grant issuance uses the same common lock/order and `phase=control`. If the exact recorded-event watermark is not yet
   applied, it commits only the durable grant; the later gate apply observes it. If the watermark is applied, grant
   issuance must exact-lock the already-created resume-v2 row in the command segment before it locks intent/grant, then
   perform the registered one-time `retry_wait -> queued` reawaken and create/exact-replay its outbox. An applied
   watermark with a missing row returns
   `not_applied(reason=business_precondition_conflict, detail_code=resume_convergence_missing)` with zero writes. The immutable recorded event
   plus existing gate-apply command/idempotency identity remain the durable replay source for normal reducer/recovery
   exact replay; the grant UoW neither schedules repair nor inserts a command after reaching intent/grant.
4. A claimed resume command acquires the common order and, before entering intent, reserves or exact-locks the
   deterministic successor verification command plus every current command idempotency target in the
   all-participating-command segment. It then locks intent/grant, calls `phase=resume_after_grant`, and requires the exact
   awaiting intent, recorded event id, applied watermark, post-apply digest, and active grant. Only after that predicate
   succeeds does it CAS the lineage into the pending successor-convergence path, fully populate/make claimable the
   already-reserved successor command, and append/exact-replay its outbox occurrence; successor intent creation remains
   only in that source command's claim-bound Stage B. Resume never decrements grant capacity; only §6.5 physical
   dispatch does.

Record-first persists awaiting/event before gate apply; grant-first persists the active grant until record and gate apply.
Either serialized gate apply creates the durable resume row, so duplicate delivery or crash/retry has no one-shot wakeup
window. Exact replay returns the existing command/outbox/watermark aggregate without a second event, phase change, or
epoch advance. Identity/digest collision, different scope/review/intent/phase/event, stale gate watermark, mismatched
grant, or stale claim returns `not_applied` with the applicable closed reason and zero new intent/event/command/outbox/
source-tuple/domain writes.

### 6.3 Why generic claim does not create ActivityAttempt in Stage A

The repository has 29 production claim callers across six modules. Their command types include provider attempts,
domain mutation, orchestration-only work, and planned activity boundaries. Some commands legitimately have zero
ActivityAttempts until an owner-specific execution phase; their activity type, input envelope, idempotency key, and
evidence policy belong to the command owner, not the generic claim repository.

Forcing generic claim and ActivityAttempt creation into one transaction would:

- make the workflow repository invent command-specific activity semantics;
- create placeholder attempts for orchestration commands that should not have one;
- widen a D3 prerequisite into a 29-caller activity-spine rewrite;
- couple every claim to payload construction and increase lock duration;
- create a second activity owner inside the generic repository.

The two-stage design has the same stale-owner safety because Stage B locks the command row and validates the complete
claim identity plus the centralized phase-parameterized business predicate before its first insert. A crash after Stage A leaves only an expiring claim, not a phantom effect. A
crash inside Stage B rolls back attempt and intent together. This closes the D3 phantom-attempt window without claiming
the rest of R-019 is atomic.

### 6.4 Verification-intent timing decision

Generic retry/requeue does **not** mint a successor verification intent. The command-control UoW advances
`control_epoch`, invalidates the token, and emits only its normal control evidence; that synchronous command fence blocks
the old intent immediately. Event -> reducer -> owner command convergence schedules the replacement work. The resulting
typed owner command physically pins the exact expected predecessor intent id, phase generation, source control epoch,
and decision-source event id; these are typed columns/input pins, not JSON inferred at execution. For a transition that
requires a successor, the async command records/schedules the convergence but does not mint or supersede the phase before
a new claim-bound attempt exists. When that owner wins a new claim, Stage B invokes the shared predecessor-CAS primitive,
atomically supersedes the exact still-live predecessor, and creates the successor intent with the new ActivityAttempt.
There is therefore no Stage-B commit containing both a live predecessor and a new successor, and no successor can exist
without its current claim-bound attempt.

Cancel/timeout/human-decision paths that intentionally create no successor still converge through the verification-owner
supersession command. That command calls the same repository primitive and exact predecessor tuple, but uses the
no-successor transition variant; the already-advanced command/review/gate epoch blocks old Stage B/application during
that asynchronous window. A competing tuple mismatch writes zero; exact transition replay returns the one committed
result. A failed convergence command is retryable and observable; readers never perform supersession as repair.

Both successor and no-successor supersession acquire the section-6.5 common advisory coordination lock and use the same
global order as every other multi-aggregate D3 UoW: operation root -> optional plan/review/gate -> all participating
`workflow_commands` in deterministic order -> intent/current predecessor -> ActivityRun/Attempt -> optional
grant/cost/receipt. The complete command segment includes the supersession command and any current idempotency target.
They skip untouched aggregates but never reverse the order or begin at intent and later return to a command or operation
row.

This decision resolves the conflicting D3 prose that otherwise both minted an intent at requeue time and required a
new claim/attempt before minting it.

### 6.5 Dispatch authorization and the network boundary

A pre-transport claim check by itself is not sufficient: cancel/requeue can commit after that check and before the first
wire byte. D3 therefore uses the existing D0 cost-ledger concept as the single transport linearization boundary; it
does not create a second D3-only transport owner.

Plan/review/gate/grant are separate owner aggregates, so row locks alone do not provide a common first lock. Every D3
dispatch; every plan recompile, review decision, gate-epoch/human-transition, or grant issue/revoke/supersede mutation;
every D3 `OperationRun` terminal/cancel/retry/requeue/resume/reset/rebuild/recovery mutation that can invalidate dispatch;
and every strict D3 command transition that can invalidate dispatch authorization must first acquire the same
transaction-scoped advisory coordination lock for the canonical plan-review lineage. The command set explicitly includes
generic and owner-specific cancel, retry/requeue/resume, timeout, rebuild/reset/recovery requeue, executable-input change,
and first result-terminalization to `succeeded`/`failed_terminal`.

The pre-review source session-create command is intentionally outside that positive-review set. Before a session exists,
it uses only the §5.2 `scoped-session-bootstrap-lock-v1` key and closed bootstrap predicate; it cannot dispatch or write
domain/transport state. Once the session is committed, every normal strict-D3 mutation uses `d3-dispatch-v2`. The
bootstrap path is not a command-only exemption from coordinated D3 execution and cannot be reused by a command that
already has a positive review lineage.

The **v2** key is the signed first 64 bits of SHA-256 over
`d3-dispatch-v2\0runtime_namespace\0provider_mode\0workspace_id\0coordination_plan_review_id`: exactly the canonical
`scope_v1` tuple plus its plan-review lineage, without a Stage-B identifier or a redundant derived digest. The final
component is the canonical unsigned-decimal ASCII encoding of the positive BIGINT
`plan_review_sessions.review_id`, with no sign and no leading zero; NULL, zero, negative, alternate decimal spelling,
or text coercion is invalid. Review obtains it from its own row; plan,
gate, and grant obtain it from their typed relation to that row; OperationRun obtains its server-side immutable pin;
commands, intents, activities, and exposures obtain exact copies from their parent. `ClaimAuthority`, `ClaimReceipt`, and
every centralized business-predicate invocation bind and compare the same value. No owner derives it from a Stage-B
`root_intent_id`, payload, or ambient request. Missing/NULL/non-positive lineage or any copy mismatch fails closed before the
advisory lock is treated as acquired for the operation. Reusing one review lineage may safely over-serialize its phases;
hash collision may serialize unrelated work but cannot authorize it.

The strict equality chain is
`operation_runs.coordination_plan_review_id = plan_review_sessions.review_id = workflow_commands.coordination_plan_review_id = authority.expected_coordination_plan_review_id = receipt.identity.coordination_plan_review_id`;
intent/activity/grant/exposure envelopes exact-copy that same value. Every equality is typed and physical.

The acquisition budget is the checked-in, non-caller-configurable
`d3_dispatch_coordination_lock_acquire_budget_ms=250`. The repository loops on `pg_try_advisory_xact_lock` using a
monotonic deadline and takes no row lock/write before success. Exhaustion returns `dispatch_coordination_busy`, rolls
back, sends nothing, and follows the command's bounded retry policy; it never falls back to a blocking advisory lock.

After the advisory lock, every participating owner uses this fixed row-lock order:

```text
operation_runs scope/status parent
-> optional canonical plan row
-> optional plan-review session
-> optional gate current-state row
-> all participating workflow_commands rows in ascending (scope_digest, operation_id, command_id)
-> verification_intent / current predecessor
-> workflow_activity_runs / workflow_activity_attempts
-> optional identity_search_budget_grant
-> optional cost reservation / physical-call exposure / transport evidence receipt
```

The immutable operation scope parent is locked/read first so every later aggregate is checked against one durable root;
the global mutable sequence is therefore exactly `operation root -> optional plan/review/gate -> all participating
workflow_commands in deterministic order -> intent/predecessor -> ActivityRun/Attempt -> optional grant/cost/transport evidence receipt`.
The command segment includes every current owner, source, record, resume, supersession, and current idempotency-target row
the transaction can touch. Every deterministic command identity is reserve/create-or-locked there; a newly reserved row
is non-claimable until fully populated in the same transaction. Once the transaction enters intent it may not insert,
lock, or discover another command row. Missing participation fails closed and the caller retries from the common lock.
Every participating Stage B, async supersession, plan/review/gate/grant/control/dispatch, record, and terminal owner uses
that order after the common advisory lock, skips only untouched aggregates, and never starts at a later aggregate before
returning to an earlier one. The narrow command-row-only exemption is limited to
operations proven not to invalidate or widen dispatch authority: a heartbeat occurrence and a read-only/exact terminal
replay. They may serialize at `workflow_commands`, never touch another aggregate, and never return to an earlier row.
Status/input/token/epoch/terminal mutations, including every cancel variant, are not exempt merely because their current
implementation happens to update one command row.

There is one separate **post-network transport-evidence-only** lock-order exception, not a third command-row exemption.
After a pre-existing exposure authorized the send, response-receipt or attempt-failure-receipt get-or-create,
late-quarantine insert, and cost reconciliation may run in the fixed local order
`committed physical-call exposure FOR UPDATE -> applicable typed transport evidence receipt ->
optional response-only quarantine row / cost-state axis`. Such a UoW never locks, inserts, or returns to OperationRun, command, intent,
Activity, plan/review/gate, or domain rows; it cannot authorize a send, terminal event, result apply, child, or retry.
Normal terminalization is not in this exception: it uses the complete global order and at the final evidence segment
locks the same exposure `FOR UPDATE` before it locks/reads the exact pre-existing receipt. No database transaction,
including this evidence UoW, spans network
I/O.

For each physical call, the transport owner executes one PG UoW:

1. lock/validate the OperationRun scope/status/coordination parent, then plan/review/gate and command under the fixed order;
2. validate the sealed authority + current receipt and require exact scope/coordination equality across command, operation,
   ActivityRun/Attempt, intent, review/gate, grant, and ledger;
3. invoke the section-8.1 centralized predicate with `phase=dispatch`; this proves non-terminal OperationRun and every
   typed plan/review/gate/intent/business pin before any reservation/exposure write;
4. for Tier-2 only, require the exact grant id + issuance generation + policy revision to be `active`, namespace/mode/
   workspace equal, not expired/superseded/revoked, and have remaining searches; atomically decrement it. Tier-1 must
   have no Tier-2 grant consumption;
5. validate route/policy/schema pins, deadline, registered transport kind, OB-10.4 execution context, and remaining
   operation budget;
6. reserve worst-case cost and create or CAS the exact
   `(budget_reservation_ref, activity_attempt_id, physical_call_index)` exposure from `prepared` to `dispatching`,
   binding command scope, claim generation, control epoch, authority digest, attempt, grant, and all plan/review/gate
   pins;
7. commit. This commit is the dispatch-authorization linearization point;
8. only after commit, the same transport owner may perform the one network send for that exposure identity.

The strict D3 migration must expose plan/review/gate revisions, digests, epochs, human-transition state, and
blocking-reason digest as typed durable columns; parsing `plan_json` or `gate_json` inside an authorization CAS is
forbidden. Until those columns and OB-10.2/10.3/10.4 exist, the D3 transport predicate is unimplemented and dispatch
remains disabled.

Every cross-owner invalidating transition (plan recompile, review/human decision, gate transition, grant control, or any
dispatch-invalidating D3 OperationRun transition), plus every strict D3 dispatch-invalidating command transition listed above,
first acquires the common advisory lock, then follows the complete
`operation root -> optional plan/review/gate -> all participating command rows -> intent/predecessor ->
ActivityRun/Attempt -> optional grant/cost/receipt` order. A command-only control transition may
start at `workflow_commands` only when it is one of the two non-invalidating exemptions; all invalidating transitions
start at the common lock and operation root. There is no third exemption for an OperationRun owner or a mutation that
happens to touch only one row. If the applicable control transition commits its revision/epoch/token invalidation first, the dispatch UoW
fails and there is no send. If dispatch authorization commits first, the call is already authorized in-flight work: a
later control transition may not retract bytes or guarantee zero cost, but its new revision/epoch makes every result-slot,
event, command, artifact, EntityDelta, and domain-apply CAS reject the old result. The cost-ledger owner may reconcile
only the already-created exposure to `sent`, `confirmed`, `uncertain`, or `no_call`; that is not permission for another
send or a command/domain write. A late response is handled only through the durable quarantine contract in section 9.3.

No database transaction is held across network I/O. A crash after `dispatching` is treated conservatively according to
the D0 contract, and only proof that no wire call occurred permits `no_call`. Any provider/model helper that sends
directly without this transport-owner authorization row is non-compliant and blocks D3 activation.

### 6.6 Atomic terminal result and event UoW

Transport-backed terminalization consumes one of two typed durable evidence receipts selected by a closed three-variant
registry. A valid terminal response uses
`TransportResponseReceipt`; a transport/protocol failure after a committed exposure but before any valid terminal
response uses `TransportAttemptFailureReceipt`. These are evidence identities, not alternative D0 envelopes. A response
receipt may later coexist with an already-terminal attempt-failure receipt only when the real response arrived late; one
terminal event consumes exactly one variant and never combines them.

The checked-in `TERMINAL_PROVENANCE_SPECS` registry is the sole semantic owner. Its closed
`TerminalProvenanceSpec` union has these variants and fields:

```text
TerminalProvenanceSpec =
    TransportResponseSpec
  | TransportAttemptFailureSpec
  | NoExposureTerminalSpec

TransportResponseSpec {
  terminal_provenance_spec_id
  allowed_command_types, allowed_stage_ids
  allowed_terminal_statuses, allowed_terminal_event_types, allowed_terminal_outcomes
  allowed_terminal_reasons
  response_spec_digest
}

TransportAttemptFailureSpec {
  terminal_provenance_spec_id
  allowed_command_types, allowed_stage_ids
  allowed_terminal_statuses, allowed_terminal_event_types, allowed_terminal_outcomes
  failure_codes
  retry_policy_revision
  retry_disposition_by_failure_code           # each code -> retryable | terminal
  failure_spec_digest
}

NoExposureTerminalSpec {
  terminal_provenance_spec_id
  allowed_command_types, allowed_stage_ids
  allowed_terminal_statuses, allowed_terminal_event_types, allowed_terminal_outcomes
  no_exposure_spec_digest
}
```

All three variant digests use the checked-in canonical encoder over every displayed field except the digest field itself. Lists are sorted,
duplicate-free, and non-empty where the variant is enabled. The registry, not a command payload, caller, exception, or
receipt repository, selects the exact command type/stage/status/event/outcome. Any registry change requires a new spec
digest; a failure retry-policy change also requires a new `retry_policy_revision`. Neither can reinterpret an existing receipt.
The fast registry preflight enumerates the full applicable space and requires exactly one entry for each
`(command_type, stage_id, terminal_transport_variant, terminal_status, terminal_event_type, terminal_outcome,
terminal_reason if exposure, failure_code if attempt_failure)` tuple;
zero or duplicate entries fail closed. `CommandTypeSpec.terminal_provenance_policy_digest` is derived from the canonical
sorted applicable-spec manifest. The command's existing `claim_authority_spec_digest`, the committed exposure policy
pin, and receipt/event exact replay retain the historical policy/spec digest; no new command column is added and old
spec entries remain checked in until no active or retained
command/exposure/receipt/event/source-intent/quarantine/tombstone/cost-audit identity pins them.

After a committed dispatch exposure receives a valid terminal response, the response-receipt creator first performs
`SELECT ... FOR UPDATE` on that exact committed exposure row, validates all scope/call/claim/business pins and the
canonical D0 §2.2 `ModelInvocationEnvelopeV1`, and get-or-creates one immutable
`transport_response_receipts` row containing:

```text
transport_response_receipt_id
runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id
operation_run_id, command_id, activity_run_id, activity_attempt_id
claim_generation, control_epoch, claim_authority_spec_digest, d3_business_fence_digest
terminal_provenance_policy_digest, response_spec_digest
dispatch_exposure_id, physical_call_index
provider_call_id_state                         # present | missing_by_registered_transport
provider_call_id
model_invocation_envelope_ref, model_invocation_envelope_digest
terminal_reason
canonical_delivery_identity, response_occurrence_id
canonical_response_digest, canonical_result_digest
result_artifact_ref, result_artifact_digest
```

The response creator resolves exactly one `TransportResponseSpec` from the historical policy digest pinned by the
command/exposure, exact-copies `response_spec_digest`, and rejects policy/spec drift. A D0-valid terminal response with
registered `terminal_reason=length` or `terminal_reason=content_filter` remains response evidence: it creates the normal
response receipt. While current it may enter only the registered response terminal branch and its typed business
outcome must fail closed to the applicable non-authorizable/evidence-insufficient/needs-human disposition; terminal
reason alone never auto-confirms or authorizes the domain result. When late it is response-only quarantined. Only an incomplete or
truncated wire envelope, or bytes that fail the registered protocol parser and therefore cannot form a D0-valid
`ModelInvocationEnvelopeV1`, is attempt-failure evidence.

The repository derives `canonical_delivery_identity` in this strict priority: canonical provider callback/event id;
registered transport idempotency/delivery id; otherwise the registered protocol's stable
`body:<canonical_response_digest>:ordinal:<protocol_ordinal>` form. A non-streaming protocol fixes ordinal `0`; a
streaming protocol must supply a replay-stable sequence ordinal. A transport with neither a durable delivery id nor a
stable body-digest/ordinal rule fails closed instead of generating a random callback UUID. `provider_call_id` is required
unless the committed exposure's transport spec explicitly admits `missing_by_registered_transport`, in which case the
state is persisted and the id remains SQL NULL.

`response_occurrence_id` is the lowercase SHA-256 of the canonical length-delimited tuple
`transport-response-occurrence-v1 + scope_digest + dispatch_exposure_id + canonical_delivery_identity`. The receipt has
unique identities on `(scope_digest, dispatch_exposure_id, canonical_delivery_identity)` and
`(scope_digest, dispatch_exposure_id, response_occurrence_id)`. Redelivery therefore returns the same receipt. Exact
replay requires equality of exposure/call/provider, D0 envelope ref/digest, response/result/artifact digests, and every
scope/attempt/generation/epoch/business pin, historical terminal-provenance policy/spec digest, and terminal reason; any
mismatch is `transport_response_receipt_collision` with zero receipt,
terminal, quarantine, event, or domain write. This receipt is transport evidence authorized by the committed exposure,
not permission to apply a result.

The response receipt's `result_artifact_ref/result_artifact_digest` is either a complete canonical pair or both absent.
For an exposure terminal, `result_ref` is defined only as `receipt.result_artifact_ref or ''`, `result_digest` is exactly
`receipt.canonical_result_digest`, and `artifact_ref/artifact_digest` exact-copy the same optional receipt pair. A
same-result-digest/different-`result_ref` request, a half artifact pair, or any independently supplied result/artifact ref
is `terminal_receipt_provenance_mismatch` with zero terminal/event/domain write.

If the committed exposure instead reaches a registered transport/protocol failure before a valid terminal response,
the failure-receipt creator first performs the same `SELECT ... FOR UPDATE` on the exact committed exposure row,
validates all scope/call/claim/business pins, proves under that lock that no response receipt exists, and get-or-creates one immutable
`transport_attempt_failure_receipts` row:

```text
transport_attempt_failure_receipt_id
runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id
operation_run_id, command_id, activity_run_id, activity_attempt_id
claim_generation, control_epoch, claim_authority_spec_digest, d3_business_fence_digest
terminal_provenance_policy_digest
dispatch_exposure_id, physical_call_index
provider_call_id_state                         # present | not_observed_before_failure
provider_call_id
failure_occurrence_id
failure_code, failure_spec_digest, canonical_failure_digest
retry_policy_revision, retry_disposition       # retryable | terminal
failure_artifact_ref, failure_artifact_digest
```

`failure_code` comes from the checked-in closed `TransportAttemptFailureSpec` registry selected under the exact
historical `terminal_provenance_policy_digest`; the canonical serialization and
digest of that entry is `failure_spec_digest`. The receipt exact-copies that entry's `retry_policy_revision` and the
code's `retry_disposition`. `canonical_failure_digest` covers the registered code, retry policy/disposition, normalized typed
transport/protocol failure facts, exposure/call identity, and required failure-artifact pair. Caller text, exception
string hashing, and model output cannot choose the code, retry pins, or either digest. The failure artifact ref is non-empty and its
digest is lowercase 64-hex; it is the durable typed failure evidence. An attempt-failure receipt contains no
`ModelInvocationEnvelopeV1`, response receipt, response occurrence, canonical response/result digest, or result
artifact fields and cannot fabricate any of them.

`failure_occurrence_id` is the lowercase SHA-256 of the canonical length-delimited tuple
`transport-attempt-failure-occurrence-v1 + scope_digest + dispatch_exposure_id + failure_code + canonical_failure_digest`.
The table is unique on `(scope_digest, dispatch_exposure_id)` and
`(scope_digest, dispatch_exposure_id, failure_occurrence_id)`. Exact replay compares every field above; any difference is
`transport_attempt_failure_receipt_collision` with zero receipt, terminal, event, quarantine, or domain write.

Only `retry_disposition=terminal` may feed an `attempt_failure` event with `terminal_status=failed_terminal`. A
`retryable` receipt is transport/cost/audit evidence only: while the claim is current, the command owner may take the
registered `active claim -> retry_wait` transition. That transition acquires the §6.5 common coordination and command
locks and then locks the exact committed exposure `FOR UPDATE` before it rechecks the receipt/response absence and
advances the control epoch; it writes zero terminal event, terminal command pair, or intent source-terminal tuple. If a
response receipt committed first, retry is rejected and the owner must use that response while the claim/business fence
is current, or route it to response-only quarantine if it is already stale. If retry commits first, its epoch advance
invalidates application; a later D0-valid response may persist its receipt but only quarantine. Neither callers nor the
terminal owner may promote a retryable receipt to terminal or downgrade a terminal disposition to retryable.

This variant is legal only after dispatch authorization committed and the registered transport attempt started. A
failure before committed exposure, or a proven pre-call/no-send outcome, cannot create an attempt-failure receipt: it
either remains retryable under the command owner's retry transition or uses a checked-in typed
`NoExposureTerminalSpec` whose registry digest and allowed terminal outcome authorize the complete-none `no_exposure`
variant. The registry is the sole no-exposure owner; caller flags and a fabricated D0 envelope are forbidden.

Response-receipt creation, failure-receipt creation, response/failure terminalization, and the retry transition all
serialize by locking the same committed `dispatch_exposure` row with `SELECT ... FOR UPDATE` before reading/creating
receipts or advancing the epoch. The locked absence proof is therefore race-free:

- if the response receipt commits first, a later failure creator/`attempt_failure` branch sees it, creates no failure
  terminal, and either uses the response branch while the claim/business fence is current or treats the response as
  late;
- if a terminal-disposition failure receipt and `attempt_failure` terminal event commit first, a later valid response
  may still create/exact-replay its response receipt from the authorized exposure, but it can only create/exact-replay
  the response-backed quarantine row; it can never replace the terminal event;
- if a response receipt commits before a retry transition, the retry writes zero and the current owner consumes the
  response when still current; if the retry transition locks first, it advances the epoch and a later valid response is
  response-receipt evidence only, eligible only for response-backed quarantine.

The `no_exposure` branch does **not** lock a nonexistent exposure row. It holds the §6.5 common coordination lock plus
the complete participating-command lock segment and proves exposure absence while holding those locks; physical
dispatch/exposure creation uses that same coordination+command prefix. Consequently an exposure insert cannot race the
absence proof, and no predicate lock, synthetic exposure, or `FOR UPDATE` against a missing row is treated as a fence.

An already-authorized exposure that later becomes stale may persist either exact receipt type despite the failed apply
predicate. Neither receipt is permission to apply command/domain state. Attempt-failure evidence is only a cost/audit
sink; it never creates quarantine. Only a subsequently valid D0 response with its own response receipt may do so.

A D3 source or record command cannot commit `succeeded`/`failed_terminal` result state separately from its terminal
result event. This result-terminalization is dispatch-invalidating and therefore acquires the section-6.5 coordination lock; only a read-only exact
terminal replay is exempt. The command-owner repository performs one PG UoW under the complete authority/receipt
predicate and fixed row order:

1. acquire the section-6.5 advisory lock and use its complete operation root -> optional plan/review/gate -> record,
   source, and every other participating command row in deterministic order -> intent/predecessor ->
   ActivityRun/Attempt -> optional grant/cost/exposure/transport-evidence-receipt order; require current generation, token, epoch,
   authority digest, and attempt;
2. select exactly one closed terminal provenance branch while holding the common coordination and participating-command
   locks from step 1:
   - `exposure`: `SELECT ... FOR UPDATE` the committed exposure used by the response creator and exact-compare one
     `TransportResponseReceipt`, including exposure,
     physical-call, provider, D0 envelope, response occurrence, canonical response/result, optional complete artifact
     pair, historical policy/response-spec digest, terminal reason, and every scope/attempt/generation/epoch/business
     pin; every attempt-failure field is empty;
   - `attempt_failure`: require `terminal_status=failed_terminal`, `SELECT ... FOR UPDATE` the committed exposure used
     by the failure creator, and exact-compare one
     `TransportAttemptFailureReceipt`, including physical-call/provider state, failure occurrence/code/spec/canonical
     digest, retry policy revision, `retry_disposition=terminal`, required failure artifact, and every
     scope/attempt/generation/epoch/business pin; prove no response receipt
     exists at this serialization point and leave every envelope/response/result/result-artifact field empty;
   - `no_exposure`: without attempting `FOR UPDATE` on a missing row, exact-compare the registered
     `NoExposureTerminalSpec` and historical policy digest, prove there is no exposure or response/failure receipt while
     the common coordination+command locks exclude dispatch creation, and require the complete-none transport shape;
3. invoke the same section-8.1 predicate with `phase=terminal`, including that typed exposure variant, exact immutable
   command business-fence digest, and current typed OperationRun/plan/review/gate/intent pins, before any
   result/event/command/source-tuple write;
4. build the exact server-owned payload below. For `exposure`, set
   `result_ref=receipt.result_artifact_ref or ''`, `result_digest=receipt.canonical_result_digest`, and exact-copy
   `artifact_ref/artifact_digest` from the receipt's complete-or-absent result artifact pair. For `attempt_failure`, set
   result and artifact ref/digest fields empty and exact-copy the receipt's failure artifact pair into
   `error_ref/error_digest`. For `no_exposure`, copy result/error/artifact fields only from the registered typed owner.
   Any half-pair, same-digest/different-ref, or typed-outcome mismatch is
   `not_applied(reason=business_precondition_conflict, detail_code=terminal_receipt_provenance_mismatch)` with zero
   terminal/event/domain write. Normalize only schema-declared optional refs/digests to the empty string, serialize it with
   the checked-in canonical JSON encoder, and compute
   `terminal_outcome_digest = sha256(canonical_json(terminal_outcome_v1)).hexdigest()` server-side;
5. insert the immutable workflow terminal event with exact physical identity over scope digest, coordination review id,
   workflow run id, operation id, command id, activity run/attempt ids, claim generation, control epoch, authority-spec
   and business-fence digests, historical terminal-provenance policy/spec digests, transport variant, exact response-
   or attempt-failure-receipt/exposure/provider/
   envelope/response/result/failure provenance, event
   family/type/id, terminal outcome digest, and idempotency key,
   where the existing event `operation_id` equals the command `operation_id` and root
   `operation_runs.operation_run_id`;
6. terminalize the ActivityAttempt and command, atomically copy the exact event id + outcome digest into
   `workflow_commands.terminal_event_id/terminal_outcome_digest`, clear lease and token digest, and retain only
   non-secret generation/epoch/terminal identity;
7. for a source verification command, fill the intent's full expected-null source terminal status/event/outcome plus
   terminal provenance tuple from that same locked variant in the same UoW;
8. commit all rows together.

A terminal business-predicate miss is a zero-command/domain-write conflict. A physical transport exposure whose
dispatch authorization had already committed may persist its immutable response or attempt-failure receipt; neither can
terminalize the command, append the source tuple, or create a workflow/domain event result after the predicate miss.
Only a valid response receipt may then feed the section-9.3 quarantine insert; an attempt-failure receipt remains a
cost/audit sink. In particular, if `attempt_failure` terminalizes first and a valid response later arrives,
the response repository creates/exact-replays its normal response receipt from the same exposure and the result goes
only to quarantine. If the response receipt wins the exposure serialization first, `attempt_failure` cannot terminalize.
A stale pre-dispatch or non-transport execution has no quarantine carve-out.

```text
terminal_outcome_v1 {
  schema = "terminal_outcome_v1"
  schema_version = 1
  command_id
  terminal_status                 # succeeded | failed_terminal
  terminal_event_type             # includes final_adjudication / evidence_insufficient
  record_outcome                  # closed enum for final_adjudication and company_identity_verification_recorded; empty otherwise
  result_ref, result_digest
  error_ref, error_digest
  artifact_ref, artifact_digest
  source_claim_generation
  source_control_epoch
  activity_run_id, activity_attempt_id
  runtime_namespace, provider_mode, workspace_id, scope_digest
  coordination_plan_review_id
  claim_authority_spec_digest, d3_business_fence_digest
  terminal_provenance_policy_digest
  terminal_transport_variant       # exposure | attempt_failure | no_exposure
  response_spec_digest
  transport_response_receipt_id
  transport_attempt_failure_receipt_id
  dispatch_exposure_id, physical_call_index
  provider_call_id_state, provider_call_id
  model_invocation_envelope_ref, model_invocation_envelope_digest
  terminal_reason
  response_occurrence_id, canonical_response_digest, canonical_result_digest
  failure_occurrence_id, failure_code, failure_spec_digest, canonical_failure_digest
  retry_policy_revision, retry_disposition
  failure_artifact_ref, failure_artifact_digest
  no_exposure_spec_digest
}
```

The terminal event type, final-adjudication `record_outcome`, and error refs/digests come only from the registered typed
outcome/evidence owner; payload JSON, model text, and caller-supplied values cannot override them. For `exposure`, result
and artifact provenance is constrained by the locked receipt exactly as step 4 specifies; the typed owner may not
substitute a same-result/different-ref or half-pair value, and the historical policy/response-spec digest plus terminal
reason exact-copy from that receipt. Registered D0-valid `length` and `content_filter` terminal reasons remain this
response branch. For `attempt_failure`, the failure receipt alone fixes
`error_ref/error_digest`; every response, result, result-artifact, and envelope field is empty and no D0 envelope is
created. For `no_exposure`, the registered typed owner supplies result/error/artifact refs and lowercase 64-hex digests
under its complete-none transport manifest. `record_outcome` must
be one of the section-6.2.1 values when event type is source
`final_adjudication` or record-command `company_identity_verification_recorded`, and must be empty otherwise. The event
stores this canonical payload and its digest, and the command stores the same event
id/digest pair. An `exposure` event exact-references its response receipt; an `attempt_failure` event exact-references
its failure receipt; a `no_exposure` event has every receipt/exposure/provider/envelope/response/result/failure field
empty under a local complete-none shape check. A receipt from another exposure, call index, provider delivery, failure
occurrence, envelope, attempt, generation, epoch, response/result/failure digest, or artifact ref/digest cannot
terminalize this command.

`workflow_events` therefore needs the scope/coordination/generation/epoch/authority/business/result-digest and typed
transport receipt/provenance columns listed in section 5.2,
reuses its existing `operation_id`, and has both a scope-aware unique idempotency identity and the composite unique key
`(scope_digest, coordination_plan_review_id, operation_id, command_id, event_id, terminal_outcome_digest)`. The nullable command pair participates in
one `MATCH SIMPLE DEFERRABLE` composite foreign key:

```text
workflow_commands(
  scope_digest, coordination_plan_review_id, operation_id, command_id,
  terminal_event_id, terminal_outcome_digest
)
REFERENCES workflow_events(
  scope_digest, coordination_plan_review_id, operation_id, command_id,
  event_id, terminal_outcome_digest
)
MATCH SIMPLE DEFERRABLE
```

`MATCH SIMPLE` is required because a strict row's non-null scope/positive-coordination/operation/command prefix plus a
null terminal pair is a valid non-result-terminal command; `MATCH FULL` would reject that partial-null composite. The command table separately
owns only local checks that require terminal event id/outcome digest both null or both non-null, validate digest shape when
non-null, and require status `succeeded`/`failed_terminal` exactly when the pair is present. Thus any present pair has
all six referencing columns non-null and the FK enforces the exact event. Control terminal `cancelled` retains a null
pair and its separate control evidence. A `CHECK` never claims to prove an event row in another table; the composite FK
provides that forward cross-table proof. A command cannot become terminal without the event. The FK does **not** prove
the reverse direction and does not prevent a standalone orphan event insert; the terminal repository's one-PG UoW,
write-set restriction, and rollback/injected-failure acceptance tests enforce that no terminal event commits without
the matching terminalized ActivityAttempt/command (and source-intent tuple where applicable). No structural claim of a
reverse FK is made.

For `terminal_transport_variant=exposure`, an additional nullable `MATCH SIMPLE DEFERRABLE` composite FK from the event's
`(scope_digest, dispatch_exposure_id, transport_response_receipt_id)` references the receipt's unique identity, while
repository acceptance exact-compares every call/envelope/occurrence/result and claim pin. The local variant check requires
response-receipt/exposure provenance complete and failure provenance empty. For `attempt_failure`, a separate nullable
`MATCH SIMPLE DEFERRABLE` composite FK from
`(scope_digest, dispatch_exposure_id, transport_attempt_failure_receipt_id)` references the failure receipt's unique
identity, with failure provenance, registry spec digest, retry policy revision, and terminal disposition complete and
response/envelope/result provenance empty. `no_exposure` requires both
receipt families, response-spec/terminal-reason fields, and every transport field complete-none plus the registered
no-exposure spec and historical policy digests. These checks do
not pretend to prove another table's row. Native-PG acceptance must exercise all three variants, each wrong-variant and
half-pair shape, and mismatched physical response/failure receipts.

Every population-sensitive terminal status/variant/provenance `CHECK` described in this section is guarded by
`command_type IN strict_d3_command_types_v1` (or by the event's structurally referenced strict-D3 command type).
Nullable FKs may exist additively, but legacy/non-D3 terminal writers are not required to acquire D3 scope, receipts,
or terminal pins. Registry-manifest parity, not sentinel presence, selects the strict population.

Terminal exact replay does not use a cleared claim token. It reads by scope-aware event idempotency key, requires exact
equality of every physical identity field and terminal digest, and returns the committed command/event/attempt aggregate
without a write. A key match with any command terminal pair, event physical identity, canonical outcome field, or digest
difference—including `result_ref`, either receipt id, every response/failure provenance field, or a complete-pair
difference—returns
typed `terminal_replay_conflict` with zero write, encoded as
`not_applied(reason=business_precondition_conflict, detail_code=terminal_replay_conflict)`; it cannot create a replacement event or mutate the
command, attempt, intent source binding, artifact, EntityDelta, or domain state.

## 7. Complete command-transition table

Every accepted transition is idempotent: an exact replay returns the committed state and never increments generation or
epoch a second time.

Every dispatch-invalidating row below inherits the section-6.5 common advisory coordination lock and global row order.
The `Required identity` cell is an additional CAS/business predicate, never a substitute for coordination. Pre-dispatch
creation/selection/Stage-A claim rows are repository CAS operations that cannot send; after claim, the only explicit
command-only exemptions are heartbeat occurrence and read-only terminal exact replay.

| Transition | Required identity | Coordination | `claim_generation` | token digest | `control_epoch` | Lease | Decision |
|---|---|---|---:|---|---:|---|---|
| insert a new command | source-event/idempotency identity + trusted immutable command scope/coordination/business pins; no claim | coordinated command creation reserves row in all-command segment, then fills typed predecessor/business pins after later intent locks; non-D3 pre-dispatch CAS remains non-sending | initialize `0` | initialize empty | initialize `0` | empty | strict row cannot commit half-populated; scope/positive coordination/predecessor/business digest are copied once; creation is not execution authorization |
| exact idempotent command upsert replay | exact existing command identity, scope, coordination, business digest, and typed input pins | pre-dispatch read-only exact replay; §6.5 not applicable | unchanged | unchanged | unchanged | unchanged | mismatch returns `not_applied(reason=business_precondition_conflict, detail_code=command_identity_collision)`; otherwise return existing row |
| select/reselect exact claimable command | trusted scheduler selection UoW + current status/epoch + prior execution lease and selection reservation absent/expired | pre-dispatch repository CAS; §6.5 not applicable | unchanged | unchanged | unchanged | persist existing lease owner/expiry as the exact unexpired selection reservation | atomically increment `claim_selection_generation` first, then clear the current-selection consumed-authority slot and bind the sealed one-use authority to repository-time expiry; the slot is not historical audit |
| queued/retry-wait payload or causality update, semantic change | command-current-state owner; exact expected prior payload/revision | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | changing executable input invalidates all earlier results; a changed business-fence input requires a new command identity; exact no-op update does not advance |
| prerequisite reawaken `retry_wait -> queued` | command-current-state owner + exact prerequisite identity | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | reawaken is a resume/requeue transition |
| new claim from `queued` / eligible `retry_wait` | sealed pre-claim authority + exact current unexpired selection reservation | pre-dispatch Stage-A CAS; §6.5 not applicable | `+1` | rotate to new digest | unchanged | overwrite reservation with bound execution owner/expiry | Stage A returns current receipt and resets heartbeat occurrence; it never requires an absent/expired reservation |
| expired reclaim from `running` or opt-in `claimed` | sealed pre-claim authority minted only after the selection UoW proves the prior execution lease expired and installs a new exact reservation | pre-dispatch Stage-A CAS; §6.5 not applicable | `+1` | rotate to new digest | unchanged | overwrite reservation with bound execution owner/expiry | generation/token separate old/new claim; reset heartbeat occurrence |
| `claimed -> running` | exact authority + current ClaimIdentity receipt + live lease | §6.5 common lock and global order mandatory for D3 Stage B | unchanged | unchanged | unchanged | retain | may occur in Stage B; Stage B skips untouched aggregates but never reverses the order |
| heartbeat / lease renewal | exact authority/receipt + next repository-issued occurrence | explicit §6.5 command-only exemption | unchanged | unchanged | unchanged | repository-computed extension | occurrence increments once; replay does not re-extend; expired claimant cannot revive |
| active claim -> `succeeded` | exact authority/receipt + section-6.6 result/event UoW success | §6.5 common lock and global order mandatory | unchanged | clear | unchanged | clear lease | atomically set terminal event id/outcome digest; replay never uses capability |
| active claim -> `failed_terminal` | exact authority/receipt + section-6.6 terminal failure UoW | §6.5 common lock and global order mandatory | unchanged | clear | unchanged | clear lease | atomically set terminal event id/outcome digest |
| active claim -> `retry_wait` after retryable failure | exact authority + current ClaimIdentity + retryable `TransportAttemptFailureReceipt` and its committed exposure | §6.5 common lock/command order, then exact exposure `FOR UPDATE` mandatory | unchanged | clear | `+1` | clear | response-first rejects retry and current processing uses the response; retry-first advances epoch, so a later response receipt is quarantine-only |
| active claim -> `queued` for partial progress | exact authority + current ClaimIdentity receipt | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | existing `attempt` accounting may decrement independently |
| active claim -> `retry_wait` for prerequisite wait | exact authority + current ClaimIdentity receipt | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | existing `attempt` accounting may decrement independently |
| `retry_wait -> queued` resume | current row/control authority | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | resume is an invalidating control transition |
| result-terminal `succeeded/failed_terminal -> queued` retry/reopen or control-terminal `cancelled -> queued` retry | current row/control authority | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | only result-terminal reopen clears the present command terminal pair before requeue; cancelled already has a null pair; historical event stays immutable; attempt may reset, generation never resets |
| `queued` / `retry_wait -> cancelled` generic cancel | current row/control authority | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | blocks results from any prior claim; command-only exemption forbidden |
| `claimed` / `running -> cancelled` owner-specific cancel | locked owner UoW + exact authority/current receipt or reviewed force policy | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | module rows and command cancellation retain their existing fixed-UoW requirement |
| owner-specific running resume/requeue | exact authority/current receipt or reviewed expired/force policy | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | request path does not execute owner work |
| timeout that invalidates owner execution | exact current row/control authority | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | late result is stale immediately |
| explicit rebuild/reset/recovery requeue | exact current row/control authority | §6.5 common lock and global order mandatory | unchanged | clear | `+1` | clear | if recovery directly reclaims only an expired lease, use the reclaim row above instead |
| terminal exact replay | matching command terminal event id + outcome digest + event idempotency identity; no claim token | explicit §6.5 read-only command-only exemption | unchanged | remains empty | unchanged | unchanged | returns committed row/event/attempt; performs no new write and cannot authorize another effect |

The epoch closure rule is exact: every accepted executable-input change, invalidating control, resume/requeue/reopen,
cancel, timeout, reset, rebuild, or recovery transition advances `control_epoch` once and clears token/lease before any
replacement claim; an exact replay advances zero times. First result-terminalization is coordinated and clears
token/lease but deliberately retains the current epoch while atomically installing the immutable terminal pair. Creation,
selection, Stage-A mint, `claimed -> running`, heartbeat, and read-only terminal exact replay also retain the epoch for
their documented reasons. No other mutation may retain it. Every dispatch-invalidating D3 OperationRun transition uses
the same advisory lock/global order even though OperationRun has no command `control_epoch`; its own typed status/revision
pin is checked by the centralized predicate, and it has no extra command-row-only exception.

Plan recompile and human decision remain separate aggregate transitions. Their owners synchronously advance the
plan/review/gate revision or epoch and install the fail-closed pending/blocking state defined by D3. They do not directly
write `workflow_commands.control_epoch`. If they require command cancellation or requeue, event -> reducer -> command
owner performs the matching row above. Thus the cross-owner convergence window is already fenced by the plan/review
epoch before command-domain supersession completes.

## 8. Central claim predicate

The workflow-runtime repository owns one central predicate. Domain owners pass the sealed pre-claim `ClaimAuthority`
plus its matching current `ClaimReceipt`/`ClaimIdentity`; they do not copy SQL, pass expected owner/type strings, or
compare only `lease_owner`.

Conceptually, every irreversible owner boundary joins no ambient or JSON-derived scope; it qualifies the physical command
row directly. In the predicate below, `wc` is the exact locked `workflow_commands` row; `authority` is the sealed
`ClaimAuthority` defined in section 5.1; `receipt` is the sealed `ClaimReceipt` whose `identity` is the committed
`ClaimIdentity`; and `repository_operation` is a checked-in internal typed repository operation, not caller input. That
operation definition owns `allowed_current_statuses` and exactly one requested activity, terminal-event, transport, or
effect type as applicable.

```text
verify_authority_seal(
  authority,
  issuer_revision=authority.issuer_revision,
  issuer_digest=authority.issuer_digest
)
AND receipt.authority_id = authority.authority_id
AND wc.command_id = authority.expected_command_id
AND receipt.identity.command_id = wc.command_id
AND wc.command_type = authority.expected_command_type
AND receipt.identity.command_type = wc.command_type
AND wc.owner = authority.expected_command_owner
AND receipt.identity.command_owner = wc.owner
AND wc.stage_id = authority.expected_stage_id
AND receipt.identity.stage_id = wc.stage_id
AND registry_stage_policy_allows(
      authority.stage_policy,
      authority.allowed_stage_ids,
      wc.stage_id
    )
AND wc.operation_id = authority.expected_operation_id
AND receipt.identity.operation_id = wc.operation_id
AND wc.runtime_namespace = authority.trusted_runtime_namespace
AND receipt.identity.runtime_namespace = wc.runtime_namespace
AND wc.provider_mode = authority.trusted_provider_mode
AND receipt.identity.provider_mode = wc.provider_mode
AND wc.workspace_id = authority.trusted_workspace_id
AND receipt.identity.workspace_id = wc.workspace_id
AND wc.scope_digest = authority.trusted_scope_digest
AND receipt.identity.scope_digest = wc.scope_digest
AND wc.coordination_plan_review_id = authority.expected_coordination_plan_review_id
AND receipt.identity.coordination_plan_review_id = wc.coordination_plan_review_id
AND wc.expected_predecessor_intent_id = authority.expected_predecessor_intent_id
AND receipt.identity.expected_predecessor_intent_id = wc.expected_predecessor_intent_id
AND wc.expected_predecessor_phase_generation = authority.expected_predecessor_phase_generation
AND receipt.identity.expected_predecessor_phase_generation = wc.expected_predecessor_phase_generation
AND wc.expected_predecessor_source_control_epoch = authority.expected_predecessor_source_control_epoch
AND receipt.identity.expected_predecessor_source_control_epoch = wc.expected_predecessor_source_control_epoch
AND wc.expected_predecessor_decision_source_event_id = authority.expected_predecessor_decision_source_event_id
AND receipt.identity.expected_predecessor_decision_source_event_id = wc.expected_predecessor_decision_source_event_id
AND wc.d3_business_fence_digest = authority.expected_d3_business_fence_digest
AND receipt.identity.d3_business_fence_digest = wc.d3_business_fence_digest
AND wc.claim_authority_spec_digest = authority.claim_authority_spec_digest
AND receipt.identity.claim_authority_spec_digest = wc.claim_authority_spec_digest
AND wc.claim_selection_generation = authority.expected_claim_selection_generation
AND wc.consumed_claim_authority_id = authority.authority_id
AND wc.lease_owner = receipt.identity.lease_owner
AND receipt.identity.lease_owner = authority.lease_identity
AND wc.lease_expires_at = receipt.identity.lease_expires_at
AND wc.lease_expires_at > repository_now
AND wc.status IN repository_operation.allowed_current_statuses
AND wc.claim_generation = receipt.identity.claim_generation
AND wc.attempt = receipt.identity.attempt
AND wc.control_epoch = receipt.identity.control_epoch
AND wc.heartbeat_sequence = receipt.identity.heartbeat_sequence
AND wc.last_heartbeat_id = receipt.identity.last_heartbeat_id
AND wc.claim_token_digest = sha256(hex_decode(receipt.identity.claim_token))
```

`authority_expires_at` is a one-use mint deadline checked by Stage A. Once Stage A atomically consumes the authority,
current execution lifetime is governed by the persisted claim identity and lease/heartbeat policy; a short selection
deadline cannot silently shorten or extend an already committed claim.

The repository owns `repository_now`; a caller cannot extend acceptance by supplying a timestamp. The applicable
`repository_operation.requested_*_type` must also be a member of the authority's exact allowed activity, terminal-event,
transport, or effect set; a repository operation with no applicable requested type cannot smuggle one through caller
payload. Token decoding,
seal/digest shape, missing values, overflow, registry drift, worker/lease mismatch, namespace/mode mismatch, and malformed
identity all fail closed before a write. Heartbeat additionally requires the exact next occurrence contract in section
5.3; heartbeat sequence is not an effect-authorization substitute.

After Stage B, an effect CAS additionally binds the exact `activity_run_id + activity_attempt_id` and their copied
scope/generation/post-claim-command-attempt/epoch/authority digest, including
`workflow_activity_attempts.command_attempt = receipt.identity.attempt = workflow_commands.attempt`. The source verification command binds its own claim/attempt to the immutable
source terminal event; the independent record command validates its separate current claim plus that source binding.
D3 record/apply UoWs then compose the exact business predicate below rather than adding a local approximation.

### 8.1 Centralized phase-parameterized D3 business predicate

The verification owner provides one repository implementation with one closed API:

```text
verify_d3_business_fence(phase, locked_rows, context)

sealed ClaimedCommandContext =
    StageBContext
  | TerminalContext
  | RecordContext
  | DispatchContext
  | ResumeAfterGrantContext

# every ClaimedCommandContext variant contains exactly:
locked_command + sealed ClaimAuthority + matching sealed ClaimReceipt
+ checked-in repository_operation + its phase-specific typed rows/inputs

sealed AggregateControlContext = ControlContext {
  registered_typed_control_authority
  control_operation
  locked_target_operation
  locked_target_plan
  locked_target_review
  locked_target_gate
  locked_target_intent
  all_affected_command_expectations {
    command_id, expected_status, expected_control_epoch,
    expected_d3_business_fence_digest, expected_terminal_pair
  }
}
```

`AggregateControlContext` contains no claim token, token digest, `ClaimAuthority`, or `ClaimReceipt`; its registered
typed control authority and the old locked aggregate/command pins are its authorization inputs. For an affected command,
`control_epoch` is the physical dispatch-invalidating command revision; this context does not invent a second generic
command-revision column. The discriminant and
`phase` must match exactly: `stage_b/terminal/record/dispatch/resume_after_grant` accept only their corresponding
`ClaimedCommandContext` variant, and `control` accepts only `AggregateControlContext`. Every variant and its
`locked_rows` tuple is sealed/closed; a missing required row, extra unregistered input, wrong context/phase pair, or
attempt to deserialize a caller context fails closed as
`not_applied(reason=business_precondition_conflict, detail_code=context_shape_mismatch)`.

All six phases call this same implementation after the common advisory lock and global row order and before their first
durable effect write. The sole pre-evaluator SQL mutation is a deterministic non-claimable command reservation when the
phase's `locked_rows` must include a not-yet-existing command identity. It occurs only in the all-participating-command
segment, remains invisible inside the uncommitted transaction, and any evaluator failure rolls back the reservation
with the entire transaction; no claimable/sentinel command row or outbox occurrence may commit. Thus every returned
`not_applied` still has zero durable writes. This exception includes gate-result apply and claimed-resume successor
reservation; it does not permit an ActivityRun/Attempt, intent, event, state, terminal pair, domain row, or exposure
before the evaluator. Gate-result apply and grant issue/revoke/supersede use `phase=control`, not a local command-owner
predicate. No owner copies a subset of its SQL. A control UoW verifies the complete **old** typed pins and every affected
command expected revision before writing, then atomically advances its registered revision/epoch or command epoch so
every old command context becomes invalid. The checked-in phase manifest may add requirements; it may never remove this
common subset:

1. the exact OperationRun scope and positive BIGINT `coordination_plan_review_id` match every locked aggregate; for a
   claimed context they also match command/authority/receipt, and the OperationRun is non-terminal;
2. the canonical plan id, immutable plan-bundle digest, and `plan_revision` exact-match the command's typed creation pins
   or, for control, every affected command's expected old pins;
3. the exact canonical plan-review session is still `pending`, its `review_revision` matches, it has no terminal decision,
   and `human_transition_pending=false`;
4. the exact gate control epoch, gate revision, and blocking-reason digest match, and the company-identity reason remains
   blocking until its registered owner event is applied;
5. the applicable base/current intent id, phase generation, complete-or-initial predecessor shape, source binding,
   fingerprint version/digest, decision generation/source-event id, accepted-policy revision, schema revision,
   route revision and effective-route-snapshot digest exact-match typed pins;
6. each applicable row's immutable `d3_business_fence_digest` equals a server recomputation of the canonical creation
   snapshot below; claimed contexts additionally require authority and receipt equality. The four predecessor values
   are read from the locked typed command columns, never re-derived from intent or payload. Typed relational columns are
   the only inputs; `payload`, `plan_json`,
   `gate_json`, model output, process environment, and caller-supplied digests are forbidden.

```text
d3_business_fence_v1 {
  schema = "d3_business_fence_v1"
  schema_version = 1
  runtime_namespace, provider_mode, workspace_id, scope_digest
  operation_id, coordination_plan_review_id
  plan_id, plan_bundle_digest, plan_revision
  review_revision
  gate_control_epoch, gate_revision, gate_blocking_reason_digest
  base_intent_id, base_intent_phase_generation
  expected_predecessor_intent_id
  expected_predecessor_phase_generation
  expected_predecessor_source_control_epoch
  expected_predecessor_decision_source_event_id
  fingerprint_version, fingerprint_digest
  decision_generation, decision_source_event_id
  accepted_policy_revision, schema_revision, route_revision
  effective_route_snapshot_digest
}
```

Canonical serialization uses the checked-in encoder and fixed field order. Mutable `intent_state` is not a digest input;
the phase manifest checks it explicitly. A resume command is created only after the recorded event has been applied to
the gate and therefore freezes the post-apply typed pins; it never reuses a pre-apply digest. Initial Stage B is the sole legal sentinel
form: all four predecessor fields and the absent base-intent identity are SQL NULL and the locked lineage proves no
current phase. Every other applicable tuple is complete and typed as defined in §6.2. A missing typed row, NULL/non-positive
coordination review id, half-sentinel predecessor, digest mismatch, changed revision, terminal operation/review decision,
or cleared/replaced gate reason returns
`not_applied(reason=business_precondition_conflict, detail_code=business_fence_mismatch)`.

The digest never updates in place. A plan/review/gate/base-intent pin change invalidates the old command and requires a
new registered command identity under the new canonical digest. Retry/requeue of the same command is legal only while
all digest inputs remain exact; its epoch may advance, but neither payload mutation nor a control owner may rewrite the
business snapshot to make an old command current again.

The phase manifests are closed:

The §5.2 `scoped_session_bootstrap_v1` path is deliberately absent from this table. It is evaluated only by
`verify_scoped_session_bootstrap(ScopedSessionBootstrapContext, locked_rows)`, with the context carrying the exact
one-use authority and its already-minted current-claim receipt, before any positive review lineage exists;
it cannot be coerced into `phase=terminal` or any other normal `d3_business` phase.

| `phase` | Additional required pins/rows | Explicitly not required |
|---|---|---|
| `stage_b` | registered command stage/activity policy; initial-no-phase proof or exact complete predecessor; exact source/record command role | grant consumption, cost reservation, exposure |
| `terminal` | current ActivityRun/Attempt; normal positive-review source or record terminal-event family/type; exact source binding/adjudication where applicable; exact `TerminalProvenanceSpec` command/stage/status/event/outcome membership; exactly one registered `exposure`, terminal-disposition committed-exposure `attempt_failure`, or complete-none `no_exposure` variant from §6.6 | scoped-session bootstrap, creating a new grant/exposure, terminalizing a retryable failure, fabricating a D0 envelope for failure/no-send, or substituting another response/failure; terminalization cannot revive budget |
| `record` | independent record claim/attempt; exact frozen source terminal command/event/outcome; exact typed `record_outcome` mapping and adjudication manifest | transport kind, physical-call index, grant consumption, cost reservation/exposure |
| `dispatch` | current pending intent; registered transport; applicable adjudication manifest and exact active Tier-2 grant; OB-10.4 context; cost reservation and unique physical-call exposure pins | record terminal tuple |
| `resume_after_grant` | exact `awaiting_budget` intent, exact `recorded_event_id`, applied gate watermark, active exact grant, and post-apply command business digest | pre-record `pending`, grant decrement, or physical-call exposure |
| `control` | registered typed control authority, exact old operation/plan/review/gate/intent pins, all affected command expected revisions/epochs, and registered transition; gate-result apply and grant issue use this phase; control timeout uses its distinct event | command claim token/receipt, provider dispatch, record outcome, grant consumption |

The claim/authority predicate is evaluated first for the five command-owner phases; aggregate control owners use their
closed control context but the same business evaluator and coordination lineage. In every phase, a control/input/requeue
mutation that commits first changes an exact typed pin or command epoch, so a later command invocation fails before any
ActivityRun/Attempt, intent/predecessor, event, child/resume command/outbox, source terminal tuple, result, artifact,
EntityDelta, exposure, or domain write. Transport evidence from an already committed authorized exposure is the sole
exception and may persist/exact-replay only its immutable §6.6 response or attempt-failure receipt; only a valid response
receipt may additionally create §9.3 quarantine. Neither receipt can apply a result. This is the required **control-first zero
attempt/intent/event/command/source-tuple write** invariant.

### 8.2 Mandatory consumers

The central predicate is required at every current-claim boundary:

1. `claimed -> running`, heartbeat, and lease renewal;
2. ActivityRun/ActivityAttempt creation or mutation;
3. the section-6.5 claim-bound provider/model dispatch-authorization UoW;
4. result-slot acceptance or consumption;
5. artifact publication and domain apply;
6. EntityDelta creation and the atomic section-6.6 terminal result/event UoW;
7. command `succeeded`, `failed_terminal`, `retry_wait`, partial-progress, or prerequisite-wait transition;
8. any owner path that directly creates a child command before a committed source event exists;
9. D3 verification-intent bind, successor convergence, and the independent verification-record UoW.

Normal event -> reducer child planning does not receive the raw token. Its authority is the already committed terminal
result event, whose creation was claim-fenced and whose child idempotency key binds the source event. A worker may not
bypass that chain by treating a previously successful claim as standing permission to create later children.

## 9. Rejection, in-flight, and business-outcome taxonomy

The implementation must distinguish four outcomes; collapsing them would either hide a stale owner, promise impossible
network revocation, or suppress a valid current-owner business result.

Every command/domain effect boundary has one closed top-level return shape:

```text
EffectBoundaryResult =
    applied(committed_aggregate)
  | not_applied(reason, detail_code)

reason = stale_claim | business_precondition_conflict
```

`detail_code` refines only the chosen reason; it cannot add a third zero-write category. `not_applied` is never an
event, state, table row, or durable audit record. Transport evidence authorized by an already-committed exposure—the
immutable response or attempt-failure receipt, exposure reconciliation, and response-only late quarantine—is outside the stale command/domain
effect transaction and does not turn `not_applied` into an applied command result.

### 9.1 Claim-fence rejection — zero durable writes

Any mismatch in namespace, mode, workspace, command/owner, lease owner, lease validity, status, generation, token, epoch,
or post-Stage-B attempt id returns `not_applied(reason=stale_claim, detail_code=...)` with a detail such as:

- `stale_generation`;
- `control_epoch_advanced`;
- `claim_token_mismatch`;
- `lease_expired`;
- `status_not_executable`;
- `claim_attempt_mismatch`.

Before dispatch authorization, the transaction writes nothing: no command/action/run/activity/intent/event/EntityDelta/
domain/artifact/child/dispatch row and no external call. At a post-dispatch effect boundary, the stale command-owner CAS
likewise writes none of those rows and applies no result. Process metrics and structured logs may record the rejection
without token material. They are not workflow or business evidence.

### 9.2 Current-claim business CAS conflict — also zero durable writes

If the complete claim predicate succeeds but a stored business precondition changed concurrently—for example review
revision, decision generation, fingerprint, manifest hash, policy/schema/route pin, or OperationRun terminal winner—the
result is `not_applied(reason=business_precondition_conflict, detail_code=...)`, not a stale-claim reason. Under R-019 it still produces zero durable writes. The
authorized owner must re-read and either stop or issue a new owner command; it cannot append a `not_applied` event merely
because the claim itself remained current.

### 9.3 Already-authorized in-flight call — cost may occur, application remains fenced

If the section-6.5 dispatch transaction committed while the claim was current, a later cancel/requeue does not turn the
wire call into an unauthorized send. The transport/cost owner may update only that pre-existing exposure for conservative
reconciliation and may persist its exact response or attempt-failure receipt. If a valid D0 response arrives after the
command/intent/plan/review/gate fence changed, the result-acceptance owner writes only its response receipt plus durable
quarantine below; the old command owner still cannot create a terminal workflow event, child, EntityDelta, or domain
write. A protocol/transport failure instead persists only its typed attempt-failure receipt for cost/audit and never a
quarantine row. This category is neither `stale_claim` at dispatch time nor a promise of zero provider cost.

#### 9.3.1 Durable late-result quarantine

The future PG-only `workflow_late_result_quarantine` table is an authorization sink, never a result slot. Its exact row
contains:

```text
quarantine_id
runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id
operation_run_id, workflow_run_id, command_id
activity_run_id, activity_attempt_id
claim_generation, control_epoch, claim_authority_spec_digest, d3_business_fence_digest
dispatch_exposure_id, physical_call_index, provider_call_id
provider_call_id_state
transport_response_receipt_id, canonical_delivery_identity, response_occurrence_id, terminal_reason
model_invocation_envelope_ref, model_invocation_envelope_digest
canonical_response_digest, canonical_result_digest, result_artifact_ref, result_artifact_digest
rejection_reason, cost_state, retention_state, authorizable=false
idempotency_key, recorded_at, retention_until
```

The shared quarantine repository is the **single SQL mutation gateway**. Its typed result-acceptance insert entrypoint is
the only insert writer and may insert only from the same immutable `TransportResponseReceipt` defined in §6.6, proving
its previously committed dispatch exposure and exact scope/coordination/business/attempt/call/envelope/delivery identity;
a stale `ClaimReceipt` alone cannot authorize the insert. Normal terminal and quarantine never build parallel receipt
identities. The insert
atomically fixes the immutable identity/digest fields, `authorizable=false`, `cost_state=pending_reconciliation`, and
`retention_state=retained`.

The exact quarantine idempotency key is
`late-response-v1:<scope_digest>:<dispatch_exposure_id>:<canonical_delivery_identity>`, encoded with the checked-in
length-delimited component encoder so component punctuation is unambiguous. The table has a unique constraint on that
key and on `(scope_digest, dispatch_exposure_id, response_occurrence_id)`. Both delivery and occurrence values are copied
from the receipt, retaining its canonical `transport-response-occurrence-v1` derivation; callback retry/redelivery
therefore exact-replays one quarantine row. Exact replay requires all
receipt/envelope/response/result/artifact digests and terminal reason to match; any mismatch returns a typed collision
with zero quarantine mutation. Only a terminal response that already passed D0 envelope validation is representable;
the registered missing-provider-call-id response state remains valid when its transport spec admits it. A complete,
parse-valid D0 envelope whose provider terminal reason is `length` or `content_filter` remains a response receipt and,
while current, may only take the registered fail-closed non-authorizable/evidence-insufficient/needs-human response
outcome—never automatic confirmation. When stale it may enter this response-only quarantine. An incomplete/truncated
wire envelope or registered-protocol parse
failure that cannot produce a D0-valid envelope is attempt-failure evidence, not a response receipt or quarantine row.

The row is not globally append-only: its insert identity and retained digests are immutable, while exactly two monotonic
axes may advance through disjoint typed CAS entrypoints on the same repository. Direct table SQL from result, cost, or
retention owners is forbidden.

Cost reconciliation and data retention are orthogonal state machines; one mixed `disposition` is forbidden:

- `cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain | reconciled_no_call` is owned by
  the typed transport/cost reconciliation CAS entrypoint and may update only the referenced exposure plus this cost state;
- `retention_state: retained -> purged_tombstone` is owned by the typed quarantine retention CAS entrypoint and advances
  at the fixed `retention_until` deadline independently of cost progress; that same entrypoint alone clears purgeable
  payload refs/timestamps while retaining identity and digests.

Neither axis resets or gates the other: cost reconciliation cannot extend retention, and payload purge cannot erase the
digest/tombstone identity needed for later conservative cost reconciliation. Neither owner may change `authorizable`,
rebind the row to a new claim, emit a workflow/domain event, or feed reducer planning. At retention expiry the protected
payload artifact is deleted and the row becomes a digest-only tombstone, preserving duplicate detection and provenance.
There is no promotion, retry, or restore transition; retry means a new command claim and a new physical call.

Quarantine rows and artifacts are absent from public command/activity APIs, model context, normal evidence bundles, and
product projections. They may be read only by authenticated audit/cost-reconciliation tooling. Writing this bounded
transport evidence does not weaken the R-019 rule: the stale command/business transaction itself still writes no command,
action, run, activity, intent, event, EntityDelta, artifact publication, child, or domain state.

### 9.4 Current-claim deterministic non-authorized outcome — owner write allowed

An evaluated result such as evidence insufficiency, ambiguity, budget exhaustion, model fallback, or timeout is not a
CAS miss when every stored claim and business predicate still matches. The independent current record-command owner may
atomically apply the declared typed non-authorized `record_outcome` as one of `awaiting_budget`, `needs_human`, `failed`,
or `timed_out` and emit the single section-6.2.1 discriminated owner event. The mapping is closed: evidence remains insufficient after a
step/cost/search envelope is exhausted and policy permits a later grant -> `awaiting_budget`; hard envelope/policy
terminal exhaustion, semantic ambiguity, protocol-invalid output, model fallback, or policy-invalid result -> `needs_human`; non-retryable execution failure ->
`failed`; deadline/`max_wall` -> `timed_out`. `awaiting_budget` is a true intent/domain transition that leaves the
verification current-state row unauthorized; it is not a stale/business-CAS miss and must not be relabeled
`needs_human` while a future grant remains permitted. Conversely, hard no-more-grant exhaustion must not advertise a
recoverable grant action. The owner may not mislabel any of these business outcomes as stale.
The record branch neither locks nor consumes a grant; the durable section-6.2.3 intent/event/watermark convergence
observes an already-active or later grant without freezing a pre-gate business digest.

This taxonomy resolves the earlier D3 wording that wrote a durable `not_applied` event for every stale mismatch. R-019
controls claim and business CAS misses: both are zero command/domain durable-write outcomes unless the residual ledger
later records an explicit reviewed exception. The only in-flight carve-outs are reconciliation of an exposure whose
dispatch authorization already committed, its stable response or attempt-failure evidence receipt, and insert-once
late-result quarantine identity tied only to a response receipt; none is a fresh command/domain effect authorization.

## 10. Public projection and token secrecy

Before any new internal descriptor column becomes readable, the backend command query adapter must switch from
implicit-field dict spreading to one checked-in public allowlist.

The owner decision is:

- authenticated command list/detail/provenance responses may expose `claim_generation` and `control_epoch` as
  non-authoritative diagnostics;
- they never expose raw `claim_token`, `claim_token_digest`, a token preview/hash prefix, authority id/seal,
  `last_heartbeat_id`, private `ClaimAuthority`, `ClaimReceipt`, or `ClaimIdentity` object;
- public schemas and TypeScript adapters list the two diagnostic fields explicitly;
- frontend `raw` records are built only from the server-safe public object and cannot recover omitted internal fields;
- logs, result JSON, operation events, ActivityAttempt envelopes, artifacts, fixtures, model messages, and error payloads
  must not contain token or digest material.
- transport-response receipts and quarantine rows/artifacts are not provenance/list/detail/activity/product projections;
  only the dedicated authenticated audit/cost path may read their model-safe digest/ref metadata.

Token/digest leak is an authorization failure, not a formatting defect. A leak blocks D3 activation and requires token
rotation for every affected active claim. Terminal transitions clear the digest, minimizing the retained verifier;
terminal exact replay relies on result/event idempotency and never needs token material.

## 11. Brownfield migration contract

This decision-lock batch contains no migration. The later implementation uses four deployment-safe steps; owner-specific
verification-intent, grant, cost-ledger, and typed plan/review/gate DDL may land in their owning migrations, but all must
reach the identities below before strict D3 dispatch is enabled.

### 11.1 Migration A — additive foundation

Migration A:

1. uses `SET LOCAL lock_timeout = '5s'`;
2. adds `runtime_namespace/provider_mode/workspace_id/scope_issuer/scope_digest`, the five typed
   `creation_source_*`/`creation_plan_*` causal fields, and `creation_idempotency_key` from section 5.2 to
   `plan_review_sessions`; all are additive sentinels, and the existing legacy creator remains unscoped/ineligible;
3. adds the operation root exact-copy scope columns and nullable BIGINT `coordination_plan_review_id` from section 5.2 to
   `operation_runs`;
4. reuses `workflow_commands.operation_id` as the sole operation link and adds exactly these **twenty** strict command
   columns from section 5: `runtime_namespace`, `provider_mode`, `workspace_id`, `scope_digest`,
   `coordination_plan_review_id`, `claim_authority_spec_digest`, `expected_predecessor_intent_id`,
   `expected_predecessor_phase_generation`, `expected_predecessor_source_control_epoch`,
   `expected_predecessor_decision_source_event_id`, `d3_business_fence_digest`, `claim_selection_generation`,
   `consumed_claim_authority_id`, `claim_generation`, `claim_token_digest`, `control_epoch`, `heartbeat_sequence`,
   `last_heartbeat_id`, `terminal_event_id`, and `terminal_outcome_digest`; it does not add an
   `operation_run_id` alias to that table;
5. adds scope/coordination/spec/business-fence columns to `workflow_activity_runs`, and
   operation/scope/coordination/spec/business-fence/generation/post-claim `command_attempt`/epoch columns to
   `workflow_activity_attempts`;
6. reuses `workflow_events.operation_id` and adds scope/nullable-BIGINT-coordination/activity-run/generation/epoch/spec/
   business-fence/outcome-digest plus the seven-field immutable source core and complete §6.2.1
   response/failure/no-exposure provenance columns—including
   historical terminal-provenance policy, response-spec, and terminal-reason pins—to
   `workflow_events` and the verification-intent source terminal tuple;
7. creates `transport_response_receipts` with the §6.6 stable delivery/occurrence/exposure/envelope identities plus
   `terminal_provenance_policy_digest`, `response_spec_digest`, and `terminal_reason`,
   `transport_attempt_failure_receipts` with the stable failure occurrence/spec/artifact identity plus
   `terminal_provenance_policy_digest/retry_policy_revision/retry_disposition`, and
   creates `workflow_late_result_quarantine` with the section-9.3 receipt FK, idempotency formula, and valid new-table
   constraints;
8. adds shape checks as `NOT VALID` on brownfield tables (scope checks permit empty sentinel during rollout):
   - `workflow_commands_runtime_namespace_shape_ck` (empty or canonical non-blank text);
   - `workflow_commands_provider_mode_shape_ck` (empty or one of the four normalized modes);
   - `workflow_commands_workspace_id_shape_ck` (empty or canonical non-blank text);
   - coordination review id checks (SQL NULL sentinel or positive BIGINT) on each affected table; every FK/index uses
     the same BIGINT type as `plan_review_sessions.review_id`;
   - scope/authority/business-fence digest checks (empty or lowercase 64-hex) on each affected table;
   - terminal outcome digest checks (null or lowercase 64-hex) and the local both-null-or-both-non-null pair-shape
     check; status parity waits for Migration C cutover;
   - consumed authority id shape check (empty sentinel or canonical opaque non-blank id);
   - `workflow_commands_claim_generation_nonnegative_ck`;
   - `workflow_commands_claim_selection_generation_nonnegative_ck`;
   - `workflow_commands_control_epoch_nonnegative_ck`;
   - `workflow_commands_heartbeat_sequence_nonnegative_ck`;
   - `workflow_commands_claim_token_digest_shape_ck` (empty or lowercase 64-hex);
   - quarantine enum checks for the independent `cost_state` and `retention_state` axes plus immutable
     `authorizable=false`;
   - response-receipt and attempt-failure-receipt provider-id variants, lowercase digests, stable occurrence identities,
     complete artifact pairs, registry-valid historical policy/response/failure spec and retry disposition, and
     immutable exposure/evidence shapes; D0-valid `length`/`content_filter` remains response-shaped, while incomplete/
     truncated/protocol-parse failure remains attempt-failure-shaped; event
     fields match exactly one of the three §6.6 variants, while quarantine admits only a D0-valid response receipt;
   - the command-specific expected-predecessor local shape check: all four SQL NULL or all four non-null with canonical
     ids, positive phase generation, and non-negative source epoch; no half-sentinel row;
9. restores the local lock timeout before later migration-runner work;
10. does not validate populated rows in the installation transaction.

`ScopedReviewSessionBootstrapAuthority`, the pre-session Stage-A `ScopedReviewSessionBootstrapReceipt`, and
`ScopedReviewSessionCreateResult` are private typed values, not generic database tables or command columns. The receipt
contains the raw current-claim token/lease and therefore is never persisted or projected; the result alone carries the
new review/event/terminal aggregate. Migration A installs only their durable source/session identities and the repository
constraints needed to replay the committed session-created aggregate.

All existing rows therefore retain their existing `operation_id`, receive selection/consumption sentinel `(0, '')`,
claim-fence sentinel triple `(0, '', 0)`, scope sentinel `runtime_namespace/provider_mode/workspace_id/scope_digest = ('', '', '', '')`, NULL coordination/predecessor tuples, empty spec/business identities, null terminal pair, and
heartbeat sentinel `(0, '')`; existing review sessions also receive empty scope/issuer/causal/idempotency sentinels.
Adding columns does
not make those rows strict, and migration never copies `operation_id` into a second command column.

### 11.2 Migration B — exact scoped-session/backfill cutover

The scoped review-session repository remains the sole scope/coordination issuer. New OperationRuns only exact-copy one
session's scope, and new commands/activities/attempts/events copy the exact parent scope. Brownfield backfill is
permitted only along a fully durable chain:

1. a poll-mode review session is newly issued through private
   `create_or_exact_replay_scoped_plan_review_session(...)` from the closed server-owned
   `ScopedSessionBootstrapContext`. Its `ScopedSessionCreateContext` variant carries a sealed one-use
   `ScopedReviewSessionBootstrapAuthority` and already-minted current-claim `ScopedReviewSessionBootstrapReceipt` under
   the dedicated `scoped-session-bootstrap-lock-v1` scope+idempotency key. Before any write it exact-locks the source
   OperationRun/command/event/plan and checks current generation/epoch/post-claim command attempt/lease, authenticated
   workspace, immutable source event, and plan pins; it does **not** bind a preexisting ActivityAttempt. The specialized
   bootstrap Stage B creates the claim-bound ActivityRun/ActivityAttempt in the same PG UoW as the session/event/source
   terminal aggregate, without requiring a review/gate or normal `ClaimAuthority`, and parses no request/plan/gate JSON.
   Its `ScopedSessionCommittedReplayContext` variant carries no authority, receipt, raw token, or mutation permission and
   only exact-reads the already committed aggregate after token loss. The scope-aware key and all causal/plan fields
   replay the same positive `review_id` and `ScopedReviewSessionCreateResult`; collision writes zero. Strict D3
   OperationRun creation then locks that already-existing session and exact-copies its full scope plus canonical typed
   review id. An action-backed scoped review/root may gain scope only
   after the action owner satisfies the Plan section 6 item 6 action-root durable-scope gate with exact durable action
   namespace/mode/workspace/issuer/digest pins; this is not OB-10.4. Existing unscoped review sessions are never
   backfilled from JSON or adopted by a strict OperationRun; a new typed scoped session is required. A retry/recovery
   child copies the exact parent/root
   scope/coordination; current process environment, root-intent id, request defaults, or PG schema name are never backfill evidence;
2. a command may adopt strict scope only when its existing `operation_id` resolves to exactly one scoped OperationRun
   whose `operation_run_id` equals it, whose positive BIGINT coordination review relation is unique, whose current typed
   plan/review/gate/base-intent pins can reproduce `d3_business_fence_v1`, and whose workflow/action linkage agrees;
   after the command identity is reserved in the command segment, the UoW must lock the typed current intent lineage in
   its later segment and fill either the exact complete predecessor tuple or the candidate-initial all-null tuple before
   commit. An ambiguous/brownfield predecessor, active command, or half tuple remains legacy and cannot be strict;
   otherwise its scope/coordination/business pins stay sentinel. No
   operation-link alias is populated;
3. an ActivityRun copies only from one exact strict command and matching operation/workspace/coordination/business fence;
4. an ActivityAttempt copies only when its ActivityRun and command agree on full scope, coordination, business fence,
   operation, command, claim generation, post-claim command attempt, and control epoch;
5. an event copies only when its command/attempt identity and coordination/business pins are exact; it additionally
   exact-references one response receipt, one attempt-failure receipt, or a registry-admitted complete-none
   `no_exposure` variant; JSON payload is never a backfill source;
6. legacy plan-review JSON is never parsed into authorization revisions/digests. Only a new typed owner transition/session
   may create the strict plan/review/gate pins required by section 6.5.

Any ambiguity is recorded in report-visible `legacy_unscoped` counters and remains sentinel. A legacy row is not upgraded
by claim. Only a scoped, coordination/business/authority-pinned, unclaimed command may receive its first Stage-A claim. An existing
`claimed`/`running` sentinel row is `legacy_unfenced`; no code may fabricate scope, registry digest, or token for it or
treat environment, owner text, `default`, or `attempt` as equivalent proof.

### 11.3 Migration C — structural scope chain and active-row enforcement

After new-row cutover and exact backfill, create scope-aware unique indexes without blocking table rewrite, then attach
composite foreign keys `NOT VALID`:

```text
plan_review_sessions(scope_digest, review_id)
plan_review_sessions(scope_digest, creation_idempotency_key)
operation_runs(scope_digest, coordination_plan_review_id, operation_run_id)
workflow_commands(scope_digest, coordination_plan_review_id, operation_id, command_id)
workflow_activity_runs(scope_digest, coordination_plan_review_id, operation_run_id, command_id, activity_run_id)
workflow_activity_attempts(scope_digest, coordination_plan_review_id, operation_run_id, command_id, activity_run_id, attempt_id)
transport_response_receipts(scope_digest, dispatch_exposure_id, canonical_delivery_identity)
transport_response_receipts(scope_digest, dispatch_exposure_id, response_occurrence_id)
transport_response_receipts(scope_digest, dispatch_exposure_id, transport_response_receipt_id)
transport_attempt_failure_receipts(scope_digest, dispatch_exposure_id)
transport_attempt_failure_receipts(scope_digest, dispatch_exposure_id, failure_occurrence_id)
transport_attempt_failure_receipts(scope_digest, dispatch_exposure_id, transport_attempt_failure_receipt_id)
workflow_events(scope_digest, coordination_plan_review_id, operation_id, command_id, activity_run_id, activity_attempt_id, idempotency_key)
workflow_events(scope_digest, coordination_plan_review_id, operation_id, command_id, event_id, terminal_outcome_digest)
workflow_late_result_quarantine(scope_digest, dispatch_exposure_id, response_occurrence_id)
```

The strict OperationRun coordination pin references
`plan_review_sessions(scope_digest, review_id)` and each child key then references the immediately preceding parent identity, mapping the child's `operation_run_id` or event's
`operation_id` to the command's existing `operation_id`, so a cross-scope child is structurally rejected. The terminal
event additionally exposes the composite unique event key above. The nullable command terminal pair uses a `MATCH SIMPLE
DEFERRABLE` composite FK over
`(scope_digest, coordination_plan_review_id, operation_id, command_id, terminal_event_id, terminal_outcome_digest)` to that event key. The event
idempotency key separately has an exact-replay collision check on coordination/generation/epoch/spec/business/result digests.
The scoped-session idempotency key uses exact collision equality across every scope/causal/plan/initialization field.
Every coordination component above is nullable BIGINT in brownfield DDL and the same physical type as
`plan_review_sessions.review_id`; strict rows require it positive. Transport-backed events and quarantine rows also use
nullable `MATCH SIMPLE DEFERRABLE` receipt FKs over the applicable unique identities; event-local checks enforce exactly
one response, attempt-failure, or registered `no_exposure` shape, while quarantine has only the response-receipt FK.

Future native-PG migration acceptance must install that actual FK and prove all of the following against populated DDL:

- strict OperationRun insertion with a NULL/non-positive review id or a review from another scope is rejected, while the
  exact canonical review id is accepted and copied through command/activity/event rows;
- bootstrap Stage A consumes one exact-selection authority once and returns a pre-session receipt whose current source
  claim generation/epoch/post-claim command attempt/lease exact-match the command; no ActivityRun/ActivityAttempt exists
  yet. Duplicate consume, forged receipt, wrong raw token, expired lease, or authority/receipt drift writes zero;
- `ScopedSessionCreateContext` without either exact authority or receipt is rejected; the receipt field inventory has no
  ActivityRun/ActivityAttempt, `review_id`, `session_created_event_id`, or session terminal digest. The specialized
  bootstrap Stage-B/session UoW creates ActivityRun/ActivityAttempt and returns them only in
  `ScopedReviewSessionCreateResult`; the result has no raw claim token/lease or authority-refresh behavior;
- a crash after Stage A but before the create UoW commits no ActivityRun/ActivityAttempt/session/event and requires lease
  expiry/reselection plus newly minted create credentials. A crash after aggregate commit but before reply uses only
  `ScopedSessionCommittedReplayContext`, returns the same review id, source ActivityRun/ActivityAttempt, session-created
  event, source review-request command terminal pair, and `ScopedReviewSessionCreateResult`, and never reconstructs or
  returns authority/receipt/raw token. Failure between create writes rolls all back; same-key causal, plan, scope, event,
  terminal-pair, committed aggregate identity, or initialization drift writes zero, and the legacy JSON-scan creator
  cannot seed a strict row;
- the bootstrap advisory key contains only the exact scope tuple plus `creation_idempotency_key`, never a review/gate;
  it serializes duplicate creation while normal `d3-dispatch-v2` is unavailable, and cannot authorize transport/domain
  or a second command/event family;
- stale/cancelled source review-request bootstrap authority, non-current/mismatched bootstrap receipt, source
  generation/epoch/post-claim-command-attempt/lease drift, mismatched plan/event, or authenticated-workspace/source-
  OperationRun scope mismatch creates no ActivityRun/ActivityAttempt/session/event/command write; a normal
  `ClaimAuthority` requiring positive review lineage is rejected at this entrypoint;
- the two command-population manifests exact-match their disjoint registry predicates. Bootstrap active and committed-
  terminal rows satisfy only the bootstrap-specific guarded shapes above; normal `d3_v1` active/result-terminal rows
  satisfy only the positive-review/business-fence guarded shapes below;
- every normal ActivityAttempt exact-copies
  `command_attempt = ClaimIdentity.attempt = workflow_commands.attempt`, while the specialized bootstrap UoW exact-copies
  `command_attempt = ScopedReviewSessionBootstrapReceipt.source_command_attempt = workflow_commands.attempt`; stale
  command-attempt drift rejects before the attempt, intent, event, result, or domain write, and the verification intent's
  immutable source core preserves that exact `source_command_attempt`;
- strict command creation with an absent or non-recomputable typed `d3_business_fence_v1` digest is rejected;
- `pg_constraint.confmatchtype='s'` and `condeferrable=true` for the terminal FK;
- a non-result-terminal command with the terminal pair both null inserts/updates successfully;
- either half-null terminal pair is rejected by the command-local pair `CHECK`;
- a complete pair referencing a missing or mismatched event is rejected, while the exact event identity is accepted;
- referenced-event update/delete cannot orphan an existing command terminal pair.
- direct SQL can still create an unreferenced event because the FK is one-way; repository acceptance must prove every
  public terminal-event writer is the one-PG terminal UoW and injected failure before command/attempt terminalization
  rolls the event back;
- the registry preflight proves the full three-variant union and exactly one applicable entry per
  `(command_type, stage_id, terminal_transport_variant, terminal_status, terminal_event_type, terminal_outcome,
  terminal_reason if exposure, failure_code if attempt_failure)`; the command/exposure historical policy digest and old
  spec retention survive a registry upgrade across every retained receipt/event/source-intent/quarantine/audit reference;
- an `exposure` terminal event must exact-reference one response receipt with the same historical policy/response-spec,
  terminal reason, exposure/call/envelope/occurrence/canonical response/result/artifact and claim pins; another physical response,
  same-result/different-`result_ref`, or half artifact pair is rejected;
- an `attempt_failure` terminal event must exact-reference one failure receipt with its exact exposure/call/failure
  occurrence/code/spec/canonical digest/retry revision/terminal disposition/artifact and claim pins, has no D0
  envelope/response/result fields, and rejects any cross-variant or half-pair shape; a retryable disposition can only
  reach `retry_wait` with zero terminal/source-tuple write; registered `no_exposure` is accepted only with complete-none
  transport fields and exact spec digest;
- response creator, failure creator, retry transition, and response/failure terminal branches lock the same committed
  exposure `FOR UPDATE`: response-first rejects retry/attempt-failure and uses the current response, retry-first advances
  epoch and makes a later response quarantine-only, while attempt-failure terminal first permits only a later response
  receipt plus quarantine;
- `no_exposure` terminalization holds the common coordination+participating-command locks, proves absence because
  dispatch creation shares that prefix, and never treats `FOR UPDATE` against a nonexistent exposure as a fence;
- D0-valid `terminal_reason=length|content_filter` follows response receipt/current fail-closed typed outcome or late-
  quarantine semantics and never auto-confirms; only
  incomplete/truncated wire or protocol parse failure follows attempt-failure semantics, which never inserts quarantine;
- receipt and quarantine retry/redelivery reuse the stable occurrence and exact `late-response-v1` key; a digest or
  canonical-delivery collision writes zero.

Before installing population-sensitive checks, Migration C materializes the canonical
`scoped_session_bootstrap_command_types_v1=(...)` literal set and digest from the exact sorted
`CommandTypeSpec.claim_fence_policy=scoped_session_bootstrap_v1` entries and the disjoint
`strict_d3_command_types_v1=(...)` literal set and digest from the exact sorted
`CommandTypeSpec.claim_fence_policy=d3_v1` entries. It also materializes/verifies the canonical terminal-provenance
manifest digest and exactly-one-applicable-spec relation used by every fenced command entry. The migration test proves
parity with both command-population manifests and the terminal-provenance registry.
Existing immutable `workflow_commands.command_type` is the physical discriminator; an empty scope, coordination id,
authority digest, or business digest is never accepted as a guard. A future bootstrap or strict-D3 type requires its
matching manifest migration before the registry entry can activate.

After the bootstrap writer is deployed and bootstrap sentinel claimants have drained, Migration C first installs a
bootstrap-specific `NOT VALID` active-row invariant guarded only by
`scoped_session_bootstrap_command_types_v1`. A claimed/running bootstrap row must have canonical server-owned
namespace/mode/workspace/scope, a non-empty authority-spec digest, positive selection and claim generations, consumed
authority identity, current token/lease shape, and its exact current post-claim command attempt/control epoch. Because
the review session does not exist yet, it must simultaneously have
`coordination_plan_review_id IS NULL`, `d3_business_fence_digest=''`, and a null terminal pair. It is not subject to the
normal positive-review/business-fence checks below. On its specialized Stage-B/session terminal commit, token/lease are
cleared and its exact terminal pair must equal the `source_terminal_event_id/source_terminal_outcome_digest` in the
committed `ScopedReviewSessionCreateResult`; the same result must exact-reference the newly created source
ActivityRun/ActivityAttempt and scoped session event. Any bootstrap row outside these active or committed-terminal shapes
is rejected. Native-PG acceptance exercises both shapes and proves that a bootstrap row cannot satisfy the normal
`d3_v1` branch or vice versa.

After the strict D3 mint writer is deployed and active sentinel rows for that materialized D3 population have
terminalized or expired, Migration C adds this `NOT VALID` invariant. Non-D3 command types remain outside it and keep
running during the scope-local strict-D3 consumer cutover; the full 29-wrapper inventory remains observational only:

```text
command_type NOT IN strict_d3_command_types_v1
OR status NOT IN ('claimed', 'running')
OR (
  operation_id is non-empty
  AND runtime_namespace is non-empty
  AND provider_mode is a normalized supported mode
  AND workspace_id is non-empty
  AND coordination_plan_review_id IS NOT NULL
  AND coordination_plan_review_id > 0
  AND scope_digest, claim_authority_spec_digest, and d3_business_fence_digest are lowercase 64-hex
  AND claim_selection_generation > 0
  AND consumed_claim_authority_id is non-empty
  AND claim_generation > 0
  AND claim_token_digest matches lowercase 64-hex
  AND heartbeat_sequence >= 0
)
```

Strict D3 active rows additionally require a non-blank `stage_id` admitted by the pinned registry spec and exact
operation/command/activity/event coordination lineage. Command creation/backfill acceptance recomputes
`d3_business_fence_v1` from typed rows and rejects any mismatch. The command-specific predecessor check is strengthened
from shape-only to semantic acceptance: all-null is admitted only for a registered initial Stage-B command whose locked
lineage has no current phase; every successor/no-successor command has the complete tuple and a matching physical
predecessor. These cross-row facts are repository CAS/PG acceptance tests, not fictional `CHECK` expressions.

Strict D3 result-terminal rows also receive a separate registry-manifest-guarded local `NOT VALID` pair/status check:
`command_type NOT IN strict_d3_command_types_v1 OR (...)`; inside the guarded branch,
`succeeded`/`failed_terminal` requires a non-null `terminal_event_id`, lowercase 64-hex non-null
`terminal_outcome_digest`, and empty token digest; every other status, including control-terminal `cancelled`, requires
the result-terminal pair to be null. Non-D3 legacy success writers are unaffected. Exact event-row existence is
enforced by the composite FK, never asserted by a cross-table
`CHECK`. Reopening a terminal command through the registered retry transition nulls the command's terminal pair before
it becomes claimable, while the immutable historical event remains.

Native-PG transition acceptance also proves the epoch partition mechanically: each executable-input/control/requeue/
reopen row advances exactly once and clears token/lease; first result-terminalization retains the epoch, clears
token/lease, and installs the immutable pair; exact replay advances zero. Every D3 OperationRun terminal/cancel/retry/
requeue/resume/reset/rebuild/recovery race participates in the same v2 advisory key and has no third exemption.

Installing this invariant before fleet-wide mint cutover would make an old claimant or heartbeat fail unpredictably;
therefore it is separate. No constraint is treated as validated merely because new D3 rows satisfy it.

### 11.4 Migration D — validation and sentinel deletion eligibility

Only after the rolling/deletion gates pass are shape checks, bootstrap/session exact replay, review-lineage/downstream/
receipt composite foreign keys, business-digest recomputation reports, predecessor-shape acceptance, three-variant
registry parity, stable response/failure-occurrence, response-vs-retry ordering, no-exposure absence fencing, quarantine
replay, and the active-row check validated,
each in bounded later transactions. Constraint installation and validation never share one migration transaction. The
empty sentinels remain readable for retained terminal history; deleting compatibility branches does not rewrite them into
synthetic scope. The action-root durable-scope gate, OB-10.1/10.2/10.3/10.4 owner migrations, and typed plan/review/gate
columns must be present and validated for the applicable strict D3 population before transport activation, even if
global legacy validation is still staged.

## 12. Rolling deployment and compatibility bridge

The rollout order is fixed:

1. deploy the explicit public allowlist so internal columns cannot leak;
2. install Migration A;
3. extend canonical `CommandTypeSpec` with the three closed `claim_fence_policy` values, stage policy/ids, and
   `terminal_provenance_policy_digest`; install the sole three-variant `TERMINAL_PROVENANCE_SPECS` registry and its
   exactly-one-applicable-spec preflight; install `mint_scoped_review_session_bootstrap_authority(...)` plus
   the bootstrap exact-selection/one-use Stage-A receipt path, `verify_scoped_session_bootstrap(...)`, and their private
   sealed types **dormant**, before any session creator or
   `d3_v1` path activates; generate the hash-bound bootstrap, strict-D3, and terminal-provenance manifests and retain
   every historical spec while any durable lifecycle reference remains;
4. deploy the scoped review-session repository as the sole scope/coordination issuer, using only the dedicated
   `scoped-session-bootstrap-lock-v1` order and closed `ScopedSessionBootstrapContext`: the create variant carries the
   one-use authority plus already-minted current-claim receipt and atomically creates its ActivityRun/ActivityAttempt;
   the committed-replay variant carries no credential and is read-only. Return only `ScopedReviewSessionCreateResult`
   and deploy causal exact replay. Then deploy OperationRun/downstream exact-copy writers without adopting legacy pins.
   Only after the session exists may normal positive-review `d3-dispatch-v2`/`d3_business` paths run. Separately satisfy the
   action-root durable-scope gate before enabling any action-backed strict root;
5. deploy selection-generation/expiry and one-use authority consumption, Stage-A receipt minting, central
   authority/claim predicate, private normal exact-selection factory, four typed expected-predecessor command pins,
   heartbeat occurrences, and compatibility counters; mint one fresh command/selection-bound normal authority only
   after each scheduler selection, with no startup-minted or standing wrapper authority;
6. migrate every invalidating command input/control/requeue transition in section 7 to advance epoch and invalidate
   token, retain epoch on first result-terminalization, and route every dispatch-invalidating D3 OperationRun mutation
   in the exact terminal/cancel/retry/requeue/resume/reset/rebuild/recovery inventory through the same v2 advisory key;
7. land the canonical business-fence digest and the six-phase sealed-context plan/review/gate/intent evaluator, satisfy
   the action-root durable-scope gate for any action-backed population, and close OB-10.1/10.2/10.3/10.4 for the strict
   D3 population; all strict D3 owner paths remain dormant and non-claimable until this step is complete;
8. deploy Stage B atomic successor convergence, distinct source/record claim binding, the one-transaction record/terminal
   UoW, stable response and attempt-failure receipts, historical policy/spec pins, response/failure/retry shared-exposure-
   lock fencing, coordination+command-fenced no-exposure absence proof, exact three-variant terminal provenance and
   retry disposition, post-gate awaiting-budget resume
   convergence, and typed quarantine insert/cost/retention entrypoints for the exact D3 command set; only then deploy the
   advisory-coordinated dispatch boundary, still disabled for live;
9. allow legacy active sentinel claims to finish, or after lease expiry move them through an explicit owner recovery
   transition and a new fenced claim; never adopt them in place;
10. run Migration B exact cutover/backfill, then install Migration C structural plus separate bootstrap-manifest-guarded
   and strict-D3-manifest-guarded active/terminal `NOT VALID` constraints; unlisted legacy command types remain
   unaffected;
11. validate D3 strict races and enable only the fake/scripted non-live D3 command path;
12. migrate every consumer eligible to receive a bootstrap or strict-D3 manifest type until the scope-local fenced-
   population compatibility count is zero. Keep the generic legacy compatibility path while any
   `legacy_unfenced` registry entry/caller remains; D3b does not invent the authority policy for that population;
13. observe the scoped deletion window, run Migration D validations in bounded deployments, and remove only the
   bootstrap/strict-D3 compatibility branches proven dead by §13.

The compatibility bridge is fail-closed and report-visible:

- sentinel or malformed identity is never accepted by a strict D3 consumer;
- hits are counted by command type, caller module, status, namespace, and provider mode without token material;
- the bridge cannot construct a pre-claim authority from strings/rows, mint one at wrapper startup, bypass exact
  selection generation/expiry/one-use consumption or registry stage policy, infer command scope/coordination/business
  pins from ambient environment/JSON/root-intent ids, mint a token after
  claim, infer generation from `attempt`, or authorize from `lease_owner`;
- a bridge hit blocks D3 live/manual/product/milestone signoff for that scope;
- a non-D3 compatibility path does not make D3 strict behavior less strict, but it keeps the generic legacy bridge and
  global R-019 open.

## 13. Scope-local fenced-bridge deletion gate

Only the bootstrap/strict-D3 compatibility branches may be deleted when all of these are true:

1. every wrapper/caller eligible to receive a command type in either fenced manifest receives only its matching fresh
   per-selection exact-command authority from the private composition-installed factory; selection generation/expiry
   and atomic consumed-authority id prove one use, wrappers hold no standing capability, and Stage A returns matching
   current receipts from the single writer;
2. every effect consumer for either fenced manifest uses its central bootstrap or normal claim/business predicate and
   direct claim SQL outside the registered writer remains zero for those populations;
3. no active row whose `command_type` is in the hash-bound `strict_d3_command_types_v1` population has empty
   operation/scope/spec/business identity, NULL/non-positive coordination identity, `claim_generation=0`, empty token
   digest, or malformed digest/predecessor shape; and every active or committed-terminal row in
   `scoped_session_bootstrap_command_types_v1` matches its separate pre-review or committed-result shape. Legacy command
   types need not acquire either fenced population's pins;
4. unfenced claim attempts and unfenced consumer hits are zero for one complete release window;
5. the observation window is also longer than the maximum configured lease plus retry/recovery horizon, so dormant old
   work cannot reappear after measurement ends;
6. Migration A-D bootstrap and strict-D3 population shape/chain/active/local-terminal checks, terminal/receipt composite FKs, stable
   response/failure occurrence identities, bootstrap one-use Stage-A receipt shape/current-claim checks plus separate
   session create-result exact replay, three-variant manifest parity, and exact
   twenty-column/twenty-six-owner inventories have been validated in their later separate transactions; no report
   counts legacy/non-D3 absence of D3 pins as a failure;
7. list/detail/provenance/frontend token-leak tests are zero-hit;
8. authority-forgery/duplicate-consume/stage mismatch, all-participating-command lock order, every
   terminal/cancel/retry/requeue/resume/reset/rebuild/recovery OperationRun path, initial-all-null/complete-predecessor
   tuple ABA, post-gate awaiting-budget grant-first/record-first convergence, heartbeat replay, source-vs-record
   claim/current-source locking, one-use bootstrap authority + pre-session current-claim receipt carried only by the
   create context, receipt-vs-command/post-claim-attempt/lease equality, specialized Stage-B ActivityRun/ActivityAttempt
   creation, credential-free committed-replay context, create-result separation, both crash windows, and dedicated lock, six-context normal
   business-fence drift, record/terminal atomicity, and stale-race matrices are green on native PG;
9. every strict-D3 authorized transport response has one stable receipt/exposure/envelope occurrence; it is either
   normally accepted under a current claim or has one insert-once `late-response-v1` non-authorizable quarantine
   identity, with typed monotonic cost/retention axes and no reducer/domain consumer; each committed-exposure
   pre-response failure uses the exact typed attempt-failure receipt and never fabricates a response envelope; PG races
   prove response-first versus failure-terminal-first plus response-first versus retry-first ordering, prove
   `no_exposure` absence under coordination+command locks without a missing-row lock, preserve D0-valid
   `length|content_filter` as response evidence with current fail-closed/no-auto-confirm outcome, and prove incomplete/
   truncated/protocol-parse failure never quarantines;
10. the Plan section 6 item 6 action-root durable-scope gate is satisfied for every action-backed strict row;
    OB-10.1/10.2/10.3/10.4 are scope-locally closed for D3 strict rows and no typed plan/review/gate predicate reads JSON;
11. strict-D3 terminal exact replay is proven field-for-field against command terminal event id/outcome digest,
    immutable event identity, and the exact response-receipt, attempt-failure-receipt, or registered complete-none
    `no_exposure` provenance, including historical terminal-provenance policy/response/failure/no-exposure spec digests,
    terminal reason, retry revision/disposition, full source tuple, `result_ref`, and complete-pair shapes;
12. scope-local bootstrap/strict-D3 compatibility branch deletion has a mutation test proving the old path cannot be
    restored silently.

The complete 29-caller inventory remains the observation and zero-unauthorized-increase denominator; it is **not** a D3b
requirement to invent authority policies for or cut over every legacy caller. D3 scope/business/terminal pins and their
structural/status checks are required only for the hash-bound `strict_d3_command_types_v1` population, while the separate
bootstrap shapes apply only to `scoped_session_bootstrap_command_types_v1`. While any `legacy_unfenced` registry entry or
caller remains, the generic legacy claim compatibility path is explicitly not deletable and R-019 remains open. Retained
legacy command types need not gain D3 pins merely to satisfy this scope-local gate.

## 14. R-019 bounded subclosure

This decision document closes no runtime residual by itself. A later D3b implementation may claim only these bounded
subclosures after exact evidence exists:

- physical claim generation/token/control-epoch minting has one writer;
- scoped review-session bootstrap no longer depends on the review id it creates: the dedicated scope+idempotency lock,
  one-use bootstrap authority and pre-session Stage-A current-claim receipt carried by the closed context, source
  generation/epoch/post-claim-command-attempt/lease, authenticated workspace, event, and plan pins authorize only the
  specialized Stage-B/session create UoW, which creates the ActivityRun/ActivityAttempt and session aggregate together;
  review/activity/event/terminal identities exist only in the create result. Post-commit replay uses a credential-free
  read-only context, while the pre-commit crash window requires lease expiry/reselection, before normal positive-review
  D3 begins;
- every D3 wrapper receives a private per-selection exact-command authority rather than caller owner/type text or a
  wrapper-held standing capability, and Stage A consumes it once;
- D3 Stage B cannot create a phantom ActivityAttempt or verification intent after command invalidation;
- D3 Stage B atomically converges predecessor supersession with successor attempt/intent creation;
- initial Stage B proves no current phase under a positive BIGINT canonical review-lineage key, while every successor uses one
  complete physical predecessor tuple; half-sentinel and root-intent-derived coordination are impossible;
- D3 record applies and terminalizes in one transaction only under its own current claim plus immutable source
  terminal-event/transport-receipt provenance;
- Stage B, terminal, record, dispatch, resume, and control evaluate one immutable command business-fence digest through
  the centralized phase predicate, and control-first races write no attempt/intent/event/command/source tuple;
- record-first and grant-first `awaiting_budget` races converge through recorded-event/gate-watermark/grant durable state
  and one post-apply resume-v2 command/outbox without consuming grant capacity outside physical dispatch;
- a control transition that linearizes before D3 dispatch authorization causes zero send, while a previously authorized
  in-flight call cannot apply a stale result;
- every D3 effect/terminal boundary rejects a stale claim with zero command/domain durable writes;
- terminal command/attempt/event/source binding commits atomically against one exact response receipt, terminal-
  disposition attempt-failure receipt, or registered no-exposure variant; retryable failure writes only receipt plus
  `retry_wait`; response/failure/retry share the committed exposure lock, response-first rejects retry, retry-first makes
  a later response quarantine-only, and no-exposure proves absence under coordination+command locks without locking a
  missing row; attempt-failure provenance is an envelope-free cost/audit sink, while D0-valid `length|content_filter`
  remains response evidence and only a late valid response uses one stable response-receipt-backed immutable quarantine
  identity with two typed monotonic state axes;
- the requeue-before-reclaim ABA window is fenced.

R-019 remains open for:

- the 26 production action/operation state-sync call ratchet;
- generic operation + action + event/command transaction unification;
- retry reservation/child/event atomicity outside the migrated D3 path;
- other command owners that can still create phantom children or attempts;
- linked OperationRun/AgentAction post-sync;
- reducer event -> command -> outbox -> state multi-commit and concurrent count coherence;
- monotonic total acquisition budget and permanently held transaction-lock behavior.

No D3b report may describe this contract as global operation atomicity, global workflow-command fencing, or R-019
closure. The residual row remains `pending remediation`, and its 26-call ratchet must not increase.

## 15. Served, migration, and activation boundary

- Served Agent tool population remains **zero**. This document does not add an ActionSpec, request schema, dispatch
  adapter, Activity policy, result schema, serializer, or served predicate.
- All production actions remain subject to R-029; D3b does not validate D1 `NOT VALID` constraints or move an action
  into the served registry.
- The claim-fence columns and checks described here do not exist until a later implementation/migration commit.
- the action-root durable-scope gate, OB-10.1/10.2/10.3/10.4, typed plan/review/gate columns, cost exposure, and
  quarantine are still future physical work;
  describing their consumption here is not implementation evidence.
- No full `tests/test_pipeline.py`, provider/model, live, W6/nightly, browser/manual, or product validation belongs to
  this decision-lock batch.
- A valid scope-matched non-author review is required before implementation signoff. Advisory model output, author
  evidence, and local review cannot become formal `GO` evidence.
- Future Thinking Machines Lab or other company canaries remain downstream of fake/scripted completion, strict D3
  claim-fence evidence, and the formal review/live authorization gates.

## 16. Implementation closure matrix

| Gate | Exact required evidence |
|---|---|
| schema install | populated native PG; exact §5/§11 20-command-column and §4 26-owner-row inventory; nullable-BIGINT coordination types match canonical `review_id`; five-second lock timeout; install rollback; brownfield constraints `NOT VALID`; concurrent RowExclusive DML continues |
| private authority | knowing command id or constructing owner/type/stage strings cannot claim; normal and bootstrap factory mint occurs only after exact scheduler selection; selection generation/expiry plus atomic consumed authority id enforce one use; bootstrap Stage A returns its matching pre-session receipt; wrapper-startup/standing authority and issuer/registry/spec/stage/worker/scope drift fail closed |
| scope chain | scoped review-session repository is the sole issuer; before review creation, only `ScopedSessionCreateContext` carrying its one-use bootstrap authority plus already-minted current-claim receipt and `scoped-session-bootstrap-lock-v1(scope tuple, creation_idempotency_key)` authorize the specialized Stage-B/session UoW; both exact-match source generation/epoch/post-claim command attempt/lease, no ActivityAttempt exists before that UoW, and review/activity/event/terminal ids belong only to `ScopedReviewSessionCreateResult`. `ScopedSessionCommittedReplayContext` is credential-free/read-only after commit; no normal ClaimAuthority/review/gate/transport/domain is admitted. Afterward OperationRun exact-copies canonical review lineage and the command -> ActivityRun -> ActivityAttempt chain; ambiguous backfill remains ineligible |
| mint | normal or bootstrap authority-bound concurrent claimant winner; selection authority is consumed once, generation increments once, token is unique per claim, digest only is in PG, and the matching private receipt carries current generation/epoch/attempt/lease; receipt never carries later session result ids |
| reset independence | retry, resume, partial-progress, prerequisite-wait, and retryable failure change `attempt` without resetting generation |
| epoch | every executable-input/control/requeue/reopen row advances once and clears token/lease; first result terminal retains epoch and installs immutable pair; exact replay advances zero; OperationRun terminal/cancel/retry/requeue/resume/reset/rebuild/recovery all use v2 coordination |
| Stage A/B | crash after A leaves no attempt/intent; control between A/B yields zero-write; all 4 nullable predecessor pins are physical command columns; initial Stage B admits only all-null while proving no phase; every successor uses the complete tuple; fault rolls back attempt + predecessor supersession + successor intent together |
| source/record | only `final_adjudication` plans the single typed-outcome record command; exact five-row outcome table and distinct control timeout; source intent binds command/generation/epoch/post-claim command attempt/ActivityAttempt/status/event plus historical terminal-policy/spec/reason and every response/failure/no-exposure provenance/retry field; record validates `phase=record` then `phase=terminal` and commits domain+intent+single physical event+attempt+command atomically under its independent claim |
| awaiting convergence | record-first, grant-first, duplicate, retry, and crash orders converge through awaiting intent + `recorded_event_id` + applied gate watermark + durable grant; gate apply creates/exact-replays one post-apply `resume_after_grant` v2 command/outbox; no pre-gate digest, record/resume grant consumption, late command lock, or one-shot wakeup |
| lease/heartbeat | expired owner rejected before reclaim; reclaim wins once; occurrence replay never re-extends; stale/future/wrong-generation heartbeat and renewal-horizon overflow write zero |
| dispatch coordination | after session creation, common bounded v2 key = scope tuple + canonical positive BIGINT review-id encoding plus exact global order; all current owner/source/record/resume/supersession/idempotency-target command rows lock before intent; busy = retry/no write/no send; every command and full OperationRun invalidation inventory participates; only heartbeat/read-only exact replay are exempt; bootstrap uses only its pre-review dedicated key |
| dispatch predicates | one normal `verify_d3_business_fence(phase, locked_rows, context)` implementation accepts five closed `ClaimedCommandContext` variants plus one closed `AggregateControlContext`; all six positive-review phases exact-check typed pins/manifests and OB-10.1-10.4; bootstrap is excluded and `verify_scoped_session_bootstrap(...)` exact-checks both one-use authority and matching current-claim receipt; wrong context fails closed; control-first is zero command/domain effect/send |
| consumers | Stage B, terminal, record, dispatch, resume, and control use the central business evaluator; all provider/model sends use transport authorization |
| mechanical counters | physical command mutators remain 13 or intentionally decrease; claim/running remain 29; success/failure/partial/wait remain 39/71/15/3; 34 child planners stay event-owned or intentionally decrease |
| stale | missing/foreign authority seal, owner/type/stage/scope/coordination, generation, token, epoch, lease, status, or attempt return only `not_applied(reason=stale_claim, detail_code)` with identical zero command/domain durable-write snapshots; pre-dispatch rejection proves zero send |
| business conflict | changed operation/plan/review/gate/intent/business digest or phase pin returns only `not_applied(reason=business_precondition_conflict, detail_code)` with zero command/domain writes; `not_applied` is never an event/state; already-authorized transport may persist either receipt, but only a valid response receipt may quarantine |
| business outcome | exact table maps `authorizable/awaiting_budget/needs_human/failed/timed_out` to declared verification+intent state and one discriminated `company_identity_verification_recorded` event; control timeout is distinct; `not_applied` remains zero-write |
| projection | generation/epoch explicit and token/digest absent across list, detail, provenance, schema, adapter, and frontend raw record |
| terminal atomicity | attempt + event + command terminal pair + full source provenance binding commit or roll back together; response/failure/retry branches lock their committed exposure, while `no_exposure` proves absence under the common coordination+command locks and never locks a missing row; `exposure` uses response receipt + D0 envelope/spec, only terminal-disposition `attempt_failure` may terminalize without envelope/result, and retryable goes only `retry_wait`; response-first rejects retry and retry-first makes a later response quarantine-only; race/replay mismatch is zero-write |
| late quarantine | only a D0-valid response receipt—including valid `length`/`content_filter`, which is never auto-confirmed and must use a fail-closed typed outcome while current—may feed quarantine; incomplete/truncated/protocol-parse and other attempt-failure evidence is cost/audit-only; exact `late-response-v1:<scope_digest>:<dispatch_exposure_id>:<canonical_delivery_identity>` replays one immutable non-authorizable identity; collision writes zero; shared SQL gateway exposes disjoint monotonic cost/retention CAS entrypoints |
| brownfield | sentinel active rows are never adopted; compatibility counters are exact; Migration C waits for fleet/scope cutover and Migration D validates later |
| obligations | the no-OB-ID action-root durable-scope gate and OB-10.1/10.2/10.3/10.4 remain open in this doc and require applicable implementation evidence before D3 dispatch activation |
| deletion | all twelve section-13 gates pass for the two fenced manifests; complete 29-wrapper inventory remains the observation/zero-unauthorized-increase denominator, while the generic bridge remains until every `legacy_unfenced` entry/caller has its own policy and cutover |
| residual | R-019 remains open; 26-call ratchet unchanged; no global atomicity claim |

This matrix is the acceptance contract for the later implementation batch. Passing unit tests without native PG races,
public-projection secrecy, brownfield evidence, exact cross-document outcome-table parity, and exact zero-write snapshots
is insufficient.

## 17. Mechanism × 10-invariant self-audit

This is the required `DESIGN_INVARIANT_CHECKLIST` author self-audit, not independent review evidence. `I1..I10` mean:
single writer/aggregate owner; tenant key; generation/physical fence; lifecycle; late/partial result; cost honesty;
physical identity; provenance/trust boundary; self-contained cross-document consistency; runtime/mode isolation.
Every newly introduced D3b mechanism appears as a row and every invariant appears as a column.

| Mechanism | I1 | I2 | I3 | I4 | I5 | I6 | I7 | I8 | I9 | I10 |
|---|---|---|---|---|---|---|---|---|---|---|
| Canonical registry + sealed pre-claim authority | one `CommandTypeSpec` registry owns `legacy_unfenced`, `scoped_session_bootstrap_v1`, and `d3_v1`; normal/bootstrap factories and Stage A are private (§5.1-5.2) | authority and Stage-A receipt bind authenticated workspace | selection generation advances before consumed slot clears; bootstrap authority is consumed once and receipt binds current claim generation/epoch/attempt/lease + plan pins | drift/reselection requires new authority; receipt cannot be refreshed or converted into create result | stale/foreign/duplicate/wrong-policy authority or non-current receipt rejected | bootstrap has no transport; normal transport manifest bounded | sealed authority + exact selection and private raw-token receipt identity | constructors private; caller text forbidden; normal/bootstrap authority/receipt/result types not interchangeable | existing registry gains stage/provenance manifest; no second owner registry | both bind namespace/mode/scope; bootstrap exact-binds authenticated workspace |
| Scoped review session -> OperationRun -> command -> ActivityRun -> Attempt scope | scoped repository issues root only from `ScopedSessionCreateContext(authority, receipt)`; specialized Stage B creates ActivityRun/Attempt with the session; OperationRun/descendants exact-copy committed result (§5.2) | authenticated workspace exact-equals authority, receipt, source OperationRun/command; committed replay exact-checks durable aggregate | dedicated scope+idempotency lock; receipt current claim generation/epoch/post-claim command attempt/lease; no preexisting ActivityAttempt; stale is zero-write | Stage-A receipt exists before create; create result alone gains review/activity/event/terminal ids; credential-free committed replay returns same result | cross-scope/review result cannot match; bootstrap cannot dispatch/domain-write; replay cannot create | receipt has no activity/ledger/result ids; later ledger inherits committed scope/review | private receipt claim identity + create-result ActivityRun/Attempt/event/terminal aggregate + type-compatible FKs | closed union exact-checks create credentials or read-only committed identity; normal terminal/business predicate forbidden pre-session | one propagation schema; explicit create/replay/receipt/result separation removes review-id and crash-recovery cycles | lock has no review/gate; namespace/mode at every row; **action-root durable-scope gate open** |
| Canonical D3 business fence | strict command-creation repository writes typed predecessor columns plus one immutable digest after the session exists (§4/§8.1) | digest binds workspace/scope/positive review | five closed claimed contexts + one aggregate-control context fence all six normal phases; bootstrap uses its separate context | command identity locks before intent; Stage B/terminal/record/dispatch/resume/control manifests are closed | control-first returns only `not_applied`; either authorized receipt may persist, response alone may quarantine | grant/exposure required only by their normal phase; bootstrap has no spend | `d3_business_fence_v1` + command/authority/receipt equality | typed owner rows only; bootstrap/JSON/ambient/model digest forbidden | one normal API after session; wrong phase/context fails closed | namespace/mode/scope/review are canonical digest fields |
| Claim generation/token/epoch + heartbeat occurrence | workflow selection/claim/control repositories (§4) | command scope in every CAS | stored selection/consumption + generation/token/epoch + occurrence (§5/§8) | input/control/requeue advances epoch; result terminal retains; heartbeat/reclaim table complete (§7) | stale writes zero; authorized failure is audit-only and valid late response alone quarantines | dispatch is separate ledger UoW | one-use authority + sealed receipt + heartbeat UUID/sequence | raw token private; mint/lease expiry repository-owned | terminology and transition source are this doc | scope equality precedes claim/effect |
| Stage B successor convergence + source/record split | verification owner; reducer plans record only from `final_adjudication` (§6.2-6.4) | all intent/attempt identities carry workspace | four physical predecessor pins; initial all-null proof or complete tuple + independent source/record claims | requeue successor atomic; record+terminal is one PG transaction | stale record/current-source/half-sentinel mismatch returns one closed `not_applied` shape | Stage B/record perform no network/cost | predecessor event + full source terminal receipt, historical policy/spec/reason provenance + record attempt | exact five-outcome table; one physical recorded event; control timeout distinct | one final-adjudication planner resolves prior routing conflict | **OB-10.1 open** until intent columns/CAS land |
| Awaiting-budget resume convergence | gate-event owner calls typed workflow-command repository entrypoint (§6.2.3) | resume-v2 key binds scope/positive review/intent phase/recorded event | awaiting intent + recorded event + gate watermark + active grant converge durably | gate apply creates post-apply queued/retry-wait row; grant only reawakens existing row | missing row/drift returns zero-write typed conflict | record/resume never consume grant; dispatch alone decrements | exact resume-v2 idempotency tuple and post-apply digest | no transient delivery/grant id, caller key, or pre-gate payload inference | closes lost-wakeup and command-lock inversion without a second SQL owner | mode/scope/review included in key and predicate |
| Cross-owner dispatch + grant/cost exposure | each aggregate retains owner; common v2 positive-review coordination includes full OperationRun inventory (§6.5) | exact workspace across every locked row | claim/attempt + plan/review/gate/grant revisions/epochs | all participating command identities lock before intent; bounded retry; no third normal exemption | response/failure/retry share committed exposure lock; no-exposure absence is fenced by coordination+command prefix; only valid late response quarantines | reserve + grant decrement + `dispatching` in one UoW | exposure/call/pins/authority exact identity; missing exposure is never a locked identity | typed server columns; JSON/model assertions forbidden | one global positive-review root-to-receipt order; heartbeat/exact replay only normal exemptions | **OB-10.2/10.3/10.4 open**; all block dispatch |
| Terminal provenance registry + atomic command/attempt/event/source binding | `TERMINAL_PROVENANCE_SPECS` owns response/failure/no-exposure specs and retry semantics; command UoW and receipt writer retain distinct ownership (§6.6) | terminal event/receipt/command scope and positive review equal | current claim + business fence + historical policy/spec digest + stable occurrence | response/failure/retry exposure lock; no-exposure coordination+command absence proof; terminal failure only; retryable -> `retry_wait`; source fields commit once | response-first rejects failure/retry; retry-first or failure-terminal-first permits only later valid-response quarantine | already-authorized exposure reconciles; failure receipt is cost/audit-only | exact response-spec/envelope/reason/result or envelope-free failure/spec/retry pins + variant FKs | registry/server compute digests; caller retry/outcome forbidden; exactly-one preflight | real-PG three-variant + response/failure/retry order + missing-row-free no-exposure acceptance | event/receipt scope/mode/review columns required by §5.2/§11 |
| Durable late-result quarantine | shared repository consumes a D0-valid response receipt, including valid `length`/`content_filter` that never auto-confirms, never attempt-failure evidence (§9.3) | scope/workspace/positive review in immutable identity | stable delivery/occurrence + exposure + old claim/business identity | exact `late-response-v1` insert plus independent monotonic cost/retention axes | explicit sink only for late valid responses; current branch is fail-closed and incomplete/truncated/protocol-parse failure excluded | reconciliation axis never authorizes a send | response spec/envelope/reason/call/result/artifact digests retained through tombstone | `authorizable=false`; no reducer/public/model path | one PG mutation gateway; redelivery exact-replays; no mixed disposition | namespace/mode prevent non-live/live reuse |
| Public allowlist + brownfield bridge | projection and migration owners remain separate | no default/foreign workspace adoption | sentinel never authorizes | rollout and twelve scope-local deletion gates (§12-13) | quarantine excluded from products | bridge cannot dispatch or hide cost | two registry-derived fenced manifests plus exact observation counters over all 29 wrappers | capability/token/heartbeat/quarantine secrecy | served=0; generic bridge and R-019 remain open while `legacy_unfenced` exists | legacy scope cannot be promoted; action gate remains explicit |

The open cells are the existing stable obligations printed above—OB-10.1, OB-10.2, OB-10.3, and OB-10.4—plus the
separately named Plan section 6 item 6 action-root durable-scope gate for action-backed roots. They are carried into
implementation and activation gates rather than replaced with new D3b OB-IDs. All other cells are
locked decisions whose implementation evidence is still absent; “decision satisfied” here never means runtime complete
or formal `GO`.
