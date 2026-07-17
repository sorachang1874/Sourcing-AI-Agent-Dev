# Track D D1n Remaining Action and Agent Tool Serving Plan

> Status: Ultra-review fixed-forward implementation plan. It resolves the 17 findings in
> `runtime/reviews/20260717T021541Z_Track_D_D1n_top_down_remaining_actions_and_local_agent_vertical_slice.md`.
> It is not implementation evidence, an independent-review verdict, a served-tool activation, or live-provider
> authorization. Every version/digest marked `implementation-pinned` must become a concrete checked-in value before
> that node can activate.

Date: 2026-07-17

## 1. Outcome, layers, and 24-hour critical path

D1n closes the remaining schema-less action population from the top down and builds one tool contract that covers
both action-backed tools and read-only query tools. Scripted and product E2E confirm an already-specified contract;
they are not the mechanism for discovering request ownership, result ownership, or rollout behavior.

The work is deliberately split into three gates so unrelated migrations do not block a bounded local Agent path:

| Layer | Required outcome | Explicit non-claim |
| --- | --- | --- |
| **D1n implementation** | all 15 API-submittable actions have current request pins, retained historical lookup, owner binders, registered adapters, result contracts, and exact rollout state; R-029 can begin its final zero-hit observation epoch | does not make all 15 actions model-served and does not authorize any provider |
| **local fake Agent slice** | isolated local PG executes `plan_acquisition -> exact action approval -> start_acquisition_run -> inspect_operation -> filter_projection`; provider transport and model turn are scripted/simulated, while AgentAction/Operation/Command/result persistence is production-shaped | `local_canary` is not the global `served=true` state; public/hosted served population remains zero |
| **paid local live gate** | the same reviewed slice uses the real Agent model route and canonical Cohort Harvest boundary only after durable live lane checkpoints, cost reservation, manifest identity, result-slot identity, and bounded canary approval are present | does not activate hosted serving, widen company/provider scope, or close R-019/R-028/R-029 |

The intended 24-hour critical path is:

```text
P0 D1m settle (async for denominator/hosted)        L1 Cohort live checkpoint (parallel)
                 \                                /
F1 result core -> V1 plan preview -> V2 start-v2 -> V3 AgentToolSpec/query/filter-v2
                       \             /                  |
                        F4 touched-path UoWs ------------+
                                                         v
                                      PG terminal-success simulate
                                                         v
                                      TML + second-lab scripted proof
                                                         v
                                      local_canary scripted Agent
                                                         v
                            paid local TML canary only if every paid gate is green
```

The other four remaining schema-less actions (`fetch_profile_sample`, `continue_acquisition_run`,
`promote_person_assertion`, and `external_intake`) continue in parallel after their shared foundations are pinned.
They are required for D1n completion and hosted serving, but they are not smuggled into the TML canary dependency set
when the canary does not call them. Review requests launch as soon as each batch is committed; review latency blocks
only that batch's activation/live gate.

No full `tests/test_pipeline.py`, paid provider call, sourcing live environment, or hosted deployment is authorized by
this plan.

## 2. Predecessor and normative 15-action roster

### 2.1 D1m is an explicit reviewed predecessor

`refresh_company_public_web_assets` is not a D1n migration. D1n may develop in parallel, but it may claim a normative
`10 defined + 5 remaining`, claim `15/15`, start the final R-029 observation epoch, or enter hosted serving only after
all of the following are recorded and mechanically verified:

1. one exact D1m implementation commit containing `company_public_web_refresh_request_v1` and its binder/adapter;
2. a scope-matched, valid pinned non-author `GO` artifact for that exact commit and D1m files;
3. the generated 15-row roster from that commit reporting exactly 10 nonempty request pins before D1n activation;
4. no scoped dirty/staged/untracked replacement invalidating that artifact at consumption.

At this plan revision D1m is a stable author candidate with review pending. Therefore the values below describe the
current candidate registry, not reviewed predecessor evidence. `P0` remains red until the exact commit, review path,
scope digest, and verifier result are written to the D1n gate report. A D1m `NO-GO` does not stop leaf D1n work; it
does stop `15/15`, R-029 epoch start, hosted activation, and any claim that the predecessor is reviewed.

### 2.2 Normative roster

The source of truth is `operation_runtime.DEFAULT_ACTION_REGISTRY`; the checked-in generated roster and its digest are
the activation artifact. Handwritten counts are never authority. Current concrete digests below were generated from
the 2026-07-17 candidate tree. Target digests for new versions are
`sha256(canonical ActionRequestSpec.request_schema)` and must be concrete 64-hex values in the generated report before
activation; `implementation-pinned` is a fail-closed placeholder, not an accepted runtime value.

| # | Action | Current request version / digest | Target request state | Canonical target binder | Dispatch adapter | Migration/review state |
| ---: | --- | --- | --- | --- | --- | --- |
| 1 | `plan_acquisition` | empty / empty | `acquisition_plan_preview_request_v1` / implementation-pinned | `AcquisitionPlanPreviewTargetBinder` mints workspace/requester/company target | new `commandless_preview` | D1n V1; no activation before pinned review |
| 2 | `start_acquisition_run` | `acquisition_root_request_v1` / `3409f3c878b2ca8e67bb4d4511c9e188c94ead14797abd2e27ebdc7b138bfd70` | retain v1 historically; new submissions use `acquisition_root_request_v2` / implementation-pinned | `AcquisitionRootTargetBinder` plus exact approved preview/action receipt | `agent_callable_workflow_command` | D1i v1 remains historical; D1n V2 review required |
| 3 | `fetch_profile_sample` | empty / empty | `profile_sample_request_v1` / implementation-pinned | new `ProfileSampleTargetBinder`; no Activity/Delta mint at ingress | `agent_callable_workflow_command` | D1n M1 |
| 4 | `continue_acquisition_run` | empty / empty | `acquisition_continue_request_v1` / implementation-pinned | new discriminated `AcquisitionContinuationTargetBinder` | `agent_callable_workflow_command` | D1n M2 |
| 5 | `search_projection` | `projection_search_request_v1` / `40ffcb4b5632cfe75bfa9667f726f73e8fdaffc4c8f9f92f89bbda863b6cb1fa` | retain v1 text-search contract | `projection_search_service._bind_operation_projection_membership` | `projection_read` | schema exists; D1l `NO-GO 0/4/7/0` and Agent result/tool review still block activation |
| 6 | `filter_projection` | `projection_filter_request_v1` / `f43d31eb284bfecc6a2b3ee84e69358899eb6827ffb18e0c050b8f325db7f5c8` | retain v1 historically; add `projection_filter_request_v2` / implementation-pinned for canonical Cohort mapping | same projection membership owner | `projection_read` | D1n V3 fixes D1l cohort/result gaps; fresh review required |
| 7 | `add_to_crm` | `crm_projection_selection_request_v1` / `de58847cfc064b1fe156ed8f73a12bbaea580aeaa57ca61f50767e3a6b091113` | unchanged current pin | `CRMProjectionSelectionTargetBinder` | `crm_writer` | request active; Agent result/tool review pending |
| 8 | `set_crm_stage` | `crm_set_stage_request_v1` / `a6359c447a44fa597d242cf97d00ed5946bc97bc1aa1ad91670a5d372c00a1cb` | unchanged current pin | `CRMRecordTargetBinder` | `crm_writer` | request active; Agent result/tool review pending |
| 9 | `add_crm_note` | `crm_add_note_request_v1` / `2e5dcb023b15da5090d179f0ba7d73e2f0c90ab0f43a57eef05833d94bb36637` | unchanged current pin | `CRMRecordTargetBinder` | `crm_writer` | request active; Agent result/tool review pending |
| 10 | `create_crm_task` | `crm_create_task_request_v1` / `8bf7d372ae725b7a9c6e753023a9472c9d586d604db5a57d1eb3d466a1e3ab7e` | unchanged current pin | `CRMRecordTargetBinder` | `crm_writer` | request active; Agent result/tool review pending |
| 11 | `enrich_person_public_web` | `crm_public_web_enrichment_request_v1` / `57029688ddc085b75fe62add97577f7d514a830664be8786edca9058d9ed1666` | unchanged current pin | `CRMRecordBatchTargetBinder` | `person_public_web` | request active; Agent result/tool review pending |
| 12 | `refresh_company_public_web_assets` | `company_public_web_refresh_request_v1` / `505a71c45001e97844d1f68eb9b2a776845017f48bc9cab1cd9583ad3c005dd6` | unchanged only after P0 | `CompanyPublicWebTargetBinder` | `agent_callable_workflow_command` | reviewed D1m predecessor required; not a D1n leaf |
| 13 | `promote_person_assertion` | empty / empty | `person_assertion_promotion_request_v1` / implementation-pinned | new `PersonEvidenceAssertionTargetBinder` | new `person_assertion_writer` | D1n M4 |
| 14 | `export_candidates` | `projection_export_request_v1` / `7f3406b04a6c05793d9a60bd17822a634a79a474c5a028e2fcc5f91b66bb4c13` | unchanged current pin | projection membership owner | `export` | request active; Agent result/tool review pending |
| 15 | `external_intake` | empty / empty | `external_intake_excel_request_v1` / implementation-pinned | new `StagedIntakeArtifactTargetBinder` | new `excel_intake` | D1n M5 |

The generated roster record must include, for every row: action type, owner module, request version/digest, binder id
and revision, adapter id and revision, allowed command manifest digest, request activation epoch, result
version/digest/serializer owner, tool release state, migration status, and exact review artifact/scope digest or
`unreviewed`. Any duplicate, missing row, non-64-hex active digest, or mismatch with `ActionRegistry` fails activation.

## 3. Canonical Cohort selection and durable confirmation

### 3.1 One population selector

D1n reuses the exact `cohort_selection.v1` object; it does not introduce `role_buckets`, another enum, or another
digest:

```json
{
  "schema_version": "cohort_selection.v1",
  "role_bucket_ids": ["research", "engineering"],
  "employment_statuses": ["current", "former"],
  "role_match": "any",
  "source": "user_explicit"
}
```

- `role_bucket_ids` is ordered and duplicate-free from `ROLE_BUCKET_KNOWLEDGE`; **empty means all roles**.
- `employment_statuses` is a nonempty ordered subset of `current|former`.
- `role_match` is `any|all`; `source=user_explicit` is the only external value.
- callers never author `registry_version`, `registry_digest`, or `cohort_selection_digest`; the server derives them
  through `cohort_selection_registry_digest()` and `cohort_selection_digest()`.
- the exact canonical object plus registry version/digest and selection digest is copied without re-derivation through
  preview, persisted start action, approval receipt, OperationRun, provider planning manifest, command, result,
  projection query, and Agent journal.

The canary may require a nonempty role selection as a **release policy** for its first run, but that policy cannot
alter the schema or change the canonical empty-list meaning. Fake/scripted tests must include empty-role/all-roles,
one role, multiple roles, `any`, `all`, current-only, former-only, and both statuses.

### 3.2 Preview and approval are the durable user confirmation

`plan_acquisition` writes one immutable `acquisition_plan_preview.v1` in its commandless PG UoW. The preview contains:

- workspace/requester, canonical company id, company-registry revision/digest;
- exact canonical effective request and digest;
- exact Cohort object, registry pins, and selection digest;
- source preferences, coverage intent, thematic constraints, and bounded budget proposal;
- capability-free provider planning manifest id/version/digest;
- plan request/result schema pins and the intended start-v2 request digest;
- immutable `preview_id`, monotonically increasing `preview_revision`, content digest, creation time, and expiry.

`start_acquisition_run` v2 requires exactly `preview_id + preview_revision + preview_digest`. It accepts no inline
company, cohort, source, coverage, or budget override. The binder reloads the preview under exact workspace/requester
ownership and mints the complete canonical start request/target before the first action write. Missing, stale,
expired, foreign, or conflicting preview identity is one indistinguishable not-found/conflict class with zero action,
run, command, budget, or provider writes.

The sole confirmation is explicit approval of that exact persisted start AgentAction. The `ActionApproved` event is
the durable `acquisition_confirmation_receipt.v1`; it binds approval actor, workspace/requester, action id, preview
id/revision/digest, full effective-request digest, company and Cohort identities, budget, request/result/tool schema
pins, approval policy revision, and timestamp. Approval reloads and compares the immutable preview inside the same
UoW before creating the OperationRun. The root command exact-copies the receipt id/digest. A model may propose or
submit the action, but it cannot mint this receipt or approve itself.

## 4. Shared result, invocation, and tool contracts

### 4.1 F1 result registry

`src/sourcing_agent/action_result_schema.py` and `tests/test_d1n_action_result_schema.py` are an in-progress F1 leaf
at the time of this plan. Their presence is not a commit, test result, or activation claim. The accepted F1 contract
must provide one immutable `ActionResultSpec` per eligible tool with:

- result schema version and canonical digest;
- exact success, deferred, and terminal-error closed variants;
- serializer owner and canonical validator owner;
- deterministic byte/item/depth limits and opaque artifact-reference policy;
- a parallel, digest-bound per-field provenance map: `server_derived`, `owner_state`, `user_supplied`,
  `provider_observed`, or `model_inferred`.

Externally controlled text remains display/evidence data and cannot drive control, identity, budget, approval, or
promotion. Unknown/private fields, raw paths, non-JSON values, unregistered artifact schemes, and oversize payloads
fail before `ToolResultMessage` creation. A serializer never copies arbitrary `workflow_commands.result`.

### 4.2 Result occurrence and terminal-winner pins

The authoritative serialization source is one exact result occurrence, not an action type plus “latest output”. A
PG `agent_tool_invocations`/journal record (or the already-designed durable result-slot owner if implemented under a
different table name) persists this immutable identity:

```text
workspace_id, actor_id, runtime_namespace, provider_mode
turn_id, step_id, result_slot_id, slot_generation
tool_name, tool_spec_version, tool_spec_digest
canonical_args_digest, occurrence_ordinal
request_schema_version/digest, result_schema_version/digest, serializer_owner/revision
action_id, operation_run_id, workflow_command_id (nullable for query tools)
activity_run_id, activity_attempt_id, command_attempt, command_generation, control_epoch
owner_target_revision/generation, terminal_winner_id, owner_result_ref/digest
```

The same tool/result pins are exact-copied into AgentAction/OperationRun when present, the accepted terminal result,
`ToolResultMessage`, and the accepted-action journal. Serialization reloads the exact workspace/action/run,
command/attempt, physical target revision, and terminal CAS winner. A changed registry cannot reinterpret an old
result; historical result specs remain lookup-capable. A late/old attempt is retained only as quarantined attempt
evidence and never wins or serializes. Repeated identical tool calls in one turn use the D0 stable
`occurrence_ordinal`; provider call ids are evidence, not idempotency authority.

### 4.3 One `AgentToolSpec` for actions and queries

Introduce one immutable registry projection rather than a second query-tool path:

```text
AgentToolSpec(
  tool_name,
  tool_kind = action | query,
  request_schema_version/digest,
  request_validator_owner,
  workspace_actor_binder_id/revision,
  adapter_id/revision,
  action_type = <one of 15> | null,
  query_owner_id = <registered read owner> | null,
  result_schema_version/digest,
  serializer_owner/revision,
  byte/item/depth limits,
  simulate_fixture_id/revision,
  release_state,
)
```

Exactly one of `action_type` and `query_owner_id` is present. Action tools are derived from the 15-row
`ActionRegistry`; query tools do not change that denominator. `inspect_operation` is the first query tool. It performs
the same exact workspace/run/action owner preflight as the Operation APIs and returns only canonical `control_state`,
`control_policy`, `display_contract`, progress, result readiness, and bounded provenance. It cannot invent “next
controls”, repair state, execute a command, or reinterpret terminal statuses.

### 4.4 Release state machine

`AgentToolReleaseRegistry` owns routing/visibility; absence, stale evidence, unknown state, or expired activation is
`disabled`:

```text
disabled -> shadow -> local_canary -> hosted
                 \-> disabled      \-> disabled
```

Each transition binds tool/version/digest, exact code commit and scope digest, valid review artifact, runtime
namespace and provider-mode allowlist, optional workspace/requester allowlist digest, activation/expiry times, and
transition actor/reason. Meanings are exact:

- `shadow`: registry-visible to preflight only; never sent to a model.
- `local_canary`: available only in an isolated named local runtime and explicitly allowed workspace. This is reported
  as `available_in_local_canary=true`, **not** global `served=true`.
- `hosted`: the only state deriving `served=true`; requires R-029 closed for the complete 15-action denominator,
  scope-matched hosted reviews, non-stale activation, and hosted owner/runtime gates.

A paid local TML canary is an additional expiring authorization on `local_canary`; it does not transition the tool to
`hosted`. Public production served population remains zero until at least one tool reaches `hosted`.

## 5. Action and query contract locks

### M1. `fetch_profile_sample`

The binder targets an exact existing sample scope:

```text
workspace_id + acquisition_run_id + lane_id + lane_revision
+ canonical_profile_url_set_digest + sample_scope_revision
```

It verifies that tuple and mints only the owner-bound target. It does **not** create or resolve an unversioned “current”
Activity/EntityDelta. `linkedin_profile_activity_owner` creates the ActivityRun, ActivityAttempt, and EntityDelta only
after `linkedin.profile_fetch.activity.run` is planned from the pinned action. Submit, approval, replay, retry, and
claim exact-compare the target tuple. The result binds the winning ActivityAttempt and reports bounded sample counts,
profile summaries, cache/fetch disposition, lane identity, and opaque artifacts.

### M2. `continue_acquisition_run`

Use one closed discriminated schema with exactly seven variants:

1. `linkedin.discovery_query.run`;
2. `linkedin.profile_fetch.activity.run`;
3. `linkedin.profile_fetch.provider.fetch`;
4. `linkedin.profile_terminal.admit`;
5. `projection.profile_admission.apply`;
6. `projection.person_search_index.build`;
7. `collection.authoritative.merge`.

Every variant declares required target revision/generation, canonical fields, binder, payload builder, command owner,
and result serializer. There is no opaque `command_payload`; `linkedin.profile_refill.submit_batch` remains rejected.
The normalized result binds the selected variant, command/activity/attempt winner, bounded effect summary, canonical
control state, and next canonical control refs.

### M3. `plan_acquisition`

This is a commandless pure preview, not an alias for side-effecting `plan_workflow`. Its owner validates company
identity and `cohort_selection.v1`, compiles the capability-free canonical provider manifest, and persists
AgentAction/OperationRun/event plus `acquisition_plan_preview.v1` in one PG UoW. It creates no provider call, plan
review, acquisition run, or workflow command. Its success result returns the immutable preview and exact confirmation
instructions; deferred is not accepted as simulate-success evidence.

### M4. `promote_person_assertion`

The request binds exact person, evidence id/revision/digest, proposed assertion type/value, suggestion id/revision when
present, and an approval-required AgentAction. Explicit user approval of that exact action is the durable
`person_assertion_promotion_receipt.v1`; an Agent cannot author it.

Inside one scoped UoW the writer reloads workspace/person/evidence ownership, current evidence revision, suppression,
promotion eligibility, approval receipt, and current assertion/supersession state. The state chain remains distinct:

```text
raw evidence -> model-reviewed signal -> agent_suggested -> human-approved promotion
-> operator_confirmed PersonAssertion -> export eligibility
```

Without the human receipt, an Agent suggestion remains `agent_suggested` and non-selected/non-exportable. With a valid
receipt, the resulting assertion is `operator_confirmed`; export eligibility is still derived by the existing owner,
not asserted by this action. Promotion, assertion, audit event, and index plan commit together. The result reports
those typed states without evidence bodies or unrestricted CRM notes.

### M5. `external_intake`

V1 is Excel-only. Authenticated upload is a separate transport that creates a PG-owned,
content-addressed `staged_intake_artifact.v1`:

```text
artifact_id/revision, workspace_id, requester_id, content_sha256, byte_size, media_type
created_at, expires_at, state=pending_validation|ready|rejected|expired
storage_object_ref, validation_revision, retention/cleanup state
```

The Agent request contains only the server-minted artifact id/revision plus bounded intake options. Raw path, URL,
base64, bytes, or caller digest is forbidden. Approval, dispatch, claim, retry, and result exact-copy the artifact
identity; claim reloads workspace/requester/content digest/type/size/state/expiry. Changed bytes always mint a new
artifact revision. Expiry/cleanup tombstones the row while retaining digest/audit identity and cannot silently retarget
an existing action.

Model-assisted normalization runs only as a typed, idempotent ActivityAttempt bound to artifact digest, action,
route snapshot, and invocation occurrence. The checked-in product route is `external_intake.normalize_excel.v1`; it
pins requested/effective provider/model, route revision, token/cost/deadline ceilings, retry limit, circuit identity,
and `fallback_policy=fail_closed`. No silent model/provider reroute is allowed. Model output is a non-authoritative
normalization proposal until deterministic intake validation accepts/rejects rows. Retry reuses the exact invocation
identity and never pays again after an exact terminal result. The result reports counts, bounded diagnostics, model
identity/usage disposition, and opaque produced-asset refs.

### V2. `start_acquisition_run` v2

V2 accepts only the immutable preview reference and confirmation flow in §3.2. It copies the canonical company,
Cohort, effective request, provider planning manifest, budget, and all schema pins into the action/run/root command.
No free-text re-inference occurs after preview. V1 remains historical-only under §8; it is never reinterpreted as v2
or exposed as a new Agent tool.

### V3. Projection reads

`search_projection` v1 remains a text search (`search_keyword + offset/limit`) and is useful as an early serializer
pilot; it does not claim to carry Cohort roles or employment status.

Structured canary results use `filter_projection` v2 through the canonical `projection_search_service` owner. V2
binds the exact canonical Cohort object and server-derived digests plus projection id/membership revision. Mapping is:

| Cohort field | Projection owner behavior |
| --- | --- |
| empty `role_bucket_ids` | omit a function restriction and accept all roles; never infer defaults |
| nonempty roles + `role_match=any` | require at least one matching server-owned `cohort_lane_membership.role_bucket_id` |
| nonempty roles + `role_match=all` | require every requested role across the same candidate's verified lane membership for a qualifying status; do not use a primary-function OR shortcut |
| `employment_statuses` | match server-owned `cohort_lane_membership.employment_status` |
| registry/selection digest | exact-match the projection publication's Cohort identity; mismatch is stale/reselection, not empty success |

The owner, not the Agent/UI, compiles these predicates and validates that role ids remain in the canonical registry.
The result includes projection/membership revision, selection/planning/execution/result/publication digests,
freshness/readiness, provider mode/runtime namespace/cache provenance, requested-lane coverage, lane summaries,
`total_count`, `returned_count`, and explicit truncation. Raw cache/artifact paths never enter the model result.

## 6. Canonical provider manifest and paid-live predecessor

### 6.1 `provider_execution_manifest` v2

The sole provider planning authority remains the capability-free `provider_execution_manifest` emitted by
`CohortProviderCompiler` and exact-recompiled by the acquisition adapter. D1n extends it as
`cohort_provider_manifest.v2`; it does not create a TML manifest type. V2 retains all v1 fields and adds a digest-bound
server-owned company target:

```text
company_target = {
  canonical_company_id,
  company_registry_revision,
  company_registry_digest,
  provider_company_labels,       # derived only from that registry revision
  company_target_digest
}
planning_manifest_digest         # capability-free persisted plan identity
```

User-authored aliases never enter this object. The planner resolves company identity first; unknown or ambiguous
identity fails before action, manifest, or provider work. Runtime exact-recompiles from the same company target,
canonical Cohort, base thematic filters, schema pins, and limits plus a separately owner-issued execution capability.
It records a distinct `execution_manifest_digest`. Both digests and the exact manifest version travel through
preview/action/run/commands/lane checkpoints/result/projection/journal. Existing v1 manifests remain historical and
non-live; they are not silently upgraded.

Manifest-only scaling is proven with parameterized fake/scripted compilation for Thinking Machines Lab and at least
one materially different second lab (Anthropic for the first matrix). The two cases must have the same schema version,
compiler/adapter ids, command trace, lane construction rules, result/tool pins, and branch-coverage id; only registry
company data, cohort choices, thematic data, and budgets may differ. A source guard forbids production branches or
action/provider/tool names containing either lab literal. Unknown/ambiguous company and unexpected provider-label
override tests must fail before connector invocation.

### 6.2 `CohortExecutionCapability` live checkpoint predecessor (CS6/L1)

Paid `live` remains unavailable until a separately reviewed predecessor adds durable per-lane execution ownership.
The minimum PG row is keyed by stable
`workspace + operation_run + execution_manifest_digest + lane_id + lane_digest + work_item_id` and records:

- immutable provider request digest, provider mode, runtime namespace, provider/connector revision;
- `planned -> budget_reserved -> dispatching -> submitted|submission_ambiguous -> terminal` state;
- cost reservation/exposure id, physical call occurrence, provider run/dataset id, submission timestamps;
- poll/resume cursor, retry/circuit decision, terminal result digest, and late-result accounting;
- command generation/control epoch/ActivityAttempt owner and exact terminal winner.

Before the first network byte, one transaction reserves the manifest-wide budget and moves the exact lane to
`dispatching`. A successful submit persists the returned run id; an ambiguous submit cannot issue another call and
must reconcile or conservatively terminalize from the same work item. Resume/poll reuses the provider run id. Exact
terminal replay reuses the result and makes no provider call. Late results attach to the original exposure and cannot
win against a newer generation. Circuit-open and budget-exhausted outcomes are explicit and zero-call.

Only `cohort_execution_capability_for_runtime()` (extended by this reviewed live owner) can issue a live capability,
and only after exact manifest/lane checkpoint rows, budget reservation, runtime identity, and proof verifier are
validated. A manifest boolean, caller flag, or local file cannot issue it.

### 6.3 Paid local TML canary prerequisites

The paid gate requires all of the following; none may be waived implicitly:

1. V1/V2/V3/F1/F3/F4 and the result-slot occurrence path committed with valid scope-matched `GO` reviews;
2. local PG terminal-success simulate plus TML/Anthropic scripted proof green and contamination clean;
3. L1/CS6 live checkpoint committed, reviewed, and exercised with fake ambiguous-submit/resume/terminal reuse;
4. the `agent.planner.loop` model route in `local_canary` with full `ModelTurnExecutionContext`, exact requested/effective
   model identity, cost reservation/exposure, circuit, no silent fallback, and durable accepted-result slot;
5. canonical TML company identity and `cohort_provider_manifest.v2`, with user-confirmed Cohort and budget;
6. an expiring canary receipt binding workspace/requester, exact commits/review scope digests, model/provider routes,
   manifest/tool/request/result digests, maximum calls/items/candidates/cost/time, and `provider_mode=live`;
7. provider health/preflight and operator-owned credentials supplied only at execution time; no secrets in artifacts.

The first run is one company, one confirmed Cohort, one manifest, a bounded result limit, and no automatic retry after
ambiguous exposure. Promotion, external intake, outreach, export, and company Public Web are disabled for this canary.
Failure leaves inspectable checkpoint/result evidence and does not widen the manifest.

## 7. Scoped UoWs and concurrency proof

F4 closes only the newly touched paths; R-019/R-028 remain open globally. Every D1n write UoW uses one PG transaction,
one overall lock deadline, and this global order (skipping absent groups, never reversing):

```text
workflow/operation event-stream advisory lock
-> OperationRun -> AgentAction
-> tool invocation/result slot
-> preview/approval/staged-artifact/evidence target rows
-> WorkflowCommand rows in deterministic command-id order
-> ActivityRun -> ActivityAttempt
-> domain current/effect rows in deterministic owner key order
-> append-only events -> runtime_outbox/wakeup -> linked state projections
```

Required invariants for each touched UoW:

- an idempotency identity includes workspace, action/run, request/tool/result pins, physical target revision, command
  generation/control epoch, and occurrence where applicable;
- planning/claim/terminal compares current generation, attempt, lease, control epoch, and nonterminal owner before any
  domain effect;
- terminal state is monotonic and one CAS winner owns result/event/outbox; stale owner and loser write nothing;
- cancel versus plan/effect, retry versus terminal, two attempt winners, and preview/evidence/artifact revision drift
  are serialized under the same ordered locks;
- event and required outbox/wakeup commit with the state/effect they announce;
- a post-commit lost acknowledgement exact-reloads the committed identity and returns the same result/event/outbox;
  it does not repeat an effect, model call, provider call, or child creation;
- no transaction remains open across provider/model/network I/O.

The command creation UoW covers command + action/run/event/outbox. The terminal UoW covers exact winning
command/attempt result + action/run/event/outbox. `plan_acquisition`, projection reads, and query-result persistence use
their commandless specialization. Promotion additionally includes promotion/assertion/index-plan rows. Intake
includes staged-artifact claim plus typed model-attempt/result before accepted intake effects. Fault injection at each
write is necessary but insufficient; the PG concurrency matrix must exercise the races above.

## 8. Rolling, brownfield, activation, and backout

### 8.1 Historical version lookup

Persisted request/result/tool pins are immutable. The runtime registry retains all historical specs referenced by any
nonterminal action/run/command, retry child, accepted result slot, terminal result, or journal. Dispatch, approval,
retry, replay, and serialization resolve by persisted `(version,digest)`, never by “current”. Unsupported historical
identity fails visibly; it is not revalidated under a newer schema.

### 8.2 Mixed-replica rollout

A small PG activation owner records, per action/tool, `disabled|shadow|current`, the current submission pin, retained
pin-set digest, activation epoch, minimum code release, and update revision. Rollout order is:

1. deploy historical lookup, result pins, readers, and migrations everywhere with all new entries `disabled`;
2. prove every replica reports the same local registry/retained-set digest;
3. inventory blank-pin and v1 pending rows; drain normally or explicitly cancel/reissue them—never mutate their pins;
4. activate one action atomically by CAS on its PG activation row; a replica lacking the exact local pin fails closed;
5. activate start/filter v2 only after all replicas read both v1 and v2, then stop new v1 submission while retaining
   v1 execution/replay;
6. begin the R-029 zero-hit observation epoch only after P0 plus all five D1n action rows are current and the complete
   generated roster is 15/15.

Migration checks install `NOT VALID` first, are validated separately after data inventory, and become required only
after every eligible row is pinned. If a bridge is needed, it is report-visible, never served, and deleted only after
one full release window with zero complete-population hits and no durable references.

Backout moves the activation row to `disabled`, stops new submission/tool visibility, and lets exact pinned historical
work drain or be cancelled. It never reopens schema-less submission, rewrites pins, downgrades v2 rows to v1, or
deletes historical serializers. Hosted serving is fail-closed during registry/activation disagreement.

## 9. Executable implementation DAG

| Node | Depends on | Single integration owner / parallel leaves | Deliverable and activation assertion |
| --- | --- | --- | --- |
| `P0-d1m` | none | D1m owner | exact D1m commit + valid review + 10/5 generated roster; required for 15/15/R-029/hosted, not for independent leaf coding |
| `F1-result-core` | plan | result-contract leaf | immutable `ActionResultSpec` registry core and focused tests; no action marked ready |
| `F0-version-activation` | plan | migration/registry owner | historical request/result lookup + PG activation row + mixed-replica/backout tests |
| `F4-uow` | plan | repository/UoW owner | touched-path command creation/terminal/commandless UoWs and concurrency/fault matrix |
| `M1-profile-sample` | F0, F4 | profile leaf | closed request/binder/adapter/result owner; ingress mints no Activity/Delta |
| `M2-continue` | F0, F4 | acquisition leaf | seven closed variants, exact owners, results, success fixtures |
| `V1-plan-preview` | F0, F1, F4 | planner leaf | pure preview request/result/UoW and immutable preview artifact |
| `M4-promotion` | F0, F1, F4 | assertion leaf | approval receipt + promotion/assertion/event/index UoW |
| `M5-intake` | F0, F1, F4 | intake leaf | staged content contract, Excel adapter, typed model activity/result |
| `V2-start-v2` | V1, F0, F4 | acquisition integration owner | preview-only request, approval receipt, historical v1 coexistence |
| `V3-filter-query` | F1, F4 | projection/query owner | filter v2 Cohort mapping + `inspect_operation` query spec/results |
| `F3-tool-registry` | F1, F0, V1, V2, V3 | Agent registry owner | unified `AgentToolSpec`, occurrence/result pins, release state; no public served tools |
| `B0-15-row-integration` | P0, M1, M2, V1, M4, M5, F0 | one registry/API owner | generated 15/15 roster; no schema-less current submissions; R-029 epoch may start |
| `S1-pg-simulate` | F3, V1, V2, V3, F4 | simulate owner | mandatory PG terminal-success for every canary tool; negative states separate |
| `S2-lab-scripted` | S1, manifest-v2 | E2E owner | TML + Anthropic same-path fake/scripted proof, clean provider-mode/contamination report |
| `S3-local-agent` | S2 + scope reviews | local runtime owner | scripted model drives the exact local Agent slice in isolated PG; `local_canary`, not hosted |
| `L1-cohort-live-checkpoint` | manifest-v2, F4 | separate provider-runtime owner | durable lane/cost/ambiguous-submit/resume/reuse capability; reviewed predecessor for paid call |
| `L2-agent-live-route` | F3, result slot | model-turn owner | reviewed local-canary model route/context/cost/circuit/result slot |
| `C1-paid-tml` | S3, L1, L2 + §6.3 | canary owner | one bounded paid local TML manifest and result review; no automatic scale-out |
| `H1-hosted` | B0 + R-029 closed + hosted reviews | release owner | first `served=true`; outside 24-hour local canary goal |

Shared hotspots—`operation_runtime.py`, `orchestrator.py`, public API routing, registry aggregation, migrations, and
release-state derivation—have one serial integration owner. Leaf modules/tests may be developed in parallel. No leaf
stages or commits another worker's files.

## 10. Exact commands, report contract, and review artifacts

### 10.1 Gate report

Implementation must add `scripts/verify_track_d_d1n.py`; until it exists, the commands below are planned acceptance
commands, not evidence. Every invocation writes
`runtime/audits/track_d_d1n/<node>.json` with schema `track_d_d1n_gate_report.v1` and exact fields:

```text
node_id, status, generated_at, git_commit, git_tree, scope_digest
commands[{argv, exit_code, passed, failed, subtests, deselected}]
registry{action_count, schema_defined_count, schema_less_count, roster_digest,
         historical_pin_set_digest, tool_counts_by_state}
provider{modes, invocation_count_by_mode, live_invocation_count, contamination_status}
artifacts[{path, sha256, schema_version}]
reviews[{path, title, scope_digest, verifier_status, verdict}]
assertions[{id, expected, actual, status}]
```

Missing fields, moving refs, nonzero exits, an unexpected provider invocation, or a non-`passed` assertion makes the
report `failed`. The verifier must call the shared independent-review artifact validator rather than parse a final
word.

### 10.2 Commands

Run from the repository root with no live provider variables:

```bash
# C0: start the required PG owner for durable tests
make local-pg-up

# C1: roster/version/result/tool contract
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1_action_request_contract.py \
  tests/test_d1n_action_result_schema.py \
  tests/test_d1n_action_contract_activation.py \
  tests/test_d1n_agent_tool_registry.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python scripts/verify_track_d_d1n.py \
  --node contract --output runtime/audits/track_d_d1n/contract.json

# C2: five remaining action migrations
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1n_fetch_profile_sample.py \
  tests/test_d1n_continue_acquisition.py \
  tests/test_d1n_plan_acquisition.py \
  tests/test_d1n_promote_person_assertion.py \
  tests/test_d1n_external_intake.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python scripts/verify_track_d_d1n.py \
  --node actions-15 --expect-action-count 15 --expect-schema-defined-count 15 \
  --output runtime/audits/track_d_d1n/actions-15.json

# C3: start-v2, filter-v2, read-only query, and UoW concurrency
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1n_start_acquisition_v2.py \
  tests/test_d1n_projection_cohort_filter.py \
  tests/test_d1n_operation_query_tool.py \
  tests/test_d1n_operation_uow_pg.py

# C4: PG-backed simulate; terminal success is mandatory
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1n_simulate_agent_tools_pg.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python scripts/verify_track_d_d1n.py \
  --node pg-simulate --require-terminal-success --expect-live-invocations 0 \
  --output runtime/audits/track_d_d1n/pg-simulate.json

# C5: two-lab same-path fake/scripted proof
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1n_provider_manifest_v2.py \
  tests/test_d1n_tml_agent_scripted_e2e_pg.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python scripts/verify_track_d_d1n.py \
  --node labs-scripted --require-company thinking-machines-lab --require-company anthropic \
  --require-provider-mode scripted --expect-live-invocations 0 --require-clean-contamination \
  --output runtime/audits/track_d_d1n/labs-scripted.json

# C6: live checkpoint predecessor, still with fake transport
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1n_cohort_live_checkpoint_pg.py \
  tests/test_d1n_agent_live_route_contract.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python scripts/verify_track_d_d1n.py \
  --node live-prerequisites --expect-live-invocations 0 \
  --output runtime/audits/track_d_d1n/live-prerequisites.json

# C7: quality ratchets; full tests/test_pipeline.py remains forbidden
make lint
make typecheck
git diff --check
```

Each committed node then launches, without waiting to begin unrelated work:

```bash
make independent-review-gate \
  REVIEW_TITLE="Track_D_D1n_<node>" \
  REVIEW_FILES="<exact committed files for the node>" \
  REVIEW_BASE="<exact immediate pinned base SHA>" \
  REVIEW_EXECUTE=1
```

The review output must be under `runtime/reviews/`, have exact commit/scope binding, zero reviewer exit, verified
effective model/effort/tier and rollout evidence, and final `GO`. Author evidence or advisory review is never recorded
as formal `GO`. Paid canary consumption adds the exact artifact paths and scope digests to
`runtime/audits/track_d_d1n/paid-tml-preflight.json` and reruns the shared verifier immediately before network I/O.

## 11. Contract-first test matrix

Before fake/scripted E2E:

- exact generated 15-row enumeration, no duplicate action/tool, no active empty request pin;
- current plus historical request/result/tool pin lookup, mixed-replica mismatch, per-action CAS activation, disable
  backout, blank-pin drain/cancel/reissue, v1 pending approve/retry/replay, and new v2 submission;
- exact canonical Cohort round-trip including empty roles=all and server-derived registry/selection digests;
- missing/stale/foreign/conflicting preview and approval receipt with zero writes;
- per-action binder/adapter/command/result matrix and presence-sensitive alias conflict tests;
- exact result occurrence/terminal winner, same-call ordinal replay, stale/late attempt quarantine, registry upgrade
  under old result, and per-field provenance enforcement;
- result rejection for unknown/private/raw-path/oversize/nondeterministic data;
- `inspect_operation` parity with canonical control/display contracts and zero repair/domain writes;
- filter-v2 `any|all|all-roles` mapping, status lanes, stale publication, total/returned/truncation, and provenance;
- action/query release-state mutation tests: removing any pin/binder/adapter/success fixture/review/expiry condition makes
  it unavailable; `local_canary` never yields global `served=true`;
- UoW fault plus concurrency races from §7;
- simulate/scripted/live namespace contamination negatives.

PG simulate uses real AgentAction, OperationRun, WorkflowCommand, Activity, result-slot, event, and outbox stores; only
provider/model transport is simulated. Every action and each of M2's seven command variants needs a deterministic
terminal-success fixture. Deferred, stale, error, retry, cancel, ambiguous-submit, and late-result cases are separate
tests and cannot substitute for success. `simulate_preflight_passed=true` is derived only from terminal success plus
the exact registered serializer.

The assembled loop is:

```text
scripted model terminal tool call -> occurrence/result-slot accept CAS
-> AgentToolSpec validation + owner bind -> action or query execution
-> approval/budget where required -> dispatch/claim/terminal winner
-> exact owner result -> ActionResultSpec serializer -> ToolResultMessage
-> journal pins -> next scripted model turn
```

Only after that loop and the two-lab proof pass should browser/service E2E validate tool discovery, the canonical
role/employment/role-match picker, explicit approval, progress/control, and final Cohort result rendering.

## 12. Thinking Machines Lab and scale boundary

The bounded TML manifest contains data only: canonical company id/registry revision, user-confirmed
`cohort_selection.v1`, thematic constraints for pre-training work, provider/model routes, and explicit call/item/
candidate/cost/time limits. The first selection is whatever the user confirms; `research + engineering` and
`current + former` are examples, not defaults hidden in code.

After a successful paid canary, result review checks canonical identity, requested-lane coverage, role proof,
current/former provenance, provider/cache provenance, duplicates, truncation, cost, and final publication digest before
any broader run. Scaling to OpenAI, Anthropic, Google DeepMind, xAI, Meta, and later labs changes company-registry data,
thematic inputs, cohort choices, and explicit budget only. It must not add an action, provider branch, role enum,
served predicate, result serializer, or live checkpoint state.

Hosted activation still waits for D1n 15/15, R-029's complete-population release-window zero-hit and constraint
validation, scope-matched hosted reviews, and the `hosted` state transition. A successful local paid canary is quality
and operability evidence, not automatic scale or production signoff.

## 13. Ultra review response matrix (17/17)

| # | Finding | Fixed-forward location | Mechanical acceptance |
| ---: | --- | --- | --- |
| 1 | missing sixth action / false denominator | §2.1-2.2 | P0 exact D1m review + generated 15-row roster; no 15/15 claim while placeholder remains |
| 2 | second population-selector contract | §3.1, §5 V3 | exact `cohort_selection.v1`, server digests, empty roles=all through full chain |
| 3 | optional confirmation | §3.2 | required preview ref plus exact persisted-action user approval receipt; absent/stale/foreign/conflict zero-write |
| 4 | no v1/v2 rolling/brownfield | §8 | historical lookup, mixed-replica order, per-action activation CAS, drain/cancel/reissue, disable-only backout |
| 5 | search v1 cannot carry roles/status | §5 V3 | text search stays v1; filter v2 owns Cohort any/all/all-role mapping and result provenance |
| 6 | query tool outside readiness path | §4.3 | one `AgentToolSpec` for action/query; query owner/binder/result/simulate/release pins; canonical controls only |
| 7 | result not bound to occurrence | §4.1-4.2 | result-slot/ordinal/action/run/command/attempt/target/terminal-winner pins and provenance map |
| 8 | incomplete serving ownership | §4.4 | fail-closed `disabled -> shadow -> local_canary -> hosted`; only hosted derives global served |
| 9 | missing Cohort live prerequisite | §6.2-6.3 | reviewed durable lane checkpoint/cost/ambiguous-submit/resume/reuse before live capability issuance |
| 10 | noncanonical canary manifest | §6.1, §12 | `cohort_provider_manifest.v2`, company-registry target, TML+Anthropic same-path proof, no lab branches |
| 11 | evidence revision not promotion approval | §5 M4 | durable human approval receipt, eligibility recheck, distinct suggestion/promotion/export states |
| 12 | mutable staged intake | §5 M5 | PG content-addressed artifact with owner/digest/type/size/expiry/state/revision and claim-time revalidation |
| 13 | incomplete model-assisted intake route | §5 M5 | typed idempotent ActivityAttempt, route/model identity, budgets/retry/circuit, no fallback, non-authoritative output |
| 14 | contradictory profile binder | §5 M1 | binder mints stable existing sample-scope target only; command owner creates Activity/Delta |
| 15 | UoWs lack fences | §7 | fixed lock order, identities, generation/lease/control epoch, terminal winner, event/outbox, lost-ack and races |
| 16 | deferred simulate can pass | §10-11 | mandatory PG-backed terminal-success per action/variant; all other states separate |
| 17 | nonexecutable DAG/gates | §9-10 | real dependencies, first-class start/filter/query nodes, exact commands/report schema/artifact/review verifier |

## 14. Exact next batches

1. **N1, now:** settle F1 result core and P0 D1m independently; commit/review each scope separately.
2. **N2, parallel:** implement F0 historical activation and F4 touched-path UoWs; begin V1 pure preview and M1/M2/M4/M5
   leaf schemas without touching shared integration files concurrently.
3. **N3, critical slice:** integrate V1 -> V2 -> V3, then unified `AgentToolSpec`, result occurrence pins, and canonical
   release-state derivation.
4. **N4:** run PG terminal-success simulate, then TML + Anthropic fake/scripted same-path Agent E2E; open
   `local_canary` only after exact reviews verify green.
5. **N5, parallel paid predecessor:** implement and review L1 Cohort live checkpoints plus L2 Agent model-route live
   owner. Keep all network transport fake until both are complete.
6. **N6:** run the paid TML preflight artifact verifier. Only if every §6.3 item is green, execute one expiring bounded
   local canary and review its result before any other lab.
7. **N7:** finish B0 15/15 if not already complete, start R-029 observation, and keep hosted serving disabled until the
   separate hosted gate closes.

This order preserves the 24-hour product path while preventing scripted E2E from becoming a substitute for schema,
owner, result, concurrency, or paid-call correctness.
