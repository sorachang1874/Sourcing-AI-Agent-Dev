# Track D D3c1 — Workflow-command public projection seal

> Status: implementation candidate; non-live only. This batch installs the first rollout step required by
> `TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md` §10/§12. It contains no migration, claim mint/CAS,
> Stage A/B, dispatch, provider/model call, served Agent command, or R-019 remediation claim. Author validation and a
> fresh pinned non-author review are required before this status can be promoted.

> Fixed-forward note (2026-07-15):
> [`TRACK_D_D3C1A_WORKFLOW_COMMAND_PUBLIC_PROJECTION_FIXED_FORWARD.md`](TRACK_D_D3C1A_WORKFLOW_COMMAND_PUBLIC_PROJECTION_FIXED_FORWARD.md)
> supersedes this original candidate as the current review scope and addresses the five direct findings from D3c1's
> invalid pinned-review artifact. D3c1a also contains separately labelled precommit adversarial author-audit fixes for
> structured Activity refs, `4 singular + 3 served plural` control carriers/derived fields, `8 canonical + normalized aliases` recursive
> Activity carrier keys, nested workflow-command/operation-sync/direct-Activity closure, hazardous-key rejection,
> typed malformed-input parity, generic-derived provenance stripping, and compact-observation closure,
> and mathematical safe-integer canonicalization; those are not
> retroactive findings from the invalid artifact and are not formal review evidence. D3c1a final candidate author
> evidence for commit `4cfd1916da8bd98483d1ecfdba1f66639b122da9` is recorded in the fixed-forward document. Its
> later `20260714T215839Z_*` pinned artifact is itself invalid/advisory because
> `causal_binding.final_response_item_exact=false`; its substantive text is not formal `NO-GO`. Findings #1/#2/#4-#11
> are fixed-forwarded in the current follow-up, including replacement of lazy repair with constructor-only
> `CommandKernel` ownership; #3 remains `R-019`. A second pinned attempt,
> `20260714T231243Z_*`, is also invalid/advisory only for the same Desktop response-item memory-annotation mismatch;
> its action-outcome, wrapper-budget, demo, policy-owner, PG N+1, operation-sync sentinel, mutable-Activity, and stale-doc
> evidence is fixed-forwarded in the D3c1a document. A final integration audit additionally closed DAG-alias output
> amplification, nonempty projected-empty frontend operation sync, foreign Activity lineage, and whitespace-wrapped
> outcomes. Current targeted validation is recorded there; a fresh
> highest-effort pinned review remains pending;
> this document is retained as the original implementation baseline, not as a current formal verdict.

## 1. Outcome and impact

The public workflow-command boundary no longer spreads arbitrary repository dictionaries. One checked-in owner in
`CommandKernel` now selects a closed field set before any list/detail/provenance/control response can expose a command.
This prevents a later internal descriptor or private claim capability from becoming public merely because a database
row, nested payload/result, execution summary, operation mirror, or frontend adapter gained a new key.

The implementation deliberately preserves existing product diagnostics and payload/result visibility. It removes only
unknown top-level command fields and recursively removes private capability material. Safe `claim_generation` and
`control_epoch` remain optional, non-authoritative diagnostics; neither authorizes an effect.

## 2. Canonical owner and closed field contract

The single backend owner is:

- `CommandKernel._workflow_command_api_record(...)` for a command record;
- `CommandKernel._workflow_command_operation_sync_api_record(...)` for the typed operation-sync mirror;
- `CommandKernel._workflow_command_public_carrier_api_record(...)` for recursive capability removal from enclosing
  action/run/event/execution-summary carriers.

The command allowlist is a checked-in literal. It is not generated from `WORKFLOW_COMMANDS`, reflection, a schema, a
caller payload, or a model response. Its **42 optional public fields** are:

1. **33 existing descriptor fields**:
   `command_id`, `workflow_run_id`, `operation_id`, `command_type`, `owner`, `stage_id`, `causal_group_id`,
   `parent_command_id`, `source_event_id`, `source_event_type`, `input_artifact_refs`, `output_artifact_refs`,
   `produced_entity_counts`, `no_op_reason`, `readiness_effect`, `downstream_command_ids`,
   `causality_schema_version`, `status`, `idempotency_key`, `payload`, `artifact_refs`, `not_before_at`, `attempt`,
   `max_attempts`, `retry_policy`, `lease_owner`, `lease_expires_at`, `heartbeat_at`, `last_error`, `result`,
   `schema_version`, `created_at`, `updated_at`;
2. **two safe diagnostics**: `claim_generation`, `control_epoch`;
3. **seven derived fields**: `agent_exposure_gate`, `agent_exposure_status`, `display_contract`, `control_policy`,
   `control_state`, `activity_spine_policy`, and optional `execution_summary`.

The base projector therefore emits at most 41 keys; the execution-summary variant emits at most 42. Missing or null
source fields stay absent. The projector does not synthesize nulls and does not mutate its input.

## 3. Recursive private-capability rule

Every allowed value and every derived carrier is traversed through dictionaries and lists/tuples. The public mirror
removes exact or normalized snake/camel/hyphen variants of:

- raw claim token/verifier/preview/prefix material;
- claim authority id/seal/spec/private object material, including bootstrap authority id/digest and issuer pins;
- `ClaimAuthority`, `ClaimReceipt`, and `ClaimIdentity` carriers;
- claim selection/consumed-authority identity;
- lease token/identity, claim capability/secret, and private heartbeat occurrence id.

Prefix variants of `claim_token_*`, `claim_authority_*`, `bootstrap_authority_*`, and
`scoped_review_session_bootstrap_*` fail closed. Non-JSON objects and non-finite numbers are omitted rather than
stringified into an invalid JSON mirror. Ordinary product fields such as artifact/business digests, payload/result
content, lease-owner diagnostics, and artifact refs remain visible. The sanitizer never mutates the durable/internal
record and is not an authorization predicate.

## 4. Public carrier closure

The D3b characterization found seven direct nested raw-command returns. D3c1 routes all seven through the same command
projector and sanitizes the complete enclosing operation-sync object:

- one `CommandKernel._sync_operation_run_from_workflow_command` return;
- four `AcquisitionCommandOwner` operation-sync returns;
- two branches of `SourcingOrchestrator._sync_operation_run_from_workflow_command_control`.

The compact job-materialization command view remains a deliberately smaller presentation, but it now receives the
canonical safe command record before compaction; it is not a second field-ownership boundary.

Scout also found that the prior **14 routes / 15 method-route variants** inventory omitted GET action detail and POST
action reject. The corrected public carrier denominator is **16 unique routes / 17 method-route variants**:

- workflow command list/detail/cancel/retry/resume;
- operation run list/detail/provenance/cancel/retry/resume/dispatch;
- operation action GET list, POST submit, GET detail, approve, and reject.

Action/run/event records, execution summaries, operation control responses, and provenance timelines share the same
recursive capability sanitizer so nested `input`, `result_ref`, `metadata`, and event `payload` mirrors cannot recover a
private capability.

## 5. Typed `operation_sync` and schema-reference accounting

`WorkflowCommandOperationSync` is a closed nine-field optional object:

`status`, `reason`, `operation_run_id`, `operation_status`, `control_action`, `command_status`, `operation_run`, `event`,
and `workflow_command`.

The frontend schema keeps the **six existing** `WorkflowCommandRecord` consumer refs and adds **one deliberate typed
nested ref** for `operation_sync.workflow_command`; the mechanical total is therefore **seven**, not six. Preserving the
existing nested command shape avoids a silent response compatibility break. This accounting is explicit rather than
duplicating the 42-field schema or hiding the reference to preserve a headline count.

`WorkflowCommandRecord` has `additionalProperties=false`; its TypeScript interface has no `JsonObject` extension or
string index signature. The public adapter does not spread its source. The frontend demo builds its `raw` mirror from
the same 42-key projection rather than retaining the unfiltered server object. Both frontend mappers recursively omit
private capability keys while retaining legitimate business data.

## 6. Acceptance and regression gates

The batch must prove, on its final stable commit:

- literal 33 + 2 + 7 field equality and sparse/non-mutating behavior;
- unknown top-level keys and private capability aliases/objects are absent at every nesting depth;
- valid payload/result/artifact/business data is preserved;
- the raw nested-command AST count is zero and all seven former bypasses use the central operation-sync projector;
- the corrected 16-route / 17 method-route-variant inventory is exact;
- schema has exactly 42 closed properties, the typed operation-sync has exactly nine, and schema refs total 7 = 6 + 1;
- backend, public adapter, and demo mappings reject malicious nested-capability fixtures executably;
- D3a/D3b characterization oracles are intentionally advanced rather than weakened or deleted;
- targeted API/runtime tests, frontend build, lint, and the existing mypy ceiling remain green/unchanged.

Author evidence on the final candidate tree:

- D3c1 projection contract: **7 passed**;
- combined D3a + D3b + D3c1: **45 passed**;
- targeted operation/control + dispatch adjacency: **14 passed**;
- targeted pre-Agent operation/frontend contract adjacency: **4 passed**;
- standalone frontend contract + adapter TypeScript compile: **passed**;
- frontend production build: **81 modules transformed**;
- lint: **58 files already formatted; all checks passed**;
- mypy: expected existing ceiling held at **81 errors in 4 files**;
- `git diff --check`: clean.

Review evidence: a local precommit adversarial pass first found bootstrap authority/receipt alias leakage and non-finite
backend-number drift (`P0/P1/P2/P3=0/1/1/0`). The three implementations and independent fixtures were fixed forward;
the fresh local advisory recheck is clean (`0/0/0/0`). This remains author-session advisory evidence, not formal `GO`;
a fresh pinned non-author review was attempted against exact commit `2e3e7d9a`. The reviewer completed substantive
output with five new findings (`P0/P1/P2/P3=0/2/3/0`), but the runner rejected the artifact because it could not
causally bind active settings, exact prompt, final output, and the single completed turn. The artifact is therefore
invalid formal evidence: it is neither formal `NO-GO` nor `GO`. Its concrete findings remain direct advisory evidence
and must be verified/fixed forward before Live/W6/manual/signoff; pending review still does not freeze unrelated
non-live batches.

The later D3c1a pinned attempts `20260714T215839Z_*` and `20260714T231243Z_*` both completed substantive reviews but
failed the gate at `causal_binding.final_response_item_exact=false`. Their printed `NO-GO` text is therefore advisory
only, not a formal verdict. D3c1a records fixed-forward mechanisms for the first artifact's #1/#2/#4-#11 and every
actionable finding from the second, plus the four final integration-audit gaps; `R-019` remains open. Current targeted
validation is recorded in D3c1a, while a
fresh scope-matched review at the operator-owned highest supported reasoning effort is still required.

## 7. Explicit non-closure

D3c1 closes only the public projection prerequisite. The following remain open and cannot be credited to this batch:

- physical D3 migration and scoped review-session scope issuer;
- bootstrap/strict-D3 registry manifests and policy pins;
- `ScopedReviewSessionBootstrapAuthority`/receipt factory, Stage A/B, normal ClaimAuthority/ClaimReceipt, and central
  business predicate;
- 29 claim callers, generation/token/control CAS, heartbeat occurrence, terminal provenance, quarantine, and dispatch;
- action-root durable-scope gate, R-019, and OB-10.1/10.2/10.3/10.4;
- served Agent tool population, which remains zero;
- any fake/scripted/live provider activation or product/milestone signoff.

The timed-out D3b formal-review attempt is invalid review evidence and does not become either a scope-matched `GO` or a
substantive reviewer `NO-GO`. D3c1 may continue non-live under the repository rule that pending/invalid review does not
freeze unrelated bounded work; Live and signoff remain fail closed.
