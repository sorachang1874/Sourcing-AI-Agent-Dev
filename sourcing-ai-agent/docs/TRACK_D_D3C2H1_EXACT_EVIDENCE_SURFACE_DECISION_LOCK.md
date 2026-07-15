# Track D D3c2h1 — Exact evidence-surface decision lock

> Status: **decision lock only** (2026-07-15). This batch ratifies exact future manifests, owner/store paths,
> full-PFX relations, occurrence encoders, state machines, and transaction composition. It adds no SQL, migration,
> descriptor, repository, runtime writer, provider/model/Harvest call, served Agent tool, live activation, or product
> gate. D0f is implemented at `539c689`; the five surfaces below remain physically absent.

## 1. Outcome and bounded impact

D3c2h0 fixed the cross-contract relationships but deliberately deferred exact schemas. D3c2h1 closes that decision
gap without pretending to implement it. The exact future surfaces are:

1. `verification_intents`, owned only by `VerificationIntentRepository`;
2. `transport_response_receipts` and `transport_attempt_failure_receipts`, owned only by one
   `TransportEvidenceRepository`;
3. `transport_response_classification_intents`, owned only by `ResponseClassificationIntentRepository`;
4. `workflow_late_result_quarantine`, owned only by `LateResultQuarantineRepository`.

The classification-intent row is the fifth durable surface. It closes the D3c2h0 exposure-first gap: a committed valid
response receipt that was not classified under the global owner-row lock prefix is never silently treated as current or
stale. It remains non-authorizing and retryable until the classification owner records one terminal classification.

The following declaration block is the machine-readable authority for this decision:

```text
EXACT_EVIDENCE_SURFACE_DECISION_V1
scope_prefix = runtime_namespace | provider_mode | workspace_id | scope_digest | coordination_plan_review_id
eligible_provider_modes = live | simulate | scripted
transport_kind = model_tool_v1
replay_behavior = fail_closed_zero_write
provider_search_behavior = deferred_owner_ratified_variant
verification_intents_column_count = 52
transport_response_receipts_column_count = 30
transport_attempt_failure_receipts_column_count = 28
transport_response_classification_intents_column_count = 18
workflow_late_result_quarantine_column_count = 41
verification_source_core_count = 7
verification_terminal_tuple_count = 29
verification_intents_local_check_count = 10
transport_response_receipts_local_check_count = 9
transport_attempt_failure_receipts_local_check_count = 9
transport_response_classification_intents_local_check_count = 7
workflow_late_result_quarantine_local_check_count = 12
local_check_total = 47
response_occurrence_domain = transport-response-occurrence-v2
failure_occurrence_domain = transport-attempt-failure-occurrence-v2
classification_intent_domain = transport-response-classification-intent-v1
quarantine_idempotency_domain = late-response-v2
quarantine_retention_policy = quarantine_retention_30d_v1
quarantine_retention_deadline = recorded_at + interval '30 days'
exposure_first_quarantine_permission = forbidden
classification_authority = stored_current_state_under_d3_dispatch_v2_global_lock_prefix
implementation_status = decision_locked_not_implemented
```

Every column wrapper not already fixed by D3b/D3c2h0 is a new D3c2h1 owner decision. In particular, the minimal
verification-intent wrapper, the classification-intent table, and the quarantine retention/version columns are newly
ratified here. They are not represented as mechanically pre-existing prose or code.

## 2. Sole owners and store paths

| Surface | Sole future owner | Sole store path | Physical source of truth | Allowed mutations | Forbidden mutations |
|---|---|---|---|---|---|
| `verification_intents` | `src/sourcing_agent/repositories/verification_intents.py::VerificationIntentRepository` | `store.repos.verification_intents` | one future PG row per full-PFX intent phase | typed create/exact-replay, append-source-terminal-once, record-outcome CAS, owner control/successor CAS | generic Store upsert, transport/callback writes, reader repair, caller terminal tuple |
| both transport receipt tables | `src/sourcing_agent/repositories/transport_evidence.py::TransportEvidenceRepository` | `store.repos.transport_evidence` | immutable future PG response/failure receipt rows | typed insert-or-exact-replay while the exact exposure is locked | envelope issuance, exposure SQL, quarantine SQL, command/domain/event writes |
| `transport_response_classification_intents` | `src/sourcing_agent/repositories/response_classification_intents.py::ResponseClassificationIntentRepository` | `store.repos.response_classification_intents` | one future PG classification intent per response occurrence | typed create/claim/retry/reclaim/complete/fail CAS | caller current/stale flag, receipt mutation, quarantine insert, workflow/domain apply |
| `workflow_late_result_quarantine` | `src/sourcing_agent/repositories/late_result_quarantine.py::LateResultQuarantineRepository` | `store.repos.late_result_quarantine` | one future PG non-authorizable response tombstone | insert/exact-replay plus disjoint cost and retention CAS entrypoints | attempt-failure/no-call insert, promotion, reducer/domain/public/model consumption, exposure SQL |
| post-network composition | future `D3PostNetworkEvidenceCoordinator`, a transaction composition seam only | no `store.repos` entry and no table | one transaction order over the typed repositories above | compose the two orders in §10 and pass transaction-local classification capability | direct SQL, second ownership, network I/O in a PG transaction, serialized classification authority |

`ModelInvocationEnvelopeV1` remains the sole canonical envelope shape/digest owner. D0f's
`ModelInvocationEnvelopeRepository` at `store.repos.model_invocation_envelopes` remains the sole durable envelope-ref
issuer. Receipt, classification, and quarantine owners only exact-copy and structurally reference its PFX-bound ref and
digest. `CostLedgerRepository` remains the only exposure/settlement owner.

## 3. Common physical prefix and manifest notation

`PFX` is documentation shorthand for these five physical columns in this exact order:

```text
runtime_namespace TEXT NOT NULL
provider_mode TEXT NOT NULL
workspace_id TEXT NOT NULL
scope_digest TEXT NOT NULL
coordination_plan_review_id BIGINT NOT NULL
```

All five columns appear at positions 1–5 of every table below. `provider_mode` is exactly
`live|simulate|scripted`; `scope_digest` is lowercase 64-hex; coordination lineage is positive and exact-equals
`plan_review_sessions.review_id`. Every PK, unique identity, FK, lookup, occurrence/idempotency input, exact-replay
comparison, and mutating predicate carries all five fields. No scope-digest-only alias or inferred tenant/mode fallback
exists.

In the manifest tables, `none` means no SQL default. `owner INSERT expression` means the owner supplies a value derived
inside the same PG transaction; it is not caller input and is not an ambient clock.

## 4. Exact `verification_intents` manifest — 52 columns

The seven-field source core and 29-field terminal tuple are copied verbatim from the controlling D3b contract. D3c2h1
ratifies the smallest coherent wrapper needed for phase identity, state, source append-once CAS, record convergence,
versioning, and DB-clock audit. It deliberately adds no route/policy ladder outside `d3_business_fence_digest`.

| # | Column | SQL type | Nullable | Default |
|---:|---|---|---:|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `intent_id` | `TEXT` | no | none |
| 7 | `operation_run_id` | `TEXT` | no | none |
| 8 | `phase_generation` | `BIGINT` | no | none |
| 9 | `intent_state` | `TEXT` | no | none |
| 10 | `d3_business_fence_digest` | `TEXT` | no | none |
| 11 | `record_outcome` | `TEXT` | yes | none |
| 12 | `recorded_event_id` | `TEXT` | yes | none |
| 13 | `source_verification_command_id` | `TEXT` | no | none |
| 14 | `source_claim_generation` | `BIGINT` | no | none |
| 15 | `source_control_epoch` | `BIGINT` | no | none |
| 16 | `source_command_attempt` | `BIGINT` | no | none |
| 17 | `source_activity_run_id` | `TEXT` | no | none |
| 18 | `source_activity_attempt_id` | `TEXT` | no | none |
| 19 | `source_claim_authority_spec_digest` | `TEXT` | no | none |
| 20 | `expected_source_terminal_status` | `TEXT` | yes | none |
| 21 | `expected_source_terminal_event_id` | `TEXT` | yes | none |
| 22 | `expected_source_terminal_outcome_digest` | `TEXT` | yes | none |
| 23 | `expected_source_terminal_transport_variant` | `TEXT` | yes | none |
| 24 | `expected_source_terminal_provenance_policy_digest` | `TEXT` | yes | none |
| 25 | `expected_source_response_spec_digest` | `TEXT` | yes | none |
| 26 | `expected_source_transport_response_receipt_id` | `TEXT` | yes | none |
| 27 | `expected_source_transport_attempt_failure_receipt_id` | `TEXT` | yes | none |
| 28 | `expected_source_dispatch_exposure_id` | `TEXT` | yes | none |
| 29 | `expected_source_physical_call_index` | `BIGINT` | yes | none |
| 30 | `expected_source_provider_call_id_state` | `TEXT` | yes | none |
| 31 | `expected_source_provider_call_id` | `TEXT` | yes | none |
| 32 | `expected_source_model_invocation_envelope_ref` | `TEXT` | yes | none |
| 33 | `expected_source_model_invocation_envelope_digest` | `TEXT` | yes | none |
| 34 | `expected_source_terminal_reason` | `TEXT` | yes | none |
| 35 | `expected_source_response_occurrence_id` | `TEXT` | yes | none |
| 36 | `expected_source_canonical_response_digest` | `TEXT` | yes | none |
| 37 | `expected_source_canonical_result_digest` | `TEXT` | yes | none |
| 38 | `expected_source_result_artifact_ref` | `TEXT` | yes | none |
| 39 | `expected_source_result_artifact_digest` | `TEXT` | yes | none |
| 40 | `expected_source_failure_occurrence_id` | `TEXT` | yes | none |
| 41 | `expected_source_failure_code` | `TEXT` | yes | none |
| 42 | `expected_source_failure_spec_digest` | `TEXT` | yes | none |
| 43 | `expected_source_canonical_failure_digest` | `TEXT` | yes | none |
| 44 | `expected_source_retry_policy_revision` | `TEXT` | yes | none |
| 45 | `expected_source_retry_disposition` | `TEXT` | yes | none |
| 46 | `expected_source_failure_artifact_ref` | `TEXT` | yes | none |
| 47 | `expected_source_failure_artifact_digest` | `TEXT` | yes | none |
| 48 | `expected_source_no_exposure_spec_digest` | `TEXT` | yes | none |
| 49 | `state_version` | `BIGINT` | no | `0` |
| 50 | `source_terminal_appended_at` | `TIMESTAMPTZ` | yes | none |
| 51 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 52 | `updated_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |

### 4.1 Source core — exactly seven immutable fields

```text
source_verification_command_id
source_claim_generation
source_control_epoch
source_command_attempt
source_activity_run_id
source_activity_attempt_id
source_claim_authority_spec_digest
```

All seven are non-null at intent creation and immutable. The surrounding wrapper, not this core, owns PFX,
`operation_run_id`, `phase_generation`, and `d3_business_fence_digest`.

### 4.2 Terminal tuple — exact 29-field truth table

Legend: `R` = required non-null, `N` = SQL NULL, `O` = complete-or-absent pair or registry-admitted provider id,
and a literal is the only admitted value. The initial row is the `unbound` column. Every row in one branch is appended
in one expected-null CAS; half tuples fail closed.

| terminal field | unbound | exposure | attempt_failure | no_exposure |
|---|---|---|---|---|
| `expected_source_terminal_status` | N | R | `failed_terminal` | R |
| `expected_source_terminal_event_id` | N | R | R | R |
| `expected_source_terminal_outcome_digest` | N | R | R | R |
| `expected_source_terminal_transport_variant` | N | `exposure` | `attempt_failure` | `no_exposure` |
| `expected_source_terminal_provenance_policy_digest` | N | R | R | R |
| `expected_source_response_spec_digest` | N | R | N | N |
| `expected_source_transport_response_receipt_id` | N | R | N | N |
| `expected_source_transport_attempt_failure_receipt_id` | N | N | R | N |
| `expected_source_dispatch_exposure_id` | N | R | R | N |
| `expected_source_physical_call_index` | N | R | R | N |
| `expected_source_provider_call_id_state` | N | R | R | N |
| `expected_source_provider_call_id` | N | O | O | N |
| `expected_source_model_invocation_envelope_ref` | N | R | N | N |
| `expected_source_model_invocation_envelope_digest` | N | R | N | N |
| `expected_source_terminal_reason` | N | R | N | N |
| `expected_source_response_occurrence_id` | N | R | N | N |
| `expected_source_canonical_response_digest` | N | R | N | N |
| `expected_source_canonical_result_digest` | N | R | N | N |
| `expected_source_result_artifact_ref` | N | O | N | N |
| `expected_source_result_artifact_digest` | N | O | N | N |
| `expected_source_failure_occurrence_id` | N | N | R | N |
| `expected_source_failure_code` | N | N | R | N |
| `expected_source_failure_spec_digest` | N | N | R | N |
| `expected_source_canonical_failure_digest` | N | N | R | N |
| `expected_source_retry_policy_revision` | N | N | R | N |
| `expected_source_retry_disposition` | N | N | `terminal` | N |
| `expected_source_failure_artifact_ref` | N | N | R | N |
| `expected_source_failure_artifact_digest` | N | N | R | N |
| `expected_source_no_exposure_spec_digest` | N | N | N | R |

For exposure, `provider_call_id` is nullable only when the receipt state is
`missing_by_registered_transport`; otherwise it is required. For attempt failure it is nullable only when the state is
`not_observed_before_failure`. The exposure result-artifact columns are both null or both non-null. Attempt-failure
artifact columns are both required. The applicable receipt row and immutable workflow event must exact-match the
complete branch. `source_terminal_appended_at` is set from the transaction DB clock only when the tuple first binds.

### 4.3 Intent state and CAS surface

`intent_state` is exactly `pending|awaiting_budget|applied|cancelled|timed_out|superseded`.

| Method | Exact transition/effect |
|---|---|
| `create_or_exact_replay_intent` | absent -> `pending`; source core complete, terminal 29 all null, record pair null, version 0 |
| `append_source_terminal_once` | `pending` + exact source core + all-null terminal tuple -> one complete truth-table branch; set DB-clock append time; version +1 |
| `apply_record_outcome` | `pending` with complete immutable terminal branch -> `awaiting_budget` for only `record_outcome=awaiting_budget`, otherwise -> `applied`; atomically set complete `record_outcome/recorded_event_id`; version +1 |
| `transition_control` | `pending|awaiting_budget` -> `cancelled|timed_out|superseded`; preserve any prior recorded pair and terminal tuple; version +1 |
| `create_successor_phase` | lock predecessor then create exactly one next `phase_generation`; predecessor becomes `superseded`, successor starts `pending`; no same-row revival |

Exact replay compares all 52 logical values except owner-derived timestamps after first commit. Same-key drift is
`verification_intent_collision`; missing/current-state/fence mismatch is a typed zero-write conflict. No caller may
supply `state_version`, any DB clock, terminal branch, or recorded event identity.

## 5. Exact `transport_response_receipts` manifest — 30 columns

| # | Column | SQL type | Nullable | Default |
|---:|---|---|---:|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `transport_response_receipt_id` | `TEXT` | no | none |
| 7 | `operation_run_id` | `TEXT` | no | none |
| 8 | `command_id` | `TEXT` | no | none |
| 9 | `activity_run_id` | `TEXT` | no | none |
| 10 | `activity_attempt_id` | `TEXT` | no | none |
| 11 | `command_attempt` | `BIGINT` | no | none |
| 12 | `claim_generation` | `BIGINT` | no | none |
| 13 | `control_epoch` | `BIGINT` | no | none |
| 14 | `claim_authority_spec_digest` | `TEXT` | no | none |
| 15 | `d3_business_fence_digest` | `TEXT` | no | none |
| 16 | `terminal_provenance_policy_digest` | `TEXT` | no | none |
| 17 | `response_spec_digest` | `TEXT` | no | none |
| 18 | `dispatch_exposure_id` | `TEXT` | no | none |
| 19 | `physical_call_index` | `BIGINT` | no | none |
| 20 | `provider_call_id_state` | `TEXT` | no | none |
| 21 | `provider_call_id` | `TEXT` | yes | none |
| 22 | `model_invocation_envelope_ref` | `TEXT` | no | none |
| 23 | `model_invocation_envelope_digest` | `TEXT` | no | none |
| 24 | `terminal_reason` | `TEXT` | no | none |
| 25 | `canonical_delivery_identity` | `TEXT` | no | none |
| 26 | `response_occurrence_id` | `TEXT` | no | none |
| 27 | `canonical_response_digest` | `TEXT` | no | none |
| 28 | `canonical_result_digest` | `TEXT` | no | none |
| 29 | `result_artifact_ref` | `TEXT` | yes | none |
| 30 | `result_artifact_digest` | `TEXT` | yes | none |

This is immutable insert-or-exact-replay evidence. `command_attempt` exact-copies the committed exposure and linked
ActivityAttempt. The provider id is nullable only with `provider_call_id_state=missing_by_registered_transport`; the
artifact pair is both null or both non-null. Valid registered `length|content_filter` terminal responses remain this
response shape. Incomplete/truncated/protocol-parse failures cannot create this row.

## 6. Exact `transport_attempt_failure_receipts` manifest — 28 columns

| # | Column | SQL type | Nullable | Default |
|---:|---|---|---:|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `transport_attempt_failure_receipt_id` | `TEXT` | no | none |
| 7 | `operation_run_id` | `TEXT` | no | none |
| 8 | `command_id` | `TEXT` | no | none |
| 9 | `activity_run_id` | `TEXT` | no | none |
| 10 | `activity_attempt_id` | `TEXT` | no | none |
| 11 | `command_attempt` | `BIGINT` | no | none |
| 12 | `claim_generation` | `BIGINT` | no | none |
| 13 | `control_epoch` | `BIGINT` | no | none |
| 14 | `claim_authority_spec_digest` | `TEXT` | no | none |
| 15 | `d3_business_fence_digest` | `TEXT` | no | none |
| 16 | `terminal_provenance_policy_digest` | `TEXT` | no | none |
| 17 | `dispatch_exposure_id` | `TEXT` | no | none |
| 18 | `physical_call_index` | `BIGINT` | no | none |
| 19 | `provider_call_id_state` | `TEXT` | no | none |
| 20 | `provider_call_id` | `TEXT` | yes | none |
| 21 | `failure_occurrence_id` | `TEXT` | no | none |
| 22 | `failure_code` | `TEXT` | no | none |
| 23 | `failure_spec_digest` | `TEXT` | no | none |
| 24 | `canonical_failure_digest` | `TEXT` | no | none |
| 25 | `retry_policy_revision` | `TEXT` | no | none |
| 26 | `retry_disposition` | `TEXT` | no | none |
| 27 | `failure_artifact_ref` | `TEXT` | no | none |
| 28 | `failure_artifact_digest` | `TEXT` | no | none |

The provider id is nullable only with `provider_call_id_state=not_observed_before_failure`. `retry_disposition` is
exactly `retryable|terminal` from the historical failure spec. This row contains no envelope, response, result, or
result-artifact field and can never create quarantine. One exposure may have one failure occurrence; a later valid
response may still create its distinct response receipt and then be classified under the current stored fence.

## 7. Exact `transport_response_classification_intents` manifest — 18 columns

| # | Column | SQL type | Nullable | Default |
|---:|---|---|---:|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `classification_intent_id` | `TEXT` | no | none |
| 7 | `transport_response_receipt_id` | `TEXT` | no | none |
| 8 | `dispatch_exposure_id` | `TEXT` | no | none |
| 9 | `response_occurrence_id` | `TEXT` | no | none |
| 10 | `classification_idempotency_key` | `TEXT` | no | none |
| 11 | `classification_state` | `TEXT` | no | none |
| 12 | `attempt_count` | `BIGINT` | no | `0` |
| 13 | `next_attempt_at` | `TIMESTAMPTZ` | no | owner INSERT expression |
| 14 | `last_error_code` | `TEXT` | yes | none |
| 15 | `state_version` | `BIGINT` | no | `0` |
| 16 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 17 | `updated_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 18 | `terminal_at` | `TIMESTAMPTZ` | yes | none |

### 7.1 Classification lifecycle and recovery

`classification_state` is exactly
`pending|claimed|classified_current|classified_stale|failed_terminal`. Terminal states never reopen. The fixed checked-in
`response-classification-retry-v1` rule is not a tenant policy ladder: claim lease = 30 seconds, retry delay =
`min(2 ** (attempt_count - 1), 60)` seconds after a failed claimed attempt, and maximum attempts = 8. A future policy
change requires a new table/schema decision after all nonterminal v1 rows drain; callers cannot select it.

| Method | Exact transition/effect |
|---|---|
| `create_or_exact_replay_pending` | absent -> `pending`; DB clock sets `next_attempt_at`; exact receipt/PFX/occurrence replay joins |
| `claim_due` | due `pending` -> `claimed`; attempt +1, version +1, DB clock sets the 30-second lease deadline; return a private non-serializable claim capability |
| `retry_claim` | current `claimed` -> `pending`; registered transient error only, fixed DB-clock delay, version +1 |
| `reclaim_expired_claim` | expired `claimed` -> `pending`; `last_error_code=claim_lease_expired`, DB-clock due time, version +1 |
| `complete_current` | current claimed capability + global stored-state proof -> `classified_current`; set terminal DB clock, version +1; no quarantine |
| `complete_stale_with_quarantine` | current claimed capability + global stored-state proof -> `classified_stale`; same UoW inserts/exact-replays quarantine, sets terminal DB clock, version +1 |
| `fail_terminal` | attempt 8 plus registered non-provable/permanent classification error -> `failed_terminal`; set terminal DB clock, version +1; authorizes neither apply nor quarantine |

The private claim capability binds full PFX, classification id, receipt/exposure/occurrence, claimed state version,
attempt count, and DB lease deadline. It is not stored, serialized, or reconstructed from ids. A failed global lock or
unprovable classification rolls back the classification UoW first; only then may the classification owner perform its
own row-only retry/fail CAS. That CAS records no current/stale label and grants no result authority.

## 8. Exact `workflow_late_result_quarantine` manifest — 41 columns

| # | Column | SQL type | Nullable | Default |
|---:|---|---|---:|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `quarantine_id` | `TEXT` | no | none |
| 7 | `operation_run_id` | `TEXT` | no | none |
| 8 | `command_id` | `TEXT` | no | none |
| 9 | `activity_run_id` | `TEXT` | no | none |
| 10 | `activity_attempt_id` | `TEXT` | no | none |
| 11 | `command_attempt` | `BIGINT` | no | none |
| 12 | `claim_generation` | `BIGINT` | no | none |
| 13 | `control_epoch` | `BIGINT` | no | none |
| 14 | `claim_authority_spec_digest` | `TEXT` | no | none |
| 15 | `d3_business_fence_digest` | `TEXT` | no | none |
| 16 | `dispatch_exposure_id` | `TEXT` | no | none |
| 17 | `physical_call_index` | `BIGINT` | no | none |
| 18 | `provider_call_id_state` | `TEXT` | no | none |
| 19 | `provider_call_id` | `TEXT` | yes | none |
| 20 | `transport_response_receipt_id` | `TEXT` | no | none |
| 21 | `canonical_delivery_identity` | `TEXT` | no | none |
| 22 | `response_occurrence_id` | `TEXT` | no | none |
| 23 | `terminal_reason` | `TEXT` | no | none |
| 24 | `model_invocation_envelope_ref` | `TEXT` | no | none |
| 25 | `model_invocation_envelope_digest` | `TEXT` | no | none |
| 26 | `canonical_response_digest` | `TEXT` | no | none |
| 27 | `canonical_result_digest` | `TEXT` | no | none |
| 28 | `result_artifact_ref` | `TEXT` | yes | none |
| 29 | `result_artifact_digest` | `TEXT` | yes | none |
| 30 | `rejection_reason` | `TEXT` | no | none |
| 31 | `cost_state` | `TEXT` | no | none |
| 32 | `retention_state` | `TEXT` | no | none |
| 33 | `authorizable` | `BOOLEAN` | no | `false` |
| 34 | `idempotency_key` | `TEXT` | no | none |
| 35 | `retention_policy_version` | `TEXT` | no | none |
| 36 | `recorded_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 37 | `retention_until` | `TIMESTAMPTZ` | no | owner INSERT expression |
| 38 | `cost_reconciled_at` | `TIMESTAMPTZ` | yes | none |
| 39 | `purged_at` | `TIMESTAMPTZ` | yes | none |
| 40 | `cost_state_version` | `BIGINT` | no | `0` |
| 41 | `retention_state_version` | `BIGINT` | no | `0` |

`workflow_run_id` is forbidden. Insert is reachable only from a response receipt classified stale under the global
stored-state lock prefix. `rejection_reason` is the closed owner value `stale_claim|business_precondition_conflict`.
`authorizable` is structurally and semantically always false.

Cost and retention are independent:

```text
cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain
retention_state: retained -> purged_tombstone
```

The cost CAS changes only `cost_state`, `cost_reconciled_at`, and `cost_state_version`; it composes with the cost-ledger
owner without writing exposure SQL itself. The retention CAS changes only `retention_state`, `result_artifact_ref`,
`purged_at`, and `retention_state_version`. It retains all PFX, receipt/exposure/occurrence/envelope identities and every
digest. `reconciled_no_call`, reset, promotion, restore, delete, and mixed disposition do not exist.

The fixed policy is `quarantine_retention_30d_v1`. Insert sets both `recorded_at` and
`retention_until = recorded_at + interval '30 days'` from the same transaction DB clock. Purge is admitted only when
`transaction_timestamp() >= retention_until`; caller clocks/deadlines are forbidden. Cost reconciliation cannot extend
the deadline, and purge does not wait for cost. A retained row has `purged_at IS NULL`; a tombstone has
`purged_at >= retention_until` and `result_artifact_ref IS NULL`. The artifact digest remains when one existed.

## 9. Exact keys, full-PFX FKs, and rollback boundary

Every relation below expands `PFX` into the five physical columns in §3. Constraint names and column order are exact
future migration inputs.

| Order | Name | Kind | Exact child columns / target |
|---:|---|---|---|
| 1 | `verification_intents_pkey` | PK | `(PFX, intent_id)` |
| 2 | `verification_intents_operation_phase_uk` | UNIQUE | `(PFX, operation_run_id, phase_generation)` |
| 3 | `verification_intents_operation_fk` | FK | `(PFX, operation_run_id)` -> `operation_runs(PFX, operation_run_id)` |
| 4 | `verification_intents_source_attempt_fk` | FK | `(PFX, operation_run_id, source_verification_command_id, source_activity_run_id, source_activity_attempt_id)` -> `workflow_activity_attempts(PFX, operation_run_id, command_id, activity_run_id, attempt_id)` |
| 5 | `verification_intents_source_event_fk` | nullable MATCH SIMPLE DEFERRABLE FK | `(PFX, operation_run_id, source_verification_command_id, expected_source_terminal_event_id, expected_source_terminal_outcome_digest)` -> `workflow_events(PFX, operation_id, command_id, event_id, terminal_outcome_digest)` |
| 6 | `verification_intents_response_receipt_fk` | nullable MATCH SIMPLE DEFERRABLE FK | `(PFX, expected_source_dispatch_exposure_id, expected_source_response_occurrence_id, expected_source_transport_response_receipt_id)` -> response receipt exact identity below |
| 7 | `verification_intents_failure_receipt_fk` | nullable MATCH SIMPLE DEFERRABLE FK | `(PFX, expected_source_dispatch_exposure_id, expected_source_failure_occurrence_id, expected_source_transport_attempt_failure_receipt_id)` -> failure receipt exact identity below |
| 8 | `verification_intents_recorded_event_fk` | nullable MATCH SIMPLE DEFERRABLE FK | `(PFX, recorded_event_id)` -> `workflow_events(PFX, event_id)` |
| 9 | `transport_response_receipts_pkey` | PK | `(PFX, transport_response_receipt_id)` |
| 10 | `transport_response_receipts_delivery_uk` | UNIQUE | `(PFX, dispatch_exposure_id, canonical_delivery_identity)` |
| 11 | `transport_response_receipts_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id)` |
| 12 | `transport_response_receipts_child_fk_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id, transport_response_receipt_id)` |
| 13 | `transport_response_receipts_exposure_fk` | FK | `(PFX, dispatch_exposure_id)` -> `dispatch_exposures(PFX, dispatch_exposure_id)` |
| 14 | `transport_response_receipts_attempt_fk` | FK | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` -> `workflow_activity_attempts(PFX, operation_run_id, command_id, activity_run_id, attempt_id)` |
| 15 | `transport_response_receipts_envelope_fk` | FK | `(PFX, model_invocation_envelope_ref, model_invocation_envelope_digest)` -> `model_invocation_envelopes(PFX, model_invocation_envelope_ref, envelope_digest)` |
| 16 | `transport_attempt_failure_receipts_pkey` | PK | `(PFX, transport_attempt_failure_receipt_id)` |
| 17 | `transport_attempt_failure_receipts_exposure_uk` | UNIQUE | `(PFX, dispatch_exposure_id)` |
| 18 | `transport_attempt_failure_receipts_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, failure_occurrence_id)` |
| 19 | `transport_attempt_failure_receipts_child_fk_uk` | UNIQUE | `(PFX, dispatch_exposure_id, failure_occurrence_id, transport_attempt_failure_receipt_id)` |
| 20 | `transport_attempt_failure_receipts_exposure_fk` | FK | `(PFX, dispatch_exposure_id)` -> `dispatch_exposures(PFX, dispatch_exposure_id)` |
| 21 | `transport_attempt_failure_receipts_attempt_fk` | FK | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` -> `workflow_activity_attempts(PFX, operation_run_id, command_id, activity_run_id, attempt_id)` |
| 22 | `transport_response_classification_intents_pkey` | PK | `(PFX, classification_intent_id)` |
| 23 | `transport_response_classification_intents_idempotency_uk` | UNIQUE | `(PFX, classification_idempotency_key)` |
| 24 | `transport_response_classification_intents_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id)` |
| 25 | `transport_response_classification_intents_receipt_fk` | FK | `(PFX, dispatch_exposure_id, response_occurrence_id, transport_response_receipt_id)` -> response receipt child-FK unique identity |
| 26 | `workflow_late_result_quarantine_pkey` | PK | `(PFX, quarantine_id)` |
| 27 | `workflow_late_result_quarantine_idempotency_uk` | UNIQUE | `(PFX, idempotency_key)` |
| 28 | `workflow_late_result_quarantine_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id)` |
| 29 | `workflow_late_result_quarantine_receipt_fk` | FK | `(PFX, dispatch_exposure_id, response_occurrence_id, transport_response_receipt_id)` -> response receipt child-FK unique identity |
| 30 | `workflow_late_result_quarantine_envelope_fk` | FK | `(PFX, model_invocation_envelope_ref, model_invocation_envelope_digest)` -> `model_invocation_envelopes(PFX, model_invocation_envelope_ref, envelope_digest)` |

### 9.1 Exact local CHECK inventory — 47 constraints

The following notation is only a compact documentation macro that the future migration generator must expand into the
displayed SQL operators; it is not a database function or a runtime fallback:

```text
NB(x)  := x ~ '[^[:space:]]'
SHA(x) := x ~ '^[0-9a-f]{64}$'
OPT_NB(x)  := x IS NULL OR NB(x)
OPT_SHA(x) := x IS NULL OR SHA(x)
PFX_VALID := NB(runtime_namespace)
             AND provider_mode IN ('live', 'simulate', 'scripted')
             AND NB(workspace_id)
             AND SHA(scope_digest)
             AND coordination_plan_review_id > 0
PAIR(a, b) := (a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL)
```

`TERMINAL_TUPLE_V1` means the exact four-way SQL disjunction mechanically generated from all 29 rows of §4.2: every
`N` expands to `IS NULL`, every `R` to `IS NOT NULL`, every literal to equality, and every `O` to its immediately
documented provider-id or artifact-pair predicate. It is not permission to substitute an application-only validator.
The executable oracle asserts the complete 29-row input and this one-to-one expansion contract.

| # | Constraint name | Table | Exact predicate |
|---:|---|---|---|
| 1 | `verification_intents_pfx_ck` | `verification_intents` | `PFX_VALID` |
| 2 | `verification_intents_identity_ck` | `verification_intents` | `NB(intent_id) AND NB(operation_run_id) AND NB(source_verification_command_id) AND NB(source_activity_run_id) AND NB(source_activity_attempt_id)` |
| 3 | `verification_intents_counter_ck` | `verification_intents` | `phase_generation > 0 AND source_claim_generation > 0 AND source_control_epoch >= 0 AND source_command_attempt > 0 AND state_version >= 0` |
| 4 | `verification_intents_owner_digest_ck` | `verification_intents` | `SHA(d3_business_fence_digest) AND SHA(source_claim_authority_spec_digest)` |
| 5 | `verification_intents_state_ck` | `verification_intents` | `intent_state IN ('pending', 'awaiting_budget', 'applied', 'cancelled', 'timed_out', 'superseded')` |
| 6 | `verification_intents_record_shape_ck` | `verification_intents` | `((intent_state = 'pending' AND record_outcome IS NULL AND recorded_event_id IS NULL) OR (intent_state = 'awaiting_budget' AND record_outcome = 'awaiting_budget' AND NB(recorded_event_id)) OR (intent_state = 'applied' AND record_outcome IN ('authorizable', 'needs_human', 'failed', 'timed_out') AND NB(recorded_event_id)) OR (intent_state IN ('cancelled', 'timed_out', 'superseded') AND ((record_outcome IS NULL AND recorded_event_id IS NULL) OR (record_outcome IN ('authorizable', 'awaiting_budget', 'needs_human', 'failed', 'timed_out') AND NB(recorded_event_id))))) IS TRUE` |
| 7 | `verification_intents_terminal_tuple_ck` | `verification_intents` | `TERMINAL_TUPLE_V1` |
| 8 | `verification_intents_terminal_digest_ck` | `verification_intents` | `OPT_SHA(expected_source_terminal_outcome_digest) AND OPT_SHA(expected_source_terminal_provenance_policy_digest) AND OPT_SHA(expected_source_response_spec_digest) AND OPT_SHA(expected_source_model_invocation_envelope_digest) AND OPT_SHA(expected_source_response_occurrence_id) AND OPT_SHA(expected_source_canonical_response_digest) AND OPT_SHA(expected_source_canonical_result_digest) AND OPT_SHA(expected_source_result_artifact_digest) AND OPT_SHA(expected_source_failure_occurrence_id) AND OPT_SHA(expected_source_failure_spec_digest) AND OPT_SHA(expected_source_canonical_failure_digest) AND OPT_SHA(expected_source_failure_artifact_digest) AND OPT_SHA(expected_source_no_exposure_spec_digest)` |
| 9 | `verification_intents_terminal_ref_ck` | `verification_intents` | `OPT_NB(expected_source_terminal_event_id) AND OPT_NB(expected_source_transport_response_receipt_id) AND OPT_NB(expected_source_transport_attempt_failure_receipt_id) AND OPT_NB(expected_source_dispatch_exposure_id) AND (expected_source_physical_call_index IS NULL OR expected_source_physical_call_index >= 0) AND OPT_NB(expected_source_provider_call_id) AND OPT_NB(expected_source_model_invocation_envelope_ref) AND OPT_NB(expected_source_result_artifact_ref) AND OPT_NB(expected_source_failure_artifact_ref)` |
| 10 | `verification_intents_timestamp_ck` | `verification_intents` | `updated_at >= created_at AND ((expected_source_terminal_status IS NULL AND source_terminal_appended_at IS NULL) OR (expected_source_terminal_status IS NOT NULL AND source_terminal_appended_at >= created_at))` |
| 11 | `transport_response_receipts_pfx_ck` | `transport_response_receipts` | `PFX_VALID` |
| 12 | `transport_response_receipts_identity_ck` | `transport_response_receipts` | `NB(transport_response_receipt_id) AND NB(operation_run_id) AND NB(command_id) AND NB(activity_run_id) AND NB(activity_attempt_id) AND NB(dispatch_exposure_id) AND NB(canonical_delivery_identity)` |
| 13 | `transport_response_receipts_counter_ck` | `transport_response_receipts` | `command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0 AND physical_call_index >= 0` |
| 14 | `transport_response_receipts_digest_ck` | `transport_response_receipts` | `SHA(claim_authority_spec_digest) AND SHA(d3_business_fence_digest) AND SHA(terminal_provenance_policy_digest) AND SHA(response_spec_digest) AND SHA(model_invocation_envelope_digest) AND SHA(response_occurrence_id) AND SHA(canonical_response_digest) AND SHA(canonical_result_digest) AND OPT_SHA(result_artifact_digest)` |
| 15 | `transport_response_receipts_provider_ck` | `transport_response_receipts` | `((provider_call_id_state = 'present' AND NB(provider_call_id)) OR (provider_call_id_state = 'missing_by_registered_transport' AND provider_call_id IS NULL)) IS TRUE` |
| 16 | `transport_response_receipts_envelope_ref_ck` | `transport_response_receipts` | `model_invocation_envelope_ref ~ '^mie:v1:[0-9a-f]{64}:[0-9a-f]{64}$'` |
| 17 | `transport_response_receipts_terminal_reason_ck` | `transport_response_receipts` | `terminal_reason IN ('end_turn', 'tool_calls', 'length', 'content_filter')` |
| 18 | `transport_response_receipts_occurrence_ck` | `transport_response_receipts` | `transport_response_receipt_id = concat('trr:v2:', response_occurrence_id)` |
| 19 | `transport_response_receipts_artifact_pair_ck` | `transport_response_receipts` | `PAIR(result_artifact_ref, result_artifact_digest)` |
| 20 | `transport_attempt_failure_receipts_pfx_ck` | `transport_attempt_failure_receipts` | `PFX_VALID` |
| 21 | `transport_attempt_failure_receipts_identity_ck` | `transport_attempt_failure_receipts` | `NB(transport_attempt_failure_receipt_id) AND NB(operation_run_id) AND NB(command_id) AND NB(activity_run_id) AND NB(activity_attempt_id) AND NB(dispatch_exposure_id)` |
| 22 | `transport_attempt_failure_receipts_counter_ck` | `transport_attempt_failure_receipts` | `command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0 AND physical_call_index >= 0` |
| 23 | `transport_attempt_failure_receipts_digest_ck` | `transport_attempt_failure_receipts` | `SHA(claim_authority_spec_digest) AND SHA(d3_business_fence_digest) AND SHA(terminal_provenance_policy_digest) AND SHA(failure_occurrence_id) AND SHA(failure_spec_digest) AND SHA(canonical_failure_digest) AND SHA(failure_artifact_digest)` |
| 24 | `transport_attempt_failure_receipts_provider_ck` | `transport_attempt_failure_receipts` | `((provider_call_id_state = 'present' AND NB(provider_call_id)) OR (provider_call_id_state = 'not_observed_before_failure' AND provider_call_id IS NULL)) IS TRUE` |
| 25 | `transport_attempt_failure_receipts_occurrence_ck` | `transport_attempt_failure_receipts` | `transport_attempt_failure_receipt_id = concat('tafr:v2:', failure_occurrence_id)` |
| 26 | `transport_attempt_failure_receipts_retry_ck` | `transport_attempt_failure_receipts` | `NB(failure_code) AND NB(retry_policy_revision) AND retry_disposition IN ('retryable', 'terminal')` |
| 27 | `transport_attempt_failure_receipts_artifact_ck` | `transport_attempt_failure_receipts` | `NB(failure_artifact_ref)` |
| 28 | `transport_attempt_failure_receipts_failure_code_ck` | `transport_attempt_failure_receipts` | `failure_code ~ '^[a-z][a-z0-9_]*$'` |
| 29 | `transport_response_classification_intents_pfx_ck` | `transport_response_classification_intents` | `PFX_VALID` |
| 30 | `transport_response_classification_intents_identity_ck` | `transport_response_classification_intents` | `classification_intent_id ~ '^trci:v1:[0-9a-f]{64}$' AND transport_response_receipt_id ~ '^trr:v2:[0-9a-f]{64}$' AND NB(dispatch_exposure_id) AND SHA(response_occurrence_id) AND classification_idempotency_key ~ '^classification-intent-v1:[0-9a-f]{64}$'` |
| 31 | `transport_response_classification_intents_binding_ck` | `transport_response_classification_intents` | `substring(classification_intent_id from 9) = substring(classification_idempotency_key from 26)` |
| 32 | `transport_response_classification_intents_state_ck` | `transport_response_classification_intents` | `classification_state IN ('pending', 'claimed', 'classified_current', 'classified_stale', 'failed_terminal')` |
| 33 | `transport_response_classification_intents_counter_ck` | `transport_response_classification_intents` | `attempt_count >= 0 AND attempt_count <= 8 AND state_version >= 0` |
| 34 | `transport_response_classification_intents_error_ck` | `transport_response_classification_intents` | `OPT_NB(last_error_code)` |
| 35 | `transport_response_classification_intents_timestamp_ck` | `transport_response_classification_intents` | `updated_at >= created_at AND next_attempt_at >= created_at AND ((classification_state IN ('pending', 'claimed') AND terminal_at IS NULL) OR (classification_state IN ('classified_current', 'classified_stale', 'failed_terminal') AND terminal_at >= created_at))` |
| 36 | `workflow_late_result_quarantine_pfx_ck` | `workflow_late_result_quarantine` | `PFX_VALID` |
| 37 | `workflow_late_result_quarantine_identity_ck` | `workflow_late_result_quarantine` | `quarantine_id ~ '^lrq:v2:[0-9a-f]{64}$' AND NB(operation_run_id) AND NB(command_id) AND NB(activity_run_id) AND NB(activity_attempt_id) AND NB(dispatch_exposure_id) AND transport_response_receipt_id ~ '^trr:v2:[0-9a-f]{64}$' AND NB(canonical_delivery_identity) AND idempotency_key ~ '^late-response-v2:[0-9a-f]{64}$'` |
| 38 | `workflow_late_result_quarantine_counter_ck` | `workflow_late_result_quarantine` | `command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0 AND physical_call_index >= 0 AND cost_state_version >= 0 AND retention_state_version >= 0` |
| 39 | `workflow_late_result_quarantine_digest_ck` | `workflow_late_result_quarantine` | `SHA(claim_authority_spec_digest) AND SHA(d3_business_fence_digest) AND SHA(response_occurrence_id) AND SHA(model_invocation_envelope_digest) AND SHA(canonical_response_digest) AND SHA(canonical_result_digest) AND OPT_SHA(result_artifact_digest)` |
| 40 | `workflow_late_result_quarantine_provider_ck` | `workflow_late_result_quarantine` | `((provider_call_id_state = 'present' AND NB(provider_call_id)) OR (provider_call_id_state = 'missing_by_registered_transport' AND provider_call_id IS NULL)) IS TRUE` |
| 41 | `workflow_late_result_quarantine_artifact_retention_ck` | `workflow_late_result_quarantine` | `((retention_state = 'retained' AND PAIR(result_artifact_ref, result_artifact_digest)) OR (retention_state = 'purged_tombstone' AND result_artifact_ref IS NULL)) IS TRUE` |
| 42 | `workflow_late_result_quarantine_state_ck` | `workflow_late_result_quarantine` | `cost_state IN ('pending_reconciliation', 'reconciled_confirmed', 'reconciled_uncertain') AND retention_state IN ('retained', 'purged_tombstone')` |
| 43 | `workflow_late_result_quarantine_authorizable_ck` | `workflow_late_result_quarantine` | `authorizable = false` |
| 44 | `workflow_late_result_quarantine_rejection_ck` | `workflow_late_result_quarantine` | `rejection_reason IN ('stale_claim', 'business_precondition_conflict')` |
| 45 | `workflow_late_result_quarantine_idempotency_ck` | `workflow_late_result_quarantine` | `substring(quarantine_id from 8) = substring(idempotency_key from 18)` |
| 46 | `workflow_late_result_quarantine_retention_ck` | `workflow_late_result_quarantine` | `retention_policy_version = 'quarantine_retention_30d_v1' AND retention_until = recorded_at + interval '30 days'` |
| 47 | `workflow_late_result_quarantine_timestamp_ck` | `workflow_late_result_quarantine` | `((cost_state = 'pending_reconciliation' AND cost_reconciled_at IS NULL) OR (cost_state IN ('reconciled_confirmed', 'reconciled_uncertain') AND cost_reconciled_at >= recorded_at)) AND ((retention_state = 'retained' AND purged_at IS NULL) OR (retention_state = 'purged_tombstone' AND purged_at >= retention_until))` |

All 47 constraints must be installed as valid `CHECK` constraints in the same future `CREATE TABLE` batch as their
table; there may be no brownfield sentinels and no later `NOT VALID` reinterpretation. Registry applicability,
parent-row equality, exact SHA recomputation, state CAS, and DB-clock eligibility remain repository/FK acceptance
because a local `CHECK` cannot truthfully prove another row, recompute owner registry semantics, or authorize a
transition.

The upstream full-PFX unique keys on OperationRun, ActivityAttempt, WorkflowEvent, and dispatch exposure remain required
Migration-C prerequisites. The later migration must install those exact parent keys before these FKs; it may not weaken
them to id-only, scope-digest-only, JSON, or application-only checks. All new-table shape checks must be installed valid
at creation: nonblank ids, positive/nonnegative counters, lowercase digest shapes, provider-id and artifact-pair truth
tables, exact terminal branch truth table, exact state enums, and DB-clock timestamp relations.

Any collision, FK/PFX/attempt mismatch, invalid state transition, optional quarantine failure, classification terminal
CAS failure, or exposure terminalization mismatch rolls back every mutation in that UoW. Generic replace-all upsert is
forbidden for all five surfaces.

## 10. Full-PFX v2 occurrence and idempotency encoder

The decision-locked future encoder is `canonical-length-delimited-v2`. Each text component is already NFC and is
encoded as `ascii_decimal(byte_length) + ':' + UTF-8 bytes`; components are concatenated with no separator. Integers
use canonical non-negative decimal with no sign or leading zero. The hash is lowercase SHA-256 of the complete bytes.

```text
response_occurrence_id = sha256(LD2(
  "transport-response-occurrence-v2", PFX, dispatch_exposure_id, canonical_delivery_identity
))
failure_occurrence_id = sha256(LD2(
  "transport-attempt-failure-occurrence-v2", PFX, dispatch_exposure_id, failure_code,
  canonical_failure_digest
))
classification_digest = sha256(LD2(
  "transport-response-classification-intent-v1", PFX, dispatch_exposure_id, response_occurrence_id
))
quarantine_digest = sha256(LD2(
  "late-response-v2", PFX, dispatch_exposure_id, canonical_delivery_identity
))
```

Owner-issued text identities are exactly:

```text
transport_response_receipt_id = trr:v2:<response_occurrence_id>
transport_attempt_failure_receipt_id = tafr:v2:<failure_occurrence_id>
classification_intent_id = trci:v1:<classification_digest>
classification_idempotency_key = classification-intent-v1:<classification_digest>
quarantine_id = lrq:v2:<quarantine_digest>
idempotency_key = late-response-v2:<quarantine_digest>
```

The scope-only v1 formulas and `late-response-v1:<scope_digest>:...` text grammar are superseded. Same logical ids in
another namespace, mode, workspace, or coordination review produce different occurrences.

### 10.1 Golden vectors

All four vectors use:

```text
PFX = ("agent-v1", "scripted", "ws-α", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", 17)
dispatch_exposure_id = "exp:42"
canonical_delivery_identity = "callback/evt:7"
failure_code = "protocol_parse_failed"
canonical_failure_digest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
```

| Vector | Exact encoded bytes (hex) | SHA-256 |
|---|---|---|
| response occurrence | `33323a7472616e73706f72742d726573706f6e73652d6f6363757272656e63652d7632383a6167656e742d7631383a7363726970746564353a77732dceb136343a61616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161323a3137363a6578703a343231343a63616c6c6261636b2f6576743a37` | `7fa6c3b88a30a731cc9cf0e30ed718a717cf4161be190c6ec033b702606a54b6` |
| attempt-failure occurrence | `33393a7472616e73706f72742d617474656d70742d6661696c7572652d6f6363757272656e63652d7632383a6167656e742d7631383a7363726970746564353a77732dceb136343a61616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161323a3137363a6578703a343232313a70726f746f636f6c5f70617273655f6661696c656436343a62626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262626262` | `36a2aa9e22f584098bb489b72831f3795c5830e3873d40698f191c61e903a2e7` |
| classification intent, using the response hash above | `34333a7472616e73706f72742d726573706f6e73652d636c617373696669636174696f6e2d696e74656e742d7631383a6167656e742d7631383a7363726970746564353a77732dceb136343a61616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161323a3137363a6578703a343236343a37666136633362383861333061373331636339636630653330656437313861373137636634313631626531393063366563303333623730323630366135346236` | `6468cb448058ff6cee00a8732fe8638b5f9e1b5ebd5f84bdd17ed4b0a01c07f7` |
| quarantine idempotency | `31363a6c6174652d726573706f6e73652d7632383a6167656e742d7631383a7363726970746564353a77732dceb136343a61616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161616161323a3137363a6578703a343231343a63616c6c6261636b2f6576743a37` | `c607dcbd3972acedea835543d050f29b870e21a86381242cd3f2c04784ad6aeb` |

## 11. Exactly two post-network compositions

No PG transaction crosses DNS, connect, request bytes, response streaming, provider polling, or any network I/O.

### 11.1 Exposure-first evidence UoW — never classifies and never quarantines

```text
dispatch_exposure_lock
-> applicable_transport_receipt_insert_or_exact_replay
-> response_only_classification_intent_create_or_exact_replay
-> dispatch_exposure_terminalization_or_exact_replay
-> commit
```

The classification-intent step is mandatory for a response receipt and skipped for an attempt-failure receipt. This UoW
does not lock OperationRun, plan/review/gate, command, verification intent, or Activity rows. It has zero current/stale
classification authority and zero quarantine permission. It only ensures that a valid response cannot commit without a
durable classification work item. Any classification-intent collision or exposure terminalization failure rolls the
receipt back as well.

### 11.2 Response-classification UoW — stored-state proof only

```text
d3_dispatch_v2
-> operation_root
-> optional_plan_review_gate
-> participating_commands_sorted
-> verification_intent_and_predecessor
-> activity_run_attempt
-> dispatch_exposure_lock
-> transport_response_receipt_exact_replay
-> response_classification_intent_lock
-> optional_response_only_quarantine_insert_or_exact_replay_if_stale
-> response_classification_intent_terminal_CAS
-> dispatch_exposure_terminalization_exact_replay
-> commit
```

Only a transaction-local proof derived from the locked stored rows can choose current or stale. Current completes the
intent as `classified_current` with no quarantine; later normal terminal/record UoWs still perform their own current
business fence. Stale inserts/exact-replays the response-only quarantine then completes `classified_stale`. Missing rows,
lock-budget exhaustion, or an unprovable relation rolls this UoW back. A caller flag, callback label, stale
`ClaimReceipt`, serialized capability, receipt field, or exposure field cannot choose the branch.

Once either composition enters `dispatch_exposure_lock`, it never returns to an earlier aggregate. Neither composition
writes a workflow/domain event, EntityDelta, artifact publication, child command, verification/domain current state, or
send/retry permission. Attempt failure and proven no-call never create a classification intent or quarantine row.

## 12. Mode/transport applicability and explicit deferral

| Mode / transport | Five-surface eligibility | Cost/evidence relation | Decision |
|---|---|---|---|
| `live + model_tool_v1` | eligible only after later migration/runtime/review gates | positive cost exposure; D0f envelope and response/failure evidence required as applicable | decision locked, not activated |
| `simulate + model_tool_v1` | eligible | durable exposure/receipt/classification chain; all money zero | future deterministic validation only |
| `scripted + model_tool_v1` | eligible | durable exposure/receipt/classification chain; all money zero | future deterministic E2E only |
| `replay` | no row in any D3c2h1 table | no current D0 envelope/pricing/schema path | fail closed before writes |
| Harvest/provider-search | ineligible for this model-only variant | may not fabricate model, envelope, pricing, or response fields | deferred to a separately owner-ratified transport variant |

Zero money is not zero evidence. Simulate/scripted use the same immutable identity chain and cannot replay into live.
Thinking Machines Lab/Harvest live work remains blocked from this Track-D evidence path until a later bounded decision
ratifies its request/call/result identity, pricing relation, terminal provenance, receipt variant, and mode isolation.

## 13. Mechanism × ten-invariant matrix — 90 populated cells

| mechanism | 1 owner | 2 tenant | 3 fence | 4 lifecycle | 5 late/partial | 6 cost | 7 physical identity | 8 provenance | 9 consistency | 10 mode isolation |
|---|---|---|---|---|---|---|---|---|---|---|
| verification intent | sole intent repository; §2/§4 | PFX in PK/FKs/CAS; §3/§9 | source 7 + state version; §4 | six states, no revival; §4.3 | 29-field append-once branch; §4.2 | no amount or send authority; §4 | operation/phase unique; §9 | exact event/receipt truth table; §4/§9 | one 52-column manifest; §4 | three model modes only; §12 |
| response receipt | sole transport-evidence repository; §2/§5 | PFX in every identity; §3/§9 | exposure lock + attempt/gen/epoch; §5/§11 | immutable exact replay; §5 | valid late response retained; §11 | binds one committed exposure; §5 | v2 delivery/occurrence unique; §9/§10 | D0f ref/digest FK; §5/§9 | one 30-column manifest; §5 | PFX mode exact; §12 |
| attempt-failure receipt | sole transport-evidence repository; §2/§6 | PFX in every identity; §3/§9 | exposure lock + attempt/gen/epoch; §6/§11 | immutable exact replay; §6 | never quarantine; §6/§11 | conservative exposure evidence; §6 | v2 failure occurrence unique; §9/§10 | registry failure spec only; §6 | one 28-column manifest; §6 | PFX mode exact; §12 |
| response classification intent | sole classification repository; §2/§7 | PFX in PK/unique/FK; §7/§9 | private claimed version capability; §7 | pending/claimed/three terminals; §7 | durable retry/reclaim; §7 | no money mutation; §7 | one per response occurrence; §9/§10 | stored-state proof only; §7/§11 | one 18-column manifest; §7 | cross-mode replay impossible; §12 |
| late quarantine | sole quarantine repository; §2/§8 | PFX in PK/unique/FKs; §8/§9 | stale classification capability only; §11 | orthogonal monotonic axes; §8 | response-only tombstone; §8 | confirmed/uncertain, no no-call; §8 | v2 idempotency + receipt FK; §9/§10 | authorizable always false; §8 | one 41-column manifest; §8 | no non-live/live alias; §12 |
| full-PFX v2 encoders | each domain owner derives its id; §10 | all five PFX components encoded; §10 | canonical bytes reject aliases; §10 | immutable digest forever; §10 | redelivery joins exact row; §10 | one physical call identity; §10 | four golden vectors; §10.1 | NFC/UTF-8/decimal rules; §10 | scope-only v1 superseded; §10 | provider mode changes hash; §10 |
| two post-network UoWs | coordinator owns order, repos own rows; §2/§11 | one exact PFX throughout; §11 | global prefix only for classification; §11 | atomic rollback at tail; §11 | pending intent prevents stranding; §7/§11 | exposure terminalizes through cost owner; §11 | receipt/intent/quarantine chain; §11 | caller classification forbidden; §11 | exactly two compositions; §11 | no cross-mode lookup; §3/§12 |
| D0f envelope relation | D0f sole durable issuer; §2 | PFX-bound ref; §2/§9 | specialized exact replay; D0f | retained-to-tombstone; D0f | evidence never authorizes apply; §2 | exposure ref required; D0f/D3c2g | ref+digest FK; §9 | canonical schema remains one; §2 | no second envelope schema; §2 | live/simulate/scripted only; §12 |
| transport/mode boundary | future variant owner only; §12 | mode is PFX identity; §3 | eligibility before row creation; §12 | replay remains absent; §12 | non-live keeps full chain; §12 | live positive, non-live zero; §12 | model and Harvest cannot alias; §12 | model-only pins never fabricated; §12 | Harvest explicitly deferred; §12 | exact three-mode closure; §12 |

Every one of the 90 invariant cells is populated. No cell claims implementation, rollout, provider readiness, or formal
review approval.

## 14. Executable oracle, non-closure, and next order

`tests/test_d3c2h1_exact_evidence_surface_decision_lock.py` mechanically checks:

- exact 52/30/28/18/41 ordered name/type/null/default manifests;
- the exact source-seven list and 29-row terminal branch truth table;
- all 47 named local CHECK predicates, their `10/9/9/7/12` table split, and same-create-table installation contract;
- owner/store exclusivity, full-PFX PK/unique/FK shapes, and no weak scope-only identity;
- classification-intent lifecycle, retry/reclaim/fail closure, and the two transaction orders;
- v2 length-delimited golden bytes/digests and supersession of the old scope-only formulas;
- fixed DB-clock 30-day quarantine retention and disjoint cost/retention mutation sets;
- model-only three-mode eligibility, replay zero-write, and Harvest/provider-search deferral;
- the complete 9×10 matrix and current physical absence of all five future tables/owners;
- actual D0f durable owner/migration presence at `539c689`, including the exact TIMESTAMPTZ substrate, without claiming
  a Decimal cost substrate.

D3c2h1 closes no migration, repository, runtime, rollout, formal-review, provider, live, W6, manual, product, Migration
A–D, served-action, or residual gate. It does not authorize SQL by itself. The next bounded order is:

1. fresh pinned non-author review of this decision lock;
2. dormant new-table migration(s) plus exact upstream full-PFX keys/FKs, real-PG constraints, rollback, race, and lock
   acceptance;
3. specialized repositories/CAS and the two composition APIs, still with no provider call;
4. strict writers and fake/simulate/scripted E2E;
5. separately reviewed provider-search variant and only then a separately gated bounded live canary.

Local decision-oracle validation uses no provider/model credentials:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3c2h1_exact_evidence_surface_decision_lock.py \
  tests/test_d3c2h0_evidence_cross_contract_ratification.py \
  tests/test_d3c2g_cost_ledger_decision_lock.py \
  tests/test_d0f_model_invocation_envelope_repository.py \
  tests/test_model_invocation_contract.py
.venv/bin/ruff check tests/test_d3c2h1_exact_evidence_surface_decision_lock.py
.venv/bin/ruff format --check tests/test_d3c2h1_exact_evidence_surface_decision_lock.py
git diff --check -- \
  docs/TRACK_D_D3C2H1_EXACT_EVIDENCE_SURFACE_DECISION_LOCK.md \
  tests/test_d3c2h1_exact_evidence_surface_decision_lock.py \
  docs/TRACK_D_AGENT_RUNTIME_PLAN.md
```

Author evidence is not a formal `GO`.
