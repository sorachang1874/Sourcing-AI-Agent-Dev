# Track D D3c2h1 — Exact evidence-surface decision lock

> Status: **decision-lock repair only** (2026-07-15). The first pinned highest-effort non-author review of commit
> `1c4a2d9177dcb3470117700086b12fd533898bb7` returned formal `NO-GO 0/3/3/0`. A later Ultra review attempt of
> `f0a0069c83b7ec582682ad79f7a278e604cdd4a0` produced substantive advisory findings but failed closed as
> `invalid_transport`; it is not formal review evidence. The fresh scope-local advisory against
> `af4db419bddb9b18dd61f6dcca6191bb70f4733c` returned `NO-GO 0/0/1/0`: its sole P2 found stale wording that told a
> future migration to add `workflow_commands.workspace_id` even though migration `0003` already installed it. This
> fixed-forward at `fdb3b792c14fa02e986f99decd3bf510173ea6cf` repairs that installed-parent adoption contract in addition to the executable
> DDL DAG, PostgreSQL identifier limit, attempt-8 access path, nullable timestamp checks, and exact oracle coverage in
> addition to the first six lifecycle/relation/index/check repairs. Its fresh exact-object non-author local advisory is
> `GO 0/0/0/0`, recorded at
> `runtime/reviews/20260715T215450Z_Track_D_D3c2h1_fixed-forward_local_advisory.md`; this is not a formal highest-effort
> `GO`. The batch still adds no SQL, migration, descriptor,
> repository, runtime writer, provider/model/Harvest call, served Agent tool, live activation, or product gate. D0f is
> implemented at `539c689`; the five evidence surfaces and the two D3c2g cost surfaces below remain physically absent.

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
ratified_upstream_constraint_count = 13
ratified_seven_table_constraint_count = 52
ratified_seven_table_fk_count = 29
combined_index_count = 11
combined_access_path_count = 12
deferred_internal_fk_count = 6
forward_ddl_action_count = 17
rollback_ddl_action_count = 16
postgres_identifier_max_bytes = 63
classification_pending_access_path_count = 2
unratified_parent_prerequisite_count = 2
response_occurrence_domain = transport-response-occurrence-v2
failure_occurrence_domain = transport-attempt-failure-occurrence-v2
classification_intent_domain = transport-response-classification-intent-v1
quarantine_idempotency_domain = late-response-v2
quarantine_retention_policy = quarantine_retention_30d_v1
quarantine_retention_deadline = recorded_at + interval '30 days'
exposure_first_quarantine_permission = forbidden
classification_authority = stored_current_state_under_d3_dispatch_v2_global_lock_prefix
classification_nonterminal_states = pending | claimed | current_pending_apply
classification_terminal_states = applied_current | classified_stale | failed_terminal
classification_max_attempts = 8
post_network_ingress_composition_count = 2
current_apply_continuation_count = 1
migration_authority = blocked_pending_parent_decision_and_fresh_pinned_go
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
| cost_reservations + dispatch_exposures | `src/sourcing_agent/repositories/cost_ledger.py::CostLedgerRepository` | `store.repos.cost_ledger` | future inseparable D3c2g 21/75-column reservation/exposure aggregate | typed reservation/exposure CAS, locked-prestate terminalize-or-exact-validate, credential-free settlement | receipt/quarantine SQL, caller money/evidence, rewriting an immutable terminal exposure |
| post-network composition | future `D3PostNetworkEvidenceCoordinator`, a transaction composition seam only | no `store.repos` entry and no table | two ingress orders plus one recoverable current-apply continuation over the typed repositories | compose §11, pass only transaction-local stored-state proof, and invoke typed owners on one shared PG transaction | direct SQL, second ownership, network I/O in a PG transaction, serialized classification authority, caller-selected branch |

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
result-artifact field and can never create quarantine. One exposure may have one failure occurrence. If a terminal-
disposition failure has already made the exposure terminal, a later valid response may still create a distinct response
receipt and classification intent, but the immutable failure terminal/cost record is only exact-validated and never
rewritten. The global stored-state proof can then reach only `classified_stale` plus response quarantine; it can never
reach `current_pending_apply` or domain apply.

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
`pending|claimed|current_pending_apply|applied_current|classified_stale|failed_terminal`.
`pending|claimed|current_pending_apply` are nonterminal; `applied_current|classified_stale|failed_terminal` are terminal
and never reopen. The fixed checked-in
`response-classification-retry-v1` rule is not a tenant policy ladder: claim lease = 30 seconds, retry delay =
`min(2 ** (attempt_count - 1), 60)` seconds after a failed claimed attempt, and maximum attempts = 8. A future policy
change requires a new table/schema decision after all nonterminal v1 rows drain; callers cannot select it.

| Method | Exact transition/effect |
|---|---|
| `create_or_exact_replay_pending` | absent -> `pending`; DB clock sets `next_attempt_at`; exact receipt/PFX/occurrence replay joins |
| `claim_due` | due `pending` with `attempt_count < 8` -> `claimed`; attempt +1, version +1, DB clock sets the 30-second lease deadline; return a private non-serializable claim capability |
| `retry_claim` | current `claimed` at attempt 1..7 plus registered transient error -> `pending` with fixed DB-clock delay/version +1; attempt 8 -> `failed_terminal` with `classification_retry_exhausted` |
| `reclaim_expired_claim` | expired `claimed` at attempt 1..7 -> `pending` with `claim_lease_expired`, DB-clock due time/version +1; attempt 8 -> `failed_terminal` with `classification_claim_lease_exhausted` |
| `converge_exhausted_pending` | owner-only recovery scan over the admitted pending-state index for `pending, attempt_count=8`; oldest-first `SKIP LOCKED`, exact PFX + classification id + state version -> `failed_terminal` with `classification_retry_exhausted`; it is never claimable |
| `mark_current_pending_apply` | current claimed capability + locked stored-current proof -> `current_pending_apply`; set DB-clock `next_attempt_at`, terminal remains NULL, version +1; no domain write and no quarantine |
| `complete_stale_with_quarantine` | current claimed capability + global stored-state proof -> `classified_stale`; same UoW inserts/exact-replays quarantine, sets terminal DB clock, version +1 |
| `complete_current_apply` | `current_pending_apply` + fresh global stored-current proof -> `applied_current`; the same normal terminal/record UoW commits domain/attempt/command/event/source writes before this terminal CAS, then sets terminal DB clock/version +1 |
| `reclassify_current_pending_stale_with_quarantine` | `current_pending_apply` + fresh global stored-stale proof -> `classified_stale`; same UoW inserts/exact-replays quarantine, writes zero domain state, and sets terminal DB clock/version +1 |
| `fail_terminal` | current `claimed` plus registered permanent/non-provable error at any attempt, or registered attempt-8 transient/lease exhaustion -> `failed_terminal`; set terminal DB clock/version +1; authorizes neither apply nor quarantine |

The private claim capability binds full PFX, classification id, receipt/exposure/occurrence, claimed state version,
attempt count, and DB lease deadline. It is not stored, serialized, or reconstructed from ids. A failed global lock or
unprovable classification rolls back the classification UoW first; only then may the classification owner perform its
own row-only retry/fail CAS. That CAS records no current/stale label and grants no result authority.

`claim_due` and `converge_exhausted_pending` are the two admitted readers of the same pending-state partial index.
`claim_due` filters `attempt_count < 8` plus DB-clock due time; defensive convergence filters `attempt_count = 8` and
does not wait for a new claim. Both use deterministic `(next_attempt_at, classification_intent_id)` order and bounded
`FOR UPDATE SKIP LOCKED`. No reader repair, heap-wide scan, or hidden broader index is allowed.

The attempt boundary is total and is a second machine-readable authority:

| Source state / attempt | Owner-observed outcome | Exact target/effect |
|---|---|---|
| `pending / 0..7` | due claim | `claimed / 1..8`; never increments above 8 |
| `pending / 8` | defensive convergence | `failed_terminal / classification_retry_exhausted` |
| `claimed / 1..7` | registered transient error | `pending / same attempt`; fixed retry delay |
| `claimed / 8` | registered transient error | `failed_terminal / classification_retry_exhausted` |
| `claimed / 1..7` | lease expired | `pending / same attempt`; `claim_lease_expired` |
| `claimed / 8` | lease expired | `failed_terminal / classification_claim_lease_exhausted` |
| `claimed / 1..8` | permanent or non-provable error | `failed_terminal / exact registered error` |
| `claimed / 1..8` | stored current | `current_pending_apply / same attempt`; no terminal/domain write |
| `claimed / 1..8` | stored stale | `classified_stale`; quarantine and terminal CAS in one UoW |
| `current_pending_apply / 1..8` | fresh stored current | normal terminal/record apply + `applied_current` in one UoW |
| `current_pending_apply / 1..8` | fresh stored stale | zero domain write + quarantine + `classified_stale` in one UoW |
| any terminal / 0..8 | replay or mismatch | exact replay is zero-write; mismatch fails closed; never reopens |

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

## 9. Exact combined ratified-schema relations, indexes, and rollback boundary

This is one combined relation boundary for D3c2g's two cost tables and D3c2h1's five evidence tables. Every `PFX`
below expands to the five columns in §3, in order. For the review-session parent only, the child
`coordination_plan_review_id` maps to the parent's canonical `review_id`; no duplicate parent column is invented.

All ordinary FKs use this exact action contract:

```text
FK_STD := MATCH SIMPLE DEFERRABLE INITIALLY IMMEDIATE ON UPDATE RESTRICT ON DELETE RESTRICT
FK_CYCLE := MATCH SIMPLE DEFERRABLE INITIALLY DEFERRED ON UPDATE RESTRICT ON DELETE RESTRICT
```

`FK_CYCLE` is used only by the two nullable exposure-to-receipt reverse links. Their local terminal-shape checks require
the link tuple to be all NULL or complete, and the deferred target must exist by commit. All other FKs use `FK_STD`.

### 9.1 Ratified upstream parent constraints — exactly 13

These are required additions to already-physical parents, except the already-present D0f envelope unique. They do not
activate a strict-D3 row or reinterpret a brownfield sentinel.

| Order | Name | Kind | Exact columns / target |
|---:|---|---|---|
| 1 | `plan_review_sessions_d3_scope_review_uk` | UNIQUE | `(runtime_namespace, provider_mode, workspace_id, scope_digest, review_id)` |
| 2 | `operation_runs_d3_scope_operation_uk` | UNIQUE | `(PFX, operation_run_id)` |
| 3 | `operation_runs_d3_review_fk` | FK_STD | operation `PFX` -> session `(runtime_namespace, provider_mode, workspace_id, scope_digest, review_id)` |
| 4 | `workflow_commands_d3_scope_operation_command_uk` | UNIQUE | `(PFX, operation_id, command_id)` |
| 5 | `workflow_commands_d3_operation_fk` | FK_STD | `(PFX, operation_id)` -> `operation_runs(PFX, operation_run_id)` |
| 6 | `workflow_activity_runs_d3_scope_operation_command_run_uk` | UNIQUE | `(PFX, operation_run_id, command_id, activity_run_id)` |
| 7 | `workflow_activity_runs_d3_command_fk` | FK_STD | `(PFX, operation_run_id, command_id)` -> `workflow_commands(PFX, operation_id, command_id)` |
| 8 | `workflow_activity_attempts_d3_scope_op_cmd_run_attempt_uk` | UNIQUE | `(PFX, operation_run_id, command_id, activity_run_id, attempt_id)` |
| 9 | `workflow_activity_attempts_d3_run_fk` | FK_STD | `(PFX, operation_run_id, command_id, activity_run_id)` -> `workflow_activity_runs(PFX, operation_run_id, command_id, activity_run_id)` |
| 10 | `workflow_events_d3_scope_event_uk` | UNIQUE | `(PFX, event_id)` |
| 11 | `workflow_events_d3_terminal_event_uk` | UNIQUE | `(PFX, operation_id, command_id, event_id, terminal_outcome_digest)` |
| 12 | `workflow_events_d3_attempt_fk` | FK_STD | `(PFX, operation_id, command_id, activity_run_id, activity_attempt_id)` -> `workflow_activity_attempts(PFX, operation_run_id, command_id, activity_run_id, attempt_id)` |
| 13 | `model_invocation_envelopes_ref_digest_uk` | existing UNIQUE | `(PFX, model_invocation_envelope_ref, envelope_digest)` |

Migration `0003_workflow_command_claim_fence_foundation.sql` already installs D3b's ratified
`workflow_commands.workspace_id` as `TEXT DEFAULT '' NOT NULL` with the named
`workflow_commands_workspace_id_shape_ck` constraint `NOT VALID`. The future combined migration must adopt and
validate that existing column/check, preserve empty-string brownfield sentinels, and must not add, drop, rewrite, or
reinterpret it. It then validates the complete full-PFX strict-row chain, builds these uniques without weakening column
order, and only then attaches the FKs.

### 9.2 Ratified seven-table constraints — exactly 52, including 29 FKs

| Order | Name | Kind | Exact child columns / target |
|---:|---|---|---|
| 1 | `cost_reservations_pkey` | PK | `(PFX, budget_reservation_ref)` |
| 2 | `cost_reservations_operation_idempotency_uk` | UNIQUE | `(PFX, operation_run_id, reservation_idempotency_key)` |
| 3 | `cost_reservations_review_fk` | FK_STD | `PFX` -> session scope/review unique |
| 4 | `cost_reservations_operation_fk` | FK_STD | `(PFX, operation_run_id)` -> `operation_runs(PFX, operation_run_id)` |
| 5 | `dispatch_exposures_pkey` | PK | `(PFX, dispatch_exposure_id)` |
| 6 | `dispatch_exposures_physical_call_uk` | UNIQUE | `(PFX, budget_reservation_ref, activity_attempt_id, physical_call_index)` |
| 7 | `dispatch_exposures_reservation_fk` | FK_STD | `(PFX, budget_reservation_ref)` -> `cost_reservations(PFX, budget_reservation_ref)` |
| 8 | `dispatch_exposures_review_fk` | FK_STD | `PFX` -> session scope/review unique |
| 9 | `dispatch_exposures_operation_fk` | FK_STD | `(PFX, operation_run_id)` -> operation unique |
| 10 | `dispatch_exposures_command_fk` | FK_STD | `(PFX, operation_run_id, command_id)` -> command unique |
| 11 | `dispatch_exposures_activity_run_fk` | FK_STD | `(PFX, operation_run_id, command_id, activity_run_id)` -> activity-run unique |
| 12 | `dispatch_exposures_activity_attempt_fk` | FK_STD | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` -> activity-attempt unique |
| 13 | `dispatch_exposures_base_intent_fk` | FK_STD | `(PFX, operation_run_id, base_intent_id, base_intent_phase_generation)` -> verification exposure-parent unique |
| 14 | `dispatch_exposures_predecessor_intent_fk` | FK_STD | `(PFX, operation_run_id, expected_predecessor_intent_id, expected_predecessor_phase_generation)` -> verification exposure-parent unique |
| 15 | `dispatch_exposures_decision_event_fk` | FK_STD | `(PFX, decision_source_event_id)` -> workflow-event scope unique |
| 16 | `dispatch_exposures_predecessor_event_fk` | FK_STD | `(PFX, expected_predecessor_decision_source_event_id)` -> workflow-event scope unique |
| 17 | `dispatch_exposures_response_receipt_fk` | FK_CYCLE | `(PFX, dispatch_exposure_id, transport_response_receipt_id)` -> response exposure-ref unique |
| 18 | `dispatch_exposures_failure_receipt_fk` | FK_CYCLE | `(PFX, dispatch_exposure_id, transport_attempt_failure_receipt_id)` -> failure exposure-ref unique |
| 19 | `verification_intents_pkey` | PK | `(PFX, intent_id)` |
| 20 | `verification_intents_operation_phase_uk` | UNIQUE | `(PFX, operation_run_id, phase_generation)` |
| 21 | `verification_intents_exposure_parent_uk` | UNIQUE | `(PFX, operation_run_id, intent_id, phase_generation)` |
| 22 | `verification_intents_operation_fk` | FK_STD | `(PFX, operation_run_id)` -> operation unique |
| 23 | `verification_intents_source_attempt_fk` | FK_STD | `(PFX, operation_run_id, source_verification_command_id, source_activity_run_id, source_activity_attempt_id)` -> activity-attempt unique |
| 24 | `verification_intents_source_event_fk` | FK_STD | `(PFX, operation_run_id, source_verification_command_id, expected_source_terminal_event_id, expected_source_terminal_outcome_digest)` -> workflow terminal-event unique |
| 25 | `verification_intents_response_receipt_fk` | FK_STD | `(PFX, expected_source_dispatch_exposure_id, expected_source_response_occurrence_id, expected_source_transport_response_receipt_id)` -> response child-FK unique |
| 26 | `verification_intents_failure_receipt_fk` | FK_STD | `(PFX, expected_source_dispatch_exposure_id, expected_source_failure_occurrence_id, expected_source_transport_attempt_failure_receipt_id)` -> failure child-FK unique |
| 27 | `verification_intents_recorded_event_fk` | FK_STD | `(PFX, recorded_event_id)` -> workflow-event scope unique |
| 28 | `transport_response_receipts_pkey` | PK | `(PFX, transport_response_receipt_id)` |
| 29 | `transport_response_receipts_delivery_uk` | UNIQUE | `(PFX, dispatch_exposure_id, canonical_delivery_identity)` |
| 30 | `transport_response_receipts_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id)` |
| 31 | `transport_response_receipts_exposure_ref_uk` | UNIQUE | `(PFX, dispatch_exposure_id, transport_response_receipt_id)` |
| 32 | `transport_response_receipts_child_fk_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id, transport_response_receipt_id)` |
| 33 | `transport_response_receipts_exposure_fk` | FK_STD | `(PFX, dispatch_exposure_id)` -> dispatch exposure identity |
| 34 | `transport_response_receipts_attempt_fk` | FK_STD | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` -> activity-attempt unique |
| 35 | `transport_response_receipts_envelope_fk` | FK_STD | `(PFX, model_invocation_envelope_ref, model_invocation_envelope_digest)` -> envelope ref/digest unique |
| 36 | `transport_attempt_failure_receipts_pkey` | PK | `(PFX, transport_attempt_failure_receipt_id)` |
| 37 | `transport_attempt_failure_receipts_exposure_uk` | UNIQUE | `(PFX, dispatch_exposure_id)` |
| 38 | `transport_attempt_failure_receipts_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, failure_occurrence_id)` |
| 39 | `transport_attempt_failure_receipts_exposure_ref_uk` | UNIQUE | `(PFX, dispatch_exposure_id, transport_attempt_failure_receipt_id)` |
| 40 | `transport_attempt_failure_receipts_child_fk_uk` | UNIQUE | `(PFX, dispatch_exposure_id, failure_occurrence_id, transport_attempt_failure_receipt_id)` |
| 41 | `transport_attempt_failure_receipts_exposure_fk` | FK_STD | `(PFX, dispatch_exposure_id)` -> dispatch exposure identity |
| 42 | `transport_attempt_failure_receipts_attempt_fk` | FK_STD | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` -> activity-attempt unique |
| 43 | `transport_response_classification_intents_pkey` | PK | `(PFX, classification_intent_id)` |
| 44 | `transport_response_classification_intents_idempotency_uk` | UNIQUE | `(PFX, classification_idempotency_key)` |
| 45 | `transport_response_classification_intents_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id)` |
| 46 | `transport_response_classification_intents_receipt_fk` | FK_STD | `(PFX, dispatch_exposure_id, response_occurrence_id, transport_response_receipt_id)` -> response child-FK unique |
| 47 | `workflow_late_result_quarantine_pkey` | PK | `(PFX, quarantine_id)` |
| 48 | `workflow_late_result_quarantine_idempotency_uk` | UNIQUE | `(PFX, idempotency_key)` |
| 49 | `workflow_late_result_quarantine_occurrence_uk` | UNIQUE | `(PFX, dispatch_exposure_id, response_occurrence_id)` |
| 50 | `workflow_late_result_quarantine_receipt_fk` | FK_STD | `(PFX, dispatch_exposure_id, response_occurrence_id, transport_response_receipt_id)` -> response child-FK unique |
| 51 | `workflow_late_result_quarantine_classification_fk` | FK_STD | `(PFX, dispatch_exposure_id, response_occurrence_id)` -> classification occurrence unique |
| 52 | `workflow_late_result_quarantine_envelope_fk` | FK_STD | `(PFX, model_invocation_envelope_ref, model_invocation_envelope_digest)` -> envelope ref/digest unique |

The two nullable intent tuples on an exposure are each all NULL or complete under D3c2g's local checks. A terminal
exposure's response/failure reverse tuple is likewise all NULL or complete and immutable after the first terminal CAS.

### 9.3 Exact combined index inventory and access paths — exactly 11

| Order | Name | Table | Exact ordered columns and predicate |
|---:|---|---|---|
| 1 | `cost_reservations_scope_operation_state_idx` | `cost_reservations` | `(PFX, operation_run_id, reservation_state)` |
| 2 | `dispatch_exposures_parent_settlement_idx` | `dispatch_exposures` | `(PFX, budget_reservation_ref, parent_settlement_state, dispatch_exposure_id)` |
| 3 | `dispatch_exposures_scope_attempt_call_idx` | `dispatch_exposures` | `(PFX, activity_attempt_id, physical_call_index)` |
| 4 | `verification_intents_source_attempt_idx` | `verification_intents` | `(PFX, operation_run_id, source_verification_command_id, source_activity_run_id, source_activity_attempt_id)` |
| 5 | `transport_response_receipts_attempt_idx` | `transport_response_receipts` | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` |
| 6 | `transport_attempt_failure_receipts_attempt_idx` | `transport_attempt_failure_receipts` | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` |
| 7 | `transport_response_classification_intents_pending_state_idx` | `transport_response_classification_intents` | `(PFX, next_attempt_at, classification_intent_id) WHERE classification_state = 'pending'` |
| 8 | `transport_response_classification_intents_claimed_expiry_idx` | `transport_response_classification_intents` | `(PFX, next_attempt_at, classification_intent_id) WHERE classification_state = 'claimed'` |
| 9 | `transport_response_classification_intents_current_apply_due_idx` | `transport_response_classification_intents` | `(PFX, next_attempt_at, classification_intent_id) WHERE classification_state = 'current_pending_apply'` |
| 10 | `workflow_late_result_quarantine_pending_cost_idx` | `workflow_late_result_quarantine` | `(PFX, recorded_at, quarantine_id) WHERE cost_state = 'pending_reconciliation'` |
| 11 | `workflow_late_result_quarantine_retention_idx` | `workflow_late_result_quarantine` | `(PFX, retention_until, quarantine_id) WHERE retention_state = 'retained'` |

| Access path | Sole exact index |
|---|---|
| reservation state lookup and close | `cost_reservations_scope_operation_state_idx` |
| child settlement scan in deterministic exposure order | `dispatch_exposures_parent_settlement_idx` |
| exposure lookup by physical ActivityAttempt call | `dispatch_exposures_scope_attempt_call_idx` |
| verification lookup by immutable source ActivityAttempt | `verification_intents_source_attempt_idx` |
| response receipt audit by ActivityAttempt | `transport_response_receipts_attempt_idx` |
| failure receipt audit by ActivityAttempt | `transport_attempt_failure_receipts_attempt_idx` |
| `claim_due` oldest-due pending work with `attempt_count < 8` | `transport_response_classification_intents_pending_state_idx` |
| `converge_exhausted_pending` oldest pending work with `attempt_count = 8` | `transport_response_classification_intents_pending_state_idx` |
| `reclaim_expired_claim` oldest expired lease | `transport_response_classification_intents_claimed_expiry_idx` |
| current-apply continuation oldest-due work | `transport_response_classification_intents_current_apply_due_idx` |
| quarantine cost reconciliation oldest-first | `workflow_late_result_quarantine_pending_cost_idx` |
| quarantine purge deadline oldest-first | `workflow_late_result_quarantine_retention_idx` |

No owner lookup may rely on a broader hidden scan. The pending-state index intentionally has two admitted access paths;
the other ten indexes each have one. Real-PG acceptance must verify index names, ordered columns, predicates, both
pending filters, eligible-row plans, bounded locks, and `SKIP LOCKED` recovery behavior.

### 9.4 Two unresolved parent-owner prerequisites and exact DDL order

| Blocker | Plan/OB owner | Missing decision | Required closure |
|---|---|---|---|
| typed plan/review/gate parent | Plan §6 item 6; R-019 | physical parent table or tables, typed key columns, and scope-aware unique target for exposure columns 19–25 are not ratified | separate owner decision lock plus pinned non-author `GO` |
| Tier-2 grant parent | OB-10.2; Plan §6 item 7 | physical grant table, complete grant key, lifecycle, and scope-aware unique target for exposure columns 40–43 are not ratified | separate owner decision lock plus pinned non-author `GO` |

No placeholder FK, JSON comparison, unscoped parent, nullable waiver, or application-only assertion is allowed. These
two blockers mean this repair **does not authorize a dormant migration**, even if every relation above receives `GO`.

Once both parent decisions and this repaired decision lock have matching pinned `GO` artifacts, the future migration must
use the following exact forward create/attach DAG. A constraint listed as "attach later" is omitted from its table's
`CREATE TABLE` and installed as an immediately valid `ALTER TABLE ... ADD CONSTRAINT` only after every referenced table
and unique target exists. `FK_STD` versus `FK_CYCLE` still owns validation timing; inline versus attach-later is a separate
dependency-topology decision.

#### 9.4.1 Exact forward create/attach DAG — 17 actions

| Order | Operation | Object | Requires present | Exact constraint scope / effect |
|---:|---|---|---|---|
| 1 | `adopt_validate_attach` | strict upstream parents | none | §9.1 #1-13; preserve adopted parent rows |
| 2 | `create_table` | `cost_reservations` | strict upstream parents | §9.2 #1-4 inline |
| 3 | `create_table` | `verification_intents` | strict upstream parents | §9.2 #19-24 and #27 inline; #25-26 attach later |
| 4 | `create_table` | `dispatch_exposures` | strict upstream parents, `cost_reservations` | §9.2 #5-12 and #15-16 inline; #13-14 and #17-18 attach later |
| 5 | `create_table` | `transport_response_receipts` | strict upstream parents, `dispatch_exposures` | §9.2 #28-35 inline |
| 6 | `create_table` | `transport_attempt_failure_receipts` | strict upstream parents, `dispatch_exposures` | §9.2 #36-42 inline |
| 7 | `create_table` | `transport_response_classification_intents` | `transport_response_receipts` | §9.2 #43-46 inline |
| 8 | `create_table` | `workflow_late_result_quarantine` | strict upstream parents, `transport_response_receipts`, `transport_response_classification_intents` | §9.2 #47-52 inline |
| 9 | `attach_fk` | `dispatch_exposures_base_intent_fk` | `dispatch_exposures`, `verification_intents` | §9.2 #13 as valid `FK_STD` |
| 10 | `attach_fk` | `dispatch_exposures_predecessor_intent_fk` | `dispatch_exposures`, `verification_intents` | §9.2 #14 as valid `FK_STD` |
| 11 | `attach_fk` | `verification_intents_response_receipt_fk` | `verification_intents`, `transport_response_receipts` | §9.2 #25 as valid `FK_STD` |
| 12 | `attach_fk` | `verification_intents_failure_receipt_fk` | `verification_intents`, `transport_attempt_failure_receipts` | §9.2 #26 as valid `FK_STD` |
| 13 | `attach_fk` | `dispatch_exposures_response_receipt_fk` | `dispatch_exposures`, `transport_response_receipts` | §9.2 #17 as valid `FK_CYCLE` |
| 14 | `attach_fk` | `dispatch_exposures_failure_receipt_fk` | `dispatch_exposures`, `transport_attempt_failure_receipts` | §9.2 #18 as valid `FK_CYCLE` |
| 15 | `attach_separately_ratified_fk_set` | `dispatch_exposures` | typed plan/review/gate parent, Tier-2 grant parent | only exact names and targets ratified by their separate pinned `GO`; this document supplies none |
| 16 | `create_indexes` | all seven future tables | all seven tables, six attached internal FKs, separately ratified parent FKs | §9.3 indexes #1-11 in listed order |
| 17 | `validate_acceptance` | combined D3 evidence schema | all prior actions | real-PG constraint, identifier, rollback, race, plan, bounded-lock, and `SKIP LOCKED` acceptance |

The order is executable without a forward reference: `verification_intents` exists before the two exposure-to-intent
links are attached; both receipt tables exist before the two intent-to-receipt and two exposure-to-receipt links are
attached. The two separately owned parent sets remain symbolic blockers only—this document neither names nor guesses
their tables, keys, unique targets, or FK names.

#### 9.4.2 Exact rollback dependency order — 16 actions

| Order | Operation | Object / exact effect |
|---:|---|---|
| 1 | `drop_indexes` | §9.3 indexes #11 through #1 |
| 2 | `detach_separately_ratified_fk_set` | reverse the separately reviewed parent-FK order without changing parent rows |
| 3 | `detach_fk` | `dispatch_exposures_failure_receipt_fk` |
| 4 | `detach_fk` | `dispatch_exposures_response_receipt_fk` |
| 5 | `detach_fk` | `verification_intents_failure_receipt_fk` |
| 6 | `detach_fk` | `verification_intents_response_receipt_fk` |
| 7 | `detach_fk` | `dispatch_exposures_predecessor_intent_fk` |
| 8 | `detach_fk` | `dispatch_exposures_base_intent_fk` |
| 9 | `drop_table` | `workflow_late_result_quarantine` |
| 10 | `drop_table` | `transport_response_classification_intents` |
| 11 | `drop_table` | `transport_attempt_failure_receipts` |
| 12 | `drop_table` | `transport_response_receipts` |
| 13 | `drop_table` | `dispatch_exposures` |
| 14 | `drop_table` | `verification_intents` |
| 15 | `drop_table` | `cost_reservations` |
| 16 | `detach_drop_if_created` | §9.1 #13 through #1; preserve all adopted upstream parent data |

Rollback performs these actions in exactly this dependency order. It never uses `CASCADE`, never drops an upstream
parent table, and never removes or rewrites adopted upstream parent data.

### 9.5 Exact local CHECK inventory — 47 constraints

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
ARTIFACT_PAIR(ref, digest) := ((ref IS NULL AND digest IS NULL) OR (NB(ref) AND SHA(digest))) IS TRUE
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
| 10 | `verification_intents_timestamp_ck` | `verification_intents` | `(updated_at >= created_at AND ((expected_source_terminal_status IS NULL AND source_terminal_appended_at IS NULL) OR (expected_source_terminal_status IS NOT NULL AND source_terminal_appended_at IS NOT NULL AND source_terminal_appended_at >= created_at))) IS TRUE` |
| 11 | `transport_response_receipts_pfx_ck` | `transport_response_receipts` | `PFX_VALID` |
| 12 | `transport_response_receipts_identity_ck` | `transport_response_receipts` | `NB(transport_response_receipt_id) AND NB(operation_run_id) AND NB(command_id) AND NB(activity_run_id) AND NB(activity_attempt_id) AND NB(dispatch_exposure_id) AND NB(canonical_delivery_identity)` |
| 13 | `transport_response_receipts_counter_ck` | `transport_response_receipts` | `command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0 AND physical_call_index >= 0` |
| 14 | `transport_response_receipts_digest_ck` | `transport_response_receipts` | `SHA(claim_authority_spec_digest) AND SHA(d3_business_fence_digest) AND SHA(terminal_provenance_policy_digest) AND SHA(response_spec_digest) AND SHA(model_invocation_envelope_digest) AND SHA(response_occurrence_id) AND SHA(canonical_response_digest) AND SHA(canonical_result_digest) AND OPT_SHA(result_artifact_digest)` |
| 15 | `transport_response_receipts_provider_ck` | `transport_response_receipts` | `((provider_call_id_state = 'present' AND NB(provider_call_id)) OR (provider_call_id_state = 'missing_by_registered_transport' AND provider_call_id IS NULL)) IS TRUE` |
| 16 | `transport_response_receipts_envelope_ref_ck` | `transport_response_receipts` | `model_invocation_envelope_ref ~ '^mie:v1:[0-9a-f]{64}:[0-9a-f]{64}$'` |
| 17 | `transport_response_receipts_terminal_reason_ck` | `transport_response_receipts` | `terminal_reason IN ('end_turn', 'tool_calls', 'length', 'content_filter')` |
| 18 | `transport_response_receipts_occurrence_ck` | `transport_response_receipts` | `transport_response_receipt_id = concat('trr:v2:', response_occurrence_id)` |
| 19 | `transport_response_receipts_artifact_pair_ck` | `transport_response_receipts` | `ARTIFACT_PAIR(result_artifact_ref, result_artifact_digest)` |
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
| 32 | `transport_response_classification_intents_state_ck` | `transport_response_classification_intents` | `classification_state IN ('pending', 'claimed', 'current_pending_apply', 'applied_current', 'classified_stale', 'failed_terminal')` |
| 33 | `transport_response_classification_intents_counter_ck` | `transport_response_classification_intents` | `attempt_count >= 0 AND attempt_count <= 8 AND state_version >= 0` |
| 34 | `transport_response_classification_intents_error_ck` | `transport_response_classification_intents` | `OPT_NB(last_error_code)` |
| 35 | `transport_response_classification_intents_timestamp_ck` | `transport_response_classification_intents` | `(updated_at >= created_at AND next_attempt_at >= created_at AND ((classification_state IN ('pending', 'claimed', 'current_pending_apply') AND terminal_at IS NULL) OR (classification_state IN ('applied_current', 'classified_stale', 'failed_terminal') AND terminal_at IS NOT NULL AND terminal_at >= created_at))) IS TRUE` |
| 36 | `workflow_late_result_quarantine_pfx_ck` | `workflow_late_result_quarantine` | `PFX_VALID` |
| 37 | `workflow_late_result_quarantine_identity_ck` | `workflow_late_result_quarantine` | `quarantine_id ~ '^lrq:v2:[0-9a-f]{64}$' AND NB(operation_run_id) AND NB(command_id) AND NB(activity_run_id) AND NB(activity_attempt_id) AND NB(dispatch_exposure_id) AND transport_response_receipt_id ~ '^trr:v2:[0-9a-f]{64}$' AND NB(canonical_delivery_identity) AND idempotency_key ~ '^late-response-v2:[0-9a-f]{64}$'` |
| 38 | `workflow_late_result_quarantine_counter_ck` | `workflow_late_result_quarantine` | `command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0 AND physical_call_index >= 0 AND cost_state_version >= 0 AND retention_state_version >= 0` |
| 39 | `workflow_late_result_quarantine_digest_ck` | `workflow_late_result_quarantine` | `SHA(claim_authority_spec_digest) AND SHA(d3_business_fence_digest) AND SHA(response_occurrence_id) AND SHA(model_invocation_envelope_digest) AND SHA(canonical_response_digest) AND SHA(canonical_result_digest) AND OPT_SHA(result_artifact_digest)` |
| 40 | `workflow_late_result_quarantine_provider_ck` | `workflow_late_result_quarantine` | `((provider_call_id_state = 'present' AND NB(provider_call_id)) OR (provider_call_id_state = 'missing_by_registered_transport' AND provider_call_id IS NULL)) IS TRUE` |
| 41 | `workflow_late_result_quarantine_artifact_retention_ck` | `workflow_late_result_quarantine` | `((retention_state = 'retained' AND ARTIFACT_PAIR(result_artifact_ref, result_artifact_digest)) OR (retention_state = 'purged_tombstone' AND result_artifact_ref IS NULL AND OPT_SHA(result_artifact_digest))) IS TRUE` |
| 42 | `workflow_late_result_quarantine_state_ck` | `workflow_late_result_quarantine` | `cost_state IN ('pending_reconciliation', 'reconciled_confirmed', 'reconciled_uncertain') AND retention_state IN ('retained', 'purged_tombstone')` |
| 43 | `workflow_late_result_quarantine_authorizable_ck` | `workflow_late_result_quarantine` | `authorizable = false` |
| 44 | `workflow_late_result_quarantine_rejection_ck` | `workflow_late_result_quarantine` | `rejection_reason IN ('stale_claim', 'business_precondition_conflict')` |
| 45 | `workflow_late_result_quarantine_idempotency_ck` | `workflow_late_result_quarantine` | `substring(quarantine_id from 8) = substring(idempotency_key from 18)` |
| 46 | `workflow_late_result_quarantine_retention_ck` | `workflow_late_result_quarantine` | `retention_policy_version = 'quarantine_retention_30d_v1' AND retention_until = recorded_at + interval '30 days'` |
| 47 | `workflow_late_result_quarantine_timestamp_ck` | `workflow_late_result_quarantine` | `(((cost_state = 'pending_reconciliation' AND cost_reconciled_at IS NULL) OR (cost_state IN ('reconciled_confirmed', 'reconciled_uncertain') AND cost_reconciled_at IS NOT NULL AND cost_reconciled_at >= recorded_at)) AND ((retention_state = 'retained' AND purged_at IS NULL) OR (retention_state = 'purged_tombstone' AND purged_at IS NOT NULL AND purged_at >= retention_until))) IS TRUE` |

All 47 constraints must be installed as valid `CHECK` constraints in the same future `CREATE TABLE` batch as their
table; there may be no brownfield sentinels and no later `NOT VALID` reinterpretation. Every predicate that can touch a
nullable timestamp must make the required branch timestamp explicitly `IS NOT NULL` and wrap the whole predicate in
`IS TRUE`; PostgreSQL `CHECK` acceptance of `UNKNOWN` is never a valid state. Registry applicability,
parent-row equality, exact SHA recomputation, state CAS, and DB-clock eligibility remain repository/FK acceptance
because a local `CHECK` cannot truthfully prove another row, recompute owner registry semantics, or authorize a
transition.

Sections 9.1–9.4, not an implementation author's inference, own the complete currently-ratified relation, index, parent,
and creation-order boundary. A later migration may not weaken it to id-only, scope-digest-only, JSON, or
application-only checks. All new-table shape checks must be installed valid at creation: nonblank ids,
positive/nonnegative counters, lowercase digest shapes, provider-id and artifact-pair truth tables, exact terminal
branch truth table, exact state enums, and DB-clock timestamp relations.

Any collision, FK/PFX/attempt mismatch, invalid state transition, optional quarantine failure, classification terminal
CAS failure, or exposure terminalization mismatch rolls back every mutation in that UoW. Generic replace-all upsert is
forbidden for all five surfaces.

Every exact constraint and index identifier in §§9.1–9.3 and §9.5 is UTF-8 byte-counted against PostgreSQL's 63-byte
identifier limit. The oracle rejects an over-limit name, any duplicate exact name, and any collision after a defensive
63-byte prefix projection; relying on PostgreSQL's silent identifier truncation is forbidden.

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

## 11. Two post-network ingress UoWs plus one recoverable current-apply continuation

No PG transaction crosses DNS, connect, request bytes, response streaming, provider polling, or any network I/O.

### 11.1 Exposure-first evidence ingress — never classifies and never quarantines

```text
dispatch_exposure_lock
-> applicable_transport_receipt_insert_or_exact_replay
-> response_only_classification_intent_create_or_exact_replay
-> exposure_terminalize_if_nonterminal_or_exact_validate_terminal_unchanged
-> commit
```

The classification-intent step is mandatory for a response receipt and skipped for an attempt-failure receipt. This UoW
does not lock OperationRun, plan/review/gate, command, verification intent, Activity, or domain rows. It has zero
current/stale classification authority and zero quarantine permission. A valid response cannot commit without a durable
classification work item. Any receipt/intent collision or cost-owner terminal validation failure rolls back every write.

For a nonterminal exposure, the cost owner installs the applicable immutable response- or failure-backed terminal. For
an already-terminal exposure it never rewrites the terminal or cost vector: exact redelivery validates the winning
terminal, while an allowed late response after failure, retry, or a distinct earlier response exact-validates the prior
terminal and records only the new response receipt/classification work. Attempt failure after a response terminal is
rejected. The cost owner exposes one typed `terminalize_or_validate_immutable_terminal` method; the coordinator never
selects or edits a cost evidence variant.

### 11.2 Response-classification ingress — stored-state proof only

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
-> stored_state_branch
-> current_mark_current_pending_apply_or_stale_quarantine_and_terminal_CAS
-> exposure_terminal_exact_validate_unchanged
-> commit
```

Only a transaction-local proof derived from all locked stored rows can choose a branch. `current` additionally requires
the immutable exposure terminal to be response-backed by this exact receipt. It moves the intent only to nonterminal
`current_pending_apply`; it writes no terminal/domain/event/source result. Failure-backed terminals, retry/epoch drift,
business-fence drift, and a distinct response receipt are necessarily stale. The stale branch inserts/exact-replays the
response-only quarantine and completes `classified_stale` in this UoW. Missing rows, lock-budget exhaustion, or an
unprovable relation rolls the UoW back. A caller flag, callback label, stale `ClaimReceipt`, serialized capability,
receipt field, or exposure field cannot choose the branch.

### 11.3 Recoverable current-apply continuation — fresh proof in the normal terminal UoW

```text
d3_dispatch_v2
-> operation_root
-> optional_plan_review_gate
-> participating_commands_sorted_and_terminal_identities_reserved
-> verification_intent_and_predecessor
-> activity_run_attempt
-> dispatch_exposure_lock
-> transport_response_receipt_exact_replay
-> response_classification_current_pending_apply_lock
-> fresh_stored_state_branch
-> current_normal_terminal_record_apply_or_stale_quarantine
-> response_classification_terminal_CAS
-> exposure_terminal_exact_validate_unchanged
-> commit
```

This is a normal global terminal/record UoW, not the transport-evidence-only exception. All deterministic command/event
identities and every earlier row it can mutate are reserved or locked before the exposure segment; after entering the
exposure segment it may update only those already-locked/reserved rows and may not discover, insert, or lock a new
earlier identity. Fresh current proof atomically performs the normal terminal/record/domain/source writes and
`applied_current`. Fresh stale proof performs zero domain/source/result writes and atomically inserts/exact-replays
quarantine plus `classified_stale`. A crash before commit leaves `current_pending_apply` due and recoverable; there is no
terminal `classified_current` state that can strand a response.

### 11.4 Exact response/failure/retry race outcomes

| First committed condition | Later ingress | Exact outcome |
|---|---|---|
| nonterminal exposure, response first | exact response redelivery | same receipt and classification intent exact-replay; zero new row |
| nonterminal exposure, response first | failure or retry | reject; immutable response terminal and cost remain unchanged |
| failure terminal first | distinct valid response | new response receipt + classification intent; exposure/cost exact-validated unchanged; classification must stale + quarantine; never apply |
| retry/epoch advance first, exposure nonterminal | valid response | response receipt + response-backed cost terminal may commit; classification must stale + quarantine; never apply |
| retry/epoch advance first, exposure already terminal | valid response | response receipt + classification intent; exposure/cost exact-validated unchanged; classification must stale + quarantine |
| any response delivery | same delivery identity with drifted digest/shape | collision; entire ingress zero-write |
| response terminal already names an earlier response | distinct second delivery | distinct receipt + classification intent; immutable exposure/cost unchanged; second response must stale + quarantine |
| response marked current_pending_apply, then control/epoch/business advance | current-apply continuation | fresh proof chooses stale; zero domain/source/result write; quarantine + `classified_stale` |

Attempt failure and proven no-call never create a classification intent or quarantine row. Distinct response delivery
does not overwrite the exposure's winning receipt reference. Exact replay compares every immutable row field, and every
mismatch fails closed without a partial receipt, classification, quarantine, cost, event, or domain write.

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

## 13. Mechanism × ten-invariant matrix — 110 populated cells

| mechanism | 1 owner | 2 tenant | 3 fence | 4 lifecycle | 5 late/partial | 6 cost | 7 physical identity | 8 provenance | 9 consistency | 10 mode isolation |
|---|---|---|---|---|---|---|---|---|---|---|
| verification intent | sole intent repository; §2/§4 | PFX in PK/FKs/CAS; §3/§9 | source 7 + state version; §4 | six states, no revival; §4.3 | 29-field append-once branch; §4.2 | no amount or send authority; §4 | operation/phase unique; §9 | exact event/receipt truth table; §4/§9 | one 52-column manifest; §4 | three model modes only; §12 |
| response receipt | sole transport-evidence repository; §2/§5 | PFX in every identity; §3/§9 | exposure lock + attempt/gen/epoch; §5/§11 | immutable exact replay; §5 | valid late response retained; §11 | binds one committed exposure; §5 | v2 delivery/occurrence unique; §9/§10 | D0f ref/digest FK; §5/§9 | one 30-column manifest; §5 | PFX mode exact; §12 |
| attempt-failure receipt | sole transport-evidence repository; §2/§6 | PFX in every identity; §3/§9 | exposure lock + attempt/gen/epoch; §6/§11 | immutable exact replay; §6 | never quarantine; §6/§11 | conservative exposure evidence; §6 | v2 failure occurrence unique; §9/§10 | registry failure spec only; §6 | one 28-column manifest; §6 | PFX mode exact; §12 |
| response classification intent | sole classification repository; §2/§7 | PFX in PK/unique/FK; §7/§9 | private claimed version capability; §7 | three nonterminals/three terminals; §7 | durable retry/reclaim/current-apply recovery; §7/§11 | no money mutation; §7 | one per response occurrence; §9/§10 | fresh stored-state proof only; §7/§11 | one 18-column manifest; §7 | cross-mode replay impossible; §12 |
| late quarantine | sole quarantine repository; §2/§8 | PFX in PK/unique/FKs; §8/§9 | stale classification capability only; §11 | orthogonal monotonic axes; §8 | response-only tombstone; §8 | confirmed/uncertain, no no-call; §8 | v2 idempotency + receipt FK; §9/§10 | authorizable always false; §8 | one 41-column manifest; §8 | no non-live/live alias; §12 |
| cost reservation and dispatch exposure | sole cost-ledger repository; §2/D3c2g | PFX in every relation/CAS; §9 | global locks + state version; §11/D3c2g | immutable terminal and split settlement; §11/D3c2g | late response validates, never rewrites; §11 | owner-only vectors; D3c2g | reservation/call/receipt relations; §9 | terminal evidence variants are owner-built; D3c2g | combined seven-table boundary; §9 | live positive, non-live zero; §12 |
| full-PFX v2 encoders | each domain owner derives its id; §10 | all five PFX components encoded; §10 | canonical bytes reject aliases; §10 | immutable digest forever; §10 | redelivery joins exact row; §10 | one physical call identity; §10 | four golden vectors; §10.1 | NFC/UTF-8/decimal rules; §10 | scope-only v1 superseded; §10 | provider mode changes hash; §10 |
| two ingress UoWs plus continuation | coordinator owns order, repos own rows; §2/§11 | one exact PFX throughout; §11 | global prefix for classification/apply; §11 | atomic rollback at tail; §11 | current pending apply prevents stranding; §7/§11 | immutable cost exact-validation; §11 | receipt/intent/quarantine chain; §9/§11 | caller classification forbidden; §11 | exactly two ingress plus one continuation; §11 | no cross-mode lookup; §3/§12 |
| D0f envelope relation | D0f sole durable issuer; §2 | PFX-bound ref; §2/§9 | specialized exact replay; D0f | retained-to-tombstone; D0f | evidence never authorizes apply; §2 | exposure ref required; D0f/D3c2g | ref+digest FK; §9 | canonical schema remains one; §2 | no second envelope schema; §2 | live/simulate/scripted only; §12 |
| combined relation and index boundary | each parent/child retains one owner; §2/§9 | all relations use PFX; §9 | exact FK actions and predicates; §9 | ordered create/rollback; §9.4 | recovery paths have exact partial indexes; §9.3 | cost and evidence co-ordered; §9 | 13 parent + 52 child constraints; §9 | no placeholder parent proof; §9.4 | one combined manifest; §9 | provider mode participates in every key; §9 |
| transport/mode boundary | future variant owner only; §12 | mode is PFX identity; §3 | eligibility before row creation; §12 | replay remains absent; §12 | non-live keeps full chain; §12 | live positive, non-live zero; §12 | model and Harvest cannot alias; §12 | model-only pins never fabricated; §12 | Harvest explicitly deferred; §12 | exact three-mode closure; §12 |

Every one of the 110 invariant cells is populated. No cell claims implementation, rollout, provider readiness, or formal
review approval.

## 14. Executable oracle, non-closure, and next order

`tests/test_d3c2h1_exact_evidence_surface_decision_lock.py` mechanically checks:

- exact 52/30/28/18/41 ordered name/type/null/default manifests;
- the exact source-seven list and 29-row terminal branch truth table;
- all 47 named local CHECK predicates, their `10/9/9/7/12` table split, and same-create-table installation contract;
- owner/store exclusivity, the complete exact tuples for 13 upstream constraints and 52 seven-table constraints including
  29 FKs, PostgreSQL's 63-byte identifier ceiling, exact FK actions, and the two unresolved parent-owner blockers;
- all 11 complete index tuples and 12 complete access-path tuples; the two pending-state readers deliberately share one
  index while every other access path retains its named sole index;
- the complete 17-action create/attach DAG and 16-action rollback order, including mechanical dependency validation of
  all six deferred internal FKs without guessing either unresolved parent schema;
- classification-intent six-state lifecycle, complete attempt-8 boundary and admitted recovery access, two ingress
  orders, recoverable current-apply continuation, and all three fields of every eight-row response/failure/retry race
  tuple;
- response/quarantine artifact ref+digest SQL truth tables, including blank/whitespace rejection and retained-digest
  tombstones;
- v2 length-delimited golden bytes/digests and supersession of the old scope-only formulas;
- fixed DB-clock 30-day quarantine retention, disjoint cost/retention mutation sets, and explicit rejection of nullable
  timestamp `UNKNOWN` in all three affected local checks;
- model-only three-mode eligibility, replay zero-write, and Harvest/provider-search deferral;
- the complete 11×10 matrix and current physical absence of all seven future tables/owners;
- actual D0f durable owner/migration presence at `539c689`, including the exact TIMESTAMPTZ substrate, without claiming
  a Decimal cost substrate.
- migration `0003`'s already-installed `workflow_commands.workspace_id` exact default/nullability and named `NOT VALID`
  shape check, plus the future combined migration's adopt/validate/no-add-or-drop contract.

D3c2h1's first pinned `gpt-5.6-sol / ultra / priority` non-author review of `1c4a2d9177dcb3470117700086b12fd533898bb7`
was formal `NO-GO 0/3/3/0`. The later Ultra attempt against `f0a0069c83b7ec582682ad79f7a278e604cdd4a0`
failed closed as `invalid_transport`; its substantive output is advisory only and cannot be promoted into a formal verdict.
The fresh scope-local advisory against `af4db419bddb9b18dd61f6dcca6191bb70f4733c` returned
`NO-GO 0/0/1/0`; its sole P2 was the stale `workspace_id` installation wording now fixed above. The fresh pinned local
advisory against `fdb3b792c14fa02e986f99decd3bf510173ea6cf` returned `GO 0/0/0/0` and closed that re-raise, but is not a
formal highest-effort review result. It closes no migration, repository, runtime,
rollout, formal-review, provider, live, W6, manual, product, Migration A–D, served-action, or residual gate and does not
authorize SQL by itself. The next bounded order is:

1. formal highest-effort review of this repaired decision lock;
2. separate pinned review of the typed plan/review/gate-parent and Tier-2 grant-parent owner decision lock;
3. only after both scopes have matching formal `GO`, a dormant combined migration with real-PG
   constraint/index/rollback/race/plan/lock acceptance;
4. specialized repositories/CAS, two ingress APIs, and the current-apply continuation, still with no provider call;
5. strict writers and fake/simulate/scripted E2E;
6. separately reviewed provider-search variant and only then a separately gated bounded live canary.

Current P2-repair author evidence is exact H1 `13 passed`, the five-file related battery `139 passed`, Ruff
check/format clean, and `git diff --check` clean. It uses no provider/model credentials and remains author evidence only:

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
