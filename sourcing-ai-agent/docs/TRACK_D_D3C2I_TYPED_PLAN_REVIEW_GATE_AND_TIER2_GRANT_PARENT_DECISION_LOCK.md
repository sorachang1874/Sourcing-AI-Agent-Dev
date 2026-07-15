# Track D D3c2i — Typed plan/review/gate and Tier-2 grant parent decision lock

> Status: **decision lock only; `decision_locked_not_implemented`; no runtime activation.** This batch creates no SQL,
> migration, descriptor, repository, writer, provider/model call, live path, or served Agent tool. It closes the two
> physical-parent *decisions* deliberately left symbolic by D3c2h1 §9.4. It does not claim that either parent exists.
>
> Review status: author evidence only. A fresh pinned non-author review of this exact commit is required. Author tests,
> an advisory result, or a review of D3c2h1 cannot be represented as a formal `GO` for D3c2i.

## 1. Outcome and bounded impact

D3b requires the business fence to read typed plan/review/gate columns and explicitly forbids `plan_json`, `gate_json`,
model output, or caller digests as authorization inputs. D3c2g freezes those pins in `dispatch_exposures` columns
19–25 and freezes a Tier-2 grant reference in columns 40–43. D3c2h1 correctly stopped before migration because neither
scope-aware parent target had been physically ratified.

D3c2i resolves only that decision boundary:

1. the sole future owner is
   `src/sourcing_agent/repositories/plan_review_authority.py::PlanReviewAuthorityRepository`, reached only through
   `store.repos.plan_review_authority`;
2. that owner controls one aggregate with three future tables:
   `plan_review_gate_authority_versions`, `identity_search_budget_grants`, and
   `identity_search_budget_consumptions`;
3. immutable authority-version history, rather than a mutable current row or legacy JSON, is the FK parent for exposure
   columns 19–25;
4. the exact immutable grant issuance identity is the FK parent for Tier-2 exposure columns 40–43; Tier-1's all-NULL
   tuple remains outside that FK under `MATCH SIMPLE`;
5. grant issue/consume/revoke/exhaust/supersede/reconcile and `human_transition_pending` recovery have one writer,
   exact CAS surfaces, bounded scan paths, and a complete lifecycle;
6. Plan §6 item 6/R-019 and item 7/OB-1.1/OB-4.1/OB-9.1/OB-10.2 advance only to
   `decision_locked_not_implemented` for this slice.

The current `plan_review_sessions` row remains the canonical positive-BIGINT coordination root and audit/session record.
Its mutable `request_json`, `plan_json`, `gate_json`, `decision_json`, and `execution_bundle_json` may remain product
payloads, but none is an authorization source, a revision owner, an FK parent, or a backfill source for strict D3.

## 2. Exact declaration block

The executable oracle exact-compares this block and every tuple below.

```text
D3C2I_PARENT_DECISION_V1
implementation_status = decision_locked_not_implemented
scope_prefix = runtime_namespace | provider_mode | workspace_id | scope_digest | coordination_plan_review_id
eligible_provider_modes = live | simulate | scripted
sole_owner = src/sourcing_agent/repositories/plan_review_authority.py::PlanReviewAuthorityRepository
store_entrypoint = store.repos.plan_review_authority
authority_table = plan_review_gate_authority_versions
grant_table = identity_search_budget_grants
consumption_table = identity_search_budget_consumptions
authority_column_count = 32
grant_column_count = 37
consumption_column_count = 22
authority_local_check_count = 16
grant_local_check_count = 15
consumption_local_check_count = 9
local_check_total = 40
new_structural_constraint_count = 25
new_fk_count = 16
new_index_count = 4
new_access_path_count = 5
exposure_plan_parent_column_count = 7
exposure_grant_parent_column_count = 3
human_transition_recovery_max_attempts = 8
default_searches_per_grant = 3
combined_relation_count = 10
combined_structural_constraint_count = 77
combined_fk_count = 45
combined_index_count = 15
combined_forward_action_count = 19
combined_rollback_action_count = 18
matrix_mechanism_count = 3
matrix_invariant_count = 10
matrix_cell_count = 30
legacy_json_authority = forbidden
runtime_activation = forbidden
migration_authorization = forbidden_until_matching_pinned_go
```

`PFX` below is documentation shorthand for the exact five stored columns in the declaration block. It is never a JSON
object, generated digest, single synthetic column, or substitute for the five physical columns.

## 3. Owner and aggregate boundary

`PlanReviewAuthorityRepository` is the only writer of all three rows. It owns their descriptors, typed row models,
canonical encoders, transition policy, grant-envelope validation, and every method in §8. A generic Store facade,
orchestrator, reducer, scheduler, transport adapter, review HTTP handler, recovery daemon, or legacy
`ControlPlaneStore.review_plan_session` path may not issue SQL against them. Cross-owner work arrives as a registered
typed command/event; reducers never write the aggregate.

The aggregate deliberately uses immutable authority versions instead of updating the seven business-fence pins in
place. An exposure is retained for audit and therefore cannot FK to a row whose plan/review/gate revisions later change.
Each accepted transition retires the old current version and inserts one successor; the old seven-pin tuple remains
immutable and referenceable. Currentness is proved separately under the partial unique index and repository lock. An FK
proves that a typed historical tuple existed; it does **not** by itself authorize dispatch. Dispatch still locks and
exact-compares the one current authority row plus the current active grant before the pre-network write.

## 4. Exact ordered manifests

### 4.1 `plan_review_gate_authority_versions` — exactly 32 columns

| position | column | SQL type | nullable | default |
|---:|---|---|---|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `authority_version_id` | `TEXT` | no | none |
| 7 | `transition_idempotency_key` | `TEXT` | no | none |
| 8 | `predecessor_authority_version_id` | `TEXT` | yes | none |
| 9 | `plan_id` | `TEXT` | no | none |
| 10 | `plan_bundle_digest` | `TEXT` | no | none |
| 11 | `plan_revision` | `BIGINT` | no | none |
| 12 | `review_revision` | `BIGINT` | no | none |
| 13 | `review_state` | `TEXT` | no | none |
| 14 | `review_decision_source_event_id` | `TEXT` | yes | none |
| 15 | `gate_control_epoch` | `BIGINT` | no | none |
| 16 | `gate_revision` | `BIGINT` | no | none |
| 17 | `gate_blocking_reason_digest` | `TEXT` | no | none |
| 18 | `gate_state` | `TEXT` | no | none |
| 19 | `identity_result_watermark` | `BIGINT` | no | `0` |
| 20 | `identity_decision_source_event_id` | `TEXT` | yes | none |
| 21 | `human_transition_pending` | `BOOLEAN` | no | `false` |
| 22 | `human_transition_convergence_state` | `TEXT` | no | `'none'` |
| 23 | `human_transition_source_event_id` | `TEXT` | yes | none |
| 24 | `human_transition_started_at` | `TIMESTAMPTZ` | yes | none |
| 25 | `human_transition_deadline_at` | `TIMESTAMPTZ` | yes | none |
| 26 | `human_transition_recovery_attempt` | `BIGINT` | no | `0` |
| 27 | `human_transition_last_error_code` | `TEXT` | yes | none |
| 28 | `is_current` | `BOOLEAN` | no | `true` |
| 29 | `row_version` | `BIGINT` | no | `0` |
| 30 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 31 | `updated_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 32 | `retired_at` | `TIMESTAMPTZ` | yes | none |

The exact exposure-parent tuple is:

```text
PFX | plan_id | plan_bundle_digest | plan_revision | review_revision |
gate_control_epoch | gate_revision | gate_blocking_reason_digest
```

The seven non-PFX values exactly equal `dispatch_exposures` columns 19–25. `is_current`, review/gate status, watermark,
last applied identity-decision event, and human-transition fields are deliberately not copied to an exposure; they are
mutable eligibility facts rechecked under the current-row lock. The last event remains stored after pending clears so
final commit can exact-compare the latest human decision with verification provenance. Every eligibility change inserts
a successor that advances at least `review_revision` or `gate_revision`, so an old business fence cannot become current
through an ABA rewrite.

### 4.2 `identity_search_budget_grants` — exactly 37 columns

| position | column | SQL type | nullable | default |
|---:|---|---|---|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `grant_id` | `TEXT` | no | none |
| 7 | `grant_issuance_generation` | `BIGINT` | no | none |
| 8 | `grant_policy_revision` | `TEXT` | no | none |
| 9 | `grant_event_id` | `TEXT` | no | none |
| 10 | `operation_run_id` | `TEXT` | no | none |
| 11 | `verification_intent_id` | `TEXT` | no | none |
| 12 | `verification_intent_phase_generation` | `BIGINT` | no | none |
| 13 | `plan_id` | `TEXT` | no | none |
| 14 | `plan_bundle_digest` | `TEXT` | no | none |
| 15 | `plan_revision` | `BIGINT` | no | none |
| 16 | `review_revision` | `BIGINT` | no | none |
| 17 | `gate_control_epoch` | `BIGINT` | no | none |
| 18 | `gate_revision` | `BIGINT` | no | none |
| 19 | `gate_blocking_reason_digest` | `TEXT` | no | none |
| 20 | `grant_state` | `TEXT` | no | none |
| 21 | `state_version` | `BIGINT` | no | `0` |
| 22 | `searches_granted` | `BIGINT` | no | none |
| 23 | `searches_remaining` | `BIGINT` | no | none |
| 24 | `fetches_granted` | `BIGINT` | no | none |
| 25 | `fetches_remaining` | `BIGINT` | no | none |
| 26 | `model_tokens_granted` | `BIGINT` | no | none |
| 27 | `model_tokens_remaining` | `BIGINT` | no | none |
| 28 | `wall_time_ms_granted` | `BIGINT` | no | none |
| 29 | `wall_time_ms_remaining` | `BIGINT` | no | none |
| 30 | `supersedes_grant_id` | `TEXT` | yes | none |
| 31 | `supersedes_issuance_generation` | `BIGINT` | yes | none |
| 32 | `superseded_by_grant_id` | `TEXT` | yes | none |
| 33 | `superseded_by_issuance_generation` | `BIGINT` | yes | none |
| 34 | `terminal_source_event_id` | `TEXT` | yes | none |
| 35 | `issued_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 36 | `updated_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 37 | `terminal_at` | `TIMESTAMPTZ` | yes | none |

The immutable issuance identity and exposure-parent target are exactly:

```text
PFX | grant_id | grant_issuance_generation | grant_policy_revision
```

The full grant authority also binds the positive coordination review id in PFX, exact operation and verification-intent
phase, the seven typed authority pins, and the immutable issuance `grant_event_id`. `grant_issuance_generation` is
monotonic within PFX and is allocated only while holding the PFX coordination lock; it is not a global sequence and is
never inferred from row count. Each re-grant receives a new `grant_id`, new generation, and new event.

The envelope has four independent nonnegative dimensions: searches, fetches, model tokens, and wall milliseconds.
`default_searches_per_grant=3` is the TD-5 soft default, not a plan-lifetime ceiling. The plan-review owner must persist
all four exact granted values from a typed owner-created decision after validating the selected immutable policy
revision and the plan's bounded resource envelope. Raw HTTP/model values cannot supply balances. A later policy may
change defaults only under a new immutable `grant_policy_revision`; it never changes an existing row.

### 4.3 `identity_search_budget_consumptions` — exactly 22 columns

| position | column | SQL type | nullable | default |
|---:|---|---|---|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `grant_consumption_id` | `TEXT` | no | none |
| 7 | `grant_id` | `TEXT` | no | none |
| 8 | `grant_issuance_generation` | `BIGINT` | no | none |
| 9 | `grant_policy_revision` | `TEXT` | no | none |
| 10 | `operation_run_id` | `TEXT` | no | none |
| 11 | `command_id` | `TEXT` | no | none |
| 12 | `activity_run_id` | `TEXT` | no | none |
| 13 | `activity_attempt_id` | `TEXT` | no | none |
| 14 | `physical_call_index` | `BIGINT` | no | none |
| 15 | `searches_debit` | `BIGINT` | no | none |
| 16 | `fetches_debit` | `BIGINT` | no | none |
| 17 | `model_tokens_debit` | `BIGINT` | no | none |
| 18 | `wall_time_ms_debit` | `BIGINT` | no | none |
| 19 | `expected_grant_state_version` | `BIGINT` | no | none |
| 20 | `resulting_grant_state_version` | `BIGINT` | no | none |
| 21 | `resulting_grant_state` | `TEXT` | no | none |
| 22 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |

This immutable child is the exact-replay proof for a pre-transport debit. Its physical-call unique key prevents a
retry from spending twice. The debit row and grant balance/state CAS commit in the same PG transaction before the cost
owner may create the exposure; if exposure creation or dispatch linearization fails, the whole transaction rolls back.
There is no refund-on-retry path. A provider call that may have started consumes its reserved grant envelope honestly.

## 5. Exact structural constraints and indexes

All FKs below use the exact ordered stored columns, `MATCH SIMPLE DEFERRABLE INITIALLY IMMEDIATE`, `ON UPDATE RESTRICT`,
and `ON DELETE RESTRICT`. Optional event/self tuples are guarded by the complete-or-all-NULL checks in §6. No id-only,
scope-digest-only, JSON, trigger-only, or application-only substitute is allowed.

### 5.1 Newly ratified structural constraints — exactly 25, including 16 FKs

| order | name | kind | exact child columns / target |
|---:|---|---|---|
| 1 | `plan_review_gate_authority_versions_pkey` | PK | `(PFX, authority_version_id)` |
| 2 | `plan_review_gate_authority_versions_transition_uk` | UNIQUE | `(PFX, transition_idempotency_key)` |
| 3 | `plan_review_gate_authority_versions_exposure_uk` | UNIQUE | `(PFX, plan_id, plan_bundle_digest, plan_revision, review_revision, gate_control_epoch, gate_revision, gate_blocking_reason_digest)` |
| 4 | `plan_review_gate_authority_versions_review_fk` | FK_STD | `PFX` -> `plan_review_sessions(runtime_namespace, provider_mode, workspace_id, scope_digest, review_id)` |
| 5 | `plan_review_gate_authority_versions_predecessor_fk` | FK_STD | `(PFX, predecessor_authority_version_id)` -> authority identity |
| 6 | `plan_review_gate_authority_versions_decision_event_fk` | FK_STD | `(PFX, review_decision_source_event_id)` -> workflow-event scope unique |
| 7 | `plan_review_gate_authority_versions_human_event_fk` | FK_STD | `(PFX, human_transition_source_event_id)` -> workflow-event scope unique |
| 8 | `plan_review_gate_authority_versions_identity_event_fk` | FK_STD | `(PFX, identity_decision_source_event_id)` -> workflow-event scope unique |
| 9 | `identity_search_budget_grants_pkey` | PK | `(PFX, grant_id)` |
| 10 | `identity_search_budget_grants_exposure_uk` | UNIQUE | `(PFX, grant_id, grant_issuance_generation, grant_policy_revision)` |
| 11 | `identity_search_budget_grants_issuance_uk` | UNIQUE | `(PFX, grant_issuance_generation)` |
| 12 | `identity_search_budget_grants_event_uk` | UNIQUE | `(PFX, grant_event_id)` |
| 13 | `identity_search_budget_grants_review_fk` | FK_STD | `PFX` -> session scope/review unique |
| 14 | `identity_search_budget_grants_authority_fk` | FK_STD | `(PFX, plan_id, plan_bundle_digest, plan_revision, review_revision, gate_control_epoch, gate_revision, gate_blocking_reason_digest)` -> authority exposure unique |
| 15 | `identity_search_budget_grants_intent_fk` | FK_STD | `(PFX, operation_run_id, verification_intent_id, verification_intent_phase_generation)` -> verification-intent exposure-parent unique |
| 16 | `identity_search_budget_grants_event_fk` | FK_STD | `(PFX, grant_event_id)` -> workflow-event scope unique |
| 17 | `identity_search_budget_grants_terminal_event_fk` | FK_STD | `(PFX, terminal_source_event_id)` -> workflow-event scope unique |
| 18 | `identity_search_budget_grants_supersedes_fk` | FK_STD | `(PFX, supersedes_grant_id, supersedes_issuance_generation, grant_policy_revision)` -> grant exposure unique |
| 19 | `identity_search_budget_grants_superseded_by_fk` | FK_STD | `(PFX, superseded_by_grant_id, superseded_by_issuance_generation, grant_policy_revision)` -> grant exposure unique |
| 20 | `identity_search_budget_consumptions_pkey` | PK | `(PFX, grant_consumption_id)` |
| 21 | `identity_search_budget_consumptions_call_uk` | UNIQUE | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id, physical_call_index)` |
| 22 | `identity_search_budget_consumptions_grant_fk` | FK_STD | `(PFX, grant_id, grant_issuance_generation, grant_policy_revision)` -> grant exposure unique |
| 23 | `identity_search_budget_consumptions_attempt_fk` | FK_STD | `(PFX, operation_run_id, command_id, activity_run_id, activity_attempt_id)` -> activity-attempt unique |
| 24 | `dispatch_exposures_plan_review_gate_authority_fk` | FK_STD | exposure `(PFX, columns 19-25)` -> authority exposure unique |
| 25 | `dispatch_exposures_identity_search_grant_fk` | FK_STD | exposure `(PFX, columns 41-43)` -> grant exposure unique; Tier-1 all-NULL tuple skips under `MATCH SIMPLE` |

The grant table's exposure unique is the scope-aware target requested by D3c2h1; its narrower PK independently prevents
reuse of one immutable `grant_id` under another issuance. `grant_tier='tier2'` already requires all three child fields,
so FK #25 is mandatory for every Tier-2 exposure. Tier-1 has all three fields SQL NULL and therefore
does not fabricate a grant. The authority unique target retains every historical seven-pin tuple, so retiring current
authority never invalidates a retained exposure.

### 5.2 Exact new indexes — exactly 4; admitted paths — exactly 5

| order | name | table | exact ordered columns and predicate |
|---:|---|---|---|
| 1 | `plan_review_gate_authority_versions_current_uk` | `plan_review_gate_authority_versions` | UNIQUE `(PFX) WHERE is_current` |
| 2 | `plan_review_gate_authority_versions_human_due_idx` | `plan_review_gate_authority_versions` | `(runtime_namespace, provider_mode, human_transition_deadline_at, workspace_id, scope_digest, coordination_plan_review_id) WHERE is_current AND human_transition_convergence_state = 'pending'` |
| 3 | `identity_search_budget_grants_active_intent_uk` | `identity_search_budget_grants` | UNIQUE `(PFX, operation_run_id, verification_intent_id, verification_intent_phase_generation) WHERE grant_state = 'active'` |
| 4 | `identity_search_budget_consumptions_grant_audit_idx` | `identity_search_budget_consumptions` | `(PFX, grant_id, grant_issuance_generation, grant_policy_revision, created_at, grant_consumption_id)` |

| access path | sole exact index/constraint |
|---|---|
| current typed authority lock by complete PFX | `plan_review_gate_authority_versions_current_uk` |
| oldest due `human_transition_pending` recovery by namespace/mode | `plan_review_gate_authority_versions_human_due_idx` |
| active exact grant for one verification-intent phase | `identity_search_budget_grants_active_intent_uk` |
| one grant's immutable consumption history | `identity_search_budget_consumptions_grant_audit_idx` |
| exact debit replay/collision by physical call | `identity_search_budget_consumptions_call_uk` |

No owner lookup may rely on a broader hidden scan. Recovery uses bounded `FOR UPDATE SKIP LOCKED`, then obtains the
common coordination advisory lock and restarts the full global order before any aggregate mutation.

## 6. Exact local check inventory — 40

The future new-table migration installs these checks valid at creation. `NB(x)` means `x ~ '[^[:space:]]'`; `SHA(x)`
means `x ~ '^[0-9a-f]{64}$'`. Those are documentation macros only. Every nullable disjunction below must use explicit
`IS NULL`/`IS NOT NULL` and wrap the whole predicate in `IS TRUE` so PostgreSQL `UNKNOWN` cannot pass.

### 6.1 Authority checks — exactly 16

| order | constraint name | exact predicate |
|---:|---|---|
| 1 | `prga_versions_runtime_namespace_nonblank_ck` | `NB(runtime_namespace)` |
| 2 | `prga_versions_provider_mode_ck` | `provider_mode IN ('live', 'simulate', 'scripted')` |
| 3 | `prga_versions_workspace_id_nonblank_ck` | `NB(workspace_id)` |
| 4 | `prga_versions_scope_digest_shape_ck` | `SHA(scope_digest)` |
| 5 | `prga_versions_coordination_positive_ck` | `coordination_plan_review_id > 0` |
| 6 | `prga_versions_identity_shape_ck` | `NB(authority_version_id) AND NB(transition_idempotency_key) AND (predecessor_authority_version_id IS NULL OR NB(predecessor_authority_version_id))` |
| 7 | `prga_versions_plan_pin_shape_ck` | `NB(plan_id) AND SHA(plan_bundle_digest) AND plan_revision >= 0 AND review_revision >= 0` |
| 8 | `prga_versions_review_shape_ck` | `((review_state = 'pending' AND review_decision_source_event_id IS NULL) OR (review_state IN ('approved', 'rejected') AND NB(review_decision_source_event_id))) IS TRUE` |
| 9 | `prga_versions_gate_pin_shape_ck` | `gate_control_epoch >= 0 AND gate_revision >= 0 AND SHA(gate_blocking_reason_digest)` |
| 10 | `prga_versions_gate_state_ck` | `gate_state IN ('blocking', 'clear')` |
| 11 | `prga_versions_watermark_source_shape_ck` | `(identity_result_watermark >= 0 AND ((identity_result_watermark = 0 AND identity_decision_source_event_id IS NULL) OR (identity_result_watermark > 0 AND NB(identity_decision_source_event_id)))) IS TRUE` |
| 12 | `prga_versions_human_transition_shape_ck` | `((human_transition_convergence_state = 'none' AND NOT human_transition_pending AND human_transition_source_event_id IS NULL AND human_transition_started_at IS NULL AND human_transition_deadline_at IS NULL AND human_transition_recovery_attempt = 0 AND human_transition_last_error_code IS NULL) OR (human_transition_convergence_state = 'pending' AND human_transition_pending AND NB(human_transition_source_event_id) AND human_transition_started_at IS NOT NULL AND human_transition_deadline_at IS NOT NULL AND human_transition_deadline_at >= human_transition_started_at AND human_transition_recovery_attempt BETWEEN 0 AND 7 AND (human_transition_last_error_code IS NULL OR NB(human_transition_last_error_code))) OR (human_transition_convergence_state = 'reconciliation_required' AND human_transition_pending AND NB(human_transition_source_event_id) AND human_transition_started_at IS NOT NULL AND human_transition_deadline_at IS NOT NULL AND human_transition_deadline_at >= human_transition_started_at AND human_transition_recovery_attempt = 8 AND NB(human_transition_last_error_code))) IS TRUE` |
| 13 | `prga_versions_current_retirement_shape_ck` | `((is_current AND row_version = 0 AND retired_at IS NULL) OR (NOT is_current AND row_version = 1 AND retired_at IS NOT NULL)) IS TRUE` |
| 14 | `prga_versions_no_self_predecessor_ck` | `predecessor_authority_version_id IS NULL OR predecessor_authority_version_id <> authority_version_id` |
| 15 | `prga_versions_timestamp_order_ck` | `updated_at >= created_at AND (retired_at IS NULL OR retired_at >= created_at)` |
| 16 | `prga_versions_approved_gate_shape_ck` | `review_state <> 'approved' OR (gate_state = 'clear' AND NOT human_transition_pending)` |

### 6.2 Grant checks — exactly 15

| order | constraint name | exact predicate |
|---:|---|---|
| 1 | `isbg_runtime_namespace_nonblank_ck` | `NB(runtime_namespace)` |
| 2 | `isbg_provider_mode_ck` | `provider_mode IN ('live', 'simulate', 'scripted')` |
| 3 | `isbg_workspace_id_nonblank_ck` | `NB(workspace_id)` |
| 4 | `isbg_scope_digest_shape_ck` | `SHA(scope_digest)` |
| 5 | `isbg_coordination_positive_ck` | `coordination_plan_review_id > 0` |
| 6 | `isbg_identity_shape_ck` | `NB(grant_id) AND grant_issuance_generation > 0 AND NB(grant_policy_revision) AND NB(grant_event_id)` |
| 7 | `isbg_intent_shape_ck` | `NB(operation_run_id) AND NB(verification_intent_id) AND verification_intent_phase_generation > 0` |
| 8 | `isbg_authority_pin_shape_ck` | `NB(plan_id) AND SHA(plan_bundle_digest) AND plan_revision >= 0 AND review_revision >= 0 AND gate_control_epoch >= 0 AND gate_revision >= 0 AND SHA(gate_blocking_reason_digest)` |
| 9 | `isbg_state_ck` | `grant_state IN ('active', 'revoked', 'exhausted', 'superseded', 'reconciled')` |
| 10 | `isbg_state_version_nonnegative_ck` | `state_version >= 0` |
| 11 | `isbg_balance_shape_ck` | `searches_granted > 0 AND searches_remaining BETWEEN 0 AND searches_granted AND fetches_granted >= 0 AND fetches_remaining BETWEEN 0 AND fetches_granted AND model_tokens_granted >= 0 AND model_tokens_remaining BETWEEN 0 AND model_tokens_granted AND wall_time_ms_granted > 0 AND wall_time_ms_remaining BETWEEN 0 AND wall_time_ms_granted` |
| 12 | `isbg_supersedes_tuple_ck` | `((supersedes_grant_id IS NULL AND supersedes_issuance_generation IS NULL) OR (NB(supersedes_grant_id) AND supersedes_issuance_generation > 0)) IS TRUE` |
| 13 | `isbg_superseded_by_tuple_ck` | `((grant_state = 'superseded' AND NB(superseded_by_grant_id) AND superseded_by_issuance_generation > 0) OR (grant_state <> 'superseded' AND superseded_by_grant_id IS NULL AND superseded_by_issuance_generation IS NULL)) IS TRUE` |
| 14 | `isbg_terminal_shape_ck` | `((grant_state = 'active' AND terminal_source_event_id IS NULL AND terminal_at IS NULL) OR (grant_state <> 'active' AND NB(terminal_source_event_id) AND terminal_at IS NOT NULL)) IS TRUE` |
| 15 | `isbg_timestamp_order_ck` | `updated_at >= issued_at AND (terminal_at IS NULL OR terminal_at >= issued_at)` |

### 6.3 Consumption checks — exactly 9

| order | constraint name | exact predicate |
|---:|---|---|
| 1 | `isbgc_runtime_namespace_nonblank_ck` | `NB(runtime_namespace)` |
| 2 | `isbgc_provider_mode_ck` | `provider_mode IN ('live', 'simulate', 'scripted')` |
| 3 | `isbgc_workspace_id_nonblank_ck` | `NB(workspace_id)` |
| 4 | `isbgc_scope_digest_shape_ck` | `SHA(scope_digest)` |
| 5 | `isbgc_coordination_positive_ck` | `coordination_plan_review_id > 0` |
| 6 | `isbgc_identity_shape_ck` | `NB(grant_consumption_id) AND NB(grant_id) AND grant_issuance_generation > 0 AND NB(grant_policy_revision)` |
| 7 | `isbgc_physical_call_shape_ck` | `NB(operation_run_id) AND NB(command_id) AND NB(activity_run_id) AND NB(activity_attempt_id) AND physical_call_index >= 0` |
| 8 | `isbgc_debit_shape_ck` | `searches_debit >= 0 AND fetches_debit >= 0 AND model_tokens_debit >= 0 AND wall_time_ms_debit >= 0 AND (searches_debit + fetches_debit + model_tokens_debit + wall_time_ms_debit) > 0` |
| 9 | `isbgc_version_transition_ck` | `expected_grant_state_version >= 0 AND resulting_grant_state_version = expected_grant_state_version + 1 AND resulting_grant_state IN ('active', 'exhausted')` |

Cross-row digest equality, currentness, policy applicability, a debit fitting all four remaining balances, exact transfer
equality, and scoped issuance monotonicity are repository/FK predicates; they are not fictional local checks.

## 7. Lifecycles and revision rules

### 7.1 Typed authority versions

Initial creation inserts one current version with no predecessor. Every later accepted transition performs one CAS:

1. lock the one current PFX row and require `is_current=true`, `row_version=0`, exact expected
   `authority_version_id`, exact seven business pins, and the method-specific expected statuses/watermark;
2. update only that old row to `is_current=false`, `row_version=1`, DB-clock `updated_at/retired_at`;
3. insert one successor with a complete predecessor FK, deterministic owner-issued version/idempotency identities,
   `is_current=true`, `row_version=0`;
4. create the typed event/command/grant side effects named by the method in the same UoW;
5. on unique collision, exact-compare the complete immutable row and return exact replay only if every field matches;
   otherwise roll back and return `business_precondition_conflict` with zero durable writes.

Every eligibility change advances at least one dispatch-invalidating typed pin:

| transition | exact revision rule |
|---|---|
| typed plan recompile | `plan_revision+1`, `review_revision+1`, `gate_control_epoch+1`, `gate_revision+1`; new bundle digest |
| grant issue/revoke/supersede or terminal plan decision | `review_revision+1`, `gate_control_epoch+1`, `gate_revision+1`; unchanged plan pins unless this is also a recompile |
| identity-result/gate apply | `gate_revision+1`; watermark monotonically advances and exact-copies `identity_decision_source_event_id`; control epoch is unchanged unless the apply invalidates dispatch |
| begin human transition | `review_revision+1`, `gate_control_epoch+1`, `gate_revision+1`; install blocking pending tuple atomically with decision event |
| human recovery retry/manual-required transition | `gate_control_epoch+1`, `gate_revision+1`; pending remains fail-closed |
| matching human-confirmed apply | `gate_revision+1`; clears pending tuple only while exact source event and verification provenance match, while retaining that event as `identity_decision_source_event_id` |

Revision values never decrement or wrap and an old version never becomes current again.

### 7.2 Human-transition convergence (`OB-4.1`, `OB-9.1`)

The plan-review owner begins a human identity transition in the same UoW that writes the human decision event and the
blocking current successor. The successor is immediately ineligible for dispatch because
`human_transition_pending=true`; asynchronous verification supersession cannot reopen the old window.

The owner schedules/reawakens the deterministic supersession/apply command chain before commit. If it remains pending
through `human_transition_deadline_at`, the sole recovery scanner uses the due index, bounded `SKIP LOCKED`, the common
coordination lock, and the full row order. Current attempts 0–6 re-drive only the same source-event identities and insert
a pending successor with attempt+1. A due current attempt 7 performs the eighth and final recovery; failure inserts
`reconciliation_required` with attempt 8, remains blocking, and exposes a typed manual repair action. It never clears
the gate. A new human decision may start a new source-event transition; it still
increments the typed revisions and cannot revive an old row. Only the matching human-confirmed apply clears pending.

### 7.3 Tier-2 grant lifecycle and balance

The complete transition graph is:

```text
new issuance -> active
active -> active       (one exact pre-transport consumption; state_version + 1)
active -> exhausted    (the accepted debit leaves no registered operation envelope that fits)
active -> revoked      (cancel/recompile/policy invalidation; owner event)
active -> superseded   (same UoW creates successor active grant with exact remaining vector)
active -> reconciled   (clean plan/intent terminal event; unused balance is closed, not transferred)
```

All four non-active states are terminal; there is no terminal-to-terminal transition and no transition back to `active`.
`reconciled` is the clean owner-event close of a still-active grant, not a rewrite of revoked/exhausted/superseded
history. Re-grant is a new row, and the partial unique index permits
at most one active grant for an exact verification-intent phase. Supersede-with-transfer locks the old grant, allocates
the next scoped issuance generation, defers only `identity_search_budget_grants_superseded_by_fk` for that transaction,
updates the old row to `superseded`, then inserts the successor with
`granted == remaining == old.remaining` for all four dimensions. It may not clear or refill any dimension. The deferred
self-FK is restored/validated at commit; both self-FKs and old/new changes are atomic. A zero vector creates no successor
and the old row becomes `exhausted` instead. A policy-revision change revokes the old grant and requires a separately
issued grant; supersede-with-transfer is permitted only under the same immutable `grant_policy_revision`.

Grant issue and every terminal transition append their typed workflow event in the same owner-composed UoW. When that
event is appended after the grant state statement, the repository explicitly sets only the applicable grant event FK to
`DEFERRED`; the immutable event identity is reserved in the earlier command segment and every FK is restored and
validated before commit. This is dependency ordering, not an FK waiver. `finalize_typed_plan_review` atomically closes
every still-active grant for that exact review/intent through `reconcile_identity_search_budget_grant`; a terminal review
can never leave spendable budget behind.

Pre-transport consumption accepts only an owner-derived registered operation envelope. Under exact current authority,
current intent, current claim/attempt, `grant_state='active'`, expected state version, and sufficient four-dimensional
balance, it inserts one immutable consumption and subtracts all four values in the same UoW. Exact physical-call replay
returns the existing result only after full-field compare; different debit is a collision and writes zero. The owner
sets `exhausted` when no registered operation vector fits the resulting balance. This consumes OB-1.1's field-level
single-writer gap: Tier-2 commands request a debit but never write balance/state themselves.

## 8. Exact repository CAS surface — 11 methods

| order | method | admitted mutation / exact replay boundary |
|---:|---|---|
| 1 | `create_or_exact_replay_typed_authority` | create the initial authority version only from committed scoped-session result plus server-owned typed plan pins |
| 2 | `recompile_typed_plan` | retire current, insert recompiled successor, invalidate old commands under the revision rule |
| 3 | `begin_human_identity_transition` | human decision event + blocking successor + deterministic convergence command reservation in one UoW |
| 4 | `apply_identity_result_to_gate` | exact source event/generation/watermark apply; optional matching pending clear; one successor |
| 5 | `finalize_typed_plan_review` | pending -> approved/rejected only after canonical typed gate recheck; one terminal successor |
| 6 | `recover_due_human_transition` | bounded due claim, full lock order, same-event replay/reawaken, attempt successor or reconciliation-required successor |
| 7 | `issue_identity_search_budget_grant` | partial review decision event + authority successor + next immutable active grant in one UoW |
| 8 | `consume_identity_search_budget_pre_transport` | current authority/intent/claim/grant CAS + immutable debit + balance/state update before exposure/send |
| 9 | `revoke_identity_search_budget_grant` | owner event + authority successor + active -> revoked; no direct cancel/recompile writer |
| 10 | `supersede_identity_search_budget_grant_with_transfer` | owner event + authority successor + old terminal/new active exact remaining-vector transfer |
| 11 | `reconcile_identity_search_budget_grant` | active -> reconciled on exact clean terminal owner event; balances immutable |

Every method receives sealed typed contexts from the registered owner path; none accepts dict/JSON rows, caller-created
digests, expected-owner strings, raw token values, or money. Methods 2–11 require the common coordination lock and full
expected current authority tuple. Any stale, missing, foreign-mode, foreign-workspace, wrong-event, wrong-version,
insufficient-balance, half-tuple, or collision condition rolls back the entire UoW and returns a typed zero-write result.

## 9. Lock order and network boundary

The existing `d3-dispatch-v2` advisory key remains exactly immutable PFX plus positive coordination review id. Within
that lock the complete row order is:

```text
operation_runs
-> plan_review_sessions
-> current plan_review_gate_authority_versions
-> all participating workflow_commands in (scope_digest, operation_id, command_id) order
-> verification_intent and current predecessor
-> workflow_activity_run / workflow_activity_attempt
-> optional identity_search_budget_grants
-> optional identity_search_budget_consumptions insert
-> optional cost_reservations / dispatch_exposures
-> response/failure receipt and quarantine/cost tail where applicable
```

An owner may skip a non-applicable optional segment but never go backward. The current authority successor is reserved
and inserted in its segment; command reservations are later. Grant issue/supersession reaches the grant segment only
after all earlier rows are locked. Grant consumption and its immutable child happen before cost exposure. No PG
transaction spans DNS, request bytes, provider polling, response streaming, or any other network I/O.

## 10. Brownfield, adoption, and deletion conditions

All three tables are additive and initially empty. There is no sentinel row and no JSON-derived backfill.

1. Install the new tables/checks/keys/indexes while strict-D3 consumers remain disabled.
2. For a newly committed scoped review session, create the typed initial authority from the bootstrap result's typed plan
   pins. A scoped session without an exact authority row remains legacy-only and cannot satisfy strict D3.
3. Existing sessions may enter strict D3 only through an explicit owner recompile/import command that reconstructs and
   reviews a typed plan from canonical server inputs. Reading `plan_json`/`gate_json` and declaring it equivalent is
   forbidden.
4. Dual-write/shadow comparison may log JSON differences diagnostically, but authorization, grant issue, and dispatch
   read only the typed aggregate. A mismatch fails strict D3 closed and does not repair from JSON.
5. Enable non-live strict population only after the matching D3c2h1 and D3c2i pinned `GO` artifacts, real-PG combined
   schema acceptance, repository/CAS/race acceptance, and a hash-bound complete population manifest.
6. Live/provider activation remains a later gate. No decision in this document permits it.

There is no delete API. Historical authority versions remain while any command, intent, grant, consumption, exposure,
receipt, event, quarantine/tombstone, cost, or audit row references them. Grants and consumptions remain while the same
retained graph exists. Legacy JSON columns may be retained for display/audit; their *authorization readers* may be
deleted only after a full-denominator zero-hit window proves all strict readers use typed rows. The typed aggregate is
never deleted in favor of JSON.

## 11. Combined D3 evidence-schema DAG and rollback

D3c2i replaces only D3c2h1's symbolic forward action 15 / rollback action 2. It does not edit the pinned D3c2h1
artifact. With this decision, the eventual combined boundary is exact: ten new relations, 77 structural constraints
including 45 FKs, and 15 indexes. D3c2h1's 52/29/11 remain unchanged; D3c2i adds 25/16/4.

### 11.1 Exact forward DAG — 19 actions

| order | operation | object | requires present / exact effect |
|---:|---|---|---|
| 1 | `adopt_validate_attach` | strict upstream parents | D3c2h1 §9.1 exact 13 constraints; preserve parent data |
| 2 | `create_table` | `plan_review_gate_authority_versions` | scoped review/session and workflow-event unique targets; D3c2i constraints #1-8 inline |
| 3 | `create_table` | `cost_reservations` | D3c2h1 §9.2 #1-4 inline |
| 4 | `create_table` | `verification_intents` | D3c2h1 §9.2 #19-24/#27 inline; receipt FKs later |
| 5 | `create_table` | `identity_search_budget_grants` | authority, verification-intent, review/event targets; D3c2i #9-19 inline |
| 6 | `create_table` | `identity_search_budget_consumptions` | grant and activity-attempt targets; D3c2i #20-23 inline |
| 7 | `create_table` | `dispatch_exposures` | cost, authority, grant, upstream targets; D3c2h1 inline set plus D3c2i #24-25; six cycle/forward FKs later |
| 8 | `create_table` | `transport_response_receipts` | D3c2h1 §9.2 #28-35 inline |
| 9 | `create_table` | `transport_attempt_failure_receipts` | D3c2h1 §9.2 #36-42 inline |
| 10 | `create_table` | `transport_response_classification_intents` | D3c2h1 §9.2 #43-46 inline |
| 11 | `create_table` | `workflow_late_result_quarantine` | D3c2h1 §9.2 #47-52 inline |
| 12 | `attach_fk` | `dispatch_exposures_base_intent_fk` | D3c2h1 §9.2 #13 |
| 13 | `attach_fk` | `dispatch_exposures_predecessor_intent_fk` | D3c2h1 §9.2 #14 |
| 14 | `attach_fk` | `verification_intents_response_receipt_fk` | D3c2h1 §9.2 #25 |
| 15 | `attach_fk` | `verification_intents_failure_receipt_fk` | D3c2h1 §9.2 #26 |
| 16 | `attach_fk` | `dispatch_exposures_response_receipt_fk` | D3c2h1 §9.2 #17 |
| 17 | `attach_fk` | `dispatch_exposures_failure_receipt_fk` | D3c2h1 §9.2 #18 |
| 18 | `create_indexes` | all ten future tables | D3c2h1 11 plus D3c2i 4 in listed order |
| 19 | `validate_acceptance` | combined strict-D3 evidence schema | exact relation/constraint/index/access/DAG, identifier, lock, race, rollback, population, and PG acceptance |

### 11.2 Exact rollback dependency order — 18 actions

| order | operation | object / exact effect |
|---:|---|---|
| 1 | `drop_indexes` | all 15 combined indexes in reverse order |
| 2 | `detach_fk` | `dispatch_exposures_failure_receipt_fk` |
| 3 | `detach_fk` | `dispatch_exposures_response_receipt_fk` |
| 4 | `detach_fk` | `verification_intents_failure_receipt_fk` |
| 5 | `detach_fk` | `verification_intents_response_receipt_fk` |
| 6 | `detach_fk` | `dispatch_exposures_predecessor_intent_fk` |
| 7 | `detach_fk` | `dispatch_exposures_base_intent_fk` |
| 8 | `drop_table` | `workflow_late_result_quarantine` |
| 9 | `drop_table` | `transport_response_classification_intents` |
| 10 | `drop_table` | `transport_attempt_failure_receipts` |
| 11 | `drop_table` | `transport_response_receipts` |
| 12 | `drop_table` | `dispatch_exposures` |
| 13 | `drop_table` | `identity_search_budget_consumptions` |
| 14 | `drop_table` | `identity_search_budget_grants` |
| 15 | `drop_table` | `verification_intents` |
| 16 | `drop_table` | `cost_reservations` |
| 17 | `drop_table` | `plan_review_gate_authority_versions` |
| 18 | `detach_drop_if_created` | D3c2h1 §9.1 #13 through #1; preserve adopted upstream parent data |

Rollback never uses `CASCADE`, never drops `plan_review_sessions` or workflow parents, never rewrites legacy JSON, and
never deletes adopted parent data. Every identifier in §§5–6 and §11 must be independently byte-counted to at most 63
UTF-8 bytes with no defensive-prefix collision before migration is authorized.

## 12. Mechanism × ten-invariant matrix — exactly 30 cells

| mechanism | 1 single writer | 2 tenant key | 3 generation/fence | 4 lifecycle | 5 late/partial | 6 cost honesty | 7 physical identity | 8 provenance/trust | 9 self-contained | 10 mode isolation |
|---|---|---|---|---|---|---|---|---|---|---|
| typed authority version history | §3 sole repository; reducers/readers never write | full PFX in row/key/FK/CAS (§§4–5) | immutable versions + current partial unique + successor revisions (§7.1) | create/retire/terminal review; old versions retained (§§7.1,10) | old exposure keeps historical FK; current lock rejects late fence (§3) | no transport or money; grant/cost segments remain ordered (§9) | positive review root + exact seven pins + source-event FKs (§5) | typed server pins only; JSON/model/caller forbidden (§§1,10) | exact manifest/constraints/DAG/oracle (§§2,4–6,11) | physical namespace/mode in identity, every FK and CAS (§§2,5) |
| human transition convergence | plan-review owner methods 3/4/6 only (§8) | due scan and commands remain PFX scoped (§§5.2,7.2) | begin/retry advance review/gate fences before async apply (§7.1) | none→pending→none or reconciliation_required; bounded 8 (§7.2) | stale machine result cannot clear mismatched source event (§7.2) | pending blocks provider sends; recovery has no network (§§7.2,9) | exact human event, watermark, current authority, typed commands (§§4.1,7.2) | human event cannot be forged through machine/JSON fields (§§7.2,8) | exact state/check/index/access path and Plan sync (§§5–7) | namespace/mode exact in scan, commands, event FK and CAS (§§5,7.2) |
| Tier-2 grant and consumption | plan-review repository owns issue/state/balance/debit (§§3,8) | full PFX in grant, debit, exposure parent and every CAS (§§4–5) | immutable issuance generation + state_version + physical-call replay (§§4.2–4.3,7.3) | full five-state graph; terminal never revives; exact transfer (§7.3) | stale/late command cannot consume new issuance; collisions zero-write (§7.3) | four-dimensional debit and cost exposure commit before send in one UoW (§§4.3,9) | review/intent/authority/event/attempt/call identities are physical FKs (§5.1) | owner-created policy envelope/event only; caller/model balances forbidden (§4.2) | exact manifests, 15 checks, CAS list, FK targets and DAG (§§4–11) | PFX includes immutable namespace/mode; non-live cannot fund live (§§2,5) |

No cell claims implementation. The matrix records why the decision is complete enough for a pinned review and which
later migration/repository/runtime acceptance must prove it physically.

## 13. Current physical absence and legacy non-authority baseline

At this decision baseline `0001_baseline.sql` declares exactly 83 tables and current repository descriptors declare 41.
None of the three D3c2i table names appears in any migration or descriptor, and the repository module/class or
`store.repos.plan_review_authority` does not exist in
`src/sourcing_agent`. The current baseline `plan_review_sessions` has `request_json`, `plan_json`, `gate_json`,
`execution_bundle_json`, and `decision_json`; migration 0004 adds only the dormant scoped-root/creation pins. Current
`create_plan_review_session`/`review_plan_session` still serialize and mutate JSON through legacy Store paths and have no
typed seven-pin version CAS. The oracle proves these facts mechanically; this paragraph is not a claim that code exists.

The absence test excludes this decision document and its own oracle from source discovery. It exact-parses migration
DDL and repository descriptor declarations, checks the legacy column tuple, checks the current writer tokens, and rejects
any silent appearance of a future table/repository without an implementation batch updating the contract.

## 14. Explicit non-closure and next step

D3c2i does **not** close R-019, R-023, R-027, R-028, R-029, the action-root durable-scope gate, verification-intent
OB-10.1 implementation, cost-ledger OB-2.2/OB-10.3 implementation, execution-context OB-10.4, complete Migration A,
Migration B–D, registry population, repository/runtime writers, provider/live validation, or served Agent tools.

The next step for this slice is a fresh pinned non-author review. Only matching pinned `GO` artifacts for the repaired
D3c2h1 boundary and this D3c2i boundary may authorize a separate dormant combined-migration implementation. That future
batch must run real-PG exact constraints/indexes/FKs, 63-byte identifiers, brownfield/adoption, lock timeout, rollback,
crash/replay, debit race, human-transition recovery, cross-mode and mutation acceptance. It still may not activate a
provider or live path without later runtime and product gates.

## 15. Author validation command

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3c2i_typed_plan_review_gate_tier2_grant_parent_decision_lock.py \
  tests/test_d3c2h1_exact_evidence_surface_decision_lock.py \
  tests/test_d3c2g_cost_ledger_decision_lock.py \
  tests/test_d3_workflow_command_claim_fence_contract.py

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m ruff check \
  tests/test_d3c2i_typed_plan_review_gate_tier2_grant_parent_decision_lock.py

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m ruff format --check \
  tests/test_d3c2i_typed_plan_review_gate_tier2_grant_parent_decision_lock.py
```
