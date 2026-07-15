# Track D D3c2g — Cost reservation / dispatch exposure physical decision lock

> Status: **decision lock only; no runtime activation; `decision_locked_not_implemented`.** This batch creates no SQL, migration,
> descriptor, repository, registry constant, runtime writer, provider/model call, or served Agent path. It ratifies the
> future cost-ledger aggregate so a later migration batch does not guess physical ownership or accounting semantics.
>
> Review provenance: the pinned medium-effort non-author review of `4945ab7ae17764af0a0ca705ffd4257002c2f279`
> returned **ADVISORY NO-GO**, `P0/P1/P2/P3 = 0/2/1/0`. This fixed-forward closes those two P1 and one P2 findings in
> the decision contract and executable oracle; it remains review-pending until a fresh pinned non-author verdict. Neither
> the advisory result nor author validation is a formal GO.

## 1. Outcome and bounded impact

D0 §2.3 requires a worst-case reservation plus one durable exposure per physical call. D0c §6 leaves tenant and
runtime-mode identity open as OB-2.2/OB-10.3. D3b §6.5 reuses that ledger as the dispatch linearization boundary, but
does not name its physical owner, tables, complete fields, registry records, or settlement lock order. D3c2c therefore
correctly recorded dispatch exposure as unratified/undetermined; D3c2e/f intentionally left it outside the event-core
fragment.

D3c2g ratifies only this bounded slice:

1. the sole future owner is
   `src/sourcing_agent/repositories/cost_ledger.py::CostLedgerRepository`, reached only through
   `store.repos.cost_ledger`;
2. `cost_reservations` and `dispatch_exposures` are one inseparable aggregate, with exact ordered manifests, local
   checks, keys, indexes, CAS transitions, amount vectors, and lock orders below;
3. checked-in immutable `COST_BUDGET_POLICY_SPECS`, `COST_PRICING_SPECS`, and
   `COST_RECONCILIATION_SPECS` are the only budget, pricing, and reconciliation policy sources; their exact canonical
   records and amount derivations are closed below, and callers never supply money;
4. D0 `ModelInvocationEnvelopeV1.cost_exposure_ref` is exactly the committed `dispatch_exposure_id` for live,
   simulate, and scripted strict-D3 model turns; there is no second exposure alias;
5. OB-2.2 and OB-10.3 advance only to `decision_locked_not_implemented`.

The tables remain physically absent. Verification intent, response/failure receipt schemas, late quarantine, event
transport columns, registry implementation, migration/adoption, and every runtime/live writer remain later batches.

## 2. Decision provenance: derived versus newly ratified

`[A]` means mechanically derived from already accepted D0/D3/OB contracts. `[R]` means a new owner ratification made
by this decision lock; it must not be described as current code or schema.

| ID | class | locked decision |
|---|---|---|
| A-1 | `[A] derived` | D0 owns the reservation-plus-per-physical-call exposure abstraction and conservative crash accounting. |
| A-2 | `[A] derived` | D3b uses that same aggregate as the single pre-network dispatch linearization boundary; no D3-only transport owner is allowed. |
| A-3 | `[A] derived` | The canonical scope prefix is runtime namespace, provider mode, workspace, scope digest, and positive coordination review id. |
| A-4 | `[A] derived` | Every authorization/consume/release CAS is tenant- and mode-scoped under OB-2.2/OB-10.3. |
| A-5 | `[A] derived` | Tier-2 dispatch exact-copies a complete grant identity; Tier-1 consumes no grant. |
| A-6 | `[A] derived` | No PG transaction spans network I/O; post-network evidence cannot return to operation/command/domain owners. |
| A-7 | `[A] derived` | One physical call has one immutable exposure and uncertain crash outcomes are conservatively accounted. |
| A-8 | `[A] derived` | D3b response/failure receipts require a committed exposure, while `no_exposure` forbids response-envelope fields; valid strict-D3 simulate/scripted D0 responses therefore require the same durable exposure identity as live. |
| R-1 | `[R] new` | `CostLedgerRepository` via `store.repos.cost_ledger` is the sole future writer. |
| R-2 | `[R] new` | `cost_reservations` and `dispatch_exposures` are inseparable parent/child tables with the exact 21/75-column manifests below. |
| R-3 | `[R] new` | Every row and physical key begins with the exact five-field scope prefix. |
| R-4 | `[R] new` | Correcting the superseded live-only draft from A-8 evidence: initial physical v1 admits exactly `live`, `simulate`, and `scripted`; all three create durable rows, live money is positive, simulate/scripted money is zero, and global-D3 `replay` requires a future D0 envelope plus schema revision before admission. |
| R-5 | `[R] new` | All stored money is USD `NUMERIC(38,12)`. Immutable budget/pricing records and validated `ModelUsage` alone derive `R/W/A` with canonical `Decimal` arithmetic and positive `ROUND_CEILING`; caller money and FX are forbidden. |
| R-6 | `[R] new` | Parent conservation is `reserved + overrun = available + held + accounted + released`. |
| R-7 | `[R] new` | The eight mutating repository methods and their exact replay/CAS surfaces are closed below; none accepts caller-supplied `reserved_amount`, `worst_case_amount`, `observed_amount`, or `accounted_amount`. |
| R-8 | `[R] new` | Provider-call identity remains receipt-owned; exposure rows contain only nullable receipt references, never provider-call columns. |
| R-9 | `[R] new` | Post-network evidence terminalizes only the exposure and marks parent settlement pending; it never locks the reservation. Cost terminal evidence is one self-contained canonical record built and persisted by the cost owner, not an opaque caller proof reference. |
| R-10 | `[R] new` | Credential-free settlement locks reservation then pending exposures in exposure-id order, and updates the parent only after all child locks are held. |
| R-11 | `[R] new` | `uncertain` is terminal; a late invoice belongs to a future append-only adjustment owner and never rewrites exposure history. |
| R-12 | `[R] new` | Rows have no delete API and remain retained while any lifecycle, receipt, quarantine, terminal, registry-history, or audit reference exists. |

## 3. Owner, aggregate, and initial-v1 eligibility

The sole future owner is exactly
`src/sourcing_agent/repositories/cost_ledger.py::CostLedgerRepository`, exposed only as
`store.repos.cost_ledger`. That module owns both table descriptors, all mutations, `COST_BUDGET_POLICY_SPECS`,
`COST_PRICING_SPECS`, `COST_RECONCILIATION_SPECS`, and the private
`DispatchAuthorizationCapabilityV1` and `CostTerminalEvidenceRecord` factories. No generic Store facade, transport helper, reducer, receipt repository, or
reconciliation daemon may write these rows. Cross-owner work enters through typed repository methods; readers never
repair or enqueue. The registered low-level transport adapter is authoritative only for its closed wire observation; it
cannot write the ledger or construct JSON/digests/money. The cost repository validates that typed observation and is the
sole issuer/persistence owner of the resulting canonical cost evidence record in the same terminal CAS.

The immutable key prefix is exactly:

```text
(runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id)
```

Every component is physically present and included in PKs, unique keys, the child-parent FK, lookup indexes, every CAS,
idempotency scope, and authorization predicate. Initial physical v1 `provider_mode` is exactly one of
`live|simulate|scripted`, and positive review lineage is mandatory; there are no empty/zero brownfield identity
sentinels because both tables are new. Every replay/collision comparison includes exact provider-mode equality. The
same logical ids in different modes are distinct PFX identities, never an exact-replay hit or fallback.

Initial schema v1 is a deliberately closed transport variant: `transport_kind='model_tool_v1'`, eligible only for
`provider_mode IN ('live', 'simulate', 'scripted')`. This makes
`requested_model`, `model_safe_schema_revision`, route snapshot, and the other D0 model-envelope pins truthfully
non-null. A provider-search or other transport may not reuse blanks or fabricated model values; it requires a later
owner-ratified transport variant/schema before it can write either table.

Strict-D3 live, simulate, and scripted model turns all create one reservation plus one exposure per physical call.
This corrects the earlier live-only draft: D3b transport response/failure receipts require a committed exposure, while
the `no_exposure` branch forbids a fabricated D0 envelope or response fields, so a valid non-live response would
otherwise be unrepresentable. For every committed eligible-mode exposure,
`ModelInvocationEnvelopeV1.cost_exposure_ref == dispatch_exposure_id` exactly. `cost_exposure_id`,
`physical_call_exposure_id`, and other aliases are forbidden. Non-strict D0 fixtures without a durable owner-issued
exposure may still use typed `None`, but cannot enter the strict-D3 receipt/terminal path.

The broader global D3 scope grammar retains `replay`, but current `ModelInvocationEnvelopeV1.provider_mode` is closed to
`simulate|scripted|live`. D3c2g therefore does not invent replay model-envelope support: replay has no applicable v1
pricing spec and is rejected by both v1 table mode checks, so it creates no ledger row and fails closed until a future
D0 envelope revision, schema revision, and new owner-ratified eligibility decision.

## 4. Exact `cost_reservations` ordered manifest — 21 columns

| position | column | SQL type | nullable | default |
|---:|---|---|---|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `budget_reservation_ref` | `TEXT` | no | none |
| 7 | `operation_run_id` | `TEXT` | no | none |
| 8 | `reservation_idempotency_key` | `TEXT` | no | none |
| 9 | `budget_policy_digest` | `TEXT` | no | none |
| 10 | `currency_code` | `TEXT` | no | none |
| 11 | `reserved_amount` | `NUMERIC(38,12)` | no | none |
| 12 | `available_amount` | `NUMERIC(38,12)` | no | none |
| 13 | `held_amount` | `NUMERIC(38,12)` | no | none |
| 14 | `accounted_amount` | `NUMERIC(38,12)` | no | none |
| 15 | `released_amount` | `NUMERIC(38,12)` | no | none |
| 16 | `overrun_amount` | `NUMERIC(38,12)` | no | none |
| 17 | `reservation_state` | `TEXT` | no | none |
| 18 | `state_version` | `BIGINT` | no | `0` |
| 19 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 20 | `updated_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 21 | `closed_at` | `TIMESTAMPTZ` | yes | none |

Creation starts exactly at `(reserved, available, held, accounted, released, overrun) = (R, R, 0, 0, 0, 0)`, where
`R > 0` for live and `R = 0` for simulate/scripted. Replay is rejected by the initial-v1 physical mode check; a later
D0/transport/schema revision must explicitly ratify its money relation rather than relying on this v1.

## 5. Exact `dispatch_exposures` ordered manifest — 75 columns

The Scout worksheet used bare `NUMERIC` for three child amounts. D3c2g deliberately corrects that internal
inconsistency to `NUMERIC(38,12)` so parent and child money never cross a scale boundary; the column count remains 75.

| position | column | SQL type | nullable | default |
|---:|---|---|---|---|
| 1 | `runtime_namespace` | `TEXT` | no | none |
| 2 | `provider_mode` | `TEXT` | no | none |
| 3 | `workspace_id` | `TEXT` | no | none |
| 4 | `scope_digest` | `TEXT` | no | none |
| 5 | `coordination_plan_review_id` | `BIGINT` | no | none |
| 6 | `dispatch_exposure_id` | `TEXT` | no | none |
| 7 | `budget_reservation_ref` | `TEXT` | no | none |
| 8 | `operation_run_id` | `TEXT` | no | none |
| 9 | `command_id` | `TEXT` | no | none |
| 10 | `activity_run_id` | `TEXT` | no | none |
| 11 | `activity_attempt_id` | `TEXT` | no | none |
| 12 | `physical_call_index` | `BIGINT` | no | none |
| 13 | `command_attempt` | `BIGINT` | no | none |
| 14 | `claim_generation` | `BIGINT` | no | none |
| 15 | `control_epoch` | `BIGINT` | no | none |
| 16 | `claim_authority_spec_digest` | `TEXT` | no | none |
| 17 | `terminal_provenance_policy_digest` | `TEXT` | no | none |
| 18 | `d3_business_fence_digest` | `TEXT` | no | none |
| 19 | `plan_id` | `TEXT` | no | none |
| 20 | `plan_bundle_digest` | `TEXT` | no | none |
| 21 | `plan_revision` | `BIGINT` | no | none |
| 22 | `review_revision` | `BIGINT` | no | none |
| 23 | `gate_control_epoch` | `BIGINT` | no | none |
| 24 | `gate_revision` | `BIGINT` | no | none |
| 25 | `gate_blocking_reason_digest` | `TEXT` | no | none |
| 26 | `base_intent_id` | `TEXT` | yes | none |
| 27 | `base_intent_phase_generation` | `BIGINT` | yes | none |
| 28 | `expected_predecessor_intent_id` | `TEXT` | yes | none |
| 29 | `expected_predecessor_phase_generation` | `BIGINT` | yes | none |
| 30 | `expected_predecessor_source_control_epoch` | `BIGINT` | yes | none |
| 31 | `expected_predecessor_decision_source_event_id` | `TEXT` | yes | none |
| 32 | `fingerprint_version` | `TEXT` | no | none |
| 33 | `fingerprint_digest` | `TEXT` | no | none |
| 34 | `decision_generation` | `BIGINT` | no | none |
| 35 | `decision_source_event_id` | `TEXT` | no | none |
| 36 | `accepted_policy_revision` | `TEXT` | no | none |
| 37 | `schema_revision` | `TEXT` | no | none |
| 38 | `route_revision` | `TEXT` | no | none |
| 39 | `effective_route_snapshot_digest` | `TEXT` | no | none |
| 40 | `grant_tier` | `TEXT` | no | none |
| 41 | `grant_id` | `TEXT` | yes | none |
| 42 | `grant_issuance_generation` | `BIGINT` | yes | none |
| 43 | `grant_policy_revision` | `TEXT` | yes | none |
| 44 | `route_id` | `TEXT` | no | none |
| 45 | `effective_route_snapshot_ref` | `TEXT` | no | none |
| 46 | `provider` | `TEXT` | no | none |
| 47 | `api_style` | `TEXT` | no | none |
| 48 | `requested_model` | `TEXT` | no | none |
| 49 | `transport_kind` | `TEXT` | no | none |
| 50 | `transport_spec_digest` | `TEXT` | no | none |
| 51 | `permission_scope_revision` | `TEXT` | no | none |
| 52 | `outbound_policy_revision` | `TEXT` | no | none |
| 53 | `model_safe_schema_revision` | `TEXT` | no | none |
| 54 | `canonical_request_digest` | `TEXT` | no | none |
| 55 | `cost_pricing_spec_digest` | `TEXT` | no | none |
| 56 | `worst_case_amount` | `NUMERIC(38,12)` | no | none |
| 57 | `observed_amount` | `NUMERIC(38,12)` | yes | none |
| 58 | `accounted_amount` | `NUMERIC(38,12)` | yes | none |
| 59 | `usage_status` | `TEXT` | yes | none |
| 60 | `exposure_state` | `TEXT` | no | none |
| 61 | `parent_settlement_state` | `TEXT` | no | none |
| 62 | `state_version` | `BIGINT` | no | `0` |
| 63 | `cost_reconciliation_spec_digest` | `TEXT` | yes | none |
| 64 | `transport_response_receipt_id` | `TEXT` | yes | none |
| 65 | `transport_attempt_failure_receipt_id` | `TEXT` | yes | none |
| 66 | `cost_terminal_evidence_variant` | `TEXT` | yes | none |
| 67 | `cost_terminal_evidence_id` | `TEXT` | yes | none |
| 68 | `cost_terminal_evidence_digest` | `TEXT` | yes | none |
| 69 | `cost_terminal_evidence_record` | `JSONB` | yes | none |
| 70 | `created_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |
| 71 | `dispatching_at` | `TIMESTAMPTZ` | yes | none |
| 72 | `sent_at` | `TIMESTAMPTZ` | yes | none |
| 73 | `reconciled_at` | `TIMESTAMPTZ` | yes | none |
| 74 | `parent_settled_at` | `TIMESTAMPTZ` | yes | none |
| 75 | `updated_at` | `TIMESTAMPTZ` | no | `transaction_timestamp()` |

`provider_call_id`, `provider_request_id`, `provider_run_id`, `wire_call_id`, `response_occurrence_id`, and
`failure_occurrence_id` are deliberately absent. Their stable identities belong to the response/failure receipt owners;
the exposure keeps only nullable receipt row references.

## 6. Exact keys, foreign key, and indexes

`PFX` below expands to the exact five-field scope prefix in §3; it is documentation shorthand only and never a stored
column or digest replacement.

| order | name | kind | exact columns | action |
|---:|---|---|---|---|
| 1 | `cost_reservations_pkey` | `PRIMARY KEY` | `PFX, budget_reservation_ref` | identity |
| 2 | `cost_reservations_operation_idempotency_uk` | `UNIQUE` | `PFX, operation_run_id, reservation_idempotency_key` | exact replay collision key |
| 3 | `dispatch_exposures_pkey` | `PRIMARY KEY` | `PFX, dispatch_exposure_id` | identity and D0 cost reference target |
| 4 | `dispatch_exposures_physical_call_uk` | `UNIQUE` | `PFX, budget_reservation_ref, activity_attempt_id, physical_call_index` | one exposure per physical call |
| 5 | `dispatch_exposures_reservation_fk` | `FOREIGN KEY` | `PFX, budget_reservation_ref` | references parent identical columns; `MATCH SIMPLE DEFERRABLE INITIALLY IMMEDIATE ON UPDATE RESTRICT ON DELETE RESTRICT` |

| order | name | table | exact columns |
|---:|---|---|---|
| 1 | `cost_reservations_scope_operation_state_idx` | `cost_reservations` | `PFX, operation_run_id, reservation_state` |
| 2 | `dispatch_exposures_parent_settlement_idx` | `dispatch_exposures` | `PFX, budget_reservation_ref, parent_settlement_state, dispatch_exposure_id` |
| 3 | `dispatch_exposures_scope_attempt_call_idx` | `dispatch_exposures` | `PFX, activity_attempt_id, physical_call_index` |

Plan/review, OperationRun, command, ActivityRun/Attempt, grant, and response/failure receipt FKs remain Migration-C
work until their complete scope-prefixed parent unique keys exist. Later batches must install those full FKs; they may
not substitute a weaker id-only FK, JSON comparison, or application-only claim.

## 7. Exact local checks

The later new-table migration installs these checks immediately; unlike brownfield `ALTER TABLE`, it has no sentinel
population to protect. Cross-row equality and registry applicability remain repository/FK acceptance, not fictional
`CHECK` expressions.

### 7.1 Parent checks — exact 16

| order | constraint name | exact predicate |
|---:|---|---|
| 1 | `cost_reservations_runtime_namespace_nonblank_ck` | `runtime_namespace ~ '[^[:space:]]'` |
| 2 | `cost_reservations_provider_mode_ck` | `provider_mode IN ('live', 'simulate', 'scripted')` |
| 3 | `cost_reservations_workspace_id_nonblank_ck` | `workspace_id ~ '[^[:space:]]'` |
| 4 | `cost_reservations_scope_digest_shape_ck` | `scope_digest ~ '^[0-9a-f]{64}$'` |
| 5 | `cost_reservations_coordination_positive_ck` | `coordination_plan_review_id > 0` |
| 6 | `cost_reservations_identity_nonblank_ck` | `budget_reservation_ref ~ '[^[:space:]]' AND operation_run_id ~ '[^[:space:]]' AND reservation_idempotency_key ~ '[^[:space:]]'` |
| 7 | `cost_reservations_budget_policy_digest_shape_ck` | `budget_policy_digest ~ '^[0-9a-f]{64}$'` |
| 8 | `cost_reservations_currency_usd_ck` | `currency_code = 'USD'` |
| 9 | `cost_reservations_reserved_mode_relation_ck` | `(provider_mode = 'live' AND reserved_amount > 0) OR (provider_mode IN ('simulate', 'scripted') AND reserved_amount = 0)` |
| 10 | `cost_reservations_amounts_mode_relation_ck` | `(provider_mode = 'live' AND available_amount >= 0 AND held_amount >= 0 AND accounted_amount >= 0 AND released_amount >= 0 AND overrun_amount >= 0) OR (provider_mode IN ('simulate', 'scripted') AND available_amount = 0 AND held_amount = 0 AND accounted_amount = 0 AND released_amount = 0 AND overrun_amount = 0)` |
| 11 | `cost_reservations_conservation_ck` | `reserved_amount + overrun_amount = available_amount + held_amount + accounted_amount + released_amount` |
| 12 | `cost_reservations_state_ck` | `reservation_state IN ('open', 'closing', 'closed')` |
| 13 | `cost_reservations_state_version_nonnegative_ck` | `state_version >= 0` |
| 14 | `cost_reservations_closed_at_shape_ck` | `(reservation_state = 'closed') = (closed_at IS NOT NULL)` |
| 15 | `cost_reservations_closed_balances_ck` | `reservation_state <> 'closed' OR (available_amount = 0 AND held_amount = 0)` |
| 16 | `cost_reservations_timestamp_order_ck` | `updated_at >= created_at AND (closed_at IS NULL OR closed_at >= created_at)` |

### 7.2 Child checks — exact 29

| order | constraint name | exact predicate |
|---:|---|---|
| 1 | `dispatch_exposures_runtime_namespace_nonblank_ck` | `runtime_namespace ~ '[^[:space:]]'` |
| 2 | `dispatch_exposures_provider_mode_ck` | `provider_mode IN ('live', 'simulate', 'scripted')` |
| 3 | `dispatch_exposures_workspace_id_nonblank_ck` | `workspace_id ~ '[^[:space:]]'` |
| 4 | `dispatch_exposures_scope_digest_shape_ck` | `scope_digest ~ '^[0-9a-f]{64}$'` |
| 5 | `dispatch_exposures_coordination_positive_ck` | `coordination_plan_review_id > 0` |
| 6 | `dispatch_exposures_identity_nonblank_ck` | `dispatch_exposure_id ~ '[^[:space:]]' AND budget_reservation_ref ~ '[^[:space:]]' AND operation_run_id ~ '[^[:space:]]' AND command_id ~ '[^[:space:]]' AND activity_run_id ~ '[^[:space:]]' AND activity_attempt_id ~ '[^[:space:]]'` |
| 7 | `dispatch_exposures_physical_call_index_nonnegative_ck` | `physical_call_index >= 0` |
| 8 | `dispatch_exposures_claim_numbers_ck` | `command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0` |
| 9 | `dispatch_exposures_claim_digests_shape_ck` | `claim_authority_spec_digest ~ '^[0-9a-f]{64}$' AND terminal_provenance_policy_digest ~ '^[0-9a-f]{64}$' AND d3_business_fence_digest ~ '^[0-9a-f]{64}$'` |
| 10 | `dispatch_exposures_plan_pin_shape_ck` | `plan_id ~ '[^[:space:]]' AND plan_bundle_digest ~ '^[0-9a-f]{64}$' AND plan_revision >= 0 AND review_revision >= 0 AND gate_control_epoch >= 0 AND gate_revision >= 0 AND gate_blocking_reason_digest ~ '^[0-9a-f]{64}$'` |
| 11 | `dispatch_exposures_base_intent_tuple_ck` | `((base_intent_id IS NULL AND base_intent_phase_generation IS NULL) OR (base_intent_id ~ '[^[:space:]]' AND base_intent_phase_generation > 0)) IS TRUE` |
| 12 | `dispatch_exposures_predecessor_tuple_ck` | `((expected_predecessor_intent_id IS NULL AND expected_predecessor_phase_generation IS NULL AND expected_predecessor_source_control_epoch IS NULL AND expected_predecessor_decision_source_event_id IS NULL) OR (expected_predecessor_intent_id ~ '[^[:space:]]' AND expected_predecessor_phase_generation > 0 AND expected_predecessor_source_control_epoch >= 0 AND expected_predecessor_decision_source_event_id ~ '[^[:space:]]')) IS TRUE` |
| 13 | `dispatch_exposures_decision_pin_shape_ck` | `fingerprint_version ~ '[^[:space:]]' AND fingerprint_digest ~ '^[0-9a-f]{64}$' AND decision_generation > 0 AND decision_source_event_id ~ '[^[:space:]]' AND accepted_policy_revision ~ '[^[:space:]]' AND schema_revision ~ '[^[:space:]]' AND route_revision ~ '^[0-9a-f]{64}$' AND effective_route_snapshot_digest ~ '^[0-9a-f]{64}$'` |
| 14 | `dispatch_exposures_grant_tuple_ck` | `((grant_tier = 'tier1' AND grant_id IS NULL AND grant_issuance_generation IS NULL AND grant_policy_revision IS NULL) OR (grant_tier = 'tier2' AND grant_id ~ '[^[:space:]]' AND grant_issuance_generation > 0 AND grant_policy_revision ~ '[^[:space:]]')) IS TRUE` |
| 15 | `dispatch_exposures_transport_v1_shape_ck` | `transport_kind = 'model_tool_v1' AND provider_mode IN ('live', 'simulate', 'scripted') AND route_id ~ '[^[:space:]]' AND effective_route_snapshot_ref ~ '[^[:space:]]' AND provider ~ '[^[:space:]]' AND api_style ~ '[^[:space:]]' AND requested_model ~ '[^[:space:]]' AND permission_scope_revision ~ '[^[:space:]]' AND outbound_policy_revision ~ '[^[:space:]]' AND model_safe_schema_revision ~ '[^[:space:]]'` |
| 16 | `dispatch_exposures_transport_digests_shape_ck` | `transport_spec_digest ~ '^[0-9a-f]{64}$' AND canonical_request_digest ~ '^[0-9a-f]{64}$' AND cost_pricing_spec_digest ~ '^[0-9a-f]{64}$'` |
| 17 | `dispatch_exposures_worst_case_mode_relation_ck` | `(provider_mode = 'live' AND worst_case_amount > 0) OR (provider_mode IN ('simulate', 'scripted') AND worst_case_amount = 0)` |
| 18 | `dispatch_exposures_optional_amounts_mode_relation_ck` | `(provider_mode = 'live' AND (observed_amount IS NULL OR observed_amount >= 0) AND (accounted_amount IS NULL OR accounted_amount >= 0)) OR (provider_mode IN ('simulate', 'scripted') AND (observed_amount IS NULL OR observed_amount = 0) AND (accounted_amount IS NULL OR accounted_amount = 0))` |
| 19 | `dispatch_exposures_usage_status_ck` | `usage_status IS NULL OR usage_status IN ('reported', 'unavailable', 'invalid')` |
| 20 | `dispatch_exposures_state_ck` | `exposure_state IN ('prepared', 'dispatching', 'sent', 'confirmed', 'uncertain', 'no_call')` |
| 21 | `dispatch_exposures_parent_settlement_state_ck` | `parent_settlement_state IN ('not_ready', 'pending', 'applied')` |
| 22 | `dispatch_exposures_state_version_nonnegative_ck` | `state_version >= 0` |
| 23 | `dispatch_exposures_settlement_state_relation_ck` | `(exposure_state IN ('prepared', 'dispatching', 'sent') AND parent_settlement_state = 'not_ready') OR (exposure_state IN ('confirmed', 'uncertain', 'no_call') AND parent_settlement_state IN ('pending', 'applied'))` |
| 24 | `dispatch_exposures_receipt_exclusive_ck` | `transport_response_receipt_id IS NULL OR transport_attempt_failure_receipt_id IS NULL` |
| 25 | `dispatch_exposures_cost_evidence_shape_ck` | `((cost_reconciliation_spec_digest IS NULL OR cost_reconciliation_spec_digest ~ '^[0-9a-f]{64}$') AND ((cost_terminal_evidence_variant IS NULL AND cost_terminal_evidence_id IS NULL AND cost_terminal_evidence_digest IS NULL AND cost_terminal_evidence_record IS NULL) OR (cost_terminal_evidence_variant IS NOT NULL AND cost_terminal_evidence_id IS NOT NULL AND cost_terminal_evidence_digest IS NOT NULL AND cost_terminal_evidence_record IS NOT NULL AND cost_terminal_evidence_variant IN ('response_reported_v1', 'response_usage_unavailable_v1', 'response_usage_invalid_v1', 'attempt_failure_conservative_v1', 'timeout_crash_conservative_v1', 'prepared_no_dispatch_v1', 'zero_wire_abort_v1') AND cost_terminal_evidence_id ~ '[^[:space:]]' AND cost_terminal_evidence_digest ~ '^[0-9a-f]{64}$' AND jsonb_typeof(cost_terminal_evidence_record) = 'object'))) IS TRUE` |
| 26 | `dispatch_exposures_terminal_evidence_shape_ck` | `((exposure_state IN ('prepared', 'dispatching', 'sent') AND observed_amount IS NULL AND accounted_amount IS NULL AND usage_status IS NULL AND cost_reconciliation_spec_digest IS NULL AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL AND cost_terminal_evidence_variant IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'confirmed' AND observed_amount IS NOT NULL AND accounted_amount = observed_amount AND usage_status = 'reported' AND cost_reconciliation_spec_digest IS NOT NULL AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL AND cost_terminal_evidence_variant = 'response_reported_v1' AND reconciled_at IS NOT NULL) OR (exposure_state = 'uncertain' AND observed_amount IS NULL AND accounted_amount = worst_case_amount AND cost_reconciliation_spec_digest IS NOT NULL AND reconciled_at IS NOT NULL AND ((cost_terminal_evidence_variant = 'response_usage_unavailable_v1' AND usage_status = 'unavailable' AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL) OR (cost_terminal_evidence_variant = 'response_usage_invalid_v1' AND usage_status = 'invalid' AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL) OR (cost_terminal_evidence_variant = 'attempt_failure_conservative_v1' AND usage_status = 'unavailable' AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NOT NULL) OR (cost_terminal_evidence_variant = 'timeout_crash_conservative_v1' AND usage_status = 'unavailable' AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL))) OR (exposure_state = 'no_call' AND observed_amount = 0 AND accounted_amount = 0 AND usage_status IS NULL AND cost_reconciliation_spec_digest IS NOT NULL AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL AND cost_terminal_evidence_variant IN ('prepared_no_dispatch_v1', 'zero_wire_abort_v1') AND reconciled_at IS NOT NULL)) IS TRUE` |
| 27 | `dispatch_exposures_timestamp_order_ck` | `updated_at >= created_at AND (dispatching_at IS NULL OR dispatching_at >= created_at) AND (sent_at IS NULL OR (dispatching_at IS NOT NULL AND sent_at >= dispatching_at)) AND (reconciled_at IS NULL OR reconciled_at >= created_at) AND (parent_settled_at IS NULL OR (reconciled_at IS NOT NULL AND parent_settled_at >= reconciled_at))` |
| 28 | `dispatch_exposures_state_timestamp_shape_ck` | `((exposure_state = 'prepared' AND dispatching_at IS NULL AND sent_at IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'dispatching' AND dispatching_at IS NOT NULL AND sent_at IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'sent' AND dispatching_at IS NOT NULL AND sent_at IS NOT NULL AND reconciled_at IS NULL) OR (exposure_state = 'confirmed' AND dispatching_at IS NOT NULL AND sent_at IS NOT NULL AND reconciled_at IS NOT NULL) OR (exposure_state = 'uncertain' AND dispatching_at IS NOT NULL AND reconciled_at IS NOT NULL AND (cost_terminal_evidence_variant NOT IN ('response_usage_unavailable_v1', 'response_usage_invalid_v1') OR sent_at IS NOT NULL)) OR (exposure_state = 'no_call' AND sent_at IS NULL AND reconciled_at IS NOT NULL AND ((cost_terminal_evidence_variant = 'prepared_no_dispatch_v1' AND dispatching_at IS NULL) OR (cost_terminal_evidence_variant = 'zero_wire_abort_v1' AND dispatching_at IS NOT NULL)))) IS TRUE` |
| 29 | `dispatch_exposures_parent_settled_at_shape_ck` | `(parent_settlement_state = 'applied') = (parent_settled_at IS NOT NULL)` |

The base-intent pair and four-field predecessor tuple are independently all-null or complete. A complete tuple's
cross-row meaning is still exact-compared under repository locks; the local checks only reject half tuples.
The nullable tuple and terminal predicates end in explicit `IS TRUE` so PostgreSQL CHECK UNKNOWN is rejected; an
all-NULL evidence tuple, half grant/intent tuple, or NULL comparison can never pass through three-valued logic.

## 8. Immutable budget, pricing, reconciliation, and cost-evidence contracts

### 8.1 One canonical encoder and exact Decimal arithmetic

`COST_BUDGET_POLICY_SPECS`, `COST_PRICING_SPECS`, `COST_RECONCILIATION_SPECS`, and every embedded
`CostTerminalEvidenceRecord` use `canonical-cost-record-v1`. The encoder accepts the exact declared keyset only; encodes
UTF-8 compact JSON with keys in lexicographic order and separators `,`/`:`; encodes integers as canonical base-10 JSON
numbers; declared booleans as JSON `true|false`; tuples as ordered arrays; and every Decimal as a quoted fixed-point
string. A boolean in an integer/string/Decimal field, binary floats, exponents, plus signs, negative zero, padded integer
parts, unknown keys, or omitted keys are rejected. Text must already be Unicode NFC; the encoder rejects rather than
silently normalizes any other form. Registry digests are
`sha256(<ASCII domain tag> + 0x00 + canonical_json(record))`, using respectively
`cost-budget-policy-v1`, `cost-pricing-spec-v1`, and `cost-reconciliation-spec-v1` as the domain tag.

Stored USD amounts use canonical `usd-amount-v1` strings matching
`0\.[0-9]{12}|[1-9][0-9]*\.[0-9]{12}` and fitting `NUMERIC(38,12)`. Per-token rates use canonical
`usd-rate-v1` strings matching `0\.[0-9]{18}|[1-9][0-9]*\.[0-9]{18}` and fitting `DECIMAL(38,18)`. Every token/count
integer is in canonical signed-`BIGINT` non-negative range `0..9223372036854775807`. Arithmetic parses strings into
exact `Decimal` under a local precision-80 context, traps any rounding/inexact result during multiplication and sum,
uses no binary float, and then permits rounding only for the single final quantization to `0.000000000001` with
`ROUND_CEILING`. The declared input bounds need at most 59 coefficient digits before that final quantization. Negative
values, context overflow, and a quantized integer part longer than 26 digits fail closed.

The only billable quantities are this fixed ordered four-tuple:

```text
uncached_input_tokens = input_tokens - cached_input_tokens
cached_input_tokens = cached_input_tokens
non_reasoning_output_tokens = output_tokens - reasoning_output_tokens
reasoning_output_tokens = reasoning_output_tokens
```

`total_tokens` is validation-only and is never a fifth billed component. A complete reported `ModelUsage` is valid for
cost only when all five fields are present non-negative integers, `cached_input_tokens <= input_tokens`,
`reasoning_output_tokens <= output_tokens`, and `total_tokens = input_tokens + output_tokens`. Missing reported fields,
boolean/coerced values, or any inconsistent relation selects an unavailable/invalid conservative evidence variant; it
never substitutes zero and never produces `A`.

The variant split is exact: absent, incomplete, non-integer, boolean, out-of-range, or otherwise non-canonical usage is
`response_usage_unavailable_v1`; a complete canonical five-integer record that fails either overlap relation, the total
equality, or the final `NUMERIC(38,12)` amount bound is `response_usage_invalid_v1`. Thus the invalid variant can embed
its complete canonical-but-cost-invalid usage record, while unavailable usage is never fabricated into one.

For a four-quantity vector `Q` and the exact pricing-rate vector `P`, the single amount function is:

```text
price_v1(Q, P) = quantize_ceiling_12(sum(Q[i] * Decimal(P[i].usd_per_token) for i in fixed_four_order))
```

### 8.2 `COST_BUDGET_POLICY_SPECS`: reservation and per-call ceilings

`COST_BUDGET_POLICY_SPECS` is the sole immutable budget-policy owner. Its exact canonical record keyset is:

```text
budget_policy_id, schema_version, transport_kind, provider_mode, budget_class,
effective_route_snapshot_digest, cost_pricing_spec_digest, currency_code, monetary_ceiling,
max_physical_calls, max_uncached_input_tokens_per_call, max_cached_input_tokens_per_call,
max_non_reasoning_output_tokens_per_call, max_reasoning_output_tokens_per_call,
max_total_tokens_per_call, reservation_amount_rule, per_call_worst_case_rule
```

For each eligible `(transport_kind, provider_mode, budget_class, effective_route_snapshot_digest,
cost_pricing_spec_digest)` tuple there is exactly one record. `currency_code='USD'`,
`reservation_amount_rule='monetary_ceiling_v1'`, and
`per_call_worst_case_rule='four_quantity_ceiling_v1'`. Token ceilings and `max_physical_calls` are canonical
non-negative integers with `max_physical_calls > 0`, and:

```text
max_total_tokens_per_call
= max_uncached_input_tokens_per_call + max_cached_input_tokens_per_call
  + max_non_reasoning_output_tokens_per_call + max_reasoning_output_tokens_per_call
R = Decimal(monetary_ceiling)
W = price_v1(the four per-call ceiling fields, applicable pricing component_rates)
```

Applicability and the registry preflight are exact by mode:

| provider mode | exact applicable-record count | exact `R/W` relation | reservation-coverage preflight |
|---|---:|---|---|
| `live` | one per route/snapshot/budget/pricing tuple | `R > 0 and W > 0` | `R >= max_physical_calls * W`, with exact Decimal multiplication and no overflow |
| `simulate` | one immutable zero-money policy per eligible tuple | `R = W = 0` | applicable pricing has four zero rates; coverage is exactly zero |
| `scripted` | one immutable zero-money policy per eligible tuple | `R = W = 0` | applicable pricing has four zero rates; coverage is exactly zero |
| `replay` | zero in initial v1 | not applicable | row creation fails closed |

Reservation creation looks up the immutable policy by its digest and derives `reserved_amount=available_amount=R`;
the caller cannot pass either value. Exposure preparation locks the parent, requires the child route/snapshot/pricing
pins to equal that policy, derives `worst_case_amount=W`, and counts already-created non-replay children under the same
parent lock. A new physical-call identity is rejected once the exact count reaches `max_physical_calls`; an exact replay
does not consume another slot. This parent-serialized count plus `R >= max_physical_calls * W` proves the reservation
covers the maximum physical-call population before any authorization.

### 8.3 `COST_PRICING_SPECS`: closed rates and owner-derived observed cost

`COST_PRICING_SPECS` is a checked-in immutable mapping from its domain-separated digest to this exact record keyset:

```text
pricing_spec_id, schema_version, transport_kind, provider_mode, provider, api_style, requested_model,
effective_route_snapshot_digest, currency_code, numeric_precision, numeric_scale, rate_scale,
rounding_mode, minimum_positive_quantum, fx_policy, component_rates
```

`component_rates` is exactly four records in the fixed order from §8.1. Each record has the exact keyset
`billing_quantity, unit, usd_per_token`; `unit='token'`; billing quantities are duplicate-free and equal the four closed
names; and `usd_per_token` is canonical `usd-rate-v1`. For each eligible `model_tool_v1` route/snapshot tuple,
applicability is mode-specific:

| provider mode | exact applicable-record count | `component_rates` | policy `R/W` / observed `A` |
|---|---:|---|---|
| `live` | one | exact four-record tuple, with at least one positive applicable rate | `R/W` positive; owner-derived `A` may be zero |
| `simulate` | one immutable zero-cost spec per eligible route/snapshot tuple | exact four-record tuple with all rates zero | `R/W/A` exactly zero |
| `scripted` | one immutable zero-cost spec per eligible route/snapshot tuple | exact four-record tuple with all rates zero | `R/W/A` exactly zero |
| `replay` | zero in initial v1 | not applicable | row creation fails closed |

All applicable v1 records exact-copy their mode and use `currency_code='USD'`, `numeric_precision=38`,
`numeric_scale=12`, `rate_scale=18`, `rounding_mode='ROUND_CEILING'`,
`minimum_positive_quantum='0.000000000001'`, and `fx_policy='forbidden'`. Price changes append a new immutable
mode-specific spec/digest and retain the old record while referenced. No wildcard mode/price, live settings lookup,
floating point, implicit currency conversion, or FX fallback is allowed.

For `usage_status='reported'`, the cost repository obtains `ModelUsage` only from the exact authenticated response
receipt/envelope owner, applies §8.1 validation and overlap subtraction, and derives `observed_amount=A` with
`price_v1`. If validation fails or usage is unavailable, `observed_amount` remains SQL NULL and the exposure reconciles
conservatively at `W`; missing usage is never zero. Callers may supply neither `R`, `W`, `A`, `accounted_amount`, a
Decimal string, a billing vector, nor a pricing/budget record. Same-digest registry mismatch fails closed.

### 8.4 Owner-issued self-contained `CostTerminalEvidenceRecord`

Opaque `no_call_proof_ref` and `uncertain_reconciliation_ref` do not exist. The sole persistent evidence owner is the
private `CostTerminalEvidenceRecord` factory inside `CostLedgerRepository`. It builds one canonical record while holding
the exact exposure and applicable receipt locks, and the same terminal CAS atomically stores its variant, id, digest,
and complete JSONB record in columns 66-69. Callers cannot supply a record, ref, id, digest, amount, timestamp, receipt
classification, byte count, or boolean send claim.

Every evidence record has this exact top-level keyset:

```text
schema_version, variant, runtime_namespace, provider_mode, workspace_id, scope_digest,
coordination_plan_review_id, dispatch_exposure_id, source_exposure_state, source_state_version,
source_updated_at, evidence_observed_at, payload
```

The factory exact-copies PFX/exposure identity and the locked pre-CAS state/version. Both timestamps come from PG and use
canonical UTC `YYYY-MM-DDTHH:MM:SS.ffffffZ`. The evidence id is the lowercase SHA-256 of
`cost-terminal-evidence-id-v1 + 0x00 + canonical_json([runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id, dispatch_exposure_id, source_state_version, variant])`; the
evidence digest is `sha256('cost-terminal-evidence-record-v1' + 0x00 + canonical_json(the complete record))`.
JSONB round-trip must reproduce the same canonical record and digest.

The seven variants and their exact payload keysets are:

| evidence variant | eligible locked source | required receipt | exact payload keyset | terminal / `sent_at` rule |
|---|---|---|---|---|
| `response_reported_v1` | `dispatching or sent` | authenticated response | `transport_response_receipt_id, model_invocation_envelope_digest, canonical_usage_record, billing_quantities, derived_observed_amount` | `confirmed`; dispatching source sets `sent_at` from this CAS DB clock, sent source preserves it |
| `response_usage_unavailable_v1` | `dispatching or sent` | authenticated response | `transport_response_receipt_id, model_invocation_envelope_digest, usage_status, usage_validation_code` | `uncertain`; dispatching source sets `sent_at` from this CAS DB clock, sent source preserves it |
| `response_usage_invalid_v1` | `dispatching or sent` | authenticated response | `transport_response_receipt_id, model_invocation_envelope_digest, canonical_usage_record, usage_validation_code` | `uncertain`; dispatching source sets `sent_at` from this CAS DB clock, sent source preserves it |
| `attempt_failure_conservative_v1` | `dispatching or sent` | authenticated attempt-failure | `transport_attempt_failure_receipt_id, failure_spec_digest, failure_code, retry_disposition` | `uncertain`; dispatching source leaves `sent_at` NULL, sent source preserves it |
| `timeout_crash_conservative_v1` | `dispatching or sent` | none; locked receipt absence | `reconciliation_spec_digest, eligible_at, response_receipt_absent, failure_receipt_absent` | `uncertain`; dispatching source leaves `sent_at` NULL, sent source preserves it |
| `prepared_no_dispatch_v1` | `prepared` | none; locked receipt absence | `dispatching_at, sent_at, response_receipt_absent, failure_receipt_absent` | `no_call`; both timestamps are canonical JSON null and parent vector is unchanged |
| `zero_wire_abort_v1` | `dispatching` | none | `runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id, dispatch_exposure_id, source_state_version, canonical_request_digest, transport_adapter_id, transport_adapter_revision, failure_phase, request_bytes_written, response_bytes_read, provider_call_id_state` | `no_call`; `sent_at` remains NULL and the held amount is released |

Every `response_receipt_absent`/`failure_receipt_absent` payload value is owner-derived JSON `true`; `false`, caller
input, or an unlocked absence test is invalid for that variant. The prepared variant's two timestamp payload values are
owner-derived JSON null, not omitted fields or caller values.

After `authorize_dispatch` commits, the repository returns one private `DispatchAuthorizationCapabilityV1` bound to
the exact PFX, `dispatch_exposure_id`, post-CAS `source_state_version`, `canonical_request_digest`, and frozen adapter
id/revision. It is the only input that allows the registered low-level transport adapter to attempt that physical call.
The adapter is the sole ephemeral issuer of sealed typed `ZeroWireObservationV1`; this is not a D3c2h receipt and has no
independent durable identity. Its private-constructor exact keyset is the fourteen fields in the
`zero_wire_abort_v1` payload, copied from that capability plus the closed wire outcome. The adapter may issue it only for
`failure_phase IN ('dns', 'connect', 'tls', 'before_first_request_byte')`, exact integer
`request_bytes_written=0`, exact integer `response_bytes_read=0`, and
`provider_call_id_state='not_observed'`. The cost factory exact-compares the eight exposure/request identity fields with
the locked exposure and the adapter id/revision with its frozen transport spec before embedding the complete
observation. A capability or observation
from another PFX, exposure, state version, request digest, or adapter is a typed collision with zero writes; it can
never release the current exposure's held amount. Dicts, exceptions, caller flags, positive/unknown byte counts, or a
provider-call id cannot produce `zero_wire_abort_v1` and instead reconcile conservatively. A crash that did not persist
this record can never be reconstructed as no-call. Exact replay is available only from the already-persisted terminal
record for that same exposure/version/digest; replaying an ephemeral observation is not an authority surface.

`timeout_crash_conservative_v1` is repository-derived: under the exposure lock, PG
`transaction_timestamp()` must be at or after `source_timestamp + minimum_age_seconds` from the immutable applicable
reconciliation spec, and both receipt families must be absent under their documented locks. No caller deadline or clock
is accepted. Existing terminal evidence exact-replays only when stored id/digest/record, receipt pins, amounts, and
terminal state are all exact; any drift is a typed collision with zero writes.

### 8.5 `COST_RECONCILIATION_SPECS`: exact evidence-to-state rules

`COST_RECONCILIATION_SPECS` has this exact record keyset:

```text
spec_id, schema_version, terminal_state, evidence_variant, eligible_source_states,
required_receipt_variant, usage_status_policy, accounted_amount_rule, parent_delta_rule,
sent_at_rule, minimum_age_seconds, terminal, late_invoice_policy
```

It contains exactly these seven v1 records:

| spec id | terminal | evidence variant | source | receipt / usage | amount and parent rule | `sent_at` rule | minimum age / late invoice |
|---|---|---|---|---|---|---|---|
| `cost-confirmed-response-reported-v1` | `confirmed` | `response_reported_v1` | `dispatching or sent` | response / complete valid reported usage | `A=price_v1(validated usage)`; held to accounted, release unused, record overrun | `set_db_clock_if_dispatching_else_preserve` | none / append-only adjustment |
| `cost-uncertain-response-unavailable-v1` | `uncertain` | `response_usage_unavailable_v1` | `dispatching or sent` | response / unavailable | `A=NULL; account W` | `set_db_clock_if_dispatching_else_preserve` | none / append-only adjustment |
| `cost-uncertain-response-invalid-v1` | `uncertain` | `response_usage_invalid_v1` | `dispatching or sent` | response / invalid | `A=NULL; account W` | `set_db_clock_if_dispatching_else_preserve` | none / append-only adjustment |
| `cost-uncertain-attempt-failure-v1` | `uncertain` | `attempt_failure_conservative_v1` | `dispatching or sent` | attempt-failure / unavailable | `A=NULL; account W` | `leave_null_if_dispatching_else_preserve` | none / append-only adjustment |
| `cost-uncertain-timeout-crash-v1` | `uncertain` | `timeout_crash_conservative_v1` | `dispatching or sent` | locked receipt absence / unavailable | `A=NULL; account W` | `leave_null_if_dispatching_else_preserve` | positive registry seconds / append-only adjustment |
| `cost-no-call-prepared-v1` | `no_call` | `prepared_no_dispatch_v1` | `prepared` | locked receipt absence / none | `A=0`; parent unchanged | `remain_null` | none / no adjustment |
| `cost-no-call-zero-wire-v1` | `no_call` | `zero_wire_abort_v1` | `dispatching` | sealed zero-wire observation / none | `A=0`; held to released | `remain_null` | none / no adjustment |

Every record has `terminal=true`. They apply to `live|simulate|scripted` only through an exposure with the exact same PFX
provider mode. An exposure exact-copies the applicable reconciliation digest at terminalization; cross-mode lookup is
forbidden and old specs remain immutable while referenced. Confirmed simulate/scripted usage may remain `reported`, but
its owner-derived observed/accounted money is exactly zero.

## 9. Repository CAS surface, lifecycle, and amount transitions

The mutating surface is exactly eight methods. Every method requires the full prefix, exact immutable pins,
`expected_state_version`, and an enumerated expected state. Exact replay returns the existing exact result with zero
writes only under exact provider-mode equality; cross-mode input is a scope mismatch, never a replay hit. Same-key field
drift is a typed collision; missing rows never mean zero cost or `no_call`. `R/W/A`, accounted deltas, timestamps, and
cost-evidence records are repository outputs only. No public or cross-owner method parameter may carry any of them.

| order | repository method | exact transition / effect |
|---:|---|---|
| 1 | `create_or_exact_replay_reservation` | absent to `open`; exact budget-policy lookup derives `(R,R,0,0,0,0)`, where live `R>0` and simulate/scripted `R=0`; replay ineligible; caller money forbidden |
| 2 | `prepare_or_exact_replay_exposure` | absent to `prepared/not_ready`; under the parent lock enforce the policy call-count cap and derive exact mode-specific `W` from ceiling quantities plus pricing; parent vector unchanged; caller money forbidden |
| 3 | `authorize_dispatch` | `prepared/not_ready` to `dispatching/not_ready`; live atomically moves `available -= W`, `held += W`; simulate/scripted make the same authorization CAS with zero vector delta; after commit return the private exposure/version/request-bound dispatch capability |
| 4 | `mark_sent` | `dispatching/not_ready` to `sent/not_ready`; parent unchanged; exposure-only CAS after the send attempt |
| 5 | `reconcile_exposure_terminal` | source/evidence-dependent closed mapping only: `prepared -> no_call(prepared_no_dispatch)`; `dispatching -> confirmed(response) / uncertain(response, failure, or timeout) / no_call(zero_wire)`; `sent -> confirmed(response) / uncertain(response, failure, or timeout)`; owner derives `A` or conservative `W/0`, installs the canonical evidence record, and sets `parent_settlement_state=pending`; parent unchanged |
| 6 | `settle_reservation_pending_exposures` | credential-free `pending` to `applied`; apply each child delta once under parent-first sorted locks |
| 7 | `begin_close_reservation` | parent `open` to `closing`; forbid new prepare/authorize while pending settlement drains |
| 8 | `finish_close_reservation` | parent `closing` to `closed`; require no held/pending/not-ready child, move remaining available to released, set `closed_at` |

Allowed exposure transitions are closed:

```text
prepared -> dispatching | no_call
dispatching -> sent | confirmed | uncertain | no_call
sent -> confirmed | uncertain
confirmed | uncertain | no_call -> terminal forever
```

All lifecycle clocks are repository-owned PostgreSQL clocks. Inserts and every CAS derive `created_at`,
`dispatching_at`, `sent_at`, `reconciled_at`, `parent_settled_at`, `updated_at`, and `closed_at` only from the current
transaction's `transaction_timestamp()`; callers may supply neither timestamps nor a boolean/flag that claims a send
occurred.

An authenticated response receipt proves wire send for both `confirmed` and response-backed `uncertain`. When either
branch starts at `dispatching`, the same terminal CAS fills the previously-null `sent_at` from PostgreSQL
`transaction_timestamp()`; this is the durable time send became proven, not a caller-estimated wire timestamp. A
`sent`-origin response transition preserves the already committed `sent_at`. A direct `dispatching -> uncertain` backed
only by attempt failure or timeout/crash leaves `sent_at` NULL; `dispatching -> no_call(zero_wire)` also leaves it NULL.
The evidence variant, not target state alone, owns this distinction.

`prepared -> no_call` is admitted only by the owner-built locked `prepared_no_dispatch_v1` evidence and has no prior
hold, so parent settlement is a no-op vector. `dispatching -> no_call` requires a complete owner-built
`zero_wire_abort_v1` record and moves `held -= W; released += W`. Confirmed settlement uses only owner-derived rounded
actual `A` and applies `held -= W; accounted += A; released += max(W-A,0); overrun += max(A-W,0)`. Every uncertain
variant applies `held -= W; accounted += W`. Every vector preserves:

```text
reserved_amount + overrun_amount
= available_amount + held_amount + accounted_amount + released_amount
```

Multiple prepared exposures may exist only within the budget policy's exact `max_physical_calls` count, serialized by
the parent lock. Only `authorize_dispatch` reserves available money; concurrent authorization CAS permits only live
calls whose owner-derived worst-case amounts still fit. Simulate/scripted execute the same durable authorization and
settlement CASs with `R=W=A=0`, so every money column and delta remains exactly zero while validated response usage may
still be `reported`. Replay has no initial-v1 row or CAS path. `uncertain` never transitions to `confirmed`.

## 10. Lock orders and the network boundary

There are exactly three write orders:

1. **Pre-network authorization:** acquire `d3-dispatch-v2`, then
   `operation root -> optional plan/review/gate -> participating commands sorted -> intent/predecessor -> ActivityRun/Attempt -> optional grant -> cost_reservations -> dispatch_exposures`.
   The reservation available-to-held move and exposure `prepared -> dispatching` commit in one PG UoW. Only after
   commit may the transport owner send once using that owner-issued dispatch capability for its exact exposure id.
2. **Post-network evidence:**
   `dispatch_exposures FOR UPDATE -> applicable receipt -> optional response quarantine`, then owner-build the embedded
   `CostTerminalEvidenceRecord` and commit exposure-only terminal state plus `parent_settlement_state=pending`. This UoW
   never locks or updates `cost_reservations`, and never returns to operation/command/intent/activity/domain rows. That
   restriction removes the exposure-to-parent lock inversion.
3. **Credential-free settlement/close:**
   `cost_reservations FOR UPDATE -> pending dispatch_exposures ORDER BY dispatch_exposure_id FOR UPDATE`; after every
   child is locked and exact-checked, update the parent vector, then mark children `applied` in the same sorted order.
   Closing uses the same parent-first order.

`mark_sent` is an exposure-only CAS. No method holds a DB transaction during DNS, connect, request bytes, response
streaming, provider polling, invoice lookup, or any other network I/O. Settlement credentials are neither accepted nor
read; it consumes only immutable rows, registry digests, authenticated receipt rows, and the complete owner-issued cost
evidence record already committed on the exposure.

## 11. Mode semantics, retention, and late invoices

- strict-D3 `live|simulate|scripted`: durable parent plus one child per physical call; every resulting envelope exact-copies
  `cost_exposure_ref=dispatch_exposure_id`;
- strict-D3 `live`: reservation/worst-case positive; strict-D3 `simulate|scripted`: every reservation/exposure/accounting
  amount exactly zero, but valid responses and reported usage still use the normal receipt/evidence path;
- `replay`: global PFX grammar retained, but no current D0 envelope enum or applicable pricing spec, so initial
  `model_tool_v1` row creation fails closed before network;
- non-strict D0 fixtures without the durable strict-D3 owner: no ledger row and typed `cost_exposure_ref=None`; that
  absence is not valid strict-D3 response/failure terminal evidence;
- any eligible strict-D3 mode missing context, registry, reservation, exposure, grant pins, or exact scope relation:
  typed fail-closed before network;
- Tier-1: `grant_tier='tier1'` and all three grant pins SQL NULL;
- Tier-2: `grant_tier='tier2'` and all three grant pins complete; half tuples fail locally and semantically;
- missing usage is never zero: `unavailable|invalid` terminalizes conservatively as `uncertain` at worst case;
- no provider-call id is inferred from model output or exposure text; receipts own it;
- no delete, purge, rewrite, or state-reopen API exists. Closed rows remain historical accounting evidence;
- `uncertain` is immutable. A later authenticated invoice uses a future append-only cost-adjustment owner/table, which
  may add a separate accounting entry but may not mutate the exposure state, original accounted amount, evidence,
  pricing digest, or settlement identity. That adjustment schema is not ratified here.

## 12. Mechanism × ten-invariant matrix — exactly 30 cells

| mechanism | 1 owner | 2 tenant | 3 fence | 4 lifecycle | 5 late/partial | 6 cost | 7 physical identity | 8 provenance | 9 consistency | 10 mode isolation |
|---|---|---|---|---|---|---|---|---|---|---|
| cost reservation aggregate root | sole repository owner; §3 | full PFX in identity/CAS; §3/§6 | state version and parent-first lock; §9/§10 | open to closing to closed; §9 | retained terminal history; §11 | budget policy derives R and covers max calls; §8/§9 | operation/idempotency unique; §6 | immutable budget-policy digest server-owned; §8 | one owner and one amount vector; §3/§9 | live positive, simulate/scripted zero, replay none; §8/§11 |
| dispatch exposure physical-call child | sole repository owner; §3 | full PFX in PK/unique/FK/CAS; §3/§6 | claim/epoch/attempt plus state version; §5/§9 | exact six states and settlement axis; §7/§9 | seven owner-built evidence variants; §7/§8 | policy ceilings derive W and validated usage derives A; §8/§9 | attempt/call unique and D0 ref equality; §3/§6 | provider call remains receipt-owned; cost evidence self-contained; §5/§8 | 75-field single manifest; §5 | live/simulate/scripted durable; replay deferred; §3/§11 |
| reconciliation registry and settlement flow | immutable registry plus repository CAS; §8/§9 | settlement selects one exact-mode PFX parent; §10 | pending to applied once; §7/§9 | terminal specs retained; late adjustment separate; §8/§11 | exposure-first evidence then parent-first settlement; §10 | live deltas or simulate/scripted zero deltas; §8/§9 | spec/evidence records and digests exact; §7/§8 | receipts typed by their owner; cost record built only by ledger owner; §8/§10 | seven exact records and one lock order; §8/§10 | pricing applicability exact by mode; replay none; §3/§8 |

Every one of the 30 cells is populated. No cell claims runtime implementation.

## 13. Physical absence baseline and executable oracle

At this decision point, `0001_baseline.sql` still declares exactly **83** distinct tables and repository source still
defines exactly **41** distinct `TableDescriptor` tables. Neither set contains `cost_reservations` or
`dispatch_exposures`. All current migrations/source also lack `CostLedgerRepository`, `store.repos.cost_ledger`,
`COST_BUDGET_POLICY_SPECS`, `COST_PRICING_SPECS`, `COST_RECONCILIATION_SPECS`, and
`DispatchAuthorizationCapabilityV1`/`CostTerminalEvidenceRecord`/`ZeroWireObservationV1` definitions. Those are
current facts; the names above are target ratifications, not implementation claims.

`tests/test_d3c2g_cost_ledger_decision_lock.py` independently locks:

- exact ordered 21/75 name/type/null/default manifests and the child money-scale correction;
- exact PK/unique/FK/index and 16/29 local-check manifests;
- exact eight-method CAS surface, six-state lifecycle, amount transitions, three lock orders, no-network rule,
  mode/retention semantics, replay rejection, and provider-call column prohibition;
- exact budget/pricing/reconciliation registry schemas, canonical Decimal/encoding rules, four non-overlapping billing
  quantities, usage-variant classification, max-call reservation coverage, domain-separated digest golden vectors, and
  seven reconciliation records;
- exact self-contained cost-evidence record/variant shapes, zero-wire authority, timeout/crash conservative path, and
  response-backed `sent_at` behavior, including cross-exposure zero-wire rejection and mutations that fail if overlap
  subtraction, owner-only amounts, evidence identity, or source-dependent timestamps drift;
- PostgreSQL three-valued NULL truth tables proving every nullable tuple/terminal predicate's explicit `IS TRUE`
  wrapper rejects UNKNOWN rather than accepting a forged terminal row;
- `[A]` versus `[R]` decision inventory and the complete 3×10 matrix;
- OB-2.2/OB-10.3 status `decision_locked_not_implemented` plus all non-closure tokens;
- mechanical 83-table/41-descriptor current absence and zero future owner/registry/store wiring;
- current generic codec/replace-all incompatibility and absent durable `model_invocation_envelope_ref` issuer.

## 14. Explicit non-closure and next bounded batch

D3c2g closes no runtime or rollout gate. It leaves open `R-019`, `R-023`, `R-027`, `R-029`, action-root durable scope,
OB-10.1, OB-10.2, OB-10.4, complete Migration A, Migration B-D, terminal-provenance registry implementation,
verification-intent DDL/CAS, response/failure receipt DDL, late-quarantine DDL, event transport-provenance columns,
upstream/receipt FKs, backfill/adoption, descriptor/repository wiring, strict writers, settlement daemon, provider/model
transport, live/W6/manual signoff, and served Agent population.

The next decision batch must ratify the still-deferred verification-intent, response/failure receipt, and late-
quarantine owners/manifests/CAS/retention before any SQL for those surfaces. D3c2g does not authorize a cost-ledger
migration by itself: the later cost-ledger implementation still needs its own bounded migration plan, real-PG
new-table/constraint/FK/index/rollback/lock acceptance, registry implementation, repository CAS tests, race mutations,
and fresh pinned independent review.

Because authenticated response/failure receipt physical schemas and locks are prerequisites of six of the seven cost
evidence variants, their deferral blocks a cost-ledger migration too: no partial cost-only DDL, nullable placeholder
receipt relation, caller proof bridge, or runtime writer may land first. The later migration plan must co-order the
ratified receipt owners/keys/FKs/locks with these exposure evidence columns and fail closed if that dependency is absent.

Two current substrate gaps are explicit implementation prerequisites, not hidden descriptor work. The generic
`control_plane_repository.Kind` has no exact `Decimal`/`NUMERIC(38,12)` or `TIMESTAMPTZ` codec family, and generic
`TableDescriptor.upsert_sql()` is `REPLACE_ALL`. A later cost-ledger implementation must first add exact Decimal plus
timezone-aware TIMESTAMPTZ codecs and specialized insert-once/exact-replay/CAS primitives; it may not encode money as
float/text, accept caller timestamps/amounts/evidence JSON, or route immutable ledger rows through generic replace-all
upsert. It must implement the exact owner-only canonical registries and embedded evidence factory before either table
can receive a runtime writer. D3c2g claims none of those primitives exist.

Receipt implementation also remains blocked on a durable D0 envelope owner/reference grammar. Current
`ModelInvocationEnvelopeV1` is an immutable in-memory value and source defines no `model_invocation_envelope_ref`
issuer; D3c2g does not fabricate one. Response-receipt DDL/runtime must wait for an owner-ratified durable envelope
identity, issuance/persistence UoW, tenant/mode scope, retention, and exact replay rule.

D3c2g does not resolve D3c2h's response-only quarantine `reconciled_no_call` reachability or receipt-side
`command_attempt` identity. Those remain explicit D3c2h ratifications; nothing here silently changes D3b receipt or
quarantine semantics.
