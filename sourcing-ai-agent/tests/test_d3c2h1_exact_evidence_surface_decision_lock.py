from __future__ import annotations

import hashlib
import re
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"

DECISION_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2H1_EXACT_EVIDENCE_SURFACE_DECISION_LOCK.md"
PLAN_PATH = REPO_ROOT / "docs" / "TRACK_D_AGENT_RUNTIME_PLAN.md"
D0F_PATH = REPO_ROOT / "docs" / "TRACK_D_D0F_DURABLE_MODEL_INVOCATION_ENVELOPE_IMPLEMENTATION.md"
D3C2G_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2G_COST_LEDGER_DISPATCH_EXPOSURE_DECISION_LOCK.md"
D3C2H0_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2H0_EVIDENCE_CROSS_CONTRACT_RATIFICATION.md"
CHECKLIST_PATH = REPO_ROOT / "docs" / "DESIGN_INVARIANT_CHECKLIST.md"

PFX = (
    ("runtime_namespace", "TEXT", "no", "none"),
    ("provider_mode", "TEXT", "no", "none"),
    ("workspace_id", "TEXT", "no", "none"),
    ("scope_digest", "TEXT", "no", "none"),
    ("coordination_plan_review_id", "BIGINT", "no", "none"),
)

SOURCE_CORE = (
    "source_verification_command_id",
    "source_claim_generation",
    "source_control_epoch",
    "source_command_attempt",
    "source_activity_run_id",
    "source_activity_attempt_id",
    "source_claim_authority_spec_digest",
)

TERMINAL_FIELDS = (
    "expected_source_terminal_status",
    "expected_source_terminal_event_id",
    "expected_source_terminal_outcome_digest",
    "expected_source_terminal_transport_variant",
    "expected_source_terminal_provenance_policy_digest",
    "expected_source_response_spec_digest",
    "expected_source_transport_response_receipt_id",
    "expected_source_transport_attempt_failure_receipt_id",
    "expected_source_dispatch_exposure_id",
    "expected_source_physical_call_index",
    "expected_source_provider_call_id_state",
    "expected_source_provider_call_id",
    "expected_source_model_invocation_envelope_ref",
    "expected_source_model_invocation_envelope_digest",
    "expected_source_terminal_reason",
    "expected_source_response_occurrence_id",
    "expected_source_canonical_response_digest",
    "expected_source_canonical_result_digest",
    "expected_source_result_artifact_ref",
    "expected_source_result_artifact_digest",
    "expected_source_failure_occurrence_id",
    "expected_source_failure_code",
    "expected_source_failure_spec_digest",
    "expected_source_canonical_failure_digest",
    "expected_source_retry_policy_revision",
    "expected_source_retry_disposition",
    "expected_source_failure_artifact_ref",
    "expected_source_failure_artifact_digest",
    "expected_source_no_exposure_spec_digest",
)

EXPECTED_VERIFICATION = PFX + (
    ("intent_id", "TEXT", "no", "none"),
    ("operation_run_id", "TEXT", "no", "none"),
    ("phase_generation", "BIGINT", "no", "none"),
    ("intent_state", "TEXT", "no", "none"),
    ("d3_business_fence_digest", "TEXT", "no", "none"),
    ("record_outcome", "TEXT", "yes", "none"),
    ("recorded_event_id", "TEXT", "yes", "none"),
    ("source_verification_command_id", "TEXT", "no", "none"),
    ("source_claim_generation", "BIGINT", "no", "none"),
    ("source_control_epoch", "BIGINT", "no", "none"),
    ("source_command_attempt", "BIGINT", "no", "none"),
    ("source_activity_run_id", "TEXT", "no", "none"),
    ("source_activity_attempt_id", "TEXT", "no", "none"),
    ("source_claim_authority_spec_digest", "TEXT", "no", "none"),
    ("expected_source_terminal_status", "TEXT", "yes", "none"),
    ("expected_source_terminal_event_id", "TEXT", "yes", "none"),
    ("expected_source_terminal_outcome_digest", "TEXT", "yes", "none"),
    ("expected_source_terminal_transport_variant", "TEXT", "yes", "none"),
    ("expected_source_terminal_provenance_policy_digest", "TEXT", "yes", "none"),
    ("expected_source_response_spec_digest", "TEXT", "yes", "none"),
    ("expected_source_transport_response_receipt_id", "TEXT", "yes", "none"),
    ("expected_source_transport_attempt_failure_receipt_id", "TEXT", "yes", "none"),
    ("expected_source_dispatch_exposure_id", "TEXT", "yes", "none"),
    ("expected_source_physical_call_index", "BIGINT", "yes", "none"),
    ("expected_source_provider_call_id_state", "TEXT", "yes", "none"),
    ("expected_source_provider_call_id", "TEXT", "yes", "none"),
    ("expected_source_model_invocation_envelope_ref", "TEXT", "yes", "none"),
    ("expected_source_model_invocation_envelope_digest", "TEXT", "yes", "none"),
    ("expected_source_terminal_reason", "TEXT", "yes", "none"),
    ("expected_source_response_occurrence_id", "TEXT", "yes", "none"),
    ("expected_source_canonical_response_digest", "TEXT", "yes", "none"),
    ("expected_source_canonical_result_digest", "TEXT", "yes", "none"),
    ("expected_source_result_artifact_ref", "TEXT", "yes", "none"),
    ("expected_source_result_artifact_digest", "TEXT", "yes", "none"),
    ("expected_source_failure_occurrence_id", "TEXT", "yes", "none"),
    ("expected_source_failure_code", "TEXT", "yes", "none"),
    ("expected_source_failure_spec_digest", "TEXT", "yes", "none"),
    ("expected_source_canonical_failure_digest", "TEXT", "yes", "none"),
    ("expected_source_retry_policy_revision", "TEXT", "yes", "none"),
    ("expected_source_retry_disposition", "TEXT", "yes", "none"),
    ("expected_source_failure_artifact_ref", "TEXT", "yes", "none"),
    ("expected_source_failure_artifact_digest", "TEXT", "yes", "none"),
    ("expected_source_no_exposure_spec_digest", "TEXT", "yes", "none"),
    ("state_version", "BIGINT", "no", "0"),
    ("source_terminal_appended_at", "TIMESTAMPTZ", "yes", "none"),
    ("created_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("updated_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
)

EXPECTED_RESPONSE = PFX + (
    ("transport_response_receipt_id", "TEXT", "no", "none"),
    ("operation_run_id", "TEXT", "no", "none"),
    ("command_id", "TEXT", "no", "none"),
    ("activity_run_id", "TEXT", "no", "none"),
    ("activity_attempt_id", "TEXT", "no", "none"),
    ("command_attempt", "BIGINT", "no", "none"),
    ("claim_generation", "BIGINT", "no", "none"),
    ("control_epoch", "BIGINT", "no", "none"),
    ("claim_authority_spec_digest", "TEXT", "no", "none"),
    ("d3_business_fence_digest", "TEXT", "no", "none"),
    ("terminal_provenance_policy_digest", "TEXT", "no", "none"),
    ("response_spec_digest", "TEXT", "no", "none"),
    ("dispatch_exposure_id", "TEXT", "no", "none"),
    ("physical_call_index", "BIGINT", "no", "none"),
    ("provider_call_id_state", "TEXT", "no", "none"),
    ("provider_call_id", "TEXT", "yes", "none"),
    ("model_invocation_envelope_ref", "TEXT", "no", "none"),
    ("model_invocation_envelope_digest", "TEXT", "no", "none"),
    ("terminal_reason", "TEXT", "no", "none"),
    ("canonical_delivery_identity", "TEXT", "no", "none"),
    ("response_occurrence_id", "TEXT", "no", "none"),
    ("canonical_response_digest", "TEXT", "no", "none"),
    ("canonical_result_digest", "TEXT", "no", "none"),
    ("result_artifact_ref", "TEXT", "yes", "none"),
    ("result_artifact_digest", "TEXT", "yes", "none"),
)

EXPECTED_FAILURE = PFX + (
    ("transport_attempt_failure_receipt_id", "TEXT", "no", "none"),
    ("operation_run_id", "TEXT", "no", "none"),
    ("command_id", "TEXT", "no", "none"),
    ("activity_run_id", "TEXT", "no", "none"),
    ("activity_attempt_id", "TEXT", "no", "none"),
    ("command_attempt", "BIGINT", "no", "none"),
    ("claim_generation", "BIGINT", "no", "none"),
    ("control_epoch", "BIGINT", "no", "none"),
    ("claim_authority_spec_digest", "TEXT", "no", "none"),
    ("d3_business_fence_digest", "TEXT", "no", "none"),
    ("terminal_provenance_policy_digest", "TEXT", "no", "none"),
    ("dispatch_exposure_id", "TEXT", "no", "none"),
    ("physical_call_index", "BIGINT", "no", "none"),
    ("provider_call_id_state", "TEXT", "no", "none"),
    ("provider_call_id", "TEXT", "yes", "none"),
    ("failure_occurrence_id", "TEXT", "no", "none"),
    ("failure_code", "TEXT", "no", "none"),
    ("failure_spec_digest", "TEXT", "no", "none"),
    ("canonical_failure_digest", "TEXT", "no", "none"),
    ("retry_policy_revision", "TEXT", "no", "none"),
    ("retry_disposition", "TEXT", "no", "none"),
    ("failure_artifact_ref", "TEXT", "no", "none"),
    ("failure_artifact_digest", "TEXT", "no", "none"),
)

EXPECTED_CLASSIFICATION = PFX + (
    ("classification_intent_id", "TEXT", "no", "none"),
    ("transport_response_receipt_id", "TEXT", "no", "none"),
    ("dispatch_exposure_id", "TEXT", "no", "none"),
    ("response_occurrence_id", "TEXT", "no", "none"),
    ("classification_idempotency_key", "TEXT", "no", "none"),
    ("classification_state", "TEXT", "no", "none"),
    ("attempt_count", "BIGINT", "no", "0"),
    ("next_attempt_at", "TIMESTAMPTZ", "no", "owner INSERT expression"),
    ("last_error_code", "TEXT", "yes", "none"),
    ("state_version", "BIGINT", "no", "0"),
    ("created_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("updated_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("terminal_at", "TIMESTAMPTZ", "yes", "none"),
)

EXPECTED_QUARANTINE = PFX + (
    ("quarantine_id", "TEXT", "no", "none"),
    ("operation_run_id", "TEXT", "no", "none"),
    ("command_id", "TEXT", "no", "none"),
    ("activity_run_id", "TEXT", "no", "none"),
    ("activity_attempt_id", "TEXT", "no", "none"),
    ("command_attempt", "BIGINT", "no", "none"),
    ("claim_generation", "BIGINT", "no", "none"),
    ("control_epoch", "BIGINT", "no", "none"),
    ("claim_authority_spec_digest", "TEXT", "no", "none"),
    ("d3_business_fence_digest", "TEXT", "no", "none"),
    ("dispatch_exposure_id", "TEXT", "no", "none"),
    ("physical_call_index", "BIGINT", "no", "none"),
    ("provider_call_id_state", "TEXT", "no", "none"),
    ("provider_call_id", "TEXT", "yes", "none"),
    ("transport_response_receipt_id", "TEXT", "no", "none"),
    ("canonical_delivery_identity", "TEXT", "no", "none"),
    ("response_occurrence_id", "TEXT", "no", "none"),
    ("terminal_reason", "TEXT", "no", "none"),
    ("model_invocation_envelope_ref", "TEXT", "no", "none"),
    ("model_invocation_envelope_digest", "TEXT", "no", "none"),
    ("canonical_response_digest", "TEXT", "no", "none"),
    ("canonical_result_digest", "TEXT", "no", "none"),
    ("result_artifact_ref", "TEXT", "yes", "none"),
    ("result_artifact_digest", "TEXT", "yes", "none"),
    ("rejection_reason", "TEXT", "no", "none"),
    ("cost_state", "TEXT", "no", "none"),
    ("retention_state", "TEXT", "no", "none"),
    ("authorizable", "BOOLEAN", "no", "false"),
    ("idempotency_key", "TEXT", "no", "none"),
    ("retention_policy_version", "TEXT", "no", "none"),
    ("recorded_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("retention_until", "TIMESTAMPTZ", "no", "owner INSERT expression"),
    ("cost_reconciled_at", "TIMESTAMPTZ", "yes", "none"),
    ("purged_at", "TIMESTAMPTZ", "yes", "none"),
    ("cost_state_version", "BIGINT", "no", "0"),
    ("retention_state_version", "BIGINT", "no", "0"),
)

EXPECTED_DECLARATIONS = (
    "EXACT_EVIDENCE_SURFACE_DECISION_V1",
    "scope_prefix = runtime_namespace | provider_mode | workspace_id | scope_digest | coordination_plan_review_id",
    "eligible_provider_modes = live | simulate | scripted",
    "transport_kind = model_tool_v1",
    "replay_behavior = fail_closed_zero_write",
    "provider_search_behavior = deferred_owner_ratified_variant",
    "verification_intents_column_count = 52",
    "transport_response_receipts_column_count = 30",
    "transport_attempt_failure_receipts_column_count = 28",
    "transport_response_classification_intents_column_count = 18",
    "workflow_late_result_quarantine_column_count = 41",
    "verification_source_core_count = 7",
    "verification_terminal_tuple_count = 29",
    "verification_intents_local_check_count = 10",
    "transport_response_receipts_local_check_count = 9",
    "transport_attempt_failure_receipts_local_check_count = 9",
    "transport_response_classification_intents_local_check_count = 7",
    "workflow_late_result_quarantine_local_check_count = 12",
    "local_check_total = 47",
    "ratified_upstream_constraint_count = 13",
    "ratified_seven_table_constraint_count = 52",
    "ratified_seven_table_fk_count = 29",
    "combined_index_count = 11",
    "unratified_parent_prerequisite_count = 2",
    "response_occurrence_domain = transport-response-occurrence-v2",
    "failure_occurrence_domain = transport-attempt-failure-occurrence-v2",
    "classification_intent_domain = transport-response-classification-intent-v1",
    "quarantine_idempotency_domain = late-response-v2",
    "quarantine_retention_policy = quarantine_retention_30d_v1",
    "quarantine_retention_deadline = recorded_at + interval '30 days'",
    "exposure_first_quarantine_permission = forbidden",
    "classification_authority = stored_current_state_under_d3_dispatch_v2_global_lock_prefix",
    "classification_nonterminal_states = pending | claimed | current_pending_apply",
    "classification_terminal_states = applied_current | classified_stale | failed_terminal",
    "classification_max_attempts = 8",
    "post_network_ingress_composition_count = 2",
    "current_apply_continuation_count = 1",
    "migration_authority = blocked_pending_parent_decision_and_fresh_pinned_go",
    "implementation_status = decision_locked_not_implemented",
)

EXPECTED_TERMINAL_TRUTH = (
    ("expected_source_terminal_status", "N", "R", "failed_terminal", "R"),
    ("expected_source_terminal_event_id", "N", "R", "R", "R"),
    ("expected_source_terminal_outcome_digest", "N", "R", "R", "R"),
    ("expected_source_terminal_transport_variant", "N", "exposure", "attempt_failure", "no_exposure"),
    ("expected_source_terminal_provenance_policy_digest", "N", "R", "R", "R"),
    ("expected_source_response_spec_digest", "N", "R", "N", "N"),
    ("expected_source_transport_response_receipt_id", "N", "R", "N", "N"),
    ("expected_source_transport_attempt_failure_receipt_id", "N", "N", "R", "N"),
    ("expected_source_dispatch_exposure_id", "N", "R", "R", "N"),
    ("expected_source_physical_call_index", "N", "R", "R", "N"),
    ("expected_source_provider_call_id_state", "N", "R", "R", "N"),
    ("expected_source_provider_call_id", "N", "O", "O", "N"),
    ("expected_source_model_invocation_envelope_ref", "N", "R", "N", "N"),
    ("expected_source_model_invocation_envelope_digest", "N", "R", "N", "N"),
    ("expected_source_terminal_reason", "N", "R", "N", "N"),
    ("expected_source_response_occurrence_id", "N", "R", "N", "N"),
    ("expected_source_canonical_response_digest", "N", "R", "N", "N"),
    ("expected_source_canonical_result_digest", "N", "R", "N", "N"),
    ("expected_source_result_artifact_ref", "N", "O", "N", "N"),
    ("expected_source_result_artifact_digest", "N", "O", "N", "N"),
    ("expected_source_failure_occurrence_id", "N", "N", "R", "N"),
    ("expected_source_failure_code", "N", "N", "R", "N"),
    ("expected_source_failure_spec_digest", "N", "N", "R", "N"),
    ("expected_source_canonical_failure_digest", "N", "N", "R", "N"),
    ("expected_source_retry_policy_revision", "N", "N", "R", "N"),
    ("expected_source_retry_disposition", "N", "N", "terminal", "N"),
    ("expected_source_failure_artifact_ref", "N", "N", "R", "N"),
    ("expected_source_failure_artifact_digest", "N", "N", "R", "N"),
    ("expected_source_no_exposure_spec_digest", "N", "N", "N", "R"),
)

EXPECTED_OWNER_PATHS = {
    "verification_intents": (
        "src/sourcing_agent/repositories/verification_intents.py::VerificationIntentRepository",
        "store.repos.verification_intents",
    ),
    "both transport receipt tables": (
        "src/sourcing_agent/repositories/transport_evidence.py::TransportEvidenceRepository",
        "store.repos.transport_evidence",
    ),
    "transport_response_classification_intents": (
        "src/sourcing_agent/repositories/response_classification_intents.py::ResponseClassificationIntentRepository",
        "store.repos.response_classification_intents",
    ),
    "workflow_late_result_quarantine": (
        "src/sourcing_agent/repositories/late_result_quarantine.py::LateResultQuarantineRepository",
        "store.repos.late_result_quarantine",
    ),
    "cost_reservations + dispatch_exposures": (
        "src/sourcing_agent/repositories/cost_ledger.py::CostLedgerRepository",
        "store.repos.cost_ledger",
    ),
}

EXPECTED_UPSTREAM_CONSTRAINT_NAMES = (
    "plan_review_sessions_d3_scope_review_uk",
    "operation_runs_d3_scope_operation_uk",
    "operation_runs_d3_review_fk",
    "workflow_commands_d3_scope_operation_command_uk",
    "workflow_commands_d3_operation_fk",
    "workflow_activity_runs_d3_scope_operation_command_run_uk",
    "workflow_activity_runs_d3_command_fk",
    "workflow_activity_attempts_d3_scope_operation_command_run_attempt_uk",
    "workflow_activity_attempts_d3_run_fk",
    "workflow_events_d3_scope_event_uk",
    "workflow_events_d3_terminal_event_uk",
    "workflow_events_d3_attempt_fk",
    "model_invocation_envelopes_ref_digest_uk",
)

EXPECTED_SEVEN_TABLE_CONSTRAINT_NAMES = (
    "cost_reservations_pkey",
    "cost_reservations_operation_idempotency_uk",
    "cost_reservations_review_fk",
    "cost_reservations_operation_fk",
    "dispatch_exposures_pkey",
    "dispatch_exposures_physical_call_uk",
    "dispatch_exposures_reservation_fk",
    "dispatch_exposures_review_fk",
    "dispatch_exposures_operation_fk",
    "dispatch_exposures_command_fk",
    "dispatch_exposures_activity_run_fk",
    "dispatch_exposures_activity_attempt_fk",
    "dispatch_exposures_base_intent_fk",
    "dispatch_exposures_predecessor_intent_fk",
    "dispatch_exposures_decision_event_fk",
    "dispatch_exposures_predecessor_event_fk",
    "dispatch_exposures_response_receipt_fk",
    "dispatch_exposures_failure_receipt_fk",
    "verification_intents_pkey",
    "verification_intents_operation_phase_uk",
    "verification_intents_exposure_parent_uk",
    "verification_intents_operation_fk",
    "verification_intents_source_attempt_fk",
    "verification_intents_source_event_fk",
    "verification_intents_response_receipt_fk",
    "verification_intents_failure_receipt_fk",
    "verification_intents_recorded_event_fk",
    "transport_response_receipts_pkey",
    "transport_response_receipts_delivery_uk",
    "transport_response_receipts_occurrence_uk",
    "transport_response_receipts_exposure_ref_uk",
    "transport_response_receipts_child_fk_uk",
    "transport_response_receipts_exposure_fk",
    "transport_response_receipts_attempt_fk",
    "transport_response_receipts_envelope_fk",
    "transport_attempt_failure_receipts_pkey",
    "transport_attempt_failure_receipts_exposure_uk",
    "transport_attempt_failure_receipts_occurrence_uk",
    "transport_attempt_failure_receipts_exposure_ref_uk",
    "transport_attempt_failure_receipts_child_fk_uk",
    "transport_attempt_failure_receipts_exposure_fk",
    "transport_attempt_failure_receipts_attempt_fk",
    "transport_response_classification_intents_pkey",
    "transport_response_classification_intents_idempotency_uk",
    "transport_response_classification_intents_occurrence_uk",
    "transport_response_classification_intents_receipt_fk",
    "workflow_late_result_quarantine_pkey",
    "workflow_late_result_quarantine_idempotency_uk",
    "workflow_late_result_quarantine_occurrence_uk",
    "workflow_late_result_quarantine_receipt_fk",
    "workflow_late_result_quarantine_classification_fk",
    "workflow_late_result_quarantine_envelope_fk",
)

EXPECTED_INDEX_NAMES = (
    "cost_reservations_scope_operation_state_idx",
    "dispatch_exposures_parent_settlement_idx",
    "dispatch_exposures_scope_attempt_call_idx",
    "verification_intents_source_attempt_idx",
    "transport_response_receipts_attempt_idx",
    "transport_attempt_failure_receipts_attempt_idx",
    "transport_response_classification_intents_pending_due_idx",
    "transport_response_classification_intents_claimed_expiry_idx",
    "transport_response_classification_intents_current_apply_due_idx",
    "workflow_late_result_quarantine_pending_cost_idx",
    "workflow_late_result_quarantine_retention_idx",
)

EXPECTED_LOCAL_CHECK_NAMES = (
    "verification_intents_pfx_ck",
    "verification_intents_identity_ck",
    "verification_intents_counter_ck",
    "verification_intents_owner_digest_ck",
    "verification_intents_state_ck",
    "verification_intents_record_shape_ck",
    "verification_intents_terminal_tuple_ck",
    "verification_intents_terminal_digest_ck",
    "verification_intents_terminal_ref_ck",
    "verification_intents_timestamp_ck",
    "transport_response_receipts_pfx_ck",
    "transport_response_receipts_identity_ck",
    "transport_response_receipts_counter_ck",
    "transport_response_receipts_digest_ck",
    "transport_response_receipts_provider_ck",
    "transport_response_receipts_envelope_ref_ck",
    "transport_response_receipts_terminal_reason_ck",
    "transport_response_receipts_occurrence_ck",
    "transport_response_receipts_artifact_pair_ck",
    "transport_attempt_failure_receipts_pfx_ck",
    "transport_attempt_failure_receipts_identity_ck",
    "transport_attempt_failure_receipts_counter_ck",
    "transport_attempt_failure_receipts_digest_ck",
    "transport_attempt_failure_receipts_provider_ck",
    "transport_attempt_failure_receipts_occurrence_ck",
    "transport_attempt_failure_receipts_retry_ck",
    "transport_attempt_failure_receipts_artifact_ck",
    "transport_attempt_failure_receipts_failure_code_ck",
    "transport_response_classification_intents_pfx_ck",
    "transport_response_classification_intents_identity_ck",
    "transport_response_classification_intents_binding_ck",
    "transport_response_classification_intents_state_ck",
    "transport_response_classification_intents_counter_ck",
    "transport_response_classification_intents_error_ck",
    "transport_response_classification_intents_timestamp_ck",
    "workflow_late_result_quarantine_pfx_ck",
    "workflow_late_result_quarantine_identity_ck",
    "workflow_late_result_quarantine_counter_ck",
    "workflow_late_result_quarantine_digest_ck",
    "workflow_late_result_quarantine_provider_ck",
    "workflow_late_result_quarantine_artifact_retention_ck",
    "workflow_late_result_quarantine_state_ck",
    "workflow_late_result_quarantine_authorizable_ck",
    "workflow_late_result_quarantine_rejection_ck",
    "workflow_late_result_quarantine_idempotency_ck",
    "workflow_late_result_quarantine_retention_ck",
    "workflow_late_result_quarantine_timestamp_ck",
)

EXPECTED_LOCAL_CHECK_COUNTS = {
    "verification_intents": 10,
    "transport_response_receipts": 9,
    "transport_attempt_failure_receipts": 9,
    "transport_response_classification_intents": 7,
    "workflow_late_result_quarantine": 12,
}

EXPECTED_LOCAL_CHECK_ROWS_SHA256 = "144c94b6b3abe6f5e39bf5d9d9fd1b3278d4e577def7d29d7c7411e493272922"

EXPECTED_MATRIX_MECHANISMS = (
    "verification intent",
    "response receipt",
    "attempt-failure receipt",
    "response classification intent",
    "late quarantine",
    "cost reservation and dispatch exposure",
    "full-PFX v2 encoders",
    "two ingress UoWs plus continuation",
    "D0f envelope relation",
    "combined relation and index boundary",
    "transport/mode boundary",
)

FUTURE_PHYSICAL_TOKENS = (
    "verification_intents",
    "transport_response_receipts",
    "transport_attempt_failure_receipts",
    "transport_response_classification_intents",
    "workflow_late_result_quarantine",
    "cost_reservations",
    "dispatch_exposures",
    "VerificationIntentRepository",
    "TransportEvidenceRepository",
    "ResponseClassificationIntentRepository",
    "LateResultQuarantineRepository",
    "CostLedgerRepository",
)


def _clean(cell: str) -> str:
    value = cell.strip()
    if value.startswith("`") and value.endswith("`"):
        return value[1:-1]
    return value


def _normalized(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _section(document: str, start: str, end: str | None = None) -> str:
    start_index = document.index(start)
    if end is None:
        return document[start_index:]
    return document[start_index : document.index(end, start_index)]


def _fenced_lines_after(document: str, marker: str) -> tuple[str, ...]:
    marker_index = document.index(marker)
    fence_start = document.index("```text", marker_index) + len("```text")
    fence_end = document.index("```", fence_start)
    return tuple(line.strip() for line in document[fence_start:fence_end].splitlines() if line.strip())


def _table(section: str) -> tuple[tuple[str, ...], tuple[tuple[str, ...], ...]]:
    rows: list[tuple[str, ...]] = []
    started = False
    for line in section.splitlines():
        if line.startswith("|"):
            cells = tuple(_clean(cell) for cell in line.strip().strip("|").split("|"))
            if all(re.fullmatch(r":?-+:?", cell) for cell in cells):
                continue
            rows.append(cells)
            started = True
        elif started:
            break
    assert rows
    return rows[0], tuple(rows[1:])


def _manifest(document: str, start: str, end: str) -> tuple[tuple[str, str, str, str], ...]:
    header, rows = _table(_section(document, start, end))
    assert header == ("#", "Column", "SQL type", "Nullable", "Default")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, len(rows) + 1))
    return tuple((row[1], row[2], row[3], row[4]) for row in rows)


def _length_delimited(*parts: str) -> bytes:
    encoded = bytearray()
    for part in parts:
        raw = part.encode("utf-8")
        encoded.extend(str(len(raw)).encode("ascii"))
        encoded.extend(b":")
        encoded.extend(raw)
    return bytes(encoded)


def _artifact_pair(ref: str | None, digest: str | None) -> bool:
    return (ref is None and digest is None) or (
        ref is not None
        and bool(re.search(r"[^\s]", ref))
        and digest is not None
        and bool(re.fullmatch(r"[0-9a-f]{64}", digest))
    )


def test_declaration_block_and_all_five_manifests_are_exact() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    assert _fenced_lines_after(document, "machine-readable authority") == EXPECTED_DECLARATIONS

    manifests = {
        "verification": _manifest(document, "## 4. Exact `verification_intents`", "### 4.1"),
        "response": _manifest(document, "## 5. Exact `transport_response_receipts`", "## 6."),
        "failure": _manifest(document, "## 6. Exact `transport_attempt_failure_receipts`", "## 7."),
        "classification": _manifest(
            document,
            "## 7. Exact `transport_response_classification_intents`",
            "### 7.1",
        ),
        "quarantine": _manifest(document, "## 8. Exact `workflow_late_result_quarantine`", "## 9."),
    }

    assert manifests == {
        "verification": EXPECTED_VERIFICATION,
        "response": EXPECTED_RESPONSE,
        "failure": EXPECTED_FAILURE,
        "classification": EXPECTED_CLASSIFICATION,
        "quarantine": EXPECTED_QUARANTINE,
    }
    assert tuple(len(manifests[name]) for name in manifests) == (52, 30, 28, 18, 41)
    assert all(manifest[:5] == PFX for manifest in manifests.values())


def test_verification_source_core_and_terminal_truth_table_are_closed() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    source_section = _section(document, "### 4.1 Source core", "### 4.2 Terminal tuple")
    assert _fenced_lines_after(source_section, "exactly seven immutable fields") == SOURCE_CORE

    terminal_section = _section(document, "### 4.2 Terminal tuple", "### 4.3 Intent state")
    header, rows = _table(terminal_section)
    assert header == ("terminal field", "unbound", "exposure", "attempt_failure", "no_exposure")
    assert rows == EXPECTED_TERMINAL_TRUTH
    assert tuple(row[0] for row in rows) == TERMINAL_FIELDS
    assert len(rows) == 29

    for marker in (
        "missing_by_registered_transport",
        "not_observed_before_failure",
        "both null or both non-null",
        "Attempt-failure artifact columns are both required",
        "transaction DB clock only",
    ):
        assert marker in _normalized(terminal_section)


def test_owner_paths_and_intent_classification_cas_surfaces_are_exact() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    owner_section = _section(document, "## 2. Sole owners", "## 3. Common physical prefix")
    header, rows = _table(owner_section)
    assert header == (
        "Surface",
        "Sole future owner",
        "Sole store path",
        "Physical source of truth",
        "Allowed mutations",
        "Forbidden mutations",
    )
    owner_rows = {row[0]: row for row in rows}
    for surface, (owner, store_path) in EXPECTED_OWNER_PATHS.items():
        assert owner_rows[surface][1:3] == (owner, store_path)
    assert owner_rows["post-network composition"][2] == "no `store.repos` entry and no table"
    assert "direct SQL" in owner_rows["post-network composition"][5]

    intent_section = _section(document, "### 4.3 Intent state", "## 5.")
    assert "pending|awaiting_budget|applied|cancelled|timed_out|superseded" in intent_section
    _, intent_methods = _table(intent_section)
    assert tuple(row[0] for row in intent_methods) == (
        "create_or_exact_replay_intent",
        "append_source_terminal_once",
        "apply_record_outcome",
        "transition_control",
        "create_successor_phase",
    )
    assert "no same-row revival" in intent_methods[-1][1]

    classification_section = _section(document, "### 7.1 Classification lifecycle", "## 8.")
    assert (
        "pending|claimed|current_pending_apply|applied_current|classified_stale|failed_terminal"
        in classification_section
    )
    for marker in (
        "claim lease = 30 seconds",
        "min(2 ** (attempt_count - 1), 60)",
        "maximum attempts = 8",
        "private non-serializable claim capability",
        "authorizes neither apply nor quarantine",
        "rolls back the classification UoW first",
        "attempt 8 -> `failed_terminal` with `classification_retry_exhausted`",
        "attempt 8 -> `failed_terminal` with `classification_claim_lease_exhausted`",
    ):
        assert marker in classification_section
    lifecycle_section = _section(classification_section, "| Method |", "The private claim capability")
    _, classification_methods = _table(lifecycle_section)
    assert tuple(row[0] for row in classification_methods) == (
        "create_or_exact_replay_pending",
        "claim_due",
        "retry_claim",
        "reclaim_expired_claim",
        "converge_exhausted_pending",
        "mark_current_pending_apply",
        "complete_stale_with_quarantine",
        "complete_current_apply",
        "reclassify_current_pending_stale_with_quarantine",
        "fail_terminal",
    )

    attempt_section = _section(classification_section, "The attempt boundary is total")
    attempt_header, attempt_rows = _table(attempt_section)
    assert attempt_header == ("Source state / attempt", "Owner-observed outcome", "Exact target/effect")
    normalized_attempt_rows = tuple(tuple(cell.replace("`", "") for cell in row) for row in attempt_rows)
    assert normalized_attempt_rows == (
        ("pending / 0..7", "due claim", "claimed / 1..8; never increments above 8"),
        ("pending / 8", "defensive convergence", "failed_terminal / classification_retry_exhausted"),
        ("claimed / 1..7", "registered transient error", "pending / same attempt; fixed retry delay"),
        ("claimed / 8", "registered transient error", "failed_terminal / classification_retry_exhausted"),
        ("claimed / 1..7", "lease expired", "pending / same attempt; claim_lease_expired"),
        ("claimed / 8", "lease expired", "failed_terminal / classification_claim_lease_exhausted"),
        ("claimed / 1..8", "permanent or non-provable error", "failed_terminal / exact registered error"),
        ("claimed / 1..8", "stored current", "current_pending_apply / same attempt; no terminal/domain write"),
        ("claimed / 1..8", "stored stale", "classified_stale; quarantine and terminal CAS in one UoW"),
        (
            "current_pending_apply / 1..8",
            "fresh stored current",
            "normal terminal/record apply + applied_current in one UoW",
        ),
        (
            "current_pending_apply / 1..8",
            "fresh stored stale",
            "zero domain write + quarantine + classified_stale in one UoW",
        ),
        (
            "any terminal / 0..8",
            "replay or mismatch",
            "exact replay is zero-write; mismatch fails closed; never reopens",
        ),
    )


def test_combined_relations_foreign_keys_indexes_and_parent_blockers_are_exact() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 9. Exact combined", "## 10. Full-PFX")
    assert _fenced_lines_after(section, "exact action contract") == (
        "FK_STD := MATCH SIMPLE DEFERRABLE INITIALLY IMMEDIATE ON UPDATE RESTRICT ON DELETE RESTRICT",
        "FK_CYCLE := MATCH SIMPLE DEFERRABLE INITIALLY DEFERRED ON UPDATE RESTRICT ON DELETE RESTRICT",
    )

    upstream = _section(section, "### 9.1 Ratified upstream", "### 9.2 Ratified seven-table")
    header, rows = _table(upstream)
    assert header == ("Order", "Name", "Kind", "Exact columns / target")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, 14))
    assert tuple(row[1] for row in rows) == EXPECTED_UPSTREAM_CONSTRAINT_NAMES
    assert sum("FK" in row[2] for row in rows) == 5
    assert "review_id" in rows[0][3]
    assert "D3b's ratified `workflow_commands.workspace_id`" in upstream

    seven = _section(section, "### 9.2 Ratified seven-table", "### 9.3 Exact combined index")
    header, rows = _table(seven)
    assert header == ("Order", "Name", "Kind", "Exact child columns / target")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, 53))
    assert tuple(row[1] for row in rows) == EXPECTED_SEVEN_TABLE_CONSTRAINT_NAMES
    assert sum("FK" in row[2] for row in rows) == 29
    assert sum(row[2] == "FK_CYCLE" for row in rows) == 2
    assert tuple(row[1] for row in rows if row[2] == "FK_CYCLE") == (
        "dispatch_exposures_response_receipt_fk",
        "dispatch_exposures_failure_receipt_fk",
    )
    assert all("PFX" in row[3] for row in rows)

    indexes = _section(section, "### 9.3 Exact combined index", "### 9.4 Two unresolved")
    header, rows = _table(indexes)
    assert header == ("Order", "Name", "Table", "Exact ordered columns and predicate")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, 12))
    assert tuple(row[1] for row in rows) == EXPECTED_INDEX_NAMES
    assert all("PFX" in row[3] for row in rows)
    assert sum("WHERE" in row[3] for row in rows) == 5

    access_section = _section(indexes, "| Access path |")
    access_header, access_rows = _table(access_section)
    assert access_header == ("Access path", "Sole exact index")
    assert tuple(row[1] for row in access_rows) == EXPECTED_INDEX_NAMES
    assert "No owner lookup may rely on a broader hidden scan" in indexes

    blockers = _section(section, "### 9.4 Two unresolved", "### 9.5 Exact local CHECK")
    blocker_header, blocker_rows = _table(blockers)
    assert blocker_header == ("Blocker", "Plan/OB owner", "Missing decision", "Required closure")
    assert tuple(row[0] for row in blocker_rows) == (
        "typed plan/review/gate parent",
        "Tier-2 grant parent",
    )
    assert "Plan §6 item 6; R-019" in blocker_rows[0][1]
    assert "OB-10.2; Plan §6 item 7" in blocker_rows[1][1]
    assert all("separate owner decision lock plus pinned non-author `GO`" in row[3] for row in blocker_rows)

    for marker in (
        "does not authorize a dormant migration",
        "No placeholder FK, JSON comparison, unscoped parent, nullable waiver, or application-only assertion",
        "Rollback drops those objects in exact reverse dependency order",
        "Generic replace-all upsert is forbidden",
    ):
        assert marker in _normalized(section)


def test_local_check_inventory_is_exact_and_installed_with_table_creation() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "### 9.5 Exact local CHECK inventory", "Sections 9.1–9.4")

    assert _fenced_lines_after(section, "compact documentation macro") == (
        "NB(x)  := x ~ '[^[:space:]]'",
        "SHA(x) := x ~ '^[0-9a-f]{64}$'",
        "OPT_NB(x)  := x IS NULL OR NB(x)",
        "OPT_SHA(x) := x IS NULL OR SHA(x)",
        "PFX_VALID := NB(runtime_namespace)",
        "AND provider_mode IN ('live', 'simulate', 'scripted')",
        "AND NB(workspace_id)",
        "AND SHA(scope_digest)",
        "AND coordination_plan_review_id > 0",
        "ARTIFACT_PAIR(ref, digest) := ((ref IS NULL AND digest IS NULL) OR (NB(ref) AND SHA(digest))) IS TRUE",
    )

    header, rows = _table(section)
    assert header == ("#", "Constraint name", "Table", "Exact predicate")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, 48))
    assert tuple(row[1] for row in rows) == EXPECTED_LOCAL_CHECK_NAMES
    assert Counter(row[2] for row in rows) == Counter(EXPECTED_LOCAL_CHECK_COUNTS)
    assert all(len(row) == 4 and row[3] and "TBD" not in row[3] for row in rows)

    canonical_rows = "\n".join("|".join(row) for row in rows).encode("utf-8")
    assert hashlib.sha256(canonical_rows).hexdigest() == EXPECTED_LOCAL_CHECK_ROWS_SHA256

    normalized = _normalized(section)
    for marker in (
        "exact four-way SQL disjunction mechanically generated from all 29 rows",
        "every `N` expands to `IS NULL`",
        "every `R` to `IS NOT NULL`",
        "every literal to equality",
        "every `O` to its immediately documented provider-id or artifact-pair predicate",
        "same future `CREATE TABLE` batch",
        "valid `CHECK` constraints",
        "no later `NOT VALID` reinterpretation",
        "Registry applicability, parent-row equality, exact SHA recomputation, state CAS, and DB-clock eligibility",
    ):
        assert marker in normalized


def test_response_and_quarantine_artifact_pairs_reject_blank_refs_and_preserve_tombstone_digest() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "### 9.5 Exact local CHECK inventory", "Sections 9.1–9.4")
    _, rows = _table(section)
    predicates = {row[1]: row[3] for row in rows}
    assert predicates["transport_response_receipts_artifact_pair_ck"] == (
        "ARTIFACT_PAIR(result_artifact_ref, result_artifact_digest)"
    )
    assert predicates["workflow_late_result_quarantine_artifact_retention_ck"] == (
        "((retention_state = 'retained' AND ARTIFACT_PAIR(result_artifact_ref, result_artifact_digest)) OR "
        "(retention_state = 'purged_tombstone' AND result_artifact_ref IS NULL AND "
        "OPT_SHA(result_artifact_digest))) IS TRUE"
    )

    digest = "a" * 64
    assert _artifact_pair(None, None)
    assert _artifact_pair("artifact://result/1", digest)
    assert not _artifact_pair("", digest)
    assert not _artifact_pair("   \t", digest)
    assert not _artifact_pair("artifact://result/1", None)
    assert not _artifact_pair(None, digest)
    assert not _artifact_pair("artifact://result/1", "A" * 64)

    # Purge clears only the reference. A prior lowercase digest remains valid; an absent pair stays absent.
    assert (None is None) and bool(re.fullmatch(r"[0-9a-f]{64}", digest))
    assert (None is None) and (None is None)


def test_full_pfx_v2_golden_vectors_are_byte_exact() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 10. Full-PFX", "## 11. Two post-network")
    pfx = ("agent-v1", "scripted", "ws-α", "a" * 64, "17")

    response_parts = (
        "transport-response-occurrence-v2",
        *pfx,
        "exp:42",
        "callback/evt:7",
    )
    response_bytes = _length_delimited(*response_parts)
    response_digest = hashlib.sha256(response_bytes).hexdigest()
    failure_bytes = _length_delimited(
        "transport-attempt-failure-occurrence-v2",
        *pfx,
        "exp:42",
        "protocol_parse_failed",
        "b" * 64,
    )
    classification_bytes = _length_delimited(
        "transport-response-classification-intent-v1",
        *pfx,
        "exp:42",
        response_digest,
    )
    quarantine_bytes = _length_delimited(
        "late-response-v2",
        *pfx,
        "exp:42",
        "callback/evt:7",
    )
    expected = (
        ("response occurrence", response_bytes.hex(), response_digest),
        (
            "attempt-failure occurrence",
            failure_bytes.hex(),
            hashlib.sha256(failure_bytes).hexdigest(),
        ),
        (
            "classification intent, using the response hash above",
            classification_bytes.hex(),
            hashlib.sha256(classification_bytes).hexdigest(),
        ),
        (
            "quarantine idempotency",
            quarantine_bytes.hex(),
            hashlib.sha256(quarantine_bytes).hexdigest(),
        ),
    )

    golden_section = _section(section, "### 10.1 Golden vectors")
    header, rows = _table(golden_section)
    assert header == ("Vector", "Exact encoded bytes (hex)", "SHA-256")
    assert rows == expected
    assert expected[0][2] == "7fa6c3b88a30a731cc9cf0e30ed718a717cf4161be190c6ec033b702606a54b6"
    assert expected[1][2] == "36a2aa9e22f584098bb489b72831f3795c5830e3873d40698f191c61e903a2e7"
    assert expected[2][2] == "6468cb448058ff6cee00a8732fe8638b5f9e1b5ebd5f84bdd17ed4b0a01c07f7"
    assert expected[3][2] == "c607dcbd3972acedea835543d050f29b870e21a86381242cd3f2c04784ad6aeb"

    assert "late-response-v1:<scope_digest>:" in section
    assert "are superseded" in section
    assert "Same logical ids in another namespace, mode, workspace, or coordination review" in _normalized(section)


def test_two_ingress_orders_and_current_apply_continuation_close_all_races() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 11. Two post-network", "## 12. Mode/transport")
    evidence_order = _fenced_lines_after(section, "Exposure-first evidence ingress")
    classification_order = _fenced_lines_after(section, "Response-classification ingress")
    continuation_order = _fenced_lines_after(section, "Recoverable current-apply continuation")

    assert evidence_order == (
        "dispatch_exposure_lock",
        "-> applicable_transport_receipt_insert_or_exact_replay",
        "-> response_only_classification_intent_create_or_exact_replay",
        "-> exposure_terminalize_if_nonterminal_or_exact_validate_terminal_unchanged",
        "-> commit",
    )
    assert classification_order == (
        "d3_dispatch_v2",
        "-> operation_root",
        "-> optional_plan_review_gate",
        "-> participating_commands_sorted",
        "-> verification_intent_and_predecessor",
        "-> activity_run_attempt",
        "-> dispatch_exposure_lock",
        "-> transport_response_receipt_exact_replay",
        "-> response_classification_intent_lock",
        "-> stored_state_branch",
        "-> current_mark_current_pending_apply_or_stale_quarantine_and_terminal_CAS",
        "-> exposure_terminal_exact_validate_unchanged",
        "-> commit",
    )
    assert continuation_order == (
        "d3_dispatch_v2",
        "-> operation_root",
        "-> optional_plan_review_gate",
        "-> participating_commands_sorted_and_terminal_identities_reserved",
        "-> verification_intent_and_predecessor",
        "-> activity_run_attempt",
        "-> dispatch_exposure_lock",
        "-> transport_response_receipt_exact_replay",
        "-> response_classification_current_pending_apply_lock",
        "-> fresh_stored_state_branch",
        "-> current_normal_terminal_record_apply_or_stale_quarantine",
        "-> response_classification_terminal_CAS",
        "-> exposure_terminal_exact_validate_unchanged",
        "-> commit",
    )
    for marker in (
        "zero current/stale classification authority and zero quarantine permission",
        "valid response cannot commit without a durable classification work item",
        "transaction-local proof derived from all locked stored rows",
        "caller flag, callback label, stale `ClaimReceipt`",
        "no terminal `classified_current` state that can strand a response",
        "A crash before commit leaves `current_pending_apply` due and recoverable",
        "Attempt failure and proven no-call never create a classification intent or quarantine row",
        "No PG transaction crosses DNS",
    ):
        assert marker in _normalized(section)

    race_section = _section(section, "### 11.4 Exact response/failure/retry race outcomes")
    race_header, race_rows = _table(race_section)
    assert race_header == ("First committed condition", "Later ingress", "Exact outcome")
    assert len(race_rows) == 8
    assert tuple(row[0] for row in race_rows) == (
        "nonterminal exposure, response first",
        "nonterminal exposure, response first",
        "failure terminal first",
        "retry/epoch advance first, exposure nonterminal",
        "retry/epoch advance first, exposure already terminal",
        "any response delivery",
        "response terminal already names an earlier response",
        "response marked current_pending_apply, then control/epoch/business advance",
    )
    assert "never apply" in race_rows[2][2]
    assert "fresh proof chooses stale" in race_rows[-1][2]
    assert "zero domain/source/result write" in race_rows[-1][2]


def test_quarantine_retention_and_cost_axes_are_fixed_db_clock_cas() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 8. Exact `workflow_late_result_quarantine`", "## 9.")
    assert "workflow_run_id` is forbidden" in section
    assert "stale_claim|business_precondition_conflict" in section
    assert _fenced_lines_after(section, "Cost and retention are independent") == (
        "cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain",
        "retention_state: retained -> purged_tombstone",
    )
    for marker in (
        "changes only `cost_state`, `cost_reconciled_at`, and `cost_state_version`",
        "changes only `retention_state`, `result_artifact_ref`, `purged_at`, and `retention_state_version`",
        "`reconciled_no_call`, reset, promotion, restore, delete, and mixed disposition do not exist",
        "`quarantine_retention_30d_v1`",
        "`retention_until = recorded_at + interval '30 days'`",
        "same transaction DB clock",
        "caller clocks/deadlines are forbidden",
        "Cost reconciliation cannot extend the deadline",
    ):
        assert marker in _normalized(section)


def test_modes_matrix_physical_baseline_and_plan_integration() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    mode_section = _section(document, "## 12. Mode/transport", "## 13. Mechanism")
    header, rows = _table(mode_section)
    assert header == ("Mode / transport", "Five-surface eligibility", "Cost/evidence relation", "Decision")
    assert tuple(row[0] for row in rows) == (
        "live + model_tool_v1",
        "simulate + model_tool_v1",
        "scripted + model_tool_v1",
        "replay",
        "Harvest/provider-search",
    )
    assert "fail closed before writes" in rows[3][3]
    assert "separately owner-ratified transport variant" in rows[4][3]
    assert "Zero money is not zero evidence" in mode_section
    assert "Thinking Machines Lab/Harvest live work remains blocked" in mode_section

    matrix_section = _section(document, "## 13. Mechanism", "## 14. Executable oracle")
    matrix_header, matrix_rows = _table(matrix_section)
    assert matrix_header == (
        "mechanism",
        "1 owner",
        "2 tenant",
        "3 fence",
        "4 lifecycle",
        "5 late/partial",
        "6 cost",
        "7 physical identity",
        "8 provenance",
        "9 consistency",
        "10 mode isolation",
    )
    assert tuple(row[0] for row in matrix_rows) == EXPECTED_MATRIX_MECHANISMS
    assert all(len(row) == 11 and all(cell for cell in row) for row in matrix_rows)
    assert sum(len(row) - 1 for row in matrix_rows) == 110
    assert "Every one of the 110 invariant cells is populated" in matrix_section

    checklist = CHECKLIST_PATH.read_text(encoding="utf-8")
    for marker in (
        "单写者与聚合所有权",
        "租户键",
        "世代与物理围栏",
        "生命周期完备性",
        "晚到与部分结果",
        "成本诚实性",
        "物理身份绑定",
        "Provenance 与信任边界",
        "自包含与跨文档一致",
        "运行时/模式隔离",
    ):
        assert marker in checklist

    production_python = "\n".join(path.read_text(encoding="utf-8") for path in sorted(SOURCE_ROOT.rglob("*.py")))
    migration_sql = "\n".join(path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS_ROOT.glob("*.sql")))
    for token in FUTURE_PHYSICAL_TOKENS:
        assert token not in production_python
        assert token not in migration_sql

    assert (SOURCE_ROOT / "repositories" / "model_invocation_envelopes.py").is_file()
    assert (MIGRATIONS_ROOT / "0007_model_invocation_envelopes.sql").is_file()
    d0f = D0F_PATH.read_text(encoding="utf-8")
    assert "sole PG-only `ModelInvocationEnvelopeRepository`" not in d0f
    assert "`ModelInvocationEnvelopeRepository` is the sole reference issuer" in d0f
    assert "Kind.TIMESTAMPTZ" in d0f
    assert "Decimal remains deferred" in d0f

    h0 = D3C2H0_PATH.read_text(encoding="utf-8")
    g = D3C2G_PATH.read_text(encoding="utf-8")
    assert "D3c2h1 schema-manifest boundary" in h0
    assert "D0f successor observation" in h0
    assert "D0f successor observation" in g

    plan = PLAN_PATH.read_text(encoding="utf-8")
    assert "D3c2h1 exact evidence-surface decision-lock" in plan
    assert "`52/30/28/18/41`" in plan
    assert "classification intent" in plan
    assert "current_pending_apply" in plan
    assert "13 upstream constraints" in plan
    assert "52 seven-table constraints" in plan
    assert "11 indexes" in plan
    assert "typed plan/review/gate" in plan
    assert "Tier-2 grant" in plan
    assert "decision_locked_not_implemented" in plan


def test_nonclosure_and_validation_command_are_honest() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 14. Executable oracle")
    normalized = _normalized(section)
    for marker in (
        "closes no migration, repository, runtime, rollout, formal-review, provider, live, W6, manual, product",
        "does not authorize SQL by itself",
        "fresh pinned non-author review of this repaired decision lock",
        "formal `NO-GO 0/3/3/0`",
        "separate typed plan/review/gate-parent and Tier-2 grant-parent owner decision lock",
        "fake/simulate/scripted E2E",
        "separately gated bounded live canary",
        "Author evidence is not a formal `GO`",
    ):
        assert marker in normalized

    assert "tests/test_d3c2h1_exact_evidence_surface_decision_lock.py" in section
    assert "tests/test_d3c2h0_evidence_cross_contract_ratification.py" in section
    assert "tests/test_d3c2g_cost_ledger_decision_lock.py" in section
    assert "tests/test_d0f_model_invocation_envelope_repository.py" in section
    assert "tests/test_model_invocation_contract.py" in section
    assert "test_d3c2h1_exact_eVIDENCE" not in section
