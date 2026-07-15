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
    "response_occurrence_domain = transport-response-occurrence-v2",
    "failure_occurrence_domain = transport-attempt-failure-occurrence-v2",
    "classification_intent_domain = transport-response-classification-intent-v1",
    "quarantine_idempotency_domain = late-response-v2",
    "quarantine_retention_policy = quarantine_retention_30d_v1",
    "quarantine_retention_deadline = recorded_at + interval '30 days'",
    "exposure_first_quarantine_permission = forbidden",
    "classification_authority = stored_current_state_under_d3_dispatch_v2_global_lock_prefix",
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
}

EXPECTED_KEY_NAMES = (
    "verification_intents_pkey",
    "verification_intents_operation_phase_uk",
    "verification_intents_operation_fk",
    "verification_intents_source_attempt_fk",
    "verification_intents_source_event_fk",
    "verification_intents_response_receipt_fk",
    "verification_intents_failure_receipt_fk",
    "verification_intents_recorded_event_fk",
    "transport_response_receipts_pkey",
    "transport_response_receipts_delivery_uk",
    "transport_response_receipts_occurrence_uk",
    "transport_response_receipts_child_fk_uk",
    "transport_response_receipts_exposure_fk",
    "transport_response_receipts_attempt_fk",
    "transport_response_receipts_envelope_fk",
    "transport_attempt_failure_receipts_pkey",
    "transport_attempt_failure_receipts_exposure_uk",
    "transport_attempt_failure_receipts_occurrence_uk",
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
    "workflow_late_result_quarantine_envelope_fk",
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

EXPECTED_LOCAL_CHECK_ROWS_SHA256 = "bb8ee3dbf87349000b2ec3400dc08187b18cd161d721ae7331f34779ab18d78d"

EXPECTED_MATRIX_MECHANISMS = (
    "verification intent",
    "response receipt",
    "attempt-failure receipt",
    "response classification intent",
    "late quarantine",
    "full-PFX v2 encoders",
    "two post-network UoWs",
    "D0f envelope relation",
    "transport/mode boundary",
)

FUTURE_PHYSICAL_TOKENS = (
    "verification_intents",
    "transport_response_receipts",
    "transport_attempt_failure_receipts",
    "transport_response_classification_intents",
    "workflow_late_result_quarantine",
    "VerificationIntentRepository",
    "TransportEvidenceRepository",
    "ResponseClassificationIntentRepository",
    "LateResultQuarantineRepository",
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
    assert "pending|claimed|classified_current|classified_stale|failed_terminal" in classification_section
    for marker in (
        "claim lease = 30 seconds",
        "min(2 ** (attempt_count - 1), 60)",
        "maximum attempts = 8",
        "private non-serializable claim capability",
        "authorizes neither apply nor quarantine",
        "rolls back the classification UoW first",
    ):
        assert marker in classification_section
    _, classification_methods = _table(classification_section)
    assert tuple(row[0] for row in classification_methods) == (
        "create_or_exact_replay_pending",
        "claim_due",
        "retry_claim",
        "reclaim_expired_claim",
        "complete_current",
        "complete_stale_with_quarantine",
        "fail_terminal",
    )


def test_keys_and_foreign_keys_are_full_pfx_and_exhaustive() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 9. Exact keys", "## 10. Full-PFX")
    header, rows = _table(section)
    assert header == ("Order", "Name", "Kind", "Exact child columns / target")
    assert tuple(int(row[0]) for row in rows) == tuple(range(1, 31))
    assert tuple(row[1] for row in rows) == EXPECTED_KEY_NAMES
    assert all("PFX" in row[3] for row in rows)
    assert sum("FK" in row[2] for row in rows) == 14
    assert "MATCH SIMPLE DEFERRABLE" in " ".join(row[2] for row in rows)

    for marker in (
        "OperationRun, ActivityAttempt, WorkflowEvent, and dispatch exposure",
        "must install those exact parent keys before these FKs",
        "id-only, scope-digest-only, JSON, or application-only",
        "Generic replace-all upsert is forbidden",
    ):
        assert marker in _normalized(section)


def test_local_check_inventory_is_exact_and_installed_with_table_creation() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "### 9.1 Exact local CHECK inventory", "The upstream full-PFX")

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
        "PAIR(a, b) := (a IS NULL AND b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL)",
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


def test_full_pfx_v2_golden_vectors_are_byte_exact() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 10. Full-PFX", "## 11. Exactly two")
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


def test_two_uow_orders_close_pending_classification_without_caller_authority() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 11. Exactly two", "## 12. Mode/transport")
    evidence_order = _fenced_lines_after(section, "Exposure-first evidence UoW")
    classification_order = _fenced_lines_after(section, "Response-classification UoW")

    assert evidence_order == (
        "dispatch_exposure_lock",
        "-> applicable_transport_receipt_insert_or_exact_replay",
        "-> response_only_classification_intent_create_or_exact_replay",
        "-> dispatch_exposure_terminalization_or_exact_replay",
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
        "-> optional_response_only_quarantine_insert_or_exact_replay_if_stale",
        "-> response_classification_intent_terminal_CAS",
        "-> dispatch_exposure_terminalization_exact_replay",
        "-> commit",
    )
    for marker in (
        "zero current/stale classification authority and zero quarantine permission",
        "valid response cannot commit without a durable classification work item",
        "transaction-local proof derived from the locked stored rows",
        "caller flag, callback label, stale `ClaimReceipt`",
        "never returns to an earlier aggregate",
        "Attempt failure and proven no-call never create a classification intent or quarantine row",
        "No PG transaction crosses DNS",
    ):
        assert marker in _normalized(section)


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
    assert sum(len(row) - 1 for row in matrix_rows) == 90
    assert "Every one of the 90 invariant cells is populated" in matrix_section

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
    assert "D3c2h1 exact evidence-surface decision-lock candidate" in plan
    assert "`52/30/28/18/41`" in plan
    assert "classification intent" in plan
    assert "decision_locked_not_implemented" in plan


def test_nonclosure_and_validation_command_are_honest() -> None:
    document = DECISION_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 14. Executable oracle")
    for marker in (
        "closes no migration, repository, runtime, rollout, formal-review, provider, live, W6, manual, product",
        "does not authorize SQL by itself",
        "fresh pinned non-author review",
        "fake/simulate/scripted E2E",
        "separately gated bounded live canary",
        "Author evidence is not a formal `GO`",
    ):
        assert marker in section

    assert "tests/test_d3c2h1_exact_evidence_surface_decision_lock.py" in section
    assert "tests/test_d3c2h0_evidence_cross_contract_ratification.py" in section
    assert "tests/test_d3c2g_cost_ledger_decision_lock.py" in section
    assert "tests/test_d0f_model_invocation_envelope_repository.py" in section
    assert "tests/test_model_invocation_contract.py" in section
    assert "test_d3c2h1_exact_eVIDENCE" not in section
