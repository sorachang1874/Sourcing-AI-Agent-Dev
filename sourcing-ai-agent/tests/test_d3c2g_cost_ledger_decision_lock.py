from __future__ import annotations

import ast
import re
from pathlib import Path

from sourcing_agent.model_tool_runtime import MODEL_INVOCATION_ENVELOPE_RECORD_KEYS

REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = REPO_ROOT / "src" / "sourcing_agent"
MIGRATIONS_ROOT = SOURCE_ROOT / "migrations"

DECISION_DOC_PATH = REPO_ROOT / "docs" / "TRACK_D_D3C2G_COST_LEDGER_DISPATCH_EXPOSURE_DECISION_LOCK.md"
D0_DESIGN_PATH = REPO_ROOT / "docs" / "TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md"
D0_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D0C_MODEL_INVOCATION_CONTRACT.md"
D3B_CONTRACT_PATH = REPO_ROOT / "docs" / "TRACK_D_D3B_WORKFLOW_COMMAND_CLAIM_FENCE_CONTRACT.md"
PLAN_PATH = REPO_ROOT / "docs" / "TRACK_D_AGENT_RUNTIME_PLAN.md"
TODO_PATH = REPO_ROOT / "docs" / "NEXT_TODO.md"
LEDGER_PATH = REPO_ROOT / "docs" / "RESIDUAL_LEDGER.md"
INDEX_PATH = REPO_ROOT / "docs" / "INDEX.md"
BASELINE_MIGRATION_PATH = MIGRATIONS_ROOT / "0001_baseline.sql"
REPOSITORIES_INIT_PATH = SOURCE_ROOT / "repositories" / "__init__.py"
FUTURE_REPOSITORY_PATH = SOURCE_ROOT / "repositories" / "cost_ledger.py"
MODEL_TOOL_RUNTIME_PATH = SOURCE_ROOT / "model_tool_runtime.py"
CONTROL_PLANE_REPOSITORY_PATH = SOURCE_ROOT / "control_plane_repository.py"

EXPECTED_SCOPE_PREFIX = (
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
)

EXPECTED_PARENT_ROWS = (
    ("1", "runtime_namespace", "TEXT", "no", "none"),
    ("2", "provider_mode", "TEXT", "no", "none"),
    ("3", "workspace_id", "TEXT", "no", "none"),
    ("4", "scope_digest", "TEXT", "no", "none"),
    ("5", "coordination_plan_review_id", "BIGINT", "no", "none"),
    ("6", "budget_reservation_ref", "TEXT", "no", "none"),
    ("7", "operation_run_id", "TEXT", "no", "none"),
    ("8", "reservation_idempotency_key", "TEXT", "no", "none"),
    ("9", "budget_policy_digest", "TEXT", "no", "none"),
    ("10", "currency_code", "TEXT", "no", "none"),
    ("11", "reserved_amount", "NUMERIC(38,12)", "no", "none"),
    ("12", "available_amount", "NUMERIC(38,12)", "no", "none"),
    ("13", "held_amount", "NUMERIC(38,12)", "no", "none"),
    ("14", "accounted_amount", "NUMERIC(38,12)", "no", "none"),
    ("15", "released_amount", "NUMERIC(38,12)", "no", "none"),
    ("16", "overrun_amount", "NUMERIC(38,12)", "no", "none"),
    ("17", "reservation_state", "TEXT", "no", "none"),
    ("18", "state_version", "BIGINT", "no", "0"),
    ("19", "created_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("20", "updated_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("21", "closed_at", "TIMESTAMPTZ", "yes", "none"),
)

EXPECTED_CHILD_ROWS = (
    ("1", "runtime_namespace", "TEXT", "no", "none"),
    ("2", "provider_mode", "TEXT", "no", "none"),
    ("3", "workspace_id", "TEXT", "no", "none"),
    ("4", "scope_digest", "TEXT", "no", "none"),
    ("5", "coordination_plan_review_id", "BIGINT", "no", "none"),
    ("6", "dispatch_exposure_id", "TEXT", "no", "none"),
    ("7", "budget_reservation_ref", "TEXT", "no", "none"),
    ("8", "operation_run_id", "TEXT", "no", "none"),
    ("9", "command_id", "TEXT", "no", "none"),
    ("10", "activity_run_id", "TEXT", "no", "none"),
    ("11", "activity_attempt_id", "TEXT", "no", "none"),
    ("12", "physical_call_index", "BIGINT", "no", "none"),
    ("13", "command_attempt", "BIGINT", "no", "none"),
    ("14", "claim_generation", "BIGINT", "no", "none"),
    ("15", "control_epoch", "BIGINT", "no", "none"),
    ("16", "claim_authority_spec_digest", "TEXT", "no", "none"),
    ("17", "terminal_provenance_policy_digest", "TEXT", "no", "none"),
    ("18", "d3_business_fence_digest", "TEXT", "no", "none"),
    ("19", "plan_id", "TEXT", "no", "none"),
    ("20", "plan_bundle_digest", "TEXT", "no", "none"),
    ("21", "plan_revision", "BIGINT", "no", "none"),
    ("22", "review_revision", "BIGINT", "no", "none"),
    ("23", "gate_control_epoch", "BIGINT", "no", "none"),
    ("24", "gate_revision", "BIGINT", "no", "none"),
    ("25", "gate_blocking_reason_digest", "TEXT", "no", "none"),
    ("26", "base_intent_id", "TEXT", "yes", "none"),
    ("27", "base_intent_phase_generation", "BIGINT", "yes", "none"),
    ("28", "expected_predecessor_intent_id", "TEXT", "yes", "none"),
    ("29", "expected_predecessor_phase_generation", "BIGINT", "yes", "none"),
    ("30", "expected_predecessor_source_control_epoch", "BIGINT", "yes", "none"),
    ("31", "expected_predecessor_decision_source_event_id", "TEXT", "yes", "none"),
    ("32", "fingerprint_version", "TEXT", "no", "none"),
    ("33", "fingerprint_digest", "TEXT", "no", "none"),
    ("34", "decision_generation", "BIGINT", "no", "none"),
    ("35", "decision_source_event_id", "TEXT", "no", "none"),
    ("36", "accepted_policy_revision", "TEXT", "no", "none"),
    ("37", "schema_revision", "TEXT", "no", "none"),
    ("38", "route_revision", "TEXT", "no", "none"),
    ("39", "effective_route_snapshot_digest", "TEXT", "no", "none"),
    ("40", "grant_tier", "TEXT", "no", "none"),
    ("41", "grant_id", "TEXT", "yes", "none"),
    ("42", "grant_issuance_generation", "BIGINT", "yes", "none"),
    ("43", "grant_policy_revision", "TEXT", "yes", "none"),
    ("44", "route_id", "TEXT", "no", "none"),
    ("45", "effective_route_snapshot_ref", "TEXT", "no", "none"),
    ("46", "provider", "TEXT", "no", "none"),
    ("47", "api_style", "TEXT", "no", "none"),
    ("48", "requested_model", "TEXT", "no", "none"),
    ("49", "transport_kind", "TEXT", "no", "none"),
    ("50", "transport_spec_digest", "TEXT", "no", "none"),
    ("51", "permission_scope_revision", "TEXT", "no", "none"),
    ("52", "outbound_policy_revision", "TEXT", "no", "none"),
    ("53", "model_safe_schema_revision", "TEXT", "no", "none"),
    ("54", "canonical_request_digest", "TEXT", "no", "none"),
    ("55", "cost_pricing_spec_digest", "TEXT", "no", "none"),
    ("56", "worst_case_amount", "NUMERIC(38,12)", "no", "none"),
    ("57", "observed_amount", "NUMERIC(38,12)", "yes", "none"),
    ("58", "accounted_amount", "NUMERIC(38,12)", "yes", "none"),
    ("59", "usage_status", "TEXT", "yes", "none"),
    ("60", "exposure_state", "TEXT", "no", "none"),
    ("61", "parent_settlement_state", "TEXT", "no", "none"),
    ("62", "state_version", "BIGINT", "no", "0"),
    ("63", "cost_reconciliation_spec_digest", "TEXT", "yes", "none"),
    ("64", "transport_response_receipt_id", "TEXT", "yes", "none"),
    ("65", "transport_attempt_failure_receipt_id", "TEXT", "yes", "none"),
    ("66", "no_call_proof_ref", "TEXT", "yes", "none"),
    ("67", "no_call_proof_digest", "TEXT", "yes", "none"),
    ("68", "uncertain_reconciliation_ref", "TEXT", "yes", "none"),
    ("69", "uncertain_reconciliation_digest", "TEXT", "yes", "none"),
    ("70", "created_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
    ("71", "dispatching_at", "TIMESTAMPTZ", "yes", "none"),
    ("72", "sent_at", "TIMESTAMPTZ", "yes", "none"),
    ("73", "reconciled_at", "TIMESTAMPTZ", "yes", "none"),
    ("74", "parent_settled_at", "TIMESTAMPTZ", "yes", "none"),
    ("75", "updated_at", "TIMESTAMPTZ", "no", "transaction_timestamp()"),
)

EXPECTED_KEY_ROWS = (
    (
        "1",
        "cost_reservations_pkey",
        "PRIMARY KEY",
        "PFX, budget_reservation_ref",
        "identity",
    ),
    (
        "2",
        "cost_reservations_operation_idempotency_uk",
        "UNIQUE",
        "PFX, operation_run_id, reservation_idempotency_key",
        "exact replay collision key",
    ),
    (
        "3",
        "dispatch_exposures_pkey",
        "PRIMARY KEY",
        "PFX, dispatch_exposure_id",
        "identity and D0 cost reference target",
    ),
    (
        "4",
        "dispatch_exposures_physical_call_uk",
        "UNIQUE",
        "PFX, budget_reservation_ref, activity_attempt_id, physical_call_index",
        "one exposure per physical call",
    ),
    (
        "5",
        "dispatch_exposures_reservation_fk",
        "FOREIGN KEY",
        "PFX, budget_reservation_ref",
        "references parent identical columns; `MATCH SIMPLE DEFERRABLE INITIALLY IMMEDIATE ON UPDATE RESTRICT ON DELETE RESTRICT`",
    ),
)

EXPECTED_INDEX_ROWS = (
    (
        "1",
        "cost_reservations_scope_operation_state_idx",
        "cost_reservations",
        "PFX, operation_run_id, reservation_state",
    ),
    (
        "2",
        "dispatch_exposures_parent_settlement_idx",
        "dispatch_exposures",
        "PFX, budget_reservation_ref, parent_settlement_state, dispatch_exposure_id",
    ),
    (
        "3",
        "dispatch_exposures_scope_attempt_call_idx",
        "dispatch_exposures",
        "PFX, activity_attempt_id, physical_call_index",
    ),
)

EXPECTED_PARENT_CHECKS = (
    ("cost_reservations_runtime_namespace_nonblank_ck", "runtime_namespace ~ '[^[:space:]]'"),
    (
        "cost_reservations_provider_mode_ck",
        "provider_mode IN ('live', 'simulate', 'scripted')",
    ),
    ("cost_reservations_workspace_id_nonblank_ck", "workspace_id ~ '[^[:space:]]'"),
    ("cost_reservations_scope_digest_shape_ck", "scope_digest ~ '^[0-9a-f]{64}$'"),
    ("cost_reservations_coordination_positive_ck", "coordination_plan_review_id > 0"),
    (
        "cost_reservations_identity_nonblank_ck",
        "budget_reservation_ref ~ '[^[:space:]]' AND operation_run_id ~ '[^[:space:]]' AND reservation_idempotency_key ~ '[^[:space:]]'",
    ),
    ("cost_reservations_budget_policy_digest_shape_ck", "budget_policy_digest ~ '^[0-9a-f]{64}$'"),
    ("cost_reservations_currency_usd_ck", "currency_code = 'USD'"),
    (
        "cost_reservations_reserved_mode_relation_ck",
        "(provider_mode = 'live' AND reserved_amount > 0) OR (provider_mode IN ('simulate', 'scripted') AND reserved_amount = 0)",
    ),
    (
        "cost_reservations_amounts_mode_relation_ck",
        "(provider_mode = 'live' AND available_amount >= 0 AND held_amount >= 0 AND accounted_amount >= 0 AND released_amount >= 0 AND overrun_amount >= 0) OR (provider_mode IN ('simulate', 'scripted') AND available_amount = 0 AND held_amount = 0 AND accounted_amount = 0 AND released_amount = 0 AND overrun_amount = 0)",
    ),
    (
        "cost_reservations_conservation_ck",
        "reserved_amount + overrun_amount = available_amount + held_amount + accounted_amount + released_amount",
    ),
    ("cost_reservations_state_ck", "reservation_state IN ('open', 'closing', 'closed')"),
    ("cost_reservations_state_version_nonnegative_ck", "state_version >= 0"),
    ("cost_reservations_closed_at_shape_ck", "(reservation_state = 'closed') = (closed_at IS NOT NULL)"),
    (
        "cost_reservations_closed_balances_ck",
        "reservation_state <> 'closed' OR (available_amount = 0 AND held_amount = 0)",
    ),
    (
        "cost_reservations_timestamp_order_ck",
        "updated_at >= created_at AND (closed_at IS NULL OR closed_at >= created_at)",
    ),
)

EXPECTED_CHILD_CHECKS = (
    ("dispatch_exposures_runtime_namespace_nonblank_ck", "runtime_namespace ~ '[^[:space:]]'"),
    (
        "dispatch_exposures_provider_mode_ck",
        "provider_mode IN ('live', 'simulate', 'scripted')",
    ),
    ("dispatch_exposures_workspace_id_nonblank_ck", "workspace_id ~ '[^[:space:]]'"),
    ("dispatch_exposures_scope_digest_shape_ck", "scope_digest ~ '^[0-9a-f]{64}$'"),
    ("dispatch_exposures_coordination_positive_ck", "coordination_plan_review_id > 0"),
    (
        "dispatch_exposures_identity_nonblank_ck",
        "dispatch_exposure_id ~ '[^[:space:]]' AND budget_reservation_ref ~ '[^[:space:]]' AND operation_run_id ~ '[^[:space:]]' AND command_id ~ '[^[:space:]]' AND activity_run_id ~ '[^[:space:]]' AND activity_attempt_id ~ '[^[:space:]]'",
    ),
    ("dispatch_exposures_physical_call_index_nonnegative_ck", "physical_call_index >= 0"),
    (
        "dispatch_exposures_claim_numbers_ck",
        "command_attempt > 0 AND claim_generation > 0 AND control_epoch >= 0",
    ),
    (
        "dispatch_exposures_claim_digests_shape_ck",
        "claim_authority_spec_digest ~ '^[0-9a-f]{64}$' AND terminal_provenance_policy_digest ~ '^[0-9a-f]{64}$' AND d3_business_fence_digest ~ '^[0-9a-f]{64}$'",
    ),
    (
        "dispatch_exposures_plan_pin_shape_ck",
        "plan_id ~ '[^[:space:]]' AND plan_bundle_digest ~ '^[0-9a-f]{64}$' AND plan_revision >= 0 AND review_revision >= 0 AND gate_control_epoch >= 0 AND gate_revision >= 0 AND gate_blocking_reason_digest ~ '^[0-9a-f]{64}$'",
    ),
    (
        "dispatch_exposures_base_intent_tuple_ck",
        "(base_intent_id IS NULL AND base_intent_phase_generation IS NULL) OR (base_intent_id ~ '[^[:space:]]' AND base_intent_phase_generation > 0)",
    ),
    (
        "dispatch_exposures_predecessor_tuple_ck",
        "(expected_predecessor_intent_id IS NULL AND expected_predecessor_phase_generation IS NULL AND expected_predecessor_source_control_epoch IS NULL AND expected_predecessor_decision_source_event_id IS NULL) OR (expected_predecessor_intent_id ~ '[^[:space:]]' AND expected_predecessor_phase_generation > 0 AND expected_predecessor_source_control_epoch >= 0 AND expected_predecessor_decision_source_event_id ~ '[^[:space:]]')",
    ),
    (
        "dispatch_exposures_decision_pin_shape_ck",
        "fingerprint_version ~ '[^[:space:]]' AND fingerprint_digest ~ '^[0-9a-f]{64}$' AND decision_generation > 0 AND decision_source_event_id ~ '[^[:space:]]' AND accepted_policy_revision ~ '[^[:space:]]' AND schema_revision ~ '[^[:space:]]' AND route_revision ~ '^[0-9a-f]{64}$' AND effective_route_snapshot_digest ~ '^[0-9a-f]{64}$'",
    ),
    (
        "dispatch_exposures_grant_tuple_ck",
        "(grant_tier = 'tier1' AND grant_id IS NULL AND grant_issuance_generation IS NULL AND grant_policy_revision IS NULL) OR (grant_tier = 'tier2' AND grant_id ~ '[^[:space:]]' AND grant_issuance_generation > 0 AND grant_policy_revision ~ '[^[:space:]]')",
    ),
    (
        "dispatch_exposures_transport_v1_shape_ck",
        "transport_kind = 'model_tool_v1' AND provider_mode IN ('live', 'simulate', 'scripted') AND route_id ~ '[^[:space:]]' AND effective_route_snapshot_ref ~ '[^[:space:]]' AND provider ~ '[^[:space:]]' AND api_style ~ '[^[:space:]]' AND requested_model ~ '[^[:space:]]' AND permission_scope_revision ~ '[^[:space:]]' AND outbound_policy_revision ~ '[^[:space:]]' AND model_safe_schema_revision ~ '[^[:space:]]'",
    ),
    (
        "dispatch_exposures_transport_digests_shape_ck",
        "transport_spec_digest ~ '^[0-9a-f]{64}$' AND canonical_request_digest ~ '^[0-9a-f]{64}$' AND cost_pricing_spec_digest ~ '^[0-9a-f]{64}$'",
    ),
    (
        "dispatch_exposures_worst_case_mode_relation_ck",
        "(provider_mode = 'live' AND worst_case_amount > 0) OR (provider_mode IN ('simulate', 'scripted') AND worst_case_amount = 0)",
    ),
    (
        "dispatch_exposures_optional_amounts_mode_relation_ck",
        "(provider_mode = 'live' AND (observed_amount IS NULL OR observed_amount >= 0) AND (accounted_amount IS NULL OR accounted_amount >= 0)) OR (provider_mode IN ('simulate', 'scripted') AND (observed_amount IS NULL OR observed_amount = 0) AND (accounted_amount IS NULL OR accounted_amount = 0))",
    ),
    (
        "dispatch_exposures_usage_status_ck",
        "usage_status IS NULL OR usage_status IN ('reported', 'unavailable', 'invalid')",
    ),
    (
        "dispatch_exposures_state_ck",
        "exposure_state IN ('prepared', 'dispatching', 'sent', 'confirmed', 'uncertain', 'no_call')",
    ),
    (
        "dispatch_exposures_parent_settlement_state_ck",
        "parent_settlement_state IN ('not_ready', 'pending', 'applied')",
    ),
    ("dispatch_exposures_state_version_nonnegative_ck", "state_version >= 0"),
    (
        "dispatch_exposures_settlement_state_relation_ck",
        "(exposure_state IN ('prepared', 'dispatching', 'sent') AND parent_settlement_state = 'not_ready') OR (exposure_state IN ('confirmed', 'uncertain', 'no_call') AND parent_settlement_state IN ('pending', 'applied'))",
    ),
    (
        "dispatch_exposures_receipt_exclusive_ck",
        "transport_response_receipt_id IS NULL OR transport_attempt_failure_receipt_id IS NULL",
    ),
    (
        "dispatch_exposures_evidence_pair_shape_ck",
        "(no_call_proof_ref IS NULL) = (no_call_proof_digest IS NULL) AND (uncertain_reconciliation_ref IS NULL) = (uncertain_reconciliation_digest IS NULL) AND (cost_reconciliation_spec_digest IS NULL OR cost_reconciliation_spec_digest ~ '^[0-9a-f]{64}$') AND (no_call_proof_digest IS NULL OR no_call_proof_digest ~ '^[0-9a-f]{64}$') AND (uncertain_reconciliation_digest IS NULL OR uncertain_reconciliation_digest ~ '^[0-9a-f]{64}$')",
    ),
    (
        "dispatch_exposures_terminal_evidence_shape_ck",
        "(exposure_state IN ('prepared', 'dispatching', 'sent') AND observed_amount IS NULL AND accounted_amount IS NULL AND usage_status IS NULL AND cost_reconciliation_spec_digest IS NULL AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL AND no_call_proof_ref IS NULL AND uncertain_reconciliation_ref IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'confirmed' AND observed_amount IS NOT NULL AND accounted_amount IS NOT NULL AND usage_status = 'reported' AND cost_reconciliation_spec_digest IS NOT NULL AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL AND no_call_proof_ref IS NULL AND uncertain_reconciliation_ref IS NULL AND reconciled_at IS NOT NULL) OR (exposure_state = 'uncertain' AND accounted_amount = worst_case_amount AND usage_status IN ('reported', 'unavailable', 'invalid') AND cost_reconciliation_spec_digest IS NOT NULL AND no_call_proof_ref IS NULL AND uncertain_reconciliation_ref IS NOT NULL AND reconciled_at IS NOT NULL) OR (exposure_state = 'no_call' AND observed_amount = 0 AND accounted_amount = 0 AND usage_status IS NULL AND cost_reconciliation_spec_digest IS NOT NULL AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL AND no_call_proof_ref IS NOT NULL AND uncertain_reconciliation_ref IS NULL AND reconciled_at IS NOT NULL)",
    ),
    (
        "dispatch_exposures_timestamp_order_ck",
        "updated_at >= created_at AND (dispatching_at IS NULL OR dispatching_at >= created_at) AND (sent_at IS NULL OR (dispatching_at IS NOT NULL AND sent_at >= dispatching_at)) AND (reconciled_at IS NULL OR reconciled_at >= created_at) AND (parent_settled_at IS NULL OR (reconciled_at IS NOT NULL AND parent_settled_at >= reconciled_at))",
    ),
    (
        "dispatch_exposures_state_timestamp_shape_ck",
        "(exposure_state = 'prepared' AND dispatching_at IS NULL AND sent_at IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'dispatching' AND dispatching_at IS NOT NULL AND sent_at IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'sent' AND dispatching_at IS NOT NULL AND sent_at IS NOT NULL AND reconciled_at IS NULL) OR (exposure_state = 'confirmed' AND dispatching_at IS NOT NULL AND sent_at IS NOT NULL AND reconciled_at IS NOT NULL) OR (exposure_state = 'uncertain' AND dispatching_at IS NOT NULL AND reconciled_at IS NOT NULL) OR (exposure_state = 'no_call' AND sent_at IS NULL AND reconciled_at IS NOT NULL)",
    ),
    (
        "dispatch_exposures_parent_settled_at_shape_ck",
        "(parent_settlement_state = 'applied') = (parent_settled_at IS NOT NULL)",
    ),
)

EXPECTED_METHODS = (
    "create_or_exact_replay_reservation",
    "prepare_or_exact_replay_exposure",
    "authorize_dispatch",
    "mark_sent",
    "reconcile_exposure_terminal",
    "settle_reservation_pending_exposures",
    "begin_close_reservation",
    "finish_close_reservation",
)

EXPECTED_METHOD_ROWS = (
    (
        "1",
        "create_or_exact_replay_reservation",
        "absent to `open`; after exact mode/pricing proof install `(R,R,0,0,0,0)`, where live `R>0` and simulate/scripted `R=0`; replay ineligible",
    ),
    (
        "2",
        "prepare_or_exact_replay_exposure",
        "absent to `prepared/not_ready`; exact mode-specific `W` is live-positive or simulate/scripted-zero; parent vector unchanged",
    ),
    (
        "3",
        "authorize_dispatch",
        "`prepared/not_ready` to `dispatching/not_ready`; live atomically moves `available -= W`, `held += W`; simulate/scripted make the same authorization CAS with zero vector delta; commit is send authorization",
    ),
    (
        "4",
        "mark_sent",
        "`dispatching/not_ready` to `sent/not_ready`; parent unchanged; exposure-only CAS after the send attempt",
    ),
    (
        "5",
        "reconcile_exposure_terminal",
        "source-dependent closed mapping only: `prepared -> no_call`; `dispatching -> confirmed/uncertain/no_call`; `sent -> confirmed/uncertain`; always `parent_settlement_state=pending`; parent unchanged",
    ),
    (
        "6",
        "settle_reservation_pending_exposures",
        "credential-free `pending` to `applied`; apply each child delta once under parent-first sorted locks",
    ),
    (
        "7",
        "begin_close_reservation",
        "parent `open` to `closing`; forbid new prepare/authorize while pending settlement drains",
    ),
    (
        "8",
        "finish_close_reservation",
        "parent `closing` to `closed`; require no held/pending/not-ready child, move remaining available to released, set `closed_at`",
    ),
)

EXPECTED_PRICING_KEYS = (
    "pricing_spec_id",
    "schema_version",
    "transport_kind",
    "provider_mode",
    "provider",
    "api_style",
    "requested_model",
    "effective_route_snapshot_digest",
    "currency_code",
    "numeric_precision",
    "numeric_scale",
    "rounding_mode",
    "minimum_positive_quantum",
    "fx_policy",
    "component_rates",
)

EXPECTED_PRICING_MODE_ROWS = (
    (
        "live",
        "one",
        "non-empty canonical sorted component/unit/USD-rate tuple",
        "positive after ceiling quantization",
    ),
    (
        "simulate",
        "one immutable zero-cost spec",
        "exact empty tuple",
        "exactly zero",
    ),
    (
        "scripted",
        "one immutable zero-cost spec",
        "exact empty tuple",
        "exactly zero",
    ),
    ("replay", "zero in initial v1", "not applicable", "row creation fails closed"),
)

EXPECTED_RECONCILIATION_KEYS = (
    "spec_id",
    "schema_version",
    "terminal_state",
    "required_evidence_variant",
    "usage_status_policy",
    "accounted_amount_rule",
    "parent_delta_rule",
    "terminal",
    "late_invoice_policy",
)

EXPECTED_RECONCILIATION_ROWS = (
    (
        "cost-reconciliation-confirmed-v1",
        "confirmed",
        "authenticated response receipt plus reported usage",
        "rounded observed amount",
        "held to accounted, release unused, record positive overrun",
        "future append-only adjustment only",
    ),
    (
        "cost-reconciliation-uncertain-v1",
        "uncertain",
        "complete uncertain reconciliation proof; receipt optional but exclusive",
        "worst-case amount",
        "held to accounted at worst case",
        "future append-only adjustment only",
    ),
    (
        "cost-reconciliation-no-call-v1",
        "no_call",
        "complete no-call proof and no receipt",
        "zero",
        "prepared parent unchanged; dispatching held to released",
        "no adjustment",
    ),
)

FORBIDDEN_EXPOSURE_PROVIDER_CALL_COLUMNS = frozenset(
    {
        "provider_call_id",
        "provider_request_id",
        "provider_run_id",
        "wire_call_id",
        "response_occurrence_id",
        "failure_occurrence_id",
    }
)

FUTURE_TABLES = frozenset({"cost_reservations", "dispatch_exposures"})
FUTURE_SYMBOLS = frozenset(
    {
        "CostLedgerRepository",
        "COST_PRICING_SPECS",
        "COST_RECONCILIATION_SPECS",
    }
)


def _normalized(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def _section(document: str, start: str, end: str | None = None) -> str:
    start_index = document.index(start)
    if end is None:
        return document[start_index:]
    return document[start_index : document.index(end, start_index)]


def _strip_code(value: str) -> str:
    if len(value) >= 2 and value.startswith("`") and value.endswith("`"):
        return value[1:-1]
    return value


def _markdown_tables(section: str) -> tuple[tuple[tuple[str, ...], tuple[tuple[str, ...], ...]], ...]:
    raw_tables: list[list[tuple[str, ...]]] = []
    current: list[tuple[str, ...]] = []
    for line in section.splitlines():
        if line.startswith("|"):
            cells = tuple(cell.strip() for cell in line.strip().strip("|").split("|"))
            if not all(re.fullmatch(r":?-+:?", cell) for cell in cells):
                current.append(cells)
        elif current:
            raw_tables.append(current)
            current = []
    if current:
        raw_tables.append(current)
    assert raw_tables
    return tuple((table[0], tuple(table[1:])) for table in raw_tables)


def _bare_rows(rows: tuple[tuple[str, ...], ...]) -> tuple[tuple[str, ...], ...]:
    return tuple(tuple(_strip_code(cell) for cell in row) for row in rows)


def _source_units() -> tuple[tuple[Path, str, ast.Module], ...]:
    return tuple(
        (path, source, ast.parse(source))
        for path in sorted(SOURCE_ROOT.rglob("*.py"))
        if (source := path.read_text(encoding="utf-8"))
    )


def _terminal_call_name(call: ast.Call) -> str:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return ""


def _descriptor_tables() -> frozenset[str]:
    tables: set[str] = set()
    for _, _, tree in _source_units():
        for call in (node for node in ast.walk(tree) if isinstance(node, ast.Call)):
            if _terminal_call_name(call) != "TableDescriptor":
                continue
            table_keyword = next((keyword for keyword in call.keywords if keyword.arg == "table"), None)
            assert table_keyword is not None
            assert isinstance(table_keyword.value, ast.Constant)
            assert isinstance(table_keyword.value.value, str)
            tables.add(table_keyword.value.value)
    return frozenset(tables)


def _defined_top_level_symbols() -> frozenset[str]:
    symbols: set[str] = set()
    for _, _, tree in _source_units():
        for node in tree.body:
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                symbols.add(node.name)
            elif isinstance(node, ast.Assign):
                symbols.update(target.id for target in node.targets if isinstance(target, ast.Name))
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                symbols.add(node.target.id)
    return frozenset(symbols)


def test_exact_ordered_parent_and_child_manifests_and_scope_prefix() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    parent_section = _section(document, "## 4. Exact `cost_reservations`", "## 5. Exact `dispatch_exposures`")
    child_section = _section(document, "## 5. Exact `dispatch_exposures`", "## 6. Exact keys")
    parent_header, parent_rows = _markdown_tables(parent_section)[0]
    child_header, child_rows = _markdown_tables(child_section)[0]

    assert tuple(_strip_code(cell) for cell in parent_header) == (
        "position",
        "column",
        "SQL type",
        "nullable",
        "default",
    )
    assert tuple(_strip_code(cell) for cell in child_header) == tuple(_strip_code(cell) for cell in parent_header)
    assert _bare_rows(parent_rows) == EXPECTED_PARENT_ROWS
    assert _bare_rows(child_rows) == EXPECTED_CHILD_ROWS
    assert tuple(row[1] for row in EXPECTED_PARENT_ROWS[:5]) == EXPECTED_SCOPE_PREFIX
    assert tuple(row[1] for row in EXPECTED_CHILD_ROWS[:5]) == EXPECTED_SCOPE_PREFIX

    child_columns = {row[1] for row in EXPECTED_CHILD_ROWS}
    assert FORBIDDEN_EXPOSURE_PROVIDER_CALL_COLUMNS.isdisjoint(child_columns)
    assert {row[2] for row in EXPECTED_PARENT_ROWS if row[1].endswith("_amount")} == {"NUMERIC(38,12)"}
    assert {row[2] for row in EXPECTED_CHILD_ROWS if row[1].endswith("_amount")} == {"NUMERIC(38,12)"}
    assert "deliberately corrects" in child_section
    for forbidden_column in FORBIDDEN_EXPOSURE_PROVIDER_CALL_COLUMNS:
        assert f"`{forbidden_column}`" in child_section


def test_exact_key_index_and_local_check_manifests() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    key_section = _section(document, "## 6. Exact keys", "## 7. Exact local checks")
    check_section = _section(document, "## 7. Exact local checks", "## 8. Immutable pricing")
    key_tables = _markdown_tables(key_section)
    check_tables = _markdown_tables(check_section)

    assert len(key_tables) == 2
    assert _bare_rows(key_tables[0][1]) == EXPECTED_KEY_ROWS
    assert _bare_rows(key_tables[1][1]) == EXPECTED_INDEX_ROWS
    assert len(check_tables) == 2
    assert tuple((_strip_code(row[1]), _strip_code(row[2])) for row in check_tables[0][1]) == EXPECTED_PARENT_CHECKS
    assert tuple((_strip_code(row[1]), _strip_code(row[2])) for row in check_tables[1][1]) == EXPECTED_CHILD_CHECKS
    assert len(EXPECTED_PARENT_CHECKS) == 16
    assert len(EXPECTED_CHILD_CHECKS) == 29
    assert "Cross-row equality and registry applicability remain repository/FK acceptance" in _normalized(check_section)


def test_registry_shapes_records_money_and_late_invoice_policy_are_exact() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    section = _section(document, "## 8. Immutable pricing", "## 9. Repository CAS")
    code_blocks = re.findall(r"```text\n(.*?)\n```", section, flags=re.DOTALL)

    assert len(code_blocks) == 2
    assert tuple(part.strip() for part in code_blocks[0].replace("\n", " ").split(",")) == EXPECTED_PRICING_KEYS
    assert tuple(part.strip() for part in code_blocks[1].replace("\n", " ").split(",")) == EXPECTED_RECONCILIATION_KEYS
    registry_tables = _markdown_tables(section)
    assert len(registry_tables) == 2
    pricing_header, pricing_rows = registry_tables[0]
    assert tuple(_strip_code(cell) for cell in pricing_header) == (
        "provider mode",
        "exact applicable-record count",
        "component_rates",
        "reservation / exposure money",
    )
    assert _bare_rows(pricing_rows) == EXPECTED_PRICING_MODE_ROWS

    registry_header, registry_rows = registry_tables[1]
    assert tuple(_strip_code(cell) for cell in registry_header) == (
        "spec id",
        "terminal state",
        "required evidence",
        "accounted amount rule",
        "parent delta rule",
        "late invoice policy",
    )
    assert _bare_rows(registry_rows) == EXPECTED_RECONCILIATION_ROWS

    normalized = _normalized(section)
    for exact_rule in (
        "`currency_code='USD'`",
        "`numeric_precision=38`",
        "`numeric_scale=12`",
        "`rounding_mode='ROUND_CEILING'`",
        "`minimum_positive_quantum='0.000000000001'`",
        "`fx_policy='forbidden'`",
        "All three have `terminal=true` and apply to `live|simulate|scripted`",
        "Price changes append a new immutable mode-specific spec/digest",
        "simulate/scripted remain exactly zero",
        "No wildcard mode/price, live settings lookup, floating point, implicit currency conversion, or FX fallback",
        "apply to `live|simulate|scripted` only through an exposure with the exact same PFX provider mode",
        "Confirmed simulate/scripted usage may remain `reported`, but its observed/accounted money is exactly zero.",
    ):
        assert exact_rule in normalized


def test_repository_surface_lifecycle_amount_transitions_and_lock_orders_are_closed() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    lifecycle = _section(document, "## 9. Repository CAS", "## 10. Lock orders")
    locks = _section(document, "## 10. Lock orders", "## 11. Mode semantics")
    _, method_rows = _markdown_tables(lifecycle)[0]

    assert tuple(_strip_code(row[1]) for row in method_rows) == EXPECTED_METHODS
    assert tuple(row[0] for row in method_rows) == tuple(str(index) for index in range(1, 9))
    assert _bare_rows(method_rows) == EXPECTED_METHOD_ROWS
    terminal_effect = _normalized(_strip_code(method_rows[4][2]))
    assert terminal_effect == _normalized(
        "source-dependent closed mapping only: `prepared -> no_call`; "
        "`dispatching -> confirmed/uncertain/no_call`; "
        "`sent -> confirmed/uncertain`; always `parent_settlement_state=pending`; parent unchanged"
    )
    normalized_lifecycle = _normalized(lifecycle)
    for exact_rule in (
        "Every method requires the full prefix, exact immutable pins, `expected_state_version`, and an enumerated expected state.",
        "Exact replay returns the existing exact result with zero writes",
        "prepared -> dispatching | no_call",
        "dispatching -> sent | confirmed | uncertain | no_call",
        "sent -> confirmed | uncertain",
        "confirmed | uncertain | no_call -> terminal forever",
        "`prepared -> no_call` has no prior hold",
        "`dispatching -> no_call` has a committed hold",
        "`held -= W; released += W`",
        "`held -= W; accounted += A; released += max(W-A,0); overrun += max(A-W,0)`",
        "`held -= W; accounted += W`",
        "reserved_amount + overrun_amount = available_amount + held_amount + accounted_amount + released_amount",
        "Exact replay returns the existing exact result with zero writes only under exact provider-mode equality",
        "Simulate/scripted execute the same durable authorization and settlement CASs with `R=W=A=0`",
        "Replay has no initial-v1 row or CAS path.",
        "`uncertain` never transitions to `confirmed`",
    ):
        assert exact_rule in normalized_lifecycle
    assert "`prepared -> confirmed`" not in normalized_lifecycle
    assert "`prepared -> uncertain`" not in normalized_lifecycle
    assert "`sent -> no_call`" not in normalized_lifecycle
    assert "`prepared`, `dispatching`, or `sent` to exactly one of" not in normalized_lifecycle

    for exact_rule in (
        "All lifecycle clocks are repository-owned PostgreSQL clocks.",
        "only from the current transaction's `transaction_timestamp()`",
        "callers may supply neither timestamps nor a boolean/flag that claims a send occurred",
        "The direct `dispatching -> confirmed` branch is legal only when an authenticated response receipt proves that wire send occurred.",
        "the repository fills the previously-null `sent_at` from PostgreSQL `transaction_timestamp()`",
        "`dispatching -> uncertain` and `dispatching -> no_call` must leave `sent_at` NULL",
        "A `sent`-origin terminal transition preserves the already committed `sent_at` exactly.",
    ):
        assert exact_rule in normalized_lifecycle

    numbered_orders = re.findall(r"(?m)^[1-3]\. \*\*([^*]+):\*\*", locks)
    assert numbered_orders == [
        "Pre-network authorization",
        "Post-network evidence",
        "Credential-free settlement/close",
    ]
    normalized_locks = _normalized(locks)
    for exact_rule in (
        "`operation root -> optional plan/review/gate -> participating commands sorted -> intent/predecessor -> ActivityRun/Attempt -> optional grant -> cost_reservations -> dispatch_exposures`",
        "`dispatch_exposures FOR UPDATE -> applicable receipt -> optional response quarantine`",
        "This UoW never locks or updates `cost_reservations`",
        "That restriction removes the exposure-to-parent lock inversion.",
        "`cost_reservations FOR UPDATE -> pending dispatch_exposures ORDER BY dispatch_exposure_id FOR UPDATE`",
        "after every child is locked and exact-checked, update the parent vector",
        "No method holds a DB transaction during DNS, connect, request bytes, response streaming, provider polling, invoice lookup, or any other network I/O.",
    ):
        assert exact_rule in normalized_locks


def test_owner_mode_grant_receipt_retention_and_d0_reference_semantics_are_exact() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    owner = _section(document, "## 3. Owner, aggregate", "## 4. Exact `cost_reservations`")
    retention = _section(document, "## 11. Mode semantics", "## 12. Mechanism")
    normalized = _normalized(owner + " " + retention)

    for exact_rule in (
        "`src/sourcing_agent/repositories/cost_ledger.py::CostLedgerRepository`",
        "`store.repos.cost_ledger`",
        "`transport_kind='model_tool_v1'`",
        "Strict-D3 live, simulate, and scripted model turns all create one reservation plus one exposure per physical call.",
        "`ModelInvocationEnvelopeV1.cost_exposure_ref == dispatch_exposure_id` exactly",
        "strict-D3 `simulate|scripted`: every reservation/exposure/accounting amount exactly zero",
        "`replay`: global PFX grammar retained, but no current D0 envelope enum or applicable pricing spec",
        "non-strict D0 fixtures without the durable strict-D3 owner: no ledger row and typed `cost_exposure_ref=None`",
        "Tier-1: `grant_tier='tier1'` and all three grant pins SQL NULL",
        "Tier-2: `grant_tier='tier2'` and all three grant pins complete",
        "missing usage is never zero",
        "no delete, purge, rewrite, or state-reopen API exists",
        "`uncertain` is immutable",
        "future append-only cost-adjustment owner/table",
    ):
        assert exact_rule in normalized

    assert "cost_exposure_ref" in MODEL_INVOCATION_ENVELOPE_RECORD_KEYS
    assert "cost_exposure_id" not in MODEL_INVOCATION_ENVELOPE_RECORD_KEYS
    assert "physical_call_exposure_id" not in MODEL_INVOCATION_ENVELOPE_RECORD_KEYS

    d0_design = D0_DESIGN_PATH.read_text(encoding="utf-8")
    assert "route_revision: str                      # route 完整内容 SHA-256" in d0_design
    assert "provider_mode: str                       # simulate | scripted | live" in d0_design
    decision_check = dict(EXPECTED_CHILD_CHECKS)["dispatch_exposures_decision_pin_shape_ck"]
    assert "route_revision ~ '^[0-9a-f]{64}$'" in decision_check

    d3b_contract = D3B_CONTRACT_PATH.read_text(encoding="utf-8")
    assert "After a committed dispatch exposure receives a valid terminal response" in d3b_contract
    assert "caller flags and a fabricated D0 envelope are forbidden" in d3b_contract
    assert "The `no_exposure` branch does **not** lock a nonexistent exposure row." in d3b_contract

    stale_contradictions = (
        "non-live: no parent, no child",
        "provider_mode live and non-live no-row",
        "live-only physical check",
        "Only strict-D3 live commands may create rows",
    )
    for contradiction in stale_contradictions:
        assert contradiction not in document
    assert document.count("live-only draft") == 2


def test_decision_inventory_matrix_and_ob_status_are_complete_and_nonimplementing() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    provenance = _section(document, "## 2. Decision provenance", "## 3. Owner, aggregate")
    matrix = _section(document, "## 12. Mechanism", "## 13. Physical absence")
    _, decision_rows = _markdown_tables(provenance)[0]
    matrix_header, matrix_rows = _markdown_tables(matrix)[0]

    assert tuple(row[0] for row in decision_rows) == tuple(
        [f"A-{index}" for index in range(1, 9)] + [f"R-{index}" for index in range(1, 13)]
    )
    assert tuple(_strip_code(row[1]) for row in decision_rows[:8]) == ("[A] derived",) * 8
    assert tuple(_strip_code(row[1]) for row in decision_rows[8:]) == ("[R] new",) * 12
    assert all(len(row) == 3 and row[2] for row in decision_rows)
    assert "valid strict-D3 simulate/scripted D0 responses therefore require" in decision_rows[7][2]
    assert "Correcting the superseded live-only draft from A-8 evidence" in decision_rows[11][2]

    assert tuple(_strip_code(cell) for cell in matrix_header) == (
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
    assert tuple(row[0] for row in matrix_rows) == (
        "cost reservation aggregate root",
        "dispatch exposure physical-call child",
        "reconciliation registry and settlement flow",
    )
    assert len(matrix_rows) == 3
    assert sum(len(row) - 1 for row in matrix_rows) == 30
    assert all(len(row) == 11 and all(cell for cell in row[1:]) for row in matrix_rows)
    assert "Every one of the 30 cells is populated. No cell claims runtime implementation." in matrix

    assert document.count("OB-2.2") >= 3
    assert document.count("OB-10.3") >= 3
    assert document.count("decision_locked_not_implemented") >= 3
    assert "OB-2.2 and OB-10.3 advance only to `decision_locked_not_implemented`." in document


def test_current_physical_absence_baseline_is_mechanical_and_exact() -> None:
    baseline_sql = BASELINE_MIGRATION_PATH.read_text(encoding="utf-8")
    baseline_tables = frozenset(
        re.findall(
            r"(?im)^CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:[a-z_][a-z0-9_]*\.)?([a-z_][a-z0-9_]*)",
            baseline_sql,
        )
    )
    all_migration_source = "\n".join(path.read_text(encoding="utf-8") for path in sorted(MIGRATIONS_ROOT.glob("*.sql")))
    descriptor_tables = _descriptor_tables()
    defined_symbols = _defined_top_level_symbols()

    assert len(baseline_tables) == 83
    assert len(descriptor_tables) == 41
    assert FUTURE_TABLES.isdisjoint(baseline_tables)
    assert FUTURE_TABLES.isdisjoint(descriptor_tables)
    for table_name in FUTURE_TABLES:
        assert re.search(rf"(?i)\bCREATE\s+TABLE\s+{table_name}\b", all_migration_source) is None
    assert FUTURE_SYMBOLS.isdisjoint(defined_symbols)
    assert FUTURE_REPOSITORY_PATH.exists() is False
    repositories_init = REPOSITORIES_INIT_PATH.read_text(encoding="utf-8")
    assert "cost_ledger" not in repositories_init

    control_plane_source = CONTROL_PLANE_REPOSITORY_PATH.read_text(encoding="utf-8")
    control_plane_tree = ast.parse(control_plane_source)
    kind_classes = [node for node in control_plane_tree.body if isinstance(node, ast.ClassDef) and node.name == "Kind"]
    assert len(kind_classes) == 1
    kind_members = {
        target.id
        for node in kind_classes[0].body
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert kind_members == {"STR", "INT", "FLOAT", "BOOL_INT", "JSON", "JSON_LIST", "JSON_STR_LIST"}
    assert {"DECIMAL", "TIMESTAMPTZ"}.isdisjoint(kind_members)
    assert 'f"ON CONFLICT ({conflict}) DO UPDATE SET {assignments} RETURNING *"' in control_plane_source

    model_runtime_source = MODEL_TOOL_RUNTIME_PATH.read_text(encoding="utf-8")
    assert 'if self.provider_mode not in {"simulate", "scripted", "live"}:' in model_runtime_source
    assert "model_invocation_envelope_ref" not in model_runtime_source
    d0_contract = D0_CONTRACT_PATH.read_text(encoding="utf-8")
    assert "Immutable in-memory value; deterministic round trip" in d0_contract


def test_trackers_record_decision_lock_without_closing_runtime_or_rollout_gates() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    nonclosure = _section(document, "## 14. Explicit non-closure")
    normalized = _normalized(nonclosure)

    for marker in (
        "`R-019`",
        "`R-023`",
        "`R-027`",
        "`R-029`",
        "action-root durable scope",
        "OB-10.1",
        "OB-10.2",
        "OB-10.4",
        "complete Migration A",
        "Migration B-D",
        "live/W6/manual signoff",
        "served Agent population",
        "verification-intent",
        "response/failure receipt",
        "late-quarantine",
        "D3c2g does not authorize a cost-ledger migration by itself",
        "no exact `Decimal`/`NUMERIC(38,12)` or `TIMESTAMPTZ` codec family",
        "specialized insert-once/exact-replay/CAS primitives",
        "generic replace-all upsert",
        "durable D0 envelope owner/reference grammar",
        "source defines no `model_invocation_envelope_ref` issuer",
        "response-only quarantine `reconciled_no_call` reachability",
        "receipt-side `command_attempt` identity",
    ):
        assert marker in normalized

    tracker_requirements = {
        INDEX_PATH: (
            "TRACK_D_D3C2G_COST_LEDGER_DISPATCH_EXPOSURE_DECISION_LOCK.md",
            "decision_locked_not_implemented",
            "live/simulate/scripted",
            "replay",
        ),
        TODO_PATH: (
            "D3c2g",
            "21/75",
            "decision_locked_not_implemented",
            "live/simulate/scripted",
            "replay",
            "D3c2h",
        ),
        LEDGER_PATH: (
            "R-019 / D3c2g",
            "OB-2.2",
            "OB-10.3",
            "decision_locked_not_implemented",
            "live/simulate/scripted",
            "generic replace-all upsert is forbidden",
        ),
        PLAN_PATH: (
            "D3c2g",
            "21/75",
            "OB-2.2",
            "OB-10.3",
            "decision_locked_not_implemented",
            "live/simulate/scripted",
            "replay",
        ),
    }
    for path, markers in tracker_requirements.items():
        source = path.read_text(encoding="utf-8")
        for marker in markers:
            assert marker in source, f"{path.name} missing {marker}"
