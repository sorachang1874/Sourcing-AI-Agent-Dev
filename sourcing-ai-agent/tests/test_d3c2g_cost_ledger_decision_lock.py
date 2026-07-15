from __future__ import annotations

import ast
import hashlib
import json
import re
from decimal import (
    ROUND_CEILING,
    Decimal,
    Inexact,
    InvalidOperation,
    Rounded,
    localcontext,
)
from decimal import (
    Overflow as DecimalOverflow,
)
from pathlib import Path

import pytest

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
    ("66", "cost_terminal_evidence_variant", "TEXT", "yes", "none"),
    ("67", "cost_terminal_evidence_id", "TEXT", "yes", "none"),
    ("68", "cost_terminal_evidence_digest", "TEXT", "yes", "none"),
    ("69", "cost_terminal_evidence_record", "JSONB", "yes", "none"),
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
        "((base_intent_id IS NULL AND base_intent_phase_generation IS NULL) OR (base_intent_id ~ '[^[:space:]]' AND base_intent_phase_generation > 0)) IS TRUE",
    ),
    (
        "dispatch_exposures_predecessor_tuple_ck",
        "((expected_predecessor_intent_id IS NULL AND expected_predecessor_phase_generation IS NULL AND expected_predecessor_source_control_epoch IS NULL AND expected_predecessor_decision_source_event_id IS NULL) OR (expected_predecessor_intent_id ~ '[^[:space:]]' AND expected_predecessor_phase_generation > 0 AND expected_predecessor_source_control_epoch >= 0 AND expected_predecessor_decision_source_event_id ~ '[^[:space:]]')) IS TRUE",
    ),
    (
        "dispatch_exposures_decision_pin_shape_ck",
        "fingerprint_version ~ '[^[:space:]]' AND fingerprint_digest ~ '^[0-9a-f]{64}$' AND decision_generation > 0 AND decision_source_event_id ~ '[^[:space:]]' AND accepted_policy_revision ~ '[^[:space:]]' AND schema_revision ~ '[^[:space:]]' AND route_revision ~ '^[0-9a-f]{64}$' AND effective_route_snapshot_digest ~ '^[0-9a-f]{64}$'",
    ),
    (
        "dispatch_exposures_grant_tuple_ck",
        "((grant_tier = 'tier1' AND grant_id IS NULL AND grant_issuance_generation IS NULL AND grant_policy_revision IS NULL) OR (grant_tier = 'tier2' AND grant_id ~ '[^[:space:]]' AND grant_issuance_generation > 0 AND grant_policy_revision ~ '[^[:space:]]')) IS TRUE",
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
        "dispatch_exposures_cost_evidence_shape_ck",
        "((cost_reconciliation_spec_digest IS NULL OR cost_reconciliation_spec_digest ~ '^[0-9a-f]{64}$') AND ((cost_terminal_evidence_variant IS NULL AND cost_terminal_evidence_id IS NULL AND cost_terminal_evidence_digest IS NULL AND cost_terminal_evidence_record IS NULL) OR (cost_terminal_evidence_variant IS NOT NULL AND cost_terminal_evidence_id IS NOT NULL AND cost_terminal_evidence_digest IS NOT NULL AND cost_terminal_evidence_record IS NOT NULL AND cost_terminal_evidence_variant IN ('response_reported_v1', 'response_usage_unavailable_v1', 'response_usage_invalid_v1', 'attempt_failure_conservative_v1', 'timeout_crash_conservative_v1', 'prepared_no_dispatch_v1', 'zero_wire_abort_v1') AND cost_terminal_evidence_id ~ '[^[:space:]]' AND cost_terminal_evidence_digest ~ '^[0-9a-f]{64}$' AND jsonb_typeof(cost_terminal_evidence_record) = 'object'))) IS TRUE",
    ),
    (
        "dispatch_exposures_terminal_evidence_shape_ck",
        "((exposure_state IN ('prepared', 'dispatching', 'sent') AND observed_amount IS NULL AND accounted_amount IS NULL AND usage_status IS NULL AND cost_reconciliation_spec_digest IS NULL AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL AND cost_terminal_evidence_variant IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'confirmed' AND observed_amount IS NOT NULL AND accounted_amount = observed_amount AND usage_status = 'reported' AND cost_reconciliation_spec_digest IS NOT NULL AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL AND cost_terminal_evidence_variant = 'response_reported_v1' AND reconciled_at IS NOT NULL) OR (exposure_state = 'uncertain' AND observed_amount IS NULL AND accounted_amount = worst_case_amount AND cost_reconciliation_spec_digest IS NOT NULL AND reconciled_at IS NOT NULL AND ((cost_terminal_evidence_variant = 'response_usage_unavailable_v1' AND usage_status = 'unavailable' AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL) OR (cost_terminal_evidence_variant = 'response_usage_invalid_v1' AND usage_status = 'invalid' AND transport_response_receipt_id IS NOT NULL AND transport_attempt_failure_receipt_id IS NULL) OR (cost_terminal_evidence_variant = 'attempt_failure_conservative_v1' AND usage_status = 'unavailable' AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NOT NULL) OR (cost_terminal_evidence_variant = 'timeout_crash_conservative_v1' AND usage_status = 'unavailable' AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL))) OR (exposure_state = 'no_call' AND observed_amount = 0 AND accounted_amount = 0 AND usage_status IS NULL AND cost_reconciliation_spec_digest IS NOT NULL AND transport_response_receipt_id IS NULL AND transport_attempt_failure_receipt_id IS NULL AND cost_terminal_evidence_variant IN ('prepared_no_dispatch_v1', 'zero_wire_abort_v1') AND reconciled_at IS NOT NULL)) IS TRUE",
    ),
    (
        "dispatch_exposures_timestamp_order_ck",
        "updated_at >= created_at AND (dispatching_at IS NULL OR dispatching_at >= created_at) AND (sent_at IS NULL OR (dispatching_at IS NOT NULL AND sent_at >= dispatching_at)) AND (reconciled_at IS NULL OR reconciled_at >= created_at) AND (parent_settled_at IS NULL OR (reconciled_at IS NOT NULL AND parent_settled_at >= reconciled_at))",
    ),
    (
        "dispatch_exposures_state_timestamp_shape_ck",
        "((exposure_state = 'prepared' AND dispatching_at IS NULL AND sent_at IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'dispatching' AND dispatching_at IS NOT NULL AND sent_at IS NULL AND reconciled_at IS NULL) OR (exposure_state = 'sent' AND dispatching_at IS NOT NULL AND sent_at IS NOT NULL AND reconciled_at IS NULL) OR (exposure_state = 'confirmed' AND dispatching_at IS NOT NULL AND sent_at IS NOT NULL AND reconciled_at IS NOT NULL) OR (exposure_state = 'uncertain' AND dispatching_at IS NOT NULL AND reconciled_at IS NOT NULL AND (cost_terminal_evidence_variant NOT IN ('response_usage_unavailable_v1', 'response_usage_invalid_v1') OR sent_at IS NOT NULL)) OR (exposure_state = 'no_call' AND sent_at IS NULL AND reconciled_at IS NOT NULL AND ((cost_terminal_evidence_variant = 'prepared_no_dispatch_v1' AND dispatching_at IS NULL) OR (cost_terminal_evidence_variant = 'zero_wire_abort_v1' AND dispatching_at IS NOT NULL)))) IS TRUE",
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
        "absent to `open`; exact budget-policy lookup derives `(R,R,0,0,0,0)`, where live `R>0` and simulate/scripted `R=0`; replay ineligible; caller money forbidden",
    ),
    (
        "2",
        "prepare_or_exact_replay_exposure",
        "absent to `prepared/not_ready`; under the parent lock enforce the policy call-count cap and derive exact mode-specific `W` from ceiling quantities plus pricing; parent vector unchanged; caller money forbidden",
    ),
    (
        "3",
        "authorize_dispatch",
        "`prepared/not_ready` to `dispatching/not_ready`; live atomically moves `available -= W`, `held += W`; simulate/scripted make the same authorization CAS with zero vector delta; after commit return the private exposure/version/request-bound dispatch capability",
    ),
    (
        "4",
        "mark_sent",
        "`dispatching/not_ready` to `sent/not_ready`; parent unchanged; exposure-only CAS after the send attempt",
    ),
    (
        "5",
        "reconcile_exposure_terminal",
        "source/evidence-dependent closed mapping only: `prepared -> no_call(prepared_no_dispatch)`; `dispatching -> confirmed(response) / uncertain(response, failure, or timeout) / no_call(zero_wire)`; `sent -> confirmed(response) / uncertain(response, failure, or timeout)`; owner derives `A` or conservative `W/0`, installs the canonical evidence record, and sets `parent_settlement_state=pending`; parent unchanged",
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
    "rate_scale",
    "rounding_mode",
    "minimum_positive_quantum",
    "fx_policy",
    "component_rates",
)

EXPECTED_PRICING_MODE_ROWS = (
    (
        "live",
        "one",
        "exact four-record tuple, with at least one positive applicable rate",
        "`R/W` positive; owner-derived `A` may be zero",
    ),
    (
        "simulate",
        "one immutable zero-cost spec per eligible route/snapshot tuple",
        "exact four-record tuple with all rates zero",
        "`R/W/A` exactly zero",
    ),
    (
        "scripted",
        "one immutable zero-cost spec per eligible route/snapshot tuple",
        "exact four-record tuple with all rates zero",
        "`R/W/A` exactly zero",
    ),
    ("replay", "zero in initial v1", "not applicable", "row creation fails closed"),
)

EXPECTED_BUDGET_POLICY_KEYS = (
    "budget_policy_id",
    "schema_version",
    "transport_kind",
    "provider_mode",
    "budget_class",
    "effective_route_snapshot_digest",
    "cost_pricing_spec_digest",
    "currency_code",
    "monetary_ceiling",
    "max_physical_calls",
    "max_uncached_input_tokens_per_call",
    "max_cached_input_tokens_per_call",
    "max_non_reasoning_output_tokens_per_call",
    "max_reasoning_output_tokens_per_call",
    "max_total_tokens_per_call",
    "reservation_amount_rule",
    "per_call_worst_case_rule",
)

EXPECTED_BUDGET_MODE_ROWS = (
    (
        "live",
        "one per route/snapshot/budget/pricing tuple",
        "R > 0 and W > 0",
        "`R >= max_physical_calls * W`, with exact Decimal multiplication and no overflow",
    ),
    (
        "simulate",
        "one immutable zero-money policy per eligible tuple",
        "R = W = 0",
        "applicable pricing has four zero rates; coverage is exactly zero",
    ),
    (
        "scripted",
        "one immutable zero-money policy per eligible tuple",
        "R = W = 0",
        "applicable pricing has four zero rates; coverage is exactly zero",
    ),
    ("replay", "zero in initial v1", "not applicable", "row creation fails closed"),
)

EXPECTED_RECONCILIATION_KEYS = (
    "spec_id",
    "schema_version",
    "terminal_state",
    "evidence_variant",
    "eligible_source_states",
    "required_receipt_variant",
    "usage_status_policy",
    "accounted_amount_rule",
    "parent_delta_rule",
    "sent_at_rule",
    "minimum_age_seconds",
    "terminal",
    "late_invoice_policy",
)

EXPECTED_RECONCILIATION_ROWS = (
    (
        "cost-confirmed-response-reported-v1",
        "confirmed",
        "response_reported_v1",
        "dispatching or sent",
        "response / complete valid reported usage",
        "`A=price_v1(validated usage)`; held to accounted, release unused, record overrun",
        "set_db_clock_if_dispatching_else_preserve",
        "none / append-only adjustment",
    ),
    (
        "cost-uncertain-response-unavailable-v1",
        "uncertain",
        "response_usage_unavailable_v1",
        "dispatching or sent",
        "response / unavailable",
        "A=NULL; account W",
        "set_db_clock_if_dispatching_else_preserve",
        "none / append-only adjustment",
    ),
    (
        "cost-uncertain-response-invalid-v1",
        "uncertain",
        "response_usage_invalid_v1",
        "dispatching or sent",
        "response / invalid",
        "A=NULL; account W",
        "set_db_clock_if_dispatching_else_preserve",
        "none / append-only adjustment",
    ),
    (
        "cost-uncertain-attempt-failure-v1",
        "uncertain",
        "attempt_failure_conservative_v1",
        "dispatching or sent",
        "attempt-failure / unavailable",
        "A=NULL; account W",
        "leave_null_if_dispatching_else_preserve",
        "none / append-only adjustment",
    ),
    (
        "cost-uncertain-timeout-crash-v1",
        "uncertain",
        "timeout_crash_conservative_v1",
        "dispatching or sent",
        "locked receipt absence / unavailable",
        "A=NULL; account W",
        "leave_null_if_dispatching_else_preserve",
        "positive registry seconds / append-only adjustment",
    ),
    (
        "cost-no-call-prepared-v1",
        "no_call",
        "prepared_no_dispatch_v1",
        "prepared",
        "locked receipt absence / none",
        "`A=0`; parent unchanged",
        "remain_null",
        "none / no adjustment",
    ),
    (
        "cost-no-call-zero-wire-v1",
        "no_call",
        "zero_wire_abort_v1",
        "dispatching",
        "sealed zero-wire observation / none",
        "`A=0`; held to released",
        "remain_null",
        "none / no adjustment",
    ),
)

EXPECTED_EVIDENCE_RECORD_KEYS = (
    "schema_version",
    "variant",
    "runtime_namespace",
    "provider_mode",
    "workspace_id",
    "scope_digest",
    "coordination_plan_review_id",
    "dispatch_exposure_id",
    "source_exposure_state",
    "source_state_version",
    "source_updated_at",
    "evidence_observed_at",
    "payload",
)

EXPECTED_EVIDENCE_ROWS = (
    (
        "response_reported_v1",
        "dispatching or sent",
        "authenticated response",
        "transport_response_receipt_id, model_invocation_envelope_digest, canonical_usage_record, billing_quantities, derived_observed_amount",
        "`confirmed`; dispatching source sets `sent_at` from this CAS DB clock, sent source preserves it",
    ),
    (
        "response_usage_unavailable_v1",
        "dispatching or sent",
        "authenticated response",
        "transport_response_receipt_id, model_invocation_envelope_digest, usage_status, usage_validation_code",
        "`uncertain`; dispatching source sets `sent_at` from this CAS DB clock, sent source preserves it",
    ),
    (
        "response_usage_invalid_v1",
        "dispatching or sent",
        "authenticated response",
        "transport_response_receipt_id, model_invocation_envelope_digest, canonical_usage_record, usage_validation_code",
        "`uncertain`; dispatching source sets `sent_at` from this CAS DB clock, sent source preserves it",
    ),
    (
        "attempt_failure_conservative_v1",
        "dispatching or sent",
        "authenticated attempt-failure",
        "transport_attempt_failure_receipt_id, failure_spec_digest, failure_code, retry_disposition",
        "`uncertain`; dispatching source leaves `sent_at` NULL, sent source preserves it",
    ),
    (
        "timeout_crash_conservative_v1",
        "dispatching or sent",
        "none; locked receipt absence",
        "reconciliation_spec_digest, eligible_at, response_receipt_absent, failure_receipt_absent",
        "`uncertain`; dispatching source leaves `sent_at` NULL, sent source preserves it",
    ),
    (
        "prepared_no_dispatch_v1",
        "prepared",
        "none; locked receipt absence",
        "dispatching_at, sent_at, response_receipt_absent, failure_receipt_absent",
        "`no_call`; both timestamps are canonical JSON null and parent vector is unchanged",
    ),
    (
        "zero_wire_abort_v1",
        "dispatching",
        "none",
        "runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id, dispatch_exposure_id, source_state_version, canonical_request_digest, transport_adapter_id, transport_adapter_revision, failure_phase, request_bytes_written, response_bytes_read, provider_call_id_state",
        "`no_call`; `sent_at` remains NULL and the held amount is released",
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
        "CostTerminalEvidenceRecord",
        "DispatchAuthorizationCapabilityV1",
        "ZeroWireObservationV1",
        "COST_BUDGET_POLICY_SPECS",
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


MODEL_USAGE_FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)
BILLING_QUANTITY_ORDER = (
    "uncached_input_tokens",
    "cached_input_tokens",
    "non_reasoning_output_tokens",
    "reasoning_output_tokens",
)
USD_RATE_PATTERN = re.compile(r"(?:0\.[0-9]{18}|[1-9][0-9]*\.[0-9]{18})\Z")
USD_QUANTUM = Decimal("0.000000000001")
MAX_BIGINT = 9_223_372_036_854_775_807
ZERO_WIRE_OBSERVATION_KEYS = EXPECTED_SCOPE_PREFIX + (
    "dispatch_exposure_id",
    "source_state_version",
    "canonical_request_digest",
    "transport_adapter_id",
    "transport_adapter_revision",
    "failure_phase",
    "request_bytes_written",
    "response_bytes_read",
    "provider_call_id_state",
)
ZERO_WIRE_BINDING_KEYS = ZERO_WIRE_OBSERVATION_KEYS[:10]
ZERO_WIRE_FAILURE_PHASES = frozenset({"dns", "connect", "tls", "before_first_request_byte"})


def _canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def _domain_digest(domain_tag: str, value: object) -> str:
    return hashlib.sha256(domain_tag.encode("ascii") + b"\x00" + _canonical_json(value)).hexdigest()


def _cost_evidence_id(pfx: tuple[object, ...], exposure_id: str, source_version: int, variant: str) -> str:
    return _domain_digest(
        "cost-terminal-evidence-id-v1",
        [*pfx, exposure_id, source_version, variant],
    )


def _usage_evidence_variant(usage: dict[str, object], rates: tuple[str, ...] | None = None) -> str:
    if set(usage) != set(MODEL_USAGE_FIELDS):
        return "response_usage_unavailable_v1"
    if any(
        type(usage[field]) is not int or usage[field] < 0 or usage[field] > MAX_BIGINT for field in MODEL_USAGE_FIELDS
    ):
        return "response_usage_unavailable_v1"
    try:
        quantities = _validated_billing_quantities(usage)
        if rates is not None:
            _price_v1(quantities, rates)
    except (OverflowError, ValueError):
        return "response_usage_invalid_v1"
    return "response_reported_v1"


def _validated_billing_quantities(usage: dict[str, object]) -> tuple[int, int, int, int]:
    if set(usage) != set(MODEL_USAGE_FIELDS):
        raise ValueError("usage keyset")
    if any(
        type(usage[field]) is not int or usage[field] < 0 or usage[field] > MAX_BIGINT for field in MODEL_USAGE_FIELDS
    ):
        raise ValueError("usage integer")
    input_tokens = usage["input_tokens"]
    cached_input_tokens = usage["cached_input_tokens"]
    output_tokens = usage["output_tokens"]
    reasoning_output_tokens = usage["reasoning_output_tokens"]
    total_tokens = usage["total_tokens"]
    assert isinstance(input_tokens, int)
    assert isinstance(cached_input_tokens, int)
    assert isinstance(output_tokens, int)
    assert isinstance(reasoning_output_tokens, int)
    assert isinstance(total_tokens, int)
    if cached_input_tokens > input_tokens:
        raise ValueError("cached overlap")
    if reasoning_output_tokens > output_tokens:
        raise ValueError("reasoning overlap")
    if total_tokens != input_tokens + output_tokens:
        raise ValueError("total validation")
    return (
        input_tokens - cached_input_tokens,
        cached_input_tokens,
        output_tokens - reasoning_output_tokens,
        reasoning_output_tokens,
    )


def _price_v1(quantities: tuple[int, ...], rates: tuple[str, ...]) -> Decimal:
    if len(quantities) != len(BILLING_QUANTITY_ORDER) or len(rates) != len(BILLING_QUANTITY_ORDER):
        raise ValueError("four quantities")
    if any(type(quantity) is not int or quantity < 0 or quantity > MAX_BIGINT for quantity in quantities):
        raise ValueError("quantity integer")
    if any(USD_RATE_PATTERN.fullmatch(rate) is None or len(rate.partition(".")[0]) > 20 for rate in rates):
        raise ValueError("canonical rate")
    try:
        with localcontext() as context:
            context.prec = 80
            context.traps[InvalidOperation] = True
            context.traps[DecimalOverflow] = True
            context.traps[Inexact] = True
            context.traps[Rounded] = True
            raw = sum(
                (Decimal(quantity) * Decimal(rate) for quantity, rate in zip(quantities, rates, strict=True)),
                Decimal(0),
            )
            context.traps[Inexact] = False
            context.traps[Rounded] = False
            amount = raw.quantize(USD_QUANTUM, rounding=ROUND_CEILING)
    except (InvalidOperation, DecimalOverflow) as exc:
        raise ValueError("decimal amount") from exc
    integer_part = format(amount, "f").partition(".")[0]
    if amount < 0 or len(integer_part) > 26:
        raise OverflowError("NUMERIC(38,12)")
    return amount


def _validate_budget_coverage(reserved: Decimal, worst_case: Decimal, max_physical_calls: int, mode: str) -> None:
    if type(max_physical_calls) is not int or not 0 < max_physical_calls <= MAX_BIGINT:
        raise ValueError("max physical calls")
    if mode == "live":
        with localcontext() as context:
            context.prec = 80
            context.traps[Inexact] = True
            context.traps[Rounded] = True
            required = Decimal(max_physical_calls) * worst_case
        if reserved <= 0 or worst_case <= 0 or reserved < required:
            raise ValueError("live coverage")
        return
    if mode in {"simulate", "scripted"}:
        if reserved != 0 or worst_case != 0:
            raise ValueError("non-live money")
        return
    raise ValueError("provider mode")


def _sql_and(*values: bool | None) -> bool | None:
    if False in values:
        return False
    if None in values:
        return None
    return True


def _sql_or(*values: bool | None) -> bool | None:
    if True in values:
        return True
    if None in values:
        return None
    return False


def _postgres_check_accepts(value: bool | None) -> bool:
    return value is not False


def _validate_zero_wire_observation(exposure: dict[str, object], observation: dict[str, object]) -> None:
    if set(observation) != set(ZERO_WIRE_OBSERVATION_KEYS):
        raise ValueError("zero-wire keyset")
    if any(observation[key] != exposure[key] for key in ZERO_WIRE_BINDING_KEYS):
        raise ValueError("zero-wire exposure binding")
    if observation["failure_phase"] not in ZERO_WIRE_FAILURE_PHASES:
        raise ValueError("zero-wire phase")
    for byte_field in ("request_bytes_written", "response_bytes_read"):
        if type(observation[byte_field]) is not int or observation[byte_field] != 0:
            raise ValueError("zero-wire bytes")
    if observation["provider_call_id_state"] != "not_observed":
        raise ValueError("zero-wire provider call")


def _assert_cost_semantics_contract(document: str) -> None:
    normalized = _normalized(document)
    child_section = _section(document, "## 5. Exact `dispatch_exposures`", "## 6. Exact keys")
    child_columns = {_strip_code(row[1]) for row in _markdown_tables(child_section)[0][1]}
    assert {
        "cost_terminal_evidence_variant",
        "cost_terminal_evidence_id",
        "cost_terminal_evidence_digest",
        "cost_terminal_evidence_record",
    }.issubset(child_columns)
    assert {"no_call_proof_ref", "uncertain_reconciliation_ref"}.isdisjoint(child_columns)
    for exact_rule in (
        "uncached_input_tokens = input_tokens - cached_input_tokens",
        "cached_input_tokens = cached_input_tokens",
        "non_reasoning_output_tokens = output_tokens - reasoning_output_tokens",
        "reasoning_output_tokens = reasoning_output_tokens",
        "`total_tokens` is validation-only and is never a fifth billed component.",
        "`total_tokens = input_tokens + output_tokens`",
        "exact `Decimal` under a local precision-80 context",
        "`cost-budget-policy-v1`, `cost-pricing-spec-v1`, and `cost-reconciliation-spec-v1` as the domain tag",
        "R >= max_physical_calls * W",
        "Callers may supply neither `R`, `W`, `A`, `accounted_amount`, a Decimal string, a billing vector, nor a pricing/budget record.",
        "The sole persistent evidence owner is the private `CostTerminalEvidenceRecord` factory inside `CostLedgerRepository`.",
        "Callers cannot supply a record, ref, id, digest, amount, timestamp, receipt classification, byte count, or boolean send claim.",
        "Every `response_receipt_absent`/`failure_receipt_absent` payload value is owner-derived JSON `true`",
        "exact integer `request_bytes_written=0`, exact integer `response_bytes_read=0`",
        "A crash that did not persist this record can never be reconstructed as no-call.",
        "cost-terminal-evidence-id-v1 + 0x00 + canonical_json([runtime_namespace, provider_mode, workspace_id, scope_digest, coordination_plan_review_id, dispatch_exposure_id, source_state_version, variant])",
        "cost-terminal-evidence-record-v1' + 0x00 + canonical_json(the complete record)",
        "A capability or observation from another PFX, exposure, state version, request digest, or adapter is a typed collision with zero writes",
        "An authenticated response receipt proves wire send for both `confirmed` and response-backed `uncertain`.",
        "A direct `dispatching -> uncertain` backed only by attempt failure or timeout/crash leaves `sent_at` NULL",
    ):
        assert exact_rule in normalized

    evidence_section = _section(document, "### 8.4", "### 8.5")
    evidence_header, evidence_rows = _markdown_tables(evidence_section)[0]
    assert tuple(_strip_code(cell) for cell in evidence_header) == (
        "evidence variant",
        "eligible locked source",
        "required receipt",
        "exact payload keyset",
        "terminal / `sent_at` rule",
    )
    assert _bare_rows(evidence_rows) == EXPECTED_EVIDENCE_ROWS


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
    check_section = _section(document, "## 7. Exact local checks", "## 8. Immutable budget")
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
    canonical = _section(document, "### 8.1", "### 8.2")
    budget = _section(document, "### 8.2", "### 8.3")
    pricing = _section(document, "### 8.3", "### 8.4")
    evidence = _section(document, "### 8.4", "### 8.5")
    reconciliation = _section(document, "### 8.5", "## 9. Repository CAS")

    budget_blocks = re.findall(r"```text\n(.*?)\n```", budget, flags=re.DOTALL)
    pricing_blocks = re.findall(r"```text\n(.*?)\n```", pricing, flags=re.DOTALL)
    evidence_blocks = re.findall(r"```text\n(.*?)\n```", evidence, flags=re.DOTALL)
    reconciliation_blocks = re.findall(r"```text\n(.*?)\n```", reconciliation, flags=re.DOTALL)
    assert len(budget_blocks) == 2
    assert len(pricing_blocks) == 1
    assert len(evidence_blocks) == 1
    assert len(reconciliation_blocks) == 1
    assert tuple(part.strip() for part in budget_blocks[0].replace("\n", " ").split(",")) == (
        EXPECTED_BUDGET_POLICY_KEYS
    )
    assert tuple(part.strip() for part in pricing_blocks[0].replace("\n", " ").split(",")) == EXPECTED_PRICING_KEYS
    assert tuple(part.strip() for part in evidence_blocks[0].replace("\n", " ").split(",")) == (
        EXPECTED_EVIDENCE_RECORD_KEYS
    )
    assert tuple(part.strip() for part in reconciliation_blocks[0].replace("\n", " ").split(",")) == (
        EXPECTED_RECONCILIATION_KEYS
    )

    budget_header, budget_rows = _markdown_tables(budget)[0]
    assert tuple(_strip_code(cell) for cell in budget_header) == (
        "provider mode",
        "exact applicable-record count",
        "exact `R/W` relation",
        "reservation-coverage preflight",
    )
    assert _bare_rows(budget_rows) == EXPECTED_BUDGET_MODE_ROWS

    pricing_header, pricing_rows = _markdown_tables(pricing)[0]
    assert tuple(_strip_code(cell) for cell in pricing_header) == (
        "provider mode",
        "exact applicable-record count",
        "component_rates",
        "policy `R/W` / observed `A`",
    )
    assert _bare_rows(pricing_rows) == EXPECTED_PRICING_MODE_ROWS

    evidence_header, evidence_rows = _markdown_tables(evidence)[0]
    assert tuple(_strip_code(cell) for cell in evidence_header) == (
        "evidence variant",
        "eligible locked source",
        "required receipt",
        "exact payload keyset",
        "terminal / `sent_at` rule",
    )
    assert _bare_rows(evidence_rows) == EXPECTED_EVIDENCE_ROWS

    registry_header, registry_rows = _markdown_tables(reconciliation)[0]
    assert tuple(_strip_code(cell) for cell in registry_header) == (
        "spec id",
        "terminal",
        "evidence variant",
        "source",
        "receipt / usage",
        "amount and parent rule",
        "`sent_at` rule",
        "minimum age / late invoice",
    )
    assert _bare_rows(registry_rows) == EXPECTED_RECONCILIATION_ROWS

    normalized = _normalized(canonical + budget + pricing + evidence + reconciliation)
    for exact_rule in (
        "`currency_code='USD'`",
        "`numeric_precision=38`",
        "`numeric_scale=12`",
        "`rate_scale=18`",
        "`rounding_mode='ROUND_CEILING'`",
        "`minimum_positive_quantum='0.000000000001'`",
        "`fx_policy='forbidden'`",
        "Every record has `terminal=true`.",
        "Price changes append a new immutable mode-specific spec/digest",
        "simulate/scripted usage may remain `reported`",
        "No wildcard mode/price, live settings lookup, floating point, implicit currency conversion, or FX fallback",
        "apply to `live|simulate|scripted` only through an exposure with the exact same PFX provider mode",
        "Confirmed simulate/scripted usage may remain `reported`, but its owner-derived observed/accounted money is exactly zero.",
        "the caller cannot pass either value",
        "an exact replay does not consume another slot",
        "No caller deadline or clock is accepted.",
    ):
        assert exact_rule in normalized


def test_decimal_billing_oracle_rejects_overlap_double_count_and_invalid_usage() -> None:
    usage = {
        "input_tokens": 100,
        "cached_input_tokens": 40,
        "output_tokens": 50,
        "reasoning_output_tokens": 10,
        "total_tokens": 150,
    }
    quantities = _validated_billing_quantities(usage)
    assert quantities == (60, 40, 40, 10)
    assert sum(quantities) == usage["total_tokens"]
    worst_case = _price_v1(
        quantities,
        (
            "0.000001000000000000",
            "0.000000500000000000",
            "0.000002000000000000",
            "0.000003000000000000",
        ),
    )
    assert worst_case == Decimal("0.000190000000")
    assert _price_v1((1, 0, 0, 0), ("0.000000000000000001",) + ("0.000000000000000000",) * 3) == USD_QUANTUM
    assert (
        _price_v1(
            (1, 1, 0, 0),
            ("0.000000000000400000", "0.000000000000400000") + ("0.000000000000000000",) * 2,
        )
        == USD_QUANTUM
    )
    _validate_budget_coverage(Decimal(4) * worst_case, worst_case, 4, "live")
    _validate_budget_coverage(Decimal(0), Decimal(0), 4, "simulate")
    _validate_budget_coverage(Decimal(0), Decimal(0), 4, "scripted")
    with pytest.raises(ValueError):
        _validate_budget_coverage(Decimal(4) * worst_case - USD_QUANTUM, worst_case, 4, "live")
    with pytest.raises(ValueError):
        _validate_budget_coverage(USD_QUANTUM, Decimal(0), 4, "simulate")

    invalid_usage = (
        {**usage, "cached_input_tokens": 101},
        {**usage, "reasoning_output_tokens": 51},
        {**usage, "total_tokens": 149},
        {**usage, "input_tokens": True},
        {**usage, "input_tokens": MAX_BIGINT + 1},
        {key: value for key, value in usage.items() if key != "cached_input_tokens"},
        {**usage, "extra_tokens": 0},
    )
    for candidate in invalid_usage:
        with pytest.raises(ValueError):
            _validated_billing_quantities(candidate)
    for rates in (
        ("1e-18",) + ("0.000000000000000000",) * 3,
        ("+0.000000000000000001",) + ("0.000000000000000000",) * 3,
        ("0.00000000000000001",) + ("0.000000000000000000",) * 3,
        ("100000000000000000000.000000000000000000",) + ("0.000000000000000000",) * 3,
    ):
        with pytest.raises(ValueError):
            _price_v1((1, 0, 0, 0), rates)
    with pytest.raises(ValueError):
        _price_v1((MAX_BIGINT + 1, 0, 0, 0), ("0.000000000000000001",) * 4)


def test_usage_variant_and_canonical_digest_golden_vectors_are_exact() -> None:
    valid_usage: dict[str, object] = {
        "input_tokens": 100,
        "cached_input_tokens": 40,
        "output_tokens": 50,
        "reasoning_output_tokens": 10,
        "total_tokens": 150,
    }
    assert _usage_evidence_variant(valid_usage) == "response_reported_v1"
    assert _usage_evidence_variant({**valid_usage, "cached_input_tokens": 101}) == "response_usage_invalid_v1"
    assert _usage_evidence_variant({**valid_usage, "total_tokens": 149}) == "response_usage_invalid_v1"
    assert _usage_evidence_variant({**valid_usage, "input_tokens": True}) == "response_usage_unavailable_v1"
    assert (
        _usage_evidence_variant({key: value for key, value in valid_usage.items() if key != "cached_input_tokens"})
        == "response_usage_unavailable_v1"
    )
    overflow_usage: dict[str, object] = {
        "input_tokens": MAX_BIGINT,
        "cached_input_tokens": 0,
        "output_tokens": 0,
        "reasoning_output_tokens": 0,
        "total_tokens": MAX_BIGINT,
    }
    overflow_rates = ("99999999999999999999.999999999999999999",) + ("0.000000000000000000",) * 3
    assert _usage_evidence_variant(overflow_usage, overflow_rates) == "response_usage_invalid_v1"

    canonical_record = {"amount": "0.000000000001", "schema_version": "v1", "terminal": True}
    assert _canonical_json(canonical_record) == b'{"amount":"0.000000000001","schema_version":"v1","terminal":true}'
    expected_digests = {
        "cost-budget-policy-v1": "3f1ec1b209ec25d248d795540ef160fe060249a31edeab5d63773be648170914",
        "cost-pricing-spec-v1": "4d765a0ee095012c8d696d3ff19c3c2b177ad8bf0cebbc02997ae77b40bd23a3",
        "cost-reconciliation-spec-v1": "e20ddaef7848f0043b4756c89cc92d223cc115fb16fce64399b3da0ae0543f67",
        "cost-terminal-evidence-record-v1": "a1692c852bdc332032ae97d33234960fb50e07cfb9dac7bd0cb178c7d0d0d7c6",
    }
    assert {tag: _domain_digest(tag, canonical_record) for tag in expected_digests} == expected_digests
    pfx = ("ns", "live", "ws", "a" * 64, 7)
    assert _cost_evidence_id(pfx, "exp-1", 3, "zero_wire_abort_v1") == (
        "1b7d9969bb2ffed23d81ff422c3cb1f47a22e48582e98726e89410c385d0b043"
    )


def test_nullable_terminal_checks_fail_closed_under_postgres_three_valued_logic() -> None:
    guarded_checks = {
        "dispatch_exposures_base_intent_tuple_ck",
        "dispatch_exposures_predecessor_tuple_ck",
        "dispatch_exposures_grant_tuple_ck",
        "dispatch_exposures_cost_evidence_shape_ck",
        "dispatch_exposures_terminal_evidence_shape_ck",
        "dispatch_exposures_state_timestamp_shape_ck",
    }
    check_map = dict(EXPECTED_CHILD_CHECKS)
    assert all(check_map[name].endswith(") IS TRUE") for name in guarded_checks)

    legacy_no_call = _sql_and(True, None, None, True, True, True, True, None, True)
    legacy_response_uncertain = _sql_and(True, True, None, True, True, True, None)
    legacy_partial_tuple = _sql_or(False, _sql_and(None, True))
    assert (legacy_no_call, legacy_response_uncertain, legacy_partial_tuple) == (None, None, None)
    assert all(
        _postgres_check_accepts(value) for value in (legacy_no_call, legacy_response_uncertain, legacy_partial_tuple)
    )
    assert all(
        not _postgres_check_accepts(value is True)
        for value in (legacy_no_call, legacy_response_uncertain, legacy_partial_tuple)
    )


def test_zero_wire_observation_is_bound_to_one_authorized_physical_call() -> None:
    exposure: dict[str, object] = {
        "runtime_namespace": "prod",
        "provider_mode": "live",
        "workspace_id": "ws-1",
        "scope_digest": "a" * 64,
        "coordination_plan_review_id": 7,
        "dispatch_exposure_id": "exp-1",
        "source_state_version": 3,
        "canonical_request_digest": "b" * 64,
        "transport_adapter_id": "adapter-1",
        "transport_adapter_revision": "rev-1",
    }
    observation = {
        **exposure,
        "failure_phase": "connect",
        "request_bytes_written": 0,
        "response_bytes_read": 0,
        "provider_call_id_state": "not_observed",
    }
    _validate_zero_wire_observation(exposure, observation)
    for field, replacement in (
        ("dispatch_exposure_id", "exp-2"),
        ("source_state_version", 4),
        ("canonical_request_digest", "c" * 64),
        ("transport_adapter_revision", "rev-2"),
        ("request_bytes_written", 1),
        ("provider_call_id_state", "observed"),
        ("caller_claimed_no_wire", True),
    ):
        with pytest.raises(ValueError):
            _validate_zero_wire_observation(exposure, {**observation, field: replacement})


def test_cost_owner_evidence_and_source_timestamp_semantics_survive_mutations() -> None:
    document = DECISION_DOC_PATH.read_text(encoding="utf-8")
    _assert_cost_semantics_contract(document)
    mutations = (
        ("input_tokens - cached_input_tokens", "input_tokens"),
        ("output_tokens - reasoning_output_tokens", "output_tokens"),
        ("total_tokens = input_tokens + output_tokens", "total_tokens = output_tokens"),
        ("cost-terminal-evidence-id-v1", "cost-terminal-evidence-id-v2"),
        ("R >= max_physical_calls * W", "R > 0"),
        ("Callers may supply neither `R`, `W`, `A`", "Callers may supply `R`, `W`, `A`"),
        ("cost_terminal_evidence_id", "caller_proof_id"),
        ("request_bytes_written=0", "request_bytes_written>=0"),
        (
            "proves wire send for both `confirmed` and response-backed `uncertain`",
            "proves wire send for `confirmed` only",
        ),
        ("failure or timeout/crash leaves `sent_at` NULL", "failure or timeout/crash sets `sent_at`"),
    )
    for original, replacement in mutations:
        assert original in document
        with pytest.raises(AssertionError):
            _assert_cost_semantics_contract(document.replace(original, replacement))


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
        "source/evidence-dependent closed mapping only: `prepared -> no_call(prepared_no_dispatch)`; "
        "`dispatching -> confirmed(response) / uncertain(response, failure, or timeout) / no_call(zero_wire)`; "
        "`sent -> confirmed(response) / uncertain(response, failure, or timeout)`; owner derives `A` or conservative `W/0`, "
        "installs the canonical evidence record, and sets `parent_settlement_state=pending`; parent unchanged"
    )
    normalized_lifecycle = _normalized(lifecycle)
    for exact_rule in (
        "Every method requires the full prefix, exact immutable pins, `expected_state_version`, and an enumerated expected state.",
        "Exact replay returns the existing exact result with zero writes",
        "prepared -> dispatching | no_call",
        "dispatching -> sent | confirmed | uncertain | no_call",
        "sent -> confirmed | uncertain",
        "confirmed | uncertain | no_call -> terminal forever",
        "`prepared -> no_call` is admitted only by the owner-built locked `prepared_no_dispatch_v1` evidence and has no prior hold",
        "`dispatching -> no_call` requires a complete owner-built `zero_wire_abort_v1` record",
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
        "An authenticated response receipt proves wire send for both `confirmed` and response-backed `uncertain`.",
        "the same terminal CAS fills the previously-null `sent_at` from PostgreSQL `transaction_timestamp()`",
        "A direct `dispatching -> uncertain` backed only by attempt failure or timeout/crash leaves `sent_at` NULL",
        "`dispatching -> no_call(zero_wire)` also leaves it NULL",
        "A `sent`-origin response transition preserves the already committed `sent_at`.",
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
        "owner-build the embedded `CostTerminalEvidenceRecord`",
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
    normalized_document = _normalized(document.replace("\n> ", " "))

    for review_marker in (
        "4945ab7ae17764af0a0ca705ffd4257002c2f279",
        "ADVISORY NO-GO",
        "P0/P1/P2/P3 = 0/2/1/0",
        "remains review-pending until a fresh pinned non-author verdict",
        "Neither the advisory result nor author validation is a formal GO.",
    ):
        assert review_marker in normalized_document

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
        "their deferral blocks a cost-ledger migration too",
        "no partial cost-only DDL",
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
