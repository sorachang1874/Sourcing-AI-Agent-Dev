from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest

from sourcing_agent.acquisition_start_command_acceptance import (
    ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "contracts"
    / "filter_projection_lineage_terminal_owner_decision_v1.json"
)
FOUNDATION_PATH = (
    REPO_ROOT / "docs" / "modules" / "serving-product" / "contracts" / "filter_projection_foundation_boundary_v1.json"
)
DECISION_PATH = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "decisions"
    / "TRACK_D_D1N_S1F0C_LINEAGE_TERMINAL_OWNER_DECISION.md"
)

TOP_LEVEL_KEYS = {
    "schema_version",
    "decision_id",
    "status",
    "scope",
    "obligation_basis",
    "owners",
    "operation_native_source_run",
    "start_authority_join",
    "propagation_contract",
    "planning_execution_recompile",
    "terminal_lineage_record",
    "candidate_set_commitment",
    "runtime_namespace",
    "freshness_readiness",
    "projection_terminal_authority",
    "result_slot_contract",
    "mechanism_invariant_matrix",
    "implementation_batches",
    "release_boundary",
    "evidence",
}
INVARIANT_COLUMNS = {
    "mechanism",
    "single_writer",
    "tenant_key",
    "generation_fence",
    "lifecycle",
    "late_partial",
    "cost",
    "physical_identity",
    "provenance",
    "consistency",
    "runtime_isolation",
}


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    assert type(value) is dict
    return value


def _exact_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    assert type(value) is dict, label
    record = dict(value)
    assert set(record) == expected, label
    return record


def _validate_closed_manifest(value: object) -> dict[str, Any]:
    manifest = _exact_keys(value, TOP_LEVEL_KEYS, "root")
    _exact_keys(
        manifest["scope"],
        {
            "batch",
            "decision_only",
            "ddl_delta",
            "runtime_write_delta",
            "provider_invocation_delta",
            "model_invocation_delta",
            "served_population_delta",
        },
        "scope",
    )
    _exact_keys(
        manifest["obligation_basis"],
        {
            "track",
            "d1_ob_id_set",
            "applicable_plan_section_6_items",
            "already_recorded_bookkeeping",
            "not_closed_by_this_batch",
            "borrowed_ob_ids",
        },
        "obligation_basis",
    )
    _exact_keys(
        manifest["owners"],
        {
            "physical_projection_writer",
            "start_authority_owner",
            "planning_manifest_owner",
            "execution_manifest_owner",
            "cohort_terminal_writer",
            "projection_terminal_writer",
            "projection_field_validator",
            "runtime_namespace_ref_owner",
            "filter_query_owner",
            "result_acceptance_writer",
            "product_owner_state",
        },
        "owners",
    )
    _exact_keys(
        manifest["operation_native_source_run"],
        {
            "canonical_field",
            "canonical_value",
            "required_join",
            "legacy_job_shell",
            "job_id_alias",
            "source_run_alias",
        },
        "operation_native_source_run",
    )
    start = _exact_keys(
        manifest["start_authority_join"],
        {
            "owner_ref_schema_version",
            "owner_ref_fields",
            "root_payload_schema_version",
            "approval_event",
            "command_acceptance_winner",
            "exact_equalities",
            "uniqueness_predicates",
            "caller_supplied_join",
        },
        "start_authority_join",
    )
    _exact_keys(
        start["approval_event"],
        {"event_type", "sequence_number", "receipt_schema_version"},
        "start_authority_join.approval_event",
    )
    _exact_keys(
        start["command_acceptance_winner"],
        {"event_type", "sequence_number", "result_link_policy"},
        "start_authority_join.command_acceptance_winner",
    )
    propagation = _exact_keys(
        manifest["propagation_contract"],
        {
            "carrier_schema_version",
            "source",
            "ordered_stages",
            "revalidation",
            "current_loss_point",
            "mutable_latest_lookup",
            "json_aliases",
        },
        "propagation_contract",
    )
    for index, stage in enumerate(propagation["ordered_stages"]):
        _exact_keys(stage, {"stage", "carrier_location", "owner"}, f"ordered_stages[{index}]")
    recompile = _exact_keys(
        manifest["planning_execution_recompile"],
        {
            "planning_schema_version",
            "planning_validator",
            "execution_schema_version",
            "execution_recompiler_owner",
            "required_entrypoint",
            "permitted_differences",
            "exact_equal_fields",
            "execution_capability",
            "result_validation",
        },
        "planning_execution_recompile",
    )
    _exact_keys(
        recompile["execution_capability"],
        {"issuer", "caller_authored", "must_bind", "may_expand_planning_budget"},
        "execution_capability",
    )
    _exact_keys(
        recompile["result_validation"],
        {
            "schema_version",
            "must_equal_execution",
            "partial_lane_result",
            "missing_required_lane_count",
            "exact_result_digest_required",
        },
        "result_validation",
    )
    terminal = _exact_keys(
        manifest["terminal_lineage_record"],
        {
            "schema_version",
            "physical_relation",
            "implementation_state",
            "identity_fields",
            "record_fields",
            "reference_fields",
            "cas",
            "artifact_paths",
            "legacy_job_id",
            "migration",
        },
        "terminal_lineage_record",
    )
    _exact_keys(
        terminal["reference_fields"],
        {
            "planning_manifest_ref",
            "execution_manifest_ref",
            "search_seed_snapshot_ref",
            "result_view_ref",
            "cohort_execution_result_ref",
            "candidate_documents_commit_ref",
        },
        "terminal_lineage_record.reference_fields",
    )
    _exact_keys(
        terminal["cas"],
        {
            "operation",
            "lock_order",
            "stored_predicates",
            "outcomes",
            "update_after_insert",
            "terminal_winner_id",
        },
        "terminal_lineage_record.cas",
    )
    _exact_keys(
        terminal["migration"],
        {"this_batch", "historical_backfill", "legacy_terminal_adoption"},
        "terminal_lineage_record.migration",
    )
    _exact_keys(
        manifest["candidate_set_commitment"],
        {
            "schema_version",
            "maximum_source_candidate_count",
            "minimum_source_candidate_count",
            "fields",
            "exclusion_policy",
            "invariants",
            "canonical_member_fields",
            "canonical_order",
            "visibility_state",
        },
        "candidate_set_commitment",
    )
    _exact_keys(
        manifest["runtime_namespace"],
        {
            "private_owner_value",
            "agent_visible_schema_version",
            "agent_visible_fields",
            "registry_owner",
            "private_binding_fields",
            "path_or_url_in_agent_result",
            "caller_minted_ref",
            "cross_workspace_or_mode_reuse",
        },
        "runtime_namespace",
    )
    state = _exact_keys(
        manifest["freshness_readiness"],
        {"freshness", "readiness", "reader_write_or_repair", "latest_file_fallback"},
        "freshness_readiness",
    )
    _exact_keys(
        state["freshness"],
        {"physical_owner", "model_safe_owner", "allowed_statuses", "fresh_rule"},
        "freshness",
    )
    _exact_keys(
        state["readiness"],
        {"physical_owner", "model_safe_owner", "allowed_statuses", "ready_rule"},
        "readiness",
    )
    _exact_keys(
        manifest["projection_terminal_authority"],
        {
            "schema_version",
            "implementation_state",
            "physical_writer",
            "field_validator",
            "authority",
            "audit_only_predecessor",
            "source_run_id",
            "target_fields",
            "terminal_fields",
            "publication_uow",
            "new_lock_order",
            "cohort_result_as_terminal_authority",
        },
        "projection_terminal_authority",
    )
    result_slot = _exact_keys(
        manifest["result_slot_contract"],
        {
            "result_link_policy",
            "effect_class",
            "common_links",
            "success",
            "deferred",
            "masked_absence",
            "prepare_failure",
            "acceptance_writes",
        },
        "result_slot_contract",
    )
    _exact_keys(result_slot["common_links"], {"required", "empty", "zero"}, "result_slot.common_links")
    _exact_keys(
        result_slot["success"],
        {
            "owner_target_kind",
            "owner_target_id",
            "owner_target_revision",
            "owner_target_generation",
            "owner_target_revision_token",
            "terminal_winner_id",
            "owner_result_ref_schema_version",
            "owner_result_ref_fields",
            "owner_result_digest",
        },
        "result_slot.success",
    )
    _exact_keys(
        result_slot["deferred"],
        {
            "allowed_outcomes",
            "owner_target_kind",
            "owner_target_id",
            "owner_target_revision_token",
            "terminal_winner_id",
            "owner_result_ref_schema_version",
            "owner_result_ref_fields",
        },
        "result_slot.deferred",
    )
    _exact_keys(
        result_slot["masked_absence"],
        {
            "outcome",
            "owner_target_kind",
            "owner_target_id",
            "owner_target_generation",
            "terminal_winner_id",
            "owner_result_ref_schema_version",
            "owner_result_ref_fields",
        },
        "result_slot.masked_absence",
    )
    _exact_keys(
        result_slot["prepare_failure"],
        {
            "conditions",
            "slot_status",
            "result_attempt_writes",
            "result_slot_writes",
            "result_journal_writes",
            "domain_projection_provider_model_writes",
        },
        "result_slot.prepare_failure",
    )
    for index, row in enumerate(manifest["mechanism_invariant_matrix"]):
        _exact_keys(row, INVARIANT_COLUMNS, f"mechanism_invariant_matrix[{index}]")
    for index, batch in enumerate(manifest["implementation_batches"]):
        _exact_keys(
            batch,
            {"batch", "depends_on", "write_owner", "outcome", "promotion_requires_review"},
            f"implementation_batches[{index}]",
        )
    _exact_keys(
        manifest["release_boundary"],
        {
            "served_population",
            "provider_invocations",
            "model_invocations",
            "live_invocations",
            "migration_delta",
            "backfill",
            "agent_reader",
            "local_live",
            "hosted",
            "open_residuals",
            "author_evidence_only",
        },
        "release_boundary",
    )
    _exact_keys(
        manifest["evidence"],
        {"foundation_manifest", "scout_handoffs", "code_symbols", "current_missing_join"},
        "evidence",
    )
    return manifest


def test_s1f0c_manifest_is_closed_and_unknown_keys_fail() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    assert manifest["schema_version"] == "filter_projection_lineage_terminal_owner_decision.v1"
    assert manifest["status"] == "decision_locked_not_implemented"

    unknown_top = copy.deepcopy(manifest)
    unknown_top["future_alias"] = True
    with pytest.raises(AssertionError, match="root"):
        _validate_closed_manifest(unknown_top)

    unknown_nested = copy.deepcopy(manifest)
    unknown_nested["mechanism_invariant_matrix"][0]["implicit_owner"] = "forbidden"
    with pytest.raises(AssertionError, match="mechanism_invariant_matrix"):
        _validate_closed_manifest(unknown_nested)


def test_s1f0c_source_run_and_start_join_are_exact_current_contracts() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    assert manifest["obligation_basis"] == {
        "track": "D1",
        "d1_ob_id_set": [],
        "applicable_plan_section_6_items": [3],
        "already_recorded_bookkeeping": [
            "RESIDUAL_LEDGER.R-029",
            "NEXT_TODO.Track_D.D1",
        ],
        "not_closed_by_this_batch": ["Plan section 6 item 4", "R-019", "R-029"],
        "borrowed_ob_ids": "forbidden",
    }
    source = manifest["operation_native_source_run"]
    assert source == {
        "canonical_field": "source_run_id",
        "canonical_value": "acquisition_runs.workflow_run_id",
        "required_join": [
            "acquisition_runs.acquisition_run_id",
            "acquisition_runs.operation_run_id",
            "acquisition_runs.workflow_run_id",
            "operation_runs.operation_run_id",
            "operation_runs.action_id",
            "workflow_commands.workflow_run_id",
            "workflow_commands.operation_id",
        ],
        "legacy_job_shell": "forbidden",
        "job_id_alias": "forbidden",
        "source_run_alias": "forbidden",
    }
    start = manifest["start_authority_join"]
    assert tuple(start["owner_ref_fields"]) == ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS
    assert start["approval_event"] == {
        "event_type": "ActionApproved",
        "sequence_number": 2,
        "receipt_schema_version": "acquisition_confirmation_receipt.v1",
    }
    assert start["command_acceptance_winner"] == {
        "event_type": "OperationCommandPlanned",
        "sequence_number": 1,
        "result_link_policy": "workflow_command_acceptance_v1",
    }


def test_s1f0c_propagation_names_the_real_loss_point_and_exact_stages() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    propagation = manifest["propagation_contract"]
    assert [stage["stage"] for stage in propagation["ordered_stages"]] == [
        "root_command",
        "resolved_intent",
        "acquisition_plan",
        "review_session",
        "plan_commit",
        "acquisition_run",
    ]
    assert propagation["mutable_latest_lookup"] == "forbidden"
    assert propagation["json_aliases"] == "forbidden"

    owner_source = (REPO_ROOT / "src" / "sourcing_agent" / "acquisition_command_owner.py").read_text(encoding="utf-8")
    for symbol in (
        "def _start_v2_root_workflow_payload",
        "def _execute_acquisition_intent_resolve_command_payload",
        "def _build_acquisition_plan_from_resolved_intent",
    ):
        assert symbol in owner_source
    assert '"source_workflow_payload": workflow_payload' in owner_source
    plan_builder = owner_source.split("def _build_acquisition_plan_from_resolved_intent", maxsplit=1)[1].split(
        "def _sync_acquisition_plan_ready_from_workflow_command", maxsplit=1
    )[0]
    assert "source_workflow_payload" not in plan_builder


def test_s1f0c_recompile_and_candidate_set_are_total_not_subset_claims() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    recompile = manifest["planning_execution_recompile"]
    assert recompile["planning_schema_version"] == "cohort_provider_manifest.v2"
    assert recompile["planning_validator"] == "CohortProviderCompiler.validate_planning_manifest"
    assert recompile["execution_schema_version"] == "cohort_provider_execution_manifest.v2"
    assert recompile["permitted_differences"] == [
        "schema_version",
        "execution_ready",
        "execution_blocker",
        "compiler_inputs.execution_capability",
        "manifest_digest",
    ]
    assert {
        "lanes",
        "physical_query_digest",
        "company_target",
        "budget_ceiling",
        "schema_pins",
    }.issubset(recompile["exact_equal_fields"])
    assert recompile["execution_capability"]["caller_authored"] is False
    assert recompile["execution_capability"]["may_expand_planning_budget"] is False
    assert recompile["result_validation"]["missing_required_lane_count"] == 0

    commitment = manifest["candidate_set_commitment"]
    assert commitment["minimum_source_candidate_count"] == 1
    assert commitment["maximum_source_candidate_count"] == 1000
    assert commitment["exclusion_policy"] == "no_exclusions_v1"
    assert "excluded_candidate_count == 0" in commitment["invariants"]
    assert any("duplicates extras omissions" in rule for rule in commitment["invariants"])


def test_s1f0c_terminal_is_commit_once_path_free_and_not_a_legacy_job_bridge() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    terminal = manifest["terminal_lineage_record"]
    assert terminal["implementation_state"] == "absent_until_S1f0c2"
    assert terminal["cas"]["operation"] == "insert_once_or_exact_replay"
    assert terminal["cas"]["outcomes"] == ["inserted", "exact_replay", "conflict_zero_write"]
    assert terminal["cas"]["update_after_insert"] == "forbidden"
    assert terminal["artifact_paths"] == "forbidden"
    assert terminal["legacy_job_id"] == "forbidden"
    assert terminal["migration"] == {
        "this_batch": "none",
        "historical_backfill": "forbidden",
        "legacy_terminal_adoption": "forbidden",
    }
    assert not any("path" in field for fields in terminal["reference_fields"].values() for field in fields)

    runtime = manifest["runtime_namespace"]
    assert runtime["agent_visible_fields"] == [
        "schema_version",
        "runtime_namespace_ref_id",
        "runtime_namespace_binding_digest",
    ]
    assert runtime["path_or_url_in_agent_result"] == "forbidden"
    assert runtime["caller_minted_ref"] == "forbidden"


def test_s1f0c_projection_is_terminal_authority_and_result_refs_are_complete() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    projection = manifest["projection_terminal_authority"]
    assert projection["physical_writer"] == manifest["owners"]["physical_projection_writer"]
    assert projection["audit_only_predecessor"] == "cohort_execution_result.v1"
    assert projection["cohort_result_as_terminal_authority"] == "forbidden"
    assert projection["source_run_id"] == "acquisition_runs.workflow_run_id"
    assert projection["new_lock_order"] == "forbidden"

    slots = manifest["result_slot_contract"]
    assert slots["result_link_policy"] == "no_command_v1"
    assert slots["effect_class"] == "read_only"
    assert slots["common_links"] == {
        "required": ["action_id", "operation_run_id"],
        "empty": ["workflow_command_id", "activity_run_id", "activity_attempt_id"],
        "zero": ["command_attempt", "command_generation", "control_epoch"],
    }
    success = slots["success"]
    assert success["owner_target_id"] == "projection_id"
    assert success["owner_target_revision"] == 0
    assert success["owner_target_generation"] == 0
    assert success["owner_target_revision_token"] == "membership_revision"
    assert success["terminal_winner_id"] == "publication_terminal_id"
    assert {
        "terminal_lineage_id",
        "terminal_lineage_digest",
        "candidate_set_digest",
        "runtime_namespace_ref",
        "freshness",
        "readiness",
        "result_occurrence_ref",
        "serialized_result_digest",
    }.issubset(success["owner_result_ref_fields"])
    assert slots["masked_absence"]["owner_target_id"] == "result_slot_id"
    assert slots["masked_absence"]["owner_target_generation"] == "slot_generation"
    assert slots["prepare_failure"]["slot_status"] == "pending"
    for field in (
        "result_attempt_writes",
        "result_slot_writes",
        "result_journal_writes",
        "domain_projection_provider_model_writes",
    ):
        assert slots["prepare_failure"][field] == 0


def test_s1f0c_dependency_graph_invariants_and_release_stay_fail_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    foundation = _load(FOUNDATION_PATH)
    foundation_batches = {row["batch_id"]: row for row in foundation["dependency_batches"]}
    assert foundation_batches["S1f0c0-lineage-decision"]["depends_on"] == []
    assert foundation_batches["S1f0c1-start-propagation"]["depends_on"] == ["S1f0c0-lineage-decision"]

    batches = {row["batch"]: row for row in manifest["implementation_batches"]}
    assert batches["S1f0c2-cohort-terminal"]["depends_on"] == ["S1f0c1-start-propagation"]
    assert set(batches["S1f0d-owner-v2"]["depends_on"]) == {
        "S1f0b-foundation",
        "S1f0c2-cohort-terminal",
    }
    assert batches["S1f1-reader-result"]["depends_on"] == ["S1f0d-owner-v2"]
    assert all(row["promotion_requires_review"] is True for row in batches.values())

    rows = manifest["mechanism_invariant_matrix"]
    assert len(rows) == 7
    assert {row["mechanism"] for row in rows} == {
        "start_authority_carrier",
        "planning_execution_recompile",
        "terminal_lineage_record",
        "runtime_namespace_ref",
        "candidate_set_commitment",
        "projection_publication_terminal",
        "result_slot_variants",
    }
    assert all(set(row) == INVARIANT_COLUMNS for row in rows)

    release = manifest["release_boundary"]
    for field in (
        "served_population",
        "provider_invocations",
        "model_invocations",
        "live_invocations",
        "migration_delta",
    ):
        assert release[field] == 0
    assert release["backfill"] == "forbidden"
    assert release["agent_reader"] == "blocked"
    assert release["open_residuals"] == ["R-019", "R-029"]
    assert release["author_evidence_only"] is True


def test_s1f0c_evidence_routes_to_real_current_code_symbols() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    expected_symbols = {
        "acquisition_start_command_acceptance.py": ("ACQUISITION_START_COMMAND_ACCEPTANCE_OWNER_REF_FIELDS",),
        "acquisition_start_v2.py": ("class AcquisitionStartV2RootCommandPayload",),
        "acquisition_command_owner.py": (
            "def _start_v2_root_workflow_payload",
            "def _build_acquisition_plan_from_resolved_intent",
        ),
        "cohort_provider_compiler.py": (
            "def compile_planning_manifest",
            "def validate_planning_manifest",
        ),
        "acquisition.py": ("def _build_cohort_execution_result",),
        "search_seed_registry.py": ("def cohort_publication_is_committed",),
        "repositories/workflow_runtime.py": (
            "class WorkflowRuntimeRepository",
            "def upsert_acquisition_run",
        ),
        "serving_projection_writer.py": (
            "class ServingProjectionWriter",
            "def publish_run_scope_projection",
        ),
        "agent_projection_query.py": ("def execute_filter_projection_v2",),
        "agent_tool_result_slot.py": ("class AgentToolTerminalResult",),
    }
    source_root = REPO_ROOT / "src" / "sourcing_agent"
    for relative_path, symbols in expected_symbols.items():
        source = (source_root / relative_path).read_text(encoding="utf-8")
        for symbol in symbols:
            assert symbol in source, f"{relative_path}:{symbol}"
    assert len(manifest["evidence"]["code_symbols"]) == 12


def test_s1f0c_markdown_routes_every_decision_without_claiming_implementation() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    decision = DECISION_PATH.read_text(encoding="utf-8")
    assert decision.startswith("# Track D D1n S1f0c0")
    assert "> Status: decision-locked author candidate." in decision
    for text in (
        "acquisition_runs.workflow_run_id",
        "AcquisitionCommandOwner._build_acquisition_plan_from_resolved_intent",
        "cohort_provider_execution_manifest.v2",
        "cohort_projection_terminal_lineage.v1",
        "insert_once_or_exact_replay",
        "cohort_candidate_set_commitment.v1",
        "runtime_namespace_ref.v1",
        "filter_projection_publication_terminal.v1",
        "filter_projection_masked_error_v1",
        "S1F-LIN-01",
        "OB-ID set (`∅`)",
        "served=0",
        "R-019/R-029",
    ):
        assert text in decision
    assert "implements no runtime writer" in decision
    assert "S1f0c0 creates no DDL" in decision

    for handoff in manifest["evidence"]["scout_handoffs"]:
        assert (REPO_ROOT / handoff).is_file()
    assert manifest["scope"] == {
        "batch": "S1f0c0-lineage-decision",
        "decision_only": True,
        "ddl_delta": 0,
        "runtime_write_delta": 0,
        "provider_invocation_delta": 0,
        "model_invocation_delta": 0,
        "served_population_delta": 0,
    }
