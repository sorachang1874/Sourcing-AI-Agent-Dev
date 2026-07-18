from __future__ import annotations

import copy
import hashlib
import json
import os
import re
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "contracts"
    / "filter_projection_lineage_fixed_forward_decision_v1.json"
)
DECISION_PATH = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "decisions"
    / "TRACK_D_D1N_S1F0C_LINEAGE_FIXED_FORWARD_DECISION.md"
)
MIGRATIONS_DIR = REPO_ROOT / "src" / "sourcing_agent" / "migrations"

TOP_LEVEL_KEYS = {
    "schema_version",
    "decision_id",
    "status",
    "scope",
    "obligation_basis",
    "owners",
    "schema_registry_lock",
    "contract_digest_equation",
    "contract_digests",
    "pg_aggregate_surfaces",
    "start_authority_contracts",
    "runtime_namespace_capability_envelope",
    "execution_record_contracts",
    "physical_relations",
    "product_terminal_records",
    "result_v3_slot_contract",
    "owner_graph",
    "lock_order",
    "count_digest_equations",
    "retention_replay_migration_gates",
    "hostile_mutation_oracles",
    "mechanism_invariant_matrix",
    "implementation_batches",
    "implementation_dag",
    "transition_states",
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

ADOPTED_LITERALS = {
    "acquisition_start_authority_carrier.v1",
    "acquisition_execution_authority.v1",
    "agent_runtime_namespace_ref.v1",
    "cohort_execution_capability.v2",
    "cohort_execution_envelope.v1",
    "cohort_execution_lane_result.v2",
    "cohort_candidate_member.v1",
    "cohort_candidate_set.v1",
    "cohort_execution_result.v2",
    "cohort_execution_commit.v1",
    "filter_projection_product_terminal.v1",
    "filter_projection_freshness_ref.v1",
    "filter_projection_readiness_ref.v1",
    "filter_projection_success_owner_ref.v1",
    "filter_projection_stale_owner_ref.v1",
    "filter_projection_not_ready_owner_ref.v1",
    "filter_projection_masked_absence_owner_ref.v1",
    "filter_projection_result_v3",
    "filter_projection_result_serializer_v3",
    "filter_projection_tool_v3",
    "acquisition.cohort.execute",
}
DIGESTED_LITERALS = {
    "acquisition_start_authority_carrier.v1",
    "acquisition_execution_authority.v1",
    "agent_runtime_namespace_ref.v1",
    "cohort_execution_capability.v2",
    "cohort_execution_envelope.v1",
    "cohort_execution_lane_result.v2",
    "cohort_candidate_member.v1",
    "cohort_candidate_set.v1",
    "cohort_execution_result.v2",
    "cohort_execution_commit.v1",
    "filter_projection_product_terminal.v1",
    "filter_projection_freshness_ref.v1",
    "filter_projection_readiness_ref.v1",
    "filter_projection_product_ref.v1",
    "filter_projection_success_owner_ref.v1",
    "filter_projection_stale_owner_ref.v1",
    "filter_projection_not_ready_owner_ref.v1",
    "filter_projection_masked_absence_owner_ref.v1",
}
REGISTRY_ONLY_LITERALS = {
    "filter_projection_result_v3",
    "filter_projection_result_serializer_v3",
    "filter_projection_tool_v3",
    "acquisition.cohort.execute",
}
NEW_LITERALS = ADOPTED_LITERALS | {"filter_projection_product_ref.v1", "filter_projection_terminal_winner.v1"}
NEW_RELATION_NAMES = {
    "agent_runtime_namespace_refs",
    "cohort_execution_attempts",
    "cohort_execution_lane_results",
    "cohort_candidate_set_members",
    "cohort_execution_commits",
    "filter_projection_product_terminals",
}
RETAINED_LITERALS = {
    "acquisition_root_command_payload.v2",
    "cohort_execution_capability.v1",
    "cohort_provider_manifest.v1",
    "cohort_provider_manifest.v2",
    "cohort_execution_result.v1",
    "filter_projection_result_v2",
    "filter_projection_tool_v2",
}
REJECTED_LITERALS = {
    "acquisition_start_lineage_ref.v1",
    "cohort_provider_execution_manifest.v2",
    "filter_projection_publication_terminal.v1",
}

SCAN_EXCLUDE_DIRS = {
    ".git",
    ".venv",
    ".venv-tests",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".cache",
    ".coord",
    ".claude",
    ".playwright-cli",
    ".vendor",
    "__pycache__",
    "node_modules",
}
# Untracked local artifact prefixes (present only in a dirty integration tree).
# The collision scan covers tracked source; these prefixes are never tracked
# and can hold gigabytes of runtime payloads that would hang the walk.
SCAN_EXCLUDE_PATH_PREFIXES = {
    "output",
    "logs",
    "object_sync",
    "hot_cache_company_assets",
    "company_assets",
    "job_locks",
    "jobs",
    "runtime_metrics",
    "local_asset_packages",
    "configs/scripted/samples",
    "frontend-demo/dist",
    "frontend-demo/public/tml",
}
SCAN_SKIP_FILES = {MANIFEST_PATH, DECISION_PATH, Path(__file__).resolve()}


def _scan_path_excluded(rel: str) -> bool:
    if rel in SCAN_EXCLUDE_PATH_PREFIXES:
        return True
    for prefix in SCAN_EXCLUDE_PATH_PREFIXES:
        if rel.startswith(prefix + "/"):
            return True
    if rel == "runtime" or rel.startswith("runtime/"):
        return not (rel == "runtime/reviews" or rel.startswith("runtime/reviews/"))
    return False


def _load(path: Path) -> dict[str, Any]:
    def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            assert key not in result, f"duplicate key {key!r} in {path.name}"
            result[key] = value
        return result

    value = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_no_duplicate_keys)
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
    basis = _exact_keys(
        manifest["obligation_basis"],
        {
            "track",
            "design_source",
            "design_source_pinned_base",
            "canonization_base",
            "precedent_commit",
            "precedent_manifest",
            "controlling_review",
            "wave0_inputs",
            "not_closed_by_this_batch",
            "review_required_before",
        },
        "obligation_basis",
    )
    _exact_keys(
        basis["controlling_review"],
        {"artifact", "verdict", "new_findings", "residuals"},
        "obligation_basis.controlling_review",
    )
    _exact_keys(
        manifest["owners"],
        {
            "start_command_owner",
            "start_carrier_owner",
            "execution_authority_owner",
            "namespace_registry_owner",
            "compiler_derivation_owner",
            "cohort_execution_writer",
            "provider_execution_command_owner",
            "physical_projection_writer",
            "product_terminal_writer",
            "freshness_readiness_owner",
            "owner_ref_owner",
            "serializer_owner",
            "result_v3_route_owner",
            "tool_registry_owner",
            "result_registry",
            "result_acceptance_writer",
            "product_owner_state",
        },
        "owners",
    )
    for index, row in enumerate(manifest["schema_registry_lock"]):
        _exact_keys(
            row,
            {"contract", "literal", "contract_owner", "physical_writer", "collision_rule"},
            f"schema_registry_lock[{index}]",
        )
    _exact_keys(
        manifest["contract_digest_equation"],
        {"equation", "canonical_json_rules", "type_strictness", "consumption_rule"},
        "contract_digest_equation",
    )
    for index, row in enumerate(manifest["contract_digests"]):
        _exact_keys(
            row,
            {"literal", "contract_owner", "ordered_fields", "contract_digest"},
            f"contract_digests[{index}]",
        )
    surfaces = _exact_keys(
        manifest["pg_aggregate_surfaces"],
        {
            "new_relation_names",
            "relation_owners",
            "owner_aggregates",
            "sqlite_ddl_fallback_mirror",
            "migration_reservation",
        },
        "pg_aggregate_surfaces",
    )
    _exact_keys(
        surfaces["migration_reservation"],
        {"path", "slot", "predecessor_slot", "status", "authoring_packet", "stale_if"},
        "pg_aggregate_surfaces.migration_reservation",
    )
    start = _exact_keys(
        manifest["start_authority_contracts"],
        {"carrier", "execution_authority", "writer_taxonomy", "source_join"},
        "start_authority_contracts",
    )
    _exact_keys(
        start["carrier"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "internal_only",
            "may_contain_private_runtime_namespace",
            "model_safe_results_expose_carrier",
            "propagation_chain",
            "propagation_rule",
            "root_v2_mutation",
        },
        "start_authority_contracts.carrier",
    )
    _exact_keys(
        start["execution_authority"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "plan_body_digest_exclusion",
            "carried_by",
            "later_stages",
        },
        "start_authority_contracts.execution_authority",
    )
    for index, row in enumerate(start["writer_taxonomy"]):
        _exact_keys(
            row,
            {"stage", "physical_writer", "command_owner", "event_actor", "required_behavior"},
            f"writer_taxonomy[{index}]",
        )
    _exact_keys(
        start["source_join"],
        {
            "predicate",
            "lifecycle_alias_in_identity",
            "join_token",
            "public_miss_outcome",
            "internal_diagnostics_visibility",
        },
        "start_authority_contracts.source_join",
    )
    runtime = _exact_keys(
        manifest["runtime_namespace_capability_envelope"],
        {
            "namespace_ref",
            "capability",
            "envelope",
            "rejected_execution_manifest_literal",
            "retained_v1_capability_literal",
        },
        "runtime_namespace_capability_envelope",
    )
    _exact_keys(
        runtime["namespace_ref"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "ref_digest_rule",
            "generation_rule",
            "pg_private_columns",
            "resolution_rule",
            "public_fields",
            "public_boundary",
        },
        "namespace_ref",
    )
    _exact_keys(
        runtime["capability"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "provider_mode_allowed",
            "positive_exact_integer_fields",
            "max_output_candidates_bound",
            "capability_digest_rule",
        },
        "capability",
    )
    _exact_keys(
        runtime["envelope"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "planned_lane_ref_fields",
            "ordinal_rule",
            "lane_order_rule",
            "envelope_digest_rule",
            "recompile_sequence",
            "mismatch_effect",
        },
        "envelope",
    )
    records = _exact_keys(
        manifest["execution_record_contracts"],
        {"lane_result", "candidate_member", "candidate_set", "execution_result", "execution_commit"},
        "execution_record_contracts",
    )
    _exact_keys(
        records["lane_result"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "ordinal_mapping",
            "completeness_rule",
            "missing_required_lane_count_as_coverage",
        },
        "lane_result",
    )
    _exact_keys(
        records["candidate_member"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "membership_fields",
            "membership_order",
            "identity_order",
            "public_profile_url_rule",
            "duplicate_rule",
        },
        "candidate_member",
    )
    _exact_keys(
        records["candidate_set"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "authoritative_collection",
            "header_rebuild_rule",
            "digest_rule",
            "product_publication_bounds",
        },
        "candidate_set",
    )
    _exact_keys(
        records["execution_result"],
        {"schema_version", "contract_owner", "ordered_fields", "lane_coverage_status", "derivation_rule"},
        "execution_result",
    )
    _exact_keys(
        records["execution_commit"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "ordered_fields_note",
            "insert_once",
            "digest_rule",
            "uow_rule",
        },
        "execution_commit",
    )
    relations = manifest["physical_relations"]
    assert type(relations) is dict
    for name, row in relations.items():
        _exact_keys(row, {"owner", "columns", "constraints"}, f"physical_relations.{name}")
    terminal = _exact_keys(
        manifest["product_terminal_records"],
        {
            "product_terminal",
            "terminal_digest_equality",
            "freshness_ref",
            "readiness_ref",
            "zero_member_rule",
            "deferred_reason_precedence",
            "missing_foreign_rule",
        },
        "product_terminal_records",
    )
    _exact_keys(
        terminal["product_terminal"],
        {"schema_version", "contract_owner", "ordered_fields", "terminal_digest_rule"},
        "product_terminal",
    )
    _exact_keys(
        terminal["freshness_ref"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "status",
            "fresh_rule",
            "stale_rule",
            "never_stale_proof",
        },
        "freshness_ref",
    )
    _exact_keys(
        terminal["readiness_ref"],
        {
            "schema_version",
            "contract_owner",
            "ordered_fields",
            "status",
            "reason",
            "projection_state",
            "prerequisite_set",
            "excluded_diagnostics",
            "diagnostic_mutation_effect",
        },
        "readiness_ref",
    )
    _exact_keys(
        terminal["zero_member_rule"],
        {"outcome", "reason", "retryable", "reselection_required"},
        "zero_member_rule",
    )
    result = _exact_keys(
        manifest["result_v3_slot_contract"],
        {
            "result_schema_version",
            "serializer",
            "tool_spec",
            "registration_rule",
            "result_link_policy",
            "effect_class",
            "projection_ref",
            "public_runtime_namespace_ref_fields",
            "success_root_fields",
            "deferred_root_fields",
            "deferred_variant_constant",
            "masked_root_fields",
            "masked_root_constants",
            "decision_ref_rule",
            "candidate_ref_rule",
            "terminal_tuple_common",
            "success_target",
            "deferred_masked_target",
            "terminal_winner",
            "owner_refs",
            "prepare_rule",
            "replay_rule",
            "quarantine_rule",
            "retention_consequence",
        },
        "result_v3_slot_contract",
    )
    _exact_keys(result["serializer"], {"owner_name", "revision", "owner"}, "result_v3.serializer")
    _exact_keys(result["projection_ref"], {"schema_version", "ordered_fields"}, "result_v3.projection_ref")
    _exact_keys(
        result["masked_root_constants"],
        {"variant", "status", "reason", "retryable"},
        "result_v3.masked_root_constants",
    )
    _exact_keys(
        result["terminal_tuple_common"],
        {
            "action_id",
            "operation_run_id",
            "workflow_command_id",
            "activity_run_id",
            "activity_attempt_id",
            "command_attempt",
            "command_generation",
            "control_epoch",
            "provider_call_id_template",
            "tool_call_id",
            "result_attempt_id",
            "owner_result_digest",
            "serialized_result_digest",
        },
        "result_v3.terminal_tuple_common",
    )
    _exact_keys(
        result["success_target"],
        {
            "owner_target_kind",
            "owner_target_id",
            "owner_target_revision",
            "owner_target_generation",
            "owner_target_revision_token",
            "is_error",
        },
        "result_v3.success_target",
    )
    _exact_keys(
        result["deferred_masked_target"],
        {
            "owner_target_kind",
            "owner_target_id",
            "owner_target_revision",
            "owner_target_generation",
            "owner_target_revision_token",
            "is_error",
        },
        "result_v3.deferred_masked_target",
    )
    _exact_keys(
        result["terminal_winner"],
        {"literal", "equation", "determinism"},
        "result_v3.terminal_winner",
    )
    owner_refs = _exact_keys(
        result["owner_refs"],
        {"success", "stale", "not_ready", "masked"},
        "result_v3.owner_refs",
    )
    for variant in ("success", "stale", "not_ready"):
        _exact_keys(
            owner_refs[variant],
            {"schema_version", "ordered_field_rules"},
            f"result_v3.owner_refs.{variant}",
        )
    _exact_keys(
        owner_refs["masked"],
        {"schema_version", "ordered_field_rules", "excluded_content"},
        "result_v3.owner_refs.masked",
    )
    for index, row in enumerate(manifest["owner_graph"]):
        _exact_keys(
            row,
            {"edge", "contract_data", "physical_writer", "contract_owner", "event_actor", "reader_consumer"},
            f"owner_graph[{index}]",
        )
    lock = _exact_keys(
        manifest["lock_order"],
        {"rules", "groups", "write_race_outcomes"},
        "lock_order",
    )
    for index, row in enumerate(lock["groups"]):
        _exact_keys(
            row,
            {"order", "lock_group", "used_by", "first_write_after"},
            f"lock_order.groups[{index}]",
        )
    for index, row in enumerate(lock["write_race_outcomes"]):
        _exact_keys(
            row,
            {"case", "locked_decision", "permitted_write"},
            f"lock_order.write_race_outcomes[{index}]",
        )
    _exact_keys(
        manifest["count_digest_equations"],
        {
            "definitions",
            "lane_count_equations",
            "occurrence_count_equations",
            "member_set_equations",
            "missing_required_lane_count_rule",
            "digest_equations",
            "cross_surface_equations",
            "excluded_counts",
            "v3_page_equations",
            "reader_repair",
        },
        "count_digest_equations",
    )
    _exact_keys(
        manifest["retention_replay_migration_gates"],
        {"retained_literals", "retained_rule", "lookup_forbidden", "rejected_literals", "gates"},
        "retention_replay_migration_gates",
    )
    oracles = _exact_keys(
        manifest["hostile_mutation_oracles"],
        {"rule", "families", "concurrency_coverage"},
        "hostile_mutation_oracles",
    )
    for index, row in enumerate(oracles["families"]):
        _exact_keys(row, {"family", "mutations"}, f"hostile_mutation_oracles.families[{index}]")
    for index, row in enumerate(manifest["mechanism_invariant_matrix"]):
        _exact_keys(row, INVARIANT_COLUMNS, f"mechanism_invariant_matrix[{index}]")
    for index, row in enumerate(manifest["implementation_batches"]):
        _exact_keys(
            row,
            {"packet", "wave", "depends_on", "exclusive_write_paths", "review_edge", "promotion_requires_review"},
            f"implementation_batches[{index}]",
        )
    _exact_keys(
        manifest["implementation_dag"],
        {"wave1_exclusions", "integration_order", "cherry_pick_rule"},
        "implementation_dag",
    )
    for index, row in enumerate(manifest["transition_states"]):
        _exact_keys(
            row,
            {"state", "current_value", "fixed_forward_can_do", "first_change_condition"},
            f"transition_states[{index}]",
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
        {
            "handoff",
            "precedent_decision",
            "precedent_test",
            "collision_method",
            "migration_inventory",
            "retained_literal_evidence",
        },
        "evidence",
    )
    return manifest


def _scan_repo_bytes() -> list[tuple[Path, bytes]]:
    payloads: list[tuple[Path, bytes]] = []
    for root, dirs, files in os.walk(REPO_ROOT):
        rel_root = Path(root).relative_to(REPO_ROOT)
        rel_root_text = "" if str(rel_root) == "." else rel_root.as_posix()
        kept_dirs: list[str] = []
        for name in sorted(dirs):
            if name in SCAN_EXCLUDE_DIRS:
                continue
            rel_dir = f"{rel_root_text}/{name}" if rel_root_text else name
            if _scan_path_excluded(rel_dir):
                continue
            kept_dirs.append(name)
        dirs[:] = kept_dirs
        for name in sorted(files):
            path = Path(root) / name
            if path in SCAN_SKIP_FILES:
                continue
            rel_file = f"{rel_root_text}/{name}" if rel_root_text else name
            if _scan_path_excluded(rel_file):
                continue
            payloads.append((path, path.read_bytes()))
    return payloads


def _contract_digest(literal: str, owner: str, ordered_fields: list[str]) -> str:
    blob = json.dumps(
        {
            "schema_version": literal,
            "owner": owner,
            "ordered_fields": ordered_fields,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def test_s1f0c_ff_manifest_is_closed_and_unknown_keys_fail() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    assert manifest["schema_version"] == "filter_projection_lineage_fixed_forward_decision.v1"
    assert manifest["decision_id"] == "track_d.d1n.s1f0c.fixed_forward_lineage"
    assert manifest["status"] == "decision_locked_not_implemented"

    unknown_top = copy.deepcopy(manifest)
    unknown_top["future_alias"] = True
    with pytest.raises(AssertionError, match="root"):
        _validate_closed_manifest(unknown_top)

    unknown_nested = copy.deepcopy(manifest)
    unknown_nested["mechanism_invariant_matrix"][0]["implicit_owner"] = "forbidden"
    with pytest.raises(AssertionError, match="mechanism_invariant_matrix"):
        _validate_closed_manifest(unknown_nested)

    unknown_lock_row = copy.deepcopy(manifest)
    unknown_lock_row["schema_registry_lock"][0]["extra_writer"] = "forbidden"
    with pytest.raises(AssertionError, match="schema_registry_lock"):
        _validate_closed_manifest(unknown_lock_row)


def test_s1f0c_ff_registry_lock_declares_every_adopted_literal_exactly_once() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    rows = manifest["schema_registry_lock"]
    literals = [row["literal"] for row in rows]
    assert len(rows) == 21
    assert len(set(literals)) == 21, "every adopted literal must be declared exactly once"
    assert set(literals) == ADOPTED_LITERALS
    contracts = [row["contract"] for row in rows]
    assert len(set(contracts)) == 21

    digest_rows = manifest["contract_digests"]
    digest_literals = [row["literal"] for row in digest_rows]
    assert len(digest_rows) == 18
    assert len(set(digest_literals)) == 18
    assert set(digest_literals) == DIGESTED_LITERALS
    assert set(literals) - set(digest_literals) == REGISTRY_ONLY_LITERALS

    owners = manifest["owners"]
    for row in rows:
        assert row["contract_owner"], row
        assert row["physical_writer"], row
        assert row["collision_rule"].startswith("zero"), row
    owner_values = set(owners.values())
    for row in digest_rows:
        assert row["contract_owner"] in owner_values, row["literal"]


def test_s1f0c_ff_new_literals_and_relations_have_zero_repo_collisions() -> None:
    payloads = _scan_repo_bytes()
    assert payloads, "repo scan must read real tracked content"
    needles = sorted(NEW_LITERALS | NEW_RELATION_NAMES)
    for needle in needles:
        encoded = needle.encode("utf-8")
        hits = [str(path.relative_to(REPO_ROOT)) for path, content in payloads if encoded in content]
        assert hits == [], f"{needle} collides with existing tracked content: {hits}"


def test_s1f0c_ff_retained_and_rejected_literals_are_history_never_retyped() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    gates = manifest["retention_replay_migration_gates"]
    assert set(gates["retained_literals"]) == RETAINED_LITERALS
    assert len(gates["retained_literals"]) == 7
    assert set(gates["rejected_literals"]) == REJECTED_LITERALS
    assert gates["lookup_forbidden"] == [
        "shape inference",
        "lexicographic latest",
        "mutable current alias",
        "auto-upgrade",
    ]

    adopted = {row["literal"] for row in manifest["schema_registry_lock"]}
    assert not (RETAINED_LITERALS & adopted), "retained literals are history, not re-declared"
    assert not (REJECTED_LITERALS & adopted), "rejected literals must never be adopted"
    digested = {row["literal"] for row in manifest["contract_digests"]}
    assert not (RETAINED_LITERALS & digested)
    assert not (REJECTED_LITERALS & digested)
    for rejected, rule in gates["rejected_literals"].items():
        assert any(literal in rule for literal in adopted), rejected

    payloads = _scan_repo_bytes()
    for retained in sorted(RETAINED_LITERALS):
        encoded = retained.encode("utf-8")
        hits = [path for path, content in payloads if encoded in content]
        assert hits, f"retained literal {retained} must remain byte-referenced in the current tree"


def test_s1f0c_ff_migration_slot_0015_is_reserved_next_free_after_0014() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    migration_files = sorted(path.name for path in MIGRATIONS_DIR.glob("*.sql"))
    assert migration_files, "migration inventory must be readable"
    slots = [int(name.split("_", maxsplit=1)[0]) for name in migration_files]
    assert len(set(slots)) == len(slots), "migration slots must be unique"
    assert max(slots) == 14
    assert "0014_agent_tool_result_attempt_effect_contract.sql" in migration_files
    assert not any(name.startswith("0015") for name in migration_files)

    reservation = manifest["pg_aggregate_surfaces"]["migration_reservation"]
    assert reservation["slot"] == max(slots) + 1 == 15
    assert reservation["predecessor_slot"] == 14
    assert reservation["path"] == "src/sourcing_agent/migrations/0015_s1f0c_filter_projection_lineage.sql"
    assert reservation["status"] == "reserved_not_authored"
    assert reservation["authoring_packet"] == "FF-PG-SCHEMA"
    assert not (MIGRATIONS_DIR / "0015_s1f0c_filter_projection_lineage.sql").exists()


def test_s1f0c_ff_contract_digests_recompute_exactly_under_the_pinned_equation() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    equation = manifest["contract_digest_equation"]
    assert "SHA256(UTF8(canonical_json(" in equation["equation"]
    assert equation["canonical_json_rules"] == [
        "sorted object keys",
        "compact separators",
        "unicode preserved",
        "allow_nan=false",
        "duplicate keys rejected at decode",
        "recursive type-strict equality",
    ]
    assert equation["type_strictness"] == [
        "true != 1",
        "1 != 1.0",
        "a copied digest is never proof without rebuilding its source object",
    ]
    for row in manifest["contract_digests"]:
        assert len(row["ordered_fields"]) == len(set(row["ordered_fields"])), row["literal"]
        recomputed = _contract_digest(row["literal"], row["contract_owner"], row["ordered_fields"])
        assert row["contract_digest"] == recomputed, row["literal"]
        assert re.fullmatch(r"[0-9a-f]{64}", row["contract_digest"]), row["literal"]


def test_s1f0c_ff_digest_fields_match_the_section_field_manifests() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    digest_fields = {row["literal"]: row["ordered_fields"] for row in manifest["contract_digests"]}
    start = manifest["start_authority_contracts"]
    runtime = manifest["runtime_namespace_capability_envelope"]
    records = manifest["execution_record_contracts"]
    terminal = manifest["product_terminal_records"]
    result = manifest["result_v3_slot_contract"]
    assert digest_fields["acquisition_start_authority_carrier.v1"] == start["carrier"]["ordered_fields"]
    assert digest_fields["acquisition_execution_authority.v1"] == start["execution_authority"]["ordered_fields"]
    assert digest_fields["agent_runtime_namespace_ref.v1"] == runtime["namespace_ref"]["ordered_fields"]
    assert digest_fields["cohort_execution_capability.v2"] == runtime["capability"]["ordered_fields"]
    assert digest_fields["cohort_execution_envelope.v1"] == runtime["envelope"]["ordered_fields"]
    assert digest_fields["cohort_execution_lane_result.v2"] == records["lane_result"]["ordered_fields"]
    assert digest_fields["cohort_candidate_member.v1"] == records["candidate_member"]["ordered_fields"]
    assert digest_fields["cohort_candidate_set.v1"] == records["candidate_set"]["ordered_fields"]
    assert digest_fields["cohort_execution_result.v2"] == records["execution_result"]["ordered_fields"]
    assert digest_fields["cohort_execution_commit.v1"] == records["execution_commit"]["ordered_fields"]
    assert digest_fields["filter_projection_product_terminal.v1"] == terminal["product_terminal"]["ordered_fields"]
    assert digest_fields["filter_projection_freshness_ref.v1"] == terminal["freshness_ref"]["ordered_fields"]
    assert digest_fields["filter_projection_readiness_ref.v1"] == terminal["readiness_ref"]["ordered_fields"]
    assert digest_fields["filter_projection_product_ref.v1"] == result["projection_ref"]["ordered_fields"]
    owner_refs = result["owner_refs"]
    assert digest_fields["filter_projection_success_owner_ref.v1"] == owner_refs["success"]["ordered_field_rules"]
    assert digest_fields["filter_projection_stale_owner_ref.v1"] == owner_refs["stale"]["ordered_field_rules"]
    assert digest_fields["filter_projection_not_ready_owner_ref.v1"] == owner_refs["not_ready"]["ordered_field_rules"]
    assert digest_fields["filter_projection_masked_absence_owner_ref.v1"] == owner_refs["masked"]["ordered_field_rules"]

    expected_counts = {
        "acquisition_start_authority_carrier.v1": 6,
        "acquisition_execution_authority.v1": 11,
        "agent_runtime_namespace_ref.v1": 8,
        "cohort_execution_capability.v2": 19,
        "cohort_execution_envelope.v1": 12,
        "cohort_execution_lane_result.v2": 15,
        "cohort_candidate_member.v1": 7,
        "cohort_candidate_set.v1": 9,
        "cohort_execution_result.v2": 21,
        "cohort_execution_commit.v1": 17,
        "filter_projection_product_terminal.v1": 26,
        "filter_projection_freshness_ref.v1": 11,
        "filter_projection_readiness_ref.v1": 14,
        "filter_projection_product_ref.v1": 4,
    }
    for literal, count in expected_counts.items():
        assert len(digest_fields[literal]) == count, literal
        if literal == "cohort_execution_commit.v1":
            # Commit fields mirror the section-5.5 physical columns, which carry no schema_version column.
            assert digest_fields[literal][0] == "execution_commit_id", literal
        else:
            assert digest_fields[literal][0] == "schema_version", literal
    for literal in (
        "acquisition_start_authority_carrier.v1",
        "acquisition_execution_authority.v1",
        "agent_runtime_namespace_ref.v1",
        "cohort_execution_capability.v2",
        "cohort_execution_envelope.v1",
        "cohort_execution_lane_result.v2",
        "cohort_candidate_member.v1",
        "cohort_candidate_set.v1",
        "cohort_execution_result.v2",
        "cohort_execution_commit.v1",
        "filter_projection_product_terminal.v1",
        "filter_projection_freshness_ref.v1",
        "filter_projection_readiness_ref.v1",
    ):
        assert digest_fields[literal][-1].endswith("_digest"), literal


def test_s1f0c_ff_physical_relations_match_surface_owners_and_columns() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    surfaces = manifest["pg_aggregate_surfaces"]
    assert set(surfaces["new_relation_names"]) == NEW_RELATION_NAMES
    assert len(surfaces["new_relation_names"]) == 6
    assert surfaces["sqlite_ddl_fallback_mirror"] == "forbidden"
    assert set(surfaces["relation_owners"]) == NEW_RELATION_NAMES
    assert surfaces["owner_aggregates"] == [
        "namespace registry",
        "Cohort execution aggregate",
        "serving projection aggregate",
    ]

    relations = manifest["physical_relations"]
    assert set(relations) == NEW_RELATION_NAMES
    for name, row in relations.items():
        assert row["owner"] == surfaces["relation_owners"][name], name
        assert len(row["columns"]) == len(set(row["columns"])), name
        assert row["constraints"], name
    assert surfaces["relation_owners"]["agent_runtime_namespace_refs"] == manifest["owners"]["namespace_registry_owner"]
    for name in (
        "cohort_execution_attempts",
        "cohort_execution_lane_results",
        "cohort_candidate_set_members",
        "cohort_execution_commits",
    ):
        assert surfaces["relation_owners"][name] == manifest["owners"]["cohort_execution_writer"], name
    assert surfaces["relation_owners"]["filter_projection_product_terminals"] == manifest["owners"]["product_terminal_writer"]

    attempts = relations["cohort_execution_attempts"]
    assert len(attempts["columns"]) == 26
    assert "unique (workspace_id,acquisition_run_id,execution_generation)" in attempts["constraints"]
    members = relations["cohort_candidate_set_members"]
    assert "primary key (execution_commit_id,candidate_identity_key)" in members["constraints"]
    commits = relations["cohort_execution_commits"]
    assert len(commits["columns"]) == 18
    for column in ("candidate_set_digest", "candidate_count", "commit_digest"):
        assert column in commits["columns"]
    terminals = relations["filter_projection_product_terminals"]
    assert len(terminals["columns"]) == 30
    for column in ("candidate_count", "visible_member_count", "excluded_member_count", "terminal_digest"):
        assert column in terminals["columns"]
    assert any("no UPDATE/DELETE normal path" in rule for rule in terminals["constraints"])
    assert any("run_scope" in rule for rule in terminals["constraints"])


def test_s1f0c_ff_terminal_freshness_readiness_records_are_exact() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    terminal = manifest["product_terminal_records"]
    equality = terminal["terminal_digest_equality"]
    parts = [part.strip() for part in equality.split("==")]
    assert parts == [
        "serialized_result.projection_ref.terminal_digest",
        "success_owner_ref.terminal_digest",
        "freshness_ref.terminal_digest",
        "readiness_ref.terminal_digest",
        "filter_projection_product_terminals.terminal_digest",
    ]
    assert terminal["product_terminal"]["ordered_fields"][-1] == "terminal_digest"

    freshness = terminal["freshness_ref"]
    assert freshness["status"] == "fresh"
    assert freshness["never_stale_proof"] == [
        "missing target",
        "inaccessible target",
        "unrelated current route",
        "arbitrary digest mismatch",
        "timestamps",
    ]

    readiness = terminal["readiness_ref"]
    assert readiness["status"] == "ready"
    assert readiness["reason"] == ""
    assert readiness["projection_state"] == "serving"
    assert readiness["prerequisite_set"] == [
        "execution_lanes_complete",
        "candidate_set_nonempty",
        "candidate_set_exact",
        "projection_state_serving",
        "route_active",
        "membership_exact",
        "all_members_visible",
        "all_rows_ready",
    ]
    assert len(readiness["excluded_diagnostics"]) == 7

    zero_rule = terminal["zero_member_rule"]
    assert zero_rule == {
        "outcome": "deferred not_ready",
        "reason": "projection_candidate_set_empty",
        "retryable": False,
        "reselection_required": True,
    }
    precedence = terminal["deferred_reason_precedence"]
    assert precedence == [
        "projection_candidate_set_empty",
        "projection_publication_pending",
        "projection_state_not_serving",
        "projection_membership_pending",
        "projection_members_not_ready",
    ]
    assert precedence[0] == zero_rule["reason"]
    assert "projection_not_found" in terminal["missing_foreign_rule"]


def test_s1f0c_ff_v3_result_and_slot_mapping_is_commandless_and_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    result = manifest["result_v3_slot_contract"]
    assert result["result_link_policy"] == "no_command_v1"
    assert result["effect_class"] == "read_only"
    assert result["result_schema_version"] == "filter_projection_result_v3"
    assert result["tool_spec"] == "filter_projection_tool_v3"
    assert result["serializer"] == {
        "owner_name": "projection_search_service.filter_projection_result_serializer_v3",
        "revision": "filter_projection_result_serializer_v3",
        "owner": "projection_search_service",
    }
    assert "neither current nor served" in result["registration_rule"]

    assert len(result["success_root_fields"]) == 21
    assert result["deferred_root_fields"] == [
        "variant",
        "status",
        "reason",
        "retryable",
        "reselection_required",
        "requested_target_ref",
        "decision_ref",
    ]
    assert result["masked_root_fields"] == ["variant", "status", "reason", "retryable", "decision_ref"]
    assert result["masked_root_constants"] == {
        "variant": "error",
        "status": "failed",
        "reason": "projection_not_found",
        "retryable": False,
    }
    assert result["deferred_variant_constant"] == "deferred"
    assert result["public_runtime_namespace_ref_fields"] == [
        "schema_version",
        "namespace_ref_id",
        "ref_digest",
    ]

    common = result["terminal_tuple_common"]
    for field in ("action_id", "operation_run_id", "workflow_command_id", "activity_run_id", "activity_attempt_id"):
        assert common[field] == "", field
    for field in ("command_attempt", "command_generation", "control_epoch"):
        assert common[field] == 0, field
    assert common["provider_call_id_template"] == "no_provider:filter_projection:<logical_occurrence_digest>"

    success = result["success_target"]
    assert success["owner_target_kind"] == "filter_projection_product_terminal"
    assert success["owner_target_revision"] == 1
    assert success["owner_target_generation"] == "terminal_generation"
    assert success["owner_target_revision_token"] == "terminal_digest"
    assert success["is_error"] is False
    deferred = result["deferred_masked_target"]
    assert deferred["owner_target_kind"] == "filter_projection_occurrence_decision"
    assert deferred["owner_target_revision"] == 0
    assert deferred["owner_target_generation"] == "occurrence.slot_generation"
    assert deferred["is_error"] == "false for stale/not-ready; true for masked"

    winner = result["terminal_winner"]
    assert winner["literal"] == "filter_projection_terminal_winner.v1"
    assert winner["equation"].startswith('SHA256("filter_projection_terminal_winner.v1\\0"')
    assert "owner_result_digest" in winner["equation"]
    assert "never authority" in result["prepare_rule"]
    assert "zero writes" in result["quarantine_rule"]
    assert "cannot be deleted inside the replay/quarantine retention window" in result["retention_consequence"]

    masked_ref = result["owner_refs"]["masked"]
    assert any("projection_not_found" in rule for rule in masked_ref["ordered_field_rules"])
    assert "no target id/workspace/existence/foreign owner bytes" in masked_ref["excluded_content"]


def test_s1f0c_ff_lock_order_and_write_race_outcomes_fail_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    lock = manifest["lock_order"]
    assert [group["order"] for group in lock["groups"]] == [0, 1, 2, 3, 4, 5, 6]
    assert any("lower number after a higher number" in rule for rule in lock["rules"])
    assert any("UTF-8" in rule for rule in lock["rules"])

    outcomes = lock["write_race_outcomes"]
    assert len(outcomes) == 18
    first = outcomes[0]
    assert "projection_not_found" in first["locked_decision"]
    assert first["permitted_write"].startswith("zero")
    zero_write_cases = [row for row in outcomes if "zero" in row["permitted_write"]]
    assert len(zero_write_cases) >= 8
    quarantine_cases = [row for row in outcomes if "quarantine attempt only" in row["permitted_write"]]
    assert {row["case"] for row in quarantine_cases} == {
        "different late result after accept",
        "wrong pending generation",
    }
    assert outcomes[-1]["case"] == "owner unavailable for a new quarantine"
    assert outcomes[-1]["permitted_write"] == "zero writes"


def test_s1f0c_ff_count_and_digest_equations_are_internally_consistent() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    equations = manifest["count_digest_equations"]
    records = manifest["execution_record_contracts"]
    runtime = manifest["runtime_namespace_capability_envelope"]

    result_fields = records["execution_result"]["ordered_fields"]
    for field in (
        "planned_lane_count",
        "executed_lane_count",
        "raw_occurrence_count",
        "accepted_occurrence_count",
        "unique_candidate_count",
        "truncated_count",
        "rejected_unverified_count",
        "missing_required_lane_count",
        "candidate_set_digest",
        "result_digest",
    ):
        assert field in result_fields, field
    assert records["execution_result"]["lane_coverage_status"] == "complete"

    lane_fields = records["lane_result"]["ordered_fields"]
    for field in (
        "raw_occurrence_count",
        "accepted_occurrence_count",
        "rejected_occurrence_count",
        "truncated_occurrence_count",
        "ordered_accepted_occurrence_digests",
        "lane_result_digest",
    ):
        assert field in lane_fields, field

    joined_lane = " ".join(equations["lane_count_equations"])
    assert "planned_lane_count" in joined_lane
    assert "executed_lane_count" in joined_lane
    assert "lane_coverage_status = complete" in joined_lane
    joined_counts = " ".join(equations["occurrence_count_equations"])
    for name in ("raw_occurrence_count", "accepted_occurrence_count", "truncated_count", "rejected_unverified_count"):
        assert name in joined_counts, name
    joined_members = " ".join(equations["member_set_equations"])
    assert "unique_candidate_count" in joined_members
    assert "<= 1000" in joined_members
    assert runtime["capability"]["max_output_candidates_bound"] == 1000
    assert "1000" in records["candidate_set"]["product_publication_bounds"]

    assert "not lane coverage" in equations["missing_required_lane_count_rule"]
    assert "role_match=all requires it to be zero" in equations["missing_required_lane_count_rule"]

    digest_lhs = [equation.split(" = ")[0] for equation in equations["digest_equations"]]
    assert digest_lhs == [
        "lane_result_digest_i",
        "member_digest_m",
        "candidate_set_digest",
        "execution_result_digest",
        "execution_commit_digest",
        "product_terminal_digest",
        "owner_result_digest",
        "serialized_result_digest",
    ]
    for equation in equations["digest_equations"]:
        assert "SHA256(" in equation

    cross = equations["cross_surface_equations"]
    assert len(cross) == 3
    assert "execution_commit.candidate_count" in cross[0]
    assert "product_terminal.candidate_count" in cross[0]
    assert "product_terminal.visible_member_count" in cross[0]
    assert cross[1].count("candidate_set_digest") == 3
    assert len(equations["excluded_counts"]) == 5
    joined_pages = " ".join(equations["v3_page_equations"])
    for name in ("total_count", "returned_count", "truncated"):
        assert name in joined_pages, name
    assert "no reader uses max, floors, supplied counts, or repairs" in equations["reader_repair"]


def test_s1f0c_ff_gates_oracles_matrix_dag_and_transitions_stay_fail_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    gates = manifest["retention_replay_migration_gates"]
    assert len(gates["gates"]) == 8
    assert "Age alone is never proof" in gates["gates"][5]
    assert "append-only" in gates["gates"][7]

    oracles = manifest["hostile_mutation_oracles"]
    families = [row["family"] for row in oracles["families"]]
    assert families == [
        "eight-row source join",
        "carrier/authority",
        "namespace/capability",
        "planning/lane mapping",
        "execution attempt",
        "candidate source/set",
        "execution commit",
        "product publication",
        "freshness",
        "readiness",
        "terminal digest",
        "V3 registry/schema",
        "terminal tuple",
        "missing/foreign",
        "result accept/replay",
        "reader behavior",
    ]
    assert len(oracles["concurrency_coverage"]) == 7
    assert "deltas are zero" in oracles["rule"]

    rows = manifest["mechanism_invariant_matrix"]
    assert len(rows) == 8
    assert {row["mechanism"] for row in rows} == {
        "carrier_execution_authority",
        "namespace_registry",
        "capability_envelope",
        "execution_attempt",
        "lane_result_candidate_aggregate",
        "product_terminal",
        "fresh_ready_refs",
        "v3_result_generic_slot",
    }
    assert all(set(row) == INVARIANT_COLUMNS for row in rows)

    graph = manifest["owner_graph"]
    assert len(graph) == 13
    assert graph[0]["contract_owner"] == manifest["owners"]["start_command_owner"]
    assert graph[-1]["contract_owner"] == manifest["owners"]["result_acceptance_writer"]

    batches = {row["packet"]: row for row in manifest["implementation_batches"]}
    assert list(batches) == [
        "FF-SCHEMA",
        "FF-CARRIER",
        "FF-COMPILER",
        "FF-SOURCE",
        "FF-ACQ-INTEGRATE",
        "FF-PG-SCHEMA",
        "FF-PG-UOW",
        "FF-RESULT",
        "FF-XO",
    ]
    assert batches["FF-SCHEMA"]["depends_on"] == ["FF-DI-decision-GO"]
    assert batches["FF-PG-SCHEMA"]["exclusive_write_paths"][0] == (
        "src/sourcing_agent/migrations/0015_s1f0c_filter_projection_lineage.sql"
    )
    assert batches["FF-PG-UOW"]["depends_on"] == ["FF-ACQ-INTEGRATE", "FF-PG-SCHEMA"]
    assert batches["FF-RESULT"]["depends_on"] == ["FF-PG-UOW"]
    assert set(batches["FF-XO"]["depends_on"]) == {"FF-ACQ-INTEGRATE", "FF-PG-SCHEMA", "FF-PG-UOW", "FF-RESULT"}
    assert all(row["promotion_requires_review"] is True for row in batches.values())
    order = manifest["implementation_dag"]["integration_order"]
    assert "FF-PG-UOW -> FF-RESULT -> FF-XO" in order

    states = {row["state"] for row in manifest["transition_states"]}
    assert states == {
        "R-019",
        "R-029",
        "public/default served population",
        "provider invocations",
        "model invocations",
        "live invocations",
        "migration delta",
    }

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


def test_s1f0c_ff_markdown_routes_decision_without_claiming_implementation() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    decision = DECISION_PATH.read_text(encoding="utf-8")
    assert decision.startswith("# Track D D1n S1f0c")
    assert "> Status: decision-locked author candidate." in decision
    for text in (
        ".coord/handoffs/s1f0c-ff-di-v1.md",
        "3bd3ed2fd803e428846961982d76f74c0fb4437e",
        "c3efff2aa07330dee52408ebc9dcc7f1786564f5",
        "acquisition_start_authority_carrier.v1",
        "acquisition_execution_authority.v1",
        "agent_runtime_namespace_ref.v1",
        "cohort_execution_capability.v2",
        "cohort_execution_envelope.v1",
        "cohort_execution_result.v2",
        "cohort_execution_commit.v1",
        "filter_projection_product_terminal.v1",
        "filter_projection_result_v3",
        "filter_projection_result_serializer_v3",
        "filter_projection_tool_v3",
        "acquisition.cohort.execute",
        "filter_projection_terminal_winner.v1",
        "agent_runtime_namespace_refs",
        "cohort_execution_attempts",
        "cohort_execution_lane_results",
        "cohort_candidate_set_members",
        "cohort_execution_commits",
        "filter_projection_product_terminals",
        "0015_s1f0c_filter_projection_lineage.sql",
        "acquisition_start_lineage_ref.v1",
        "cohort_provider_execution_manifest.v2",
        "filter_projection_publication_terminal.v1",
        "projection_not_found",
        "no_command_v1",
        "R-019",
        "R-029",
        "served=0",
    ):
        assert text in decision, text
    assert "implements no runtime writer" in decision
    assert "creates no DDL" in decision
    assert "filter_projection_lineage_fixed_forward_decision_v1.json" in decision

    assert manifest["scope"] == {
        "batch": "S1f0c-fixed-forward-decision",
        "decision_only": True,
        "ddl_delta": 0,
        "runtime_write_delta": 0,
        "provider_invocation_delta": 0,
        "model_invocation_delta": 0,
        "served_population_delta": 0,
    }
    basis = manifest["obligation_basis"]
    assert basis["design_source"] == ".coord/handoffs/s1f0c-ff-di-v1.md"
    assert basis["controlling_review"]["verdict"] == "NO-GO"
    assert basis["controlling_review"]["new_findings"] == "P0/P1/P2/P3=0/9/1/0"
    assert basis["controlling_review"]["residuals"] == ["R-019", "R-029"]
    assert len(basis["wave0_inputs"]) == 5
    assert basis["review_required_before"] == [
        "canonical S1f0c implementation (FF-SCHEMA, FF-CARRIER, FF-COMPILER, FF-SOURCE)",
        "migration 0015 authoring",
    ]
    evidence = manifest["evidence"]
    assert (REPO_ROOT / evidence["precedent_decision"]).is_file()
    assert (REPO_ROOT / evidence["precedent_test"]).is_file()
