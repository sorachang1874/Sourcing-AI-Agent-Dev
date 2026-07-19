from __future__ import annotations

import copy
import hashlib
import json
import math
import re
import subprocess
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

# The collision oracle scans exact tracked Git content at the pinned HEAD commit and
# excludes only the three candidate blobs by exact repo-relative path.
CANDIDATE_BLOBS = {
    "docs/modules/serving-product/contracts/filter_projection_lineage_fixed_forward_decision_v1.json",
    "docs/modules/serving-product/decisions/TRACK_D_D1N_S1F0C_LINEAGE_FIXED_FORWARD_DECISION.md",
    "tests/test_d1n_s1f0c_lineage_fixed_forward_decision.py",
}

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
    "digest_dependency_dag",
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
DIGESTED_LITERALS = ADOPTED_LITERALS | {"filter_projection_product_ref.v1"}
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

DESCRIPTOR_KEYS = {
    "name",
    "type",
    "required",
    "nonempty",
    "constant",
    "enum",
    "format",
    "minimum",
    "maximum",
    "items",
    "fields",
    "ref",
    "derivation",
    "variants",
    "ordering",
}
DESCRIPTOR_TYPES = {"string", "integer", "boolean", "object", "array"}
DESCRIPTOR_FORMATS = {"sha256_hex", "https_url"}
RESULT_VARIANTS = {"success", "deferred", "error"}

CORE_NODE = "filter_projection_product_terminal.v1#core"
TERMINAL_ENVELOPE_NODE = "filter_projection_product_terminal.v1"

COMMAND_IDENTITIES = ["root", "intent", "build", "review", "commit"]
EXPECTED_COMMAND_SPECS = {
    "root": ("acquisition.run.create", "acquisition_run_create", "acquisition_run_writer", None),
    "intent": ("acquisition.intent.resolve", "acquisition_intent_resolve", "acquisition_planner", "root"),
    "build": ("acquisition.plan.build", "acquisition_plan_build", "acquisition_planner", "intent"),
    "review": ("acquisition.plan_review.request", "acquisition_plan_review_request", "acquisition_planner", "build"),
    "commit": ("acquisition.plan.commit", "acquisition_plan_commit", "acquisition_planner", "review"),
}
PG_COLUMN_TYPES = {"text", "bigint", "integer", "jsonb", "timestamptz"}
IDENTIFIER_RE = re.compile(r"[a-z][a-z0-9_]*")
SHA256_RE = re.compile(r"[0-9a-f]{64}")

JOIN_SCOPE = {
    "acquisition_run_id": "run-1",
    "workspace_id": "ws-1",
    "requester_id": "req-1",
    "runtime_namespace": "ns-1",
    "provider_mode": "simulate",
}


def _load(path: Path) -> dict[str, Any]:
    def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            assert key not in result, f"duplicate key {key!r} in {path.name}"
            result[key] = value
        return result

    def _no_non_json_constants(token: str) -> Any:
        raise AssertionError(f"non-JSON constant {token!r} in {path.name}")

    value = json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=_no_duplicate_keys,
        parse_constant=_no_non_json_constants,
    )
    assert type(value) is dict
    return value


def _type_strict_equal(actual: object, expected: object) -> bool:
    """Recursive type-strict equality: 1 is not true, False is not 0, 1 is not 1.0.

    Non-finite floats are never equal to anything, including themselves.
    """
    if type(actual) is not type(expected):
        return False
    if isinstance(expected, dict):
        if set(actual) != set(expected):
            return False
        return all(_type_strict_equal(actual[key], expected[key]) for key in expected)
    if isinstance(expected, list):
        if len(actual) != len(expected):
            return False
        return all(_type_strict_equal(left, right) for left, right in zip(actual, expected, strict=True))
    if isinstance(expected, float):
        return math.isfinite(actual) and math.isfinite(expected) and actual == expected
    if isinstance(expected, bool):
        return actual is expected
    return actual == expected


def _assert_type_strict_equal(actual: object, expected: object, label: str) -> None:
    assert _type_strict_equal(actual, expected), label


def _exact_keys(value: object, expected: set[str], label: str) -> dict[str, Any]:
    assert type(value) is dict, label
    record = dict(value)
    assert set(record) == expected, label
    return record


def _validate_descriptor(descriptor: object, label: str, *, named: bool) -> None:
    assert type(descriptor) is dict, label
    assert set(descriptor) <= DESCRIPTOR_KEYS, f"{label}: unknown descriptor keys {set(descriptor) - DESCRIPTOR_KEYS}"
    if named:
        assert type(descriptor.get("name")) is str and descriptor["name"], label
        assert type(descriptor.get("required")) is bool, label
    elif "required" in descriptor:
        assert type(descriptor["required"]) is bool, label
    assert descriptor.get("type") in DESCRIPTOR_TYPES, label
    if "nonempty" in descriptor:
        assert descriptor["nonempty"] is True, label
    if "format" in descriptor:
        assert descriptor["format"] in DESCRIPTOR_FORMATS, label
    if "minimum" in descriptor:
        assert type(descriptor["minimum"]) is int, label
    if "maximum" in descriptor:
        assert type(descriptor["maximum"]) is int, label
    if "enum" in descriptor:
        assert type(descriptor["enum"]) is list and descriptor["enum"], label
        assert all(type(value) is str for value in descriptor["enum"]), label
    if "constant" in descriptor:
        assert type(descriptor["constant"]) in (str, int, bool), label
    if "variants" in descriptor:
        assert type(descriptor["variants"]) is list and descriptor["variants"], label
        assert set(descriptor["variants"]) <= RESULT_VARIANTS, label
    if "ref" in descriptor:
        assert type(descriptor["ref"]) is str and descriptor["ref"], label
    if "derivation" in descriptor:
        assert type(descriptor["derivation"]) is str and descriptor["derivation"], label
    if "ordering" in descriptor:
        assert type(descriptor["ordering"]) is str and descriptor["ordering"], label
    if "items" in descriptor:
        assert descriptor["type"] == "array", label
        _validate_descriptor(descriptor["items"], f"{label}.items", named=False)
    if "fields" in descriptor:
        assert descriptor["type"] == "object", label
        assert type(descriptor["fields"]) is list, label
        for index, sub in enumerate(descriptor["fields"]):
            _validate_descriptor(sub, f"{label}.fields[{index}]", named=True)


def _validate_schema_object(schema: object, literal: str, owner: str) -> None:
    record = _exact_keys(schema, {"schema_version", "owner", "fields"}, f"schema[{literal}]")
    assert record["schema_version"] == literal, literal
    assert record["owner"] == owner, literal
    assert type(record["fields"]) is list and record["fields"], literal
    for index, descriptor in enumerate(record["fields"]):
        _validate_descriptor(descriptor, f"schema[{literal}].fields[{index}]", named=True)
    names = [field["name"] for field in record["fields"]]
    assert len(set(names)) == len(names) or literal == "filter_projection_result_v3", literal
    if len(set(names)) != len(names):
        by_name: dict[str, list[dict[str, Any]]] = {}
        for field in record["fields"]:
            by_name.setdefault(field["name"], []).append(field)
        for name, group in by_name.items():
            if len(group) > 1:
                seen: set[str] = set()
                for descriptor in group:
                    variants = set(descriptor.get("variants", []))
                    assert variants, (literal, name)
                    assert not (variants & seen), (literal, name)
                    seen |= variants


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
            "design_evidence_closure",
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
    closure = _exact_keys(
        basis["design_evidence_closure"],
        {"hash_algorithm", "coord_files_tracked", "files", "adoption_check_rule"},
        "obligation_basis.design_evidence_closure",
    )
    for index, row in enumerate(closure["files"]):
        _exact_keys(
            row,
            {"path", "sha256", "role", "lines", "tracked"},
            f"design_evidence_closure.files[{index}]",
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
        {"equation", "schema_object_rule", "canonical_json_rules", "type_strictness", "consumption_rule"},
        "contract_digest_equation",
    )
    for index, row in enumerate(manifest["contract_digests"]):
        record = _exact_keys(
            row,
            {"literal", "contract_owner", "ordered_fields", "schema", "contract_digest"},
            f"contract_digests[{index}]",
        )
        _validate_schema_object(record["schema"], record["literal"], record["contract_owner"])
    dag = _exact_keys(manifest["digest_dependency_dag"], {"rule", "nodes"}, "digest_dependency_dag")
    for index, row in enumerate(dag["nodes"]):
        node = _exact_keys(row, {"node", "refs", "binds", "depends_on"}, f"digest_dependency_dag.nodes[{index}]")
        for bind_index, bind in enumerate(node["binds"]):
            _exact_keys(bind, {"field", "contract"}, f"digest_dependency_dag.nodes[{index}].binds[{bind_index}]")
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
        {
            "path",
            "slot",
            "predecessor_slot",
            "status",
            "authoring_packet",
            "stale_if",
            "creation_order",
            "rollback_order",
            "descriptor_source",
        },
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
    join = _exact_keys(
        start["source_join"],
        {
            "predicate",
            "scope_inputs",
            "rows",
            "command_chain",
            "requester_bindings",
            "lock_keys",
            "lifecycle_alias_in_identity",
            "join_token",
            "public_miss_outcome",
            "internal_diagnostics_visibility",
        },
        "start_authority_contracts.source_join",
    )
    _exact_keys(
        join["scope_inputs"],
        {"requested", "transport_derived", "value_rule", "provider_mode_boundary", "discovery_rule"},
        "source_join.scope_inputs",
    )
    for index, row in enumerate(join["rows"]):
        join_row = _exact_keys(
            row,
            {
                "ordinal",
                "relation",
                "row_role",
                "cardinality",
                "alternate_keys",
                "expect",
                "links",
                "equality_predicate",
                "uniqueness_outcome",
                "command_identity",
                "command_type",
                "command_owner",
                "stage_id",
                "parent_command",
                "idempotency_formula",
            },
            f"source_join.rows[{index}]",
        )
        for link_index, link in enumerate(join_row["links"]):
            _exact_keys(
                link,
                {"local_field", "remote_row", "remote_field"},
                f"source_join.rows[{index}].links[{link_index}]",
            )
    _exact_keys(
        join["command_chain"],
        {"identities", "parent_predicates", "same_group_rule", "cardinality_rule", "commit_parent_fallback"},
        "source_join.command_chain",
    )
    for index, row in enumerate(join["requester_bindings"]):
        _exact_keys(
            row,
            {"field", "source_of_truth", "required_comparisons", "forbidden_substitutions"},
            f"source_join.requester_bindings[{index}]",
        )
    lock_keys = _exact_keys(
        join["lock_keys"],
        {"advisory_groups", "row_lock_order", "group_value_order", "start_idempotency_formula", "extension_rule"},
        "source_join.lock_keys",
    )
    for index, row in enumerate(lock_keys["advisory_groups"]):
        _exact_keys(row, {"order", "keys"}, f"source_join.lock_keys.advisory_groups[{index}]")
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
        relation = _exact_keys(row, {"owner", "columns", "constraints", "descriptor"}, f"physical_relations.{name}")
        descriptor = _exact_keys(
            relation["descriptor"],
            {"columns", "primary_key", "unique_constraints", "check_constraints", "foreign_keys", "indexes"},
            f"physical_relations.{name}.descriptor",
        )
        for col_index, column in enumerate(descriptor["columns"]):
            _exact_keys(column, {"name", "type", "nullable", "default"}, f"{name}.descriptor.columns[{col_index}]")
        _exact_keys(descriptor["primary_key"], {"name", "columns"}, f"{name}.descriptor.primary_key")
        for group_index, group in enumerate(descriptor["unique_constraints"]):
            _exact_keys(group, {"name", "columns"}, f"{name}.descriptor.unique_constraints[{group_index}]")
        for check_index, check in enumerate(descriptor["check_constraints"]):
            _exact_keys(check, {"name", "expression"}, f"{name}.descriptor.check_constraints[{check_index}]")
        for fk_index, foreign_key in enumerate(descriptor["foreign_keys"]):
            fk_row = _exact_keys(
                foreign_key,
                {"name", "columns", "references", "on_delete", "on_update", "deferrable", "initially_deferred"},
                f"{name}.descriptor.foreign_keys[{fk_index}]",
            )
            _exact_keys(
                fk_row["references"], {"table", "columns"}, f"{name}.descriptor.foreign_keys[{fk_index}].references"
            )
        for ix_index, index_row in enumerate(descriptor["indexes"]):
            _exact_keys(index_row, {"name", "columns", "unique"}, f"{name}.descriptor.indexes[{ix_index}]")
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
        {"schema_version", "contract_owner", "ordered_fields", "terminal_core_digest_rule", "terminal_digest_rule"},
        "product_terminal",
    )
    _exact_keys(terminal["terminal_digest_equality"], {"core", "envelope"}, "terminal_digest_equality")
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
            {
                "edge",
                "contract_data",
                "physical_writer",
                "contract_owner",
                "owner_label",
                "event_actor",
                "reader_consumer",
            },
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


def _contract_digest(schema: dict[str, Any]) -> str:
    blob = json.dumps(schema, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _git_output(*args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, (args, result.stderr)
    return result.stdout


def _pinned_head() -> str:
    head = _git_output("rev-parse", "HEAD").strip()
    assert re.fullmatch(r"[0-9a-f]{40}", head), head
    return head


def _tracked_paths_at_head(head: str) -> list[str]:
    listing = _git_output("ls-tree", "-r", "--name-only", head, "--", ".")
    paths = [line for line in listing.splitlines() if line]
    assert len(paths) > 100, "tracked scan must cover the real repository tree"
    return paths


def _git_grep_paths(needle: str, head: str) -> list[str]:
    excludes = [f":(exclude,literal){path}" for path in sorted(CANDIDATE_BLOBS)]
    result = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "grep", "-F", "-l", "-e", needle, head, "--", ".", *excludes],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode == 1:
        return []
    assert result.returncode == 0, result.stderr
    return sorted(line.split(":", 1)[1] for line in result.stdout.splitlines() if line)


def _schema_refs(descriptor: dict[str, Any]) -> set[str]:
    refs: set[str] = set()
    if "ref" in descriptor:
        refs.add(descriptor["ref"])
    if "items" in descriptor:
        refs |= _schema_refs(descriptor["items"])
    for sub in descriptor.get("fields", []):
        refs |= _schema_refs(sub)
    return refs


def _schema_bind_field(schema: dict[str, Any], dotted: str) -> dict[str, Any]:
    fields = schema["fields"]
    parts = dotted.split(".")
    current: dict[str, Any] | None = None
    for part in parts:
        current = next((field for field in fields if field.get("name") == part), None)
        assert current is not None, f"{dotted}: missing field {part}"
        fields = current.get("fields", [])
        if "items" in current and isinstance(current["items"], dict):
            fields = current["items"].get("fields", fields)
    assert current is not None
    return current


def _dag_edges(nodes: list[dict[str, Any]]) -> dict[str, set[str]]:
    return {node["node"]: set(node["depends_on"]) for node in nodes}


def _assert_acyclic(edges: dict[str, set[str]]) -> list[str]:
    indegree = {node: 0 for node in edges}
    for node, deps in edges.items():
        for dep in deps:
            assert dep in edges, f"{node} depends on unknown node {dep}"
            indegree[node] += 1
    ready = sorted(node for node, degree in indegree.items() if degree == 0)
    order: list[str] = []
    pending = dict(indegree)
    while ready:
        node = ready.pop(0)
        order.append(node)
        for other, deps in edges.items():
            if node in deps:
                pending[other] -= 1
                if pending[other] == 0:
                    ready.append(other)
                    ready.sort()
    assert len(order) == len(edges), f"digest dependency cycle among {sorted(set(edges) - set(order))}"
    return order


def _assert_pg_descriptors_valid(manifest: dict[str, Any]) -> None:
    relations = manifest["physical_relations"]
    reservation = manifest["pg_aggregate_surfaces"]["migration_reservation"]
    assert reservation["creation_order"] == list(reversed(reservation["rollback_order"]))
    assert set(reservation["rollback_order"]) == set(relations) == NEW_RELATION_NAMES
    assert reservation["rollback_order"][0] == "filter_projection_product_terminals"
    assert reservation["rollback_order"][-1] == "agent_runtime_namespace_refs"

    constraint_names: list[str] = []
    identifiers: list[str] = []
    rollback_position = {name: index for index, name in enumerate(reservation["rollback_order"])}
    for name, relation in relations.items():
        descriptor = relation["descriptor"]
        column_names = [column["name"] for column in descriptor["columns"]]
        assert column_names == relation["columns"], name
        assert len(column_names) == len(set(column_names)), name
        identifiers.append(name)
        identifiers.extend(column_names)
        for column in descriptor["columns"]:
            assert column["type"] in PG_COLUMN_TYPES, (name, column["name"])
            assert type(column["nullable"]) is bool, (name, column["name"])
            assert column["default"] in (None, "now()"), (name, column["name"])
        pk = descriptor["primary_key"]
        assert set(pk["columns"]) <= set(column_names), name
        constraint_names.append(pk["name"])
        for group in descriptor["unique_constraints"]:
            assert set(group["columns"]) <= set(column_names), name
            constraint_names.append(group["name"])
        for check in descriptor["check_constraints"]:
            assert check["expression"], name
            constraint_names.append(check["name"])
        for foreign_key in descriptor["foreign_keys"]:
            assert set(foreign_key["columns"]) <= set(column_names), name
            target = foreign_key["references"]
            assert target["table"] in relations, (name, foreign_key["name"])
            target_columns = {column["name"] for column in relations[target["table"]]["descriptor"]["columns"]}
            assert set(target["columns"]) <= target_columns, (name, foreign_key["name"])
            assert foreign_key["on_delete"] == "restrict", (name, foreign_key["name"])
            assert foreign_key["on_update"] == "restrict", (name, foreign_key["name"])
            assert foreign_key["deferrable"] is False, (name, foreign_key["name"])
            assert foreign_key["initially_deferred"] is False, (name, foreign_key["name"])
            constraint_names.append(foreign_key["name"])
            if target["table"] != name:
                assert rollback_position[name] < rollback_position[target["table"]], (
                    f"rollback order must drop {name} before {target['table']}"
                )
        for index_row in descriptor["indexes"]:
            assert set(index_row["columns"]) <= set(column_names), name
            assert type(index_row["unique"]) is bool, name
            constraint_names.append(index_row["name"])
        constraint_names.append(name)
    assert len(constraint_names) == len(set(constraint_names)), "constraint/index identifiers must be globally unique"
    for identifier in identifiers + constraint_names:
        assert IDENTIFIER_RE.fullmatch(identifier), identifier
        assert len(identifier.encode("utf-8")) <= 63, identifier


def _join_positive_rows() -> list[dict[str, Any]]:
    return [
        {
            "relation": "operation_runs",
            "operation_run_id": "op-1",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "workspace_id": "ws-1",
            "action_id": "act-1",
            "start_idempotency": "agent-start-v2:abc",
            "result_ref_json": {"owner": "ref"},
        },
        {
            "relation": "agent_actions",
            "action_id": "act-1",
            "action_type": "start_acquisition_run",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "workspace_id": "ws-1",
            "start_idempotency": "agent-start-v2:abc",
            "result_ref_json": {"owner": "ref"},
            "bound_requester_id": "req-1",
            "owner_ref_provider_mode": "simulate",
            "owner_ref_runtime_namespace": "ns-1",
        },
        {
            "relation": "workflow_commands",
            "command_identity": "root",
            "command_id": "cmd-root",
            "command_type": "acquisition.run.create",
            "command_owner": "acquisition_run_writer",
            "stage_id": "acquisition_run_create",
            "parent_command_id": "",
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
        },
        {
            "relation": "workflow_commands",
            "command_identity": "intent",
            "command_id": "cmd-intent",
            "command_type": "acquisition.intent.resolve",
            "command_owner": "acquisition_planner",
            "stage_id": "acquisition_intent_resolve",
            "parent_command_id": "cmd-root",
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
        },
        {
            "relation": "workflow_commands",
            "command_identity": "build",
            "command_id": "cmd-build",
            "command_type": "acquisition.plan.build",
            "command_owner": "acquisition_planner",
            "stage_id": "acquisition_plan_build",
            "parent_command_id": "cmd-intent",
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
        },
        {
            "relation": "workflow_commands",
            "command_identity": "review",
            "command_id": "cmd-review",
            "command_type": "acquisition.plan_review.request",
            "command_owner": "acquisition_planner",
            "stage_id": "acquisition_plan_review_request",
            "parent_command_id": "cmd-build",
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
        },
        {
            "relation": "workflow_commands",
            "command_identity": "commit",
            "command_id": "cmd-commit",
            "command_type": "acquisition.plan.commit",
            "command_owner": "acquisition_planner",
            "stage_id": "acquisition_plan_commit",
            "parent_command_id": "cmd-review",
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
        },
        {
            "relation": "acquisition_runs",
            "acquisition_run_id": "run-1",
            "workspace_id": "ws-1",
            "operation_run_id": "op-1",
            "workflow_run_id": "wf-1",
            "metadata_source_command_id": "cmd-commit",
        },
    ]


def _evaluate_source_join(spec: dict[str, Any], scope: dict[str, str], rows: list[dict[str, Any]]) -> str:
    """Evaluate the manifest's structured join predicate. Returns accepted or the public miss."""
    scope_inputs = spec["scope_inputs"]
    for field in scope_inputs["requested"] + scope_inputs["transport_derived"]:
        if type(scope.get(field)) is not str or not scope[field]:
            return "projection_not_found"
    if scope["provider_mode"] not in scope_inputs["provider_mode_boundary"]:
        return "projection_not_found"

    selected: dict[str, dict[str, Any]] = {}
    for spec_row in spec["rows"]:
        matched = [
            row
            for row in rows
            if row.get("relation") == spec_row["relation"]
            and all(_type_strict_equal(row.get(key), value) for key, value in spec_row["expect"].items())
        ]
        if len(matched) != spec_row["cardinality"]:
            return "projection_not_found"
        selected[spec_row["relation"] if spec_row["row_role"] == "base" else spec_row["command_identity"]] = matched[0]

    for spec_row in spec["rows"]:
        local = selected[spec_row["relation"] if spec_row["row_role"] == "base" else spec_row["command_identity"]]
        for link in spec_row["links"]:
            remote = selected.get(link["remote_row"])
            if remote is None:
                return "projection_not_found"
            if not _type_strict_equal(local.get(link["local_field"]), remote.get(link["remote_field"])):
                return "projection_not_found"

    bindings = {binding["field"] for binding in spec["requester_bindings"]}
    assert bindings == {"workspace_id", "requester_id", "provider_mode", "runtime_namespace"}
    for relation in ("operation_runs", "agent_actions", "acquisition_runs"):
        if selected[relation].get("workspace_id") != scope["workspace_id"]:
            return "projection_not_found"
    action = selected["agent_actions"]
    if action.get("bound_requester_id") != scope["requester_id"]:
        return "projection_not_found"
    if action.get("owner_ref_provider_mode") != scope["provider_mode"]:
        return "projection_not_found"
    if action.get("owner_ref_runtime_namespace") != scope["runtime_namespace"]:
        return "projection_not_found"
    return "accepted"


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

    unknown_join_row = copy.deepcopy(manifest)
    unknown_join_row["start_authority_contracts"]["source_join"]["rows"][0]["extra_predicate"] = "forbidden"
    with pytest.raises(AssertionError, match="source_join.rows"):
        _validate_closed_manifest(unknown_join_row)

    unknown_descriptor = copy.deepcopy(manifest)
    unknown_descriptor["physical_relations"]["cohort_execution_commits"]["descriptor"]["fk_policy"] = "cascade"
    with pytest.raises(AssertionError, match="cohort_execution_commits.descriptor"):
        _validate_closed_manifest(unknown_descriptor)

    unknown_schema_key = copy.deepcopy(manifest)
    unknown_schema_key["contract_digests"][0]["schema"]["fields"][0]["alias"] = "forbidden"
    with pytest.raises(AssertionError, match="unknown descriptor keys"):
        _validate_closed_manifest(unknown_schema_key)


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
    assert len(digest_rows) == 22
    assert len(set(digest_literals)) == 22
    assert set(digest_literals) == DIGESTED_LITERALS
    assert set(literals) - set(digest_literals) == set(), "every adopted literal now carries a contract digest"

    owners = manifest["owners"]
    owner_values = set(owners.values())
    for row in rows:
        assert row["contract_owner"], row
        assert row["physical_writer"], row
        assert row["collision_rule"].startswith("zero"), row
        assert row["contract_owner"] in owner_values, row["literal"]
    for row in digest_rows:
        assert row["contract_owner"] in owner_values, row["literal"]
    v3_row = next(row for row in rows if row["literal"] == "filter_projection_result_v3")
    assert v3_row["contract_owner"] == owners["result_v3_route_owner"] == "filter_projection"


def test_s1f0c_ff_new_literals_and_relations_have_zero_repo_collisions() -> None:
    head = _pinned_head()
    tracked = _tracked_paths_at_head(head)
    assert CANDIDATE_BLOBS <= set(tracked), "the three candidate blobs must be tracked at the pinned HEAD"
    kept = [path for path in tracked if path not in CANDIDATE_BLOBS]
    assert len(tracked) - len(kept) == 3, "only the three candidate blobs are excluded, by exact path"
    needles = sorted(NEW_LITERALS | NEW_RELATION_NAMES)
    for needle in needles:
        hits = _git_grep_paths(needle, head)
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

    head = _pinned_head()
    for retained in sorted(RETAINED_LITERALS):
        hits = _git_grep_paths(retained, head)
        assert hits, f"retained literal {retained} must remain byte-referenced in the tracked tree at {head}"


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
    assert "SHA256(UTF8(canonical_json(schema)))" in equation["equation"]
    assert "ordered field descriptor" in equation["equation"]
    assert (
        "adding an enum value or deleting a derivation rule changes the schema bytes" in equation["schema_object_rule"]
    )
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
        schema_fields = row["schema"]["fields"]
        assert row["ordered_fields"] == list(dict.fromkeys(field["name"] for field in schema_fields)), row["literal"]
        assert len(row["ordered_fields"]) == len(set(row["ordered_fields"])), row["literal"]
        recomputed = _contract_digest(row["schema"])
        assert row["contract_digest"] == recomputed, row["literal"]
        assert SHA256_RE.fullmatch(row["contract_digest"]), row["literal"]

    # hostile: semantics drift must move the digest (enum widening / derivation deletion)
    capability = next(row for row in manifest["contract_digests"] if row["literal"] == "cohort_execution_capability.v2")
    widened = copy.deepcopy(capability["schema"])
    mode_field = next(field for field in widened["fields"] if field["name"] == "provider_mode")
    mode_field["enum"] = [*mode_field["enum"], "live"]
    assert _contract_digest(widened) != capability["contract_digest"], "adding live to the enum must change the digest"
    thinned = copy.deepcopy(capability["schema"])
    del next(field for field in thinned["fields"] if field["name"] == "capability_digest")["derivation"]
    assert _contract_digest(thinned) != capability["contract_digest"], (
        "deleting a derivation rule must change the digest"
    )


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
        "cohort_candidate_set.v1": 8,
        "cohort_execution_result.v2": 21,
        "cohort_execution_commit.v1": 19,
        "filter_projection_product_terminal.v1": 27,
        "filter_projection_freshness_ref.v1": 11,
        "filter_projection_readiness_ref.v1": 14,
        "filter_projection_product_ref.v1": 4,
        "filter_projection_success_owner_ref.v1": 19,
        "filter_projection_stale_owner_ref.v1": 16,
        "filter_projection_not_ready_owner_ref.v1": 19,
        "filter_projection_masked_absence_owner_ref.v1": 11,
        "filter_projection_result_v3": 26,
        "filter_projection_result_serializer_v3": 5,
        "filter_projection_tool_v3": 8,
        "acquisition.cohort.execute": 5,
    }
    assert set(digest_fields) == set(expected_counts)
    registry_first_fields = {
        "filter_projection_result_v3": "variant",
        "filter_projection_result_serializer_v3": "owner_name",
        "filter_projection_tool_v3": "tool_spec",
        "acquisition.cohort.execute": "command_type",
    }
    for literal, count in expected_counts.items():
        assert len(digest_fields[literal]) == count, literal
        if literal in registry_first_fields:
            assert digest_fields[literal][0] == registry_first_fields[literal], literal
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
        "filter_projection_success_owner_ref.v1",
        "filter_projection_stale_owner_ref.v1",
        "filter_projection_not_ready_owner_ref.v1",
        "filter_projection_masked_absence_owner_ref.v1",
    ):
        assert digest_fields[literal][-1].endswith("_digest"), literal

    # the acyclic candidate-set / terminal-core invariants are part of the field manifests
    candidate_set_fields = digest_fields["cohort_candidate_set.v1"]
    assert "execution_result_digest" not in candidate_set_fields
    result_fields = digest_fields["cohort_execution_result.v2"]
    assert result_fields[-2] == "candidate_set_digest"
    assert result_fields[-1] == "result_digest"
    terminal_fields = digest_fields["filter_projection_product_terminal.v1"]
    assert terminal_fields[23] == "terminal_core_digest"
    assert terminal_fields[-3:-1] == ["freshness_ref", "readiness_ref"]
    assert terminal_fields[-1] == "terminal_digest"
    for ref_literal in ("filter_projection_freshness_ref.v1", "filter_projection_readiness_ref.v1"):
        fields = digest_fields[ref_literal]
        assert "terminal_core_digest" in fields
        assert "terminal_digest" not in fields


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
    assert (
        surfaces["relation_owners"]["filter_projection_product_terminals"]
        == manifest["owners"]["product_terminal_writer"]
    )

    attempts = relations["cohort_execution_attempts"]
    assert len(attempts["columns"]) == 26
    assert "unique (workspace_id,acquisition_run_id,execution_generation)" in attempts["constraints"]
    members = relations["cohort_candidate_set_members"]
    assert "primary key (execution_commit_id,candidate_identity_key)" in members["constraints"]
    commits = relations["cohort_execution_commits"]
    assert len(commits["columns"]) == 20
    for column in (
        "schema_version",
        "candidate_set_digest",
        "candidate_count",
        "commit_contract_digest",
        "commit_digest",
    ):
        assert column in commits["columns"]
    terminals = relations["filter_projection_product_terminals"]
    assert len(terminals["columns"]) == 31
    for column in (
        "candidate_count",
        "visible_member_count",
        "excluded_member_count",
        "terminal_core_digest",
        "terminal_digest",
    ):
        assert column in terminals["columns"]
    assert any("no UPDATE/DELETE normal path" in rule for rule in terminals["constraints"])
    assert any("run_scope" in rule for rule in terminals["constraints"])


def test_s1f0c_ff_terminal_freshness_readiness_records_are_exact() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    terminal = manifest["product_terminal_records"]
    equality = terminal["terminal_digest_equality"]
    core_parts = [part.strip() for part in equality["core"].split("==")]
    assert core_parts == [
        "freshness_ref.terminal_core_digest",
        "readiness_ref.terminal_core_digest",
        "filter_projection_product_terminals.terminal_core_digest",
        "SHA256(canonical product-terminal fields 1-23)",
    ]
    envelope_parts = [part.strip() for part in equality["envelope"].split("==")]
    assert envelope_parts == [
        "serialized_result.projection_ref.terminal_digest",
        "success_owner_ref.terminal_digest",
        "filter_projection_product_terminals.terminal_digest",
        "SHA256(canonical product-terminal fields except terminal_digest)",
    ]
    assert "acyclic terminal core" in terminal["product_terminal"]["terminal_core_digest_rule"]
    assert (
        "terminal core -> freshness/readiness refs -> terminal envelope"
        in terminal["product_terminal"]["terminal_digest_rule"]
    )

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
    _assert_type_strict_equal(
        zero_rule,
        {
            "outcome": "deferred not_ready",
            "reason": "projection_candidate_set_empty",
            "retryable": False,
            "reselection_required": True,
        },
        "zero_member_rule",
    )
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

    # the schema objects bind the same constants and the prerequisite-set semantics
    digests = {row["literal"]: row for row in manifest["contract_digests"]}
    readiness_schema = digests["filter_projection_readiness_ref.v1"]["schema"]
    prerequisite_field = next(
        field for field in readiness_schema["fields"] if field["name"] == "prerequisite_set_digest"
    )
    for prerequisite in readiness["prerequisite_set"]:
        assert prerequisite in prerequisite_field["derivation"], prerequisite
    for literal, constants in (
        ("filter_projection_freshness_ref.v1", {"status": "fresh"}),
        ("filter_projection_readiness_ref.v1", {"status": "ready", "reason": "", "projection_state": "serving"}),
    ):
        schema_fields = {field["name"]: field for field in digests[literal]["schema"]["fields"]}
        for name, constant in constants.items():
            assert schema_fields[name].get("constant") == constant, (literal, name)


def test_s1f0c_ff_v3_result_and_slot_mapping_is_commandless_and_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    result = manifest["result_v3_slot_contract"]
    assert result["result_link_policy"] == "no_command_v1"
    assert result["effect_class"] == "read_only"
    assert result["result_schema_version"] == "filter_projection_result_v3"
    assert result["tool_spec"] == "filter_projection_tool_v3"
    _assert_type_strict_equal(
        result["serializer"],
        {
            "owner_name": "projection_search_service.filter_projection_result_serializer_v3",
            "revision": "filter_projection_result_serializer_v3",
            "owner": "projection_search_service",
        },
        "result_v3.serializer",
    )
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
    _assert_type_strict_equal(
        result["masked_root_constants"],
        {
            "variant": "error",
            "status": "failed",
            "reason": "projection_not_found",
            "retryable": False,
        },
        "result_v3.masked_root_constants",
    )
    assert result["deferred_variant_constant"] == "deferred"
    assert result["public_runtime_namespace_ref_fields"] == [
        "schema_version",
        "namespace_ref_id",
        "ref_digest",
    ]

    common = result["terminal_tuple_common"]
    for field in ("action_id", "operation_run_id", "workflow_command_id", "activity_run_id", "activity_attempt_id"):
        _assert_type_strict_equal(common[field], "", field)
    for field in ("command_attempt", "command_generation", "control_epoch"):
        _assert_type_strict_equal(common[field], 0, field)
    assert common["provider_call_id_template"] == "no_provider:filter_projection:<logical_occurrence_digest>"

    success = result["success_target"]
    assert success["owner_target_kind"] == "filter_projection_product_terminal"
    _assert_type_strict_equal(success["owner_target_revision"], 1, "owner_target_revision")
    assert success["owner_target_generation"] == "terminal_generation"
    assert success["owner_target_revision_token"] == "terminal_digest"
    _assert_type_strict_equal(success["is_error"], False, "success_target.is_error")
    deferred = result["deferred_masked_target"]
    assert deferred["owner_target_kind"] == "filter_projection_occurrence_decision"
    _assert_type_strict_equal(deferred["owner_target_revision"], 0, "deferred_masked_target.owner_target_revision")
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
    assert masked_ref["ordered_field_rules"][0] == "schema_version"
    assert "reason" in masked_ref["ordered_field_rules"]
    assert "no target id/workspace/existence/foreign owner bytes" in masked_ref["excluded_content"]
    masked_schema = next(
        row for row in manifest["contract_digests"] if row["literal"] == "filter_projection_masked_absence_owner_ref.v1"
    )
    masked_reason = next(field for field in masked_schema["schema"]["fields"] if field["name"] == "reason")
    assert masked_reason["constant"] == "projection_not_found"

    # the registry literals carry full contract digests and exact schema constants
    digests = {row["literal"]: row for row in manifest["contract_digests"]}
    result_schema_fields = {
        field["name"]: field for field in digests["filter_projection_result_v3"]["schema"]["fields"]
    }
    assert set(result["success_root_fields"]) <= set(result_schema_fields)
    assert set(result["deferred_root_fields"]) <= set(result_schema_fields)
    assert set(result["masked_root_fields"]) <= set(result_schema_fields)
    masked_expectations = {"variant": "error", "status": "failed", "reason": "projection_not_found", "retryable": False}
    for name, constant in masked_expectations.items():
        descriptor = next(
            field
            for field in digests["filter_projection_result_v3"]["schema"]["fields"]
            if field["name"] == name and field.get("variants") == ["error"]
        )
        assert descriptor.get("constant") == constant, name
    tool_fields = {field["name"]: field for field in digests["filter_projection_tool_v3"]["schema"]["fields"]}
    assert tool_fields["effect_class"]["constant"] == "read_only"
    assert tool_fields["result_link_policy"]["constant"] == "no_command_v1"
    serializer_fields = {
        field["name"]: field for field in digests["filter_projection_result_serializer_v3"]["schema"]["fields"]
    }
    assert serializer_fields["owner_name"]["constant"] == result["serializer"]["owner_name"]
    command_fields = {field["name"]: field for field in digests["acquisition.cohort.execute"]["schema"]["fields"]}
    assert command_fields["command_type"]["constant"] == "acquisition.cohort.execute"
    assert command_fields["provider_mode"]["enum"] == ["simulate", "scripted"]


def test_s1f0c_ff_lock_order_and_write_race_outcomes_fail_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    lock = manifest["lock_order"]
    assert [group["order"] for group in lock["groups"]] == [0, 1, 2, 3, 4, 5, 6]
    assert any("lower number after a higher number" in rule for rule in lock["rules"])
    assert any("UTF-8" in rule for rule in lock["rules"])
    assert "start_authority_contracts.source_join.lock_keys" in lock["groups"][1]["lock_group"]
    assert "as specified by FF-JC" not in lock["groups"][1]["lock_group"]

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
        "product_terminal_core_digest",
        "product_terminal_digest",
        "owner_result_digest",
        "serialized_result_digest",
    ]
    for equation in equations["digest_equations"]:
        assert "SHA256(" in equation
    assert "never execution_result_digest" in equations["digest_equations"][2]
    assert "binds the completed candidate_set_digest" in equations["digest_equations"][3]
    assert "schema_version and commit_contract_digest" in equations["digest_equations"][4]
    assert "excludes the nested freshness/readiness refs" in equations["digest_equations"][5]

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
    mutations = {row["family"]: row["mutations"] for row in oracles["families"]}
    assert "cycle reintroduction" in mutations["candidate source/set"]
    assert "core vs envelope digest substitution" in mutations["terminal digest"]
    assert "commit_contract_digest" in mutations["execution commit"]

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
    owner_values = set(manifest["owners"].values())
    for row in graph:
        assert row["contract_owner"] in owner_values, row["edge"]
        assert type(row["owner_label"]) is str and row["owner_label"], row["edge"]
    labels = {row["edge"]: row["owner_label"] for row in graph}
    assert labels["run -> compiler"] == "cohort_runtime compiler contract"
    assert labels["compiler -> attempt"] == "cohort_runtime schemas"
    assert labels["provider -> lane results"] == "Cohort execution aggregate"
    assert labels["lanes -> candidate set/result"] == "cohort_runtime schemas"
    assert labels["terminal -> fresh/ready read"] == "serving_projection_owner retained refs"
    assert graph[0]["contract_owner"] == manifest["owners"]["start_command_owner"]
    assert graph[-1]["contract_owner"] == manifest["owners"]["result_acceptance_writer"]
    for edge in ("run -> compiler", "compiler -> attempt", "lanes -> candidate set/result"):
        assert next(row for row in graph if row["edge"] == edge)["contract_owner"] == "cohort_runtime"
    assert (
        next(row for row in graph if row["edge"] == "provider -> lane results")["contract_owner"]
        == "cohort_provider_runtime"
    )
    assert (
        next(row for row in graph if row["edge"] == "terminal -> fresh/ready read")["contract_owner"]
        == "serving_projection_owner"
    )

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
    assert "typed physical_relations descriptors" in batches["FF-PG-SCHEMA"]["review_edge"]
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
        _assert_type_strict_equal(release[field], 0, field)
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
        "design_evidence_closure",
        "terminal_core_digest",
        "commit_contract_digest",
        "digest_dependency_dag",
        "owner_label",
    ):
        assert text in decision, text
    assert "implements no runtime writer" in decision
    assert "creates no DDL" in decision
    assert "filter_projection_lineage_fixed_forward_decision_v1.json" in decision

    _assert_type_strict_equal(
        manifest["scope"],
        {
            "batch": "S1f0c-fixed-forward-decision",
            "decision_only": True,
            "ddl_delta": 0,
            "runtime_write_delta": 0,
            "provider_invocation_delta": 0,
            "model_invocation_delta": 0,
            "served_population_delta": 0,
        },
        "scope",
    )
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


def test_s1f0c_ff_digest_dependency_dag_is_acyclic_and_rejects_back_edges() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    dag = manifest["digest_dependency_dag"]
    assert "must be acyclic" in dag["rule"]
    assert "constant self-binding" in dag["rule"]
    nodes = dag["nodes"]
    digested = {row["literal"] for row in manifest["contract_digests"]}
    node_ids = {node["node"] for node in nodes}
    assert node_ids - {CORE_NODE} == digested, "every digested literal owns exactly one DAG node"

    schemas = {row["literal"]: row["schema"] for row in manifest["contract_digests"]}
    for node in nodes:
        declared = set(node["refs"]) | {bind["contract"] for bind in node["binds"] if bind["contract"] != node["node"]}
        assert node["depends_on"] == sorted(declared), node["node"]
        if node["node"] == CORE_NODE:
            core_fields = schemas[TERMINAL_ENVELOPE_NODE]["fields"][:24]
            assert core_fields[-1]["name"] == "terminal_core_digest"
            schema_refs = set().union(*(_schema_refs(field) for field in core_fields))
        elif node["node"] == TERMINAL_ENVELOPE_NODE:
            all_refs = set().union(*(_schema_refs(field) for field in schemas[node["node"]]["fields"]))
            core_refs = set().union(*(_schema_refs(field) for field in schemas[node["node"]]["fields"][:24]))
            schema_refs = all_refs - core_refs
        else:
            schema_refs = set().union(*(_schema_refs(field) for field in schemas[node["node"]]["fields"]))
        assert schema_refs == set(node["refs"]), node["node"]
        for bind in node["binds"]:
            owner_schema = schemas[TERMINAL_ENVELOPE_NODE if node["node"] == CORE_NODE else node["node"].split("#")[0]]
            field = _schema_bind_field(owner_schema, bind["field"])
            is_digest_value = field.get("format") == "sha256_hex" or (
                field.get("type") == "array" and field.get("items", {}).get("format") == "sha256_hex"
            )
            assert is_digest_value, (node["node"], bind["field"])
            assert bind["contract"] in node_ids, (node["node"], bind["contract"])

    # the two reviewed anti-cycles are structurally absent
    candidate_set = next(node for node in nodes if node["node"] == "cohort_candidate_set.v1")
    assert "cohort_execution_result.v2" not in candidate_set["depends_on"]
    assert candidate_set["depends_on"] == ["cohort_candidate_member.v1"]
    result_node = next(node for node in nodes if node["node"] == "cohort_execution_result.v2")
    assert "cohort_candidate_set.v1" in result_node["depends_on"]
    for ref_literal in ("filter_projection_freshness_ref.v1", "filter_projection_readiness_ref.v1"):
        ref_node = next(node for node in nodes if node["node"] == ref_literal)
        assert ref_node["depends_on"] == ["cohort_candidate_set.v1", CORE_NODE]
        assert TERMINAL_ENVELOPE_NODE not in ref_node["depends_on"]
    envelope_node = next(node for node in nodes if node["node"] == TERMINAL_ENVELOPE_NODE)
    assert envelope_node["depends_on"] == [
        "filter_projection_freshness_ref.v1",
        CORE_NODE,
        "filter_projection_readiness_ref.v1",
    ]
    commit_node = next(node for node in nodes if node["node"] == "cohort_execution_commit.v1")
    assert {bind["field"] for bind in commit_node["binds"] if bind["contract"] == "cohort_execution_commit.v1"} == {
        "commit_contract_digest"
    }, "commit_contract_digest is a constant self-binding, not a construction edge"

    order = _assert_acyclic(_dag_edges(nodes))
    assert order.index("cohort_candidate_set.v1") < order.index("cohort_execution_result.v2")
    assert order.index(CORE_NODE) < order.index("filter_projection_freshness_ref.v1")
    assert order.index("filter_projection_freshness_ref.v1") < order.index(TERMINAL_ENVELOPE_NODE)
    assert order.index("filter_projection_readiness_ref.v1") < order.index(TERMINAL_ENVELOPE_NODE)

    # hostile back-edges reintroducing either reviewed cycle must be rejected
    cyclic_candidate = _dag_edges(nodes)
    cyclic_candidate["cohort_candidate_set.v1"].add("cohort_execution_result.v2")
    with pytest.raises(AssertionError, match="digest dependency cycle"):
        _assert_acyclic(cyclic_candidate)
    cyclic_terminal = _dag_edges(nodes)
    cyclic_terminal["filter_projection_freshness_ref.v1"].add(TERMINAL_ENVELOPE_NODE)
    with pytest.raises(AssertionError, match="digest dependency cycle"):
        _assert_acyclic(cyclic_terminal)
    unknown_dep = _dag_edges(nodes)
    unknown_dep["cohort_candidate_set.v1"].add("ghost_contract.v9")
    with pytest.raises(AssertionError, match="unknown node"):
        _assert_acyclic(unknown_dep)


def test_s1f0c_ff_source_join_is_structured_and_evaluates_every_case() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    spec = manifest["start_authority_contracts"]["source_join"]
    assert spec["predicate"].startswith("exact eight-row")
    assert spec["lifecycle_alias_in_identity"] == "forbidden"
    assert "projection_not_found" in spec["public_miss_outcome"]

    rows = spec["rows"]
    assert [row["ordinal"] for row in rows] == [1, 2, 3, 4, 5, 6, 7, 8]
    assert [row["relation"] for row in rows] == [
        "operation_runs",
        "agent_actions",
        "workflow_commands",
        "workflow_commands",
        "workflow_commands",
        "workflow_commands",
        "workflow_commands",
        "acquisition_runs",
    ]
    assert all(row["cardinality"] == 1 for row in rows)
    assert all(row["alternate_keys"] for row in rows)
    command_rows = [row for row in rows if row["row_role"] == "command"]
    assert [row["command_identity"] for row in command_rows] == COMMAND_IDENTITIES
    owner_values = set(manifest["owners"].values())
    for row in command_rows:
        command_type, stage_id, command_owner, parent = EXPECTED_COMMAND_SPECS[row["command_identity"]]
        assert row["command_type"] == command_type
        assert row["stage_id"] == stage_id
        assert row["command_owner"] == command_owner
        assert row["command_owner"] in owner_values
        assert row["parent_command"] == parent
        assert row["expect"]["command_type"] == command_type
        assert row["expect"]["command_owner"] == command_owner
        assert row["expect"]["stage_id"] == stage_id
    assert rows[2]["expect"]["parent_command_id"] == ""
    assert rows[2]["idempotency_formula"] == "acquisition.run.create:start-v2:{confirmation_receipt_digest}"
    assert rows[7]["idempotency_formula"] == "acquisition_run:plan_commit:{commit_command_id}"

    chain = spec["command_chain"]
    assert chain["identities"] == COMMAND_IDENTITIES
    assert chain["parent_predicates"] == {
        "root": None,
        "intent": "root",
        "build": "intent",
        "review": "build",
        "commit": "review",
    }
    assert chain["commit_parent_fallback"].startswith("forbidden")
    assert "do not count as duplicates" in chain["cardinality_rule"]

    bindings = spec["requester_bindings"]
    assert [binding["field"] for binding in bindings] == [
        "workspace_id",
        "requester_id",
        "provider_mode",
        "runtime_namespace",
    ]
    provider_binding = next(binding for binding in bindings if binding["field"] == "provider_mode")
    assert "provider_mode_intent" in provider_binding["forbidden_substitutions"]
    requester_binding = next(binding for binding in bindings if binding["field"] == "requester_id")
    assert "approval actor" in requester_binding["forbidden_substitutions"]

    lock_keys = spec["lock_keys"]
    assert [group["order"] for group in lock_keys["advisory_groups"]] == [1, 2, 3, 4]
    assert lock_keys["advisory_groups"][0]["keys"][0].startswith("acquisition_run source id advisory key")
    assert (
        lock_keys["advisory_groups"][3]["keys"][1] == "the five command id/idempotency identities in UTF-8 byte order"
    )
    assert lock_keys["row_lock_order"] == [
        "operation_runs",
        "agent_actions",
        "workflow_commands ordered by command_id bytes",
        "acquisition_runs",
    ]
    assert lock_keys["start_idempotency_formula"] == "agent-start-v2:{logical_occurrence_digest}"
    assert lock_keys["group_value_order"] == "values inside a group sort by UTF-8 bytes"

    # positive and same-owner positive
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), _join_positive_rows()) == "accepted"
    positive_rows = _join_positive_rows()
    for row in positive_rows:
        if row["relation"] == "workflow_commands":
            assert row["command_owner"] == EXPECTED_COMMAND_SPECS[row["command_identity"]][2]
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), positive_rows) == "accepted"

    # every single-row miss fails closed
    for index in range(len(positive_rows)):
        mutated = _join_positive_rows()
        del mutated[index]
        assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found", f"miss row {index}"

    # every duplicate fails closed, including a duplicate with a different identity
    for index in range(len(positive_rows)):
        mutated = _join_positive_rows()
        mutated.append(copy.deepcopy(mutated[index]))
        assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found", (
            f"duplicate row {index}"
        )
    mutated = _join_positive_rows()
    second_intent = copy.deepcopy(mutated[3])
    second_intent["command_id"] = "cmd-intent-2"
    mutated.append(second_intent)
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"

    # foreign workspace/requester and intent-alias substitutions fail closed
    mutated = _join_positive_rows()
    mutated[0]["workspace_id"] = "ws-foreign"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[7]["workspace_id"] = "ws-foreign"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[1]["bound_requester_id"] = "req-foreign"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    intent_scope = dict(JOIN_SCOPE, provider_mode="scripted")
    assert _evaluate_source_join(spec, intent_scope, _join_positive_rows()) == "projection_not_found"
    live_scope = dict(JOIN_SCOPE, provider_mode="live")
    assert _evaluate_source_join(spec, live_scope, _join_positive_rows()) == "projection_not_found"
    empty_scope = dict(JOIN_SCOPE, workspace_id="")
    assert _evaluate_source_join(spec, empty_scope, _join_positive_rows()) == "projection_not_found"

    # split identity across alternate keys fails closed
    mutated = _join_positive_rows()
    mutated[1]["action_id"] = "act-2"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[0]["start_idempotency"] = "agent-start-v2:zzz"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[7]["metadata_source_command_id"] = "cmd-root"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"

    # wrong parent and forbidden commit-parent fallback fail closed
    mutated = _join_positive_rows()
    mutated[6]["parent_command_id"] = "cmd-root"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[4]["parent_command_id"] = "cmd-root"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"

    # malformed owner-ref payloads fail closed through the type-strict link comparison
    mutated = _join_positive_rows()
    mutated[1]["result_ref_json"] = {"owner": "other"}
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[1]["result_ref_json"] = 0
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"

    # other downstream command types in the same workflow are allowed, not duplicates
    mutated = _join_positive_rows()
    mutated.append(
        {
            "relation": "workflow_commands",
            "command_identity": "cohort_execute",
            "command_id": "cmd-cohort-execute",
            "command_type": "acquisition.cohort.execute",
            "command_owner": "cohort_provider_runtime",
            "stage_id": "acquisition_cohort_execute",
            "parent_command_id": "cmd-commit",
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
        }
    )
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "accepted"


def test_s1f0c_ff_pg_descriptors_are_typed_and_identifiers_fit_63_bytes() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    _assert_pg_descriptors_valid(manifest)

    reservation = manifest["pg_aggregate_surfaces"]["migration_reservation"]
    assert "typed PG descriptor" in reservation["descriptor_source"]
    assert "63 bytes" in reservation["descriptor_source"]
    relations = manifest["physical_relations"]
    commits = relations["cohort_execution_commits"]["descriptor"]
    commit_checks = {check["name"]: check["expression"] for check in commits["check_constraints"]}
    assert (
        commit_checks["ck_cohort_execution_commits_schema_version"] == "schema_version = 'cohort_execution_commit.v1'"
    )
    attempts_fk = relations["cohort_execution_attempts"]["descriptor"]["foreign_keys"]
    assert {fk_row["references"]["table"] for fk_row in attempts_fk} == {
        "agent_runtime_namespace_refs",
        "cohort_execution_attempts",
    }
    terminal_fk = relations["filter_projection_product_terminals"]["descriptor"]["foreign_keys"]
    assert {fk_row["references"]["table"] for fk_row in terminal_fk} == {
        "cohort_execution_commits",
        "agent_runtime_namespace_refs",
        "filter_projection_product_terminals",
    }

    # hostile: 64-byte identifiers, unknown FK targets, and deferrable FKs are rejected
    hostile = copy.deepcopy(manifest)
    hostile["physical_relations"]["cohort_execution_attempts"]["descriptor"]["indexes"][0]["name"] = "i" * 64
    with pytest.raises(AssertionError):
        _assert_pg_descriptors_valid(hostile)
    hostile = copy.deepcopy(manifest)
    hostile["physical_relations"]["cohort_execution_attempts"]["descriptor"]["foreign_keys"][0]["references"][
        "table"
    ] = "ghost_table"
    with pytest.raises(AssertionError):
        _assert_pg_descriptors_valid(hostile)
    hostile = copy.deepcopy(manifest)
    hostile["physical_relations"]["cohort_execution_attempts"]["descriptor"]["foreign_keys"][0]["deferrable"] = True
    with pytest.raises(AssertionError):
        _assert_pg_descriptors_valid(hostile)
    hostile = copy.deepcopy(manifest)
    hostile["physical_relations"]["cohort_execution_attempts"]["descriptor"]["columns"][0]["type"] = "uuid"
    with pytest.raises(AssertionError):
        _assert_pg_descriptors_valid(hostile)
    hostile = copy.deepcopy(manifest)
    hostile["pg_aggregate_surfaces"]["migration_reservation"]["rollback_order"] = list(
        reversed(hostile["pg_aggregate_surfaces"]["migration_reservation"]["rollback_order"])
    )
    with pytest.raises(AssertionError):
        _assert_pg_descriptors_valid(hostile)


def test_s1f0c_ff_design_evidence_closure_is_content_hash_bound() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    basis = manifest["obligation_basis"]
    closure = basis["design_evidence_closure"]
    assert "SHA-256" in closure["hash_algorithm"]
    assert closure["coord_files_tracked"] is False
    assert "never committed" in closure["adoption_check_rule"]

    files = closure["files"]
    expected_paths = {
        ".coord/handoffs/s1f0c-ff-di-v1.md": (969, "design source"),
        ".coord/handoffs/s1f0c-ff-jc-v1.md": (300, "wave-0 input"),
        ".coord/handoffs/s1f0c-ff-cr-v1.md": (257, "wave-0 input"),
        ".coord/handoffs/s1f0c-ff-cs-v1.md": (250, "wave-0 input"),
        ".coord/handoffs/s1f0c-ff-rs-v1.md": (244, "wave-0 input"),
        ".coord/handoffs/s1f0c-ff-fr-v1.md": (322, "wave-0 input"),
        ".coord/handoffs/s1f0c-ff-decision-v1.md": (164, "FF-DI lane decision"),
        "runtime/reviews/20260718T113429Z_Track_D_D1n_S1f0c0_lineage_terminal_owner_decision.md": (
            170,
            "controlling prior review",
        ),
    }
    assert {row["path"] for row in files} == set(expected_paths)
    for row in files:
        lines, role_prefix = expected_paths[row["path"]]
        assert row["lines"] == lines, row["path"]
        assert row["role"].startswith(role_prefix), row["path"]
        assert SHA256_RE.fullmatch(row["sha256"]), row["path"]
        assert row["tracked"] is False, row["path"]
    closure_by_path = {row["path"]: row for row in files}
    assert basis["design_source"] in closure_by_path
    for wave0 in basis["wave0_inputs"]:
        assert wave0 in closure_by_path, wave0
    assert basis["controlling_review"]["artifact"] in closure_by_path

    # the closure sources stay untracked: no .coord or review-artifact blob exists at HEAD
    head = _pinned_head()
    tracked = set(_tracked_paths_at_head(head))
    for row in files:
        assert row["path"] not in tracked, row["path"]
    assert _git_output("ls-tree", "-r", "--name-only", head, "--", ".coord") == ""


def test_s1f0c_ff_loader_and_equality_are_recursively_type_strict(tmp_path: Path) -> None:
    # scalar alias lattice: bool/int/float never alias each other
    assert not _type_strict_equal(1, True)
    assert not _type_strict_equal(True, 1)
    assert not _type_strict_equal(False, 0)
    assert not _type_strict_equal(0, False)
    assert not _type_strict_equal(1, 1.0)
    assert not _type_strict_equal(0.0, False)
    assert _type_strict_equal(1, 1)
    assert _type_strict_equal(True, True)
    assert _type_strict_equal(0, 0)
    # non-finite numbers never equal anything, including themselves
    assert not _type_strict_equal(float("nan"), float("nan"))
    assert not _type_strict_equal(float("inf"), float("inf"))
    assert not _type_strict_equal(1.0, float("nan"))
    # recursive structures compare type-strictly at every depth
    assert _type_strict_equal({"a": [1, {"b": False}]}, {"a": [1, {"b": False}]})
    assert not _type_strict_equal({"a": [1, {"b": False}]}, {"a": [1, {"b": 0}]})
    assert not _type_strict_equal({"a": [1]}, {"a": [1, 1]})
    assert not _type_strict_equal({"a": True}, {"a": True, "b": 1})

    # the loader rejects NaN/Infinity parse constants and duplicate keys
    nan_file = tmp_path / "nan.json"
    nan_file.write_text('{"a": NaN}', encoding="utf-8")
    with pytest.raises(AssertionError, match="non-JSON constant"):
        _load(nan_file)
    inf_file = tmp_path / "inf.json"
    inf_file.write_text('{"a": -Infinity}', encoding="utf-8")
    with pytest.raises(AssertionError, match="non-JSON constant"):
        _load(inf_file)
    dup_file = tmp_path / "dup.json"
    dup_file.write_text('{"a": 1, "a": 2}', encoding="utf-8")
    with pytest.raises(AssertionError, match="duplicate key"):
        _load(dup_file)

    # hostile boolean/int/float alias mutations of the real manifest are rejected
    manifest = _load(MANIFEST_PATH)
    scope = copy.deepcopy(manifest["scope"])
    scope["served_population_delta"] = False
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(scope, manifest["scope"], "scope alias")
    scope = copy.deepcopy(manifest["scope"])
    scope["decision_only"] = 1
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(scope, manifest["scope"], "scope bool alias")
    zero_rule = copy.deepcopy(manifest["product_terminal_records"]["zero_member_rule"])
    zero_rule["retryable"] = 0
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(
            zero_rule,
            {
                "outcome": "deferred not_ready",
                "reason": "projection_candidate_set_empty",
                "retryable": False,
                "reselection_required": True,
            },
            "zero_rule alias",
        )
    masked = copy.deepcopy(manifest["result_v3_slot_contract"]["masked_root_constants"])
    masked["retryable"] = 0.0
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(
            masked,
            {"variant": "error", "status": "failed", "reason": "projection_not_found", "retryable": False},
            "masked alias",
        )
    common = copy.deepcopy(manifest["result_v3_slot_contract"]["terminal_tuple_common"])
    common["command_attempt"] = 0.0
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(common["command_attempt"], 0, "tuple int alias")
    target = copy.deepcopy(manifest["result_v3_slot_contract"]["success_target"])
    target["owner_target_revision"] = True
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(target["owner_target_revision"], 1, "target revision alias")
    release = copy.deepcopy(manifest["release_boundary"])
    release["served_population"] = False
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(release["served_population"], 0, "release served alias")
    nan_manifest = copy.deepcopy(manifest["release_boundary"])
    nan_manifest["served_population"] = float("nan")
    with pytest.raises(AssertionError):
        _assert_type_strict_equal(nan_manifest["served_population"], 0, "release nan alias")


def test_s1f0c_ff_execution_commit_persists_schema_identity() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    commit_fields = manifest["execution_record_contracts"]["execution_commit"]["ordered_fields"]
    assert commit_fields[0] == "schema_version"
    assert commit_fields[-2] == "commit_contract_digest"
    assert commit_fields[-1] == "commit_digest"
    assert (
        "schema_version and commit_contract_digest"
        in manifest["execution_record_contracts"]["execution_commit"]["digest_rule"]
    )
    assert (
        "indistinguishable commit bytes"
        in manifest["execution_record_contracts"]["execution_commit"]["ordered_fields_note"]
    )

    commit_row = next(row for row in manifest["contract_digests"] if row["literal"] == "cohort_execution_commit.v1")
    schema_fields = {field["name"]: field for field in commit_row["schema"]["fields"]}
    assert schema_fields["schema_version"]["constant"] == "cohort_execution_commit.v1"
    assert schema_fields["commit_contract_digest"]["format"] == "sha256_hex"
    assert "contract_digest" in schema_fields["commit_contract_digest"]["derivation"]
    assert "schema_version and commit_contract_digest" in schema_fields["commit_digest"]["derivation"]

    relation = manifest["physical_relations"]["cohort_execution_commits"]
    columns = relation["descriptor"]["columns"]
    by_name = {column["name"]: column for column in columns}
    assert by_name["schema_version"] == {"name": "schema_version", "type": "text", "nullable": False, "default": None}
    assert by_name["commit_contract_digest"] == {
        "name": "commit_contract_digest",
        "type": "text",
        "nullable": False,
        "default": None,
    }
    checks = {check["name"]: check["expression"] for check in relation["descriptor"]["check_constraints"]}
    assert checks["ck_cohort_execution_commits_schema_version"] == "schema_version = 'cohort_execution_commit.v1'"
    assert (
        checks["ck_cohort_execution_commits_commit_contract_digest_hex"] == "commit_contract_digest ~ '^[0-9a-f]{64}$'"
    )
    # the physical columns mirror the record fields plus created_at
    assert [column["name"] for column in columns if column["name"] != "created_at"] == commit_fields

    # hostile: dropping schema identity from the record moves the contract digest
    mutated = copy.deepcopy(commit_row["schema"])
    mutated["fields"] = [field for field in mutated["fields"] if field["name"] != "commit_contract_digest"]
    assert _contract_digest(mutated) != commit_row["contract_digest"]
