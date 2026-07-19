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

from sourcing_agent.acquisition_plan_preview import (
    _PREVIEW_RECORD_TOOL_SPEC,
    ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
    ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE,
    build_acquisition_plan_preview,
)
from sourcing_agent.action_result_schema import (
    ACTION_RESULT_INTERPRETATION_CONTRACT_DIGEST,
    ACTION_RESULT_VALIDATOR_OWNER,
)
from sourcing_agent.cohort_selection import COHORT_SELECTION_REGISTRY_VERSION
from sourcing_agent.model_tool_runtime import ModelToolSchemaError

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
    "retained_contract_pins",
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
    "min_items",
    "max_items",
    "max_length",
    "items",
    "fields",
    "ref",
    "ref_digest",
    "provenance",
    "value_role",
    "derivation",
    "variants",
    "ordering",
}
DESCRIPTOR_TYPES = {"string", "integer", "boolean", "object", "array"}
DESCRIPTOR_FORMATS = {"sha256_hex", "https_url"}
DESCRIPTOR_PROVENANCE = {"server_derived", "user_supplied", "owner_state"}
DESCRIPTOR_VALUE_ROLES = {"control", "identifier", "display_text", "web_url"}
RESULT_VARIANTS = {"success", "deferred", "error"}

RETAINED_PIN_LITERALS = {
    "acquisition_root_command_payload.v2",
    "acquisition_start_command_acceptance_owner_result_ref.v1",
}
# External retained specs are pinned by their exact live schema digest, recomputed
# from the pinned source at the canonization base: this value equals
# acquisition_plan_preview._PREVIEW_RECORD_TOOL_SPEC.input_schema_digest.
EXTERNAL_RETAINED_SPEC_REFS = {
    "acquisition_plan_preview_record_v2": "dace854d5ef4b31e545d983c3fedaf36eee1ea933567daf3f8c0d254a8fcd187",
}
NON_CONSTRUCTION_REFS = RETAINED_PIN_LITERALS | set(EXTERNAL_RETAINED_SPEC_REFS)

ENFORCEMENT_SOURCES = {"prose", "schema", "physical", "sql_target"}
REPOSITORY_MECHANISMS = {"transition_guard", "uow_rule", "reader_boundary", "crash_recovery"}
PREDICATE_KINDS = {
    "row_field_eq",
    "path_eq",
    "path_constant",
    "path_formula",
    "schema_valid",
    "digest_recompute",
    "fields_valid",
    "rows_equal",
    "deterministic_command_id",
}

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
    declared_type = descriptor.get("type")
    assert declared_type in DESCRIPTOR_TYPES, label
    if "nonempty" in descriptor:
        assert declared_type == "string", label
        assert descriptor["nonempty"] is True, label
    if "format" in descriptor:
        assert declared_type == "string", label
        assert descriptor["format"] in DESCRIPTOR_FORMATS, label
    for bound in ("minimum", "maximum"):
        if bound in descriptor:
            assert declared_type == "integer", label
            assert type(descriptor[bound]) is int, label  # rejects bool and float aliases
    if "minimum" in descriptor and "maximum" in descriptor:
        assert descriptor["minimum"] <= descriptor["maximum"], label
    for bound in ("min_items", "max_items"):
        if bound in descriptor:
            assert declared_type == "array", label
            assert type(descriptor[bound]) is int, label  # rejects bool and float aliases
            assert descriptor[bound] >= 0, label
    if "min_items" in descriptor and "max_items" in descriptor:
        assert descriptor["min_items"] <= descriptor["max_items"], label
    if "max_length" in descriptor:
        assert declared_type == "string", label
        assert type(descriptor["max_length"]) is int, label  # rejects bool and float aliases
        assert descriptor["max_length"] >= 1, label
    if "enum" in descriptor:
        assert declared_type == "string", label
        assert type(descriptor["enum"]) is list and descriptor["enum"], label
        assert all(type(value) is str for value in descriptor["enum"]), label
    if "constant" in descriptor:
        # exact constant type against the declared descriptor type: 0 is not False,
        # True is not 1, and a string constant is never an integer.
        assert declared_type in ("string", "integer", "boolean"), label
        expected_python_type = {"string": str, "integer": int, "boolean": bool}[declared_type]
        assert type(descriptor["constant"]) is expected_python_type, label
    if "variants" in descriptor:
        assert type(descriptor["variants"]) is list and descriptor["variants"], label
        assert set(descriptor["variants"]) <= RESULT_VARIANTS, label
    if "ref" in descriptor:
        assert declared_type == "object", label
        assert type(descriptor["ref"]) is str and descriptor["ref"], label
    if "ref_digest" in descriptor:
        assert "ref" in descriptor, label
        assert type(descriptor["ref_digest"]) is str and SHA256_RE.fullmatch(descriptor["ref_digest"]), label
    if "provenance" in descriptor:
        assert descriptor["provenance"] in DESCRIPTOR_PROVENANCE, label
    if "value_role" in descriptor:
        assert declared_type == "string", label
        assert descriptor["value_role"] in DESCRIPTOR_VALUE_ROLES, label
    if "derivation" in descriptor:
        assert type(descriptor["derivation"]) is str and descriptor["derivation"], label
    if "ordering" in descriptor:
        assert type(descriptor["ordering"]) is str and descriptor["ordering"], label
    if "items" in descriptor:
        assert descriptor["type"] == "array", label
        _validate_descriptor(descriptor["items"], f"{label}.items", named=False)
    if "fields" in descriptor:
        assert descriptor["type"] == "object", label
        assert "ref" not in descriptor, label
        assert type(descriptor["fields"]) is list, label
        for index, sub in enumerate(descriptor["fields"]):
            _validate_descriptor(sub, f"{label}.fields[{index}]", named=True)


def _validate_join_predicate(
    predicate: object,
    label: str,
    row_names: set[str],
    predicate_ids: set[str],
) -> None:
    """Validate one machine-encoded join predicate against its closed kind vocabulary."""

    def _path(value: object, path_label: str) -> list[str]:
        assert type(value) is list and value, path_label
        assert all(type(part) is str and part for part in value), path_label
        return value

    def _endpoint(value: object, endpoint_label: str, *, allow_scope: bool) -> None:
        assert type(value) is dict, endpoint_label
        if allow_scope and set(value) == {"scope"}:
            assert type(value["scope"]) is str and value["scope"], endpoint_label
            return
        record = _exact_keys(value, {"row", "path"}, endpoint_label)
        assert record["row"] in row_names, f"{endpoint_label}: unknown row {record['row']}"
        _path(record["path"], endpoint_label)

    assert type(predicate) is dict, label
    assert "id" in predicate, label
    assert type(predicate["id"]) is str and predicate["id"], label
    assert predicate["id"] not in predicate_ids, f"{label}: duplicate predicate id"
    predicate_ids.add(predicate["id"])
    kind = predicate.get("kind")
    assert kind in PREDICATE_KINDS, f"{label}: unknown predicate kind {kind}"
    if kind == "row_field_eq":
        record = _exact_keys(predicate, {"id", "kind", "row", "field", "other_row", "other_field"}, label)
        assert record["row"] in row_names and record["other_row"] in row_names, label
        assert type(record["field"]) is str and record["field"], label
        assert type(record["other_field"]) is str and record["other_field"], label
    elif kind == "path_eq":
        record = _exact_keys(predicate, {"id", "kind", "left", "right"}, label)
        _endpoint(record["left"], f"{label}.left", allow_scope=False)
        _endpoint(record["right"], f"{label}.right", allow_scope=True)
    elif kind == "path_constant":
        record = _exact_keys(predicate, {"id", "kind", "row", "path", "constant"}, label)
        assert record["row"] in row_names, label
        _path(record["path"], label)
        assert type(record["constant"]) in (str, int, bool), label
    elif kind == "path_formula":
        record = _exact_keys(predicate, {"id", "kind", "row", "path", "template", "binds"}, label)
        assert record["row"] in row_names, label
        _path(record["path"], label)
        assert type(record["template"]) is str and record["template"], label
        assert type(record["binds"]) is dict and record["binds"], label
        for name, endpoint in record["binds"].items():
            assert type(name) is str and name, label
            assert f"{{{name}}}" in record["template"], f"{label}: unbound formula name {name}"
            _endpoint(endpoint, f"{label}.binds.{name}", allow_scope=True)
    elif kind == "schema_valid":
        record = _exact_keys(predicate, {"id", "kind", "row", "path", "ref"}, label)
        assert record["row"] in row_names, label
        _path(record["path"], label)
        assert type(record["ref"]) is str and record["ref"], label
    elif kind == "digest_recompute":
        record = _exact_keys(predicate, {"id", "kind", "row", "path", "source", "exclude"}, label)
        assert record["row"] in row_names, label
        _path(record["path"], label)
        _endpoint(record["source"], f"{label}.source", allow_scope=False)
        assert type(record["exclude"]) is list, label
        assert all(type(field) is str and field for field in record["exclude"]), label
    elif kind == "fields_valid":
        record = _exact_keys(predicate, {"id", "kind", "row", "fields"}, label)
        assert record["row"] in row_names, label
        assert type(record["fields"]) is list and record["fields"], label
        for field_index, field in enumerate(record["fields"]):
            _validate_descriptor(field, f"{label}.fields[{field_index}]", named=True)
    elif kind == "rows_equal":
        record = _exact_keys(predicate, {"id", "kind", "rows", "path"}, label)
        assert type(record["rows"]) is list and len(record["rows"]) >= 2, label
        assert all(row in row_names for row in record["rows"]), label
        _path(record["path"], label)
    elif kind == "deterministic_command_id":
        record = _exact_keys(
            predicate,
            {"id", "kind", "row", "command_id_field", "workflow_run_id_field", "idempotency_field", "formula"},
            label,
        )
        assert record["row"] in row_names, label
        for key in ("command_id_field", "workflow_run_id_field", "idempotency_field"):
            assert type(record[key]) is str and record[key], label
        assert record["formula"].startswith('command_id == "cmd_" + sha1('), label


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
    scope = _exact_keys(
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
    assert type(scope["decision_only"]) is bool, "scope.decision_only"
    for numeric in (
        "ddl_delta",
        "runtime_write_delta",
        "provider_invocation_delta",
        "model_invocation_delta",
        "served_population_delta",
    ):
        assert type(scope[numeric]) is int, f"scope.{numeric}"  # rejects bool and float aliases
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
    digested_literals = {row["literal"] for row in manifest["contract_digests"]}
    pin_rows = manifest["retained_contract_pins"]
    assert type(pin_rows) is list and len(pin_rows) == 2, "retained_contract_pins"
    pin_digests: dict[str, str] = {}
    for index, row in enumerate(pin_rows):
        record = _exact_keys(row, {"literal", "retention", "schema", "pin_digest"}, f"retained_contract_pins[{index}]")
        assert record["literal"] in RETAINED_PIN_LITERALS, record["literal"]
        assert record["literal"] not in digested_literals, record["literal"]
        assert type(record["retention"]) is str and record["retention"], record["literal"]
        _validate_schema_object(record["schema"], record["literal"], record["schema"]["owner"])
        pin_digests[record["literal"]] = record["pin_digest"]
    assert set(pin_digests) == RETAINED_PIN_LITERALS
    # every ref is an immutable exact-version-and-digest reference: adopted contracts
    # bind the live contract digest, retained pins bind the pin digest, and external
    # retained specs bind the closed pinned constant
    all_schemas = [row["schema"] for row in manifest["contract_digests"]]
    all_schemas.extend(row["schema"] for row in pin_rows)
    digest_by_literal = {row["literal"]: row["contract_digest"] for row in manifest["contract_digests"]}

    def _check_ref(descriptor: dict[str, Any], label: str) -> None:
        ref = descriptor.get("ref")
        if ref is not None:
            assert "ref_digest" in descriptor, f"{label}: ref without ref_digest"
            if ref in digest_by_literal:
                expected = digest_by_literal[ref]
            elif ref in pin_digests:
                expected = pin_digests[ref]
            else:
                assert ref in EXTERNAL_RETAINED_SPEC_REFS, f"{label}: unknown ref {ref}"
                expected = EXTERNAL_RETAINED_SPEC_REFS[ref]
            assert descriptor["ref_digest"] == expected, f"{label}: stale ref_digest for {ref}"
        for sub in descriptor.get("fields", []):
            _check_ref(sub, label)
        items = descriptor.get("items")
        if isinstance(items, dict):
            _check_ref(items, label)

    for schema in all_schemas:
        for field in schema["fields"]:
            _check_ref(field, schema["schema_version"])
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
    assert type(surfaces["migration_reservation"]["slot"]) is int, "migration_reservation.slot"
    assert type(surfaces["migration_reservation"]["predecessor_slot"]) is int, "migration_reservation.predecessor_slot"
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
    join_row_names: set[str] = set()
    pending_predicates: list[tuple[str, dict[str, Any]]] = []
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
                "predicates",
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
        assert type(join_row["ordinal"]) is int, f"source_join.rows[{index}].ordinal"
        assert type(join_row["cardinality"]) is int, f"source_join.rows[{index}].cardinality"
        row_name = join_row["relation"] if join_row["row_role"] == "base" else join_row["command_identity"]
        assert type(row_name) is str and row_name, f"source_join.rows[{index}]"
        join_row_names.add(row_name)
        assert type(join_row["alternate_keys"]) is list and join_row["alternate_keys"], f"source_join.rows[{index}]"
        for ak_index, alternate_key in enumerate(join_row["alternate_keys"]):
            ak_row = _exact_keys(
                alternate_key,
                {"fields", "source"},
                f"source_join.rows[{index}].alternate_keys[{ak_index}]",
            )
            assert type(ak_row["fields"]) is list and ak_row["fields"], f"source_join.rows[{index}]"
            assert all(type(field) is str and field for field in ak_row["fields"]), f"source_join.rows[{index}]"
            assert type(ak_row["source"]) is str and ak_row["source"], f"source_join.rows[{index}]"
        for link_index, link in enumerate(join_row["links"]):
            _exact_keys(
                link,
                {"local_field", "remote_row", "remote_field"},
                f"source_join.rows[{index}].links[{link_index}]",
            )
        assert type(join_row["predicates"]) is list and join_row["predicates"], f"source_join.rows[{index}]"
        for pred_index, predicate in enumerate(join_row["predicates"]):
            pending_predicates.append((f"source_join.rows[{index}].predicates[{pred_index}]", predicate))
    predicate_ids: set[str] = set()
    for label, predicate in pending_predicates:
        _validate_join_predicate(predicate, label, join_row_names, predicate_ids)
    _exact_keys(
        join["command_chain"],
        {"identities", "parent_predicates", "same_group_rule", "cardinality_rule", "commit_parent_fallback"},
        "source_join.command_chain",
    )
    for index, row in enumerate(join["requester_bindings"]):
        binding = _exact_keys(
            row,
            {"field", "source_of_truth", "required_comparisons", "forbidden_substitutions", "predicate_ids"},
            f"source_join.requester_bindings[{index}]",
        )
        assert type(binding["predicate_ids"]) is list and binding["predicate_ids"], (
            f"source_join.requester_bindings[{index}].predicate_ids"
        )
        assert all(type(predicate_id) is str and predicate_id for predicate_id in binding["predicate_ids"]), (
            f"source_join.requester_bindings[{index}].predicate_ids"
        )
        # every declared comparison predicate is one of the encoded executed predicates
        for predicate_id in binding["predicate_ids"]:
            assert predicate_id in predicate_ids, (
                f"source_join.requester_bindings[{index}]: unknown predicate id {predicate_id}"
            )
    lock_keys = _exact_keys(
        join["lock_keys"],
        {"advisory_groups", "row_lock_order", "group_value_order", "start_idempotency_formula", "extension_rule"},
        "source_join.lock_keys",
    )
    for index, row in enumerate(lock_keys["advisory_groups"]):
        advisory_group = _exact_keys(row, {"order", "keys"}, f"source_join.lock_keys.advisory_groups[{index}]")
        assert type(advisory_group["order"]) is int, f"source_join.lock_keys.advisory_groups[{index}].order"
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
    descriptor_mechanisms: dict[str, set[str]] = {}
    for name, row in relations.items():
        relation = _exact_keys(
            row,
            {"owner", "columns", "constraints", "descriptor", "invariant_enforcement"},
            f"physical_relations.{name}",
        )
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
            index_record = _exact_keys(
                index_row,
                {"name", "columns", "unique"} | ({"where"} if "where" in index_row else set()),
                f"{name}.descriptor.indexes[{ix_index}]",
            )
            assert type(index_record["unique"]) is bool, f"{name}.descriptor.indexes[{ix_index}]"
            if "where" in index_record:
                assert type(index_record["where"]) is str and index_record["where"], (
                    f"{name}.descriptor.indexes[{ix_index}].where"
                )
        descriptor_mechanisms[name] = (
            {descriptor["primary_key"]["name"]}
            | {group["name"] for group in descriptor["unique_constraints"]}
            | {check["name"] for check in descriptor["check_constraints"]}
            | {foreign_key["name"] for foreign_key in descriptor["foreign_keys"]}
            | {index_row["name"] for index_row in descriptor["indexes"]}
        )
    all_mechanisms = set().union(*descriptor_mechanisms.values())
    for name, row in relations.items():
        enforcement_rows = row["invariant_enforcement"]
        assert type(enforcement_rows) is list and enforcement_rows, name
        for row_index, enforcement_row in enumerate(enforcement_rows):
            label = f"physical_relations.{name}.invariant_enforcement[{row_index}]"
            record = _exact_keys(
                enforcement_row,
                {"invariant", "source", "enforcement", "mechanism"}
                | ({"covers_fields"} if "covers_fields" in enforcement_row else set()),
                label,
            )
            assert type(record["invariant"]) is str and record["invariant"], label
            assert record["source"] in ENFORCEMENT_SOURCES, label
            assert record["enforcement"] in ("sql", "repository"), label
            assert type(record["mechanism"]) is list and record["mechanism"], label
            assert all(type(mechanism) is str and mechanism for mechanism in record["mechanism"]), label
            if record["enforcement"] == "sql":
                for mechanism in record["mechanism"]:
                    assert mechanism in all_mechanisms, f"{label}: unknown sql mechanism {mechanism}"
            else:
                for mechanism in record["mechanism"]:
                    assert mechanism in REPOSITORY_MECHANISMS, f"{label}: unknown repository mechanism {mechanism}"
            if "covers_fields" in record:
                assert type(record["covers_fields"]) is list and record["covers_fields"], label
                assert all(type(field) is str and field for field in record["covers_fields"]), label
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
        lock_group = _exact_keys(
            row,
            {"order", "lock_group", "used_by", "first_write_after"},
            f"lock_order.groups[{index}]",
        )
        assert type(lock_group["order"]) is int, f"lock_order.groups[{index}].order"
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
    allowed_waves = {1, 2, 3}
    batch_packets: set[str] = set()
    batch_waves: dict[str, int] = {}
    batch_dependencies: dict[str, list[str]] = {}
    for index, row in enumerate(manifest["implementation_batches"]):
        batch = _exact_keys(
            row,
            {"packet", "wave", "depends_on", "exclusive_write_paths", "review_edge", "promotion_requires_review"},
            f"implementation_batches[{index}]",
        )
        label = f"implementation_batches[{index}]"
        assert type(batch["packet"]) is str and batch["packet"], label
        assert batch["packet"] not in batch_packets, f"{label}: duplicate packet {batch['packet']}"
        batch_packets.add(batch["packet"])
        # exact integer wave inside the closed allowed set: True/1.0/4 are hostile aliases
        assert type(batch["wave"]) is int, f"{label}.wave"
        assert batch["wave"] in allowed_waves, f"{label}.wave"
        batch_waves[batch["packet"]] = batch["wave"]
        assert type(batch["depends_on"]) is list, f"{label}.depends_on"
        assert all(type(dep) is str and dep for dep in batch["depends_on"]), f"{label}.depends_on"
        assert len(set(batch["depends_on"])) == len(batch["depends_on"]), f"{label}.depends_on"
        assert type(batch["promotion_requires_review"]) is bool, f"{label}.promotion_requires_review"
        batch_dependencies[batch["packet"]] = list(batch["depends_on"])
    assert batch_packets, "implementation_batches"
    for packet, dependencies in batch_dependencies.items():
        for dep in dependencies:
            if dep == "FF-DI-decision-GO":
                continue
            assert dep in batch_waves, f"{packet}: unknown dependency {dep}"
            assert batch_waves[dep] <= batch_waves[packet], (
                f"{packet}: dependency {dep} sits in a later wave than its dependent"
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
    release = _exact_keys(
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
    for numeric in (
        "served_population",
        "provider_invocations",
        "model_invocations",
        "live_invocations",
        "migration_delta",
    ):
        assert type(release[numeric]) is int, f"release_boundary.{numeric}"  # rejects bool and float aliases
    assert type(release["author_evidence_only"]) is bool, "release_boundary.author_evidence_only"
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
    if "ref" in descriptor and descriptor["ref"] not in NON_CONSTRUCTION_REFS:
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


def _sha256_json(value: Any) -> str:
    blob = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _command_id(workflow_run_id: str, idempotency_key: str) -> str:
    return "cmd_" + hashlib.sha1(f"{workflow_run_id}:{idempotency_key}".encode("utf-8")).hexdigest()[:24]


def _preview_fixture_record() -> dict[str, Any]:
    """Build one canonical preview record with the exact digest-pinned owner builder.

    The record is bound to the join scope (ws-1/req-1) so the positive fixture
    exercises the same tenant/requester boundary the join predicates enforce.
    """

    registry_digest = hashlib.sha256(b"company-registry-v1").hexdigest()
    start_request_digest = hashlib.sha256(b"acquisition-root-request-v2").hexdigest()
    input_payload = {
        "cohort_selection": {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research", "engineering"],
            "employment_statuses": ["current", "former"],
            "role_match": "any",
            "source": "user_explicit",
        },
        "source_preferences": [ACQUISITION_PLAN_PREVIEW_SOURCE_PREFERENCE],
        "coverage_intent": ACQUISITION_PLAN_PREVIEW_COVERAGE_INTENT,
        "thematic_constraints": ["Pre-training"],
        "provider_mode_intent": "simulate",
        "budget": {
            "max_provider_calls": 4,
            "max_provider_items": 20,
            "max_output_candidates": 10,
            "max_cost_micro_usd": 2_000_000,
            "max_elapsed_seconds": 900,
        },
    }
    target_ref = {
        "workspace_id": "ws-1",
        "requester_id": "req-1",
        "company_target": {
            "canonical_company_id": "thinkingmachineslab",
            "canonical_name": "Thinking Machines Lab",
            "company_registry_revision": "company_registry.v1",
            "company_registry_digest": registry_digest,
            "provider_company_labels": ["Thinking Machines Lab", "thinkingmachinesai"],
        },
    }
    preview = build_acquisition_plan_preview(
        input_payload=input_payload,
        target_ref=target_ref,
        preview_id="pv-1",
        preview_revision=1,
        created_at="2026-07-17T00:00:00Z",
        expires_at="2026-07-17T01:00:00Z",
        intended_start_request_schema_version="acquisition_root_request_v2",
        intended_start_request_schema_digest=start_request_digest,
    )
    record = preview.to_record()
    assert _validate_external_retained_spec("acquisition_plan_preview_record_v2", record)
    return record


def _join_fixture() -> dict[str, Any]:
    """Deterministic positive fixture: every digest/idempotency/command id is recomputed."""

    receipt = {"receipt_id": "rcpt-1", "receipt_digest": "a" * 64}
    occurrence = {"result_slot_id": "slot-1", "slot_generation": 3, "logical_occurrence_digest": "b" * 64}
    snapshot = {
        "schema_version": "acquisition_start_snapshot.v2",
        "preview": _preview_fixture_record(),
        "request_pins": {"schema_version": "acquisition_root_request_v2", "schema_digest": "c" * 64},
        "result_pins": {
            "schema_version": "acquisition_start_result_v2",
            "schema_digest": "d" * 64,
            "serializer_owner": "acquisition.start_result_serializer_v2",
            "serializer_revision": "acquisition_start_result_serializer_v2",
            "serializer_contract_digest": "e" * 64,
            "interpretation_contract_version": "action_result_interpretation_contract_v3",
            "interpretation_contract_digest": "f" * 64,
        },
        "tool_pins": {"tool_spec_version": "v2.1", "tool_spec_digest": "0" * 64},
    }
    snapshot["snapshot_digest"] = _sha256_json(snapshot)
    payload = {
        "schema_version": "acquisition_root_command_payload.v2",
        "command_type": "acquisition.run.create",
        "action_id": "act-1",
        "operation_run_id": "op-1",
        "workflow_run_id": "wf-1",
        "confirmation_receipt_ref": copy.deepcopy(receipt),
        "start_snapshot": copy.deepcopy(snapshot),
        "start_snapshot_digest": snapshot["snapshot_digest"],
    }
    payload["payload_digest"] = _sha256_json(payload)
    root_idempotency = f"acquisition.run.create:start-v2:{receipt['receipt_digest']}"
    root_command_id = _command_id("wf-1", root_idempotency)
    owner_ref = {
        "schema_version": "acquisition_start_command_acceptance_owner_result_ref.v1",
        "runtime_namespace": "ns-1",
        "provider_mode": "simulate",
        "workspace_id": "ws-1",
        "action_id": "act-1",
        "operation_run_id": "op-1",
        "workflow_run_id": "wf-1",
        "workflow_command_id": root_command_id,
        "terminal_winner_id": "winner-1",
        "terminal_winner_sequence_number": 1,
        "command_source_event_id": "evt-src-root",
        "command_source_event_sequence_number": 2,
        "command_source_event_contract_digest": "1" * 64,
        "confirmation_receipt_ref": copy.deepcopy(receipt),
        "parent_budget_envelope_ref": {
            "owner_id": "budget-owner-1",
            "owner_revision": "rev-1",
            "owner_contract_digest": "2" * 64,
            "confirmation_receipt_id": "rcpt-1",
            "confirmation_receipt_digest": receipt["receipt_digest"],
            "budget_digest": "3" * 64,
        },
        "start_snapshot_digest": snapshot["snapshot_digest"],
        "root_command_payload_digest": payload["payload_digest"],
        "result_occurrence_ref": copy.deepcopy(occurrence),
    }
    carrier = {
        "schema_version": "acquisition_start_authority_carrier.v1",
        "root_command_payload": copy.deepcopy(payload),
        "root_command_payload_digest": payload["payload_digest"],
        "command_acceptance_owner_ref": copy.deepcopy(owner_ref),
        "command_acceptance_owner_ref_digest": _sha256_json(owner_ref),
    }
    carrier["carrier_digest"] = _sha256_json(carrier)
    bundle = {
        "schema_version": "acquisition_execution_authority.v1",
        "owner": "acquisition_planner",
        "workspace_id": "ws-1",
        "operation_run_id": "op-1",
        "workflow_run_id": "wf-1",
        "plan_id": "plan-1",
        "plan_review_id": 11,
        "request_digest": "4" * 64,
        "plan_body_digest": "5" * 64,
        "start_authority_carrier": copy.deepcopy(carrier),
    }
    bundle["authority_digest"] = _sha256_json(bundle)

    start_idempotency = f"agent-start-v2:{occurrence['logical_occurrence_digest']}"
    rows: list[dict[str, Any]] = [
        {
            "relation": "operation_runs",
            "operation_run_id": "op-1",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "workspace_id": "ws-1",
            "action_id": "act-1",
            "start_idempotency": start_idempotency,
            "result_ref_json": copy.deepcopy(owner_ref),
        },
        {
            "relation": "agent_actions",
            "action_id": "act-1",
            "action_type": "start_acquisition_run",
            "owner_module": "acquisition_run_writer",
            "operation_type": "acquisition_run",
            "workspace_id": "ws-1",
            "start_idempotency": start_idempotency,
            "result_ref_json": copy.deepcopy(owner_ref),
            "bound_requester_id": "req-1",
            "status": "queued",
            "approval_status": "approved",
        },
    ]
    command_specs = [
        (
            "root",
            "acquisition.run.create",
            "acquisition_run_writer",
            "acquisition_run_create",
            "",
            root_idempotency,
            "evt-src-root",
        ),
        (
            "intent",
            "acquisition.intent.resolve",
            "acquisition_planner",
            "acquisition_intent_resolve",
            None,
            "acquisition.intent.resolve:wf-1:op-1",
            "evt-src-intent",
        ),
        (
            "build",
            "acquisition.plan.build",
            "acquisition_planner",
            "acquisition_plan_build",
            None,
            "acquisition.plan.build:wf-1:op-1",
            "evt-src-build",
        ),
        (
            "review",
            "acquisition.plan_review.request",
            "acquisition_planner",
            "acquisition_plan_review_request",
            None,
            "acquisition.plan_review.request:wf-1:op-1",
            "evt-src-review",
        ),
        (
            "commit",
            "acquisition.plan.commit",
            "acquisition_planner",
            "acquisition_plan_commit",
            None,
            "acquisition.plan.commit:wf-1:op-1",
            "evt-src-commit",
        ),
    ]
    command_ids: dict[str, str] = {}
    for identity, command_type, owner, stage_id, _parent, idempotency, source_event in command_specs:
        command_ids[identity] = _command_id("wf-1", idempotency)
    parents = {
        "root": "",
        "intent": command_ids["root"],
        "build": command_ids["intent"],
        "review": command_ids["build"],
        "commit": command_ids["review"],
    }
    for identity, command_type, owner, stage_id, _parent, idempotency, source_event in command_specs:
        row: dict[str, Any] = {
            "relation": "workflow_commands",
            "command_identity": identity,
            "command_id": command_ids[identity],
            "command_type": command_type,
            "command_owner": owner,
            "stage_id": stage_id,
            "parent_command_id": parents[identity],
            "workflow_run_id": "wf-1",
            "operation_id": "op-1",
            "idempotency_key": idempotency,
            "causal_group_id": "cg-1",
            "source_event_id": source_event,
            "source_event_type": "CommandPlanRequested",
            "command_payload_causality": {
                "causal_group_id": "cg-1",
                "source_event_id": source_event,
                "source_event_type": "CommandPlanRequested",
            },
        }
        if identity == "root":
            row["payload_json"] = copy.deepcopy(payload)
        else:
            row["carrier_json"] = copy.deepcopy(carrier)
        if identity in ("build", "review", "commit"):
            row["plan_id"] = "plan-1"
        if identity in ("review", "commit"):
            row["plan_review_id"] = 11
        rows.append(row)
    rows.append(
        {
            "relation": "acquisition_runs",
            "acquisition_run_id": "run-1",
            "workspace_id": "ws-1",
            "operation_run_id": "op-1",
            "workflow_run_id": "wf-1",
            "metadata_source_command_id": command_ids["commit"],
            "idempotency_key": f"acquisition_run:plan_commit:{command_ids['commit']}",
            "plan_id": "plan-1",
            "plan_review_id": 11,
            "execution_bundle_json": copy.deepcopy(bundle),
        }
    )
    return {"rows": rows, "command_ids": command_ids}


def _join_positive_rows() -> list[dict[str, Any]]:
    return _join_fixture()["rows"]


def _rebind_preview_digest(preview: dict[str, Any]) -> None:
    """Recompute a mutated preview's own digests under the owner's placeholder rule."""

    candidate = {key: value for key, value in preview.items() if key != "preview_digest"}
    candidate["confirmation"]["preview_digest"] = "0" * 64
    digest = _sha256_json(candidate)
    preview["preview_digest"] = digest
    preview["confirmation"]["preview_digest"] = digest


def _rebind_join_chain(rows: list[dict[str, Any]]) -> None:
    """Recompute the dependent snapshot/payload/owner-ref/carrier/bundle digest chain in place.

    The root row's payload_json is the canonical payload and the Action row's
    result_ref_json is the canonical owner ref; every copy and digest derives
    from them exactly as the positive fixture builds them.
    """

    by_relation: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = row.get("command_identity") or row["relation"]
        by_relation[key] = row
    payload = by_relation["root"]["payload_json"]
    snapshot = payload["start_snapshot"]
    snapshot["snapshot_digest"] = _sha256_json({key: value for key, value in snapshot.items() if key != "snapshot_digest"})
    payload["start_snapshot_digest"] = snapshot["snapshot_digest"]
    payload["payload_digest"] = _sha256_json({key: value for key, value in payload.items() if key != "payload_digest"})
    owner_ref = by_relation["agent_actions"]["result_ref_json"]
    owner_ref["start_snapshot_digest"] = snapshot["snapshot_digest"]
    owner_ref["root_command_payload_digest"] = payload["payload_digest"]
    owner_ref_digest = _sha256_json(owner_ref)
    by_relation["operation_runs"]["result_ref_json"] = copy.deepcopy(owner_ref)
    carriers: dict[str, Any] = {}
    for identity in ("intent", "build", "review", "commit"):
        carrier = by_relation[identity]["carrier_json"]
        carrier["root_command_payload"] = copy.deepcopy(payload)
        carrier["root_command_payload_digest"] = payload["payload_digest"]
        carrier["command_acceptance_owner_ref"] = copy.deepcopy(owner_ref)
        carrier["command_acceptance_owner_ref_digest"] = owner_ref_digest
        carrier["carrier_digest"] = _sha256_json(
            {key: value for key, value in carrier.items() if key != "carrier_digest"}
        )
        carriers[identity] = carrier
    bundle = by_relation["acquisition_runs"]["execution_bundle_json"]
    bundle["start_authority_carrier"] = copy.deepcopy(carriers["intent"])
    bundle["authority_digest"] = _sha256_json(
        {key: value for key, value in bundle.items() if key != "authority_digest"}
    )


def _resolve_path(row: dict[str, Any], path: list[str]) -> Any:
    current: Any = row
    for part in path:
        if type(current) is not dict or part not in current:
            return _MISSING
        current = current[part]
    return current


class _Missing:
    pass


_MISSING = _Missing()


def _validate_external_retained_spec(ref: str, value: Any) -> bool:
    """Execute the exact digest-pinned external retained validator against one value.

    The executed validator is pinned by its live schema digest: if the runtime
    schema drifts from the pinned digest this oracle fails loudly instead of
    silently certifying against a different contract.
    """

    if ref == "acquisition_plan_preview_record_v2":
        assert _PREVIEW_RECORD_TOOL_SPEC.input_schema_digest == EXTERNAL_RETAINED_SPEC_REFS[ref], (
            "external preview validator drifted from the pinned schema digest"
        )
        try:
            _PREVIEW_RECORD_TOOL_SPEC.validate_input(value)
        except ModelToolSchemaError:
            return False
        return True
    raise AssertionError(f"unknown external retained spec {ref}")


def _validate_value_against_descriptor(value: Any, descriptor: dict[str, Any], schemas: dict[str, Any]) -> bool:
    """Execute one manifest descriptor against a concrete value, recursively closed."""

    declared = descriptor["type"]
    if declared == "string":
        if type(value) is not str:
            return False
        if descriptor.get("nonempty") and not value:
            return False
        if "max_length" in descriptor and len(value) > descriptor["max_length"]:
            return False
        if "enum" in descriptor and value not in descriptor["enum"]:
            return False
        if descriptor.get("format") == "sha256_hex" and not SHA256_RE.fullmatch(value):
            return False
        if descriptor.get("format") == "https_url" and not (
            value.startswith("https://") and not any(character.isspace() for character in value)
        ):
            return False
    elif declared == "integer":
        if type(value) is not int:
            return False
        if "minimum" in descriptor and value < descriptor["minimum"]:
            return False
        if "maximum" in descriptor and value > descriptor["maximum"]:
            return False
    elif declared == "boolean":
        if type(value) is not bool:
            return False
    elif declared == "object":
        if type(value) is not dict:
            return False
        if "ref" in descriptor:
            ref = descriptor["ref"]
            if ref in EXTERNAL_RETAINED_SPEC_REFS:
                return _validate_external_retained_spec(ref, value)
            schema = schemas.get(ref)
            if schema is None:
                return False
            return _validate_object_against_fields(value, schema["fields"], schemas)
        fields = descriptor.get("fields")
        if fields is None:
            return False
        return _validate_object_against_fields(value, fields, schemas)
    elif declared == "array":
        if type(value) is not list:
            return False
        if "min_items" in descriptor and len(value) < descriptor["min_items"]:
            return False
        if "max_items" in descriptor and len(value) > descriptor["max_items"]:
            return False
        items = descriptor.get("items")
        if not isinstance(items, dict):
            return False
        return all(_validate_value_against_descriptor(item, items, schemas) for item in value)
    if "constant" in descriptor:
        return _type_strict_equal(value, descriptor["constant"])
    return True


def _validate_object_against_fields(
    value: dict[str, Any], fields: list[dict[str, Any]], schemas: dict[str, Any]
) -> bool:
    declared = {field["name"]: field for field in fields}
    required = {name for name, field in declared.items() if field.get("required") is True}
    if not required <= set(value):
        return False
    if not set(value) <= set(declared):
        return False
    return all(_validate_value_against_descriptor(value[name], declared[name], schemas) for name in value)


def _manifest_schemas(manifest: dict[str, Any]) -> dict[str, Any]:
    schemas = {row["literal"]: row["schema"] for row in manifest["contract_digests"]}
    schemas.update({row["literal"]: row["schema"] for row in manifest["retained_contract_pins"]})
    return schemas


def _execute_predicate(
    predicate: dict[str, Any],
    selected: dict[str, dict[str, Any]],
    scope: dict[str, str],
    schemas: dict[str, Any],
) -> bool:
    """Execute one machine-encoded join predicate; every kind is machine-enforced data."""

    kind = predicate["kind"]

    def _endpoint(endpoint: dict[str, Any]) -> Any:
        if "scope" in endpoint:
            return scope.get(endpoint["scope"], _MISSING)
        row = selected.get(endpoint["row"])
        if row is None:
            return _MISSING
        return _resolve_path(row, endpoint["path"])

    if kind == "row_field_eq":
        left = selected[predicate["row"]].get(predicate["field"], _MISSING)
        right = selected[predicate["other_row"]].get(predicate["other_field"], _MISSING)
        return left is not _MISSING and right is not _MISSING and _type_strict_equal(left, right)
    if kind == "path_eq":
        left = _endpoint(predicate["left"])
        right = _endpoint(predicate["right"])
        return left is not _MISSING and right is not _MISSING and _type_strict_equal(left, right)
    if kind == "path_constant":
        value = _resolve_path(selected[predicate["row"]], predicate["path"])
        return value is not _MISSING and _type_strict_equal(value, predicate["constant"])
    if kind == "path_formula":
        binds: dict[str, str] = {}
        for name, endpoint in predicate["binds"].items():
            bound = _endpoint(endpoint)
            if type(bound) is not str:
                return False
            binds[name] = bound
        rendered = predicate["template"]
        for name, bound in binds.items():
            rendered = rendered.replace("{" + name + "}", bound)
        value = _resolve_path(selected[predicate["row"]], predicate["path"])
        return type(value) is str and value == rendered
    if kind == "schema_valid":
        value = _resolve_path(selected[predicate["row"]], predicate["path"])
        if value is _MISSING:
            return False
        ref = predicate["ref"]
        if ref in EXTERNAL_RETAINED_SPEC_REFS:
            return _validate_external_retained_spec(ref, value)
        schema = schemas.get(ref)
        if schema is None:
            return False
        return _validate_object_against_fields(value, schema["fields"], schemas)
    if kind == "digest_recompute":
        digest_value = _resolve_path(selected[predicate["row"]], predicate["path"])
        source = _endpoint(predicate["source"])
        if type(digest_value) is not str or type(source) is not dict:
            return False
        rebuilt = {key: value for key, value in source.items() if key not in set(predicate["exclude"])}
        return digest_value == _sha256_json(rebuilt)
    if kind == "fields_valid":
        row = selected[predicate["row"]]
        for field in predicate["fields"]:
            value = row.get(field["name"], _MISSING)
            if value is _MISSING:
                return False
            if not _validate_value_against_descriptor(value, field, schemas):
                return False
        return True
    if kind == "rows_equal":
        values = [_resolve_path(selected[row], predicate["path"]) for row in predicate["rows"]]
        if any(value is _MISSING for value in values):
            return False
        first = values[0]
        return all(_type_strict_equal(first, value) for value in values[1:])
    if kind == "deterministic_command_id":
        row = selected[predicate["row"]]
        workflow_run_id = row.get(predicate["workflow_run_id_field"])
        idempotency = row.get(predicate["idempotency_field"])
        command_id = row.get(predicate["command_id_field"])
        if type(workflow_run_id) is not str or type(idempotency) is not str or type(command_id) is not str:
            return False
        return command_id == _command_id(workflow_run_id, idempotency)
    raise AssertionError(f"unknown predicate kind {kind}")


def _evaluate_source_join(
    spec: dict[str, Any],
    scope: dict[str, str],
    rows: list[dict[str, Any]],
    witness: list[str] | None = None,
    schemas: dict[str, Any] | None = None,
) -> str:
    """Evaluate the manifest's structured join predicate. Returns accepted or the public miss.

    Every encoded predicate is executed in row order; when ``witness`` is given, the
    ids of all evaluated predicates are appended so the test can prove full coverage.
    """

    if schemas is None:
        schemas = _manifest_schemas(_validate_closed_manifest(_load(MANIFEST_PATH)))
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

    # every requester-binding comparison is an executed predicate, never evaluator prose:
    # the manifest binds each comparison to predicate ids and the witness proves execution
    bindings = {binding["field"] for binding in spec["requester_bindings"]}
    assert bindings == {"workspace_id", "requester_id", "provider_mode", "runtime_namespace"}
    encoded_ids = {predicate["id"] for spec_row in spec["rows"] for predicate in spec_row["predicates"]}
    for binding in spec["requester_bindings"]:
        assert set(binding["predicate_ids"]) <= encoded_ids, binding["field"]

    for spec_row in spec["rows"]:
        for predicate in spec_row["predicates"]:
            if not _execute_predicate(predicate, selected, scope, schemas):
                return "projection_not_found"
            if witness is not None:
                witness.append(predicate["id"])
    return "accepted"


def _sabotage_for_predicate(predicate: dict[str, Any], rows: list[dict[str, Any]]) -> None:
    """Apply one hostile mutation derived from an encoded predicate's own target."""

    def _find_row(name: str) -> dict[str, Any]:
        for row in rows:
            if row.get("command_identity") == name:
                return row
            if row.get("relation") == name and row.get("command_identity") is None:
                return row
        raise AssertionError(f"unknown join row {name}")

    kind = predicate["kind"]
    descriptor: dict[str, Any] | None = None
    if kind == "row_field_eq":
        row_name, path = predicate["row"], [predicate["field"]]
    elif kind == "path_eq":
        row_name, path = predicate["left"]["row"], predicate["left"]["path"]
    elif kind in ("path_constant", "path_formula", "schema_valid", "digest_recompute"):
        row_name, path = predicate["row"], predicate["path"]
    elif kind == "fields_valid":
        row_name = predicate["row"]
        descriptor = predicate["fields"][0]
        path = [descriptor["name"]]
    elif kind == "rows_equal":
        row_name, path = predicate["rows"][-1], predicate["path"]
    elif kind == "deterministic_command_id":
        row_name, path = predicate["row"], [predicate["command_id_field"]]
    else:  # pragma: no cover - guarded by _validate_join_predicate
        raise AssertionError(f"unknown predicate kind {kind}")
    target = _find_row(row_name)
    parent = target
    for part in path[:-1]:
        parent = parent[part]
    leaf = path[-1]
    if descriptor is not None:
        # violate the declared descriptor itself: integer bounds or nonempty strings
        if descriptor["type"] == "integer":
            parent[leaf] = descriptor.get("minimum", 1) - 1
        else:
            parent[leaf] = ""
        return
    current = parent[leaf]
    if type(current) is bool:
        parent[leaf] = 0
    elif type(current) is int:
        parent[leaf] = "sabotaged"
    elif type(current) is str:
        parent[leaf] = 0
    else:
        parent[leaf] = {"__sabotage__": 0}


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
    for row in manifest["retained_contract_pins"]:
        assert _contract_digest(row["schema"]) == row["pin_digest"], row["literal"]
        assert SHA256_RE.fullmatch(row["pin_digest"]), row["literal"]

    # the tool contract binds the exact result and serializer contract digests
    digests = {row["literal"]: row for row in manifest["contract_digests"]}
    tool_fields = {field["name"]: field for field in digests["filter_projection_tool_v3"]["schema"]["fields"]}
    assert (
        tool_fields["result_contract_digest"]["constant"] == digests["filter_projection_result_v3"]["contract_digest"]
    )
    assert (
        tool_fields["serializer_contract_digest"]["constant"]
        == digests["filter_projection_result_serializer_v3"]["contract_digest"]
    )
    # the carrier retained root/owner-ref pins are exact-version-and-digest references
    pin_digests = {row["literal"]: row["pin_digest"] for row in manifest["retained_contract_pins"]}
    carrier_fields = {
        field["name"]: field for field in digests["acquisition_start_authority_carrier.v1"]["schema"]["fields"]
    }
    assert carrier_fields["root_command_payload"]["ref"] == "acquisition_root_command_payload.v2"
    assert carrier_fields["root_command_payload"]["ref_digest"] == pin_digests["acquisition_root_command_payload.v2"]
    assert carrier_fields["command_acceptance_owner_ref"]["ref"] == (
        "acquisition_start_command_acceptance_owner_result_ref.v1"
    )
    assert (
        carrier_fields["command_acceptance_owner_ref"]["ref_digest"]
        == pin_digests["acquisition_start_command_acceptance_owner_result_ref.v1"]
    )
    # every adopted ref carries the exact target contract digest
    for row in manifest["contract_digests"]:
        for field in row["schema"]["fields"]:
            for descriptor in [field, *field.get("fields", []), field.get("items", {})]:
                if isinstance(descriptor, dict) and descriptor.get("ref") in digests:
                    assert descriptor["ref_digest"] == digests[descriptor["ref"]]["contract_digest"], (
                        f"{row['literal']}.{descriptor.get('name')}"
                    )

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
        "filter_projection_result_serializer_v3": 18,
        "filter_projection_tool_v3": 27,
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
    assert (
        tool_fields["result_contract_digest"]["constant"] == digests["filter_projection_result_v3"]["contract_digest"]
    )
    assert (
        tool_fields["serializer_contract_digest"]["constant"]
        == digests["filter_projection_result_serializer_v3"]["contract_digest"]
    )
    serializer_fields = {
        field["name"]: field for field in digests["filter_projection_result_serializer_v3"]["schema"]["fields"]
    }
    assert serializer_fields["owner_name"]["constant"] == result["serializer"]["owner_name"]
    command_fields = {field["name"]: field for field in digests["acquisition.cohort.execute"]["schema"]["fields"]}
    assert command_fields["command_type"]["constant"] == "acquisition.cohort.execute"
    assert command_fields["provider_mode"]["enum"] == ["simulate", "scripted"]

    # exact union discrimination: each variant projects exactly its declared root fields
    variant_roots = {
        "success": result["success_root_fields"],
        "deferred": result["deferred_root_fields"],
        "error": result["masked_root_fields"],
    }
    v3_descriptors = digests["filter_projection_result_v3"]["schema"]["fields"]
    for variant, root_fields in variant_roots.items():
        projected = sorted({field["name"] for field in v3_descriptors if variant in set(field.get("variants", []))})
        assert projected == sorted(root_fields), f"variant {variant} projection drift"
    for name in {field["name"] for field in v3_descriptors}:
        variants_covered = set().union(
            *(set(field.get("variants", [])) for field in v3_descriptors if field["name"] == name)
        )
        expected_variants = {variant for variant, root_fields in variant_roots.items() if name in root_fields}
        assert variants_covered == expected_variants, name
    # the success discriminator and status are exact constants, not open enums
    success_variant = next(
        field for field in v3_descriptors if field["name"] == "variant" and field.get("variants") == ["success"]
    )
    assert success_variant.get("constant") == "success" and "enum" not in success_variant
    success_status = next(
        field for field in v3_descriptors if field["name"] == "status" and field.get("variants") == ["success"]
    )
    assert success_status.get("constant") == "ready"
    deferred_status = next(
        field for field in v3_descriptors if field["name"] == "status" and field.get("variants") == ["deferred"]
    )
    assert deferred_status.get("enum") == ["stale", "not_ready"]


def test_s1f0c_ff_lock_order_and_write_race_outcomes_fail_closed() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    lock = manifest["lock_order"]
    _assert_type_strict_equal([group["order"] for group in lock["groups"]], [0, 1, 2, 3, 4, 5, 6], "lock order")
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
    _assert_type_strict_equal([row["ordinal"] for row in rows], [1, 2, 3, 4, 5, 6, 7, 8], "join ordinals")
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
    _assert_type_strict_equal([row["cardinality"] for row in rows], [1] * 8, "join cardinality")
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
    _assert_type_strict_equal(
        [group["order"] for group in lock_keys["advisory_groups"]], [1, 2, 3, 4], "advisory order"
    )
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

    # every decisive clause is machine-encoded as an executed predicate, never prose
    predicate_ids = [predicate["id"] for row in rows for predicate in row["predicates"]]
    assert len(predicate_ids) == len(set(predicate_ids)) == 100
    assert all(predicate["kind"] in PREDICATE_KINDS for row in rows for predicate in row["predicates"])

    # positive and same-owner positive; the witness proves every predicate executes
    witness: list[str] = []
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), _join_positive_rows(), witness) == "accepted"
    assert witness == predicate_ids
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

    # retained start authority: the exact digest-pinned external preview validator executes
    assert _PREVIEW_RECORD_TOOL_SPEC.input_schema_digest == EXTERNAL_RETAINED_SPEC_REFS[
        "acquisition_plan_preview_record_v2"
    ]
    positive_preview = _join_positive_rows()[2]["payload_json"]["start_snapshot"]["preview"]
    assert _validate_external_retained_spec("acquisition_plan_preview_record_v2", positive_preview)
    assert not _validate_external_retained_spec("acquisition_plan_preview_record_v2", {})
    assert not _validate_external_retained_spec(
        "acquisition_plan_preview_record_v2", {"preview_id": "pv-1", "marker": "external-preview-bytes"}
    )
    # malformed/empty previews fail closed even with the full dependent digest chain recomputed
    mutated = _join_positive_rows()
    mutated[2]["payload_json"]["start_snapshot"]["preview"] = {
        "preview_id": "pv-1",
        "marker": "external-preview-bytes",
    }
    _rebind_join_chain(mutated)
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[2]["payload_json"]["start_snapshot"]["preview"] = {}
    _rebind_join_chain(mutated)
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # foreign preview workspace/requester: schema-valid records whose scope binding rejects
    for field, foreign in (("workspace_id", "ws-foreign"), ("requester_id", "req-foreign")):
        mutated = _join_positive_rows()
        preview = mutated[2]["payload_json"]["start_snapshot"]["preview"]
        preview[field] = foreign
        _rebind_preview_digest(preview)
        assert _validate_external_retained_spec("acquisition_plan_preview_record_v2", preview), field
        _rebind_join_chain(mutated)
        assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found", field
    # foreign owner_ref workspace with the full chain recomputed fails closed
    mutated = _join_positive_rows()
    mutated[1]["result_ref_json"]["workspace_id"] = "ws-foreign"
    _rebind_join_chain(mutated)
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # every requester-binding comparison maps to at least one executed predicate
    binding_ids = {pid for binding in spec["requester_bindings"] for pid in binding["predicate_ids"]}
    assert binding_ids <= set(predicate_ids)
    assert {
        "p_op_workspace",
        "p_act_workspace",
        "p_run_workspace",
        "p_root_preview_workspace",
        "p_root_preview_requester",
        "p_act_requester",
        "p_act_ref_workspace",
        "p_act_ref_mode",
        "p_act_ref_namespace",
    } <= binding_ids

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

    # one hostile mutation for every encoded predicate, derived from the predicate itself
    all_predicates = [predicate for row in rows for predicate in row["predicates"]]
    for predicate in all_predicates:
        mutated = _join_positive_rows()
        _sabotage_for_predicate(predicate, mutated)
        assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found", predicate["id"]

    # reviewer-executed hostile clauses, one explicit mutation each
    # approval state: an unapproved Action can never mint lineage authority
    mutated = _join_positive_rows()
    mutated[1]["approval_status"] = "pending_approval"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[1]["status"] = "completed"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # requested acquisition_run_id: a foreign run joined by shared lineage ids is rejected
    mutated = _join_positive_rows()
    mutated[7]["acquisition_run_id"] = "foreign-run"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # owner-ref/root rebuilding: nested owner-ref identity and root payload bytes are rebuilt and compared
    mutated = _join_positive_rows()
    mutated[1]["result_ref_json"]["workflow_command_id"] = "cmd-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[2]["payload_json"]["action_id"] = "act-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[2]["payload_json"]["payload_digest"] = "9" * 64
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # deterministic command IDs and idempotency formulas
    mutated = _join_positive_rows()
    mutated[2]["command_id"] = "cmd-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[2]["idempotency_key"] = "acquisition.run.create:start-v2:zzz"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[7]["idempotency_key"] = "acquisition_run:plan_commit:cmd-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # physical causality: columns must equal the payload causality and share one causal group
    mutated = _join_positive_rows()
    mutated[2]["causal_group_id"] = "cg-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[3]["source_event_id"] = "evt-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[6]["command_payload_causality"]["source_event_type"] = "CommandCompleted"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # source events: the owner ref binds the root command source event exactly
    mutated = _join_positive_rows()
    mutated[1]["result_ref_json"]["command_source_event_id"] = "evt-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # carrier equality: a wrong carrier on any workflow command fails, even a uniformly wrong one
    for index in (3, 4, 5, 6):
        mutated = _join_positive_rows()
        mutated[index]["carrier_json"]["carrier_digest"] = "8" * 64
        assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found", index
    mutated = _join_positive_rows()
    wrong_carrier = copy.deepcopy(mutated[3]["carrier_json"])
    wrong_carrier["root_command_payload"]["workflow_run_id"] = "wf-x"
    for index in (3, 4, 5, 6):
        mutated[index]["carrier_json"] = copy.deepcopy(wrong_carrier)
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # plan/review identity across build/review/commit and the AcquisitionRun
    mutated = _join_positive_rows()
    mutated[5]["plan_review_id"] = 0
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[6]["plan_review_id"] = 12
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[5]["plan_id"] = "plan-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    # AcquisitionRun bundle equality: carrier, tenant, lineage, plan, and digest
    mutated = _join_positive_rows()
    mutated[7]["execution_bundle_json"]["plan_id"] = "plan-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[7]["execution_bundle_json"]["start_authority_carrier"]["carrier_digest"] = "7" * 64
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[7]["execution_bundle_json"]["workspace_id"] = "ws-x"
    assert _evaluate_source_join(spec, dict(JOIN_SCOPE), mutated) == "projection_not_found"
    mutated = _join_positive_rows()
    mutated[7]["execution_bundle_json"]["authority_digest"] = "6" * 64
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
    # the commit descriptor enforces the exact canonical bounds and contract-digest constant
    commit_schema = next(row for row in manifest["contract_digests"] if row["literal"] == "cohort_execution_commit.v1")
    commit_fields = {field["name"]: field for field in commit_schema["schema"]["fields"]}
    count_field = commit_fields["candidate_count"]
    assert commit_checks["ck_cohort_execution_commits_candidate_count"] == (
        f"candidate_count >= {count_field['minimum']} AND candidate_count <= {count_field['maximum']}"
    )
    assert commit_checks["ck_cohort_execution_commits_commit_contract_digest"] == (
        f"commit_contract_digest = '{commit_schema['contract_digest']}'"
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
    assert checks["ck_cohort_execution_commits_commit_contract_digest"] == (
        f"commit_contract_digest = '{commit_row['contract_digest']}'"
    )
    # the physical columns mirror the record fields plus created_at
    assert [column["name"] for column in columns if column["name"] != "created_at"] == commit_fields

    # hostile: dropping schema identity from the record moves the contract digest
    mutated = copy.deepcopy(commit_row["schema"])
    mutated["fields"] = [field for field in mutated["fields"] if field["name"] != "commit_contract_digest"]
    assert _contract_digest(mutated) != commit_row["contract_digest"]


LINKED_RELATION_SCHEMAS = {
    "agent_runtime_namespace_refs": "agent_runtime_namespace_ref.v1",
    "cohort_execution_lane_results": "cohort_execution_lane_result.v2",
    "cohort_candidate_set_members": "cohort_candidate_member.v1",
    "cohort_execution_commits": "cohort_execution_commit.v1",
    "filter_projection_product_terminals": "filter_projection_product_terminal.v1",
}
CONTRACT_IDENTITY_CHECKS = {
    ("cohort_execution_commits", "commit_contract_digest"): "cohort_execution_commit.v1",
    ("agent_runtime_namespace_refs", "ref_contract_digest"): "agent_runtime_namespace_ref.v1",
}
FIELD_COLUMN_ALIASES = {
    ("cohort_execution_lane_results", "ordinal"): "lane_ordinal",
}


def _canonical_check_expressions(field: dict[str, Any], nullable: bool) -> list[str]:
    """Render the exact SQL check a column-backed canonical field descriptor requires."""

    name = field["name"]
    if "constant" in field:
        constant = field["constant"]
        if type(constant) is str:
            return [f"{name} = '{constant}'"]
        return [f"{name} = {constant}"]
    expressions: list[str] = []
    if "enum" in field:
        if len(field["enum"]) == 1:
            expressions.append(f"{name} = '{field['enum'][0]}'")
        else:
            values = ",".join(f"'{value}'" for value in field["enum"])
            expressions.append(f"{name} IN ({values})")
    if field.get("format") == "sha256_hex":
        if nullable:
            expressions.append(f"{name} IS NULL OR {name} ~ '^[0-9a-f]{{64}}$'")
        else:
            expressions.append(f"{name} ~ '^[0-9a-f]{{64}}$'")
    has_minimum = "minimum" in field
    has_maximum = "maximum" in field
    if has_minimum and has_maximum:
        expressions.append(f"{name} >= {field['minimum']} AND {name} <= {field['maximum']}")
    elif has_minimum:
        minimum = field["minimum"]
        if minimum == 0:
            expressions.append(f"{name} >= 0")
        elif minimum == 1:
            expressions.append(f"{name} > 0")
        else:
            expressions.append(f"{name} >= {minimum}")
    elif has_maximum:
        expressions.append(f"{name} <= {field['maximum']}")
    if field.get("nonempty"):
        if nullable:
            expressions.append(f"{name} IS NULL OR {name} <> ''")
        else:
            expressions.append(f"{name} <> ''")
    return expressions


def test_s1f0c_ff_pg_invariant_enforcement_is_one_to_one() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    relations = manifest["physical_relations"]
    digests = {row["literal"]: row for row in manifest["contract_digests"]}
    for name, relation in relations.items():
        rows = relation["invariant_enforcement"]
        # prose constraints map bijectively to prose-source enforcement rows
        prose_rows = [row for row in rows if row["source"] == "prose"]
        assert sorted(row["invariant"] for row in prose_rows) == sorted(relation["constraints"]), name
        assert len({row["invariant"] for row in prose_rows}) == len(prose_rows), name
        # every declared descriptor mechanism is classified by at least one enforcement row
        descriptor = relation["descriptor"]
        mechanisms = (
            {descriptor["primary_key"]["name"]}
            | {group["name"] for group in descriptor["unique_constraints"]}
            | {check["name"] for check in descriptor["check_constraints"]}
            | {foreign_key["name"] for foreign_key in descriptor["foreign_keys"]}
            | {index["name"] for index in descriptor["indexes"]}
        )
        classified = set().union(*(set(row["mechanism"]) for row in rows))
        assert mechanisms <= classified, f"{name}: unclassified mechanisms {sorted(mechanisms - classified)}"
    for name, literal in LINKED_RELATION_SCHEMAS.items():
        relation = relations[name]
        descriptor = relation["descriptor"]
        check_expressions = {check["expression"] for check in descriptor["check_constraints"]}
        columns = {column["name"]: column for column in descriptor["columns"]}
        covered_fields = set().union(*(set(row.get("covers_fields", [])) for row in relation["invariant_enforcement"]))
        for field in digests[literal]["schema"]["fields"]:
            column_name = FIELD_COLUMN_ALIASES.get((name, field["name"]), field["name"])
            column = columns.get(column_name)
            if column is None or column["type"] == "jsonb":
                # transition/UoW-only invariant: explicitly repository-owned
                assert field["name"] in covered_fields, f"{name}.{field['name']}: no enforcement owner"
                continue
            identity_literal = CONTRACT_IDENTITY_CHECKS.get((name, column_name))
            if identity_literal is not None:
                expected = f"{column_name} = '{digests[identity_literal]['contract_digest']}'"
                assert expected in check_expressions, f"{name}: missing exact contract-digest check {expected!r}"
                continue
            for expression in _canonical_check_expressions({**field, "name": column_name}, column["nullable"]):
                assert expression in check_expressions, f"{name}: missing exact check {expression!r}"
    # both contract-identity digest columns pin the exact recomputed contract digests
    namespace_checks = {
        check["name"]: check["expression"]
        for check in relations["agent_runtime_namespace_refs"]["descriptor"]["check_constraints"]
    }
    assert namespace_checks["ck_agent_runtime_namespace_refs_ref_contract_digest"] == (
        f"ref_contract_digest = '{digests['agent_runtime_namespace_ref.v1']['contract_digest']}'"
    )
    # tenant/mode equality across references is SQL-enforced through composite scoped FKs
    attempts_fk = relations["cohort_execution_attempts"]["descriptor"]["foreign_keys"]
    assert any(
        fk_row["columns"] == ["namespace_ref_id", "workspace_id", "provider_mode"]
        and fk_row["references"]
        == {
            "table": "agent_runtime_namespace_refs",
            "columns": ["namespace_ref_id", "workspace_id", "provider_mode"],
        }
        for fk_row in attempts_fk
    )
    commits_fk = relations["cohort_execution_commits"]["descriptor"]["foreign_keys"]
    assert any(
        fk_row["columns"]
        == [
            "execution_attempt_id",
            "workspace_id",
            "acquisition_run_id",
            "operation_run_id",
            "workflow_run_id",
            "execution_generation",
        ]
        and fk_row["references"]["table"] == "cohort_execution_attempts"
        for fk_row in commits_fk
    )
    terminal_fk = relations["filter_projection_product_terminals"]["descriptor"]["foreign_keys"]
    assert any(
        fk_row["columns"]
        == [
            "execution_commit_id",
            "workspace_id",
            "acquisition_run_id",
            "operation_run_id",
            "workflow_run_id",
            "candidate_set_digest",
            "candidate_count",
        ]
        and fk_row["references"]["table"] == "cohort_execution_commits"
        for fk_row in terminal_fk
    )
    assert any(
        fk_row["columns"] == ["predecessor_terminal_id", "predecessor_terminal_digest"]
        and fk_row["references"]["table"] == "filter_projection_product_terminals"
        for fk_row in terminal_fk
    )
    # state-dependent attempt fields and predecessor parity are exact row-local checks
    attempts_checks = {
        check["expression"] for check in relations["cohort_execution_attempts"]["descriptor"]["check_constraints"]
    }
    assert (
        "status <> 'accepted' OR (provider_exposure_id IS NOT NULL AND provider_call_id IS NOT NULL "
        "AND provider_response_digest IS NOT NULL AND result_digest IS NOT NULL)"
    ) in attempts_checks
    terminal_checks = {
        check["expression"]
        for check in relations["filter_projection_product_terminals"]["descriptor"]["check_constraints"]
    }
    assert "(predecessor_terminal_id IS NULL) = (predecessor_terminal_digest IS NULL)" in terminal_checks
    assert "route_state = 'active'" in terminal_checks
    # unique-when-sent is a partial unique index, not a plain nullable unique
    attempts_indexes = {
        index["name"]: index for index in relations["cohort_execution_attempts"]["descriptor"]["indexes"]
    }
    assert attempts_indexes["uq_cohort_execution_attempts_exposure_sent"]["unique"] is True
    assert attempts_indexes["uq_cohort_execution_attempts_exposure_sent"]["where"] == "provider_exposure_id IS NOT NULL"
    assert attempts_indexes["uq_cohort_execution_attempts_call_sent"]["where"] == "provider_call_id IS NOT NULL"

    # hostile: dropping one exact bound or weakening an expression breaks one-to-one coverage
    hostile = copy.deepcopy(manifest)
    hostile_checks = hostile["physical_relations"]["cohort_execution_commits"]["descriptor"]["check_constraints"]
    for check in hostile_checks:
        if check["name"] == "ck_cohort_execution_commits_candidate_count":
            check["expression"] = "candidate_count >= 0"
    hostile_manifest = hostile["physical_relations"]
    relaxed = {
        check["expression"] for check in hostile_manifest["cohort_execution_commits"]["descriptor"]["check_constraints"]
    }
    commit_fields = {field["name"]: field for field in digests["cohort_execution_commit.v1"]["schema"]["fields"]}
    expected = _canonical_check_expressions(commit_fields["candidate_count"], False)
    assert expected and expected[0] not in relaxed


def _walk_descriptors(fields: list[dict[str, Any]], prefix: str = "") -> list[tuple[str, dict[str, Any]]]:
    walked: list[tuple[str, dict[str, Any]]] = []
    for field in fields:
        path = f"{prefix}/{field['name']}"
        walked.append((path, field))
        if isinstance(field.get("items"), dict):
            walked.extend(_walk_item_descriptor(field["items"], f"{path}/*"))
        for sub in field.get("fields", []):
            walked.extend(_walk_descriptors([sub], path))
    return walked


def _walk_item_descriptor(items: dict[str, Any], prefix: str) -> list[tuple[str, dict[str, Any]]]:
    walked = [(prefix, items)]
    if isinstance(items.get("items"), dict):
        walked.extend(_walk_item_descriptor(items["items"], f"{prefix}/*"))
    for sub in items.get("fields", []):
        walked.extend(_walk_descriptors([sub], prefix))
    return walked


def test_s1f0c_ff_v3_provenance_value_roles_and_closed_items() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    digests = {row["literal"]: row for row in manifest["contract_digests"]}
    v3_fields = digests["filter_projection_result_v3"]["schema"]["fields"]
    walked = _walk_descriptors(v3_fields)

    # every descriptor at every depth carries an explicit provenance pin
    missing_provenance = [path for path, descriptor in walked if "provenance" not in descriptor]
    assert missing_provenance == []
    for path, descriptor in walked:
        if path.split("/")[-1] in ("variant", "status") and path.count("/") == 1:
            assert descriptor["provenance"] == "server_derived", path
        elif path == "/cohort_selection" or path.startswith("/cohort_selection/"):
            assert descriptor["provenance"] == "user_supplied", path
        else:
            assert descriptor["provenance"] == "owner_state", path

    # every string-typed leaf carries the exact value-role map, at least as strict as v2
    def _expected_value_role(path: str, descriptor: dict[str, Any]) -> str:
        if path in ("/candidates/*/display_name", "/candidates/*/headline"):
            return "display_text"
        if path == "/candidates/*/public_profile_url":
            return "web_url"
        if "constant" in descriptor or "enum" in descriptor:
            return "control"
        return "identifier"

    string_leaves = [
        (path, descriptor)
        for path, descriptor in walked
        if descriptor["type"] == "string" and not descriptor.get("fields")
    ]
    assert string_leaves, "v3 must expose string leaves"
    for path, descriptor in string_leaves:
        assert descriptor.get("value_role") == _expected_value_role(path, descriptor), path

    # closed lane-summary items with exact v2 enums/bounds and item bounds
    lane_summaries = next(field for field in v3_fields if field["name"] == "lane_summaries")
    assert lane_summaries["max_items"] == 64
    lane_item_fields = {field["name"]: field for field in lane_summaries["items"]["fields"]}
    assert set(lane_item_fields) == {
        "lane_id",
        "employment_status",
        "role_bucket_id",
        "coverage_status",
        "result_count",
    }
    assert lane_item_fields["employment_status"]["enum"] == ["current", "former"]
    assert lane_item_fields["role_bucket_id"]["enum"] == [
        "all_roles",
        "research",
        "engineering",
        "product_management",
        "infra_systems",
        "founding",
    ]
    assert lane_item_fields["coverage_status"]["enum"] == ["complete", "partial", "missing"]
    assert lane_item_fields["result_count"]["minimum"] == 0
    assert lane_item_fields["result_count"]["maximum"] == 1_000_000

    # closed candidate items with v2 display/URL policies, exact required/optional split
    candidates = next(field for field in v3_fields if field["name"] == "candidates")
    assert candidates["max_items"] == 250
    candidate_fields = {field["name"]: field for field in candidates["items"]["fields"]}
    assert set(candidate_fields) == {
        "candidate_ref",
        "display_name",
        "headline",
        "public_profile_url",
        "employment_statuses",
        "role_bucket_ids",
    }
    required = {name for name, field in candidate_fields.items() if field["required"] is True}
    assert required == {"candidate_ref", "display_name", "headline", "employment_statuses", "role_bucket_ids"}
    assert candidate_fields["candidate_ref"]["format"] == "sha256_hex"
    assert candidate_fields["display_name"]["max_length"] == 500
    assert candidate_fields["display_name"]["nonempty"] is True
    assert candidate_fields["headline"]["max_length"] == 1000
    assert candidate_fields["public_profile_url"]["format"] == "https_url"
    assert candidate_fields["public_profile_url"]["max_length"] == 2048
    assert candidate_fields["employment_statuses"]["min_items"] == 1
    assert candidate_fields["employment_statuses"]["max_items"] == 2
    assert candidate_fields["employment_statuses"]["items"]["enum"] == ["current", "former"]
    assert candidate_fields["role_bucket_ids"]["max_items"] == 5
    assert candidate_fields["role_bucket_ids"]["items"]["enum"] == [
        "research",
        "engineering",
        "product_management",
        "infra_systems",
        "founding",
    ]

    # closed requested-target and requested-lane-coverage schemas; exact paging bounds
    requested_target = next(field for field in v3_fields if field["name"] == "requested_target_ref")
    target_fields = {field["name"]: field for field in requested_target["fields"]}
    assert set(target_fields) == {
        "projection_id",
        "membership_revision",
        "requested_terminal_id",
        "requested_terminal_digest",
        "route_revision_token",
    }
    assert target_fields["projection_id"]["required"] is True
    assert target_fields["membership_revision"]["type"] == "string"
    assert target_fields["membership_revision"]["nonempty"] is True
    assert target_fields["membership_revision"]["value_role"] == "identifier"
    assert "minimum" not in target_fields["membership_revision"]
    assert target_fields["requested_terminal_digest"]["required"] is False
    assert target_fields["requested_terminal_digest"]["format"] == "sha256_hex"
    coverage = next(field for field in v3_fields if field["name"] == "requested_lane_coverage")
    coverage_fields = {field["name"]: field for field in coverage["fields"]}
    assert coverage_fields["status"]["enum"] == ["complete", "partial", "unavailable"]
    assert coverage_fields["requested_lane_count"]["minimum"] == 1
    assert coverage_fields["requested_lane_count"]["maximum"] == 64
    paging = {
        field["name"]: field
        for field in v3_fields
        if field["name"] in ("offset", "limit", "total_count", "returned_count")
    }
    assert paging["offset"]["maximum"] == 100_000
    assert paging["limit"]["maximum"] == 250
    assert paging["total_count"]["maximum"] == 1000
    assert paging["returned_count"]["maximum"] == 250

    # hostile: dropping one value-role or widening one item bound is detected
    hostile = copy.deepcopy(v3_fields)
    hostile_walked = _walk_descriptors(hostile)
    target = next(descriptor for path, descriptor in hostile_walked if path == "/candidates/*/display_name")
    del target["value_role"]
    assert any(
        "value_role" not in descriptor
        for path, descriptor in _walk_descriptors(hostile)
        if descriptor["type"] == "string"
    )
    widened = copy.deepcopy(v3_fields)
    next(field for field in widened if field["name"] == "candidates")["max_items"] = 251
    assert next(field for field in widened if field["name"] == "candidates")["max_items"] != candidates["max_items"]
    assert (
        _contract_digest(
            {"schema_version": "filter_projection_result_v3", "owner": "filter_projection", "fields": widened}
        )
        != digests["filter_projection_result_v3"]["contract_digest"]
    )


def test_s1f0c_ff_retained_pins_are_closed_and_exact() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    pins = {row["literal"]: row for row in manifest["retained_contract_pins"]}
    assert set(pins) == RETAINED_PIN_LITERALS

    owner_ref = pins["acquisition_start_command_acceptance_owner_result_ref.v1"]
    owner_fields = owner_ref["schema"]["fields"]
    assert [field["name"] for field in owner_fields] == [
        "schema_version",
        "runtime_namespace",
        "provider_mode",
        "workspace_id",
        "action_id",
        "operation_run_id",
        "workflow_run_id",
        "workflow_command_id",
        "terminal_winner_id",
        "terminal_winner_sequence_number",
        "command_source_event_id",
        "command_source_event_sequence_number",
        "command_source_event_contract_digest",
        "confirmation_receipt_ref",
        "parent_budget_envelope_ref",
        "start_snapshot_digest",
        "root_command_payload_digest",
        "result_occurrence_ref",
    ]
    by_name = {field["name"]: field for field in owner_fields}
    assert by_name["terminal_winner_sequence_number"]["constant"] == 1
    assert by_name["command_source_event_sequence_number"]["constant"] == 2
    assert {field["name"] for field in by_name["confirmation_receipt_ref"]["fields"]} == {
        "receipt_id",
        "receipt_digest",
    }
    assert {field["name"] for field in by_name["parent_budget_envelope_ref"]["fields"]} == {
        "owner_id",
        "owner_revision",
        "owner_contract_digest",
        "confirmation_receipt_id",
        "confirmation_receipt_digest",
        "budget_digest",
    }
    occurrence_fields = {field["name"]: field for field in by_name["result_occurrence_ref"]["fields"]}
    assert occurrence_fields["slot_generation"]["minimum"] == 1

    root_payload = pins["acquisition_root_command_payload.v2"]
    root_fields = {field["name"]: field for field in root_payload["schema"]["fields"]}
    assert list(root_fields) == [
        "schema_version",
        "command_type",
        "action_id",
        "operation_run_id",
        "workflow_run_id",
        "confirmation_receipt_ref",
        "start_snapshot",
        "start_snapshot_digest",
        "payload_digest",
    ]
    assert root_fields["command_type"]["constant"] == "acquisition.run.create"
    snapshot_fields = {field["name"] for field in root_fields["start_snapshot"]["fields"]}
    assert snapshot_fields == {
        "schema_version",
        "preview",
        "request_pins",
        "result_pins",
        "tool_pins",
        "snapshot_digest",
    }
    preview = next(field for field in root_fields["start_snapshot"]["fields"] if field["name"] == "preview")
    assert preview["ref"] == "acquisition_plan_preview_record_v2"
    assert preview["ref_digest"] == EXTERNAL_RETAINED_SPEC_REFS["acquisition_plan_preview_record_v2"]

    # no open object/array descriptor remains anywhere in the manifest schemas
    def _assert_closed(descriptor: dict[str, Any], label: str) -> None:
        if descriptor["type"] == "object":
            assert "fields" in descriptor or ("ref" in descriptor and "ref_digest" in descriptor), label
        if descriptor["type"] == "array":
            items = descriptor.get("items")
            assert isinstance(items, dict), label
            _assert_closed(items, f"{label}.items")
        for sub in descriptor.get("fields", []):
            _assert_closed(sub, f"{label}.{sub['name']}")

    for row in manifest["contract_digests"]:
        for field in row["schema"]["fields"]:
            _assert_closed(field, f"{row['literal']}.{field['name']}")
    for row in manifest["retained_contract_pins"]:
        for field in row["schema"]["fields"]:
            _assert_closed(field, f"{row['literal']}.{field['name']}")

    # hostile: a retained pin constant mutation moves the pin digest
    mutated = copy.deepcopy(owner_ref["schema"])
    next(field for field in mutated["fields"] if field["name"] == "terminal_winner_sequence_number")["constant"] = 2
    assert _contract_digest(mutated) != owner_ref["pin_digest"]


def test_s1f0c_ff_descriptor_constants_and_numerics_reject_type_aliases() -> None:
    # wrong-type constants are rejected against every declared descriptor type
    for descriptor in (
        {"name": "x", "type": "boolean", "required": True, "constant": 0},
        {"name": "x", "type": "boolean", "required": True, "constant": 1},
        {"name": "x", "type": "integer", "required": True, "constant": True},
        {"name": "x", "type": "integer", "required": True, "constant": False},
        {"name": "x", "type": "integer", "required": True, "constant": 1.0},
        {"name": "x", "type": "string", "required": True, "constant": 1},
        {"name": "x", "type": "string", "required": True, "constant": True},
    ):
        with pytest.raises(AssertionError):
            _validate_descriptor(descriptor, "hostile constant", named=True)
    # every numeric descriptor field rejects bool/float aliases
    for key, alias in (
        ("minimum", True),
        ("minimum", 0.0),
        ("maximum", False),
        ("maximum", 1000.0),
        ("min_items", True),
        ("min_items", 0.0),
        ("max_items", False),
        ("max_items", 250.0),
        ("max_length", True),
        ("max_length", 500.0),
    ):
        declared = (
            "array" if key in ("min_items", "max_items") else ("integer" if key in ("minimum", "maximum") else "string")
        )
        descriptor = {"name": "x", "type": declared, "required": True, key: alias}
        if declared == "array":
            descriptor["items"] = {"type": "string"}
        with pytest.raises(AssertionError):
            _validate_descriptor(descriptor, f"hostile {key}", named=True)

    # exhaustive alias sweep over every numeric/boolean constant in every manifest schema
    manifest = _load(MANIFEST_PATH)
    schemas = [row["schema"] for row in manifest["contract_digests"]]
    schemas.extend(row["schema"] for row in manifest["retained_contract_pins"])
    numeric_keys = ("minimum", "maximum", "min_items", "max_items", "max_length")
    mutations = 0
    for schema in schemas:
        for _path, descriptor in _walk_descriptors(schema["fields"]):
            if "constant" in descriptor:
                constant = descriptor["constant"]
                if type(constant) is bool:
                    aliases = [0 if constant is False else 1, 0.0 if constant is False else 1.0]
                elif type(constant) is int:
                    aliases = [constant == 0 if constant == 0 else True, float(constant)]
                else:
                    aliases = [0, True]
                for alias in aliases:
                    hostile = dict(descriptor, constant=alias)
                    with pytest.raises(AssertionError):
                        _validate_descriptor(hostile, "alias sweep constant", named=True)
                    mutations += 1
            for key in numeric_keys:
                if key in descriptor:
                    for alias in (bool(descriptor[key]), float(descriptor[key])):
                        hostile = dict(descriptor)
                        hostile[key] = alias
                        with pytest.raises(AssertionError):
                            _validate_descriptor(hostile, f"alias sweep {key}", named=True)
                        mutations += 1
    assert mutations > 100, "alias sweep must cover every schema numeric"

    # manifest-level numeric fields reject bool/float aliases inside the closed validator
    for mutate in (
        lambda m: m["start_authority_contracts"]["source_join"]["rows"][0].update(ordinal=True),
        lambda m: m["start_authority_contracts"]["source_join"]["rows"][0].update(cardinality=True),
        lambda m: m["lock_order"]["groups"][0].update(order=False),
        lambda m: m["start_authority_contracts"]["source_join"]["lock_keys"]["advisory_groups"][0].update(order=True),
        lambda m: m["pg_aggregate_surfaces"]["migration_reservation"].update(slot=True),
        lambda m: m["pg_aggregate_surfaces"]["migration_reservation"].update(predecessor_slot=False),
        lambda m: m["start_authority_contracts"]["source_join"]["rows"][0].update(ordinal=1.0),
        lambda m: m["scope"].update(served_population_delta=True),
        lambda m: m["scope"].update(ddl_delta=0.0),
        lambda m: m["scope"].update(decision_only=1),
        lambda m: m["release_boundary"].update(migration_delta=False),
        lambda m: m["release_boundary"].update(served_population=0.0),
        lambda m: m["release_boundary"].update(author_evidence_only=1),
    ):
        hostile = copy.deepcopy(manifest)
        mutate(hostile)
        with pytest.raises(AssertionError):
            _validate_closed_manifest(hostile)

    # implementation wave semantics: exact integer waves in the closed set, packet
    # uniqueness, and dependency/wave order are all validator-enforced
    for mutate in (
        lambda m: m["implementation_batches"][0].update(wave=True),
        lambda m: m["implementation_batches"][0].update(wave=1.0),
        lambda m: m["implementation_batches"][0].update(wave=0),
        lambda m: m["implementation_batches"][0].update(wave=4),
        lambda m: m["implementation_batches"][8].update(wave=1),  # FF-XO deps sit in later waves
        lambda m: m["implementation_batches"][1].update(packet="FF-SCHEMA"),  # duplicate packet
        lambda m: m["implementation_batches"][1].update(depends_on=["FF-GHOST"]),
        lambda m: m["implementation_batches"][4].update(depends_on=["FF-CARRIER", "FF-CARRIER"]),
    ):
        hostile = copy.deepcopy(manifest)
        mutate(hostile)
        with pytest.raises(AssertionError):
            _validate_closed_manifest(hostile)

    # no float value exists anywhere in the manifest
    def _assert_no_float(value: object, path: str) -> None:
        assert type(value) is not float, path
        if type(value) is dict:
            for key, item in value.items():
                _assert_no_float(item, f"{path}.{key}")
        elif type(value) is list:
            for index, item in enumerate(value):
                _assert_no_float(item, f"{path}[{index}]")

    _assert_no_float(manifest, "manifest")


def test_s1f0c_ff_membership_revision_is_an_opaque_equality_token() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    schemas = [row["schema"] for row in manifest["contract_digests"]]

    # every membership_revision descriptor in every S1f0c schema is a non-empty
    # opaque string: never a positive integer, never ordered
    descriptors = [
        (path, descriptor)
        for schema in schemas
        for path, descriptor in _walk_descriptors(schema["fields"])
        if descriptor.get("name") == "membership_revision"
    ]
    assert len(descriptors) == 7
    for path, descriptor in descriptors:
        assert descriptor["type"] == "string", path
        assert descriptor["nonempty"] is True, path
        assert "minimum" not in descriptor and "maximum" not in descriptor, path
        assert "opaque" in descriptor["derivation"], path
        assert "never" in descriptor["derivation"], path

    # the descriptor accepts arbitrary opaque tokens and rejects the old integer shape
    schemas_by_literal = _manifest_schemas(manifest)
    terminal_field = descriptors[0][1]
    for token in ("9", "mr-20260719-a", "rev-9f"):
        assert _validate_value_against_descriptor(token, terminal_field, schemas_by_literal)
    for hostile in (9, 1, True, "", 0):
        assert not _validate_value_against_descriptor(hostile, terminal_field, schemas_by_literal)

    # exact-token parity: identical tokens compare equal; distinct tokens never merge,
    # including a same-timestamp-distinct-token pair that a chronological alias would
    # wrongly collapse
    stamp = "2026-07-19T00:00:00Z"
    terminal_a = {"projection_id": "proj-1", "membership_revision": "mr-20260719-a", "created_at": stamp}
    terminal_b = {"projection_id": "proj-1", "membership_revision": "mr-20260719-b", "created_at": stamp}
    assert terminal_a["created_at"] == terminal_b["created_at"]
    assert _type_strict_equal(terminal_a["membership_revision"], terminal_a["membership_revision"])
    assert not _type_strict_equal(terminal_a["membership_revision"], terminal_b["membership_revision"])
    assert not _type_strict_equal(terminal_a, terminal_b)
    terminal_c = copy.deepcopy(terminal_a)
    assert _type_strict_equal(terminal_a, terminal_c)

    # the physical column is text with a non-empty check, never bigint/positive
    terminals = manifest["physical_relations"]["filter_projection_product_terminals"]
    by_name = {column["name"]: column for column in terminals["descriptor"]["columns"]}
    assert by_name["membership_revision"]["type"] == "text"
    checks = {check["name"]: check["expression"] for check in terminals["descriptor"]["check_constraints"]}
    assert checks["ck_filter_projection_product_terminals_membership_revision"] == "membership_revision <> ''"
    unique_groups = [group["columns"] for group in terminals["descriptor"]["unique_constraints"]]
    assert ["workspace_id", "projection_id", "membership_revision"] in unique_groups

    # no manifest text anywhere orders the token numerically, lexically, or chronologically
    forbidden_substrings = (
        "order by membership_revision",
        "max(membership_revision",
        "min(membership_revision",
        "latest membership_revision",
        "membership_revision::bigint",
    )
    ordering_operator = re.compile(r"membership_revision\s*(?:>=|<=|>|<(?!\>))")

    def _walk_strings(value: object, path: str) -> list[str]:
        hits: list[str] = []
        if type(value) is str:
            lowered = value.lower()
            if any(pattern in lowered for pattern in forbidden_substrings) or ordering_operator.search(lowered):
                hits.append(path)
        elif type(value) is dict:
            for key, item in value.items():
                hits.extend(_walk_strings(item, f"{path}.{key}"))
        elif type(value) is list:
            for index, item in enumerate(value):
                hits.extend(_walk_strings(item, f"{path}[{index}]"))
        return hits

    assert _walk_strings(manifest, "manifest") == []


def test_s1f0c_ff_v3_cohort_selection_retains_the_closed_object() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    digests = {row["literal"]: row for row in manifest["contract_digests"]}
    v3_fields = {field["name"]: field for field in digests["filter_projection_result_v3"]["schema"]["fields"]}
    cohort = v3_fields["cohort_selection"]

    # the exact closed Cohort selection object, never an arbitrary string
    assert cohort["type"] == "object"
    assert [field["name"] for field in cohort["fields"]] == [
        "schema_version",
        "role_bucket_ids",
        "employment_statuses",
        "role_match",
        "source",
    ]
    sub = {field["name"]: field for field in cohort["fields"]}
    assert sub["schema_version"]["constant"] == "cohort_selection.v1"
    assert sub["source"]["constant"] == "user_explicit"
    assert sub["role_match"]["enum"] == ["any", "all"]
    assert sub["role_bucket_ids"]["items"]["enum"] == [
        "research",
        "engineering",
        "product_management",
        "infra_systems",
        "founding",
    ]
    assert sub["role_bucket_ids"]["max_items"] == 5
    assert sub["employment_statuses"]["items"]["enum"] == ["current", "former"]
    assert sub["employment_statuses"]["min_items"] == 1
    assert sub["employment_statuses"]["max_items"] == 2
    assert all(field["provenance"] == "user_supplied" for field in cohort["fields"])

    # the registry pin and the selection digest bind the object byte-for-byte
    assert v3_fields["cohort_selection_registry_version"]["constant"] == COHORT_SELECTION_REGISTRY_VERSION
    assert "cohort_selection_digest(cohort_selection)" in v3_fields["cohort_selection_digest"]["derivation"]
    assert "cohort_selection_digest(cohort_selection)" in cohort["derivation"]

    # executed validation: the canonical object passes; every hostile shape fails closed
    schemas = _manifest_schemas(manifest)
    canonical = {
        "schema_version": "cohort_selection.v1",
        "role_bucket_ids": ["research", "engineering"],
        "employment_statuses": ["current", "former"],
        "role_match": "any",
        "source": "user_explicit",
    }
    assert _validate_value_against_descriptor(canonical, cohort, schemas)
    for hostile in (
        "anthropic-research",  # the reviewed unbound string shape
        {**canonical, "tenant_hint": "ws-1"},
        {**canonical, "source": "inferred"},
        {key: value for key, value in canonical.items() if key != "role_match"},
        {**canonical, "employment_statuses": []},
        {
            **canonical,
            "role_bucket_ids": [
                "research",
                "engineering",
                "product_management",
                "infra_systems",
                "founding",
                "founding",
            ],
        },
        {**canonical, "role_match": "any|all"},
    ):
        assert not _validate_value_against_descriptor(hostile, cohort, schemas), repr(hostile)


def _materialize_constant_object(fields: list[dict[str, Any]], *, skip: set[str] | None = None) -> dict[str, Any]:
    """Rebuild the pinned JSON object from fully constant field descriptors."""

    skipped = skip or set()
    record: dict[str, Any] = {}
    for field in fields:
        name = field["name"]
        if name in skipped:
            continue
        if "constant" in field:
            record[name] = field["constant"]
        elif field["type"] == "object" and "fields" in field:
            record[name] = _materialize_constant_object(field["fields"], skip=skip)
        elif (
            field["type"] == "array"
            and field.get("min_items") == field.get("max_items")
            and field.get("min_items") == len(field.get("items", {}).get("enum", []))
        ):
            record[name] = list(field["items"]["enum"])
        else:
            raise AssertionError(f"field {name} is not fully decision-locked")
    return record


def test_s1f0c_ff_v3_serializer_and_tool_fingerprints_are_fully_decision_locked() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    digests = {row["literal"]: row for row in manifest["contract_digests"]}

    serializer = digests["filter_projection_result_serializer_v3"]
    assert serializer["ordered_fields"] == [
        "owner_name",
        "revision",
        "owner",
        "tool_name",
        "tool_kind",
        "owner_binding_action_type",
        "result_schema_version",
        "result_contract_digest",
        "serializer_contract",
        "validator_owner",
        "interpretation_contract_version",
        "interpretation_contract_digest",
        "externally_controlled_identifier_paths",
        "max_serialized_bytes",
        "max_items",
        "max_depth",
        "artifact_ref_schemes",
        "allowed_variants",
    ]
    s_fields = {field["name"]: field for field in serializer["schema"]["fields"]}
    # ActionResultSpec identity and validator/interpretation contract, executed against the runtime pins
    assert s_fields["tool_name"]["constant"] == "filter_projection"
    assert s_fields["tool_kind"]["constant"] == "action"
    assert s_fields["owner_binding_action_type"]["constant"] == "filter_projection"
    assert s_fields["validator_owner"]["constant"] == ACTION_RESULT_VALIDATOR_OWNER
    assert s_fields["interpretation_contract_version"]["constant"] == "action_result_interpretation_contract_v3"
    assert s_fields["interpretation_contract_digest"]["constant"] == ACTION_RESULT_INTERPRETATION_CONTRACT_DIGEST
    # exact model-safety limits: 64 KiB, 8192 items, depth 10, empty artifact schemes
    assert s_fields["max_serialized_bytes"]["constant"] == 65536
    assert s_fields["max_items"]["constant"] == 8192
    assert s_fields["max_depth"]["constant"] == 10
    assert s_fields["artifact_ref_schemes"]["min_items"] == 0
    assert s_fields["artifact_ref_schemes"]["max_items"] == 0
    assert set(s_fields["externally_controlled_identifier_paths"]["items"]["enum"]) == {
        "/cohort_selection/schema_version",
        "/cohort_selection/role_bucket_ids/*",
        "/cohort_selection/employment_statuses/*",
        "/cohort_selection/role_match",
        "/cohort_selection/source",
    }
    assert s_fields["externally_controlled_identifier_paths"]["min_items"] == 5
    assert s_fields["externally_controlled_identifier_paths"]["max_items"] == 5
    assert s_fields["allowed_variants"]["items"]["enum"] == ["success", "deferred", "error"]
    # serializer semantics are a closed constant object, not prose
    contract_fields = {field["name"]: field for field in s_fields["serializer_contract"]["fields"]}
    assert set(contract_fields) == {
        "schema_version",
        "owner_output",
        "membership_source",
        "variant_selection",
        "canonical_json",
        "owner_result_ref_rule",
        "serialized_result_digest_rule",
        "is_error_parity",
    }
    assert contract_fields["schema_version"]["constant"] == "filter_projection_result_serializer_contract_v3"

    tool = digests["filter_projection_tool_v3"]
    assert tool["ordered_fields"] == [
        "tool_spec",
        "owner",
        "effect_class",
        "result_link_policy",
        "result_schema_version",
        "result_contract_digest",
        "serializer_owner_name",
        "serializer_revision",
        "serializer_contract_digest",
        "registration_state",
        "fingerprint_schema_version",
        "tool_name",
        "model_description",
        "tool_kind",
        "action_type",
        "request",
        "result_pin",
        "workspace_actor_binder",
        "adapter",
        "simulate_fixture",
        "release_state_ref",
        "execution_subject",
        "budget",
        "capability",
        "approval",
        "command_exposure",
        "control_policy",
    ]
    t_fields = {field["name"]: field for field in tool["schema"]["fields"]}
    assert t_fields["fingerprint_schema_version"]["constant"] == "agent_tool_spec_v2"
    assert t_fields["tool_name"]["constant"] == "filter_projection"
    assert t_fields["tool_kind"]["constant"] == "action"
    assert t_fields["action_type"]["constant"] == "filter_projection"
    assert t_fields["command_exposure"]["constant"] == "none"

    request_fields = {field["name"]: field for field in t_fields["request"]["fields"]}
    assert request_fields["schema_version"]["constant"] == "projection_filter_request_v3"
    assert request_fields["query_owner"]["constant"] == "null"
    result_pin_fields = {field["name"]: field for field in t_fields["result_pin"]["fields"]}
    assert result_pin_fields["validation_contract_version"]["constant"] == "action_result_interpretation_contract_v3"
    assert result_pin_fields["max_serialized_bytes"]["constant"] == 65536
    assert result_pin_fields["max_items"]["constant"] == 8192
    assert result_pin_fields["max_depth"]["constant"] == 10
    assert result_pin_fields["query_owner"]["constant"] == "null"
    budget_fields = {field["name"]: field for field in t_fields["budget"]["fields"]}
    assert budget_fields["mode"]["constant"] == "not_required"
    capability_fields = {field["name"]: field for field in t_fields["capability"]["fields"]}
    assert capability_fields["mode"]["constant"] == "not_required"
    approval_fields = {field["name"]: field for field in t_fields["approval"]["fields"]}
    assert approval_fields["mode"]["constant"] == "not_required"

    # every constant owner-pin digest recomputes from its own pinned contract bytes
    def _check_owner_pin(pin_descriptor: dict[str, Any], label: str) -> None:
        pin_fields = {field["name"]: field for field in pin_descriptor["fields"]}
        pin = _materialize_constant_object(pin_descriptor["fields"], skip={"owner_contract_digest"})
        recomputed = _sha256_json(pin)
        assert pin_fields["owner_contract_digest"]["constant"] == recomputed, label

    _check_owner_pin(request_fields["validator_owner"], "request.validator_owner")
    _check_owner_pin(t_fields["workspace_actor_binder"], "workspace_actor_binder")
    _check_owner_pin(t_fields["adapter"], "adapter")
    release_fields = {field["name"]: field for field in t_fields["release_state_ref"]["fields"]}
    _check_owner_pin(release_fields["release_owner"], "release_owner")
    assert release_fields["release_key"]["constant"] == "action:filter_projection"
    subject_fields = {field["name"]: field for field in t_fields["execution_subject"]["fields"]}
    _check_owner_pin(subject_fields["subject_validator_owner"], "subject_validator_owner")
    _check_owner_pin(subject_fields["permission_policy"], "permission_policy")
    _check_owner_pin(approval_fields["approval_policy"], "approval_policy")
    # the subject schema digest recomputes from the pinned subject contract bytes
    subject_contract = _materialize_constant_object(subject_fields["subject_validator_owner"]["fields"], skip={"owner_contract_digest"})[
        "contract"
    ]
    assert subject_fields["subject_schema_digest"]["constant"] == _sha256_json(subject_contract)
    # the result serializer owner pin binds the runtime serializer_contract_digest formula
    serializer_pin = _materialize_constant_object(result_pin_fields["serializer_owner"]["fields"], skip={"owner_contract_digest"})
    serializer_contract_digest = _sha256_json(
        {
            "schema_version": "action_result_serializer_contract_v1",
            "serializer_owner": serializer_pin["owner_id"],
            "serializer_revision": serializer_pin["owner_revision"],
            "contract": serializer_pin["contract"],
        }
    )
    serializer_pin_fields = {field["name"]: field for field in result_pin_fields["serializer_owner"]["fields"]}
    assert serializer_pin_fields["owner_contract_digest"]["constant"] == serializer_contract_digest
    result_validator_fields = {field["name"]: field for field in result_pin_fields["validator_owner"]["fields"]}
    assert result_validator_fields["owner_contract_digest"]["constant"] == ACTION_RESULT_INTERPRETATION_CONTRACT_DIGEST
    # the fixture digest recomputes from the materialized fixture record
    fixture_descriptor = t_fields["simulate_fixture"]
    fixture_record = _materialize_constant_object(fixture_descriptor["fields"], skip={"fixture_digest"})
    fixture_fields = {field["name"]: field for field in fixture_descriptor["fields"]}
    assert fixture_fields["fixture_digest"]["constant"] == _sha256_json(fixture_record)
    assert fixture_record["result_link_policy"] == "no_command_v1"
    assert fixture_record["approval_required"] is False

    # registration-computed digests stay digest-typed with an explicit derivation, never constants
    for descriptor in (
        request_fields["schema_digest"],
        request_fields["action_contract_digest"],
        result_pin_fields["schema_digest"],
    ):
        assert descriptor["format"] == "sha256_hex"
        assert "constant" not in descriptor
        assert "computed at registration" in descriptor["derivation"]
    control_fields = {field["name"]: field for field in t_fields["control_policy"]["fields"]}
    assert control_fields["owner_id"]["constant"] == "operation_runtime.ActionRegistry.allowed_workflow_command_contracts"

    # hostile: drifting one model-safety limit moves the serializer contract digest
    widened = copy.deepcopy(serializer["schema"])
    next(field for field in widened["fields"] if field["name"] == "max_serialized_bytes")["constant"] = 64000
    assert _contract_digest(widened) != serializer["contract_digest"]
    thinned = copy.deepcopy(tool["schema"])
    thinned["fields"] = [field for field in thinned["fields"] if field["name"] != "capability"]
    assert _contract_digest(thinned) != tool["contract_digest"]


def test_s1f0c_ff_lane_provider_evidence_binds_the_owning_attempt_and_terminals_are_append_only() -> None:
    manifest = _validate_closed_manifest(_load(MANIFEST_PATH))
    relations = manifest["physical_relations"]

    # lane provider evidence equals the owning attempt's exact tuple, repository-compared
    # before any lane write
    lanes = relations["cohort_execution_lane_results"]
    lane_rule_rows = [
        row
        for row in lanes["invariant_enforcement"]
        if row["enforcement"] == "repository"
        and row["mechanism"] == ["uow_rule"]
        and set(row.get("covers_fields", []))
        == {"provider_exposure_id", "provider_call_id", "provider_response_digest"}
    ]
    assert len(lane_rule_rows) == 1, "exactly one repository rule binds lane provider evidence to the attempt"
    rule = lane_rule_rows[0]["invariant"]
    assert "before any lane write" in rule
    for field in ("provider_exposure_id", "provider_call_id", "provider_response_digest"):
        assert field in rule
    assert rule in lanes["constraints"]

    # hostile: without the rule the prose/enforcement bijection breaks and the coverage assert fails
    hostile = copy.deepcopy(manifest)
    hostile_lanes = hostile["physical_relations"]["cohort_execution_lane_results"]
    hostile_lanes["invariant_enforcement"] = [
        row for row in hostile_lanes["invariant_enforcement"] if row["invariant"] != rule
    ]
    prose = sorted(row["invariant"] for row in hostile_lanes["invariant_enforcement"] if row["source"] == "prose")
    assert prose != sorted(hostile_lanes["constraints"]), "dropping the rule must break one-to-one coverage"
    hostile2 = copy.deepcopy(manifest)
    for row in hostile2["physical_relations"]["cohort_execution_lane_results"]["invariant_enforcement"]:
        if row["invariant"] == rule:
            row["enforcement"] = "sql"
    assert not any(
        row["enforcement"] == "repository"
        and row["mechanism"] == ["uow_rule"]
        and set(row.get("covers_fields", []))
        == {"provider_exposure_id", "provider_call_id", "provider_response_digest"}
        for row in hostile2["physical_relations"]["cohort_execution_lane_results"]["invariant_enforcement"]
    ), "SQL enforcement cannot compare lane evidence to the attempt tuple"

    # terminal append-only is split: SQL keeps positive generation; a repository
    # transition_guard owns insert-once/no-UPDATE/no-DELETE
    terminals = relations["filter_projection_product_terminals"]
    constraints = terminals["constraints"]
    assert not any("positive terminal generation; insert once" in rule for rule in constraints)
    assert any("no UPDATE/DELETE normal path" in rule for rule in constraints)
    by_invariant = {row["invariant"]: row for row in terminals["invariant_enforcement"]}
    generation_row = next(row for invariant, row in by_invariant.items() if invariant.startswith("positive terminal generation"))
    assert generation_row["enforcement"] == "sql"
    assert generation_row["mechanism"] == ["ck_filter_projection_product_terminals_generation"]
    append_only_rows = [
        row for invariant, row in by_invariant.items() if "no UPDATE/DELETE normal path" in invariant
    ]
    assert len(append_only_rows) == 1
    assert append_only_rows[0]["enforcement"] == "repository"
    assert "transition_guard" in append_only_rows[0]["mechanism"]

    # hostile: the coverage test fails when the append-only mechanism is absent or misassigned
    hostile3 = copy.deepcopy(manifest)
    for row in hostile3["physical_relations"]["filter_projection_product_terminals"]["invariant_enforcement"]:
        if "no UPDATE/DELETE normal path" in row["invariant"]:
            row["enforcement"] = "sql"
            row["mechanism"] = ["ck_filter_projection_product_terminals_generation"]
    assert not any(
        row["enforcement"] == "repository"
        and "transition_guard" in row["mechanism"]
        and "no UPDATE/DELETE normal path" in row["invariant"]
        for row in hostile3["physical_relations"]["filter_projection_product_terminals"]["invariant_enforcement"]
    ), "the generation check cannot enforce append-only behavior"
    hostile4 = copy.deepcopy(manifest)
    hostile4["physical_relations"]["filter_projection_product_terminals"]["invariant_enforcement"] = [
        row
        for row in hostile4["physical_relations"]["filter_projection_product_terminals"]["invariant_enforcement"]
        if "no UPDATE/DELETE normal path" not in row["invariant"]
    ]
    prose4 = sorted(
        row["invariant"]
        for row in hostile4["physical_relations"]["filter_projection_product_terminals"]["invariant_enforcement"]
        if row["source"] == "prose"
    )
    assert prose4 != sorted(hostile4["physical_relations"]["filter_projection_product_terminals"]["constraints"])
