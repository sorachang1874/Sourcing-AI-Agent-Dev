from __future__ import annotations

import ast
import json
from pathlib import Path

from sourcing_agent import agent_projection_query
from sourcing_agent.agent_canary_registry import FILTER_PROJECTION_TOOL_SPEC
from sourcing_agent.projection_search_index_contract import PROJECTION_SEARCH_INDEX_BINDING_KEYS

REPO_ROOT = Path(__file__).resolve().parents[1]
DECISION = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "decisions"
    / "TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md"
)
MANIFEST = (
    REPO_ROOT / "docs" / "modules" / "serving-product" / "contracts" / "filter_projection_foundation_boundary_v1.json"
)
ROUTERS = (
    REPO_ROOT / "docs" / "INDEX.md",
    REPO_ROOT / "docs" / "NEXT_TODO.md",
    REPO_ROOT / "docs" / "PRE_AGENT_CONTRACT_REVIEW.md",
    REPO_ROOT / "docs" / "TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md",
    REPO_ROOT / "docs" / "modules" / "serving-product" / "README.md",
)

_TOP_LEVEL_FIELDS = {
    "schema_version",
    "decision_id",
    "status",
    "reviewed_candidate",
    "ownership",
    "request_target_fields",
    "lineage_state",
    "ratified_foundation_capabilities",
    "forbidden_claims",
    "dependency_batches",
    "required_lineage_decisions",
    "writer_inventory",
    "release_state",
}
_DEPENDENCY_BATCHES = (
    "S1f0b-foundation",
    "S1f0c0-lineage-decision",
    "S1f0c1-start-propagation",
    "S1f0c2-cohort-terminal",
    "S1f0d-owner-v2",
    "S1f1-reader-result",
)
_REQUEST_TARGET_FIELDS = (
    "projection_id",
    "membership_revision",
    "cohort_selection_registry_version",
    "cohort_selection_registry_digest",
    "cohort_selection_digest",
)
_WRITER_INVENTORY = (
    "serving_projection_writer.ServingProjectionWriter.publish_run_scope_projection",
    "serving_projection_writer.ServingProjectionWriter.publish_filter_projection_foundation_run_scope_projection",
    "serving_projection_writer.ServingProjectionWriter.publish_collection_authoritative_projection",
    "repositories.serving_projection.ServingProjectionRepository.upsert",
    "repositories.serving_projection.ServingProjectionRepository.upsert_members",
    "repositories.serving_projection.ServingProjectionRepository.replace_members",
    "repositories.serving_projection.ServingProjectionRepository.upsert_with_members",
    "repositories.serving_projection.ServingProjectionRepository.upsert_with_replaced_members",
    "repositories.serving_projection.ServingProjectionRepository.publish_run_scope_projection",
    "repositories.serving_projection.ServingProjectionRepository.publish_filter_projection_foundation_run_scope_projection",
    "repositories.serving_projection.ServingProjectionRepository.publish_collection_authoritative_projection",
    "repositories.serving_projection.ServingProjectionRepository.patch_publication_fields_under_lock",
    "repositories.serving_projection.ServingProjectionRepository.update_person_search_index_build_state",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.insert_row_with_generated_id",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.upsert_row_with_generated_id",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.update_row_returning",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.delete_rows",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.update_rows",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.upsert_row",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.bulk_upsert_rows",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.replace_rows",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.write_serving_projection_members_with_input_revision",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.upsert_row_and_upsert_rows",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.upsert_row_and_replace_rows",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.publish_serving_projection",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.patch_serving_projection_publication_fields",
    "control_plane_live_postgres.LiveControlPlanePostgresAdapter.update_projection_person_search_index_generation_state",
    "orchestrator.SourcingOrchestrator._publish_run_scope_projection_from_result_view_owner",
    "orchestrator.SourcingOrchestrator._execute_operation_native_projection_admission_command_payload",
    "orchestrator.SourcingOrchestrator._extend_run_scope_projection_from_board_visible_records",
    "serving_projection_migration.ServingProjectionMigrationBackfill.backfill_run_scope_projection",
    "asset_consolidation_repair_apply.apply_asset_consolidation_repair",
)


def _manifest() -> dict[str, object]:
    value = json.loads(MANIFEST.read_text(encoding="utf-8"))
    assert type(value) is dict
    return value


def test_s1f0_fixed_forward_manifest_is_closed_and_retracts_product_authority() -> None:
    manifest = _manifest()
    assert set(manifest) == _TOP_LEVEL_FIELDS
    assert manifest["schema_version"] == "filter_projection_foundation_boundary.v1"
    assert manifest["decision_id"] == "track_d.d1n.s1f0a.fixed_forward"
    assert manifest["status"] == "foundation_only_unbound"

    reviewed = manifest["reviewed_candidate"]
    assert type(reviewed) is dict
    assert set(reviewed) == {"commit", "verdict", "finding_counts"}
    assert reviewed["commit"] == "64a7dfc8f84f416a166a6ded69c666c7e50e523e"
    assert reviewed["verdict"] == "NO-GO"
    assert reviewed["finding_counts"] == {"p0": 0, "p1": 10, "p2": 2, "p3": 2}

    ownership = manifest["ownership"]
    assert ownership == {
        "physical_writer": "serving_projection_owner",
        "candidate_field_validator": "projection_search_service.filter_projection_publication_candidate",
        "candidate_field_validator_revision": "filter_projection_publication_candidate_v1",
        "product_owner": "unratified",
    }
    assert tuple(manifest["forbidden_claims"]) == (
        "exact_start_v2_product_owner",
        "product_eligible_carrier",
        "agent_owner_reader",
        "result_slot_acceptance",
        "start_v2_end_to_end",
        "served_readiness",
        "local_live_readiness",
        "hosted_readiness",
        "migration_or_backfill",
    )


def test_s1f0_dependency_graph_and_release_boundary_are_exact() -> None:
    manifest = _manifest()
    batches = manifest["dependency_batches"]
    assert type(batches) is list
    assert tuple(item["batch_id"] for item in batches) == _DEPENDENCY_BATCHES
    assert all(type(item) is dict and set(item) == {"batch_id", "depends_on", "owner", "outcome"} for item in batches)
    assert batches[0]["depends_on"] == []
    assert batches[1]["depends_on"] == []
    assert batches[2]["depends_on"] == ["S1f0c0-lineage-decision"]
    assert batches[3]["depends_on"] == ["S1f0c1-start-propagation"]
    assert batches[4]["depends_on"] == ["S1f0b-foundation", "S1f0c2-cohort-terminal"]
    assert batches[5]["depends_on"] == ["S1f0d-owner-v2"]

    release = manifest["release_state"]
    assert release == {
        "served_population": 0,
        "provider_invocations": 0,
        "model_invocations": 0,
        "live_invocations": 0,
        "migration_delta": 0,
        "backfill": "forbidden",
        "open_residuals": ["R-019", "R-029"],
    }


def test_s1f0_request_contract_remains_closed_and_commandless() -> None:
    manifest = _manifest()
    assert tuple(manifest["request_target_fields"]) == _REQUEST_TARGET_FIELDS
    schema = agent_projection_query.filter_projection_v2_request_schema()
    target = schema["properties"]["target_ref"]
    assert tuple(target["required"]) == _REQUEST_TARGET_FIELDS
    assert set(target["properties"]) == set(_REQUEST_TARGET_FIELDS)
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.effect_class == "read_only"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.result_link_policy == "no_command_v1"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.command_exposure == "none"


def test_s1f0_writer_inventory_is_complete_and_uses_dedicated_carrier_registry() -> None:
    manifest = _manifest()
    assert tuple(manifest["writer_inventory"]) == _WRITER_INVENTORY
    all_source = "\n".join(
        path.read_text(encoding="utf-8") for path in sorted((REPO_ROOT / "src" / "sourcing_agent").rglob("*.py"))
    )
    for qualified_name in _WRITER_INVENTORY:
        assert qualified_name.rsplit(".", 1)[-1] in all_source

    control_source = (REPO_ROOT / "src" / "sourcing_agent" / "control_plane_live_postgres.py").read_text(
        encoding="utf-8"
    )
    control_tree = ast.parse(control_source)
    adapter_class = next(
        node
        for node in control_tree.body
        if isinstance(node, ast.ClassDef) and node.name == "LiveControlPlanePostgresAdapter"
    )
    generic_native_mutators: dict[str, ast.FunctionDef] = {}
    for node in adapter_class.body:
        if not isinstance(node, ast.FunctionDef):
            continue
        call_names = {
            call.func.id if isinstance(call.func, ast.Name) else call.func.attr
            for call in ast.walk(node)
            if isinstance(call, ast.Call) and isinstance(call.func, (ast.Name, ast.Attribute))
        }
        if "_require_dedicated_workflow_command_writer" in call_names:
            generic_native_mutators[node.name] = node

    discovered_inventory = {
        f"control_plane_live_postgres.LiveControlPlanePostgresAdapter.{name}" for name in generic_native_mutators
    }
    assert discovered_inventory <= set(_WRITER_INVENTORY)

    prohibited_unscoped_dml = {
        "insert_row_with_generated_id",
        "upsert_row_with_generated_id",
        "update_row_returning",
        "delete_rows",
        "update_rows",
    }
    for name in prohibited_unscoped_dml:
        node = generic_native_mutators[name]
        assert any(
            isinstance(call, ast.Call)
            and (
                (isinstance(call.func, ast.Name) and call.func.id == "_reject_generic_serving_projection_mutation")
                or (
                    isinstance(call.func, ast.Attribute)
                    and call.func.attr == "_reject_generic_serving_projection_mutation"
                )
            )
            for call in ast.walk(node)
        )
    assert "filter_projection_owner_v1" not in PROJECTION_SEARCH_INDEX_BINDING_KEYS


def test_s1f0_is_zero_migration_and_routed_with_the_same_status() -> None:
    migration_sql = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted((REPO_ROOT / "src" / "sourcing_agent" / "migrations").glob("*.sql"))
    )
    assert "filter_projection_owner" not in migration_sql
    assert "filter_projection_membership" not in migration_sql

    decision = DECISION.read_text(encoding="utf-8")
    assert "foundation_only_unbound" in decision
    assert "NO-GO 0/10/2/2" in decision
    assert "serving_projection_owner" in decision
    assert "product_owner_state=unratified" in decision
    for router in ROUTERS:
        text = router.read_text(encoding="utf-8")
        assert "TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md" in text
        assert "foundation_only_unbound" in text
