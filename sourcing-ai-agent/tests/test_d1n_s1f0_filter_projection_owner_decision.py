from __future__ import annotations

import ast
import re
from pathlib import Path

from sourcing_agent import agent_projection_query
from sourcing_agent.agent_canary_registry import FILTER_PROJECTION_TOOL_SPEC
from sourcing_agent.cohort_provider_compiler import NON_LIVE_PROVIDER_MODES
from sourcing_agent.projection_search_index_contract import PROJECTION_SEARCH_INDEX_BINDING_KEYS
from sourcing_agent.serving_projection_reader import SHARED_CANONICAL_PROJECTION_TYPES

REPO_ROOT = Path(__file__).resolve().parents[1]
DECISION = (
    REPO_ROOT
    / "docs"
    / "modules"
    / "serving-product"
    / "decisions"
    / "TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md"
)
PLAN = REPO_ROOT / "docs" / "TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md"
TODO = REPO_ROOT / "docs" / "NEXT_TODO.md"
PRE_AGENT = REPO_ROOT / "docs" / "PRE_AGENT_CONTRACT_REVIEW.md"
INDEX = REPO_ROOT / "docs" / "INDEX.md"
MIGRATION = REPO_ROOT / "src" / "sourcing_agent" / "migrations" / "0001_baseline.sql"
ORCHESTRATOR = REPO_ROOT / "src" / "sourcing_agent" / "orchestrator.py"
REPOSITORY = REPO_ROOT / "src" / "sourcing_agent" / "repositories" / "serving_projection.py"

PARENT_CARRIER_KEY = "filter_projection_owner_v1"
MEMBER_CARRIER_KEY = "filter_projection_membership_v1"
ELIGIBLE_PROJECTION_TYPES = ("run_scope_projection",)
ELIGIBLE_PROVIDER_MODES = ("simulate", "scripted")
OWNER_RECORD_FIELDS = (
    "schema_version",
    "owner",
    "owner_revision",
    "source_run_id",
    "result_view_id",
    "snapshot_id",
    "cohort_selection_registry_version",
    "cohort_selection_registry_digest",
    "cohort_selection_digest",
    "selection_digest",
    "planning_digest",
    "execution_digest",
    "result_digest",
    "publication_digest",
    "provider_mode",
    "runtime_namespace",
    "cache_provenance",
    "requested_lane_coverage",
    "lane_summaries",
    "terminal_owner_ref",
    "terminal_owner_digest",
)
TERMINAL_OWNER_FIELDS = (
    "schema_version",
    "owner",
    "owner_revision",
    "source_run_id",
    "result_view_id",
    "snapshot_id",
    "planning_manifest_digest",
    "execution_manifest_digest",
    "result_digest",
    "publication_digest",
)
MEMBER_OWNER_FIELDS = (
    "schema_version",
    "owner",
    "owner_revision",
    "parent_owner_digest",
    "candidate_identity_key",
    "memberships",
    "membership_digest",
)


def _function_return_keys(path: Path, function_name: str) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function_name:
            for child in ast.walk(node):
                if isinstance(child, ast.Return) and isinstance(child.value, ast.Dict):
                    keys = {
                        key.value
                        for key in child.value.keys
                        if isinstance(key, ast.Constant) and isinstance(key.value, str)
                    }
                    if "candidate_identity_key" in keys and "provenance" in keys:
                        return keys
    raise AssertionError(f"return mapping not found: {path}:{function_name}")


def _table_block(sql: str, table_name: str) -> str:
    match = re.search(
        rf"CREATE TABLE {re.escape(table_name)} \((.*?)\n\);",
        sql,
        flags=re.DOTALL,
    )
    assert match is not None, table_name
    return match.group(1)


def test_s1f0_decision_is_routed_and_records_exact_non_live_boundary() -> None:
    decision = DECISION.read_text(encoding="utf-8")
    assert decision.startswith("# Track D D1n S1f0a")
    for token in (
        "owner=projection_search_service.filter_projection_publication_owner",
        "eligible_projection_type=run_scope_projection",
        "eligible_provider_modes=simulate,scripted",
        f"parent_carrier=serving_projections.metadata.{PARENT_CARRIER_KEY}",
        f"member_carrier=serving_projection_members.provenance.{MEMBER_CARRIER_KEY}",
        "result_link_policy=no_command_v1",
        "migration_delta=0",
        "backfill=forbidden",
        "collection_authoritative_adoption=forbidden",
        "default_public_agent_served_population=0",
        "R-019 and R-029 remain open",
    ):
        assert token in decision
    for routed_path in (PLAN, TODO, PRE_AGENT, INDEX):
        assert "TRACK_D_D1N_S1F0_FILTER_PROJECTION_OWNER_DECISION.md" in routed_path.read_text(encoding="utf-8")


def test_s1f0_decision_closes_parent_terminal_and_member_field_sets() -> None:
    decision = DECISION.read_text(encoding="utf-8")
    for field in (*OWNER_RECORD_FIELDS, *TERMINAL_OWNER_FIELDS, *MEMBER_OWNER_FIELDS):
        assert re.search(rf"(?m)^(?:\| )?`?{re.escape(field)}`?(?: |$)", decision)
    assert len(OWNER_RECORD_FIELDS) == 21
    assert len(TERMINAL_OWNER_FIELDS) == 10
    assert len(MEMBER_OWNER_FIELDS) == 7
    assert len(set(OWNER_RECORD_FIELDS)) == len(OWNER_RECORD_FIELDS)
    assert len(set(TERMINAL_OWNER_FIELDS)) == len(TERMINAL_OWNER_FIELDS)
    assert len(set(MEMBER_OWNER_FIELDS)) == len(MEMBER_OWNER_FIELDS)


def test_existing_filter_contract_needs_no_new_request_field_or_command_link() -> None:
    schema = agent_projection_query.filter_projection_v2_request_schema()
    target = schema["properties"]["target_ref"]
    assert tuple(target["required"]) == (
        "projection_id",
        "membership_revision",
        "cohort_selection_registry_version",
        "cohort_selection_registry_digest",
        "cohort_selection_digest",
    )
    assert set(target["properties"]) == set(target["required"])
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.effect_class == "read_only"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.result_link_policy == "no_command_v1"
    assert FILTER_PROJECTION_TOOL_SPEC.behavior.command_exposure == "none"
    assert ELIGIBLE_PROJECTION_TYPES == ("run_scope_projection",)
    assert set(ELIGIBLE_PROVIDER_MODES).issubset(NON_LIVE_PROVIDER_MODES)
    assert "replay" in NON_LIVE_PROVIDER_MODES
    assert "replay" not in ELIGIBLE_PROVIDER_MODES
    assert set(ELIGIBLE_PROJECTION_TYPES).issubset(SHARED_CANONICAL_PROJECTION_TYPES)
    assert "collection_authoritative_projection" in SHARED_CANONICAL_PROJECTION_TYPES
    assert "collection_authoritative_projection" not in ELIGIBLE_PROJECTION_TYPES


def test_existing_json_and_atomic_publication_surfaces_make_s1f0_zero_ddl() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    projection = _table_block(sql, "serving_projections")
    member = _table_block(sql, "serving_projection_members")
    assert "metadata_json text NOT NULL" in projection
    assert "provenance_json text NOT NULL" in member

    repository = REPOSITORY.read_text(encoding="utf-8")
    assert "def publish_run_scope_projection(" in repository
    assert '"publish_serving_projection"' in repository
    assert 'scope_kind="run_scope"' in repository
    assert '"projection_row": projection_row' in repository
    assert '"member_rows": member_rows' in repository
    assert '"routing_row": routing_row' in repository
    assert "def hold_publication_lock(" in repository


def test_s1f0_characterizes_unimplemented_carrier_and_mapper_gaps() -> None:
    repository = REPOSITORY.read_text(encoding="utf-8")
    orchestrator = ORCHESTRATOR.read_text(encoding="utf-8")
    member_return_keys = _function_return_keys(
        ORCHESTRATOR,
        "_serving_projection_member_from_candidate_record",
    )

    # S1f0a is decision-only. S1f0b must deliberately flip these assertions as
    # it adds the reserved keys, exact member carrier, and owner-aware reader.
    assert PARENT_CARRIER_KEY not in PROJECTION_SEARCH_INDEX_BINDING_KEYS
    assert PARENT_CARRIER_KEY not in repository
    assert MEMBER_CARRIER_KEY not in repository
    assert PARENT_CARRIER_KEY not in orchestrator
    assert MEMBER_CARRIER_KEY not in orchestrator
    assert "metadata" not in member_return_keys
    member_function = orchestrator[
        orchestrator.index("def _serving_projection_member_from_candidate_record") :
        orchestrator.index("def _serving_projection_members_from_records")
    ]
    assert "cohort_lane_membership" not in member_function
