from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class PytestInvocation:
    label: str
    args: tuple[str, ...]
    reason: str
    runner: str = "pytest"
    cwd: str = ""


_STORAGE_AND_CONTROL_PLANE_SUITES = (
    PytestInvocation(
        label="control-plane-live",
        args=("tests/test_control_plane_live_postgres.py",),
        reason="storage/control-plane authoritative read-write contract",
    ),
    PytestInvocation(
        label="control-plane-postgres",
        args=("tests/test_control_plane_postgres.py",),
        reason="control-plane snapshot sync contract",
    ),
    PytestInvocation(
        label="candidate-artifacts-focus",
        args=("tests/test_candidate_artifacts.py", "-k", "materialization or generation or hot_cache"),
        reason="materialization shard and hot-cache behavior",
    ),
)

_ASSET_IMPORT_SUITES = (
    PytestInvocation(
        label="asset-sync-cloud-import",
        args=("tests/test_asset_sync.py", "tests/test_cloud_asset_import.py"),
        reason="generation-first object-storage import/export",
    ),
    PytestInvocation(
        label="control-plane-postgres",
        args=("tests/test_control_plane_postgres.py",),
        reason="control-plane snapshot sync contract",
    ),
)

_WORKFLOW_SMOKE_SUITES = (
    PytestInvocation(
        label="hosted-workflow-smoke-focus",
        args=(
            "tests/test_hosted_workflow_smoke.py",
            "-k",
            (
                "default_explain_matrix "
                "or hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs "
                "or hosted_simulate_reuse_queries_preserve_follow_up_planning_contract "
                "or hosted_simulate_completed_history_round_trip_exposes_results_recovery"
            ),
        ),
        reason="hosted explain/simulate smoke guardrail",
    ),
)

_WORKFLOW_EXPLAIN_SUITES = (
    PytestInvocation(
        label="workflow-explain-focus",
        args=(
            "tests/test_workflow_explain.py",
            "-k",
            (
                "api_workflows_explain_route_returns_dry_run_payload "
                "or plan_workflow_reuses_same_effective_request_and_dispatch_as_explain "
                "or explain_workflow_exposes_generation_watermarks_and_cloud_asset_operations "
                "or preserves_common_directional_keywords "
                "or nvidia_world_model_direction_uses_large_org_scoped_search "
                "or does_not_treat_company_name_ai_suffix_as_ambiguous"
            ),
        ),
        reason="workflow explain dry-run, API route, and stable intent guardrail",
    ),
)

_ORCHESTRATOR_AND_RESULTS_SUITES = (
    PytestInvocation(
        label="control-plane-live",
        args=("tests/test_control_plane_live_postgres.py",),
        reason="orchestrator/control-plane PG contract",
    ),
    PytestInvocation(
        label="results-api-focus",
        args=("tests/test_results_api.py", "tests/test_frontend_history_recovery.py"),
        reason="results/history API contract",
    ),
    *_WORKFLOW_EXPLAIN_SUITES,
)

_FRONTEND_CONTRACT_SUITES = (
    PytestInvocation(
        label="frontend-contract-focus",
        args=("tests/test_frontend_history_recovery.py", "tests/test_results_api.py"),
        reason="frontend history recovery and results API contract",
    ),
    PytestInvocation(
        label="frontend-build",
        args=("npm", "run", "build"),
        reason="frontend production build guardrail",
        runner="command",
        cwd="frontend-demo",
    ),
)

_SCRIPTED_PROVIDER_SUITES = (
    PytestInvocation(
        label="scripted-provider-scenarios",
        args=("tests/test_scripted_provider_scenario.py", "tests/test_scripted_test_runtime.py"),
        reason="scripted provider scenario coverage and isolated runtime contract",
    ),
    PytestInvocation(
        label="workflow-smoke-behavior-report",
        args=("tests/test_workflow_smoke.py", "-k", "provider_case_report or summarize_smoke_timings"),
        reason="workflow behavior guardrail and provider/materialization report contract",
    ),
)

_EXCEL_INTAKE_SUITES = (
    PytestInvocation(
        label="excel-intake-product-flow",
        args=("tests/test_excel_intake.py", "tests/test_frontend_history_recovery.py", "-k", "excel or Excel"),
        reason="Excel intake upload, split-job, continuation, and history contract",
    ),
)

_ASSET_GOVERNANCE_SUITES = (
    PytestInvocation(
        label="asset-governance-contract",
        args=(
            "tests/test_asset_governance.py",
            "tests/test_organization_execution_profile.py",
            "-k",
            "coverage_baseline or lifecycle or promoted",
        ),
        reason="canonical/default pointer, lifecycle, promotion, and coverage baseline governance",
    ),
)

_PUBLIC_WEB_SUITES = (
    PytestInvocation(
        label="public-web-core",
        args=(
            "tests/test_public_web_search.py",
            "tests/test_public_web_quality.py",
            "tests/test_linkedin_url_normalization.py",
        ),
        reason="candidate Public Web search, quality gate, URL-shape, and identity-key normalization",
    ),
    PytestInvocation(
        label="crm-public-web-runtime-boundary",
        args=(
            "tests/test_crm_public_web_runtime_boundary.py",
            "tests/test_target_candidate_public_web.py",
            "tests/test_results_api.py",
            "-k",
            "target_candidate_public_web or public_web_api or promotion",
        ),
        reason="CRM Public Web normal boundary plus migration-only target-candidate retirement contract",
    ),
    PytestInvocation(
        label="target-candidate-public-web-pg",
        args=(
            "tests/test_control_plane_live_postgres.py",
            "-k",
            (
                "target_candidate_public_web_state_is_postgres_authoritative "
                "or postgres_only_uses_ephemeral_sqlite_shadow "
                "or postgres_only_skips_sqlite_fallback"
            ),
        ),
        reason="PG-authoritative Public Web tables and no SQLite fallback guardrail",
    ),
)

_PRODUCT_JOURNEY_SUITES = (
    PytestInvocation(
        # The god-file journey suite retired with tests/test_pipeline.py
        # (T-008); the hosted simulate smoke matrix is the modern end-to-end
        # journey equivalent.
        label="hosted-journey-smoke",
        args=(
            "tests/test_hosted_workflow_smoke.py",
            "-k",
            "default_explain_matrix or hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs",
        ),
        reason="hosted end-to-end journey regression (modern replacement for the god-file journey suite)",
    ),
    PytestInvocation(
        label="frontend-build",
        args=("npm", "run", "build"),
        reason="frontend production build guardrail",
        runner="command",
        cwd="frontend-demo",
    ),
)

_SMOKE_SUITES = (
    *_STORAGE_AND_CONTROL_PLANE_SUITES,
    *_ASSET_IMPORT_SUITES,
    *_WORKFLOW_EXPLAIN_SUITES,
    *_WORKFLOW_SMOKE_SUITES,
)

_STORAGE_RELATED_PATHS = {
    "src/sourcing_agent/storage.py",
    "src/sourcing_agent/control_plane_live_postgres.py",
    "src/sourcing_agent/control_plane_postgres.py",
    # Store-layer split modules (T-008 remap: rode the god-file fallback).
    "src/sourcing_agent/control_plane_repository.py",
    "src/sourcing_agent/control_plane_serde.py",
    "src/sourcing_agent/control_plane_time.py",
    "src/sourcing_agent/json_contract.py",
    "src/sourcing_agent/request_ownership.py",
    "src/sourcing_agent/repositories/criteria_confidence.py",
    "src/sourcing_agent/repositories/crm_core.py",
    "src/sourcing_agent/repositories/linkedin_profile_registry.py",
    "src/sourcing_agent/repositories/manual_review.py",
    "src/sourcing_agent/repositories/model_invocation_envelopes.py",
    "src/sourcing_agent/repositories/person_company_assets.py",
    "src/sourcing_agent/repositories/public_web.py",
    "src/sourcing_agent/repositories/serving_projection.py",
    "src/sourcing_agent/repositories/workflow_runtime.py",
}
_ASSET_IMPORT_RELATED_PATHS = {
    "src/sourcing_agent/asset_sync.py",
    "src/sourcing_agent/cloud_asset_import.py",
}
_ARTIFACT_RELATED_PATHS = {
    "src/sourcing_agent/candidate_artifacts.py",
    "src/sourcing_agent/organization_assets.py",
    "src/sourcing_agent/artifact_cache.py",
    "src/sourcing_agent/asset_paths.py",
}
_WORKFLOW_SMOKE_RELATED_PATHS = {
    "src/sourcing_agent/workflow_smoke.py",
    "src/sourcing_agent/hosted_smoke_surface.py",
    "src/sourcing_agent/workflow_explain_matrix.py",
    "src/sourcing_agent/smoke_runtime_seed.py",
    "src/sourcing_agent/scripted_provider_scenario.py",
    "src/sourcing_agent/scripted_test_runtime.py",
    "src/sourcing_agent/runtime_tuning.py",
    "scripts/run_simulate_smoke_matrix.py",
    "scripts/run_explain_dry_run_matrix.py",
}
_WORKFLOW_EXPLAIN_RELATED_PATHS = {
    "src/sourcing_agent/workflow_explain_matrix.py",
    "src/sourcing_agent/organization_execution_profile.py",
    "src/sourcing_agent/asset_reuse_planning.py",
    "scripts/run_explain_dry_run_matrix.py",
}
# command_kernel.py is the store-only command-execution kernel extracted from
# SourcingOrchestrator; it mirrors durable_runtime.py's mapping (the paired
# tests/test_durable_runtime.py suite) since the kernel owns the same
# workflow-command spine contracts.
_COMMAND_KERNEL_RELATED_PATHS = {
    "src/sourcing_agent/command_kernel.py",
}
# cli_parsers.py is the argparse-only configurator registry extracted from
# cli.py (WS2 slice 2, 2026-07-22); both route to the paired CLI suite.
_CLI_RELATED_PATHS = {
    "src/sourcing_agent/cli.py",
    "src/sourcing_agent/cli_parsers.py",
}
_CLI_SUITES = (
    PytestInvocation(
        label="paired::tests/test_cli.py",
        args=("tests/test_cli.py",),
        reason="CLI command registry / parser configurators changed",
    ),
)
_COMMAND_KERNEL_SUITES = (
    PytestInvocation(
        label="paired::tests/test_durable_runtime.py",
        args=("tests/test_durable_runtime.py",),
        reason="command kernel extracted from the orchestrator workflow-command spine",
    ),
)
# recovery_drain_registry.py holds the recovery-tick drain bindings consumed by
# run_worker_recovery_once; changes must re-run the drain characterization
# contract plus the recovery/workflow-command spine suites.
_RECOVERY_DRAIN_REGISTRY_RELATED_PATHS = {
    "src/sourcing_agent/recovery_drain_registry.py",
}
_RECOVERY_DRAIN_REGISTRY_SUITES = (
    PytestInvocation(
        label="paired::tests/test_recovery_drain_registry.py",
        args=("tests/test_recovery_drain_registry.py",),
        reason="recovery-tick drain binding registry and characterization contract",
    ),
    PytestInvocation(
        label="paired::tests/test_durable_runtime.py",
        args=("tests/test_durable_runtime.py",),
        reason="recovery tick consumes the drain registry inside the workflow-command spine",
    ),
    PytestInvocation(
        label="paired::tests/test_worker_recovery_daemon.py",
        args=("tests/test_worker_recovery_daemon.py",),
        reason="worker recovery daemon drives the recovery tick that iterates the drain registry",
    ),
)
# recovery_phases.py holds the recovery-tick phase objects + TickContext +
# registry seam (Phase 4 Step 2 A2) that run_worker_recovery_once iterates for
# the migrated self-contained phases and the drain group. Changes must re-run
# the whole-tick characterization oracle plus the recovery/workflow-command
# spine suites that drive the tick.
_RECOVERY_PHASES_RELATED_PATHS = {
    "src/sourcing_agent/recovery_phases.py",
}
_RECOVERY_PHASES_SUITES = (
    PytestInvocation(
        label="paired::tests/test_recovery_tick_characterization.py",
        args=("tests/test_recovery_tick_characterization.py",),
        reason="whole-tick characterization oracle pins the phase sequence/owners/skip reasons the registry seam must reproduce",
    ),
    PytestInvocation(
        label="paired::tests/test_recovery_drain_registry.py",
        args=("tests/test_recovery_drain_registry.py",),
        reason="the 14 uniform drains flow through the recovery-phase registry as a phase-group",
    ),
    PytestInvocation(
        label="paired::tests/test_durable_runtime.py",
        args=("tests/test_durable_runtime.py",),
        reason="recovery tick iterates the phase registry inside the workflow-command spine",
    ),
    PytestInvocation(
        label="paired::tests/test_worker_recovery_daemon.py",
        args=("tests/test_worker_recovery_daemon.py",),
        reason="worker recovery daemon drives the recovery tick that iterates the phase registry",
    ),
)
# enrichment.py owns the fetch-profile batch-division ladder pinned by the WS7
# ruling-① plan-record characterization oracle (2026-07-23); edits there must
# re-run the oracle alongside the paired enrichment suite, mirroring the
# recovery_phases -> tick-oracle precedent.
_ENRICHMENT_RELATED_PATHS = {
    "src/sourcing_agent/enrichment.py",
}
_ENRICHMENT_SUITES = (
    PytestInvocation(
        label="paired::tests/test_enrichment.py",
        args=("tests/test_enrichment.py",),
        reason="paired test for enrichment.py",
    ),
    PytestInvocation(
        label="paired::tests/test_fetch_profile_batch_characterization.py",
        args=("tests/test_fetch_profile_batch_characterization.py",),
        reason="WS7 ruling-① plan-record oracle pins the fetch-profile batch-division ladder",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_prefetch_scheduler_contract.py",
        args=("tests/test_profile_prefetch_scheduler_contract.py",),
        reason="profile-prefetch scheduler R1-R7 contract guards ride every enrichment edit",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_shadow.py",
        args=("tests/test_profile_batch_division_shadow.py",),
        reason="W7.2 S3 shadow mint-seam pins: dispatch byte-identity + completion-path non-invocation",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_wave_identity.py",
        args=("tests/test_profile_batch_division_wave_identity.py",),
        reason="W7.2 S4 R6 division_id wave identity: mint-seam writer threading + scalar-claim carry byte-compat",
    ),
)
# profile_batch_division_contract.py is the WS7/W7.2 S1 divider-output contract
# (schema sourcing.profile_prefetch.ai_batch_division.v1 + validator battery
# V1-V10). The validators formalize the ladder rules the WS7 oracle pins, so an
# edit here must re-run the paired contract suite AND the characterization
# oracle (docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §1.3 oracle-pin column).
_PROFILE_BATCH_DIVISION_CONTRACT_RELATED_PATHS = {
    "src/sourcing_agent/profile_batch_division_contract.py",
    # W7.2 S2: the divider orchestration helper (model call + envelope assembly
    # + F1-F6 mapping) rides the same contract family.
    "src/sourcing_agent/profile_batch_division.py",
}
_PROFILE_BATCH_DIVISION_CONTRACT_SUITES = (
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_contract.py",
        args=("tests/test_profile_batch_division_contract.py",),
        reason="ai_batch_division.v1 schema + V1-V10 validator battery contract",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_model_surface.py",
        args=("tests/test_profile_batch_division_model_surface.py",),
        reason="W7.2 S2 divider model-invocation surface: OQ5 gate, OQ7 scripted client, F1-F6 audit mapping",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_shadow.py",
        args=("tests/test_profile_batch_division_shadow.py",),
        reason="W7.2 S3 shadow hook: record-only integration, exception isolation, ladder divergence digest",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_wave_identity.py",
        args=("tests/test_profile_batch_division_wave_identity.py",),
        reason="W7.2 S4 R6 division_id extension: identity carry, heterogeneous windows, structural unreachability",
    ),
    PytestInvocation(
        label="paired::tests/test_fetch_profile_batch_characterization.py",
        args=("tests/test_fetch_profile_batch_characterization.py",),
        reason="the validator battery formalizes the ladder rules the WS7 plan-record oracle pins",
    ),
)
# model_provider.py previously rode only the generic paired fallback
# (tests/test_model_provider.py), which silently missed the v1 protocol
# characterization pins; W7.2 S2 makes the mapping explicit and adds the
# divider surface suite (the ModelClient Protocol gained a divider method).
_MODEL_PROVIDER_RELATED_PATHS = {
    "src/sourcing_agent/model_provider.py",
}
_MODEL_PROVIDER_SUITES = (
    PytestInvocation(
        label="paired::tests/test_model_provider.py",
        args=("tests/test_model_provider.py",),
        reason="model provider client behavior + fail-closed live selection",
    ),
    PytestInvocation(
        label="paired::tests/test_model_client_v1_characterization.py",
        args=("tests/test_model_client_v1_characterization.py",),
        reason="ModelClient v1 protocol surface + consumer call-point pins",
    ),
    PytestInvocation(
        label="paired::tests/test_profile_batch_division_model_surface.py",
        args=("tests/test_profile_batch_division_model_surface.py",),
        reason="W7.2 S2 divider method conventions (timeout/circuit/raw-output contract)",
    ),
)
_ORCHESTRATOR_RELATED_PATHS = {
    "src/sourcing_agent/orchestrator.py",
    # T-008 remap: operation/agent contract band + orchestrator-extracted
    # modules that previously rode the god-file fallback.
    "src/sourcing_agent/candidate_source_resolver.py",
    "src/sourcing_agent/harvest_support.py",
    "src/sourcing_agent/harvest_offline_harness.py",
    "src/sourcing_agent/acquisition_plan_preview.py",
    "src/sourcing_agent/acquisition_start_command_acceptance.py",
    "src/sourcing_agent/acquisition_start_v2.py",
    "src/sourcing_agent/acquisition_start_v2_control.py",
    "src/sourcing_agent/acquisition_start_v2_create_postgres.py",
    "src/sourcing_agent/acquisition_start_v2_postgres.py",
    "src/sourcing_agent/acquisition_start_v2_result_postgres.py",
    "src/sourcing_agent/action_contract_identity.py",
    "src/sourcing_agent/action_request_schema.py",
    "src/sourcing_agent/action_result_schema.py",
    "src/sourcing_agent/action_target_binding.py",
    "src/sourcing_agent/agent_canary_registry.py",
    "src/sourcing_agent/agent_contract_activation.py",
    "src/sourcing_agent/agent_contract_identity.py",
    "src/sourcing_agent/agent_operation_query_postgres.py",
    "src/sourcing_agent/agent_projection_query.py",
    "src/sourcing_agent/agent_tool_registry.py",
    "src/sourcing_agent/agent_tool_result_postgres.py",
    "src/sourcing_agent/agent_tool_result_slot.py",
    "src/sourcing_agent/crm_contract.py",
    "src/sourcing_agent/filter_projection_publication_owner.py",
    "src/sourcing_agent/model_route_registry.py",
    "src/sourcing_agent/model_usage.py",
    "src/sourcing_agent/projection_search_index_contract.py",
    "src/sourcing_agent/provider_task_runtime.py",
    "src/sourcing_agent/workflow_progressed_child_contract.py",
    "src/sourcing_agent/acquisition_command_owner.py",
    "src/sourcing_agent/profile_fetch_owner.py",
    "src/sourcing_agent/api.py",
    "src/sourcing_agent/workflow_refresh.py",
    "src/sourcing_agent/workflow_submission.py",
    "src/sourcing_agent/manual_review.py",
    "src/sourcing_agent/plan_review.py",
    "src/sourcing_agent/results_store.py",
}
_EXCEL_INTAKE_RELATED_PATHS = {
    "src/sourcing_agent/excel_intake.py",
    "src/sourcing_agent/excel_intake_owner.py",
    "frontend-demo/src/components/ExcelWorkflowIntakePanel.tsx",
    "frontend-demo/scripts/run_excel_intake_e2e.mjs",
}
_ASSET_GOVERNANCE_RELATED_PATHS = {
    "src/sourcing_agent/asset_governance.py",
    "src/sourcing_agent/asset_registration.py",
    "src/sourcing_agent/organization_assets.py",
    "src/sourcing_agent/organization_execution_profile.py",
}
_PUBLIC_WEB_RELATED_PATHS = {
    # company_public_web_action_mixin.py is the verbatim family move of the
    # orchestrator's track-d public-web action band (WS2 slice 3, 2026-07-22).
    "src/sourcing_agent/company_public_web_action_mixin.py",
    "src/sourcing_agent/linkedin_url_normalization.py",
    "src/sourcing_agent/public_web_quality.py",
    "src/sourcing_agent/public_web_search.py",
    "src/sourcing_agent/crm_public_web_owner.py",
    "src/sourcing_agent/crm_public_web_runtime.py",
    "src/sourcing_agent/public_web_runtime_core.py",
    "src/sourcing_agent/legacy_public_web_storage.py",
    "tests/test_linkedin_url_normalization.py",
    "tests/test_public_web_quality.py",
    "tests/test_public_web_search.py",
    "tests/test_crm_public_web_runtime_boundary.py",
    "tests/test_target_candidate_public_web.py",
    "frontend-demo/scripts/run_target_public_web_e2e.mjs",
    "frontend-demo/scripts/run_target_public_web_promotion_export_e2e.mjs",
}
_PRODUCT_JOURNEY_RELATED_PATHS = {
    "src/sourcing_agent/company_asset_supplement.py",
    "src/sourcing_agent/manual_review.py",
    "src/sourcing_agent/results_store.py",
    "frontend-demo/src/pages/SearchPage.tsx",
    "frontend-demo/src/pages/ResultsPage.tsx",
    "frontend-demo/src/components/TargetCandidatesPanel.tsx",
    "frontend-demo/src/components/SupplementIntakePanel.tsx",
    "frontend-demo/src/components/ManualReviewQueuePanel.tsx",
}
_FRONTEND_RELATED_PREFIXES = (
    "frontend-demo/",
    "contracts/",
)


def normalize_changed_paths(changed_paths: Iterable[str]) -> list[str]:
    normalized: set[str] = set()
    for raw_path in changed_paths:
        text = str(raw_path or "").strip().replace("\\", "/")
        if not text:
            continue
        while text.startswith("./"):
            text = text[2:]
        normalized.add(text)
    return sorted(normalized)


def smoke_pytest_invocations() -> list[PytestInvocation]:
    deduped: dict[str, PytestInvocation] = {}
    for invocation in _SMOKE_SUITES:
        deduped.setdefault(invocation.label, invocation)
    return list(deduped.values())


def infer_pytest_invocations(
    changed_paths: Iterable[str],
    *,
    repo_root: str | Path | None = None,
) -> list[PytestInvocation]:
    normalized_paths = normalize_changed_paths(changed_paths)
    if not normalized_paths:
        return smoke_pytest_invocations()

    root = Path(repo_root).expanduser() if repo_root else None
    selected: dict[str, PytestInvocation] = {}
    saw_backend_change = False

    def add(invocation: PytestInvocation) -> None:
        selected.setdefault(invocation.label, invocation)

    for path in normalized_paths:
        if path.endswith(".md") or path.startswith("docs/"):
            continue
        if path in _STORAGE_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _STORAGE_AND_CONTROL_PLANE_SUITES:
                add(invocation)
            continue
        if path in _ASSET_IMPORT_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _ASSET_IMPORT_SUITES:
                add(invocation)
            continue
        if path in _ARTIFACT_RELATED_PATHS:
            saw_backend_change = True
            # candidate-artifacts focus; the god-file materialization focus
            # retired with tests/test_pipeline.py (T-008).
            add(_STORAGE_AND_CONTROL_PLANE_SUITES[2])
            continue
        if path in _CLI_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _CLI_SUITES:
                add(invocation)
            continue
        if path in _COMMAND_KERNEL_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _COMMAND_KERNEL_SUITES:
                add(invocation)
            continue
        if path in _RECOVERY_DRAIN_REGISTRY_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _RECOVERY_DRAIN_REGISTRY_SUITES:
                add(invocation)
            continue
        if path in _RECOVERY_PHASES_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _RECOVERY_PHASES_SUITES:
                add(invocation)
            continue
        if path in _ENRICHMENT_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _ENRICHMENT_SUITES:
                add(invocation)
            continue
        if path in _PROFILE_BATCH_DIVISION_CONTRACT_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _PROFILE_BATCH_DIVISION_CONTRACT_SUITES:
                add(invocation)
            continue
        if path in _MODEL_PROVIDER_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _MODEL_PROVIDER_SUITES:
                add(invocation)
            continue
        if path == "src/sourcing_agent/orchestrator.py":
            # run_worker_recovery_once and its deferred inline phase clusters
            # still live in orchestrator.py (Phase 4 Step 2 was partial), so an
            # edit here can drift the pinned recovery phase sequence — the tick
            # oracle must be selected for orchestrator.py too, not only for
            # recovery_phases.py. Additive (no continue): orchestrator.py also
            # gets its standard orchestrator+results suites below.
            saw_backend_change = True
            for invocation in _RECOVERY_PHASES_SUITES:
                add(invocation)
        if path in _ORCHESTRATOR_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _ORCHESTRATOR_AND_RESULTS_SUITES:
                add(invocation)
            continue
        matched_explain = False
        if path in _WORKFLOW_EXPLAIN_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _WORKFLOW_EXPLAIN_SUITES:
                add(invocation)
            matched_explain = True
        if path in _WORKFLOW_SMOKE_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _WORKFLOW_EXPLAIN_SUITES:
                add(invocation)
            for invocation in _WORKFLOW_SMOKE_SUITES:
                add(invocation)
            for invocation in _SCRIPTED_PROVIDER_SUITES:
                add(invocation)
            continue
        if matched_explain:
            continue
        if path in _EXCEL_INTAKE_RELATED_PATHS:
            saw_backend_change = saw_backend_change or path.startswith("src/")
            for invocation in _EXCEL_INTAKE_SUITES:
                add(invocation)
            if path.startswith("frontend-demo/"):
                for invocation in _FRONTEND_CONTRACT_SUITES:
                    add(invocation)
            continue
        if path in _ASSET_GOVERNANCE_RELATED_PATHS:
            saw_backend_change = True
            for invocation in _ASSET_GOVERNANCE_SUITES:
                add(invocation)
            continue
        if path in _PUBLIC_WEB_RELATED_PATHS:
            saw_backend_change = saw_backend_change or path.startswith("src/")
            for invocation in _PUBLIC_WEB_SUITES:
                add(invocation)
            if path.startswith("frontend-demo/"):
                for invocation in _FRONTEND_CONTRACT_SUITES:
                    add(invocation)
            continue
        if path in _PRODUCT_JOURNEY_RELATED_PATHS:
            saw_backend_change = saw_backend_change or path.startswith("src/")
            for invocation in _PRODUCT_JOURNEY_SUITES:
                add(invocation)
            continue
        if path.startswith("configs/scripted/"):
            saw_backend_change = True
            for invocation in _SCRIPTED_PROVIDER_SUITES:
                add(invocation)
            continue
        if any(path.startswith(prefix) for prefix in _FRONTEND_RELATED_PREFIXES):
            for invocation in _FRONTEND_CONTRACT_SUITES:
                add(invocation)
            continue
        if path.startswith("tests/") and path.endswith(".py"):
            add(
                PytestInvocation(
                    label=f"direct::{path}",
                    args=(path,),
                    reason="directly changed test module",
                )
            )
            continue
        if path.startswith("src/sourcing_agent/") and path.endswith(".py"):
            saw_backend_change = True
            candidate_test_path = f"tests/test_{Path(path).stem}.py"
            if root is not None and (root / candidate_test_path).exists():
                add(
                    PytestInvocation(
                        label=f"paired::{candidate_test_path}",
                        args=(candidate_test_path,),
                        reason=f"paired test for {Path(path).name}",
                    )
                )
            else:
                for invocation in smoke_pytest_invocations():
                    add(invocation)
            continue

    if selected:
        return list(selected.values())
    if saw_backend_change:
        return smoke_pytest_invocations()
    return smoke_pytest_invocations()


def durations_pytest_args(*, top: int = 25, min_seconds: float = 0.5) -> tuple[str, ...]:
    normalized_top = max(1, int(top or 25))
    normalized_min_seconds = max(0.0, float(min_seconds or 0.0))
    return (
        "-q",
        f"--durations={normalized_top}",
        f"--durations-min={normalized_min_seconds}",
    )
