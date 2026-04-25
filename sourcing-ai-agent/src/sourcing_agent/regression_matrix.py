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
    PytestInvocation(
        label="pipeline-materialization-focus",
        args=(
            "tests/test_pipeline.py",
            "-k",
            "(materialization or generation or shard or registry) and not queue_workflow",
        ),
        reason="pipeline generation and registry integration",
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
        label="pipeline-orchestrator-focus",
        args=(
            "tests/test_pipeline.py",
            "-k",
            (
                "manual_review or plan_review or query_dispatch or agent_runtime or organization_asset_registry "
                "or execution_profile or frontend_history"
            ),
        ),
        reason="orchestrator workflow integration and control-plane handoff",
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
        args=("tests/test_asset_governance.py", "tests/test_organization_execution_profile.py", "-k", "coverage_baseline or lifecycle or promoted"),
        reason="canonical/default pointer, lifecycle, promotion, and coverage baseline governance",
    ),
)

_PRODUCT_JOURNEY_SUITES = (
    PytestInvocation(
        label="product-journey-regression",
        args=(
            "tests/test_results_api.py",
            "tests/test_frontend_history_recovery.py",
            "tests/test_company_asset_supplement.py",
            "tests/test_pipeline.py",
            "-k",
            "manual_review or supplement or target_candidate or export",
        ),
        reason="results, supplement/manual-review writeback, and frontend history product flow",
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
_ORCHESTRATOR_RELATED_PATHS = {
    "src/sourcing_agent/orchestrator.py",
    "src/sourcing_agent/api.py",
    "src/sourcing_agent/workflow_refresh.py",
    "src/sourcing_agent/workflow_submission.py",
    "src/sourcing_agent/manual_review.py",
    "src/sourcing_agent/plan_review.py",
    "src/sourcing_agent/results_store.py",
}
_EXCEL_INTAKE_RELATED_PATHS = {
    "src/sourcing_agent/excel_intake.py",
    "frontend-demo/src/components/ExcelWorkflowIntakePanel.tsx",
    "frontend-demo/scripts/run_excel_intake_e2e.mjs",
}
_ASSET_GOVERNANCE_RELATED_PATHS = {
    "src/sourcing_agent/asset_governance.py",
    "src/sourcing_agent/asset_registration.py",
    "src/sourcing_agent/organization_assets.py",
    "src/sourcing_agent/organization_execution_profile.py",
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
            add(_STORAGE_AND_CONTROL_PLANE_SUITES[2])
            add(_STORAGE_AND_CONTROL_PLANE_SUITES[3])
            continue
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
