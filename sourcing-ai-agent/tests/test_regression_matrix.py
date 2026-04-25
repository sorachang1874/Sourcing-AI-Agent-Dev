from __future__ import annotations

from pathlib import Path

from sourcing_agent.regression_matrix import (
    durations_pytest_args,
    infer_pytest_invocations,
    normalize_changed_paths,
    smoke_pytest_invocations,
)


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_normalize_changed_paths_dedupes_and_normalizes() -> None:
    assert normalize_changed_paths(["./src/sourcing_agent/storage.py", "src\\sourcing_agent\\storage.py"]) == [
        "src/sourcing_agent/storage.py"
    ]


def test_infer_pytest_invocations_for_storage_changes_selects_high_signal_suites() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/storage.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "control-plane-live" in labels
    assert "control-plane-postgres" in labels
    assert "candidate-artifacts-focus" in labels
    assert "pipeline-materialization-focus" in labels


def test_infer_pytest_invocations_for_cloud_import_changes_selects_import_suites() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/cloud_asset_import.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "asset-sync-cloud-import" in labels
    assert "control-plane-postgres" in labels


def test_infer_pytest_invocations_for_frontend_changes_selects_frontend_contract_suites() -> None:
    invocations = infer_pytest_invocations(
        ["frontend-demo/src/App.tsx"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert labels == {"frontend-contract-focus", "frontend-build"}


def test_infer_pytest_invocations_for_workflow_explain_changes_selects_explain_suite() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/workflow_explain_matrix.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "workflow-explain-focus" in labels
    assert "hosted-workflow-smoke-focus" in labels


def test_infer_pytest_invocations_for_orchestrator_changes_selects_orchestrator_matrix() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/orchestrator.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "control-plane-live" in labels
    assert "pipeline-orchestrator-focus" in labels
    assert "results-api-focus" in labels
    assert "workflow-explain-focus" in labels
    assert "hosted-workflow-smoke-focus" not in labels


def test_infer_pytest_invocations_for_scripted_provider_changes_selects_behavior_suites() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/scripted_provider_scenario.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "scripted-provider-scenarios" in labels
    assert "workflow-smoke-behavior-report" in labels


def test_infer_pytest_invocations_for_excel_intake_changes_selects_product_flow() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/excel_intake.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "excel-intake-product-flow" in labels


def test_infer_pytest_invocations_for_asset_governance_changes_selects_governance_suite() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/asset_governance.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "asset-governance-contract" in labels


def test_infer_pytest_invocations_for_product_journey_frontend_changes_selects_journey_and_build() -> None:
    invocations = infer_pytest_invocations(
        ["frontend-demo/src/components/TargetCandidatesPanel.tsx"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "product-journey-regression" in labels
    assert "frontend-build" in labels


def test_infer_pytest_invocations_defaults_to_smoke_when_no_changes_are_known() -> None:
    invocations = infer_pytest_invocations([], repo_root=_repo_root())
    assert invocations == smoke_pytest_invocations()


def test_durations_pytest_args_are_stable() -> None:
    assert durations_pytest_args(top=10, min_seconds=1.25) == (
        "-q",
        "--durations=10",
        "--durations-min=1.25",
    )
