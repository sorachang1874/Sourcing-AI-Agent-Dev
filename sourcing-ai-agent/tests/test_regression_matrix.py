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


# Source modules that currently have NO focused regression mapping: for these,
# infer_pytest_invocations falls back to the generic smoke list, which is
# silent coverage loss. New src/sourcing_agent modules MUST be mapped (via an
# explicit *_RELATED_PATHS entry in regression_matrix.py or a paired
# tests/test_<module>.py file) — do NOT extend this list; shrink it.
KNOWN_UNMAPPED_LEGACY = (
    "src/sourcing_agent/acquisition.py",
    "src/sourcing_agent/acquisition_strategy.py",
    "src/sourcing_agent/asset_consolidation_plan.py",
    "src/sourcing_agent/asset_consolidation_repair_proposal.py",
    "src/sourcing_agent/asset_coverage_contracts.py",
    "src/sourcing_agent/asset_policy.py",
    "src/sourcing_agent/authoritative_candidates.py",
    "src/sourcing_agent/candidate_materialization.py",
    "src/sourcing_agent/company_asset_writer.py",
    "src/sourcing_agent/connectors.py",
    "src/sourcing_agent/control_plane_job_progress.py",
    "src/sourcing_agent/crm_migration.py",
    "src/sourcing_agent/crm_writer.py",
    "src/sourcing_agent/domain.py",
    "src/sourcing_agent/execution_preferences.py",
    "src/sourcing_agent/ingestion.py",
    "src/sourcing_agent/legacy_target_candidate_public_web_runtime.py",
    "src/sourcing_agent/media_asset_owner.py",
    "src/sourcing_agent/person_asset_writer.py",
    "src/sourcing_agent/person_identity.py",
    "src/sourcing_agent/planning.py",
    "src/sourcing_agent/process_supervision.py",
    "src/sourcing_agent/public_web_signal_identity.py",
    "src/sourcing_agent/publication_planning.py",
    "src/sourcing_agent/recovery_contract.py",
    "src/sourcing_agent/request_normalization.py",
    "src/sourcing_agent/retrieval_runtime.py",
    "src/sourcing_agent/search_seed_registry.py",
    "src/sourcing_agent/service_gate_coverage.py",
    "src/sourcing_agent/serving_projection_migration.py",
    "src/sourcing_agent/serving_projection_reader.py",
    "src/sourcing_agent/smoke_expectation_contract.py",
    "src/sourcing_agent/snapshot_materializer.py",
    "src/sourcing_agent/source_snapshot_coverage.py",
    "src/sourcing_agent/web_fetch.py",
)


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
    # god-file focus retired with tests/test_pipeline.py (T-008, 2026-07-22)


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
    # god-file focus retired with tests/test_pipeline.py (T-008, 2026-07-22)
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


def test_infer_pytest_invocations_for_public_web_changes_selects_public_web_suite() -> None:
    invocations = infer_pytest_invocations(
        ["src/sourcing_agent/public_web_search.py"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    assert "public-web-core" in labels
    assert "crm-public-web-runtime-boundary" in labels
    assert "target-candidate-public-web-pg" in labels


def test_infer_pytest_invocations_for_product_journey_frontend_changes_selects_journey_and_build() -> None:
    invocations = infer_pytest_invocations(
        ["frontend-demo/src/components/TargetCandidatesPanel.tsx"],
        repo_root=_repo_root(),
    )
    labels = {invocation.label for invocation in invocations}
    # god-file journey suite retired with tests/test_pipeline.py (T-008, 2026-07-22)
    assert "frontend-build" in labels


def test_orchestrator_change_selects_recovery_tick_oracle() -> None:
    # The recovery tick (run_worker_recovery_once + its deferred inline phase
    # clusters) still lives in orchestrator.py after the partial Phase 4 Step 2,
    # so an edit there must route to the tick characterization oracle — not only
    # changes to recovery_phases.py. Regression lock for the async-review catch
    # (2026-06-14): the oracle's safety net must be connected to the file it
    # actually guards.
    labels = {
        invocation.label
        for invocation in infer_pytest_invocations(
            ["src/sourcing_agent/orchestrator.py"],
            repo_root=_repo_root(),
        )
    }
    assert "paired::tests/test_recovery_tick_characterization.py" in labels
    # ...and still keeps its standard orchestrator+results coverage.
    assert any("results" in label or "orchestrator" in label.lower() for label in labels)


def test_enrichment_change_selects_batch_division_characterization_oracle() -> None:
    # WS7 ruling-① (2026-07-23): the fetch-profile batch-division ladder in
    # enrichment.py is pinned by the plan-record characterization oracle; an
    # enrichment edit must route to the oracle and the scheduler contract guards,
    # not only the paired suite (recovery_phases -> tick-oracle precedent).
    labels = {
        invocation.label
        for invocation in infer_pytest_invocations(
            ["src/sourcing_agent/enrichment.py"],
            repo_root=_repo_root(),
        )
    }
    assert "paired::tests/test_fetch_profile_batch_characterization.py" in labels
    assert "paired::tests/test_profile_prefetch_scheduler_contract.py" in labels
    assert "paired::tests/test_enrichment.py" in labels


def test_profile_batch_division_contract_change_selects_contract_suite_and_oracle() -> None:
    # WS7/W7.2 S1 (2026-07-23): the divider-output contract module's validator
    # battery formalizes the ladder rules the plan-record oracle pins, so an
    # edit there must route to BOTH the paired contract suite and the oracle
    # (docs/WS7_AI_BATCH_DIVIDER_DESIGN.md §1.3).
    labels = {
        invocation.label
        for invocation in infer_pytest_invocations(
            ["src/sourcing_agent/profile_batch_division_contract.py"],
            repo_root=_repo_root(),
        )
    }
    assert "paired::tests/test_profile_batch_division_contract.py" in labels
    assert "paired::tests/test_fetch_profile_batch_characterization.py" in labels


def test_profile_batch_division_helper_change_selects_the_full_divider_family() -> None:
    # WS7/W7.2 S2 (2026-07-23): the orchestration helper rides the same family
    # as the contract module — contract suite + model-surface suite + oracle.
    labels = {
        invocation.label
        for invocation in infer_pytest_invocations(
            ["src/sourcing_agent/profile_batch_division.py"],
            repo_root=_repo_root(),
        )
    }
    assert "paired::tests/test_profile_batch_division_contract.py" in labels
    assert "paired::tests/test_profile_batch_division_model_surface.py" in labels
    assert "paired::tests/test_fetch_profile_batch_characterization.py" in labels


def test_model_provider_change_selects_protocol_and_divider_surface_suites() -> None:
    # WS7/W7.2 S2 (2026-07-23): explicit mapping replaces the generic paired
    # fallback that silently missed the v1 protocol characterization pins.
    labels = {
        invocation.label
        for invocation in infer_pytest_invocations(
            ["src/sourcing_agent/model_provider.py"],
            repo_root=_repo_root(),
        )
    }
    assert "paired::tests/test_model_provider.py" in labels
    assert "paired::tests/test_model_client_v1_characterization.py" in labels
    assert "paired::tests/test_profile_batch_division_model_surface.py" in labels


def test_recovery_phases_change_selects_recovery_tick_oracle() -> None:
    labels = {
        invocation.label
        for invocation in infer_pytest_invocations(
            ["src/sourcing_agent/recovery_phases.py"],
            repo_root=_repo_root(),
        )
    }
    assert "paired::tests/test_recovery_tick_characterization.py" in labels


def test_infer_pytest_invocations_defaults_to_smoke_when_no_changes_are_known() -> None:
    invocations = infer_pytest_invocations([], repo_root=_repo_root())
    assert invocations == smoke_pytest_invocations()


def _unmapped_source_modules(root: Path) -> list[str]:
    """Source files whose only inference result is the generic smoke fallback."""
    smoke = smoke_pytest_invocations()
    unmapped: list[str] = []
    for source_path in sorted((root / "src" / "sourcing_agent").rglob("*.py")):
        if source_path.name == "__init__.py" or "__pycache__" in source_path.parts:
            continue
        relative = source_path.relative_to(root).as_posix()
        invocations = infer_pytest_invocations([relative], repo_root=root)
        if not invocations or invocations == smoke:
            unmapped.append(relative)
    return unmapped


def test_known_unmapped_legacy_allowlist_is_sorted_and_unique() -> None:
    assert list(KNOWN_UNMAPPED_LEGACY) == sorted(set(KNOWN_UNMAPPED_LEGACY))


def test_every_source_module_maps_to_focused_suites_or_is_known_legacy() -> None:
    unmapped = _unmapped_source_modules(_repo_root())

    newly_unmapped = sorted(set(unmapped) - set(KNOWN_UNMAPPED_LEGACY))
    assert not newly_unmapped, (
        "These src/sourcing_agent modules trigger no focused regression suites "
        "(only the generic smoke fallback). Map each one in "
        "src/sourcing_agent/regression_matrix.py (a *_RELATED_PATHS entry) or add a "
        "paired tests/test_<module>.py. Do NOT add them to KNOWN_UNMAPPED_LEGACY: "
        f"{newly_unmapped}"
    )

    stale_allowlist = sorted(set(KNOWN_UNMAPPED_LEGACY) - set(unmapped))
    assert not stale_allowlist, (
        "These KNOWN_UNMAPPED_LEGACY entries are now mapped (or deleted) — remove "
        f"them from the allowlist so it only shrinks: {stale_allowlist}"
    )


def test_durations_pytest_args_are_stable() -> None:
    assert durations_pytest_args(top=10, min_seconds=1.25) == (
        "-q",
        "--durations=10",
        "--durations-min=1.25",
    )
