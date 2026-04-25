# 2026-04-25 Productization Tracker

> Status: Active tracker for the post-green productization pass. After context compaction, read this file plus `../PROGRESS.md` and `NEXT_TODO.md` before running commands.

## Scope

This pass continues from the green baseline:

- full repo `pytest -q`: `1105 passed, 8 skipped, 25 subtests passed`
- repo typecheck: green

The goal is to turn the remaining TODOs into executable contracts, not one-off fixes.

## Active Workstreams

- [x] Runner / recovery production ownership
  - Add explicit shutdown/cancel contract for runtime background services and job-owned workers.
  - Keep hosted workflow dispatch/recovery ownership single-owner and observable.
- [x] Multi-snapshot promoted aggregate coverage
  - Replace `source_snapshot_selection.mode` proxy with an explicit promoted aggregate coverage contract.
  - Keep front-end labels and back-end execution semantics aligned.
- [x] Harvest / materialization performance
  - Continue from worker-completion incremental ingest toward tighter coalescing and writer budget.
  - Add large snapshot benchmark/report fields before deeper optimization.
- [x] Scripted provider scenarios
  - Expand fixtures for retryable errors, partial results, timeout/pending rounds, and batch ready/fetch.
  - Promote behavior reports into high-signal regression gates.
- [x] Asset governance productization
  - Standardize canonical/default pointers, replacement lifecycle, and historical retention.
- [x] SQLite legacy surface
  - Keep live/hosted PG-only.
  - Add explicit legacy banners and migration exits for SQLite tooling.
- [x] Excel intake enhancements
  - Add throughput benchmark and continuation/export/target-candidate follow-up gates.
- [x] Product journey regression matrix
  - Cover supplement/manual-review writeback, concurrency isolation, exports, timezone sorting, and SearchPage orchestration.
- [x] Session closeout quality pass
  - Review whether the testing system reflects user-relevant workflow quality, not only "tests passed".
  - Run scripted/simulated/browser/full validation and fix any regressions found.
  - Review all Markdown docs for freshness, especially environment, ECS deploy/ops, PG-only/runtime isolation, frontend/backend startup, and testing commands.
  - Review `AGENTS.md` and development-habit docs; record root-cause discipline, no dual-track implementations, shared abstractions, TODO/progress update habits, and version-regression prevention.
  - Create a new branch for the current stabilized version, commit intended files only, and push to GitHub.
  - Produce a session handoff summary so the next session can resume without relying on compressed chat context.

## Guardrails

- Do not reintroduce default Public Web Stage 2.
- Do not reintroduce live/hosted SQLite fallback.
- Do not convert small supplemental/import snapshots into authoritative coverage baselines without proof.
- Do not restore blank former profile search for scoped keyword cases.
- Do not use special-case provider behavior when a shared execution contract is possible.
- After context compaction, read this tracker, `../PROGRESS.md`, and `NEXT_TODO.md` before running commands.
- Do not use `git add .` blindly; the worktree contains generated/runtime/vendor artifacts that must not be pushed.

## Session Closeout Checklist

These items were added before the final validation/documentation/push pass so they do not get lost after context compaction.

- [x] Testing-system quality review
  - Confirm scripted/simulate reports cover duplicate provider dispatch, wrong provider calls, default-off stage violations, prerequisite-to-downstream gaps, provider-response-to-materialization gaps, board readiness lag, candidate-count consistency, retrieval strategy correctness, and frontend hydration stability.
  - If the report/test matrix is missing a user-relevant metric, add the metric or document the remaining gap explicitly.
- [x] Validation pass
  - Run high-signal scripted provider and workflow smoke tests.
  - Run simulated test-environment workflow matrix in an isolated runtime.
  - Run browser E2E for frontend flows that are available locally.
  - Run frontend production build, repo typecheck, Markdown status test, and full pytest.
  - Fix regressions at the shared contract/root-cause level before continuing.
- [x] Markdown review pass
  - Review every `*.md` file except generated/vendor output.
  - Update stale guidance around ECS deployment, runtime isolation, PG-only operation, virtualenv usage, frontend/backend startup, testing commands, and known pitfalls.
  - Keep `PROGRESS.md`, `NEXT_TODO.md`, and this tracker aligned with actual verification results.
- [x] Development-habit docs
  - Update `AGENTS.md` and related development docs with lessons from this session:
    - read trackers after context compaction
    - identify upstream root causes before patching symptoms
    - avoid dual-track systems unless there is a short-lived migration gate
    - prefer shared abstractions/modules over one-off fixes
    - update TODO/progress docs while working
    - preserve validated provider/workflow strategies and prevent accidental version rollback
- [x] GitHub handoff
  - Inspect branch/remotes and `.gitignore`.
  - Create a new branch for the stabilized version.
  - Stage intended code/docs/config/test files carefully, excluding runtime/cache/vendor/build output unless intentionally tracked.
  - Commit and push the branch to GitHub.
  - Pushed branch: `productization-2026-04-25-stable`
  - Commits:
    - `ef1ea4c Stabilize productization workflow contracts`
    - `53d30f1 Add workspace development guardrails`
- [x] Session handoff
  - Summarize current branch/commit, validation results, remaining risks, environment commands, and next-session resume steps.

## Progress Log

- 2026-04-25: Tracker created before implementation. Source of truth read: `PROGRESS.md`, `docs/NEXT_TODO.md`, and `docs/SESSION_TRACKER_2026-04-24_LONG_TAIL.md`.
- 2026-04-25: First productization slice completed:
  - Runner / recovery now has an explicit cooperative shutdown contract:
    - `service_daemon.py` persists `services/<service_name>/stop_request.json`
    - `WorkerDaemonService` observes matching stop requests between ticks and during idle sleep
    - `POST /api/runtime/services/shutdown` requests service shutdown without killing arbitrary PIDs
    - `POST /api/jobs/{job_id}/cancel` marks workflow jobs terminal, retires active workers, releases the workflow lease, and requests the job-scoped recovery service stop
    - job cancel intentionally does not stop shared recovery unless explicitly requested
  - Multi-snapshot coverage proof no longer treats `source_snapshot_selection.mode` as proof:
    - new shared helper: `source_snapshot_coverage.py`
    - `preferred_snapshot_subset` alone does not unlock population-default reuse
    - explicit `coverage_proof` / promoted aggregate contract is required for multi-snapshot authoritative aggregate reuse
  - Frontend/backend plan strategy labels now share backend semantics:
    - `compile_execution_semantics(...)` emits `execution_strategy_label`
    - frontend `PlanCard` mapping prefers this backend label before local inference
  - SQLite legacy surface gained a CLI banner:
    - `show-control-plane-runtime` now includes `legacy_sqlite_banner`
    - non-PG runtime is explicitly marked as legacy/emergency with migration exit instructions
  - Verification completed:
    - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/service_daemon.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/source_snapshot_coverage.py src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/organization_execution_profile.py`
    - `./.venv-tests/bin/python -m pytest tests/test_service_daemon.py -q`
    - `./.venv-tests/bin/python -m pytest tests/test_planning_modules.py -q -k 'multi_snapshot_subset or multi_snapshot_historical'`
    - `./.venv-tests/bin/python -m pytest tests/test_organization_execution_profile.py -q -k 'coverage_baseline or multi_snapshot_selection or promoted_aggregate'`
    - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'cancel_workflow_job_marks_job_terminal or light_routes_are_classified'`
    - `./.venv-tests/bin/python -m pytest tests/test_execution_semantics.py -q`
    - `npm run build`
    - `./.venv-tests/bin/python -m pytest tests/test_cli.py -q -k 'legacy_sqlite_runtime_banner'`
- 2026-04-25: Second productization slice completed:
  - Harvest / materialization reporting now has executable streaming and writer-budget contracts:
    - `runtime_tuning.py` exposes `materialization_coalescing_window_ms`
    - `build_materialization_streaming_budget_report(...)` reports provider-response streaming readiness, pending delta count, coalescing action, and writer-slot availability
    - running job inline incremental sync now enters a shared `materialization_writer` in-flight slot before rebuilding candidate artifacts
    - sync results include writer-slot and streaming-budget metadata
    - `workflow_smoke.py` includes `materialization_streaming` and aggregate `streaming_materialization` guardrail metrics in provider reports
  - Scripted provider scenarios now have first-class coverage validation:
    - `summarize_scripted_provider_scenario(...)`
    - `validate_scripted_provider_scenario(...)`
    - `configs/scripted/provider_behavior_matrix.json` covers retryable 429, timeout, partial result, and staged ready/fetch behavior
  - Asset governance productization gained a shared default-pointer contract:
    - `asset_governance.py`
    - canonical replacement plans supersede the previous default pointer and retain historical snapshot ids
    - `partial/draft/empty/superseded/archived` are rejected as default canonical pointers
  - Excel intake now emits a throughput/follow-up plan:
    - company grouping
    - direct LinkedIn profile fetch batch plan
    - search-required count
    - row-level continuation support
    - target-candidate / export linkage gates
  - Product regression matrix now maps productized surfaces to high-signal tests:
    - scripted provider scenarios
    - workflow smoke behavior report
    - Excel intake product flow
    - asset governance
    - product journey supplement/manual-review/results/frontend build
  - Verification completed:
    - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/runtime_tuning.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/scripted_provider_scenario.py src/sourcing_agent/asset_governance.py src/sourcing_agent/excel_intake.py src/sourcing_agent/regression_matrix.py`
    - `./.venv-tests/bin/python -m pytest tests/test_runtime_tuning.py tests/test_scripted_provider_scenario.py tests/test_asset_governance.py -q`
    - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py -q -k 'provider_case_report or summarize_smoke_timings'`
    - `./.venv-tests/bin/python -m pytest tests/test_excel_intake.py -q -k 'throughput_plan or local_exact_hit_and_local_manual_review'`
    - `./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py -q`
    - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or reconcile_completed_workflow_after_background_harvest_prefetch or reconcile_completed_workflow_after_background_company_roster'`
    - `./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q`
    - `bash ./scripts/run_python_quality.sh typecheck`
    - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover'`
- 2026-04-25: Third productization slice completed:
  - Asset governance now has persisted writers instead of helper-only contracts:
    - SQLite/PG control-plane tables: `asset_default_pointers`, `asset_default_pointer_history`
    - storage writer: `promote_asset_default_pointer(...)`
    - API writer/list routes:
      - `POST /api/assets/governance/promote-default`
      - `GET /api/assets/governance/default-pointers`
    - CLI writer:
      - `promote-asset-default-pointer`
    - non-canonical lifecycle remains rejected before persistence
  - Excel intake target/export linkage is now executable:
    - `POST /api/target-candidates/import-from-job` imports completed workflow candidates into target candidates
    - Excel group cards expose `导入目标候选人` and `导出候选人包`
    - export action reuses existing profile-bundle archive endpoint
  - Provider response-level streaming moved into the live adapter contract:
    - `HarvestProfileConnector.fetch_profiles_by_urls(..., on_batch_result=...)`
    - enrichment live fetch consumes batch callbacks to update fetched cache and profile registry before the whole chunk returns
  - Materialization writer budget/backpressure is now safer across nested paths:
    - `runtime_inflight_slot(...)` is reentrant per thread/lane/budget
    - snapshot candidate-document synchronization enters shared `materialization_writer` budget
  - Verification completed:
    - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_governance.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/cli.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/enrichment.py src/sourcing_agent/runtime_tuning.py src/sourcing_agent/snapshot_materializer.py`
    - `./.venv-tests/bin/python -m pytest tests/test_asset_governance.py tests/test_runtime_tuning.py tests/test_harvest_connectors.py tests/test_results_api.py tests/test_excel_intake.py tests/test_regression_matrix.py -q`
    - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or reconcile_completed_workflow_after_background_harvest_prefetch or reconcile_completed_workflow_after_background_company_roster or snapshot_materializer_reuses_normalize_artifacts or pre_retrieval_refresh_uses_foreground_fast'`
    - `npm run build`
    - `bash ./scripts/run_python_quality.sh typecheck`
    - `./.venv-tests/bin/python -m pytest -q` -> `1134 passed, 8 skipped, 25 subtests passed`
- 2026-04-25: Closeout validation/documentation/GitHub handoff pass completed:
  - Fixed async plan history recovery semantics:
    - frontend history metadata now persists `dispatch_preview`, `lane_preview`, `asset_reuse_plan`, `organization_execution_profile`, `request_preview`, and `effective_execution_semantics`
    - frontend history recovery maps those fields back into the plan card
    - `full_company_roster` now displays as `全量 live roster` for fresh live acquisition
  - Verified:
    - `./.venv-tests/bin/python -m pytest tests/test_execution_semantics.py tests/test_frontend_history_recovery.py -q`
    - `cd frontend-demo && npm run build`
    - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q` -> `5 passed, 2 skipped, 2 subtests passed`
    - `./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q`
    - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
    - `bash ./scripts/run_python_quality.sh typecheck`
    - `./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q`
    - isolated simulate smoke matrix with strict timing summary
    - scripted Google long-tail strict smoke
    - `./.venv-tests/bin/python -m pytest -q` -> `1135 passed, 8 skipped, 25 subtests passed`
  - Reviewed Markdown status across 54 first-party Markdown files and updated:
    - `AGENTS.md`
    - `README.md`
    - `docs/DEVELOPMENT_GUIDE.md`
    - `docs/TESTING_PLAYBOOK.md`
    - `docs/WORKFLOW_BEHAVIOR_GUARDRAILS.md`
    - `docs/ECS_ACCESS_PLAYBOOK.md`
    - `docs/ECS_PRELAUNCH_CHECKLIST.md`
    - `docs/ALIYUN_ECS_TRIAL_ROLLOUT.md`
    - `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`
    - `docs/INDEX.md`
    - `docs/SESSION_HANDOFF_2026-04-25.md`
  - GitHub handoff:
    - branch: `productization-2026-04-25-stable`
    - pushed to `origin/productization-2026-04-25-stable`
    - excluded runtime/cache/vendor/build output, generated `object_sync`, and `frontend-demo/public/tml` offline assets from the commit

## Remaining After First Slice

- Runner / recovery:
  - shutdown is cooperative and observable, but not yet a full process-manager cancel protocol
  - still worth adding service stop status polling and a hard-timeout escalation hook for managed deployments
- Multi-snapshot coverage:
  - explicit proof contract is now enforced at planner/profile gates
  - still need a promotion-time writer that stamps `coverage_proof` automatically when an aggregate is promoted
- Harvest / materialization:
  - first worker-completion incremental ingest remains intact
  - report-level time-window coalescing / writer-budget contract is now executable
  - provider batch response callback is now live in the adapter and consumed by enrichment
  - remaining work is deeper partial-dataset streaming from provider APIs if Harvest exposes it, plus real large-snapshot benchmark expansion
- Scripted provider scenarios:
  - current guardrail suite covers major smoke behavior
  - 429 / timeout / partial result / staged ready-fetch scenario coverage now has a validation helper and fixture
  - still need more quality recall baselines and CI promotion breadth
- Asset governance:
  - lifecycle gates exist
  - default pointer / canonical replacement / historical retention plan helper exists
  - DB/CLI/API writers now persist default pointers and history as first-class control-plane objects
  - remaining work is product UI for governance review and automated promotion-time coverage-proof stamping
- Excel intake:
  - upload and multi-company split are enabled
  - throughput/follow-up plan is now emitted by backend intake
  - target candidates / export action wiring is now live for completed Excel groups
  - remaining work is line-level continuation UI and large-batch UX/throughput benchmarking
- Product journey matrix:
  - high-signal matrix entries now cover supplement/manual-review/results/frontend build surfaces
  - remaining work is broader concurrency isolation, CSV/profile bundle export behavior tests, timezone sorting, and deeper SearchPage orchestration split tests
