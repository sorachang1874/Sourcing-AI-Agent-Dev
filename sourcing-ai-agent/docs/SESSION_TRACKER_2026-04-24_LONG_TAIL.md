# 2026-04-24 Long-Tail Hardening Tracker

> Status: Closed tracker. Use as the source of truth for this hardening pass and regression commands.

> Active tracker for the post-stabilization hardening pass. After any context compact or session resume, read this file plus `../PROGRESS.md` before running commands.

## Scope

This pass closes the remaining architectural tails that can otherwise regress into dual-track behavior, duplicated provider calls, opaque workflow ownership, or slow cold materialization.

## Checklist

- [x] Runner / recovery ownership
  - Goal: one clear owner for hosted workflow dispatch, recovery takeover, job lease release, and visible runtime health.
  - Expected guardrail: recovery must not duplicate a fresh runner dispatch, and a queued job with a live dispatch marker must not be auto-taken over prematurely.
- [x] Plan hydration request-signature dedupe
  - Goal: coalesce background plan hydration by normalized request signature, not only by `history_id`.
  - Expected guardrail: repeated same-query submissions share one in-flight hydration and expose queue/running/wait metadata.
- [x] Runtime independent PG/schema bootstrap
  - Goal: production, local dev, and test/scripted runtimes must have explicit PG database/schema bootstrap boundaries.
  - Expected guardrail: test/scripted/replay never silently inherit production/repo PG; production remains PG-only unless an explicit emergency override is used.
- [x] Harvest global in-flight budget / backpressure
  - Goal: provider calls, background prefetch, and materialization writers share one runtime budget contract instead of each lane inventing its own limits.
  - Expected guardrail: no duplicate provider dispatch, no unsafe high-concurrency profile scraping, and no stage barrier waiting for unrelated work.
- [x] Asset governance / promotion
  - Goal: promotion, canonical pointers, lane coverage, and planner reuse eligibility share executable governance rules.
  - Expected guardrail: tiny Excel/import/supplemental snapshots can enrich assets but cannot become coverage baselines without proof.
- [x] Cold materialization optimization
  - Goal: reduce `prepare_candidates`, payload build, and view write hotspots before optimizing PG state upsert.
  - Expected guardrail: benchmark reports separate provider time from materialization time and expose cold/hot deltas.
- [x] SQLite compatibility surface cleanup
  - Goal: live/production paths are PG-only; SQLite remains explicit legacy/dev portability only.
  - Expected guardrail: no silent SQLite fallback in hosted/live mode.

## Session Notes

- 2026-04-24: Created tracker before implementation. Initial source of truth read: `PROGRESS.md`, `docs/NEXT_TODO.md`, and existing runtime/workflow guardrail docs.
- 2026-04-24: Request-signature plan hydration dedupe, PG schema bootstrap, Harvest global in-flight budgets, and asset lifecycle promotion gates were verified in code and tests.
- 2026-04-24: Cold materialization now avoids one redundant per-candidate normalize pass, caches profile/source JSON by file signature, builds a reusable selector index for multi-item source files, and routes materialization write phases through the shared global writer budget.
- 2026-04-24: Production runtime now requires PG by runtime contract, not only by startup script. `SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE=1` is the explicit emergency escape hatch; otherwise production cannot silently drift to disk SQLite when DSN env is missing.
- 2026-04-24: Small artifact benchmark written to `output/candidate_artifact_benchmark_long_tail_20260424.json`. On 240 synthetic candidates, optimized cold full was `3288.54ms` vs legacy-like `4530.96ms` (`1.38x`), and optimized incremental patch was `1811.73ms` vs `2001.92ms` (`1.10x`).
- 2026-04-24: Isolated scripted smoke report written to `output/scripted_smoke_current/google_long_tail_final_20260424_report.json`; strict summary reports duplicate provider dispatch `0`, unexpected public-web stage `0`, prerequisite gaps `0ms`, and final-results/board inconsistency `0`.
- 2026-04-25 Asia/Shanghai: Full-suite rerun exposed two hosted smoke long-tail races and both are now closed:
  - completed jobs with only nonblocking `harvest_profile_batch` workers must still continue recovery/reconcile; nonblocking means “do not block board readiness”, not “ignore profile tail”
  - snapshot sync / inline incremental apply now invalidates asset-population caches for that snapshot before subsequent results reads
  - hosted smoke teardown now waits for runtime-control / recovery / hydration / background materialization threads and tolerates transient `ENOENT` during tempdir cleanup

## Verification Log

- `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py tests/test_frontend_history_recovery.py`
- `./.venv-tests/bin/python -m pytest tests/test_frontend_history_recovery.py -q -k 'plan_hydration or plan_submit'`
- `./.venv-tests/bin/python -m py_compile src/sourcing_agent/local_postgres.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py`
- `./.venv-tests/bin/python -m pytest tests/test_local_postgres.py tests/test_runtime_environment.py -q`
- `./.venv-tests/bin/python -m py_compile src/sourcing_agent/runtime_tuning.py src/sourcing_agent/enrichment.py src/sourcing_agent/company_asset_completion.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition.py tests/test_runtime_tuning.py tests/test_enrichment.py`
- `./.venv-tests/bin/python -m pytest tests/test_runtime_tuning.py tests/test_enrichment.py -q -k 'global_inflight or bounded_parallel_live_batches or higher_parallelism_for_roster_heavy_live_batches'`
- `./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_reuse_planning.py tests/test_organization_execution_profile.py`
- `./.venv-tests/bin/python -m pytest tests/test_organization_execution_profile.py -q -k 'promotion or lifecycle or supplemental or candidate_inventory'`
- `./.venv-tests/bin/python -m py_compile src/sourcing_agent/profile_timeline.py src/sourcing_agent/candidate_artifacts.py tests/test_profile_timeline.py tests/test_candidate_artifacts.py`
- `./.venv-tests/bin/python -m pytest tests/test_profile_timeline.py tests/test_candidate_artifacts.py -q -k 'profile_snapshot or records_phase_timings or legacy_like_benchmark_flags or reuses_candidate_shards'`
- `./.venv-tests/bin/python -m py_compile src/sourcing_agent/storage.py tests/test_control_plane_live_postgres.py`
- `./.venv-tests/bin/python -m pytest tests/test_control_plane_live_postgres.py -q -k 'postgres_only_uses_ephemeral or require_control_plane_postgres or production_runtime_requires_postgres or production_sqlite_control_plane'`
- `./.venv-tests/bin/python scripts/run_candidate_artifact_benchmark.py --candidate-count 240 --dirty-candidates 24 --evidence-per-candidate 2 --repeat 1 --build-profile foreground_fast --env-file '' --report-json output/candidate_artifact_benchmark_long_tail_20260424.json`
- `./.venv-tests/bin/python -m pytest tests/test_frontend_history_recovery.py tests/test_local_postgres.py tests/test_runtime_environment.py tests/test_runtime_tuning.py tests/test_enrichment.py tests/test_organization_execution_profile.py tests/test_profile_timeline.py tests/test_candidate_artifacts.py tests/test_control_plane_live_postgres.py -q -k 'plan_hydration or plan_submit or schema or runtime_environment or global_inflight or bounded_parallel_live_batches or higher_parallelism_for_roster_heavy_live_batches or promotion or lifecycle or supplemental or candidate_inventory or profile_snapshot or records_phase_timings or legacy_like_benchmark_flags or reuses_candidate_shards or postgres_only_uses_ephemeral or require_control_plane_postgres or production_runtime_requires_postgres or production_sqlite_control_plane'`
  - result: `30 passed, 140 deselected`
- `./.venv-tests/bin/python -m pytest tests/test_pipeline.py tests/test_worker_recovery_daemon.py -q -k 'resume_queued_workflow_prefers_hosted_dispatch_when_requested or resume_queued_hosted_workflow_dispatches_without_holding_job_lock or start_workflow_dispatches_hosted_runner_before_recovery_daemons or start_hosted_workflow_thread_skips_duplicate_dispatch_with_fresh_marker or progress_auto_recovery_skips_fresh_hosted_dispatch_for_queued_job or release_stale_workflow_job_lease_for_recovery or acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_company_roster or reconcile_completed_workflow_after_background_harvest_prefetch or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or micro_batches_completed_harvest_prefetch_workers_and_syncs_once'`
  - result: `15 passed, 273 deselected`
- `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py tests/test_hosted_workflow_smoke.py -q -k 'duplicate_provider_dispatch or stage_summary_digest or synthetic_terminal_sample or build_case_level_smoke_exports or hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover or hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch or hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch or large_org_full_roster_overflow_completes_without_live_provider'`
  - result: `7 passed, 23 deselected`
- `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_final_20260424 --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_final_20260424_report.json --summary-json output/scripted_smoke_current/google_long_tail_final_20260424_summary.json`
  - result: strict summary green; total `5506.81ms`
- `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch -q`
  - result: `2 passed`
- `./.venv-tests/bin/python -m pytest tests/test_pipeline.py tests/test_worker_recovery_daemon.py tests/test_hosted_workflow_smoke.py -q -k 'resume_queued_workflow_prefers_hosted_dispatch_when_requested or resume_queued_hosted_workflow_dispatches_without_holding_job_lock or start_workflow_dispatches_hosted_runner_before_recovery_daemons or start_hosted_workflow_thread_skips_duplicate_dispatch_with_fresh_marker or progress_auto_recovery_skips_fresh_hosted_dispatch_for_queued_job or release_stale_workflow_job_lease_for_recovery or reconcile_completed_workflow_after_background_harvest_prefetch or hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch or hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider or hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch'`
  - result: `11 passed, 293 deselected`
- `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
  - result: `15 passed, 1 skipped, 2 subtests passed`
- `bash ./scripts/run_python_quality.sh typecheck`
  - result: `Success: no issues found in 16 source files`
- `./.venv-tests/bin/python -m pytest -q`
  - result: `1105 passed, 8 skipped, 25 subtests passed in 525.82s`
