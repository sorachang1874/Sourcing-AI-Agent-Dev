# Sourcing AI Agent Dev Progress — 2026-04 and earlier archive

> Status: Archived 2026-06-11. Historical progress log for 2026-04 and earlier; rotated out of `PROGRESS.md` to keep the active file small. Do not append here.

## 2026-04-30 (Asia/Shanghai)

### Legacy backfill implementation and CLI hardening (rebuild slice 8)

- Implemented `backfill_job_result_lifecycle` in `src/sourcing_agent/job_result_lifecycle_backfill.py` to create lifecycle rows for historical jobs without validated rows.
- Backfill logic:
  - Scans all jobs via `store.list_jobs()`.
  - Skips jobs that already have `source_validation_status='validated'` lifecycle rows.
  - For legacy jobs, builds projection using quarantined `_build_result_view_lifecycle_payload` with `_allow_for_legacy_backfill=True`.
  - Persists via `_persist_canonical_lifecycle` with `source_validation_status='validated'` so backfilled rows are trusted by fast path reader.
  - Supports dry-run mode for safe preview, batch processing, and progress callbacks.
  - Idempotent: re-running backfill on jobs with validated rows is safe and skips them.
- CLI integration: added `backfill-job-result-lifecycle` command with `--dry-run` flag. Uses `build_orchestrator()` for correct initialization.
- 2026-05-02 hardening after real CLI dry-run exposed two service-path gaps:
  - Added schema preflight before the first lifecycle read. The command now force-syncs the single `job_result_lifecycle` table into PG with validation, avoiding the prior `UndefinedTable` failure when PG's sync fingerprint predated the new table.
  - Fixed the CLI progress callback contract and added a command-level test that exercises verbose progress output.
  - Replaced the remaining public-read `get_job_candidate_page` call to deprecated `_compute_and_record_stage1_progress` with non-mutating `_build_linkedin_stage1_progress_payload`; the deprecated helper now has no call sites and is retained only until production backfill completes.
- Test coverage (7 backfill tests plus CLI command coverage, all passing):
  - `test_backfill_creates_lifecycle_rows_for_legacy_jobs` — verifies backfill creates rows for jobs without lifecycle rows.
  - `test_backfill_dry_run_does_not_persist` — verifies dry-run mode does not persist changes.
  - `test_backfill_handles_multiple_jobs` — verifies batch processing of mixed legacy/validated jobs.
  - `test_backfill_skips_jobs_with_validated_lifecycle_rows` — verifies validated rows are not overwritten.
  - `test_backfilled_rows_marked_as_validated` — verifies backfilled rows use `source_validation_status='validated'` for fast path trust.
  - `test_backfill_idempotency` — verifies re-running backfill does not modify existing validated rows.
  - `test_backfill_runs_schema_preflight_before_first_lifecycle_read` — verifies schema preflight happens before the first lifecycle read.
  - `test_backfill_job_result_lifecycle_cli_dry_run_delegates_with_progress_callback` — verifies CLI dry-run delegates correctly and verbose progress callback signature matches the backfill API.
- Validation:
  - `.venv-tests/bin/pytest tests/test_job_result_lifecycle_backfill.py -q` -> `7 passed`
  - `.venv-tests/bin/pytest tests/test_cli.py -q -k 'backfill_job_result_lifecycle or control_plane_storage_banner'` -> `3 passed, 29 deselected`
  - `.venv-tests/bin/python -m sourcing_agent.cli backfill-job-result-lifecycle --dry-run --batch-size 2` -> completed, `249` jobs would be backfilled, `0` errors, schema preflight `status=synced`
  - `.venv-tests/bin/python -m sourcing_agent.cli backfill-job-result-lifecycle --dry-run --batch-size 200 --verbose` -> completed with progress events and `0` errors
  - `.venv-tests/bin/pytest tests/test_results_api.py -q -k 'validated_stage1_row_is_not_mutated_by_public_reads'` -> `1 passed`
  - `.venv-tests/bin/pytest tests/test_results_api.py -q` -> `97 passed, 1 teardown cleanup failure`; the failed test passed when rerun alone, and the failure was an `OSError: Directory not empty` while deleting a temp `company_assets/openai` directory, not an assertion failure. Track as a test cleanup/flakiness risk before final release validation.
- Legacy helpers retained: `_build_result_view_lifecycle_payload`, `_lifecycle_row_needs_stage1_repair`, and `_compute_and_record_stage1_progress` are kept as safety fallback until production backfill runs. These methods are quarantined with `_allow_for_legacy_backfill` guard and documented for deletion after backfill execution.
- Next steps:
  - Run `backfill-job-result-lifecycle --dry-run` in production to preview changes.
  - Execute production backfill: `backfill-job-result-lifecycle`.
  - After backfill completes, delete the three legacy helper methods outright.

### Failed-state lifecycle writer wired at terminal failure paths (rebuild slice 7)

- Wired `mark_job_result_lifecycle_terminal(outcome="failed")` at three terminal workflow failure paths:
  1. `_mark_workflow_failed` (line 24930) — the primary workflow failure handler that transitions job status to "failed" and stage to "failed"
  2. Excel intake failure (line 18384) — when Excel parsing or validation fails during workflow execution
  3. Retrieval job failure (line 26039) — when a retrieval job fails terminally
- All three call sites now write `phase="failed"`, `state="failed"`, `phase_status="terminal"` to the canonical lifecycle row before returning failure to the caller.
- Fixed critical bugs discovered during validation:
  1. `_normalize_lifecycle_for_terminal_phase` was incorrectly overwriting `state="failed"` with materialization state for failed jobs. Added `status != "failed"` guard to preserve failed state.
  2. `_lifecycle_row_needs_stage1_repair` was triggering repair for terminal rows. Added terminal phase detection to skip repair logic for `phase='failed'` or `phase='completed' with phase_status='terminal'`.
  3. Fixed indentation bug in `get_job_candidate_page` cache block (introduced in commit 60f4a7c) that caused `UnboundLocalError`.
  4. Fixed `allow_materialization_fallback` logic inversion: when `source_path` is `candidate_documents.json`, materialization fallback should be **enabled**, not disabled. Changed line 6450 from `not allow_candidate_documents_fallback` to `allow_candidate_documents_fallback`.
  5. Fixed Stage 1 repair detection to use correct field names from `linkedin_stage_1_progress`: `profile_fetch_required_count` and `profile_fetched_count` (not `delta_profile_*`).
  6. Fixed state normalization to check job stage: when `stage=acquiring`, return `delta_applying` instead of `current_snapshot_materializing`.
- Added comprehensive failed-state test coverage:
  - `test_job_result_lifecycle_failed_workflow_writes_terminal_row` — proves that failed workflows write terminal lifecycle rows with `phase="failed"`, `state="failed"`, and that `/dashboard` correctly returns `state="failed"`.
  - `test_job_result_lifecycle_excel_intake_failure_writes_terminal_row` — proves Excel intake terminal failure writes a failed lifecycle row.
  - `test_job_result_lifecycle_retrieval_job_failure_writes_terminal_row` — proves retrieval job terminal failure writes a failed lifecycle row.
  - `test_job_result_lifecycle_failed_state_consistent_across_endpoints` — proves `/dashboard`, `/progress`, and `/candidates` all report identical failed lifecycle state for failed jobs.
  - `test_job_result_lifecycle_failed_public_read_does_not_mutate` — proves validated failed rows are never mutated by public reads; the terminal writer is the sole source of truth.
- Validation:
  - `.venv-tests/bin/pytest tests/test_results_api.py -q` -> `98 passed in 83.29s`
  - `.venv-tests/bin/ruff check tests/test_results_api.py` -> `All checks passed!`
- All lifecycle tests pass, including the three previously failing tests:
  - `test_asset_population_overlay_count_overrides_full_snapshot_manifest_count` — fixed by correcting overlay loading logic
  - `test_delta_progress_exposes_search_and_profile_fetch_counts` — fixed by correcting repair detection field names and state normalization
  - `test_job_results_auto_materialize_snapshot_candidate_documents_for_asset_population` — fixed by correcting materialization fallback logic
- Historical failure now impossible: failed workflows cannot appear as "unavailable" or other incorrect states in `/dashboard`, `/progress`, or `/candidates` because the terminal failure writer commits the canonical failed state before any public read. Cross-endpoint consistency is guaranteed by all three APIs reading from the same canonical row.
- Remaining gaps after this slice:
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair`, `_compute_and_record_stage1_progress`, and the fallback projection outright.

### Canonical reader fallback projection retired (rebuild slice 6)

- Retired `_build_result_view_lifecycle_payload` from the canonical reader fast path. Public reads (`/dashboard`, `/progress`, `/candidates`) now trust the validated `job_result_lifecycle` row as the source of truth, even when no stored `result_view` exists yet.
- Fixed `_lifecycle_row_needs_stage1_repair` to not require Stage 1 counters when the row has `served_snapshot_id` (materialization writer is authoritative). This allows materialization-writer-updated rows to hit the fast path without falling back to projection.
- Quarantined `_build_result_view_lifecycle_payload` for legacy/backfill only: added `_allow_for_legacy_backfill` parameter that raises `ValueError` if called for validated rows without explicit flag. The slow path in `_load_job_result_lifecycle` passes `_allow_for_legacy_backfill=True` when calling the fallback projection.
- Enabled full cross-API consistency assertions in `test_job_result_lifecycle_baseline_delta_overlay_for_health_whisper_shape`. The test now asserts that `/dashboard`, `/progress`, and `/candidates` return identical lifecycle payloads for the Health/Whisper baseline+delta overlay shape (baseline serving + incomplete delta = not stable current serving).
- Added three new regression tests:
  - `test_job_result_lifecycle_validated_row_without_result_view_uses_canonical_row` — proves validated row without stored result_view is projected from canonical row, not rebuilt
  - `test_job_result_lifecycle_terminal_invariant_baseline_served_incomplete_delta` — enforces terminal state invariant (baseline serving + incomplete delta = not stable current serving)
  - `test_job_result_lifecycle_fallback_projection_quarantined_for_validated_rows` — proves guard prevents accidental fallback use for validated rows
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'lifecycle'` -> `22 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
  - `./.venv-tests/bin/ruff format src/ tests/` -> formatted 9 files
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Historical failure now impossible: the Health/Whisper baseline+delta overlay regression (where `/dashboard`, `/progress`, and `/candidates` returned divergent lifecycle payloads) cannot happen because all three APIs read from the same canonical row, and the fallback projection is quarantined for legacy/backfill only.
- Remaining gaps after this slice:
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair`, `_compute_and_record_stage1_progress`, and the fallback projection outright.

### Materialization/repoint lifecycle writers wired at event-time (rebuild slice 5)

- Wired `update_job_result_lifecycle_from_materialization` at two key materialization event-time hooks:
  1. `_recover_delta_only_result_view_to_baseline_delta_overlay` line 4807 — after the current-snapshot repoint upsert. When a delta-only result view is detected and repaired to serve the full current snapshot, the materialization writer updates the canonical lifecycle row with `served_snapshot_id = current`, `served_candidate_count = current total`, and `current_snapshot_id = current`.
  2. `_recover_delta_only_result_view_to_baseline_delta_overlay` line 4888 — after the baseline+delta overlay upsert. When a delta-only result view is overlaid with the baseline to serve `baseline + delta`, the materialization writer updates the canonical lifecycle row with `served_snapshot_id = current`, `served_candidate_count = baseline + delta`, and the overlay serving phase.
- Both event-time hooks run immediately after `upsert_job_result_view`, so the canonical lifecycle row is updated before any public read can observe the new result view.
- Added `test_job_result_lifecycle_baseline_delta_overlay_for_health_whisper_shape` — proves that the materialization writer records `served_snapshot_id = current` and `served_candidate_count = baseline + delta` for the Health/Whisper shape (baseline+delta job where current lane returns 0 and former lane returns >0). The test documents the current gap: the canonical reader's fast path requires a stored result_view to exist, so the public-read assertions are commented out with a TODO. Once the fallback projection is retired, those assertions will pass.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle or health_whisper'` -> `32 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `85 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6439` in `_build_job_asset_population_payload` — verified independent of this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Historical failure partially addressed: baseline+delta jobs cannot complete into raw delta-only result views at the event-time level (the materialization writer records the overlay serving state). However, the canonical reader's fallback projection can still rebuild from old sources when no stored result_view exists, so the full cross-API consistency invariant is not yet enforced. This gap is documented in the test and will be closed when the fallback projection is retired.
- Remaining gaps after this slice:
  - **Canonical reader fallback projection retirement.** `_build_result_view_lifecycle_payload` is still called from the slow path when the persisted row doesn't exist or isn't validated. Once all event-time writers are wired and legacy backfill ships, this fallback can be deleted.
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair`, `_compute_and_record_stage1_progress`, and the fallback projection outright.

### Stage 1 lifecycle writer moved to event-time (rebuild slice 4)

- Wired `update_job_result_lifecycle_from_stage1` at the real workflow event-time hook: `_persist_running_job_inline_reconcile_state` (line 22380+). This method is called when `acquisition_progress.latest_state` is updated with a new snapshot_id and search_seed_snapshot after search-seed/roster workers complete and their results are applied. The Stage 1 writer runs immediately after `save_job`, building the coherent projection from the fresh `latest_state` and persisting it into the canonical `job_result_lifecycle` row. Public reads (`/dashboard`, `/progress`, `/candidates`) now find the row already up to date.
- Removed public-read mutation: reverted the three public-API call sites (`_build_job_results_context`, `get_job_candidate_page`, `get_job_progress`) from `_compute_and_record_stage1_progress` back to the non-mutating `_build_linkedin_stage1_progress_payload`. Public reads no longer write to the canonical lifecycle row for Stage 1 fields.
- Deprecated `_compute_and_record_stage1_progress`: marked as deprecated with a comment explaining it's retained only for compatibility during the transition. It now just calls `_build_linkedin_stage1_progress_payload` and returns the payload without any lifecycle write.
- Narrowed `_lifecycle_row_needs_stage1_repair` to pure legacy/backfill semantics: it now only triggers for rows with no `projection_source_snapshot_id` AND no Stage 1 counters. The event-time writer always sets both, so new jobs hit the validated fast path. The helper is documented as deletable once a one-shot legacy backfill ships.
- Added `test_job_result_lifecycle_stage1_event_time_write_before_public_read`: proves that once the Stage 1 writer has run, subsequent public reads do NOT mutate the canonical row's Stage 1 fields. This is the slice-4 invariant.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'stage1 or job_result_lifecycle or lifecycle_consistent or lifecycle_writer'` -> `14 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle'` -> `31 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `84 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6408` in `_build_job_asset_population_payload` — verified independent of this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Historical failure now impossible: the OpenAI Infra mixed-source bug (`current=0, former=10, profile_required=77`) cannot happen in public lifecycle because the event-time writer commits one coherent projection (lane counts + profile denominator from the same snapshot), and public reads no longer rebuild or mutate Stage 1 fields.
- Remaining gaps after this slice:
  - **Materialization-repoint call-site wiring.** `update_job_result_lifecycle_from_materialization` exists but is not yet called from `_repair_delta_only_result_view_to_overlay` and current-snapshot repoint paths.
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair` and `_compute_and_record_stage1_progress` outright.
  - **Fallback projection retirement.** Once materialization writers are wired and legacy backfill ships, `_build_result_view_lifecycle_payload` can be deleted from the public-read flow.

### Stage 1 lifecycle writer landed (rebuild slice 3)

- Added `SourcingOrchestrator.update_job_result_lifecycle_from_stage1`. The writer commits a coherent Stage 1 projection — current/former/all lane counts, deduped candidate count, deduped profile URL count, profile fetch required, profile fetched, plus mirrored `delta_profile_required/fetched_count` for delta workflows — in one row update. `projection_source_snapshot_id` is recorded so concurrent older lane reads cannot regress newer fields (snapshot ids are timestamp-prefixed and therefore monotonic).
- The writer enforces the OpenAI Infra mixed-source invariant directly: when the lane snapshot returns no rows, `stage1_profile_fetch_required_count` stays 0 even if workers report a queued URL count. Worker-derived URL totals are accepted via `queued_profile_url_count_from_workers` and stored in `metadata_json.workers.queued_profile_url_count` for observability — never in the public-facing denominator. This makes the `current=0, former=10, profile_required=77` failure mode impossible by construction.
- Phase progression: when an existing row is at `planning` or `baseline_serving` and Stage 1 evidence is observed, the writer transitions to `delta_applying` (delta workflows) or `current_snapshot_serving` (full local reuse / live roster). It never regresses out of more advanced phases.
- `expected_candidate_count` for in-progress delta workflows is derived from `baseline_candidate_count + coherent_required_count`, capped by the served count.
- Added `_compute_and_record_stage1_progress` as the seam between the existing `_build_linkedin_stage1_progress_payload` and the canonical writer. The three primary public-API callsites (`_build_job_results_context`, `get_job_candidate_page`, `get_job_progress`) now go through it, so Stage 1 fields land in the canonical row before `_load_job_result_lifecycle` runs.
- Narrowed `_lifecycle_row_needs_stage1_repair` to a legacy/backfill repair detector. New jobs hit the validated fast path because the writer has run; it only falls through for rows with no `projection_source_snapshot_id` AND no Stage 1 counters AND evidence in the supplied progress payload that Stage 1 has progressed. Documented as deletable once a one-shot legacy backfill ships.
- Added four Stage 1 writer regression tests in `tests/test_results_api.py`:
  - `test_job_result_lifecycle_writer_stage1_persists_atomic_projection`
  - `test_job_result_lifecycle_writer_stage1_rejects_stale_snapshot_writes`
  - `test_job_result_lifecycle_writer_stage1_rejects_mixed_source_infra_shape`
  - `test_job_result_lifecycle_validated_stage1_row_is_not_mutated_by_public_reads`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'stage1 or job_result_lifecycle or lifecycle_consistent or lifecycle_writer'` -> `13 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle'` -> `30 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `83 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6408` in `_build_job_asset_population_payload` — verified independent of this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py tests/test_results_api.py` -> passed
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` remains green.
- Remaining gaps after this slice:
  - **Materialization-repoint call-site wiring.** `update_job_result_lifecycle_from_materialization` exists but is not yet called from `_repair_delta_only_result_view_to_overlay` and current-snapshot repoint paths.
  - **Failure-path call-site wiring.** `mark_job_result_lifecycle_terminal(outcome='failed')` is implemented but not yet called from workflow failure paths.
  - **One-shot legacy backfill.** Required to delete `_lifecycle_row_needs_stage1_repair` outright.
  - **Workflow-event-time Stage 1 writes.** The writer is currently invoked at public-API entry. Moving the call into `enrichment.py` / `seed_discovery.py` so the row updates without any public read would let the projection-fallback inside `_load_job_result_lifecycle` retire entirely.

### Event-driven lifecycle writers + reader fast-path landed

- Added four event-driven canonical lifecycle writers on `SourcingOrchestrator`: `initialize_job_result_lifecycle`, `publish_baseline_job_result_lifecycle`, `update_job_result_lifecycle_from_materialization`, `mark_job_result_lifecycle_terminal`. Each writes through `ControlPlaneStore.upsert_job_result_lifecycle` with patch-style merge, mapping concrete transition inputs (baseline snapshot id, baseline count, served snapshot id, delta counts, terminal outcome) onto canonical row fields. The terminal writer encodes the architectural invariant directly: a finished job whose served snapshot equals the baseline with materialization incomplete is recorded as `current_snapshot_materializing` (or `delta_applying`), not stable `baseline_serving`.
- Wired the workflow-create writer at `_create_workflow_job` (initial planning row) and the baseline-publication writer at `_persist_delta_baseline_serving_result_view` (transitions to `baseline_serving` + view_id binding). Both run before any public API touches the job.
- `_load_job_result_lifecycle` now has a validated fast path: if the persisted row has `source_validation_status='validated'` and is not in a transition the current writer set does not yet keep current, public reads return the persisted row unchanged (terminal-phase invariant still applied at render time only). Writes only happen on the slow/repair path. This retires the "rebuild and persist on every public read" loop.
- `_lifecycle_row_needs_stage1_repair` is the narrow stale-detection helper: a row at `phase=planning` or `baseline_serving` falls through to projection when `linkedin_stage_1_progress` shows actual work has happened (profile fetched, profile required, search returned). This is the explicit gap until the Stage 1 progress writer lands.
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` extended to assert the canonical row exists post-read and is pinned to the correct baseline.
- Four new writer tests in `tests/test_results_api.py`:
  - `test_job_result_lifecycle_writer_initialize_persists_planning_row_without_public_read`
  - `test_job_result_lifecycle_writer_publish_baseline_persists_row_before_public_read` (also asserts validated public reads do not mutate canonical fields)
  - `test_job_result_lifecycle_writer_materialization_repoint_updates_row`
  - `test_job_result_lifecycle_writer_terminal_normalizes_stale_baseline_infra_shape`
- Triaged the two pre-existing lifecycle-adjacent failures (`test_result_view_lifecycle_counts_profile_detail_materialized_to_board_from_state`, `test_result_view_lifecycle_infers_baseline_count_from_stage1_preview`). Both encoded contracts retired by the canonical reader/projection: the first asserted profile-richness completeness as the materialization metric (contradicts `WORKFLOW_PROGRESS_CONTRACT.md` board-serving definition); the second asserted on-disk `candidate_documents.json` row count as `served_candidate_count` (contradicts the canonical-result-view-summary contract that makes cross-API consistency possible). Updated both expectations with explicit pointers to the design and contract docs; both now pass.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'job_result_lifecycle or lifecycle_consistent or lifecycle_writer'` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent or job_result_lifecycle'` -> `26 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `79 passed, 5 failed`. The 5 failures are all the pre-existing dirty-tree `UnboundLocalError: payload` at `orchestrator.py:6408` in `_build_job_asset_population_payload` (uncommitted dirty-tree caching write that runs before `payload` is bound; verified independent of the lifecycle slice in the previous handoff).
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py tests/test_results_api.py` -> passed
- Remaining gaps in this slice (called out in `docs/JOB_RESULT_LIFECYCLE_DESIGN.md` §"Implementation status"):
  - **Stage 1 projection writer.** Lane counters (`stage1_current_search_returned_count`, …, `stage1_profile_fetched_count`) and `delta_profile_*` fields on the row are still 0 unless the materialization writer fills them. The current behavior is that `_lifecycle_row_needs_stage1_repair` falls back to projection so users see fresh counters; once the dedicated Stage 1 writer lands, the row stays current without a fall-through.
  - **Materialization-repoint wiring.** The writer exists but is not yet called from `_repair_delta_only_result_view_to_overlay` / current-snapshot repoint paths. Today these paths still write the lifecycle dict into `result_view.metadata`; the canonical row picks the change up via the slow path.
  - **Failed-state writer.** Only `outcome=completed` is wired into the workflow create / baseline publication paths; failed jobs are normalized at render time but no failure-event writer call exists yet.
  - **One-shot backfill.** Existing jobs without a row work via the slow path. A startup migration would precompute rows for legacy jobs.

### Canonical job_result_lifecycle slice landed

- Implemented the persisted canonical lifecycle from `docs/JOB_RESULT_LIFECYCLE_DESIGN.md`. New table `job_result_lifecycle` (PG-first via control-plane allowlist; SQLite mirror; primary key `job_id`) holds baseline/current/served snapshot ids, baseline/served/expected counts, delta required/fetched/applied/materialized/board-visible counts, Stage 1 lane projection counters, phase, projection-source snapshot id, and `source_validation_status`.
- Added `ControlPlaneStore.upsert_job_result_lifecycle` (patch-style merge) and `get_job_result_lifecycle` plus the `_job_result_lifecycle_from_row` helper, modeled after `job_result_views`.
- Added `SourcingOrchestrator._load_job_result_lifecycle` as the single canonical reader. It builds the in-flight projection (still via `_build_result_view_lifecycle_payload` for now), applies a terminal-phase invariant, persists the row, and returns the public-API payload through `_project_canonical_lifecycle_payload`. Architectural invariant enforced at the lifecycle layer: a terminal job whose served snapshot equals the baseline with `delta_profile_required > delta_profile_materialized` is reported as `current_snapshot_materializing` (or `delta_applying`), never stable `baseline_serving`.
- Routed `/dashboard`, `/candidates` (both asset_population and ranked_results branches), and `/progress` through the canonical reader. The reader derives `served_candidate_count` uniformly from `job_result_views.summary.candidate_count` rather than from caller-specific `asset_population` payloads — this is the change that makes the OpenAI Infra cross-API regression pass.
- Anchor regression `tests/test_results_api.py::ResultsApiTest::test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` now passes. Before this slice it failed with `/dashboard served_candidate_count=5` vs `/progress served_candidate_count=1200` for hosted OpenAI Infra job `c5248ea4b3b4`; after the slice all three public APIs return identical canonical fields and the terminal-state invariant is satisfied.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases or lifecycle_consistent'` -> `20 passed, 2 failed`. Both failures are pre-existing dirty-tree expectations against direct calls to `_build_result_view_lifecycle_payload` (`...counts_profile_detail_materialized_to_board_from_state`, `...infers_baseline_count_from_stage1_preview`); they are unaffected by the canonical reader and have been failing since before this slice.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/orchestrator.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py` -> passed
- Pre-existing dirty-tree failures unrelated to the lifecycle slice (verified by reverting the canonical reader call sites and confirming the same failures): `test_asset_population_candidate_page_exposes_seed_query_as_matched_keyword`, `test_job_results_auto_materialize_snapshot_candidate_documents_for_asset_population`, `test_job_results_default_to_asset_population_for_full_company_snapshot_even_when_delta_is_required`, `test_job_results_default_to_asset_population_for_scoped_snapshot_even_when_delta_is_required`, `test_job_results_expose_asset_population_for_snapshot_reuse_even_when_plan_payload_is_missing`. Most trip an `UnboundLocalError: payload` at `orchestrator.py:6393` introduced by the dirty-tree `_asset_population_payload_cache` write that runs before `payload` is bound.
- Remaining slice scope (deferred to follow-up commits):
  - Move writers from "rebuild on every read" to dedicated workflow-event hooks (workflow create / baseline publication / Stage 1 projection / materialization-repoint). Today the canonical row is refreshed on every public read; this is correct but not yet event-driven.
  - Populate Stage 1 lane projection columns (`stage1_current_search_returned_count`, …) — the schema is in place but the writer still leaves them at 0.
  - Migrate existing jobs (a one-shot backfill) and flip `source_validation_status` semantics from "always validated on read" to "validated only after a writer commits".
  - Extend the anchor regression to also assert lifecycle persistence (currently only cross-API consistency is asserted).

### Canonical job_result_lifecycle design + failing Infra cross-API regression

- Mapped every current writer and reader of `result_view_lifecycle`. Findings: there is no persisted lifecycle row; `_build_result_view_lifecycle_payload` (`src/sourcing_agent/orchestrator.py:7485-7859`) is rebuilt per request with a five-source fallback ladder for `delta_profile_materialized_count`, and `/dashboard` (5735), `/candidates` (5841/5889), and `/progress` (8608) feed it different inputs. State today lives only inside `job_result_views.metadata_json`, `jobs.summary_json`, runtime stage files, and `candidate_materialization_states`.
- Added `docs/JOB_RESULT_LIFECYCLE_DESIGN.md` covering the canonical persisted `job_result_lifecycle` table (schema, indexes, phase enum), the four-writer ownership model (workflow create / baseline publication / Stage 1 projection / materialization-repoint), the read path via a new `_load_job_result_lifecycle` helper, the derived sources retired, the failure classes made impossible, and the non-blocking migration plan that backfills existing jobs as `pending_validation`.
- Added the anchor regression `tests/test_results_api.py::ResultsApiTest::test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape`. It reproduces hosted job `c5248ea4b3b4`: terminal workflow, baseline-serving result view at `20260430T130836`, `delta_profile_required/fetched=89/89`, `delta_profile_materialized=19`, `expected=1200`. Asserts `/dashboard`, `/progress`, `/candidates` observe identical lifecycle rows and that a terminal job cannot expose stable `results` while serving a stale baseline-only board with materialization incomplete.
- Test currently fails as designed: `/dashboard served_candidate_count=5` (counts on-disk baseline candidate_documents.json rows) vs `/progress served_candidate_count=1200` (reads the persisted lifecycle dict). Same job, same instant, two public APIs disagree by 1195 — direct evidence the per-request rebuild path must be retired.
- Linked the design from `docs/INDEX.md` and recorded the slice + failing test in `docs/NEXT_TODO.md` under the highest-priority rebuild track.

### Service-grade streaming workflow rebuild handoff documented

- Promoted the repeated OpenAI Infra/Health/Whisper/ChatGPT streaming failures from local bug backlog into a service-grade workflow rebuild track.
- Added `docs/archive/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md` for the next Claude Code session: product expectations, anchor failure `c5248ea4b3b4`, earlier incident classes, required canonical lifecycle architecture, scheduler/materialization boundaries, scripted/browser guardrails, anti-patterns, and a ready-to-use starting prompt.
- Added `docs/archive/STREAMING_WORKFLOW_REBUILD_PLAN.md` with the implementation order: persisted `job_result_lifecycle`, atomic Stage 1 progress, provider completion/next-submit/materialization decoupling, partial delta board streaming, and upgraded service-level scripted/browser tests.
- Updated `docs/NEXT_TODO.md` so this rebuild is the first Highest Priority item before more workflow code changes, and updated `docs/INDEX.md` so the new docs are canonical entry points.
- Clarified the target scheduler shape after reviewing the OpenAI Infra small-batch/provider-slot failure: durable item queues are the retry/dedupe/progress/materialization units, while provider batches are only adaptive remote-run envelopes. Tiny live batches such as `2` or `3` profiles should auto-merge into ready/near-ready work by default because each remote run has fixed webhook/watcher/recovery/dataset/local-apply overhead. Exceptions require queue-quiescence proof after a coalescing window, or explicit retry-isolation/low-volume-company policy.
- Clarified the target candidate board shape: the ideal serving unit is a continuous projection, not a complete snapshot switch. The board should serve baseline generation + ordered delta patches + row-level materialization state, while full snapshot/retrieval/index rebuild becomes background compaction rather than the first moment new candidates can be visible.
- Updated `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`, `docs/WORKFLOW_PROGRESS_CONTRACT.md`, `docs/archive/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md`, `docs/archive/STREAMING_WORKFLOW_REBUILD_PLAN.md`, and `docs/NEXT_TODO.md` to make queue-first scheduling, adaptive batch packing, per-item retry/dedupe, continuous serving projection, and incremental board-visible apply part of the canonical rebuild scope.
- Added a stricter planning critique to the Claude Code context and rebuild plan: each implementation slice must name the durable state it introduces, the stale source/fallback it retires, the historical failure it makes impossible, and the negative scripted/browser assertion that proves the behavior.

### Hosted OpenAI Infra live diagnosis added to TODO

- User case `ae907436-5996-4e04-a109-c9943cdfa399` / job `c5248ea4b3b4` (`帮我找OpenAI做Infra方向的人`) was diagnosed read-only on ECS; no code or service changes were made.
- The plan was correctly `delta_from_snapshot` from baseline `20260430T130836` because Infra current/former shard coverage is missing from selected OpenAI source snapshots.
- Final snapshot files for `20260430T155907` show `current/entries=83`, `former/entries=10`, deduped `89`, so the UI state `新取回在职候选人0 / 新取回离职候选人10 / 需补取 LinkedIn Profile77` was a progress-source atomicity bug, not a true provider result shape.
- Provider scheduling did not keep the 4-slot profile-scraper budget full: batches were `8`, then `33/34`, then a small cached `3`; local watcher often beat provider webhook, and callback-side profile prefetch/cache-marker confirmation spent about `295-302s` with `dispatched_url_count=0`.
- The job later reached `completed/completed`, but `job_result_views` still served only the initial baseline view (`20260430T130836`); lifecycle showed `delta_profile_required/fetched=89/89` but `delta_profile_materialized_count=19`, and `/dashboard` / `/candidates` timed out during the baseline-serving post-fetch state.
- Added concrete TODO bullets under progress atomicity, result-view lifecycle completion/repoint, and profile-scraper slot utilization so this hosted Infra shape can become a regression fixture.

### Authoritative registry now preserves reusable source shard snapshots

- Root cause from hosted OpenAI Agent planning regression: the user request still parsed `Agent` correctly, but newer OpenAI Health/Whisper serving snapshots narrowed authoritative `organization_asset_registry.selected_snapshot_ids`, so planner could no longer see previously migrated Agent/ChatGPT shard coverage.
- Added registry-write inheritance for reusable source snapshots: when a newer authoritative serving snapshot is promoted, the registry row now preserves previous selected source snapshots that have acquisition-shard registry rows. This keeps OpenAI Agent/ChatGPT/Health/Whisper and future scoped shards from overwriting each other while still allowing the newest serving snapshot to be primary.
- Disabled default `current_snapshot_only_large_org` materialization selection. Large-org materialization no longer silently drops historical source snapshots just because the current snapshot is 1000+ candidates. Explicit `preferred_source_snapshot_ids` remains the bounded path for safe subset materialization until snapshot hygiene/clean-source selection is formalized. There is no separate `former_snapshot_only_large_org` mode in the codebase.
- Synced the fix to ECS and repaired OpenAI source coverage:
  - copied the missing Agent source snapshot `20260427T141011` to `/srv/sourcing-ai-agent/runtime/company_assets/openai/20260427T141011`
  - reran no-provider OpenAI shard registry backfill; authoritative OpenAI remains serving `20260430T130836`, with reusable selected source snapshots now including `20260427T141011` plus the existing ChatGPT/Health/Whisper/Coding/Reasoning/Multimodal/Audio/Pre-train sources
  - hosted `/api/plan` for `帮我找OpenAI做Agent方向的人` now returns `planner_mode=reuse_snapshot_only`, `requires_delta_acquisition=false`, `baseline_snapshot_id=20260430T130836`, and current/former Agent coverage counts `1/1`
  - repaired the already-created hosted plan history `9a497df4-25ee-4ced-a212-d5703a8054e6` to the same reuse-only plan so refreshing that record no longer shows the stale delta strategy
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_organization_execution_profile.py -q -k 'inherits_reusable_shard_source_snapshots or large_org'` -> `4 passed, 7 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k 'large_org'` -> `2 passed, 50 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_organization_execution_profile.py tests/test_candidate_artifacts.py -q -k 'inherits_reusable_shard_source_snapshots or large_org or materialized_view_large_org'` -> `6 passed, 57 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/asset_registration.py src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/candidate_materialization.py tests/test_organization_execution_profile.py tests/test_candidate_artifacts.py` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/asset_registration.py src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/candidate_materialization.py` -> passed

### Hosted OpenAI Whisper delta-only result-view drift fix prepared

- Live ECS case: `bcc09c89db36` / `a03597f9-e7e7-4dba-9715-b83b55454683` (`帮我找OpenAI在Whisper组的人`) was user-visible as `failed / Failed to fetch`, but direct PG inspection showed the workflow was `completed/completed` and frontend history phase was `results`.
- Root cause found: the job result view was serving the current delta snapshot `20260430T130836` as an `asset_population` view with `candidate_count=2`, while the intended serving population was baseline `20260430T090520` plus 2 delta profiles. The delta snapshot also lacked normalized artifact manifest files, and completed-workflow reconcile was blocked by a stale terminal workflow lease, so background materialization/repoint did not close the gap.
- Fix prepared locally:
  - baseline+delta direct finalization now falls back to a job-scoped `asset_population` overlay when durable generation patch creation is unavailable, instead of dropping back to a raw delta-only result view
  - result-view read repair now detects completed delta-only result views with a `delta_baseline_snapshot_id`, loads baseline and delta candidate documents, writes a job-scoped overlay, and persists the repaired result view before serving dashboard/candidate pages
  - follow-up hot-path fix: read repair now loads only existing baseline/delta `candidate_documents` and explicitly disables materialization fallback, so public dashboard/candidate requests cannot rebuild full candidate artifacts while trying to repair a result view. Job-scoped overlay writes also avoid synchronous thematic facet/role-bucket derivation; they preserve existing values and use a cheap fallback only when missing.
  - overlay merge semantics now preserve duplicate baseline rows and baseline ordering, then update or append only the delta rows. This prevents a baseline-serving board from shrinking when a repair overlay is built from member keys.
  - if the current workflow snapshot already has at least the baseline-sized candidate document population, read repair now repoints the result view to those current `candidate_documents` instead of building a baseline+delta overlay. Candidate-document serving paths also disable materialization fallback so public reads do not turn into artifact rebuilds.
  - stale `asset_population_overlay_path` metadata no longer forces overlay serving when the authoritative result view has no valid patch; manifest-backed current snapshots may fall back to sibling `candidate_documents.json` without materialization.
  - `delta_profile_materialized_count` treats a current snapshot that is serving at least the baseline population after all delta profiles were fetched as fully materialized, because delta profiles may update existing candidates rather than increasing net board size. In that state, `expected_candidate_count` is normalized to the actual served candidate count.
  - terminal completed-workflow reconcile leases can be released after their heartbeat is stale even if the long lease TTL has not expired, preventing completed jobs from waiting many minutes before materialization/repoint recovery can run
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'delta_direct_finalization_uses_overlay or delta_only_result_view_recovers or current_candidate_documents or terminal_completed_workflow_releases_stale_reconcile_lease'` -> `4 passed, 75 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k 'asset_population_overlay_preserves'` -> `2 passed, 50 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync or delta_result_view or delta_direct_finalization or delta_only_result_view or terminal_completed_workflow_releases'` -> `20 passed, 64 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py` -> passed

### ECS production is now serving current code, curated assets, hosted webhook, and latest frontend

- Current ECS backend service has been switched to the canonical code root:
  - active repo: `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`
  - active runtime: `/srv/sourcing-ai-agent/runtime`
  - systemd env file: `/etc/sourcing-ai-agent.env`
  - service status: `sourcing-ai-agent.service=active`
  - the attempted separate worker service is intentionally `inactive`; `serve` owns the built-in `worker-recovery-daemon` and hosted runtime watchdog.
- Production control plane is PG-only:
  - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
  - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
  - `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`
  - post-schema-migration journal check showed no `UndefinedTable`, `Traceback`, or `service_failed` in the last hosted gate window.
- Curated asset migration is serving the selected high-value baselines:
  - OpenAI `20260430T090520` -> `1110`
  - Google `20260428T011339` -> `9359`
  - Anthropic `20260416T225318` -> `3455`
  - Google and Anthropic serving-view audits now pass from selected snapshot ids; audit outputs are under `/srv/sourcing-ai-agent/runtime/deployment/final_audit_20260430T030333Z`.
- Anthropic exposed a registry migration edge case: remote ECS still had an older `20260409T045403` row with higher candidate count, so the guarded selector would retain/reselect it even though the local manifest selected `20260416T225318`.
  - Fix applied on ECS without deleting old assets: mark non-selected Anthropic registry rows `superseded` and promote `20260416T225318` as authoritative.
  - Migration lesson: when promoting a curated selected snapshot that is intentionally not the highest-count historical row, supersede/archive non-selected registry rows in PG so runtime selection, execution profiles, and reuse cannot drift back to old snapshots.
- Hosted webhook preflight now passes on ECS:
  - output directory: `/srv/sourcing-ai-agent/runtime/deployment/hosted_gates_20260430T030422Z`
  - `status=ready`
  - hosted callback route accepts connectivity probe with `HTTP 202`
  - submit contract has `webhook_definition_count=1` and `would_attach_webhooks=true`
  - callback URL: `https://api.111874.xyz/api/providers/apify/webhook`
- Public probes passed:
  - `https://api.111874.xyz/health`
  - `https://api.111874.xyz/api/runtime/health`
  - `https://api.111874.xyz/api/providers/health`
  - `https://demo.111874.xyz/health`
  - `https://demo.111874.xyz/api/runtime/health`
  - `https://demo.111874.xyz/api/providers/health`
- Cloudflare Pages frontend has been rebuilt and deployed in same-origin mode:
  - command: `VITE_API_BASE_URL=same-origin npm run build:hosted`
  - production deploy: `npx wrangler pages deploy dist --project-name sourcing-ai-agents-demo --branch main`
  - deployment URL: `https://757c4996.sourcing-ai-agents-demo.pages.dev`
  - custom domain now serves bundle `index-C5fqLPSd.js` / `index-C2fbpkw3.css`
  - quick bundle probe confirms current UI wording is present (`新增 LinkedIn Profile`, `已物化到看板`, `新取回在职候选人`, `打开LinkedIn`) and the old `查看原始资料` wording is gone.
- User-facing test URL: `https://demo.111874.xyz`
- Post-launch OpenAI Health plan check:
  - User-visible plan `8effc850-6a08-4cb4-aa22-83c524a29ee3` initially showed `Baseline 复用 + 缺口增量` even though OpenAI authoritative baseline was already `20260430T090520`.
  - Root cause was not wrong ECS env/DSN: runtime was `production/live`, PG authoritative OpenAI row was `20260430T090520`, and artifact files contained `search_seed_discovery/current|former` summaries for `health`.
  - Missing piece was PG `acquisition_shard_registry`: migration/backfill had not registered the current snapshot's `health` current/former search shards, so `asset_reuse_plan` could not prove shard coverage and set `requires_delta_acquisition=true`.
  - Ran local-only ECS repair, no provider calls:
    - `ensure_acquisition_shard_registry_for_snapshot(openai, 20260430T090520)` -> `shard_records=2`
    - selected-manifest shard backfill for 24 companies -> no failures; output saved under `/srv/sourcing-ai-agent/runtime/deployment/selected_shard_backfill_<timestamp>.json`
  - Replanned the same history id after repair:
    - `baseline_snapshot_id=20260430T090520`
    - `requires_delta_acquisition=false`
    - `baseline_sufficiency=ready`
    - missing current/former profile-search queries are empty.
- Post-launch selected-source snapshot parity repair:
  - Meta initially matched the selected serving baseline only (`20260427T203601` / Audio) on ECS, so `帮我找Meta做Agent方向的人` planned `Baseline 复用 + 缺口增量`. Local authoritative registry also selected source snapshots carrying `Multimodal` and `Agent`.
  - Copied/restored Meta source snapshots `20260427T190455`, `20260423T062947`, and `20260427T153312`; ECS `organization_asset_registry.selected_snapshot_ids` now matches local (`20260427T203601`, `20260427T190455`, `20260423T062947`, `20260427T153312`) and shard registry covers Audio/Multimodal/Agent current+former.
  - OpenAI selected source coverage was restored to include Health, ChatGPT, Reasoning/Reasoning model, Pre-train, Reinforcement Learning, and Coding source snapshots. Hosted plan checks for Health/ChatGPT/Coding now return `requires_delta_acquisition=false`.
  - Follow-up hosted matrix check: OpenAI `Agent` / `Health` / `ChatGPT` / `Coding` all return `reuse_snapshot_only` from baseline `20260430T130836`. OpenAI `Reinforcement Learning` still correctly returns `delta_from_snapshot` because local and ECS shard registries only have completed former-lane `Reinforcement Learning` coverage; the current lane is not proven reusable.
  - Google had the same migration-shape issue: the serving baseline `20260428T011339` was present, but reusable source shard snapshots for Multimodal/Veo/Nano Banana/Video generation/Vision-language were not selected in the hosted registry contract. Restored Google selected source snapshots to `20260428T011339`, `20260410T123708`, `20260411T174325`, `20260411T215236`, `20260413T073549`, and `20260413T100525`.
  - Hosted plan checks now return `requires_delta_acquisition=false` for Meta Agent, Meta Multimodal, Google Veo, Google Nano Banana, and Google Multimodal. Google Gemini still correctly returns `requires_delta_acquisition=true` because the Gemini current/former shard rows are `status=incomplete` (`615/736` current and `568/991` former), so they are not full coverage proof.
  - Migration rule refined: production asset migration must copy the serving snapshot plus source snapshots that provide real shard/profile reuse proof. Repeated no-increment history such as Thinking Machines Lab / Reflection AI should not be copied wholesale; keep the fullest/current baseline hot and move old duplicates to archive/test storage.
- Hosted Meta Agent full-reuse result-view/timeline follow-up:
  - User case: `dfde1d7b8af0` (`帮我找Meta做Agent方向的人`) eventually served candidate page `1727/1727`, but the execution page could show an empty timeline because `/api/jobs/{job_id}/progress` was about `50 MB`.
  - Root cause: progress/dashboard polling endpoints were shipping full workflow summaries and latest metrics from reused source snapshots, including large `background_reconcile` / deferred URL lists and old source snapshot Stage 1 payloads. Candidate paging stayed healthy because `/candidates` is lightweight and paginated.
  - Fix: `/progress` now compacts public workflow payloads before returning milestones/latest metrics/workflow stage summaries/LinkedIn progress, and `/dashboard` compacts `job.summary` instead of sending the full persisted workflow summary. Full local reuse with `delta_profile_progress_applicable=false` and `served_candidate_count >= expected_candidate_count` no longer treats inherited source snapshot LinkedIn profile progress as active `LinkedIn Stage 1` work.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'job_dashboard_is_summary_only_and_candidate_page_paginates_asset_population or job_progress_compacts_large_polling_payloads or execution_phase_contract_does_not_treat_full_reuse_snapshot_progress_as_pending or execution_phase_contract_labels_local_materialization_without_public_web'` -> `4 passed, 71 deselected`
    - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py` -> passed
    - `npm --prefix frontend-demo run build -- --mode development` -> passed
  - Hosted deployment:
    - Synced `src/sourcing_agent/orchestrator.py`, `frontend-demo/src/lib/api.ts`, tests, and docs to `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`.
    - Remote `py_compile` passed; `systemctl restart sourcing-ai-agent.service` completed and service is active.
    - Rebuilt hosted frontend with `VITE_API_BASE_URL=same-origin`; redeployed Cloudflare Pages from `frontend-demo` cwd so Pages Functions were included. First deploy from repo root uploaded only static assets and made `/api/frontend-history/*` fall through to the SPA shell; second deploy fixed same-origin `/api/*`.
    - Post-deploy probes:
      - `https://demo.111874.xyz/api/frontend-history/e5a1ab27-91e1-46a6-a672-3452fc36d812` returns JSON.
      - `https://demo.111874.xyz/api/jobs/dfde1d7b8af0/progress` is about `2.1 MB` and reports `active_phase_id=final_results`, `profile_work_pending=false`.
      - Browser check on `https://demo.111874.xyz/?history=e5a1ab27-91e1-46a6-a672-3452fc36d812&job=dfde1d7b8af0` shows execution timeline instead of empty placeholder, and candidate board sync `1727/1727`.
  - Remaining non-blocking observations:
    - Runtime health can still become very large because daemon `last_summary` may retain historical workflow payloads; add the same public-payload compaction to runtime health/status endpoints later.
    - Full-local-reuse timeline still uses generic historical stage names such as `LinkedIn Stage 1 completed`; the next durable progress-lifecycle pass should replace these with job-scoped reuse/materialization phases.

### ECS code/data migration staged, production switch still blocked

- Added the reusable migration entrypoint `docs/ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md` and the read-only manifest builder `scripts/build_ecs_asset_migration_manifest.py`.
- The manifest builder now emits JSON, Markdown, and a per-file `rsync --files-from` list so future ECS migrations do not depend on ad-hoc shell inventory generation.
- Current manifest copied to ECS:
  - local/remote JSON: `runtime/deployment/ecs_asset_migration_manifest_20260430T015713Z.json`
  - local/remote Markdown: `runtime/deployment/ecs_asset_migration_manifest_20260430T015713Z.md`
  - local/remote rsync list: `runtime/deployment/ecs_asset_rsync_all_files_20260430T015713Z.txt`
- Selected asset copy completed with no deletion:
  - 24 production companies selected
  - selected snapshot size about `2.465 GB`
  - `56,243` selected snapshot files in the rsync list
  - rsync transferred about `2.034 GB`
- ECS read-only validation:
  - hard failures: `0`
  - high-value snapshots present and readable: OpenAI `20260430T090520`, Google `20260428T011339`, Meta `20260427T203601`, Lovable `20260426T193540`, Mistral AI `20260426T163408`, Perplexity `20260424T184223`, Windsurf `20260427T131256`
  - one non-blocking count mismatch remains: `humansand/20260414T162042` manifest expected `25`, remote `candidate_documents.json` has `26`; this is not on the high-value deploy path but should be reconciled before treating the manifest as a strict byte-for-byte audit ledger
  - ECS data disk now has about `144 GB` available under `/srv/sourcing-ai-agent`
- Production service has not been switched:
  - systemd still runs `/opt/sourcing-ai-agent` with old code
  - `SOURCING_RUNTIME_ENVIRONMENT=production` is still missing from the active unit
  - only `/opt/sourcing-ai-agent/.local-postgres.env` was found as an env file; the migration playbook now recommends consolidating secret-bearing production values into `/etc/sourcing-ai-agent.env`
  - local hosted route `POST /api/providers/apify/webhook` on ECS still returns `404`
  - do not restart/replace production until the current-code replacement gate and hosted webhook preflight are run from the intended systemd env
- Staging code validation:
  - current working tree has been synced to `/srv/sourcing-ai-agent/repo/sourcing-ai-agent-stage`
  - `src/sourcing_agent/asset_catalog.py` no longer hard-fails when legacy sibling skill packages (`anthropic-employee-scan`, `investor-chinese-scan`, `biz-visit-onepager`) are absent; production runtime summary only needs the in-repo `local_asset_packages/anthropic` package
  - remote staging `py_compile` and import checks pass with the existing Python 3.12 venv
  - remote staging `show-control-plane-runtime` passes under the intended production env and reports `pg_only` with `shared_memory` compatibility shadow
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_catalog.py tests/test_cli.py -q -k 'asset_catalog or control_plane_storage_banner'` -> `3 passed, 29 deselected`
  - `./.venv/bin/python -m py_compile src/sourcing_agent/asset_catalog.py scripts/build_ecs_asset_migration_manifest.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/build_ecs_asset_migration_manifest.py --company openai --skip-size-scan --output-dir /tmp/sourcing_manifest_test` -> generated JSON/Markdown plus rsync file list
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `git diff --check -- src/sourcing_agent/asset_catalog.py tests/test_asset_catalog.py scripts/build_ecs_asset_migration_manifest.py docs/ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md docs/NEXT_TODO.md PROGRESS.md` -> passed

### Health live smoke closed a stage-summary/result-view drift gap

- Live case: `43179a67ffb5` (`我想要OpenAI在health组的人`) exposed that the board/result view could eventually repair to current snapshot while the user saw old progress in the interim.
- Root cause was not a Health query-width issue. A baseline-serving `job_result_view` pointed to old snapshot `20260429T174612`; before current snapshot `20260430T090520` fully repointed, `_load_workflow_stage_summaries()` used that baseline result view as the stage-summary directory and read old ChatGPT `stage_1_preview/stage_2_final` files. Completed-summary merge then copied stale `stage1_preview/background_snapshot_materialization/background_reconcile` fields into the Health job, so progress briefly showed old counts like `166/100/298`.
- Fixes:
  - workflow stage summary resolution now prefers explicit current workflow summary/acquisition/materialization snapshot hints before falling back to the served result view, so a baseline-serving pointer cannot decide current job stage-summary files
  - completed summary restoration filters stage/runtime fields by the final summary's primary snapshot id before merging preserved/latest summaries
  - `result_view_lifecycle` only uses `stage1_preview.baseline_selection_explanation.baseline_candidate_count` when that stage preview actually matches the lifecycle baseline snapshot
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'workflow_stage_summaries_prefer_current_snapshot_over_baseline_result_view or completed_summary_merge_rejects_stage_fields_from_other_snapshots or result_view_lifecycle_ignores_stale_stage1_baseline_count_after_repoint or result_view_lifecycle or workflow_stage_summaries_ignore_stale'` -> `10 passed, 63 deselected`
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py -q -k 'result_view_lifecycle or workflow_stage_summaries or completed_summary_merge or candidate_sync'` -> `16 passed, 63 deselected`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
- Remaining structural follow-up: `result_view_lifecycle` still needs a single atomic persisted source-of-truth for `baseline/current/served/progress/materialization` instead of being assembled from summary/result-view/candidate-source payloads.
- ECS deploy readiness follow-up recorded:
  - `docs/ECS_PRELAUNCH_CHECKLIST.md` now has a `Current-Code Replacement Gate` for the old hosted `/opt/sourcing-ai-agent` code root: stop old backend/worker first, deploy one canonical current code root, align systemd `WorkingDirectory` / `ExecStart` / venv / env file / Nginx docs, then archive or remove the old root only after probes pass.
  - The same gate explicitly blocks deploy if `show-control-plane-runtime` is not `postgres_only + shared_memory`, if `/api/providers/apify/webhook` is still `404`, if production is not `production + live`, or if Apify webhook preflight would not attach webhooks.
  - `docs/NEXT_TODO.md` now expands the atomic lifecycle refactor into a concrete `job_result_lifecycle` follow-up with one persisted source for baseline/current/served snapshot ids, expected/served counts, delta fetch/materialization counters, lifecycle phase, and public API reads.
  - 2026-04-30 pre-deploy probe reconfirmed the boundary: local hosted submit contract is `ready` and would attach the default webhook, but the current public hosted route still returns `HTTP 404` until ECS is redeployed with current code.

### Heavy scripted browser fixtures now replay real samples with remote-wait timing

- Tightened the OpenAI ChatGPT and Lovable heavy scripted fixtures so provider payloads come from real captured sample files (`configs/scripted/samples/...`) instead of generated placeholders for the default browser/manual path.
- Fixed scripted Harvest timing realism: profile/company actor `execute_sleep_seconds` can now run as submit-after `remote_wait` (`execute_sleep_position=remote_wait`). The worker gets `run_id/dataset_id` immediately, enters `waiting_remote_harvest`, and browser scripted webhook driving waits for `scripted_remote_ready_epoch_ms` before posting the Apify-shaped terminal event. This prevents false `true active slots=4 / remote actor workers=1` submit-blocking readings caused by sleeping before the pending checkpoint existed.
- Clarified result lifecycle semantics: `delta_profile_materialized_count` means "delta rows visible on the candidate board." During partial materialization it can use candidate state or explicit patch counts; once the current snapshot serves the full delta, it uses the served delta count instead of being held back by lower profile-richness/detail counts.
- Browser observer now treats materialization stuck after profile fetch as a budgeted condition (`30s`) rather than failing on a normal short repoint tail. The OpenAI ChatGPT heavy run observed a `~9.6s` fetched-complete baseline-serving window before repointing to current snapshot, so it is diagnostic but not a failure.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py tests/test_harvest_connectors.py -q -k 'result_view_lifecycle or candidate_sync or scripted_remote_wait_after_submit'` -> `13 passed, 160 deselected`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 SOURCING_HEAVY_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=30 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_chatgpt_delta_streaming_contract_observation'` -> `1 passed, 11 deselected`
  - same command with `-k 'lovable_live_roster_streaming_contract_observation'` -> `1 passed, 11 deselected`
- Latest report highlights:
  - OpenAI ChatGPT: `harvest_profile_search=4`, `harvest_profile_scraper_batch=12`, max queued/webhook-eligible profile workers `4`, final sync `550/550`, profile `250/250`, no progress/snapshot/materialization guardrail failures.
  - Lovable: `harvest_company_employees=1`, `harvest_profile_search=2`, `harvest_profile_scraper_batch=7`, final sync `140/140`, profile `140/140`, no progress/snapshot/materialization guardrail failures.

### Live Harvest actor submit now defaults Apify webhook URLs by runtime

- Closed the remaining workflow-level webhook gap: `src/sourcing_agent/harvest_connectors.py` now resolves an Apify ad-hoc webhook URL even when the launcher did not explicitly set `SOURCING_APIFY_WEBHOOK_URL`, as long as the runtime is live and the environment is recognized:
  - `production + live` -> `https://api.111874.xyz/api/providers/apify/webhook`
  - `local_dev + live` -> `https://api.111874.xyz/local-dev/providers/apify/webhook`
- Explicit request/env URLs still win (`apify_webhook_url`, `provider_webhook_url`, `SOURCING_APIFY_WEBHOOK_URL`, `APIFY_WEBHOOK_URL`), and `scripted/simulate/replay` do not auto-attach external Apify webhooks.
- Added `SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED=0` as the deliberate opt-out for no-webhook diagnostics.
- Updated `scripts/apify_webhook_preflight.py` so it checks the effective submit contract, including default URLs, instead of only checking whether a manual URL env var exists.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py -q -k 'webhook or ad_hoc_webhook or default_webhook'` -> `6 passed, 85 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py -q -k 'preflight or remote_provider_event or webhook or local_provider_event_watcher'` -> `16 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py scripts/apify_webhook_preflight.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode hosted --skip-connectivity-probe --json-only` -> `status=ready`, `would_attach_webhooks=true`, default URL `https://api.111874.xyz/api/providers/apify/webhook`
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode local-live --skip-connectivity-probe --json-only` -> `status=ready`, `would_attach_webhooks=true`, default URL `https://api.111874.xyz/local-dev/providers/apify/webhook`
- Remaining boundary: default URL attachment does not prove network delivery. Hosted production still needs deployment verification that `/api/providers/apify/webhook` is current code and returns `202`; local-dev still needs the reverse tunnel running for real Apify callbacks.

### Runtime preflight unified entrypoint added

- Added `docs/RUNTIME_PREFLIGHT.md` as the canonical startup/readiness entrypoint before local dev, scripted/browser test environments, local live smoke, and hosted/ECS workflows.
- The preflight doc consolidates previously scattered guidance from `DEVELOPMENT_GUIDE.md`, `TEST_ENVIRONMENT.md`, `APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`, `ECS_PRELAUNCH_CHECKLIST.md`, and `frontend-demo/README.md`.
- It explicitly records the local process-lifetime failure mode: foreground dev servers, raw `nohup ... &`, and background `&` can be cleaned up with transient tool sessions; use durable launch targets like `make dev-launch-backend` / `make dev-launch-frontend`, or a named `screen` session for scripted interactive environments.
- It also centralizes the webhook boundary: local-dev relay smoke does not prove hosted production route readiness, and actors submitted without `SOURCING_APIFY_WEBHOOK_URL` in the actual backend/worker process do not get Apify ad-hoc webhooks.
- Linked it from `docs/INDEX.md` and added an `AGENTS.md` local-environment guardrail requiring agents to read the preflight before launching runtime services or live smoke.

### Apify webhook preflight and bidirectional duplicate-event guard documented

- Added `scripts/apify_webhook_preflight.py` as a no-cost readiness gate before live Apify/Harvest workflows. It verifies live webhook URL presence, hosted vs local-dev relay path, Harvest profile-scraper token availability, submit-contract ad-hoc webhook attachment, and optional HTTP `202` connectivity probe without submitting actors.
- Locked the watcher/webhook/recovery race contract in tests:
  - watcher-first then webhook-late: completed worker records `remote_provider_event: received_late`, `recovery_count=0`
  - webhook-first then watcher-late: completed worker records `remote_provider_event: received_late`, `recovery_count=0`
  - neither late path can retrigger recovery, provider submit, ingest, materialization, or reconcile
- Documented the operational rule in `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`, `docs/ECS_PRELAUNCH_CHECKLIST.md`, `docs/TESTING_PLAYBOOK.md`, and `docs/NEXT_TODO.md`: the backend/worker process that submits Harvest actors must be started with `SOURCING_APIFY_WEBHOOK_URL`; otherwise the submit contract attaches no Apify ad-hoc webhook and the job may only be completed by local watcher/recovery.
- Reconciled the 2026-04-29 OpenAI ChatGPT live-smoke finding: DB events were all `source=local_provider_event_watcher`, so that run did not validate external Apify webhook delivery. The likely causes are missing webhook env in the submitting process, public relay/backend reachability failure, or token/endpoint mismatch.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py -q -k 'preflight or remote_provider_event or webhook or local_provider_event_watcher'` -> `15 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py tests/test_remote_provider_events.py -q -k 'ad_hoc_webhook or provider_webhook or remote_provider_event or local_provider_event_watcher or preflight'` -> `16 passed, 86 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check scripts/apify_webhook_preflight.py tests/test_remote_provider_events.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --help` -> passed
  - `PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode scripted --json-only` -> `status=ready`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `git diff --check -- PROGRESS.md docs/ECS_PRELAUNCH_CHECKLIST.md docs/TESTING_PLAYBOOK.md docs/NEXT_TODO.md` -> passed
- Remaining boundary: hosted production external webhook still needs deployment verification on `https://api.111874.xyz/api/providers/apify/webhook`; local-dev relay smoke does not prove the hosted production route.

### Execution progress wording and materialized-board count contract tightened

- Frontend execution-process metrics now hide internal `Evidence` and `Workers / lanes` concepts from the user-facing flow. Visible metrics use product wording: `新取回在职候选人`, `新取回离职候选人`, `总候选人数量`, and `需人工审核候选人`.
- `result_view_lifecycle` now exposes `delta_profile_materialized_count`; the candidate sync card prefers that explicit backend contract for `已物化到看板` instead of deriving materialized progress from fetched count or `served_candidate_count - baseline_candidate_count`.
- Added `configs/scripted/openai_agent_and_lovable_streaming.json` and include-merging support in the scripted provider scenario loader. The interactive scripted environment now seeds OpenAI baseline plus Lovable identity and can test both `帮我找OpenAI做Agent方向的人` and `帮我找Lovable的全部成员` from one no-cost backend/frontend.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py tests/test_scripted_provider_scenario.py -q` -> `11 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "delta_profile_progress_uses_profile_batches_for_fetch_required_count or result_view_lifecycle_uses_explicit_materialized_count_for_board_progress or result_view_lifecycle_infers_baseline_count_from_stage1_preview or full_local_reuse_suppresses_delta_profile_progress"` -> `3 passed, 63 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/scripted_provider_scenario.py tests/test_results_api.py tests/test_scripted_provider_scenario.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
- Manual scripted test env restored:
  - frontend: `http://127.0.0.1:4185`
  - backend: `http://127.0.0.1:8785`
  - provider mode: scripted; no live Harvest/DataForSEO calls.

### Delta/browser hard gates cover stage contract, sync lag, and overlay ordering

- Closed the remaining hard-browser regressions after the OpenAI ChatGPT/Agent live-smoke follow-up:
  - `/api/jobs/{job_id}/dashboard` and `/progress` now expose `execution_phase_contract`; the frontend prefers that contract for execution-process stage labels/details so it does not surface `Public Web Stage 2` while LinkedIn acquisition or local asset materialization is still active.
  - Candidate dashboard hydration now carries refreshed `linkedin_stage_1_progress` and `result_view_lifecycle` through page merges. Active delta/materialization result views poll at 1s while stable asset views keep the 5s interval, so the sync card stops lingering on stale `300/300` after the backend knows `expected_candidate_count` has grown.
  - The browser observer still fails on stale completed sync, but now uses a 2.5s budget so one-frame API/UI sampling skew does not hide or create false failures.
  - Delta asset overlays now preserve baseline order and append true delta additions, preventing the temporary overlay first page from jumping from `0103/0104/0105` to `0001/0002/0003` after final materialization.
- Added/updated regression coverage:
  - frontend sync formatter covers baseline-serving delta pending, e.g. `300/388` with `新增 LinkedIn Profile 已取回 0/88`.
  - candidate materialization covers stable baseline-first overlay ordering.
  - browser hard-mode assertions now cover premature public-web labels, stale complete sync beyond budget, materialized-ahead-of-fetched, progress regressions, and snapshot divergence.
- Latest heavy browser pass:
  - OpenAI Agent baseline+delta: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`, job `557df2315ab8`, `harvest_profile_search=4`, `harvest_profile_scraper_batch=12`, baseline-first and final-current observed, no stale-sync/progress/snapshot/materialization guardrail failures.
  - OpenAI ChatGPT baseline+delta: `output/playwright/frontend-browser-e2e-openai-chatgpt-delta-streaming-report.json`, job `3e936f18d309`, `harvest_profile_search=4`, `harvest_profile_scraper_batch=10`, baseline-first and final-current observed, no stale-sync/progress/snapshot/materialization guardrail failures.
  - Lovable live roster: `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`, job `3ca5152d880e`, `harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=7`, final live-roster sync `145/145`, no guardrail failures.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py -q` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k "asset_population_overlay_preserves_baseline_order"` -> `1 passed, 50 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/candidate_materialization.py tests/test_candidate_artifacts.py tests/test_frontend_candidate_sync_summary.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k "openai_agent_delta_streaming_contract_observation or openai_chatgpt_delta_streaming_contract_observation or lovable_live_roster_streaming_contract_observation"` -> `3 passed, 9 deselected`
- Boundary:
  - Scripted browser mode validates local webhook endpoint/event ingestion and job-scoped recovery using Apify-shaped terminal events. The real external Apify network callback roundtrip remains an explicit tunnel/hosted live smoke, not part of the default no-cost loop.


## 2026-04-29 (Asia/Shanghai)

### Result-view pointer consistency and heavy scripted browser gates re-run

- Closed the remaining live-roster progress gap exposed by the Lovable heavy browser fixture: `linkedin_stage_1_progress` now consumes completed `harvest_company_employees` roster snapshots in addition to search-seed entries, so full live roster execution metrics count current roster rows instead of showing `已取回在职候选人0`.
- Added a results API regression that builds a Lovable live-roster shape with company-employees current rows, former search-seed rows, and a profile-scraper batch, then asserts `current=3`, `former=2`, `deduped=5`, and profile required/fetched/queued counts from the same contract.
- Tightened the Lovable heavy browser assertion so it fails unless live-roster samples observe at least `100` current returned rows in dashboard/progress, preventing the previous “browser passed while execution metrics were wrong” gap.
- Re-ran both heavy scripted browser paths after the fix:
  - OpenAI ChatGPT baseline+delta: report `output/playwright/frontend-browser-e2e-openai-chatgpt-delta-streaming-report.json`, job `5e96732b7d1c`, provider invocations `14` (`harvest_profile_search=4`, `harvest_profile_scraper_batch=10`), max progress `151 current / 78 former / 229 deduped`, no snapshot divergence, no progress regression, no materialization-stuck-after-fetch.
  - Lovable live roster: report `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`, job `16b3ebc2844d`, provider invocations `9` (`harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=7`), max progress `120 current / 25 former / 145 deduped`, final sync `候选人同步145/145本次 LinkedIn Profile 已取回 145/145，已物化到看板 145/145`, no snapshot divergence, no progress regression, no materialization-stuck-after-fetch.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "live_roster_stage1_progress_counts_company_employee_roster_entries or delta_profile_progress_uses_profile_batches_for_fetch_required_count or baseline_serving_result_view or live_roster_preview"` -> `4 passed, 58 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py -q` -> `3 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py tests/test_frontend_browser_e2e.py` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k "openai_chatgpt or lovable_live_roster"` -> `2 passed, 10 deselected`

### OpenAI ChatGPT live-smoke scheduler/materialization follow-ups fixed

- Completed-job inline reconcile now has a narrow cross-process in-flight slot keyed as `completed_workflow_reconcile:{job_id}:{kind}`. Worker-completion callbacks still avoid the broad workflow job lease, but duplicate concurrent callbacks for the same job/kind now emit a structured `phase=coalesced` skip instead of re-entering materialize/retrieval.
- Callback reconcile re-collects unconsumed inline workers after acquiring the slot, so stale worker lists captured before another callback writes `inline_incremental_ingest` cannot drive repeated materialization.
- Materialize signatures now include worker IDs, and scripted smoke expectations can cap `repeated_materialize_signature_count` and `same_worker_reconcile_repeat_count` at zero.
- Provider completion discovery is more robust in local/hosted live smoke: local provider-event watcher fallback is enabled by default even when an Apify webhook URL is configured. Strict external webhook roundtrip smoke can still disable it with `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=0`.
- Added the OpenAI ChatGPT scoped-delta scripted fixture and matrix (`configs/scripted/openai_chatgpt_scoped_delta_streaming.json`, `configs/scripted/openai_chatgpt_scoped_delta_smoke_matrix.json`) seeded from the 2026-04-29 live-smoke shape: baseline `890`, final `1061`, search `166 current / 84 former`, deduped `250`, profile batch scope `165`.
- Tightened the ChatGPT matrix so it can no longer pass as a generic new job: expectations now require `dispatch_strategy=delta_from_snapshot`, `planner_mode=delta_from_snapshot`, `requires_delta_acquisition=true`, `effective_acquisition_mode=baseline_reuse_with_delta`, and keyword `ChatGPT`.
- The ChatGPT matrix explicitly sets `harvest_profile_actor_global_inflight=4` / `harvest_profile_batch_submit_global_inflight=4`; the latest isolated run observed `queued_worker_count=4`, `waiting_remote_harvest_count=4`, and `active_worker_count=4`.
- Latest isolated ChatGPT scripted run (`output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_report_current.json`) passed with `provider_invocations=14` (`harvest_profile_search=4`, `harvest_profile_scraper_batch=10`), `profile_url_total_count=529`, `fetched_profile_count=529`, `board_total_candidates=529`, `repeated_materialize_signature_count=0`, and `same_worker_reconcile_repeat_count=0`. The synthetic reference runtime uses a smaller OpenAI baseline than the live `890` run, but still exercises the large scoped-delta provider/materialization path.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "wrong_delta_dispatch_contract or repeated_materialize_churn or remote_actor_worker_count"` -> `2 passed, 24 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/openai_chatgpt_scoped_delta_streaming_foundation_current --seed-reference-runtime --provider-mode scripted --scripted-scenario configs/scripted/openai_chatgpt_scoped_delta_streaming.json --matrix-file configs/scripted/openai_chatgpt_scoped_delta_smoke_matrix.json --case openai_chatgpt_scoped_delta_streaming --fast-runtime --runtime-tuning-profile fast_smoke --poll-seconds 0.1 --max-poll-seconds 180 --strict --timing-summary --report-json output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_report_current.json --summary-json output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_summary_current.json` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "harvest_prefetch or completed_workflow or inline_incremental or background_snapshot_materialization"` -> `26 passed, 305 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_remote_provider_events.py -q` -> `55 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -q` -> `33 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q` -> `30 passed`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed

### OpenAI ChatGPT live-smoke result-view and progress regressions first-pass fixed

- Live-smoke case:
  - Query: `我想要OpenAI在ChatGPT组的人`
  - Job: `6fccb376681e`, history `e1ed3b16-ae33-4e97-a2ed-c4e9e29388fc`, current snapshot `20260429T174612`
- Backend fixes:
  - `job_result_views` recovery now prefers the final `candidate_source` / current workflow snapshot over stale Stage 1 baseline artifacts. The live job now repairs from baseline `20260428T183413 / 890` to current `20260429T174612 / 1061`.
  - `linkedin_stage_1_progress.profile_fetch_required_count` now uses the actual profile-scraper batch scope when profile workers exist, instead of jumping to all deduped search seed URLs after the search snapshot is restored. For the live job this stabilizes at search returned `166 current + 84 former = 250 deduped`, profile batch required/fetched `165/165`.
  - `result_view_lifecycle` now infers `baseline_candidate_count` from the Stage 1 baseline preview when the served view has already repointed to the current snapshot, so frontend materialized-delta math no longer has to guess.
  - Workflow stage summaries ignore stale runtime fields from reused snapshot summary files when their timestamps predate the current job. This prevents a new delta job from showing another job's old Stage 1 timestamps.
- Frontend/scripted fixes:
  - Candidate sync uses backend lifecycle served/expected counts instead of the number of hydrated frontend candidate pages, avoiding `890 -> 256 -> 96` display regressions during background page hydration.
  - Execution metrics label the profile-detail scope as `需补取 LinkedIn Profile` to distinguish it from all deduped search results.
  - When LinkedIn profile work is still pending, the execution timeline keeps LinkedIn Stage 1 visibly running instead of advancing to Public Web/Final stages based only on stale stage summary status.
  - Browser scripted observation now records candidate-source/result-view/lifecycle snapshot IDs, candidate-sync parsed counts, non-monotonic progress regressions, snapshot divergence samples, and "fetched complete but materialization still baseline" samples.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_results_api.py -k "delta_progress_exposes_search_and_profile_fetch_counts or profile_batches_for_fetch_required_count or baseline_count_from_stage1_preview or stale_snapshot_stage_runtime_fields or full_local_asset_reuse_lifecycle" tests/test_frontend_candidate_sync_summary.py` -> `5 passed, 57 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_pipeline.py -k "final_candidate_source_over_stage1_baseline or materialized_current_job_snapshot_over_stale_result_view or stage_summary" tests/test_results_api.py -k "delta_progress or result_view_lifecycle or full_local_asset_reuse_lifecycle or stale_snapshot_stage_runtime_fields" tests/test_frontend_candidate_sync_summary.py` -> `4 passed, 388 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_results_api.py tests/test_pipeline.py -k "result_view or stage_summary or delta_progress or full_local_asset_reuse or harvest_profile_completion or harvest_prefetch or completed_workflow or inline_incremental"` -> `37 passed, 352 deselected`
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
- Remaining follow-ups:
  - Materialization/reconcile repeat coalescing and provider-slot utilization are superseded by the scheduler/materialization follow-up entry above.
  - External Apify/Harvest webhook network roundtrip remains a separate live/tunnel smoke; the scripted browser mode only validates the local webhook endpoint/recovery path.

### Excel action buttons hidden and ChatGPT live-smoke regressions documented

- Frontend safety change:
  - The Excel batch group buttons `导入目标候选人` and `导出候选人包` are hidden from the new-search page until the real product contract is completed.
  - The underlying handlers are left in place behind a closed feature flag so the action can be re-enabled after job-scoped import/export semantics and browser E2E are ready.
- Live-smoke handoff:
  - The OpenAI ChatGPT-group run `6fccb376681e` / history `e1ed3b16-ae33-4e97-a2ed-c4e9e29388fc` exposed non-trivial regressions in execution stage semantics, unstable profile/candidate progress counters, board hydration oscillation, result-view repoint divergence, repeated materialization/reconcile churn, and profile-scraper actor-slot utilization metrics.
  - `docs/NEXT_TODO.md` now has a dedicated high-priority section with the observed failure modes and required scripted/browser regression coverage.
- Additional diagnosis:
  - Postgres showed the completed job summary candidate source at snapshot `20260429T174612` with `1061` candidates, while `job_result_views` still served a recovered baseline view at `20260428T183413`; this supports treating the candidate-board count churn as a result-view lifecycle/repoint bug, not just frontend copy.
- Runtime stability fix:
  - The same validation pass found `worker-recovery-daemon` could crash while writing service status when a callback summary contained a domain object such as `Candidate`.
  - `service_daemon` now serializes status/log payloads through a JSON-safe conversion that respects `to_record()`, so diagnostic summaries cannot kill the daemon.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_service_daemon.py -k "serializes_domain_objects or service_run_writes_status"` -> `2 passed, 12 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Excel intake result views expose a job-scoped import filter

- Product issue:
  - Excel intake now serves the company asset board quickly, but the board can contain the whole company snapshot; users need a direct way to locate the rows imported by the current workbook.
- Contract:
  - Excel workflow records a result-view-scoped `job_scoped_candidate_markers` entry with `marker_id=excel_intake:current_job`, label `本次Excel导入`, and only the candidate IDs matched or created by this Excel job.
  - Dashboard serialization attaches that marker as candidate provenance only for the current job result view; it does not mutate global candidate assets.
  - The frontend recall filter adds a `本次Excel导入` bucket when marked candidates are present, so imported rows can be isolated from the full company board.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_frontend_history_recovery.py -k "excel_intake_workflow"` -> `4 passed, 17 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_frontend_candidate_filters.py` -> `1 passed`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_excel_intake.py` -> `17 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Excel intake result-view serving no longer blocks on full artifact rebuild

- Root-cause finding from the 17:10 Excel upload retry:
  - The three Excel jobs (`ed2560030342` OpenAI, `5dd7314aefc2` Google, `34cd5da542b6` Meta) were left `running/acquiring` after the local dev backend/frontend process died.
  - Before that process exit, Excel intake had already started writing large snapshot artifacts (notably Meta `normalized_artifacts/candidates/*`), because the workflow passed `build_artifacts=True` into `ExcelIntakeService.ingest_contacts`.
  - The orchestrator only wrote `acquiring/completed`, preview, result view, and history `results` after `ingest_contacts()` returned, so a large inline artifact rebuild could make Excel upload look stuck and prevent the candidate board from becoming visible.
- Backend fix:
  - Excel workflow now keeps the user-requested `build_artifacts=True` as intent, but forces the synchronous intake/merge call to `build_artifacts=False`.
  - The workflow serves the candidate board from merged `candidate_documents.json` plus the job-scoped asset-population overlay/result view.
  - The final summary and company-asset stage expose `artifact_build_deferred=true`, making the behavior explicit instead of silently pretending full normalized artifacts were rebuilt inline.
- Frontend polish:
  - Manual-review candidate action text is now `打开LinkedIn`.
  - Manual-review bottom actions align the LinkedIn button with the review-status select control.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_frontend_history_recovery.py -k "excel_intake_workflow"` -> `4 passed, 17 deselected`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_excel_intake.py` -> `17 passed`
  - `npm run build` in `frontend-demo` -> passed

### Excel intake LinkedIn identity matching contract tightened

- Root-cause finding from the stuck 16:19 Excel upload jobs: the workbook LinkedIn URLs were present in `linkedin_profile_registry` as `fetched`, but Excel intake built local candidate inventory before checking registry/cache. On a large local asset set this can stall the thread before any result event is written.
- Backend fix:
  - direct LinkedIn URL rows now first load fetched registry raw profile cache by normalized/sanity URL key, without scanning full candidate inventory and without rejecting on stale/wrong Excel company fields
  - URL/slug local exact match is now global identity matching; company fields no longer veto a URL identity hit
  - company+name/email/near match remains a fallback for rows without URL or without URL identity/cache hit
  - `OpenAI & Meta` and `Meta/OpenAI` company cells split into separate OpenAI/Meta route jobs; each route does company+name fallback against its route company
- Ops contract documented in `docs/WORKFLOW_OPERATIONS_PLAYBOOK.md`.
- Validation:
  - `PYTHONPATH=src ./.venv/bin/python -m py_compile src/sourcing_agent/excel_intake.py src/sourcing_agent/storage.py` -> passed
  - `PYTHONPATH=src ./.venv/bin/python -m pytest tests/test_excel_intake.py -q -k 'registry_cache or compound_company or exact_local_linkedin or routes_profile'` -> `4 passed, 13 deselected`
  - `PYTHONPATH=src ./.venv/bin/python -m pytest tests/test_excel_intake.py -q` -> `17 passed`

### Candidate sync card separates board load, intent recall, and provider fetch

- Live smoke `de24677af2d1` (`帮我找OpenAI做Agent方向的人`) exposed a frontend population-contract bug:
  - the card displayed `候选人同步 890/890` from the served full OpenAI asset snapshot, not from frontend board hydration progress
  - it also displayed `新增 LinkedIn Profile 已取回 4/4，已物化到看板 4/4`, which conflated this run's provider fetch micro-progress with the Agent recall/filter population (`Agent 46`)
- Frontend fix:
  - added `frontend-demo/src/lib/candidateSyncSummary.ts` as the canonical sync-card formatter
  - `候选人同步` now uses loaded board candidates over expected board candidates, so hydration can visibly progress instead of always inheriting `result_view_lifecycle.served_candidate_count`
  - recall bucket counts now render separately as `当前意图匹配 Agent 46 人`
  - initial split made `4/4` a separate provider-fetch line instead of a materialized-delta line; the follow-up entry below tightens full-local-reuse further and hides that line when no delta/new profile acquisition is applicable
- Validation:
  - `./.venv/bin/python -m pytest tests/test_frontend_candidate_sync_summary.py -q` -> `2 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Full-local-reuse candidate sync no longer shows provider micro-progress

- Follow-up on the same live smoke: full local asset reuse should not show `本次 LinkedIn Profile 已取回 4/4`, because no delta/new profile acquisition is part of the serving contract.
- Backend fix:
  - `result_view_lifecycle` now exposes `delta_profile_progress_applicable` and `delta_profile_progress_reason`.
  - For `full_local_asset_reuse` / `reuse_snapshot_only` with `requires_delta_acquisition=false`, lifecycle `delta_profile_*` counters are zeroed and marked `not_applicable_full_local_asset_reuse`, even if diagnostic `linkedin_stage_1_progress` still has historical/search-seed micro counts.
- Frontend fix:
  - `DashboardData` now carries `effectiveExecutionSemantics`.
  - `candidateSyncSummary` hides profile fetch text when lifecycle marks delta profile progress as not applicable, or when frontend sees `fullLocalAssetReuse && !requiresDeltaAcquisition`.
  - Live roster/full acquisition remains covered: when progress is applicable and full-local-reuse is false, the card still renders `本次 LinkedIn Profile 已取回 fetched/required`.
- Verified on current local page:
  - `http://127.0.0.1:4173/?history=a817f06e-a3d7-4c9a-8a48-3fb1e2a223ee&job=de24677af2d1`
  - visible text is now `候选人同步 890/890` and `当前意图匹配 Agent 46 人`; no `LinkedIn Profile 已取回 4/4` line.
- Validation:
  - `./.venv/bin/python -m pytest tests/test_results_api.py -q -k 'delta_progress_exposes_search_and_profile_fetch_counts or full_local_asset_reuse_lifecycle_suppresses_delta_profile_progress'` -> `2 passed, 54 deselected`
  - `./.venv/bin/python -m pytest tests/test_frontend_candidate_sync_summary.py -q` -> `3 passed`
  - `npm --prefix frontend-demo run build` -> passed

### Scripted webhook-event browser mode and handoff metrics split

- Added a no-cost scripted webhook-event driver to the OpenAI Agent delta-streaming browser observation:
  - `frontend-demo/scripts/run_workflow_e2e.mjs --drive-provider-webhook-events` finds waiting remote provider workers, posts terminal Apify-shaped payloads to `/api/providers/apify/webhook?sync=1`, and records `providerWebhookEvents`.
  - This is deliberately different from the older `--drive-worker-recovery` path: the browser driver no longer calls `/api/workers/daemon/run-once` directly when webhook mode is enabled.
  - Coverage boundary: this validates local webhook endpoint -> `remote_provider_event` -> job-scoped recovery -> next worker submit. It still does not test the external Apify network callback roundtrip.
- Split event-level metric naming so scheduler handoff and provider-attempt time are not conflated:
  - `local_completion_to_next_submit_start_ms` is the pure local handoff from completion apply to next-submit start.
  - `next_submit_provider_attempt_elapsed_ms` is the next-submit attempt duration and can include scripted provider sleep/pending behavior.
  - Existing `local_to_next_submit_start_ms` and `next_submit_attempt_elapsed_ms` remain as compatibility aliases.
- Candidate board sync card layout was adjusted:
  - right card now renders `候选人同步 N/M` on the first line and `新增 LinkedIn Profile 已取回 X/Y，已物化到看板 A/Y` on the second line
  - search field width was reduced so the sync card can fit two aligned lines on desktop
  - browser observer now recognizes the new `LinkedIn Profile 已取回 X/Y` wording for partial-progress assertions
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_remote_provider_events.py -q` -> `17 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'provider_case_report or event_level_efficiency'` -> `5 passed, 19 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_efficiency.py tests/test_workflow_efficiency.py tests/test_remote_provider_events.py tests/test_frontend_browser_e2e.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_agent_delta_streaming_contract_observation'` -> `1 passed, 9 deselected`
- Latest browser report:
  - `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`
  - job `61b3d7b4a5de`
  - `providerWebhookDriven=true`, `workerRecoveryDriven=false`, `providerWebhookEvents=4`
  - `baselineFirstBoardObserved=true`, `finalCurrentSnapshotObserved=true`, `candidateSyncProfileProgressObserved=true`
  - observed partial sync text: `候选人同步400/597新增 LinkedIn Profile 已取回 100/297，已物化到看板 100/297`

### Lovable 100+ scripted live-roster smoke added

- Replaced the too-small Physical Intelligence live-roster scripted gate with a Lovable fixture that exercises a 100+ person full-roster path without live provider calls.
- Added `configs/scripted/lovable_live_roster.json`:
  - `harvest_company_employees` returns `120` unique Lovable roster rows.
  - `harvest_profile_search` returns `25` former Lovable rows, preventing broad former-lane zero-result retry from being misread as duplicate dispatch in this smoke.
  - `harvest_profile_scraper_batch` hydrates profile details with realistic work/education fields from requested URLs.
- Updated `configs/scripted/small_company_live_roster_smoke_matrix.json` to run `帮我找Lovable的全部成员` and require:
  - terminal job
  - no duplicate provider dispatch
  - no unexpected Public Web/DataForSEO stage
  - no event-level efficiency violation
  - `harvest_company_employees>=1`, `harvest_profile_scraper_batch>=1`
  - `board_total_candidates>=100`
- First Lovable run failed usefully: all generated roster rows shared the same `fullName` (`Lovable Roster`), so canonicalization collapsed `120` rows into `1` candidate; the former-lane broad profile-search also had no fixture rule and triggered zero-result retry duplicates. The fixture now generates unique names and has an explicit former profile-search rule.
- Passing run:
  - report: `output/scripted_smoke_current/lovable_live_roster_scripted_report.json`
  - summary: `output/scripted_smoke_current/lovable_live_roster_scripted_summary.json`
  - job: `7933a0ff59cf`
  - `expectation_failures=[]`
  - provider invocations: `10` total (`harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=6`, plus 2 scripted company identity searches)
  - board: `145` total candidates, first page `24`, profile fetch progress `145/145`
  - guardrails: duplicate provider dispatch `false`, default-off public web violation `false`, event-level efficiency violation `false`
  - post-terminal recovery settled in `1` round with no remaining recoverable workers
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py -q -k 'lovable_live_roster_scripted_fixture or company_employees_scripted_completion_exposes_dataset_items_artifact or harvest_profile_batch_execute_with_checkpoint_supports_scripted_pending_rounds'` -> `3 passed, 84 deselected`
  - `./.venv-tests/bin/ruff check tests/test_harvest_connectors.py` -> passed
  - Lovable scripted smoke command in `docs/TESTING_PLAYBOOK.md` -> passed

### OpenAI Agent slow/strict browser observation completed

- Ran the full OpenAI Agent slow/strict scripted browser observation against isolated runtime `runtime/test_env/openai_agent_delta_streaming_slow_strict`.
- Coverage boundary confirmed:
  - This does not test the real external Apify/Harvest webhook network callback.
  - It tests webhook-equivalent local provider completion/recovery/reconcile scheduling after scripted provider completion, including long elapsed waits and post-terminal recovery.
- Result:
  - job: `4f0f5f991133`
  - report: `output/playwright/openai-agent-slow-strict-browser-report.json`
  - `contractReady=true`, `baselineFirstBoardObserved=true`, `finalCurrentSnapshotObserved=true`
  - baseline board first observed at baseline `20260414T120300`, `300/300`
  - final current snapshot observed at `20260429T134659`, expected `597`
  - provider invocations: `17` total (`harvest_profile_search=4`, `harvest_profile_scraper_batch=13`)
  - profile batch workers drained with `recoverable_worker_count=0`
  - full post-terminal tail finalized around `37m41s` after job creation
- Observability gaps found and left as backlog:
  - browser observer currently exits when workflow reaches completed, before very long post-terminal provider/materialization tails are always drained
  - `/health` can clear remote-wait metrics after the job is terminal even if completed-job background workers remain active/recoverable

### Delta candidate sync card progress wording corrected

- Manual interactive scripted testing showed the candidate board could display `候选人同步 597/597` while only `50/297` new LinkedIn profiles had been fetched. Root cause: the card used the discovered/current snapshot candidate count as "served" even while the delta profile tail was still running.
- Frontend sync card now uses `baseline_candidate_count + fetched_delta_profile_count` while new LinkedIn profile fetch is still in progress, then switches back to final `served/expected` once the delta tail is complete.
- User-facing wording changed from `LinkedIn fetched/total，追平，排队，可重试` to `新增 LinkedIn Profile 已取回 fetched/required，已物化到看板 materialized/required` plus retryable only when non-zero. `materialized` is derived from board-visible candidate sync progress so the card separates provider fetch progress from what the user can actually consume on the board.
- Validation:
  - `npm --prefix frontend-demo run build` -> passed

### OpenAI Agent scripted slow/strict worker timing matrix added

- Added a live-like timing matrix to `configs/scripted/openai_agent_scoped_delta_streaming.json` for scheduler/recovery reliability testing:
  - profile-scraper batches now have deterministic varied durations: `45s`, `60s`, `90s`, `120s`, and `180s`.
  - A later current-lane batch is intentionally faster than earlier batches to exercise out-of-order completion.
  - A current mid-tail batch carries a retryable provider delay.
  - A former long-tail batch carries a retryable timeout on the primary OpenAI Agent path, not only a dormant fixture rule.
- Added `scripts/dev_scripted_openai_agent_delta.sh --slow-strict-runtime` / `SCRIPTED_SLOW_STRICT_RUNTIME=1`:
  - default interactive mode still caps scripted Harvest sleeps at `3s`
  - fast mode still caps at `0.1s`
  - slow/strict mode sets `SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=none`, preserving fixture sleep durations for live-like elapsed-time tests
- Scripted Harvest timeout errors can now opt into retryable behavior with `"retryable": true`; non-retryable timeout rules still fail as before.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k 'openai_agent_scripted_profile_scraper or openai_agent_scoped_delta_streaming_fixture or scripted_profile_scraper_timing_matrix'` -> `3 passed, 86 deselected`
  - `bash -n scripts/dev_scripted_openai_agent_delta.sh` -> passed
  - `bash ./scripts/dev_scripted_openai_agent_delta.sh --slow-strict-runtime --print-config` -> `slow_strict_runtime=1`, `scripted_harvest_sleep_seconds_cap=none`
  - `bash ./scripts/dev_scripted_openai_agent_delta.sh --fast-runtime --print-config` -> `slow_strict_runtime=0`, `scripted_harvest_sleep_seconds_cap=0.1`

### Interactive OpenAI Agent scripted delta environment exposed

- Added a user-facing interactive scripted test entry for the Delta asset board streaming scenario:
  - `make test-env-scripted-openai-agent` seeds the OpenAI reference baseline, starts an isolated scripted backend/worker daemon, and launches the frontend.
  - `make test-env-scripted-openai-agent-backend` starts backend only; `status` / `logs` / `stop` targets are available for the same runtime namespace.
  - Default runtime: `runtime/test_env/openai_agent_delta_streaming`; backend `8785`, frontend `4185`.
- Safety boundary:
  - The entrypoint forces `SOURCING_EXTERNAL_PROVIDER_MODE=scripted` and uses `configs/scripted/openai_agent_scoped_delta_streaming.json`.
  - Default secrets now live under the isolated runtime (`runtime/test_env/openai_agent_delta_streaming/secrets/providers.local.json`) and are initialized as empty JSON, so this interactive path does not read repo live provider secrets by default.
  - It does not test the external Apify webhook network path; it tests scripted provider completion plus local daemon/recovery-driven reconcile and downstream snapshot/result-view behavior.
- Corrected the interactive realism gap found in manual testing:
  - The interactive entry now defaults to `SCRIPTED_FAST_RUNTIME=0`; scripted Harvest sleeps are capped at `3s`, not `0.1s`, so profile-search/profile-scraper pending and recovery are visible in the execution page.
  - `configs/scripted/openai_agent_scoped_delta_streaming.json` now gives profile-search rules explicit sleeps, so candidate-list acquisition is not instant.
  - Reference smoke baseline candidates now include structured `experience_lines` / `education_lines`, `education`, `work_history`, profile capture metadata, and avatar URLs; the baseline-first board no longer renders as 300 empty profiles.
  - Startup now resets the dedicated `runtime/test_env/openai_agent_delta_streaming` by default before seeding, so stale history/job artifacts from earlier scripted runs cannot hide fixture changes. `SCRIPTED_RESET_RUNTIME=0` is available only for old-job debugging.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile scripts/seed_reference_smoke_runtime.py` -> passed
  - `bash -n scripts/dev_scripted_openai_agent_delta.sh` -> passed
  - `make -n test-env-scripted-openai-agent*` command expansion -> passed
  - seed-only validation against `runtime/test_env/openai_agent_delta_streaming_validation` -> passed
  - short backend startup smoke on port `8786` -> `/health` and `/api/runtime/health` returned `ok` with `provider_mode=scripted`, then the temporary service was stopped
  - After the manual realism correction:
    - `bash -n scripts/dev_scripted_openai_agent_delta.sh` -> passed
    - `bash ./scripts/dev_scripted_openai_agent_delta.sh --print-config` -> `fast_runtime=0`, `scripted_harvest_sleep_seconds_cap=3`, `reset_runtime=1`
    - `bash ./scripts/dev_scripted_openai_agent_delta.sh --fast-runtime --print-config` -> `fast_runtime=1`, `scripted_harvest_sleep_seconds_cap=0.1`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_smoke_runtime_seed.py tests/test_harvest_connectors.py -q -k 'smoke_runtime_seed or openai_agent_scripted_profile_search or openai_agent_scripted_profile_scraper'` -> `3 passed, 82 deselected`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
    - `./.venv-tests/bin/ruff check src/sourcing_agent/smoke_runtime_seed.py tests/test_smoke_runtime_seed.py tests/test_harvest_connectors.py` -> passed
    - Restarted `make test-env-scripted-openai-agent`; `/api/frontend-history?limit=5` now returns an empty history after reset, and the seeded OpenAI baseline has `300` candidates with non-empty `experience_lines` / `education_lines`.
- Docs updated:
  - `docs/TEST_ENVIRONMENT.md` now documents the interactive OpenAI Agent scripted delta environment and its coverage/caveats.
  - `docs/TESTING_PLAYBOOK.md` links the interactive entry from the browser/user-interaction driver section.

### Delta asset board streaming hard-mode browser validation passed

- Closed the hosted/browser hard assertion gate for the OpenAI Agent scoped-delta fixture:
  - `frontend-demo/scripts/run_workflow_e2e.mjs` now runs a background delta-streaming observer independent of the main result-wait loop, so it can capture baseline-serving board states before final results are ready.
  - The observer can opt into test-side worker recovery driving (`--drive-worker-recovery`) to mirror the hosted daemon path in isolated browser tests; it waits until the baseline board has been observed before driving recovery, so recovery cannot hide the baseline-first window.
  - Hard assertions now cover baseline-first board visibility, final current-snapshot serving, execution-process metrics, candidate-sync metrics, profile progress from non-zero to completed, and visible UI text for both execution and candidate sync profile counts.
- Latest hard-mode report:
  - report: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`
  - `contractReady=true`, `baselineFirstBoardObserved=true`, `finalCurrentSnapshotObserved=true`
  - first board sample: `~3.1s`, baseline `20260414T120300`, visible board `300/400`, lifecycle `delta_applying`
  - profile progress samples: execution page observed `已取回 LinkedIn Profile21`, then candidate sync observed `LinkedIn 100/297`, and final sync reached `LinkedIn 297/297，追平 0，排队 0，可重试 0`
  - provider long-tail: `harvest_profile_search=4`, `harvest_profile_scraper_batch=13`, `total=17`
  - current snapshot: `597/597`, first-page returned count `24`
- Validation:
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py` -> passed
  - `npm --prefix frontend-demo run build` -> passed
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_agent_delta_streaming_contract_observation'` -> `1 passed, 9 deselected`
- Remaining follow-up:
  - Use the now-green browser/user-interaction fixture to tune remaining UX bottlenecks: current candidate board still reaches the final current snapshot quickly, but materialization/finalization and post-result layer refresh should continue to be measured and improved.

### Delta asset board streaming lifecycle split started

- Implemented the first product-facing `Delta asset board streaming` contract:
  - Delta workflows now persist a baseline-serving `job_result_view` as soon as the workflow job is queued when a reusable `delta_baseline_snapshot_id` exists. This lets `/dashboard` serve the baseline asset population before current snapshot materialization finishes.
  - `/api/jobs/{job_id}/progress`, `/dashboard`, and `/candidates` now expose structured `result_view_lifecycle` with states such as `baseline_serving`, `delta_applying`, `current_snapshot_materializing`, `current_snapshot_serving`, and `post_result_layering`.
  - The same endpoints expose `linkedin_stage_1_progress`: current/former search returned counts, deduped seed/profile URL counts, profile required/fetched/queued/retryable/pending counts.
- Frontend changes:
  - The execution-process card now renders concrete Stage 1 metrics (`已取回在职候选人`, `已取回离职候选人`, `经去重得到`, `经去重需取回 LinkedIn Profile`, `已取回 LinkedIn Profile`) instead of relying only on worker counts.
  - The candidate board sync card now uses lifecycle counts, e.g. baseline/current `候选人同步 825/890`, and keeps LinkedIn profile progress (`LinkedIn fetched/required，追平，排队，可重试`) visible while the delta tail is still running.
  - Dashboard polling now continues while lifecycle state is `baseline_serving` / `delta_applying` / `current_snapshot_materializing` / `post_result_layering`, so later materialization or layering changes can refresh without a manual reload.
- Browser/scripted observation updated:
  - `frontend-demo/scripts/run_workflow_e2e.mjs --observe-delta-streaming` records lifecycle/progress payloads plus visible execution-process and candidate-sync text.
  - `SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1` now also asserts the user-facing process metrics and candidate sync card are observed.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `55 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "result_view or asset_population_fast_path or get_job_progress_uses_count_queries or get_job_progress_uses_materialized_event_summary or progress_auto_recovery or current_job_snapshot_over_stale_result_view"` -> `10 passed, 319 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `npm --prefix frontend-demo run build` -> passed
- Follow-up hard-mode browser validation is now closed in the entry above.

### OpenAI Agent delta-streaming scripted/browser foundation upgraded to real baseline+delta

- Corrected the OpenAI Agent scoped-delta foundation so it no longer uses a "claimed 1620, actual 2 candidate docs" OpenAI baseline. The reference OpenAI baseline now seeds `300` real synthetic candidate documents (`260` current, `40` former), so the scenario exercises baseline + Agent delta materialization instead of a delta-only artifact.
- Added a scripted/browser observation path for Delta asset board streaming:
  - `frontend-demo/scripts/run_workflow_e2e.mjs --observe-delta-streaming` samples DOM board count plus `/api/jobs/{job_id}/dashboard` and `/progress` while the workflow is still running.
  - `tests/test_frontend_browser_e2e.py::test_browser_openai_agent_delta_streaming_contract_observation_when_enabled` is opt-in via `SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1`.
  - Diagnostic mode records the current product gap; `SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1` flips it into the future hard assertion that baseline board must be visible before current snapshot serving.
  - Browser report path: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`.
- Added `SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP` for isolated scripted runtimes. This keeps browser/UI-started scripted provider waits bounded without changing live provider behavior; explicit request context still wins.
- Validation:
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_agent_delta_streaming_contract_observation'` -> `1 passed, 9 deselected`
  - Browser diagnosis: first visible board came only after job terminal, current snapshot `597/597`, `baselineFirstBoardObserved=false`, `finalCurrentSnapshotObserved=true`, `contractGapDetected=true`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py ... strict_tail_v7b ...` -> passed with `expectation_failures=[]`.
  - Strict v7b report: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v7b_report.json`; summary: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v7b_summary.json`.
  - v7b key metrics: `profile_url_total_count=597`, `fetched_profile_count=597`, `board_total_candidates=597`, `post_terminal_recovery.settled=true`, `round_count=4`, `preview_to_first_materialize_start_ms=33000`, `preview_to_finalization_completed_ms=58000`, `materialize_sync_duration_ms.max=21000`, event-level efficiency violation `false`.
- Current conclusion:
  - The test foundation now captures the real UX gap: the UI still waits for current snapshot/result-view materialization before showing the board. Delta asset board streaming should next split result-view lifecycle into baseline serving, delta applying/materializing, current serving, and post-result layering.

### OpenAI Agent scoped-delta strict smoke settle contract closed

- Tightened the long-tail OpenAI Agent scripted smoke so it can no longer pass only because the front workflow reaches `completed` while profile-tail/background reconcile work is still recoverable.
- `workflow_smoke._settle_post_terminal_worker_recovery(...)` now returns a first-class `post_terminal_recovery` state:
  - `settled`
  - `remaining_recoverable_worker_count`
  - `remaining_recoverable_worker_ids`
  - `round_count`
  - `max_rounds_exhausted`
  - `no_progress_rounds`
- Added `require_post_terminal_recovery_settled=true` to `configs/scripted/openai_agent_scoped_delta_smoke_matrix.json`, so the OpenAI Agent foundation must prove terminal-job recovery drained recoverable workers before strict mode passes.
- Reclassified `behavior_guardrails.prerequisite_gaps` as diagnostic rather than a hard behavior failure. Large Stage gaps remain visible in report/summary, but a long-tail fixture no longer produces the confusing state "strict expectations passed while `behavior_guardrails.violation_detected=true`" unless an actual hard guardrail fails.
- Re-ran the strict OpenAI Agent scoped-delta smoke:
  - report: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v4_report.json`
  - summary: `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v4_summary.json`
  - job: `ac72507be38e`
  - expectation failures: `[]`
  - provider invocations: `17` total (`harvest_profile_search=4`, `harvest_profile_scraper_batch=13`)
  - long-tail counts: `profile_url_total_count=299`, `fetched_profile_count=299`, `board_total_candidates=299`
  - post-terminal settle: `settled=true`, `round_count=3`, `remaining_recoverable_worker_count=0`
  - event-level efficiency: `harvest_completion_event_count=13`, `local_to_next_submit_start_ms.max=0`, `next_submit_attempt_elapsed_ms.max=575`, `repeated_materialize_signature_count=0`
  - behavior hard guardrails: `violation_detected=false`; diagnostic prerequisite gap remains visible (`stage_1_preview_to_stage_2_final_start=16000ms`)
  - service metrics caught the next UX bottleneck: max worker handoff gap `23000ms`, `bottlenecks.top_bottlenecks[0].kind=worker_handoff_gap`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'post_terminal_worker_recovery or expectations or prerequisite_gap or provider_case_report'` -> `13 passed, 10 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py`
  - JSON validation for `configs/scripted/openai_agent_scoped_delta_smoke_matrix.json` and `configs/scripted/openai_agent_scoped_delta_streaming.json`
  - strict hosted scripted smoke command above -> passed
- Next step:
  - Wire this foundation into isolated hosted/browser E2E for baseline-first board assertions, then start the Delta asset board streaming result-view lifecycle split.

### Scripted smoke can opt into live LLM planning

- Added a bounded `SOURCING_SCRIPTED_LIVE_MODEL_PLANNING=1` contract for scripted provider tests:
  - `SOURCING_EXTERNAL_PROVIDER_MODE=scripted` still keeps Harvest/DataForSEO/profile providers offline/scripted.
  - request normalization, plan review instruction normalization, refinement normalization, intent brief drafting, and search strategy planning may use the configured live model.
  - candidate-level outreach AI verification, public-web asset analysis, profile membership AI judging, and result summarization remain deterministic/offline to avoid model fan-out during large scripted fixtures.
- Added `ScriptedLivePlanningModelClient` as a planning-only wrapper around the configured live model; default scripted/simulate/replay behavior remains `OfflineModelClient`.
- Added `scripts/run_simulate_smoke_matrix.py --live-model-planning` for isolated hosted scripted runs. The flag requires `--runtime-dir` and `--provider-mode scripted`, so it cannot silently affect an already-running server or a live-provider smoke. Because isolated runtimes use their own secrets file, the CLI forwards only model-related env (`DASHSCOPE_*` / `MODEL_PROVIDER_*`) from the current settings into the isolated run.
- Updated `docs/TESTING_PLAYBOOK.md` and `docs/NEXT_TODO.md` with the live-planning scripted command for the OpenAI Agent scoped-delta foundation.
- Ran the OpenAI Agent scoped-delta scripted smoke with live planning enabled:
  - command wrote `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_live_planning_report.json` and `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_live_planning_summary.json`
  - live planning preserved `keywords=['Agent']`, current/former `search_seed_queries=['Agent']`, and `filter_hints.keywords=['Agent']`
  - dispatch stayed `delta_from_snapshot` / `baseline_reuse_with_delta`
  - guardrails showed no duplicate provider dispatch, no unexpected public-web stage, and no event-level efficiency violation
  - remaining foundation gap: hosted smoke still accepts `results_ready_nonterminal` too early for this scenario, so the run only observed `profile_url_total_count=4`, `fetched_profile_count=2`, and `board_total_candidates=4`; it did not yet exercise the intended hundreds-row provider search plus batched profile-scraper tail.
- Next step:
  - tighten the OpenAI Agent scripted smoke settle/expectation contract so this fixture must drive the long-tail profile-search/profile-scraper path before it can pass as the Delta asset board streaming foundation.

## 2026-04-28 (Asia/Shanghai)

### Service-grade scripted metrics foundation added

- Added `src/sourcing_agent/workflow_service_metrics.py` as the reusable smoke-report metrics layer for future Delta asset board streaming and full event-level workflow optimization.
- The report now captures:
  - per-worker start/end, duration, lane/status counts, global next-worker-start gap, same-lane next-worker-start gap, slow workers, and slow handoff gaps
  - user-visible timings: first progress observed, Stage 1 preview, final results, board ready/non-empty, dashboard/candidate-page fetch, loading-feedback violations, board-readiness violations, and long post-preview finalization
  - bottleneck recommendations for slow workers, worker handoff gaps, board readiness lag, and post-preview finalization lag
- `workflow_smoke.run_hosted_smoke_case` now fetches `/api/jobs/{job_id}/trace` and passes `agent_trace_spans` into `provider_case_report.service_metrics`, so hosted/scripted reports can correlate worker rows with trace span timing.
- `summarize_smoke_timings(...)` now rolls service metrics across cases:
  - worker count / trace span count / worker duration
  - global handoff gaps
  - slow worker/gap counts
  - board-ready/non-empty and post-preview finalization UX metrics
  - bottleneck kind/severity counts
- Docs updated:
  - `docs/TESTING_PLAYBOOK.md` now defines service-grade scripted metrics as the default AI-in-loop optimization surface before touching workflow code.
  - `docs/NEXT_TODO.md` records this as the next foundation layer for the larger OpenAI Agent scoped-delta fixture and Delta asset board streaming.
  - `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` adds the service metrics regression target.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q` -> `16 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k 'openai_agent or scripted_provider_scenario or provider_behavior_matrix or scripted or profile_search or profile_batch or zero_result'` -> `19 passed, 69 deselected`
- Next step:
  - Wire `configs/scripted/openai_agent_scoped_delta_streaming.json` into isolated hosted/browser E2E and use `provider_case_report.service_metrics` to find the first blocking point before implementing result-view lifecycle changes.

### OpenAI Agent scoped-delta scripted test foundation started

- Corrected the Delta asset board streaming test strategy:
  - Do not use the lightweight OpenAI/Pulse two-profile delta as the main driver; it is too small to expose realistic provider scheduling, dedupe, materialization, and board-readiness risks.
  - The canonical scripted driver is now an older OpenAI baseline plus a larger `帮我找OpenAI做Agent方向的人` scoped-search delta.
- Added `configs/scripted/openai_agent_scoped_delta_streaming.json`:
  - models current/former Harvest profile-search probe + scaled calls
  - returns hundreds of synthetic provider rows with duplicate LinkedIn URLs
  - models profile-scraper pending rounds, retryable delay, and actor-slot pressure
  - explicitly records that default Stage 1 DataForSEO/public-web fallback is not allowed
- Extended scripted Harvest fixtures with `generated_body` support so large provider payloads can be modeled without hand-writing hundreds of JSON rows:
  - profile-search rows honor `maxItems`, `takePages`, `startPage`, pagination totals, current/former company filters, and duplicate-url stride
  - profile-scraper rows are generated from requested URLs and include timeline fields used by downstream materialization
  - company-employees synthetic generation is available for later full-roster/employee API scenarios
- Added connector-level regressions:
  - OpenAI Agent current/former profile-search does two calls per lane (`probe` then `scale`) and never submits a live Harvest run in scripted mode
  - OpenAI Agent profile-scraper batches resume through pending/retryable rounds and generate usable profile timeline payloads
  - scenario validation now treats generated bodies as first-class fixture output
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/harvest_connectors.py src/sourcing_agent/scripted_provider_scenario.py`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py src/sourcing_agent/scripted_provider_scenario.py tests/test_harvest_connectors.py tests/test_scripted_provider_scenario.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k 'openai_agent or scripted_provider_scenario or provider_behavior_matrix'` -> `6 passed, 82 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_harvest_connectors.py -q -k 'scripted or profile_search or profile_batch or zero_result'` -> `15 passed, 69 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py tests/test_scripted_provider_scenario.py -q` -> `5 passed`
- Next step:
  - Wire this fixture into an isolated hosted/browser E2E that seeds the older OpenAI baseline, runs the Agent scoped query, records provider invocation/timing reports, and asserts baseline-first board visibility while current snapshot materialization is still running.

### Infra thematic keyword no longer becomes `infra_systems` role bucket

- Product correction:
  - `Infra` is an AI research/theme shard like `Multimodal` / `Reasoning`, not a primary role bucket.
  - LinkedIn acquisition should use provider keyword `Infra` and broad technical function ids `8/24`; it should not show or require `Infra Systems` as a search keyword.
- Fixes:
  - Removed bare `infra` / `infrastructure` from role-bucket aliasing. Explicit role phrases such as `infrastructure engineer`, `infra engineer`, `platform engineer`, and `systems engineer` still map to `infra_systems`.
  - Added `Infra` to the research-direction default thematic labels, so directional Infra requests still normalize to the default technical population (`researcher` + `engineer`) and function ids `24/8`.
  - Added request-intent sanitization so model-assisted payloads that still emit `must_have_primary_role_buckets=["infra_systems"]` for an Infra directional query are downgraded back to thematic `keywords=["Infra"]`, unless the raw text explicitly contains an infrastructure role phrase.
  - Frontend plan keyword display now filters role-bucket tokens (`infra_systems`, `product_management`, `engineering`, etc.) instead of falling back to showing them as "检索关键词". The field should reflect actual provider/search keywords such as `Infra`, not internal role labels.
- Local reproduction:
  - `帮我找PostHog做infra方向的人` now supplements to `keywords=['Infra']`, `must_have_primary_role_buckets=[]`.
  - Strategy filter hints remain `function_ids=['24','8']`, `keywords=['Infra']`; request preview no longer exposes `infra_systems`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/query_signal_knowledge.py src/sourcing_agent/domain.py src/sourcing_agent/request_normalization.py tests/test_semantic_intent.py tests/test_request_matching.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_semantic_intent.py tests/test_request_matching.py` -> `13 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_planning_modules.py -k 'infra or keyword_priority or scoped_search_provider_keywords_exclude_role_facet_terms'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_semantic_retrieval.py tests/test_post_acquisition_refinement.py -k 'infra or primary_role_bucket'` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'plan_workflow_scoped_search_override_updates_effective_execution_semantics or plan_workflow_infers_openai_scope_from_chatgpt_product_manager_query or facet or infra_systems or current_job_snapshot_over_stale_result_view'` -> `4 passed`
  - `npm --prefix frontend-demo run build` -> passed

### PostHog/Coding scoped Harvest query pollution fixed

- 真实 query/job `4850149b9183`（PostHog, `20260428T134840`）排查结论：
  - 本地 request manifest 确认不是 webhook 回传错误；5 次 Harvest actor run 都来自本地 `search_seed_discovery`
  - 计划内 payload 是 2 个：`searchQuery=Coding` + current company、`searchQuery=Coding` + former company
  - 额外 payload 是本地 query resolution 生成的：`searchQuery=Coding research` + current/former，其中 current 第一次返回 0 后触发 zero-result retry，所以 API 后台看到 5 次 run
  - 错误 summary 行为 `query=PostHog`、`effective_query_text=Coding research`：`scope_keywords=PostHog` 被当作额外 paid query，随后 `_normalize_harvest_query_text` 把公司名剥空并从 `filter_hints.keywords=['Coding','research']` 回填，导致 provider-facing query 被污染
- 上游修复：
  - `acquisition_strategy` 将 `research/researcher/employee/engineering/engineer` 定义为 generic role query keys；这些词仍可驱动 functionIds / role/facet / scoring，但不再进入 provider acquisition keyword hints
  - PostHog/Coding scoped plan 现在输出 `search_seed_queries=['Coding']`、`filter_hints.keywords=['Coding']`，不再把 `research` 拼到 Harvest search text
  - `plan_review` 过滤 `confirmed_company_scope` 中等于 target company 或其 LinkedIn company URL 的值；只确认目标公司本身时会清掉旧 `scope_keywords`，避免 `scope_keywords=PostHog` 再变成额外 paid query
- Provider 边界防御保留：
  - `_resolve_provider_people_search_queries(...)` 忽略等于 target company identity 的 `scope_keywords`
  - `_normalize_harvest_query_text(...)` 的 keyword fallback 会丢弃 generic role terms，避免旧 metadata 或手工 payload 再打出 `Coding research`
- 验证：
  - 直接复现：相同 PostHog/Coding scoped request 现在得到 `scoped_search_roster`、`search_seed_queries=['Coding']`、`filter_hints={'current_companies':['PostHog'],'function_ids':['24','8'],'keywords':['Coding']}`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/acquisition_strategy.py src/sourcing_agent/plan_review.py src/sourcing_agent/seed_discovery.py tests/test_planning_modules.py tests/test_seed_discovery.py tests/test_search_planning.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_planning_modules.py -k 'scoped_search_provider_keywords_exclude_role_facet_terms or confirmed_scope_does_not_write_target_company or keyword_priority or publication_coverage_includes_engineering_and_blog or intent_view_scope_and_keywords or organization_execution_profile_prefers_scoped_search'` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_seed_discovery.py -k 'target_company_scope_keyword or keyword_fallback_drops_generic_role_terms or resolve_provider_people_search_queries_dedupes or normalize_harvest_query_text_is_alias_canonical or provider_people_search_dedupes_after_effective_harvest_query_normalization'` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_search_planning.py tests/test_pipeline.py -k 'plan_workflow_scoped_search_override_updates_effective_execution_semantics or plan_review_persists_execution_preferences_into_review_request_and_queued_job or build_sourcing_plan_omits_public_web_stage_by_default or public_interview_bundle_is_compiled_for_interview_queries or relationship_bundle_dedupes_company_scope_terms or scoped_search_plan_keeps_paid_people_search_as_fallback or targeted_people_search_bundle_uses_natural_keyword_queries'` -> `7 passed`
- 手测注意：
  - 需要重启 backend/daemon 才会吃到新代码
  - 下次 PostHog/Coding scoped query 预期最多只有 `Coding` current + `Coding` former 两个不同 Harvest payload；若 provider 真 0，可看到同 payload 的 zero-result retry，但不应再出现 `Coding research`

### Runtime backlog cleanup before manual recovery testing

- 手动测试前做了只读 runtime backlog audit：
  - 清理前 `active_job_count=4`，全部是 `webhook_roundtrip_20260426*` 旧 smoke residue；没有 active provider slot / remote-wait worker
  - `recoverable_worker_count=0`
  - completed-job backlog 有 4 条本地收口任务：OpenAI `185af9c5b847`、OpenAI `7967d481646c`、Safe SuperIntelligence `5d57f71a890e`、Reflection AI `02f139ea791b`
- OpenAI `185af9c5b847` 结论：
  - 用户可见 workflow 已完成，`candidate_source` / `job_result_view` 均指向 job snapshot `20260428T094936`
  - 未完成的是后台本地阶段：`background_snapshot_materialization.status=scheduled` 与 `outreach_layering.status=scheduled`
  - 尝试用完整 completed-workflow reconcile 收口时，snapshot artifact 已写出，但后续 retrieval/outreach 路径仍进入重 CPU 文件读取；为避免手测时抢 CPU，停止长跑并按已落盘 artifact 做显式归档
- 已做 cleanup：
  - `185af9c5b847`: `background_snapshot_materialization` 标为 `completed`，记录 `manifest_path` / `artifact_summary_path` / `candidate_count=887` / `page_count=18`；`outreach_layering` 标为 `skipped`；写入 `search_seed` 与 `harvest_prefetch` background reconcile cursor
  - `7967d481646c`: `outreach_layering` 标为 `skipped`；写入 `harvest_prefetch` cursor。该 job 没有 `job_result_view`，不会改 OpenAI canonical serving pointer
  - `5d57f71a890e`: `outreach_layering` 标为 `skipped`；写入 `company_roster` cursor
  - `02f139ea791b`: 写入 `search_seed` cursor
  - 4 条 `webhook_roundtrip_20260426*` running residue 已 supersede，reason=`manual_cleanup_before_recovery_test`
- 清理后验证：
  - `show-system-progress --force-refresh`: runtime `status=ok`, `active_job_count=0`, `recoverable_worker_count=0`, background reconcile counts all `0`
  - `show-recoverable-workers --stale-after-seconds 1`: `0`
  - `_discover_completed_workflow_jobs_pending_background_reconcile(limit=200)`: `[]`
  - OpenAI `185af9c5b847` 保持 `candidate_source_snapshot=20260428T094936` / `result_view_snapshot=20260428T094936`；未污染公司 canonical pointer

### Plan review scoped-search override execution semantics

- 修复 PostHog plan review 展示/语义漂移：
  - 用户在 Review 的“继续补充或修改要求”里写 `做scoped search` 后，后端 plan 已正确变成 `acquisition_strategy.strategy_type=scoped_search_roster`
  - 但 `effective_execution_semantics` 仍只读取 organization execution profile 的 fallback default (`default_acquisition_mode=full_company_roster`)，导致前端优先显示旧的 `execution_strategy_label=全量 live roster`
- 修复方式：
  - `compile_execution_semantics(...)` 现在读取 `request.execution_preferences.acquisition_strategy_override`
  - 显式 override（如 `scoped_search_roster`）会覆盖 profile default，生成 `effective_acquisition_mode=scoped_live_search` / `execution_strategy_label=定向搜索 roster`
  - payload 同时保留 `profile_default_acquisition_mode` 与 `request_acquisition_strategy_override`，方便审计“组织默认策略”和“本次 review/request 覆盖策略”的差异
- 真实 PostHog 复现：
  - request `帮我找PostHog做Coding方向的人 检索策略使用scoped search`
  - 修复后 `/api/plan` 返回 `strategy_type=scoped_search_roster`、`profile_default_acquisition_mode=full_company_roster`、`request_acquisition_strategy_override=scoped_search_roster`、`effective_acquisition_mode=scoped_live_search`、`execution_strategy_label=定向搜索 roster`
- 验证：
  - 新增 `tests/test_pipeline.py::PipelineTest::test_plan_workflow_scoped_search_override_updates_effective_execution_semantics`，覆盖 `/api/plan` 同层 plan envelope：profile default 为 `full_company_roster` 时，本次 request override 仍必须输出 `定向搜索 roster`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/execution_semantics.py tests/test_execution_semantics.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_execution_semantics.py` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'plan_workflow_scoped_search_override_updates_effective_execution_semantics or compile_plan_review_instruction or build_sourcing_plan_omits_public_web_stage_by_default or current_job_snapshot_over_stale_result_view'` -> `5 passed`
  - `npm --prefix frontend-demo run build` -> passed

### LinkedIn Stage 1 no-DataForSEO seed fallback by default

- Product boundary clarified from live OpenAI Infra job `185af9c5b847`: `LinkedIn Stage 1` must only use LinkedIn-related providers by default (`company-employees`, `linkedin-profile-search`, `linkedin-profile-scraper`). DataForSEO/public-web seed discovery is not a fallback inside Stage 1; it is Stage 2 or explicit opt-in only.
- Fixes:
  - `compile_search_strategy(...)` no longer emits `relationship_web`, `publication_surface`, or `public_interviews` Stage-1 seed bundles unless `execution_preferences.allow_stage1_web_seed_fallback=true` (alias `allow_public_web_seed_fallback`).
  - model-assisted search plans are filtered through the same policy, so an LLM cannot silently reintroduce public-web seed bundles into default Stage 1.
  - `intent_axes.execution_preferences` now supports the same opt-in key, so model-normalized requests do not silently drop an explicit Stage-1 web seed override.
  - `SearchSeedAcquirer.discover(...)` suppresses non-paid web search specs unless `cost_policy/intent_view` explicitly allows Stage-1 web seed fallback. Old `search_seed_queries` still feed Harvest profile-search fallback/primary logic; they no longer imply DataForSEO.
  - `execution_preferences` now recognizes `allow_stage1_web_seed_fallback`; default cost policy records `prefer_low_cost_web_search=false` and `allow_stage1_web_seed_fallback=false`.
  - Historical zero-delta `search_seed_discovery` workers (e.g. DataForSEO returned pages but no LinkedIn seed entries) are no-op reconciled: write final marker with `sync_reason=search_seed_no_candidate_delta`, skip profile prefetch, skip full materialize.
  - Candidate serving API/frontend contract now exposes `source_matches`/`sourceMatches` from materialized artifacts, so one candidate can be consumed as belonging to multiple scoped query/source shards instead of only flattening to `matched_keywords`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/search_planning.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition_strategy.py src/sourcing_agent/execution_preferences.py tests/test_seed_discovery.py tests/test_pipeline.py tests/test_planning_modules.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_seed_discovery.py tests/test_planning_modules.py` -> `104 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'search_seed_no_candidate_delta or build_sourcing_plan_omits_public_web_stage_by_default or current_job_snapshot_over_stale_result_view or release_stale_workflow_job_lease or scoped_search or former_search_seed'` -> `21 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py` -> `327 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py tests/test_remote_provider_events.py tests/test_runtime_lease_utils.py tests/test_workflow_event_response.py` -> `25 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_cli.py -k 'audit_company_serving_view or repoint_job_result_view'` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'asset_population_seed_query_keyword or legacy_sqlite_candidate_source or result_view_without_summary_candidate_source or dashboard_is_summary_only or auto_materialize_snapshot_candidate_documents'` -> `4 passed`
  - `npm --prefix frontend-demo run build` -> passed
  - Runtime smoke: `POST /api/workflows/explain` for OpenAI Infra only emits `targeted_people_search/linkedin_people_search/paid_fallback`; `GET /api/jobs/185af9c5b847/candidates` returns `source_matches` for OpenAI serving candidates.

### Event-level workflow review pass 5 — H4 query dedupe + H5 source_matches recall + provider-slot idle metric

- All three follow-ups Codex queued after pass 4 closed in this entry. No live Harvest/Apify calls.
- **H4 — Harvest query dedupe normalization** (`src/sourcing_agent/seed_discovery.py:_normalize_harvest_query_text`).
  - Symptom: hyphen / underscore / case variants of the same scope keyword (`Reasoning-Model`, `Reasoning_Model`, `reasoning model`) deduped at the *signature* level (`_search_query_signature` already collapses `[\s\-_]+` and lowercases) but the *provider-facing* query string was the raw variant. Result: even though only one provider call ran, summaries and per-variant cache keys diverged, and any second variant that happened to slip through dedupe (e.g. via different upstream filtering) would have hit Harvest with a needlessly different query.
  - Fix: extracted `_canonicalize_provider_query_alias` (mirrors the alias resolution `_clean_provider_query_text` already applies on the keyword path: `_PROVIDER_QUERY_CANONICAL_ALIASES` → `thematic_signal_search_query_aliases` → `canonicalize_thematic_signal_label` → `canonicalize_scope_signal_label`). `_normalize_harvest_query_text` now routes its three return paths through it. Hyphen/underscore variants collapse to the space-separated form before alias lookup so `Reasoning-Model` and `Reasoning_Model` both map to canonical `Reasoning`.
  - Tests (`tests/test_seed_discovery.py`):
    - `test_provider_query_signature_collapses_hyphen_underscore_case_variants` — the dedupe-key invariant.
    - `test_provider_query_family_key_aliases_canonicalize_synonyms` — alias-mappable variants share family keys.
    - `test_resolve_provider_people_search_queries_dedupes_alias_and_case_variants` — end-to-end: alias-equivalent inputs produce one provider query.
    - `test_resolve_provider_people_search_queries_dedupes_hyphen_underscore_variants` — non-aliased hyphen/underscore variants still collapse.
    - `test_normalize_harvest_query_text_is_alias_canonical` — provider-facing text uses the canonical form.
- **H5 — recall consumes `source_matches` / `matched_keywords`** (`src/sourcing_agent/scoring.py`).
  - Symptom: scoped sharding records "this candidate came from the `Reasoning` profile-search shard" in `metadata.matched_keywords` and `metadata.source_matches`. But `_candidate_blob` (used by `candidate_matches_structured_filters` and `_score_keyword_pool`) only included `candidate_searchable_text` + facets + role bucket. A candidate whose raw text fields didn't mention `Reasoning` was silently hidden by the recall filter even though the workflow had already paid Harvest to fetch them for that exact keyword.
  - Fix: added `_candidate_source_match_keyword_text(candidate)` extracting the dedupe'd text from `metadata.matched_keywords` + `metadata.source_matches[*].matched_on`. `_candidate_blob` now appends this signal so `must_have_keywords` filtering sees it. Added a new `source_match_keywords` field weight (3) to `SEARCH_FIELDS` so scoring also matches on the provenance keywords; `_candidate_field_value` routes it through the new helper.
  - Tests (`tests/test_scoring.py::ScoringSourceMatchProvenanceTests`):
    - `test_candidate_matches_structured_filters_honors_matched_keywords_metadata` — must_have gate accepts a candidate matched only via source_matches.
    - `test_score_candidates_returns_candidate_when_only_source_matches_carry_keyword` — recall returns the candidate; matched_fields explanation surfaces the provenance keyword.
    - `test_score_candidates_excludes_candidate_when_neither_text_nor_source_matches_match` — negative control to keep H5 from over-recalling.
- **Provider-slot idle metric** (`src/sourcing_agent/enrichment.py`, `src/sourcing_agent/workflow_efficiency.py`).
  - Symptom: a worker stuck in `waiting_remote_harvest` for 10+ minutes with no progress was the documented "actor slot idle" failure mode but only spottable via the Apify console.
  - Fix: enrichment stamps `remote_wait_started_at` on the worker checkpoint when transitioning into `waiting_remote_harvest`. `workflow_efficiency._extract_worker_efficiency_metrics` accepts an optional `now` and computes `remote_wait_age_ms` (per-batch p50/max/min/count) for workers currently in remote-wait, plus `provider_lease_age_ms` from `provider_limiter_lease.created_at` for cross-checking. Both metrics flow through `extract_event_level_efficiency_metrics`, `aggregate_event_level_efficiency_metrics`, and the runtime subset. Diagnostic only — never gates submit / materialize.
  - Tests (`tests/test_workflow_efficiency.py`):
    - `test_event_level_efficiency_surfaces_provider_slot_idle_window` — worker stuck 12 min vs 30 s; aggregated min/max distinguish them.
    - `test_aggregate_event_level_efficiency_metrics_includes_provider_slot_idle` — multi-report aggregation preserves min/max boundaries.
- Validation:
  - `pytest tests/test_pipeline.py tests/test_workflow_efficiency.py tests/test_seed_discovery.py tests/test_scoring.py tests/test_remote_provider_events.py tests/test_runtime_lease_utils.py tests/test_workflow_event_response.py -q` → **405 passed**.
  - `pytest tests/test_enrichment.py -q` → 44 passed (sanity for the `remote_wait_started_at` addition).
- With pass 1–5 and the two pass-4 fixes, every documented backlog item from Codex's reviews is closed in code or carried forward as a post-manual-test follow-up. Pipeline is fully green; ready to start a local backend/frontend manual test pass when desired.

### Event-level workflow review pass 4 — closed two pre-existing pipeline failures

- Pass 3 had left two pipeline tests failing as "pre-existing working-tree state, out of scope". Codex pushed back: those are not noise, they're real semantic regressions that should clear before any live/manual test. Both fixed in this entry; full pipeline now 324/324 with no deselects.
- 1 (exploration vs outreach reconcile priority) — `_reconcile_completed_workflow_if_needed` was hitting `_outreach_layering_requires_background_reconcile(..., allow_missing=True)` *before* checking for pending exploration workers, so a `full_company_asset` job with completed exploration output to apply silently fell through to the outreach-layering branch and returned `reconciled_outreach_layering`. Fix: compute `pending_exploration_workers` first; only call the outreach-layering gate when there are no pending exploration workers. This restores the original "exploration before outreach" priority while keeping the `allow_missing=True` semantics for jobs without exploration output. Test `test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` now passes (`status="reconciled"`).
- 2 (large-org full-company baseline reuse) — `_large_scoped_profile_queries_require_explicit_coverage` was firing for full-company queries on large orgs, flipping `requires_delta_acquisition=True` because the planner's embedded profile queries weren't explicitly covered by the baseline. But a full-company query (e.g. "给我Anthropic的全部成员") explicitly wants the *entire* roster — its embedded profile queries are scoped within the complete population, not a narrower scoped search, so reusing a complete authoritative baseline is the whole point. Fix: short-circuit the rule when `current_strategy_type == "full_company_roster"`. Scoped queries (`scoped_search_roster`, `former_employee_search`) and large-org default `scoped_search_roster` mode still require explicit coverage as before. Test `test_large_org_full_company_query_reuses_complete_baseline_without_former_delta` now passes (`requires_delta_acquisition=False`, `planner_mode="reuse_snapshot_only"`).
- Validation:
  - `pytest tests/test_pipeline.py -q` (no deselects) → **324 passed**.
  - `pytest tests/test_workflow_efficiency.py tests/test_remote_provider_events.py tests/test_runtime_lease_utils.py tests/test_workflow_event_response.py -q` → 23 passed.
  - `pytest tests/test_pipeline.py -q -k "asset_reuse or large_org or full_company or scoped_search or baseline or delta or former"` → 57 passed (sanity for the asset-reuse rule change).
- Pipeline is fully green; H4 query dedupe, H5 source_matches recall regression, and provider-slot idle metrics are the next items in the queue. Live/manual test still gated on those reaching a known-good baseline.

### Event-level workflow review pass 3 — true marker recoverability + Phase B-failure short-circuit

- Codex review of pass 2 surfaced three semantic gaps in the marker contract. All three closed; the pass-2 entry below is left in place for history but its "Phase C 完成后才写 ingest" claim was over-stated for the completed-job paths and the running-job sync-failure tail.
- 1 (completed-job paths still wrote `inline_incremental_ingest` before sync): the pre-sync `_record_inline_incremental_ingest_on_workers_if_missing(..., sync_result={"status":"materializing"|"deferred", ...})` calls in `_reconcile_completed_workflow_after_company_roster`, `_reconcile_completed_workflow_after_search_seed`, and `_reconcile_completed_workflow_after_harvest_prefetch` were removed. The post-sync `overwrite_existing=True` write at the end of each function is now the only place a final ingest marker is written, and it only runs after sync returns `completed`/`skipped`/`deferred`.
- 2 (running-job prefetch failure still wrote final marker): added `_phase_b_prefetch_indicates_retryable_failure(profile_prefetch)` helper. When true, `_process_inline_incremental_worker_batch`, `_reconcile_completed_workflow_after_company_roster`, `_reconcile_completed_workflow_after_search_seed`, and `_reconcile_completed_workflow_after_harvest_prefetch` all short-circuit before Phase C: skip sync, skip the gating ingest marker, return `status="prefetch_failed_retryable"`. The worker keeps only its `inline_incremental_apply` marker, so the next recovery tick re-picks it. Also gated the running-job final per-worker `_record_inline_incremental_ingest_on_worker` loop on `sync_result.status in {completed, skipped, deferred}`; on sync failure (or any non-terminal-good status) the gating marker is not written and an explanatory event is appended.
- 3 (search_seed `apply_already_consumed` lost SearchSeedSnapshot): the synthesized apply_result in both the running-job branch (orchestrator.py:~19458) and the completed-job branch (orchestrator.py:~20705) now restores `search_seed_snapshot` via `_restore_search_seed_snapshot_from_snapshot_dir`, so retry prefetch in Phase B actually runs instead of silently skipping with `search_seed_snapshot_missing`.
- New / updated tests:
  - `test_company_roster_running_job_prefetch_failure_leaves_worker_repickable` rewritten — no manual marker clear. First tick: prefetch raises, sync NOT called, no ingest marker written. Second tick: collector re-picks, Phase A short-circuits on apply marker (apply not called twice), Phase B succeeds, sync runs, ingest marker finally written.
  - `test_completed_company_roster_reconcile_prefetch_failure_leaves_worker_repickable` (new): completed-job company_roster Phase B failure path — sync not called, no ingest marker, apply marker persists.
  - `test_completed_search_seed_reconcile_prefetch_failure_leaves_worker_repickable` (new): completed-job search_seed Phase B failure path — same invariants.
  - `test_search_seed_apply_already_consumed_restores_search_seed_snapshot` (new): exercises the apply-already-consumed branch and asserts the restored `SearchSeedSnapshot` is forwarded to `_queue_background_profile_prefetch_from_search_seed_snapshot`.
  - `test_completed_workflow_harvest_reconcile_marks_consumed_before_materialize_failure` rewritten to match the new contract — sync failure must NOT write `inline_incremental_ingest`; apply marker persists.
- Validation (no live providers):
  - `pytest tests/test_pipeline.py -q -k "writer_lock or completion_event_queues_next_batch or harvest_profile_completion or harvest_prefetch or company_roster or search_seed or completed_workflow or background_reconcile or background_exploration or out_of_order or scoped_search or inline_incremental"` (minus 2 pre-existing failures) → 77 passed.
  - Full pipeline minus the 2 pre-existing working-tree failures → 322 passed.
- H4 / H5 / provider-slot idle metrics and the two pre-existing pipeline failures remain explicitly out of scope until pass 3 is signed off.

### Event-level workflow review findings — C1+M2+H1 fixed, H3/M3/M5 deferred

- Claude Code event-level review identified two contract violations and one observability gap; option (b) implemented per user direction.
- C1 (critical): `_process_inline_incremental_worker_batch` for `company_roster` / `search_seed` ran the snapshot-delta apply, profile prefetch (next-submit opportunity), and downstream sync all inside one per-job writer lock (`_inline_incremental_writer_lock_for_job`). A peer remote completion hitting the same job had to wait for the full materialize/sync. The `harvest_prefetch` path already had a pre-writer prefetch hop and was unaffected.
- M2 (medium): `_queue_background_profile_prefetch_from_search_seed_snapshot` and the `company_roster` inline reconcile callsite both inherited the default `load_cached_profile_payloads=True`, so next-submit gating for those worker kinds parsed raw Harvest profile JSON instead of using registry-only markers.
- H1 (high): no metric exposed how long an inline reconcile waited on the per-job writer lock, so writer-lock contention (a documented top failure mode) was invisible to live smoke / runtime dashboards.
- Implementation:
  - new `_inline_incremental_writer_lock_scope(job_id, scope=...)` context manager records `lock_requested_at` / `lock_acquired_at` / `lock_released_at` plus `writer_lock_wait_ms` / `writer_lock_held_ms`. Diagnostic only — never gates submit/materialize.
  - `_process_inline_incremental_worker_batch` split into three phases:
    - Phase A under per-job writer lock: collect completed workers, completed-job branch, snapshot-delta `_apply_background_*_workers_to_snapshot`, provisional `inline_incremental_ingest` marker so a peer reconcile tick will skip these workers via `_worker_has_inline_incremental_ingest_output`.
    - Phase B outside the lock: profile prefetch (next-submit opportunity). For `company_roster` / `search_seed` callers pass `load_cached_profile_payloads=False`. Failures here only leave the provisional marker; subsequent recovery resubmits prefetch because the workers have an applied delta but no queued/dispatched marker yet.
    - Phase C outside the lock: `_inline_incremental_sync_for_running_job` (full materialize), per-worker final marker, persisted reconcile state, and per-kind structured events. Concurrent same-snapshot syncs continue to serialize through the global `materialization_writer` writer-budget slot.
  - `_queue_background_profile_prefetch_from_search_seed_snapshot` now forwards `load_cached_profile_payloads=False` to the underlying baselines helper.
  - `writer_lock` block embedded in `company_roster` / `search_seed` inline reconcile event payloads + the harvest_prefetch reconcile event; legacy harvest_prefetch pre-writer prefetch hop preserved.
  - `workflow_efficiency.py` adds `_extract_writer_lock_wait_metrics` (per-event aggregation with by-scope breakdown), surfaces it in `extract_event_level_efficiency_metrics`, `aggregate_event_level_efficiency_metrics`, and `event_level_efficiency_runtime_subset`.
- Tests added (`tests/test_pipeline.py`, `tests/test_workflow_efficiency.py`):
  - `test_company_roster_inline_reconcile_runs_prefetch_outside_writer_lock` — apply→prefetch→sync ordering and `load_cached_profile_payloads=False`.
  - `test_company_roster_inline_reconcile_prefetch_does_not_block_peer_remote_completion` — peer can grab the per-job writer lock during the prefetch fire (non-blocking acquire).
  - `test_search_seed_inline_reconcile_runs_prefetch_outside_writer_lock` — same invariants for the search_seed path; explicitly exercises the wrapper helper to assert `load_cached_profile_payloads=False` propagates to the underlying baselines call.
  - `test_inline_writer_lock_emits_writer_lock_wait_metric_event` — holds the lock externally for ~50ms, asserts `writer_lock_wait_ms ≥ 40` in the inline reconcile event payload and that `extract_event_level_efficiency_metrics` rolls it up.
  - `test_event_level_efficiency_aggregates_writer_lock_wait_metric` and `test_aggregate_event_level_efficiency_metrics_includes_writer_lock` — verify per-event extraction, by-scope breakdown, and aggregation across reports.
  - existing `test_harvest_profile_completion_event_queues_next_batch_before_apply_and_defers_materialization` and `test_harvest_profile_completion_prefetch_runs_before_writer_lock` continue to pass; the harvest_prefetch pre-writer prefetch hop is intentionally left intact.
- H3 re-verified, no code change: in `enrichment.py:_execute_harvest_profile_batch_worker`, `acquire_runtime_provider_limiter_slot` is called at line 2139 *before* `begin_worker` at line 2178; on limiter `RuntimeError` the function returns `worker_status="backpressure"` without creating a worker; on `begin_worker` failure the lease is released. The post-`begin_worker` checkpoint immediately moves the worker into `submitting_remote_harvest` with the lease attached. No phantom-without-lease window.
- M3 (foreground_fast stat) and M5 (corrupt-manifest fail-loud) already mitigated for the active code paths; left in NEXT_TODO for future audit / regression coverage.
- H5 (recall/filter prefers `source_matches`/`matched_keywords`) deferred — needs a recall provenance regression first before changing API/frontend behavior.
- Targeted validation (read-only / no provider calls):
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q` → 5 passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "inline_incremental or scoped_search or out_of_order or harvest_profile_completion or harvest_prefetch or completed_workflow_harvest or company_roster_inline or search_seed_inline or writer_lock"` → 23 passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q --deselect ...test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` → 316 passed, 1 pre-existing failure deselected
  - Full pipeline run had 2 pre-existing failures unrelated to this change set:
    - `test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` — caused by the working tree's `_outreach_layering_requires_background_reconcile(..., allow_missing=True)` change at orchestrator.py:18055; reverting that single keyword-arg makes the test pass without my edits.
    - `test_large_org_full_company_query_reuses_complete_baseline_without_former_delta` — the working tree's 349-line diff in `asset_reuse_planning.py` flips `requires_delta_acquisition` to True; not touched by this change set.
  - Both pre-existing failures noted in NEXT_TODO so they are not lost.

### Event-level workflow review pass 2 — closed completed-job lock + marker recoverability

- Codex review of pass 1 surfaced five must-fix items before claiming completion. All addressed in this entry; running-job + completed-job paths now share the same Phase A/B/C contract.
- 1 (completed-job lock contention): `_process_inline_incremental_worker_batch`'s completed-job branch was still dispatching `_reconcile_completed_workflow_after_company_roster` / `_reconcile_completed_workflow_after_search_seed` / `_reconcile_completed_workflow_after_harvest_prefetch` *inside* the per-job writer lock, and each of those functions did apply + prefetch + materialize + retrieval + outreach_layering as one block. Refactor: dispatch happens outside the outer lock; each reconcile sub-function takes its own short `_inline_incremental_writer_lock_scope` around just the apply call.
- 2 (load_cached_profile_payloads): completed company_roster prefetch (`_reconcile_completed_workflow_after_company_roster`) now passes `load_cached_profile_payloads=False` so it stays on the registry-only marker path.
- 3 (provisional marker recoverability): the running-job and completed-job paths previously wrote `inline_incremental_ingest` provisionally under the lock, which `_collect_inline_incremental_worker_batch` treats as terminal — so a Phase B prefetch failure permanently skipped the worker. Split into two markers:
  - `inline_incremental_apply` (new): written under Phase A lock immediately after `_apply_background_*_workers_to_snapshot` returns `applied`. Idempotency guard for Phase A re-runs (filter `pending_workers` to those without an apply marker for this snapshot before calling apply). Does NOT block collection.
  - `inline_incremental_ingest` (existing): written only at Phase C completion. Continues to be the gating signal for `_collect_inline_incremental_worker_batch`.
  - Net effect: Phase B/C failures leave the apply marker behind; the worker is re-pickable; the next recovery tick skips Phase A apply and retries Phase B prefetch + Phase C materialize.
- 4 (`workflow_efficiency.report_available`): now includes `writer_lock_event_count`, so a job whose only telemetry is writer-lock metrics is no longer dropped by `aggregate_event_level_efficiency_metrics`.
- 5 (docs): orphan bullets at PROGRESS.md:44 restored under their heading; NEXT_TODO downgraded the C1/M2 over-claimed status from "fully done" to "running-job done in pass 1, completed-job closed in pass 2".
- New tests:
  - `test_completed_company_roster_reconcile_runs_prefetch_outside_writer_lock` — completed-job path: apply→prefetch→sync ordering, peer can grab the per-job lock during prefetch and during sync, prefetch is registry-only.
  - `test_completed_search_seed_reconcile_runs_prefetch_outside_writer_lock` — same invariants for the search_seed completed-job path.
  - `test_company_roster_running_job_prefetch_failure_leaves_worker_repickable` — Phase B prefetch raises; on next recovery tick the apply marker short-circuits Phase A and apply is not called again.
  - `test_event_level_efficiency_aggregates_writer_lock_wait_metric` and `test_aggregate_event_level_efficiency_metrics_includes_writer_lock` already verified per-event extraction and aggregation in pass 1; no change needed for the `report_available` fix to be exercised, but the broader suite now also runs writer-lock-only payloads through `report_available`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q` → 5 passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "writer_lock or completion_event_queues_next_batch or harvest_profile_completion_prefetch or company_roster_inline_reconcile or search_seed_inline_reconcile or completed_workflow_harvest_reconcile or completed_company_roster_reconcile or completed_search_seed_reconcile or company_roster_running_job_prefetch_failure"` → 17 passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "inline_incremental or scoped_search or out_of_order or harvest_profile_completion or harvest_prefetch or completed_workflow or company_roster or search_seed or writer_lock or background_reconcile or background_exploration"` (minus the one pre-existing failure) → 69 passed.
  - Full pipeline suite minus the two pre-existing working-tree failures (`test_worker_recovery_reconciles_completed_workflow_results_after_background_exploration` and `test_large_org_full_company_query_reuses_complete_baseline_without_former_delta`) → 319 passed.
- No live Harvest/Apify calls.

### Event-level workflow review handoff documented

- 已把流式响应级 workflow 的预期和工程边界补充到 `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`：
  - 产品预期：provider completion 后先 local apply / progress / next-submit，full materialization 异步追平
  - 业务流程：scoped search current/former lane、profile-search、profile registry、profile-scraper、candidate board serving 的端到端状态流
  - 不变量：真实 provider slot lease、`inline_incremental_ingest` marker、next-submit 不等 writer lock、completed reconcile job-level lease、registry-only marker path、zero-result retry vs degraded page coverage
  - 已踩坑：baseline 误覆盖 scoped lane、duplicate normalized Harvest query、phantom active worker、writer lock 空窗、重复 materialize、hot-cache/result-view 漂移、大 snapshot 本地热路径
  - 下一轮 review checklist：按模块审查 orchestrator/enrichment/seed_discovery/workflow_efficiency/candidate_artifacts/api 是否仍有人工测试才会暴露的服务级漏洞
- 已新增 `docs/archive/CLAUDE_CODE_EVENT_WORKFLOW_REVIEW_PROMPT.md`，用于启动 Claude Code 新 session 做 workflow design review；`docs/INDEX.md` 已加入入口。
- `docs/NEXT_TODO.md` 已新增 `Event-level workflow design review handoff`，要求 Claude review 后把 findings、修复和验证及时同步回 `PROGRESS.md` / `NEXT_TODO.md`。
- 这次只更新文档与 handoff prompt，不触发真实 provider 调用。

### Harvest profile-search zero-result retry and serving-view CLI formalized

- 根据 Harvest/API 服务商回复，`linkedin-profile-search` 同一 input 偶发 `0 profiles` 是已知 LinkedIn/provider transient issue；本地 contract 已调整为主要针对“真 0 结果”做可恢复重试，而不是把常见的总量漂移/缺页都视为强 incomplete。
- 已实现 provider search 语义调整：
  - `HarvestProfileSearchConnector.search_profiles(...)` 支持 `zero_result_retry_attempts` / `zero_result_retry_backoff_seconds`
  - live 模式下如果 raw/shared cache 中是 `rows=[]` 且 `pagination.total_elements<=0`，会忽略该 0 结果 cache 并重新请求
  - probe、scale、page chunk、single-page fallback 都透传 zero-result retry 设置
  - 只有重试耗尽后仍是真 0 结果，才写 `status=incomplete` / `incomplete_reason=provider_zero_results_after_retry`
  - provider total 在不同 run 间变化、或 chunk/page coverage 小于 probe total，默认记录 `status=degraded` / `provider_search_degraded=true`，用于审计和后续补跑，不再作为本地开发流程的强阻断条件
- 已补正式 serving-view 运维入口：
  - `build-company-candidate-artifacts` 增加 `--build-profile` 和 repeatable `--preferred-source-snapshot-id`
  - 新增 `rebuild-company-serving-view`，作为 scoped/source provenance projection repair 的显式 rebuild CLI
  - 新增 `audit-company-serving-view`，只读输出 registry、manifest/pages、`source_matches/matched_keywords` 采样、projection version、build profile、job result view drift
  - 新增 `repoint-job-result-view`，默认 dry-run；只有 `--policy serve_latest_company_asset --apply` 才会改变旧 job 指针，`historical_replay` 明确 no-op
- artifact summary/manifest 现在写入：
  - `build_profile`
  - `projection_version`
  - 现有 `timings_ms` / `source_snapshot_selection`
- 真实 Google/Gemini artifact 只读审计已通过：
  - `audit-company-serving-view --company Google --snapshot-id 20260428T011339 --job-id 92eb11472da4 --sample-pages 1`
  - `status=ok`、`candidate_count=9359`、`page_count=188`
  - 旧 artifact summary/manifest 尚无新顶层 `build_profile`，但 audit 可从 candidate shard sample 推断 `projection_version=candidate_artifact_projection_v20260427_source_matches`
  - sample page 读到 `source_matches_records=54`、`matched_keywords_records=54`，job result view drift 为 false
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/cli.py tests/test_harvest_connectors.py tests/test_seed_discovery.py tests/test_candidate_artifacts.py tests/test_cli.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py tests/test_seed_discovery.py` -> `121 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py -k 'records_phase_timings'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_cli.py -k 'audit_company_serving_view or repoint_job_result_view'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'blocks_incomplete_provider_probe_fallback or scoped_search_seed_pool_starts_former_search_lane_in_parallel or default_former_search_seed_preserves_scoped_keywords'` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`

### Company canonical serving view boundary clarified

- 复盘 scoped artifact rebuild 时明确数据边界：
  - `canonical_merged` 是某个 snapshot 下的 artifact view，不是自动吞掉所有历史 snapshot 的 company-wide evergreen index
  - `source_snapshot_selection` 决定一个 artifact view 合并哪些 source snapshots；无边界 all-history union 只能作为 legacy / repair fallback，不能作为 scoped provenance rebuild 默认策略
  - 一人多 sharding 必须通过同一候选人的 `source_matches` / `matched_keywords` 表达，而不是把多个历史 snapshot 的候选人无差别塞进旧 job snapshot
  - `job_result_view` 是 job serving 指针；新 scoped workflow 完成后应指向本次 current snapshot。是否把旧 job repoint 到更新的 company canonical view，需要显式区分 historical replay 与 serve latest company asset
- 已记录方法论：
  - `docs/DATA_ASSET_GOVERNANCE.md` 新增 `Snapshot Selection Is Not Shard Membership`
  - `docs/NEXT_TODO.md` 新增 company canonical serving view / job result view 边界与后续 backlog
- 真实状态修正：
  - 停止了误按旧 all-history union 继续重建 OpenAI snapshots 的批处理
  - `OpenAI 20260422T161258` 已恢复为该 job snapshot 边界的 `352` 人 artifact，避免误写成全历史 `826` 人
  - Anthropic / xAI 这类先有 full authoritative asset、后续 query 复用本地资产的记录不纳入 scoped provenance rebuild；旧 artifact 可继续走文本 fallback，除非明确要发布新版 company canonical serving projection

### Google/Gemini scoped serving artifact rebuild completed

- 收口 query/job `92eb11472da4`（`帮我找Google在Gemini组的人`）的本地 serving 状态：
  - `runtime/company_assets/google/20260428T011339/normalized_artifacts/manifest.json` 已生成，`candidate_count=9359`、`page_count=188`
  - `source_snapshot_selection.mode=current_snapshot_only_large_org`，只选 `20260428T011339`，明确排除旧 Google historical snapshots，避免 848MB 级历史 baseline 预读和历史污染
  - page artifacts 只读复核：`page_rows=9359`、`source_matches_records=3940`、`matched_keywords_records=3940`、`gemini_source_hits=43`
  - PG `job_result_view` 和 job progress latest metrics 已 repoint 到 `snapshot_id=20260428T011339`、`view_kind=asset_population`、`source_kind=company_snapshot`
  - completed event 已写入：`Scoped serving artifacts rebuilt and job result view repointed to Google 20260428T011339.`
- 明确保留 provider incomplete 边界：
  - Harvest profile-search 对 current/former Gemini 的 scale/page chunk 仍存在 provider-empty page range；不把这类 search-seed 视为完整覆盖
  - 相关 search worker 被按 `provider_search_incomplete_retry_later` 处理，不再触发额外 provider 调用；后续同 query 可重新补 current lane / former lane 的缺页
  - 本次 materialization 表示“当前已落地 snapshot 的 serving artifact 完整”，不表示 Harvest provider 已返回 Gemini 全量理论结果
- 手动 rebuild 过程暴露并修复的通用性能边界：
  - `background_snapshot_materialization_reconcile` 现在和 pre-retrieval / background-harvest refresh 一样使用 `foreground_fast` artifact profile
  - `materialize_company_candidate_view(...)` 会先加载当前 snapshot；当前 snapshot 已达大组织阈值或存在显式 preferred snapshot set 时，不再预读全历史 candidate documents
  - `foreground_fast` artifact projection 优先使用 candidate metadata 中已经嵌入的 profile timeline / signals，不为每个候选人先解析 raw profile source path
  - `foreground_fast` fingerprint 不再对 candidate/evidence/profile raw source paths 做 `stat()`，只保留路径字符串进入 fingerprint，避免大 snapshot 重投影时的无收益文件系统探测
  - Google 真实 artifact timing 复核：`prepare_candidates≈54.8s`、`view_write_total≈67.4s`；剩余主要是 9k 人文本/facet projection CPU 成本，后续可继续做增量和缓存优化
- 已跑或已保留的验证信号：
  - `tests/test_pipeline.py::test_snapshot_materializer_background_snapshot_reconcile_uses_foreground_fast_artifact_profile`
  - `tests/test_candidate_artifacts.py::test_materialized_view_large_org_does_not_preload_all_history`
  - `tests/test_candidate_artifacts.py::test_candidate_materialization_fingerprint_can_skip_source_path_stat`
  - 真实 artifact 只读检查确认 manifest/pages/source provenance 与 job result view 指针一致

### Local manual-test startup contract tightened

- 复盘本地手测启动时发现的脚本契约问题：
  - 旧 `scripts/dev_backend.sh --no-daemon` 只是不启动外置 worker daemon，但 `serve` 进程仍会启动 server-side runtime watchdog
  - 这会在“只想打开前端手动看页面”时抢跑 `background_snapshot_materialization.status=scheduled` 的 backlog，和用户预期不一致
- 已修复启动语义：
  - `--no-daemon` 现在默认同时传 `--disable-runtime-watchdog`
  - 如需只关闭外置 daemon 但保留 serve 内置 watchdog，必须显式传 `--enable-runtime-watchdog`
  - 当前只读进程检查显示后端、worker daemon、前端均未运行；只剩 `scripts/ecs_webhook_reverse_tunnel.py` 在运行

### Event-level profile apply/materialize separation hardening

- 复盘 Google/Gemini job `92eb11472da4` 暴露的工程审查缺口：
  - 之前验证了“单次 callback 内 next-submit 在 apply/materialize 之前调用”，但没有验证“下一次 remote completion 到达时，即使 job writer lock 被长时间占用，也能先触发 next-submit”
  - live 证据显示第三波 profile actor 在 `19:22:39-19:22:43` 已完成，但上一轮 callback 仍在 writer lock 内做 apply/delta-sync，第四波补位直到 `19:26:19` 才发生
  - 这是本地工作流状态机和测试标准问题，不是 Apify webhook/provider 问题
- 已修复状态拆分：
  - Harvest profile completion 的 post-ingest next-submit 保持在 writer lock 之前执行
  - completed workflow reconcile 先回填旧 `inline_incremental_ingest` marker，再处理新的 pending worker，避免旧 consumed worker 因 sibling pending 被跳过 backfill
  - provider dataset apply 成功后立即写 `inline_incremental_ingest`；full materialize/retrieval/layering 作为 `background_snapshot_materialization` 的 scheduled downstream 阶段单独恢复
  - 如果 materialize 进程被杀，worker 不会再被视为未消费；后续 recovery 只需继续 scheduled materialization
  - terminal workflow 的 completed-reconcile lease 如果持有者明确是同机已死亡 PID，会自动 safe-release 并重试 reconcile；远端或不可判定 owner 仍不释放
- 已修复 local apply 热点：
  - `apply_harvest_profile_workers_to_snapshot(...)` 以前在 LinkedIn URL 没命中候选人时退化为每个 profile 扫描全量 candidates 做姓名匹配
  - 现在新增 name-index 预筛选；大 snapshot 不再做全表 fallback，避免 9k candidates x profile batch 的 CPU 热点拖慢 marker 写入
- 真实 Google/Gemini 状态已修到安全边界：
  - `17/17` 个 Harvest profile worker 已有 `inline_incremental_ingest` marker
  - `13` 个旧 worker marker 为 `deferred`，最后 `4` 个为 `materializing`
  - 后续已通过独立 scoped artifact builder 收完 `background_snapshot_materialization` 的 serving artifact，并 repoint job result view；未新发 Harvest provider run
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/snapshot_materializer.py tests/test_pipeline.py tests/test_candidate_artifacts.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_harvest_reconcile_marks_consumed_before_materialize_failure or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent or completed_workflow_reconcile_backfills_consumed_marker_without_rebuild or harvest_profile_completion_prefetch_runs_before_writer_lock or scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize or completed_workflow_reconcile_releases_dead_terminal_lease'` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py -k 'harvest_profile_apply_uses_name_index_instead_of_large_full_scan or build_company_candidate_artifacts_materializes_backlog_and_reusable_docs'` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py tests/test_workflow_event_response.py tests/test_runtime_lease_utils.py` -> `13 passed`

### Google/Gemini Harvest profile-search provider anomaly guard (superseded by zero-result retry contract)

- 排查 query/job `92eb11472da4`（`帮我找Google在Gemini组的人`）时确认一个 provider 级边界：
  - Harvest `linkedin-profile-search` 对 `Gemini + currentCompanies=Google` probe 返回 `736` total，对 `Gemini + pastCompanies=Google` probe 返回 `991` total
  - 同一参数放大到 scale 请求时 provider 多次返回空数组；进一步按 `startPage/takePages` 分块和单页 retry 后，也仍有大量页段空返回
  - 这不是本地 current/former lane 生成缺失；current/former 请求都已发出，问题是 provider 对部分 page range 返回不完整结果
- 当时先停止 live repair，避免继续消耗 Harvest API；已给 job 写入 provider anomaly event：
  - `provider_issue=harvest_profile_search_empty_page_ranges_after_probe_total`
  - `policy=mark_incomplete_do_not_treat_as_completed`
- 当时的状态机防回退已保留，但当前最新语义已收窄：
  - `HarvestProfileSearchConnector.search_profiles(...)` 支持 `start_page`
  - SearchSeed 的 scale-empty fallback 会按 page chunk 补拉；chunk 空时降级到单页 retry
  - 最新 contract 只把重试耗尽后的真 0 结果标记为 `provider_search_incomplete`
  - provider total 漂移、返回数小于 probe total/effective limit、或局部 page range 空返回，默认标记 `status=degraded` 用于审计/后续补跑，不再作为强 blocking 条件
  - former scoped-search lane 默认保留原 scoped keyword（例如 `Gemini`），不再退化成 broad past-company-only pass
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition.py tests/test_seed_discovery.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_seed_discovery.py -k 'page_chunks_when_scaled_harvest_result_is_empty or retries_empty_page_chunk_as_single_pages or probe_fallback_without_chunks_is_marked_incomplete'` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'blocks_incomplete_provider_probe_fallback or default_former_search_seed_preserves_scoped_keywords or scoped_search_seed_pool_starts_former_search_lane_in_parallel'` -> `3 passed`

### Scoped-search streaming E2E hardening

- 补齐了 scripted E2E，覆盖真实 Meta scoped-search + baseline reuse 的乱序完成形态：
  - job 先以 baseline snapshot 完成并服务旧结果
  - 两个 search-seed shard 乱序完成，先完成的 shard 会先 apply 并触发 profile prefetch opportunity
  - 同一 job 仍有 search-seed sibling worker 未 drain 时，completed-job reconcile 不再进入 full candidate materialize
  - 两个 Harvest profile batch 乱序完成，先完成的 profile batch 先 local apply / 写 consumption marker / 触发 next-submit opportunity
  - 同一 job 仍有 profile sibling worker 未 drain 时，full materialize 继续 defer
  - 最后一个 profile batch 完成后，candidate artifacts、job result view、candidate board、outreach layering 都收敛到同一个 current `snapshot_id`
  - 重复触发 search/profile completion batch processor 时，返回 `no_completed_unconsumed_inline_workers`，不重复消费 worker，也不重复 materialize
- 修复 completed-job search-seed reconcile 的调度缺口：
  - `_reconcile_completed_workflow_after_search_seed(...)` 现在复用 same-kind remaining-worker 判定
  - 有 pending search-seed sibling worker 时，只记录 `materialize_deferred` 结构化事件并写 consumption marker，不调用 `_synchronize_snapshot_candidate_documents`
  - sibling search shard drain 后才执行 full candidate artifact sync / retrieval / layering，避免 search shard 每到一个就重建一次
- 调度审视结论：
  - profile prefetch worker 仍是 lease-first：拿不到真实 provider limiter slot 会返回 backpressure，不创建 active worker，不占 actor 口径
  - active profile worker 计数只把 `waiting_remote_harvest`、已有 remote `run_id/dataset_id`、或持有 provider limiter lease 的 worker 计入预算
  - remaining-worker/materialize defer 已覆盖本轮目标的 scoped-search + profile tail；company-roster completed-job 分支仍可继续做同样的泛化收口，已记入 TODO
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'reconcile_completed_workflow_after_background_search_seed or search_seed_worker_completion_prefetches_profiles_before_full_materialize or scripted_scoped_search_baseline_reuse_materializes_layers_and_serves_current_snapshot or scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize or completed_workflow_reconcile or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent'` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_runtime_tuning.py tests/test_harvest_connectors.py -k 'profile_prefetch or harvest_profile_batch or global_inflight_budget or mixed_success or code_22'` -> `20 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py` -> `3 passed`

## 2026-04-27 (Asia/Shanghai)

### Event-level efficiency metrics productization

- 把事件级 workflow 的验收口径从“最终 artifact 正确”继续推进到“状态机和效率也正确”：
  - 新增 `src/sourcing_agent/workflow_efficiency.py` 作为共享聚合器，只读取 job events / workers / summary，不读取 provider raw payload 或 candidate artifacts
  - 指标覆盖 `remote completion -> local event`、`remote completion -> local Harvest completion marker`、`local marker -> next-submit start`、post-ingest prefetch elapsed、重复 reconcile、materialize call count、真实 provider slot occupancy
  - provider slot 指标区分三种状态：持有 limiter lease 的真实 slot、已有 remote actor/run 的 worker、没有 limiter lease 却处于 active/remote-wait 口径的 phantom/pre-submit worker
  - 这些指标已接入 `get_runtime_metrics(...)`，并进入 hosted/scripted smoke 的 `provider_case_report.event_level_efficiency` 与 matrix 汇总
- 工程复盘：
  - 之前测试主要覆盖 eventual correctness，不能发现“结果最终正确但中间重复 full rebuild、next submit 被慢路径拖住、worker active 口径不真实”的问题
  - 现在新增负向断言：重复 reconcile/materialize 会被计数，phantom provider worker 会触发 violation，next-submit lag 与 remote-marker lag 会进入 smoke report
  - 当前指标是观测和回归入口，不作为 submit/materialize gate；后续若要设 SLO，应在 smoke/CI 层按环境 profile 判断，而不是让 runtime 自己阻塞业务流
- 已补回归：
  - `tests/test_workflow_efficiency.py` 覆盖 remote event lag、local completion marker lag、next-submit lag、provider slot occupancy、phantom worker、重复 reconcile/materialize
  - `tests/test_workflow_smoke.py` 覆盖 provider case report 与 smoke timing summary 暴露 event-level efficiency
  - `tests/test_pipeline.py::test_runtime_metrics_reports_refresh_and_reconcile_counters` 覆盖 runtime metrics 暴露 event-level efficiency rollup
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/orchestrator.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -k 'event_level_efficiency or materialization_streaming_rollup or exposes_provider_roster_profile'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'runtime_metrics_reports_refresh_and_reconcile_counters'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or queue_background_profile_prefetch or remote_provider_event or remote_event_lane or event_level_efficiency or completed_workflow_reconcile'` -> `34 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `git diff --check -- src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/orchestrator.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py tests/test_pipeline.py PROGRESS.md docs/NEXT_TODO.md docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md docs/archive/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md`

### Completed reconcile structured observability

- completed workflow reconcile 现在会写结构化 job events，而不是只靠 `detail` 文本排障：
  - `event_family=completed_workflow_reconcile`
  - `phase=lease_acquired|lease_skipped|started|marker_backfilled|materialize_started|materialize_completed|materialize_deferred|completed|failed`
  - `reconcile_kind=coordinator|company_roster|search_seed|harvest_prefetch|outreach_layering|exploration|snapshot_materialization`
  - 统一包含 `snapshot_id`、`worker_ids`、`worker_count`、`materialize_call`、`materialize_signature`、`marker_backfill_count`、`lease_acquired` / `skip_reason` 等机器可读字段
- `workflow_efficiency.py` 已改为结构化优先：
  - 只要 job events 中出现 `completed_workflow_reconcile`，metrics 就完全按结构化字段统计
  - 旧自然语言 `detail` 解析只作为历史 job fallback，避免新旧事件同时存在时重复计数
  - 新增计数包括 `lease_skipped_count`、`marker_backfill_count`、`materialize_started_count`、`materialize_completed_count`、`materialize_deferred_count` 和 `structured_event_count`
- 已补回归：
  - lease 被持有时必须落 `lease_skipped` 结构化事件
  - marker backfill 必须落 `marker_backfilled` 结构化事件，且不触发 rebuild
  - harvest completed reconcile 必须落 `materialize_started/materialize_completed`
  - metrics 必须优先读结构化事件，并忽略同 job 中残留的 legacy materialization 文本事件
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_efficiency.py tests/test_pipeline.py tests/test_workflow_efficiency.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_efficiency.py` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_reconcile or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent'` -> `3 passed`

### Meta Audio completed-reconcile state monotonicity hardening

- 复盘 query/job `d964b1d42ac5`（`帮我找Meta做Audio方向的人`）暴露的第二层效率问题：
  - provider/webhook 不是当前瓶颈；Stage 1 `806` candidates 的 profile registry 已经 fetched
  - snapshot `runtime/company_assets/meta/20260427T203601/` 已追平：`candidate_count=1727`、`profile_detail_count=1727`、`profile_completion_backlog_count=0`
  - 真正残余是 completed-job reconcile 的状态单调性：worker `394` 已被旧路径 materialize，但 worker output 没有 `inline_incremental_ingest` consumption marker
  - 这会让排障口径依赖 `background_reconcile.last_worker_updated_at` 间接判断，且多个 recovery 入口有机会重复进入 full materialize / outreach layering 慢路径
- 工程原因复盘：
  - 之前 scripted/regression 偏“最终一致性”：能恢复、能去重、能最终生成 artifact
  - 没有把效率与状态机不变量纳入硬断言：同一 provider completion 只消费一次、completed reconcile 必须有 job-level lease、worker 未拿到真实 provider slot 前不能占 actor budget、post-ingest next-submit 不能解析 raw payload/materialize
  - 因此事件级流式改造的部分失败模式表现为“结果最终对，但 tail latency 和重复重建不对”，旧测试不会失败
- 已修复：
  - `_reconcile_completed_workflow_if_needed(...)` 现在使用 store-backed job lease；同一个 completed workflow 的 background reconcile 不会被多个 recovery tick / 手动 recovery 并发执行
  - completed-job reconcile 直接消费 company-roster / search-seed / harvest-prefetch worker 后，会写统一 `inline_incremental_ingest` marker
  - 对旧状态增加轻量 marker backfill：如果 `background_reconcile.<kind>.worker_ids` 已证明 worker 被消费，但 worker output 缺 marker，只补 consumption marker，不重新 apply/materialize
  - 真实 Meta Audio 已用新代码跑 job-scoped recovery：worker `394` 补齐 `harvest_prefetch` marker，worker `347` 补齐 `search_seed` marker，未触发新的 provider run 或 full rebuild
- 新增回归：
  - completed Harvest worker 被 completed-job reconcile 消费后必须写 marker，第二次 reconcile 不能再次 apply/materialize
  - completed workflow reconcile 在 job lease 被持有时必须跳过，不能并发重建
  - 旧 `background_reconcile` 已记录消费但 worker 缺 marker 时，只做 marker backfill，不触发 rebuild
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent or completed_workflow_reconcile_skips_when_job_lease_is_held or completed_workflow_reconcile_backfills_consumed_marker_without_rebuild or harvest_profile_completion_callback_coalesces_completed_workers_before_prefetch'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or queue_background_profile_prefetch or harvest_completion_event or remote_event_lane or handle_remote_provider_event or completed_workflow_harvest_reconcile or completed_workflow_reconcile'` -> `29 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`

### Meta Multimodal scoped-search event-level response hardening

- 排查 query/job `2212a9f2e1c7`（`帮我找Meta做Multimodal方向的人`）里 `linkedin-profile-scraper` 看似只提交 4 个 actor 的问题：
  - 真实 Apify actor run list 显示该 job 的 profile scraper 实际有 `12` 个 `SUCCEEDED` run，时间覆盖 `2026-04-27T11:07:28Z` 到 `11:20:09Z`
  - 本地 `runtime/company_assets/meta/20260427T190455/harvest_profiles/` 也有 `12` 个 `harvest_profile_batch_*.queue_summary.json`
  - snapshot 已完整：`candidate_count=961`、`profile_detail_count=961`、`profile_completion_backlog_count=0`
  - 误导点是部分 cache-hit completion 路径把已有 `run_id/dataset_id` 写成空字符串，导致本地 summary / worker checkpoint 无法和 Apify UI/API 对账
- 已修复 profile batch remote identifier contract：
  - `HarvestProfileConnector.execute_batch_with_checkpoint(...)` 在 shared-cache hit 时会继承已有 checkpoint 的 `run_id/dataset_id`
  - enrichment writer 会从 `run_get/run_post/cache_hit` artifacts 兜底回填 remote identifiers，再写 queue summary 和 worker checkpoint
  - 已对真实 Meta job 回填：12 个 batch summary 和 12 个 worker checkpoint 现在全部有 `run_id/dataset_id`
- 已修复 completion event 后 next-submit 判定的同步慢点：
  - 真实 Meta `20260427T190455` 证明 webhook/event 已被记录，问题不是“没有事件唤醒”
  - 慢点之一是 `_handle_harvest_profile_completion_event(...)` 后续为了找下一批 URL 会从 full baseline 重新检查已 fetched profiles，并解析大量 raw Harvest payload
  - 新增 registry-only cache marker 路径：post-ingest prefetch opportunity 只用 PG `linkedin_profile_registry` 的 fetched/queued 状态判定是否需要 submit，不读取/解析已抓取 raw profile JSON
  - 完整 profile payload 解析仍留给下游 apply/materialize；next-submit path 不再被 materialization payload parsing 拖住
  - 真实 Meta 快路径复核：`471` 个 URL 全部 registry-cache 命中，`dispatched_url_count=0`，耗时约 `270ms`；旧路径本地直接调用超过 `25s` 仍未返回
  - 进一步补齐运行态观测：`remote_provider_event` payload 现在记录 `source`、`remote_completed_at`、`local_event_seen_at`、`remote_to_local_event_lag_ms`；Harvest completion event 记录 `post_ingest_prefetch_candidate_count`、`post_ingest_prefetch_dispatched_url_count`、`registry_cache_marker_count`、`post_ingest_prefetch_elapsed_ms`
  - registry-only 快路径复核更新：同一真实 Meta snapshot `471` 个 URL 全部 registry-cache marker 命中，`dispatched_url_count=0`，本地外层调用约 `126ms`，prefetch 自身约 `117ms`
- 已继续收口 scoped-search current/former lane 编排：
  - scoped search sibling former lane 的并行启动条件从 `task_type == acquire_full_roster` 放宽为基于 `strategy_type=scoped_search_roster + include_former_search_seed + 当前 lane 非 former`
  - 因此即使入口是 `acquire_search_seed_pool`，也可以启动 former sibling lane，不再依赖 acquire-full-roster 这个历史 task shape
  - 如果 former sibling lane 已经在上游并行完成并写入 durable search-seed snapshot，后续独立 `acquire_former_search_seed` task 会复用 existing former lane，避免重复提交 provider search
- 已校正 webhook/local watcher 默认策略：
  - Meta 这次不是 webhook 失效导致；主要原因是旧 scoped-search task shape 串行，以及 cache-hit completion 写空 remote identifiers
  - 当前已存在 ECS 稳定 webhook URL，因此配置了 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 时默认不启动本地 long-poll watcher，避免双路唤醒噪声
  - 只有本地 tunnel 调试或怀疑 webhook URL 失效时，才显式设置 `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=1` 启用 watcher fallback
- 真实运行诊断结论：
  - 这次不是 profile scraper 只跑了 4 个，也不是 profile 获取中断；真实慢点来自旧运行态里 current/full task 完成后才启动独立 former task，以及部分 actor 完成发现依赖 recovery cadence
  - 新代码已修复 scoped-search sibling lane 并行入口、duplicate former task 复用和 remote id 对账；webhook 已配置时继续以 provider push 为主
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/acquisition.py src/sourcing_agent/enrichment.py src/sourcing_agent/harvest_connectors.py tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py -k 'scoped_search_roster_starts_former_search_lane_in_parallel or scoped_search_seed_pool_starts_former_search_lane_in_parallel or acquire_former_search_seed_reuses_existing_durable_former_lane or cache_hit_preserves_remote_identifiers or remote_identifiers_fall_back_to_run_artifacts or local_provider_event_watcher'` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or queue_background_profile_prefetch or scoped_search or acquire_former_search_seed or local_provider_event_watcher'` -> `34 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py -k 'scoped_search_seed_pool_starts_former_search_lane_in_parallel or acquire_former_search_seed_reuses_existing_durable_former_lane or queue_background_profile_prefetch_can_use_registry_only_cache_markers or defaults_submit_budget_to_actor_budget or profile_search_prefetch or local_provider_event_watcher or handle_remote_provider_event'` -> `11 passed`

### Meta scoped-search stale serving pointer and hot-cache fallback repair

- 后续追查同一 job 的 `华人线索信息分层` 全是 Layer 0：
  - Meta 正确 snapshot `20260427T153312` 已有 515 candidates / 515 profile detail，但没有 canonical `layered_segmentation`
  - 旧 snapshot `20260423T062947` 有 `greater_china_outreach_20260427T075517Z` 分层产物，且只有 2 candidates，证明此前 background outreach layering 使用了 stale serving pointer
  - 前端同时存在误导性兜底：后端返回 `outreach_layer=null` 时，adapter 会转成 `0`，让“未生成分层”看起来像真实 Layer 0
- 本轮补充修复：
  - completed-workflow outreach layering reconcile 现在在缺失分层或分层 snapshot 与 `candidate_source.snapshot_id` 不一致时，会以当前 candidate source snapshot 为准重新分层
  - outreach layering 生成默认读取/写入 canonical `runtime/company_assets`；只有 canonical 不可读时才退回 hot-cache，避免新增分层资产只落 serving cache
  - 分层 context 读取同时检查 canonical 与 hot-cache，并按 run token 选择最新、同 token 优先 canonical，防止 hot-cache 路径字母序遮蔽 authoritative artifact
  - cached outreach summary 现在保留 `final_layer_distribution`
  - 前端 `outreachLayer` contract 改为 nullable；缺失分层显示“分层未生成/未分层”，不再计入真实 Layer 0
- 已修复真实 Meta job：
  - canonical 分层产物：`runtime/company_assets/meta/20260427T153312/layered_segmentation/greater_china_outreach_20260427T104417Z/layered_analysis.json`
  - job summary `outreach_layering` 已指向 canonical path，`candidate_count=515`
  - 重启 backend/frontend 后 API 全量分页复核：`Layer 0=353`、`Layer 1=33`、`Layer 2=17`、`Layer 3=112`
- 排查 query/job `ae500773be42`（`帮我找Meta做Agent方向的人`）候选人看板长期只显示 2 人的问题：
  - provider/search/profile 数据没有丢失：`runtime/company_assets/meta/20260427T153312/` 已有 `search_seed_discovery.entry_count=513`、`candidate_count=515`、`profile_detail_count=515`、`profile_completion_backlog_count=0`
  - `harvest_profile_batch_*.queue_summary.json` 共 `15` 个且全部 `completed`；requested URL sum 为 `503`，剩余候选人来自既有 cache/registry 复用，不是 profile 抓取中断
  - 真正的 serving 漂移是 job `candidate_source` 与 PG `job_result_view(ae500773be42)` 仍指向 4 月 23 日 Excel/import 形成的旧 Meta snapshot `20260423T062947`（2 人）
- 根因拆解：
  - completed-worker reconcile 中，`snapshot_dir` 会优先用 worker metadata 指向的新 snapshot，但 `snapshot_id` 先读 stale `job_result_view`
  - 这会形成不一致：新 worker 输出 apply 到新目录，但 `_execute_retrieval(... workflow_snapshot_id=old)` 又把 job summary/result-view 重写回旧 2 人 view
  - 真实 repair 还暴露出 hot-cache 风险：hot-cache manifest 存在但 shard 丢失时，authoritative loader 会被坏 hot-cache 截断，没能回退 canonical `company_assets`
- 泛化修复：
  - `resolve_reconcile_snapshot_id(...)` 现在在 worker-driven reconcile 中优先使用实际存在的 worker snapshot dir；只有 worker snapshot 不存在时才回退 `job_result_view` / summary
  - 继续保留 summary sparse 场景：如果 worker metadata 里只有缺失目录，仍可用 result-view 找回 snapshot
  - authoritative candidate artifact loader 遇到坏 hot-cache manifest/shard 时，会跳过坏 serving cache，并在 fallback 路径中强制回到 canonical snapshot / candidate documents；hot-cache 不再能遮蔽 canonical asset store
- 已修复真实 Meta job：
  - `job_result_view(ae500773be42)` 已指向 `snapshot_id=20260427T153312`、`view_kind=asset_population`、`candidate_count=515`
  - job summary `candidate_source` 同步指向 `20260427T153312`
  - direct candidate page 复核：`result_mode=asset_population`、`total_candidates=515`、`profile_fetch_progress=515/515`，首屏候选人带 LinkedIn URL
- 已补回归：
  - completed Harvest worker 指向新 snapshot、旧 job_result_view 指向旧 snapshot 时，reconcile 后 job summary/result-view 必须采用 worker snapshot
  - worker snapshot 缺失时仍用 job_result_view 恢复，避免破坏 sparse summary 兼容路径
  - hot-cache manifest 缺 shard 时 authoritative loader 会回退 canonical candidate documents
  - completed job 缺失 outreach layering 或 layering 指向旧 snapshot 时，会用当前 `candidate_source.snapshot_id` 重新 reconcile
  - canonical 与 hot-cache 同时有分层产物时，不再让旧 hot-cache path 遮蔽较新的 canonical analysis
  - 新增 scripted E2E 回归：baseline authoritative snapshot + current scoped search shard + local profile cache reuse + candidate artifact materialization + outreach layering + candidate board serving 必须收敛到同一个 current `snapshot_id`，并断言不会为了已缓存 profile 再提交新的 Harvest profile actor

### OpenAI scoped-search recall provenance and ECS deployment readiness check

- 排查 query/job `828b63063fb3`（`04/27 14:09`，`我想要OpenAI做Agent方向的人`）候选人看板中 `Agent` recall filter 数量与 Stage 1 preview 不一致的问题：
  - Stage 1 scoped-search preview 是 `54` candidates
  - final candidate board 服务的是 OpenAI full asset population，`total_candidates=826`，不是只服务 Stage 1 preview 子集
  - `linkedin-profile-scraper` 只新拉取 `27 + 11 = 38` 人，是因为另外 `16` 个 profile 已从 cache/registry 复用，不是 provider 少抓
  - 真实问题是 serving projection 丢了 query/source provenance：`candidate_documents.json` 里已有 `metadata.seed_query=Agent`，但 normalized/page shard 在 canonical/history merge 或旧 shard 复用后不稳定暴露这类来源字段
- 已做泛化修复：
  - canonicalization 合并时保留 `seed_query/source_query/query/scope_keywords/seed_keywords/intent_keywords/matched_keywords`，并归一进 `metadata.matched_keywords`
  - candidate artifact writer 复用同一 source-match helper，把来源关键词写入 normalized/page records，并额外写入可审计的多条 `source_matches`
  - frontend recall filter 现在优先用 `matchedKeywords` provenance 判断 recall bucket；只有缺 provenance 时才 fallback 到全文匹配
  - shard fingerprint 纳入 `_CANDIDATE_ARTIFACT_PROJECTION_VERSION`，让 serving projection/schema 变化能使同一 snapshot/view 的旧 serving shard cache 失效
- 设计边界已确认：
  - 一个候选人可以同时属于多个 query/shard，这是正常的；projection version 不是“全局删除旧 sharding 记录”
  - serving layer 仍保持“一个 canonical candidate 一个当前 materialized shard file”，但该文件内部的 `source_matches` 是多值列表，不再表达成“候选人只能属于一个 query/source shard”
  - 当前实现只在同一个 `artifact_dir` / `asset_view` 的 serving cache 中清理 stale shard path，避免继续服务旧投影；历史 snapshot、旧 view、其它 shard provenance 不会被全局抹掉
  - 本轮只为测试重建真实 OpenAI snapshot `runtime/company_assets/openai/20260427T141011/normalized_artifacts/`；其它历史 serving artifacts 先保留，后续按 maintenance backlog 分批 repair
- 真实 OpenAI snapshot 已重建并复核：
  - `candidate_documents.linkedin_stage_1.json` 为 `54`
  - full `candidate_documents.json` / `normalized_artifacts/artifact_summary.json` 为 `826`
  - `profile_fetch_progress=826/826`
  - API 复核 `Agent` provenance count 为 `54`，此前缺失的 `Aarash Heydari`、`Chi Jin`、`Tsubasa S.`、`Vincent Zhao` 已能通过 provenance 进入 Agent recall bucket
  - 二次定向 rebuild 已升级到 `candidate_artifact_projection_v20260427_source_matches`
  - page artifacts 复核：`source_matches_records=826`、`multi_source_match_records=147`、`Agent=54`、预期缺失姓名 `[]`
- 重建暴露的 materialization 慢点已记录为后续 backlog：
  - 首次真实重建 `prepare_candidates≈52s`、`state_upsert≈16s`
  - projection-version full dirty rebuild `prepare_candidates≈54s`、`state_upsert≈0.4-0.7s`、`dirty_candidate_count=826`
  - 当前主要慢点在 `prepare_candidates`，不是 provider/API；后续应从 profile timeline/registry lookup、candidate shard payload prepare、并行度和增量 projection 入手，而不是把 provider submit 重新串回 materialize
- Anthropic 复用 query 边界：
  - `帮我找Anthropic做Pre-training的人` 这类 full asset reuse query 仍可快速分页展示 authoritative snapshot；当前 Anthropic `20260416T225318` 有 `manifest/pages`，`candidate_count=3455`
  - 该旧 snapshot 的 pages 还没有新版 `matched_keywords/source_matches` provenance；召回排序会按前端 fallback 走候选人文本匹配，不需要为了手动测试先全量 rebuild Anthropic
  - 如果后续要求 Anthropic recall filter 与 OpenAI 新版一样走可审计 source provenance，再定向 rebuild 对应 Anthropic snapshot，而不是全历史重建
- ECS 部署前只读检查：
  - `https://api.111874.xyz/health` / `/api/runtime/health` / `/api/providers/health` 当前可访问，远端 service 进程 PID `116522` 仍在运行
  - 当前 ECS service unit 使用 `/opt/sourcing-ai-agent` + `/opt/sourcing-ai-agent/venv/bin/python3`，不是文档中推荐的 `/srv/sourcing-ai-agent/repo/sourcing-ai-agent` repo layout
  - 当前进程已有 PG-only 关键 env：`SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`、`SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`、`SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`、`SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - 当前进程缺少 `SOURCING_RUNTIME_ENVIRONMENT=production`、`SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL`、provider webhook token env
  - 当前 ECS hosted route `POST /api/providers/apify/webhook` 返回 `404`，说明远端仍是旧后端代码；部署当前本地代码前，不能假设 hosted provider webhook 已可用
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/domain.py src/sourcing_agent/canonicalization.py src/sourcing_agent/candidate_artifacts.py tests/test_canonicalization.py tests/test_candidate_artifacts.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_canonicalization.py tests/test_candidate_artifacts.py tests/test_results_api.py -k 'source_seed_keyword_provenance or reuses_candidate_shards_and_rebuilds_only_dirty_candidates or search_seed_preview_requires_profile_completion or seed_query_as_matched_keyword'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `cd frontend-demo && npm run build` -> passed
  - 真实 OpenAI artifact 计数复核：Stage 1 `54`、full/summary/manifest `826`、profile detail `826`、`Agent` provenance `54`、缺失预期姓名 `[]`
  - `git diff --check -- PROGRESS.md docs/NEXT_TODO.md docs/archive/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md docs/ECS_PRELAUNCH_CHECKLIST.md src/sourcing_agent/domain.py src/sourcing_agent/canonicalization.py src/sourcing_agent/candidate_artifacts.py frontend-demo/src/lib/candidateFilters.ts tests/test_canonicalization.py tests/test_candidate_artifacts.py`

### Windsurf candidate board serving artifact and profile prefetch scheduling repair

- 排查 query/job `e69cbd03045f`（`04/27 13:12`，`帮我找Windsurf的全部成员`）候选人看板中部分卡片没有“打开 LinkedIn”按钮：
  - `candidate_documents.json` 与 candidate shard 的 `materialized_candidate.linkedin_url` 已有 URL
  - `normalized_artifacts/pages/page-*.json` 只写入 normalized record，normalized contract 只保留 `has_linkedin_url/urls/public_identifier`，没有保留顶层 `linkedin_url`
  - 这是 serving artifact/page materialization contract bug，不是 provider 没返回 URL，也不是前端按钮逻辑本身缺失
- 泛化修复：
  - `_normalize_candidate(...)` 现在把 `candidate.linkedin_url` 写入 normalized record
  - 正常 `build_company_candidate_artifacts(...)` 和 `repair_paginated_candidate_artifacts_from_materialized(...)` 都会从 materialized candidate、normalized record、reusable document、profile completion backlog 回填 serving identity fields
  - 如果旧 shard/page 缺 serving identity，后续 artifact build 会标记 `serving_identity_fields_changed` 并重写，不依赖 Windsurf 特例
- 已修复真实 Windsurf snapshot：
  - `runtime/company_assets/windsurf/20260427T131256/normalized_artifacts/pages/page-0001.json` 中 `Abd samad Chbani`、`Achuil Aguer Akot`、`Ahmad Wanni` 均已有顶层 `linkedin_url`
  - HTTP 复核 `GET /api/jobs/e69cbd03045f/candidates?offset=0&limit=80&lightweight=true&force_refresh=1` 返回 `total_candidates=423`、`profile_fetch_progress=423/423`，样本候选人均返回 LinkedIn URL
- 排查同一 job 的 Harvest profile actor 调度：
  - Apify 后台的批次确实没有持续吃满 4 个 actor；本地事件线显示 actor global limiter budget 为 `4`，但 profile prefetch 的 submit budget 默认被 adaptive window 的 `recommended_max_workers` 压到 `1` 或 `2`
  - 典型事件：active worker 仍为 `1` 时，submit budget 也降为 `1`，导致 tail URL 被判 `harvest_profile_prefetch_backpressure`，只能等下一轮 recovery / webhook 事件再发
  - 修复后 adaptive window 继续决定 batch size 和建议并发，但默认 submit budget 至少等于 `harvest_profile_actor_global_inflight`；显式 `harvest_profile_batch_submit_global_inflight` 仍可覆盖以保护 provider
- 前端自动刷新修复：
  - candidate board refresh polling 不再只看 queued/failed/missing/deferred；当 `fetched + unrecoverable < total` 时也视为 profile 追平未完成并继续刷新
  - LinkedIn 同步文案增加“追平 N”，避免 `LinkedIn 211/423，排队 0，可重试 0` 让用户误以为没有 pending work
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/enrichment.py tests/test_candidate_artifacts.py tests/test_enrichment.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py tests/test_enrichment.py -k 'search_seed_preview or repair_paginated_candidate_artifacts_from_materialized_does_not_promote_registry or queue_background_profile_prefetch_defaults_submit_budget_to_actor_budget or queue_background_profile_prefetch_defers_when_active_batch_worker_exhausts_budget'` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_pipeline.py -k 'harvest_profile_batch or background_harvest_prefetch or profile_search_prefetch or background_prefetch or remote_event_followup or queue_background_profile_prefetch'` -> `16 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'asset_population_lightweight_page_preserves_linkedin_url_before_profile_detail or profile_fetch_progress or dashboard_is_summary_only'` -> `2 passed`
  - `cd frontend-demo && npm run build` -> passed

### Wispr Flow roster/search-seed lane hydration repair

- 排查 query/job `87fa38d196a8`（`04/27 11:54`，`帮我找Wispr Flow的全部成员`）候选人看板一度显示 `84/84`、随后 `102/102`，但 provider 实际返回 `102 former/search-seed + 85 current roster` 的原因：
  - company roster worker 已把 current roster artifacts 落盘
  - search-seed/former lane 的 in-memory state 仍只有 `102` 人
  - 后续 `enrich_linkedin_profiles` 直接信任 stale in-memory state 写回 `candidate_documents.json`，覆盖了 roster worker 早先落盘的 current roster 结果
  - 这不是 provider 没返回，也不是缺 LinkedIn URL；是并行 lane 完成顺序不同后，下游 enrichment 输入状态没有从 durable snapshot artifacts 重新 hydrate/merge
- 泛化修复：
  - `AcquisitionEngine._enrich_profiles(...)` 入口现在会先从当前 `snapshot_dir` hydrate roster snapshot 与 search-seed snapshot，再与内存 state 合并后进入 canonicalization/enrichment
  - 新增 `load_search_seed_snapshot_from_snapshot_dir(...)`，复用 search-seed registry 的 lane/aggregate artifacts，而不是在 acquisition 里写 Wispr 特例
  - 修复 search-seed snapshot loader 的多 worker 乱序风险：即使 root `search_seed_discovery/entries.json` 已存在，也会继续合并 lane 子目录里的 entries；例如 `Agent` seed 已在 root、`Multimodal` worker 只写入 `search_seed_discovery/current/entries.json` 时，enrichment 入口会看到两组 seed
  - roster 和 search-seed 任一 lane 先完成或后完成，都以 snapshot artifacts 作为 durable handoff 重新合并；修复边界在 enrichment 输入层，不把 profile enrichment、provider submit、delta apply、candidate artifact materialization 重新串成单条同步链
- 已修复真实 Wispr snapshot：
  - `runtime/company_assets/wisprflow/20260427T115446/candidate_documents.json` 已恢复到 `185` candidates
  - current/former 口径为 `84 current + 101 former`；`85 current + 102 former/search-seed` 中有 `2` 个重复人员被 canonicalize 合并
  - `normalized_artifacts/artifact_summary.json` 现在为 `candidate_count=185`、`profile_detail_count=185`、`profile_completion_backlog_count=0`、`missing_linkedin_count=0`
  - direct orchestrator 复核 job `87fa38d196a8` candidate page：`result_mode=asset_population`、`total_candidates=185`、`profile_fetch_progress=185/185`
- 看板追平修复：
  - asset population cache key 已包含 `artifact_summary.json` / `manifest.json` 文件 fingerprint，异步 artifact rebuild 后 backend 不应继续返回旧 profile progress summary
  - 目标候选人看板 hydration hook 增加 bounded asset-population refresh polling，异步 materialization 追平时前端不再完全依赖手动刷新
  - 就业状态 filter 不再展示非预期的 `线索` 选项；未知/lead rows 只在“在职+已离职”同时选中时进入结果
  - “打开 LinkedIn”按钮仍按 `candidate.linkedinUrl` 渲染；修复后 API 首屏候选人已返回 canonical LinkedIn URL 和 profile detail
- 当前本地 backend/worker/frontend 已重启；HTTP 复核 `GET /api/jobs/87fa38d196a8/candidates?offset=0&limit=5&lightweight=true` 返回 `total_candidates=185`、`profile_fetch_progress=185/185`，首屏候选人有 canonical LinkedIn URL。
- 新增回归：
  - state 只有 search-seed、disk 有 roster
  - state 只有 roster、disk 有 search-seed
  - state/root aggregate 只有 `Agent` scoped seed、disk lane 子目录有 `Multimodal` scoped seed
  - asset population lightweight page 在 profile detail 未完成前仍保留 `linkedin_url`，供前端渲染“打开 LinkedIn”按钮

### Scoped-search / search-seed event-level response

- 继续收口 Wispr 之后暴露的更泛化边界：enrichment 入口能从 durable roster/search-seed artifacts hydrate，但如果某个 scoped-search provider worker 刚完成、尚未被本地 apply 成 snapshot artifact，后续 profile prefetch 仍看不到这批 usable LinkedIn URLs。
- 已把 search-seed worker 纳入 LinkedIn Stage 1 事件级 contract：
  - `SearchSeedAcquirer._execute_query_spec(...)` 创建的 search workers 现在带 `recovery_kind=search_seed_discovery`
  - `workflow_event_response` 的 `linkedin_stage_1` registry 覆盖 `search_planner` / `public_media_specialist` 的 `waiting_remote_search` workers
  - worker completion callback 现在支持 `search_seed_discovery`，会进入同一 inline incremental writer，而不是等整轮 aggregate 或 pre-retrieval refresh 才处理
- 新的本地顺序为：
  - 单个 search shard completed 后，先把 entries durable apply 到 `search_seed_discovery/entries.json` 与 `candidate_documents.json`
  - apply 后立即触发 `_queue_background_profile_prefetch_from_search_seed_snapshot(...)`
  - 如果同类 search shard 仍在 queued/running，full candidate artifact / PG materialize 返回 `deferred`，不阻塞 profile prefetch；同类 shard drain 后再由 shared writer budget 追平
- 这不是只服务 live roster 的修复：同一 contract 覆盖 scoped-search 多 shard query，例如 `OpenAI Agent + Multimodal` 这类多个 query worker 乱序完成的场景。之前的 disk hydration 负责 enrichment 入口合并已落盘 lane；本轮补的是“单 shard 完成本地 apply/prefetch”的上游事件入口。
- 新增回归：
  - `search_seed_discovery` remote-wait worker 可归入 `linkedin_stage_1` registry
  - search-seed 单 shard completed 时会先 durable apply 并触发 profile prefetch；当另一个 search shard 仍在运行时，full materialize 不会被调用，而是 deferred

### Anthropic full-reuse candidate board loading repair

- 排查 query/job `b992f9df009c`（`04/27 10:46`，`帮我找Anthropic做Pre-training的人`）自动跳到候选人看板后一直显示“候选人看板加载中”的原因：
  - job 已是 `completed`，但 job summary 和 PG `job_result_view` 都保留旧 contract：`candidate_source.source_kind=sqlite_store`、`view_kind=ranked_results`、`snapshot_id/source_path` 为空
  - PG `job_results` 对该 job 为 `0` 行，因此 results API 选择 ranked-results 后返回 `total_candidates=0`
  - Anthropic authoritative organization asset registry 实际可用：`snapshot_id=20260416T225318`、`candidate_count=3455`、`profile_detail_count=3389`、`profile_completion_backlog_count=57`
- 修复路径：
  - results resolver 现在会识别 retired/不完整 candidate source（例如旧 `sqlite_store`）且请求语义是 full asset/reuse 时，从 authoritative `organization_asset_registry` 恢复成 `company_snapshot` candidate source
  - 如果 persisted `job_result_view` 已是 `asset_population/company_snapshot`，它会覆盖旧 summary source；如果 result view 本身也是旧 `sqlite_store/ranked_results`，读取时会做一次 bounded PG repair，把该 job 的 result view 持久化为 `company_snapshot/asset_population`
  - 对缺少 paginated `normalized_artifacts/manifest.json/pages` 的旧 snapshot，summary path 不再为了 `profile_fetch_progress` 读取全量 materialized JSON 并对所有候选人 derive facets，而是用 `artifact_summary` 的 profile 计数字段构造轻量进度
  - 新增 monolithic `materialized_candidate_documents.json` window reader；当 manifest/pages 缺失时，候选人首屏可直接切片 monolithic artifact，不再构造全量 `Candidate` 对象
- 已对真实 job 执行修复并重启本地 backend：
  - PG `job_result_view(b992f9df009c)` 已修为 `source_kind=company_snapshot`、`view_kind=asset_population`、`snapshot_id=20260416T225318`
  - HTTP 复核：`/api/jobs/b992f9df009c/dashboard` 返回 `asset_population.available=true`、`candidate_count=3455`、`candidate_source_kind=company_snapshot`
  - HTTP 复核：`/api/jobs/b992f9df009c/candidates?offset=0&limit=50&lightweight=true` 返回 `result_mode=asset_population`、`returned_count=50`、`total_candidates=3455`
  - 停止了一个上周五遗留的 hosted smoke Python 进程；该进程不是当前 backend/frontend/worker，但持续占 CPU 并影响本地联调延迟
- 维护审计状态：
  - 已确认当前 20 个 authoritative organization snapshot 都有 paginated `manifest.json/pages`；PG `legacy job_result_view` 为 `0`
  - 新增 `repair-paginated-candidate-artifacts` 维护 CLI，可从已有 `materialized_candidate_documents.json` 原地派生 missing `manifest/pages/candidate shards`，不提交 provider、不改变 authoritative registry
  - 全历史 dry-run 发现 138 个 view 缺 paginated artifacts，主要是历史 Humans& / Thinking Machines Lab / OpenAI / Google 等非 authoritative row；按当前联调优先级先不修大公司历史快照，后续分批维护
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/results_store.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'legacy_sqlite_candidate_source or result_view_without_summary_candidate_source or dashboard_is_summary_only or auto_materialize_snapshot_candidate_documents'` -> `4 passed`

### OpenAI candidate board profile registry/materialization drift repair

- 排查 query `b12f28496204`（`2026-04-27T02:17:33Z`，OpenAI Reasoning）里 `候选人同步 784/784` 但 `LinkedIn 494/505` 的原因：
  - 原始口径差异是 candidate board rows 与 LinkedIn profile registry URL rows 不同，但 `AJ Sakher` 暴露了更深的 registry/materialization drift
  - `AJ Sakher` 的 raw Harvest profile 已存在于 `runtime/company_assets/openai/20260423T165904/harvest_profiles/36af4eabf9797876.json`，包含 canonical URL `https://www.linkedin.com/in/aj-sakher`、Seattle 地区、完整经历/教育
  - PG `linkedin_profile_registry` 缺少 raw ACw URL 与 canonical slug 的 fetched/alias row，后续 artifact rebuild 依赖 registry 解析 timeline，导致 page/manifest 回退到 missing-profile shard
- 修复路径：
  - `build_company_candidate_artifacts(...)` 在 materialize artifact view 前会对当前 snapshot 的 `harvest_profiles` 做 scoped `backfill_linkedin_profile_registry(...)`，并把结果写入 artifact summary / sync status
  - `SnapshotMaterializer.apply_harvest_profile_workers_to_snapshot(...)` 成功从 registry 或 snapshot cache 读到 completed profile payload 后，会补写 `mark_linkedin_profile_registry_fetched(...)` 与 alias metadata，避免 provider event ingest 后留下 raw/registry drift
  - 新增 regression 覆盖：candidate URL 是 Harvest search 返回的 `ACw...` URL，raw profile payload 给出 canonical slug；即使 registry 预先为空，artifact materialization 也会先 backfill registry 并输出完整 profile detail
- 已对真实 OpenAI snapshot 执行修复：
  - `PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli build-company-candidate-artifacts --company OpenAI --snapshot-id 20260423T165904`
  - scoped registry backfill 处理 `452` 个 raw profile 文件，`452 fetched`、`0 errors`
  - 重建后 canonical artifact：`candidate_count=784`、`profile_detail_count=784`、`profile_completion_backlog_count=0`、`missing_linkedin_count=0`
  - API 复核：`AJ Sakher` 返回 canonical LinkedIn URL、Seattle 地区、headline、8 条工作经历、1 条教育经历；candidate page `profile_fetch_progress=784/784`、`missing_registry_url_count=0`
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/snapshot_materializer.py tests/test_candidate_artifacts.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_candidate_artifacts.py -k 'backfills_snapshot_profile_registry_before_materializing'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_profile_registry_backfill.py tests/test_candidate_artifacts.py -k 'profile_registry or backfill or foreground_fast'` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'harvest_profile_batch or background_harvest_prefetch or snapshot_materializer_candidate_delta'` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'candidate_page_includes_profile_timeline_preview or profile_registry_raw_path or recover_profile_top_education or search_seed_preview_requires_profile_completion or sparse_harvest_profile_detail_requires_profile_completion'` -> `5 passed`
  - `tests/test_results_api.py -k 'profile_fetch_progress or materialized_candidate or needs_profile_completion'` 没有匹配测试，pytest 返回 `48 deselected`

### LinkedIn Stage 1 webhook edge cases and adaptive profile batching

- 继续检查 Lovable / Harvest profile pipeline 的响应不及时问题，当前结论分为两层：
  - Lovable 这批 tail profile actors 已全部被本地消费，`harvest_profiles` raw profile-like 文件已推进到 `1148`，18 个 `harvest_profile_batch_*.queue_summary.json` 全部 `completed`，job `80efbad6aaec` 没有残留 recoverable worker
  - PG/control-plane 当前 `candidate_count_for_company('Lovable')=1155`、`evidence_count_for_company('Lovable')=2328`，与 full `candidate_documents.json` / evidence count 对齐
  - `candidate_documents.linkedin_stage_1=173` 是 search-seed/stage snapshot 的候选人数口径，不是已取回 LinkedIn profile payload 的数量；当前 raw profile payload 已明显超过 200
  - 系统层面仍要求 hosted/local runtime 在提交新 Harvest actor 前配置 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 和 webhook shared secret 才能使用 provider push；未配置 webhook 的历史本地 CLI run 不会被 Apify retroactively 推送，新提交的本地 profile actor run 已有 local long-poll watcher 兜底
  - Apify 后台 `2026-04-27 03:58` 两个 `72` result run 和 `04:13` 四个 `36` result run 已匹配到 Lovable snapshot，是本地 runtime/recovery drain 继续提交的 Lovable tail；`04:41` / `04:50` 两个 `1` result run 是此前 Andrew Ng one-profile webhook smoke
- 补充验证和回归覆盖：
  - Apify `ACTOR.RUN.FAILED` 事件会和 `SUCCEEDED` 事件一样唤醒匹配的 LinkedIn Stage 1 remote-wait worker
  - provider webhook path 仍 quick-ack，job-scoped recovery 禁止内联跑 post-completion reconcile / housekeeping，避免 provider webhook 30s 窗口内阻塞
  - mixed-success / unresolved URL retry 仍由 profile registry 粒度控制，已 fetched URL 不进入后续 retry payload
- 调整 background profile prefetch 的 live batch sizing：
  - 原先 priority prefetch live cap 为 `75`，再经 balanced chunk 后容易形成 Lovable 上看到的固定 `73` 左右批次
  - 现在 profile-search/former-search live lane 复用 adaptive live fetch window，默认约 `45-55` URLs/batch，以更快拿到第一批结果并进入 local apply
  - 同时提交的新 worker 仍受 `harvest_profile_batch_submit_global_inflight` 控制，真实 provider actor in-flight 仍受 `harvest_profile_actor_global_inflight` / `SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT` 控制，默认保持在 4 左右，避免触发 Harvest/Apify too-many-requests
- 继续记录 materialization/PG 优化方向，但本轮未贸然改变写入 contract：
  - 当前 profile event path 已保证 next submit 不等待 full materialize
  - 本轮已加 changed-candidate lightweight control-plane upsert：当 profile batch delta 已写入 `candidate_documents` 但同类 worker 仍未 drain 时，先按 changed candidate IDs 更新 candidates/evidence，让看板更快看到已到货 profile detail
  - Harvest background snapshot materialization 默认使用 foreground/serving-fast artifact build profile；full hot-cache/compatibility refresh 保留给后续 maintenance/repair，而不是 provider event path
  - full snapshot materialize/PG 写入仍有优化空间：actor/apply/PG/artifact lag metrics 反推 batch window，并继续把 full rebuild 收敛为 repair/maintenance path
- 无 webhook 本地 run 的完成发现也做了 bounded fallback：
  - 如果未配置 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL`，新提交的 Harvest profile actor 会启动 local long-poll watcher
  - watcher 只在观察到 provider terminal status 后注入同一 `remote_provider_event`，watch window 到期只停止本地等待，不会把 actor 标记为 failed
  - 如果已配置 Apify webhook，则不启用本地 watcher，避免重复唤醒
- Apify webhook 配置口径：
  - `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 必须是 Apify 能访问到的公开 backend URL，例如 tunnel 或 hosted API
  - `providers.local.json` 里的 Harvest `api_token` 确实是调用 Apify API 的 token，可用于 submit actor run、给 run 附带 ad-hoc webhook，或查询 `GET /v2/acts/:actorId/webhooks`
  - `SOURCING_PROVIDER_WEBHOOK_TOKEN` 是本系统校验 inbound webhook 的共享密钥，不是 Apify API token；`APIFY_WEBHOOK_TOKEN` 只是兼容别名，避免把 Apify API token 暴露给 inbound callback
  - 由于短期试运行允许复用现有 Apify API token，本轮已把 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN` 默认置为启用：submit path 会把 Harvest actor `api_token` 写入 ad-hoc webhook header，backend webhook endpoint 也会接受同一 token；可用 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=0` 关闭，长期成熟后仍建议切回独立 shared secret
  - 只读 probe 已验证三个 Harvest actor 的 `GET /v2/acts/:actorId/webhooks` 可访问，当前 persistent actor webhook 列表均为 `0`；现有实现使用 run-scoped ad-hoc webhook，不依赖预创建 persistent webhook
  - 已把 `/runtime/secrets/temporary_apify_webhook_token` 中的临时 Apify token 写入 `runtime/secrets/providers.local.json` 的三个 Harvest actor 配置，并在写入前保留 `runtime/secrets/providers.local.backup_before_apify_webhook_20260426T202500Z.json`
  - 新增 `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`，记录 ECS 稳定域名、本地 tunnel、ad-hoc webhook 参数、token 口径、connectivity probe、one-profile round-trip smoke 和排障
  - 新增 `scripts/apify_webhook_roundtrip_smoke.py`，作为显式 opt-in live smoke；默认禁用 local watcher，要求公开 webhook URL 和 `--i-understand-this-submits-live-run`
  - 已在 ECS Nginx 增加 local-dev relay：`https://api.111874.xyz/local-dev/providers/apify/webhook -> 127.0.0.1:18765/api/providers/apify/webhook`，配合 `scripts/ecs_webhook_reverse_tunnel.py` 让本地 backend 使用稳定公开 callback URL 做 webhook smoke
  - ECS relay live smoke run `SeuvL6YsW48M2t1ML` / dataset `09ivTJCt5FhZHgZaP` 的 Apify ad-hoc dispatch `4hW8x91WACMHAROc2` 已在 Apify 侧显示 `SUCCEEDED`；该 one-profile actor 约 2 秒完成，本地 polling 先完成 worker，暴露出 webhook 到达时只扫描 recoverable worker 会丢审计事件的竞态
  - 已修复该竞态：`handle_remote_provider_event` 先唤醒 recoverable remote-wait worker；若同 `run_id` / `dataset_id` 的 known worker 已完成，则记录 `remote_provider_event: received_late`，不重启 recovery、不重放 dataset ingest
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/enrichment.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/storage.py src/sourcing_agent/runtime_tuning.py src/sourcing_agent/remote_provider_events.py tests/test_pipeline.py tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_runtime_tuning.py tests/test_remote_provider_events.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_enrichment.py -k 'remote_provider_event or webhook or profile_search_prefetch or balanced_live_batches or bounded_parallel_live_batches or global_inflight_budget'` -> `10 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_runtime_tuning.py -k 'harvest_profile_batch or background_harvest_prefetch or company_employees or global_inflight_budget or code_22 or mixed_success or webhook or wait_for_finish or profile_search_prefetch'` -> `23 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_pipeline.py -k 'remote_provider_event or webhook or remote_event_followup or skip_post_completion_reconcile or skip_post_recovery_housekeeping'` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py tests/test_pipeline.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py -k 'remote_provider_event or webhook or wait_for_finish or mixed_success or code_22 or local_event_watcher or profile_search_prefetch or background_prefetch or snapshot_materializer_candidate_delta or foreground_fast_artifact_profile'` -> `16 passed`
  - `git diff --check -- src/sourcing_agent/enrichment.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/orchestrator.py src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/storage.py tests/test_enrichment.py tests/test_pipeline.py tests/test_remote_provider_events.py PROGRESS.md docs/NEXT_TODO.md docs/archive/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/api.py src/sourcing_agent/harvest_connectors.py tests/test_harvest_connectors.py tests/test_remote_provider_events.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py tests/test_remote_provider_events.py -k 'ad_hoc_webhook or api_token or provider_webhook or webhook_endpoint'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/workflow_event_response.py src/sourcing_agent/remote_provider_events.py scripts/apify_webhook_roundtrip_smoke.py tests/test_remote_provider_events.py tests/test_workflow_event_response.py tests/test_harvest_connectors.py tests/test_pipeline.py tests/test_results_api.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_workflow_event_response.py -k 'remote_provider_event or remote_event_lane or webhook'` -> `11 passed`
  - 真实 PG 路径复核：用 `run_id=SeuvL6YsW48M2t1ML` / `dataset_id=09ivTJCt5FhZHgZaP` 找到 completed worker `231`，重放 provider event 后返回 `reason=matching_remote_provider_workers_not_recoverable` 并写入 `remote_provider_event: received_late`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_remote_provider_events.py tests/test_harvest_connectors.py tests/test_pipeline.py tests/test_results_api.py tests/test_target_candidate_public_web.py tests/test_workflow_event_response.py -k 'webhook or remote_event or target_candidate_public_web or public_web_api or promotion'` -> `17 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'agent_worker or target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'` -> `2 passed`
  - `cd frontend-demo && npm run build` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `git diff --check -- <touched event/webhook/Public Web/docs files>` -> passed
- 本地前后端已重启用于后续人工联调：
  - backend: `http://localhost:8765` API OK；`GET /api/target-candidates/public-web-search?limit=5` 返回 `status=ok`
  - frontend: `http://127.0.0.1:4173` root OK
  - worker daemon: running，当前 recoverable worker count 为 `0`
  - 当前 backend/worker 已带 `SOURCING_APIFY_WEBHOOK_URL=https://api.111874.xyz/local-dev/providers/apify/webhook` 与 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1` 启动；`scripts/ecs_webhook_reverse_tunnel.py` 仍在运行
  - ECS local-dev relay connectivity probe 返回 HTTP `202` / `mode=async_recovery`；下一次本地 Harvest run 会尝试走 Apify provider webhook push，若 tunnel 断开则需要重新启动 tunnel 或回退到 local watcher/recovery
  - 重启后的本地 webhook endpoint 用 `sync=1` 重放 `SeuvL6YsW48M2t1ML` completion event，返回 `reason=matching_remote_provider_workers_not_recoverable` / `recovery_count=0`，证明 late-event path 已在当前服务进程生效

### LinkedIn Stage 1 remote completion wakeup

- 继续压缩 Lovable 暴露的 live tail latency，但保持上一段修复的核心边界：不把 `apply/materialize` 重新耦合回 next submit path。
- 新增 `workflow_event_response` 抽象，把远端等待 worker 的事件响应 lane 做成 registry，而不是在 orchestrator 中继续写 provider/company 特例：
  - 当前启用 lane 为 `linkedin_stage_1`
  - 覆盖 `acquisition_specialist` / `enrichment_specialist`
  - 覆盖 `harvest_company_employees` / `harvest_profile_batch`
  - 识别 `waiting_remote_harvest` / `waiting_remote_search`
- `run_worker_recovery_once` 现在在 shared recovery tick 后会做 bounded `remote_event_followup`：
  - 默认 `WORKFLOW_REMOTE_EVENT_FOLLOWUP_ENABLED=true`
  - 默认 `WORKFLOW_REMOTE_EVENT_FOLLOWUP_ROUNDS=1`
  - 发现 LinkedIn Stage 1 remote-wait worker 后，立即对对应 job 运行 job-scoped follow-up daemon
  - follow-up summary 合并回 `daemon`，并在 response 暴露 `remote_event_followup` 的 scope、target jobs/workers、claimed/executed counts
  - explicit job recovery 已有自己的 follow-up rounds，因此不会重复走 shared remote-event follow-up
- 同步修复一个 job-scoped recovery 语义回归：当 `workflow_resume_explicit_job=false` 且 `workflow_stale_scope_job_id` 指向当前 job 时，0 秒 stale gate 仍可恢复 scoped queued job，同时继续忽略 unrelated queued jobs。
- 新增架构文档 `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`，沉淀可复用 contract：
  - `remote_wait`
  - `completion_discovery`
  - `local_event_apply`
  - `downstream_materialize`
  - 其他 workflow 后续应通过 registry 和 regression 接入，不应复制 Lovable 专用补丁。
- 本轮复核 Lovable artifacts：
  - `candidate_documents.json=1155`
  - `candidate_documents.linkedin_stage_1.json=173`
  - `harvest_company_employees_visible.json=1008`
  - `harvest_company_employees_merged.json=1009`
  - `harvest_profiles/*.json=1220`，其中 raw profile-like payloads `1148`
  - full normalized candidate artifacts `1155`
  - strict roster candidate artifacts `818`
  - tail batch `harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json` 已为 `status=completed`、`requested_url_count=73`、`run_id=LLf6slcMDtP9WlKZf`、`dataset_id=Q95IGLwSSd9EEjoey`
- 其他 workflow 的事件级响应 backlog 已记录在 `docs/NEXT_TODO.md`：Target-candidate Public Web Search、Search seed discovery、segmented company employees roster、Public media/exploration workers、runtime lag metrics。

### Harvest profile completion event orchestration

- 继续收口 Lovable 暴露的 `linkedin-profile-scraper` 编排问题，明确这不是 roster 缺失，而是 provider completed 后本地消费、next batch submit、apply/materialize 三段耦合过深。
- 已把 Harvest profile completed worker 的本地处理收敛到统一 completion event contract：
  - worker completion callback、running workflow pre-retrieval refresh、running inline reconcile、completed workflow reconcile 都先触发 `provider completed -> local ingest visible -> next profile prefetch submit`，再执行 snapshot delta apply/materialize
  - running pre-retrieval refresh 不再保留 `apply current batch -> submit next batch -> sync` 的旧顺序分叉
  - 当 next profile worker 已 queued/deferred/active 时，当前 batch 只做 Harvest profile delta apply 与 worker-level `inline_incremental_ingest` marker，完整 candidate artifact rebuild 延后到同类 worker drain 后
  - pre-retrieval refresh 现在也会给已消费的 Harvest profile worker 写 `inline_incremental_ingest` marker，避免后续 tail worker callback 再次把同一 completed worker 纳入 micro-batch
- provider retry 行为补齐两个回归点：
  - Harvest dataset fetch 将 `Too many queued requests (code_22)` 识别为 retryable provider queue backpressure；HTTP 400 不直接短路 marker 判断
  - mixed-success profile batch 只对 unresolved URLs 做后续 retry/fallback，已成功解析的 URL 不进入后续 provider payload
- 复核 Lovable 真实 artifacts 仍与 handoff 一致：
  - job `80efbad6aaec` 仍为 `completed`，preview 为 `preview_ready`
  - `candidate_documents.json=1155`、`candidate_documents.linkedin_stage_1.json=173`
  - `harvest_company_employees_visible.json=1008`、`harvest_company_employees_merged.json=1009`
  - `harvest_profiles/*.json=1220`、raw profile-like payloads `1148`、full normalized candidate artifacts `1155`、strict roster candidate artifacts `818`
  - tail batch `harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json` 已记录 `status=completed`、`requested_url_count=73`、`run_id=LLf6slcMDtP9WlKZf`、`dataset_id=Q95IGLwSSd9EEjoey`
- 仍保留的真实边界：这次修的是“本地一旦观察到 completed worker/dataset 后的事件级响应和顺序解耦”；provider 侧完成事件的外部发现仍依赖现有 poll/recovery/worker callback，并未新增 Harvest webhook 或 provider 推送通道。

### Harvest runtime follow-up and documentation framing cleanup

- 补上这轮 company-roster / Harvest runtime 修复的 closeout 记录：
  - `orchestrator.py` 新增 completed-workflow 后台结果发现逻辑；即使没有 stale/recoverable worker，daemon 也会重新发现“workflow 已 completed，但仍有已完成 background worker 尚未被消费”的 case
  - `storage.py` 的 PG `replace_company_data(...)` 已切到 bulk delete + bulk upsert，候选人/evidence 同步不再走逐行写入；这解决了 Lovable 这类大 snapshot 在 reconcile/materialization 阶段看起来像“卡死”的主要控制面热点
  - Harvest 远端状态轮询与 dataset 下载新增 fast-yield runtime tuning：background `harvest_profile_batch` recovery 默认使用更保守的 HTTP timeout / fetch attempts，避免单个慢 provider 请求长时间占住一整个 recovery cycle
- 这轮 live 诊断确认了两个重要事实：
  - Lovable 的 `58 + 4 x 73` profile batch 不是整批重跑，而是互不重叠的 deferred tail drain
  - 当前更大的剩余问题不是重复提交，而是 remote completed run 仍主要靠 daemon/recovery 轮询节奏发现，尚不是事件级同步
- 文档整理前也把“为什么用户会感觉整条链路慢”拆得更明确：
  - `harvestapi/linkedin-profile-scraper` 在 provider 侧完成后，本地系统不一定会立刻收到并消费这个完成事件；常见情况仍是等下一轮 daemon/recovery 扫描，或等当前慢 HTTP poll/dataset download 返回
  - 结果就是 provider 已经完成，但本地 `profile_fetch_progress`、候选人详情物化、下一轮 profile batch submit 仍可能停在旧状态，形成“上游已到货、下游还没开工”的空转时间
  - 当时 orchestration 仍存在上游 submit 与下游 apply/materialize 耦合过深的问题：有些路径上，新的 profile-scraper batch submit 事实上要等前一批 completed batch 被 poll/download/apply/materialize 之后才继续推进，而不是在 provider completed 且 budget/lease 允许时就立即排下一批
  - medium former/search-seed 清单尤其受这个问题影响：理想状态应是“前一批一完成就继续排下一批 actor，同时把已完成结果异步交给下游 apply/materialize”；而不是把 submit、ingest、materialize 串成单条长链
- 最近一次 live 观察到的 Lovable profile registry 已推进到 `504 fetched / 73 queued`；说明 bulk replace + queued-run resume 修复已经让 tail 继续前进，但最后一段 completion detection 仍值得继续压缩
- Lovable 继续排查时，优先看这些现成落点：
  - job files：`runtime/jobs/80efbad6aaec.json`（当前 `status=completed`）与 `runtime/jobs/80efbad6aaec.preview.json`（当前 `status=preview_ready`）
  - snapshot root：`runtime/company_assets/lovable/20260426T193540/`
  - current/full roster provider outputs：`runtime/company_assets/lovable/20260426T193540/harvest_company_employees/`
    - `harvest_company_employees_visible.json` 当前 `1008` rows
    - `harvest_company_employees_merged.json` 当前 `1009` rows
  - former/search-seed summary：`runtime/company_assets/lovable/20260426T193540/search_seed_discovery/summary.json`（当前 `entry_count=173`）
  - main candidate snapshot：`runtime/company_assets/lovable/20260426T193540/candidate_documents.json`（当前 `1155` candidates）
  - stage-1-only snapshot：`runtime/company_assets/lovable/20260426T193540/candidate_documents.linkedin_stage_1.json`（当前 `173` candidates）
  - fetched profile/raw batch artifacts：`runtime/company_assets/lovable/20260426T193540/harvest_profiles/`（当前 `1220` 个 `*.json`，其中 raw profile-like payloads `1148`，包含 raw profile 与 batch queue/run/dataset artifacts）
  - normalized candidate artifacts：`runtime/company_assets/lovable/20260426T193540/normalized_artifacts/candidates/`（当前 full candidate JSON `1155` 个）；`strict_roster_only/candidates/` 是 filtered view（当前 `818` 个）
  - 这些数量说明当前 tail 已经从 provider/raw 层基本追平；后续重点是确认 board/API 默认读取 full candidate artifacts / PG candidate rows，而不是误把 strict filtered view 当作完整物化口径
- 当前还能直接接续的 tail batch 文件之一：
  - `runtime/company_assets/lovable/20260426T193540/harvest_profiles/harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json`
  - 最近一次检查该文件记录 `requested_url_count=73`、`status=completed`、`run_id=LLf6slcMDtP9WlKZf`、`dataset_id=Q95IGLwSSd9EEjoey`
- 调试期间临时启动过一次 live recovery 进程做验证；该进程在文档整理前已显式停止，本轮没有遗留仍在运行的恢复进程
- 本轮同时收口 Markdown 入口文档和活跃 tracker 的表述：
  - 新 session 默认应把仓库描述为招聘自动化、公开资料补全、候选人研究、provider 队列治理与后台恢复产品化
  - 减少与当前任务无关的高敏感表述，保留必须精确的 provider 名称、配置键、CLI flag 和 API contract
- 下一步仍值得继续的 runtime 工作：
  - 已完成本地 completion-event 编排收口：`submit next actor`、`ingest completed dataset`、`apply/materialize snapshot delta` 不再在 Harvest profile completed worker 消费路径上默认串行阻塞
  - 已补 `code_22` retryable backpressure 和 mixed-success batch 回归，证明只 retry failed/unresolved URL，不重跑整批
  - 后续如果继续压缩 tail latency，重点应转向 provider completion 的发现机制（poll cadence、callback handoff、可能的 webhook/long-poll），而不是再把 apply/materialize 放回上游 submit path
  - 在 bulk replace 稳住之后，再评估是否将 snapshot 同步继续收敛成真正的 delta materialization

## 2026-04-26 (Asia/Shanghai)

### Public Web target-candidate backend/frontend productization

- 完成目标候选人级 Public Web Search 的第一层后端产品化：
  - 新增 `target_candidate_public_web.py`，承接 target-candidate selected batch action、idempotency、run summary、DataForSEO-style checkpoint resume、person-level reuse asset upsert
  - 新增 PG-authoritative control-plane tables：`target_candidate_public_web_batches`、`target_candidate_public_web_runs`、`person_public_web_assets`
  - 新增 API：`POST /api/target-candidates/public-web-search`、`GET /api/target-candidates/public-web-search`
  - API trigger 只做幂等排队和 recoverable worker 创建，不在 HTTP request 内跑 live DataForSEO/fetch/LLM
  - 后台 worker 复用 `exploration_specialist` lane，metadata `recovery_kind=target_candidate_public_web_search`，初始 checkpoint 为 `waiting_remote_search`
  - per-candidate run 的 `search_checkpoint_json` 保存 query manifest、provider task id、poll count、query results、classified links 和 errors；未 ready 时后续 recovery takeover 会 poll/fetch 既有 task，不重复 submit
  - run 完成且存在 normalized LinkedIn URL key 时 upsert `person_public_web_assets`，对齐 LinkedIn profile 的 person-level reuse 模型
- 另一个 session 已继续接入第一层目标候选人页 UI，并在本 session 复核：
  - `TargetCandidatesPanel` 增加 checkbox selection、`Public Web Search` 批量按钮、状态刷新按钮、5s running-run polling、卡片级 run status/metrics/primary links 展示
  - 前端通过 `GET/POST /api/target-candidates/public-web-search` 消费后端 contract，不读 runtime 文件，不把 Public Web 结果写入 `primary_email`
  - `contracts/frontend_api_contract.ts` / `.schema.json` / `frontend_api_adapter.ts` 增加 Public Web Search typed contract
  - 该前端切片当时已从 compact run summary 继续接上 Public Web detail section；本轮后续已补齐 email/link promotion、Web Search promoted-only export 和对应 browser E2E
- 本轮继续完成 first-class Public Web signals / detail API 的后端切片：
  - 新增 PG-authoritative `person_public_web_signals`，并注册到 control-plane/live PG table registry；SQLite 仅作为既有 bootstrap/shadow，不作为 hosted truth source
  - run 完成后会按 `signals.json` 物化 email candidates 与 profile/public links，保存 source URL/domain/family、identity label、confidence、publishability、suppression reason、artifact refs、model provider/version
  - 新增 `GET /api/target-candidates/{record_id}/public-web-search`，按 target candidate record 返回 latest run、person asset summary、email candidates、profile links、grouped signals 和 evidence links
  - detail API 只返回 model-safe signal/evidence payload，不返回 `search_checkpoint`、raw HTML/PDF/search payload 或 raw document paths
  - 仍不把 Public Web email 写入 `target_candidates.primary_email`；promotion writer/API/UI 仍是后续独立工作
- 在进入 detail/export/promotion 前，补上 Public Web 质量评测层：
  - 新增 `src/sourcing_agent/public_web_quality.py`，可扫描实验产出的 `signals.json`，评估邮箱和 X/Substack/GitHub/Google Scholar 等 profile/media links 的 source URL、source family、identity label、publishability、promotion status 和 evidence presence
  - 新增 CLI：`evaluate-public-web-quality`，支持 `--experiment-dir`、`--output-dir`、`--summary-only`、`--fail-on-high-risk`，输出 JSON/CSV/Markdown 报告；其中 `--fail-on-high-risk` 用作高优先级质量门禁
  - Public Web 实验预算新增 `--max-ai-evidence-documents` / `PublicWebExperimentOptions.max_ai_evidence_documents`，默认 8；产品 API options 会按 1..20 归一化，便于正式接 UI 前跑更大候选人/query/fetch/LLM evidence 预算
  - `run_public_web_candidate_adjudication(...)` 的 AI result 现在记录发送给 LLM 的 email/link/fetched document/evidence slice 计数，方便复核预算是否真的生效
  - 用已有 replay 样本复核：4 candidates、13 signals、2 promotion-recommended emails，`cbfinn@cs.stanford.edu` 和 `dainves1@gmail.com` 均有 source URL，质量报告 0 issues
  - 用已有 live 10x10 social discovery 样本复核：10 candidates、378 profile link signals、0 trusted media links、648 medium issues；主要风险是 search-only X/Substack/GitHub/Scholar 结果多为 `unreviewed`、GitHub repo/deep link、X post/utility URL、Substack non-profile URL 或 same-name Scholar 噪声，证明正式 UI 不能把搜索结果直接当 confirmed profile
  - 用已有 live fetch+AI Scholar/academic summary 样本复核：3 candidates、94 signals、4 email candidates、21 trusted media links、180 medium issues；质量层发现旧 artifact 里仍有模型非规范 link type（如 `scholar_profile`/`github_profile`），评测器现在会归一化并标 `non_canonical_profile_link_type`
  - 跑了一轮真实更大 live quality pass：`live-public-web-quality-11x14-fetch12-ai16`，11 candidates、154 queries、880 entry links、131 fetched docs、12 email candidates、2 promotion-recommended emails；质量报告显示 94 trusted media links，但 12 fetches/candidate 的实际 fetch composition 被 discovered GitHub links 插队污染（118/131 fetched docs 是 GitHub，0 X/Substack），因此先修 fetch queue diversity，再决定是否升到 16 fetches
  - 修复 discovered GitHub front-insert：GitHub links 仍会作为 evidence candidates 保留，但不再前插抢占 Scholar/X/Substack/homepage/resume 的 fetch slots；discovered resume/CV 仍保持高优先级
  - 修复后跑 3-candidate live sanity：`live-public-web-quality-diversity-fix-3x14-fetch12-ai16`，33 fetched docs 分布为 homepage 5、resume 10、Scholar 4、GitHub 7、X 2、Substack 4、publication 3、academic profile 1；证明 12 fetches/candidate 在队列公平后可以覆盖 X/GitHub/Substack/Scholar 等主要渠道，下一轮无需先升到 16
  - 明确 LLM adjudication payload 不只依赖 fetched docs：现在会给候选人级 LLM 发送 source-balanced `entry_links` 与 `search_evidence`（DataForSEO URL/title/snippet/query/rank/provider context），避免 X/Substack 等难 fetch 平台只因没有 fetched document 就完全无法被模型判断
  - 重跑 3-candidate live LLM 对照：`live-public-web-quality-search-evidence-3x14-fetch12-ai16`。耗时约 20 分钟，其中 DataForSEO batch queue 约 5 分 15 秒，后续候选人按顺序 fetch/LLM 分析；第三个候选人因顺序 fetch 慢站点拖到约 20 分钟，总体证明 live 实验 CLI 慢主要来自 provider queue + sequential fetch/analysis，不是单次 LLM
  - `search_evidence` 后 X/Substack 判断分布改善：旧 run X=`confirmed 1 / unreviewed 25`、Substack=`unreviewed 44`；新 run X=`confirmed 1 / likely_same_person 8 / unreviewed 16`，Substack=`likely_same_person 6 / needs_review 1 / ambiguous_identity 8 / unreviewed 15`
  - 新质量报告显示 3 candidates、248 signals、8 email candidates、3 promotion-recommended emails、39 trusted media links、1 high-priority quality issue；该问题是 promotion-recommended email 缺 trusted identity，需要在 detail/promotion 前继续作为质量 gate
- 本轮继续补上 Public Web URL shape 与效率/前端 detail 收口：
  - 新增统一 `public_web_link_shape_warnings(...)` / `is_clean_profile_link(...)`，对 GitHub repo/deep link、X status/search/utility URL、Substack post/home feed/deep link、非 profile Scholar URL 产出显式 warning
  - LLM search evidence payload 现在携带 `link_shape_warnings` / `clean_profile_link`，prompt 明确这些 deep links 只能作为 evidence，不能当 clean profile
  - `summary.primary_links` 和 `person_public_web_signals.publishable` 现在同时要求 identity trusted 与 clean URL shape；detail API 会返回 top-level `link_shape_warnings` / `clean_profile_link`
  - 质量评测层复用同一 URL-shape helper；对既有 `live-public-web-quality-search-evidence-3x14-fetch12-ai16` 重新评估后，trusted media link 从旧口径 39 收紧为 9，X/Substack deep link 继续保留为 evidence/review signal
  - 实验/worker 分析支持 `max_concurrent_fetches_per_candidate`；实验 CLI 支持 `max_concurrent_candidate_analyses`，用于候选人级 LLM adjudication 并发。默认保持小并发（fetch=4、candidate analysis=2）以控制 provider/LLM 压力
  - 目标候选人页增加 Public Web detail section，按 record 调用 `GET /api/target-candidates/{record_id}/public-web-search`，展示 email candidates、profile/evidence links、identity label、publishability、suppression reason 和 URL shape warning chips；仍不提供 email promote 按钮
- 本轮继续完成 Public Web manual promotion 与专用导出包：
  - 新增 PG-authoritative `target_candidate_public_web_promotions`，保存 signal lineage、source URL/domain/family、identity/confidence、URL shape、operator、timestamp、previous/new value 和 promotion status
  - 新增 API：`GET/POST /api/target-candidates/{record_id}/public-web-promotions`；POST 默认只允许 publishable/clean signal promoted，email promotion 先写 promotion record，再更新 `target_candidates.primary_email`
  - Public Web detail API 现在会合并 latest promotion status，email/link 行可显示 `manually_promoted` / `manually_rejected`
  - 新增 `POST /api/target-candidates/public-web-export`，默认 `promoted_only`，导出 model-safe summary/signals/evidence links/promotions/manifest，并继续排除 raw HTML/PDF/search payload
  - 目标候选人页 detail section 增加 email/link promote/reject 控件；Public Web 专用导出按钮默认导出已选候选人，否则导出全量 target candidates 的人工确认公开信息
  - 新增 target-candidate Public Web browser E2E 脚本，覆盖 `/targets` selection -> trigger -> refresh/polling -> compact status card
  - 继续补一条 browser E2E，覆盖 completed Public Web detail 展开、人工确认邮箱和 clean profile link、promoted-only Public Web 导出下载；后端断言 promotion record 写入后 `primary_email` 才更新
  - 目标候选人页 toolbar/filter 做第一轮可用性优化：Public Web Search 与刷新状态合并到同一操作组，LinkedIn/Web Search 导出改为范围语义并用问号 tooltip 承载说明，新增关键词、跟进状态多选和 Public Web 状态多选筛选，隐藏容易误导的旧“查看历史记录”跳转按钮
- 本轮继续收口 Public Web 质量门禁、人工覆盖确认和导出模式产品控件：
  - `scripts/run_python_quality.sh all` 和 `pyproject.toml` 的 mypy target 现在显式纳入 `linkedin_url_normalization.py`、`public_web_quality.py`、`public_web_search.py`、`target_candidate_public_web.py` 及对应核心测试，避免 Public Web 新模块漂在默认门禁外
  - `run_pytest_matrix.py --mode changed` 对 Public Web 相关后端/测试/浏览器脚本路径会选中 `public-web-core`、`target-candidate-public-web-api`、`target-candidate-public-web-pg`，前端脚本改动也会拉起前端 contract/build suite
  - promotion API 支持对 non-publishable 或 dirty URL-shape signal 做人工覆盖确认，但必须传 `allow_unpublishable=true` 和 `override_reason`；无理由返回 `override_reason_required`，无效邮箱等 hard validation 仍不可覆盖
  - promotion detail、signal detail、promotion summary 和 `public_web_promotions.csv` 会保留 `override_reason`、原始 validation reason 和 `override_count`，覆盖路径仍先写 promotion record，再按 email promotion 更新 `primary_email`
  - 目标候选人页对需要覆盖的 email/link 显示 `覆盖确认邮箱/链接`，通过人工理由 prompt 写入 override metadata；默认 publishable/clean signal 仍走普通确认
  - Target-candidate Public Web Search 已接入 `workflow_event_response` 第一层 registry lane：shared recovery 发现 `waiting_remote_search` worker 后会做 job-scoped follow-up，不再完全依赖下一轮外层 daemon tick
  - 前端暂时隐藏尚未完全产品化的 `人工确认` / `含高置信` 导出模式控件，右侧导出容器只保留 `批量导出 LinkedIn Profile 信息` 和 `批量导出 Web Search 信息`
  - Web Search 导出按钮固定走 `promoted_and_publishable`：导出人工确认 signals 加 AI 判定 publishable 的未人工确认 signals；如果没有高置信个人主页/媒体链接，目标字段保持为空，仍排除 raw HTML/PDF/search payload
  - 浏览器 E2E 不再点击隐藏的 `含高置信` 控件；后端 API 测试已断言默认包不含未确认 GitHub link，而 `promoted_and_publishable` 包含 `ai_publishable_unpromoted` signal 并在 manifest 标记模式
- 本轮 closeout 文档已补齐新 session 恢复要点：
  - 本地 `8765` 上观察到的 `Public Web Search 接口暂不可用` 是旧后端进程导致的 404；新 session 应先重启当前分支 backend/worker 并验证 `GET /api/target-candidates/public-web-search?limit=5`
  - 当前本地 11 个 target candidates 尚未触发 Public Web Search，`未开始` 卡片状态是事实；Web Search 导出按钮里的 11 人只表示当前选择/筛选范围，不表示已有 11 人确认公开网络结果
  - 继续开发应先重启当前代码并小批量触发 2-3 个目标候选人的 Public Web Search，手动检查 detail、普通 promotion、override promotion、两种 Web Search export mode；代码层下一步再做 company-level API/CLI lane、scoped polling 和候选人卡片编辑/详情 UX
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/worker_daemon.py tests/test_target_candidate_public_web.py tests/test_control_plane_live_postgres.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web_api_queues_idempotent_runs'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_worker_recovery_daemon.py -k 'waiting_remote_search or recovery'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidates or public_web_api'`
  - `cd frontend-demo && npm run build`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback' tests/test_markdown_status.py`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m compileall -q src/sourcing_agent/storage.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py tests/test_target_candidate_public_web.py tests/test_results_api.py tests/test_control_plane_live_postgres.py`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py tests/test_target_candidate_public_web.py tests/test_results_api.py tests/test_control_plane_live_postgres.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'` -> `5 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed, 37 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `cd frontend-demo && npm run build`
  - inspected `runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates/candidates/01_replay-chelsea-finn/signals.json` to confirm persisted signal fields match real email/link payloads
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_quality.py tests/test_public_web_search.py -k 'public_web_quality or email_extraction_suppresses_pdf_title_artifacts or publication_multi_email_keeps_candidate_local_part_and_suppresses_coauthors or ai_adjudication_marks_weak_identity_social_link_ambiguous or sanitizer or ai_evidence_document_budget'`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates --output-dir runtime/public_web/quality/replay-fetch-homepage-email-4-candidates --summary-only`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-dataforseo-entry-discovery-10x10-social-links --output-dir runtime/public_web/quality/live-dataforseo-entry-discovery-10x10-social-links --summary-only`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-dataforseo-fetch-scholar-academic-summary-3x7 --output-dir runtime/public_web/quality/live-dataforseo-fetch-scholar-academic-summary-3x7 --summary-only`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m compileall -q src/sourcing_agent/public_web_search.py src/sourcing_agent/public_web_quality.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/public_web_search.py src/sourcing_agent/public_web_quality.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py -k 'public_web or target_candidate_public_web or url_shape or bounded_concurrency'` -> `40 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-public-web-quality-search-evidence-3x14-fetch12-ai16 --output-dir runtime/public_web/quality/live-public-web-quality-search-evidence-3x14-fetch12-ai16-shape-warnings --summary-only`
  - `cd frontend-demo && npm run build`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'` -> `2 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed, 37 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`
  - `git diff --check`
  - `node --check frontend-demo/scripts/run_target_public_web_promotion_export_e2e.mjs`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/target_candidate_public_web.py tests/test_target_candidate_public_web.py tests/test_results_api.py tests/test_control_plane_live_postgres.py tests/test_frontend_browser_e2e.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api or promotion'` -> `7 passed, 45 deselected`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'` -> `2 passed, 7 deselected`
  - `cd frontend-demo && npm run build`
  - `bash ./scripts/run_python_quality.sh all`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_regression_matrix.py` -> `13 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --changed-path src/sourcing_agent/public_web_search.py --dry-run`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web or public_web_api or promotion'` -> `3 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_linkedin_url_normalization.py` -> `43 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'` -> `3 passed, 37 deselected`
- 剩余产品化边界已同步到 `docs/archive/PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md`：
  - Company-level Public Web refresh 后续已落成 API/CLI-only v1；此处剩余为目标候选人页卡片编辑/备注/详情导航重做，以及更完整的人工手测/真实小批量 live run
- 新 session 交接文件：
  - `docs/archive/SESSION_HANDOFF_2026-04-26_PUBLIC_WEB.md`

### Harvest Mistral full-roster runtime root-cause hardening

- 复盘 `04/26 16:33 帮我找Mistral AI的全部成员` 暴露的根因：
  - 默认 small/unknown org full roster 预算是 `20 pages x 25 = 500`，原先 `company-employees` probe 只会把请求缩小到 provider 估算量，不会在 probe 发现 `1055 profiles total` 时从 500 扩到 1055
  - job 可以在 `profile_prefetch.status=queued`、`materialization_refresh_pending=true` 时进入候选人看板；这是 nonblocking board readiness 的设计，但 progress/summary 必须明确 profile hydration/materialization 仍在后台继续
  - background `harvest_profile_batch` 之前只有本地 submit worker 数，没有把 job-local active queued workers / registry queued URLs 当成强背压边界，容易在 recovery/reconcile 重复提交相近 batch，触发 Harvest `Too many queued requests (code_22)` 并放大成本
  - 新增 runtime tuning key 若不加入 `execution_preferences` 白名单，会被 `JobRequest` 归一化丢弃，造成“配置了限流但实际不生效”的同类治理缺口
- 已落地修复：
  - `HarvestCompanyEmployeesConnector` 的 probe sizing 现在会在估算总量高于默认请求时扩到 `min(estimated_total_count, provider_cap=2500)`，并同步放大 `takePages/maxItems/max_paid_items/timeout/charge cap`
  - `company-employees` summary / worker queue summary 现在记录 `requested_items_before_probe`、`effective_max_items`、`effective_take_pages`、`estimated_total_count`、`provider_cap_hit`、`requested_limit_would_truncate`、`partial_result` 和完整 `probe`
  - provider cap 命中时不再标成普通 completed：snapshot `stop_reason=provider_cap_reached`，summary `partial_result=true`
  - Harvest log parser 现在识别 `Max items limit reached: N`，用于审计“请求 cap 被打满”和 provider cap 的区别
  - `harvest_profile_batch_submit_global_inflight` 新增为默认 1 的全局提交背压；fast smoke 可升到 2，显式 execution preference / env 可覆盖
  - background profile prefetch 在 chunking 前过滤 registry 已经 `queued` 的 LinkedIn URL，只更新 source lineage，不再为这些 URL 再创建新 actor run
  - job-local active `harvest_profile_batch` worker 会消耗新 worker budget；超出预算的 URL 返回 `deferred_urls`，由后续 recovery/reconcile 继续排队
  - `harvest_profile_batch_submit_global_inflight` 及 Harvest/global/materialization in-flight key 已纳入 `execution_preferences` 白名单，避免 request normalization 静默丢配置
- 本轮继续把根因治理从进程内背压推进到 DB 级 actor/URL contract：
  - 新增 `runtime_provider_limiter_leases`，SQLite/PG 均支持 `acquire_runtime_provider_limiter_slot` / release；PG 使用 advisory transaction lock 保证同一 limiter key 下原子计数
  - 新增 `harvest_profile_actor_global_inflight`，默认 4，fast smoke 2，env `SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT` 可覆盖；profile-scraper actor lane 统一使用 limiter key `harvest_profile_scraper_actor`
  - background `harvest_profile_batch` 在提交前会先做 URL 级 registry lease claim；claim 失败只补 source lineage 和 `lease_contended_skip` event，不再重复 submit 同一 LinkedIn URL
  - background `harvest_profile_batch` 的 provider limiter lease 会写入 worker checkpoint；remote actor pending 时不释放，直到 recovery 完成/失败后释放，避免只限流本地 submit 而不控制真实 active actor
  - foreground `_fetch_harvest_profiles_for_urls`、company asset completion 的 live profile fetch、direct/segmented `company-employees` 均套入 DB 级 provider limiter；进程内 `runtime_inflight_slot` 仍保留为本进程二级保护
  - roster-heavy live profile fetch window 调保守：公司 roster 来源的大批量 URL 默认约 100-125/批，并发 1-2，避免 500 人清单被切成过多 60 人小 actor 同时排队
  - `parse_harvest_company_employee_rows` 会在 provider 只给 `publicIdentifier` 时提前构造 LinkedIn profile URL；候选人 metadata 也保留 `public_identifier/linkedin_url`，因此候选人看板可先显示“打开 LinkedIn”，不必等 profile detail materialization
  - job dashboard / candidate page 新增 `profile_fetch_progress`，按当前返回候选人的 LinkedIn URL 统计 `queued/fetched/failed_retryable/unrecoverable/missing/deferred`；前端候选人同步卡片展示 LinkedIn fetched/queued/retryable 概览
  - shared runtime tuning 继续扩展到 provider in-flight/backpressure 报告面：
    - `harvest_global_inflight_budget` 现在是 execution preference 白名单字段，可统一覆盖 Harvest 类 global in-flight budget，具体 lane 预算仍可用更细字段覆盖
    - 新增 `build_provider_backpressure_budget_report(...)`，统一输出 Harvest profile actor、profile batch submit、profile scrape、people search、company roster 的预算，以及 DB limiter active/wait/backlog/recommended action
    - scripted/simulate smoke per-case report 新增 `provider_backpressure`，aggregate summary 新增 provider limiter exhausted/backpressure rollup
  - workflow smoke benchmark 继续扩到同一报告口径：
    - per-case `workflow_benchmark` 现在同时包含 `search_returned_count`、`roster_returned_count`、`fetched_profile_count`、profile URL queued/total、board ready/nonempty、board total/first page returned 和 provider backpressure flags
    - aggregate `provider_case_report.workflow_benchmark.metrics` 会汇总 search/roster/profile/board-ready 数字，便于比较不同 strategy 的 workflow 行为，而不是只看 completed
- 复盘 `04/26 19:35 帮我找Lovable的全部成员` 暴露的新缺口并落地修复：
  - 实际 job `80efbad6aaec` 的 `linkedin-profile-search` 先返回 173 former seed，随后只提交了一个 58 URL profile-scraper batch；`company-employees` 之后返回 1008 visible entries，canonical `candidate_documents.json` 到 1155 candidates，但 profile registry 只有 `fetched=8 / queued=58`
  - root cause 1：`harvest_profile_batch` recovery 先看 registry，看到自己的 58 URL 已是 `queued` 后误判为“已有活跃队列”，没有用 worker checkpoint 的 `run_id=F8aV8...` / `dataset_id=atp1...` 继续 poll/download 已成功的 Apify dataset
  - root cause 2：Mistral 事故后把 background profile submit default 压到 1，导致 173 profile-search seed 这种中等批次也只排一个 actor；这属于把大 roster 成本背压错误套到 former/search-seed lane
  - root cause 3：provider 完成、dataset ingest、snapshot apply/materialize 三个阶段还没有完全拆开。即使 provider 侧 actor 已完成，本地也可能因为 recovery tick、慢 HTTP poll 或 apply/materialize 热点而延后下一轮 submit 与用户可见进度更新
  - 已修复：有同 worker checkpoint `run_id` 且 worker 未 completed 时，`_execute_harvest_profile_batch_worker` 会跳过 registry queued short-circuit，直接 resume remote run；完成后 release provider limiter、mark registry fetched、触发 existing inline/reconcile 路径
  - 已修复：completed `harvest_profile_batch` delta apply 后，会继续调用 baseline profile prefetch，把 deferred tail 继续排下去，而不是只 materialize 已完成的一个 batch
  - 已修复：profile-search/source-seed URL 现在通过 `seed_source_type/source_type/mode/account_id/source_path` 进入 source mix；live medium profile-search batches 默认按推荐窗口并行提交，173 URL 会拆成约 3 个 actor 同步排出；large company roster 仍保持保守窗口并继续受 DB limiter `harvest_profile_scraper_actor` 默认 4 保护
  - 仍未完全收口：对于已经 provider completed 的 profile-scraper run，本地还缺更强的 response-level / completion-level immediate handoff，导致“下一轮 submit”“本轮 dataset ingest”“下游 candidate detail materialization”之间仍可能出现不必要等待
  - `2026-04-26 20:40-20:56` 期间额外观察到的 `4 x 73` live Harvest profile-scraper actor 不是整批重跑：
    - 对 `runtime/company_assets/lovable/20260426T193540/harvest_profiles/harvest_profile_batch_*.queue_dataset_items.json` 做了 pairwise URL audit
    - `58 + 73 + 73 + 73 + 73` 五个 batch 共 `350` 个 unique LinkedIn URL，批次之间零重叠
    - 因此这些调用符合 deferred tail drain 预期，不是“同一个失败 batch 被完整重跑”
    - 当时 job summary 里的 `requested_url_count=1156 / deferred_url_count=798` 也与这个审计一致：`1156 - 8 cached - 350 fetched = 798 remaining deferred`
  - 前端 `progress` 映射修正：当 job status 已是 backend `completed` 但 `worker_summary.by_status` 仍有 `queued/running/waiting_remote_*` worker 时，UI 保持 `running` 语义并提示“结果已可浏览，后台仍在补全 LinkedIn profile 与候选人详情”，避免显示成完全 `results`
  - 测试缺口说明：原先 scripted/模拟覆盖了“queued URL 不应重复 submit”和 connector-level pending resume，但没有覆盖“worker 自己持有 checkpoint run_id + registry queued”这一组合状态；本轮新增真实 scripted provider pending->completed worker test 防止回退
- 这轮未跑新的 live Harvest 请求；Mistral 现有 artifact 结论仍是：canonical `candidate_documents.json` 已到 622 candidates，其中部分 profile detail 仍取决于 background prefetch/reconcile 尾巴是否完成。
- 已跑验证：
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/harvest_connectors.py src/sourcing_agent/enrichment.py src/sourcing_agent/acquisition.py src/sourcing_agent/runtime_tuning.py src/sourcing_agent/execution_preferences.py tests/test_harvest_connectors.py tests/test_enrichment.py tests/test_pipeline.py tests/test_runtime_tuning.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py tests/test_enrichment.py tests/test_runtime_tuning.py -k 'company_employees or company_checkpoint or parse_harvest_company_employee_run_log or queue_background_profile_prefetch or bounded_parallel_submit_workers or global_inflight_budget or fast_smoke or preserves_harvest_global'` -> `20 passed, 97 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'harvest_profile_batch or background_harvest_prefetch or full_roster_profile_prefetch or scoped_search_prefetch or harvest_company_roster or segmented_background_harvest'` -> `11 passed, 268 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/runtime_tuning.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/enrichment.py src/sourcing_agent/company_asset_completion.py src/sourcing_agent/acquisition.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/connectors.py src/sourcing_agent/profile_timeline.py src/sourcing_agent/orchestrator.py tests/test_runtime_tuning.py tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_harvest_connectors.py tests/test_results_api.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_runtime_tuning.py tests/test_storage_profile_registry.py tests/test_harvest_connectors.py tests/test_enrichment.py tests/test_results_api.py -k 'runtime_provider_limiter or global_inflight_budget or fast_smoke or company_employee_rows or lease_blocks_duplicate_claims or roster_heavy or head_of_line or dashboard_is_summary_only'` -> `14 passed, 160 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_storage_profile_registry.py tests/test_runtime_tuning.py` -> `24 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'dashboard_is_summary_only or asset_population_compacts or profile_completion'` -> `5 passed, 43 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py -k 'harvest_profiles_for_urls or queue_background_profile_prefetch or roster_heavy or head_of_line'` -> `7 passed, 25 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py -k 'company_employee_rows or company_employees_probe or parse_harvest_company_employee_run_log'` -> `4 passed, 66 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/runtime_tuning.py src/sourcing_agent/execution_preferences.py src/sourcing_agent/workflow_smoke.py tests/test_runtime_tuning.py tests/test_workflow_smoke.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_runtime_tuning.py tests/test_workflow_smoke.py` -> `32 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/simulate_matrix_20260426_publicweb_harvest --seed-reference-runtime --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/simulate_matrix_20260426_publicweb_harvest_report.json --summary-json output/scripted_smoke_current/simulate_matrix_20260426_publicweb_harvest_summary.json` -> `7 cases passed`; behavior guardrails all 0 for duplicate provider dispatch, unexpected Public Web Stage 2, prerequisite gaps, final-results board violations, and streaming materialization violations
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_20260426_publicweb_harvest --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_20260426_publicweb_harvest_report.json --summary-json output/scripted_smoke_current/google_long_tail_20260426_publicweb_harvest_summary.json` -> `1 case passed`; observed queued/waiting remote Harvest worker states and board-ready completion with behavior guardrails all 0
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/simulate_matrix_20260426_provider_benchmark --seed-reference-runtime --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/simulate_matrix_20260426_provider_benchmark_report.json --summary-json output/scripted_smoke_current/simulate_matrix_20260426_provider_benchmark_summary.json` -> `7 cases passed`; new aggregate report includes `provider_backpressure` and `workflow_benchmark`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_20260426_provider_benchmark --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_20260426_provider_benchmark_report.json --summary-json output/scripted_smoke_current/google_long_tail_20260426_provider_benchmark_summary.json` -> `1 case passed`; `provider_backpressure.backpressure_case_count=1` from queued/waiting remote Harvest tail and board-ready completion remained healthy
  - `cd frontend-demo && npm run build`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/enrichment.py src/sourcing_agent/orchestrator.py src/sourcing_agent/runtime_tuning.py tests/test_enrichment.py tests/test_pipeline.py`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_enrichment.py -k 'scripted_resume_polls_own_queued_registry_urls or resumes_remote_run_even_when_registry_urls_are_queued or profile_search_prefetch_defaults_to_parallel_actor_submission_for_medium_batches or queue_background_profile_prefetch_defers or bounded_parallel_submit_workers'` -> `5 passed, 30 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_pipeline.py -k 'refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or reconcile_completed_workflow_after_background_harvest_prefetch_uses_shared_snapshot_apply_contract or scoped_search_prefetch_queues_all_known_profile_urls or full_roster_profile_prefetch_is_pending'` -> `4 passed, 275 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_harvest_connectors.py -k 'profile_batch_execute_with_checkpoint_supports_scripted_pending_rounds'` -> `1 passed, 69 deselected`
  - `cd frontend-demo && npm run build`

## 2026-04-25 (Asia/Shanghai)

### Public Web target-candidate experiment: batch search and fetch validation

- 在新增 PG tables 前，先把候选人级 Public Web Search 方法打磨成 artifact-only 实验模块：
  - 新增 `public_web_search.py`，包含 source-family query planning、entry-link 分类/排序、DataForSEO batch/queue 执行、per-candidate status artifacts、内容 fetch、邮箱抽取、AI adjudication payload 与 signals summary
  - CLI 入口：`run-target-candidate-public-web-experiment`
  - 默认走 provider batch/queue；`--no-batch-search` 仅作为调试 fallback
  - `ai_extraction=auto` 现在不会在 entry-link-only 阶段调用 live LLM，避免搜索完成后被低价值 link-only adjudication 拖慢；有 fetched documents 或 email candidates 时再进入 AI review
  - unreviewed links 不再进入 `primary_links`，只有 `confirmed/likely_same_person` 才能成为 primary candidate links
  - classifier 已收紧：低价值目录/聚合页不再算个人主页或公司页，个人主页优先于 company text match 且需要更强姓名 URL/title 证据
  - query planner 已收紧首轮预算：10x4 配置现在跑 general identity、homepage with negative filters、company-constrained Scholar citations、company-constrained GitHub；name-only GitHub/Scholar 是后续 fallback，generic resume/CV 和 email/contact 已后置
  - `company_page` 现在只表示公司官方/公司域名页面，第三方文章/活动页仅提到公司名不会再归为 company page，且候选人级默认不 fetch company page
  - fetched homepage/GitHub/Scholar/publication pages 会产出二级 entry links；Scholar profile 现在抽取 homepage、verified email domain、affiliation、research interests
  - Google Scholar verified email domain 不再误提取成 `email@domain`；多邮箱论文会先压低非候选人 local-part 邮箱，避免把 coauthor email 直接 promotion
- 真实 DataForSEO batch entry discovery 验证：
  - run id: `live-dataforseo-batch-entry-discovery-10x4-ai-skip`
  - 10 candidates, 40 queries, 40 submitted tasks, 38 fetched task results, 0 task timeouts, 3 ready polls
  - wall time about 88s, earlier synchronous CLI pass was about 4min
  - corrected classifier recheck: 166 ranked links, including LinkedIn, Scholar, GitHub, X, Substack, personal homepage, resume/CV, academic profile, publication, company page
- Replay-style fetch/extraction 验证：
  - run id: `replay-fetch-homepage-email-4-candidates`
  - 4 candidates, 6 fetched documents, 2 extracted high-confidence email candidates
  - verified emails: `cbfinn@cs.stanford.edu` from Chelsea Finn Stanford homepage/CV, `dainves1@gmail.com` from Xiang Fu homepage `mailto:`
  - 邮箱仍只是 `promotion_recommended` candidates，不会写入 `primary_email`
- 已跑验证：
  - `PYTHONPATH=src .venv-tests/bin/python -m compileall -q src/sourcing_agent/public_web_search.py src/sourcing_agent/model_provider.py src/sourcing_agent/document_extraction.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_document_extraction.py tests/test_cli.py`
  - `.venv-tests/bin/ruff check src/sourcing_agent/public_web_search.py src/sourcing_agent/model_provider.py src/sourcing_agent/document_extraction.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_document_extraction.py tests/test_cli.py`
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_document_extraction.py tests/test_cli.py` -> `49 passed`
- 详细产品化 TODO / empirical notes 已更新到 `docs/archive/PUBLIC_WEB_SEARCH_PRODUCTIZATION_TODO.md`。

### SQLite product-surface retirement before Public Web

- 在 Public Web Search 产品化前完成 bounded storage cleanup：
  - 移除 `export-sqlite-snapshot` / `restore-sqlite-snapshot` CLI，以及 handoff bundle 的 SQLite 选项
  - `sqlite_snapshot` bundle kind 现在在 export/import/upload/download/restore 产品路径中显式拒绝，只能作为离线历史材料处理
  - `import-cloud-assets` 的 `control_plane_snapshot` 恢复现在要求 Postgres DSN，不能 fallback 到磁盘 SQLite
  - `show-control-plane-runtime` 输出改为 `control_plane_storage_banner`；non-PG runtime 是 error，不再是 legacy warning
  - profile-registry disk fallback、test-env seed disk fallback、hosted smoke seed disk fallback 已移除或改为 PG-only
  - 新 artifact 字段写 `control_plane_candidate_count/control_plane_evidence_count`，仅保留读取旧 `sqlite_*` 字段的兼容 fallback
- 新增/更新回归保护：
  - `tests/test_storage_surface_guardrails.py`
  - `tests/test_asset_sync.py`
  - `tests/test_cloud_asset_import.py`
  - `tests/test_object_storage.py`
  - `tests/test_cli.py`
- 已跑验证：
  - `./.venv-tests/bin/python -m compileall -q src scripts tests`
  - `./.venv-tests/bin/python -m pytest tests/test_asset_sync.py tests/test_cloud_asset_import.py tests/test_object_storage.py tests/test_cli.py tests/test_storage_surface_guardrails.py tests/test_candidate_artifacts.py tests/test_company_asset_completion.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_control_plane_postgres.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_control_plane_live_postgres.py tests/test_storage_profile_registry.py -q`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli show-control-plane-runtime`
  - `./.venv-tests/bin/python -m pytest -q`
  - 当前 full result：`1140 passed, 8 skipped, 25 subtests passed`

### Closeout validation, browser plan semantics, and documentation handoff

- 收掉最终 browser E2E 暴露的 plan 语义恢复问题：
  - `/api/plan/submit` 的异步 hydration 完成后，`frontend_history_links.metadata` 现在会持久化：
    - `request_preview`
    - `dispatch_preview`
    - `organization_execution_profile`
    - `asset_reuse_plan`
    - `lane_preview`
    - `effective_execution_semantics`
  - 前端 history recovery 会消费这些后端语义字段，不再把核心检索策略 / dispatch label 回退成 `待定` 或自行粗推断
  - 新组织 `full_company_roster` 的用户可见策略 label 统一为 `全量 live roster`，避免与已有本地资产的 `全量本地资产复用` 混淆
- 已完成 closeout 验证矩阵：
  - `./.venv-tests/bin/python -m pytest tests/test_execution_semantics.py tests/test_frontend_history_recovery.py -q`
  - `cd frontend-demo && npm run build`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q`
    - `5 passed, 2 skipped, 2 subtests passed`
  - `./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
  - `bash ./scripts/run_python_quality.sh typecheck`
  - `./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q`
  - isolated simulate matrix:
    - `scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/closeout_simulate_20260425 --seed-reference-runtime --fast-runtime --strict --timing-summary`
  - scripted Google long-tail:
    - `scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_long_tail_closeout_20260425 --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary`
  - 全仓：
    - `./.venv-tests/bin/python -m pytest -q`
    - 当前结果：`1135 passed, 8 skipped, 25 subtests passed`
- 文档 closeout 已开始：
  - 已确认 54 个一方 Markdown 均有 `> Status:` 头
  - 更新重点是 `AGENTS.md`、测试/运行/ECS 文档、active tracker、handoff，而不是机械改动每个历史 reference 文件
- GitHub handoff 已完成：
  - branch: `productization-2026-04-25-stable`
  - remote tracking: `origin/productization-2026-04-25-stable`
  - commits:
    - `ef1ea4c Stabilize productization workflow contracts`
    - `53d30f1 Add workspace development guardrails`
  - 已排除 runtime/cache/vendor/build output、generated `object_sync` 和 `frontend-demo/public/tml` 离线资产

### Productization pass: streaming/materialization reports, scripted scenarios, asset governance, Excel throughput

- 已完成 productization 第二批可执行契约：
  - Harvest / materialization：
    - `runtime_tuning.py` 新增 materialization coalescing window 与 streaming budget report
    - running job 的 inline incremental sync 现在会通过 shared `materialization_writer` in-flight slot
    - sync result 会记录 writer slot / streaming budget metadata
    - smoke provider case report 现在输出 `materialization_streaming`
    - aggregate smoke summary 会汇总 provider response count、pending delta count、provider response -> first materialization gap，并将 slow first materialization 作为 guardrail
  - scripted provider scenario：
    - 新增 scenario coverage validator
    - 新增 `configs/scripted/provider_behavior_matrix.json`
    - 覆盖 retryable 429、timeout、partial result、staged ready/fetch 四类行为
  - asset governance：
    - 新增 `asset_governance.py`
    - 建立 `(company_key, scope_kind, scope_key, asset_kind)` default pointer contract
    - canonical replacement plan 会生成新 default pointer、superseded predecessor 与 historical retention 摘要
    - 非 canonical lifecycle 不能被提升为默认指针
  - Excel intake：
    - intake summary 现在带 `throughput_plan`
    - 覆盖 company grouping、direct LinkedIn fetch batch、search-required count、row-level continuation、target candidates/export linkage gates
  - product journey regression matrix：
    - scripted provider / workflow smoke report / Excel intake / asset governance / product journey 已接入 high-signal test selection
- 已完成验证：
  - `py_compile` 覆盖本轮新增/修改的 backend modules
  - `tests/test_runtime_tuning.py`
  - `tests/test_scripted_provider_scenario.py`
  - `tests/test_asset_governance.py`
  - `tests/test_workflow_smoke.py -k 'provider_case_report or summarize_smoke_timings'`
  - `tests/test_excel_intake.py -k 'throughput_plan or local_exact_hit_and_local_manual_review'`
  - `tests/test_regression_matrix.py`
  - `tests/test_markdown_status.py`
  - repo-configured `typecheck`
  - hosted scripted Google long-tail timeout smoke sample

### Productization pass: persisted governance writers, Excel target/export actions, provider batch streaming

- 已完成 productization 第三批收尾：
  - asset governance 不再只是纯 helper：
    - 新增 `asset_default_pointers`
    - 新增 `asset_default_pointer_history`
    - storage writer 会持久化 canonical default pointer、superseded history 与 coverage proof
    - 新增 API：
      - `GET /api/assets/governance/default-pointers`
      - `POST /api/assets/governance/promote-default`
    - 新增 CLI：
      - `promote-asset-default-pointer`
  - Excel intake 的 target/export linkage 已从计划字段接成真实动作：
    - 新增 `POST /api/target-candidates/import-from-job`
    - 首页 Excel group 完成后可直接导入目标候选人
    - 首页 Excel group 完成后可直接导出 target candidates profile bundle
  - Harvest profile adapter 已支持 provider batch response callback：
    - `fetch_profiles_by_urls(..., on_batch_result=...)`
    - enrichment live fetch 会在单个 provider batch 返回后立即写 profile registry / fetched cache
  - materialization writer backpressure 继续收口：
    - `runtime_inflight_slot(...)` 支持同线程 reentrant，避免 outer sync + inner artifact write 在 budget=1 时死锁
    - `SnapshotMaterializer.synchronize_snapshot_candidate_documents(...)` 主入口也进入 shared `materialization_writer` budget
- 已完成验证：
  - `py_compile` 覆盖本轮 backend modules 与相关 tests
  - `tests/test_asset_governance.py`
  - `tests/test_runtime_tuning.py`
  - `tests/test_harvest_connectors.py`
  - `tests/test_results_api.py`
  - `tests/test_excel_intake.py`
  - `tests/test_regression_matrix.py`
  - 相关 `tests/test_pipeline.py` incremental sync / snapshot materializer 子集
  - frontend `npm run build`
  - repo-configured `typecheck`
  - 全仓 `pytest -q`：`1134 passed, 8 skipped, 25 subtests passed`

### Productization pass: runner shutdown, promoted aggregate proof, shared plan labels

- 已完成本轮 productization 第一批可执行契约：
  - runtime service 增加持久化 cooperative shutdown：
    - `services/<service_name>/stop_request.json`
    - `/api/runtime/services/shutdown`
    - `/api/jobs/{job_id}/cancel`
  - workflow cancel 现在会：
    - 将 workflow job 标记为 `cancelled`
    - retire 未完成 worker
    - release workflow lease
    - 请求 job-scoped recovery service 停止
  - multi-snapshot coverage 不再用 `source_snapshot_selection.mode` 当 proof：
    - `preferred_snapshot_subset` 只表示选择模式
    - 必须有显式 `coverage_proof` / promoted aggregate contract 才能解锁 multi-snapshot population-default reuse
  - 前后端检索策略 label 继续收口：
    - 后端 `compile_execution_semantics(...)` 输出 `execution_strategy_label`
    - 前端 plan 卡片优先展示后端语义 label，不再先自行推断
  - SQLite legacy surface 增加 CLI banner：
    - `show-control-plane-runtime` 会显式标记非 PG-only 为 legacy/emergency，并输出 migration exit
- 已完成验证：
  - targeted service daemon / pipeline cancel / planning / organization profile / execution semantics / CLI tests
  - frontend `npm run build`
- 本轮活跃 tracker：
  - `docs/archive/SESSION_TRACKER_2026-04-25_PRODUCTIZATION.md`

### Hosted smoke long-tail verification closure

- 收掉第二轮全仓 pytest 暴露的 hosted smoke 尾巴：
  - post-terminal nonblocking `harvest_profile_batch` 不再被 recovery follow-up 当成“可忽略尾巴”直接跳过；不阻塞主 job 完成，但仍会继续后台抓取并 reconcile profile detail
  - `workflow_smoke.py` 现在会在 job terminal 后继续 settle recoverable background workers，避免 smoke 把“主链完成”误判成“profile-tail 也已完成”
  - snapshot candidate documents 同步 / incremental apply 后统一失效对应 snapshot 的 asset-population 读缓存，避免候选人看板或 `/results?include_candidates=1` 读到旧 materialized payload
  - hosted smoke harness teardown 等待更多实际写 runtime 的后台线程，并对 cleanup 期间后台线程已删除子文件的 `ENOENT` 做安全处理，减少 rmtree race
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
  - `bash ./scripts/run_python_quality.sh typecheck`
  - `./.venv-tests/bin/python -m pytest -q`
  - 当前结果：`1105 passed, 8 skipped, 25 subtests passed`

## 2026-04-24

### Long-tail architecture hardening tracker

- 本轮剩余架构尾巴集中记录在：
  - `docs/archive/SESSION_TRACKER_2026-04-24_LONG_TAIL.md`
- 每次 context compact / 会话恢复后，先读该 tracker、本文最新条目和 `docs/NEXT_TODO.md`，再执行命令，避免重复排查或把已修过的策略回退。
- 当前 long-tail checklist 已收口：
  - runner/recovery ownership 已由 hosted dispatch marker、recovery takeover guard、job lease release regression 固定
  - plan hydration 已升级为 normalized request-signature inflight dedupe，并记录 coalescing / queue wait metadata
  - runtime PG/schema bootstrap 已支持按 runtime namespace 解析 schema，isolated runtime 不再默认继承 production schema
  - Harvest provider calls 与 materialization writer 已并入 shared global in-flight budget / backpressure
  - asset governance/promotion 已加入 lifecycle gate，`draft/partial/superseded/archived/empty` 不可被自动 promotion 成 authoritative coverage baseline
  - cold materialization 已去掉一次冗余 per-candidate normalize，并为 profile/source JSON 建立 mtime/size 缓存与 selector index
- SQLite compatibility surface 已继续收紧：production runtime 默认要求 PG，只有显式 `SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE=1` 才允许 emergency SQLite control plane
- `2026-04-26` 又补了一层收口，减少排障误导：
  - `show-control-plane-runtime` / runtime summary 现在明确区分 `default_db_path/settings_db_path` 只是 shadow seed path，不等于 live authoritative DB
  - store 新增 generic compatibility-shadow accessors，外围 import/watchdog/CLI 不再继续用 `getattr(... sqlite_shadow_connect_target ...)` 这一类兼容分支去猜测真实 store target
  - 文档统一改成“disk-backed live SQLite retired; ephemeral compatibility shadow remains”，避免把“SQLite 已退役”误读成 repo 内完全没有 shadow/compat 代码
- 当前验证状态与命令记录见 `docs/archive/SESSION_TRACKER_2026-04-24_LONG_TAIL.md`。

### Excel intake relaunch gate 收尾

- 首页 `Excel Intake` 入口已重新打开：
  - 默认展示；需要灰度关闭时设置 `VITE_ENABLE_EXCEL_INTAKE_WORKFLOW=false`
  - `.env.development` / `.env.production` 现显式启用该入口
- 本地浏览器上传链路已收口：
  - Vite `dev` 与 `preview` 都支持 same-origin `/api/*` proxy
  - 前端 API client 对本地 same-origin HTML fallback 增加 `127.0.0.1:8765` / `localhost:8765` loopback retry
  - Excel workflow 继续使用真实 `FormData` 上传，后端转为 `file_content_base64` 后在云端解析，不再依赖用户本地路径
- 多公司 Excel 自动拆 job 已保留并增加真实浏览器回归：
  - `frontend-demo/scripts/run_excel_intake_e2e.mjs`
  - `tests/test_frontend_browser_e2e.py::FrontendBrowserFastE2ETest::test_browser_excel_intake_upload_splits_companies_via_same_origin`
  - 覆盖选中文件、提交 `/api/intake/excel/workflow`、按 company 拆成多个 group、首页展示拆分结果
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_excel_intake.py tests/test_frontend_history_recovery.py tests/test_api_json_serialization.py -q -k 'excel or Excel'`
  - `./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py`
  - `npm run build`
  - `./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q -k 'excel_intake'`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q -k 'excel_intake_upload'`

### Runtime environment namespace 隔离收口

- 已新增统一 runtime environment contract：
  - `src/sourcing_agent/runtime_environment.py`
  - `SOURCING_RUNTIME_ENVIRONMENT=production|local_dev|test|simulate|scripted|replay|ci`
  - provider mode 仍由 `SOURCING_EXTERNAL_PROVIDER_MODE` 表达，但不再单独承担环境隔离语义
- Harvest shared provider cache 已改成 live-only 且按 runtime namespace 分目录：
  - `runtime/provider_cache/<runtime_environment>/live/<logical_provider_name>/...`
  - `simulate/scripted/replay` 不再读写 shared Harvest provider cache
  - 旧版 `runtime/provider_cache/<logical_name>/...` 不再作为可复用 cache；遇到 `_offline` legacy body 会删除
- test/scripted/replay runtime 已阻断 repo-level PG 自动继承：
  - `scripts/dev_backend.sh` 在隔离 runtime 下先设置 `SOURCING_RUNTIME_ENVIRONMENT`
  - 默认写入 `<runtime_dir>/.isolated-local-postgres.env`
  - 除非显式提供 runtime 专属 `SOURCING_LOCAL_POSTGRES_ENV_FILE` 或设置 `SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1`
- hosted production entrypoint 已收紧：
  - `scripts/run_hosted_trial_backend.sh` 默认 `SOURCING_RUNTIME_ENVIRONMENT=production`
  - hosted 默认 provider mode 从 `replay` 改为 `live`
  - production 下 `replay/simulate/scripted` 需要显式 temporary override，否则启动失败
- `/api/runtime/health` 现在会返回当前 `runtime_environment / provider_mode / provider_cache_namespace / runtime_dir`，用于部署或手动测试前确认实际运行 namespace
- 文档已落到：
  - `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`
  - `docs/TEST_ENVIRONMENT.md`
  - `docs/ECS_PRELAUNCH_CHECKLIST.md`
  - `docs/ALIYUN_ECS_TRIAL_ROLLOUT.md`
  - `docs/PG_ONLY_CUTOVER_TRACKER.md`
  - `docs/NEXT_TODO.md`

### Perplexity full-roster 语义、hosted queued 卡住与 former broad-search 收口

- Meta `TBD` / ByteDance `Seedance` plan 复核：
  - 两者实际 `strategy_type` 都是 `scoped_search_roster`，都会走带关键词的 scoped current/former profile search
  - 前端 label 差异来自 baseline 状态：ByteDance 没 baseline；Meta 曾被 4 月 23 日 Excel intake 的 2 人快照误标成 authoritative baseline
  - 已新增大组织 coverage baseline gate：Excel/import/supplemental 小样本资产仍可作为候选人资产检索，但没有 shard / standard bundle / coverage proof 时，不再解锁 `prefer_delta_from_baseline` 或 `baseline_reuse_available`
  - 已在本地 PG 刷新 Meta execution profile：`baseline_candidate_count=2`、`coverage_baseline_reuse_ready=false`、`current_lane_default=keyword_search`
- Perplexity `Agent` 方向 plan 显示 `全量公司资产` 是当前预期：
  - Perplexity 本地没有 authoritative baseline，organization execution profile 走 `fallback_unknown_company`
  - fallback 默认 acquisition mode 是 `full_company_roster`
  - `Agent` 在这个模式下应作为后续本地 retrieval/rerank 关键词，而不是限制 current company roster API
- 已确认并修复 hosted workflow `queued` 卡住：
  - 根因是 `start_workflow(...)` 先启动 recovery daemons，再启动 hosted dispatch，recovery 可能抢先持有 job lease
  - queued hosted recovery 也曾在持有 job lease 时再启动 hosted thread，导致 thread 立即看到 `already_running` 后退出
  - 现在 hosted dispatch 先于 recovery daemon 启动；recovery 接管 queued hosted job 时不再持有外层 job lock
- 已收口 former lane 的 full-roster 语义：
  - 对 full-company roster / 小公司全量成员策略，former `linkedin-profile-search` 默认只传 `pastCompanies`
  - 不再把 `Agent` 这类方向词作为 `searchQuery` 限制 former 搜索
  - 只有 scoped / large-org keyword-only 策略才保留 keyword-limited former search
- 旧 Perplexity job `b22332dfcc6f` 是修复前启动的，但修复后已从 queued 恢复并完成：
  - worker 日志记录本次 Perplexity asset population 约 `525` candidates
  - `/api/jobs/{job_id}/results` 默认仍只返回轻量/检索结果视图，不等价于资产总人口数
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_seed_discovery.py -q -k 'former_paid_fallback_keyword_only_skips_blank_query or former_paid_fallback_full_roster_uses_broad_past_company_when_not_keyword_only or paid_fallback_all_queries_union_runs_all_harvest_queries'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'resume_queued_workflow_prefers_hosted_dispatch_when_requested or resume_queued_hosted_workflow_dispatches_without_holding_job_lock or start_workflow_dispatches_hosted_runner_before_recovery_daemons or start_hosted_workflow_thread_skips_duplicate_dispatch_with_fresh_marker or progress_auto_recovery_skips_fresh_hosted_dispatch_for_queued_job'`
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/seed_discovery.py tests/test_pipeline.py tests/test_seed_discovery.py`
  - 本地 backend / frontend 已重启并健康：`http://127.0.0.1:8765`、`http://localhost:4173`

### Monica probe/full duplicate 与 Perplexity plan 卡顿根因修复

- `harvestapi/linkedin-company-employees` 的 adaptive probe 现在会在“probe 已证明完整”时直接复用 probe dataset：
  - 条件是 probe completed、未触发 provider cap、`estimated_total_count > 0`、且 `returned_item_count >= estimated_total_count`
  - 同步 `fetch_company_roster(...)` 与 queued `execute_with_checkpoint(...)` 两条入口都已覆盖
  - 因此 Monica AI 这类 probe 只返回 8 个成员的 case，不再为了同一个 payload 再发第二次 full run
- Perplexity plan 9-13 分钟的根因不是 planner 本身：
  - 独立 `explain-workflow` 对 `Perplexity Agent` 实测约 `12.87s`
  - 本地 serve 进程里同时跑了 startup `organization_asset_warmup`
  - 该 warmup 默认无上限扫描/同步所有 runtime organization assets，长期占用一个 Python 线程和 GIL，拖慢 async plan hydration
- startup organization asset warmup 已改成显式 opt-in：
  - 默认 `STARTUP_ORGANIZATION_ASSET_WARMUP_ENABLED=false`
  - 需要冷启动预热时再手动开启，不能再作为每次本地/线上 serve 的默认后台任务
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_harvest_connectors.py -q -k 'complete_probe_dataset or probe_company_roster_query_records_probe_summary or multi_page_fetch_budget'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'startup_organization_asset_warmup_is_opt_in_by_default'`
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/harvest_connectors.py src/sourcing_agent/orchestrator.py tests/test_harvest_connectors.py tests/test_pipeline.py`

### Scripted workflow guardrails 当前轮收尾

- `workflow_smoke.py` 的 progress observability 已把计数语义拆清：
  - `result_count / observed_company_candidate_count` 是单调观测计数，回退仍视为 regression
  - `manual_review_count` 是待处理 backlog，profile 补全后下降是正常行为，现单独输出 `backlog_reductions`
  - synthetic terminal fallback 在缺少终态计数字段时仍会 carry forward，避免 terminal summary 把有效计数误清零
- 当前 scripted smoke 已能结构化验证这几类用户手动测试暴露过的问题：
  - duplicate provider dispatch
  - default-off `Public Web Stage 2` 是否被误打开
  - prerequisite-ready 到下游启动之间是否存在异常空转
  - `Final Results` 与候选人看板持久化是否一致
  - progress counter regression 与 manual-review backlog reduction 分口径统计
- 新一轮验证已完成：
  - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py -q`
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/scripted_google_long_tail_current_recheck4 --provider-mode scripted --scripted-scenario configs/scripted/google_multimodal_long_tail.json --fast-runtime --case google_multimodal_pretrain --strict --timing-summary --report-json output/scripted_smoke_current/google_long_tail_recheck4_report.json --summary-json output/scripted_smoke_current/google_long_tail_recheck4_summary.json`
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover or hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider or hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch or hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch or hosted_scripted_long_tail_accepts_request_scoped_fast_smoke_profile_without_server_env'`
  - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py tests/test_harvest_connectors.py tests/test_seed_discovery.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or reconcile_completed_workflow_after_background_company_roster or reconcile_completed_workflow_after_background_harvest_prefetch or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline'`
- Google long-tail scripted strict recheck 当前结果：
  - total 约 `5502.19ms`
  - `progress_regression_case_count=0`
  - duplicate dispatch / disabled stage / prerequisite gap / final-results board violation 均为 `0`

### Search-seed profile prefetch 已前推到单 query 返回粒度

- `src/sourcing_agent/worker_daemon.py` 这轮把 worker result 消费从“按提交顺序阻塞 `future.result()`”改成了“按实际完成顺序消费”：
  - `AutonomousWorkerDaemon.run(...)` 现在会按 completed order 处理 worker result
  - 同时支持 `result_callback`
  - 这让上层不必再等最慢的那条 query，才能看到更快 query 的增量结果
- `src/sourcing_agent/seed_discovery.py` 现在新增了 `on_incremental_query_result` contract：
  - `discover(...)` 会在单个 query spec 返回 usable entries 后立即回调
  - `_provider_people_search_fallback(...)` 也同步接入，不再只在 paid fallback 全部完成后才统一吐结果
  - 当前 `web_search` / `harvest_profile_search` 两条 search-seed 入口都已经具备 query-level incremental emit
- `src/sourcing_agent/acquisition.py` 也不再把 search-seed prefetch 分成“incremental 临时分支 + final snapshot 另一套逻辑”：
  - 新增 shared helper，把 search-seed entries 统一转成 prefetch candidates
  - acquisition 侧会按 job-local seen URL 去重
  - overlapping query results 不会在同一 job 里重复派发相同的 profile prefetch
  - final snapshot prefetch 会和前面 incremental dispatch 汇总成同一份 summary，而不是把“前面已派发过”覆盖成一个误导性的 skipped 结果
- 这轮解决的是：
  - search-seed 不再必须等整轮 discover 结束，才第一次启动 profile prefetch
  - overlap query 不再在同一 job 内二次派发同一个 LinkedIn profile URL
  - worker daemon completion order 不再被最慢 worker 拖成伪串行
- 这轮还没有结束的点：
  - 还没把 `linkedin-company-employees` / `linkedin-profile-search` 的 provider response 也统一收成同一套 request-level incremental ingest
  - 也还没把 scraper batch 返回后的 delta materialization / debounce writer 收到最终形态
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_worker_daemon.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_seed_discovery.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store'`
  - 当前均已转绿

### Company-roster background output 已并入 shared snapshot apply / reconcile contract

- `src/sourcing_agent/snapshot_materializer.py` 这轮新增了 `apply_company_roster_workers_to_snapshot(...)`：
  - `harvest_company_employees` completed worker 现在不再只是“等下次整轮恢复时再重抓”
  - 会直接把 worker/shard 的 queue dataset materialize 成 worker snapshot
  - 再把它增量 merge 回 root snapshot 的：
    - `harvest_company_employees_merged.json`
    - `harvest_company_employees_visible.json`
    - `harvest_company_employees_headless.json`
    - `harvest_company_employees_summary.json`
  - 同时也会把 root `candidate_documents.json` 一并增量更新
- `src/sourcing_agent/orchestrator.py` 现已把这条 contract 接到两条主路径：
  - `pre_retrieval_refresh`
  - `completed-job background reconcile`
  - running/completed 两条路径不再对 `company_roster` 各长一套独立逻辑
- `src/sourcing_agent/workflow_refresh.py` 这轮也补了 shared classification / metrics：
  - `worker_has_completed_background_company_roster_output(...)`
  - `resolve_reconcile_snapshot_dir(...)` / `resolve_reconcile_snapshot_id(...)` 现在优先认 `root_snapshot_dir`
  - segmented shard worker 不会再错误把 shard dir 当成最终 root snapshot 去 reconcile
  - refresh metrics 现新增：
    - `inline_company_roster_worker_count`
    - `background_company_roster_reconcile_count`
- partial segmented roster 这轮也收了 completion contract：
  - `_restore_segmented_roster_snapshot_from_snapshot_dir(...)` 不再要求 all-shards-ready 才第一次恢复
  - 已完成 shard 现在可以先恢复成 partial root snapshot，供当前 workflow 继续主链
  - 但 `src/sourcing_agent/acquisition.py::_load_cached_roster_snapshot(...)` 会显式跳过 `completion_status != completed` 的 partial roster
  - 这样 partial current roster 可以服务当前 job，但不会误被后续新 job 当成“可直接复用的完整 cached roster”
- 这轮解决的是：
  - `linkedin-company-employees` completed background worker 不再游离在 search-seed 之外
  - running/completed 两种 reconcile 都共享同一套 roster apply contract
  - segmented roster 可以 partial restore，但不会错误解锁 cached reuse
- 这轮还没结束的点：
  - `linkedin-company-employees` live provider response 仍未做到“单个 provider request 返回就立即 inline ingest”，当前收口点还是 completed worker/shard output
  - scraper batch 返回后的 delta materialization / debounce writer 仍未收到终态
- 已完成验证：
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_refresh.py src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/orchestrator.py src/sourcing_agent/acquisition.py`
  - `./.venv-tests/bin/python -m pytest tests/test_worker_daemon.py tests/test_seed_discovery.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or reconcile_completed_workflow_after_background_company_roster or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline or restore_roster_snapshot_rehydrates_segmented_harvest_queue_shards or restore_roster_snapshot_rehydrates_partial_segmented_harvest_queue_shards_without_cached_reuse'`
  - 当前均已转绿

### Harvest/company-roster worker completion 现已走统一的 inline incremental ingest 主路径

- `src/sourcing_agent/snapshot_materializer.py` 这轮补上了 `apply_harvest_profile_workers_to_snapshot(...)`：
  - completed `harvest_profile_batch` worker 不再只靠后续 `CompanyAssetCompletionManager` 全量补课
  - 现在会直接把已持久化的 raw LinkedIn profile payload delta-merge 回 root `candidate_documents.json`
  - 复用现有 profile match / membership review / non-member apply helper，不再额外长一套 harvest-only merge 逻辑
- `src/sourcing_agent/orchestrator.py` 现已把 running/completed 两条路都收口到同一 helper：
  - running job:
    - worker recovery completion callback 会立即 apply company-roster / harvest-prefetch 输出
    - company-roster apply 后会立刻继续 queue baseline profile prefetch
    - full sync/materialize 现已收成 same-kind micro-batch + per-job single-writer：
      - 同 job、同 snapshot、同 kind 的多个 completed worker 会先合并成一个 apply batch
      - 同 kind worker 仍在 in-flight -> 只做 delta apply，先不重跑 full sync
      - 同 kind worker drain 完 -> 只触发一次 `_synchronize_snapshot_candidate_documents(...)`
  - completed job:
    - `_reconcile_completed_workflow_after_harvest_prefetch(...)` 现改为先走 shared harvest delta apply
    - 不再回退到 `CompanyAssetCompletionManager.complete_snapshot_profiles(...)` 作为另一套 reconcile 主路径
- `src/sourcing_agent/acquisition.py` 这轮把 in-process segmented roster 也接到了同一 callback：
  - 当 `ThreadPoolExecutor` shard 中已有 completed local shard、但整轮 segmented roster 仍需 background resume 时
  - 会立即把这批 completed shard 通过同一 inline reconcile callback merge 回 root snapshot
  - 不再等到 recovery daemon 下一拍才第一次看见这些已完成 shard
- 为了避免“callback 已吃掉 worker，但 refresh/reconcile 又二次吃同一 worker”：
  - completed background worker 现在会写 `inline_incremental_ingest` marker
  - `worker_has_completed_background_company_roster_output(...)` / `worker_has_completed_background_harvest_prefetch(...)` 会跳过已 inline-consumed worker
- 这轮实际收掉的是：
  - `linkedin-company-employees` background worker completion -> root snapshot apply -> downstream prefetch/sync
  - `harvest_profile_batch` completion -> delta merge -> same-kind micro-batch sync/materialize
  - in-process segmented `harvest_company_employees` completion -> same inline callback / apply contract
  - completed reconcile 不再保留 harvest-prefetch 的另一套 legacy completion-manager 双轨
- 还没继续做的更深一层优化：
  - 现在剩下的不是“同步 shard 有没有接入 callback”，而是更细的 runtime tuning：
    - 还没继续做 time-window coalescing
    - 还没把 per-job single-writer 再提升成更完整的 global in-flight budget / backpressure contract
- 已完成验证：
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/orchestrator.py src/sourcing_agent/worker_daemon.py src/sourcing_agent/workflow_refresh.py`
  - `./.venv-tests/bin/python -m pytest tests/test_worker_daemon.py tests/test_worker_recovery_daemon.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or reconcile_completed_workflow_after_background_harvest_prefetch_uses_shared_snapshot_apply_contract or execute_segmented_harvest_company_roster_workers_emits_inline_callback_for_completed_local_shards or micro_batches_completed_harvest_prefetch_workers_and_syncs_once'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_prefetches_incrementally_without_redispatching_overlapping_urls or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or reconcile_completed_workflow_after_background_search_seed or reconcile_completed_workflow_after_background_company_roster or reconcile_completed_workflow_after_background_harvest_prefetch or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_company_roster_outputs_and_syncs_store or refresh_running_workflow_before_retrieval_applies_completed_background_harvest_prefetch_outputs_and_syncs_store or handle_completed_recovery_worker_result_applies_harvest_prefetch_inline_and_defers_sync_while_same_kind_worker_pending or refresh_running_workflow_before_retrieval_skips_workers_already_consumed_inline'`
  - 当前均已转绿

### Full-roster blocked resume 已允许基于 partial baseline 提前继续

- `src/sourcing_agent/orchestrator.py` 这轮收掉了一个仍然存在的阶段级 barrier：
  - 之前 `acquire_full_roster` 只要还有 `acquisition_specialist / harvest_company_employees` worker 在跑，就会继续保持 `blocked`
  - 即使 former/search-seed baseline 已经足够进入 `enrich_linkedin_profiles`，workflow 也会被错误卡住
- 这轮又把同一条 contract 前推到了 `src/sourcing_agent/acquisition.py`：
  - `acquire_full_roster` 在 current-roster Harvest 已 queued、但 former/search-seed baseline 已到位时，不再先返回 `blocked`
  - 现在会直接以 partial baseline 继续主链，并提前派发 background profile prefetch
  - 这样不必再等 recovery tick 才从 blocked 状态被重新拉起
- 同时也把原先不对称的反向分支收掉了：
  - 如果 current roster 已经 ready，但 former/search-seed 还在后台 worker
  - `acquire_full_roster` 现在也会继续主链，而不是继续 `blocked`
  - current/former 两侧改为共用同一个 full-roster baseline continuation helper，不再各长一套局部分支
- 现在的 contract 改为：
  - 只要 `acquire_full_roster` 已经具备可用 baseline
  - 无论剩余后台 worker 是 `harvest_company_employees` 还是 former/search-seed
  - workflow 就允许继续 resume 到 enrichment / normalize / retrieval
  - current-roster Harvest worker 留在后台继续补 current lane，而不是阻塞主链
- 这意味着：
  - `LinkedIn Stage 1 completed` 不再必须等待所有 current roster worker terminal
  - full-roster job 可以更早进入 preview / board-ready 路径
  - readiness 与 checkpoint reuse 的 contract 现在更一致，不再出现“checkpoint 已够复用，但 readiness 还在机械等待”的分叉
  - former/search-seed baseline 上的 LinkedIn profile prefetch 也能更早开始，而不是只能等 `enrich_linkedin_profiles`
- search-seed acquisition / recovery 这轮也继续前推到更细粒度：
  - `acquire_search_seed_pool(...)` 一旦拿到带 `profile_url` 的新 entries，就会立刻派发 background profile prefetch
  - `background_search_seed_reconcile`
  - `pre_retrieval_refresh`
  - 这两条补偿路径现在也会在 merge `search_seed_snapshot` 后立刻触发同一套 prefetch，而不是只等后续 enrichment stage
- 同时把旧测试契约对齐到当前 default-off stage 设计：
  - `tests/test_pipeline.py` 不再假设 `enrich_public_web_signals` 在默认 plan 中一定存在
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers or acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists or enrich_profiles_writes_baseline_when_full_roster_profile_prefetch_is_pending or restore_completed_workflow_stage_summary_contract_keeps_latest_completed_at or scoped_search_prefetch_queues_all_known_profile_urls or execute_retrieval_skips_ranked_scoring_for_asset_population_default_view'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'resume_blocked_workflow_after_workers_complete or resume_blocked_workflow_ignores_pending_exploration_workers or job_progress_classifies_blocked_acquisition_workers or get_job_progress_triggers_auto_recovery_when_blocked_on_acquisition_workers or acquire_full_roster_blocks_when_background_harvest_is_pending'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs or acquire_full_roster_blocks_when_background_harvest_is_pending or acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists or resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers or enrich_profiles_writes_baseline_when_full_roster_profile_prefetch_is_pending'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_search_seed_pool_queues_profile_prefetch_immediately_for_recovered_entries or acquire_search_seed_pool_allows_baseline_when_background_queue_is_pending or acquire_search_seed_pool_still_blocks_when_background_queue_has_no_entries or reconcile_completed_workflow_after_background_search_seed or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or acquire_full_roster_continues_from_roster_baseline_while_former_search_runs_in_background or acquire_full_roster_continues_from_search_seed_baseline_while_background_harvest_runs'`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_full_roster or acquire_search_seed_pool or reconcile_completed_workflow_after_background_search_seed or refresh_running_workflow_before_retrieval_applies_completed_background_search_outputs_and_syncs_store or resume_blocked_workflow_uses_search_seed_baseline_with_noncritical_pending_workers or acquisition_resume_readiness_allows_background_current_roster_harvest_when_baseline_exists'`
  - 当前均已转绿

### Workflow behavior guardrails 已接入 scripted smoke report

- scripted provider 路径现会把 dispatch invocation 写入隔离 runtime：
  - `src/sourcing_agent/scripted_provider_scenario.py`
  - `src/sourcing_agent/search_provider.py`
  - `src/sourcing_agent/harvest_connectors.py`
- `src/sourcing_agent/workflow_smoke.py` 现在会在 per-case `provider_case_report` 和顶层 export 中同时输出：
  - `behavior_guardrails.duplicate_provider_dispatch`
  - `behavior_guardrails.disabled_stage_violations`
  - `behavior_guardrails.prerequisite_gaps`
  - `behavior_guardrails.final_results_board_consistency`
- 这意味着 scripted smoke 不再只看 wall-clock，而能直接回答：
  - 同一 payload 有没有被重复 dispatch
  - `Public Web Stage 2` 这种 default-off stage 有没有被误打开
  - `LinkedIn Stage 1 -> Stage 1 Preview -> stage_2_final` 之间有没有不合理空转
  - `Final Results` 之后 board 是否真正 non-empty
- 汇总层 `summarize_smoke_timings(...)` 也新增了 aggregate summary：
  - duplicate dispatch case/signature/redundant dispatch counts
  - unexpected public web stage case count
  - prerequisite gap p95/min/max
  - final-results-vs-board violation case count
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest -q tests/test_workflow_smoke.py`
  - `./.venv-tests/bin/python -m pytest -q tests/test_hosted_workflow_smoke.py -k 'large_org_full_roster_overflow_completes_without_live_provider'`
  - `./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir <tmp> --provider-mode scripted --scripted-scenario configs/scripted/large_org_full_roster_overflow.json --fast-runtime --case xai_full_roster --strict --timing-summary`
  - 实测这条 `xai_full_roster` scripted smoke 的 `behavior_guardrails` 为全绿：
    - duplicate dispatch `0`
    - unexpected public web stage `0`
    - prerequisite gaps `0ms`
    - final-results / board inconsistency `0`

### Public Web Stage 2 已改为 default-off，不再阻塞候选人看板

- workflow submission 默认 `analysis_stage_mode=single_stage`：
  - `POST /api/workflows` 不再默认把 job 送进 `two_stage`
  - 默认链路现在是 `LinkedIn Stage 1 -> Stage 1 Preview -> normalize/materialize -> final results`
- plan 默认不再加入 `enrich_public_web_signals`：
  - 只有显式 `analysis_stage_mode=two_stage` 的请求才会把 `Public Web Stage 2` 放进 acquisition plan
  - 因此默认 workflow 不会再被一个当前几乎无增量价值的 stage2 acquisition 卡住
- `Stage 1 preview` 的 summary / event 文案也已对齐：
  - default path 改为 `Continuing snapshot materialization`
  - 只有显式启用 two-stage 的 case 才会显示 `Continuing Public Web Stage 2 acquisition`
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest -q` 跑了这条变更对应的高信号子集
  - 覆盖了：
    - plan default-off / explicit two-stage opt-in
    - single-stage workflow 不再落 `public_web_stage_2`
    - two-stage 显式启用时旧链路仍可继续工作
    - `/api/workflows` 默认 submission contract 改为 `single_stage`

### `/api/plan` 已拆成 fast submit + async plan hydration

- 这轮先把“进入检索方案页”从同步 `explain_workflow()` 长尾里剥离出来：
  - 新增 `POST /api/plan/submit`
  - 前端不再阻塞等待完整 plan/explain 才切路由
  - 现在会先创建 `history_id`、立即进入 `phase=plan` 的 loading 态，再通过 frontend-history recovery 异步补全 plan
- 后端新增了按 `history_id` 去重的 plan hydration in-flight state：
  - `src/sourcing_agent/orchestrator.py`
    - `submit_plan_workflow(...)`
    - `_queue_plan_hydration(...)`
    - `_run_plan_hydration(...)`
  - `frontend_history_link.metadata.plan_generation` 现在显式记录：
    - `queued`
    - `running`
    - `completed`
    - `failed`
    - 以及 request id / timing / error message
- `/api/frontend-history/{history_id}` recovery 也补上了显式 `error_message`，前端可以直接区分：
  - “还在生成 plan”
  - “plan 已生成可展示”
  - “plan generation failed”
- 前端 `SearchPage` 已接到新 contract：
  - submit / revision 现在走 fast submit
  - 方案页允许 `plan=null` 的 loading state
  - history hydration 对 `phase=plan && plan missing` 不再死循环 recover，而是走定时 hydration polling
  - 历史侧栏在这类记录上显示“检索方案生成中”，不再直接显示“未生成方案”
- API lane 也一起收了：
  - `/api/plan/submit` 被标为 `light` lane
  - 避免它继续和重 POST 共用 shared lane，导致轻量提交也被排队拖慢
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_frontend_history_recovery.py tests/test_api_json_serialization.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'http_api_light_routes_are_classified_for_reserved_lane'`
  - `cd frontend-demo && npm run build`
  - 当前均已转绿

### Remaining note after fast submit split

- 这轮收掉的是“HTTP submit/route transition 长尾”，不是 `build_plan` 本身的纯计算成本：
  - `llm_normalize_request`
  - `_build_augmented_sourcing_plan`
  - `_build_execution_bundle`
  - 这些仍在后台 hydration 线程里执行
- 因此用户进入方案页已可接近秒级，但完整 plan detail 的最终 ready 时间，仍受 planner/LLM 本身耗时影响。

### 2026-04-23 PM tracker closure pass

- `docs/archive/SESSION_TRACKER_2026-04-23_PM.md` 现已正式关闭：
  - immediate stabilization tails are no longer tracked as an open session backlog
  - long-horizon follow-ups were promoted into `docs/NEXT_TODO.md`
- authoritative snapshot board source has been re-aligned:
  - `src/sourcing_agent/orchestrator.py` now ignores non-patch job asset-population overlays when the candidate source is an authoritative snapshot baseline
  - this fixes the class where `/api/jobs/{job_id}/dashboard` and `/api/jobs/{job_id}/candidates?offset=0` exposed different first-page prefixes for the same authoritative asset
- frontend board hydration/recovery contract is now more stable:
  - `frontend-demo/src/lib/api.ts`
    - candidate-page merge is now offset-aware
    - incomplete dashboard summaries proactively backfill page 0 before continuing hydration
- smoke/stage digest contract was tightened:
  - `src/sourcing_agent/workflow_smoke.py` now synthesizes missing canonical provider stages such as `public_web_stage_2` from later completed stages
  - hosted smoke assertions no longer flap on reuse cases where the provider stage was effectively skipped but finalization already completed
- regression coverage added:
  - `tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_authoritative_snapshot_candidate_page_ignores_non_patch_overlay_prefix_drift`
  - `tests/test_workflow_smoke.py::WorkflowSmokeTest::test_stage_summary_digest_synthesizes_missing_public_web_stage_from_final_stage`
- verified in this pass:
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'authoritative_snapshot_candidate_page_ignores_non_patch_overlay_prefix_drift or large_org_full_roster_existing_baseline_defaults_to_asset_population'`
  - `./.venv-tests/bin/python -m pytest tests/test_workflow_smoke.py tests/test_hosted_workflow_smoke.py tests/test_scripted_test_runtime.py -q`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q`
  - current result: green (`4 passed, 2 skipped` for browser E2E; relevant smoke/runtime suites green)

### Background prefetch submit 与 backend stage timing 已做 fresh smoke 复核

- `execution_preferences.py` 这轮把 runtime tuning 的数值型 override 也保留下来，避免：
  - request normalize 后把 `harvest_prefetch_submit_workers`
  - `parallel_search_workers`
  - `parallel_exploration_workers`
  - `candidate_artifact_max_workers`
  - 这类 runtime knobs 静默丢失
- `enrichment.py` 的 background Harvest prefetch submit 现已与 foreground hydration 基本对齐：
  - chunk submit 不再全串行
  - 会基于 URL/source-shard 上下文解默认 window
  - 再通过 shared runtime tuning resolver 做 cap/override
- `orchestrator.py` / `workflow_smoke.py` 这轮也补了 backend stage timestamp repair：
  - `stage_1_preview` nested payload 会带 `started_at` / `completed_at`
  - `linkedin_stage_1` 缺失 `started_at` 时会从既有 summary 或 job `created_at` 回填
  - smoke 汇总现在优先消费 backend authoritative timestamps，而不是先掉回 client timeline
- 已用隔离 runtime 重跑 fresh smoke，并落盘到 `output/scripted_smoke_v4/`：
  - `openai_reuse`
    - `reuse_snapshot`
    - total 约 `1505.78ms`
    - `job_to_board_nonempty` 约 `1020.23ms`
  - `google_scoped_cold`
    - `delta_from_snapshot`
    - total 约 `5144.26ms`
    - `job_to_board_nonempty` 约 `1076.37ms`
  - `xai_live_roster`
    - `new_job`
    - total 约 `18825.6ms`
    - `job_to_board_nonempty` 约 `15120.17ms`
    - progress 峰值：
      - `queued_worker_count=3`
      - `waiting_remote_harvest_count=3`
      - `pending_worker_count=3`
- fresh smoke 结论：
  - `provider_case_report.stage_wall_clock_ms` 不再回到空字典
  - `provider_case_report.workflow_wall_clock_ms` 在 reuse / delta / live-roster 三类代表性 case 都已产出
  - backend stage timing 修复已经体现在标准 smoke report，而不只是单测
- 尾巴已继续收掉：
  - per-case 顶层 `stage_summary_digest / stage_wall_clock_ms / workflow_wall_clock_ms` 现已与 nested `provider_case_report` 对齐导出
  - synthetic terminal sample 现会保留 monotonic counters，不再把 completed case 的 `manual_review_count` / `result_count` 打回 `0`
- 已补并通过回归：
  - `tests/test_workflow_smoke.py::WorkflowSmokeTest::test_synthetic_terminal_sample_preserves_monotonic_counts`
  - `tests/test_workflow_smoke.py::WorkflowSmokeTest::test_build_case_level_smoke_exports_surfaces_stage_digest_and_timings`
  - fresh recheck: `output/scripted_smoke_v4_recheck/openai_reuse/report.json`

### Shared runtime tuning 继续收口到 scheduler lane budget，并补上 workflow wall-clock smoke report

- `runtime_tuning.py` 这轮继续向上收口：
  - 不再只覆盖 provider query / roster shard / candidate artifact 并发
  - 同一套 profile/override resolver 现在也覆盖：
    - `parallel_search_workers`
    - `parallel_exploration_workers`
    - `search_worker_unit_budget`
    - `public_media_worker_unit_budget`
    - `exploration_worker_unit_budget`
- `worker_scheduler.py` 不再直接机械读 `cost_policy.parallel_*`：
  - 会先从 plan / task intent view 的 `execution_preferences.runtime_tuning_profile` 抽 shared runtime context
  - 再统一解析 lane limits / lane budget caps
  - 这样 `PersistentWorkerRecoveryDaemon`、scheduler summary、exploration worker daemon 不再各自长一套并发预算分叉
- `enrichment.py` / `exploratory_enrichment.py` 也继续并到同一 contract：
  - public-web exploration worker 数不再由 `acquisition.py` 直接读 `cost_policy`
  - `MultiSourceEnricher` 现按 request runtime tuning + cost policy 统一解出 exploration parallelism
  - `ExploratoryWebEnricher` 的 daemon plan 也会带上真实 plan/request runtime context，而不是临时拼一个只含 `parallel_exploration_workers` 的简化 plan
- `workflow_smoke.py` 的 provider-grade report 继续升级：
  - 新增 `workflow_wall_clock_ms`
  - 优先使用 backend stage summary 的时间戳
  - 若 backend stage summary 缺失/不稳定，则回退到 client-observed timeline + smoke timings 估算：
    - `job_to_stage_1_preview`
    - `job_to_final_results`
    - `stage_1_preview_to_final_results`
    - `final_results_to_board_ready`
    - `job_to_board_ready`
    - `job_to_board_nonempty`
  - aggregate summary 现也带：
    - `workflow_wall_clock_ms`
    - `strategy_rollups`
      - `effective_acquisition_mode`
      - `dispatch_strategy`
- `scripts/run_simulate_smoke_matrix.py` 现支持直接落盘：
  - `--report-json`
  - `--summary-json`
  - 这样不再依赖 shell redirect 去抓 stdout/stderr，scripted smoke report 更稳定
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_runtime_tuning.py tests/test_worker_scheduler.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_enrichment.py tests/test_seed_discovery.py tests/test_company_asset_completion.py tests/test_worker_recovery_daemon.py -q`
- 新 scripted smoke 基线已落到 `output/scripted_smoke_v3/`：
  - `openai_reuse`
    - `full_local_asset_reuse`
    - total 约 `1351.05ms`
    - `job_to_final_results` 约 `80.59ms`
    - `job_to_board_nonempty` 约 `129.88ms`
  - `google_scoped_cold`
    - `baseline_reuse_with_delta`
    - total 约 `18569.96ms`
    - `job_to_stage_1_preview` 约 `2266.48ms`
    - `job_to_final_results` 约 `16080.28ms`
    - `job_to_board_nonempty` 约 `16257.71ms`
    - progress 峰值：
      - `queued_worker_count=1`
      - `waiting_remote_harvest_count=1`
      - `pending_worker_count=1`
      - `active_worker_count=1`
  - `xai_live_roster`
    - `scoped_live_search`
    - total 约 `27678.67ms`
    - `job_to_stage_1_preview` 约 `8234.12ms`
    - `job_to_final_results` 约 `27340.48ms`
    - `job_to_board_nonempty` 约 `27472.91ms`
    - progress 峰值：
      - `queued_worker_count=3`
      - `waiting_remote_harvest_count=3`
      - `pending_worker_count=3`

### Shared runtime tuning 收口到 provider throughput / artifact workers，并把 smoke report 升级到 progress observability

- `runtime_tuning.py` 不再只管 cooldown/poll：
  - 现在同一套 profile/override resolver 也覆盖：
    - `provider_people_search_parallel_queries`
    - `harvest_company_roster_parallel_shards`
    - `candidate_artifact_parallel_min_candidates`
    - `candidate_artifact_max_workers`
- 已接入的主路径：
  - `seed_discovery.py`
    - Harvest `profile_search` 并行 query worker 数不再各自读 cost_policy，改为先走 shared runtime tuning resolver
  - `acquisition.py`
    - segmented `company-employees` shard fetch / shard worker dispatch 并发上限改为 shared resolver
  - `candidate_artifacts.py`
    - `foreground_fast` 的 candidate artifact 并发预算不再写死旧常数
    - 现在按 build profile + CPU + runtime tuning overrides 统一计算
    - `build_execution` 也新增：
      - `parallel_min_candidates`
      - `parallel_worker_cap`
      - `runtime_tuning_profile`
- `scripts/run_simulate_smoke_matrix.py` / `scripts/run_live_large_org_regression.py` 已清掉遗留的
  `allow_high_cost_sources` 调用参数，脚本重新与当前 active contract 对齐。
- `workflow_smoke.py` 的 provider-grade case report 继续升级：
  - 新增 `progress_observability`
  - 单 case 现在会结构化输出：
    - progress sample 数
    - `queued_worker_count / waiting_remote_harvest_count / pending_worker_count` 峰值
    - `result_count / observed_company_candidate_count` 是否回退
    - `manual_review_count` 是否作为 backlog 正常下降，而不是被误判为候选人数回退
    - 是否出现 “results 已 ready，但 progress terminal 事件滞后” 的 synthetic terminal sample
  - aggregate summary 也会带：
    - `progress_regression_case_count`
    - `progress_sample_count`
    - `progress_maxima`
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_runtime_tuning.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_candidate_artifacts.py -q -k 'records_phase_timings or foreground_fast_skips_compatibility_exports_and_hot_cache or aliases_strict_view_when_it_matches_canonical or legacy_like_benchmark_flags_disable_fast_paths or reuses_candidate_shards_and_rebuilds_only_dirty_candidates or uses_scope_replace_when_all_states_are_dirty'`
  - `./.venv-tests/bin/python -m pytest tests/test_company_asset_completion.py tests/test_enrichment.py tests/test_seed_discovery.py -q`
- 新 benchmark / smoke 观测：
  - `scripts/run_candidate_artifact_benchmark.py --candidate-count 320 --dirty-candidates 24 --repeat 3 --build-profile foreground_fast`
    - `cold_full` optimized median `3703.2ms` vs legacy-like `6745.95ms`，约 `1.82x`
    - `incremental_patch` optimized median `2007.68ms` vs legacy-like `2347.21ms`，约 `1.17x`
  - isolated/scripted smoke:
    - `openai_reasoning` full local reuse: total 约 `2115.8ms`，board probe wait 约 `17.48ms`
    - `google_multimodal_pretrain` baseline reuse + delta scripted case: total 约 `33807.25ms`
      - report 正确标出 `terminal_progress_lag_detected=true`
      - 无 progress counter regression
    - `xai_full_roster` scripted large-roster/profile-tail case: total 约 `31091.1ms`
      - `queued_worker_count / waiting_remote_harvest_count / pending_worker_count` 峰值均为 `3`
      - 无 progress counter regression，且最终 progress 已观测到 terminal

## 2026-04-23

### Evening stabilization tracker consolidated

- 已新增 [docs/archive/SESSION_TRACKER_2026-04-23_PM.md](docs/archive/SESSION_TRACKER_2026-04-23_PM.md)
  - 把 `2026-04-23 20:00` 之后讨论的收尾项集中成单一 Markdown tracker
  - 明确区分：
    - fixed
    - partially fixed
    - still open
  - 目的是避免 context compact 后再次遗漏：
    - `allow_high_cost_sources` 彻底退场
    - worker/progress/results 可见状态一致性
    - API payload slimming 剩余尾巴
    - runtime namespace isolation
    - Harvest batching / benchmark / Excel intake relaunch gate

### 4/21-4/23 变更复盘已落文档，core lane 语义与 live profile hydration 再收一层

- 已新增正式复盘文档：
  - `docs/archive/CHANGE_REVIEW_2026-04-21_2026-04-23.md`
  - 重新归类了 4/21-4/23 这轮改动里：
    - 哪些属于正确的 contract 收口
    - 哪些只是方向正确但仍有 compatibility tail
    - 哪些本质上仍是补丁式/双轨式修复，需要继续回退成单一 contract
- 已继续清理 planner/review 的 core acquisition lane 可见语义：
  - `full_company_roster` 的 `roster_sources` 现以 `harvest_company_employees` 为主
  - `scoped_search_roster` / `former_employee_search` 现以 `harvest_profile_search` 为主
  - 不再在这类 core lane 的用户可见 metadata 里继续表达成 `web_search_seed_queries` 优先
  - `planning._build_assumptions(...)` 也已去掉旧的 “low-cost web search before paid LinkedIn people search” 叙事
- 已继续收 `enrichment.py` 的 live profile hydration 瓶颈：
  - `_fetch_harvest_profiles_for_urls(...)` 不再按 batch 全串行 waterfall 执行
  - 当前改成 bounded-window 并行：
    - `live`: 最多 2 个 batch 并发
    - `non-live`: 最多 4 个 batch 并发
  - 仍保留按 chunk 记 queued/fetched/failed registry 状态的 contract，不做无界 fan-out
- 已把同一套 Harvest live batching contract 向后台 `company_asset_completion` 收口：
  - live profile completion 不再固定 `150 x 1 worker` 的大批串行模式
  - 改为复用 `enrichment` 的 URL-count + source-mix adaptive window
  - roster-heavy completion 现在也能抬到 `3` 个并发 batch，而不是继续串行补全
- 已补/更新回归：
  - `tests/test_enrichment.py`
    - balanced live batch 断言更新为顺序无关
    - 新增 bounded parallel live batch 断言，验证 `205 -> 103/102` 且 `max_inflight=2`
  - `tests/test_company_asset_completion.py`
    - live completion 改为断言 adaptive parallel batching
    - 新增 roster-heavy live completion `240 -> 60x4` 且 `max_inflight>=3`
  - `tests/test_planning_modules.py`
    - 补 core roster source 语义断言
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_enrichment.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_planning_modules.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_review_plan_instructions.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'acquire_former_search_seed_keeps_primary_provider_search_for_scoped_search or build_sourcing_plan_splits_linkedin_and_public_web_acquisition_stages or plan_stage_colloquial_full_company_request_can_skip_review'`
- 当前剩余尾巴：
  - background prefetch worker chunk 提交仍主要是顺序 dispatch，尚未做同等级别的 bounded parallel submit
  - generic high-cost toggle 本身已从 active contract / smoke helper / 当前回归 fixture 中彻底移除；剩余的是更上层的 provider policy 收口，不是字段兼容尾巴

### Generic cost toggle 删除完成，补上 investor category replace 去重护栏

- `allow_high_cost_sources` 已从 active contract / smoke helper / 当前测试 fixture 中彻底移除：
  - 不再保留 `high_cost_requires_approval` 衍生 policy tail
  - 当前代码/测试搜索仅剩 tracker 与历史进度文档中的历史描述
- 顺手补上了 `storage.py` 的更泛化去重护栏：
  - `replace_company_category_data(...)` 现在会先删除与 incoming candidate ids 冲突的旧行
  - candidate/evidence bulk replace path 也会先做一次 payload 级 dedupe
  - 这样像 `investor_firm_roster` 这种“同 candidate_id 以另一 category 已存在”的情况，不会再把 workflow 打成 `UNIQUE constraint failed: candidates.candidate_id`
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_provider_execution_policy.py tests/test_planning_modules.py tests/test_workflow_smoke.py -q`
  - `./.venv-tests/bin/python -m pytest tests/test_pipeline.py -q -k 'investor_firm_workflow_uses_tiered_firm_roster or build_sourcing_plan_full_company_defaults_do_not_require_high_cost_approval or acquire_former_search_seed_uses_primary_provider_search_mode or acquire_former_search_seed_keeps_primary_provider_search_when_high_cost_not_allowed or acquire_former_search_seed_keeps_primary_provider_search_for_scoped_search or plan_stage_colloquial_full_company_request_can_skip_review or queue_workflow_backfills_missing_target_company_from_approved_review_scope or queue_workflow_rebuilds_google_keyword_shard_plan_from_approved_review_request or compile_plan_review_instruction_uses_model_then_schema_validation or workflow_progress_schema_summarizes_stage_and_worker_state or run_workflow_blocking_triggers_job_recovery_when_acquisition_is_blocked'`
  - `./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q -k 'test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs or test_hosted_simulate_xai_boundary_and_anthropic_asset_population or test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover or test_hosted_simulate_large_org_full_roster_existing_baseline_defaults_to_asset_population'`
  - `cd frontend-demo && npm run build`
  - `./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py tests/test_hosted_workflow_smoke.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/storage.py`
- 顺手补了一轮 hosted harness teardown 修复：
  - `tests/test_hosted_workflow_smoke.py` 现在会等待：
    - `background-outreach-layering-*`
    - `background-snapshot-materialization-*`
  - 代表性的 hosted smoke 组合回归已能在同一 pytest 进程内顺序通过，不再在 teardown 阶段被后台 reconcile 线程打出 segfault
  - 整套 hosted/browser full matrix 仍未重跑，但这条高频 crash 已不再是当前 blocker

### Replay 假候选人已收口，ECS 已切回 live provider

- 已修正 `harvest_connectors.py` 中 `replay` 模式的离线行为：
  - `replay` 现在是严格的 cache-only
  - cache miss 不再合成 `Offline Member` / 假 LinkedIn profile
  - synthetic sample 仅保留给 `simulate`
- 已补测试并通过：
  - `tests/test_harvest_connectors.py` 中 replay cache-miss / company-roster probe 相关断言已更新为“返回空结果而不是合成样本”
- 已在 ECS 上完成 Surge AI 污染清理：
  - 删除了 `Surge AI / 20260423T195046` 的 PG registry / shard / generation / membership / materialization state / history-job / LinkedIn registry 记录
  - 删除了 `/srv/sourcing-ai-agent/runtime/company_assets/surgeai`
  - 删除了 `/srv/sourcing-ai-agent/runtime/hot_cache_company_assets/surgeai`
  - 删除了对应 `runtime/jobs/401ca57f9fc9*.json`
  - 清理后复核计数均为 `0`，不会被后续 baseline / snapshot 复用
- 进一步确认并补上了第二层根因修复：
  - 问题不只是 `replay` 会产出假数据，而是这些 `_offline` body 之前还能落进生产共享的 `runtime/provider_cache/*`
  - 结果是后续 `live` job 也会命中这份污染缓存，表现成“明明切回 live 了，还是读到 Surge Ai Offline Member”
  - 现已收口为：
    - `live` / `replay` 都不会复用带 `_offline` 标记的 shared provider cache
    - `replay` / `simulate` / `scripted` 产出的 harvest body 不再写回 shared provider cache
    - 发现历史 `_offline` shared cache 时会直接丢弃并删除，而不是继续命中
  - 这次清理也补齐到了 provider cache 层，不再只删 PG / runtime assets / jobs 文件
- 经验沉淀：
  - `replay` 只是 provider mode，不等于独立测试环境
  - 真正的问题是生产 live runtime 与 replay/simulate/scripted 的 cache namespace 之前隔离不严
  - 后续终态仍建议继续推进“live runtime / test runtime / scripted runtime”物理隔离，避免再次发生交叉污染
- 已将 ECS `sourcing-ai-agent.service` 的 `SOURCING_EXTERNAL_PROVIDER_MODE` 从 `replay` 改为 `live`
  - `systemctl` 已重载并重启
  - `/proc/<pid>/environ` 已确认当前进程实际运行在 `SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - `http://127.0.0.1:8765/health` 返回正常

### 检索方案页已前置 Target company LinkedIn

- `frontend-demo/src/components/PlanCard.tsx` 已将 `Target company LinkedIn` 从 `Advanced Mode` 挪到检索方案主卡片：
  - 默认展示后端当前识别到的 LinkedIn company URL
  - 以带圈问号 tooltip 展示识别来源 / 置信度 / 是否可复用本地资产
  - 仍保留手动修正输入框，只有用户填入新 URL 时才覆盖默认 slug 解析
- `frontend-demo/src/styles.css` 已补齐对应样式，并已通过 `frontend-demo` 生产构建验证
- 已完成 Cloudflare Pages production 发布：
  - Pages project: `sourcing-ai-agents-demo`
  - production deployment: `66610098-6882-4b91-9848-061aefd05229`
  - custom domain `https://demo.111874.xyz` 已确认切到新 bundle：
    - `/assets/index-CWVpTh4a.js`
    - `/assets/index-BVF0ErbD.css`
  - `https://demo.111874.xyz/health` 返回正常

### ECS 已完成整仓部署、PG control-plane bootstrap 与 Cloudflare production 发布

- 已将本地最新整仓代码增量同步到 ECS：
  - 远端代码目录确认是 `/opt/sourcing-ai-agent`
  - runtime 仍挂在 `/srv/sourcing-ai-agent/runtime`
  - 这次不是只补单文件，而是整仓 `rsync`，同时保留远端 `runtime/` 与 `venv/`
- 已确认并修复 ECS 上的真实 hosted leak：
  - 原有 `sourcing-ai-agent.service` systemd unit 仍直接执行
    `python -m sourcing_agent.cli serve`
  - drop-in `runtime.conf` 仍注入旧配置：
    - `SOURCING_EXTERNAL_PROVIDER_MODE=live`
    - `SOURCING_API_MAX_PARALLEL_REQUESTS=2`
  - 更关键的是它没有注入 `SOURCING_CONTROL_PLANE_POSTGRES_*`
  - 结果是 ECS 会自动重启回旧环境，并继续把 live control-plane 写到磁盘 SQLite shadow
- 已在 ECS 上补齐 Postgres 基础设施并完成控制面迁移：
  - 安装系统 `postgresql` / `postgresql-contrib`
  - 创建本地 `sourcing` 用户与 `sourcing_agent` 数据库
  - 将远端 `.local-postgres.env` 改成 ECS 实际可用 DSN：
    - `postgresql://sourcing:***@127.0.0.1:5432/sourcing_agent`
  - 将 live `runtime/control_plane.shadow.db` 全量 sync 到 PG
  - 已通过 `validate_postgres=true` 校验：
    - `jobs=4`
    - `job_events=138`
    - `organization_asset_registry=152`
    - `asset_materialization_generations=224`
    - `asset_membership_index=15863`
    - `candidate_materialization_state=1535`
    - `linkedin_profile_registry=452`
- 已把 ECS 的 systemd backend 改成 PG-only 生产配置：
  - 主 unit 现在显式注入：
    - `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
    - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
    - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
    - `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`
    - `SOURCING_EXTERNAL_PROVIDER_MODE=replay`
    - `SOURCING_API_MAX_PARALLEL_REQUESTS=8`
    - `SOURCING_API_LIGHT_REQUEST_RESERVED=2`
  - 已移除旧的 `runtime.conf`
  - `systemctl status sourcing-ai-agent.service` 已确认新环境生效
  - `/proc/<pid>/environ` 已确认当前 ECS live 进程实际携带上述 PG-only 变量
- 已完成 ECS 上的 OpenAI shard-family metadata 收口：
  - `OpenAI / 20260423T165904`
  - `--rebuild-from-assets` 恢复并校验 6 条 shard rows
  - family 结果已确认：
    - `Language Model -> Text`
    - `Vision -> Vision`
    - `Multimodal -> Multimodal`
- 已完成 Cloudflare Pages production 重新发布：
  - 先 build 最新 `frontend-demo/dist`
  - 再以 `--branch main` 发布到 `sourcing-ai-agents-demo`
  - 当前 `demo.111874.xyz` 已更新到最新 bundle：
    - `index-CnK2iRg-.js`
- 已完成公网验证：
  - `https://api.111874.xyz/health`
  - `https://demo.111874.xyz/health`
  - `https://demo.111874.xyz/api/runtime/health`
  - 当前均返回 `200`

### Hosted same-origin API proxy 已切到 Cloudflare Pages Functions

- 已完成 `demo.111874.xyz` 的同域 `/api/*` 收口，但实际落地路径不是独立 Worker route，而是 Pages Functions：
  - `frontend-demo/functions/api/[[path]].js`
  - `frontend-demo/functions/health.js`
  - `frontend-demo/cloudflare_api_proxy.mjs`
- 根因是当前本地 `cloudflare_pages.api_token` 足够做 Pages deploy，但不足以单独做 Workers script deploy / route bind：
  - `wrangler pages deploy ...` 可用
  - `wrangler deploy ... --route ...` 返回 `Authentication error [code: 10000]`
- 因此这轮改成把同域 proxy 收进 Pages 项目本身：
  - `demo.111874.xyz/api/* -> https://api.111874.xyz/api/*`
  - `demo.111874.xyz/health -> https://api.111874.xyz/health`
  - 发布 frontend 时同域 proxy 也一起更新，不再拆成“先 Pages、再 Worker route”两步
- 已完成验证：
  - `https://demo.111874.xyz/api/runtime/health` 返回后端 JSON，不再回落到前端 `index.html`
  - `https://demo.111874.xyz/health` 返回后端 JSON
  - `https://demo.111874.xyz` 当前已重新回到 `same-origin` bundle：`index-CnK2iRg-.js`
- 文档已同步更新：
  - `docs/CLOUDFLARE_SAME_ORIGIN_PROXY.md`

### 临时隐藏首页 Excel intake 入口，等待浏览器上传稳定性收口

- 历史记录：这一段描述的是当时的临时降级状态，已被 `2026-04-24` 的 “Excel intake relaunch gate 收尾” 取代。
- 当时首页 `Excel Intake` 入口改为 feature-flag gated：
  - `VITE_ENABLE_EXCEL_INTAKE_WORKFLOW=true` 才会显示
  - 默认本地/线上构建都不再展示该入口
- 原因不是后端 Excel 解析 contract 已失效，而是本地真实浏览器环境里仍出现间歇性：
  - `Local backend is unreachable via same-origin`
  - `Local backend is unreachable via http://localhost:8765`
  - `Local backend is unreachable via http://127.0.0.1:8765`
- Playwright 下 direct upload / workflow split 已可通过，但用户浏览器仍未稳定，因此当前策略是先隐藏，避免继续把未收敛链路暴露到首页。
- 后续重新开放前，需要补浏览器级稳定性回归并完成 direct-upload 网络路径的最终收口。

### 修复 acquisition_shard_registry 真实 PG 存量库的 legacy bool/int 迁移炸点

- 已修复 `OpenAI / Reasoning` 在 `/api/plan` 与 `/api/workflows/explain` 上触发的 `500`：
  - 根因不是 query 语义，而是本地 PG 里 `acquisition_shard_registry` 仍停留在 legacy 单表形态
  - 旧表中的 `provider_cap_hit` 是 `bigint`
  - writer schema ensure 在做 former/current split cutover 时，直接把旧列灌进新 `BOOLEAN` 列，导致类型不匹配
- 这次把修复收口到了 PG 契约边界，而不是只在某个调用点打补丁：
  - legacy table -> split tables 的迁移 SQL 现在会显式把 `provider_cap_hit` 归一为 boolean
  - `upsert_acquisition_shard_registry_rows(...)` 也会把旧 `0/1` payload 统一归一为 boolean
  - 因此同时覆盖：
    - 存量库 schema ensure
    - 后续 live writer / bulk upsert
- 已在本地真实 PG 执行 schema ensure，并确认当前状态：
  - `acquisition_shard_registry` 现为 compatibility view
  - `acquisition_shard_registry_current` / `acquisition_shard_registry_former` 均为物理表
  - 两张 split 表里的 `provider_cap_hit` 均为 `boolean`
- 已补回归：
  - `tests/test_control_plane_postgres.py`
  - 覆盖 legacy table split migration 的 `provider_cap_hit` 归一化
  - 覆盖 acquisition shard upsert 时 `0/1 -> bool` 的 PG 写入归一化
- 已完成验证：
  - `./.venv-tests/bin/python -m pytest tests/test_control_plane_postgres.py -q`
  - 本地 `POST /api/plan`
  - 本地 `POST /api/workflows/explain`
  - 当前对 `我想要OpenAI做Reasoning方向的人` 均已返回 `200`

### 收口 hosted live snapshot 在 `Public Web Stage 2 -> Final Results` 间被前台 materialization 卡住的问题

- 已锁定这次 `OpenAI / Multimodal` 在 ECS 上“Stage 2 已完成、但 Final Results 与候选人看板迟迟不出”的根因：
  - 不是 PG/SQLite 存储后端本身把 job 卡死
  - 而是 `direct asset-population finalization` 明明已可用时，`_refresh_running_workflow_before_retrieval(...)` 仍把已完成的 `harvest_prefetch` worker 视为“必须先 inline sync/materialize snapshot”的信号
  - 于是前台继续跑 `candidate_documents / normalized_artifacts / retrieval-ready`，把结果页阻塞在 post-stage2 收尾
- 这轮已把 contract 改成更清晰的两段式：
  - authoritative candidate source ready -> foreground finalize
  - snapshot sync/materialize/reconcile -> background deferred materialization
- 已完成的后端收口：
  - `orchestrator.py` 新增统一的 deferred `background_snapshot_materialization` helper
  - direct finalization 当前 snapshot 与 baseline patch 两条分支，都会显式写出 background materialization contract
  - `pre_retrieval_refresh` 新增“harvest-prefetch 可 defer 到后台”的语义，不再把这类 worker 完成机械等同于前台必须同步
  - background reconcile 事件文案也从“baseline-backed finalization”泛化成“deferred finalization”，避免误导
- 这次也补上了之前缺失的 regression fixture：
  - `direct finalization + completed harvest workers + no search-seed delta + snapshot artifacts not ready`
  - 这是此前本地回归没覆盖到、但 ECS hosted live case 正好触发的组合
- 已完成验证：
  - `python3 -m py_compile src/sourcing_agent/orchestrator.py tests/test_pipeline.py`
  - `./.venv/bin/python -m pytest tests/test_pipeline.py -q -k 'direct_finalization or harvest_prefetch_for_direct_finalization or harvest_defer_into_background_materialization or preview_can_use_stage_candidate_documents or equivalent_baseline_reuse or reused_snapshot_short_circuits_after_public_web_stage'`
  - 当前为 `9 passed`

### baseline candidate 选择收口，避免 stale authoritative row 误把已覆盖 query 判成 delta

- 已修复 `OpenAI / Coding` 这类“公司已有本地 query-family 资产，但 authoritative row 较旧”的计划误判：
  - 之前 `compile_asset_reuse_plan(...)` 只盯当前 authoritative registry row
  - 一旦 authoritative row 没及时晋升到较新的 snapshot，就会把已覆盖的 `current/former` query family 重新判成 `delta_from_snapshot`
  - 这次已改成：
    - 先算 authoritative baseline 的 reuse plan
    - 再对同公司其他 registry snapshot 做同一套 coverage 评估
    - 若存在更优 baseline candidate（更少 missing / 无 delta / coverage 更完整），自动切到那个 snapshot
- 本地真实验证结果：
  - `帮我找OpenAI做Coding方向的人`
  - 现已从：
    - `baseline_reuse_with_delta`
    - `planner_mode=delta_from_snapshot`
  - 修正为：
    - `effective_acquisition_mode=full_local_asset_reuse`
    - `planner_mode=reuse_snapshot_only`
    - `baseline_snapshot_id=20260422T171923`
- 已补回归：
  - `tests/test_workflow_explain.py::WorkflowExplainTest::test_explain_workflow_prefers_better_non_authoritative_snapshot_when_authoritative_baseline_is_stale`
- 这轮也额外回归了高信号 query：
  - `OpenAI / Reasoning`
  - `Anthropic / Pre-training`
  - 当前 reuse / delta 语义未被带坏
- 继续把 planner 与 `organization_execution_profile` 的 source 对齐也收掉了：
  - 根因不是 PG 缺数据，也不是 execution profile 缓存提前返回
  - 真正问题是 `organization_execution_profile._select_best_organization_asset_registry_row(...)`
    把 `evaluate_organization_asset_registry_promotion(...)` 当成了多轮链式比较器使用
  - `explicit_baseline_inclusion` 在链式比较里会把已经选中的更优 snapshot 又“晋升”回较旧 aggregate row，导致：
    - planner 选中较新的 baseline candidate
    - execution profile / authoritative row 仍停在旧 snapshot
- 这次已改成：
  - 提取共享 helper，统一做 organization-asset baseline candidate selection
  - 先按 coverage/quality 排序候选 row
  - 再始终以“当前 authoritative row”为固定基准，挑选第一个真正可晋升的更优候选
  - 不再在多个非 authoritative rows 之间反复链式 promotion
- 这层 shared helper 现在已同时接到：
  - `backfill_organization_asset_registry_for_company(...)`
  - `organization_execution_profile._select_best_organization_asset_registry_row(...)`
  - `compile_asset_reuse_plan(...)` 的 store-backed baseline candidate inventory
  - 避免出现“planner 已选对 baseline candidate，但 authoritative promotion / execution profile 仍各走各的”继续漂移
- 本地真实 PG/runtime 已验证：
  - `OpenAI` 当前 authoritative row 已从 `35230 / 20260422T151623` 切到
    `35232 / 20260422T171923`
  - `organization_execution_profiles.source_registry_id / source_snapshot_id` 也已同步切到同一 row
- 继续收掉了 `explicit_baseline_inclusion` 过宽导致的 live 回摆：
  - 之前它只要看到 “candidate selected_snapshot_ids 包含当前 authoritative snapshot” 就会直接允许 promotion
  - 这会让 `OpenAI` 在 authoritative 已经是 `20260422T171923` 时，又被更薄的历史 aggregate
    `20260416T095325` 反压回去
  - 现已改成：
    - `explicit_baseline_inclusion` 只有在 candidate sort key 更优时才可晋升
    - 同时必须满足：
      - 要么是显式 aggregate 且对当前 authoritative 形成非回退 subsumption
      - 要么是当前 authoritative 本身质量很差时的 `quality_recovery`
- live 验证结果：
  - `帮我找OpenAI做Coding方向的人`
    - `effective_acquisition_mode=full_local_asset_reuse`
    - `planner_mode=reuse_snapshot_only`
    - `baseline_snapshot_id=20260422T171923`
    - `organization_execution_profile.source_snapshot_id=20260422T171923`
  - `帮我找OpenAI做Pre-train方向的人`
    - 仍保持 `planner_mode=delta_from_snapshot`
    - 未被这轮 shared helper / promotion guard 收口带坏
- 已补回归：
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_promotion_candidate_selection_does_not_chain_regress_on_explicit_baseline_inclusion`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_explicit_baseline_inclusion_does_not_promote_thinner_aggregate_over_richer_authoritative`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_candidate_inventory_prioritizes_promoted_row_before_authoritative`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_select_best_registry_row_does_not_regress_after_promoting_better_candidate`
  - `tests/test_organization_execution_profile.py::OrganizationExecutionProfileTest::test_ensure_execution_profile_promotes_better_registry_row_and_aligns_source_snapshot`

### Browser E2E 候选人看板分页稳定性与真实大 snapshot cold build 基准

- `frontend-demo/scripts/run_workflow_e2e.mjs` 现支持两类入口：
  - 正常 `Search -> plan -> review -> results`
  - 直接打开 `?history=...&job=...` 的 restored-results 入口
- `tests/test_frontend_browser_e2e.py` 新增并固定了两条候选人看板分页稳定性 browser E2E：
  - large-org existing baseline workflow case
  - restored-history results recovery case
- 当前 browser 断言已经明确覆盖：
  - 切到第 2 页后不回跳到第 1 页
  - 当前页首批候选人 preview 顺序保持稳定
  - 如果没有观察到 `loadedCount` 继续增长，则必须已经处于 `loaded == total` 的全量态，不能是半加载错态
- 为了稳定制造 hydration 窗口，后端新增了一个测试专用延迟钩子：
  - `SOURCING_TEST_CANDIDATE_PAGE_DELAY_MS`
  - 仅用于 browser E2E / test harness，不改变默认生产路径
- 已完成验证：
  - `frontend-demo` 生产构建
  - `tests/test_frontend_browser_e2e.py`
    - `test_browser_workflow_e2e_covers_large_org_existing_baseline_asset_population`
    - `test_browser_results_recovery_hydration_preserves_second_page`
- 追加跑了一条真实大 snapshot 冷启动 benchmark：
  - 来源：隔离 runtime 下的 Google `20260423T040115`
  - 规模：`5897` candidates
  - `build_company_candidate_artifacts(..., build_profile=\"foreground_fast\")`
  - cold full wall time：`863216ms`，约 `14.4min`
  - 关键 timings：
    - `prepare_candidates`: `376223ms`
    - `payload_build_total`: `376552ms`
    - `view_write_total`: `381302ms`
    - `state_upsert`: `107ms`
    - `generation_register`: `651ms`
    - `finalize_total`: `1222ms`
  - 结论：
    - 当前真实大 snapshot cold build 的主瓶颈已明确不是 PG `state_upsert` / `finalize`
    - 主要耗时仍集中在 per-candidate prepare/payload/materialization 主路径

### explain/smoke 脚本隔离 runtime 收口

- `scripts/run_explain_dry_run_matrix.py` 与 `scripts/run_simulate_smoke_matrix.py` 现在都支持：
  - `--runtime-dir`
  - `--runtime-env-file`
  - `--seed-reference-runtime`
  - `--provider-mode`
  - `--scripted-scenario`
  - `--fast-runtime`
- 新增 `src/sourcing_agent/scripted_test_runtime.py`：
  - 负责启动脚本自带的 in-process backend
  - 显式把 `SOURCING_RUNTIME_DIR` 指向专用 `runtime/test_env/...`
  - 最初默认写空的 local-postgres env sentinel 来阻断仓库根 `.local-postgres.env`；later 2026-05-07 PG-only workflow confidence closeout replaced that for smoke/manual confidence with runtime-scoped `.scripted-local-postgres.env` and fail-closed PG readiness.
- 这意味着 explain/smoke 脚本现在可以稳定跑在“专用测试 runtime”上，而不是继续隐式吃当前本地运行态数据
- 近真实 scripted 模拟的推荐路径也已补档：
  - 先用 `scripts/seed_test_env_assets.py` 把当前 authoritative snapshot 种到 `runtime/test_env/...`
  - 再用 `--runtime-dir` + `--provider-mode scripted` 跑 explain/smoke
- 已补回归：
  - `tests/test_scripted_test_runtime.py`
  - 覆盖“阻断父目录 PG env 泄漏”和“seeded reference runtime 可直接提供 explain API”

### authoritative baseline completeness contract 收口与 large-specific reuse gate 去分叉

- `asset_reuse_planning.py` 已把“large org 单独 reuse helper + small/medium 通用 helper”的结构收成单一 authoritative-baseline completeness contract：
  - population-default reuse 与 embedded query reuse 现在共用同一套主判断骨架
  - 不再通过“large org 直接走另一套分支”来决定能否 `reuse_snapshot`
- `medium -> hybrid` 的语义已明确保留在 acquisition shape，而不是 reuse eligibility：
  - directional query -> `scoped_search_roster`
  - broad/full-company query -> `full_company_roster`
- 修掉了一个 hosted smoke 回归：
  - 之前把“multi-snapshot directional query 不能直接 population-default reuse”错误扩散到了 embedded query reuse
  - 已收回到 population-default 那一层，恢复 `Google multimodal + pre-train` 这类 case 的：
    - current lane `reuse_baseline`
    - former lane `delta_acquisition`
- 当前已明确的业务语义：
  - `Anthropic` 这类已经具备完整 authoritative baseline 的公司，可以直接 `reuse_snapshot`
  - `Google / OpenAI` 当前更接近 family-scoped authoritative baseline：
    - 已覆盖 family 可 reuse
    - 未覆盖 family 继续 `delta_from_snapshot`
    - 不应因为积累了多个方向性 snapshot 就自动视为“公司级 full local reuse”
- `selected_snapshot_ids > 1` 的粗代理已经进一步缩窄：
  - directional query 不再因为“snapshot 数量 > 1”被机械阻断
  - 现在看的是 `source_snapshot_selection.mode` / aggregate coverage proof
  - `all_history_snapshots` 这类历史并集仍不解锁 population-default reuse
  - `preferred_snapshot_subset` 这类显式聚合子集，在 lane coverage 完整时可解锁 population-default reuse
- 已完成验证：
  - `tests/test_planning_modules.py`
  - `tests/test_workflow_explain.py`
  - `tests/test_pipeline.py`
  - `tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_hosted_explain_dry_run_matrix_covers_reference_regressions`
  - `tests/test_hosted_workflow_smoke.py::HostedWorkflowSmokeTest::test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs`
  - 全仓 `pytest -q`
  - repo-configured `mypy`
  - 当前均已转绿

### PG former/current 物理分表 cutover、job/results 默认瘦身与全仓回归转绿

- 已完成 PG `acquisition_shard_registry` 的 former/current 物理分表 cutover：
  - live writer、SQLite->PG sync、direct-stream sync、PG->PG migration、PG snapshot export 现统一收口到：
    - `acquisition_shard_registry_current`
    - `acquisition_shard_registry_former`
  - 逻辑名 `acquisition_shard_registry` 现在作为 compatibility view 保留给上层读取契约
  - 不再把“单表 + lane contract”当成 PG 终态
- `/api/jobs/{job_id}` 现改为 summary-first 默认返回：
  - 默认只返回 job summary / status / 轻量 request preview
  - full `events` / `intent_rewrite` 需显式 `?include_details=1`
  - 这与 `/api/jobs/{job_id}/results` 之前的 summary-first 改动配套，进一步收掉前端 polling 的重 payload
- 补充并固定了几类新增测试契约：
  - PG former/current split upsert contract
  - `/api/jobs/{job_id}` summary-only default + opt-in detail
  - hosted smoke 中真正需要 full candidates 的调用显式 `?include_candidates=1`
- 顺手修掉了几处跨环境路径漂移：
  - `harvest_connectors.py` 不再把 macOS `/var` 强制提前解析成 `/private/var`
  - `service_daemon.py` 生成 systemd 单元时不再把工作目录强行 `.resolve()`
- 已完成验证：
  - 全仓 `pytest -q`
  - repo-configured `mypy`
  - 相关 control-plane / results API / hosted smoke 子集
  - 当前均已转绿

## 2026-04-21

### 手工测试缺口复盘、follow-up reuse 回归补齐与 history 恢复链路入默认矩阵

- 这轮把“为什么 simulate / explain / 已有 E2E 没挡住后续手测问题”正式沉淀进：
  - `docs/TESTING_PLAYBOOK.md`
- 核心结论已明确：
  - 之前更多在测“单次 workflow 能否完成”
  - 手工暴露的问题则集中在“workflow 写回后，下一次 explain / 下一次结果恢复 / 历史打开是否仍正确”
  - 因而必须把 `persisted state` 当成默认断言对象，而不是只看当次 API 返回
- 新增两条默认 automated regression：
  - `test_hosted_simulate_reuse_queries_preserve_follow_up_planning_contract`
    - 先跑完整 reuse workflow，再做 follow-up explain
    - 覆盖 `Reflection AI Post-train` 与 `OpenAI Reasoning`
  - `test_hosted_simulate_completed_history_round_trip_exposes_results_recovery`
    - 断言 `history_id -> results -> /api/frontend-history/{history_id}` 恢复链路稳定
- `src/sourcing_agent/regression_matrix.py` 也同步收口：
  - `hosted-workflow-smoke-focus` 默认把上述两条用户链路一起纳入
  - `orchestrator / results / history` 相关改动现在也会命中这组 smoke，而不再只跑 explain/API contract
- 当前测试体系已明确把以下模式作为后续默认原则：
  - reuse / delta planning 改动必须至少有一条 `run once -> persisted -> explain again`
  - history/results 相关改动必须至少有一条 `history_id -> completed -> recovery`
  - 本地资产复用 query 需要独立 timing budget，不再混在 large-org live 路径里看

### Mac migration prep、full snapshot lane 与 ECS access playbook

- 已把“从当前 Linux/WSL 虚拟机迁到另一台 Mac”收成正式文档与脚本：
  - `docs/archive/MAC_DEV_ENV_MIGRATION.md`
  - `scripts/prepare_mac_migration_bundle.sh`
- 迁移语义现在明确拆成两条：
  - portable migration bundle
    - 目标是让 Mac 侧快速恢复为可运行开发环境
  - full local snapshot
    - 目标是尽可能完整保留旧工作树、runtime、`runtime/secrets/`、完整 `~/.codex` 与取证上下文
- `prepare_mac_migration_bundle.sh` 现在支持：
  - `--stage-repo`
  - `--stage-repo-full`
  - `--stage-codex`
  - `--stage-codex-full`
  - `--stage-full-snapshot`
  - `--include-secrets`
  - `--include-codex-auth`
  - `--codex-session-id <id>`
- 本地 PG 发现机制已补上 env-file 入口：
  - `.local-postgres.env`
  - `.local-postgres/connection.env`
  - 不再只依赖 Linux/WSL 里的 `.local-postgres/{extract,data}`
- 当前本地已确认：
  - `~/.codex/sessions/` 中存在 `019d6630-2137-7f70-b742-43f979b8207b`
  - `~/.codex/state_5.sqlite` 的 `threads` 表里也存在该 session id
  - 因此如果完整迁移 `~/.codex`，继续 `codex resume 019d6630-2137-7f70-b742-43f979b8207b` 的成功率会明显高于只拷 `history.jsonl`
- 这轮还新增了一份可重复使用的 ECS 连接文档：
  - `docs/ECS_ACCESS_PLAYBOOK.md`
  - 统一沉淀 SSH config、端口转发、`rsync/scp`、远端 health probe、最小重启路径
- 当前推荐的传输优先级也已写明：
  - 1. 机器直连 `rsync/scp`
  - 2. 外接 SSD / 局域网共享盘
  - 3. OSS / R2 / S3 等对象存储中转
  - 一般不需要为了本地开发环境迁移专门走 OSS；只有在两台机器无法直接连通，或你更希望云端中转时才采用
- 已把“全量迁移版”的标准 resync 清单补进：
  - `docs/archive/MAC_DEV_ENV_MIGRATION.md`
  - 覆盖 source-side freeze、full snapshot 生成、`rsync` 直传、Mac 侧 restore、以及 `codex resume` 校验


## 2026-04-13

### OpenAI hosted rerun 跑通、search-seed lane merge 修复与云端 canonical bundle 补齐

- 已修复一个直接影响 hosted live run 的 acquisition 状态问题：
  - `scoped_search_roster` 的 current lane 与 former lane 之前共用单个 `search_seed_snapshot`
  - former lane 会覆盖 current lane，导致 `95 + 44` 最终只剩 `80`
  - 现已改成：
    - current/former search-seed snapshot 合并
    - 合并结果写回 `search_seed_discovery/entries.json + summary.json`
    - hosted recovery / restore 看到的也是同一份 merged snapshot
- 已补定向回归测试：
  - former search-seed merge
  - 方向型 scoped search 在模型弱化时仍保留 `functionIds=[24,8]`
  - 不回退到 `job_titles`
- 已完成一条真实 hosted OpenAI workflow 复跑：
  - `job_id=49044c9afdd2`
  - `snapshot_id=20260413T140350`
  - 不再手动 `execute-workflow`
  - 自动完成：
    - `linkedin_stage_1`
    - `stage_1_preview`
    - `public_web_stage_2`
    - `stage_2_final`
- 这次 OpenAI hosted rerun 的关键结果：
  - current lane `94`
  - former lane `44`
  - merged stage-1 candidate base `130`
  - stage-1 preview `36 matches`
  - final `40 matches`
- 已导出并开始同步新的 canonical cloud bundle：
  - `company_snapshot_openai_20260413t140350_20260413T061157Z`
  - `sqlite_snapshot_sourcing_agent_db_20260413T061157Z`
- 文档口径已继续统一：
  - 服务器恢复默认只认 `import-cloud-assets`
  - hosted 默认执行路径只认 `serve + run-worker-daemon-service`
  - `download-asset-bundle + restore-*`、`execute-workflow` 退回为排障动作

## 2026-04-12

### Canonical cloud bundle catalog 与 intent_axes 执行层下沉

- 已把服务器恢复默认基线收敛为：
  - 1 个全局 `sqlite_snapshot`
  - 每个 canonical company 1 个 `company_snapshot`
- 已新增 canonical bundle 清单文档：
  - `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`
  - 记录实际 bundle id、恢复顺序、去重策略与 Google clean snapshot 替换说明
- 已把 `intent_axes` 从展示层继续下沉到执行入口：
  - `JobRequest.from_payload` 现在可直接 materialize `intent_axes`
  - plan review instruction inference 会消费 `intent_axes`-only request payload
  - review apply / target company backfill 也会先 materialize `intent_axes`
- 已进一步把 planning/acquisition 内部消费切到统一 `intent_view`：
  - retrieval strategy / structured filters / filter layers / criteria summary / open questions 不再各自散读老字段
  - acquisition 对 `force_fresh_run / reuse_existing_roster / run_former_search_seed` 的判断也开始统一走 `intent_view`
  - 修复了一个 request merge 回归：
    - 模型直接返回的 `scope_disambiguation` 现在会被真正合入 request payload
    - 不会再被后续规则推断静默覆盖
- 已补回归测试，确保“模型只返回 intent_axes”时，planning / review / acquisition 仍能生成正确执行字段，而不只是 preview 好看。
- 已修一处 preview / request 漂移：
  - 过去 `request_preview.keywords` 可能缺少 acquisition strategy 真正执行的扩展关键词
  - 现在 `plan_workflow / start_workflow` 返回的 `request` 与 `request_preview` 都会对齐 execution-aligned request
  - 例如 Google `multimodal + Pre-train` 这类 query，会明确把 `Pre-train` 反映到返回 payload，而不再只存在于内部 `filter_hints.keywords`
- 本地与 OSS 远端 canonical bundle index 当前都已收敛为：
  - 7 个 `company_snapshot`
  - 1 个 `sqlite_snapshot`
  - legacy `company_handoff` 与旧 Google/SQLite bundle 已从索引删除

### Effective request payload 贯通、规则检索语义澄清与 Excel intake 补档

- 已把 `intent_axes -> intent_view -> effective request payload` 继续下沉到执行末端：
  - `build_effective_request_payload(...)` 已进入 acquisition worker payload、search seed discovery、retrieval ranking
  - `seed_discovery.py` 不再各自散读老扁平字段，而是消费统一 execution payload
  - `scoring.py` 与 `semantic_retrieval.py` 也开始直接按 `intent_view` 解释 `target_company / organization_keywords / employment_statuses / must_have_*`
- 已明确当前 retrieval 的真实形态，避免文档误导：
  - 默认 final ranking 不是 LLM-driven rerank
  - `scoring.py` 仍是规则/lexical/confidence 主链
  - `semantic_retrieval.py` 默认走本地 sparse semantic
  - external semantic provider 只在 `allow_high_cost_sources` 打开时参与
- 已补回归测试，覆盖新的执行语义：
  - `tests/test_planning_modules.py`
  - `tests/test_pipeline.py`
  - `tests/test_scoring.py`
  - `tests/test_semantic_retrieval.py`
- 已开始把 Excel intake 纳入当前文档口径：
  - 当前支持 `intake-excel / continue-excel-intake`
  - 上传表格后可先做 schema 识别、本地去重、manual review continuation，再决定是否触发新 LinkedIn fetch

## 2026-04-11

### Hosted 默认路径文档化、前端禁区明确化与 GitHub 上传边界收束

- 已把云端默认执行路径明确为 `serve + run-worker-daemon-service`：
  - README 新增 hosted 默认运行章节
  - operations playbook 新增云端最小启动组合与健康检查
  - 强调“手工 execute-workflow 续跑”只用于排障，不作为常规运行方式
- 已补前端边界，避免直接读 runtime 文件：
  - `docs/FRONTEND_API_CONTRACT.md` 明确前端只消费 API contract
  - 明确禁止把 `runtime/company_assets/*`、`runtime/jobs/*` 作为前端主数据源
  - 阶段反馈统一以 `workflow_stage_summaries` 为准
- 已新增集中指南：
  - `docs/HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md`
  - 汇总云端启动、前端禁区、GitHub 上传范围、上线前检查
- 已在文档索引与 README 文档地图挂载新入口：
  - `docs/INDEX.md`
  - `README.md`
- 已补 GitHub 推送边界说明（降部署成本）：
  - 建议提交 `src/tests/docs/contracts/configs-example/README/PROGRESS`
  - 不提交 `runtime/**`、`secrets/**`、raw assets 与本机缓存
  - 明确临时 live smoke 配置不应作为默认提交资产

### 两阶段工作流阶段总结透传、真实 smoke test 收敛与前端契约补齐

- 已把阶段性 workflow summary 固化为稳定返回结构：
  - `workflow_stage_summaries`
  - 同时出现在：
    - `GET /api/jobs/{job_id}/progress`
    - `GET /api/jobs/{job_id}/results`
  - 当前固定阶段顺序：
    - `linkedin_stage_1`
    - `stage_1_preview`
    - `public_web_stage_2`
    - `stage_2_final`
- snapshot 侧现也会同步落盘阶段总结文件：
  - `runtime/company_assets/{company}/{snapshot_id}/workflow_stage_summaries/*.json`
  - 这样前端不必直接读 snapshot 文件，但运维/离线调试仍可审计
- 已修复一个关键回归点：
  - 后台 harvest/search reconcile 之前会覆盖或丢失阶段 marker
  - 现在 `linkedin_stage_1 / stage1_preview / public_web_stage_2 / analysis_stage_mode` 会在 reconcile 后保留
- 已完成真实 live smoke test 验证：
  - Humans& 任务已在“不手动 execute-workflow”的情况下端到端完成
  - 最新稳定样例：
    - `job_id=b752d176a669`
    - `snapshot_id=20260411T145117`
- 前端 contract/example 已同步更新：
  - `contracts/frontend_api_contract.ts`
  - `contracts/frontend_api_contract.schema.json`
  - `contracts/frontend_api_adapter.ts`
  - `contracts/frontend_react_hooks.example.tsx`
  - 现在前端可直接拿 typed `workflow_stage_summaries`，并使用 stage helper 渲染阶段卡片
- 定向回归测试已通过：
  - `single_stage_workflow_still_publishes_stage_progress_markers`
  - `background_harvest_prefetch_reconcile_preserves_stage_progress_markers`
  - `two_stage_workflow_publishes_stage1_preview_and_continues_public_web_stage2_by_default`
  - `http_api_smoke`

## 2026-04-10

### GitHub Dev 差异审阅、操作教程补齐与推送准备

- 已完成本地工作区相对 `origin/dev` 的结构化审阅，并沉淀为可复盘文档：
  - `docs/archive/GITHUB_DEV_DIFF_REVIEW_2026-04-10.md`
  - 覆盖 workflow orchestration、acquisition/harvest、profile registry、query rewrite、frontend contract 五大变更面
- 已补“当前怎么调用 + 怎么追踪进度 + 怎么恢复执行”的实操教程：
  - `docs/WORKFLOW_OPERATIONS_PLAYBOOK.md`
  - 统一说明 CLI 与 API 的标准调用路径：
    - `plan -> review-plan -> start-workflow -> show-progress/show-workers -> show-job/show-trace`
  - 补充 query dispatch 去重策略与高成本 query 的 plan-only 审查建议
- 已将上述新文档挂到 canonical 入口：
  - `docs/INDEX.md`
  - `README.md`（子项目文档地图）
- 已对 Google keyword-first 路径做参数层修正（plan-only 复核通过）：
  - 关键词提取扩大到 `Veo / Nano Banana / vision-language / video generation / multimodality`
  - `keyword_priority_only` 下的 `search_seed_queries` 改为方向词优先，不再回落到泛化 `... Research Researcher` 模板
  - large-org keyword 模式下避免把 `job_titles` 强塞进 former profile-search 过滤，降低误收敛风险
- 当前推送前状态：
  - 文档入口、进度文档、变更评审文档已补齐
  - 代码侧已有较大规模改动，建议以 PR 方式先推送到 `dev`，通过 CI 与人工 review 后再合并

## 2026-04-08

### Canonical Asset Views、Facet Hard Filters 与文档体系收束

- 已把 Thinking Machines Lab 的分析入口收束为单一 canonical snapshot：
  - `runtime/company_assets/thinkingmachineslab/20260407T181912`
  - 并显式拆成：
    - `canonical_merged`
    - `strict_roster_only`
- retrieval 现已支持显式资产视图与更稳的功能过滤：
  - `asset_view`
  - `must_have_facet / must_have_facets`
  - `must_have_primary_role_bucket / must_have_primary_role_buckets`
- primary-role-bucket 过滤现已贯穿：
  - request normalization
  - planning summary
  - retrieval hard filter
  - baseline request-family matching
- 已收紧 retrieval scoring：
  - `notes` 不再作为 primary role bucket 查询的主要 lexical / semantic 命中来源
  - 这样 `ops / recruiting / infra_systems / multimodal` 这类切片不会再被 acquisition boilerplate 大面积污染
- Thinking Machines Lab strict-view live validation 已更新：
  - `total_matches=9`
  - `manual_review_queue_count=0`
  - top matched fields 只剩：
    - `work_history`
    - `derived_facets`
    - `focus_areas`
- 已补新的当前态文档：
  - `docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md`
  - `docs/archive/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md`
  - `docs/DATA_ASSET_GOVERNANCE.md`
  - `docs/SERVICE_EVOLUTION_STRATEGY.md`
- 已开始清理 GitHub-facing 文档入口：
  - 新增 `docs/INDEX.md` 作为 canonical docs map
  - 根目录 README / ONBOARDING / 子项目 README 已开始从 dated handoff 迁回 current-state docs
  - canonical docs 中的绝对本机路径链接和 `cd '/home/...'` 命令已改成 GitHub-friendly 相对路径或通用路径

## 2026-04-07

### Harvest Auth 恢复、Browser Search Provider 与 PDF 强解析

- 已恢复本机真实 provider 配置：
  - `runtime/secrets/providers.local.json` 现已重新写回：
    - Claude relay
    - Qwen / DashScope
    - Harvest / Apify
    - R2 object storage
  - 同时补了本机 `search_provider` 配置：
    - `serper_google -> google_browser -> duckduckgo_html`
    - `google_browser` 默认使用：
      - `runtime/vendor/playwright/node_modules`
      - `runtime/vendor/playwright-browsers`
      - `runtime/vendor/npm-cache`
  - 所有真实配置继续只保留在本机 secret/runtime 层，不进 Git
- 已重新验证 Harvest auth：
  - 新 Apify token 已真实可用
  - `linkedin-company-employees` live smoke test 返回 `STATUS 201`
  - `linkedin-profile-scraper` live smoke test 已成功返回 `Saurabh Garg` 的 profile detail
  - 因此早先的 `401 user-or-token-not-found` 已确认是旧 token 状态，不再是当前本机配置
- 已纠正 `linkedin-profile-scraper` 的关键 schema 结论：
  - 之前错误地把字段写成了 `profileUrls`
  - 当前经 live 验证确认可用字段是：
    - `urls`
    - `publicIdentifiers`
    - `queries`
    - `profileIds`
  - 当前代码已改回 `urls`
  - 另外已经确认：
    - vanity URL 可用
    - opaque LinkedIn URL 可用
    - `profileIds` 可用
- 已重新验证 Thinking Machines Lab 的 profile completion 主链：
  - `complete-company-assets --profile-detail-limit 6`
  - `fetched_profile_count=6`
  - 说明 Harvest profile-scraper 已重新进入真实补全链路
  - 当前 `completed_candidates=0` 的原因已收敛为 candidate/profile matching 仍偏保守，而不是 auth/schema 问题
- 已新增 browser-based Google search provider：
  - 新增 `scripts/google_search_browser.cjs`
  - 新增 `google_browser` provider，已接入 `src/sourcing_agent/search_provider.py`
  - `SearchProviderSettings` 已补：
    - `google_browser_node_modules_dir`
    - `google_browser_browsers_path`
    - `google_browser_script_path`
    - `google_browser_headless`
    - `google_browser_locale`
  - 当前 provider 采用真实 Chromium/Playwright 路线，而不是 requests 模拟 Google Search
  - 当前 WSL 环境下仍有一个明确阻塞：
    - browser launch 缺少系统共享库 `libnspr4.so`
    - 因此 browser lane 已实现，但本机 live Google query 暂未真正跑通
- 已补 PDF 多级文本提取链：
  - `pypdf`
  - `pdfminer.six`
  - `pdftotext`
  - `OCR (pdftoppm + tesseract)` 预留
  - 当前实现会自动选择文本质量最好的提取结果
  - `runtime/vendor/python` 已安装本机可复用的 `pdfminer.six`
  - 代码现会自动从 `runtime/vendor/python` 发现本地增强依赖，不进 Git
- 已对 Horace He 的 PDF 做真实验证：
  - 当前最佳方法为 `pdfminer`
  - 提取文本约 `3538` 个字符
  - 已成功拿到 Cornell 教育信息、Facebook/Google 工作经历和技能字段
  - 这说明 PDF 强解析链已从“只保留链接”升级到“可产出结构化草稿”
- 已补相关测试覆盖：
  - `tests/test_harvest_connectors.py`
  - `tests/test_search_provider.py`
  - `tests/test_document_extraction.py`

### HarvestAPI 调用方法沉淀与 Corner Case Exploration 升级

- 已补 Harvest 调用手册：
  - 新增 `docs/HARVESTAPI_PLAYBOOK.md`
  - 系统记录了三个 actor 的 console/readme 链接：
    - `linkedin-profile-scraper`
    - `linkedin-profile-search`
    - `linkedin-company-employees`
  - 系统记录了当前项目里真正可复用的 payload 规范、former/current 推荐调用顺序、Thinking Machines Lab live 结论和已知坑
- 已修正 `HarvestProfileConnector` 的关键 payload 错误：
  - 先前错误地改成了 `profileUrls`
  - 当前已根据 live 验证改回 `urls`
  - 并明确记录 `urls/publicIdentifiers/queries/profileIds` 都是当前 actor 可用入口
  - 对应测试已补到 `tests/test_harvest_connectors.py`
- 已重新确认 Harvest secret 的本地标准位置：
  - 默认开发环境放在 `runtime/secrets/providers.local.json`
  - 不再建议依赖旧项目 `api_accounts.json` 隐式发现
- 已确认一个关键调试结论：
  - `2026-04-07` 早先使用旧 token 做最小 API smoke test 时，Apify 返回了 `401 user-or-token-not-found`
  - 当前已切到新的可用 token，并完成 live 验证
  - 相关历史结论保留在 `docs/HARVESTAPI_PLAYBOOK.md`，用于说明“0 results”不一定是 payload 问题，也可能是 auth 问题
- 已升级 `analyze_page_asset` 输出 schema：
  - 新增：
    - `education_signals`
    - `work_history_signals`
    - `affiliation_signals`
    - `document_type`
  - deterministic fallback 现已能从 `text_blocks` 中做基础规则抽取
  - Claude / Qwen / 其他 OpenAI-compatible provider 现在也能按同一 schema 返回结构化结果
- 已补统一文档抽取模块：
  - 新增 `src/sourcing_agent/document_extraction.py`
  - 统一处理：
    - HTML homepage / CV
    - Google Docs CV
    - PDF resume
  - 新增通用能力：
    - `analyze_remote_document`
    - `build_candidate_patch_from_analysis`
    - `build_candidate_patch_from_signal_bundle`
    - `extract_pdf_text`
- 已补底层二进制资产能力：
  - `AssetLogger` 新增 `write_bytes`
  - `web_fetch.py` 新增 `fetch_binary_url`
  - 这使 PDF 原始资产、提取文本和 analysis input/output 都能按统一资产纪律落盘
- 已升级 `manual_review_resolution`：
  - 人工 review 提供的 homepage / Google Docs / PDF source link 现在不仅会保存为 evidence
  - 还会自动进入文档分析
  - 并将 education/work history/affiliation 草稿回写到 candidate
  - 对应测试已补到 `tests/test_manual_review_resolution.py`
- 已升级 `exploratory_enrichment`：
  - 搜索结果页命中的 homepage 现在会继续跟进其 `resume_urls`
  - 若命中 Google Docs CV 或 PDF resume，会进一步抽取结构化信号
  - exploration merge 阶段已能把这些信号补进 candidate，而不再只写 notes / media_url
- 已做真实小样本验证：
  - Jeremy Bernstein 的 Google Docs CV 现在可自动抽出 education/work history/Thinking Machines Lab affiliation 草稿
  - Horace He 的 PDF 现在已接入 `resume_url -> raw pdf -> extracted_text -> analysis` 链路
  - 当前 `pdfminer` 已可对 Horace 这份 PDF 抽出有效正文，`pypdf` 不是这份 PDF 的最佳方法
  - OCR 仍作为后续增强项保留，用于真正的图片型 PDF
- 已更新 onboarding/readme：
  - `README.md`
  - monorepo 根 `README.md`
  - 新增把 `docs/HARVESTAPI_PLAYBOOK.md` 纳入接手必读路径

## 2026-04-06

### Thinking Machines Lab 资产补全与可复用候选文档提炼

- 已修正 Harvest profile parser 的两个关键问题：
  - `parse_harvest_profile_payload` 现在会从 `firstName + lastName` 回填 `full_name`
  - `currentPosition.companyName` 现在会参与 `current_company` 提取
  - 这避免了已有 LinkedIn URL 的 profile detail 因姓名或 current company 解析不全而无法进入后续匹配
- 已修正 known-URL profile 匹配策略：
  - 若 `candidate.linkedin_url` 与 `profile.profile_url` 一致，且姓名一致，则优先视为同一个人
  - 不再把“必须先匹配 company 字段”作为已知 URL detail enrichment 的唯一条件
- 已补公司级历史资产物化层：
  - 新增 `candidate_artifacts.py` 中的 `materialize_company_candidate_view`
  - 会聚合同一公司所有历史 snapshot 的 `candidate_documents.json` 与当前 SQLite 主库
  - 不再因为后续 former workflow 覆盖主库，就丢失 earlier current roster
- 已补公司级后处理资产补全入口：
  - 新增 `complete-company-assets`
  - 补全流程现在是：
    - materialize company history
    - sync candidates/evidence back into SQLite
    - known-URL profile completion
    - unresolved exploration
    - follow-up profile completion
    - rebuild normalized/reusable artifacts
- 已补 normalized/reusable candidate artifact 提炼：
  - `build-company-candidate-artifacts` 现在会输出：
    - `materialized_candidate_documents.json`
    - `normalized_candidates.json`
    - `reusable_candidate_documents.json`
    - `manual_review_backlog.json`
    - `profile_completion_backlog.json`
  - 新增 `profile_completion_backlog`，用于显式管理“已有 LinkedIn URL 但尚未拿到 full detail”的候选人
- Thinking Machines Lab 当前物化结果已更新：
  - snapshot: `20260406T172703`
  - `candidate_count=55`
  - `evidence_count=75`
  - `status_counts={current: 29, former: 25, lead: 1}`
  - `manual_review_backlog_count=2`
  - `profile_completion_backlog_count=25`
- 当前 backlog 的含义已明确：
  - `manual_review_backlog`
    - `Horace He`: unresolved lead with homepage/CV evidence
    - `Tianle Li`: current candidate but missing individual LinkedIn URL
  - `profile_completion_backlog`
    - 主要是 Thinking Machines Lab former candidates
    - 当前设备上 `Harvest profile-scraper` 未启用，因此被保留为待补全资产，而不是被静默忽略
- 已确认当前设备的 provider 状态：
  - `harvest.profile_scraper.enabled = false`
  - `harvest.profile_search.enabled = false`
  - `harvest.company_employees.enabled = false`
  - 因此当前阶段依赖：
    - 已有 live snapshot / provider cache / manual review asset
    - 而不是新发起 live Harvest detail requests
- 已补测试覆盖：
  - `tests/test_harvest_connectors.py`
  - `tests/test_candidate_artifacts.py`
  - `tests/test_company_asset_completion.py`
  - 针对性测试通过

### GitHub 同步与跨设备接手

- 已将 `Sourcing AI Agent Dev/` 整理为 monorepo 根目录，准备同步到 private GitHub repo：
  - 代码、文档、示例配置、去敏后的历史方法论资产进入 Git
  - `runtime/`、`providers.local.json`、历史 `api_accounts.json`、zip/tar 原包不进入 Git
- 已补根目录 onboarding/sync 文档，降低后续在公司电脑或其他 AI 环境下继续开发的接手成本：
  - 根目录 `README.md`
  - 根目录 `ONBOARDING.md`
  - 根目录 `GITHUB_SYNC_PREP.md`
  - `docs/DEVELOPMENT_GUIDE.md`
- 已明确 GitHub repo 不是 runtime asset storage：
  - live payload、company snapshots、profile raw assets、manual review raw assets 需要单独安全存储
  - 换设备后应恢复 secrets 和必要 runtime 子集，而不是依赖 Git 自动同步
- 已补跨设备可恢复的安全资产同步设计：
  - 新增 `docs/CROSS_DEVICE_SYNC.md`
  - 明确 GitHub / secret manager / cloud object storage / local runtime 四层分工
  - 明确哪些资产应该 durable sync，哪些只应本地保留
  - 明确后续实现切入点：
    - `asset_bundle_manifest`
    - `export-asset-bundle / restore-asset-bundle`
    - exported SQLite snapshot
    - cloud asset registry
- 已将跨设备恢复方案落成可执行命令：
  - 新增 `asset_sync.py`
  - CLI 新增：
    - `export-company-snapshot-bundle`
    - `export-company-handoff-bundle`
    - `export-sqlite-snapshot`
    - `restore-asset-bundle`
  - 当前 bundle 形态为：
    - `bundle_manifest.json`
    - `export_summary.json`
    - `payload/<runtime_relative_path>`
  - 可直接作为后续 object storage durable sync 的上传单位
- 已完成 Thinking Machines Lab handoff bundle 的真实导出与恢复验证：
  - handoff bundle:
    - `runtime/asset_exports/company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z/`
    - `569` files
    - `29346219` bytes
  - sqlite snapshot:
    - `runtime/asset_exports/sqlite_snapshot_sourcing_agent_db_20260406T125538Z/`
  - restore smoke test:
    - `/tmp/sourcing-agent-restore-smoke`
- 已完成 object storage sync 第一版实现：
  - 新增 `object_storage.py`
  - 当前 provider:
    - `filesystem`
    - `s3_compatible`
  - 当前 CLI 新增：
    - `upload-asset-bundle`
    - `download-asset-bundle`
    - `restore-sqlite-snapshot`
  - 当前本地 smoke test 已跑通：
    - handoff bundle upload
    - handoff bundle download
    - sqlite snapshot upload
    - sqlite restore helper
- 已完成真实 Cloudflare R2 配置接入与 live 验证：
  - 当前使用 Cloudflare R2 的 S3-compatible endpoint，而不是 dashboard 链接
  - `sqlite_snapshot` 已成功真实上传到 R2
  - 同一 `sqlite_snapshot` 已成功从 R2 下载回来
  - 说明当前：
    - endpoint
    - region=`auto`
    - access key / secret key
    - SigV4 签名逻辑
    都可用
  - Thinking Machines Lab `company_handoff` 大 bundle 已完成真实 durable sync 闭环：
    - real upload:
      - `bundle_id=company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z`
      - `571` uploaded objects
      - `29570059` bytes
      - `max_workers=8`
    - real download:
      - `571` downloaded objects
      - `29570059` bytes
      - `max_workers=8`
    - local restore from downloaded bundle:
      - restored to `/tmp/r2-tml-handoff-restore`
      - `569` runtime files restored
- 已完成 object storage sync 第二版优化：
  - `upload-asset-bundle / download-asset-bundle` 已支持并发 `max_workers`
  - 已补 per-object retry/backoff
  - `S3CompatibleObjectStorageClient` 已改成 thread-local session
  - 已补 local/remote sync metadata：
    - local: `runtime/object_sync/bundle_index.json`
    - local: `runtime/object_sync/runs/*.json`
    - remote: `indexes/bundle_index.json`
    - remote: `indexes/sync_runs/*.json`
- 已补恢复教程：
  - 新增 `docs/archive/RECOVERY_TUTORIAL.md`
  - 明确同机换账号、新机器恢复、bundle 上传下载、SQLite 恢复的具体命令
- 已明确 Thinking Machines Lab retrospective 当前处于“已完整复盘、待继续补全资产”状态：
  - 当前没有已完成但未落盘的关键测试结论
  - 后续仅在新增 TML live execution 或新增资产时继续更新
- 已补结构化交接文档：
  - 新增 `docs/archive/HANDOFF_2026-04-06.md`
  - 用结构化 handoff 取代保留冗长聊天记录，降低新 AI session / 新设备接手成本
- 已明确后续服务器化/云端资产化方向：
  - 服务运行位置迁移到长期运行的服务器
  - provider secrets 由 secret manager 注入
  - 高价值 runtime 资产进入 object storage durable storage
  - 相似用户意图优先复用既有资产，再做 plan / filtering / rerank / presentation
- 已明确当前 GitHub repo 已足够作为代码仓库：
  - 不再建议创建第二个“包含 secrets 和 runtime 的完整 GitHub repo”
  - secrets 与高价值数据资产继续采用 secret manager + object storage 分层存储
- 已新增显式待办清单：
  - `docs/NEXT_TODO.md`
  - 用于切换账号/设备后继续执行：
    - Thinking Machines Lab 后续资产补全
    - normalized asset / reusable candidate artifact 提炼
    - object storage 的 resume/progress 继续优化
    - 服务器化/云端资产化方向继续推进

### 已记录待办

- `worker daemon` 的真实 systemd 安装与启用延后到正式服务器环境：
  - 当前开发环境是 WSL，本地可生成 unit 并验证 service 壳层逻辑
  - 但不把 `/etc/systemd/system` 安装视为当前开发阶段目标
  - 等正式上线到长期运行的 Linux 服务器后，再执行安装、`enable --now` 和运维接入
- 新增 source family 的产品化流程需要保持“先交互、后规划、再开发/测试”的阶段门：
  - 先确认用户的目标、偏好、覆盖范围、执行深度、成本容忍度
  - 再进入 source onboarding review 和具体链路实现
  - 新链路必须继续满足数据资产意识、可审计性、可扩展性要求

### 已记录的产品决策

- 明确将当前项目继续泛化为通用 sourcing workflow，而不是 Anthropic / xAI 特例扫描器
- 已实现 `AcquisitionStrategyCompiler`
  - plan 阶段可输出 `full_company_roster / scoped_search_roster / former_employee_search / investor_firm_roster`
  - 明确低成本优先的 slug resolution 顺序：relation check / web search -> LinkedIn people search API -> profile detail API
  - 对大公司查询会给出 scope confirmation points，例如 `Google Gemini` 默认建议缩到 `Google DeepMind / Gemini`
  - 已编码 HarvestAPI 成本规则：
    - 已知 URL 时默认 route 到 `linkedin-profile-scraper`
    - `linkedin-company-employees` 只适用于批量场景
    - `linkedin-profile-search` 仅作为低成本 web search 不足时的 fallback
- 已实现 `PublicationCoveragePlanner`
  - plan 阶段可输出 publication source families、seed queries、LLM extraction role、fallback steps
  - 当前将 official research / engineering / blog / docs / publication platforms 作为标准 coverage family
- 记录后续 Thinking Machines Lab 端到端验证的高质量 connector 策略：
  - HarvestAPI LinkedIn profile search，用于按意图定向检索
  - HarvestAPI LinkedIn company employees，用于获取高质量公司 roster
  - HarvestAPI LinkedIn profile scraper，用于按 LinkedIn URL 获取 full profile detail
- 明确 HarvestAPI 使用策略：
  - 默认不需要 email
  - profile scraper 默认 `Full` 模式，避免因 detail 不完整而重复调用
  - 可利用 `moreProfiles` 扩展相同公司经历或相近背景的人选
  - 因成本较高，仅在最终验证或人工确认后调用
- 明确后续产品化重点：
  - 早期阶段要根据用户意图制定 acquisition strategy，而不只是固定 company roster
  - 后期 retrieval 要升级为多层过滤与多置信度结果输出
  - 高价值 LinkedIn Profile 与最终 result artifact 未来进入云端存储，本地保留 workflow 执行态与调试缓存
- 明确 publication enrichment 的升级方向：
  - 当前 `arXiv affiliation -> author / acknowledgement -> co-author` 只能作为 baseline
  - 后续需要让 LLM 先做 source coverage planning，确保 coverage 不只包含 arXiv，也包括 official research / engineering / blog / docs 等 source families
  - 对弱结构化的 author / acknowledgement / contributor 文本，允许 LLM 参与抽取与归一化
  - 仍保留 deterministic fallback，避免模型不可用时中断工作流
- 明确 retrieval 产品策略：
  - 默认输出 `hybrid` 结果
  - 同时支持 `structured_only` 与 `semantic_heavy`
  - 结果按 `high confidence / medium confidence / lead only` 分层呈现，供用户按 precision / recall 需求选择
- 已完成执行层改造第一步：
  - acquisition runtime 不再只会执行 company roster，而是会按 strategy 分叉
  - `scoped_search_roster / former_employee_search` 已接入 low-cost search-seed acquisition
  - enrichment 的 slug/profile 解析已改为 `web-first, paid-fallback`
- 已补 Harvest profile-scraper adapter 基础层：
  - 仅在已知 LinkedIn URL 且配置了 Apify token 时启用
  - 默认 `Full`
  - 默认不抓 email
  - 作为 high-cost provider 中成本最低的一档 detail connector
- 已补 publication lead 的 second-pass enrichment：
  - publication / acknowledgement 发现的 lead 不再只停留在 `lead` 文档层
  - 在剩余 enrichment budget 内，会继续尝试解析 LinkedIn profile
  - 若 profile 验证通过，可将 `lead` 升级为 `employee` 或 `former_employee`
- 已补 `docs/DATA_ARCHITECTURE.md`
  - 明确当前 canonical schema、snapshot 资产类型、审计链路
  - 明确仍缺 `confidence persistence / criteria self-evolution / cloud asset registry`
- 已补 `further exploration` 模块：
  - 对 unresolved lead 做低成本网页探索，而不是默认直接花费 LinkedIn search 成本
  - 可从网页、X、GitHub、个人主页、CV 中回收 profile 线索
  - 若探索后发现 LinkedIn URL，可重新进入 detail enrichment
- 已补 criteria evolution persistence 第一版：
  - 新增 `criteria_feedback` 和 `criteria_patterns`
  - 新增 API/CLI 入口写入人工 review feedback
  - `accepted_alias` 这类 feedback 已可影响后续 scoring
- 已补 criteria audit persistence：
  - 新增 `criteria_versions`
  - 新增 `criteria_compiler_runs`
  - plan / workflow / retrieval 都会留下 criteria 编译版本与 provider 记录
- 已补 confidence persistence：
  - `job_results` 新增 `confidence_label / confidence_score / confidence_reason`
  - retrieval artifact 和持久化结果都会带上 `high / medium / lead_only`
- 已补 model-agnostic page analysis：
  - `further exploration` 不再把页面摘要能力写死为 Qwen
  - 当前通过通用 `analyze_page_asset` 接口接入模型，保留 deterministic fallback
- 已显式固化 raw-first / compact-context 规则：
  - 外部 API 返回、网页原文、analysis input/output 默认先落盘
  - 模型默认只读取 compact excerpt，而不是直接吃整页 HTML 或大体积 raw payload
  - `.pdf` 等 binary-like URL 默认不直接送入页面分析上下文
- 已补 centralized asset logger：
  - 新增 `asset_logger.py`
  - snapshot 下统一维护 `asset_registry.json`
  - company roster、search seed、LinkedIn profile、Harvest profile、publication raw page、exploration page、analysis input/output 统一登记
- 已补跨进程 worker recovery daemon：
  - `agent_worker_runs` 新增 `lease_owner / lease_expires_at / attempt_count / last_error`
  - 新增 recoverable worker 扫描、claim、renew、release
  - 新增 `PersistentWorkerRecoveryDaemon`
  - `stale running` worker 现在会被当作可恢复 worker 重新进入 scheduler
  - 已补 CLI/API 控制面：
    - CLI: `show-recoverable-workers / run-worker-daemon-once / run-worker-daemon`
    - API: `GET /api/workers/recoverable / POST /api/workers/daemon/run-once`
  - 已补跨连接测试，验证 DB lease 可协调独立进程式恢复器
- 已补系统级 daemon service 壳层：
  - 新增 `service_daemon.py`
  - 支持单实例 lock、心跳状态、优雅退出
  - 支持 `run-worker-daemon-service`
  - 支持 `show-daemon-status`
  - 支持 `write-worker-daemon-systemd-unit`
  - 新增 API：`GET /api/workers/daemon/status`、`POST /api/workers/daemon/systemd-unit`
  - 新增测试 `tests/test_service_daemon.py`
- 已补 criteria auto-evolution loop 第一版：
  - feedback 写入后自动触发一次 criteria recompile
  - 新 criteria version 会保留 `parent_version_id / trigger_feedback_id / evolution_stage`
  - 新增 `/api/criteria/recompile` 与 `recompile-criteria`
- 已补 feedback -> rerun -> result diff：
  - `rerun_retrieval=true` 时会在 recompile 后自动执行 retrieval rerun
  - 新增 `criteria_result_diffs`
  - baseline job 与 rerun job 的 `added / removed / moved` 会被持久化并落成 diff artifact
- 已将 result diff 升级为双层 diff：
  - `rule_changes` 会比较 baseline / rerun 的 criteria version、pattern snapshot、request、plan
  - `result_changes` 会比较 rerun 前后的候选人进入、退出、排序与置信度变化
  - 新增 `impact_explanations`，将规则变化与结果变化合并成可读的审计解释
  - criteria version 现会保存包含 disabled pattern 在内的全量 pattern snapshot，避免事后解释被当前 active state 污染
- 已补候选人级影响归因：
  - diff 现包含 `candidate_impacts`
  - 会针对 `added / removed / moved` 候选人，归因到具体 pattern change、request change，及其对应的 matched field
  - 对 alias 这类规则变更，现可直接解释“哪条规则让哪个候选人进入结果”
- 已补 rerun gating + cost policy：
  - `rerun_retrieval` 现支持 `auto / cheap / full`
  - `auto` 会按 feedback 类型、baseline 规模和预估影响决定是否 rerun
  - `cheap` rerun 会收紧 `top_k / semantic_rerank_limit`，并强制使用 deterministic summary
  - 低信号 feedback 会被 gate 掉，避免无意义 rerun 和重复模型摘要成本
- 已补 baseline job / request-family 精确匹配：
  - rerun 不再默认选择“同公司最近完成 job”
  - 会先按 request-family signature 和 family score 匹配最接近的 baseline job
  - 仅在没有足够接近的 family match 时，才退回到 latest-company fallback
  - rerun 返回和 diff artifact 现会显式包含 `baseline_selection`
- 已补 confidence evolution：
  - `must_have_signal / false_negative_pattern` 会派生出 `must_signal + confidence_boost`
  - `exclude_signal / false_positive_pattern` 会派生出 `exclude_signal + confidence_penalty`
  - scoring 现会让这些 pattern 直接影响 `confidence_score / confidence_label / confidence_reason`
  - rerun diff 已能表现“结果未换人，但 confidence label 变化”的场景
- 已补 company-level confidence band evolution：
  - 会按同公司历史 feedback 统计 precision / recall pressure
  - 自动微调 `high / medium` band 边界，而不是只做 pattern 级加减分
  - 每次 retrieval / rerun 会把当时生效的阈值保存到 `confidence_policy_runs`
  - result artifact 现会附带 `confidence_policy`
- 已补 request-family confidence policy + time decay：
  - feedback 现会自动带上 `request_signature / request_family_signature`
  - confidence policy 默认优先按 request-family 过滤历史 feedback，而不是混用同公司所有 query
  - 老 feedback 会按时间半衰，避免旧项目经验无限累积污染当前 band
- 已补 feedback-derived auto pattern suggestion：
  - 当 feedback 带 `job_id + candidate_id` 时，会结合 `matched_fields` 与 candidate context 自动生成 pattern suggestions
  - suggestion 单独持久化为 `criteria_pattern_suggestions`，默认 `suggested`，不会直接写入 active patterns
  - `show-criteria` / `/api/criteria/patterns` / `record-feedback` 返回里现在都能看到 suggestions
- 已补 suggestion review loop：
  - 支持 `review-suggestion` CLI 和 `/api/criteria/suggestions/review`
  - suggestion 可被标记为 `applied / rejected`
  - `applied` suggestion 会写入 active patterns，并可继续触发 criteria recompile 与 retrieval rerun
- 已补 manual policy freeze / override：
  - 新增 `confidence_policy_controls`
  - 支持 `request_exact / request_family / company` 三种 scope
  - 支持 `freeze_current / override / clear`
  - retrieval 会按 `request_exact -> request_family -> company` 优先级选择 active control
  - active control 会写入 retrieval artifact，方便后续审计“这次 band 是自动 policy 还是人工锁定”
- 已补 canonical request matching for manual controls：
  - confidence policy control 创建时会先 canonicalize request payload
  - 避免 raw request 与 runtime request signature 不一致，导致 freeze/override 无法命中
- 已补 Plan Review Gate：
  - plan 阶段会生成 `plan_review_gate`
  - 对 scoped roster、investor firm roster、高成本 source 等计划，workflow 会先返回 `needs_plan_review`
  - 已新增 `plan_review_sessions`
  - review 后可把 `extra_source_families / confirmed_company_scope / allow_high_cost_sources` 写回 approved plan
- 已补 Manual Review Queue：
  - retrieval 现在会自动产出 `manual_review_items`
  - 当前会优先收集 `lead_only`、缺少 LinkedIn profile、需要人工确认 membership 的候选人
  - 已新增 manual review 的 API / CLI review 入口
- 已补 Investor Firm Roster Workflow：
  - `investor_firm_roster` 不再只是 planning strategy
  - acquisition runtime 现可基于既有 investor 资产生成 tiered firm plan
  - 会先产出 firm tiering，再归一化 full investor roster，后续再做 role/involvement filter
- 已补模块级文档与操作示例：
  - 新增 `docs/MODULES.md`
  - 新增 `configs/confidence_policy_freeze.example.json`
  - 新增 `configs/confidence_policy_override.example.json`
  - 新增 `configs/suggestion_review_apply.example.json`
  - 新增 `configs/plan_review_approve.example.json`
  - 新增 `configs/manual_review_resolve.example.json`
- 已补 `LLM-driven Search Planner`：
  - plan 现会显式生成 `search_strategy`
  - query 会被编译成具名 bundle：`relationship_web / publication_surface / public_interviews / targeted_people_search`
  - “公开访谈 / Podcast / YouTube” 这类新 sourcing 方法，已经可以先沉淀为 source family，并进入 plan review 与执行链
- 已将 search planner 接入 search-seed acquisition：
  - `seed_discovery` 不再只消费裸 `search_seed_queries`
  - 会执行 query bundles，并对 `public_interviews / publication_and_blog` 生成 `public_media_lead`
  - 这些 lead 会进入后续 exploration / manual review，而不是直接丢失
- 已落地 semantic/vector retrieval 第一版：
  - 新增 `semantic_retrieval.py`
  - retrieval 现已真正执行 `structured hard filters + lexical/alias + sparse-vector semantic rerank + confidence banding`
  - 对 `post-train` 这类 lexical 不稳定的 query 变体，已能通过 semantic hit 召回候选人
- 当前 fully agentic sourcing copilot 的现状边界：
  - 已具备 model-assisted plan / search / page analysis / weakly structured exploration
  - 但 runtime 仍不是“会自动 handoff 给多个自主子 Agent”的执行架构
  - 后续若要进一步 agent 化，应增加 specialist lanes / handoff runtime / long-running agent state
- 已补 `agent runtime` 第一版：
  - 新增 `agent_runtime.py`
  - workflow / retrieval 已会生成 `agent_runtime_session`
  - acquisition / retrieval 过程已记录 `agent_trace_spans`
  - 当前 lane 为 specialist-lane runtime，而非自治 swarm；但 handoff、trace、runtime state 已具备
- 已补 `semantic provider` 第一版：
  - 新增 `semantic_provider.py`
  - 默认 `LocalSemanticProvider` fallback
  - 已可切换 DashScope/Qwen embedding + rerank
  - 当前配置面向 `text-embedding-v4 + gte-rerank-v2`，并预留 `qwen3-vl-rerank`
- 已将公开视频结果纳入低成本数据资产：
  - `public_interviews / publication_and_blog` query bundle 会保存 `public_media_results / public_media_analysis`
  - 当前先基于标题/摘要做初步关系判断，不主动抓 transcript
  - 若后续需要深挖访谈内容，可直接复用这些资产
- 已补 autonomous worker runtime 第一版：
  - `search_planner / public_media_specialist / exploration_specialist` 已支持并行 worker
  - worker 现会持久化 `budget / checkpoint / output / interrupt_requested`
  - 同一个 `job_id + lane_id + worker_key` 已支持 checkpointed resume
  - acquisition state 已把 `job_id / plan_payload / runtime_mode` 下传到 search/exploration worker
- 已补 worker 控制面：
  - CLI 新增 `show-workers / interrupt-worker`
  - API 新增 `GET /api/jobs/{job_id}/workers` 与 `POST /api/workers/interrupt`
  - `GET /api/jobs/{job_id}/results` 与 `GET /api/jobs/{job_id}/trace` 现会附带 `agent_workers`
- 已补 worker 生命周期测试：
  - 新增 `tests/test_agent_runtime.py`
  - 已验证 `checkpoint -> interrupt -> resume -> complete`
  - 全量测试现为 `57` 个通过
- 已补 lane-aware scheduler 第一版：
  - 新增 `worker_scheduler.py`
  - scheduler 会按 lane priority、resume mode 和并行上限选择下一批 runnable worker
  - 当前优先恢复 `reuse_checkpoint / resume_from_checkpoint`，再执行 fresh worker
  - 已新增 `GET /api/jobs/{job_id}/scheduler` 与 CLI `show-scheduler`
- 已补 checkpoint 恢复语义：
  - public media analysis 现在会把 `completed_urls + analysis_map` 写入 checkpoint
  - exploration worker 现在会把 `completed_queries + gathered_signals + result_summaries` 写入 checkpoint
  - 这样 interrupted/completed worker 再次执行时，不会丢失已经沉淀的中间状态
- 已补 autonomous worker daemon loop：
  - 新增 `worker_daemon.py`
  - search/public media/exploration worker 现在统一通过 daemon loop 执行
  - daemon 会执行 `resume/retry loop + lane budget arbitration`
  - failed worker 现在会按 retry limit 自动重试
  - completed worker 现在会优先复用已持久化 output，而不是重复请求外部 source
- 已补 lane budget caps：
  - cost policy 现新增 `search/public_media/exploration` 的 worker unit budget
  - scheduler summary 现会输出 `lane_budget_caps`
  - daemon 会记录 `lane_budget_used`
- 已补 daemon 测试：
  - 新增 `tests/test_worker_daemon.py`
  - 已验证 `failed -> retry` 与 `budget exhausted -> backlog`
- 全量测试现为 `62` 个通过

## 2026-04-05

### 已完成

- 解压 `Sourcing AI Agent Dev` 下 4 个压缩包，并确认核心材料分布
- 阅读 `Anthropic华人专项` 的 `README.md`、`PROGRESS.md`、`api_accounts.json`、`company_ids.json`
- 阅读 `anthropic-employee-scan`、`investor-chinese-scan`、`biz-visit-onepager` 的 `SKILL.md`
- 确认当前沉淀出的可产品化资产：
  - Anthropic 主工作簿：在职华人员工 / 已离职华人员工 / 投融资历史 / 主要投资方华人成员
  - Scholar 扫描结果
  - 投资机构成员原始 JSON
  - 项目方法论、阶段门、审计要求
- 新建 `sourcing-ai-agent/`，开始实现后端 MVP
- 完成后端 MVP 第一版实现：
  - `AssetCatalog` 自动发现解压资产
  - 标准库 `.xlsx` 解析器
  - SQLite candidate/evidence/job store
  - criteria-driven scoring
  - CLI 与 HTTP API
- 将项目从 Anthropic 特例扩展为通用 sourcing workflow engine：
  - 新增 planning 层，显式输出 `SourcingPlan`
  - 新增 acquisition task 抽象
  - 新增 workflow job，显式阶段：`planning` / `acquiring` / `retrieving` / `completed`
  - 新增 retrieval strategy 抽象：`structured` / `hybrid` / `semantic`
  - API 新增 `/api/plan`、`/api/workflows`、`/api/providers/health`
- 接入 Qwen provider：
  - 本地 secret 读取：`runtime/secrets/providers.local.json`
  - Provider 支持 DashScope Responses API
  - 加入 timeout fallback，模型超时时自动退回 deterministic 实现
- 完成真实链路验证：
  - bootstrap 导入 `258` 位候选人
  - demo job 1：基础设施方向当前 Anthropic 员工，Top 1 为 `Da Yan`
  - demo job 2：直接参与 Anthropic 投资决策的华人投资方成员，返回结构化结果
  - demo workflow 1：`xAI` 进入 `blocked@acquiring`，明确暴露缺失 connector
  - demo workflow 2：`Anthropic` 完整跑通 `planning -> acquiring -> retrieving -> completed`
- 完成 Qwen 联通验证：
  - `test-model` 返回 `QWEN_OK`
  - `plan` / `workflow` 可使用 Qwen 生成 intent summary，失败时自动回退
- 完成自动化验证：`PYTHONPATH=src python3 -m unittest discover -s tests -v` 全部通过
- 落地 connector 第一版：
  - 新增 live company identity resolver
  - 新增 RapidAPI 账号自动发现与排序
  - 新增 LinkedIn `company/people` roster connector，支持多账号 fallback、429 熔断、分页落盘、重复页检测
  - 新增 `runtime/company_assets/{company}/{snapshot_id}` snapshot 目录约定
- 完成真实 xAI acquisition + retrieval 验证：
  - `account_014` 可稳定拉取 xAI roster
  - 首次 xAI workflow 获取 `98` 条 roster 行，其中 `97` 条可见、`1` 条 headless
  - 归一化后向 SQLite 写入 `97` 个 xAI baseline candidates
  - 中文 infra criteria 已可通过 alias expansion 命中英文 headline
  - demo workflow 返回 `Jake Palmer / Jesik Min / Neal Bayya` 等结构化结果
- 完成 enrichment connector 第二版：
  - 新增 provider-first slug resolution：`search/people -> /api/profile -> profile detail`
  - 新增 publication author / acknowledgement / co-author baseline connector
  - 新增跨 snapshot search/basic profile 缓存复用，避免重复消耗 LinkedIn search quota
  - 新增 cached roster snapshot fallback：live roster 超限或空返回时，workflow 可继续复用最近一次成功的本地资产
  - 新增 enrichment 单测，`unittest` 总数增至 `8`
- 完成 enrichment 实测验证：
  - 在已有 xAI snapshot 上，top 5 infra 候选中成功解析 `Jake Palmer / Jesik Min / Neal Bayya` 的 LinkedIn profile detail
  - 最新 live xAI workflow 已能在 end-to-end 链路中落下 enriched artifact，`20260405T220122` snapshot 至少成功合并 `Jake Palmer` 的完整 LinkedIn profile
  - 发现真实约束：`search/people` 所在 z-real-time 账号配额已接近耗尽，后续 live 验证需要继续复用缓存或补充新账号

### 当前结论

- 现有项目已经具备产品雏形，核心不是“再写一次扫描脚本”，而是把已有流程产品化为：
  - 可重复运行的 job
  - 可持续扩展的 source adapter
  - 可审计的结果存储
- 技术栈先采用 Python 标准库，避免因为外部依赖阻塞 MVP
- 模型调用层做接口预留，不把 Codex / Claude 写死在业务逻辑里
- acquisition 已经不再只有本地 Anthropic asset：xAI 的 LinkedIn roster baseline 已能真实执行
- 当前“全量资产”仍是 baseline：受 LinkedIn headless 用户、页数上限、search quota、publication 覆盖率影响，覆盖率还不是最终上限
- 高价值资产的最终形态不应长期只放本地：LinkedIn Profile 和最终结果需要进入云端存储设计

### 正在推进

- 继续补 The Org / Hunter / Scholar / web adapters，并把现有 publication/co-author 真正用于 retrieval
- 设计外部 API 接入策略、云端资产存储方案和人工 review 阶段门

### 下一步

- 在小公司样本上做完整端到端验证，例如 Thinking Machines Lab
- 把 candidate document 向量化或语义索引纳入 retrieval 层
- 设计云端资产存储：本地保留执行日志，云端持久化 LinkedIn Profile 与最终结果 artifact
- 引入真正的 LLM criteria compiler
- 补前端 Demo

### 2026-04-06 Claude Relay 与 Thinking Machines Lab 准备

- 新增通用 `model_provider` 配置层：
  - `settings.py` 新增 `ModelProviderSettings`
  - `build_model_client(...)` 现优先选择通用 `model_provider`，未配置时再回退到 `qwen`
- 新增 `OpenAICompatibleChatModelClient`：
  - 面向 OpenAI-compatible `chat/completions` + `models`
  - 已完成 `summarize / interpret_intent / analyze_page_asset / plan_search_strategy / healthcheck`
  - 当前实现使用 `requests`，避免 relay 对默认 `urllib` 请求头的拦截
- 完成 Claude relay 联通验证：
  - `GET /v1/models` 成功返回 `claude-sonnet-4-6`
  - `POST /v1/chat/completions` 已通过 relay 成功返回结果
  - 项目内 `healthcheck_model()` 已显示 provider `ready`
  - 项目内 `interpret_intent(...)` 已成功通过 `claude-sonnet-4-6` 返回文本
- 新增 `tests/test_model_provider.py`
  - 覆盖通用 provider 选择与 OpenAI-compatible 响应解析
- 新增 `configs/demo_workflow_thinking_machines_lab.json`
  - 用于 Thinking Machines Lab 的 scoped roster 端到端测试起点
- 新增 `configs/demo_workflow_thinking_machines_lab_full_roster.json`
  - 用于 Thinking Machines Lab 全量 current roster acquisition
  - 当前建议纯 roster 采集时设置 `slug_resolution_limit=0`、`profile_detail_limit=0`
- 更新 README：
  - 将“Qwen 配置”改为“模型配置”
  - 明确 `model_provider` 的优先级和 Claude relay 的 base URL 形态
  - 补充 Thinking Machines Lab 的 `plan -> review -> start-workflow` 建议入口
- 新增 HarvestAPI live connector 基础层：
  - `HarvestProfileSearchConnector`
  - `HarvestCompanyEmployeesConnector`
  - `HarvestProfileConnector`
  - `former_employee_search / scoped_search_roster` 现可走 Harvest search fallback
  - `full_company_roster` 现可在允许高成本 source 时优先走 Harvest company-employees
- 完成 Harvest live 参数核对与最小调用验证：
  - `linkedin-profile-search` + `pastCompanies` + `excludeCurrentCompanies` 已成功调用，Thinking Machines Lab 最小预算测试返回 `0` 条结果
  - `linkedin-company-employees` 已成功调用，Thinking Machines Lab 最小预算测试返回 `10` 条当前成员
  - 已确认关键参数坑位：
    - `maxTotalChargeUsd` 不能低于 actor 最小允许值
    - company-employees actor 的 `profileScraperMode` 必须使用完整枚举字符串，如 `Short ($4 per 1k)`
  - company-employees actor 返回字段更接近 `firstName / lastName / currentPositions / location.linkedinText`，而不是旧假设中的 `fullName / headline`
- 完成 Thinking Machines Lab 全量 current roster live acquisition：
  - workflow `1d19590b1e4c` 已完整跑通 `planning -> acquiring -> retrieving -> completed`
  - 使用 Harvest `linkedin-company-employees` 获取 current roster
  - snapshot `20260406T160415` 共捕获 `25` 条 roster rows，其中 `25` 可见、`0` headless
  - 已将 `25` 位成员归一化进 SQLite，并生成 `candidate_documents.json / retrieval_index_summary.json`
  - 当前 live roster 质量良好：实测样本中名字、headline、location、LinkedIn URL 均完整
- 完成 Thinking Machines Lab former / detail 链路校正：
  - 已通过 Harvest live 对照确认 former employee search 更适合先用 `pastCompanies` 做 recall，不默认加 `excludeCurrentCompanies`
  - 两页 `pastCompanies=Thinking Machines Lab` live search 可返回 `19` 条 former leads，并命中 `Alexis Dunn`
  - 已修正 `linkedin-profile-scraper` 的 `profileScraperMode` 枚举，必须使用 `Profile details no email ($4 per 1k)`
  - 已修正 roster -> candidate 映射，`build_candidates_from_roster(...)` 现会保留 `linkedin_url / metadata.profile_url`
  - 已为 known-profile enrichment 增加 Harvest batch profile fetch，避免逐人串行 profile scrape
- 完成 Thinking Machines Lab prioritized current detail live acquisition：
  - 最新有效 workflow `3f5e25153ab0` 已完整跑通 `planning -> acquiring -> retrieving -> completed`
  - snapshot `20260406T165131` 成功落下 `12` 个 `harvest_profiles/*.json`
  - acquisition event 明确记录：`Resolved 12 profile details and matched 0 publications`
  - 合并后的 candidate assets 已成功吸收 profile detail 信息，包括：
    - vanity LinkedIn URL
    - education
    - work_history
    - moreProfiles metadata
  - 实测已成功 enrich 的代表成员包括：
    - `Mira Murati`
    - `Lilian Weng`
    - `Soumith Chintala`
    - `Andy Hwang`
    - `Saurabh Garg`
- 完成 Thinking Machines Lab publication supplementation：
  - enrichment 现已执行 official surface publication collection，而不再只依赖 Anthropic 本地 publication 或通用 arXiv affiliation
  - Thinking Machines Lab 官方 surface 已成功抽出 `4` 个 publication/blog leads：
    - `Kevin Lu`
    - `John Schulman`
    - `Jeremy Bernstein`
    - `Horace He`
  - publication raw/index/page assets 已落到 snapshot `20260406T172439/publications/official_surfaces`
  - publication lead 现会自动进入 targeted Harvest resolution，再进入 exploration fallback
  - 当前 TML publication lead 的 live 现状是：
    - targeted Harvest current/past exact-name search 可执行且可审计
    - 但在这 `4` 个 lead 上返回 `0`
    - exploration fallback 当前受 DuckDuckGo SSL 问题影响，仍保留为 unresolved lead / manual review 输入
- 完成 Harvest runtime 级 cache/live-test bridge：
  - `harvest_profile_search` 与 `harvest_company_employees` 现会按 payload hash 复用：
    - 当前 snapshot 本地 raw asset
    - `runtime/provider_cache/*`
    - `runtime/live_tests/*` 下历史手工 live 资产
  - 这样即便当前进程没有 Harvest token，也能把已有 live asset 重新接入 workflow，而不是重复调用或退化成 0 结果
  - 同时修复了 `HarvestProfileConnector.fetch_profiles_by_urls(...)` 中未定义 `take_pages` 的 batch profile bug
- 完成 Thinking Machines Lab former fallback 修复与复跑：
  - 新增 refreshed plan review `9`，former strategy 的 `provider_people_search_min_expected_results` 已从 `10` 提升到 `50`
  - former workflow 现已通过 Harvest cached live bridge 重新接回历史 `pastCompanies` live asset
  - 最新有效 former snapshot 为 `20260406T175245`
  - `search_seed_discovery/summary.json` 当前记录：
    - `entry_count=25`
    - `query="__past_company_only__"`
    - `mode="harvest_profile_search"`
    - `seed_entry_count=25`
  - 这批 former candidates 已成功归一化进 `candidate_documents.json`，其中包含：
    - `Alexis Dunn`
    - `Andrew Tulloch`
    - `Barret Zoph`
    - `Joshua Gross`
    - `Songlin Yang`
    - 以及其他 former leads
  - 这说明 current roster、current detail、publication supplement、former fallback 四条 Thinking Machines Lab 主链都已具备可复盘的端到端资产
- 完成 manual review resolution 正式写回链路：
  - `manual_review_resolution.py` 已接到 orchestrator/store，而不是只停留在 helper
  - 人工 review 现在可以：
    - 直接写回 `candidate`
    - upsert `evidence`
    - 在 `runtime/manual_review_assets/...` 下落盘 source manifest、fetched html、analysis input/output
  - 已对 Thinking Machines Lab 的 publication leads 做真实写回：
    - `Kevin Lu`：confirmed current employee，LinkedIn 已写回
    - `John Schulman`：confirmed current employee，homepage evidence 已写回
    - `Jeremy Bernstein`：confirmed current employee，homepage + CV evidence 已写回
    - `Horace He`：homepage 未直接确认 TML affiliation，保留为 unresolved lead
- 完成 publication lead low-cost-first 调整：
  - `targeted Harvest name search` 不再默认执行
  - 当前默认顺序改为：
    - slug/web exploration
    - manual review / unresolved lead 保留
    - 只有显式批准时才进入 targeted Harvest name search
- 完成 manual review / exploration 信号质量修正：
  - 页面信号提取已过滤 `static.licdn.com / gstatic / fonts` 等静态资源 URL
  - 避免像 Kevin Lu 这类 LinkedIn 页面把静态资源误写成 `media_url`
- 复核 DuckDuckGo SSL 问题：
  - 旧 snapshot 中的报错不是单纯的 `urllib` 代码问题
  - 已用新的 `requests + endpoint fallback` live 复测，当前环境对 DuckDuckGo HTML endpoint 依然会返回 `SSL: UNEXPECTED_EOF_WHILE_READING`
  - 结论是：
    - 当前网络路径对 DuckDuckGo HTML 搜索不稳定
    - low-cost web search 必须继续走 provider abstraction / best-effort 策略
    - unresolved lead 必须保留，不能因为搜索失败就静默丢弃
- 新增 Thinking Machines Lab 复盘文档：
  - `docs/archive/THINKING_MACHINES_LAB_RETROSPECTIVE.md`
  - 记录了：
    - Harvest 参数修正
    - current roster / current detail / former fallback / publication supplement 结果
    - manual review 写回结果
    - DuckDuckGo SSL 问题与后续建议
- 完成稳定 search provider abstraction 第一版：
  - 新增 `src/sourcing_agent/search_provider.py`
  - 当前 provider chain 支持 `serper_google -> duckduckgo_html`
  - `search_seed_discovery / slug_resolution / exploratory_enrichment` 已统一接到 provider abstraction
  - search raw payload 现按 provider 的 `html/json` 形态落盘，便于缓存复用、审计和后续替换 provider
  - 这意味着 low-cost search 不再写死到 DuckDuckGo HTML endpoint
- 补齐 search provider 回归测试：
  - 新增 `tests/test_search_provider.py`
  - 覆盖 DuckDuckGo HTML parsing、Serper parsing、provider chain fallback、response roundtrip
- 完成 monorepo 级 GitHub 同步准备：
  - 新增根目录 `README.md`
  - 新增根目录 `ONBOARDING.md`
  - 新增根目录 `.gitignore`
  - 新增 `docs/DEVELOPMENT_GUIDE.md`
  - 明确忽略：
    - 原始 zip 包
    - `runtime/` 数据资产
    - `providers.local.json`
    - 历史 `api_accounts.json`
    - SQLite / cache / Python 临时文件
  - 当前结论是可以先整理成 publish-ready monorepo，但不能把现有目录原样直接推到 GitHub
- 2026-04-10（并行与恢复稳定性更新）：
  - 修复 workflow 恢复门控：`run_queued_workflow` 与 daemon 恢复现在都覆盖 `running+acquiring`，不再只支持 `blocked+acquiring`。
  - 修复 Harvest company filter 归一化误改写：避免把 `past_companies=[google, deepmind]` 错误写成 `[google, google]`。
  - 同义 query 泛化去重：`-`/空格/下划线变体统一签名，避免 `Vision-language` 与 `Vision Language` 重复执行。
  - `company-employees` shard worker 改为并行提交，减少 current roster 侧串行等待。
  - former 与 current 采集并行化：
    - current roster queued 时并行触发 former seed。
    - 非 queued 同步路径也并行启动 former，并在 enrichment 前 join。
  - former queued 后自动阻塞等待恢复，以保证后续会进入增量 enrichment，而不是“former 到了但没有再吃进召回”。
  - paid fallback 增加 probe overlap 剪枝：高重叠 query 标记 `skipped_high_overlap`，降低大组织重复调用成本。
  - 2026-05-05 historical results-board contract follow-up: public lifecycle rendering now normalizes legacy current-snapshot-serving rows whose materialized counter proves completion while old board-visible patch mirrors still show zero. Frontend sync/facet/hydration freshness now shares `resultViewLifecycle.ts`, and function facet all-selected summary renders `全量`. Validation: frontend contract tests, targeted lifecycle tests, ruff, diff check, and frontend build passed.
  - 2026-05-05 candidate board/detail serving projection follow-up: paginated artifact repair/build now writes public serving card projections instead of normalized-only rows, preserving normalized/index semantics plus materialized profile/card fields. `authoritative_candidates` returns materialized shard records for detail reads; single and batch candidate detail overlay rank/confidence metadata without letting stale job-results or candidate-table fields override shard-owned timeline/avatar fields. `/candidates?lightweight=1` and `/results?include_candidates=1` now read materialized serving rows without raw LinkedIn timeline hydration, and the frontend candidate board requests lightweight pages by default. Remaining risk: raw profile timeline resolver still exists for legacy ranked-results/profile-completion/backfill paths; do not add new normal public-read dependencies on it. Validation: targeted results API tests, candidate artifact repair regression, ruff, and frontend build passed.
	  - 2026-05-05 public-read raw timeline quarantine follow-up: `/api/jobs/{job_id}/results` now serializes ranked results with materialized/summary fields only, so dashboard fallback and public results reads cannot trigger raw profile timeline parsing. The bare `/api/jobs/{job_id}/candidates` endpoint defaults to lightweight serving-card mode, and single-candidate public detail only enters legacy raw timeline hydration through explicit `hydrate_legacy_timeline=1` diagnostic opt-in. Internal `get_job_results(...)`, export, profile-completion, and backfill paths can still use the resolver deliberately. Added regression coverage for ranked `/results` API and public detail no-hydration behavior.
	  - 2026-05-05 overlay source-selection follow-up: job-scoped baseline+delta overlay reads now treat overlay rows as serving projection membership, not profile-field truth. If an overlay row is sparse, public candidate page/detail/profile-progress serialization loads the current and baseline materialized serving shards and merges their card/timeline/avatar/location fields before returning the row. This closes the reuse/history shape where a second run could select an overlay or historical job-scoped projection and show roster-preview cards even though the full materialized candidate record already existed. Regression coverage proves sparse Lovable overlay rows are enriched from materialized shards while raw timeline hydration remains disabled.
	  - 2026-05-05 raw candidate-doc fallback quarantine follow-up: normal public reads no longer auto-materialize or fallback to raw `candidate_documents.json`. `_candidate_source_allows_candidate_documents_fallback(...)` now requires explicit legacy opt-in metadata, old tests were moved to materialized serving fixtures, and completed-workflow current-snapshot repair now synchronizes candidate docs into serving artifacts before publishing the result view source. This closes the path where a historical/full-reuse board could appear correct only because `/dashboard` or `/candidates` rebuilt artifacts on read. Validation: targeted candidate-doc fallback tests passed, related asset-population source-selection slice passed, full `tests/test_results_api.py` passed with `131 passed, 1 existing thread-cleanup warning`, and ruff/diff-check passed for touched files.
	  - 2026-05-05 raw profile timeline default-off follow-up: internal `get_job_candidate_detail(...)` now defaults to no legacy raw timeline hydration, matching the HTTP public detail contract. `hydrate_legacy_timeline=1` remains the explicit diagnostic opt-in, and target-candidate export now calls detail in no-hydration mode before using its own export-only raw profile resolver for CSV/profile packaging. Added regression coverage for default internal no-hydration, explicit HTTP opt-in, and export not relying on implicit detail hydration. Validation: targeted candidate-detail/export regressions passed, full `tests/test_results_api.py` passed with `132 passed, 1 existing thread-cleanup warning`, and ruff/diff-check passed for touched files.
	  - 2026-05-05 job result-view consistency maintenance follow-up: added `audit-job-result-view-consistency [--job-id ...] [--company ...] [--apply]` to compare `job_result_view.snapshot_id`, job summary candidate source, and latest authoritative organization registry. The command defaults to read-only and only applies repoints for jobs with explicit full-local/full-asset reuse proof; scoped/delta/overlay jobs are reported as `manual_review_required` so maintenance cannot silently rewrite scoped history to the latest full-company snapshot. Regression coverage proves a full reuse job is auto-repoint eligible while a scoped overlay job remains manual-only.
	  - 2026-05-05 hot-cache serving artifact governance audit: added read-only `audit-hot-cache-serving-artifacts [--company ...] [--snapshot-id ...] [--limit ...] [--output ...]`. The audit scans local hot-cache serving manifests for missing manifest-referenced shard/page/backlog/aux files, orphan JSON files, and canonical serving source availability, then emits cleanup/rehydrate plans without changing public-read behavior. Regression coverage proves missing hot-cache shard emits a rehydrate plan from canonical artifacts while orphan-only drift emits cleanup-only guidance.
	  - 2026-05-05 hot-cache explicit cleanup operator path: added `cleanup-hot-cache-serving-artifacts [--company ...] [--snapshot-id ...] [--apply]`, defaulting to dry-run. The command delegates to the existing hot-cache cleanup/retention engine with explicit operator flags for compatibility-export cleanup, TTL, size budget, per-company budget, snapshot retention, and generation compaction, so cleanup remains maintenance-owned instead of request-time repair.
	  - 2026-05-05 hot-cache governance validation: `PYTHONPATH=src .venv-tests/bin/pytest tests/test_candidate_artifacts.py -q` -> `54 passed`; `PYTHONPATH=src .venv-tests/bin/pytest tests/test_cli.py -q` -> `53 passed`; `PYTHONPATH=src .venv-tests/bin/ruff check src/sourcing_agent/candidate_artifacts.py src/sourcing_agent/cli.py tests/test_candidate_artifacts.py tests/test_cli.py` -> passed; both new CLI help commands and `git diff --check` passed.
	  - 2026-05-05 hot-cache heat/LRU and post-publish/hydrate compaction follow-up: hot-cache access markers now persist `access_count`, `first_access_at`, `last_accessed_at`, and a recency-weighted `heat_score`; `collect_hot_cache_inventory` and runtime summary expose `hottest_snapshots` for service-level governance. `publish-candidate-generation`, `hydrate_cloud_generation`, and generation-first imports now run a hot-cache governance cycle by default and return `hot_cache_governance`, with CLI opt-out via `--skip-hot-cache-governance`.
	  - 2026-05-05 hot-cache heat/LRU validation: `PYTHONPATH=src .venv-tests/bin/pytest tests/test_candidate_artifacts.py -q` -> `55 passed`; `PYTHONPATH=src .venv-tests/bin/pytest tests/test_cloud_asset_import.py -q` -> `10 passed`; `PYTHONPATH=src .venv-tests/bin/pytest tests/test_cli.py -q` -> `54 passed`; ruff passed for `artifact_cache.py`, `cloud_asset_import.py`, `orchestrator.py`, `cli.py`, and touched tests; new publish/import/hydrate CLI help and `git diff --check` passed.
	  - 2026-05-05 scripted environment guardrail follow-up: restored the local live UI boundary to `runtime` + `local_dev` on `4173/8765` and the scripted UI boundary to `runtime/test_env/openai_agent_delta_streaming` + `scripted` on `4185/8785`. During post-patch scripted regression, the Whisper late-webhook case exposed an SLO metric ambiguity rather than a recovery failure: the actionable watcher event arrived quickly, while the deliberately late duplicate webhook exceeded 30s and was incorrectly counted as terminal wakeup lag. Service metrics now split full audit lag, actionable wakeup lag, and late-duplicate audit lag; `max_remote_provider_event_lag_ms` gates actionable wakeup only.
	  - 2026-05-05 frontend contract/root-cause audit: candidate-board user state now keys only on `historyId/jobId`, not `snapshotId`; standalone results route was aligned with SearchFlow, hydration polling no longer restarts solely on snapshot changes, and the unused legacy `CandidateBoard` implementation was deleted. `SearchPage` keeps a latest renderable dashboard ref so transient completed/reuse result refresh failures do not clear an already-visible board and reset filters. Plan strategy display no longer infers `全量本地资产复用` from `dispatch_preview.strategy` alone; frontend fallback requires explicit execution semantics or reuse/coverage proof. Validation: `PYTHONPATH=src .venv-tests/bin/pytest tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py tests/test_frontend_dashboard_hydration.py -q` -> `13 passed`; `.venv-tests/bin/ruff check tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py tests/test_frontend_dashboard_hydration.py` -> passed; `cd frontend-demo && npm run build` -> passed.
	  - 2026-05-05 board-visible progress/detail serving convergence: harvest-prefetch board-visible delta apply now chunks large candidate deltas into durable `board_visible_delta_apply` items and writes canonical lifecycle after each completed chunk, so `delta_profile_materialized_count` / `delta_profile_board_visible_count` can advance event-by-event instead of jumping from an early patch to final current-snapshot serving. Candidate board hydration now keeps full-board rows lightweight and overlays rich detail only for the current page via `getCandidateDetailsBatch`; filtering, counts, pagination, and sync copy remain owned by canonical lightweight rows and lifecycle. Regression coverage pins chunked lifecycle progress and prevents frontend detail hydration from using `dashboard.candidates` / `visibleCandidates` as a global rich-load source.
	  - 2026-05-05 board-runtime contract follow-up: the candidate board now has a backend-driven `board_runtime_state` contract that owns sync numerator, publication phase, facet readiness, and filter scope across `/progress`, `/dashboard`, and `/candidates`. Frontend sync cards, hydration banners, and facet gating now prefer `board_runtime_state`; `result_view_lifecycle` remains a compatibility/serving-view contract rather than a competing board business source. Validation: frontend dashboard hydration, candidate sync summary, candidate filter contract tests, `ruff`, `py_compile`, and backend board-runtime/lifecycle regressions passed; one broader lifecycle batch still shows an existing tempdir cleanup warning in `tests/test_results_api.py` when run alongside the threaded workflow smoke.
	  - 2026-05-06 backend-filtered candidate paging closure: `/api/jobs/{job_id}/candidates` now accepts board filter parameters and filters the complete materialized served population before `offset/limit`, returning `filtered_candidate_count`, `applied_filter`, `filter_signature`, and `filter_contract.row_filter_scope=backend_filtered_served_population`. The candidate board now fetches the current backend-filtered page for active filters/page changes and treats local row hydration as cache only, so facet selections no longer show false empty/wrong counts when matches live outside the already-loaded window. Validation: targeted backend candidate-page filter test, board-runtime/patch regressions, frontend candidate filter/hydration/sync tests, ruff, and frontend build passed. Follow-up validation on 2026-05-06: fast scripted OpenAI Agent and clean Lovable live-roster smoke both passed with no expectation failures; the smoke gate now treats same-second preview/finalization timestamps as valid only when durable materialization and finalization boundaries are present, avoiding false failures from second-precision timestamps while still rejecting missing finalization boundaries.
	  - 2026-05-06 unified card-ready board streaming closeout: live roster and baseline+delta now share the same backend-driven board contract. Board-visible patch writers freeze `card_materialization_summary` at event time, so `/progress`, `/dashboard`, `/candidates`, and `/board-patches` no longer recompute old patch quality from final mutable overlays. Roster-only/basic shell rows are deferred from board-visible completion until profile/detail fields or explicit low-richness/needs-completion card state exists. Lovable scripted smoke now shows display-ready progression `23 -> 71 -> 119 -> 140` with first board visibility around `9s`; seeded OpenAI Agent baseline+delta shows baseline `300` plus delta card-ready progression `25 -> 73 -> 121 -> 125 -> 173 -> 221 -> 224 -> 272 -> 297`, ending at `597/597` with first board visibility around `8s`. Validation: seeded OpenAI and Lovable strict scripted smokes, `190` targeted backend/frontend/smoke tests, ruff, py_compile, and frontend build passed; the only observed warning is the existing temporary `job_locks` cleanup race in threaded results-api tests.
	  - 2026-05-05 scripted provider timing metrics follow-up: out-of-order profile batch detection now uses provider submit/remote-wait start (`provider_started_at`) rather than local recovery trace span start, because late webhook handling can create spans only when the terminal event is processed. This keeps Lovable/OpenAI scripted smoke gates measuring provider behavior instead of local observer timing. Validation: out-of-order service metric regressions, `ruff`, OpenAI hosted scripted smoke, and Lovable hosted scripted smoke passed.
	  - 2026-05-07 PG-only workflow confidence closeout: scripted/manual/browser workflow confidence no longer has a SQLite control-plane fallback. `isolated_hosted_test_runtime`, `run_simulate_smoke_matrix.py --runtime-dir`, `run_explain_dry_run_matrix.py --runtime-dir`, `seed_reference_smoke_runtime.py`, and `dev_scripted_openai_agent_delta.sh` require runtime-scoped PG-only env files with `postgres_only + shared_memory`; startup now prepares a safe `sourcing_scripted*` / `sourcing_simulate*` / `sourcing_replay*` / `sourcing_test*` schema and fails closed if PG is missing, unreachable, or unsafe. The manual launcher rejects the removed `--sqlite-control-plane` option. Validation so far: `tests/test_scripted_test_runtime.py` -> `20 passed`; `tests/test_run_simulate_smoke_matrix.py` -> `7 passed`; representative hosted smoke now reaches business explain-matrix expectation failures instead of fake-DSN SQLite fallback. Remaining follow-up: migrate the older hand-rolled hosted smoke harness fully onto shared `isolated_hosted_test_runtime` and separately fix existing explain-matrix expectation drift.
	  - 2026-05-11 event-level drain budget follow-up: Google large baseline + large shard pressure smoke exposed a slow non-total recovery phase where `profile_refill_event_level_materialization_followup` serialized heavy `local_apply_closure` and `board_visible_delta_apply` work into one measured tick. `_drain_event_level_materialization_for_job(...)` now records local-apply elapsed time and skips same-phase board-visible apply when local apply already exceeds `EVENT_LEVEL_BOARD_VISIBLE_AFTER_LOCAL_APPLY_BUDGET_MS` (default 12s), leaving the durable board-visible item for the next bounded recovery/service phase. This preserves user-visible eventual progress while keeping recovery phases small-step and measurable.
	  - 2026-05-20 Phase 12 scripted matrix closeout: full PG-backed nightly long-latency matrix passed strict for all 5 scripted cases (`full_matrix_contract_rerun4`). The remaining Lovable failure was a scheduler accounting bug, not a real provider-slot underuse: a repeated submit containing already provider-owned `planned_dispatch` URLs hit registry lease contention and was incorrectly written back to `deferred_budget`. `_execute_harvest_profile_batch_worker(...)` now rechecks contended URLs against the same provider-owned/same-scope contract used by `_partition_already_queued_profile_urls(...)`, reports them as queued, and only records true registry lease contention as deferred. Targeted regressions cover provider-owned planned dispatch, provider-owned race, provider-owned lease contention, and true lease contention.
	  - 2026-05-20 projection layering checkpoint follow-up: the Google large matrix exposed that `projection_facet_layering_build` was bounded by chunk/time, but its state file persisted full `processed_records`; Google large produced a 37MB checkpoint for 7384 candidates. The builder now checkpoints compact `layer_assignments` during partial progress, strips assignments on completion, and reconstructs final overlay rows only at publication time. This preserves the public overlay/layering contract while preventing checkpoint/report paths from carrying full candidate payloads.
	  - 2026-05-20 containerized gate environment follow-up: added `make docker-start`, `make docker-doctor`, `scripts/docker_start.sh`, `scripts/docker_doctor.sh`, and updated `docs/TEST_ENVIRONMENT.md` with Docker Desktop/Colima setup. Local Docker CLI + Colima are installed and the Make/Testcontainers path now exports the current Docker context socket, uses the ECR mirror for Postgres, and disables Ryuk for explicit-cleanup contract tests. `make docker-doctor`, `make ci-pg-contract`, and `make ci-workflow-fake-provider` pass locally.
	  - 2026-05-20 containerized frontend browser gate follow-up: added `make ci-frontend-browser-gate`, `tests/testcontainers_frontend_browser_gate.py`, and `frontend-demo/scripts/run_projection_board_probe.mjs` / `npm run browser:projection-board`. The gate starts disposable PG, seeds a canonical `run_scope_projection`, builds the projection person search index, starts the real backend API plus Vite preview in same-origin mode, and drives `/projections/{projection_id}` with Playwright. It validates canonical sync count `30/30`, first page render, page 2 pagination, backend-filtered search, and `filter_contract.fallback_used=false`. Validation: `make ci-frontend-browser-gate`, `py_compile`, `ruff`, `npm run browser:check --prefix frontend-demo`, and `git diff --check` for touched gate files passed.
	  - 2026-05-20 projection public-reader fallback cleanup: active `/api/projections/{projection_id}/candidates` filters now fail closed with `projection_person_search_index_unavailable` when `projection_person_search_index` is missing, instead of scanning all `serving_projection_members` as a request-path legacy fallback. The old scan path is now explicit migration/debug opt-in via `SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK=1` and remains report-visible if used. This narrows the public-reader boundary: unfiltered pages may page membership rows; active search/filter must use the index or report not-ready. Validation: `tests.test_serving_projection_writer`, projection/person/CRM API contract tests, targeted projection index recovery tests, ruff, py_compile, and `make ci-frontend-browser-gate` passed.
	  - 2026-05-20 containerized pre-release aggregation follow-up: added `make ci-containerized-pre-release` as the current required aggregation target for Docker preflight, disposable PG storage/writer contracts, fake-provider webhook/object-store integration, and frontend projection-board browser validation. Added `.github/workflows/containerized-pre-release.yml` so the aggregation is no longer only documented; CI bootstraps Python, frontend dependencies, Playwright, and then runs the same Make target from the `sourcing-ai-agent` workspace. This closes the single-command pre-release entrypoint gap without pretending the fake-provider workflow is already browser-driven end-to-end. Remaining Phase 12 work is a workflow-driven browser gate that starts a run through the fake provider and opens the resulting projection. Validation: `make ci-containerized-pre-release` passed locally under Colima; workflow YAML was syntax-reviewed locally but not executed by GitHub Actions in this session.
	  - 2026-05-20 legacy public-reader retirement coverage follow-up: expanded the cutover regression from `/api/jobs/{job_id}/results` to all old public read endpoints that can otherwise compose board state (`/results`, `/dashboard`, `/candidates`, and candidate detail). With `SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS=1`, each returns `410` plus the canonical projection pointer and `read_contract.fallback_used=false`. Validation: `./.venv-tests/bin/python -m unittest tests.test_projection_crm_api_contracts -v` passed.
- 2026-05-22 01:35 CST durable runtime W2b profile-refill submit cutover:
  - LinkedIn profile prefetch/refill submit chunks now plan `CommandPlanRequested` through `DurableRuntimeWriter` and materialize `workflow_commands(command_type='linkedin.profile_refill.submit_batch')` before provider submit.
  - Added `run_linkedin_profile_refill_submit_command_once(...)` as the `linkedin_profile_owner` execution entrypoint. The existing scheduler calls this owner entrypoint synchronously for now, but command claim/running/provider-submit/terminal recording semantics are no longer scheduler-local code.
  - Matching claimed/running/retry/terminal commands now block duplicate provider submit and return `runtime_command_contention=true` evidence instead of silently resubmitting.
  - Added `legacy_job_workflow_run_id`, `legacy_job_operation_id`, and scoped profile-refill submit idempotency helpers. Submit idempotency includes job, snapshot, URL set, and submit scope so retry waves are not suppressed by a prior normal-wave command.
  - Public profile-prefetch summaries now expose `workflow_commands`, `workflow_command_count`, and status counts for signoff visibility.
  - Remaining gap: the owner entrypoint is still drained synchronously by the scheduler. Next W2b follow-up should move it into a daemon/service loop so the scheduler only emits events/commands and wakes the owner.
- 2026-05-22 02:20 CST durable runtime W2b owner phase closeout:
  - Split profile-refill command planning from provider-submit execution inside recovery/service ticks. `_run_profile_prefetch_refill_queue_once(...)` now uses `execute_profile_refill_submit_commands=false` and reports `planned_command_count` / `planned_url_count`; it no longer drains typed commands itself.
  - Added the distinct `profile_refill_command_owner` recovery phase owned by `linkedin_profile_refill_command_owner`. It drains ready `linkedin.profile_refill.submit_batch` commands and reports `command_count`, `executed_command_count`, `dispatched_url_count`, and `queued_worker_count`.
  - Same-tick refill scanning now treats planned commands as a handoff signal (`profile_refill_command_handoff_to_owner`) so the daemon does not re-scan and re-plan the same registry rows before the owner phase runs.
  - Service heartbeat/activity compaction now includes planned profile-refill command counts and owner execution counts, so a tick that only plans commands is not misclassified as idle.
  - Validation: `py_compile` and `ruff` for touched runtime/recovery/service/tests; targeted durable/enrichment/recovery/service suite (`20 passed`); `make ci-pg-contract` passed. Full PG-backed nightly matrix remains deferred until W2c/W4 boundaries are tighter.
- 2026-05-22 03:10 CST durable runtime W2c.1 profile URL terminal-state owner:
  - Added the typed `linkedin.profile_url_terminal.record` command contract, owner registry mapping, and idempotency helper. The idempotency key is based on job, snapshot, terminal scope, and normalized terminal entries so replayed profile-worker chunks do not duplicate URL terminal writes.
  - Harvest profile terminal persistence now plans reducer-owned `CommandPlanRequested` events and executes the `linkedin_profile_url_terminal_record_command_owner` path to mutate `linkedin_profile_registry`. Normal ControlPlaneStore paths fail closed/report-visible if command planning fails; claimed/running terminal-record commands leave the worker recoverable instead of advancing `terminal_persist_progress` without registry proof.
  - Added `drain_linkedin_profile_url_terminal_record_commands(...)` and a distinct `profile_url_terminal_record_command_owner` recovery/service phase. Recovery/service summaries now expose `command_count`, `executed_command_count`, `recorded_count`, `fetched_count`, and `failed_count`, and service activity/cumulative summaries count terminal-record work as real daemon activity.
  - Profile-worker terminal-persist progress now carries cumulative terminal-record command evidence across bounded partial replays, so reports can prove all chunks reached registry terminal state before local apply/board-visible follow-up.
  - Validation so far: `py_compile` and `ruff` passed for touched runtime/recovery/service/test files; targeted durable/enrichment/recovery/service tests passed (`14 passed`). Broader W2b/W2c targeted suite is next. Full PG-backed nightly matrix remains intentionally deferred until local apply and board-visible command owners are tighter.
- 2026-05-22 W3b/W6 durable runtime command-owned payload checkpoint:
  - Adopted W6 方案 A for W2c normal paths: local apply, board-visible publish, projection person-search index build, and collection authoritative merge now store full execution payloads in `workflow_commands`; `legacy_materialization_item` is emitted only for report-visible migration-adapter commands.
  - Removed the unused collection-authoritative legacy enqueue helper so new run-scope projection publication cannot accidentally write `collection_authoritative_merge` rows on the normal path.
  - Tightened W3b completion policy semantics: `serving_finalized` is now a hard completion gate backed by `workflow_current_state.completion_proofs.serving_finalized`. `job.status=completed` is not enough for workflow terminal truth without typed policy proof.
  - Updated `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md` and `docs/NEXT_TODO.md` to record W6 方案 A, command-owned normal-path payloads, migration-adapter-only legacy refs, and the remaining signoff/deletion scope.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_completion_policy.py tests/test_workflow_completion_policy.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_workflow_completion_policy.py -q` -> `5 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py::LegacyMaterializationAdapterTest tests/test_durable_runtime.py::DurableRuntimeStorageTest -q` -> `25 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_results_api.py -k "terminal_proof or projection_event_time or collection_authoritative_merge or projection_person_search_index" -q` -> `13 passed, 268 deselected`
  - Remaining: full W6 signoff still needs targeted OpenAI/Lovable/Google, full PG-backed scripted matrix, fake-provider/containerized/browser gates, and service metrics proof that `normal_path_write_count=0` and `legacy_bridge_used=false`. Legacy `job_materialization_items` remains available only for historical migration-adapter input until that signoff passes.
- 2026-05-22 W6 scheme-A signoff preflight expansion:
  - Extended command-owned payload migration to the remaining W6 normal-path queues: search-seed discovery, projection facet/layering build, and snapshot compaction now plan/drain through `workflow_commands`; the legacy adapter can still convert historical rows for those kinds, but normal execution does not write or require `job_materialization_items`.
  - Public materialization diagnostics and smoke service metrics now include command-owned queue evidence, so snapshot/search backlog and status are visible without reviving legacy queue reads as a normal source of truth.
  - Updated W6 targeted regressions to assert strict normal-write mode with `SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES=1`, no normal-path `legacy_materialization_item` payload refs, and command-owned projection-facet partial/completed progress.
  - Validation so far:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_durable_runtime.py tests/test_workflow_service_metrics.py tests/test_results_api.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py tests/test_workflow_service_metrics.py tests/test_results_api.py -k "workflow_command or legacy_materialization or snapshot_full_materialization or search_seed_discovery or projection_facet_layering or materialization_items_api" -q` -> `12 passed, 357 deselected`
  - Remaining: run strict W6 service/signoff gates and then the full containerized/scripted signoff before physical deletion or permanent hard-disable of migration-era bridge code.
- 2026-05-23 W6b durable causality / PG-only preflight closeout:
  - Upgraded W6b from a loose auditability goal to a stricter durable-execution gate. Normal durable runtime storage for `workflow_events`, `workflow_current_state`, `workflow_commands`, and `runtime_outbox` now fails closed outside PG-only mode; SQLite durable runtime access is limited to an explicit pytest-only escape hatch while legacy unit tests are migrated.
  - `workflow_commands` causality is now persisted as physical columns, not only `payload.causality`: stage, causal group, parent/source event, artifact refs, produced counts, no-op reason, readiness effect, downstream refs, and causality schema version. Metrics and preflight read those physical columns as source of truth.
  - The reducer now fills default `stage_id`, `readiness_effect`, and inferred produced counts for known command types so normal `CommandPlanRequested` output forms a typed causality graph instead of sparse payload metadata.
  - Post-profile SLO pairing now exposes typed causal-group pair counts versus legacy snapshot heuristic pair counts. W6/nightly cases require `require_no_post_profile_heuristic_slo_pairing=true`, so long signoff validates latency/stability after contract proof instead of discovering hidden timestamp/snapshot metric drift.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py -q` -> `35 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_workflow_service_metrics.py -q` -> `67 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_scripted_smoke_signoff.py -q` -> `42 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok`
    - `make ci-pg-contract` -> passed and verified physical workflow command causality columns in disposable Postgres
    - `make ci-workflow-fake-provider` -> passed
    - `make ci-containerized-pre-release` -> passed for Docker doctor, PG contract, fake provider, seeded projection browser, and workflow-driven projection browser gates
  - Remaining: fresh targeted OpenAI/Lovable/Google scripted rerun and full W6/nightly matrix + Pre-Manual Signoff with `workflow_causality_contract.violation_detected=false`, `heuristic_pairing_used=false`, `normal_path_write_count=0`, and `legacy_bridge_used=false`; then delete or permanently hard-disable migration-era bridge paths and remove the pytest SQLite durable-runtime escape hatch after PG-backed unit fixture migration.
- 2026-05-23 W5c.4 cold archive manifest foundation:
  - Added `asset_consolidation_cold_archive_manifest_v1` plus `scripts/build_asset_consolidation_cold_archive_manifest.py` to turn a W5c plan into a read-only cold-backup manifest.
  - The manifest records snapshot source dirs, backup keys, file counts/sizes, optional per-file sha256, and a stable manifest digest, but it explicitly sets `deletion_allowed=false` and performs no file or registry mutation.
  - Archive candidates fail closed unless the plan decision is `archive_ready_for_cold_backup_review`, overlap status is `subsumed_by_reference`, unique identity count is zero, deletion blockers are absent, the local snapshot directory exists, and the file listing is complete.
  - Blocked archive candidates now fail before any directory/file/hash scan. This keeps W5c.4 bounded by contract gates first and avoids turning a mostly-blocked manifest into large unnecessary I/O.
  - Real local evidence was generated at `runtime/audits/asset_consolidation/w5c4_cold_archive_manifest_20260523T000000Z/large_companies_cold_archive_manifest.json`: status `partial_ready_for_cold_backup_review`; `22` archive candidates; `1` manifest-ready OpenAI snapshot `20260501T222111` (`2280` files, `87342301` bytes); `21` blocked. This closes the previous gap between overlap evidence and a reviewed cold-backup plan without introducing an archive executor or silent normal-reuse exclusion.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_consolidation_cold_archive_manifest.py scripts/build_asset_consolidation_cold_archive_manifest.py tests/test_asset_consolidation_cold_archive_manifest.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_asset_consolidation_cold_archive_manifest.py -q` -> `4 passed`
- 2026-05-24 W7/Public Web bridge hardening:
  - Retired the remaining normal-path CRM-to-target Public Web compatibility bridge. Legacy `/api/target-candidates/public-web...` aliases still require their migration/test override, but they now operate only on existing legacy target-candidate ids and no longer synthesize `target_candidates` rows for CRM records.
  - `sync_public_web_batch_to_crm_owner(...)` is now disabled by default and returns report-visible migration evidence instead of writing `target_candidate_public_web_v1` runs into `crm_public_web_*` owner tables. Historical target->CRM sync requires explicit `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC=1`.
  - Removed unused orchestrator sync helpers that copied target-candidate Public Web batches/runs/promotions into CRM owner tables with the retired execution backend.
  - Moved projection/CRM API contract tests onto the PG-only durable runtime fixture because CRM Public Web promotion can enqueue projection person-search index work through `workflow_commands`; SQLite is not a valid normal-path runtime for that contract.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/target_candidate_public_web.py tests/test_projection_crm_api_contracts.py tests/test_target_candidate_public_web.py tests/test_results_api.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/target_candidate_public_web.py tests/test_projection_crm_api_contracts.py tests/test_target_candidate_public_web.py tests/test_results_api.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_target_candidate_public_web.py -q` -> `14 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_projection_crm_api_contracts.py -k "crm_public_web_reads_fail_closed_without_owner_projection or public_web" -q` -> `7 passed, 4 deselected`
    - `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_results_api.py -k "legacy_target_public_web_endpoint_does_not_bridge_crm_record_ids or target_candidate_public_web_api_queues_idempotent_runs or target_candidate_public_web" -q` -> `8 passed, 288 deselected`
- 2026-05-24 W7/CRM Public Web typed command owner:
  - Added first-class `workflow_commands(command_type='crm.public_web.queue_batch', owner='crm_public_web_owner')` for CRM Public Web worker enqueue. `start_crm_record_public_web_search(...)` still creates CRM-owned batch/run rows and synchronously drains the one command for current UX, but worker creation is now a typed command-owner side effect rather than route-local execution.
  - Added command idempotency/causality coverage for `crm.public_web.queue_batch`; produced counts capture CRM Public Web runs and CRM records, readiness effect is `crm_public_web_workers_queued`, and disabling `CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_OWNER_ENABLED` leaves the command queued with report-visible skip instead of falling back to target-candidate bridges.
  - Updated `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `docs/CRM_STATE_CONTRACT.md`, `docs/TESTING_PLAYBOOK.md`, and `docs/NEXT_TODO.md` with the new owner boundary and follow-up signoff metric work.
- 2026-05-24 W7/CRM Public Web command-owner signoff extraction:
  - Extended `workflow_service_metrics.target_candidate_public_web` with `queue_batch_command_*` counts for `crm.public_web.queue_batch` status, owner, missing proof, and causality completeness. A CRM-owned batch without a succeeded `crm_public_web_owner` queue command now fails the fast contract layer before W6/nightly.
  - Updated hosted smoke action extraction so CRM Public Web's independent workflow command evidence is appended to the case-level `workflow_commands` diagnostics; the legacy job materialization endpoint is no longer the only diagnostic source for this post-workflow action.
  - Added `require_crm_public_web_queue_batch_command` and zero-tolerance pending/failed/invalid-owner/incomplete-causality expectations to the target Public Web service matrix and service-gate manifest.
  - Hard-disabled legacy target-candidate Public Web orchestrator start/cancel/retry/list methods unless `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS=1` is set for explicit migration/test coverage. This closes the internal bypass where API aliases returned `410` but direct orchestrator calls could still enqueue `target_candidate_public_web_v1` workers.
- 2026-05-24 W7/target-candidate Public Web direct-helper retirement:
  - Extended the retirement guard below API/orchestrator into the legacy helper layer. `start_target_candidate_public_web_batch(...)`, `cancel_target_candidate_public_web_run(...)`, `execute_target_candidate_public_web_run_once(...)`, and `sync_public_web_batch_summary(...)` now return a report-visible `legacy_target_public_web_execution_disabled` envelope unless `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS=1` is set.
  - Kept CRM Public Web normal path unchanged: CRM-owned batches/runs still use `crm.public_web.queue_batch`, `crm_public_web_v1`, and `crm_public_web_search` worker recovery.
  - Split worker recovery onto a CRM-named wrapper: `crm_public_web_search` now calls `execute_crm_public_web_run_to_local_idle(...)`, while legacy `target_candidate_public_web_search` remains migration-only. Deleted the generic recovery-kind resolver that defaulted to the target-candidate owner.
  - Validation:
    - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/target_candidate_public_web.py tests/test_target_candidate_public_web.py` -> passed
    - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py -q` -> `15 passed`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k "target_candidate_public_web_orchestrator_start_is_retired_without_migration_override or target_candidate_public_web_api_queues_idempotent_runs or legacy_target_public_web_endpoint_does_not_bridge_crm_record_ids or crm_public_web"` -> `3 passed, 294 deselected`
    - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_worker_daemon.py tests/test_target_candidate_public_web.py -q` -> `22 passed`
    - Targeted CRM Public Web service smoke passed at `output/w7_target_public_web_direct_helper_retirement_20260524/`: case `target_public_web_service_slo_from_workflow_result`, `expectation_failures=[]`, Pre-Manual Signoff `passed`, blocking findings `0`, manual-review findings `0`, warnings `0`, all `7` provider invocations scripted, and `target_public_web_contract` passed.
