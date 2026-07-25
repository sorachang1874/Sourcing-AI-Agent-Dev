# Canonical Job Result Lifecycle Design

> Status: Implemented contract. Companion to `archive/STREAMING_WORKFLOW_REBUILD_PLAN.md`. Updated 2026-05-10 after final public-read/backfill fallback retirement.

## Why this slice first

Before this contract, every public API path rebuilt `result_view_lifecycle` independently from a different mix of inputs:

- `get_job_dashboard` (`orchestrator.py:5735`) feeds `candidate_source` + `result_view` + `asset_population`.
- `get_job_candidate_page` (`orchestrator.py:5841` and `5889`) feeds the same builder twice with different inputs depending on whether result mode is `asset_population` or `ranked_results`.
- `get_job_progress` (`orchestrator.py:8608`) feeds the builder with `candidate_source={}` and `asset_population={}`.

The old lifecycle projection helper reconstructed lifecycle from job summary, candidate source, result-view metadata, asset population, stage1 preview, organization execution profile, candidate materialization states, registry rows, and snapshot files, with a five-fallback ladder for `delta_profile_materialized_count`. That helper is now deleted; public reads cannot call it and backfill cannot use it.

The OpenAI Infra failure (`c5248ea4b3b4`) is a structural consequence: the job is terminal, `job_result_views` still serves baseline `20260430T130836`, lifecycle reads `delta_profile_required/fetched=89/89` but `delta_profile_materialized_count=19`, and `/dashboard` and `/candidates` time out trying to rebuild artifacts on the request hot path.

This design retires the rebuild-per-request path and routes every public reader through one persisted lifecycle row.

## Storage shape

New table `job_result_lifecycle`, owned by the workflow writer. PG-only via `_write_control_plane_row_to_postgres`(Track B B4.3 后 SQLite mirror 机器已删除;写路径 fail-closed 到 PG)。

| column | type | notes |
| --- | --- | --- |
| `job_id` | TEXT PK | one row per workflow job |
| `view_id` | TEXT | matches `job_result_views.view_id` once a result view is published |
| `company_key` | TEXT | normalized company key (matches `job_result_views.company_key`) |
| `workflow_kind` | TEXT | e.g. `linkedin_stage_1`, `excel_intake`, `live_roster` |
| `phase` | TEXT | enum, see Phases below |
| `phase_status` | TEXT | `active` / `paused` / `awaiting_provider` / `awaiting_writer_budget` / `error` |
| `baseline_snapshot_id` | TEXT | empty when no baseline applies |
| `current_snapshot_id` | TEXT | snapshot the workflow is building (may equal served until repoint) |
| `served_snapshot_id` | TEXT | snapshot currently backing the board |
| `served_generation_key` | TEXT | optional; matches result view metadata |
| `serving_projection_id` | TEXT | identifies a partial-delta projection if active |
| `serving_projection_phase` | TEXT | `baseline_only` / `patch_applying` / `patch_serving` / `snapshot_compacting` / `current_snapshot_serving` |
| `baseline_candidate_count` | INTEGER | |
| `expected_candidate_count` | INTEGER | |
| `served_candidate_count` | INTEGER | |
| `delta_profile_required_count` | INTEGER | |
| `delta_profile_fetched_count` | INTEGER | |
| `delta_profile_applied_count` | INTEGER | applied to job snapshot |
| `delta_profile_materialized_count` | INTEGER | board-visible (result of materialization patch) |
| `stage1_current_search_returned_count` | INTEGER | |
| `stage1_former_search_returned_count` | INTEGER | |
| `stage1_all_search_returned_count` | INTEGER | |
| `stage1_deduped_candidate_count` | INTEGER | |
| `stage1_deduped_profile_url_count` | INTEGER | |
| `stage1_profile_fetch_required_count` | INTEGER | |
| `stage1_profile_fetched_count` | INTEGER | |
| `source_validation_status` | TEXT | `validated` / `pending_validation` / `stale_pending_repair` |
| `last_event_id` | INTEGER | projection cursor into `job_events` (or workflow event log) |
| `projection_source_snapshot_id` | TEXT | snapshot whose Stage 1 lane files were the source of the current projection — used to reject older lane writes |
| `metadata_json` | TEXT | open-ended (timestamps, debug info) |
| `created_at`, `updated_at` | TEXT | |

Indexes: `(company_key, updated_at DESC)`, `(view_id)`, `(phase, updated_at DESC)`.

### Phases

```
planning
  -> baseline_serving                 (delta workflows with valid baseline)
  -> acquiring                        (Stage 1 search/roster)
  -> profiles_fetching                (provider profile workers in flight)
  -> delta_applying                   (local apply / projection patches)
  -> current_materializing            (writer-budget materialization)
  -> current_serving                  (snapshot or projection serves complete delta)
  -> post_result_layering             (outreach/facets refresh)
  -> failed
  -> abandoned                        (user cancel / retry replaced)
```

Phase transitions are append-only via the projection event log; the row stores the *current* phase and timestamp, but the log is replayable.

## Writer ownership

Exactly four writer paths update the row, and they are the only writers:

1. **Workflow create / planner** (`orchestrator._create_workflow_job`) writes the initial row in `phase=planning`.
2. **Baseline-serving publication** (`_publish_baseline_serving_result_view`) transitions to `baseline_serving` and sets `view_id`/`served_snapshot_id`/baseline counts.
3. **Stage 1 / scheduler projection** (a new `update_job_result_lifecycle_from_stage1` writer fed by the existing Stage 1 progress builder) updates Stage 1 lane counts, dedupe, profile required/fetched/applied. It rejects writes whose `projection_source_snapshot_id` is older than the stored one.
4. **Materialization / repoint** (`snapshot_materializer` + completed-workflow reconcile) updates `current_snapshot_id`, `served_snapshot_id`, `delta_profile_materialized_count`, and transitions phase to `current_materializing` / `current_serving`.

Repair paths (`_recover_delta_only_result_view`, completed-summary merge) become *validators* rather than writers: they may propose a new `phase`/`served_snapshot_id`, but they must call into one of the four writers, not mutate `job.summary` or `result_view.metadata` directly.

## Reader ownership

`get_job_dashboard`, `get_job_candidate_page`, and `get_job_progress` all read this row via `_load_job_result_lifecycle(job_id)`. They do not reconstruct lifecycle from `candidate_source` / `asset_population` / `result_view.metadata` / stage summary files.

Public reads render the validated `job_result_lifecycle` row. If the row is
missing or unvalidated, they return a `lifecycle_repair_required` diagnostic
and do not rebuild, patch, or write lifecycle state from job/result-view
summaries or dynamic Stage 1 progress.

## Derived sources retired

This slice retires:

- Per-request reconstruction of lifecycle in three public APIs (different inputs → different answers for the same job at the same time).
- The five-source `delta_profile_materialized_count` ladder. The persisted value is updated by exactly one writer (materialization).
- `stage1_preview_baseline_count` leaking from a stale baseline-serving result view into the current job (the projection writer rejects stale `projection_source_snapshot_id`).
- Frontend reading `payload.progress.result_view_lifecycle` *and* `payload.result_view_lifecycle` separately — server populates both from the same row, so they cannot disagree.

## Failure classes made impossible

- **Terminal job + stale baseline-only board** (Infra `c5248ea4b3b4`): a job cannot reach `phase=current_serving` while `served_snapshot_id == baseline_snapshot_id` and `delta_profile_required_count > delta_profile_materialized_count`. The materialization writer is the only path that can advance phase out of `delta_applying`.
- **Mixed-source Stage 1 progress** (Infra `current=0/former=10/profile_required=77`): atomic Stage 1 writer commits all lane counts + profile denominator from one projection snapshot. Worker-derived URL counts go to a separate `queued_profile_url_count` field that the frontend renders as a technical metric, never in the same place as `current/former/deduped`.
- **Stage-summary leakage from a baseline-serving view to the current job** (Health drift): readers consult only the lifecycle row; baseline result-view stage summaries are not in the read path.
- **`delta_profile_materialized_count` differing between `/dashboard` and `/progress`**: same row, same value, regardless of caller.

## Migration

1. Create the table(现状:表在 `migrations/0001_baseline.sql` 基线内,由 migration runner / `ensure_bootstrapped()` 建;backfill 入口的建表保障已改走 `ensure_bootstrapped()`,B4.3f)。
2. `backfill-job-result-lifecycle` migrates only existing serialized canonical evidence from `job_result_views.metadata.result_view_lifecycle` or `jobs.summary.result_view_lifecycle` into the new row and marks the row `validated`.
3. `_load_job_result_lifecycle` never falls back to in-flight rebuild. Missing or unvalidated rows are explicit repair-required states that must be handled by event-time writers or the backfill/repair command.
4. Jobs without serialized lifecycle evidence are reported as repair-required; they are not synthesized into validated rows from mixed legacy sources.

## Validation

- New cross-API consistency test (`test_lifecycle_consistent_across_dashboard_progress_and_candidate_page`) reproduces the Infra shape and asserts identical lifecycle from all three public APIs.
- Regression tests assert the legacy projection helper is absent and that public reads do not infer lifecycle counters from candidate materialization state or dynamic Stage 1 progress.
- Add scheduler latency test for next-submit lag (a separate slice, scheduled in `archive/STREAMING_WORKFLOW_REBUILD_PLAN.md` §3).

## Out of scope for this slice

- Adaptive batch packer / queue-first scheduling (slice 4).
- Partial delta board streaming (slice 5).
- Item-level state machine (slice 4 prerequisite).

The lifecycle row design intentionally pre-allocates fields (`serving_projection_id`, `serving_projection_phase`) so those slices can extend the contract without another schema change.

## Implementation status (2026-04-30)

Landed (slice 1 — canonical reader + persistence):

- `job_result_lifecycle` table in `storage.py`, registered in `control_plane_postgres.DEFAULT_CONTROL_PLANE_TABLES` and `control_plane_live_postgres.CONTROL_PLANE_LIVE_TABLES` with primary key `job_id`.
- `ControlPlaneStore.upsert_job_result_lifecycle` (patch-style merge) and `get_job_result_lifecycle`.
- `SourcingOrchestrator._load_job_result_lifecycle` as the single canonical reader.
- Public readers `get_job_dashboard`, `get_job_candidate_page` (both branches), and `get_job_progress` route through `_load_job_result_lifecycle`. Served candidate count is derived uniformly from `job_result_views.summary.candidate_count`, retiring the per-caller `asset_population` divergence.
- Anchor regression `test_lifecycle_consistent_across_dashboard_progress_and_candidate_page_for_openai_infra_shape` passes.

Landed (slice 2 — event-driven writers + reader fast path):

- Four canonical writers on `SourcingOrchestrator`:
  - `initialize_job_result_lifecycle` — workflow create / planning row.
  - `publish_baseline_job_result_lifecycle` — baseline-serving transition.
  - `update_job_result_lifecycle_from_materialization` — current-snapshot repoint with delta counters.
  - `mark_job_result_lifecycle_terminal` — terminal-state normalization (encodes the OpenAI Infra invariant directly: a finished job at stale baseline with materialization incomplete is recorded as `current_snapshot_materializing`/`delta_applying`, never `baseline_serving`).
- `_create_workflow_job` calls `initialize_job_result_lifecycle`; `_persist_delta_baseline_serving_result_view` calls `publish_baseline_job_result_lifecycle`. Both run before any public API touches the job, so dashboards see a persisted row from the first read.
- `_load_job_result_lifecycle` validated fast path: when the row has `source_validation_status='validated'` and `_lifecycle_row_needs_stage1_repair` is false, the public read returns the row unchanged. The terminal-phase invariant is still applied at render time.
- `_lifecycle_row_needs_stage1_repair` is the explicit narrow stale-detection helper. A row at `phase=planning` or `baseline_serving` falls through to projection when Stage 1 progress shows real work has happened. This goes away when the Stage 1 writer lands.
- Tests: `test_job_result_lifecycle_writer_initialize_persists_planning_row_without_public_read`, `test_job_result_lifecycle_writer_publish_baseline_persists_row_before_public_read` (also asserts validated public reads do not mutate canonical fields), `test_job_result_lifecycle_writer_materialization_repoint_updates_row`, `test_job_result_lifecycle_writer_terminal_normalizes_stale_baseline_infra_shape`.
- Test contract updates: two pre-existing direct-builder tests (`...counts_profile_detail_materialized_to_board_from_state`, `...infers_baseline_count_from_stage1_preview`) encoded contracts the canonical reader retired (profile-richness completeness as materialization metric; on-disk `candidate_documents.json` row count as `served_candidate_count`). Updated with explicit pointers to the design doc and `WORKFLOW_PROGRESS_CONTRACT.md`.

Landed (slice 3 — Stage 1 projection writer):

- `update_job_result_lifecycle_from_stage1` commits one coherent Stage 1 projection: current/former/all lane counts, deduped candidate count, deduped profile URL count, profile fetch required, profile fetched, plus mirrored `delta_profile_required/fetched_count` for delta workflows. `projection_source_snapshot_id` is recorded on every write; concurrent older-snapshot writes are rejected by lex comparison (snapshot ids are timestamp-prefixed and monotonic).
- Mixed-source invariant enforced at the writer: when the lane snapshot returns no rows, `stage1_profile_fetch_required_count` stays 0 even if workers report queued URLs. Worker URL totals come in via `queued_profile_url_count_from_workers` and land in `metadata_json.workers.queued_profile_url_count` only — never in the public-facing denominator. The OpenAI Infra `current=0/former=10/profile_required=77` failure mode is impossible by construction.
- Phase progression: from `planning`/`baseline_serving` to `delta_applying` (delta workflows) or `current_snapshot_serving` (full local reuse / live roster), monotonic.
- `expected_candidate_count` for in-progress delta workflows holds at the stable baseline/served value until the Stage 1 discovery-lane registry reports terminal; promotion to `baseline + delta_profile_required_count` is one-shot and metadata-backed.
- `_compute_and_record_stage1_progress` is deprecated and no longer used by public readers. Stage 1 writes happen at workflow event-time; public readers render the validated row or return repair-required diagnostics.
- `_lifecycle_row_needs_stage1_repair` is no longer a public-read fallback trigger. It is retained only as a diagnostic/legacy guard until production backfill evidence allows deleting it with the legacy builder.
- Tests: `test_job_result_lifecycle_writer_stage1_persists_atomic_projection`, `test_job_result_lifecycle_writer_stage1_rejects_stale_snapshot_writes`, `test_job_result_lifecycle_writer_stage1_rejects_mixed_source_infra_shape`, `test_job_result_lifecycle_validated_stage1_row_is_not_mutated_by_public_reads`.

Landed (slice 4 — Stage 1 event-time writes):

- Wired `update_job_result_lifecycle_from_stage1` at the real workflow event-time hook: `_persist_running_job_inline_reconcile_state`. This method is called when `acquisition_progress.latest_state` is updated with a new snapshot_id and search_seed_snapshot after search-seed/roster workers complete. The Stage 1 writer runs immediately after `save_job`, building the coherent projection from the fresh `latest_state` and persisting it into the canonical row. Public reads now find the row already up to date.
- Removed public-read mutation: the three public-API call sites (`_build_job_results_context`, `get_job_candidate_page`, `get_job_progress`) reverted from `_compute_and_record_stage1_progress` back to the non-mutating `_build_linkedin_stage1_progress_payload`. Public reads no longer write to the canonical lifecycle row for Stage 1 fields.
- Deprecated `_compute_and_record_stage1_progress`: marked as deprecated, retained only for compatibility. It now just calls `_build_linkedin_stage1_progress_payload` without any lifecycle write.
- Narrowed `_lifecycle_row_needs_stage1_repair` to pure legacy/backfill: only triggers for rows with no `projection_source_snapshot_id` AND no Stage 1 counters. The event-time writer always sets both, so new jobs hit the validated fast path. Documented as deletable once legacy backfill ships.
- Test: `test_job_result_lifecycle_stage1_event_time_write_before_public_read` proves public reads do NOT mutate Stage 1 fields after the writer has run.
- Historical failure now impossible: the OpenAI Infra mixed-source bug (`current=0, former=10, profile_required=77`) cannot happen because the event-time writer commits one coherent projection and public reads no longer rebuild Stage 1 fields.

Landed (slice 5 — materialization/repoint event-time writes):

- Wired `update_job_result_lifecycle_from_materialization` at two key materialization event-time hooks:
  1. `_recover_delta_only_result_view_to_baseline_delta_overlay` line 4807 — after the current-snapshot repoint upsert.
  2. `_recover_delta_only_result_view_to_baseline_delta_overlay` line 4888 — after the baseline+delta overlay upsert.
- Both hooks run immediately after `upsert_job_result_view`, so the canonical lifecycle row is updated before any public read can observe the new result view.
- Test: `test_job_result_lifecycle_baseline_delta_overlay_for_health_whisper_shape` proves the materialization writer records `served_snapshot_id = current` and `served_candidate_count = baseline + delta` for the Health/Whisper shape.
- Historical failure addressed at the public-read layer: baseline+delta jobs cannot complete into raw delta-only result views at the event-time level, and public reads no longer rebuild lifecycle from old sources when no stored result_view exists.

Landed (slice 6 — canonical reader fallback projection retirement):

- Retired `_build_result_view_lifecycle_payload` from the canonical reader fast path. Public reads (`/dashboard`, `/progress`, `/candidates`) now trust the validated `job_result_lifecycle` row as the source of truth, even when no stored `result_view` exists yet.
- Fixed `_lifecycle_row_needs_stage1_repair` to not require Stage 1 counters when the row has `served_snapshot_id` (materialization writer is authoritative). This allows materialization-writer-updated rows to hit the fast path without falling back to projection.
- Quarantined `_build_result_view_lifecycle_payload` for legacy/backfill only: added `_allow_for_legacy_backfill` parameter that raises `ValueError` without explicit opt-in. `_load_job_result_lifecycle` no longer calls this builder; missing or unvalidated rows render `lifecycle_repair_required`.
- Enabled full cross-API consistency assertions in `test_job_result_lifecycle_baseline_delta_overlay_for_health_whisper_shape`. The test now asserts that `/dashboard`, `/progress`, and `/candidates` return identical lifecycle payloads for the Health/Whisper baseline+delta overlay shape.
- Tests: `test_job_result_lifecycle_validated_row_without_result_view_uses_canonical_row`, `test_job_result_lifecycle_terminal_invariant_baseline_served_incomplete_delta`, `test_job_result_lifecycle_fallback_projection_quarantined_for_validated_rows`.
- Historical failure now impossible: the Health/Whisper baseline+delta overlay regression (where `/dashboard`, `/progress`, `/candidates` returned divergent lifecycle payloads) cannot happen because all three APIs read from the same canonical row, and the fallback projection is quarantined for legacy/backfill only.

Landed (slice 7 — failed-state writer wiring):

- Wired `mark_job_result_lifecycle_terminal(outcome='failed')` at three terminal failure paths:
  1. `_mark_workflow_failed` (orchestrator.py:24930) — workflow-level failure handler.
  2. `_run_excel_intake_workflow` exception handler (orchestrator.py:18384) — Excel intake terminal failure.
  3. `_run_retrieval_job` exception handler (orchestrator.py:26039) — retrieval job terminal failure.
- Terminal failed rows have `phase='failed'`, `state='failed'`, `phase_status='terminal'`, and `source_validation_status='validated'`.
- `_lifecycle_row_needs_stage1_repair` now recognizes terminal failed rows as authoritative and never triggers repair for them.
- Tests: `test_job_result_lifecycle_failed_workflow_writes_terminal_row`, `test_job_result_lifecycle_excel_intake_failure_writes_terminal_row`, `test_job_result_lifecycle_retrieval_job_failure_writes_terminal_row`, `test_job_result_lifecycle_failed_state_consistent_across_endpoints`, `test_job_result_lifecycle_failed_public_read_does_not_mutate`.
- Cross-endpoint consistency: `/dashboard`, `/progress`, and `/candidates` all report identical failed lifecycle state for failed jobs, reading from the same canonical row.
- Public read immutability: validated failed rows are never mutated by public reads; the terminal writer is the sole source of truth.

Landed (slice 7b — completed-state writer wiring):

- Successful workflow completion now closes the same lifecycle state machine. `_save_workflow_job_state(status='completed', stage='completed')` and the direct `_execute_retrieval(... persist_job_state=True, job_type='workflow')` completion path call `_mark_completed_workflow_lifecycle_terminal_if_present`, which delegates to `mark_job_result_lifecycle_terminal(outcome='completed')` when a lifecycle row already exists.
- Public readers remain non-mutating. A job that is stored as `completed/completed` must not rely on `/progress`, `/dashboard`, or `/candidates` to infer terminal lifecycle status; the writer must persist `phase_status='terminal'` at completion time.
- Regression: `test_completed_workflow_save_marks_existing_lifecycle_terminal` covers the no-baseline/current-snapshot shape where profile/card work is complete but the lifecycle row previously stayed `current_snapshot_materializing/active`, causing scripted runners to wait despite `jobs.status='completed'`.

Landed (slice 8 — fail-closed legacy migration):

- `backfill_job_result_lifecycle` now migrates only already-serialized canonical lifecycle payloads from `job_result_views.metadata.result_view_lifecycle` or `jobs.summary.result_view_lifecycle`.
- The command no longer builds a lifecycle projection from job summary, result view, asset population, Stage 1 progress, worker ledger, registry rows, or candidate materialization state. Jobs without serialized lifecycle evidence are counted under `jobs_repair_required` / `jobs_legacy_projection_retired` and remain without a validated row.
- The migration keeps schema preflight and idempotency: validated rows are skipped, dry-run reports migratable evidence without writing, and re-running after migration leaves `updated_at` unchanged.
- CLI command `backfill-job-result-lifecycle` remains as an audit/migration tool with `--dry-run`; it is no longer a synthetic repair path.
- Tests prove validated rows are skipped, serialized result-view/job-summary evidence can be migrated, jobs without evidence are repair-required, dry-run does not persist, and schema preflight runs before the first lifecycle read.

Landed (slice 9 — final helper deletion):

- Deleted the legacy lifecycle projection helper, stale Stage 1 repair detector, and deprecated public-read Stage 1 compute wrapper from `SourcingOrchestrator`.
- `_load_job_result_lifecycle` now has exactly two outcomes: render a validated canonical row with terminal render-time normalization, or return `lifecycle_repair_required`. It does not overlay dynamic `linkedin_stage_1_progress` onto the lifecycle payload.
- Direct-builder unit tests were replaced with canonical-row/public-read regressions. One regression confirms candidate materialization state cannot backfill `delta_profile_materialized_count` on read; another confirms stale board-visible counters are normalized only from canonical lifecycle row fields.

Deferred:

- Historical jobs that lack validated lifecycle rows and lack serialized canonical evidence need targeted repair or rerun through event-time writers. They must not be auto-repaired by public-read/backfill projection.
