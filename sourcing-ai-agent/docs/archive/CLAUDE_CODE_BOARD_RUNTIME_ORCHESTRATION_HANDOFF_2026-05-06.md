# Claude Code Board Runtime Orchestration Handoff - 2026-05-06

> Status: Archived 2026-06-11. Historical record only — do not treat as active guidance; see `docs/INDEX.md` for current docs. (Previous status: Current incident handoff. Start here when fixing the PG-backed manual scripted candidate-board streaming failures. Read with `CLAUDE_CODE_STREAMING_WORKFLOW_REB)

## Engineering Goal

Fix the candidate-board streaming workflow as a service-level state-machine problem, not as a frontend wording bug.

The product target is a single backend-driven board contract where provider fetch, local apply, card-ready serving projection, full snapshot compaction, candidate filtering, pagination, and layering are explicit, monotonic, auditable stages. The frontend should consume that contract and stop deriving canonical business state from local row caches, pending page length, or stale dashboard merges.

## Current Runtime Evidence

Use the PG-backed manual scripted runtime. Do not reproduce this against disk SQLite; SQLite hid or distorted the same failure class.

- Runtime dir: `runtime/test_env/openai_agent_delta_streaming`
- PG schema: `sourcing_scripted_openai_agent_delta`
- OpenAI Agent job: `55738bb72427`
- OpenAI snapshot: `20260506T073020`
- OpenAI expected final population: baseline `300` + delta `297` = `597`
- Lovable live-roster job: `ae5e2a1487a2`
- Lovable snapshot: `20260506T074044`
- Lovable expected final population: `140`

## User-Visible Failures

These are the failures Claude should reproduce or verify before changing code:

- OpenAI candidate-board denominator changed during execution: `300/351`, `300/425`, `300/525`. The business denominator should be stable at `597`; the delta denominator should be stable at `297`.
- OpenAI profile fetch progressed, but card-ready progress stayed stuck: `新增 LinkedIn Profile 已取回 117/297`, then `276/297`, while `卡片详情已合入看板 0/297`; later it jumped to final.
- OpenAI execution process showed impossible Stage 1 math, for example `current=0`, `former=74`, `deduped=241`, `profile_required=241`. Final intended projection is `223 + 74 = 297`.
- OpenAI recall filter `Agent` could be selected while the run was active, but matching newly materialized candidates were not reliably visible until `results`.
- OpenAI pagination got stuck on page 1 even when the footer said `第 1 / 13 页`; clicking next did not advance because frontend total/page state could collapse while a backend-filtered page request was pending.
- Lovable execution process showed impossible math such as `current=120`, `former=0`, `deduped=132`.
- Lovable board stayed at the empty placeholder even while profile fetch progress existed, then jumped to final `140/140`.
- Layering could remain `分层未生成` after left history had already changed to `results`; manual refresh could show the correct state, proving stale cache/freshness merge is still involved.

## Root-Cause Interpretation

The failure is not simply provider latency and not simply frontend copy.

Provider/profile workers did complete and remote provider events were observed. The broken point is the dependency edge between provider completion and board-visible serving:

- Provider/profile completion creates input data.
- Local apply closure sometimes runs before the candidate document prerequisite is visible.
- `local_apply_closure` then fails as `candidate_documents_missing`.
- Later daemon/recovery/reconcile groups the work and succeeds.
- The UI sees profile fetch increase, card-ready stay at `0`, then a large jump.

Treat `local apply 依赖顺序错误` as a streaming workflow orchestration defect. It means provider completion, dataset ingest, candidate document creation, card-ready projection, patch publication, lifecycle update, and candidate page visibility are not one explicit event-time dependency graph.

## Do Not Overstate Provider Underuse

The PG scripted run does not prove Harvest/Apify actors were globally idle.

- OpenAI had profile batch waves that filled provider limiter budget `4`.
- Lovable also had waves with active counts `1,2,3,4`.
- Raw `agent_worker_runs` overlap can exceed provider budget because local worker rows include local/recovery/apply lifetime, not just remote actor occupancy.
- Use `provider_limiter.active_count`, `run_id`, `dataset_id`, and remote provider timestamps to evaluate actor occupancy.

The main service inefficiency found here is downstream event application and card-ready publication lag. If Claude optimizes only batch size or actor count, the board can still fail.

## SQL Verification Packet

Run these against local Postgres:

```bash
psql 'postgresql://changyuyi@127.0.0.1:5432/sourcing_agent' -P pager=off -F $'\t' -A -c "
select job_id,state,phase,phase_status,current_snapshot_id,served_snapshot_id,
serving_projection_phase,baseline_candidate_count,expected_candidate_count,
served_candidate_count,delta_profile_required_count,delta_profile_fetched_count,
delta_profile_applied_count,delta_profile_materialized_count,
delta_profile_board_visible_count,
stage1_current_search_returned_count,stage1_former_search_returned_count,
stage1_deduped_candidate_count,stage1_profile_fetch_required_count,
stage1_profile_fetched_count,background_snapshot_materialization_status,
outreach_layering_status,updated_at
from sourcing_scripted_openai_agent_delta.job_result_lifecycle
where job_id in ('55738bb72427','ae5e2a1487a2')
order by updated_at desc;"
```

Current bad evidence to look for:

- OpenAI can have `served_candidate_count=597`, `delta_profile_required_count=297`, `delta_profile_fetched_count=297`, but lower `delta_profile_materialized_count` / `delta_profile_board_visible_count`.
- Lovable can show final serving counts while delta/apply fields do not represent live-roster card-readiness the same way as baseline+delta.

```bash
psql 'postgresql://changyuyi@127.0.0.1:5432/sourcing_agent' -P pager=off -F $'\t' -A -c "
select job_id,sequence_index,patch_kind,patch_phase,reason,candidate_count,
cumulative_candidate_count,served_candidate_count,published_at,
metadata_json::jsonb #>> '{card_materialization_summary,candidate_count}' as card_count,
metadata_json::jsonb #>> '{card_materialization_summary,display_ready_candidate_count}' as display_ready,
metadata_json::jsonb #>> '{card_materialization_summary,profile_detail_candidate_count}' as profile_detail,
metadata_json::jsonb #>> '{card_materialization_summary,explicit_profile_capture_candidate_count}' as explicit_capture,
metadata_json::jsonb #>> '{card_materialization_summary,preview_candidate_count}' as preview_count
from sourcing_scripted_openai_agent_delta.job_board_visible_patches
where job_id in ('55738bb72427','ae5e2a1487a2')
order by job_id,sequence_index;"
```

Current bad evidence to look for:

- OpenAI patch ledger can mix partial cumulative patch counts with final mutable card-quality counts.
- Lovable patch ledger can mix shell roster publication and complete-card readiness under one board-visible concept.

```bash
psql 'postgresql://changyuyi@127.0.0.1:5432/sourcing_agent' -P pager=off -F $'\t' -A -c "
select job_id,item_kind,status,item_id,attempt_count,left(coalesce(last_error,''),120) as last_error,
created_at,updated_at,
round(extract(epoch from (updated_at::timestamp-created_at::timestamp))::numeric,1) as item_wall_s,
metadata_json::jsonb #>> '{worker_id}' as worker_id,
metadata_json::jsonb #>> '{worker_ids}' as worker_ids
from sourcing_scripted_openai_agent_delta.job_materialization_items
where job_id in ('55738bb72427','ae5e2a1487a2')
and item_kind in ('local_apply_closure','board_visible_delta_apply')
order by job_id,created_at,item_id;"
```

Current bad evidence to look for:

- `local_apply_closure` rows failing with `candidate_documents_missing`.
- Board-visible apply rows starting much later than remote provider completion.
- Retry storms or repeated failed attempts for what is actually a waiting prerequisite.

```bash
psql 'postgresql://changyuyi@127.0.0.1:5432/sourcing_agent' -P pager=off -F $'\t' -A -c "
select worker_id,job_id,worker_key,status,attempt_count,created_at,updated_at,
round(extract(epoch from (updated_at::timestamp-created_at::timestamp))::numeric,1) as worker_wall_s,
jsonb_array_length(coalesce(input_json::jsonb -> 'profile_urls', '[]'::jsonb)) as input_urls,
output_json::jsonb #>> '{summary,run_id}' as run_id,
output_json::jsonb #>> '{summary,requested_url_count}' as requested_urls,
output_json::jsonb #>> '{summary,dispatched_url_count}' as dispatched_urls,
output_json::jsonb #>> '{summary,persisted_profile_count}' as persisted_profiles,
output_json::jsonb #>> '{summary,provider_limiter,budget}' as limiter_budget,
output_json::jsonb #>> '{summary,provider_limiter,active_count}' as limiter_active_at_submit,
output_json::jsonb #>> '{summary,provider_limiter,wait_ms}' as limiter_wait_ms
from sourcing_scripted_openai_agent_delta.agent_worker_runs
where job_id in ('55738bb72427','ae5e2a1487a2')
and worker_key like 'harvest_profile_batch::%'
order by job_id,worker_id;"
```

Use this query to split provider occupancy from local worker lifetime. Do not infer provider slot underuse from `created_at/updated_at` overlap alone.

## Backend Code To Review First

Start with these files and functions. Line numbers are current as of 2026-05-06 and may drift.

- `src/sourcing_agent/orchestrator.py:4665` - `_publish_partial_board_visible_delta_overlay`
- `src/sourcing_agent/orchestrator.py:5422` - `_board_visible_delta_apply_item_id`
- `src/sourcing_agent/orchestrator.py:5432` - `_enqueue_board_visible_delta_apply_item`
- `src/sourcing_agent/orchestrator.py:5714` - `_process_local_apply_closure_item`
- `src/sourcing_agent/orchestrator.py:5962` - `_run_local_apply_closure_item_queue_once`
- `src/sourcing_agent/orchestrator.py:6424` - `_process_local_apply_closure_worker`
- `src/sourcing_agent/orchestrator.py:6483` - `_process_board_visible_delta_apply_item`
- `src/sourcing_agent/orchestrator.py:6855` - `candidate_documents_missing` branch
- `src/sourcing_agent/orchestrator.py:10373` - `get_job_dashboard`
- `src/sourcing_agent/orchestrator.py:10515` - `get_job_candidate_page`
- `src/sourcing_agent/orchestrator.py:10772` - `get_job_board_visible_patch_log`
- `src/sourcing_agent/orchestrator.py:12451` - `_build_linkedin_stage1_progress_payload`
- `src/sourcing_agent/orchestrator.py:12572` - `_stage1_progress_payload_from_lifecycle_row`
- `src/sourcing_agent/orchestrator.py:13674` - `update_job_result_lifecycle_from_board_visible_patch`
- `src/sourcing_agent/orchestrator.py:13771` - `mark_job_result_lifecycle_terminal`
- `src/sourcing_agent/orchestrator.py:13811` - `update_job_result_lifecycle_from_stage1`
- `src/sourcing_agent/orchestrator.py:14271` - `_load_job_result_lifecycle`
- `src/sourcing_agent/orchestrator.py:14498` - `_build_board_runtime_state`
- `src/sourcing_agent/orchestrator.py:15559` - `get_job_progress`
- `src/sourcing_agent/worker_daemon.py` - recovery loop ordering and activity semantics
- `src/sourcing_agent/remote_provider_events.py` - provider terminal event ingestion and late-event handling
- `src/sourcing_agent/workflow_event_response.py` - event lane registry and follow-up ownership
- `src/sourcing_agent/workflow_service_metrics.py` - service metrics for local apply backlog, board projection, worker/provider timing
- `src/sourcing_agent/candidate_artifacts.py` and `src/sourcing_agent/snapshot_materializer.py` - candidate-doc prerequisite and serving-card materialization boundaries
- `src/sourcing_agent/control_plane_postgres.py` - PG tables involved: `job_result_lifecycle`, `job_board_visible_patches`, `job_materialization_items`, `agent_worker_runs`

## Frontend Code To Review First

Do this after backend contract shape is clear. Do not solve by adding another frontend fallback ladder.

- `frontend-demo/src/components/ResultsBoardPanel.tsx:449` - `currentPage`
- `frontend-demo/src/components/ResultsBoardPanel.tsx:812` - backend-filtered paging support
- `frontend-demo/src/components/ResultsBoardPanel.tsx:825` - `filteredCandidateCount` / visible count derivation
- `frontend-demo/src/components/ResultsBoardPanel.tsx:900` - deferred page state
- `frontend-demo/src/components/ResultsBoardPanel.tsx:903` - page reset when `currentPage > totalPages`
- `frontend-demo/src/components/ResultsBoardPanel.tsx:921` - backend page fetch effect
- `frontend-demo/src/pages/SearchPage.tsx:293` - `refreshDashboardFromBoardPatchLog`
- `frontend-demo/src/pages/SearchPage.tsx:377` - cached dashboard merged with progress
- `frontend-demo/src/pages/SearchPage.tsx:491` - progress-triggered board patch refresh
- `frontend-demo/src/pages/SearchPage.tsx:500` - completed branch freshness behavior
- `frontend-demo/src/hooks/useDashboardCandidateHydration.ts:75` - hydration pending logic from board runtime
- `frontend-demo/src/hooks/useDashboardCandidateHydration.ts:360` - dashboard replacement trigger fields
- `frontend-demo/src/lib/api.ts:3062` - `mergeDashboardRuntimeProgress`
- `frontend-demo/src/lib/api.ts:3178` - `boardRuntimeStateFreshnessScore`
- `frontend-demo/src/lib/api.ts:3209` - `pickFresherBoardRuntimeState`
- `frontend-demo/src/lib/api.ts:3229` - candidate page merge into dashboard
- `frontend-demo/src/lib/candidateSyncSummary.ts:178` - sync count builder
- `frontend-demo/src/lib/candidateSyncSummary.ts:203` - board runtime card-ready count selection

## Required Fix Shape

The fix should retire ambiguity rather than add compatibility displays.

- Make `board_runtime_state` an event-time projection from durable facts.
- Do not recompute earlier patch card-quality from mutable final overlays.
- Persist or project these counters separately: candidate discovery, profile required, profile fetched, local apply completed, card display-ready, published rows, expected population, served population.
- Treat `candidate_documents_missing` as `waiting_prerequisite` or deferred dependency, not a failed retryable item that later relies on recovery luck.
- Publish card-ready patches when profile detail has been locally applied to the serving projection; do not require full snapshot rebuild first.
- Use the same board runtime semantics for live roster and baseline+delta. Only the denominator shape differs.
- Make `/progress`, `/dashboard`, `/candidates`, and `/board-patches` return the same board runtime for the same watermark.
- Keep `/candidates` backend-filtered. Frontend-loaded rows are a window/cache only.
- Preserve last known backend-filtered count and total pages while a page request is pending.
- Force fresh dashboard/candidates after completion before showing stable final state; stale cached dashboards must not be rendered as final truth.

## Expected Regression Gates

Add tests that would fail on this exact incident:

- OpenAI PG manual scripted browser gate: denominator stays `597`, delta stays `297`, no `300/351` style transient denominator.
- OpenAI Stage 1 gate: no impossible math; `deduped_candidate_count` must not exceed coherent lane totals.
- OpenAI card-ready streaming gate: `profile_fetched_count` increases and `display_ready_candidate_count` advances before terminal `results`, not only after recovery.
- OpenAI filter gate: selecting recall bucket `Agent` while running returns Agent rows as soon as they are card-ready.
- OpenAI pagination gate: page 2 remains page 2 while backend page request is pending; `totalPages` does not collapse to `1`.
- Lovable live-roster gate: board shows profile/card-ready progress before final completion, not placeholder to final jump.
- Local apply gate: no `local_apply_closure` retry storm with `candidate_documents_missing`; waiting prerequisites are visible and bounded.
- Cross-endpoint gate: `/progress`, `/dashboard`, `/candidates`, and `/board-patches` agree on `expected`, `published`, `display_ready`, `profile_fetched`, `layering_status`, and watermark.
- Layering gate: after left history changes to `results`, layering becomes visible through polling within SLO, without manual refresh.
- Log gate: PG manual scripted run has no `database is locked`, no `another row available`, and no accidental fallback to disk SQLite.

## Suggested Validation Commands

Use targeted tests first. Do not trigger live Harvest/Apify unless the user explicitly asks.

```bash
cd /Users/changyuyi/projects/Sourcing\ AI\ Agent\ Dev/sourcing-ai-agent
PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "board_runtime or board_visible or local_apply_closure or stage1"
PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "local_apply_closure or board_visible or progress_auto or remote_wait"
PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py tests/test_frontend_candidate_sync_summary.py tests/test_frontend_dashboard_hydration.py -q
PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/worker_daemon.py src/sourcing_agent/workflow_service_metrics.py tests/test_results_api.py tests/test_pipeline.py
npm --prefix frontend-demo run build
```

Then run PG-backed scripted service/browser parity. The exact command may need adjustment to the current script flags, but the environment must remain PG-backed:

```bash
bash ./scripts/dev_scripted_openai_agent_delta.sh --print-config
```

The printed config must show:

- `control_plane=postgres`
- `postgres_schema=sourcing_scripted_openai_agent_delta`
- scripted local provider event watcher enabled

Use the scripted smoke matrix and browser/manual-parity gate only after the targeted tests pass.

## Boundaries For Claude

- Do not revert unrelated dirty worktree changes.
- Do not add another frontend-only patch to hide backend ambiguity.
- Do not rely on SQLite to validate concurrency, workflow recovery, or browser polling pressure.
- Do not mark the work done because final candidate count is correct; the intermediate workflow state is part of the product.
- Do not treat provider watcher fallback as proof that live webhook behavior is correct.
- Update `PROGRESS.md`, `docs/NEXT_TODO.md`, and relevant contract docs in the same pass as code changes.

## Ready-To-Send Claude Prompt

```text
You are working in an existing production-oriented recruiting automation / public-information enrichment codebase.

Repository:
/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent

Read first, in this order:
1. AGENTS.md
2. PROGRESS.md latest 2026-05-06 entries
3. docs/NEXT_TODO.md Highest Priority
4. docs/CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md
5. docs/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md
6. docs/WORKFLOW_PROGRESS_CONTRACT.md
7. docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md
8. docs/STREAMING_WORKFLOW_REBUILD_PLAN.md
9. docs/TESTING_PLAYBOOK.md

Task:
Fix the PG-backed manual scripted candidate-board streaming failures as a root-cause workflow orchestration problem. Do not solve this by adding frontend wording fallbacks.

The concrete failing runtime is PG schema sourcing_scripted_openai_agent_delta:
- OpenAI Agent job 55738bb72427, snapshot 20260506T073020, baseline 300, delta 297, expected final 597.
- Lovable job ae5e2a1487a2, snapshot 20260506T074044, expected final 140.

Observed failures:
- OpenAI board denominator changed during execution: 300/351, 300/425, 300/525. It should remain 597, with delta denominator 297.
- OpenAI Stage 1 showed impossible mixed-source math, e.g. current=0, former=74, deduped=241.
- OpenAI profile fetch progressed but card-ready board progress stayed 0, then jumped.
- OpenAI Agent filter selected during running did not reliably show newly card-ready Agent rows before results.
- OpenAI pagination could stay on page 1 even when total pages existed.
- Lovable showed current=120, former=0, deduped=132 and placeholder-to-final board jump.
- Layering could remain stale after results until manual refresh.

Root-cause hypothesis to verify:
Provider/profile workers and provider events are not the primary bottleneck. Local apply closure and board-visible serving projection are not unlocked event-by-event. local_apply_closure items can fail with candidate_documents_missing, then daemon/recovery/reconcile later groups work and succeeds. This causes profile_fetched to advance while card-ready stays at 0 and then jumps.

Review these backend targets first:
- src/sourcing_agent/orchestrator.py: _run_local_apply_closure_item_queue_once, _process_local_apply_closure_worker, candidate_documents_missing handling, _process_board_visible_delta_apply_item, _publish_partial_board_visible_delta_overlay, _publish_partial_board_visible_current_snapshot_overlay, _publish_full_snapshot_board_visible_serving, update_job_result_lifecycle_from_stage1, update_job_result_lifecycle_from_board_visible_patch, _build_board_runtime_state, get_job_progress, get_job_dashboard, get_job_candidate_page, get_job_board_visible_patch_log.
- src/sourcing_agent/worker_daemon.py
- src/sourcing_agent/remote_provider_events.py
- src/sourcing_agent/workflow_event_response.py
- src/sourcing_agent/workflow_service_metrics.py
- src/sourcing_agent/candidate_artifacts.py
- src/sourcing_agent/snapshot_materializer.py
- src/sourcing_agent/control_plane_postgres.py

Review these frontend targets after backend contract shape is clear:
- frontend-demo/src/components/ResultsBoardPanel.tsx
- frontend-demo/src/pages/SearchPage.tsx
- frontend-demo/src/hooks/useDashboardCandidateHydration.ts
- frontend-demo/src/lib/api.ts
- frontend-demo/src/lib/candidateSyncSummary.ts

Required implementation principles:
- board_runtime_state must be a backend event-time projection from durable facts.
- /progress, /dashboard, /candidates, and /board-patches must agree for the same watermark.
- candidate_documents_missing should become waiting_prerequisite/deferred dependency, not a retry storm that relies on later recovery luck.
- Live roster and baseline+delta must use the same card-ready/display-ready board contract.
- Frontend-loaded rows are only a cache/window; canonical counts, filters, pagination totals, and sync copy come from backend-filtered paging and board runtime.
- Preserve pagination totals while backend page requests are pending.
- Keep PG scripted/live parity. Do not validate this class of issue with SQLite.

Regression gates to add or update:
- stable OpenAI denominator 597 and delta denominator 297 through the whole run.
- no impossible Stage 1 math.
- no local_apply_closure candidate_documents_missing retry storm.
- OpenAI card-ready streaming visible before results.
- Lovable live-roster card-ready progress visible before final completion.
- Agent filter returns card-ready Agent candidates during running.
- pagination does not reset to page 1 during pending backend page refresh.
- /progress, /dashboard, /candidates, and /board-patches board-runtime counters match.
- results -> layering visible SLO passes without manual refresh.
- PG manual scripted logs have no SQLite lock errors or disk-SQLite fallback.

Validation:
Use PYTHONPATH=src ./.venv-tests/bin/pytest targeted tests first, then npm --prefix frontend-demo run build, then PG-backed scripted service/browser parity. Do not trigger live Harvest/Apify calls unless explicitly asked. Update PROGRESS.md, docs/NEXT_TODO.md, and contract docs with the final behavior and residual risks.
```
