# Claude Code Streaming Workflow Rebuild Context

> Status: Archived 2026-06-11. Historical record only — do not treat as active guidance; see `docs/INDEX.md` for current docs. (Previous status: Current handoff context. Use this when starting a Claude Code session to redesign the provider-backed streaming workflow to service-grade quality. Read with `EV)

## Purpose

This handoff exists because the current streaming workflow has been improved through many local fixes, but the product still fails under real hosted/live pressure in ways that should have been impossible under a cleaner architecture.

The next implementation pass should not add another frontend label patch, one-off result-view repair, or case-specific scripted fixture. The goal is a service-grade workflow state machine with one canonical lifecycle source of truth, atomic progress snapshots, event-level provider handoff, independently paced materialization, and browser tests that fail before a user finds the issue.

## Product Expectations

For a large-org scoped search such as `帮我找OpenAI做Infra方向的人` or `帮我找OpenAI做Agent方向的人`, the user-facing contract is:

- A reusable baseline board should become visible quickly when a valid baseline exists.
- Stage 1 counters must be atomic and explainable: new current rows, new former rows, deduped rows, profile URLs required, profiles fetched, applied-to-snapshot, and materialized-to-board must not come from mixed partial sources.
- Provider actor slots should stay filled while there is known eligible work and budget remains.
- A provider completion event should trigger local apply and next-submit opportunity before heavy materialization or retrieval-index rebuild.
- Candidate-board sync must separate served board rows, profile-fetch progress, and rows materialized/visible on the board.
- `results` must not mean "the job object is terminal but the board/result view is stale or not serving." If post-result materialization is still active, that phase must be explicit and the candidate page must still be usable.
- `/progress`, `/dashboard`, and `/candidates` must read the same lifecycle contract and remain fast. Public reads must not synchronously rebuild large artifacts.
- Webhook, watcher, recovery, and daemon paths must be idempotent and auditable. Watcher fallback success is not proof that hosted external webhook roundtrip is working.

## Anchor Failure

Use the hosted OpenAI Infra job as the current anchor failure:

- History: `ae907436-5996-4e04-a109-c9943cdfa399`
- Job: `c5248ea4b3b4`
- Query: `帮我找OpenAI做Infra方向的人`
- Baseline snapshot: `20260430T130836`
- Current snapshot: `20260430T155907`

Confirmed behavior:

- The plan correctly chose `delta_from_snapshot` because Infra current/former shard coverage was not fully reusable.
- Final snapshot files showed `current/entries=83`, `former/entries=10`, and deduped `89`.
- The execution UI showed mixed states such as `新取回在职候选人0`, `新取回离职候选人10`, and `需补取 LinkedIn Profile77`.
- The profile denominator had advanced from worker/prefetch URL state while lane counts were still read from a partial search-seed snapshot.
- Profile-scraper slot usage did not fill the intended 4-slot budget. Observed batches were `8`, then `33/34`, then a small cached `3`; active provider occupancy reached about `2`.
- Local watcher often advanced before provider webhook. Some local provider completion observations lagged remote completion by minutes.
- Callback-side profile prefetch/cache-marker confirmation blocked for about `295-302s` with `dispatched_url_count=0`.
- The job reached `completed/completed`, but `job_result_views` still served only baseline snapshot `20260430T130836`.
- Lifecycle expected `1200`, delta required/fetched `89/89`, but materialized-to-board remained `19/89`.
- `/dashboard` and `/candidates` could time out for 20-30s during the baseline-serving post-fetch state.

This failure class is not a single bug. It is evidence that progress, lifecycle, scheduler, materialization, and public serving are still loosely coupled through multiple derived records.

## Recent OpenAI Case Summary

The latest OpenAI incidents should be treated as architecture failures, not bad luck in live testing.

## 2026-05-06 PG Scripted Board Runtime / Local Apply Dependency Audit

Focused implementation handoff: `CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md`. Use that document as the first Claude Code entrypoint for this incident, then return here for the broader architecture context.

Use this section as the concrete repair packet for the PG-backed manual scripted failures observed on 2026-05-06. These are not frontend-only copy issues. They are cross-endpoint lifecycle, board-runtime, local-apply, and scheduler orchestration failures that surfaced only after the manual scripted environment moved from disk SQLite to a dedicated Postgres schema.

### Runtime Under Review

- Runtime: `runtime/test_env/openai_agent_delta_streaming`
- PG schema: `sourcing_scripted_openai_agent_delta`
- OpenAI Agent job: `55738bb72427`
  - Snapshot: `20260506T073020`
  - Baseline: `300`
  - Delta: `297`
  - Expected final served population: `597`
- Lovable live-roster job: `ae5e2a1487a2`
  - Snapshot: `20260506T074044`
  - Expected final served population: `140`

### Main Finding: Local Apply Dependency Ordering Is a Streaming-Orchestration Bug

The phrase `local apply 依赖顺序错误` should be interpreted as a workflow orchestration/state-machine defect, not as a provider throughput problem.

The provider/profile workers did start and complete in waves, and provider terminal events arrived. The failure was that the downstream local apply closure and board-visible serving projection were not unlocked as event-level dependencies became ready. Several `local_apply_closure` items attempted to apply before `candidate_documents` existed and failed with `candidate_documents_missing`. Later, daemon/recovery or post-completion reconcile grouped the work and eventually succeeded. This explains the user-facing pattern:

- `LinkedIn Profile 已取回` keeps increasing.
- `卡片详情已合入看板` stays at `0`.
- The board then jumps directly to a large partial/final value.

This is exactly the class of bug the event-level streaming workflow is supposed to make impossible. It means provider completion, local dataset ingest, candidate-doc prerequisite creation, board-visible patch publication, and full snapshot compaction are still coupled through loose recovery/reconcile timing instead of explicit durable prerequisites.

### Concrete Symptoms

- OpenAI candidate-board progress showed unstable denominators such as `300/351`, `300/425`, `300/525`; the business denominator should have stayed at `597`, with delta denominator `297`.
- OpenAI board progress showed `新增 LinkedIn Profile 已取回 117/297` and later `276/297`, while `卡片详情已合入看板 0/297` remained stuck until after profile fetch completion.
- OpenAI execution progress showed mixed-source Stage 1 math such as `current=0`, `former=74`, `deduped=241`, `profile_required=241`, even though final Stage 1 projection should be `223 + 74 = 297`.
- Lovable execution progress showed impossible intermediate math such as `current=120`, `former=0`, `deduped=132`.
- Lovable candidate board stayed in an empty/placeholder state despite profile fetch progress, then jumped to final `140/140`.
- OpenAI pagination could get stuck on page 1 because frontend paging resets `currentPage` from transient zero totals while the backend-filtered page request is pending.
- Layering and facet display could lag after `results`, because board runtime, dashboard cache, candidate page payloads, and local hydration state still compete as frontend inputs.

### Evidence To Verify

Use these queries against the local PG DB:

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

Expected current bad evidence:

- OpenAI row can show `served_candidate_count=597`, `delta_profile_required_count=297`, `delta_profile_fetched_count=297`, but `delta_profile_materialized_count=172` and `delta_profile_board_visible_count=172`.
- Lovable row can show `served_candidate_count=140` while delta/apply fields are `0`, because live-roster and baseline+delta still do not share one precise card-ready counter model.

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

Expected current bad evidence:

- OpenAI patch ledger can say `cumulative_candidate_count=172`, but later patch metadata says `display_ready=597` and `explicit_capture=297`. This mixes patch-time delta progress with final mutable serving projection state.
- Lovable patch ledger starts with a `full_snapshot_board_visible_patch` at `120` but `display_ready=76` and `preview=44`, then later profile-detail patches still keep `cumulative_candidate_count=120`. This mixes basic roster shell publication and complete-card readiness under one board-visible concept.

```bash
psql 'postgresql://changyuyi@127.0.0.1:5432/sourcing_agent' -P pager=off -F $'\t' -A -c "
select job_id,item_kind,status,item_id,attempt_count,left(coalesce(last_error,''),80) as last_error,
created_at,updated_at,
round(extract(epoch from (updated_at::timestamp-created_at::timestamp))::numeric,1) as item_wall_s,
metadata_json::jsonb #>> '{worker_id}' as worker_id,
metadata_json::jsonb #>> '{worker_ids}' as worker_ids
from sourcing_scripted_openai_agent_delta.job_materialization_items
where job_id in ('55738bb72427','ae5e2a1487a2')
and item_kind in ('local_apply_closure','board_visible_delta_apply')
order by job_id,created_at,item_id;"
```

Expected current bad evidence:

- OpenAI has multiple `local_apply_closure` rows failed with `candidate_documents_missing`, for example items created around `23:31:09`, `23:31:38`, `23:31:40`, `23:31:41`, `23:32:25`, `23:32:31`, `23:32:46`.
- Successful OpenAI board-visible apply rows only start around `23:37:11`, even though remote provider events started at `23:31:07`.

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

Observed worker facts from this run:

- OpenAI launched profile batches in waves. A key wave was workers `15,16,17,18`, all started at `23:32:46` with `25` URLs each and provider limiter submit active counts `1,2,3,4`; this indicates the profile actor budget can be filled in that wave.
- OpenAI later launched tail workers `23,25,26,27,28,33` with URL counts `16,21,13,10,7,9`; these are not necessarily wrong by themselves, but they show refill/tail scheduling rather than one continuous full-slot stream.
- Lovable launched workers `46,47,48,49` around `23:41:23-23:41:28` with limiter active counts `1,2,3,4`, then later tail workers `60,62,64,66` with `1,1,1,3` URLs. The tiny tail can be acceptable when remaining work is small, but it must not block card-ready publication for already completed batches.
- Postgres change-point concurrency query showed max local active profile workers above the limiter budget (`OpenAI=10`, `Lovable=8`) because local worker rows include long-running local/recovery/apply lifetime, not only remote provider slots. Use `provider_limiter.active_count` and remote run ids for provider-slot occupancy, not raw worker row overlap.

### Interpretation For Scheduler Efficiency

The current evidence does not prove that Harvest/Apify actors were globally underutilized in this PG scripted run. It shows mixed behavior:

- Provider submit waves can fill the configured `budget=4`.
- However, local worker lifetime, local apply closure, candidate-doc prerequisite creation, and board-visible publication are still too coupled.
- The inefficient product behavior is not just "actors idle"; it is "provider completion does not immediately advance card-ready serving projection or next durable state."
- Scheduler metrics should therefore separate:
  - remote provider slot occupancy
  - provider terminal event lag
  - local event apply lag
  - candidate-doc prerequisite lag
  - board-visible patch lag
  - full snapshot compaction lag

### Code Areas To Review / Change

Backend:

- `src/sourcing_agent/orchestrator.py`
  - `_run_local_apply_closure_item_queue_once`
  - `_process_local_apply_closure_worker`
  - `candidate_documents_missing` handling
  - `_publish_partial_board_visible_delta_overlay`
  - `_publish_partial_board_visible_current_snapshot_overlay`
  - `_publish_full_snapshot_board_visible_serving`
  - `update_job_result_lifecycle_from_materialization`
  - `update_job_result_lifecycle_from_board_visible_patch`
  - `update_job_result_lifecycle_from_stage1`
  - `_build_linkedin_stage1_progress_payload`
  - `_stage1_progress_payload_from_lifecycle_row`
  - `_build_board_runtime_state`
  - `get_job_progress`
  - `get_job_candidate_page`
  - `get_job_board_visible_patch_log`
- `src/sourcing_agent/worker_daemon.py`
- `src/sourcing_agent/remote_provider_events.py`
- `src/sourcing_agent/workflow_event_response.py`
- `src/sourcing_agent/workflow_service_metrics.py`
- `src/sourcing_agent/candidate_artifacts.py`
- `src/sourcing_agent/snapshot_materializer.py`

Frontend:

- `frontend-demo/src/components/ResultsBoardPanel.tsx`
  - backend page pending currently allows transient zero to collapse `totalPages` and reset `currentPage` to `1`.
  - board header text must not derive canonical counts from local `dashboard.candidates`.
- `frontend-demo/src/pages/SearchPage.tsx`
  - progress polling, patch-log polling, dashboard cache, and renderable-dashboard warmup still merge competing sources.
- `frontend-demo/src/hooks/useDashboardCandidateHydration.ts`
  - background row hydration should be a cache/window loader only, not a business state source.
- `frontend-demo/src/lib/api.ts`
  - freshness scoring can keep stale/final mutable board runtime over newer event-time partial states if the score encodes the wrong semantics.
- `frontend-demo/src/lib/candidateSyncSummary.ts`
  - must treat backend `board_runtime_state` as canonical once present.

### Desired Fix Shape

Do not patch by adding more fallback display logic.

The durable fix should:

- Make `board_runtime_state` an event-time projection from durable facts, not a read-time `max(...)` merge of lifecycle, asset population, dynamic Stage 1, and local row cache.
- Persist separate counters for:
  - `candidate_discovery_count`
  - `profile_fetch_required_count`
  - `profile_fetched_count`
  - `local_apply_completed_count`
  - `card_display_ready_count`
  - `published_row_count`
  - `expected_candidate_count`
  - `served_candidate_count`
- Treat `candidate_documents_missing` as `waiting_prerequisite` / deferred dependency, not a failed item that burns retries and later relies on recovery.
- Publish card-ready board patches when a profile-detail batch has been locally applied to serving-card projection; do not require full snapshot rebuild first.
- For live roster, separate:
  - discovered/basic roster rows
  - profile fetched
  - complete card-ready rows
  - low-richness/needs-completion rows after a real completion attempt
- Make `/progress`, `/dashboard`, `/candidates`, and `/board-patches` consume the same board-runtime projection.
- Fix frontend pagination by preserving last known backend-filtered `filteredCandidateCount/totalPages` while a page request is pending.
- Remove frontend canonical count inference from local loaded rows after backend-filtered paging is available.

### Regression Gates Needed

Add PG-backed scripted/browser parity gates before trusting another manual run:

- OpenAI Agent mid-run denominator remains stable at `597`, and delta profile denominator remains `297`.
- OpenAI Stage 1 never emits impossible math such as `current=0, former=74, deduped=241`.
- OpenAI with recall filter `Agent` selected while running shows newly card-ready Agent candidates before final `results`.
- Lovable live roster shows card-ready progress before final completion, not only placeholder -> `140/140`.
- `local_apply_closure` does not produce `candidate_documents_missing` failed retries.
- Board-visible patch ledger counters are monotonic and do not mix shell roster rows with card-ready counts.
- `/progress`, `/dashboard`, `/candidates`, and `/board-patches` return the same board-runtime counters for the same watermark.
- Pagination page 2 remains page 2 while backend page request is pending.
- `results -> layering visible` SLO is measured with browser polling and cannot pass by manual refresh only.

### Case A: Health / Whisper-Style Lifecycle Drift

Representative failures:

- OpenAI Health produced progress/result-view drift: stale baseline or old ChatGPT stage summaries could leak into the current job while a newer snapshot was materializing.
- OpenAI Whisper exposed a delta-only serving failure: the provider search shape was acceptable, including a zero-result lane, but the final result view could still fall back to a raw two-person delta snapshot instead of baseline + delta.
- Newer authoritative OpenAI serving snapshots narrowed selected source shard coverage and made previously reusable Agent/ChatGPT/Health/Whisper shards disappear from planning.

What this proved:

- `job_result_view`, `candidate_source`, stage-summary files, selected source snapshots, and shard registry coverage were not governed by one atomic lifecycle.
- A job could become `results` while the board was still relying on repair fallbacks or stale pointers.
- "Latest/current snapshot" was overloaded: sometimes it meant serving pointer, sometimes scoped delta, sometimes source-shard proof, and sometimes materialization target.

Required architectural answer:

- Persist a canonical `job_result_lifecycle` and make every public API read it.
- Treat source shard coverage as explicit reusable provenance, not as a side effect of candidate count or latest snapshot.
- Make zero-result lane and one-sided delta normal cases in baseline+delta workflows; they must still complete through the same serving projection.

### Case B: Infra Scheduler / Progress / Board-Serving Failure

Representative failure:

- OpenAI Infra job `c5248ea4b3b4` planned correctly as baseline+delta, but progress showed mixed-source counters (`current=0`, `former=10`, `profile_required=77`) while final files showed `current=83`, `former=10`, `deduped=89`.
- Profile-scraper batches were inefficient under a 4-slot budget: observed batches included `8`, `33/34`, and a tiny cached `3`, while active provider occupancy only reached about `2`.
- Local watcher often beat provider webhook, but local apply and next-submit still stalled behind cache-marker/profile-prefetch work.
- The job reached terminal status while the board still served the baseline and `delta_profile_materialized_count` lagged at `19/89`.
- Public APIs could time out during the baseline-serving post-fetch state.

What this proved:

- The scheduler still had group/batch-shaped barriers. Known ready work did not reliably fill provider slots.
- Progress was not an atomic projection; it mixed lane snapshots, worker URL queues, registry fetched state, and lifecycle materialization state.
- Board visibility was still too tied to full snapshot materialization/repoint instead of row-level patch/projection serving.
- Test reports were not strict enough: they could pass final state while missing tiny batches, slot underuse, mixed counters, and stale terminal board state.

Required architectural answer:

- Replace group-drain scheduling with durable item queues and adaptive provider envelopes.
- Auto-coalesce tiny live batches into larger envelopes unless queue quiescence or retry/low-volume policy proves otherwise.
- Split fetched, locally applied, board-visible, and compacted-snapshot states.
- Make public APIs serve continuous projections and forbid synchronous full rebuilds.

## Earlier Failure Classes

Claude Code should review these as the historical regression set, not as isolated incidents:

- ChatGPT live delta showed unstable progress, candidate sync regressions, result-view repoint drift, and profile-scraper slot underuse.
- Health hosted query showed stale stage-summary/result-view drift: an old baseline-serving result view caused current-job progress to read old snapshot stage files.
- Whisper hosted query showed a raw delta-only result view instead of baseline+delta serving when one lane returned zero results.
- Meta full-reuse hosted query showed progress payloads so large that execution timeline rendering failed or appeared empty.
- Excel intake exposed that synchronous artifact rebuilds and job-scoped result views were not consistently separated.
- OpenAI/Meta/Google ECS migration exposed that serving snapshot pointers, selected source snapshots, and shard registry coverage must be explicit, not inferred from `latest_snapshot.json` or candidate count.

## User Requirements To Preserve

The rebuild should preserve these product and engineering requirements:

- Optimize for service-level UX, not just final correctness. The user should see explainable progress and useful board rows while work is still running.
- Prefer architecture that generalizes across OpenAI, Meta, Google, Lovable, Excel intake, and future providers. Avoid company/query-specific repair paths.
- Minimize remote-run communication overhead. Batches should amortize webhook/watcher/recovery/dataset/apply overhead unless latency constraints justify an exception.
- Keep workflow state auditable. Every state transition should explain why it happened, what source it read, and what lifecycle/projection it updated.
- Make scripted/browser tests simulate real provider content, timing, edge cases, UI polling, public API latency, and scheduler efficiency.
- Keep live provider calls as final smoke gates, not the place where architecture-level regressions are discovered.
- When a bug recurs, promote the missing abstraction into a durable contract instead of adding another fallback.

## Planning Critique And Updated Bar

Previous planning was not strict enough because it accepted bounded local fixes before forcing the system to answer broader questions:

- What is the single source of truth for lifecycle?
- What is the durable unit of scheduling, retry, dedupe, and materialization?
- Can the board serve safe partial state without waiting for full snapshot compaction?
- Can a scripted browser test fail on the exact intermediate UX failure the user would see?
- Does the design improve performance under real communication overhead, or only under local fast mocks?

Future planning must meet this bar before implementation:

- Define canonical state first, then patch readers/writers.
- Define item-level state machines before tuning provider batch sizes.
- Define public-serving projection before adding more result-view repair.
- Define negative browser assertions before trusting scripted green runs.
- Record why a tiny batch, stale terminal state, API timeout, or slot underuse is impossible by construction; if it is only detectable after the fact, the design is incomplete.

## Required Architecture

### 1. Canonical `job_result_lifecycle`

Create or converge on one persisted lifecycle record keyed by `job_id` and preferably also by `view_id`.

Minimum owned fields:

- `job_id`
- `view_id`
- `company_key`
- `workflow_kind`
- `baseline_snapshot_id`
- `current_snapshot_id`
- `served_snapshot_id`
- `served_generation_id` or result generation key
- `phase`: for example `planning`, `baseline_serving`, `acquiring`, `profiles_fetching`, `delta_applying`, `current_materializing`, `current_serving`, `post_result_layering`, `failed`
- `phase_status`
- `baseline_candidate_count`
- `expected_candidate_count`
- `served_candidate_count`
- `delta_profile_required_count`
- `delta_profile_fetched_count`
- `delta_profile_applied_count`
- `delta_profile_materialized_count`
- `delta_profile_board_visible_count`
- `stage1_current_search_returned_count`
- `stage1_former_search_returned_count`
- `stage1_all_search_returned_count`
- `stage1_deduped_candidate_count`
- `stage1_deduped_profile_url_count`
- `stage1_profile_fetch_required_count`
- `stage1_profile_fetched_count`
- `timestamps`: baseline published, provider first submitted, provider last completed, local apply last completed, materialization started/completed, current view published
- `last_event_id` or equivalent projection cursor
- `source_snapshot_ids` and `source_validation_status`

Rules:

- Workflow completion, baseline-serving publication, delta apply, current materialization, result-view repoint, progress API, dashboard API, and candidate page API must write/read this record through one backend path.
- Stage summary files, `job.summary`, `job_result_views.metadata`, `asset_population.candidate_source`, and hot-cache manifests may be repair inputs only after snapshot/source validation. They must not be independent public sources of truth.
- A job cannot become stable `results` if the lifecycle still points to an unusable result view. Either publish a valid baseline/current/overlay view or expose an explicit post-result materialization phase while the board remains usable.

### 2. Append-Only Workflow Events With Projection

Provider completion, local apply, next-submit, materialization, result-view publication, and layering should be append-only events projected into `job_result_lifecycle`.

The event log should support:

- idempotency by provider run id, dataset id, worker id, URL set, and materialization signature
- late webhook/watch events recorded as audit without re-triggering recovery
- recovery from any stage without replaying unrelated work
- service metrics based on structured timestamps rather than natural-language timeline text

### 3. Atomic Stage 1 Progress

Stage 1 progress must be one coherent snapshot.

Do not present:

- lane counts from search-seed entries
- profile denominator from worker queued URL state
- fetched counts from profile registry
- board materialization counts from result-view lifecycle

as if they were all the same business snapshot unless they were projected atomically.

Valid options:

- Advance `profile_fetch_required_count` only from the same lane/search snapshot used for current/former/deduped counts.
- Or expose worker-derived URL state separately as technical queue metrics such as `queued_profile_url_count` and `known_profile_urls_from_workers`.

### 4. Scheduler And Provider Handoff

The provider completion path must be split into these stages:

- `remote_wait`: worker has provider checkpoint such as run id and dataset id.
- `completion_discovery`: webhook, watcher, or recovery sees the remote terminal state.
- `local_event_apply`: dataset is ingested, profile registry/progress markers are updated, and next-submit opportunity is triggered.
- `downstream_materialize`: candidate docs, retrieval index, result views, facets, and layering catch up under writer budget.

Non-negotiable:

- Next-submit opportunity must not wait on full materialization, facet projection, raw payload scans, or cache-marker confirmation that can take minutes.
- Actor slot occupancy must be measured continuously, not only at final summary.
- Scheduler metrics must distinguish provider runtime, remote-to-local event lag, local apply duration, local apply-to-next-submit lag, provider limiter wait, cache marker wait, writer-lock wait, and materialization time.

### 5. Queue-First Scheduling, Batch-As-Envelope

The ideal workflow shape is not "create a group of workers, wait for the group to end, then retry/dedupe/materialize." The durable unit should be an item, not a batch.

Core model:

- `discovery_item`: one provider search/roster row or one candidate seed, keyed by company, request scope, lane, normalized LinkedIn URL, and source query.
- `profile_url_item`: one canonical LinkedIn URL needing profile detail, keyed by normalized URL and request/job scope.
- `apply_item`: one fetched profile/candidate delta that can be applied to the job snapshot and board overlay.
- `materialization_item`: one bounded set of changed candidate ids or one generation checkpoint, not an entire company snapshot by default.

Batches should be remote-provider envelopes built by an adaptive packer:

- A batch exists to satisfy provider economics, provider limits, and latency targets. It is not the semantic unit of retry, dedupe, progress, or materialization.
- The scheduler should continuously refill provider slots from a durable ready queue while budget remains.
- The packer should use a cost-aware coalescing model: provider run setup, webhook delivery, watcher polling, recovery wakeup, dataset download, local apply, and bookkeeping all have fixed overhead per remote run. Batch size should be large enough to amortize that overhead while still keeping first-result latency bounded.
- The packer should use explicit flush conditions: target batch size, minimum non-tail batch size, max wait age of oldest ready item, provider slot availability, expected upstream completion, queue quiescence, retry-isolation need, and final-tail status.
- Very small batches such as `2` or `3` profiles should normally be merged into other ready or near-ready work. They should not be submitted just because a lane/group boundary ended. They are allowed only after a bounded coalescing window proves the queue is truly quiescent, or when an explicit retry-isolation/low-volume-company policy records why merging would be worse.
- An urgent user-visible item should still prefer joining an already-open envelope; a tiny urgent batch is an exception that must record the user-latency tradeoff and must not starve the provider slot budget.
- Current/former lanes can remain separately observable, but their ready profile URLs should feed a shared deduped queue when safe so one lane cannot starve actor slots while the other lane is visible.

Per-item state machine:

- `discovered`
- `deduped`
- `profile_required`
- `ready_for_submit`
- `leased_for_submit`
- `remote_wait`
- `remote_completed`
- `locally_applied`
- `board_visible`
- `fully_materialized`
- `retryable_failed`
- `terminal_failed`

Retry and dedupe rules:

- Retry must be per URL, query shard, or provider checkpoint, not per worker group.
- Provider partial success should mark successful items fetched and immediately requeue only unresolved/retryable items with backoff.
- Duplicate URLs discovered by current/former/search/roster lanes should converge before provider submit; late duplicates should become source/evidence updates, not new profile fetches.
- A failed batch envelope must not poison successful items inside it.

Materialization rules:

- Local event apply should update registry/progress and create board-visible patches for changed candidates as soon as possible.
- Full artifact/retrieval/index materialization should run behind a writer budget and coalesce by generation or changed candidate ids.
- "Same-kind worker drain" may be a full-materialization optimization, but it cannot block next-submit or board-visible incremental apply.
- Stable `results` requires a valid serving lifecycle, not the end of a worker group.

Observability rules:

- Reports must show ready queue length, queued URL count, leased count, remote-wait count, active provider slot occupancy, tail-flush count, small-batch reason, per-item retry count, local apply lag, and board-visible lag.
- A small batch without a recorded reason is a scheduler smell.
- A provider slot idle while ready items exist is a workflow bug unless a limiter, cost policy, backoff, or upstream dependency is recorded.

### 6. Candidate Board Serving

Board serving must be independent from heavy finalization. The ideal board is not "wait until a complete new snapshot exists, then switch." It is a continuous serving projection whose durable checkpoint may later compact into a snapshot.

- Baseline+delta workflows should publish baseline-serving view immediately when baseline is valid, then layer job-scoped delta patches over it.
- Partial delta board streaming should make fetched/applied profile batches visible incrementally when safe, instead of keeping `delta_profile_materialized_count=0` until full current snapshot repoint.
- The public board should read from a serving projection: baseline generation + ordered delta patches + row-level materialization state. A full current snapshot is a compaction/checkpoint artifact, not the only valid serving state.
- Row-level or small-generation materialization should update candidate rows, filters, recall counts, and lifecycle counters as patches become visible.
- A raw delta-only snapshot must never be final board serving when a baseline exists.
- Public candidate APIs should serve paginated rows from a valid result view/projection/overlay. They must not rebuild artifacts synchronously.
- Candidate board metrics must not use frontend-loaded page length as the authoritative sync count.

## Test And Observation Requirements

Scripted/browser tests must become service-level simulations, not only "workflow eventually reaches results."

Required heavy cases:

- OpenAI ChatGPT baseline+delta with hundreds of profile-search rows, out-of-order current/former lanes, realistic profile payloads, and multiple profile-scraper batches.
- OpenAI Infra-shaped baseline+delta that reproduces lane visibility skew: current lane URLs can be queued before current lane counts are visible.
- Lovable 100+ live roster with company-employees, profile-search/former, profile-scraper batches, and realistic profile fields.
- Zero-result lane cases where current returns zero but former returns delta, and vice versa.
- Webhook-first/watcher-late and watcher-first/webhook-late duplicate events.
- Slow-tail provider batches, retryable timeout, cache-hit batches, and materialization writer contention.

Browser assertions should fail on:

- stale `results` while board is baseline-only and not marked as post-result materialization
- result-view/job-summary/candidate-source snapshot divergence
- progress counter regression
- `profile_fetch_required_count` exceeding the coherent deduped population
- materialized-to-board ahead of fetched
- `delta_profile_materialized_count` stuck beyond budget after all profiles fetched
- `/progress`, `/dashboard`, or `/candidates` public API timeout
- actor budget underuse while eligible queued work exists
- execution stage labels that imply the wrong business phase

## Anti-Patterns To Avoid

- Patching frontend wording to hide ambiguous backend state.
- Adding one more fallback reader for result-view lifecycle instead of creating a canonical writer.
- Letting completed jobs become `results` while result view repair still needs to happen.
- Treating local watcher fallback as proof that hosted provider webhook works.
- Blocking next-submit on profile raw JSON scans, cache marker confirmation, full artifact rebuild, or retrieval-index work.
- Writing scripted fixtures that are too clean, too fast, generated-only, or missing realistic profile fields.
- Passing tests because final candidate count is correct while intermediate UX and scheduler efficiency are broken.

## Claude Code Starting Prompt

```text
You are working in /Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent.

Goal:
Redesign the provider-backed streaming workflow to service-grade quality. Do not add another local patch unless it moves the system toward the canonical lifecycle/progress/scheduler architecture.

Read first:
- AGENTS.md
- PROGRESS.md latest 2026-04-30 entries
- docs/NEXT_TODO.md Highest Priority
- docs/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md
- docs/STREAMING_WORKFLOW_REBUILD_PLAN.md
- docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md
- docs/WORKFLOW_PROGRESS_CONTRACT.md
- docs/TESTING_PLAYBOOK.md

Primary objective:
Create a canonical persisted job result lifecycle and atomic progress projection that all public APIs read. Then split provider completion, next-submit, local apply, materialization, and board serving so live workflows can stream at event-level without stale result views or mixed progress counters.

Do not trigger live Harvest/Apify calls unless explicitly asked.
Do not revert unrelated dirty worktree changes.

Expected implementation style:
- Start with a concrete design note or migration sketch if schema changes are needed.
- Search all lifecycle/progress readers and writers before editing.
- Add regression tests that would have failed on the OpenAI Infra job `c5248ea4b3b4`.
- Keep docs and TODO updated in the same pass.
- Run targeted tests first and report residual risks.

Deliverables:
1. Canonical lifecycle source-of-truth design and first implementation slice.
2. Atomic Stage 1 progress projection, including a test for mixed-source current/former/profile denominator skew.
3. Scheduler metrics and tests for provider completion to next-submit latency and actor-slot occupancy.
4. Browser/scripted guardrails that catch stale result-view and partial materialization failures before live testing.
```
