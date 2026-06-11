# Session Handoff 2026-04-26 Public Web Search

> Status: Archived 2026-06-11. Historical record only — do not treat as active guidance; see `docs/INDEX.md` for current docs. (Previous status: Current handoff for continuing target-candidate Public Web Search productization. Read this with `../PROGRESS.md`, `NEXT_TODO.md`, `PUBLIC_WEB_SEARCH_PRODUCTIZA)

## First Entry: Lovable Runtime Incident / Resume Checklist

If the next session is resuming the `Lovable` live workflow/runtime problem, read this section first before touching Public Web UI work.

### What the issue actually is

- This is not a “no roster yet” problem.
- `Lovable` already has:
  - current/full roster output
  - former/search-seed output
  - many fetched LinkedIn profile raw files
  - partially materialized candidate artifacts
- The original unresolved part was:
  - provider-completed `linkedin-profile-scraper` runs were not always consumed locally in an event-like way
  - next profile batch submit could still be delayed by poll/download/apply/materialize timing
  - candidate-detail materialization had not fully caught up to the fetched/raw state
- 2026-04-27 update:
  - local completed-worker consumption now uses a shared Harvest profile completion event contract across worker callback, running pre-retrieval refresh, running inline reconcile, and completed-workflow reconcile
  - next profile prefetch submit now happens before local profile apply/materialize in those paths
  - when a next/tail profile worker is queued/deferred/active, current profile batch materialization is deferred after delta apply instead of blocking the provider tail
  - shared recovery now has a bounded `remote_event_followup` wakeup path for LinkedIn Stage 1 remote-wait workers; after one recovery tick observes matching workers, it runs job-scoped follow-up instead of waiting for the next outer daemon tick
  - `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` records the reusable event-level response pattern and the backlog for other workflows
  - Apify provider webhook support has been added for future runs: configure `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` and `SOURCING_PROVIDER_WEBHOOK_TOKEN` / `APIFY_WEBHOOK_TOKEN` before actor submit; unconfigured historical local CLI runs still depend on poll/recovery/worker callback or manual provider-event injection
  - new local profile actor runs without webhook now get a local long-poll watcher; this watcher only injects a terminal provider event and never treats the watch window as actor failure
  - if `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` is configured, local watcher is disabled by default because ECS/hosted webhook should be the primary completion-discovery path; enable `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=1` only for tunnel debugging or explicit fallback
  - background profile prefetch live chunking now uses an adaptive live window for profile-search/former lane instead of the old fixed priority cap that produced repeated `73`-sized Lovable batches
  - profile delta apply can now lightweight-upsert changed candidate IDs to the control plane while full artifact rebuild remains deferred behind writer budget
  - provider completion 后的 next-submit opportunity 已改成 registry-only cache marker 判定：已 fetched/queued URL 不再触发 raw Harvest profile payload 解析；完整解析仍属于下游 apply/materialize
  - webhook token note: the Harvest `api_token` from `providers.local.json` is still the Apify API token used to submit actor runs, attach ad-hoc webhooks, and query actor webhooks; `SOURCING_PROVIDER_WEBHOOK_TOKEN` is a separate inbound shared secret generated for this app, and `APIFY_WEBHOOK_TOKEN` is only a compatibility alias for that inbound secret; short-term trial runs default to reusing the Apify API token for inbound webhook validation and can opt out with `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=0`
  - `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md` now records the durable ECS/local tunnel setup, token contract, connectivity probe, one-profile smoke, and troubleshooting flow; `scripts/apify_webhook_roundtrip_smoke.py` is the opt-in live smoke entrypoint
  - ECS local-dev relay is configured at `https://api.111874.xyz/local-dev/providers/apify/webhook`; it works only while `scripts/ecs_webhook_reverse_tunnel.py` is running locally and should not be used as hosted production callback
  - Apify relay smoke `SeuvL6YsW48M2t1ML` / dataset `09ivTJCt5FhZHgZaP` proved ad-hoc dispatch creation and delivery on Apify (`dispatch=4hW8x91WACMHAROc2`, `status=SUCCEEDED`), but the one-profile actor completed before local webhook handling recorded the event
  - that fast-actor race is now handled: if webhook matching by `run_id` / `dataset_id` finds an already completed worker, the runtime records `remote_provider_event: received_late` and does not rerun recovery/materialization
  - event metrics are now recorded in job events: provider events include `remote_completed_at`, `local_event_seen_at`, `remote_to_local_event_lag_ms`, and Harvest completion events include post-ingest prefetch candidate/dispatch counts, registry marker counts, and elapsed ms
  - event efficiency metrics are now productized through `workflow_efficiency.py`, runtime metrics, and hosted/scripted smoke reports: remote completion to local marker lag, local marker to next-submit lag, duplicate reconcile/materialize counts, and true provider slot occupancy are visible without parsing raw provider payloads
  - provider slot observability now distinguishes a real limiter lease, a remote actor/run worker, and a phantom active worker with no lease; the last one is treated as a state-machine violation in smoke metrics
  - completed workflow background reconcile now follows the same event-level state contract: it acquires a job-level lease before heavy rebuild work, records `inline_incremental_ingest` markers on consumed roster/search-seed/profile workers, and may backfill missing markers from old `background_reconcile.worker_ids` without rerunning apply/materialize
  - Meta Audio `d964b1d42ac5` was used to repair old marker drift: worker `394` now has a `harvest_prefetch` consumption marker and worker `347` has a `search_seed` marker; snapshot `20260427T203601` remains `1727` candidates / `1727` profile details / backlog `0`

### Runtime objects to inspect first

- Job files:
  - `runtime/jobs/80efbad6aaec.json`
  - `runtime/jobs/80efbad6aaec.preview.json`
- Snapshot root:
  - `runtime/company_assets/lovable/20260426T193540/`
- First files to open under that snapshot:
  - `candidate_documents.json`
  - `candidate_documents.linkedin_stage_1.json`
  - `search_seed_discovery/summary.json`
  - `harvest_company_employees/harvest_company_employees_visible.json`
  - `harvest_company_employees/harvest_company_employees_merged.json`
  - `harvest_profiles/`
  - `normalized_artifacts/strict_roster_only/candidates/`

### Current observed counts

- `candidate_documents.json`: `1155` candidates
- `candidate_documents.linkedin_stage_1.json`: `173` candidates; this is the stage/search-seed candidate snapshot count, not the fetched profile payload count
- `harvest_company_employees_visible.json`: `1008` rows
- `harvest_company_employees_merged.json`: `1009` rows
- `harvest_profiles` raw profile-like files: `1148` profile payloads
- `harvest_profiles/*.json`: `1220` JSON files including queue/run/dataset artifacts
- `harvest_profile_batch_*.queue_summary.json`: `18`, all `completed`
- profile batch requested URL sum: `1148`; unique requested URLs: `1148`; duplicate URL count: `0`
- `normalized_artifacts/candidates/*.json`: `1155` files
- `normalized_artifacts/strict_roster_only/candidates/*.json`: `818` files currently on disk; this is a filtered strict-roster view, not the full candidate artifact count
- PG/control-plane `candidate_count_for_company('Lovable')`: `1155`
- PG/control-plane evidence rows for `Lovable`: `2328`
- Last worker observation: job `80efbad6aaec` has no recoverable workers; all known Harvest profile batch summaries are `completed`

### Tail state still worth checking

- `runtime/company_assets/lovable/20260426T193540/harvest_profiles/harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json`
- Last inspected values for that formerly stuck batch:
  - `requested_url_count=73`
  - `status=completed`
  - `run_id=LLf6slcMDtP9WlKZf`
  - `dataset_id=Q95IGLwSSd9EEjoey`
- Also inspect all `harvest_profile_batch_*.queue_summary.json`; the latest local observation showed 18 completed batch summaries and zero recoverable workers.

### What has already been proven

- The observed `58 + 73 + 73 + 73 + 73` profile batches were disjoint deferred-tail batches, not full reruns.
- PG `replace_company_data(...)` row-by-row writes were a real hotspot and have already been replaced with a bulk path.
- `harvest_profile_batch` self-owned queued URLs with checkpoint `run_id/dataset_id` now resume the remote run instead of short-circuiting.
- The local orchestration shape has been decoupled for completed-worker consumption:
  - provider completion
  - local ingest
  - next-batch submit
  - downstream materialization
  now follow next-submit-before-materialization semantics once the completed dataset is observed locally.
- Provider retry behavior now treats `Too many queued requests (code_22)` as retryable queue backpressure, and mixed-success profile batches only retry unresolved URLs.
- Provider webhook edge cases are covered at the event contract level: `SUCCEEDED` and `FAILED` terminal Apify events both wake matching remote-wait workers, but recovery/materialize still stays outside the webhook HTTP response.
- Remaining materialization optimization is downstream: changed-candidate control-plane upsert and faster Harvest background artifact build profiles are in place; remaining work is phase timing, full rebuild maintenance policy, and broader workflow registry adoption.

### First actions for the next session

1. Re-read the detailed section below: `Lovable Harvest Profile Tail Finding`.
2. Confirm whether the on-disk tail batch state above has changed.
3. If Lovable tail latency is still the active problem, focus on remote completion discovery/wakeup cadence rather than re-coupling materialization to submit.
4. Keep the local pipeline shape intact:
   - `provider completed -> local ingest`
   - `local ingest -> next batch submit`
   - `apply/materialize` remains downstream and must not become the default blocker for the next provider submit
5. If extending this beyond LinkedIn Stage 1, add a registry entry and regression under the `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` contract instead of adding workflow-specific orchestration branches.

## 2026-04-27 Candidate Board Loading Incident

- Query/job `b992f9df009c` (`帮我找Anthropic做Pre-training的人`) exposed a separate results-serving issue, not a Harvest/runtime acquisition issue.
- Root cause:
  - job summary and PG `job_result_view` were rebuilt from a retired `sqlite_store/ranked_results` contract
  - `snapshot_id` and `source_path` were empty
  - PG `job_results` had zero rows, so the candidates API returned `total_candidates=0` even though Anthropic authoritative assets existed
- Current fix:
  - results resolver can recover retired/incomplete candidate sources from authoritative `organization_asset_registry` for full asset/reuse jobs
  - recovered result views are persisted back to PG as `company_snapshot/asset_population`
  - persisted `asset_population/company_snapshot` result views now override stale summary candidate sources
  - summary/profile progress avoids full monolithic candidate loading when only `artifact_summary.json` is available
  - candidate paging can slice `materialized_candidate_documents.json` directly when paginated `manifest.json/pages` are missing
- Live repair already applied:
  - `job_result_view(b992f9df009c)` now points to Anthropic snapshot `20260416T225318`
  - dashboard and candidate page return `asset_population` with `3455` candidates
  - local backend/worker were restarted after the code change
- Maintenance audit update:
  - current authoritative organization snapshots have paginated `manifest.json/pages`; `legacy job_result_view` count is `0`
  - historical/non-authoritative rows still have missing paginated serving artifacts; dry-run found `138` affected canonical/strict views
  - use `repair-paginated-candidate-artifacts --include-history --dry-run` for audit; defer large historical Google/OpenAI/xAI/Anthropic repair until after current manual testing unless a specific historical snapshot is needed
- Follow-up is tracked in `docs/NEXT_TODO.md`: add a maintenance audit to rebuild missing paginated serving artifacts and prevent legacy runtime job import from recreating `sqlite_store/ranked_results` views for full asset reuse jobs.

## 2026-04-27 OpenAI Scoped-Query Recall Provenance Incident

- Query/job `828b63063fb3` (`我想要OpenAI做Agent方向的人`) exposed a serving projection issue after the workflow itself completed normally.
- Clarified counts:
  - Stage 1 scoped-search preview: `54` Agent candidates
  - final board: full OpenAI asset population `826` candidates
  - profile fetch progress after rebuild: `826/826`
  - profile scraper live calls fetched `38` new profiles because `16` Stage 1 profiles were reused from cache/registry
- Root cause:
  - `candidate_documents.json` had source provenance such as `metadata.seed_query=Agent`
  - normalized/page serving artifacts did not reliably expose this provenance after canonical/history merge or old shard reuse
  - candidate/evidence/profile fingerprints could remain unchanged even when the serving projection logic changed, so old shards were silently reused
- Current fix:
  - canonicalization preserves `seed_query/source_query/query/scope_keywords/seed_keywords/intent_keywords/matched_keywords`
  - candidate artifact projection writes both `matched_keywords` and explicit multi-row `source_matches` into serving records
  - frontend recall filters prefer `matchedKeywords` provenance before falling back to full-text matching
  - candidate shard fingerprints include a projection schema/version so current view caches are invalidated when serving projection changes
- Important boundary:
  - projection version is a current serving-view cache invalidation mechanism, not a global rule that a candidate can belong to only one shard
  - serving may still use one materialized shard file per canonical candidate, but that file can carry many `source_matches`; do not confuse serving file layout with acquisition/query membership semantics
  - old historical snapshots/shards can remain; do not do a broad historical rebuild before manual testing unless a specific query requires it
  - only OpenAI `runtime/company_assets/openai/20260427T141011/normalized_artifacts/` was rebuilt in this pass; the final projection version is `candidate_artifact_projection_v20260427_source_matches`
  - OpenAI page artifacts now have `source_matches_records=826`, `multi_source_match_records=147`, and `Agent=54`
  - Anthropic full-asset reuse can continue to load fast from its existing authoritative paginated snapshot; old Anthropic pages do not have source-provenance fields yet, so recall filter uses text fallback until that snapshot is intentionally rebuilt
- Performance observation:
  - true rebuild exposed `prepare_candidates≈52-54s`
  - first `state_upsert≈16s`, subsequent projection-version rebuild `state_upsert≈0.4-0.7s`
  - track this as materialization performance backlog, especially `prepare_candidates`

## 2026-04-27 ECS Pre-Deployment Drift Check

- Read-only ECS probes show the hosted endpoint is alive, but it is not yet aligned with current local code:
  - current service process still runs from `/opt/sourcing-ai-agent` with `/opt/sourcing-ai-agent/venv/bin/python3`
  - documented target layout is `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`; decide whether to migrate or update the runbook before deploy
  - PG-only env is present in the live process: `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`, `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`, `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`, `SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - live process is missing `SOURCING_RUNTIME_ENVIRONMENT=production` and Apify provider webhook env
  - `POST /api/providers/apify/webhook` currently returns `404`, so hosted webhook support requires deploying current backend code first
- Before syncing current local work to ECS:
  - keep PG as authoritative and disk-backed live SQLite retired
  - start backend/worker with `SOURCING_RUNTIME_ENVIRONMENT=production`
  - set hosted webhook callback to `https://api.111874.xyz/api/providers/apify/webhook`
  - keep `https://api.111874.xyz/local-dev/providers/apify/webhook` only for local reverse-tunnel smoke
  - run `show-control-plane-runtime`, `/health`, `/api/runtime/health`, `/api/providers/health`, and one-profile Apify webhook smoke after deploy

## 2026-04-27 Wispr Flow Lane Hydration Incident

- Query/job `87fa38d196a8` (`帮我找Wispr Flow的全部成员`) exposed a parallel-lane handoff bug:
  - Harvest/API data was present
  - current roster artifacts were already written to the snapshot
  - search-seed/former state in memory still contained only the former/search-seed lane
  - `enrich_linkedin_profiles` used the stale in-memory state and overwrote `candidate_documents.json` back to the smaller lane-only population
- This has been fixed as a generalized acquisition contract:
  - before enrichment, hydrate both roster and search-seed snapshots from the current `snapshot_dir`
  - merge hydrated artifacts with any in-memory state
  - canonicalize once and only then enter profile enrichment
  - search-seed hydration now merges root aggregate entries with lane subdirectory entries even when the root aggregate already exists; this covers scoped-search workers completing out of order, such as one worker contributing `Agent` results and another later lane artifact contributing `Multimodal` results
- 2026-04-27 follow-up: scoped-search/search-seed worker completion is now event-level too:
  - new search workers carry `recovery_kind=search_seed_discovery`
  - `linkedin_stage_1` remote-event registry covers `search_planner` / `public_media_specialist` workers in `waiting_remote_search`
  - a single completed search shard is applied to durable `search_seed_discovery` and `candidate_documents` artifacts, then profile prefetch is queued immediately
  - if sibling search shards are still queued/running, full candidate artifact / PG materialization is deferred under the same-kind writer policy instead of blocking profile prefetch
- The intended architecture boundary is explicit:
  - durable lane artifacts are the handoff source for enrichment input recovery
  - enrichment/provider submit remains separate from downstream artifact materialization
  - candidate artifact rebuild / PG refresh / frontend serving assets remain downstream async stages and must not become a default barrier before profile submit
- Real Wispr repair status:
  - snapshot root: `runtime/company_assets/wisprflow/20260427T115446/`
  - `candidate_documents.json`: `185` candidates
  - status count: `84 current`, `101 former`
  - profile progress: `185/185`
  - job candidate page direct read returns `result_mode=asset_population`, `total_candidates=185`
- Frontend/API serving follow-up:
  - asset-population cache token now includes artifact summary/manifest file fingerprints
  - candidate board hydration now does bounded refresh polling for asset-population snapshots
  - employment filter no longer exposes the internal `lead` bucket as a user-facing `线索` option
  - asset-population lightweight candidate pages preserve `linkedin_url` even when profile detail is still incomplete, so the frontend can show an “打开 LinkedIn” action during the roster/search-seed preview phase
  - existing backend processes must be restarted after this change; an old process can still hold stale profile progress cache even after the disk artifacts are correct
- Backlog remains in `docs/NEXT_TODO.md`:
  - add browser/E2E coverage for asset-population growth without manual refresh
  - expose hydration restore summary in workflow progress/events
  - keep broader Public Web workflow event-level response work separate from this LinkedIn Stage 1 fix

## 2026-04-27 Windsurf Candidate Board / Profile Prefetch Incident

- Query/job `e69cbd03045f` (`帮我找Windsurf的全部成员`) exposed two additional LinkedIn Stage 1 follow-ups:
  - serving artifact pages could lose top-level `linkedin_url` even when candidate documents and shard `materialized_candidate` had the URL
  - profile prefetch submit budget could shrink to adaptive `recommended_max_workers=1/2`, leaving the real actor global budget underused during tail drain
- Serving artifact fix:
  - normalized candidate records now retain `linkedin_url`
  - page/shard writers and repair-from-materialized hydrate serving identity fields from materialized candidate, normalized record, reusable document, and profile completion backlog
  - old shards missing serving identity are treated as dirty via `serving_identity_fields_changed`
  - real Windsurf snapshot `runtime/company_assets/windsurf/20260427T131256/` was rebuilt; page artifacts and API now return `423/423` profile progress and top-level LinkedIn URLs
- Scheduling fix:
  - adaptive live window still determines batch size and recommended concurrency
  - default submit budget now falls back to `harvest_profile_actor_global_inflight` instead of the adaptive recommended worker count, so active tail workers do not prevent new safe-budget workers from being submitted
  - explicit `harvest_profile_batch_submit_global_inflight` remains the hard override if provider backpressure appears
- Frontend refresh fix:
  - candidate board polling treats `fetched + unrecoverable < total` as pending profile work even when queued/retry counters are zero or stale
  - the LinkedIn sync label now includes a “追平 N” count
- Follow-up is tracked in `docs/NEXT_TODO.md`: add browser/E2E coverage for automatic profile progress refresh and runtime metrics for actor budget vs submit budget vs deferred tail URLs.

## 2026-04-27 Meta Stale Serving Pointer / Hot-Cache Fallback Incident

- Query/job `ae500773be42` (`帮我找Meta做Agent方向的人`) exposed a serving-pointer drift after provider/profile acquisition had already completed.
- Confirmed data state:
  - correct snapshot: `runtime/company_assets/meta/20260427T153312/`
  - `search_seed_discovery.entry_count=513`
  - `candidate_count=515`
  - `profile_detail_count=515`
  - `profile_completion_backlog_count=0`
  - `harvest_profile_batch_*.queue_summary.json`: `15`, all `completed`
  - requested profile URL sum: `503`; the remaining candidates were served from cache/registry reuse, not interrupted provider runs
- Root cause:
  - completed-worker reconcile resolved `snapshot_dir` from worker metadata, but resolved `snapshot_id` from stale `job_result_view`
  - `_execute_retrieval(... workflow_snapshot_id=old)` then rewrote job summary/result-view to old Meta snapshot `20260423T062947` with 2 candidates
  - the first manual repair also exposed an authoritative-loader bug: a broken hot-cache manifest with a missing candidate shard could block fallback to canonical `company_assets`
- Current fix:
  - worker-driven reconcile now prefers an actually existing worker snapshot dir for `snapshot_id`; result-view/summary are fallbacks only when worker snapshot is unavailable
  - the sparse-summary path is preserved: if worker metadata points at a missing dir, result-view can still recover the snapshot
  - authoritative candidate artifact loading now treats hot-cache as a cache: broken manifest/shards fall back to canonical snapshot/candidate documents instead of returning empty results
- Live repair applied:
  - `job_result_view(ae500773be42)` now points to `20260427T153312`
  - job summary `candidate_source` also points to `20260427T153312`
  - direct candidate page returns `result_mode=asset_population`, `total_candidates=515`, `profile_fetch_progress=515/515`, and first-page LinkedIn URLs
- Follow-up layering repair:
  - The same stale pointer had caused outreach layering to run on old `20260423T062947` with only 2 candidates.
  - Correct Meta snapshot `20260427T153312` now has canonical layering at `runtime/company_assets/meta/20260427T153312/layered_segmentation/greater_china_outreach_20260427T104417Z/layered_analysis.json`.
  - Job summary `outreach_layering` points to the canonical artifact; API full pagination after backend restart returns `Layer 0=353`, `Layer 1=33`, `Layer 2=17`, `Layer 3=112`.
  - Backend contract now repairs missing/stale outreach layering against current `candidate_source.snapshot_id`, and layering writes canonical assets by default.
  - Frontend contract now treats missing `outreach_layer` as null / “分层未生成”, not real Layer 0.
- Follow-up is tracked in `docs/NEXT_TODO.md`: hot-cache missing-shard audit, snapshot-resolution source metrics, and a maintenance command for job result-view / summary / authoritative-registry consistency.
- Regression guard added: `test_scripted_scoped_search_baseline_reuse_materializes_layers_and_serves_current_snapshot` now exercises baseline reuse plus a new scoped-search shard end-to-end through local profile cache reuse, materialized artifacts, canonical outreach layering, and candidate-board serving on the same current snapshot.

## Real Goal

Productize the old optional `Public Web Stage 2` as a target-candidate page batch action:

- The default sourcing workflow stays single-stage and does not run Public Web Search.
- Users add people to `target_candidates`, then explicitly select target candidates and trigger `Public Web Search`.
- The system writes durable, reusable, auditable person-level public-web assets keyed by normalized LinkedIn URL when available.
- Raw HTML/PDF/search payloads are internal analysis inputs and excluded from default exports.
- Public-web emails can be high-confidence candidates, but `target_candidates.primary_email` must not change until a manual promotion record has been written first.

## What Has Been Built

### 1. Storage cleanup prerequisite

- Product-facing SQLite snapshot import/export/restore paths were retired before adding Public Web persistence.
- New Public Web tables use the PG-authoritative control-plane schema/live registration path.
- Remaining SQLite behavior must stay migration-only or ephemeral shared-memory shadow only.
- LinkedIn URL normalization is storage-neutral in `src/sourcing_agent/linkedin_url_normalization.py`; new modules should not call identity helpers through a store facade.

### 2. Method calibration / experiment harness

`src/sourcing_agent/public_web_search.py` contains the candidate-level method work:

- source-family query planning
- DataForSEO batch/queue search
- entry-link classification/ranking
- source-aware fetch slicing
- deterministic email extraction and suppression
- candidate-level AI adjudication payloads
- sanitizer that blocks model-invented emails and unknown URLs
- academic summary extraction from Scholar/publication/homepage slices
- artifact-only CLI: `run-target-candidate-public-web-experiment`
- configurable AI evidence budget: `--max-ai-evidence-documents` / `PublicWebExperimentOptions.max_ai_evidence_documents`
- candidate-level LLM adjudication sees source-balanced `entry_links` plus explicit `search_evidence` from DataForSEO URL/title/snippet/query/rank/provider context, not only fetched document evidence

Important empirical runs:

- `live-dataforseo-batch-entry-discovery-10x4-ai-skip`
  - 10 candidates, 40 queries, DataForSEO queue path, about 88 seconds.
  - Confirmed batch/queue is much faster than synchronous search.
- `live-dataforseo-entry-discovery-10x10-social-links`
  - 10 candidates, 100 queries.
  - High recall for GitHub/Scholar/homepage; X/Substack links remain noisy and need identity adjudication.
- `replay-fetch-homepage-email-4-candidates`
  - Extracted `cbfinn@cs.stanford.edu` and `dainves1@gmail.com` from homepage/CV evidence as promotion-recommended email candidates.
- `live-dataforseo-fetch-scholar-academic-summary-3x7`
  - Confirmed Google Scholar HTML can include publication rows.
  - Parser was fixed to be attribute-order independent for `gsc_a_at` anchors.
  - Qwen produced useful academic summaries after source-aware slice rebuild.

Known calibration cases:

- Noah Yonack GitHub profile can expose `noah.yonack@gmail.com` in a rendered/authenticated view, but current unauthenticated fetch did not capture it. Track this for GitHub API/browser-backed profile extraction or better rendered fetch.
- Company-page queries are low-ROI for candidate email discovery and should stay out of target-candidate default fetch.
- Direct resume/CV and generic email/contact search are noisy; homepage/Scholar/GitHub/publication fetch should produce the email evidence first.

### 2.5 Quality evaluation gate

A quality layer now exists and should remain the gate before expanding export/promote semantics beyond the current promoted-only default:

- Module: `src/sourcing_agent/public_web_quality.py`
- CLI: `evaluate-public-web-quality`
- Inputs: experiment directories or direct `signals.json` paths
- Outputs: JSON report, signal CSV, Markdown summary
- Checks:
  - email source URL/family/evidence/publishability/promotion status/trusted identity
  - X/Substack/GitHub/Google Scholar trusted vs needs-review counts
  - GitHub repo/deep links, X utility/post links, Substack non-profile pages, non-profile Scholar URLs, and search-only/unreviewed identity
- Useful commands:

```bash
PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality \
  --experiment-dir runtime/public_web/experiments/replay-fetch-homepage-email-4-candidates \
  --output-dir runtime/public_web/quality/replay-fetch-homepage-email-4-candidates \
  --summary-only

PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality \
  --experiment-dir runtime/public_web/experiments/live-dataforseo-entry-discovery-10x10-social-links \
  --output-dir runtime/public_web/quality/live-dataforseo-entry-discovery-10x10-social-links \
  --summary-only
```

Observed quality conclusion:

- Replay homepage/CV sample: two promotion-recommended emails had source URLs/evidence and produced no quality issues.
- Live 10x10 social discovery sample: media/profile recall was high, but search-only X/Substack/GitHub/Scholar links were noisy and remained untrusted; detail UI must show these as review-needed until fetch + AI identity adjudication upgrades them.
- Live fetch + AI Scholar/academic sample: trusted media links did appear after evidence fetch/adjudication, but many links remained review-needed or ambiguous; the report also caught legacy non-canonical model labels such as `scholar_profile`/`github_profile`, now normalized and flagged for audit.
- Larger live quality pass `live-public-web-quality-11x14-fetch12-ai16`: 11 candidates, 154 queries, 131 fetched docs, 12 email candidates, 2 promotion-recommended emails, 94 trusted media links. It exposed a fetch-queue bug: discovered GitHub links were front-inserted and starved X/Substack/Scholar fetch diversity. GitHub discovered links are now appended; retest 12 fetches/candidate before raising to 16.
- Fetch diversity sanity `live-public-web-quality-diversity-fix-3x14-fetch12-ai16`: 3 candidates, 33 fetched docs, 7 email candidates, 3 promotion-recommended emails. Fetched docs covered homepage, resume, Scholar, GitHub, X, Substack, publication, and academic profile. Keep 12 fetches/candidate for product-cap validation; use 16 only for exploratory stress tests.
- Search evidence check `live-public-web-quality-search-evidence-3x14-fetch12-ai16`: 3 candidates, 248 signals, 8 email candidates, 3 promotion-recommended emails, 39 trusted media links under the old identity-only media count. X/Substack changed from mostly `unreviewed` to model-adjudicated labels after source-balanced DataForSEO `search_evidence`; one high-severity quality issue remained.
- URL-shape hardening follow-up: shared URL-shape warnings now mark X/Substack/GitHub/Scholar non-profile/deep links. Re-running the same quality artifact gives 9 trusted media links under the clean-profile publishable definition; dirty X/Substack deep links remain evidence/review signals.
- Runtime-efficiency follow-up: candidate analysis supports bounded concurrent URL fetches, and the experiment CLI supports bounded concurrent candidate finalization/LLM adjudication. DataForSEO queue wait remains external tail latency.

### 3. Backend productization slice

New service boundary:

- `src/sourcing_agent/target_candidate_public_web.py`

New PG-authoritative tables:

- `target_candidate_public_web_batches`
- `target_candidate_public_web_runs`
- `person_public_web_assets`
- `person_public_web_signals`
- `target_candidate_public_web_promotions`

New API:

- `POST /api/target-candidates/public-web-search`
- `GET /api/target-candidates/public-web-search`
- `GET /api/target-candidates/{record_id}/public-web-search`
- `GET/POST /api/target-candidates/{record_id}/public-web-promotions`
- `POST /api/target-candidates/public-web-export`

Backend behavior:

- POST is intentionally light: creates/join idempotent batch and per-candidate runs, queues recoverable workers, and returns.
- It does not run live DataForSEO/fetch/LLM inside the request thread.
- Per-candidate runs are the source of truth; batch rows are progress aggregates only.
- Worker lane is `exploration_specialist`.
- Worker metadata uses `recovery_kind=target_candidate_public_web_search`.
- Search checkpoint persists query manifest, provider task IDs, poll count, query results, classified links, and errors.
- Recovery takeover should poll/fetch existing provider tasks instead of resubmitting.
- Completed runs with a normalized LinkedIn URL key upsert `person_public_web_assets`.
- Signal rows are first-class, model-safe detail/export inputs.
- Promotion rows are written before any Public Web email updates `target_candidates.primary_email`.
- Public Web export defaults to promoted-only model-safe signals/evidence/promotions/manifest and excludes raw HTML/PDF/search payloads.
- Manual override is supported for non-publishable / dirty URL-shape signals only when the request includes `allow_unpublishable=true` plus a non-empty `override_reason`; hard-invalid email candidates remain blocked.
- Public Web export supports two explicit modes:
  - `promoted_only`
  - `promoted_and_publishable`

### 4. Frontend first slice

Another session added the first target-candidate page integration and this session reviewed it.

Files:

- `frontend-demo/src/components/TargetCandidatesPanel.tsx`
- `frontend-demo/src/lib/api.ts`
- `frontend-demo/src/types.ts`
- `frontend-demo/src/styles.css`
- `contracts/frontend_api_contract.ts`
- `contracts/frontend_api_contract.schema.json`
- `contracts/frontend_api_adapter.ts`

Current frontend behavior:

- Checkbox selection on target candidates.
- `Public Web Search` button submits selected `record_ids`.
- State refresh button and 5s polling while any run is non-terminal.
- Card-level status chip, counts, recommended email count, and primary links from run summary.
- Public Web detail section calls `GET /api/target-candidates/{record_id}/public-web-search`.
- Detail shows email candidates and profile/evidence links with identity labels, publishability/suppression state, source links, and URL-shape warning chips.
- Frontend consumes `GET/POST /api/target-candidates/public-web-search`.
- Browser state is selection/cache only; it is not the Public Web truth source.
- The UI promotes Public Web emails/clean profile links through `POST /api/target-candidates/{record_id}/public-web-promotions`; it does not mutate `primaryEmail` directly.
- The UI requires a reason when the operator chooses an override promotion for a non-publishable or dirty URL-shape signal.
- The right-side export panel currently shows only `批量导出 LinkedIn Profile 信息` and `批量导出 Web Search 信息`; the unfinished `人工确认` / `含高置信` mode switch is hidden.
- The Web Search export button calls `POST /api/target-candidates/public-web-export` with `mode=promoted_and_publishable`, so it exports manually promoted signals plus AI publishable signals while leaving missing high-confidence fields blank.
- Browser E2E now covers both selected-candidate search trigger/polling and completed-run detail promotion -> `promoted_and_publishable` export download.
- Public Web modules are part of default quality gates: `run_python_quality.sh all`, repo mypy targets, and changed-path regression matrix.

## Cross-Session Review Result

The other session's frontend/API work is directionally aligned with the product contract:

- It keeps Public Web Search user-triggered, not a default workflow stage.
- It uses backend API state, not localStorage/runtime files, as the source of truth.
- It keeps export separate and does not include raw HTML/PDF.
- It implements email/link promotion through the promotion persistence/API path.
- It does not claim company-level Public Web refresh is done.

No code rollback is recommended from this review.

## Remaining Gaps

These are expected gaps, not regressions:

- No company-level Public Web API/CLI lane yet.
- Current frontend polls up to 500 runs globally; this is acceptable for v1 but should become scoped/filtered as the target-candidate pool grows.
- Target-candidate card editing/navigation still needs a redesign. The old "查看历史记录" jump was hidden because it routes through the candidate detail page and returns to the wrong board; remarks/comment editing is still a later UX task.
- Cancellation/retry controls for target-candidate Public Web runs are not productized yet.

## Lovable Harvest Profile Tail Finding

The `04/26 19:35 帮我找Lovable的全部成员` live run exposed a workflow-tail bug outside Public Web:

- Job `80efbad6aaec` reached backend `completed/results` while a `harvest_profile_batch` worker was still queued with checkpoint `run_id=F8aV8VRgmDqPQY0Lj`.
- Harvest backend had already completed that 58-result profile-scraper actor, but local recovery treated the same 58 registry URLs as generic `queued` URLs and returned without polling/downloading the checkpoint run.
- The former `linkedin-profile-search` lane returned 173 seed candidates, but the Mistral backpressure default caused only one 58-URL profile actor to be submitted instead of submitting the medium search-seed chunks together.
- More importantly, this exposed an orchestration-shape problem, not just a missing resume branch:
  - provider completion, local dataset ingest, next-batch submit, and downstream candidate-detail materialization are still not fully decoupled
  - when a profile-scraper actor finishes on the provider side, the local system can still wait for the next daemon/recovery pass before it notices and consumes that result
  - in some paths, the next profile batch is effectively delayed until the current completed batch has been polled, downloaded, applied, and partially materialized, instead of using a more pipelined “provider complete -> keep feeding next batch” model
  - user-visible symptom: Harvest dashboard can already show completed actor runs while the board-side `profile_fetch_progress`, candidate detail hydration, and tail draining still look stalled

Current code fixes in this handoff:

- self-owned queued registry URLs with a checkpoint `run_id/dataset_id` now resume the remote Harvest run instead of short-circuiting
- completed `harvest_profile_batch` reconcile queues the next baseline prefetch chunk to drain deferred tails
- `harvest_profile_search` / former seed source labels now drive a more parallel medium-batch submit window, while large roster lanes stay conservative and DB-limited
- frontend progress mapping keeps a post-completion running state when `worker_summary` still has queued/running/waiting remote workers

Additional runtime fixes after the original productization handoff:

- completed workflows now have a daemon-side rediscovery path for already-finished background workers that still need reconcile/apply
- PG `replace_company_data(...)` now uses a bulk path, which removed the main reconcile-time write hotspot seen on large Lovable syncs
- background `harvest_profile_batch` recovery now uses fast-yield provider timeout/attempt defaults so one slow provider status/dataset read is less likely to monopolize an entire recovery cycle
- completed workflow reconcile now emits structured `completed_workflow_reconcile` events for lease acquire/skip, marker backfill, materialize start/completion/defer, and branch completion; event-level metrics prefer those fields and only use `detail` parsing for historical jobs
- scoped-search completed-job reconcile now defers full candidate materialization while same-kind search-seed sibling workers are still in flight; scripted E2E covers baseline completed result + out-of-order search shards + out-of-order profile batches converging to one current snapshot
- live audit confirmed the observed `58 + 73 + 73 + 73 + 73` Lovable profile batches were disjoint deferred-tail batches, not full reruns
- the last live registry observation before this doc refresh was `504 fetched / 73 queued`
- the most useful local resume paths are:
  - job files:
    - `runtime/jobs/80efbad6aaec.json`
    - `runtime/jobs/80efbad6aaec.preview.json`
  - snapshot root:
    - `runtime/company_assets/lovable/20260426T193540/`
  - snapshot files worth checking first:
    - `candidate_documents.json` (`1155` candidates)
    - `candidate_documents.linkedin_stage_1.json` (`173` candidates)
    - `search_seed_discovery/summary.json` (`entry_count=173`)
    - `harvest_company_employees/harvest_company_employees_visible.json` (`1008` rows)
    - `harvest_company_employees/harvest_company_employees_merged.json` (`1009` rows)
    - `harvest_profiles/` (`1220` json files on disk, including `1148` raw profile-like payloads and per-batch queue/run/dataset artifacts)
    - `normalized_artifacts/candidates/` (`1155` full candidate json files on disk)
    - `normalized_artifacts/strict_roster_only/candidates/` (`818` filtered strict-roster candidate json files on disk)
  - one directly relevant tail-batch artifact:
    - `runtime/company_assets/lovable/20260426T193540/harvest_profiles/harvest_profile_batch_2d3bece946f8ad9c.queue_summary.json`
    - at last inspection it recorded `requested_url_count=73`, `status=completed`, `run_id=LLf6slcMDtP9WlKZf`, `dataset_id=Q95IGLwSSd9EEjoey`

What still remains after these fixes:

- the runtime is better at resuming and draining tails, but still not fully event-driven
- the preferred target shape is:
  - provider run completed -> local completion signal observed immediately
  - local completion observed -> completed dataset ingested immediately
  - once budget/lease allows, next deferred profile batch can be submitted immediately
  - candidate-detail apply/materialize continues as a downstream asynchronous stage and should not become the default blocker for the next upstream provider submit
- medium former/search-seed runs should especially behave this way; they should not degrade into “one batch finishes, then ingest/materialize, then submit next batch”
- Lovable in particular already has meaningful on-disk state. The unresolved part is not “no list yet”; it is “list and many raw profiles already exist, but detail completion/materialization has not fully caught up”.

An ad hoc live recovery daemon was started during debugging and intentionally stopped before this handoff refresh. There should be no leftover recovery process from that pass. Restarting the worker daemon on this branch may resume/poll existing queued Harvest runs such as Lovable; that should not submit a new profile-scraper actor, but it will contact the provider to fetch completed datasets and apply them locally.

## Local Runtime State On 2026-04-26

The local frontend warning `Public Web Search 接口暂不可用，请确认后端服务已更新并重启。` is expected if the backend on port `8765` is still the old process:

- The observed `8765` process was started on `2026-04-25` before these routes existed.
- `GET /api/target-candidates` returned the local 11 target candidates.
- `GET /api/target-candidates/public-web-search?limit=5` returned `404 {"error":"not found"}` from that stale backend.
- The local control-plane state observed for those 11 target candidates had `target_candidate_public_web_runs = 0` and `target_candidate_public_web_batches = 0`; the new promotions table is created by the updated schema bootstrap after the backend restarts on current code.

2026-04-27 update: local backend/worker/frontend have since been restarted on current code. `GET /api/target-candidates/public-web-search?limit=5` now returns `status=ok`, and the backend/worker were launched with the ECS local-dev Apify webhook URL for manual testing.

So the card text `未开始 / 尚未触发候选人级 Public Web Search` is the accurate state for the current local 11 candidates. The toolbar export label that says it will export 11 candidates describes the selected/filter scope, not "11 candidates already have confirmed Web Search results". Promoted-only Web Search export will only contain manually promoted Public Web email/link signals when such promotions exist.

## Recommended Next Sequence

1. Local backend/worker/frontend are already restarted on current code for manual testing.
   - If the processes are restarted again, run `bash ./scripts/dev_backend.sh --print-config` first if DSN/runtime selection is unclear.
   - Verify `GET /api/target-candidates/public-web-search?limit=5` still returns `status=ok`.
2. Trigger a small Public Web Search batch from the target-candidate page.
   - Start with 2-3 selected candidates and confirm state moves through queued/searching/analyzing/completed or needs_review.
   - Inspect the detail section for source URLs, identity labels, URL-shape warnings, and publishability before promotion.
3. Run or review the Public Web quality gate.
   - Use existing replay/live artifacts first if live provider/model budget is unavailable.
   - For a broader live pass, use `--limit 12-15`, `--max-queries-per-candidate 14-16`, `--max-results-per-query 20`, `--max-fetches-per-candidate 12`, `--max-ai-evidence-documents 16`, and `--ai-extraction on|auto`.
4. Manually verify promotion/export quality on the fresh local run.
   - Check normal promotion and override promotion with reason.
   - Confirm the visible Web Search export button sends `promoted_and_publishable`, includes AI publishable signals, leaves missing high-confidence fields blank, and still excludes raw HTML/PDF/search payloads.
   - Keep `promoted_only` as backend/manual-QA mode until the manual promotion UI is redesigned.
5. Add cancellation/retry, scoped polling, target-candidate card UX, and company-level Public Web later.
   - Company lane stays lower priority and API/CLI-only until candidate-level ROI is stable.

## Validation Already Run

From this review session:

```bash
cd frontend-demo && npm run build
PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py src/sourcing_agent/worker_daemon.py tests/test_target_candidate_public_web.py tests/test_results_api.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web_api_queues_idempotent_runs' tests/test_target_candidate_public_web.py tests/test_markdown_status.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_results_api.py -k 'target_candidate_public_web or public_web_api'
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback' tests/test_markdown_status.py
PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/public_web_search.py src/sourcing_agent/public_web_quality.py src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_target_candidate_public_web.py -k 'public_web or target_candidate_public_web or url_shape or bounded_concurrency'
PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli evaluate-public-web-quality --experiment-dir runtime/public_web/experiments/live-public-web-quality-search-evidence-3x14-fetch12-ai16 --output-dir runtime/public_web/quality/live-public-web-quality-search-evidence-3x14-fetch12-ai16-shape-warnings --summary-only
node --check frontend-demo/scripts/run_target_public_web_promotion_export_e2e.mjs
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'
git diff --check
bash ./scripts/run_python_quality.sh all
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_regression_matrix.py
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --changed-path src/sourcing_agent/public_web_search.py --dry-run
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k 'target_candidate_public_web or public_web_api or promotion'
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_linkedin_url_normalization.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'
```

Observed results:

- frontend build passed
- ruff passed
- targeted backend/API/Public Web tests passed
- webhook/event-registry tests passed, including `received_late` handling for fast one-profile Apify actors
- ECS local-dev relay connectivity probe returned HTTP `202` after backend restart
- frontend detail build passed
- URL-shape quality re-evaluation passed; trusted media count now uses clean-profile publishability
- markdown status test passed
- browser E2E passed for both selected-candidate trigger/polling and completed-run promotion/export download
- default Python quality gate passed with Public Web modules included
- changed-path regression matrix now selects Public Web core/API/PG suites

## GitHub Handoff Notes

The current handoff target is branch `productization-2026-04-25-stable` on `origin`.

Before any future commit, inspect staged files deliberately and keep runtime/cache/vendor/build outputs excluded. Do not use `git add .` in this repository; use explicit source/doc/test/frontend paths so local runtime assets and generated output do not leak into Git.
