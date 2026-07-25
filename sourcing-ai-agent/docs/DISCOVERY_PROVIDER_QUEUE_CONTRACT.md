# Discovery / Provider Queue Contract

> Status: Active queue-first scheduler contract. Web/DataForSEO discovery item creation, worker completion-event closure, Harvest retry-wait/exhausted item drain, historical discovery-item backfill, explicit search-seed recovery ownership, completed-workflow worker-summary merge retirement, late webhook/watcher observability gates, out-of-order scripted hard gates, and completed-worker owner-gap service metrics are implemented locally.

## Historical Failure

OpenAI/Google/Meta hosted runs exposed a recurring scheduling class of bugs:

- Search-seed discovery state lives across provider workers, query summaries, recovery scans, and `provider_search_retry` terminal items.
- A provider query can be `queued`, a worker can later complete, and materialization/retry logic can still depend on scanning worker output or summary fields instead of a claimable durable unit.
- The current `provider_search_retry` item correctly records terminal zero-result exhaustion, but it is not a safe drain queue. Claiming and rerunning it directly would create another owner besides the original search-seed query and snapshot merge path.

## Product Contract

Discovery must behave like a service queue, not a batch summary:

- User-visible progress reports query-level discovery state: queued, provider-owned, returned, exhausted, retry-wait, and terminal failure.
- A normal zero-result lane is not a failure. A provider anomaly is a failure only when the provider retry policy is exhausted or the provider explicitly returns retryable backpressure.
- Search-seed discovery may stream entries into profile prefetch as each query returns, without waiting for all query groups to finish.
- Provider batches/workers are remote-run envelopes only. The durable item is the retry, dedupe, progress, and recovery unit.
- Former-member broad scans are explicit strategy, not fallback. `__past_company_only__` / empty Harvest query text is valid only when the planner/runtime cost policy sets `former_broad_past_company_only`; scoped or directional former lanes must dispatch non-empty scoped query text or mark the provider query skipped/degraded without widening the user intent.

## Canonical Owner

Current W6+ normal execution is split into a domain discovery item and a typed command owner:

- Domain/current-state row: `job_materialization_items(item_kind='search_seed_discovery_query')`.
- Executable work item: `workflow_commands(command_type='linkedin.discovery_query.run', owner='linkedin_acquisition_owner')`.
- Logical queue: `job_stage_items`.
- Terminal anomaly item kind: `provider_search_retry`.

The domain row is not an Agent-callable execution API. Normal provider work must be planned through reducer-owned `CommandPlanRequested` events into `workflow_commands`; the command owner claims and executes the query. `job_materialization_items` rows may remain as owner-private discovery state and migration/backfill evidence, but they must not become a second execution queue or a public operation surface. Future Agent/OperationRun entrypoints should target typed operations/commands, not write `search_seed_discovery_query` rows directly.

`provider_search_retry` remains a terminal incident/guardrail record for exhausted provider retries. It must not be drained independently. Future retryable provider attempts must be represented on the owning `search_seed_discovery_query` item so retry, provider execution, snapshot merge, and profile prefetch remain one state machine.

## State Machine

`search_seed_discovery_query` states:

- `queued`: query is claimable and has no remote/provider owner yet.
- `dispatch_claimed`: a scheduler tick has claimed the query and is preparing a provider call or worker envelope.
- `provider_owned`: a worker/provider run owns the query; metadata records worker id, run id, dataset id, provider payload hash, and search manifest key when available.
- `retry_wait`: provider returned retryable failure or transient empty result before policy exhaustion; `not_before_at` owns the retry timer.
- `completed`: query entries and raw artifacts are persisted and merged into the search-seed snapshot.
- `failed`: non-retryable terminal failure.
- `exhausted`: provider retry policy was exhausted; a linked `provider_search_retry` terminal item may be written for reporting.
- `superseded`: query no longer belongs to the current request/snapshot generation.

Allowed transitions:

- `queued -> dispatch_claimed -> provider_owned`
- `provider_owned -> completed`
- `provider_owned -> retry_wait -> dispatch_claimed`
- `provider_owned -> exhausted`
- `provider_owned -> failed`
- any non-terminal state -> `superseded` when the workflow snapshot generation changes

Public reads must not create, claim, complete, retry, or merge these items.

## Required Fields

Each `search_seed_discovery_query` item must include:

- `job_id`, `target_company`, `snapshot_id`, `asset_view`
- `item_kind="search_seed_discovery_query"`
- deterministic `item_id` from job id, snapshot id, employment scope, bundle id, query index, effective query text, and provider mode
- `source="search_seed_discovery"`
- `reason` as one of `initial_query`, `retryable_provider_failure`, `provider_zero_result_retry`, `manual_requeue`
- `source_worker_ids` for the current remote-run envelope
- metadata: `query`, `effective_query_text`, `bundle_id`, `source_family`, `employment_status`, `strategy_type`, `filter_hints`, `provider`, `provider_run_id`, `provider_dataset_id`, `provider_payload_hash`, `search_manifest_key`, `raw_path`, `entries_path`, `retry_policy`

## Event-Time Writers

The normal writers are:

- Planning/acquisition query compilation: enqueue deterministic `search_seed_discovery_query` rows before remote provider work starts.
- Provider submit: move item to `provider_owned` and attach the worker/provider envelope.
- Provider completion: persist raw artifact, entries, query summary, and mark item `completed` before emitting incremental profile prefetch.
- Retryable provider failure: mark item `retry_wait` with `not_before_at`; no separate retry queue.
- Retry exhaustion: mark item `exhausted`/`failed` and write linked `provider_search_retry` for operator reporting.
- Snapshot apply: merge completed query items into `SearchSeedSnapshot`; do not scan arbitrary completed workers as the normal owner.

## Service Loop

The recovery daemon must drain in this order:

1. Ready `search_seed_discovery_query` items.
2. Profile URL refill queue from `linkedin_profile_registry`.
3. Board-visible/local-apply/full-materialization items.

Reason: discovery results create profile work; profile work creates board-visible/materialization work. Draining downstream queues first can make the UI appear idle while upstream provider capacity is ready.

## Compatibility Boundary

Existing worker scans may remain only as explicit migration/backfill tools:

- They may create missing `search_seed_discovery_query` / `local_apply_closure` items for historical workers through explicit dry-run/apply CLIs.
- They must not directly merge completed discovery output in the normal recovery service. Completed workflow reconcile must return a structured migration-required/owner-required result instead of applying search-seed worker output from summaries.
- Service metrics must flag completed search-seed workers without matching durable owners as `discovery_worker_without_item_count` and `discovery_worker_without_local_apply_count`.
- Same-tick recovery must not let an item-owned discovery query and the old workflow resume path both execute provider acquisition for the same job. If a recovery tick claims a discovery item, workflow resume for that job is skipped until a later tick.
- Search-seed workers must declare `recovery_kind="search_seed_discovery"` in metadata or checkpoint. Empty legacy recovery kinds are not valid discovery owners and must be handled only by explicit migration policy, not normal recovery.

## Service Metrics

The workflow service report must expose:

- `search_seed_discovery_queue.item_count`
- `queued_count`, `dispatch_claimed_count`, `provider_owned_count`, `retry_wait_count`, `ready_retry_count`, `completed_count`, `exhausted_count`, `failed_count`, `stale_provider_owned_count`
- `completed_search_seed_worker_count`
- `discovery_worker_without_item_count`
- `discovery_worker_without_local_apply_count`
- `discovery_worker_owner_gap_count`
- `item_without_worker_owner_count`
- `retry_backlog_present`
- `exhausted_without_provider_retry_count`
- oldest ready age, oldest provider-owned age, retry wait age
- provider counts, employment-scope counts, query samples, error samples
- `remote_provider_events` source/status counts, late/in-flight duplicate counts, target worker counts, and `remote_to_local_event_lag_ms` distribution. Late duplicate events are idempotency/latency observations; they must not be interpreted as discovery/apply backlog.

Hard-gate violations:

- provider-owned item without worker/provider owner
- completed worker output without item
- completed worker output without `local_apply_closure`
- exhausted provider retry without linked `provider_search_retry`
- retryable provider failure represented only in query summary
- public read path mutating discovery items

## Test Matrix

Required scripted cases before claiming production-final discovery scheduling:

- Normal current query returns entries and immediately queues profile prefetch.
- Current lane zero results but former lane returns entries; workflow remains baseline+delta coherent and does not fail.
- Provider returns retryable timeout; item enters `retry_wait`, then service loop retries after `not_before_at`.
- Provider returns empty result repeatedly; item becomes `exhausted` and writes terminal `provider_search_retry`.
- Worker completes before webhook/recovery tick; service loop merges through item, not worker scan.
- Late webhook/watcher duplicates a completed provider envelope; item idempotency prevents duplicate merge/prefetch/recovery, and smoke can enforce `max_remote_provider_event_lag_ms` without treating duplicates as backlog.
- Mixed current/former out-of-order completion; progress remains lane-coherent.

## Implementation Order

1. Add deterministic item-id builder and projection from compiled search-seed query specs. Status: first slice landed for web discovery in `_execute_query_spec`.
2. Write query items before worker/provider submit and attach the worker owner at submit time. Status: first slice landed for DataForSEO/web-search worker envelopes.
3. Mark query items completed/retry-wait/exhausted at provider completion. Status: landed for Harvest people-search direct provider calls, including retryable exception handling and zero-result exhaustion linkage to `provider_search_retry`.
4. Add service-loop drain for ready/retry-wait query items. Status: landed for non-worker-owned Harvest people-search retry items; recovery drains the durable item before worker daemon recovery and blocks same-tick workflow resume for the same job.
5. Change completed/running reconcile to consume completed discovery items first; worker scan becomes migration/backfill only. Status: landed locally. Normal worker completion closes/creates the discovery item before `local_apply_closure` is enqueued; completed-workflow discovery no longer adds search-seed worker-summary candidates, and explicit completed reconcile skips with `search_seed_reconcile_requires_durable_local_apply_closure_item` / `search_seed_reconcile_owned_by_local_apply_closure_item` instead of merging worker outputs.
6. Add `backfill-search-seed-discovery-items --dry-run/--apply` for historical workers. Status: landed. The command creates deterministic items from legacy workers without rerunning providers or merging artifacts.
7. Add late webhook/watcher gates. Status: first slice landed for handler idempotency, smoke metrics, matrix SLO config, case-level exports, and browser `providerWebhookSummary`; webhook-first then watcher-late does not restart recovery or create durable items, and `remote_provider_events` lag is bounded by `max_remote_provider_event_lag_ms` in local smoke matrices.
8. Rename legacy direct-repair helpers. Status: `_reconcile_completed_workflow_after_search_seed(...)` has been renamed to `_process_completed_search_seed_local_apply_closure(...)` with no compatibility alias. The business-level out-of-order scoped-search/profile streaming regression now uses durable `local_apply_closure` enqueue/drain, and `allowed_worker_ids` scopes only item consumption while sibling workers remain materialization blockers. Direct test calls to `_process_inline_incremental_worker_batch(...)` have been retired from normal workflow regressions.
9. Require explicit search-seed recovery ownership. Status: landed. `_worker_is_search_seed_inline_worker(...)` rejects empty legacy `recovery_kind` values; normal discovery backfill/recovery only accepts workers marked `search_seed_discovery`.
10. Expand scripted/browser out-of-order matrix execution. Status: local before-ECS service-gate manifest is `20/20`; Agent/ChatGPT/Lovable matrices require profile-batch inversion and remote event lag SLOs.
11. Add owner-gap hard gates for completed search-seed workers. Status: `workflow_service_metrics.search_seed_discovery_queue` now cross-checks completed search-seed workers against both `search_seed_discovery_query` and `local_apply_closure` items; strict smoke fails through `require_no_service_recovery_violation=true` if either owner is missing.
