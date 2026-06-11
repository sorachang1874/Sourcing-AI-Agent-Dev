# Streaming Workflow Rebuild Plan

> Status: Archived 2026-06-11. Historical record only — do not treat as active guidance; see `docs/INDEX.md` for current docs. (Previous status: Current implementation plan. Use this with `CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md` before changing provider-backed workflow orchestration, result-vi)

## Goal

Rebuild the streaming workflow around canonical state transitions instead of derived state scattered across job summaries, result-view metadata, stage files, candidate source payloads, frontend cache length, and repair fallbacks.

The target is service-grade behavior:

- user-visible progress is coherent and monotonic
- provider completion wakes local apply and next-submit quickly
- materialization catches up asynchronously without blocking provider slots
- candidate board is always backed by a valid serving view
- public APIs are fast and do not rebuild large artifacts
- scripted/browser tests reproduce realistic live timing and fail on UX regressions

## Priority Order

### 0. Prove Asset Reuse Before Scheduling

Planner correctness is a prerequisite for the streaming rebuild. If the planner
incorrectly returns `reuse_snapshot_only`, no downstream queue, provider batch,
or board-streaming path will be created.

Implementation requirements:

- Define `requested_population_boundary` before consulting registry or
  authoritative assets. `target_scope=full_company_asset` is the serving domain,
  not proof that the user explicitly requested a full-company population.
- Treat `organization_asset_registry.authoritative` as a serving pointer only.
- Require explicit or compatible legacy full-company coverage proof before a
  snapshot can satisfy an all-members/full-company query.
- Require exact scoped shard coverage before a directional query can skip delta
  acquisition, unless the asset carries an explicit directional reuse contract.
- For directional queries with all-members wording, use full-company local assets
  first only when full-company coverage is proven; otherwise keep the request as
  scoped directional acquisition.
- Preserve selected source snapshots with reusable shard registry rows when
  promoting a newer authoritative serving snapshot.
- Expose the coverage contract in plan/explain payloads so ECS/local strategy
  drift can be diagnosed without inspecting raw registry rows.

Regression target:

- A small-company authoritative snapshot created from a scoped search must not
  plan `reuse_snapshot_only` for a later full-company query.
- The same snapshot may still plan `reuse_snapshot_only` for the exact current
  scoped shard it proves.
- OpenAI/Meta/Google migrated scoped shards remain reusable by exact coverage,
  but unrelated directional queries continue to plan baseline+delta.
- xAI-like full-company baselines may satisfy "direction + all members" by local
  full-asset filtering; OpenAI-like scoped-shard-only coverage may not.

### 1. Persist Canonical Job Result Lifecycle

Create a single backend-owned lifecycle record for each workflow/result view.

Implementation requirements:

- Add a persisted `job_result_lifecycle` equivalent in PG-first control plane.
- Make baseline publication, current snapshot creation, delta apply, materialization, result-view repoint, and final completion update this record.
- Make `/progress`, `/dashboard`, `/candidates`, and frontend history read the same lifecycle record.
- Stop public APIs from reconstructing lifecycle by independently merging `job.summary`, `job_result_views.metadata`, `candidate_source`, and stage summary files.
- Keep repair paths, but make them write back into the lifecycle record and validate snapshot ids before using stale inputs.

Regression target:

- Reproduce the OpenAI Infra shape where a job is terminal but `job_result_views` still serves the baseline and lifecycle says only `19/89` delta rows are visible. The test must fail unless the public APIs report a consistent lifecycle and usable serving view.

### 2. Make Stage 1 Progress Atomic

Split and project Stage 1 progress into coherent snapshots.

Implementation requirements:

- Define one projection path for current lane, former lane, all/unknown lane, dedupe, profile URL denominator, fetched count, applied-to-snapshot count, and materialized-to-board count.
- Do not mix worker-derived queued URLs into business-facing current/former/deduped counts unless the same projection includes the matching lane rows.
- If worker URL state is needed earlier for observability, expose it as a separate technical metric.
- Store projection timestamps/source ids so stale lane files cannot overwrite newer aggregate state.

Regression target:

- A scripted OpenAI Infra-shaped run should not display `current=0`, `former=10`, and `profile_required=77` when the final lane evidence is `current=83`, `former=10`, `deduped=89`.

### 3. Decouple Provider Completion, Next Submit, And Materialization

The scheduler must treat provider completion as a wakeup signal, not as a materialization request.

Implementation requirements:

- On remote completion, perform minimal local apply: persist raw payload marker, registry/profile marker, progress event, and worker consumption marker.
- Trigger next-submit opportunity before full candidate artifact build, retrieval-index rebuild, facet derivation, or outreach layering.
- Move cache-marker confirmation and raw profile scans out of the next-submit critical path.
- Record structured latency metrics for each segment: remote completed to local observed, local observed to apply complete, apply complete to next submit start, provider limiter wait, cache marker wait, writer-lock wait, and materialization duration.
- Keep webhook, watcher, and recovery duplicate-safe in both directions.

Regression target:

- When eligible profile URLs remain and provider budget is 4, active remote actor occupancy should reach the budget promptly unless a recorded limiter/cost policy says otherwise.
- A slow materialization writer must not prevent submitting the next eligible profile batch.

### 4. Replace Group-Drain Batching With Queue-First Scheduling

Current failures show that static worker groups and provider batches are still treated as business barriers. The target shape is a durable item queue with adaptive remote-provider envelopes.

Implementation requirements:

- Introduce or converge on durable item-level queues for discovery rows, canonical profile URLs, local apply deltas, and materialization deltas.
- Treat provider batches as envelopes selected by a batch packer, not as the unit of retry, dedupe, progress, or materialization.
- Continuously refill provider slots from ready items. Do not wait for a lane group or same-kind worker group to finish if eligible URLs are already known.
- Add an adaptive batch packer with a fixed-overhead cost model for provider run setup, webhook/watcher/recovery wakeup, dataset retrieval, local apply, and bookkeeping.
- Configure explicit target size, minimum non-tail batch size, coalescing window, max oldest-item wait, queue-quiescence proof, final-tail flush, retry-isolation flush, and small-company mode.
- Merge tiny ready sets into other ready or near-ready work by default. A `2` or `3` profile live batch should not be submitted unless the queue is proven quiescent after the coalescing window, or a recorded retry-isolation/low-volume policy explains why merging would reduce reliability or latency.
- Move retry to item/checkpoint granularity: successful URLs in a partial batch stay fetched, unresolved URLs requeue with backoff, terminal failures remain visible.
- Run dedupe continuously at ingest/queue boundaries using normalized LinkedIn URL and candidate identity. Do not defer all dedupe until a group completes.
- Allow full materialization to wait for same-kind drain when that is efficient, but never let same-kind drain block next-submit or lightweight board-visible apply.

Regression target:

- Reproduce the hosted Infra profile-scraper shape where only `8`, then `33/34`, then `3` profile batches were submitted under a 4-slot budget. The test must fail if ready items exist while provider slots are idle and no limiter/backoff/tail-flush reason is recorded.
- The same test must fail on an unexplained tiny batch and must assert that near-ready items are coalesced before final-tail submission.
- Reproduce a partial-success profile batch and assert successes become board-visible/apply-ready while only failed URLs retry.

### 5. Add Partial Delta Board Streaming

Status note as of 2026-05-02: profile URL scheduling has crossed the first
queue-first boundary. `linkedin_profile_registry` now stores refill item state,
provider-completion events and the recovery service both drain `deferred_budget`
items through the same `ProfilePrefetchBatchPlan`, and the service loop treats
refill dispatch as activity. Do not add another profile URL queue table or
artifact-derived refill list. The next boundary is board-visible apply:
profile-fetched rows need durable patch/materialization items so candidate-board
visibility is not gated on full current snapshot compaction.

Status note as of 2026-05-02 later: the first board-visible projection slice is
implemented. Completed Harvest profile batches can publish a job-scoped
baseline+delta overlay while same-kind profile workers remain in flight. The
contract is intentionally strict: `delta_profile_board_visible_count` advances
only after a real serving projection path is written, `job_result_view` points at
that projection, and canonical `job_result_lifecycle` records the projection id
and phase. Follow-up batches for the same current snapshot accumulate into the
existing partial projection instead of being blocked by the first partial repoint.
The board-visible overlay path now persists ordered idempotent
`job_board_visible_patches` and replays them before consulting metadata mirrors,
so follow-up patches survive `job_result_view.metadata` / lifecycle metadata loss.
Smoke/provider reports now expose `service_metrics.board_visible_projection`,
including projection presence, patch-log replay lag, fetched-to-board-visible
lag, and metadata replay dependency. This makes the user-visible streaming
contract measurable before we finish the broader queue-first scheduler.
The next bounded queue slice also landed: `job_materialization_items` now owns
`board_visible_delta_apply` work before a patch is published. Inline Harvest
completion enqueues an item after candidate-delta control-plane sync, then tries
to claim/process it immediately. If the overlay writer fails or the process dies,
the worker recovery service claims the retryable item on a later tick and
publishes the same board-visible projection without waiting for another provider
event. `job_board_visible_patches` remains the successful publication log; it is
not the pending/retry queue.
The follow-up bounded queue slice moved scheduled full snapshot compaction into
the same item queue: `job_materialization_items(item_kind='snapshot_full_materialization')`
owns full normalized artifacts/retrieval/index work before those artifacts exist.
Workflow completion and background reconcile scheduling enqueue the item; the
worker recovery service claims/retries it without relying on an in-process
background thread or a later provider webhook. `background_snapshot_materialization`
is now a compatibility/display mirror for this path, not a normal scheduling source.
Normal service recovery and completed-workflow discovery must not create
snapshot-full work from this summary mirror; they drain only existing durable
items. This keeps event-time enqueue/backfill as the explicit ownership boundary.
Historical jobs use the same contract through `backfill-snapshot-full-materialization-items`:
run it without `--apply` to inspect eligible scheduled/deferred jobs, then run
with `--apply` to enqueue deterministic snapshot-full items before migration or
service restart.
The smoke/operator observation path now reads the same durable queue directly:
`GET /api/jobs/{job_id}/materialization-items` exposes item rows and
`service_metrics.snapshot_full_materialization_queue` reports backlog,
retryable, ready-retry, stale-running, terminal-failed, status/phase counts, and
error samples. This keeps full-compaction health visible even when
`background_snapshot_materialization` summary mirrors are stale or missing.
The smoke/service observation path also exposes local apply closure:
`service_metrics.local_apply_backlog` reports workers that have written
`output.inline_incremental_apply` but not the matching
`output.inline_incremental_ingest` gate. It surfaces applied-not-ingested count,
stale count, age summary, worker/recovery kind breakdowns, samples, and
bottleneck entries. This is an observability contract over the existing worker
marker semantics, not a new pending-work queue. The service loop now has a
callback-only drain for that exact backlog: it claims completed apply-only
workers and invokes the existing completion callback to resume local Phase B/C
closure from stored output and snapshot state. It must not re-execute provider
actors, must not re-run Phase A apply when the apply marker already matches the
snapshot, and must not write successful drain activity as `last_error`.
The next durable local-apply bridge is also in place:
`job_materialization_items(item_kind='local_apply_closure')` owns pending local
closure after the apply marker is written. Apply marker writes enqueue a
deterministic item; ingest marker writes complete it; the service loop backfills
items from legacy apply-only markers and drains the item queue. This narrows the
scan-based fallback while preserving the callback-only no-provider-rerun
invariant.
This is still not the final queue-first workflow: discovery rows, provider retry
attempts, and cross-event adaptive batching still need the same durable
item-queue treatment.
The first provider-retry owner is now in place for terminal Harvest people-search
zero-result exhaustion: `provider_retry_items` are emitted by search-seed
discovery and projected into
`job_materialization_items(item_kind='provider_search_retry')` by acquisition.
`service_metrics.provider_search_retry_queue` and smoke summary rollups consume
that item queue directly for retry backlog, terminal failure, and stale-running
guardrails.
Do not add new summary-field parsers for provider retry state. The remaining
work is to make pending discovery rows and future delayed provider retries
claimable queue items with service metrics and adaptive drain semantics.

Baseline-first board serving is already the first step. The next step is making fetched/applied delta batches visible before full current snapshot repoint.

Implementation requirements:

- Keep the baseline board stable while delta profiles are fetched.
- Add a partial delta overlay or lightweight board-visible patch channel.
- Treat the board as a continuously updated serving projection: baseline generation plus ordered durable delta patches and row-level materialization state. A full current snapshot is the later compaction/checkpoint, not the only valid serving target.
- Treat board-visible apply as an item-level queue: `job_materialization_items` records pending/running/retryable attempts, while `job_board_visible_patches` records successful projection publication.
- Treat full snapshot compaction as an item-level queue too: `snapshot_full_materialization` items record pending/running/retryable/completed full artifact/retrieval/index work, while normalized artifacts and job result views remain success outputs.
- Treat local apply closure as an item-level queue: `local_apply_closure` items record pending/running/retryable/completed downstream closure after provider output has already been applied locally, while `inline_incremental_ingest` remains the worker collector gate.
- Expose materialization queue state as a first-class smoke/service metric, not as a derived summary field. Backlog, retryable, and stale-running snapshot-full items are product-visible risks because the board may be usable while durable normalized artifacts are still catching up.
- Expose local apply closure as a first-class smoke/service metric. A worker with `inline_incremental_apply` but no `inline_incremental_ingest` means provider output reached local state but the downstream ingestion/materialization gate did not close; this must be counted separately from slow provider workers and full-compaction backlog.
- Recover local apply backlog through callback-only replay until durable local-apply item ownership lands. The replay is allowed to run profile-refill/materialization close steps, but it is not allowed to submit the original remote provider worker again.
- Do not update board-visible counters without a serving projection. Counts, result view, lifecycle, and candidate page rows must move together.
- Update lifecycle counts separately: fetched, applied-to-snapshot, materialized/board-visible, current snapshot serving.
- Update filters, recall counts, candidate rows, and visible sync metrics from the serving projection as each patch becomes visible.
- Preserve baseline ordering and pagination stability.
- Repoint/compact to current snapshot only after the durable serving projection is valid; the user should not wait for full snapshot rebuild to see safe row-level updates.
- Layering/outreach refresh should be a post-result phase with its own status and auto-refresh path.

Regression target:

- During OpenAI ChatGPT-style baseline+delta, `profile_fetch_status_text` may advance ahead of `card_materialization_status_text`; `卡片详情已合入看板` should then advance in batches as display-ready cards are published, not stay at zero until a final jump unless the lifecycle explicitly says batch materialization is disabled for that run.
- Candidate rows and filters should update from board-visible patches before full current snapshot compaction; browser assertions should distinguish patch-visible, projection-valid, and compacted-snapshot states.
- Scripted/hosted reports should fail or flag any case where `delta_profile_board_visible_count > 0` but `serving_projection_id` or replayable board-visible patches are missing. Fetched-to-board-visible lag is an optimization metric; it should stay bounded and explainable, not hidden behind final snapshot success.

### 6. Upgrade Scripted And Browser Test Foundation

Tests must model service conditions, not just final success.

Implementation requirements:

- Use real captured provider samples by default for OpenAI/Lovable heavy fixtures.
- Include lane skew, out-of-order completion, slow tails, retryable timeout, zero-result lane, cache hits, late webhook, watcher fallback, and writer contention.
- Record per-sample frontend text, backend lifecycle, result-view snapshot id, candidate-source snapshot id, worker state, item queue state, batch envelope size/reason, provider event timestamps, actor occupancy, and API response times.
- Fail tests on stale complete, lifecycle divergence, progress regression, materialized-ahead-of-fetched, public API timeout, actor underuse while eligible work exists, unexplained tiny batches, and wrong stage label.
- Keep final live provider smoke as a user-run gate after scripted/browser confidence is high.

Regression target:

- OpenAI ChatGPT baseline+delta, OpenAI Infra-shaped baseline+delta, and Lovable 100+ live roster should run in isolated scripted/browser mode and produce reports that are useful for optimizing service latency.

## Workstream Boundaries

Do first:

- Create the canonical lifecycle source of truth and route public readers through it.
- Add the Infra-shaped atomic progress regression.
- Add scheduler latency/occupancy metrics that explain provider slot underuse and small-batch flush reasons.
- Add a design review gate for each implementation slice: name the canonical state it writes, the stale source it removes, the negative browser assertion it adds, and the communication/performance overhead it reduces.

Do not do first:

- More frontend wording patches without backend contract changes.
- More result-view repair fallbacks that do not converge into one lifecycle writer.
- More generated-only fixtures that hide materialization and filter-quality problems.
- Large live-provider experiments before scripted reproductions are stable.

## Planning Review Loop

Each implementation slice should be reviewed against these questions before coding:

- Which previously observed failure class does this make impossible, not merely detectable?
- Which old derived source or fallback does this retire?
- What is the durable unit of state for this slice: lifecycle row, item queue row, projection patch, or materialization checkpoint?
- How does this behave when current lane and former lane complete out of order?
- How does this behave when provider webhook is late and watcher/recovery wins?
- How does this avoid tiny remote runs and amortize webhook/watcher/recovery overhead?
- What does the user see before full snapshot compaction completes?
- Which scripted/browser assertion will fail if the behavior regresses?

## Service-Grade Slice Protocol

Before implementing each optimization, record the historical failure class and the
service metric the slice is expected to improve. The default metrics are:

- **State coherence:** `/dashboard`, `/progress`, and `/candidates` expose the same
  lifecycle snapshot ids, served/expected counts, profile fetch/materialization
  counters, and terminal state for the same job sample.
- **Latency segmentation:** provider completed -> local observed, local observed ->
  lightweight apply done, apply done -> next submit started, next submit -> remote
  actor running, profile fetched -> board-visible materialization, and board-visible
  -> full snapshot compaction.
- **Provider efficiency:** actor slot occupancy, queued eligible item count,
  submitted batch size, small-batch reason, limiter/backoff reason, and webhook /
  watcher / recovery event source.
- **Batch envelope audit:** every live profile provider run must explain whether
  the submitted URL count is a normal adaptive envelope, final tail, retry
  isolation, urgent user-visible item, low-volume company, or a policy/backpressure
  outcome. Unexplained `2`/`3` profile batches and idle actor slots while eligible
  profile URLs remain are service-level violations, not just diagnostics.
- **User-visible continuity:** no stale-complete state, no progress regression, no
  materialized-ahead-of-fetched counter, no misleading stage label, no candidate
  board header claiming final sync from frontend-local cache length.
- **Interaction fitness:** the board should remain usable while background work
  continues; filters and counts must describe the currently served projection rather
  than an internal worker or snapshot rebuild state.

Each slice must add or update at least one negative test or scripted/browser
observation that would have caught a known incident. If a transition design is
temporary, document the transition explicitly and name the follow-up deletion or
migration step before starting the next workstream.

Current transition note:

- 2026-05-02: Harvest profile prefetch now emits `batch_envelopes` and coalesces
  tiny chunks within a single ready dispatch wave before submitting provider runs.
  This prevents one class of unexplained `2`/`3` profile batches and makes the
  rest fail scripted event-level guardrails. It is not yet the full durable
  queue-first scheduler because it does not coalesce across future/near-ready
  item arrivals or persist item-level scheduling checkpoints.
- 2026-05-02: Harvest profile prefetch now also emits a first-class
  `profile_prefetch_queue` snapshot. It exposes the explicit item-store contract
  (`linkedin_profile_registry`), requested/cached/ready/newly-queued/already-queued/
  deferred/failed/pending URL counts, local and remote queue-quiescence flags,
  oldest pending item age, worker-slot occupancy basis, and registry status counts.
  `workflow_efficiency` aggregates this queue snapshot so scripted/browser reports
  can inspect queue watermarks without scraping URL lists, worker summaries, or
  provider UI logs. This is still an observability/contract slice, not the final
  queue-first scheduler: provider batches are not yet generated from durable
  item-level scheduling checkpoints across event boundaries.
- 2026-05-02: running inline materialization now emits structured
  `workflow_materialization` events, and smoke/efficiency reports count them
  alongside `completed_workflow_reconcile`. The OpenAI Agent scoped-delta strict
  scripted smoke now observes post-preview finalization (`materialize_completed=1`)
  instead of requiring manual artifact inspection. Provider case reports also roll
  up materialization streaming budgets from structured event payloads, so the same
  run exposes `materialization_streaming.report_available=true`, event samples,
  provider response count, pending delta count, and budget action counts.
- 2026-05-02: strict smoke exposed and fixed two scheduler-envelope edge cases:
  provider/limiter `backpressure` is not idle actor-slot underuse, and unproven
  tiny final-tail batches are deferred while sibling profile workers remain active.
  This keeps `2`/`3`/`1` live profile runs out of the normal path unless queue
  quiescence, retry isolation, low-volume company, or another explicit policy
  explains the small batch.
- 2026-05-02: tiny final-tail queue quiescence now requires a coalescing-window
  proof for non-low-volume companies. Fresh high-volume `1`-`5` URL tails are
  marked in `linkedin_profile_registry` as `deferred_coalescing` and released
  instead of immediately submitting a live provider run; a later attempt may flush
  them as `queue_quiescent_final_tail` only after
  `HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS` elapses. This reduces
  webhook/watcher/recovery overhead from premature tiny actor runs while preserving
  explicit low-volume and retry-isolation exits.
- 2026-05-03: `deferred_coalescing` timer ownership moved into the same durable
  profile URL scheduler. Fresh high-volume tiny tails now write
  `linkedin_profile_registry.refill_queue_state='deferred_coalescing'` plus
  `refill_not_before_at`; refill selectors hide the row until the deadline, and
  the service-loop refill path wakes ready rows through `ProfilePrefetchBatchPlan`.
  Normal coalescing no longer creates fake `waiting_profile_coalescing` provider
  workers. Recoverable-worker scans exclude that legacy stage, and efficiency reports
  count any remaining coalescing worker timer as scheduler pollution.
- 2026-05-03: profile retry readiness moved into the same durable scheduler instead
  of being inferred from `status='failed_retryable'`. Retryable profile failures now
  enter `linkedin_profile_registry.refill_queue_state='retry_wait'` with
  `refill_not_before_at` only when the row carries workflow recovery scope
  (`source_jobs` and `last_snapshot_dir`). Maintenance/offline failures remain
  lifecycle evidence but are not daemon-drainable retry items. The service-loop
  selector drains `deferred_budget`, `deferred_coalescing`, and `retry_wait` through
  one grouped job/snapshot refill path.
- 2026-05-03: profile dispatch ownership was split from provider ownership. The
  batch plan now records active selected URLs as `dispatch_claimed` with a short
  `refill_not_before_at`, making the submit window durable and recoverable if the
  process dies before provider submission. Only a successfully submitted remote
  provider run promotes those URLs to `planned_dispatch`; provider limiter
  backpressure and worker-begin failures return URLs to `deferred_budget`. The
  refill selector scans expired `dispatch_claimed` rows through the same grouped
  job/snapshot path, so `planned_dispatch` no longer becomes a black hole for
  work that no provider worker actually owns. The final ownership slice requires
  `planned_dispatch` to carry the remote worker/run/dataset envelope on the
  registry row and writes terminal completion back to the same item; ownerless
  `planned_dispatch` is an efficiency violation, not a healthy wait state.
- 2026-05-08: profile dispatch ownership gained a separate `dispatch_reserved`
  state. The per job/snapshot scheduler lock writes `dispatch_reserved` for
  active chunks selected by the current plan before the provider submit path
  starts; append-trigger replans may still pull not-yet-ready
  `deferred_coalescing` rows for probe coalescing, but they must not ignore
  `dispatch_reserved` or `dispatch_claimed` timers. Provider limiter, submit-slot,
  worker-begin, or ordinal backpressure returns reserved URLs to
  `deferred_budget` with the concrete reason. A provider-slot-confirmed submit
  promotes the URL to `dispatch_claimed`, and only a remote worker/run promotes it
  to `planned_dispatch`.
- 2026-05-02: profile prefetch batch planning now has an explicit
  `ProfilePrefetchBatchPlan`. The plan owns ready URL normalization, recommended
  dispatch window, tiny-batch coalescing, worker-budget splitting, deferred URL
  accounting, and plan reason before any provider worker is submitted. Both
  `queue_background_profile_prefetch` and `enrich(..., full_roster_profile_prefetch=True)`
  use it, so the profile-prefetch entry points no longer duplicate tiny-tail and
  backpressure rules. Profile URLs are now represented as `ProfilePrefetchQueueItem`
  records backed by `linkedin_profile_registry`, preserving registry key/status,
  source shards, source jobs, priority, and queue state through planning. Efficiency
  reports aggregate compact batch-plan counts (`queue_item_count`,
  `planned_dispatch_item_count`, `planned_deferred_item_count`) without logging full
  URL lists. The same plan now emits the first continuous-refill audit contract:
  `available_slot_count`, `planned_new_worker_count`,
  `unfilled_available_slot_count`, `underfilled_with_deferred_items`, and
  `refill_saturation`. This makes underfilled provider-slot opportunities visible
  before adding a standalone refill loop. The next lift is an explicit refill
  entrypoint and durable item records across discovery/apply/materialization queues.
- 2026-05-02: provider-completion profile refill is now a first-class event-time
  trigger. `_handle_harvest_profile_completion_event` wraps the compatible
  `_queue_background_profile_prefetch_after_harvest_ingest` helper with
  `_trigger_profile_prefetch_refill`, and job events now carry
  `profile_refill_trigger` with trigger kind/source/reason, item store, elapsed
  time, dispatch/deferred counts, and batch-plan slot-fill fields. Efficiency
  reports aggregate this as `profile_prefetch_refill`, including underfilled
  ready/deferred work. This is intentionally not a second scheduler: batch policy
  still comes from `ProfilePrefetchBatchPlan`; the next step is durable refillable
  item records and a loop that drains ready items across events.
- 2026-05-02: durable refill item-state now lives on `linkedin_profile_registry`.
  The registry stores `refill_queue_state`, last trigger/plan/deferred reason,
  planned timestamp, and attempt count, with matching SQLite/PG schema and a PG
  refill-queue index. `queue_background_profile_prefetch` records scheduler-owned
  states rather than inferring them from worker summaries: budget-deferred URLs use
  `deferred_budget`, scheduler-selected local submit windows use
  `dispatch_reserved`, slot-confirmed submit attempts use `dispatch_claimed`, and only
  provider-owned URLs use `planned_dispatch`. Queue/efficiency reports aggregate
  `refill_queue_state_counts`. This deliberately separates provider lifecycle state
  (`fetched`, `queued`, `failed`) from scheduler state, so neither a budget-deferred
  URL nor a failed-to-submit URL is mistaken for an active remote worker. The next
  lift is a registry-backed refill selector/loop over these item states.
- 2026-05-02: the first registry-backed refill selector is in place.
  `list_linkedin_profile_refill_queue_items` reads durable `deferred_budget`
  items by job/snapshot, and `queue_background_profile_prefetch` folds them back
  into the same `ProfilePrefetchBatchPlan` before dispatch. This means a refill
  opportunity can run from registry-only deferred work even when the current
  workflow event provides no new candidate URLs. The remaining scheduler gap is
  a daemon/service loop that invokes this selector when provider slots free up,
  independent of a new workflow event.
- 2026-05-03: workflow-scoped `retry_wait` rows use the same selector and
  `ProfilePrefetchBatchPlan` path as budget-deferred and coalescing-deferred rows.
  `failed_retryable` by itself is not a scheduler item; it becomes retryable service
  work only when it has a concrete `source_job + snapshot_dir` owner.
- These are still transition slices: partial delta rows are not yet visible through
  row-level serving patches before full projection compaction, and provider batches
  are not yet generated from durable item-level queues.

## Definition Of Done

The rebuild is not done until:

- `result_view_lifecycle` is persisted and read consistently by `/progress`, `/dashboard`, and `/candidates`.
- A terminal workflow cannot expose stable `results` while serving a stale baseline or raw delta-only board.
- Stage 1 progress counters are monotonic and sourced from one coherent projection.
- Provider completion to next-submit latency is measured and bounded independently from materialization.
- Actor slot occupancy is observable and tested under enough queued work.
- Provider batches are generated by an adaptive packer over durable item queues, with retry/dedupe/progress/materialization tracked per item rather than per worker group.
- Tiny live batches are automatically coalesced into larger envelopes by default; exceptions require queue-quiescence or retry/low-volume justification and are tested.
- Partial delta board visibility is either implemented or explicitly represented as disabled with honest lifecycle wording.
- Candidate board serving uses a continuous projection/patch model before full snapshot compaction, so "snapshot repoint" is no longer the first moment new rows can be safely visible.
- Browser/scripted reports catch the known OpenAI Infra, ChatGPT, Whisper, Health, Meta full-reuse, and Lovable live-roster failure classes.
- Docs, TODO, and runbooks describe the new contract and how to validate it locally, scripted, hosted, and live.

## Docs To Keep In Sync

- `docs/CLAUDE_CODE_STREAMING_WORKFLOW_REBUILD_CONTEXT.md`
- `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`
- `docs/WORKFLOW_PROGRESS_CONTRACT.md`
- `docs/TESTING_PLAYBOOK.md`
- `docs/NEXT_TODO.md`
- `PROGRESS.md`
