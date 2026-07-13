# Event-Level Workflow Response

> Status: Active architecture note. Use this with `docs/WORKFLOW_BEHAVIOR_GUARDRAILS.md`, `docs/NEXT_TODO.md`, and `PROGRESS.md` when changing provider-backed workflow orchestration.

## Purpose

事件级响应的目标不是“所有步骤并发”，而是让外部 provider 状态变化被本地 runtime 尽快转成可恢复、可审计、可继续推进的工作单元。

业务目标是：用户提交 scoped search / roster / profile completion 后，系统应尽快把“provider 已有新信息”反映到候选人资产、进度状态和下一批 provider 调度中。完整 materialization、retrieval、layering 可以随后追平，但不能阻塞下一批 provider actor 的提交，也不能让 UI 长时间只看到旧 snapshot 或旧 job result view。

核心原则：

- 远端完成发现属于 wakeup/recovery 层，不应和 candidate materialization 重新耦合。
- 本地一旦观察到 provider completed dataset，应先执行 ingest / registry / progress 可见更新。
- next submit 应在 lease、budget、dedupe 允许时继续推进，不应默认等待 current batch 的 full materialize。
- apply/materialize 是下游 writer-budget 阶段，应能异步追平，并保留明确的 progress marker。
- retry 必须以 URL / task / provider checkpoint 为粒度，只重试 failed 或 unresolved work，不重跑整批。

## Product Expectations

用户可见的预期：

- provider search/profile actor 完成后，候选人同步进度应在下一轮 local event apply 后前进，而不是等 full artifact rebuild。
- profile actor 有空闲 slot 时，应尽快补下一批 deferred URLs；空闲 slot 不应因为本地 writer lock、raw payload 解析或 full materialization 而空窗数分钟。
- scoped search 的 current/former lane 是独立业务 lane，应能并行发起、分别复用、分别记录 incomplete/degraded 状态。
- `LinkedIn Stage 1` 默认只使用 LinkedIn-related providers（company employees / profile-search / profile-scraper）。DataForSEO/public-web seed fallback 不属于默认 Stage 1；只能通过 `allow_stage1_web_seed_fallback` 显式打开，或放到 Public Web Stage 2。
- candidate board 应消费当前 job 的 `job_result_view` 指向的 snapshot/view；不能因为旧 hot cache、旧 summary 或旧 baseline 指针漂移回旧资产。
- recall/filter 应优先消费 serving record 的 `source_matches` / `matched_keywords`，旧 artifact 缺这些字段时才 fallback 到文本搜索。

工程预期：

- 事件级 workflow 追求的是 bounded latency、state monotonicity、idempotent recovery，不是无约束并发。
- 所有外部 provider 调用必须受 cost policy、provider limiter lease、URL/query dedupe 和 checkpoint 控制。
- 如果一个阶段失败，恢复路径应从该阶段的 durable marker 继续，而不是重跑整条 job。
- “最终 artifact 正确”不是充分验收条件；重复 materialize、phantom active worker、next-submit 延迟过高也应视为 workflow bug。

## Business Flow For LinkedIn Scoped Search

以 `帮我找 Meta/Google 做某方向的人` 这类 large-org scoped search 为例，期望业务流程是：

1. planner 解析公司、scope keywords、current/former lane，并生成同一 effective request contract。
2. asset reuse 判断只复用真正覆盖该 scope/lane 的 shard 或 baseline；不能因为 broad baseline 存在就跳过新的 scoped current lane。
3. current lane 用 `currentCompanies + scope keyword` 调 Harvest profile-search；former lane 用 `pastCompanies + scope keyword` 调 Harvest profile-search。两个 lane 可以并行。
4. profile-search 结果先落 search-seed durable entries，并增量写入 candidate snapshot。
5. 已知 LinkedIn URL 进入 profile registry / prefetch queue；已 fetched/queued URL 不再重复提交。
6. profile-scraper actor completed 后，local event apply 先写 raw artifacts、profile registry、worker consumption marker 和 progress，再触发 next-submit opportunity。
7. full materialize/retrieval/layering 在 writer budget 下异步追平；同类 search/profile sibling worker 未 drain 时，应写 `materialize_deferred` 而不是反复 full rebuild。
8. workflow 完成后，`job_result_view` 指向本次 current snapshot 的 serving artifacts；旧 job 是否 repoint 到更全 company canonical view 必须显式使用 policy。

## Workflow Shape Unification

`scoped_search`, `scoped_search + baseline`, and `live_roster` are discovery/denominator variants of the same post-discovery workflow, not separate profile workflow implementations.

- `scoped_search + baseline` may serve an existing baseline immediately and then stream delta profile/card progress over the scoped delta denominator.
- `scoped_search` without baseline starts from an empty or narrow serving projection, but after search-seed URL discovery it must use the same profile registry scheduler, retry-wave contract, local apply closure, board-visible patches, full materialization queue, lifecycle publication, and progress projection.
- `live_roster` uses company-roster discovery lanes and a run-population denominator, but profile fetch/card materialization still flow through the same registry scheduler and post-profile state machine.
- New workflow shapes should add discovery-lane registry fields or denominator metadata only. They must not add a second profile batch planner, foreground profile connector call, public-read repair path, or frontend-only progress source.

## Non-Negotiable Invariants

这些不变量是后续 review 的重点：

- `remote_wait` worker 必须有可恢复 provider checkpoint：run id、dataset id、task id、payload manifest 或 search query context。
- worker 未拿到真实 provider limiter lease 前，不得创建为 active remote worker，也不得占用 actor budget。
- provider completion event 只能消费一次；成功 apply 后必须立即写 `inline_incremental_ingest` 或等价 local marker。
- next-submit opportunity 必须在进入 job writer lock / full materialize 之前发生。
- Recovery daemon 即使为了 PG store 安全而串行执行 selected workers，也必须在每个 worker 返回后立即运行 completion callback / local-apply enqueue；不能等本轮 selected workers 全部执行完再统一 callback。否则一个慢 worker 会在同一 recovery tick 内阻塞后续已唤醒 worker 的 marker、local-apply 和 next-submit，形成 head-of-line blocking。
- Remote provider event handling must persist the terminal checkpoint and release the matching worker/provider-limiter leases before it emits one shared `worker-recovery-daemon` wake-now signal. The request path must not ensure a job-scoped service, spawn a sidecar/thread fallback, or execute a recovery tick inline.
- The shared wake file is a pure coalesced nudge, not a control envelope. It carries no `explicit_worker_ids`, job scope, stale threshold, limit, phase enablement, or fallback instruction from the webhook payload; the shared daemon discovers work from durable worker/checkpoint state. Concurrent events may coalesce safely because durable terminal markers, not wake payload merging, are the source of truth.
- local event apply 可以解析 provider output 和写 candidate delta，但不能把 full artifact rebuild 作为 marker 写入前置条件。
- full materialize/retrieval/layering 必须有独立 structured event 或 `background_snapshot_materialization` 状态，能单独恢复。
- completed-job reconcile 必须拿 job-level lease；同一个 worker 被 summary 证明已消费时只能 marker backfill，不得重新 apply/materialize。
- profile next-submit 判定必须优先用 profile registry marker，不得为了判断已 fetched/queued 而全量读取 raw Harvest profile JSON。
- search provider 真 0 结果可以 bounded retry；provider total 漂移/缺页默认 degraded/audit，不应轻易 block 本地开发流程。
- non-LinkedIn web seed discovery 必须是显式 opt-in。旧 plan / 旧 worker 如果产生了 DataForSEO search-seed 且 `seed_entry_count=0`，reconcile 应 no-op 标记 consumed，不能再触发 profile prefetch 或 full materialize。
- stale shard cleanup 只清当前 `artifact_dir` / `asset_view` 的 serving cache；不能删除历史 snapshot/shard provenance。

## Failure Modes Already Seen

这些问题都在真实 query 中出现过，后续 review 应优先找同类残留：

- Broad baseline 被误判为覆盖 scoped keyword，导致 Google/Gemini current lane 一开始没有发 `currentCompanies=Google + Gemini`。
- 同一 normalized Harvest search payload 被多个 raw query 重复提交，造成 duplicate scale run。
- search probe 有结果但 scale 返回空时，早期逻辑丢掉 probe rows 或把少量 fallback 当完整覆盖。
- current/former lane progress and profile-fetch denominators came from different snapshots in the OpenAI Infra run: a true zero current result was displayed together with a larger worker URL queue denominator. Stage 1 public counters must now prove lane-returned, deduped, and profile-required counts are one coherent projection. Scoped lanes that explicitly accept zero results should complete as `zero_result_accepted`, not as terminal `provider_search_retry` failures.
- Harvest provider 对同一 input 偶发 `0 profiles`；现在只对真 0 结果 retry，不再把常见缺页误判为强 incomplete。
- profile worker 在 submit 前就被算作 active，导致 actor slot 被 phantom worker 占住。
- completion callback 在进入 job writer lock 前做了太多 prefetch/full scan，导致多个 completion 并发扫描和补发。
- next-submit 虽然在单次 callback 内早于 materialize，但下一次 remote completion 仍会被上一轮长 writer lock 卡住。
- completed-job reconcile 没有可靠 marker backfill，导致旧 consumed worker 可能重复进入 materialize/layering 慢路径。
- URL miss 后按姓名全量扫描大 snapshot，导致 9k candidates x profile batch 的 CPU 热点。
- hot-cache 坏 manifest 或旧 job_result_view 遮蔽 canonical snapshot，前端看到旧 2 人资产而不是新 515/9359 人资产。
- 手动 `--no-daemon` 旧语义没有禁用 serve 内置 watchdog，导致 UI-only 手测时后台抢跑 scheduled materialization。
- Public Web Stage 2 虽已 default-off，但 Stage 1 里仍残留 DataForSEO seed fallback，导致 `public_web_stage_2` 关闭时仍有 `search_seed_discovery` worker 产生；现已改为显式 opt-in。

## Lessons For Future Reviews

- 测试不能只断言最终 candidate count 或最终 board 不为空；必须断言事件顺序、marker、重复 reconcile 次数、materialize 次数和 provider slot 真实占用。
- “单次 callback 内 next-submit 在 materialize 之前”不是充分条件；必须模拟 writer lock 被占用时的新 remote completion 是否仍能快速补位。
- scripted E2E 需要覆盖乱序完成：多个 search shard、多个 profile batch、cache-hit profile、provider partial/zero result、旧 result-view 指针。
- 大组织性能问题通常来自本地热路径，而不是 provider：全历史 candidate_documents 预读、raw profile stat/open、重复 facet/text projection、姓名全表扫描都应被 benchmark 和 audit 捕获。
- Provider anomaly 要和 workflow bug 分开：provider 0 result/total drift 应记录 retry/degraded；本地串行、重复 submit、marker 缺失、writer lock 空窗是工程 bug。
- Provider I/O timing 要和 local I/O timing 分开：Apify actor runtime、dataset download duration、remote event lag、candidate delta apply、artifact build、writer wait 都必须是独立指标，不能用一个 workflow wall-clock 总耗时掩盖瓶颈。
- `remote_to_next_submit_start_ms` is an end-to-end diagnostic and must be interpreted with its segment report: `remote_completed_to_event_seen_ms`, `event_seen_to_completion_marker_ms`, `completion_marker_to_next_submit_start_ms`, `event_seen_to_next_submit_start_ms`, `event_seen_to_next_submit_finish_ms`, and `next_submit_provider_attempt_elapsed_ms`. Optimizations should target the segment that is actually high; a high remote event lag is different from a slow callback marker or a slow refill-daemon provider submit.
- Remote-provider wakeups are small-step by contract. Multiple terminal events may coalesce into one shared wake file without losing work because every actionable worker already has durable terminal evidence. The shared daemon applies its own bounded claim/tick policy and leaves remaining durable workers for later ticks; the API does not calculate or forward a per-event `total_limit` or worker list.
- `remote_to_local_marker_lag_ms` means provider terminal timestamp to local terminal marker (`remote_provider_terminal_event_seen_at` / remote-event job event), not provider terminal to full worker completion callback. Completion callback latency is reported separately as `event_seen_to_completion_marker_ms`, while user-visible effects are gated by profile-file-to-board-patch and all-profiles-to-all-cards SLOs.
- Remote-provider terminal events are idempotent by remote identity (`run_id` first, then `dataset_id`). The first durable terminal event for a worker/run is the actionable wakeup and owns marker-lag accounting. Later webhook/watcher duplicates for the same identity are audit events (`received_in_flight` while worker completion is still draining, `received_late` after worker terminal) and must not count as actionable remote-event lag or create another marker-lag sample.
- 文档与进度必须记录操作边界：哪些是真实数据已修，哪些只是未来 maintenance backlog，哪些不能在 request hot path 中做。

## Review Handoff

下一轮工程 review 应重点审查：

- `src/sourcing_agent/orchestrator.py`
  - remote provider event -> durable terminal handoff -> shared-daemon signal -> completed reconcile 的锁范围、marker 写入、same-kind defer。
- `src/sourcing_agent/enrichment.py`
  - profile prefetch submit、registry-only marker、provider limiter lease、active worker 统计。
- `src/sourcing_agent/seed_discovery.py`
  - current/former scoped lane 并行、Harvest query dedupe、zero-result retry/degraded/incomplete 语义。
- `src/sourcing_agent/workflow_efficiency.py`
  - 指标是否足够捕获 next-submit latency、重复 materialize、phantom provider slot、provider actor runtime、dataset download time、local materialization I/O。
- `src/sourcing_agent/candidate_artifacts.py` / `snapshot_materializer.py`
  - 大 snapshot foreground-fast rebuild 是否仍存在全历史预读、raw path stat/open、全表 name fallback。
- `src/sourcing_agent/api.py` / frontend progress consumers
  - progress 是否区分 provider fetched、local marker、materialize pending，而不是把缺失分层显示成真实 Layer 0。

Review 输出应包含：

- 明确列出仍可能导致 actor slot 空窗、重复 provider submit、重复 full materialize、stale result view、provider anomaly 被误判的路径。
- 为每个风险给出可执行修复或测试建议。
- 如果改代码，必须更新 `PROGRESS.md` 和 `docs/NEXT_TODO.md`，并跑 targeted tests。

## Contract Shape

一个事件级 workflow lane 必须按状态 owner 拆分，而不是按 worker callback 的历史 Phase A/B/C 注释理解。Provider batch 只是远端 envelope；业务状态、重试、进度和 board 可见性都必须落到 durable item / registry / lifecycle owner 上。

Canonical states:

- `discovery_item_recorded`: search / roster 返回的候选行已经成为 durable discovery item 或 snapshot-level `candidate_documents.json` preview/projection。Owner 是 discovery-lane registry 或 roster/search-seed apply item。它不能等待 profile fetch、full artifact materialization、retrieval 或 layering。
- `profile_url_ready`: 一个 normalized LinkedIn URL 已经进入 `linkedin_profile_registry`，带 `source_job`, `last_snapshot_dir`, source lane/shard, retry budget, and queue state。Owner 是 profile URL registry item，不是 worker row。
- `profile_actor_envelope_submitted`: scheduler 将 ready URL items 打包成 provider actor envelope。Owner 仍是 URL item；worker/provider run 只是 remote-run envelope。`dispatch_reserved` 表示同一 `source_job + snapshot_dir` 的 scheduler 已经在短 TTL 内选中该 URL，防止另一个 daemon/callback 重复计划同一 wave；`dispatch_claimed` 表示 provider limiter/submit slot 已确认且 scheduler 正在尝试 submit；`planned_dispatch` 只有在远端 run/worker 已经拥有 URL 且携带 run/dataset/payload identity 后才有效。
- `profile_url_terminal_recorded`: provider result 已经按 URL item 写入 fetched / failed_retryable / unrecoverable / unresolved 状态。Partial success 是 item-level；成功 URL 立即离开 scheduler queue，失败 URL 进入 scoped `retry_wait` 或 terminal unrecoverable，不得重跑整个原始 provider batch。
- `provider_completion_observed`: webhook、watcher、worker recovery 或 poll 观察到 provider terminal state，并记录同一 `remote_provider_event` / worker completion evidence。这个状态只唤醒 runtime-owned recovery owner，不直接执行 full materialization。
- `local_delta_applied`: completed worker output 已经在短 job writer lock 内 merge 到 snapshot candidate delta，并写 `inline_incremental_apply` marker。Owner 是 `local_apply_closure` item；该状态允许解析 provider output、写 raw artifact/profile registry/candidate delta、更新 progress marker，但不能把 full snapshot compaction 作为成功条件。
- `local_delta_ingested`: local closure 已经完成到可继续服务的本地状态，并写 `inline_incremental_ingest` / completed `local_apply_closure` item。对 profile batch，这意味着 changed candidate IDs 已足以进入 board-visible apply queue，或明确无需 board-visible patch；它不等于 retrieval/index/layering 已完成。
- `board_visible_delta_published`: `board_visible_delta_apply` item 成功发布 ordered patch / serving projection event。Owner 是 `job_materialization_items(item_kind='board_visible_delta_apply')` 和 `job_board_visible_patches`。这是候选卡片进入用户可见 serving projection 的信号。
- `snapshot_full_materialized`: `snapshot_full_materialization` item 完成 normalized artifacts、retrieval/index/export/facet compaction。Owner 是 full-materialization queue；它是后台 compaction checkpoint，不是用户首次看到候选卡片的前置条件。
- `serving_finalized`: result view / serving projection 已经对当前 workflow 的 expected board population 给出稳定、单调的 public contract。它可以由 full `current_snapshot_serving` 或 baseline generation + ordered delta patches 达成。它不等于 post-result layering 完成。
- `post_result_layering`: outreach / 华人线索分层 / heavier facets 在 results 后继续追平。Owner 是 layering/materialization event-time writer；public reads 只能渲染状态，不能回写 lifecycle。

Required ordering:

- `discovery_item_recorded -> profile_url_ready -> profile_actor_envelope_submitted -> profile_url_terminal_recorded` is item-level. It must not wait for all current/former/live-roster discovery lanes unless the next state explicitly requires lane terminal proof.
- `profile_url_terminal_recorded -> provider_completion_observed -> local_delta_applied -> local_delta_ingested -> board_visible_delta_published` is the event-time user-visible path for profile cards. It must run per completed profile batch or per bounded group, not after all profile batches finish.
- `snapshot_full_materialized`, retrieval refresh, export artifact rebuild, and `post_result_layering` are downstream/background. They may follow `board_visible_delta_published`; they must not gate profile next-submit, local delta apply, or board-visible patch publication.
- `serving_finalized` requires a coherent serving projection and stable public counters. It may be reached before `snapshot_full_materialized` only if the serving projection can answer `/dashboard` and `/candidates` from baseline + ordered patches without raw delta-only drift.

Allowed synchronous work after a profile provider completion:

- record provider terminal evidence and URL-level status
- emit a next-submit/refill audit signal for the runtime-owned profile-refill daemon
- enqueue deterministic `local_apply_closure` for the completed worker/batch

Forbidden synchronous work in provider completion callbacks, webhook handlers, local watchers, and profile next-submit gates:

- claiming or processing `local_apply_closure` / `board_visible_delta_apply` durable items
- full snapshot normalization / compaction
- retrieval/index/export rebuild
- outreach/layering/facet recomputation for the full board
- public-read lifecycle repair or result-view repoint
- scanning raw Harvest profile JSON only to decide whether a URL is fetched/queued
- claiming durable work from a different `SOURCING_RUNTIME_DIR` namespace

Concurrency rules:

- Profile URL scheduler work is item-level and may keep filling provider slots while local apply/materialization for earlier URLs is running, provided registry dedupe, provider limiter, cost policy, retry budget, and runtime namespace ownership all pass.
- Local candidate delta apply is serialized by short job-scoped writer locks only for the actual merge/marker write. Lock scope must not include next submit, full compaction, retrieval, or layering.
- `board_visible_delta_apply` may be chunked and retried through durable items. The patch sequence/watermark is the publication ordering source; metadata mirrors are diagnostics only.
- Full snapshot materialization may coalesce same-kind worker drain, but that coalescing is an optimization for compaction cost. It must never block next-submit, local delta apply, or board-visible card publication.

Any state failure must resume from the owner of that state. A provider retry resumes from URL/query/provider checkpoint state; local closure resumes from `local_apply_closure`; board publication resumes from `board_visible_delta_apply`; full compaction resumes from `snapshot_full_materialization`. No recovery path may silently restart the whole pipeline to compensate for a missing marker.

## Post-Profile Completion Contract

This section is the canonical contract for everything that happens after a LinkedIn profile actor envelope reaches terminal state. It applies equally to scoped-search, baseline+delta, live-roster, cache-hit profile completion, webhook, watcher, poll, and recovery-driven completion. Workflow-specific code may change denominator shape or discovery-lane proof, but it must not define a second post-profile pipeline.

Trigger sources:

- `provider_webhook`, local watcher, direct recovery poll, worker recovery callback, and explicit job recovery all normalize into one completion evidence path.
- The trigger source may affect observability fields, but it must not change durable ownership. The same worker/result must enqueue the same `local_apply_closure` item id and advance the same URL/profile registry states.
- Late duplicate completion events are audit evidence only. They must not re-download, re-apply, re-submit, re-publish, or re-run full materialization when the owning worker/item is already terminal.

Step 1: provider terminal evidence and URL item states.

- The first durable action after observing a terminal profile run is to bind the provider envelope back to its URL items and record item-level terminal states.
- Success is per URL. Fetched URLs write fetched/profile-available state, clear scheduler queue state, and become eligible for local delta apply/board publication.
- Retryable failures write failed-retryable evidence and move only unresolved/failed URLs into scoped `retry_wait`, subject to retry budget. The default contract is one retry per URL unless scenario/provider policy explicitly says otherwise.
- Unrecoverable failures write terminal unrecoverable evidence and also clear scheduler queue state. A URL with terminal unrecoverable evidence may still produce a low-richness / needs-completion card if the board contract allows it.
- A mixed provider result must not retry successful URLs, must not rebuild the original batch plan, and must not insert retry URLs ahead of unfinished normal-wave URL items.
- URL terminal-state recording is part of provider result consumption. Retry gating and board progress must treat `planned_dispatch` URLs without terminal item state as still in-flight, not as missing normal-ready work.

Step 2: next-submit opportunity.

- After terminal URL states are recorded, the runtime must attempt a next-submit/refill opportunity before any full snapshot materialization.
- The next-submit decision reads registry markers and scheduler queue states only: fetched/queued/dispatch_reserved/dispatch_claimed/planned_dispatch/retry_wait/unrecoverable, provider slot budget, limiter lease, cost policy, and runtime namespace ownership.
- It must not parse raw Harvest profile JSON, scan all candidates, wait for retrieval/index/layering, or wait for all profile batches in the job.
- Normal ready/refill states have priority over retry. Retry may start only after normal-wave closure for the same `source_job + snapshot_dir`, including terminal-state recording for the last normal provider-owned envelope.
- If normal ready URLs are available and provider slots are available, the packer should keep envelopes near the actor URL target or documented large-wave target. Small normal envelopes require an explicit queue-quiescent/coalescing/low-volume reason.
- Provider-completion callbacks must keep the next-submit opportunity as an audit signal only. They emit `provider_submit_deferred_to_refill_daemon` for the same `source_job + snapshot_dir`, enqueue durable local-apply closure work, and return. They must not run registry scan/replan/claim/submit, acquire provider limiter slots, spawn refill threads, parse raw Harvest profile payloads, scan candidate artifacts, claim/process `local_apply_closure`, publish board-visible patches, or run full snapshot materialization. The runtime-owned recovery phases own the actual next-submit, local apply, and board-visible drain work in the same tick after callbacks return when remote provider events wake a job. This preserves a single scheduler owner and lets local-apply items coalesce instead of forcing one heavy Phase B pass per provider callback.
- Handoff SLOs must measure real runtime dispatch evidence owned by profile-completion refill (`profile_prefetch_refill_daemon_group.started_at/finished_at`). A callback signal timestamp is not a provider submit attempt and must not satisfy `remote_to_next_submit` or `next_submit_attempt_elapsed` by itself. `profile_prefetch_phase_b_group` is valid profile-submit evidence for discovery/roster/search-seed append coverage and batch accounting, but it is not a profile-completion handoff sample and must not be paired with a later completed profile actor.
- `remote_to_next_submit_start_ms` is an end-to-end diagnostic from remote provider completion to the next real submit. It includes provider event lag and marker lag, so values between the scenario soft SLO and the hard threshold indicate review-worthy latency headroom but do not fail smoke when `local_completion_to_next_submit_start_ms`, `next_submit_provider_attempt_elapsed_ms`, and `profile_scheduler_contract` are clean. The hard failure threshold is published in `event_level_efficiency.thresholds_ms.remote_to_next_submit_start_hard` and defaults to 30 s.
- `local_completion_to_next_submit_start_ms` remains the hard scheduler handoff SLO for “slot released, local state updated, next submit started.” If this exceeds its threshold, the scheduler/recovery owner is not filling slots promptly and smoke must fail.

Step 3: local apply closure enqueue.

- Every completed profile worker that owns fetched/terminal URL evidence must enqueue deterministic `job_materialization_items(item_kind='local_apply_closure')`.
- The item is keyed by job, snapshot, worker kind, and owned worker ids. Re-enqueue is idempotent and must not create duplicate closure work.
- `local_apply_closure` is the source of truth for post-profile local closure. Worker output markers are collector evidence and compatibility signals, not the queue owner.
- Enqueue may happen before candidate delta apply if the worker has no `inline_incremental_apply` marker yet. The closure processor is responsible for doing or replaying the short local apply step.

Step 4: local delta apply.

- The closure processor may download/read provider output, write raw artifacts, update profile registry/profile cache, merge changed profile/candidate fields into the snapshot, and write `inline_incremental_apply` under a short per-job writer lock.
- The writer lock scope is limited to the actual candidate delta merge and marker write. It must not include next-submit, board-visible publication, full snapshot compaction, retrieval/index refresh, or layering.
- For `harvest_profile_batch`, local apply closure must not run profile registry scan/replan/claim/submit after ingest. It may carry forward the callback's `provider_submit_deferred_to_refill_daemon` signal for observability only; the profile-refill daemon is the sole owner of next profile submit work.
- If `candidate_documents.json`, company identity prerequisites, or the candidate-document rows for a fetched profile batch are not present yet, the closure item enters `waiting_prerequisite`. This is a dependency state, not a retry failure. The prerequisite writer must reawaken it immediately; the bounded timer is crash/race fallback only.
- If local delta apply succeeds but a later step fails, the next closure attempt must replay from `inline_incremental_apply` and changed candidate ids. It must not merge the same provider output into the snapshot again.
- A coalesced `local_apply_closure` drain may contain a mixed set: some owned workers already have `inline_incremental_apply` for the snapshot and some still need Phase A apply. In that case the processor writes new provisional apply markers only for the workers that actually ran Phase A, keeps existing markers untouched, and builds the downstream closure result from the union of replayed marker candidate ids plus newly applied candidate ids. `board_visible_delta_apply` must receive that union; completing an item for worker A with worker B's apply result is a contract violation.

Step 5: event-level drain.

- Event-level drain is the bounded same-job handoff from `local_apply_closure` to `board_visible_delta_apply`.
- Its policy is `local_apply_closure_then_board_visible_only`.
- It may claim a small bounded number of local-apply items or board-visible items for the same job, publish candidate-card patches, and append structured `event_level_materialization_followup` events.
- By default, if local apply claims real work, the same phase must skip board-visible apply with `reason=local_apply_work_handoff_to_board_visible_daemon`; the durable `board_visible_delta_apply` item remains queued for a later bounded phase. `EVENT_LEVEL_BOARD_VISIBLE_AFTER_LOCAL_APPLY_ENABLED=1` is an explicit opt-in for a tighter latency path, and then `EVENT_LEVEL_BOARD_VISIBLE_AFTER_LOCAL_APPLY_BUDGET_MS` caps the second step.
- It must skip `snapshot_full_materialization`, retrieval refresh, export rebuild, global facet recomputation, and outreach/layering.
- It is a latency optimization, not a correctness-only path. Durable queues remain authoritative; if the drain is skipped, interrupted, or bounded out, the runtime-owned recovery/service loop must later claim the same durable items.
- Harvest profile completion callbacks must not run this drain directly. They enqueue deterministic local-apply closure work and return so the recovery/service loop can coalesce multiple completed profile batches into one bounded local-apply pass.
- Worker recovery may run the bounded drain before and after profile-refill queue processing only when the tick did not itself consume terminal worker evidence or submit provider refill work. If `worker_recovery` or `remote_event_followup` claimed/executed a terminal worker, or `profile_refill` submitted provider work, the same tick must yield local-apply, board-visible apply, post-followup event-level drain, and full snapshot compaction to a later tick with `reason=worker_recovery_durable_handoff_to_daemon_tick`, `remote_event_durable_handoff_to_daemon_tick`, or `profile_refill_submit_handoff_to_next_tick`.

Step 5a: recovery/callback/daemon phase ownership.

- Provider/webhook/watcher callbacks are not durable-work executors. They may record remote evidence, release matching worker leases, enqueue deterministic local-apply work, and emit `provider_submit_deferred_to_refill_daemon`. They must not scan/replan/claim/submit profile registry rows, claim local-apply or board-visible durable items, acquire provider limiter slots, run full snapshot materialization, resume workflows, or refresh global runtime metrics.
- The worker-recovery daemon owns recovering selected worker rows and invoking their completion callbacks. It is allowed to execute selected workers serially against the shared store, then return. Worker recovery does not own profile registry planning or full snapshot compaction.
- The profile-refill daemon owns registry scan/replan/claim/provider submit. It runs after worker callbacks return in a recovery tick, reads scheduler-owned URL states, and may fill available provider slots. It must not perform local candidate delta apply, board-visible patch publication, full snapshot compaction, retrieval, or layering.
- The local-apply drain owns `local_apply_closure` only. The board-visible drain owns `board_visible_delta_apply` only. When ready `board_visible_delta_apply` work exists, recovery prioritizes publishing that bounded visible unit before claiming more `local_apply_closure` work. This avoids starving already materialized cards behind a long local-apply backlog while still preventing one tick from serializing both heavy phases.
- The full-snapshot materialization daemon owns `snapshot_full_materialization`. Provider-event request handling never selects or disables recovery phases; shared/background service ticks run the phase separately when the durable item is ready. Open `snapshot_full_materialization` items are diagnostic/background backlog for a terminal or board-visible job, not permission to start a request-owned sidecar.
- Same-job workflow resume is decided inside the shared recovery tick after daemon-owned work is clear, from durable job/worker state rather than webhook controls. A completed Stage 1 candidate document with `acquisition_stage.task_type='enrich_linkedin_profiles'` or `enrichment_scope='linkedin_stage_1'` is reusable only when its profile-prefetch registry proof is terminal (`all_requested_terminal=true` or equivalent requested-vs-terminal counts). Queued/open Stage 1 candidate documents are partial artifacts and must not prevent recovery from continuing LinkedIn Stage 1.
- Stage 1 preview is anchored to candidate-list terminal evidence, not to profile/card tail completion. Once LinkedIn Stage 1 has written `candidate_documents.json` / `candidate_documents.linkedin_stage_1.json` and queued profile-prefetch work, the acquisition state must mark `linkedin_stage_completed=true` so the main runner or preview recovery bridge can publish deterministic `stage_1_preview` immediately. Open `local_apply_closure` / `board_visible_delta_apply` work remains a finalization/resume barrier, but it must not delay the preview publication boundary.
- Hosted/acquiring workflow resume is a dispatched owner state, not inline recovery work. A recovery phase may start or confirm a job-scoped acquisition-resume thread and return structured phase evidence; it must not run full `_run_workflow_from_acquisition(...)`, retrieval, finalization, or materialization synchronously inside the recovery tick.
- Workflow resume must treat open post-profile `local_apply_closure` rows as a readiness barrier when LinkedIn Stage 1 is not yet checkpoint-reusable. A terminal worker without local-apply closure is not enough; resume may only re-enter acquisition after the fetched profile batch has been merged into the snapshot contract that `_restore_acquisition_state` reads.
- Post-followup workflow resume is conditional. It runs only when remote-event follow-up or the post-followup event-level drain actually claimed/completed work; otherwise the primary workflow-resume phase is the single resume owner for that tick. This prevents duplicate same-job resume attempts and keeps recovery metrics attributable to the phase that created new readiness.
- Every recovery tick must return phase-level timing and status evidence for worker recovery, local apply, event-level drain, profile refill, board-visible apply, full snapshot materialization, remote-event follow-up, and housekeeping. A long callback or phase is a contract failure to diagnose, not an opaque daemon timeout.
- Every recovery tick also has a total wall-clock budget (`RECOVERY_TICK_TOTAL_BUDGET_MS`, default `30000`). Exhausting that budget is not permission to keep draining backlog inline: the current or next phase must return `recovery_tick_budget_exhausted`, set `next_tick_requested=true`, and leave remaining durable work queued for the next tick. The budget is a fairness and circuit-breaker contract for daemon health, not a hidden retry loop. When the same report has complete phase metrics, no phase failure/slow phase/unexpected phase, no legacy bridge, and `next_tick_requested=true`, `recovery_tick_budget_exhausted` is a cooperative budget yield; service metrics expose `cooperative_budget_yield_count` and signoff records `recovery_cooperative_budget_yield`. If `next_tick_requested` is missing or the owner contract is dirty, the budget yield remains attention evidence instead of a passed gate.
- A worker-terminal handoff yield is distinct from budget exhaustion. When a tick records terminal worker evidence and enqueues durable work, `durable_work_handoff_yield=true` and `next_tick_requested=true` prove cooperative scheduling; they must not be counted as a recovery timeout or used to hide a slow phase.
- Worker recovery, local-apply closure, and board-visible apply must split work by both elapsed budget and candidate/card count. Worker recovery checks the elapsed/candidate budget between claimed workers and must not claim additional workers after a prior worker exhausts the phase budget; remaining workers stay queued for the next tick rather than holding an idle lease. Phase payloads should expose `phase_budget_ms`, `candidate_limit`, `candidate_count`, and elapsed time so pressure runs can distinguish "large durable backlog yielded correctly" from "one callback/tick monopolized the service." Leaving queued work behind after a bounded phase is the intended behavior when the tick has already made visible progress.
- `local_apply_closure` budget accounting uses the pending profile URL count when candidate ids are not available yet. A completed profile provider batch can enqueue a closure item before URL-to-candidate resolution has produced `candidate_ids`; in that state the durable unit is still the requested/fetched profile URL batch. The enqueue path must persist `profile_url_count_for_budget` / `requested_url_count` when it can infer them, and the drain must read provider completion metadata or source worker summaries before treating the item as zero-sized. `board_visible_delta_apply` remains card/candidate-count based; it must not inherit profile-URL budget semantics.
- Provider-event handoff is a control-plane step: the request records terminal evidence, releases provider/worker leases, appends the audit event, and emits one shared-daemon nudge. It does not enqueue a request-owned runner or start a resume owner. A failed or skipped best-effort nudge does not roll back that durable handoff or change its HTTP `202`; the daemon poll remains the correctness backstop. Heavy local apply, board-visible patching, full snapshot compaction, retrieval, layering, resume, and finalization remain owned by bounded worker phases.
- Smoke and pre-manual signoff must treat missing, failed, individually slow, or unexpectedly enabled recovery phases as blocking evidence. `service_metrics.recovery_phase_metrics` is the canonical report for this; `worker_recovery` raw entries are audit samples, not a second contract. `elapsed_ms` summarizes individual non-`total` phases, while `total_elapsed_ms` summarizes the aggregate tick. The aggregate `total` phase is an optimization signal (`slow_total_phase_present`) unless it is fully explained by `cooperative_budget_yield_count`; pressure scenarios can exceed the default total threshold while each bounded owner still stays within its per-phase contract. `shared_recovery_signal_count` is exactly one only when the preserved `shared_recovery_signal.status` is `signaled`; `signal_failed`, `signal_skipped`, missing, and unknown statuses fail closed to zero. This signal evidence is not recovery execution evidence and cannot replace subsequent daemon phase metrics or worker-progress observations. Legacy `recovery_count` and `recovery_dispatch_count` remain zero on this path.

Step 6: board-visible delta apply.

- Board-visible publication starts only after changed candidate rows are synchronized into the serving/control-plane representation that `/dashboard` and `/candidates` can read.
- `board_visible_delta_apply` owns retries for overlay/patch publication. A failed overlay writer leaves the item retryable; it must not require another provider webhook or profile fetch.
- A successful item writes one or more ordered patch records with frozen card-quality counters and a monotonic publication watermark. Public readers may replay or summarize patches but must not recompute old patch quality from later final snapshots.
- Patch chunking is part of the post-profile efficiency contract. A completed provider profile batch should normally become one board-visible chunk; the chunk resolver may grow toward a configured target chunk count for unusually large waves, but it must not split a provider-batch-sized wave into dozens of tiny overlay rewrites.
- Patch identity is a fixed-length idempotency key. It must not embed raw candidate-id lists; those lists belong in `candidate_ids_json` / `cumulative_candidate_ids_json` so large chunks do not exceed Postgres primary-key/index limits.
- Row-shell reuse patches are ordered cumulative projection events. They must keep the row-shell overlay stable, store the just-published batch as `current_patch_card_materialization_summary`, and store cumulative card readiness as `card_materialization_summary`. Public readers and signoff may treat a terminal profile denominator with a lower cumulative card count as a blocking projection drift.
- Pure shell rows can be published only according to the board contract. They may expose discovery/progress, but they must not count as display-ready candidate sync unless `display_ready`, `needs_profile_completion`, or `low_profile_richness` policy says the row is user-consumable.

Step 7: full snapshot materialization and compaction.

- Full snapshot work is `snapshot_full_materialization`, not part of event-level drain.
- It may coalesce completed same-kind worker groups, rebuild normalized artifacts, retrieval indexes, exports, global facet summaries, and compaction checkpoints.
- It runs under its own durable item, lease, writer budget, and runtime namespace guard. Summary fields and public reads are mirrors/diagnostics; they must not create or execute full compaction work.
- It should consume board-visible / local-apply evidence already written. It must not be the only route through which fetched profiles become visible on the board.
- Post-profile completion may enqueue this work, but a running workflow uses `status='waiting_workflow_completion'` until final workflow publication releases the same item to `queued`. Duplicate post-profile events must not demote a released `queued` item back to waiting state.

Step 8: finalization and results.

- Finalization publishes or confirms the serving projection, lifecycle row, and stable public counters for the workflow.
- A job may enter `results` while full compaction or layering tail is still catching up only if `board_runtime_state` and `result_view_lifecycle` honestly expose pending profile/card/materialization/layering state and polling continues as required.
- `serving_finalized` means the board APIs can serve the expected population and counters monotonically. It does not mean every background artifact has been compacted.
- Partial patch summaries are streaming history. Once a full current snapshot or authoritative serving projection proves final card/materialization counters, stale partial patches must not continue to dominate public `board_runtime_state`.

Step 9: post-result layering.

- Layering starts after the board serving projection is coherent enough for results, unless a workflow explicitly requires layering before results.
- Layering status is event-time owned. Public reads may render `layering_status` from lifecycle/served facet summary, but they must not write lifecycle repair/backfill rows.
- Layering completion may update board/runtime presentation and facet/layer summaries; it must not retroactively change profile fetched, local applied, or board-visible card counters.

Idempotency and recovery matrix:

- Provider completion duplicate: record late audit, no durable work replay unless owner state is non-terminal.
- URL item failed_retryable: retry only that URL after normal-wave closure and retry budget check.
- `local_apply_closure` queued/running/stale/retryable/waiting_prerequisite: recover by claiming that item, not by resubmitting provider work.
- `board_visible_delta_apply` queued/running/stale/retryable: recover by claiming that item, not by rerunning local apply unless candidate delta evidence is missing.
- `snapshot_full_materialization` queued/running/stale/retryable: recover by claiming that item, not by public-read repair.
- completed worker with `inline_incremental_apply` but no `inline_incremental_ingest`: enqueue/claim closure and replay from apply marker.
- completed worker with both markers and completed closure item: no-op except diagnostics.

Observability required before implementation sign-off:

- Per profile envelope: URL count, fetched count, retry_wait count, unrecoverable count, unresolved count, duplicate/skipped count, provider runtime, dataset download/runtime-observation lag.
- Scheduler: ready item count, normal open item count, planned_dispatch in-flight count, retry_wait blocked/allowed count, available slots, unfilled slots with ready items, batch size distribution, and small-batch reasons.
- Handoff latency: provider terminal observed -> URL states recorded, URL states recorded -> next-submit start, local delta apply complete -> board-visible patch, all profile URLs terminal -> all display-ready/terminal cards visible.
- Drain safety: callback elapsed time, local apply item claimed/completed counts, board-visible item claimed/completed counts, full snapshot materialization skipped by event-level drain, runtime namespace skipped count.
- Public contract coherence: `/progress`, `/dashboard`, `/candidates`, and `/board-patches` expose the same comparable `board_runtime_state` profile/card/finalization counters, status text, publication tier/sequence/watermark, and filter contract. They must not regress when partial patch history is older than the final serving projection. Smoke gate `require_board_runtime_state_cross_endpoint_parity` enforces this by comparing endpoints directly, not by deriving a fifth state object from workers, Stage 1 progress, or frontend cache.

Scripted smoke SLO gates:

- `max_post_profile_url_terminal_state_leak_count=0` asserts terminal URL item states leave scheduler queue states. A fetched/unrecoverable URL cannot remain in `planned_dispatch`, `deferred_*`, `dispatch_reserved`, `dispatch_claimed`, or retry-owned normal states.
- `max_profile_file_visible_to_board_patch_visible_ms` bounds completed `local_apply_closure` -> completed/published `board_visible_delta_apply` patch latency.
- `max_all_profiles_fetched_to_all_cards_visible_ms` bounds the final profile terminal timestamp -> all expected delta cards board-visible timestamp.
- `max_event_level_materialization_callback_elapsed_ms` bounds same-job event-level drain callback work. It protects the webhook/recovery callback from becoming a hidden full materialization path.

## Current Registry Implementation

当前已接入 registry 的 lane：

- `linkedin_stage_1`
- `crm_public_web_search`

代码入口：

- `src/sourcing_agent/workflow_event_response.py` 定义事件响应 lane registry。
- `src/sourcing_agent/orchestrator.py::run_worker_recovery_once` 在一次 shared recovery 后运行 bounded remote-event follow-up。
- `src/sourcing_agent/orchestrator.py::_run_remote_event_followup_rounds` 扫描带有 durable terminal provider event marker 的 remote-wait worker，并对目标 job 做 job-scoped worker follow-up daemon run；它不执行 workflow resume、completion reconcile、local apply、board-visible apply 或 full materialization。
- `src/sourcing_agent/orchestrator.py::_handle_harvest_profile_completion_event` 保持 completed Harvest profile dataset 的本地事件 contract。

当前 `linkedin_stage_1` registry entry：

- event lane: `linkedin_stage_1`
- worker lanes: `acquisition_specialist`, `enrichment_specialist`, `search_planner`, `public_media_specialist`
- recovery kinds: `harvest_company_employees`, `harvest_profile_batch`, `search_seed_discovery`
- wait stages: `waiting_remote_harvest`, `waiting_remote_search`

`linkedin_stage_1` 还定义 discovery-lane terminal registry，用于 denominator promotion：

- scoped-search / baseline+delta: `search_seed_discovery_query` durable items 全部进入 `completed|exhausted|terminal_failed|failed`，且没有 `search_seed_discovery` remote-wait worker。
- live-roster: 没有 `harvest_company_employees` remote-wait worker，且至少一个该 lane worker 写入当前 snapshot 的 `inline_incremental_apply` marker。
- 新 discovery 形态必须扩展 `src/sourcing_agent/workflow_event_response.py` 的 registry；不要在 lifecycle writer 或 public read 里新增独立分支。

当前 `crm_public_web_search` registry entry：

- event lane: `crm_public_web_search`
- worker lanes: `exploration_specialist`
- recovery kinds: `crm_public_web_search`
- wait stages: `waiting_remote_search`

`target_candidate_public_web_search` remains in the registry only as an explicit migration/legacy lane. It is not part of `DEFAULT_REMOTE_EVENT_FOLLOWUP_LANES`, and normal CRM Public Web workers must use `crm_public_web_search`.

当前行为：

- shared recovery tick 先正常 claim/execute recoverable workers。
- 如果 tick 后仍存在 registry 覆盖且带 terminal provider event marker 的 remote-wait worker，会立即做 bounded job-scoped worker follow-up。
- follow-up 只唤醒同 job 的 terminal-event worker，不扫全局 unrelated jobs，不在该 phase 内做 workflow reconcile。
- follow-up 结果合并回 `daemon` summary，并在 response 中暴露 `remote_event_followup`；被 follow-up worker callback 入队的 `local_apply_closure` 必须由后续 `post_followup_event_level_materialization_followup` 处理，再由 `post_followup_workflow_resume` / `post_followup_post_completion_reconcile` 决定是否推进 workflow finalization。
- explicit job recovery 已有自己的 follow-up rounds，不再重复走 shared remote-event follow-up。
- Apify webhook 入口 `POST /api/providers/apify/webhook` 的 HTTP 层只做 token 校验、事件规范化、terminal checkpoint 写入、匹配 lease/limiter release 和一次 shared-daemon signal；它不启动/确认 job-scoped sidecar，也不执行 recovery/materialization。`?sync=1` 已 fail-closed 退役并返回 `410 provider_webhook_sync_recovery_retired`。
- Recovery owner state machine is single-owner:
  - `webhook` / local watcher may only write terminal provider evidence, release owned leases, and emit the pure shared wake-now signal；progress polling uses its separate durable recovery-intent contract. None may forward client recovery controls or bootstrap a request-owned runner.
  - The runtime-owned recovery service is the only normal executor that claims durable worker/materialization/refill work after a wakeup. Duplicate wakeups are idempotent audit events; they are not competing executors.
  - A recovery service must run in the same `SOURCING_RUNTIME_DIR` namespace as the durable work it claims. Cross-runtime rows are skipped before claim with `runtime_namespace_mismatch`.
  - Isolated smoke/browser runtimes must stop or join provider-webhook, job-recovery, shared-recovery, hosted-runtime-watchdog, workflow-runtime-controls, and background materialization/layering threads before restoring root/local env. A timed-out smoke that leaves runtime-owned recovery sidecars alive is a failed test-environment contract, not a tolerable cleanup warning.
- Harvest actor submit 会在存在显式或默认 callback URL 时给新 run 附带 ad-hoc webhook。显式 URL 来自 request context、`SOURCING_APIFY_WEBHOOK_URL` 或 `APIFY_WEBHOOK_URL`；默认 URL 在 `production + live` 时为 `https://api.111874.xyz/api/providers/apify/webhook`，在 `local_dev + live` 时为 `https://api.111874.xyz/local-dev/providers/apify/webhook`。`scripted/simulate/replay` 不自动附加外部 Apify webhook。
- Provider webhook handling 先匹配仍可恢复的 remote-wait worker；如果本地 watcher/poll/recovery 已经先给同一个 `run_id` / `dataset_id` 写入 terminal marker，后续同源或异源事件只能记录为 duplicate audit：worker 仍在 draining 时为 `remote_provider_event: received_in_flight`，worker 已不可恢复时为 `remote_provider_event: received_late`，不能重新启动 recovery，也不能覆盖第一条 marker 的 actionable SLO 语义。这让 webhook dispatch 的审计链路不因本地 polling 抢跑而丢失，同时避免把 scripted/hybrid observer 的迟到 duplicate 误判为真实 webhook 延迟。
- 未配置 webhook 的新本地 profile actor run 会启动 local long-poll watcher。watcher 只负责把 terminal run 转成同一 `remote_provider_event`，超过 watch window 时放弃唤醒并继续由 recovery daemon 兜底；它不是 actor fail timeout。
- 已配置 `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` 时，provider webhook 仍是主完成发现入口；local watcher fallback 默认开启，用低成本 run-status probe 把 terminal actor run 转成同一 `remote_provider_event`，避免 webhook/tunnel 临时失效时退化到分钟级 recovery。严格验证外部 webhook roundtrip 时可显式设置 `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=0`。
- Search-seed scoped-search worker 现在也有明确 `recovery_kind=search_seed_discovery`。任一 query/shard 从 `waiting_remote_search` 恢复到 completed 后，会先把 entries durable apply 到 `search_seed_discovery/entries.json` 与 `candidate_documents.json`，再触发 profile prefetch opportunity；full candidate artifact / PG materialize 仍由同类 worker drain 和 writer budget 决定。
- LinkedIn Stage 1 discovery-lane terminal detection is registry-owned for both scoped-search and live-roster. Scoped-search domain proof is `job_materialization_items(item_kind='search_seed_discovery_query')`, and normal provider execution proof is the paired `workflow_commands(command_type='linkedin.discovery_query.run', owner='linkedin_acquisition_owner')`; live-roster terminal proof is `job_materialization_items(item_kind='live_roster_discovery_lane')`, written immediately after successful company-roster apply. Promotion requires no non-terminal matching discovery workers and at least one same-snapshot terminal durable proof. Worker inline apply markers are audit evidence only and must not directly promote denominators.
- Direct Harvest people-search discovery follows the same prerequisite writer contract. `search_seed_registry.persist_search_seed_snapshot(...)` projects any non-empty durable search-seed snapshot into `candidate_documents.json`; the projection preserves existing richer profile/card fields and only fills missing shell fields from search-seed entries. A direct provider-query item may not be considered a complete discovery handoff if `search_seed_discovery/entries.json` exists but the snapshot lacks `candidate_documents.json`.
- LinkedIn Stage 1 enrichment resume follows the same root/stage split. During `harvest_profile_prefetch_pending`, root `candidate_documents.json` is canonical and merge-preserving; a resumed `_enrich_profiles()` call must not replace a broader root roster/search-seed population with a smaller Stage 1 partial view. Stage archives such as `candidate_documents.linkedin_stage_1.json` may remain partial until terminal profile-prefetch proof exists. Every root write in this path must also fire the waiting-prerequisite reawaken hook.
- `local_apply_closure` items expose a `waiting_prerequisite` state for the case where a profile-batch closure tries to ingest before the search-seed/roster apply path has produced the snapshot's `candidate_documents.json`, or before that file contains rows matching the fetched profile URLs. Returning `{"status": "waiting_prerequisite", "reason": "candidate_documents_missing|candidate_documents_empty|candidate_documents_incomplete_for_profile_batch|company_identity_missing", "prerequisite_path": ..., "snapshot_id": ...}` from the materializer instead of `skipped` is the contract; the orchestrator routes that into `mark_job_materialization_item_waiting_prerequisite` (sets `status=phase=waiting_prerequisite`, decrements `attempt_count` by 1 to roll back the speculative `claim_job_materialization_item` increment, sets a bounded `not_before_at`, no `last_error` text, no retry budget burn). The primary recovery path is the prerequisite-writer event: every successful `_apply_background_search_seed_workers_to_snapshot` / `_apply_background_company_roster_workers_to_snapshot` / `_apply_background_harvest_prefetch_workers_to_snapshot` MUST call `_reawaken_waiting_prerequisite_local_apply_closure_items(job_id, snapshot_id)` to clear `not_before_at` and reset `status=queued` so the next service-loop tick claims it immediately. Direct search-seed acquisition calls the same store reawaken helper immediately after the search-seed candidate-doc projection. The bounded retry timer (default 8 s, env override `SOURCING_LOCAL_APPLY_WAITING_PREREQUISITE_DELAY_SECONDS`, hard-capped at 30 s) is a fallback safety net for crash/restart/race windows only. **Future contract rule**: any code path that writes `candidate_documents.json` outside those three apply wrappers MUST also fire the reawaken event, otherwise items strand on the bounded timer.
- Outreach layering lifecycle status is event-time owned. `_run_outreach_layering_after_acquisition` and completed-workflow outreach reconcile may write `job_result_lifecycle.outreach_layering_status`; public dashboard/progress/candidate reads only render `board_runtime_state.layering_status` from the served facet/layer summary and must not backfill the lifecycle row.
- Runtime observability follows the same boundary:
  - `remote_provider_event` job events record `source`, `remote_completed_at`, `local_event_seen_at`, and `remote_to_local_event_lag_ms` when the provider payload exposes timestamps.
  - Harvest profile completion events record `post_ingest_prefetch_candidate_count`, `post_ingest_prefetch_dispatched_url_count`, `registry_cache_marker_count`, and `post_ingest_prefetch_elapsed_ms`.
  - `src/sourcing_agent/workflow_efficiency.py` aggregates these event fields with worker state into service-level metrics for runtime reports and smoke:
    - `remote_to_local_event_lag_ms`
    - `remote_to_local_marker_lag_ms`
    - `local_completion_to_next_submit_start_ms`: pure local handoff from completion/apply marker to next-submit start; only profile-completion refill owner dispatches may produce samples
    - `next_submit_provider_attempt_elapsed_ms`: next-submit/profile-submit attempt duration; scripted provider sleep/pending and discovery/roster/search-seed append submit work can be included here
    - `local_to_next_submit_start_ms` / `next_submit_attempt_elapsed_ms`: compatibility aliases for older reports
    - `provider_slots.true_active_provider_slot_worker_count`
    - `provider_slots.remote_actor_worker_count`
    - `provider_slots.pre_submit_provider_worker_count`
    - `reconcile.same_worker_reconcile_repeat_count`
    - `reconcile.materialize_call_count`
    - `reconcile.repeated_materialize_signature_count`
  - These metrics are diagnostic only; they must not become submit/materialize gates.
- Completed-workflow background reconcile is part of the same state machine:
  - it must acquire the job-level lease before running heavy rebuild/materialize work
  - it must record `inline_incremental_ingest` markers on consumed roster/search-seed/profile workers
  - if an older job summary already proves a worker was consumed but the marker is missing, it may backfill only the marker and must not rerun apply/materialize
- Completed-workflow reconcile emits structured events for runtime/service metrics:
  - `event_family=completed_workflow_reconcile`
  - `phase=lease_acquired|lease_skipped|started|marker_backfilled|materialize_started|materialize_completed|materialize_deferred|profile_delta_serving_started|profile_delta_served|profile_delta_skipped|completed|failed`
  - `reconcile_kind=coordinator|company_roster|search_seed|harvest_prefetch|outreach_layering|exploration|snapshot_materialization`
  - stable fields include `snapshot_id`, `worker_ids`, `worker_count`, `lease_acquired`, `skip_reason`, `marker_backfill_count`, `materialize_call`, `materialize_signature`, `sync_status`, and `sync_reason`
  - event-level metrics must prefer these fields; natural-language `detail` parsing is legacy fallback only
- Running inline workflow materialization emits the same materialization phases under a
  separate event family:
  - `event_family=workflow_materialization`
  - `phase=materialize_deferred|materialize_started|materialize_completed|materialize_failed|materialize_unknown|profile_delta_served|profile_delta_deferred|profile_delta_failed`
  - `worker_kind=company_roster|search_seed|harvest_prefetch|...`
  - stable fields include `snapshot_id`, `worker_ids`, `worker_count`, `materialize_call`, `materialize_signature`, `sync_status`, `sync_reason`, and sanitized `sync_result`
- Completed-job Harvest profile tail is not allowed to block local-apply closure on full snapshot normalization when a serving board projection already exists. In that case the event-time contract is `sync_result.materialization_contract=board_visible_profile_delta`, `full_snapshot_materialization_performed=false`, `retrieval_refresh_required=false`, and `layering_refresh_required=false`. The durable board-visible patch is the user-facing completion signal; full snapshot compaction remains a separate durable materialization item. If changed candidate ids cannot be resolved, the contract is `snapshot_full_materialization_queued`: no board-visible delta is published, no synchronous full sync runs, and the durable full-snapshot item owns convergence.

## Runtime Ownership For Durable Drains

Every durable drain is event-level only inside its owning runtime namespace.

Contract:

- Worker recovery, profile refill, search-seed discovery item drain, local-apply closure item drain, board-visible apply item drain, and snapshot-full-materialization item drain must verify runtime ownership before claim/execute.
- Ownership is inferred from durable path fields (`snapshot_dir`, `root_snapshot_dir`, `candidate_documents_path`, `artifact_path`, `summary_path`, nested `artifact_paths`, etc.) against the current process `SOURCING_RUNTIME_DIR`.
- A mismatch returns `runtime_namespace_mismatch` / `runtime_namespace_skipped_count`; it must not claim the row, increment attempt counters, write retry errors, or submit providers.
- This is a correctness and cost-control boundary. A root/local-dev daemon seeing `runtime/test_env/<case>` work means schema contamination has already happened; the daemon must skip and surface the mismatch rather than “helpfully” recover it.
  - completed-job reconcile call sites must not also emit `workflow_materialization`; they already emit `completed_workflow_reconcile`, and metrics treat both families as one structured materialization source to avoid double counting
  - `sync_result.materialization_streaming` is promoted into smoke/provider-case-report rollups, so materialization budget visibility must travel with the event and not depend on a later job-summary write

默认控制：

- `WORKFLOW_REMOTE_EVENT_FOLLOWUP_ENABLED=true`
- `WORKFLOW_REMOTE_EVENT_FOLLOWUP_ROUNDS=1`
- 默认 enabled lanes: `linkedin_stage_1,crm_public_web_search`
- 可用 `WORKFLOW_REMOTE_EVENT_FOLLOWUP_LANES` 或 payload `remote_event_followup_lanes` 显式收窄/扩展 registry lanes
- payload 可传 `remote_event_followup_enabled`、`remote_event_followup_rounds`、`remote_event_followup_sleep_seconds`
- Apify API token 仍然用于调用 Apify API，例如 submit actor run、给 run 附带 ad-hoc webhook，或通过 `GET /v2/acts/:actorId/webhooks` 查询 actor webhook 列表；该 token 已来自 Harvest actor settings / `providers.local.json`。
- webhook callback token 通过 `SOURCING_PROVIDER_WEBHOOK_TOKEN` / `APIFY_WEBHOOK_TOKEN` 配置；这是本系统校验 inbound provider callback 的 shared secret，不是提交 Apify/Harvest actor run 的 Apify API token。本地调试只有在显式设置 `SOURCING_ALLOW_UNSIGNED_PROVIDER_WEBHOOKS=1` 时才允许无签名事件。
- 临时试运行默认启用 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=true`：submit path 会把 Harvest actor `api_token` 放入 webhook header，并让 inbound endpoint 接受同一 token。可设置 `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=0` 关闭；长期部署仍应替换为独立 shared secret。
- 具体 ECS / 本地 tunnel 配置、connectivity probe、one-profile live smoke 和排障步骤记录在 `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`。不要再依赖临时粘贴脚本验证 provider callback。
- Harvest run status poll 支持 `HARVEST_RUN_STATUS_WAIT_FOR_FINISH_SECONDS` / `harvest_run_status_wait_for_finish_seconds` 的 bounded long-poll；这不是 actor fail timeout，只是单次 status call 的等待窗口。
- local fallback watcher 通过 `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED`、`SOURCING_LOCAL_PROVIDER_EVENT_WATCH_MAX_SECONDS` 和 `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED` 控制；watch window 到期只停止本地等待，不会把 provider run 标记为 failed。

## Why This Fixes The Lovable Failure Mode

Lovable 的问题不是没有 roster，也不是没有 profile raw output，而是 provider completed 后本地唤醒和下游追平不够事件级。

新 contract 修正的点：

- 一批 provider run 被本地观察到后，terminal marker 和 lease release 先落 durable state，再立即唤醒共享 daemon；不需要 webhook 请求线程创建 job-scoped runner。
- next profile batch submit 仍沿用已有 lease、registry、global limiter 和 source-aware budget，不通过 full materialize 来间接触发。
- materialize 仍作为下游异步阶段追平，不再是 next submit 的默认 barrier。
- webhook response 明确记录 `mode=shared_recovery_signal`、`shared_recovery_signal_count` 和 signal 状态；后续 recovery summary 继续记录 daemon 的 claimed/executed counts，二者不混为一个请求线程执行结果。

当前边界：

- webhook 只对配置 webhook URL 后提交的新 run 生效；未配置 webhook 的既有本地 CLI run 不会被 Apify retroactively 推送。
- provider failed/timed-out/aborted event 也会唤醒同一 recovery path，由 worker checkpoint 决定 retry 粒度；URL registry 仍负责只 retry failed/unresolved URLs。
- webhook/API 线程不执行任何 recovery、full materialize 或 reconcile；它只唤醒共享 recovery daemon。下游 materialize 仍由 writer budget 和 background reconcile 追平。`?sync=1` 已退役并 fail-closed，不是调试后门。
- late webhook audit 不是新的 retry path。`received_late` 只说明 provider push 已到达但匹配 worker 已不再 recoverable；它用于可观测性和 smoke 防误判，不会重放 dataset ingest。

## Adaptive Batch And Writer Balance

LinkedIn profile actor batch size 不应只按列表长度固定分块。当前策略：

- profile URL scheduling is item-first. `linkedin_profile_registry` rows are the dedupe/retry/scheduler units; provider actors are only remote-run envelopes over ready URL items.
- `Profile Prefetch Replan Contract v1` owns normal profile URL waves. `linkedin_profile_registry` is the only scheduler source; there is no separate future-envelope table. Each discovery/probe/scale/shard append and each registry-refill wakeup calls the same packer over unsubmitted URL items scoped by `source_job + snapshot_dir`.
- Replan may rewrite only scheduler-owned, unsubmitted normal rows: empty/new registry rows, `deferred_budget`, `deferred_coalescing`, and expired/recoverable `dispatch_reserved` / `dispatch_claimed`. It must not move or resize provider-owned `planned_dispatch`, fetched, unrecoverable, or retry-owned rows, and it must not interrupt actor envelopes already submitted to the provider.
- Append-trigger replan is allowed to read `deferred_coalescing` rows with `ready_only=false`, ignoring `refill_not_before_at`, so two near-simultaneous 25-URL probe arrivals can coalesce into one normal 50-URL envelope before the timer fires. Append-trigger replan must still respect `refill_not_before_at` for `dispatch_reserved` and `dispatch_claimed`; those states are duplicate-submit guards, not coalescing timers. Daemon/timer-trigger selectors must respect `refill_not_before_at`; the timer only controls sub-50 tail flush, not append-time full-batch coalescing.
- A sub-50 normal tail is deferred as `deferred_coalescing` unless low-volume-company policy, retry isolation, or proven queue quiescence explicitly allows the small envelope. Normal queues are not considered closed while any `deferred_coalescing` row exists for the same `source_job + snapshot_dir`, even if its timer is not yet ready.
- The default tiny-tail coalescing window is intentionally short (`HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS=1000`) because it is only meant to merge near-simultaneous current/former or shard tails. It is not a UX-visible batching hold; larger windows must be an explicit operator override and should be justified against `local_completion_to_next_submit_start_ms` and `job_to_stage_1_preview` SLOs.
- The large-wave target of `8` envelopes is a replan-window target for the current unsubmitted normal set, not a workflow-wide hard cap and not a historical budget. If 400 former URLs have already been submitted and a later 1600 current-URL shard arrives, the new replan sizes the unsubmitted 1600 URLs into about 8 large envelopes. If a second append arrives while earlier rows are still unsubmitted scheduler-owned rows, that later replan may resize those remaining unsubmitted rows together with the new rows.
- `dispatch_reserved` is a short-TTL scheduler reservation written inside the `source_job + snapshot_dir` scheduler lock for active chunks selected by the current batch plan. It prevents a second process from re-planning the same active chunk while the first process walks the provider-slot and worker-begin path. It is recoverable after `refill_not_before_at`; provider limiter, submit-slot, worker-begin, or ordinal backpressure must return the URL to `deferred_budget` with the specific backpressure reason.
- `dispatch_claimed` is not a future plan state. It may be written only after provider limiter/slot acquisition has succeeded and the submit path is entering `begin_worker` / provider submit. It is short-TTL, recoverable scheduler ownership for the narrow local submit critical section. Provider limiter backpressure or worker-begin failure must leave URLs in `deferred_budget`, not `dispatch_reserved`, `dispatch_claimed`, or `planned_dispatch`.
- `planned_dispatch` is written only after the remote worker/provider envelope owns the URLs and can record owner identity (`refill_owner_worker_id`, run id, dataset id, payload hash when available). A `planned_dispatch` row without owner identity is a service violation, not a valid wait state.
- Same-plan dispatch is ordinal-gated. A `ProfilePrefetchBatchPlan` may contain several ready envelopes, but submit attempts must happen in `chunk_index` order. If chunk `N` hits provider limiter, submit-slot, or worker-begin backpressure, chunks `N+1...` must not overtake it; they are returned to `deferred_budget` with `plan_reason=prior_batch_backpressure_ordinal_gate` and `flush_reason=ordinal_submit_gate` for observability. This prevents a 24-row tail envelope from reaching the provider while the 50-row head envelope for the same wave is still waiting.
- Replan and submit-claim critical sections are single-writer for each `source_job + snapshot_dir`. Production PG uses transaction-scoped `pg_try_advisory_xact_lock(hashtext('profile_prefetch_scheduler:<source_job>:<snapshot_dir>'))`, and a busy lock is a bounded-yield signal (`profile_prefetch_scheduler_lock_busy`) rather than a blocking wait. The protected region must stay short: select/replan/claim registry rows only, no provider calls, no raw payload parsing, no materialization. SQLite fallback is in-process compatibility for tests and is not a distributed production contract.
- Normal ready/refill states (`deferred_budget`, `deferred_coalescing`, expired `dispatch_reserved`, expired `dispatch_claimed`) are always drained before `retry_wait`. Retry URLs must not steal actor slots from first-attempt ready URLs and must not cause the normal slot plan to be recomputed around them.
- Retry starts only after normal-wave closure for the same `source_job + snapshot_dir`: no current-event new URLs, no normal open scheduler rows (`deferred_budget`, `deferred_coalescing`, `dispatch_reserved`, or `dispatch_claimed`, including not-yet-ready timers/reservations/claims), and no first-attempt `planned_dispatch` registry item still waiting for item-level terminal state. `planned_dispatch` is provider-owned in-flight state; it is visible to the retry gate with `ready_only=false`, but the normal ready selector must not dispatch it as a ready item. Retry-owned `planned_dispatch` rows are marked with `profile_retry_provider_submit` / `retry_remote_provider_submitted` so they do not reopen the normal wave.
- The normal actor-slot packer uses `50` ready URLs as the minimum full actor-slot target and sub-tail boundary (`HARVEST_PROFILE_PREFETCH_ACTOR_SLOT_URL_TARGET`). Search-driven complete `51..HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS` ready sets may remain one durable provider envelope, but company-roster-heavy waves with multiple available actor slots are split into 50-target envelopes so live-roster runs do not leave ready provider capacity idle. When the ready set is larger than `400`, the packer raises batch size toward the durable actor-envelope cap (`HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS`, default `200`) instead of picking the smallest size that merely fits the wave into `8` envelopes. The `8` envelope setting remains an observability target and planning window for very large waves; the durable cap wins when provider/request payload safety requires more envelopes.
- `ProfilePrefetchBatchPlan` is the canonical packer for scoped-search, roster, registry refill, and full-roster prefetch paths. Do not add a second chunking heuristic in a caller; change the packer and update its batch-plan fields/tests instead.
- Retryable profile failures enter `refill_queue_state='retry_wait'` only when a workflow scope is known (`source_jobs` and `last_snapshot_dir`). Default retry budget is one retry (`SOURCING_LINKEDIN_PROFILE_MAX_RETRY_ATTEMPTS=1`); a second retryable failure becomes `unrecoverable` and clears scheduler state.
- Retry dispatch is isolated: a retry-only wave records `plan_reason='retry_wait_isolated_dispatch'`, `refill_policy='retry_wait_isolated_refill'`, `retry_isolation=true`, and `flush_reason='retry_isolation'`. It may be small, because it is explicitly a tail-retry envelope, not a normal discovery/profile batch.
- 同时提交的新 profile actor 数继续受 `harvest_profile_batch_submit_global_inflight` 控制，真实 provider in-flight run 继续受 `harvest_profile_actor_global_inflight` / `SOURCING_HARVEST_PROFILE_ACTOR_GLOBAL_INFLIGHT` 控制，默认不应超过 4。
- Search-seed current/former or multi-shard discovery may trigger multiple prefetch opportunities for the same job. Merged `profile_prefetch` payloads must preserve all per-event plans and queues in `profile_prefetch_events`, `batch_plans`, and `profile_prefetch_queues`; `latest_batch_plan` / `latest_profile_prefetch_queue` are only convenience fields for the last event. Smoke/signoff and efficiency review must read the arrays first, not the legacy top-level `batch_plan` or `profile_prefetch_queue`, otherwise one lane can hide another lane's plan.
- 如果 provider 返回 partial/mixed success，已 fetched URL 进入 registry fetched 状态并清空 scheduler state；unresolved/failed URL 单独进入 `retry_wait`。normal batch 的每个 URL 必须先完成 item-level 状态落账，之后 retry gate 才能放行同一 snapshot 的 retry wave。不能因为一个 URL 失败而重跑整个原始 batch，也不能把 retry URL 混回仍在执行的 normal slot plan。
- `event_level_efficiency.profile_scheduler_contract` is the delivery gate for this scheduler. The report must be present whenever provider-backed profile work exists and fails on: same-wave ordinal overtake, missing an allowed batch-size contract (`profile_actor_slot_ready_item_packing` for a fresh ready window or `profile_actor_slot_durable_wave_item_packing` when draining the same persisted wave), sub-target normal batch without allowed reason, mixed normal+retry wave, actor-slot underfill while deferred normal URLs remain, required non-PG advisory lock, ownerless `planned_dispatch`, or terminal URL row retaining scheduler queue state. Scripted smoke should declare `require_no_profile_scheduler_contract_violation=true`; Pre-Manual Signoff treats a missing or violating report as blocking.
- The contract report reads `profile_prefetch.batch_plans`, `profile_prefetch.profile_prefetch_queues`, batch envelopes, real dispatch group events (`profile_prefetch_refill_daemon_group` / `profile_prefetch_phase_b_group`), and scheduler lock evidence. `latest_batch_plan` / `profile_prefetch_queue` remain compatibility fields only; quality gates must read the arrays/canonical report so current/former or multi-shard prefetch events cannot hide each other.
- candidate artifact / PG materialization 不参与 next submit gating；它只通过 `materialization_global_writer_budget` 和 background reconcile 追平。
- 当 profile batch delta 已 apply 但同类 worker 仍未 drain 时，runtime 会先对 changed candidate IDs 做 lightweight control-plane upsert，让候选人看板可更早看到 profile detail；full artifact rebuild 仍等 writer budget/background reconcile。
- Provider completion 后的 next-submit opportunity 必须走轻量判定：对已 fetched/queued URL 使用 profile registry marker，不读取或解析 raw Harvest profile payload。raw payload parsing 属于 `local_event_apply` 或 `downstream_materialize`，不能回到 next-submit gating path。
- The registry-only marker path is shared by `queue_background_profile_prefetch(...)`; post-ingest callers pass `load_cached_profile_payloads=false`, so the optimization applies to scoped-search, roster, and mixed roster/search-seed profile tail scheduling. Initial enrichment/materialization paths still load full payloads when they need candidate detail content.
- Completed `harvest_profile_batch` workers must enqueue durable `local_apply_closure` work from the completion callback and return. The callback may only emit the next-submit/refill audit signal; actual registry scan, replan, claim, limiter acquisition, worker begin/provider submit, local-apply closure processing, and board-visible drain belong to runtime-owned recovery phases after callbacks return. This preserves callback SLOs and lets same-snapshot profile batches coalesce before Phase B prefetch and board-visible publication. Company-roster and search-seed completion callbacks follow the same enqueue-only rule.
- Remote-provider-event recovery is a shared-daemon bounded control loop. A provider terminal event only marks/releases/signals; it does not forward `WORKFLOW_REMOTE_EVENT_RECOVERY_TOTAL_LIMIT` or explicit worker ids through the wake file. The shared daemon's normal claim/tick budget bounds each pass, while remaining terminal-marked workers stay durable for immediate wake-driven or poll-backstop ticks. Heavy registry refill, local apply, and board-visible work remain daemon-owned phases.
- The scripted smoke blocked-acquisition fallback remains deliberately small-step (`total_limit=1`). It is a test-control-plane safety net, not the production remote-event wakeup path; raising it requires a separate post-profile/finalization proof that job completion cannot outrun complete board-visible or full-snapshot materialization evidence under pressure.

后续如果继续优化 board 可见速度，优先做 lightweight candidate delta PG upsert / board-state refresh，而不是把 full snapshot materialization 放回 provider submit path。

## Target Scheduler Shape: Durable Items, Adaptive Envelopes

当前仍要继续改造的核心点：workflow 不应以“worker 组”或“provider batch”作为业务边界。理想形态是 durable item queue + adaptive provider envelope。

Durable item queue:

- Search/roster 返回的每一行先成为 `discovery_item`，带 company、scope、lane、source query、provider checkpoint 和 normalized identity。
- 每个 LinkedIn URL 成为 `profile_url_item`，统一去重、状态跟踪、重试和 provider lease。
- 每个 fetched profile/candidate delta 成为 `apply_item`，可以独立写 registry、candidate delta、progress 和 board-visible patch。
- 每个 materialization 任务成为 `materialization_item`，按 changed candidate ids 或 generation checkpoint 合并，而不是默认等整个 worker group 完成后全量重建。

Adaptive provider envelope:

- Provider batch 只是把 ready items 打包成一次远端 run 的 envelope，不是 retry、dedupe、progress、materialization 的语义边界。
- Scheduler 应持续从 ready queue 填满 provider slots；只要存在可提交 URL 且没有 limiter/cost/backoff 约束，就不应因为同组 worker 未 drain 或 materialization 未完成而空窗。
- Batch packer 必须显式建模每次 remote run 的固定通信/协作成本：provider run setup、webhook、watcher、recovery wakeup、dataset retrieval、local apply、bookkeeping。批量大小应优先摊薄这些固定成本，同时用 coalescing window 控制首批结果延迟。
- Batch packer 必须记录 flush reason：`target_size_reached`、`oldest_item_wait_exceeded`、`queue_quiescent_final_tail`、`retry_isolation`、`urgent_user_visible`、`low_volume_company`、`provider_limit`。
- live profile batch 出现 `2` 或 `3` 人时默认应继续等待 coalescing window 并自动合并到其他 ready / near-ready item。只有在 queue quiescence 已证明没有可合并工作，或 retry-isolation / low-volume-company policy 明确说明不能合并时，才允许提交这种 tiny batch。
- `urgent_user_visible` 不是默认 tiny-batch 许可；它应优先加入已打开或即将打开的 envelope。只有明确记录用户延迟收益大于通信成本时，才能作为例外。
- current/former lane 继续保持业务可观测，但 profile URL ready queue 应按 normalized URL 汇合，避免一个 lane 先恢复时只提交小批，而另一个 lane 已有可提交 URL 却未被 packer 看到。

Retry / dedupe / materialization boundaries:

- retry 粒度是 URL、query shard 或 provider checkpoint；成功 URL 不随失败 batch 重试。
- dedupe 在 ingest/queue 边界持续发生，而不是等一组 worker 全部结束后统一去重。
- same-kind worker drain 只能作为 full materialization 的 coalescing 优化，不能阻塞 next-submit、local apply 或 board-visible patch。
- public progress 应显示 item-level queue/apply/materialization counters；worker count 和 batch count 只是工程诊断指标。
- candidate board 的理想 serving 单位不是完整新 snapshot，而是 baseline generation + ordered delta patches + row-level materialization state 的连续 projection。full snapshot/retrieval/index 是后台 compaction checkpoint；它不能成为用户看到新增候选人的唯一入口。

Metrics required for this shape:

- ready item count, leased item count, remote-wait item count, fetched item count, applied item count, board-visible item count
- provider slot budget, occupied slot count, idle-with-ready-items duration
- batch size distribution, coalescing wait, and small-batch reasons
- remote completed to local observed, local observed to apply complete, apply complete to next submit start
- apply complete to board visible, board visible to full materialized

## Generalization Checklist

给其他 workflow 增加事件级响应时，先补 registry 和 contract，不要直接在 orchestrator 里写 company 或 provider 特例。

- 为 workflow 定义 event lane id，例如 `crm_public_web_search` 或 `search_seed_discovery`。
- 明确 recoverable worker lanes、recovery kinds、remote wait stages。
- 确认 provider checkpoint 足够恢复，不需要重新 submit 已存在 task。
- 确认 local event apply 能写进 canonical progress 或 registry，不只写 runtime 文件。
- 确认 next submit 入口有 dedupe、lease、budget、rate limit 和 retry 粒度控制。
- 确认 materialize/reconcile 是下游阶段，并能从 event marker 或 completed worker output 继续。
- 补 regression，覆盖 provider completed 先于 materialize、next submit 不等 materialize、只 retry failed units。
- 更新 operator 文档，说明哪些字段证明 workflow 是 waiting remote、event consumed、materialize pending。

## CRM Public Web Search Notes

Public Web 这条 lane 不是 provider webhook；它现在复用相同 registry 让 DataForSEO queued task 的 `waiting_remote_search` worker 被 shared recovery 发现后，在同一 daemon-owned tick 中触发有界 same-job follow-up。W7 后 normal path 使用 `crm_public_web_search`；`target_candidate_public_web_search` 仅是 migration/legacy lane。

关键 contract：

- `POST /api/crm/records/public-web-search` 只通过 typed command owner 排队 batch/run/worker，不在 HTTP 请求内跑 DataForSEO/fetch/LLM。
- 每个 run 的 `search_checkpoint` 保存 query manifest、provider task id、poll count、query results、classified links 和 errors。
- worker 再次执行时优先 poll/fetch 既有 provider tasks，不重新 submit。
- completed run 先写 `person_public_web_assets` / `person_public_web_signals`，export 和 detail 读取 model-safe signal rows；raw HTML/PDF/search payload 仍不进默认导出。
- frontend polling 已支持按当前 CRM record ids scoped 查询，避免目标池增长后每 5 秒拉全局 500 条 run。
- 前端导出 UI 现在隐藏尚未产品化的人工确认模式切换，右侧 export 容器只保留 LinkedIn Profile export 和 Web Search export。Web Search export 使用 `promoted_and_publishable`，即人工确认信号加 AI 判定可发布信号；没有高置信信息的字段保持为空。

## Other Workflow Backlog

这些 lane 仍需按风险和用户可见 tail latency 继续推进；已接入第一层 registry 的 lane 也可能还需要拆更细的 worker/budget 阶段。

- Target-candidate Public Web Search: 第一层 registry lane 已接入；后续优化是把 DataForSEO ready poll/fetch、URL fetch、per-candidate LLM analysis 拆成更细的 worker/budget lane，避免单个候选人的慢 fetch/analysis 占住整批恢复节奏。
- Search seed discovery: scoped-search worker 已接入 LinkedIn Stage 1 registry 和 inline apply/prefetch path；后续继续补更多 provider-specific completion discovery，例如 DataForSEO/搜索 provider 的 webhook 或长轮询事件，而不是只靠 recovery tick。
- Company employees segmented roster: 单 shard completed 后应立即 merge root partial snapshot、queue profile prefetch、更新 progress，并把 full materialize 留给 writer budget。
- Public media / exploration workers: remote search/fetch completed 后应把 raw evidence 和 review-needed signals 先落 durable state，再由 AI adjudication / export materialization 异步追平。
- Plan hydration and background explain: request-signature coalescing 已有基础，但 hydration completion 还可以用同样的 event marker 接入 runtime report。

## Regression Targets

推荐最小测试矩阵：

- registry unit test: worker projection 只要能证明 remote wait stage 和 recovery kind，就能归入正确 event lane。
- orchestrator unit test: provider event 先写 terminal marker/release lease，只发送一次不带 control payload 的 shared signal；后续 shared recovery tick 才处理 LinkedIn Stage 1 remote-wait worker。
- job-scoped stale resume test: `workflow_resume_explicit_job=false` 时，`workflow_stale_scope_job_id` 仍只恢复 scoped job，不恢复 unrelated job。
- Harvest profile event test: completed dataset 先触发 next submit，再由 materialize 下游追平。
- mixed-success provider test: retry payload 只包含 unresolved URLs。
- completed reconcile test: same completed worker/event can be consumed once only; a second recovery tick must not call apply/materialize again.
- provider slot test: local profile worker must not be created as active before a real provider limiter slot is acquired.
- efficiency regression test: scripted fixtures should assert event order and counters, not only final artifacts. At minimum record remote-completed-to-local-marker lag, post-ingest next-submit elapsed time, full materialize call count, and duplicate reconcile count.
- structured reconcile regression test: completed reconcile lease skip, marker backfill, and materialize start/end must be asserted from `payload.event_family/phase/reconcile_kind`, not from `detail` text.
- scoped-search streaming E2E: baseline completed result plus out-of-order search-seed shards and Harvest profile batches must prove first completion applies locally and triggers the next submit opportunity before full materialize; same-kind siblings still in flight must produce `materialize_deferred`; final board/result-view/layering must use the same current snapshot.
- smoke/report regression test: `provider_case_report.event_level_efficiency` and runtime `event_level_efficiency` should stay populated for jobs with provider events or worker markers, so live smoke can catch efficiency regressions before manual UI testing.
- service-grade scripted metrics test: hosted/scripted smoke must keep `provider_case_report.service_metrics` populated with worker start/end/duration, global and same-lane next-worker-start gaps, frontend board-ready/non-empty timings, and bottleneck recommendations. This is the default AI-in-loop optimization surface before implementing Delta asset board streaming or broader event-level response changes.

## Testing Lessons From Meta Audio

The Meta Audio incident showed a gap in the previous scripted tests. They proved eventual recovery, URL-level dedupe, and artifact correctness, but they did not fail when the runtime did extra work before reaching the same final state.

For event-level workflows, correctness includes efficiency and state monotonicity:

- A provider completion event must have one durable local consumption marker.
- A worker that has not acquired a real provider limiter lease must not consume provider slot / active actor budget. A short `submitting_remote_harvest` phase may hold a limiter lease before `run_id` is known, but it must be distinct from a phantom worker with no lease.
- A completed job reconcile must be job-lease protected so concurrent recovery paths cannot repeat full rebuilds.
- Materialization may be slow, but it must be counted as downstream work and must not be the hidden trigger for next submit.
- Scripted tests should include negative assertions such as "apply was not called again", "materialize was deferred", and "no worker was created while provider slot was unavailable".
- Runtime/service review should treat repeated reconcile/materialize, missing local markers, and phantom active workers as state-machine bugs even if final artifacts eventually converge.

## Testing Lessons From Google/Gemini

The Google/Gemini incident showed that a single-callback ordering assertion is not sufficient for event-level orchestration. The old test proved `next-submit` was called before materialization inside one callback, but it did not prove the next remote completion could bypass a long-held job writer lock.

The stronger service invariant is:

- `next-submit` must run before acquiring the job writer lock.
- `local_event_apply` may parse provider output and merge candidate deltas, but once it succeeds it must immediately write `inline_incremental_ingest`.
- `downstream_materialize` must be tracked separately through structured events or `background_snapshot_materialization`; it cannot be the condition for considering a provider worker consumed.
- Completed reconcile must backfill markers from older `background_reconcile` state before handling new pending workers, otherwise sibling workers can hide already-consumed batches.
- Large snapshots must not fall back from URL miss to all-candidate profile matching; profile apply needs URL/name indexes so marker latency remains bounded.
- Terminal completed-reconcile leases may be released only when the holder is proven to be a dead local PID or the lease is expired. Unknown or remote owners must still be treated as active.
- Public-read completion promotion must observe the same ownership rule: if a workflow job has a live `workflow_job_lease`, read paths may report results-ready/progress state but must not persist `completed`. Terminal promotion belongs to the active runner/recovery owner until its lease expires, goes stale under the recovery policy, or is proven to be a dead local PID.
- Profile scheduler hot paths must trust the durable URL item owner, not rediscover ownership by scanning worker summaries. For Harvest profile prefetch, `linkedin_profile_registry.refill_queue_state` plus matching `source_job + snapshot_dir` is the authoritative submit/refill owner; worker rows are remote-run envelopes and should not be the primary way to decide whether the next batch can be submitted.
- Full-roster, scoped-search, search-seed, and live-roster profile prefetch all enter the same scheduler through `queue_background_profile_prefetch(...)`. `enrich(... full_roster_profile_prefetch=True)` is not allowed to keep its own profile batch planner or parallel submitter; it writes the baseline candidate document and then waits on the same registry-owned scheduler state as every other profile lane. If the scheduler reports cache reuse, enrichment hydrates from cached registry/snapshot payloads only and does not synchronously call the provider connector.

Regression coverage should include lock contention and hot-path scale, not only eventual output equality.
