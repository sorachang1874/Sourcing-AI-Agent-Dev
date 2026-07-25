# Workflow Behavior Guardrails

> Status: Active contract. This document defines what the workflow is supposed to do, not just what it currently happens to do.

## Why this exists

当前测试体系已经覆盖了不少“接口可用 / 结果可返回 / 关键回归不再炸掉”的问题，但这还不够。

工作流还有一层更重要的正确性：

- 什么时候该调用外部 provider
- 哪些 stage 默认不该运行
- 上游条件满足后，下游是否立即推进
- 哪些昂贵任务必须后台化，而不能阻塞候选人看板

如果这层 contract 不显式写下来，就很容易出现：

- 代码能跑通，但重复 dispatch 同一批 Harvest 请求
- 已经明确 default-off 的 `Public Web Stage 2` 又被重新塞回主链路
- `LinkedIn Stage 1` 已经具备出板条件，但 `normalize/materialize` 仍被无关任务串行阻塞
- 前端显示 `Final Results`，但后端其实还没真正持久化 `asset_population`

这份文档就是为了防止这种“功能看起来还在，行为却已经退化”的回退。

Provider-backed workflow 的事件级响应抽象另见 `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`；涉及 remote completion discovery、local ingest、next submit、downstream materialize 的改动应同时保持两份文档一致。

## Workflow invariants

### 1. No duplicate provider dispatch for the same job-stage payload

- 对同一个 `job_id`、同一个 workflow stage、同一组 provider payload，不允许在同一轮执行窗口内重复派发。
- 允许的例外只有：
  - 明确的 retry，且 retry reason 被显式记录
  - 不同 lane 的不同 payload
  - 用户显式要求 `force_fresh_run`
- 不允许把以下情况误当作“合理重复”：
  - `start_workflow` 与 recovery/auto-takeover 竞争导致的二次派发
  - probe 与正式 dispatch 用同一 payload 各发一次
  - queued 状态下没有 in-flight guard，导致同一请求被重新接管
  - recovery/resume 与原始 provider call 跨进程竞争，导致同一 `harvest_profile_search` payload 二次派发
  - `linkedin_profile_registry.status=queued` 的 URL 被新的 background `harvest_profile_batch` 再次 submit
  - 本地 `harvest_prefetch_submit_workers` 提高后，跳过 provider-safe `harvest_profile_batch_submit_global_inflight`
- `harvest_profile_search` dispatch 必须有 durable artifact guard。进程内 single-flight 只能覆盖同进程竞争；跨进程 recovery 看到同一 raw artifact path 的 dispatch lock 时，必须等待 raw cache 或 stale-lock expiry，不能直接再次调用 provider。
- 对 Harvest profile prefetch，`queued` 不是“可以忽略的尾巴”：
  - job-local active `harvest_profile_batch` worker 必须消耗新 submit budget
  - 新 URL 超出 budget 时应进入 `deferred_urls`，由 recovery/reconcile 后续继续排队
  - 同一 URL 已在 registry queued 时，只能补 source lineage，不能创建新的 actor run
  - 如果 registry queued URL 属于当前 worker 自己，且 worker checkpoint 已有 remote `run_id/dataset_id`，必须继续 resume/poll/download 该 remote run；不能把自己的 queued registry row 当成别的活跃队列而短路
  - 新 actor submit 前必须先获取 `linkedin_profile_registry_leases` URL claim；claim contended 只能记录 `lease_contended_skip`
  - profile-scraper/company-employees actor 还必须经过 DB 级 `runtime_provider_limiter_leases`，不能只依赖进程内 semaphore
  - remote actor pending 时 provider limiter lease 必须随 checkpoint 保留，直到 worker recovery terminal 后释放
  - 已有 `run_id/dataset_id` 但没有 `remote_provider_terminal_event` marker 的 `waiting_remote_harvest` / `waiting_remote_search` worker 属于 provider-event owner；普通 worker recovery 只能 report-visible skip，不能 poll/download 该 remote run
  - submit budget 必须 source-aware：`harvest_profile_search` / former search-seed 中等批次应能按推荐窗口并行排出多个 actor，large company roster 才走更保守的 1-2 active actor tail
  - provider completion、local ingest、next-batch submit、snapshot apply/materialize 必须尽量解耦：
    - provider 侧 actor 一旦 completed，不应默认等下一轮长周期 recovery 扫描才被本地发现
    - next-batch submit 不应默认等待 current batch 的 candidate-detail materialization 完成
    - apply/materialize 应作为共享 writer budget 下的下游阶段，而不是上游 provider submit 的隐性 barrier
    - 对 medium former/search-seed 批次，正确行为应接近流水线：`completed actor -> ingest result -> continue submit deferred tail`

### 2. Default-off stages must stay off unless explicitly enabled

- `Public Web Stage 2` 当前属于 default-off capability。
- 默认 workflow 不应自动把它加入 acquisition 主链路。
- `LinkedIn Stage 1` 默认也不能把 DataForSEO/public-web search 当作 search-seed fallback；Stage 1 只能调用 LinkedIn-related providers：
  - Harvest/company employees
  - Harvest/linkedin-profile-search
  - Harvest/linkedin-profile-scraper
- `relationship_web` / `publication_surface` / `public_interviews` 这类 public-web seed bundles 只能在 `execution_preferences.allow_stage1_web_seed_fallback=true`（或 alias `allow_public_web_seed_fallback=true`）时进入 Stage 1。
- 只有显式 opt-in 的 case 才允许：
  - plan 中出现 `enrich_public_web_signals`
  - timeline 中出现 `public_web_stage_2`
  - stage2 acquisition 消耗 wall-clock
  - LinkedIn Stage 1 中出现 DataForSEO/search-seed web worker
- 任何“默认又长回去”的行为都应被视为 regression。

### 3. Downstream must start immediately once prerequisites are satisfied

- 一旦 `LinkedIn Stage 1` 已经满足候选人看板所需的最小输入，下游必须立即推进：
  - `Stage 1 Preview`
  - board-ready results
- 如果当前 snapshot 的 `candidate_documents` 已经是 final results 的权威 candidate source，`normalize/materialize` 与 `build_retrieval_index` 是后台 compaction tail：
  - 主链必须把它们记录为 acquisition skip / deferred background snapshot materialization
  - 主链不得同步等待它们完成后才发布 final results
  - `background_snapshot_materialization` / `snapshot_full_materialization` durable item 是后续恢复与运维可见性的 owner
  - 未命中缓存的 outreach layering 也必须由 `allow_background_defer` contract 默认 defer；它不能依赖额外 env 开关，不能在 normalized artifacts 缺失时同步失败，也不能阻塞 final results
  - smoke/signoff 必须把 board-ready/nonempty 与 post-result layering 分开计时；layering probe 超时不能把已发布的候选人看板误报成 `final_results_to_board_nonempty` 超时
- 如果 final results 仍不能从权威 candidate source 直接服务，才允许同步执行 `normalize/materialize` 与 `build_retrieval_index`。
- 对 `full_company_roster` 还要额外满足一条：
  - 如果 search-seed / partial candidate baseline 已经足够进入 enrichment
  - 后台 `harvest_company_employees` 可以继续跑，但不能再阻塞主链 resume
  - 更进一步，`acquire_full_roster` 本身就应直接继续主链，而不是先返回 blocked 再等待 recovery tick
- 同样地，如果 current roster 已经 ready，而 former/search-seed 只是后台 worker：
  - 也应继续主链
  - 不能再因为 former lane 还在补而把 current-roster baseline 卡住
- 对 segmented roster 还要再满足一条：
  - partial shard output 可以先恢复成当前 job 可用的 partial root snapshot
  - 但 partial roster 不能被后续新 job 当成完整 cached roster 直接 reuse
  - “当前 job 可继续” 与 “跨 job 可复用” 必须是两条不同 completeness contract
- search-seed 路径也要满足：
  - 最低要求是：
    - snapshot merge / reconcile 一旦拿到可用 `profile_url`
    - 应立即尝试 background profile prefetch
    - 不能再等到后续 enrichment stage 才第一次开始抓 profile
  - 更强的当前 contract 是：
    - 单个 search-seed query provider response 一旦返回 usable entries
    - 就应立即做 job-local dedupe 并派发新的 profile prefetch
    - 不应再等整轮 `discover()` 聚合完成
    - overlapping query results 不应在同一 job 内重复派发相同 profile URL
- company-roster / harvest-prefetch 路径也要满足：
  - completed `harvest_company_employees` worker/shard output 一旦可用：
    - 应立即 merge 回 root snapshot
    - 应立即继续 queue baseline profile prefetch
    - 不应再等下一次整轮 refresh/reconcile 才第一次生效
    - 即使这是同进程 segmented roster 的 completed local shard，只要整体 workflow 仍会 background resume，也应走同一 inline callback
  - completed `harvest_profile_batch` 一旦可用：
    - 应先 delta-merge 回 root `candidate_documents.json`
    - 应尽快把“本批完成”转成新的 submit opportunity，而不是把 submit 完全绑到本批 apply/materialize 的尾部
    - 多个同 kind completed worker 应先聚成 micro-batch
    - 每个 job 同时只允许一个 inline writer 做 shared sync/materialize
    - 同 kind 仍有 in-flight worker 时先 defer full sync，待 drain 后只做一次
    - sync/materialize 后应继续 queue next baseline profile prefetch，直到 queued/deferred URL tail 逐步 drain
    - 不应再回退到“整轮结束后再用另一套 completion manager 全量补课”
- 不允许继续等待与 board readiness 无关的任务，例如：
  - `Public Web Stage 2`
  - 非关键 public-web exploration
  - 可延后的 background reconcile
- 这条规则的核心不是“所有阶段都并行”，而是：
  - 不要让无关阶段成为错误的串行前置条件

### 4. Final Results must mean persisted board-ready state

- 一旦 timeline 显示 `Final Results`，就应满足：
  - `asset_population` 已可读取
  - 默认 board endpoint 可返回非空或明确的空结果
  - 不应只存在 preview artifact 而没有最终 `asset_population` / candidate docs
- 如果候选人看板在 `Final Results` 前已经通过 board-visible patch 非空，`final_results_to_board_nonempty_ms` 必须记为 `0`。后续 facet/layering 是否完成属于 post-result tail，不得污染 board-serving UX 指标。
- 不允许出现：
  - UI 显示 workflow completed
  - 但候选人看板仍是 `0/0`
  - 或只有 preview，没有最终 snapshot/materialized artifacts

### 5. Stage transitions and progress counters must use the right semantics

- `candidate_count`、`result_count`、`observed_company_candidate_count`、stage completion 状态应满足单调约束。
- 已经观测到的候选人数，不应在没有明确 reset/rebuild reason 的情况下回落到 0。
- `manual_review_count` 不是候选人数口径，而是待处理 backlog：
  - terminal / enrichment 后因为 profile 补全而下降是允许的
  - terminal fallback 缺字段时不能把它误清零
  - smoke report 应把它记录为 `backlog_reductions`，而不是 `counter_regressions`
- worker status 应体现真实状态，不允许：
  - worker 实际未 dispatch，却显示 completed
  - timeline 先显示完成，再回到 earlier stage 的 waiting 文案
- 候选人看板的 LinkedIn hydration 状态必须区分：
  - roster/search 已拿到 LinkedIn URL，可以展示“打开 LinkedIn”
  - profile detail 仍在 `queued/fetched/failed_retryable/deferred` 状态，需要通过 `profile_fetch_progress` 明示后台补全尾巴
- full roster 预算状态必须显式：
  - 默认 `max_pages/page_limit` 只是安全预算，不等同于“全部成员”
  - `company-employees` probe 发现总量超过默认预算时，应扩到 provider cap 内的估算量
  - provider cap 命中或请求 cap 被打满时，summary 必须写 `partial_result` / `provider_cap_hit` / `requested_limit_would_truncate`

### 6. Expensive stages should be justified by incremental value

- 任何会显著增加 wall-clock 的 stage，都必须有明确增量价值：
  - 带来了新 candidates
  - 带来了新 evidence
  - 带来了更高质量的 retrieval-ready artifacts
- 如果一个 stage 长期既不提供稳定增量、又阻塞主链路，就应：
  - default-off
  - 后台化
  - 或直接拆出主 workflow

### 7. Fail-closed gates must enumerate their exempt populations (2026-06-12)

教训来源：completion-policy 的 `serving_finalized` 证明门禁上线时只考虑了"有 durable workflow run 的任务"，导致没有 durable run 的 legacy/恢复任务永久无法 promote（`tests/test_frontend_history_recovery.py` 的 reconciliation 测试因此长期红）。同一缺陷族还包括 PG-only 路径在"行不存在"时 raise 而 SQLite fallback 静默 no-op（`update_agent_runtime_session_status`），与 ON CONFLICT 唯一索引缺口同属 SQLite/PG 语义分歧类。

新增任何 fail-closed 门禁（completion proof、lease、workspace 校验等）时必须：

- **枚举无法满足前置条件的存量人群**（legacy 任务、恢复任务、迁移前数据），并显式决定豁免或迁移路径——"永远等不到证明"不是 fail-closed，是 dead-closed。
- **豁免挂在入口点，不挂在 evaluator 全局**：豁免只允许在持有独立完成证据的入口生效（如 final-results reconciliation 持有 stage_2_final + 已持久化 results 才调用晋升），通过显式 opt-in 参数下传（`allow_legacy_run_absent_exemption`）；通用终态判定（`_job_is_terminal`）与 worker supervisor 必须保持 fail-closed——一个"UI 状态看似 completed 但无 durable run"的任务对它们永远不是终态（反伪造护栏 `test_workflow_completed_ui_status_requires_typed_completion_proof_for_terminal_paths` / `test_workflow_supervisor_does_not_exit_on_completed_ui_status_without_typed_proof`）。首次实现时豁免误加在 evaluator 全局，立即击穿了这两条护栏——这就是为什么范围必须按入口收窄。
- **区分"查找失败"与"确认不存在"**：查找失败必须保持 fail-closed；确认不存在才允许走豁免分支（参考 `_workflow_completion_policy_evaluation` 的 `workflow_run_absent` 与 `workflow_state_unavailable` 双信号）。
- **双向测试**：豁免人群能通过 + 非豁免人群仍被拦（`test_policy_exempts_serving_finalized_when_durable_run_is_absent` / `test_progress_reconciliation_requires_serving_finalized_proof_for_durable_runs`）。
- **双路径存储语义一致**：行为门禁依赖的 store 方法在 PG-only 与 SQLite fallback 下对"行不存在"必须同语义；292 个双路径方法 PG-pure 重写时逐一特征化这一点（Track B 要求）。

## What the test system should assert

### Unit / contract tests

- planner 是否错误把 default-off stage 放回默认 plan
- orchestrator 是否在 fresh dispatch 期间重复派发同一 provider 请求
- single-stage workflow 是否仍错误地产生 `public_web_stage_2` summary
- `Final Results` 是否仍可能在未 materialize 完成时提前发出

### Scripted smoke / simulate reports

- provider request signature 去重统计
  - 同一 `job_id + provider + normalized payload` 的调用次数
- stage wall-clock 与 waiting gap
  - 上游 prerequisite 满足后，到下游 stage 实际启动之间的空转时间
  - `acquire_full_roster` 已有 baseline 后，是否仍被后台 current-roster worker 错误阻塞
- disabled-stage violation
  - default-off stage 是否被意外调用
- board readiness lag
  - `LinkedIn Stage 1 completed -> Stage 1 Preview`
  - `Stage 1 Preview -> materialize start`
  - `materialize completed -> board non-empty`

### Browser / API E2E

- 候选人看板在后台 hydration 时，不应跳页、回页或刷新丢位置
- 执行过程页到候选人看板的自动切换只允许触发一次，且只能由“结果页首次可渲染”这一事件驱动；`selectedCandidateId` 变化、短暂的不可渲染态、或结果页刷新都不应再次把用户从执行过程页顶回看板
- `/dashboard`、`/progress`、`/candidates`、`/board-patches` 的公共读路径必须以持久化 lifecycle / store snapshot 为主，不得把 live `agent_runtime.list_workers()` 作为响应必需依赖；live worker 只作为控制面和可选诊断增强
- `Final Results` 出现后，board 应能稳定读取结果
- workflow progress 不应把已经出现过的候选人数回退到更小值

## Current gaps

截至目前，下面这些点已经有了第一层自动化收口：

- scripted smoke 已输出统一的 `behavior_guardrails`：
  - `duplicate_provider_dispatch`
  - `disabled_stage_violations`
  - `prerequisite_gaps`
  - `final_results_board_consistency`
- aggregate smoke summary 也会汇总：
  - duplicate signature / redundant dispatch counts
  - unexpected public web stage case count
  - prerequisite gap timing stats
  - final-results-vs-board violation counts
  - `materialization_streaming`：
    - provider response count
    - pending delta count
    - provider response 到 first materialization 的 gap
    - coalescing / writer-budget recommended action
  - `provider_backpressure`：
    - shared runtime tuning budgets for Harvest profile actor / profile batch submit / profile scrape / people search / company roster
    - DB limiter active count, wait ms, exhausted limiter keys, queued/waiting remote backlog, recommended action
  - `workflow_benchmark`：
    - search returned count
    - roster returned count
    - fetched profile count
    - profile URL queued/total
    - board ready/nonempty and board candidate counts
- browser E2E 快套件已经覆盖：
  - async plan hydration 后的后端 plan semantics label 恢复
  - large-org existing baseline asset population 的结果恢复
  - 候选人看板后台 hydration 时保持第二页，不回跳第一页

但还没完全结束的点仍有：

- 这些 guardrail 目前主要接进 scripted smoke / targeted tests，还没有全部升到更宽的 CI gate
- scripted coverage 目前也还没有完全模拟“provider 已完成，但本地 ingest/next-submit/materialize 仍彼此解耦”的真实流水线时序
- `prerequisite_gaps` 目前基于 stage timestamp contract；若 backend stage timestamp 再漂移，仍需继续补更强的 authoritative timing source
- 浏览器层已经有“hydration 不跳页 / 不回页”的基础断言；后续仍可继续扩到滚动位置和更多筛选/排序状态
- 当前 query-level incremental ingest 已经覆盖 search-seed discover / paid fallback，但主 acquisition provider
  - `linkedin-profile-search` 已经做到 query-level incremental emit
  - `linkedin-company-employees` completed worker/shard output 已接入 shared snapshot apply / inline callback
  - `harvest_profile_batch` completed output 也已接入 shared delta-merge / same-kind micro-batch sync
  - 当前剩下的不是“有没有 shared contract”，而是：
    - report-level time-window coalescing / writer-budget contract 已补
    - running job inline incremental sync 已接入 shared `materialization_writer` in-flight slot
    - live profile adapter 已支持 provider batch response callback，enrichment 已即时消费单 batch 返回
    - snapshot candidate-document sync 已接入 shared `materialization_writer` budget
    - per-job single-writer 还没再提升到跨进程 global queue/backpressure 执行器

## Required follow-up work

下一轮测试体系应继续补：

1. 为 scripted smoke 增加 provider dispatch signature report
2. 继续把 prerequisite-gap 指标接到更多 hosted/scripted case，不只停留在现有代表样本
3. 为 single-stage / two-stage / large-roster / scoped-search 统一输出 workflow behavior digest
4. 把这些 guardrail 接进 CI/high-signal smoke，而不是只写在文档里
5. 如果 provider 支持 partial dataset fetch，再把 `materialization_streaming` 从 batch callback 深化到 partial dataset callback
6. 增加 scripted scenario，明确覆盖：
   - provider actor completed 先于本地 materialize 完成
   - next batch submit 不等待 current batch materialize
   - local progress 能反映“provider 已完成但下游仍在消费”的中间态

## Review rule

以后 review workflow/orchestrator/planning 相关改动时，不能只问：

- “代码能不能跑”
- “结果能不能出来”

还必须问：

- 有没有重复 dispatch
- 有没有错误打开 default-off stage
- 有没有把无关阶段错误串行到 board readiness 前面
- `Final Results` 是否真的对应可读取的持久化结果
