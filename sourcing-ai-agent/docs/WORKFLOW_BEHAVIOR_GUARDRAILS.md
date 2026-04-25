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

### 2. Default-off stages must stay off unless explicitly enabled

- `Public Web Stage 2` 当前属于 default-off capability。
- 默认 workflow 不应自动把它加入 acquisition 主链路。
- 只有显式 opt-in 的 case 才允许：
  - plan 中出现 `enrich_public_web_signals`
  - timeline 中出现 `public_web_stage_2`
  - stage2 acquisition 消耗 wall-clock
- 任何“默认又长回去”的行为都应被视为 regression。

### 3. Downstream must start immediately once prerequisites are satisfied

- 一旦 `LinkedIn Stage 1` 已经满足候选人看板所需的最小输入，下游必须立即推进：
  - `Stage 1 Preview`
  - `normalize/materialize`
  - `build_retrieval_index`
  - board-ready results
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
    - 多个同 kind completed worker 应先聚成 micro-batch
    - 每个 job 同时只允许一个 inline writer 做 shared sync/materialize
    - 同 kind 仍有 in-flight worker 时先 defer full sync，待 drain 后只做一次
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

### 6. Expensive stages should be justified by incremental value

- 任何会显著增加 wall-clock 的 stage，都必须有明确增量价值：
  - 带来了新 candidates
  - 带来了新 evidence
  - 带来了更高质量的 retrieval-ready artifacts
- 如果一个 stage 长期既不提供稳定增量、又阻塞主链路，就应：
  - default-off
  - 后台化
  - 或直接拆出主 workflow

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
- browser E2E 快套件已经覆盖：
  - async plan hydration 后的后端 plan semantics label 恢复
  - large-org existing baseline asset population 的结果恢复
  - 候选人看板后台 hydration 时保持第二页，不回跳第一页

但还没完全结束的点仍有：

- 这些 guardrail 目前主要接进 scripted smoke / targeted tests，还没有全部升到更宽的 CI gate
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

## Review rule

以后 review workflow/orchestrator/planning 相关改动时，不能只问：

- “代码能不能跑”
- “结果能不能出来”

还必须问：

- 有没有重复 dispatch
- 有没有错误打开 default-off stage
- 有没有把无关阶段错误串行到 board readiness 前面
- `Final Results` 是否真的对应可读取的持久化结果
