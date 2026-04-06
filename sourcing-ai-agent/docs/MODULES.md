# Modules

## Goal

这份文档用于快速复盘当前后端各模块的职责、上下游关系，以及在 Thinking Machines Lab 正式测试前已经完成的优化。

## Core Flow

```text
User Request
  -> planning.py
  -> plan_review.py
  -> acquisition_strategy.py / publication_planning.py / search_planning.py
  -> agent_runtime.py
  -> worker_scheduler.py
  -> worker_daemon.py
  -> service_daemon.py
  -> acquisition.py
  -> connectors.py / seed_discovery.py / harvest_connectors.py
  -> enrichment.py / exploratory_enrichment.py / manual_review_resolution.py / web_fetch.py
  -> scoring.py / semantic_retrieval.py / semantic_provider.py / confidence_policy.py / manual_review.py
  -> result_diff.py / criteria_evolution.py / pattern_suggestions.py
  -> storage.py
  -> api.py / cli.py
```

## Module Responsibilities

### `planning.py`

- 将原始用户请求编译成 `JobRequest + SourcingPlan`
- 输出 acquisition tasks、retrieval strategy、open questions
- 调用 `AcquisitionStrategyCompiler` 和 `PublicationCoveragePlanner`
- 当前也会输出 `search_strategy` 和 retrieval `filter_layers`

### `search_planning.py`

- 将用户 query 编译成具名 search query bundles
- 当前标准 bundle 包括：
  - `relationship_web`
  - `publication_surface`
  - `public_interviews`
  - `targeted_people_search`
- 支持 model-assisted search planning，但保留 deterministic fallback
- 让新 sourcing 方法可以先作为 source family 沉淀到 plan，而不是直接硬编码到 connector

### `agent_runtime.py`

- 为 workflow / retrieval 提供 specialist lanes 和 handoff trace spans
- 为 `search_planner / public_media_specialist / exploration_specialist` 提供 autonomous worker lifecycle
- 当前 lane 包括：
  - `triage_planner`
  - `search_planner`
  - `acquisition_specialist`
  - `enrichment_specialist`
  - `exploration_specialist`
  - `retrieval_specialist`
  - `review_specialist`
- investor / public media 场景可自动补充专门 lane
- worker 当前支持：
  - persisted `budget / checkpoint / output`
  - interrupt flag
  - 同 key resume
- 当前不是自治多 Agent swarm，而是 fully agentic runtime 的第一层执行框架

### `worker_scheduler.py`

- 为 autonomous workers 提供 lane-aware 调度规则
- 当前负责：
  - lane parallelism limit 解析
  - `fresh_start / reuse_checkpoint / resume_from_checkpoint / retry_after_failure` 分类
  - runnable worker priority 排序
  - scheduler summary 输出
- 当前主要服务于：
  - `search_planner`
  - `public_media_specialist`
  - `exploration_specialist`

### `worker_daemon.py`

- 为 autonomous worker 提供独立的 resume/retry daemon loop
- 当前负责：
  - 读取 scheduler 选出的 runnable worker
  - 执行 lane budget arbitration
  - 对 failed worker 做有限次 retry
  - 对 completed worker 复用已持久化 output
- 当前 search/public media/exploration 三类 worker 都已经通过它执行
- 当前已补跨进程恢复器：
  - 支持数据库 lease claim / renew / release
  - 支持 recoverable worker 扫描
  - 支持独立进程 daemon loop 持续恢复 stale / interrupted / failed worker

### `service_daemon.py`

- 为 worker recovery daemon 提供系统服务化壳层
- 当前负责：
  - 单实例 lock
  - 心跳 / 状态文件
  - 优雅停止
  - systemd unit 渲染与写出
- 不直接替代 `worker_daemon.py` 的恢复逻辑，而是在其外层提供长期常驻和运维控制

### `acquisition_strategy.py`

- 根据用户意图决定 `full_company_roster / scoped_search_roster / former_employee_search / investor_firm_roster`
- 产出低成本优先的 source order 和成本提醒
- 避免默认扫整家公司

### `publication_planning.py`

- 为 publication / engineering / blog / docs 等 source family 制定 coverage plan
- 定义 deterministic parser 和 model-assisted extraction 的分工

### `plan_review.py`

- 生成 `Plan Review Gate`
- 判断 workflow 是否必须先由用户确认再执行
- 支持把 review decision 写回 plan：
  - 追加 source family
  - 确认 company scope
  - 批准或禁止高成本 source

### `acquisition.py`

- 执行 acquisition task
- 管理 snapshot 生命周期和回退逻辑
- live connector 失败时回退到最近一次成功的本地 snapshot
- investor 场景下会先生成 tiered firm plan，再归一化 investor roster

### `connectors.py`

- 公司 identity resolve
- LinkedIn company roster / people search / profile detail 的基础 connector
- 多账号 fallback、分页落盘、429 熔断

### `seed_discovery.py`

- 用低成本 web search 发现关系和 LinkedIn URL
- 为 `scoped_search_roster / former_employee_search` 提供候选 seed
- 现在会执行 search planner 产出的 query bundles
- `public_interviews / publication_and_blog` 这类 bundle 可以先产出 `public_media_lead`
- 公开访谈结果会先落 `public_media_results / public_media_analysis`
- 当前只基于标题 / 摘要做轻量关系判断，不主动抓 transcript
- query bundle 执行已支持并行 worker、budget 和 checkpoint
- `search_planner` 与 `public_media_specialist` 之间已能显式 handoff
- query bundle 当前已统一接入 autonomous daemon loop，而不是模块内各自重试
- low-cost search 现统一走 `search_provider.py`，而不是模块内直连某个搜索引擎

### `search_provider.py`

- 稳定的 search provider abstraction
- 当前支持：
  - `serper_google`
  - `duckduckgo_html`
  - provider chain fallback
- 统一输出 `SearchResponse`
- 统一承载：
  - raw payload roundtrip
  - html/json provider 兼容
  - 低成本 search 的替换能力

### `harvest_connectors.py`

- 对接高质量 HarvestAPI provider
- 当前主要承载已知 LinkedIn URL 时的 full profile scrape
- 默认不抓 email，默认 `Full`

### `enrichment.py`

- 对 roster candidate 做 slug resolution、profile enrichment、publication baseline enrichment
- publication / acknowledgement / co-author 新发现的 lead 会进入 second-pass profile acquisition

### `exploratory_enrichment.py`

- 处理 roster 未覆盖、且没有公开 LinkedIn 的 unresolved lead
- 从 web/X/GitHub/personal site/CV 中补 relation evidence、bio、social links、profile URL
- 页面分析走 model-agnostic `analyze_page_asset`
- low-cost exploration search 已统一走 `search_provider.py`
- 候选人 exploration 已支持并行 worker
- 每个 candidate worker 会持久化 `completed_queries` checkpoint，并响应 interrupt
- exploration 现在也统一通过 daemon loop 执行 retry / resume / budget arbitration

### `manual_review_resolution.py`

- 将人工 review 的链接、批注和 candidate patch 正式写回数据资产
- 支持两种入口：
  - 绑定 existing `manual_review_item`
  - ad hoc manual resolution，不依赖 queue item
- 当前会：
  - 在 `runtime/manual_review_assets/...` 下创建独立 artifact root
  - 落盘 `resolution_input / source manifest / fetched html / analysis input-output`
  - upsert candidate 与 evidence
  - 把 manual review links / signals 反写到 candidate metadata

### `web_fetch.py`

- 抽象低成本网页抓取
- 当前承载：
  - text page fetch
  - DuckDuckGo HTML fallback fetch
- 目标是把“低成本 web relation check”从具体搜索引擎解耦
- 当前真正的 provider 选择逻辑已经上移到 `search_provider.py`

### `asset_logger.py`

- 所有 connector 的中心化资产落盘入口
- 维护 snapshot 下的 `asset_registry.json`
- 保证 raw-first、compact-context 规则能真正执行

### `scoring.py`

- structured filter、alias matching、confidence scoring
- 输出 `high / medium / lead_only`
- pattern-level boost / penalty 和 confidence policy band 共同决定最终置信度
- 已与 semantic hit 融合打分，不再只接受纯 lexical match

### `semantic_retrieval.py`

- 基于 candidate multi-field document 做本地 sparse-vector semantic retrieval
- 在 structured hard filters 后，为 `hybrid / semantic` 检索补 recall 和 rerank
- 当前通过 term weighting + cosine similarity 为 corner cases 提供 semantic hit

### `semantic_provider.py`

- 抽象外部 embedding / rerank 能力
- 当前 provider：
  - `LocalSemanticProvider`
  - `DashScopeSemanticProvider`
- 默认使用本地 fallback；启用后可切换到 `text-embedding-v4 + gte-rerank-v2`
- 同时预留 `qwen3-vl-rerank` 给 public media / multimodal rerank

### `confidence_policy.py`

- 基于 feedback 构建自动 confidence band
- 支持 request-family 优先和 feedback time decay
- 支持在自动 policy 之上应用 manual `freeze / override`

### `manual_review.py`

- 从 retrieval 结果里抽取需要人工确认的候选人
- 当前会优先抓：
  - `lead_only`
  - `missing_primary_profile`
  - `needs_human_validation`

### `request_matching.py`

- 生成 `request_signature / request_family_signature`
- baseline job 选择、feedback family relevance、policy scope 匹配都依赖这里

### `rerun_policy.py`

- 控制 feedback 后是否 rerun，以及 `auto / cheap / full` 三档策略
- 避免无意义 rerun 和重复模型成本

### `criteria_evolution.py`

- 将 feedback 编译为新的 criteria version / compiler run
- 记录 lineage：`parent_version_id / trigger_feedback_id / evolution_stage`

### `pattern_suggestions.py`

- 从 `job_id + candidate_id + matched_fields` 自动归纳 pattern suggestion
- 只进入建议层，不直接污染 active pattern

### `result_diff.py`

- 生成双层 diff：
  - `rule_changes`
  - `result_changes`
- 生成 `impact_explanations` 和 `candidate_impacts`
- 回答“哪条规则让哪个候选人变了”

### `storage.py`

- 持久化 candidates、evidence、jobs、job_results
- 持久化 criteria evolution、pattern suggestions、result diffs
- 持久化 `confidence_policy_runs` 和 `confidence_policy_controls`
- 持久化 `agent_runtime_sessions / agent_trace_spans / agent_worker_runs`

### `orchestrator.py`

- 统一编排 bootstrap、plan、workflow、retrieval、feedback、rerun
- 负责 suggestion review loop 和 confidence policy control API
- retrieval 时应用 active control，并把 control 信息写入 artifact

### `api.py`

- HTTP 接口入口
- 当前已提供：
  - workflow / jobs
  - job trace
  - job workers
  - job scheduler
  - recoverable workers
  - daemon status
  - worker interrupt
  - worker daemon run-once
  - worker daemon systemd-unit write
  - criteria feedback / recompile
  - suggestion review
  - confidence policy configure

### `cli.py`

- 本地开发和调试入口
- 当前支持：
  - `start-workflow`
  - `show-workers`
  - `show-recoverable-workers`
  - `show-daemon-status`
  - `interrupt-worker`
  - `run-worker-daemon-once`
  - `run-worker-daemon`
  - `run-worker-daemon-service`
  - `write-worker-daemon-systemd-unit`
  - `record-feedback`
  - `review-suggestion`
  - `configure-confidence-policy`
  - `show-criteria`

## Current Control Layer

### Suggestion Review Loop

```text
feedback
  -> pattern suggestion
  -> review(applied/rejected)
  -> active pattern update
  -> recompile
  -> optional rerun
```

### Confidence Policy Control

```text
historical feedback
  -> auto confidence policy
  -> optional manual control(freeze/override)
  -> retrieval confidence labels
```

control precedence:

- `request_exact`
- `request_family`
- `company`

supported actions:

- `freeze_current`
- `override`
- `clear`

### Plan Review Gate

```text
draft plan
  -> plan review gate
  -> user confirms scope/source/cost
  -> approved plan enters workflow execution
```

### Manual Review Queue

```text
retrieval result
  -> unresolved / lead_only / missing_profile candidate
  -> manual_review_item
  -> human resolve / dismiss / escalate
```

## Data Rules

- raw-first: 外部返回结果先落盘再消费
- compact-context: 模型读 excerpt，不直接读 raw HTML/PDF/大 payload
- auditable: 所有重要步骤保留 request、plan、events、artifact、policy、diff

## Ready Before Thinking Machines Lab

当前已经具备：

- intent-driven planning
- LLM-driven search planning
- strategy-aware acquisition runtime
- low-cost-first seed discovery
- profile enrichment + exploratory enrichment
- sparse-vector semantic retrieval
- agent runtime + trace spans
- auditable criteria evolution
- rerun gating + cost policy
- request-family baseline matching
- confidence evolution + manual freeze/override

仍建议在正式测试前优先关注：

- HarvestAPI live connector 验证
- 把当前 sparse-vector retrieval 升级为外部 embedding / vector store
- policy freeze / override 的人工 review 规范
- Thinking Machines Lab 的 scoped roster strategy
