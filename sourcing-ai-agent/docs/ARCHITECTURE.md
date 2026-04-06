# Architecture

## 1. 设计原则

- Agent-first：系统围绕 job、artifact、adapter、state 组织，而不是围绕单次脚本组织
- Auditable：每次导入、匹配、产出都能追溯到 source asset
- Replaceable providers：模型和外部数据源都通过接口接入
- Local-first MVP：先用现有 Anthropic 资产跑通链路，再接外部 API

## 2. 模块划分

### `planning.py`

- 从原始用户请求生成 `SourcingPlan`
- 决定 acquisition tasks、retrieval strategy、assumptions、open questions
- 当前也会显式生成：
  - `search_strategy`
  - retrieval `filter_layers`
- 提供 `hydrate_sourcing_plan(...)`，让 approved plan review session 可以直接进入执行态
- 当前已能推断 `categories`、`employment_statuses`、`retrieval_strategy`
- 已接入 `AcquisitionStrategyCompiler` 与 `PublicationCoveragePlanner`
- 下一阶段需要把 acquisition strategy 也显式产品化，根据意图分叉为：
  - full company roster
  - former employee roster
  - investor-firm roster
  - domain-specific expert graph / publication graph
- 还需要增加 `acquisition strategy compiler`：
  - 先根据用户意图定义目标人群边界，而不是默认扫整家公司
  - 例如 `Google Gemini pre-train researchers/engineers` 应优先编译为 `Google DeepMind / Gemini-scoped roster`
  - 在高成本 API 执行前，先输出 `target population / roster strategy / enrichment plan / cost tradeoff` 供用户确认

### `search_planning.py`

- 将用户 query 编译为具名 search bundles，而不是把低成本搜索逻辑散落在 connector 里
- 当前 bundle 类型包括：
  - `relationship_web`
  - `publication_surface`
  - `public_interviews`
  - `targeted_people_search`
- 允许把新的 sourcing 方法沉淀为 source family，例如：
  - 公开访谈
  - podcast/video surface
  - engineering blog contributor surface
- 新 source family 不应直接进入开发执行，必须先经过 source onboarding review：
  - 用户确认目标关系和覆盖边界
  - 用户确认只做 surface capture 还是做 deeper extraction
  - 用户确认成本、暂停条件和人工 review 触发点
  - 规划结果必须满足 raw asset 落盘、可审计、可扩展
- model-assisted planner 负责：
  - query bundle 设计
  - source family 补充
  - review trigger 提示
- deterministic fallback 仍保留，确保模型不可用时 workflow 不会停摆

### `agent_runtime.py`

- 作为现有 workflow engine 之上的 agent runtime 层
- 负责定义 specialist lanes、handoff trace spans、runtime session
- 当前已开始承载 autonomous worker runtime：
  - persisted worker state
  - budget payload
  - checkpoint payload
  - interrupt flag
  - same-key resume
- 当前不是“自动孵化无限子 Agent”，而是先把以下 fully agentic 所需能力落地：
  - specialist lanes
  - handoff edges
  - trace spans
  - runtime session persistence
  - worker persistence
- 当前 lane 由 plan 动态编译：
  - 默认 lane：`triage_planner / search_planner / acquisition_specialist / enrichment_specialist / retrieval_specialist / review_specialist`
  - investor / public media 场景会追加专用 lane
- 当前真正进入 autonomous worker 的 lane：
  - `search_planner`
  - `public_media_specialist`
  - `exploration_specialist`

### `worker_scheduler.py`

- 为 autonomous worker 增加 scheduler 视图，而不是只存 worker 状态
- 当前调度原则：
  - lane-aware parallelism
  - lane budget caps
  - resume-first
  - interrupted / completed-with-checkpoint worker 优先于 fresh worker
- 当前已把“谁该先恢复、每个 lane 并行多少、每个 lane 还能消耗多少 budget、当前 backlog 如何分布”抽成独立模块

### `worker_daemon.py`

- 在 scheduler 之上再补一层独立 daemon loop
- 当前职责：
  - 按 scheduler 输出的 runnable set 启动 worker
  - 执行 retry loop
  - 执行 lane budget arbitration
  - 对已完成 worker 复用持久化 output，而不是重复外部调用
- 当前已支持跨进程常驻恢复：
  - 通过 `agent_worker_runs.lease_owner / lease_expires_at` 做 SQLite lease 协调
  - 独立 daemon 进程可扫描 recoverable worker 并 claim 后恢复
  - `stale running` worker 会在恢复前被视作 `resume_from_checkpoint`

### `service_daemon.py`

- 为 `worker_daemon.py` 增加系统服务壳层
- 当前职责：
  - 单实例守护
  - 心跳 / 状态持久化
  - SIGTERM / SIGINT 优雅退出
  - systemd unit 渲染
- 这样 worker recovery 不再只是“一个长循环命令”，而是能被系统托管的稳定后台服务

### `asset_sync.py`

- 为跨设备恢复和后续云端 durable storage 提供中间层
- 当前职责：
  - 将高价值 runtime 资产打包成 portable bundle
  - 用 `bundle_manifest.json` 显式记录 bundle 内容、统计、restore 目标
  - 支持把 bundle 恢复到新的 `runtime/`
- 当前已实现：
  - `company_snapshot bundle`
  - `company_handoff bundle`
  - `sqlite_snapshot bundle`
- 这一层暂不直接绑定 OSS/S3/R2 SDK；先用文件系统 bundle 保持 provider-agnostic，后续再在其上叠加 object storage sync

### `acquisition.py`

- 执行 acquisition tasks
- 编排 company identity、roster acquisition、snapshot normalize、retrieval index prepare
- 管理本地 `company_assets` snapshot 生命周期
- 当 live roster acquisition 失败或返回空 roster 时，优先回退到最近一次成功的本地 snapshot
- 当前 runtime 已按 acquisition strategy 初步分叉：
  - `full_company_roster` -> live company roster
  - `scoped_search_roster / former_employee_search` -> low-cost search-seed acquisition
  - `investor_firm_roster` -> tiered investor-firm plan + existing investor asset normalization
- acquisition state 现在会把 `job_id / plan_payload / runtime_mode` 下传给 search/exploration worker
- investor firm workflow 当前会先生成 firm tier plan，再将 firm roster 归一化为 investor candidates

### `connectors.py`

- 管理外部 connector 账号与 provider 细节
- 当前已接入 LinkedIn `company/people` roster connector
- 当前已识别 `search/people` / `profile` / `profile detail` 多类 LinkedIn provider 能力
- 支持多账号 fallback、429 熔断、分页落盘、重复页检测
- 后续扩展 LinkedIn profile detail / The Org / Hunter / Scholar / publication-author / co-author
- 已记录后续高质量 HarvestAPI connector 策略：
  - LinkedIn profile search：按用户意图定向搜索，支持 current / past company 等过滤
  - LinkedIn company employees：高质量 roster 获取，适合小公司端到端验证
  - LinkedIn profile scraper：按 URL / public identifier 拉取 full profile，并使用 `moreProfiles` 扩展相似经历线索
- 成本约束：
  - 默认不抓 email
  - profile scraper 默认 `Full`，避免因信息不全而重复调用
  - 仅在最终验证或人工 review 通过后启用高成本调用

### `search_provider.py`

- 统一 low-cost search provider abstraction
- 当前支持：
  - `serper_google`
  - `duckduckgo_html`
  - provider chain fallback
- 统一输出 `SearchResponse`
- search raw payload 统一按 `html/json` 形态落盘，便于复用、审计和替换 provider
- 当前 DuckDuckGo 仍是 best-effort fallback，不再是业务链路直接依赖的单点

### `enrichment.py`

- 管理 roster 之后的多源 enrichment
- 当前主链路为：
  - provider-backed low-cost web LinkedIn URL search
  - LinkedIn `search/people` fallback
  - LinkedIn basic profile `GET /api/profile`
  - LinkedIn full profile detail `GET /people/profile` / `GET /profile/detail`
  - arXiv affiliation publication scan
  - acknowledgement name extraction
  - co-author edge generation
- 支持跨 snapshot 复用 search/basic profile 缓存，降低重复运行时的 quota 压力
- 若 acquisition 已经拿到 slug / LinkedIn URL，则 enrichment 会先直连 profile detail，不会先打 paid people search
- publication / acknowledgement 新发现的 lead 会进入 second-pass profile acquisition，而不是停留在 lead 文档层
- 新增 `further exploration` 子链路：
  - 对 unresolved lead 或缺少 LinkedIn 的候选人，抓取 web page / X / GitHub / personal site / CV
  - 先补 relation evidence 和 profile URL，再决定是否回到 LinkedIn profile acquisition
- exploration 中的页面分析已改成 model-agnostic 接口：
  - 调用 `model_client.analyze_page_asset(...)`
  - 当前提供 deterministic fallback 与 Qwen adapter
  - 后续可直接并列接 Claude 等 provider，而不改业务链路
- publication / acknowledgement / co-author 当前是 deterministic connector，不依赖 LLM 搜索
- 后续可增加 model-assisted search planner，但保留 deterministic fallback，避免模型不可用时阻断 enrichment
- 针对真实复杂任务，publication enrichment 需要升级为两层：
  - `coverage planning`
    - 由 LLM 先推断应该覆盖哪些 source families
    - 例如 official research / engineering / product blog / docs / arXiv / OpenReview / GitHub / press / acknowledgements
  - `evidence extraction`
    - 能结构化解析的 source 优先走 deterministic parser
    - 纯文本 author / acknowledgement / contributor 段落再交给 LLM 提取与归一化
    - 抽取后做 name canonicalization、dedupe、cross-source validation 与 confidence scoring
- 目标不是“让 LLM 直接替代 connector”，而是让 LLM 负责 coverage completeness 与弱结构化抽取，deterministic connector 负责稳定执行与成本控制

### `asset_catalog.py`

- 自动发现解压后的资产目录
- 找到工作簿、JSON、Skill 文档等路径

### `xlsx_reader.py`

- 用 Python 标准库解析 `.xlsx`
- 避免引入 `openpyxl` 作为 MVP 阻塞点

### `ingestion.py`

- 从工作簿和 JSON 导入候选人
- 生成统一 candidate / evidence 记录

### `storage.py`

- SQLite schema
- candidate / evidence / job / job_result 持久化
- 支持按 `target_company` 替换单家公司 snapshot，而不覆盖其他公司数据
- 已新增 criteria feedback / criteria pattern 持久化
- 已新增 `criteria_versions / criteria_compiler_runs`
- `criteria_versions` 已记录 `parent_version_id / trigger_feedback_id / evolution_stage`
- `job_results` 已新增 `confidence_label / confidence_score / confidence_reason`
- 已新增 `confidence_policy_runs`
- 已新增 `confidence_policy_controls`
- 已新增 `plan_review_sessions`
- 已新增 `manual_review_items`
- `confidence_policy_controls` 支持：
  - `request_exact / request_family / company` 三种 scope
  - `freeze` 与 `override` 两种人工控制模式
  - active/inactive 生命周期与 reviewer/notes 审计字段
- 当前 alias pattern 已可回流到 retrieval scoring
- feedback 持久化现会自动补齐 `request_signature / request_family_signature`
- 已新增 `criteria_pattern_suggestions`，作为 feedback-derived 的建议层
- `criteria_pattern_suggestions` 已支持 review loop 与 active pattern apply
- `plan_review_sessions` 当前承载：
  - plan review gate
  - review decision
  - approved plan payload
- `manual_review_items` 当前承载：
  - lead_only candidate
  - missing LinkedIn profile
  - unresolved membership / needs_human_validation
- 已新增 `agent_worker_runs`
  - 持久化 worker 的 `budget_json / checkpoint_json / output_json`
  - 支持 interrupt flag
  - 通过 `job_id + lane_id + worker_key` 实现 checkpointed resume

### `scoring.py`

- 按 criteria 做过滤、打分、解释
- 当前已输出并持久化 `high / medium / lead_only` 三档 confidence label
- pattern-level confidence boost / penalty 与 company-level threshold policy 会共同决定最终 confidence band
- confidence policy 会优先按 request-family 选择相关 feedback，并对旧 feedback 做时间衰减
- retrieval 会在自动 policy 之上再应用 active control：
  - precedence 为 `request_exact -> request_family -> company`
  - `freeze` 会复用冻结时的 band
  - `override` 会直接替换 band 阈值
- 当前用规则引擎，后续可由 LLM 接管 criteria compile 和 rerank

### `semantic_retrieval.py`

- 在 structured hard filters 之后，补一层本地 sparse-vector semantic retrieval
- 当前实现方式：
  - 将 candidate 的 `role / team / focus_areas / education / work_history / notes` 组织成 multi-field semantic document
  - 对 query term 做扩展、field weighting 和稀疏向量相似度计算
  - 为 `hybrid / semantic` 结果提供 corner-case recall 与 rerank
- 这不是最终的 dense embedding / vector DB 方案，但已经让 semantic retrieval 真正进入执行链，而不是只停留在接口预留

### `semantic_provider.py`

- 作为外部语义能力的通用 provider 层
- 设计目标：
  - 不把 retrieval 绑定死在 Qwen
  - 允许后续切换到 Claude、OpenAI、其他 embedding/rerank 服务
- 当前默认 provider 组合：
  - embedding：`text-embedding-v4`
  - rerank：`gte-rerank-v2`
  - multimodal/media rerank 预留：`qwen3-vl-rerank`
- 运行策略：
  - 优先走本地 sparse retrieval 做 candidate preselect
  - 再对候选子集调用外部 embedding / rerank
  - provider 不可用时退回本地 sparse retrieval

### `orchestrator.py`

- 编排 bootstrap、plan、sync retrieval、async workflow
- 管理 artifact 落盘与 job 状态
- workflow 启动前会先检查 `Plan Review Gate`
- 若 plan 仍需确认，则 workflow 返回 `needs_plan_review`，而不是直接执行
- feedback 写入后会自动触发 `criteria auto-recompile`
- 若 feedback 携带 `job_id + candidate_id`，会基于 `matched_fields + candidate context` 自动生成 pattern suggestions
- suggestion review 支持 `apply / reject`，其中 `apply` 会把 suggestion 写入 `criteria_patterns` 并继续触发 recompile/rerun
- 通过已有 request + 最新 active patterns 生成新的 criteria version / compiler run
- 若用户开启 `rerun_retrieval`，会继续执行一次 retrieval rerun，并与 baseline job 生成结构化 result diff
- retrieval / rerun 期间会按历史 feedback 统计生成 company-level confidence policy，并持久化当次阈值
- 已新增 `configure_confidence_policy(...)`
  - `freeze_current`: 冻结当前自动 policy
  - `override`: 手动设置 band
  - `clear`: 清除 active control
  - API / CLI 均可调用
- 已新增：
  - `review_plan_session(...)`
  - `list_plan_review_sessions(...)`
  - `list_manual_review_items(...)`
  - `review_manual_review_item(...)`

### `api.py`

- 提供标准 HTTP 接口，供 Agent 或前端调用
- 当前新增 autonomous worker 控制面：
  - `GET /api/jobs/{job_id}/workers`
  - `GET /api/jobs/{job_id}/scheduler`
  - `POST /api/workers/interrupt`

### `model_provider.py`

- 抽象 LLM 能力边界
- 当前支持 deterministic fallback 和 Qwen Responses API
- 页面分析能力已抽象为通用 `analyze_page_asset`，不是 Qwen-only 命名
- 后续替换或并列支持 Claude Code adapter

### `exploratory_enrichment.py`

- 用于处理未被 roster 覆盖、也没有直接 LinkedIn profile 的候选人
- 通过低成本 web exploration 获取：
  - X bio
  - GitHub / personal site
  - CV / Resume
  - 页面中的 LinkedIn / social links
- 原始搜索页、原始详情页、analysis input、analysis output 都会落盘
- 模型只读取 compact excerpt，不直接吃 raw HTML / raw payload
- `.pdf` 等 binary-like URL 默认不进入页面分析上下文
- 为人工 review 和 second-pass profile enrichment 提供补充证据

### `asset_logger.py`

- 作为共享的中心化资产落盘入口
- 在 snapshot 目录下维护统一 `asset_registry.json`
- 记录资产路径、类型、来源、大小、是否 raw、是否 model-safe
- 避免 connector 直接 `curl` 后只在内存里消费、没有资产沉淀

### `result_diff.py`

- 比较 baseline retrieval 与 rerun retrieval
- 当前输出：
  - `rule_changes`
  - `result_changes`
  - `impact_explanations`
  - `candidate_impacts`
- 已能解释“哪条规则让哪个候选人进入、退出、降级或升级”

## 3. 数据流

```text
User Request
  -> Planning
  -> Acquisition Tasks
  -> Company Asset Snapshot
  -> Candidate/Evidence Snapshot
  -> Retrieval Strategy
  -> Ranking + Summarization
  -> Job Results + JSON Artifact
  -> HTTP API / CLI
```

## 4. 统一候选人模型

系统将不同来源的人统一成单一 candidate schema，至少覆盖：

- `category`: `employee` / `former_employee` / `investor` / `lead`
- `target_company`
- `organization`
- `role`
- `employment_status`
- `focus_areas`
- `education`
- `work_history`
- `notes`
- `linkedin_url`

这样未来无论是员工扫描、投资方网络还是其他人群，都能进入同一条 job 链路。

## 5. 外部资源映射

### 当前已接入

- Anthropic 主工作簿
- `scholar_scan_results.json`
- `investor_chinese_members_final.json`
- LinkedIn `company/people`（live roster baseline）
- LinkedIn `search/people -> /api/profile -> profile detail`
- publication author / acknowledgement / co-author baseline

### 下一步要接的 adapter

- The Org org-chart
- Hunter domain search
- Web search / Scholar / GitHub / X
- 更高召回率的 slug discovery / company identity resolver
- HarvestAPI LinkedIn profile search / company employees / profile scraper
- source coverage planner / extraction validator

### HarvestAPI 成本路由策略

- `linkedin-profile-scraper`
  - 当已知 LinkedIn URL / public identifier 时，作为默认首选
  - 默认 `Full`
  - 默认不抓 email
- `linkedin-company-employees`
  - 只在“需要批量获取某公司几十人以上候选池”时启用
  - 不用于单人查找，避免固定启动成本被摊薄失败
- `linkedin-profile-search`
  - 只在 low-cost web search 与已有 source 不足时启用
  - 应设置最小有效批量，避免为 1-2 个结果支付整页成本

## 6. Retrieval 策略

### Structured

- 用显式字段和关键词做过滤
- 适合确定性较强的 criteria

### Hybrid

- 先 structured 缩小搜索空间
- 再用 semantic matching / model rerank 处理 corner cases

### Semantic

- 对候选人 profile document 做语义匹配
- 后续可替换为 embedding + vector store

### 下一阶段的多层过滤目标

- Layer 1: roster inclusion
  - 先定义“谁应该进入全量资产池”，例如公司成员、离职员工、投资方成员、publication lead
- Layer 2: hard filters
  - 类别、在离职状态、组织关系、必须包含 / 必须排除条件
- Layer 3: lexical / alias match
  - 关键词别名、团队 / 方向 / 岗位近义词
- Layer 4: semantic rerank
  - 处理 Post-train、multimodal、research taste 等不易结构化的 criteria
- Layer 5: confidence banding
  - `high confidence` / `medium confidence` / `lead only`

### 用户可见的检索选项

- 默认：`hybrid`
  - structured hard filters + lexical / alias match + semantic rerank
- 可选：`structured_only`
  - 只返回确定性命中，适合高 precision 任务
- 可选：`semantic_heavy`
  - 放宽硬规则，扩大召回，适合探索性任务
- 可选输出：
  - 只看 `high confidence`
  - 查看 `high + medium confidence`
  - 附带 `lead only` 候选作为待人工 review 池

这些选项本质上是呈现与排序策略，不是 acquisition 方式本身，因此默认应该尽量给出合理结果，同时保留少量用户可调参数，而不是把全部复杂度暴露给用户。

## 7. 阶段门设计

历史 Skill 中已经明确存在“调用高成本 API 前需人工确认”的阶段门。产品化时应保留：

- `discovered`
- `needs_review`
- `approved_for_enrichment`
- `enriched`
- `published`

本次 MVP 先不做完整状态机，但 job schema 和存储层已为此预留。

## 8. 存储策略

- 当前：执行日志、raw page、snapshot manifest、SQLite retrieval store 全部落在本地 runtime
- 当前 raw-first / compact-context 规则：
  - 外部 API 返回默认先落盘，再做解析或模型分析
  - 模型消费 `analysis_input` 这类压缩资产，而不是直接读取完整 raw payload
  - 大体积或二进制资产先保存，不直接进入 LLM 上下文
  - snapshot 资产统一通过中心化 asset logger 记入 `asset_registry.json`
- 推荐未来形态：
  - 本地保留 workflow 执行态、临时缓存、调试日志
  - 云端持久化高价值 LinkedIn Profile 原始响应、归一化 profile 文档、最终 result artifact
  - 向量索引或检索索引可放托管数据库或专用向量库
