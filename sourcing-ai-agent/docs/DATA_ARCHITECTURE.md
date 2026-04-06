# Data Architecture

## 1. 当前已经落地的底层资产模型

### Canonical entities

- `Candidate`
  - 当前统一承载员工、前员工、投资方成员、publication lead
  - 关键字段包括：
    - `category`
    - `target_company`
    - `employment_status`
    - `role`
    - `team`
    - `focus_areas`
    - `education`
    - `work_history`
    - `linkedin_url`
    - `metadata`
- `EvidenceRecord`
  - 每条候选人的来源证据
  - 关键字段包括：
    - `source_type`
    - `source_dataset`
    - `source_path`
    - `url`
    - `summary`
    - `metadata`
- `Job / JobEvent / JobResult`
  - 承载工作流执行状态、阶段事件、最终检索结果
- `agent_runtime_sessions / agent_trace_spans / agent_worker_runs`
  - 承载 specialist lanes、handoff trace 和 autonomous worker 状态
  - worker 关键字段当前包括：
    - `budget`
    - `checkpoint`
    - `output`
    - `interrupt_requested`
    - `lease_owner / lease_expires_at`
    - `attempt_count / last_error`
- `scheduler summary`
  - 虽然当前不单独持久化为表，但可由 `agent_worker_runs + plan cost policy` 即时计算
  - 当前会输出：
    - lane parallelism limit
    - lane budget caps
    - lane backlog summary
    - resumable worker priority list
- `runtime/services/<service_name>/status.json`
  - 记录系统级 daemon 的心跳与最近一次恢复摘要
  - 用于 `show-daemon-status` 与系统运维观测

### Planning entities

- `JobRequest`
  - 用户 query、criteria、预算参数、retrieval strategy
- `AcquisitionStrategyPlan`
  - 定义应该拿哪一池人、使用哪些 source、低成本和高成本调用顺序、成本阈值
- `PublicationCoveragePlan`
  - 定义 publication / engineering / blog / docs 的 coverage family、query hint 和 extraction strategy
- `criteria_feedback / criteria_patterns`
  - 保存人工 review 对 criteria 的修正
  - 当前支持 alias / include / exclude 类 pattern 持久化
- `plan_review_sessions`
  - 保存 plan review gate、review decision、approved plan payload
- `manual_review_items`
  - 保存最终结果里需要人工进一步核验的 corner case
- `criteria_versions / criteria_compiler_runs`
  - 保存每次 criteria 编译输入、输出、provider、版本签名
  - 用于审计、回放与后续 criteria evolution
  - 当前还会保存 `parent_version_id / trigger_feedback_id / evolution_stage`
- `criteria_result_diffs`
  - 保存 feedback 后 rerun retrieval 相对 baseline retrieval 的差异摘要与详情
- `confidence_policy_runs / confidence_policy_controls`
  - `confidence_policy_runs` 保存每次 retrieval / rerun 实际生效的 confidence policy
  - `confidence_policy_controls` 保存人工 `freeze / override` 控制
  - control 支持 `request_exact / request_family / company` 三种作用域

## 2. 当前 roster 级数据资产

系统现在支持 3 类底层候选池：

- `local bootstrap asset`
  - 例如 Anthropic 历史工作簿、Scholar 资产、投资机构成员
- `company roster snapshot`
  - 例如 LinkedIn company people
  - 存放在 `runtime/company_assets/{company}/{snapshot_id}/`
- `investor firm snapshot`
  - 保存 tiered firm plan、firm roster candidates、candidate documents
- `search-seed snapshot`
  - 用于 scoped roster / former employee 任务
  - 先通过 provider-backed low-cost web search 获取关系和 LinkedIn URL，再进入 profile enrichment
  - search raw payload 会按 `html/json` 形态落盘到 asset registry
- `exploration snapshot`
  - 用于 unresolved lead 的进一步探索
  - 保存网页、X、GitHub、personal site、CV 等信号
- `public media assets`
  - 保存 YouTube / Podcast / interview surface 的标题/摘要级结果
  - 当前资产类型包括 `public_media_results / public_media_analysis`

这些 snapshot 会继续归一化成统一的 `candidate_documents.json`、SQLite `candidates` 和 `evidence`。

## 3. 表达用户意图与 Sourcing Criteria 的方式

当前表达链路已经有 3 层：

- `request`
  - 保存用户原始自然语言和显式 filters
- `plan`
  - 输出 acquisition strategy、publication coverage、retrieval strategy、open questions
- `retrieval`
  - 将 criteria 映射到 hard filters、lexical matching、future semantic rerank

在此之上，当前又新增了两层产品对象：

- `plan_review`
  - 确认 acquisition boundary、source family、成本授权
- `manual_review_queue`
  - 收集 unresolved candidate、missing profile、lead_only candidate

当前已经能表达：

- 公司边界
- current / former / investor
- 关键词、must-have、exclude
- acquisition source order
- publication coverage family
- criteria version signature
- compiler provider / compiler kind
- confidence label / score / reason

尚未完全结构化的部分：

- user-approved acquisition boundary history
- false positive / false negative 的自动归纳闭环
- criteria auto-evolution trace 的自动重编译部分

## 4. 当前可审计性

当前系统已经具备基础审计链路：

- 每次 workflow 都保存 `request_json`、`plan_json`
- 每个阶段都有 `job_events`
- 原始 raw page / API response 会落在 snapshot 目录
- snapshot 下统一维护 `asset_registry.json`
- exploration 中还会额外保存 `analysis_input.json` 与 `analysis.json`
- 每条 `Candidate` 都保留 `source_dataset`、`source_path`
- 每条 `EvidenceRecord` 都可追溯到来源文件或 URL
- `manifest.json` 会记录 snapshot 级统计和存储位置
- `criteria_versions / criteria_compiler_runs` 可回放当时的 criteria 编译上下文
- `asset_registry.json` 可回答“这个资产是否落盘、是什么类型、是否允许直接进入模型上下文”
- `criteria_result_diffs` 可回答“feedback 后结果到底变了什么”
- `plan_review_sessions` 可回答“这个 workflow 是否经过用户确认，用户加了哪些 source family，是否批准高成本调用”
- `manual_review_items` 可回答“哪些人还不能自动确认，为什么需要人工补证据”
- `agent_worker_runs` 可回答“哪个 specialist worker 跑到了哪一步、预算是什么、是否被打断、下次该从哪里恢复”

当前还明确执行一条 raw-first / compact-context 规则：

- 原始 API / 网页内容先落盘为数据资产
- 原始资产通过中心化 asset logger 统一登记
- 模型只读取压缩后的分析输入，而不是直接读取整份 raw asset
- `.pdf` 等大体积二进制内容默认不直接送入模型上下文

这意味着现在已经能回答：

- 这个人是从哪个 source 进入候选池的
- 哪个 acquisition task 产出了该 snapshot
- 哪个 evidence 支撑了这次排序结果

## 5. 当前还没完全落地的部分

### Confidence architecture

当前已经落地：

- `job_results.confidence_label`
- `job_results.confidence_score`
- `job_results.confidence_reason`
- `confidence_policy_runs`
- `confidence_policy_controls`
- company-level `high / medium` threshold persistence
- request-family-scoped confidence policy
- feedback time decay
- manual `freeze / override`

当前默认输出：

- `high`
- `medium`
- `lead_only`

当前 retrieval control precedence：

- `request_exact`
- `request_family`
- `company`

### Criteria self-evolution

当前已经有第一版 persistence：

- `criteria_feedback`
- `criteria_patterns`
- `criteria_pattern_suggestions`
- `criteria_versions`
- `criteria_compiler_runs`
- alias pattern 已可回流到 retrieval scoring
- feedback 写入后已可自动触发一次 criteria recompile，形成最基础的 evolution loop
- 若开启 `rerun_retrieval`，会继续执行 retrieval rerun，并保存 result diff
- 若 feedback 带 candidate context，系统会额外沉淀 `suggested patterns` 供后续人工 review
- suggestion review 后已可沉淀 `applied / rejected` 状态，并把 `applied` suggestion 写回 active patterns
- confidence band 已支持：
  - pattern-level confidence boost / penalty
  - request-family policy
  - feedback time decay
  - manual freeze / override

还没有完全落地的部分：

- feedback 后自动建议是否值得 rerun retrieval

### Cloud asset registry

当前高价值资产仍以本地 runtime 为主。

跨设备恢复与云端 durable storage 的设计，见：

- [CROSS_DEVICE_SYNC.md](/home/sorachang/projects/Sourcing%20AI%20Agent%20Dev/sourcing-ai-agent/docs/CROSS_DEVICE_SYNC.md)

未来推荐拆成两层：

- 本地：
  - workflow logs
  - raw debug cache
  - 临时 snapshot
- 云端：
  - 高价值 LinkedIn profile raw payload
  - normalized profile documents
  - final artifacts
  - semantic/vector index

## 6. 当前结论

底层数据资产架构已经有可运行的 MVP：

- 有 canonical schema
- 有 snapshot 约定
- 有 evidence 追溯
- 有 workflow 审计事件
- 有 criteria version / compiler run 审计记录
- 有 confidence label 持久化
- 有面向 acquisition 的 planning layer

但它还没有进入最终形态。尤其缺少：

- cloud asset registry
- semantic retrieval state
- 自动 criteria evolution loop
