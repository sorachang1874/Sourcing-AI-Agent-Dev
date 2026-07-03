# Service-Grade Architecture Plan

> Status: Current architecture planning doc for service-grade workflow closure before OpenClaw/Codex adapter work. Cross-check with `AGENT_OPERATION_CONTRACT.md`, `PRE_AGENT_CONTRACT_REVIEW.md`, `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md`, `NEXT_TODO.md`, and `PROGRESS.md` before changing runtime, provider, workflow, or Agent-facing contracts.


## Goal

把当前 Sourcing AI Agent 从“本地可用的工作流原型”升级成适合长期迭代的服务级系统，避免以后继续反复做大规模 infra / storage / runtime 重构。

2026-06-08 重新定位：本计划不再只是历史设计参考。它是 Phase 13 / OpenClaw-Codex adapter 之前的底层服务收口清单。项目可以借鉴 Temporal、LangGraph、OpenClaw/Codex 这类成熟框架的抽象，但短期目标不是直接迁移到某个框架，而是把现有候选人发现、Profile 获取、Public Web、CRM/Export、provider task runtime 和 serving/projection 改造成成熟框架会认可的 typed workflow/service boundary。只有这些边界稳定后，外层通用 Agent 才能安全调用。

2026-06-10 checkpoint：本计划仍然是 M1-M6 的架构路线。M0.5 本地资产治理已经推进到 M0.6/M0.9 runtime-retention closeout：M0.6 reviewed apply 已回收约 `14.75GB`，M0.9 cold bundle 已验证但 destructive apply 尚未执行。继续开发前，新 session 应先读 `CLAUDE_CODE_PROJECT_HANDBOOK_2026-06-10.md` 和 `archive/CLAUDE_CODE_CONTINUATION_PROMPT_2026-06-10.md`，并把资产治理收口、GitHub scoped PR、Provider Task Runtime / workflow manifest refactor 作为后续顺序。

## 2026-06-11 Plan Revision（Claude Code 深审后，经 owner 确认）

基于对 `orchestrator.py`（87,225 行 / 1,099 方法）、`storage.py`（510 方法 / 292 双路径）、contract 分散度（每 command ~14 处定义位点）、测试安全网（3,033 测试中 33.9% 触碰私有方法但确定性断点有限）和 serving 现状（ThreadingHTTPServer 8 槽、psycopg 每查询新建连接、零鉴权）的多代理审计与对抗核实，重构改为五条轨道推进。本节优先于下文与之冲突的旧表述。

- **Track A 分解**：Phase 0 测试前置修缮 → Phase 1 CommandKernel 提取（~30 个 store-only 协议方法）→ Phase 2 CommandSpec registry → Phase 3 逐域提取（crm_public_web → excel_intake → profile_fetch → acquisition 仅 command 层；facade 保留全部公私方法名）→ Phase 4 纠缠核心重设计（`run_worker_recovery_once` 注册式 phase 分发；projection/candidate_source/asset_population 网随 M3–M5 拆解）。
- **M1 重定义**：M1 不再是"从 monolith 提取 manifest"，而是"在 `durable_runtime` 建 CommandSpec registry（合并 owner/stage/readiness/display 四张字典、activity-spine 与 running-control policy set 族、metrics 表、orchestrator 的 per-type 映射），workflow/command manifest 与 Agent tool spec 是它的导出物"。这避免把 monolith 的偶然结构固化进外部契约。
- **Track B 存储/测试**："先删 SQLite fallback"被审计否决（当时默认测试套件跑在 SQLite 上；postgres_only 模式当时仍依赖内存 SQLite 影子作为执行基底）。正确顺序：测试环境契约 v2（PG schema-per-run + teardown + TTL + symlink seed）→ 51 个 SQLite 直连测试文件迁共享 PG fixture → 按表组 PG-pure 重写双路径方法 → 删影子与 SQLite DDL。Mac 本地 PG 用 Docker（`local_postgres.py` 现为 Linux 专用）；正式 migration 机制替代手工双份 DDL。（状态更新 2026-07：该顺序已全部执行完毕——`storage.py` 已 PG-pure（零 sqlite3），内存 SQLite 影子与镜像机制已删除，PG schema 由 `migrations/0001_baseline.sql` + migration runner 唯一创建，`SOURCING_PG_ONLY_SQLITE_BACKEND` 已是无操作的遗留环境变量；剩余为 B4.2 ② repository 查询方法迁移与 ③ jsonb/timestamptz。详见 `TRACK_B_PG_PURE_STORE_DESIGN.md`。）
- **Track C Serving Runtime（新增轨道，目标 ~20 并发用户）**：psycopg_pool 连接池 → 重活（plan compile / job 执行 / 导出）统一出请求线程走 enqueue+poll → worker 与 API 进程分离 → 最小鉴权与用户身份（`requester_id/tenant_id` 列转为认证产物）→ FastAPI/uvicorn 重写 api.py（已批准；pydantic→OpenAPI 成为前端 contract 生成源）+ SSE → 对象存储读穿。明确不引入 Redis 与 LISTEN/NOTIFY（当前规模收益为负）。
- **Track D 强 Agent 化**：ModelClient 升级（streaming + tool-calling；现为 14 个单发方法）→ Agent Session 契约（服务端 agentic loop，工具面 = M1 manifest 导出 + 只读上下文工具 + `model_native_search` 转正；效果全部走 typed AgentAction）→ 第一垂直切片为公司身份自验证 loop（替代 PlanCard 手动修正 LinkedIn URL）→ plan review 对话化、intent→plan 前门流式化。外脑（OpenClaw/Claude/自建）可插拔；护城河 = typed sourcing 工具 + 证据裁决 + durable 执行 + 预算审批。
- **M2 新增设计约束**：provider 级并发预算属于 Provider Task Runtime——HarvestAPI profile-fetch 有 ~8 并发 actor 的隐性限制（旧 8 槽 API 信号量的真实由来）；需 per-provider+key 信号量/令牌桶、API key 池化、batch 粒度治理；该保护就位后 HTTP 入口并发上限才可放开。
- **Track E 治理**：M0.9 独立审批流程取消（10 个冷备已 sha256 验证、`archive_verified: true`，源目录随 `runtime/test_env` TTL 清理一并回收）；Independent Review Gate 适用范围收窄到不可重建资产的破坏性操作与 contract-heavy 变更；文档治理规则见 `INDEX.md`（轮转契约、归档制度、banner 纪律）。

### 多用户 Agent Serving 拓扑（2026-06-11 确认，经业界调研核实）

隔离单位逐层选择，不做 per-user 常驻容器（业界对照：Devin/Codex cloud/Claude Code web/Manus 的 per-task VM 只为任意代码执行与浏览器存在；Temporal/LangGraph Platform/Restate/Inngest 全部采用无状态 worker + checkpoint 会话，无一使用 per-user 容器；E2B/Modal 沙箱按工具调用租用，冷启动 80ms–3s）：

- **会话状态层**：`agent_session` 升级为一等公民，按 `session_key` 标识；一次用户消息 = 一个 `agent_turn`；同一 session 的 turn 凭 lease 串行（单写者语义，复用 `workflow_commands` 的 claim/lease 机制），不同 session 并行；每个工具边界写 PG checkpoint；长工具 enqueue durable command 并挂起 turn，completion event 唤醒（与 completion policy 同构）。任何 worker 副本可恢复任何会话；API 重启不再中断用户工作。
- **算力容量层**：按角色容器化（api / agent-loop worker / provider worker / PG / 对象存储），单机 docker-compose 起步，容量 = 副本数；本地开发、测试、生产同一套镜像（对应 Mac Docker PG 决策）。20 用户规模：单机 api ×1–2、agent-worker ×2–4、provider-worker ×2。
- **流式**：worker append `agent_events`，FastAPI SSE 网关 tail（200–300ms 轮询在当前规模足够，不引入 Redis/LISTEN-NOTIFY）；断线重连按事件序号续读。
- **公平性**：per-user 并发会话/turn 限额；provider 预算按 workspace 配额叠加在 M2 的 per-provider+key 令牌桶之上（HarvestAPI ~8 并发为全局池，多用户共享时必须在 provider 层统一调度）。
- **数据隔离**：store 层强制 tenant/workspace 作用域（列已存在）；canonical 数据面（公司资产、证据）刻意跨用户共享以复用 provider 采购成本；用户私有面 = CRM/workspace/会话/导出；同实体并发写由幂等键 + advisory lock 串行化。
- **凭证边界**：provider API key 只存在于 provider worker 层；Agent loop 只能引用 typed action，接外脑（OpenClaw 等）时此边界即安全线（参照 Claude Code git-proxy / Codex setup-phase-only secrets 模式）。
- **per-session 沙箱**：现在不引入；将来加任意代码执行或自营浏览器工具时按**工具调用**临时租用沙箱池，不随用户常驻。

核心原则：

- OpenClaw/Codex/LangGraph 可以成为外层 planner、browser、Search、model orchestration runtime。
- 本项目仍必须拥有业务状态、审批、预算、幂等、重试、PG-only provenance、CRM/export 权限和 evidence materialization。
- Adapter 前的目标不是暴露更多 API，而是先让底层模块达到 Agent-native 的服务级状态机质量。
- 任何 Agent-callable action 都必须通过 `AgentAction -> OperationRun -> workflow_commands -> ActivityAttempt / EntityDelta -> module owner`，不能直接写业务 read model。
- Agent-native 设计不是 M6 之后才补的外壳。M1 开始的 manifest、M2 Provider Task Runtime、M3 Candidate Acquisition、M4 Profile Fetch、M5 Public Web 都必须把自己设计成未来 Agent 能读懂、能观察、能暂停/继续/接管的工具化服务。

## Agent-Native Target Principles

这里的 Agent-native 不是“让 Agent 直接访问数据库或旧 route”，而是让每个底层模块从一开始就具备成熟 Agent 框架会期待的工具边界：

- **Tool-first service shape.** 每个可调用能力都有 action/command manifest、JSON schema、owner、权限、预算、幂等键、retry/cancel/resume 语义、display/control contract 和输出 provenance。OpenClaw/Codex adapter 只读取这些 manifest，不反向解析 orchestrator 代码。
- **Event-visible progress.** 长任务不能再是黑盒。候选人发现、Profile fetch、Public Web、导出、CRM 写入都要暴露阶段、当前 item、pending/timeout/retry、ActivityAttempt、EntityDelta 和下一步可操作动作，让 Codex/OpenClaw 可以实时汇报或在人类同意后接管。
- **Multi-source evidence, single owner.** Candidate acquisition 和 Public Web 可以同时使用 DataForSEO/API provider 与 Agent-native Search/fetch/browser 工具，但所有结果必须落入统一 ProviderTask/Evidence/EntityDelta contract。Agent Search 只能成为带 provenance、cost、retry、circuit 和 export policy 的 evidence source，不能成为不可审计的隐藏 fallback。
- **Planner uses Search before committing side effects.** 在意图识别、公司身份确认、目标公司 LinkedIn URL、Public Web profile/email 查找等阶段，Agent 可以调用自己的 Search/fetch/browser 能力补充 DataForSEO；但 commit/promotion/export/CRM mutation 仍必须通过 typed action 和 owner adjudication。
- **Human handoff is first-class.** Agent 需要能解释“我找到哪些候选/证据、哪些不确定、下一步需要批准什么 provider/model 调用、预算是多少”，而不是只返回最终结果。审批、人工 promotion、手动排除、retry/cancel 都是可持久化状态。
- **Framework-compatible, framework-independent.** 设计应对齐 OpenClaw/LangGraph/Temporal/Codex 会认可的 graph/tool/activity/event 模型，但不要先把业务状态交给某个框架。框架可替换；业务 owner、PG provenance、evidence/export/CRM contract 不可替换。

这份方案关注 5 个问题：

- 结果服务仍然隐式依赖整包 JSON。
- 新 query 仍会把资产语义和 runtime 目录强耦合。
- SQLite 还混合承担 control plane 和 blob transport 职责。（已解决：Track B PG-pure 完成后 control plane 全面 PG-only，SQLite 路径已整体删除。）
- query-time 结果视图和 company-level authoritative asset 没有完全分层。
- 部分能力仍不是 AI-first，而是靠规则、路径约定和后置补丁兜底。

2026-06-08 新增的服务级问题：

- `orchestrator.py` 仍然是约 87k 行的中央执行体，很多 service boundary 虽已通过 typed commands 收口，但实现仍散在单体 orchestrator 中，难以 review、替换或让 Agent 安全调用。
- `storage.py` 仍然承担过多 schema/reader/writer 职责。PG-only 是正确方向，但 store API 需要按 owner/service 分层，避免每个模块绕过 owner 直接读写表。
- Workflow command / activity / provider task contract 仍大量写在 docs、registry、orchestrator 和 tests 中，没有完全中央化为可生成、可 diff、可被 OpenClaw/Codex adapter 消费的 manifest。
- Candidate acquisition、Profile fetch、Public Web enrichment 仍混合了 legacy job compatibility、typed command owner、provider worker、serving projection 等多代语义；需要先形成服务级 vertical slice，而不是让外层 Agent 直接面对这些历史层。
- DataForSEO、Harvest/Apify、document fetch、model adjudication、未来 model-native Search 应共享 Provider Task Runtime，而不是每条链路各自实现 task identity、retry、pending budget、late result quarantine、circuit breaker 和 cost accounting。
- 前端/Operation Workbench 已开始产品化，但仍是最小控制台，不是自然语言 Agent UI；它需要消费 backend-owned display/control contracts，不得重新推导状态或暴露内部 contract 文案。

## 2026-06-08 Refactor Inventory

这是一轮面向 OpenClaw/Codex adapter 的底层盘点。优先级按“直接影响 Agent 安全调用”和“当前维护成本”排序。

| area | current state | service-grade target | adapter risk if skipped |
| --- | --- | --- | --- |
| Workflow spec / command spec | Durable command registry、ActionRegistry、docs 和 tests 都存在，但规格分散。 | 单一 manifest 定义 action、command、owner、input/output schema、display/control contract、activity/entity evidence、approval/budget、retry/cancel/resume。 | Agent 只能靠文档和字符串猜工具能力，容易调用内部 command 或绕过 owner。 |
| Provider Task Runtime | DataForSEO item-level contract 已开始收口；Harvest/Apify/Profile/Public Web 仍有多套 pending/retry/callback 语义。 | 统一 `ProviderTask` / `ProviderAttempt` / `ProviderResult` contract，支持 stable item key、batch envelope、retry unit、cost budget、pending budget、late-result quarantine、circuit breaker。 | OpenClaw/Codex Search 或 provider 调用会放大重复提交、无限等待、错误归因和成本失控。 |
| Agent-native Search/fetch evidence | DataForSEO 是当前主要 Public Web/search provider；模型原生 Search 仍 default-off。 | Search/fetch/browser 能力作为 Provider Task source 接入，输出 provider-attempt/evidence/entity-delta，不直接决定 truth。 | 如果把 Agent Search 当快捷外部能力，会绕过 provider 成本、证据审计、identity matching 和 export policy。 |
| Candidate Acquisition Service | W11 已把 root/intent/plan/review/commit/probe/profile admission 切成 typed commands，但 `queue_workflow` 和 legacy job bridge 仍存在。 | `plan -> review -> probe -> discover -> normalize -> publish projection` 是明确 service API；legacy job/snapshot 只作为 migration/read compatibility。 | 外层 Agent 可能直接触发旧 job workflow，绕过 staged review 或创建不可恢复状态。 |
| Profile Fetch Service | Registry、provider fetch、terminal admit、projection admission 已有 typed command 方向，但状态散在 profile registry、activity/entity delta、legacy refill workers。 | `requested -> cache_hit/fetch_required -> provider_fetch -> terminal_admit -> projection_admission` 是显式状态机，成功/失败 URL 分桶重试。 | Agent 可能整批 retry profile，重复消耗 provider，或把未 terminal 的 profile 当成 board-visible。 |
| CRM Public Web Service | CRM-owned queue/phase commands、detail/export/promotion contract 已大幅收口；live-provider validation 仍未完成。 | Public Web search, document fetch, evidence adjudication, signal materialization, manual promotion/export 都有 owner-owned event/evidence and live quality gate。 | Agent 会重复搜索、丢失人工 promotion、把低置信 evidence 当成可导出结果。 |
| Model-native Search | `model_native_search` 已 reserved/fail-closed。 | 作为实验 provider source 接入 Provider Task Runtime，只能 supplemental evidence，与 DataForSEO A/B，不能替代正常 chain。 | 模型 Search 会变成不可审计、不可复现、绕过 DataForSEO retry/cost 的隐藏 fallback。 |
| Serving Projection / Result View | Canonical serving projection 和 result view contract 已存在；旧 snapshot/materialized JSON compatibility 仍广泛可见。 | `ProjectionReader` / `ResultViewService` 是 Agent/前端唯一 read surface；object/cache/materialization 是 owner 内部细节。 | Agent 可能读旧 snapshot 或 candidate_documents fallback，做出与前端/CRM 不一致判断。 |
| Local Asset Governance | Google、Reflection AI、早期测试/生产未隔离资产和大公司 historical snapshots 仍可能占据大量本地空间；W5 已有 audit/repair/cold-archive 基础但未作为当前 M0.5 闭环。 | 在 M1/M2 前完成只读 audit、authoritative pointer 校验、重复 shard subsumption 判断、cold archive manifest、reviewed apply；热路径只保留 authoritative baseline 和必要 scoped shards。 | Provider Task Runtime / Agent Search 可能复用污染或重复资产；直接删除会破坏 rebuild artifact、projection、audit 或 historical recovery。 |
| CRM / Export | Operation adapters 和 CRM writer/export owners 已存在；product UX 和 bulk approval polish 仍不足。 | CRM mutation/export 都通过 approval/budget-aware actions；exports carry provenance and manual promotion scope. | Agent 可触发错误 CRM stage、导出未确认 contact、或绕过人工确认。 |
| Frontend / Operation Workbench | `/operations` 是最小任务审批与执行工作台；目标候选人 Public Web UI 已局部产品化。 | 前端只显示 backend-owned display/control/status/help contracts；技术细节在 debug drilldown；自然语言 Agent UI 另建。 | 用户误触高成本 retry，或产品文案暴露内部 contract，造成误操作和理解偏差。 |
| Documentation / GitHub process | 当前文档多、历史 tracker 多、README 曾过期；worktree 巨大且包含大量未跟踪文件。 | README/INDEX/PROGRESS/NEXT_TODO/Service plan 是当前入口；每个 milestone 用 branch/PR/review artifact 固化。 | 后续 session 容易回退旧架构，或者把未审计 dirty state 直接推上 GitHub。 |

## Service-Grade Closure Milestones

这些 milestone 是 adapter 前的收口顺序。每个 milestone 都需要 targeted tests、fast contract preflight 和 Independent Review Gate；涉及 provider/live quality 的阶段还需要小范围 live validation。

1. `M0 Documentation and GitHub checkpoint`
   - 更新 README、INDEX、NEXT_TODO、PROGRESS 和本计划，明确当前不是 MVP，而是服务级 workflow closure。
   - 盘点 dirty worktree，分离 runtime/log/secret/test artifact，禁止无范围 `git add .`。
   - 建立 docs-only GitHub checkpoint PR，保存当前 reviewed docs 和后续重构计划。不要把 Operation UI、frontend API adapter、runtime registry 或大 pre-agent test 混进 M0；这些属于独立 implementation checkpoint。

2. `M0.5 Local Asset Governance`
   - 目标是治理本地大资产，而不是继续拖到 M1/M2 之后。Google、Reflection AI、Anthropic/OpenAI 早期测试/生产混杂快照应先通过只读审计确认 authoritative baseline、可复用 scoped shard、重复 no-increment snapshot、缺失 reference payload 和 projection/index 依赖。
   - 第一阶段只允许生成 audit / repair proposal / cold archive manifest，不删除、不覆盖、不移动热路径资产。任何 apply 都必须有 reviewed manifest、cold-copy/hash proof、projection pointer proof、rebuild rehearsal 和 Independent Review Gate。
   - 保留策略：热路径保留最新 authoritative baseline、已发布 collection projection 所需 payload、可证明服务具体 scope 的 shard；重复 historical snapshots 移出正常 reuse index，原始文件进入 cold archive 或外部备份位置，保留 manifest 以支持 rebuild/audit。
   - 不把 M0.5 混入 M0 docs-only checkpoint，也不和 M1 manifest / M2 Provider Task Runtime 同 PR。资产治理会影响 storage、artifact rebuild、projection pointer、provider cache 和 local disk layout，必须是单独 implementation checkpoint。

3. `M1 Workflow and command spec manifest`
   - 从 `ActionRegistry`、durable command owner registry、display/control/activity policies 生成或维护单一 manifest。
   - Manifest 必须覆盖 action/command schema、owner、allowed transitions、retry/cancel/resume、approval/budget、evidence and display contracts。
   - 前端、Operation Workbench 和未来 OpenClaw/Codex adapter 只读 manifest/API record，不硬编码 command/action 字符串。
   - Manifest 必须以 Agent tool spec 的形态可消费：包含输入 schema、输出 schema、side-effect class、cost class、approval gate、progress event contract 和 owner-readable status cards。

4. `M2 Provider Task Runtime v1`
   - 把 DataForSEO、Harvest/Apify、document fetch、model adjudication 的 provider attempt 统一到 item-level task contract。
   - 强制 stable task key、provider tag/keyword join、per-item retry、pending budget、late result quarantine、cost/circuit metrics。
   - 禁止 whole-batch retry 或 request-order fallback 成为正常路径。
   - 为 Agent-native Search/fetch/browser source 预留同一套 `ProviderTask` 接口：它可以补充 evidence，但必须受 budget、retry、rate-limit、late-result quarantine、provenance 和 source-quality contract 约束。

5. `M3 Candidate Acquisition Service closure`
   - 收口 `start_acquisition_run` 到 service-level `plan/review/probe/discover/publish` contract。
   - `queue_workflow` 和 legacy job shell 只保留 report-visible migration bridge。
   - Candidate list discovery 输出 Activity/EntityDelta + serving projection proof，不能只依赖 job snapshot。
   - 意图识别到候选人发现之间允许 Agent-native Search/fetch 作为 reviewed evidence source，例如确认目标公司官网/LinkedIn company URL、补充搜索 query、发现公开 roster hints；这些 evidence 进入同一 adjudication/projection path，而不是直接创建候选人。

6. `M4 Profile Fetch Service closure`
   - 建立 profile URL item state machine：cache hit、fetch required、provider fetch、terminal admit、projection admission。
   - 成功 URL 和失败 URL 分桶；retry 只作用于失败 item，不重跑成功 URL。
   - Board-visible/profile detail 只能读 terminal/projection admission proof。

7. `M5 CRM Public Web and evidence quality closure`
   - 完成小范围 live-provider validation：DataForSEO pending/timeout、model provider circuit、manual promotion preservation、export payload quality。
   - Public Web evidence/adjudication 输出必须可解释，Google Scholar/GitHub/X/email 等类型有统一 review/export contract。
   - Public Web 默认设计成双源 evidence pipeline：DataForSEO/API provider 是可复现基础源，Agent-native Search/fetch/browser 是 reviewed supplemental source。两者一起进入 identity/evidence adjudication，不允许任一来源直接 bypass review/export policy。
   - Model-native Search 只作为 reviewed experiment 接入，不作为 fallback。

8. `M6 Agent-callable adapter`
   - 暴露 read-only context tools：action registry、operation run/provenance、projection/CRM/Public Web detail、Activity/EntityDelta。
   - 暴露 controlled action tools：submit action、approve/reject、dispatch、poll、cancel/retry/resume。
   - OpenClaw/Codex adapter 不接数据库，不接 legacy route，不接 migration/backfill route，不直接调用 provider。
   - 如果 M1-M5 已按 Agent-native service shape 完成，M6 应主要是 thin adapter，而不是再补业务语义。

9. `M7 Natural-language Agent product layer`
   - 在 adapter 稳定后再做自然语言 planner loop、memory/context、multi-model adjudication、browser/search sandbox 和 Agent UI。
   - Agent 输出必须落到 typed action/evidence/promotion/export provenance，而不是只停留在 chat transcript。

## Immediate Decisions

- **Do not migrate wholesale to a framework first.** Temporal/LangGraph/OpenClaw abstractions应先作为服务边界标准，而不是一次性替换当前 runtime。直接迁移会把现有 legacy compatibility 和 provider long-tail 问题搬进新框架。
- **Do not expose current internals to OpenClaw/Codex yet.** Adapter 前必须先有 manifest、provider task runtime、candidate/profile/Public Web vertical slice closure。
- **Do not postpone Agent-native design until the adapter.** M1-M5 的服务边界必须已经是 Agent 可观察、可解释、可接管、可工具调用的形态；M6 只做安全暴露和协议适配。
- **Do not treat docs-only review as product signoff.** 当前文档更新只能确定方向；Public Web 和 provider runtime 仍需真实 live/provider validation。
- **Do not push the full dirty worktree without a scope gate.** 当前仓库存在大量修改和未跟踪文件。GitHub 同步应先做 scoped checkpoint branch/PR，随后按 milestone 拆 PR。

## GitHub Checkpoint Procedure

2026-06-08 repo state:

- Git root: `/Users/changyuyi/projects/Sourcing AI Agent Dev`
- Current branch: `productization-2026-04-25-stable`
- Remote: `git@github.com:sorachang1874/Sourcing-AI-Agent-Dev.git`
- Dirty scope observed during this checkpoint: hundreds of tracked and untracked files, including runtime/log/output artifacts and newly created contract/runtime files.

Checkpoint rule:

- Do not run `git add .` or push the whole branch as a milestone snapshot.
- First PR should be a scoped architecture checkpoint only. It must not include `sourcing-ai-agent/frontend-demo/**`, `sourcing-ai-agent/contracts/**`, `sourcing-ai-agent/src/**`, `sourcing-ai-agent/tests/**`, runtime artifacts, provider payloads, logs, or generated screenshots.
- Required staged files for M0 checkpoint (git-root-relative):
  - `sourcing-ai-agent/README.md`
  - `sourcing-ai-agent/PROGRESS.md`
  - `sourcing-ai-agent/docs/INDEX.md`
  - `sourcing-ai-agent/docs/NEXT_TODO.md`
  - `sourcing-ai-agent/docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md`
  - `sourcing-ai-agent/docs/AGENT_OPERATION_CONTRACT.md`
  - `sourcing-ai-agent/docs/PRE_AGENT_CONTRACT_REVIEW.md`
  - `sourcing-ai-agent/docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`
  - `sourcing-ai-agent/docs/MODEL_NATIVE_SEARCH_PROVIDER_CONTRACT.md`
  - `sourcing-ai-agent/docs/INDEPENDENT_REVIEW_BRIEF.md`
  - `sourcing-ai-agent/docs/INDEPENDENT_REVIEW_GATE.md`
- The 2026-06-07 UI/contract slice is not part of M0. If it is synchronized to GitHub, open a separate implementation PR and explicitly include its full dependency scope, including frontend route/page/API helpers, frontend API contract types/schema/adapter, backend operation/durable runtime owners, affected contract docs, and targeted tests.
- Before staging, run `git status --short`, inspect untracked files, and exclude `runtime/**`, `logs/**`, `output/**`, `runtime/secrets/**`, raw provider payloads, local cache, `.venv*`, `node_modules`, browser cache, and generated review prompts unless the review artifact itself is intentionally being committed.
- Before PR creation, run `git diff --cached --name-only` and compare its git-root-relative output against the required staged-file list above. Any missing current-entry contract doc or any extra runtime/log/secret/generated artifact blocks the PR. Do not use a subproject-relative list for this checklist unless the command is explicitly changed to `git diff --relative --cached --name-only` in both current entry docs.
- The PR description must include targeted validation commands and the Independent Review artifact path.
- After the checkpoint PR lands, split implementation work by milestone: `M0.5 local asset governance`, `M1 manifest`, `M2 Provider Task Runtime`, `M3 acquisition service`, `M4 profile fetch`, `M5 CRM Public Web quality`, `M6 adapter`. Each implementation PR gets its own targeted tests and Independent Review Gate.

Branching decision:

- Because the previous working tree has not been synchronized for more than a month, creating a new branch from the current local state is acceptable and preferable to continuing on an ambiguous stale branch.
- A branch pointer alone does not preserve the dirty working tree. The current state becomes recoverable only after scoped commits/PRs. Therefore the first branch should be a checkpoint branch whose first commit is the docs-only M0 scope above.
- Do not make a single "current local state" commit containing all modified/untracked files. Use a docs-only checkpoint PR first, then separate implementation PRs for UI/contract, runtime/operation, provider task runtime, acquisition/profile, and Public Web quality.
- Do not run local asset cleanup as a manual filesystem deletion. Use the existing asset-consolidation audit/repair/cold-archive pattern, then add a reviewed apply step that can prove hot projections and rebuild paths still work before normal reuse indexes stop seeing archived snapshots.

## Current Anti-Patterns

### 1. Runtime-copy as data plane

现在很多流程仍默认把资产“落到 runtime 再消费”。这会带来：

- 同一份 company asset 在多个 query/job 间重复复制。
- ECS / 本地磁盘同时承担真相源和缓存层职责。
- hosted / local / bundle restore 三条路径难以统一。

### 2. Snapshot monolith as hidden dependency

虽然前端已经改成 `dashboard + page + detail`，但后端直到这次 `Phase 2` 之前，分页接口底层还是会先读整份 `materialized_candidate_documents.json` 再切片。

### 3. Query result and canonical asset are not cleanly separated

当前 job 有时直接绑定 query-specific snapshot，有时又绑定 company baseline，但两者都通过 `candidate_source` 和 runtime path 约定来表达，缺少明确的“结果视图层”。

### 4. SQLite is overloaded（已解决：Track B PG-pure 完成，SQLite 已删除）

历史状态——SQLite 曾同时承担：

- job state
- review state
- candidate/evidence registry
- materialization bookkeeping
- 某些 serving 回落时的临时真相源

这会让“只改 control plane”与“只改 serving artifact”变得困难。

## Design Principles

### 1. Split planes explicitly

系统应拆成 5 层：

- Control plane
  - job
  - review state
  - execution state
  - asset registry
  - query/result view metadata
- Canonical entity plane
  - candidate
  - evidence
  - profile registry
  - company identity registry
- Materialized serving plane
  - manifest
  - candidate shards
  - list pages
  - backlog views
  - compact overlays
- Blob/object plane
  - raw provider payloads
  - materialized shards/pages
  - exported bundles
- Runtime cache plane
  - local hot cache only
  - never the canonical truth source

### 2. Query should reference assets, not copy assets

每个 query/job 最终应该只记录：

- 它引用的 authoritative snapshot / snapshot generation
- 它叠加的 delta acquisition artifact
- 它选择的 result view / filter projection

而不是在 runtime 下再复制一份 company assets 给这个 job 独享。

### 3. AI-first means AI where ambiguity exists

AI 应该主要放在高不确定性环节：

- company identity resolution
- query intent expansion
- evidence synthesis
- manual review prioritization
- outreach / segmentation reasoning

不该把 AI 用在本应 deterministic 的 control plane / storage orchestration 上。

## Service-Grade Working Rules

这份架构计划不只是“未来可选方向”，而是当前升级阶段的默认开发约束。

1. 优先做根因修复，不做只覆盖单个测试用例的表层补丁。
2. 先做影响面分析，再动代码：
  - 类型
  - schema
  - config
  - shared variable
  - callsite
  - tests
3. 一旦共享契约变更，同步更新所有下游消费者，不留下一轮再补。
4. 默认允许较大的模块化/存储/架构改造，只要能显著减少未来重复 infra 重构。
5. 不再新增新的 runtime-path 双轨约定。
6. 新代码默认不得直接拼接 `runtime/company_assets/...` 作为核心读取契约；应走共享 resolver。
7. 在交付前必须重新跑 simulate-only 验证，不真实调用 provider 也要确保整条工作流可恢复、可重放、可服务。
8. 当前升级阶段先不自动同步 ECS，等架构面收敛后再统一迁移。

## Target Architecture

## 1. Authoritative Asset Registry

新增明确的 authoritative registry 语义：

- `organization_assets`
  - authoritative snapshot / generation pointer
  - available views
  - freshness / completeness watermark
- `snapshot_materialization_runs`
  - snapshot-level materialization health
- `candidate_materialization_state`
  - candidate fingerprint
  - shard path
  - page index
  - materialized_at

这样 job 不必优先绑到 query-specific live snapshot。

full-reuse query 的正确姿势应是：

- 直接钉住 authoritative snapshot
- 只额外记录本次 query 的 intent / projection

## 2. Query Result View Layer

需要显式引入 `job_result_view` 概念。

每个 job 只存：

- `source_snapshot_ref`
- `source_generation_key`
- `view_kind`
  - `asset_population`
  - `ranked_results`
  - `hybrid`
- `filter_projection`
  - function ids
  - employment scope
  - manual review status
  - user-confirmed company linkedin url / slug
- `delta_artifact_refs`

这样：

- 同一公司 baseline 不再为每个 query 复制目录
- query 只构造“视图”，不复制“资产”

## 3. Serving Artifact Contract

统一采用以下 contract：

- `normalized_artifacts/manifest.json`
  - counts
  - pagination
  - shard index
  - backlog index
  - auxiliary index
- `normalized_artifacts/pages/page-XXXX.json`
  - 只放 board/list 所需字段
- `normalized_artifacts/candidates/<candidate_id>.<fingerprint>.json`
  - 单 candidate 的完整详情
- `normalized_artifacts/backlogs/manual_review.json`
- `normalized_artifacts/backlogs/profile_completion.json`
- `normalized_artifacts/publishable_primary_emails.json`
  - 紧凑 overlay，不再通过 strict-view monolith 回补

当前 serving API 目标应围绕 canonical projection / collection contract：

- `GET /api/collections`
- `GET /api/collections/{collection_id}/coverage`
- `GET /api/projections/{projection_id}/candidates?offset=&limit=`
- `GET /api/projections/{projection_id}/candidates/{candidate_id}`
- `GET /api/operations/runs`
- `GET /api/operations/runs/{operation_run_id}/provenance`

旧 `/api/jobs/{job_id}/results|dashboard|candidates` 只能作为 migration/read-compat surface；当 run-scope projection 已就绪时，应返回 projection pointer 或 fail closed，而不是重新组合 job/runtime artifacts。

## 4. Object Storage First, Local Cache Second

长期目标不是“把所有东西都放 ECS 本地盘”，而是：

- control plane DB
  - PG-only in local, hosted, production, and Agent-callable normal paths
  - SQLite/shadow storage has been deleted entirely (Track B B4.3 complete: `storage.py` is PG-pure; the PG schema is created solely by the versioned migration runner); it must not be reintroduced or exposed through OpenClaw/Codex adapters
- object storage
  - OSS / R2 / S3-compatible
  - raw assets
  - materialized serving artifacts
  - exports / bundles
- local disk
  - hot cache
  - temp workspace
  - active run logs

这意味着：

- 本地 runtime 目录只是缓存和工作区
- 不是 canonical storage
- 不是 query 级复制的落点

补充约束：

- canonical store 决定 authoritative snapshot / latest pointer / registry 治理结果
- hot cache 负责 serving/recovery 的本地低延迟读取
- governance / repair / registry / backfill 默认操作 canonical store
- serving / refresh / recovery 默认优先尝试 hot cache，再回落 canonical store

## 5. Materialization V2

### Current foundation

`Phase 1` 已经具备：

- candidate fingerprint
- shard materialization
- page materialization
- backlog materialization
- dirty-set tracking

### Next step

把 materialization 彻底升级为“append/update shards + regenerate indexes”：

- candidate shard only for dirty candidates
- page/index/backlog rebuild only at index layer
- no full snapshot rewrite as the default path

需要继续推进两件事：

1. `materialized_candidate_documents.json` 从 serving dependency 降级成 compatibility artifact。
2. shard/page/backlog 发布直接走 object storage，同步写本地 hot cache。

## 6. Company Identity Resolution

`Safe Superintelligence Inc -> ssi-ai` 这种问题说明当前 slug 解析仍然不够服务级。

目标应改成 4 层 resolver：

1. Deterministic registry
  - builtin
  - bundled seed
  - imported runtime registry
  - user-corrected alias registry
2. Retrieval candidates
  - LinkedIn company search candidates
  - observed company identities
3. LLM adjudication
  - 只在 ambiguity 高时介入
4. Human confirmation
  - review plan 阶段展示识别到的 company LinkedIn URL
  - 允许用户直接修正

用户修正后应写回：

- company identity registry
- alias registry
- future prompt context / observed resolution memory

这样以后同类问题不必重新问模型。

## 7. AI-First Gaps Still Remaining

以下环节还没有完全做到 AI-first：

### Company resolution

- 目前 deterministic seed 不足时，依赖 heuristic slug 和零散 fallback。
- 需要改成“retrieval candidate set + model adjudication + human correction memory”。

### Planning and review

- 计划阶段已有结构化 gate，但缺少“用户纠正后自动沉淀为 resolver memory”的闭环。

### Materialization prioritization

- 现在 dirty rebuild 已经是 candidate-level，但还缺少 AI-assisted priority lanes：
  - first-screen candidates
  - manual-review-needed candidates
  - profile-gap candidates

更合理的方式是：

- 先物化 screen-critical shards
- 再异步补完长尾

### Evidence synthesis

- 目前 evidence 仍偏“采集后再格式化”。
- 后续应更明确区分：
  - raw evidence
  - normalized evidence
  - synthesized candidate narrative

## 8. Execution Roadmap

### Phase 2

这次已经落地的方向：

- serving path 改为优先读取 `manifest/pages/shards`
- strict roster publishable email 改为轻量 overlay artifact
- detail/page path 不再默认依赖 monolith payload

### Phase 3

建议下一轮完成：

- `job_result_view` registry
- job 对 authoritative snapshot 的引用化
- 去掉“每个 query 复制 company assets 到 runtime”
- 让 runtime 只保留 hot cache

当前已进入落地中的部分：

- `job_result_view` 已进入结果服务主路径
- refresh / recovery 已开始优先从 `job_result_view` 恢复 snapshot
- snapshot/source-path 解析正收口到共享 resolver
- canonical-store / hot-cache 分层已开始进入核心读取链路

下一步约束：

- 不再新增直接依赖 `runtime/company_assets` 的热路径代码
- recovery、registry、artifact reader 要继续统一到同一套 resolver / store contract
- runtime copy 语义需要继续下沉为 cache 行为，而不是 job 级真相源

### Phase 4

服务化落地：

- Postgres 作为 hosted control plane
- object storage 作为 blob plane
- local/hosted 共用同一 artifact contract

### Phase 5

AI-first closing loop：

- company identity correction memory
- plan review corrections -> registry
- materialization priority lanes
- evidence synthesis specialization

## What This Avoids

如果按这个方案推进，后面可以避免再做这些高成本返工：

- 为了 hosted 再改一次 runtime path 语义
- 为了 ECS 磁盘压力再改一次 snapshot 存储布局
- 为了前端首屏性能再改一次结果 API contract
- 为了 slug / company identity 识别再堆更多 heuristic patch
- 为了 partial materialization 再推翻现有 shard 方案

## Practical Rule

从现在开始，任何新功能如果涉及“结果如何给前端看”，默认应遵守两个规则：

1. 先问它属于 control plane、canonical plane 还是 serving plane。
2. 新 query 不允许再以“复制公司资产目录到 runtime”作为默认实现路径。
