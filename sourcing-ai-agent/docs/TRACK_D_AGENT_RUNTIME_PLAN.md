# Track D — 强 Agent 化执行计划（跨模型设计输入）

> Status: Cross-model design input for owner review（2026-07-13，作者 = Claude Fable 5；只规划、不改码）。
> 定位：为 Track D 提供第二模型家族的独立设计视角，供接手实现的 GPT-5.6(Codex) 部分复用或反驳；
> 本文**不是任何 scope 的 GO**——实施批次仍逐批走 `INDEPENDENT_REVIEW_GATE.md`。
> 配套阅读：`SERVING_EXECUTION_NORTH_STAR.md`（已 ratified，支柱 4 = agent-native serving）、
> `AGENT_OPERATION_CONTRACT.md`（W7→W8→W9→W10→Phase 13 阶梯）、
> `TRACK_C_SERVING_RUNTIME_PLAN.md`（决策点 (d)：C5 并入 Track D）。
> 行号锚点基于 2026-07-13 勘察（commit `889848e` 工作树），实施前按惯例重新 Scout。

## 0. 基线修正：Track D 不是「从零建 Agent」

`NEXT_TODO.md` 把 Track D 记为四项全空，低估了已有地基。2026-07-13 代码勘察结论：

**已存在且 active（W8/W9）**：
- `agent_actions` / `operation_runs` / `operation_events` PG-only 持久化（`migrations/0001_baseline.sql`）。
- `operation_runtime.ActionRegistry`（`operation_runtime.py:103`）：action→owner 映射、approval/budget
  fail-closed（`:64-86`；缺预算即 raise，`:676`）、`display_contract_for`、command 暴露 allowlist。
- 完整 poll 控制面 API（`/api/operations/*`：submit/approve/reject/cancel/retry/resume/dispatch/provenance）。
- W11a–i 类型化 acquisition 命令链（intent.resolve→plan.build→plan_review→plan.commit→probe→scale→
  discovery→profile fetch→projection admission，全部 Command→ActivityRun→Attempt→EntityDelta 证据化）。
- HITL 原语：`plan_review_sessions`（W11 已接线）+ `workflow_recovery_intents` 单赢 claim（北极星点名的
  agent-turn interrupt 原语）。
- 异步任务信封：`async_task_contract.py`（202+task_id→poll→artifact ref）。

**真正缺的（Track D 的实际范围）**：
1. ModelClient 无 streaming、无 native tool-calling：`model_provider.py` 全文零命中
   `stream/tool_call/tools=`；14 个单发 Protocol 方法（`:617-650`），requests/urllib 同步阻塞
   （`:1869`/`:1932`/`:1275`），所有「结构化输出」是 prompt-instructed JSON。
2. `agent_sessions` / `agent_turns` / `agent_events` 三表不存在（baseline SQL 无此三表；
   注意 `agent_runtime_sessions`/`agent_trace_spans` 是 job 内 specialist-lane 运行时，**勿混淆**）。
3. API 无 SSE/websocket（`api.py` 零命中 `event-stream`）；进度全靠轮询。
4. 服务端 agent planner loop 不存在（W9 控制面明文「不执行 owner side effects」）。
5. `model_native_search` 只有保留 id + fail-closed 守卫（`search_provider.py:2107-2121`），无实现。

**工具面种子已就绪但未 serve**：`command_type_manifest()`（`durable_runtime.py:665-693`，docstring 自称
"Seed of the Agent tool-spec manifest"，含 cancel/resume/control 成熟度语义）目前仅测试引用；
现行 `/api/workflow/command-registry` 走的是另一函数。

## 1. 设计原则（继承既有裁决，不另起炉灶）

1. **Agent turn = durable typed command**（北极星支柱 4）：`agent.turn` 是一个 CommandTypeSpec，
   跑在既有 event/reducer/outbox + lease + recovery 基座上；**不建第二 runtime**；progress = 同一事件
   log 的 tail；HITL = `workflow_recovery_intents` 模式的 interrupt point。
2. **Agent 不是 data owner**（AGENT_OPERATION_CONTRACT 不可协商规则）：loop 的所有 effect 只能是
   AgentAction→owner command；planner 的工具面 = ActionRegistry allowlist 的投影，永不直接暴露内部函数，
   durable owner registry 不是 product allowlist。
3. **fail-closed 与预算前置**：每个付费步骤走既有 approval/budget 原语；loop 级别新增 per-turn
   步数/成本/墙钟预算，超限 fail-closed 转 HITL（见 D2）。
4. **deterministic-first 前门不动**：`intent_brief` 保持确定性（`INTENT_PLANNING_BRIEF.md`）；
   对话化只加层，不改既有 plan/review 契约。
5. **characterize-first + 变异自检**：沿用 Track A/B 纪律；动 ModelClient 前先钉死 14 方法现契约。

## 2. 阶段划分

### D0 — ModelClient v2：streaming + native tool-calling

- 现状锚点：Protocol 14 方法（`model_provider.py:618-644`）+ 3 个 utility（`:646-650`）；
  OpenAI-compatible chat/responses 双 api_style（`:1837-1842`）；Qwen urllib（`:1275`）；
  per-provider 熔断 + healthcheck 缓存已有，直接复用。
- 交付：新增能力面 `stream_tool_turn(messages, tools, budget) → streamed events`
  （text-delta / tool-call / usage / stop），挂 OpenAICompatibleChatModelClient 与 Qwen client；
  **14 个既有方法保持逐字节等价 facade，24 个消费模块零改动**——agentic 能力是加法，不是迁移。
- tool schema 来源 = D1 的 served registry，不手写。
- 测试路径：simulate/scripted 先行——参照 `OfflineModelClient`/`ScriptedLivePlanningModelClient`
  增加 scripted tool-call 转写（录制/回放），loop 逻辑全部离线可测；live 验证按 provider fail-closed
  纪律 owner 显式授权（当前 Apify/keys 已轮换未配置，live 本来就冻结）。
- 决策点 **TD-1**（HTTP 栈）：建议保留 requests + 手写 SSE 行解析（`data:` 帧协议很薄），
  不为单一能力引入 httpx/async 栈；async 化留到 C3 进程分离后按实测需要 revisit。

### D1 — 工具面 serve：tool registry 转正

- `command_type_manifest()` + `ActionRegistry.to_record()` 合并导出为只读 `/api/agent/tool-registry`：
  action 级工具（approval/budget/display_contract/allowed_workflow_command_types）+ command 级语义
  （cancel/resume handler、running-control 成熟度、activity_spine_policy）。
- 只读上下文工具组：`search_projection`/`filter_projection` 已可 dispatch（契约已接线）；
  W9 的 Activity/Attempt/EntityDelta/provenance 查询面已存在，包成 read-only tool spec 即可。
- 守卫（新增 fast guard）：tool registry 必须是 ActionRegistry allowlist 的投影——测试断言
  registry 输出 ⊆ allowlist，`legacy_internal_pending_activity_spine` 命令永不出现。

### D2 — Agent 会话与事件层（与 C4/C5 合流）

- 新表 `agent_sessions` / `agent_turns` / `agent_events`（migration 0002+）：per-session 单写者
  lease（复用既有 lease 原语），turn = `agent.turn` CommandTypeSpec，事件 append-only、
  idempotent per stream（形态照抄 `operation_events`）。
- **先 poll 后 SSE**：turn 状态复用 `async_task_contract` 202/poll 信封，可在 C4 SSE 之前落地；
  SSE tail 与 C4 是同一套设计，届时一次做（Track C plan 决策点 (d) 已裁：C5 = Track D serving 落地）。
- loop 预算与审计：turn 级 `max_steps`/`max_cost`/`max_wall` 超限 fail-closed 转 HITL；
  planner 每步（模型输入摘要、tool 选择、action id）落 `agent_events`，可审计、可重放、可 fork。
- 依赖边界：C2 身份（per-user 限额）与 C3 进程分离（agent-worker 角色）是 C5 完全体前置；
  **poll-mode 单进程版不依赖 C2/C3，可先行**。

### D3 — 第一垂直切片：公司身份自验证 loop

- 替代目标（现状锚点）：plan review gate 暴露可编辑 `target_company_linkedin_url`
  （`plan_review.py:26-115`，google-scope 歧义时 `required_before_execution=True`）；人工确认后走
  `resolve_manual_company_identity`（`connectors.py:124-191`，`resolver="manual_review_override"`）。
- 歧义源：`resolve_company_identity` 四级解析（`connectors.py:193-248`：builtin map→legacy slug→
  模型判定 observed candidates（`:251-325`，`judge_company_equivalence`）→heuristic slug 低置信）；
  低置信 heuristic 与 parent-vs-suborg（Google/DeepMind 类）是人工介入主场景。
- loop 形态（**全部走既有 provider，不依赖 model_native_search**）：
  1. 触发：plan compile 后 identity confidence < high；
  2. 步骤：DataForSEO/Serper search（既有 `search_provider`）→ documents.fetch（既有）→
     `judge_company_equivalence`（`model_provider.py:638`）多证据比对 → 置信阶梯；
  3. 出口：high → 自动确认（新 `resolver="agent_self_verified"`，证据链落 EntityDelta）；
     仍歧义 → 保持现 review gate，但附证据卡（候选 URL + 来源 + 模型裁决 rationale），
     **人只裁决、不再自己检索**；
  4. 形态 = 一个 OperationRun + 若干 typed command（复用 W11 命令链模式），审批/预算按契约
     （seed-url-only 检索可零 provider 预算，但仍显式）。
- 选它的理由：自包含、低风险（错误自动确认被 review gate 兜底）、直接消掉一个真实人工步骤、
  为 D2 的 loop 原语提供第一个真实负载。
- 验收：历史 plan 回放中需要人工编辑 URL 的比例显著下降；歧义 case 100% 仍进 HITL；
  零直接模块表写入（对抗审计项）。
- 决策点 **TD-2**（切片形态）：建议做成 plan 流水线内的 durable operation，**不等 D2 会话层**。

### D4 — 之后（本文只圈定，不展开）

plan review 对话化（intent_brief 契约不变，加对话层）；intent→plan 前门流式化（依赖 D0+C4）；
`model_native_search` 转正（按其 contract 的 owner 矩阵 + DataForSEO A/B + review GO，独立批）；
Option 3 全事件流 recovery driver（北极星，D2 事件层成熟后）。

## 3. 与 Track C 的并行边界（Codex 跑 C 时互不踩脚）

| D 批 | 依赖 C? | 主要触碰 | 与 C1–C4 冲突面 |
|---|---|---|---|
| D0 | 否 | `model_provider.py`（+新模块） | 无（C 不动 model_provider） |
| D1 | 否 | `api.py` 新只读路由 + `operation_runtime.py` 导出 | 低（C1 改重活 handler，分属不同文件段；同文件批错峰即可） |
| D3 | 否（poll-mode） | `connectors.py`/`plan_review.py`/新 owner 模块 | 低 |
| D2 | C4（SSE 完全体）、C2+C3（多用户完全体） | migrations + 新模块 | 与 C5 是同一件事，届时合流 |

**推荐顺序：D0 → D1 → D3（poll-mode）→ D2（与 C4 合流）→ D4。**
D0/D1/D3 与 Track C 的 C1/C2/C3 可完全并行。

## 4. 评审与纪律

- 每批照 handbook 式协议：characterize-first（D0 先钉 14 方法契约金快照）、scripted 回放 A/B、
  变异自检（先破坏一处确认变红）、green-modulo-ledger 验收、mypy 87 棘轮只降不增。
- 独立评审走 canonical runner（`make independent-review-gate REVIEW_BASE=<pinned SHA> …`，
  `INDEPENDENT_REVIEW_GATE.md` §Codex Reviewer Command）：runner 拉起独立只读 Codex app-server
  session（模型/effort 继承全局配置 = 最新模型最高档），异步非阻塞、不占开发 session；批 settle
  后由任一方（含本设计作者的后续 session）用 pinned scope 发起即可。设计(Fable)/实现(Codex)/
  评审(独立 runner session) 三方分离，分歧点写进对应批的决策卡，由 owner 裁决。
- 批级细化设计（本文 §2 的下一层展开）：D0+D1 见 `TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md`；
  D3 见 `TRACK_D_D3_COMPANY_IDENTITY_SELF_VERIFICATION_DESIGN.md`。

## 5. Owner 决策点汇总

- **TD-1**（已裁决 2026-07-13：owner 接受建议）ModelClient HTTP 栈 = requests + SSE 行解析，
  不引 async 栈（C3 后 revisit）。
- **TD-2**（已裁决 2026-07-13：owner 接受建议）D3 切片 = durable operation 内嵌 plan 流水线，
  不等 D2。
- **TD-3**（已裁决 2026-07-13：owner 接受建议）D2 poll-first 先行；SSE 与 C4 一次做，
  不做两套推送。
- **TD-4**（**待裁决**）agent loop 的模型路由：CRM public web 已锁 `gpt-5.6-sol`
  （`model_provider.py:33` fail-closed）；planner loop 用哪个 provider/model、simulate 训练场
  配置，需要 owner 定路由表（建议与 `INDEPENDENT_REVIEW_BRIEF.md` 的模型路由表同表管理）。
- TD-5/TD-6（D3 批内决策点）已裁决，见 `TRACK_D_D3_COMPANY_IDENTITY_SELF_VERIFICATION_DESIGN.md` §6。
