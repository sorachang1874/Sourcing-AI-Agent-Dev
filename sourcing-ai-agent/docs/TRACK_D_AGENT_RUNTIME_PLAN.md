# Track D — 强 Agent 化执行计划（跨模型设计输入）

> Status: Cross-model design input for owner review（v2 2026-07-13，作者 = Claude Fable 5；只规划、不改码）。
> **v2 修订记录**：v1（`23a2b05`）经 gpt-5.6-sol pinned reference review 得 NO-GO（24 findings，
> 提取件 `runtime/reviews/20260713T112818Z_*.extracted-reference.md`，因 runner 与 codex 0.144
> 多线程协议代差，正式 artifact fail-closed 为 invalid_transport、仅作参考输入）；v2 已按
> findings 修订，且全部代码级断言经 8 路并行独立核查（28 confirmed / 1 partial）后才采信。
> 定位：为 Track D 提供第二模型家族的独立设计视角，供接手实现的 GPT-5.6(Codex) 部分复用或反驳；
> 本文**不是任何 scope 的 GO**——实施批次仍逐批走 `INDEPENDENT_REVIEW_GATE.md`。
> 配套阅读：`SERVING_EXECUTION_NORTH_STAR.md`（已 ratified，支柱 4）、`AGENT_OPERATION_CONTRACT.md`
> （W7→W8→W9→W10→Phase 13 阶梯）、`TRACK_C_SERVING_RUNTIME_PLAN.md`（决策点 (d)：C5 并入 Track D）。
> 行号锚点基于 2026-07-13 HEAD（`5f14ed8`）核查，实施前按惯例重新 Scout。

## 0. 基线修正：Track D 不是「从零建 Agent」

`NEXT_TODO.md` 把 Track D 记为四项全空，低估了已有地基。2026-07-13 代码勘察 + 独立核查结论：

**已存在且 active（W8/W9）**：
- `agent_actions` / `operation_runs` / `operation_events` PG-only 持久化（`migrations/0001_baseline.sql`）。
- `operation_runtime.ActionRegistry`（`operation_runtime.py:103`）：action→owner 映射、approval/budget
  fail-closed 提交门（缺预算即 raise，`:667-684`）、`display_contract_for`、注册期 activity-spine 校验
  （`:121-134`）。
- 完整 poll 控制面 API（`/api/operations/*`：submit/approve/reject/cancel/retry/resume/dispatch/provenance）。
- W11a–i 类型化 acquisition 命令链（intent.resolve→plan.build→plan_review→plan.commit→probe→scale→
  discovery→profile fetch→projection admission，全链 Command→ActivityRun→Attempt→EntityDelta 证据化）。
- HITL 原语：`plan_review_sessions`（pending/ready 二态，`storage.py:3187/:3196`，pending 即阻塞）+
  `workflow_recovery_intents` 单赢 claim。
- 异步任务信封：`async_task_contract.py`（202+task_id→poll→artifact ref）。

**真正缺的（Track D 的实际范围）**：
1. ModelClient 无 streaming、无 native tool-calling（`model_provider.py` 零命中；14+3 方法全部
   单发阻塞）；「结构化输出」是 prompt-instructed JSON。
2. `agent_sessions` / `agent_turns` / `agent_events` 三表不存在（勿与 job 内 specialist-lane 的
   `agent_runtime_sessions`/`agent_trace_spans` 混淆）。
3. API 无 SSE/websocket；进度全靠轮询。
4. 服务端 agent planner loop 不存在（W9 控制面明文不执行 owner side effects）。
5. `model_native_search` 只有保留 id + fail-closed 守卫，无实现。
6. **（v2 新增）action 输入 schema 无事实源**：`ActionSpec` 无 payload schema 字段（`:59-75`），
   `to_record()` 不导出（`:241-280`），`command_type_manifest()` 无命令 payload schema
   （`durable_runtime.py:665-693`），`submit_action` 对 `input_payload` 零校验（`:660`）——
   Agent 工具面的参数契约必须先补这块地基（→ D1）。
7. **（v2 新增）产品侧模型路由注册表不存在**：现有 `model_fallback_*` 是 fail-closed 证据而非路由；
   产品模型按消费方钉死（CRM 锁）；无 use-case→route 的产品级注册表（→ TD-4，live 硬前置）。

**关键结构性事实（v2 设计的地基，均已独立核查）**：
- **plan review gate 看不见公司身份置信度**：`required_before_execution` 只由 open questions /
  google-scope / former-employee / investor 四处置 True（`plan_review.py:54/:58/:78/:82`），身份解析
  发生在 acquisition 执行期（gate 之后），低置信 heuristic identity 今天直接 `ready` 执行、无人工
  检查点。修复必须把身份解析（廉价确定性分支）前移到 plan 期（→ D3）。
- **执行期已存在一条活的身份 search+judge 路径**：`acquisition.py:970-1051` `_resolve_company` 低置信时
  调 `_discover_company_identity_candidates`（`:4078-4100`，走配置的 search provider）+
  `judge_company_equivalence`，结果**不持久化**——每 job 重解析、重付费。
- **现成交接通道**：`resolver=="manual_review_override"` 会关闭上述付费分支（守卫 `:986-990`）——
  D3 以同机制加 `agent_self_verified` 通道即可「以消费代退役」，保持 provenance 诚实。
- 工具面种子已就绪但未 serve：`command_type_manifest()` 仅测试引用；
  `/api/workflow/command-registry` 从同源 `DEFAULT_COMMAND_OWNER_REGISTRY`（`durable_runtime.py:809-811`）
  重组字段而非复用 manifest 序列化。

→ Track D 的正确表述：在已 ratified 的 durable 基座上补五块（模型 IO、输入 schema 地基、工具面
serve、会话/事件层、planner loop），用一个垂直切片证明闭环。

## 1. 设计原则（继承既有裁决，不另起炉灶）

1. **Agent turn = durable typed command**（北极星支柱 4）：`agent.turn` 是 CommandTypeSpec，跑在既有
   event/reducer/outbox + lease + recovery 基座上；**不建第二 runtime、不建第二 reducer**。
2. **Agent 不是 data owner**：loop 的所有 effect 只能是 AgentAction→owner command；工具面 =
   ActionRegistry allowlist 中**可 dispatch 子集**的投影（v2 修正：注册 ≠ 可执行，见 D1）。
3. **fail-closed 与预算前置**：每个付费步骤走既有 approval/budget 原语 + v2 新增的可执行预算信封
   （维度化、原子扣减）；live 模型调用在 typed owner + TD-4 路由表就位前 fail-closed。
4. **deterministic-first 前门不动**：`intent_brief` 保持确定性；对话化只加层。
5. **characterize-first + 变异自检**：动 ModelClient 前先钉死 14+3 方法 + **三种** payload 形状
   （chat / responses / Qwen，v2 修正）。
6. **（v2 新增）人机 provenance 永不混淆**：机器验证结果与人工确认走不同字段/不同 resolver 值，
   机器结果永不写入人工字段冒充人工决定。

## 2. 阶段划分

### D0 — ModelClient v2：streaming + native tool-calling（详设 v2：`TRACK_D_D0_MODEL_TOOL_RUNTIME_DESIGN.md`）

- 交付一个**低层能力面（非 normal path）**：`run_tool_turn`（buffered，canonical `ToolTurnResult`）
  为主契约，streaming 为传输优化；**live tool-calling 在 typed model-turn owner（D2 的 `agent.turn`
  owner 或 D3 类命令 owner）+ TD-4 路由表就位前 fail-closed**——D0 自身只对 simulate/scripted 开放。
- 能力矩阵显式化（v2 修正 v1 的自相矛盾）：OpenAI-compatible **chat_completions only**；
  `openai_responses`/Qwen/deterministic 一律网络 I/O 前 `ToolCallingNotSupported`（沿用
  `_call_prompt_result` 对未知 api_style 的 fail-closed 先例，`model_provider.py:1837-1842`）。
- 每个付费流式调用带 `ModelTurnExecutionContext`（因果 id + 审批/预算信封 + idempotency key +
  route_id）；缺 context 即拒 live。低层 helper 在 `requests.post` 前**再次**调
  `assert_live_provider_access_allowed(payload=...)`（把 `RUNTIME_ENVIRONMENT_ISOLATION.md` 的
  低层门规则显式扩展到模型中继——该契约现文只列了搜索/抓取类 provider，本批含契约文档更新）。
- 14+3 既有方法逐字节等价 facade；24 个消费模块零改动（实施 Scout 时钉死枚举清单）。

### D1 — 工具面 serve（并入 D0 批或紧随，详设同文档 §3）

- **先补地基**：`ActionSpec` 扩展版本化 `input_schema`（单一事实源；提交路径同 schema 校验——
  顺带修复 `submit_action` 零校验），planner ToolSpec / API serve / dispatch 三方消费同一份。
- serve 的是 **`agent_tool_enabled` 子集**而非 ActionRegistry 全集（v2 修正）：schema 在 +
  dispatch adapter 在（dispatch 集合从 registry 派生，替换 `orchestrator.py:47091-47096` 的硬编码
  字面集合）+ activity-spine/agent_callable 校验过 + simulate dispatch preflight 通过。当前
  `plan_acquisition`/`promote_person_assertion`/`external_intake` 均不可 dispatch（12/15），不 serve。
- 守卫改为语义断言（v2 修正 v1 的空断言）：对每个 served command 断言
  `activity_spine_policy.requirement != legacy-internal` 且 `agent_callable`（复用注册期校验）。
- command 级字段复用 `command_type_manifest()` 序列化，不再第三处重组。

### D2 — Agent 会话与事件层（与 C4/C5 合流）

- 新表 `agent_sessions`/`agent_turns`/`agent_events`（migration 0002+）；turn = `agent.turn`
  CommandTypeSpec；per-session 单写者 lease。
- **（v2 修正）`agent_events` 不是第二事件源**：每行带物理因果列（workflow_event_id /
  workflow_command_id / operation_run_id / turn_id / step_id），terminal 真值属 workflow 事件/命令表，
  `agent_events` 是 turn 粒度的投影/明细表，**禁止独立推进 workflow 状态**；poll/SSE 消费同一有序
  outbox（与 C4 同设计一次做）。
- loop 预算：turn 级 max_steps/max_cost/max_wall 超限 fail-closed 转 HITL；每步落 `agent_events`。
- 依赖：poll-mode 单进程版可先行；C2 身份/C3 拆进程是**多用户 hosted 激活**前置（见 §3 v2 拆分）。

### D3 — 第一垂直切片：公司身份自验证 loop（详设 v2：`TRACK_D_D3_COMPANY_IDENTITY_SELF_VERIFICATION_DESIGN.md`）

- v2 要点（全部为 review findings 的结构性修复）：
  - **接进 W11 命令图**：`acquisition.plan.build` 在身份置信 < high 时计划子命令
    `company.identity.verify.evidence`（同 OperationRun 因果组），`acquisition.plan_review.request`
    消费其 terminal 结果；legacy plan 前门在 plan 期做廉价确定性解析 + 读同一 current-state 读模型。
  - **身份专属 gate reason**（TD-7 (a) 起步）：`company_identity_unverified` reason 以 OR 语义并入
    `required_before_execution`，只清自己、不清别人——**这修复了今天低置信直接执行的真空洞**；
    Phase 1 shadow（人一键确认+证据卡）→ 统计达标 + owner GO 后 Phase 2（verified_auto 自动清 reason）。
  - **「人永远赢」机制化**：`decision_generation` 单调递增 + 机器结果 CAS 接受 + 人工确认原子
    supersession；晚到结果记 `superseded_not_applied`。
  - **provenance 三通道分离**：人工 override（现字段不变）/ 机器验证（新 `agent_self_verified`
    通道，同样短路执行期付费分支）/ 机器提案（仅证据卡）。「以消费代退役」执行期 search+judge 路径，
    invocation-count preflight 证明单 owner 单付费路径。
  - **两档预算拆成两个命令**（静态 ActionSpec 约束下的正解）：`verify.evidence`（fetch+judge，
    非零成本、显式小信封、免审批）与 `search.expand`（付费检索，TD-5 的 plan 级 3 次预授权信封
    原子扣减，超出转 approval_required）。
  - 新版本化裁决 schema（`identity_relation` 含 parent/suborg 枚举 + 证据 refs + provider/model/usage/
    fallback_status——fallback 非 none 永不 auto-confirm）+ 确定性接受谓词前置于模型置信。
- 不依赖 D0 tool-calling、不依赖 D2、不依赖 model_native_search；与 Track C 可完全并行。

### D4 — 之后（本文只圈定，不展开）

plan review 对话化；intent→plan 前门流式化（依赖 D0+C4）；`model_native_search` 转正（按其 contract
的 owner 矩阵 + A/B + review GO，独立批）；Option 3 全事件流 recovery driver（D2 事件层成熟后）。

## 3. 与 Track C 的并行边界（v2：区分「实现依赖」与「hosted 激活依赖」）

| D 批 | 实现/测试依赖 | **hosted 多用户激活依赖**（v2 新增列） | 主要触碰 | 与 C1–C4 冲突面 |
|---|---|---|---|---|
| D0 | 无 | TD-4 路由表 + typed owner（否则 live fail-closed） | `model_provider.py`（+新模块） | 无 |
| D1 | 无 | C2（token 鉴权覆盖新路由） | `api.py` 新只读路由 + `operation_runtime.py` | 低（分文件段错峰） |
| D3 | 无（simulate/scripted 全链可跑） | C2 身份 + C3 worker 隔离 + TD-4 + `skip_plan_review` 限 operator | `connectors/plan_review/acquisition` + 新 owner | 低 |
| D2 | C4（SSE 完全体）；poll-first 版无 | C2+C3（= C5 完全体） | migrations + 新模块 | 与 C5 同一件事，合流 |

**推荐顺序不变：D0 → D1 → D3（poll-mode）→ D2（与 C4 合流）→ D4。**
付费、user-private 的 operation 在 C2 可信身份前不做 hosted 多用户暴露（防越权创建/窥视/花费）。

## 4. 评审与纪律

- 每批照 handbook 式协议：characterize-first、scripted 回放 A/B（**语义结果等价**而非事件逐帧等价，
  v2 修正）、变异自检、green-modulo-ledger 验收、mypy 87 棘轮只降不增。
- 独立评审走 canonical runner（`make independent-review-gate REVIEW_BASE=<pinned SHA> …`），异步
  非阻塞。**已知障碍**：runner 的单 thread transcript 校验与 codex 0.144 多线程子代理协议存在代差
  （2026-07-13 三次实测：模型侧评审完成、artifact 一律 fail-closed invalid_transport）——runner 升级
  属 trust infrastructure 变更，由 Codex 侧走正常流程修；修复前评审内容可人工提取为 reference 输入
  （先例：`runtime/reviews/20260713T112818Z_*.extracted-reference.md`），但不构成任何 gate 的有效证据。
- v1→v2 评审闭环记录：v1 NO-GO（24 findings：3 critical/17 high/3 medium/1 low）→ 29 条代码断言
  独立核查（28 confirmed/1 partial）→ v2 逐条修复（两份详设文档各附 findings 覆盖映射表）→
  v2 重发 pinned review。
- 设计(Fable)/实现(Codex)/评审(独立 runner session) 三方分离；分歧写决策卡，owner 裁决。

## 5. Owner 决策点汇总

- **TD-1**（已裁决 2026-07-13：接受建议）HTTP 栈 = requests + SSE 行解析，不引 async 栈。
- **TD-2**（已裁决 2026-07-13：接受建议）D3 = durable operation 内嵌 plan 流水线，不等 D2。
  v2 具体化为 W11 子命令 + plan 期廉价解析前移。
- **TD-3**（已裁决 2026-07-13：接受建议）D2 poll-first 先行；SSE 与 C4 一次做。
- **TD-4**（**待终审，live 硬前置**）产品侧模型路由注册表：v2 已在 D0 详设 §4 给出
  `ModelRouteRegistry` 草案（route_id→provider/model/api_style/能力/预算类/simulate 映射/
  fail-closed fallback/rollout_state；调用方只传 route_id）。**与 review gate 的 reviewer 路由表
  彻底分离**（v1 建议同表管理是错的，reviewer 表是 operator 基础设施，耦合会让评审配置改变产品
  行为）。任何 live D0/D3 模型调用在 owner 批准初始路由表前 fail-closed。
- **TD-5**（已裁决 2026-07-13：暂按 3 次/plan，内部产品软默认非硬限，可配置倾向宽松）v2 落成
  可执行信封：plan 级预授权、维度化（searches/fetches/model tokens/wall）、attempt 创建同事务
  原子扣减、跨 retry/resume 不重置；未来模型原生 Search 扩展按 D4 转正批，信封结构已兼容多后端。
- **TD-6**（已裁决 2026-07-13：接受建议）v2 具体化：EntityDelta 存有界摘要 + artifact ref；
  current-state 读模型由验证 owner 物化（兼修「身份从不持久化、每 job 重付费」）；
  `company_evidence` 若写必经其既有 owner。
- **TD-7**（新，owner 2026-07-13 批准按推荐执行）auto-confirm 安全边界 = 分阶段：Phase 1 shadow
  （身份低置信即 block + 人一键确认，**比现状更严**）→ 统计门（零误确认 + 分母下限 + 上置信界）
  达标且 owner GO → Phase 2 verified_auto 自动放行；Phase 2 开关本身是 owner-gated 配置。

## 6. v1 评审 findings 处置总索引

3 critical + 17 high + 3 medium + 1 low 全部 addressed：D3 相关（#1-4、#15-19、#21、#24）见 D3
详设 v2 **§10** 覆盖映射表；D0/D1 相关（#5-14、#22-23）见 D0 详设 v2 §6 覆盖映射表；
#20（agent_events 二源歧义）在本文 §2 D2 修复；#9（路由表归属）与 #21（激活依赖拆分）由本文
§5 TD-4/§3 与两份详设联合修复。v2 定稿前经 24 路逐条覆盖度审计（22 addressed/2 partial 已补）+
跨文档一致性审计（术语统一 `agent_self_verified`、交叉引用修正）。
