# Track D — 强 Agent 化执行计划（跨模型设计输入）

> Status: Cross-model design input for owner review（v5 2026-07-13，作者 = Claude Fable 5；只规划、不改码）。
> **修订史**：v1（`23a2b05`）→ NO-GO 24 findings（提取件 `20260713T112818Z_*`）→ v2（`ffdfa7c`）→
> 有效 artifact `20260713T122908Z_*` NO-GO 16 findings + 1 事实更正 → v3（`656b368`）→ round-3
> NO-GO 17 findings（提取件 `20260713T125255Z_*`）→ v4（`4745e9c`）→ round-4 有效 artifact
> `20260713T131447Z_*` NO-GO（阻断集收窄到 6 条新 findings，re-raise 明示不构成裁决依据，
> 3 项按校准转实施批义务）→ v5（本版：单写者拆分、workspace 键恢复、expiry 去读者化、grant
> 生命周期、槽围栏、共享调用契约、成本分账；义务清单见 §6）。
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
- **执行期已存在一条活的身份 search+judge 路径，且已有文件级持久化**（v3 事实修正——v2 的
  "不持久化"说法错误，re-review 抓出）：`acquisition.py:970-1051` `_resolve_company` 低置信时调
  `_discover_company_identity_candidates`（`:4078-4100`）+ `judge_company_equivalence`，成功后写
  `identity.json` 并更新**全局文件注册表** `company_identity_registry.json`（`:1017-1031`），后续
  解析经 `company_registry.py:210-234,554-594,641-681` 回读。问题不是"没持久化"，而是该注册表
  **文件制、全局共享、无版本无租户无 generation**——D3 的 PG 读模型必须成为 canonical 并显式
  迁移/降级它（否则双持久 owner，见 D3 详设 §4）。
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

- **先补地基**：`ActionSpec` 扩展为版本化 **`ActionRequestSpec`**（v7 与 D0 §3.1 同步——schema
  同时覆盖 `input_payload` 与 `target_ref`，target 服务端绑定防旁路；单一事实源；提交路径同
  schema 校验，顺带修复 `submit_action` 零校验），planner ToolSpec / API serve / dispatch
  三方消费同一份；schema 版本+digest 作为不可变 pin 落 durable 对象。
- serve 的是 **`agent_tool_enabled` 子集**而非 ActionRegistry 全集（v2 修正）：schema 在 +
  dispatch adapter 在（dispatch 集合从 registry 派生，替换 `orchestrator.py:47091-47096` 的硬编码
  字面集合）+ activity-spine/agent_callable 校验过 + **已注册 revisioned `model_safe_result_schema`
  在位（R7#10 同步 D0 谓词第 4 条）** + simulate dispatch preflight（含行使 model-safe 序列化器）
  通过。当前
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
  outbox（与 C4 同设计一次做）。**（v3 补全存储契约）**三表 PG-only（无 SQLite 路径）；
  `agent_events` 带 per-stream `sequence_number` + `idempotency_key`（形态照抄 `operation_events`）、
  workspace 租户列与授权 scope；投影 ownership = turn owner 单写者；可重建（从 canonical workflow
  事件重放恢复，重放语义随表 DDL 声明）；fast preflight 断言 terminal 真值只存在于 canonical
  workflow 状态表。
- loop 预算：turn 级 max_steps/max_cost/max_wall 超限 fail-closed；D3 使用详设的 closed typed map（可补 grant 的
  step/cost/search-envelope exhaustion=`awaiting_budget`，hard no-more-grant policy exhaustion=`needs_human`，
  execution failure=`failed`，deadline/max_wall=`timed_out`），其他 slice 在自己的 owner contract 中映射 HITL；
  每步落 `agent_events`。
- 依赖：poll-mode 单进程版可先行；C2 身份/C3 拆进程是**多用户 hosted 激活**前置（见 §3 v2 拆分）。

### D3 — 第一垂直切片：公司身份自验证 loop（详设 v2：`TRACK_D_D3_COMPANY_IDENTITY_SELF_VERIFICATION_DESIGN.md`）

- v4 要点（与 D3 详设 v4 严格同词汇，历经三轮评审收敛）：
  - **事件驱动 W11 接入（review-first，v8 与 D3 详设逐字对齐）**：plan.build 结果事件 → reducer
    计划 plan_review.request → session 先创建并产 canonical `review_id` → strict OperationRun 固定 pin
    `coordination_plan_review_id=plan_review_sessions.review_id`（same positive `BIGINT`；brownfield `NULL`、strict
    `>0`，禁止 `TEXT`/empty）；baseline review row 没有 scope，所以 strict session 必须先由 private scoped-session
    owner 从 server runtime + authenticated workspace 写 namespace/mode/workspace/issuer/digest，OperationRun 锁后
    exact-copy scope/id，legacy unscoped session 不可采用；command exact-copy、authority/receipt bind →
    reducer 才计划 `company.identity.verify.evidence`
    → 验证 terminal 事件**判别化路由**（active grant 下的 `evidence_insufficient` 只计划 search.expand；暂无
    grant 但 policy 允许补发=`awaiting_budget`；hard envelope/policy 禁止再 grant 或 semantic/protocol-invalid=
    `needs_human`；non-retryable execution failure=`failed`；deadline/max_wall=`timed_out`；均先由验证 owner
    规范化为 typed `final_adjudication(record_outcome=awaiting_budget|failed|timed_out|needs_human)`；授权结果为
    `final_adjudication(record_outcome=authorizable)`；**reducer 仅从 final_adjudication 计划 record**）→
    唯一 command type `company.identity.verification.record`（验证 owner，exact map：authorizable→
    verification `shadow_would_verify`/intent `applied`；awaiting_budget→`pending`/`awaiting_budget`；
    needs_human→`needs_human`/`applied`；failed→`failed`/`applied`；timed_out record outcome→
    `timed_out`/`applied`，每项发 `company_identity_verification_recorded` typed discriminant event）/not_applied【仅 stale 或 business 围栏失配的 typed
    零持久写返回，不落域事件】）→ recorded 域事件
    （仅授权与非授权分支触发）→
    reducer 计划 `plan_review.identity_result.apply`（plan review owner 写 gate，watermark 单调，
    阻塞方向恒占优）；commit owner 同事务复查 canonical 验证行；legacy 前门同一共享 helper。
    `awaiting_budget` record UoW 只写 awaiting intent + `recorded_event_id`/domain event 并 terminalize；随后 gate
    owner UoW 在 gate→all-command rows 的全局顺序中先 reserve/lock
    `resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>`，再原子推进 watermark 并以
    post-gate-apply typed pins 创建 command/outbox（active grant=`queued`，否则 `retry_wait`）。gate-event owner/grant
    owner 共用 `maybe_plan_resume`；identity 包含 recorded_event_id、不含 grant/瞬时 delivery。grant-first
    由 gate apply 看见 durable grant，record-first 由 later grant reawaken，关闭 one-shot gap/old gate-digest collision。
    control timeout 单独映射 intent=`timed_out`、verification=`needs_human` + control event，不能冒充 record outcome。
  - **身份专属 gate reason**（TD-7）：`company_identity_unverified` OR 组合、只清自己；Phase 1
    shadow（`shadow_would_verify` 非授权态 + 人一键确认）→ 统计门 + owner GO + 逐行 revalidation
    + promotion 命令 → Phase 2（`verified_accepted`）。**修复今天低置信直接执行的真空洞**。
  - **「人永远赢」**：workflow registry/scheduler 只在 exact-command selection 后由 private factory 单次铸造
    不可由字符串/公共 payload 构造的 one-use `ClaimAuthority`（selection generation/repository expiry/atomic
    consumed id + 精确 owner/type/scope/worker/lease/control epoch）；wrapper 无启动期铸造/standing capability。
    selection UoW 先证明旧 execution lease/selection reservation absent/expired，再持久化 exact unexpired
    reservation；Stage A 必须匹配该 reservation 并以 execution lease 覆盖它，不能要求 lease absent/expired，且
    只有原子消费 authority 后才推进 claim generation、轮换 token verifier 并返回 current `ClaimIdentity`。
    strict registry entry 将 `claim_fence_policy=d3_v1`、`allowed_stage_ids`/stage policy 纳入 authority digest；
    Migration C 从 registry 分别生成 hash-bound `scoped_session_bootstrap_command_types_v1` 与
    `strict_d3_command_types_v1` literal manifests，并以 immutable command type 做各自物理 population guard，禁止
    用 digest sentinel 判别。authority 的 `expected_stage_id` 只来自 exact selected row。normal authority 另绑定
    pre-claim `workflow_commands.attempt`，Stage A 原子 `+1` 后 ClaimIdentity/ActivityAttempt exact-copy post-claim
    attempt。verification intent 绑定 **source verification command** 的 immutable core generation/control epoch/source command attempt +
    append-once terminal status/event/digest；后继 record command 有独立 current claim，record owner 单 UoW
    exact-compare current source row。requeue successor 由下一 Stage-B 与 async supersession
    共享 physical predecessor generation/control-source-event CAS 后创建；initial Stage B 的 predecessor tuple
    全 sentinel 且证明无 current phase，successor tuple 全非空，half-sentinel fail closed。人工/重编译/所有
    cancel、D3 OperationRun terminal/cancel/retry/requeue/resume/reset/rebuild/recovery 与 dispatch 共用 `d3-dispatch-v2` 有界
    coordination lock，key = immutable scope tuple + exact `coordination_plan_review_id`（root intent 不参与），
    唯一行锁序为 operation root → optional plan/review/gate → **all participating workflow_commands**
    （source/record/resume/supersession/current owner/idempotency target，确定序）→ intent/predecessor →
    ActivityRun/Attempt → optional grant/cost；进入 intent 后不得回头 insert/lock command，未触及 aggregate 只可
    跳过、不可逆序。四项 predecessor pins 是 `workflow_commands` typed nullable physical columns，只允许 all-null
    initial role 或 complete successor CAS，禁止 JSON/half-null。`workflow_commands.d3_business_fence_digest` 是
    canonical `d3_business_fence_v1` 的 immutable digest；closed typed context union 明确 claimed-command
    `stage_b|terminal|record|dispatch|resume_after_grant` 必须带 authority+receipt，aggregate `control` 带 registered
    control authority/expected revisions 且无 claim token；六 phase 全部 mandatory、wrong context fail closed，并
    复查 typed plan/review/gate/base-intent + phase manifest/grant/exposure pins（仅 heartbeat/read-only exact replay
    是 command-only 窄例外）。stale application/business mismatch 只返回
    `not_applied(reason=stale_claim|business_precondition_conflict)`，该返回不是 event/state，且 domain/attempt/intent/
    event/command/source/result 零写；只有 authorization 已提交的 in-flight response 才入 shared quarantine
    repository，由 insert-once immutable identity 与 disjoint monotonic cost/retention CAS entrypoints 管理，
    不伪造 command/domain terminal/event。record apply 是 terminal-UoW specialization：同一 PG transaction 执行
    `phase=record` + `phase=terminal` predicates，并原子写 verification+intent、recorded domain event、workflow
    terminal event、ActivityAttempt+command terminal pair；crash 全回滚/exact replay 全 aggregate，绝无 applied intent
    + running command gap。response-backed terminal exact-bind committed exposure/physical call/provider call/
    ModelInvocationEnvelope/result occurrence+digest，且 result-ref 唯一派生 receipt artifact ref；checked-in
    `TERMINAL_PROVENANCE_SPECS` 以 `TransportResponseSpec|TransportAttemptFailureSpec|NoExposureTerminalSpec`
    三 variant 唯一拥有 response、attempt-failure 与 proven no-send 语义；applicability key 包含
    command/stage/terminal transport variant/status/event/outcome、response reason 或 failure code。其唯一 applicable entry 集合的 digest
    在 command/exposure 创建时经 authority/business pins 冻结；已 pin 的历史 policy 不得被 deployment 重新解释，
    且保留到没有 active/retained command、exposure、receipt、event、source-intent、quarantine/tombstone 或
    cost/audit 引用。
    committed-exposure pre-response failure 走 typed `TransportAttemptFailureReceipt` + `attempt_failure`；valid
    response envelope 的 `terminal_reason=length|content_filter` 仍是 response receipt（只影响 adjudication 接受），
    只有 incomplete/truncated wire 或 protocol parse failure 才是 attempt failure。registered non-transport 或
    proven pre-call/no-send 由 `NoExposureTerminalSpec` 授权 complete-none `no_exposure`，并在 common coordination/
    command lock 下证明 exposure 缺席，不能锁一条必须不存在的 exposure。provider delivery id 或 durable inbound
    `TransportResponseReceipt` get-or-create stable response occurrence；redelivery 复用，digest mismatch collision；
    retry transition 也锁 exposure：response receipt 先赢则拒 retry，retry 先赢推进 epoch，后到 response 只可
    receipt+quarantine。post-network 只承接已授权 exposure，固定 exposure→receipt→quarantine/cost-axis order，绝不
    回到 operation/command/intent/domain，且不授权 send/apply。
  - **provenance**：机器验证 = 服务端引用（公共 ingress 禁携带）；人工 override 现字段不变；
    执行期付费 search+judge 路径「以消费代退役」+ **既有全局文件注册表收编为 PG canonical**
    （inventory 全部 4+ 写入方/快照重扫、backfill=needs_human、迁移桥+删除条件、precedence preflight）。
  - **审批与预算**：Tier-1 小信封免审批；Tier-2 检索预算 = review 卡上的类型化部分决定
    `identity_search_budget_grant`（TD-5 默认 3 次/次授予，可配置），**与 plan 终审解耦**（终审
    前置 = 全部 blocking reasons 清除，commit owner 原子复查）；每次物理 provider 调用各记一笔 +
    worst-case 预留 + 对账。
  - 裁决 = 模型半（只引用 evidence_ids，owner 服务端 resolve 证据 provenance）+ 服务端调用信封
    （身份/usage/fallback 模型不可自证）；确定性接受谓词七条前置于模型置信；统计门 = 零误确认 +
    分母 ≥120。
- 不依赖 D0 tool-calling、不依赖 D2、不依赖 model_native_search；与 Track C 可完全并行。
- D3b round4 fresh local advisory=`NO-GO`（P0/P1/P2/P3=`0/3/2/0`），独立 semantic audit=`NO-GO`
  （`0/5/4/0`）；round5 semantic=`NO-GO 0/5/2/0`、fresh broad=`NO-GO 0/3/3/0`；round6
  semantic=`NO-GO 0/1/2/0`、broad=`NO-GO 0/4/0/0`；round7 semantic=`NO-GO 0/4/0/0`、
  broad=`NO-GO 0/4/2/0`；round8 semantic=`NO-GO 0/1/0/0`、broad=`NO-GO 0/6/1/0`，全部只作
  fixed-forward 输入而非 formal verdict。两次 round9 local advisory 因 Codex operator usage limit 在 final
  response/artifact 前终止，故没有 verdict；直接 partial findings 已 fixed-forward。current author
  evidence=`32/49/58/81` + diff clean；fresh non-author local re-review 与 pinned formal review
  pending，仅对 Live/signoff fail closed，不冻结后续 non-live batch；served
  Agent tool population=`0`，R-019、action-root gate 与 OB-10.1/10.2/10.3/10.4 继续 open。
- D3c1 public projection baseline 已把 command public shape 锁成 33 descriptor + 2 safe
  diagnostics + 7 derived = **42** 个 optional fields，把 `operation_sync` 锁成 **9** fields，并明确
  `WorkflowCommandRecord` schema refs=`6 existing + 1 typed nested = 7`；七个 raw nested return 已降为 **0**，
  carrier inventory 修正为 **16 routes/17 method-route variants**。递归 sanitizer 覆盖 command、operation
  action/run/event、provenance 与 execution summary，frontend adapter/demo 不再保留 raw command dict。author
  evidence=`7/45/14`、frontend build `81 modules`、lint `58 files`、mypy `81/4` + diff clean。首个 pinned formal
  attempt 的 substantive output=`0/2/3/0`，但 causal-binding verifier 将 artifact 判 invalid；它没有 formal
  verdict，原 candidate 不再是当前评审候选。
- D3c1a current fixed-forward candidate 针对上述五项 direct findings，把 ActivityRun/ActivityAttempt/EntityDelta
  与 control target 收敛为 backend-owned closed projection（`23/27/26/10` fields），以 normalized snake + compact
  root-prefix 封闭私有 aliases，禁止 generic carrier 携带 caller-supplied `execution_summary`，并把 future
  diagnostics/Activity attempt number 限于 canonical non-negative JavaScript-safe mathematical integer。
  后续 precommit adversarial author-audit 另固定结构化 Activity `artifact_refs`、cancel/retry/resume
  **4 singular + 3 served plural** carriers + derived control fields、command `result` 内 **8 canonical + normalized aliases** recursive
  carrier keys、
  `1.0 -> 1`/`-0 -> 0`，并继续封闭 nested command/
  operation-sync/direct-Activity nested-command closure、recursive forged-summary removal、hazardous-key rejection、
  backend/frontend typed malformed-input parity、generic Activity derived-provenance stripping、compact-observation
  canonicalization、完整 Operation action/event/run 与 registry mirrors、typed trusted execution summary、normalized
  response-envelope dual-source、demo raw/provenance 与 strict required-status wrappers，并以 independent
  descriptor/literal oracles 替代 self-referential-oracle gap；这些不是 invalid artifact 五项的
  retroactive findings，也不构成 formal review evidence。primary manifest 现在是 **23 method-route variants**（既有
  17 + Activity 6），
  compact materialization 独立计数。commit `4cfd1916da8bd98483d1ecfdba1f66639b122da9` 的 author evidence=
  `13` D3c1a、`56` D3+durable、
  full operation `129`、pre-Agent `4`、Activity HTTP/route parity `2`、adjacent migration/PG `9 + 20 subtests`、
  frontend compile+`81 modules` build、lint `58 files`+D3 tests+pre-Agent oracle、mypy `81/4`、diff clean。
  后续 pinned artifact `runtime/reviews/20260714T215839Z_Track_D_D3c1a_workflow-command_public_projection_fixed-forward.md`
  因 `causal_binding.final_response_item_exact=false` 为 invalid/advisory；其 substantive `NO-GO` 不是 formal
  `NO-GO`。#1/#2/#4-#11 已以 exact-built-in bounded copier、single backend/frontend traversal budgets、ActivityRun
  descriptor 与 WorkflowCommand control-target 分 owner current evidence、once-sanitized strict envelope/applied-outcome、
  shared `NumberRecord`、producer-owned policy field families 与
  constructor-only `CommandKernel` fixed-forward；#3=`R-019` 继续 open。该 follow-up stable evidence=
  `18/63/129/60/51`、frontend `81 modules`、public mapper `85/34`、Ruff/diff clean、mypy `81/4`，fresh
  dirty-tree non-author advisory=`0/0/0/0 CLEAN`；pinned review 仍 pending，在 valid scope-matched artifact 前没有
  formal verdict。第二个 artifact `20260714T231243Z_*` 也因 Desktop 只在 rollout `response_item` 追加 terminal
  memory citation 而 `final_response_item_exact=false`，故其 `0/4/4/1` 与 printed `NO-GO` 仍仅 advisory。其
  action-specific outcome、canonical member once-capture/shared traversal、demo bounded/cycle-safe、single outcome
  owner、真实 PG 500-id 1+1 batch/zero-point-read、exact-empty operation-sync 与 mutable-Activity TOCTOU findings
  已 fixed-forward；final integration audit 又闭合 frontend nonempty-projected-empty sync、memoized DAG alias output
  放大、foreign workspace/run provenance 与 whitespace-wrapped outcome。focused evidence=`18 projection + 38
  claim-fence + 6 backend/3 subtests + 2 real-PG + 60 storage + 60 pre-Agent`、frontend build `82 modules`、mypy
  `81/4`、Ruff/diff clean，dirty-tree non-author advisory=`0/0/0/0 CLEAN`。fresh highest-effort pinned review 仍
  pending。该批零
  migration/claim/CAS/Stage A/B/dispatch/served，故 R-019、action-root gate、
  OB-10.1/10.2/10.3/10.4 和 served=0 均不变。
- D3c2a dormant migration candidate 仅安装 `workflow_commands` 的精确 20 columns + 16 个 `NOT VALID` local
  checks，使用 5s transaction-local lock budget；populated legacy sentinel、0003-only timeout 全回滚、恢复后 single
  apply + no-op 已纳入 PG acceptance。descriptor 仍为 33 columns，零 runtime read/write、claim/CAS、Stage A/B、
  dispatch 或 served 激活。这只是 Migration A 的 command-table fragment；其余 Migration A、Migration B-D、
  scope issuer/manifests/registries/bootstrap factory+verifier、terminal/race evidence、R-019、action-root gate、
  OB-10.1/10.2/10.3/10.4 与 served=0 全部 open，fresh pinned non-author review pending。
- D3c2b dormant scoped-root candidate 按 rollout step 2 继续安装 `plan_review_sessions` 精确 11 列与
  `operation_runs` 精确 5 列，以 empty/zero/NULL brownfield sentinel 和合计 16 个 `NOT VALID` local checks
  保护新写；5s lock timeout 证明先执行 session DDL、再被 OperationRun writer 阻塞时，两表 columns/checks
  与 0004 ledger 整笔回滚，恢复后 single apply + no-op。legacy mappers 继续丢弃全部新列，零 scoped
  repository/exact-copy writer/claim/dispatch/served 激活。Activity/event/receipt/quarantine Migration-A fragments
  仍 open；完整 Migration A 前不得进入 rollout step 3 的 registries/manifests/factory。implementation=
  `0aa253c7d7e5324f5c0021570e2980f358ea3922`；valid pinned review
  `runtime/reviews/20260714T222746Z_Track_D_D3c2b_scoped_root_migration_retry_1.md`=`GO`
  （P0/P1/P2/P3=`0/0/0/0`，scope digest
  `7988dd50814ba1c2cc4a3c5efa7ec40ac7eb47b80cbce901ecc8c04a83a92685`）。该 scope-local `GO` 不关闭
  R-019、完整 Migration A 或任何 runtime/live activation gate。
- D3c2c characterization-only batch 机械冻结 current ActivityRun/ActivityAttempt/event 为 `20/22/17` columns，
  `upsert_activity_run=30/5 files`、`upsert_activity_attempt=22/4 files`、`append_event_and_reduce=62/7 modules`；
  ActivityAttempt get 的 raw/external/internal 口径为 `2/1/1`，current `attempt_number` 不得解释为 future
  `command_attempt`。唯一 physical event INSERT owner 与唯一 ActivityRun direct-cancel UPDATE 已锁定；future
  ratified exact verification-intent/response-failure-receipt/late-quarantine/terminal-registry named surfaces 尚未
  出现；durable dispatch exposure owner/table name 为 `unratified/undetermined`，不作 zero claim。
  event→commands→outbox→state 仍是 R-019 multi-commit。下一批必须先 ratify DDL，
  再按 dormant ActivityRun+Attempt → event → intent/receipt/quarantine 顺序推进；本批不设计 schema，不关闭
  R-019/R-023/R-027/R-029、action-root、OB-10.1-10.4 或 served=0。首个 D3c2c pinned artifact 因
  `final_response_item_exact=false` invalid；其 exposure lexical-zero advisory 已 fixed-forward。retry
  `20260714T234336Z_*` 也仅因同一 Desktop response-item annotation mismatch invalid；其 incomplete Migration-A
  order oracle、non-closure exactness 与 stale 7-test evidence 已 fixed-forward，current oracle=`8 passed`，fresh
  fixed-forward `e01e9f0` 的 pinned medium-effort advisory=`GO 0/0/0/0`；formal highest-effort retry pending。
- D3c2d implementation candidate 已据 §5.2/§11.1 ratify 并安装 dormant ActivityRun `6` columns +
  ActivityAttempt `10` columns，以及 `7+11` 个 `NOT VALID` local checks；5s second-table contention 必须回滚
  first-table DDL 与 ledger。existing `attempt_number` 与 future post-claim exact-copy `command_attempt` 明确分离，
  current 20/22-column descriptors 和全部 runtime writers 保持 dormant。event/intent/receipt/quarantine、完整
  Migration A、R-019/R-023/R-027/R-029、action-root、OB-10.1-10.4 与 served=0 继续 open。
- D3c2e decision-lock candidate 将 WorkflowEvent fragment 拆为 exact **11-column terminal-lineage core + 11 local
  checks**，复用 existing `operation_id`/`command_id`/`activity_attempt_id`，禁止 event-side
  `operation_run_id`/`source_*`/`source_command_attempt` aliases；post-claim command attempt 仍由 linked
  ActivityAttempt 拥有并由 verification intent 单独 exact-copy。transport provenance、intent/receipt/exposure/
  quarantine、index/FK/population/adoption/runtime UoW 全部显式 deferred；零 SQL/descriptor/runtime。D3c2f 随后
  仅以 `0006_d3_workflow_event_terminal_lineage_foundation.sql` 安装该 dormant event core：one-table 11 columns/
  11 local checks/5s rollback，17-column descriptor 与 current writer 继续封闭。下一批只能先 Scout/ratify
  remaining Migration-A evidence surfaces。D3c2e commit `1fb052fe...` fresh pinned medium advisory=`GO 0/0/0/0`，
  不是 formal GO。D3c2f commit `47a7f7db...` fresh pinned medium advisory=`NO-GO 0/0/2/0`；两项均为
  stale `0006` 文案/exact static-DDL evidence gap，SQL/runtime 边界通过且 fixed-forward 已落。该
  `316741a...` fixed-forward 的 fresh medium re-review=`NO-GO 0/0/1/0`：原两项关闭，但 oracle 仍接受第 12 个
  validating constraint；every-`ADD CONSTRAINT` ordered-name/total exact assertion 已继续 fixed-forward，
  `68c901a...` re-review 又以 medium `NO-GO 0/0/1/0` 证明 quoted name/alternate whitespace 可绕过 extractor；
  所有 `ADD`/`CONSTRAINT` token 现与 whitespace-tolerant name/full-predicate extraction 独立计数并 exact-compare，
  commit `e7db34e...` fresh pinned medium re-review=`GO 0/0/0/0`，五种 mutation 均 fail closed；
  migration/runtime 不变，formal highest-effort pending。R-019/R-023/R-027/R-029、action-root、OB-10.1-10.4 与
  served=0 不变。

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
- **TD-4**（**已裁决 2026-07-13：owner 批准按推荐落档初始表**，见 D0 详设 §4——
  `agent.planner.loop` 与 `company.identity.adjudicate` 两条均 gpt-5.6-sol / chat_completions /
  fail-closed fallback / **rollout_state=draft**；live 启用 = 逐条 draft→canary，仍由 owner
  拨动）。与 reviewer 路由表彻底分离；CRM 产品模型锁与本表互不引用。
- **TD-5**（已裁决 2026-07-13：内部产品软默认非硬限，可配置倾向宽松）**额度单位 = 每次授予**
  （v7 矩阵审计统一口径：`identity_search_budget_grant` 每次授予默认 3 次检索，plan 生命周期内
  可多次授予——不是 per-plan 总量硬限）；信封维度化（searches/fetches/model tokens/wall）、
  扣减原子、余额经 supersede-with-transfer 跨 retry 继承不重置；未来模型原生 Search 扩展按
  D4 转正批，信封结构已兼容多后端。
- **TD-6**（已裁决 2026-07-13：接受建议）v2 具体化：EntityDelta 存有界摘要 + artifact ref；
  current-state 读模型由验证 owner 物化并成为 canonical（既有全局文件注册表按 §4a 迁移/降级）；
  `company_evidence` 若写必经其既有 owner。
- **TD-7**（新，owner 2026-07-13 批准按推荐执行）auto-confirm 安全边界 = 分阶段：Phase 1 shadow
  （身份低置信即 block + 人一键确认，**比现状更严**）→ 统计门（零误确认 + 分母下限 + 上置信界）
  达标且 owner GO → Phase 2（`shadow_would_verify` 经逐行 revalidation + promotion 命令升
  `verified_accepted` 后自动放行——旧影子行不随开关生效）；Phase 2 开关本身是 owner-gated 配置。

## 5a. 评审循环终局状态（2026-07-13，owner 决策点）

五轮对抗评审（R1 24 → R2 16 → R3 17 → R4 阻断集 6 → R5 阻断集 5，artifact：
`20260713T122908Z/131447Z/133324Z` 三份有效 + 两份提取件）：每轮修复 N 层机制，评审即在 N+1 层
产出新 findings（R5 例：R4 处方的单写者拆分→gate apply generation 围栏、grant 记录→re-grant
连续性、槽围栏→retry ABA）。**文档层面不收敛**——剩余 findings 已属实现级分布式细节，正是各
实施批 characterize-first + A/B + 变异自检 + per-batch gate 的处置对象。
**owner 裁定（2026-07-13）**：Track D plan 不阻塞 Codex（瓶颈在其 Track C 执行），继续打磨而非
exception 收官；同时要求方法论化解决逐层下潜问题。→ v6 起启用
`docs/DESIGN_INVARIANT_CHECKLIST.md`（v2 十类）：作者先做机制×不变量全深度自查，评审改
checklist 驱动单遍扫描；终止规则见该文 §2.3。

**round-7 处置（v8 已全部完成）**：机制类 4 条全修——R7#1 outcome digest + occurrence 身份、
R7#2 apply 全路径 namespace/mode 绑定、R7#3 判别化终态路由 + `awaiting_budget` durable 态、
R7#4 manifest 预创建 + owner 终态化 + record 非授权分支产生 needs_human；簿记类全修——
R7#5/#6/#8/#9 经矩阵 v3 重生成（200 格、含新机制行、OB-ID 稳定编号）、R7#7/#10 文字同步。
TD-4 初始路由表已按 owner 2026-07-13 裁决落档（D0 §4）。

**round-8 终局状态（artifact `20260713T154827Z_*`，NO-GO 7 新阻断 + 6 非阻断 re-raise）**：
八轮总曲线 24→16→17→6→5→11→9→7；**决定性证据 = R8#1/#2 是 v8 为修 R7#3 新引入的
`awaiting_budget` 机制自身的状态机/lost-wakeup 问题**——"每个新修复机制孵化下一轮 findings"
在协议加持下依然重演，散文层不存在不动点。R8 七条阻断（awaiting_budget 状态机一致性、grant
续跑 lost-wakeup、accepted 结果的 crash/lease 接管恢复、manifest 物理隔离、TD-4 表字段实例化、
非授权结果的 EntityDelta 因果、pre-transport 围栏泛化）+ 六条 re-raise **均转为对应实施批的
开工义务**——它们全部是"真实 PG + 竞态测试几小时内可判定，散文继续迭代只会孵化元问题"类。
**owner 裁决（2026-07-13，终局）**：接受 exception（不再发 R9），**设计定稿 = v8**（`660f5a5`）。
按 gate Failure Policy 记录：exception 引用 R8 artifact `20260713T154827Z_*`；R8 全部 findings +
矩阵 OB-ID 义务 + §6 义务清单 = 各实施批的开工义务（batch step 1 处置、per-batch gate 验收）；
本行即 gate 要求的"carried in TODO"记录（NEXT_TODO Track D 同步指向）。实施顺序 D0→D1→D3
（poll-mode）→D2（与 C4/C5 合流）→D4 不变。

## 6. 实施批义务清单（round-4 校准裁定：非设计阻断，随各实施批执行并逐条验收）

1. **注册表/快照全量 Scout**（D3 批 step 1）：round-4 补充点名 `company_asset_supplement.py:1032-1042`、
   `asset_sync.py:1066-1077` 为快照写方，`company_registry.py:641-681` 更正为读方；Scout 产出
   逐点分类清单存档。
2. `agent_events` 精确投影契约（D2 批）：stream 身份公式、source-event ordinal/基数、唯一约束、
   rebuild owner/顺序、cursor 授权、replay parity preflight。
3. 宽松 action-schema 迁移桥启用前，先落 residual 台账行 + NEXT_TODO 条目（D1 批）。
4. tool-schema 版本/digest 在 turn 创建点钉住并贯穿 terminal result/journal → AgentAction →
   approve/retry run（D0/D2 批）。
5. `judge_call_key` 追加 workspace/intent generation/有效路由/schema/policy revision 维度；
   「official domain 归属」的服务端证明规则成文（D3 批）。
6. workflow command claim-fence（migration，D3 批前置；本项没有 OB-ID）：review session 必须先创建，
   **Implementation status (2026-07-15):** D3c1 已先封闭 command public projection，D3c1a current candidate
   fixed-forward Activity spine/private aliases/trusted summary/safe-integer findings；precommit adversarial
   author-audit 又闭合结构化 Activity refs、control **4 singular + 3 served plural** carriers + derived fields、
   command-result **8 canonical + normalized aliases** recursive carrier keys、mathematical integer canonicalization、
   nested workflow-command/operation-sync/direct-Activity closure、recursive forged-summary stripping、hazardous-key
   rejection、typed malformed-input parity、generic Activity derived-provenance stripping、compact-observation
   canonicalization、完整 Operation action/event/run 与 registry mirrors、typed trusted execution summary、normalized
   response-envelope dual-source、demo raw/provenance、strict required-status wrappers、independent descriptor/literal
   oracles。
   `20260714T215839Z_*` D3c1a artifact 因 `final_response_item_exact=false` 仅为 invalid/advisory，不是 formal
   `NO-GO`；其 #1/#2/#4-#11 已按 bounded exact-built-in copier、single frontend traversal、source-exclusive current
   evidence、strict envelope/outcome、`NumberRecord`、producer-owned policy families 与 constructor-only kernel
   fixed-forward，#3=`R-019` 保持 open。第二个 `20260714T231243Z_*` 同样 invalid/advisory；其七项 actionable
   evidence 已通过 canonical outcome/budget manifest、action-specific matrix、once-captured wrapper traversal、bounded
   demo、真实 PG bounded batch、exact-empty sentinel 与 Activity capture-once fixed-forward；final integration audit
   进一步闭合 alias occurrence charging、frontend sync sentinel、foreign lineage 与 exact-string outcome。当前
   targeted validation 已记录；fresh highest-effort pinned review 尚未完成。两批都不授权 runtime write。
   D3c2a 仅安装
   `workflow_commands` 的 20-column/16-`NOT VALID` dormant command subbatch，descriptor 与 runtime writers 未激活。
   D3c2b 接续安装 `plan_review_sessions` 11-column + `operation_runs` 5-column scoped-root dormant subbatch，
   legacy mappers 仍封闭，scoped repository 与 exact-copy writer 均未激活；commit `0aa253c7...` 的 valid pinned
   artifact `20260714T222746Z_*` 为 scope-local `GO`（digest `7988dd50...a92685`），但不关闭 R-019 或后续 rollout。
   这不是完整 Migration A：activity/event/receipt/quarantine fragments、Migration B-D、
   canonical-id 最终 grammar 与本项以下所有 owner/runtime/acceptance obligations 仍 open。
   D3c2c 随后仅完成 current physical surface characterization：ActivityRun/Attempt/event descriptor=`20/22/17`，
   writer call populations=`30/22/62`；ratified exact future intent/receipt/quarantine/terminal-registry named surfaces
   尚未出现，而 dispatch-exposure owner/name 未 ratify、不得以 token heuristic 宣称为 0；详细 oracle 边界见
   `TRACK_D_D3C2C_ACTIVITY_TERMINAL_EVIDENCE_CHARACTERIZATION.md`。因此下一有界 implementation 是先取得
   owner-ratified DDL，再落 dormant ActivityRun+ActivityAttempt fragment；characterization 本身不授权 rollout。
   D3c2d candidate 已完成该 owner decision 并只安装 `0005_d3_activity_claim_chain_foundation.sql` 的
   ActivityRun/Attempt `6+10` columns、`7+11` local checks 与 both-table rollback proof；descriptors/runtime 未切换，
   因此 rollout step 3 仍须等待 workflow-event 与 intent/receipt/quarantine 等其余 Migration-A fragments。
   D3c2e 接着仅 ratify WorkflowEvent exact 11-column core、11 个 local checks 与 existing-link/no-alias mapping；
   transport provenance、verification intent、receipt/exposure/quarantine、index/FK/adoption/runtime 均留在
   D3c2e-D1..D10，零 SQL。D3c2f 已据此仅安装 one-table dormant event-core substrate，descriptor/runtime 未切换，
   仍不允许 rollout step 3；remaining intent/exposure/receipt/quarantine batch 必须先完成 owner-ratified Scout/
   decision lock，不能从 D3b prose 猜 schema。
   canonical coordination lineage 为 `coordination_plan_review_id=plan_review_sessions.review_id`，物理类型同为
   positive `BIGINT`（brownfield `NULL`，strict `>0`，禁止 `TEXT`/empty）；poll-mode 的唯一 scope issuer 是
   private scoped review-session repository：它从 server-owned runtime context + authenticated workspace
   签发 immutable namespace/mode/workspace/issuer/digest，并把
   `creation_source_workflow_command_id/creation_source_event_id/creation_plan_id/creation_plan_revision/
   creation_plan_bundle_digest/creation_idempotency_key` 固定为 durable causal/idempotency identity。该 owner
   以 `(scope_digest, creation_idempotency_key)` scope-aware unique key 与专用
   `scoped-session-bootstrap-lock-v1` transaction lock（key 只含 canonical scope tuple +
   `creation_idempotency_key`，明确不含尚不存在的 review id/gate）在一个 PG UoW 内 create-or-exact-replay session、session-created
   event 与 source command terminal pair。bootstrap command 的 registry policy 是
   `claim_fence_policy=scoped_session_bootstrap_v1`：closed typed
   `ScopedSessionBootstrapContext = ScopedSessionCreateContext | ScopedSessionCommittedReplayContext` 明确不进入
   normal six-phase business union；exact selection 后 private
   `mint_scoped_review_session_bootstrap_authority(...)` 只铸一次
   `ScopedReviewSessionBootstrapAuthority`，Stage A claim commit 后返回 matching pre-session current-claim
   `ScopedReviewSessionBootstrapReceipt`（不含 ActivityRun/ActivityAttempt/review/event/result），creator
   只通过 `verify_scoped_session_bootstrap(...)` 验证；二者绑定
   source command generation/epoch/post-claim command attempt/lease、authenticated workspace 与 typed plan pins，但 coordination review id/
   review/gate 明确 absent，且只能授权 session create，禁止 transport/domain effect。creator 在 bootstrap lock 下
   exact-validate 该 authority/receipt 与 tenant/plan；specialized bootstrap Stage B 在 session UoW 内创建
   ActivityRun/ActivityAttempt + session/event/source terminal。Stage A 后未 commit 的 crash 须等 lease expiry/
   reselection；commit 后只由 credential-free `ScopedSessionCommittedReplayContext` read/exact-compare，禁止重建
   authority/receipt/token 或写入。session commit/exact replay 返回独立 `ScopedReviewSessionCreateResult`，此后才允许 normal positive-review-id
   `d3-dispatch-v2`/`d3_business_fence_v1`。stale/cancelled/cross-tenant 只返回零写，collision 逐字段不等即零写，crash/retry 必须返回同一
   positive review id。legacy
   JSON-scan/unscoped creator 不能 seed strict D3。strict OperationRun creation 只锁 scoped session 后
   exact-copy scope 与 coordination id，禁止另行 remint；
   command/ActivityRun/ActivityAttempt/terminal event/verification intent 在各自创建 UoW exact-copy+compare；
   ActivityAttempt 物理 exact-copy post-claim `command_attempt`，verification intent source core 再持久化相同
   `source_command_attempt`。
   `workflow_commands` 复用既有 `operation_id` 作为唯一 operation link，并 exact-equal
   `operation_runs.operation_run_id`；不得新增 alias。future strict command additive columns 精确为 **20**（prior
   sixteen + 四项 typed nullable predecessor pins）；predecessor local shape 只允许 all-null initial role 或 complete
   successor CAS，禁止 JSON/half-null。它至少持久化 scope
   identity、registry authority digest（其 `CommandTypeSpec` 同时纳入由唯一 applicable terminal specs 推导的
   `terminal_provenance_policy_digest`）；Migration C 从同一 registry 分别生成 hash-bound
   `scoped_session_bootstrap_command_types_v1` 与 `strict_d3_command_types_v1` literal manifests，以 immutable
   command type 分别 guard bootstrap pre-review/terminal 与 normal positive-review D3 population（禁止 digest
   sentinel discriminator）。
   selection generation/consumed authority id、永不重置的 claim generation/private token verifier、独立 control
   epoch、heartbeat occurrence 与 nullable terminal outcome digest/event identity；不得从 payload、ambient env、
   `attempt`、`lease_owner` 或行自身 owner 自比推导授权。poll/action strict-D3 root 都只能由上述 scoped
   review-session repository 签发；action-backed root 另受
   **Plan §6#6 action-root durable-scope gate**（稳定本地名、无 OB-ID）阻断：action owner 必须先物理持久化
   namespace/mode/workspace/issuer/digest，scoped-session repository 先 lock/exact-compare action pins 再签发
   session scope，OperationRun UoW 只 exact-copy；该 gate 不得借 OB-10.4 代替。
   canonical registry/scheduler 只在 repository exact-command selection 后由 private factory 单次铸造 one-use
   `ClaimAuthority`（strict registry entry 把 `allowed_stage_ids`/stage policy 纳入 digest，`expected_stage_id`
   只来自 exact selected row；同时绑定 coordination/business pins、selection generation + repository expiry +
   pre-claim command attempt + expected control epoch）；selection UoW 必须先证明
   旧 execution lease/selection reservation absent/expired，并以 existing lease fields 持久化 exact unexpired
   reservation；Stage A exact-match 该 current reservation、以 execution lease 覆盖它（绝不要求 absent/expired），
   同一 CAS 原子写 consumed authority id、将 command attempt `+1` 并返回含 post-claim attempt 的 current
   `ClaimIdentity`。`consumed_claim_authority_id` 只是
   current-selection one-use slot；reselection 先 generation+1 并 clear，且该 slot 不承担/不宣称 durable
   historical audit SOT。完整 29-wrapper inventory 仅作 observation/zero-unauthorized-increase denominator；
   D3b 只删除 bootstrap/strict-D3 manifest population 的 scope-local bridge，任何 `legacy_unfenced` entry/caller
   尚存时 generic legacy bridge 明确保留。fenced wrapper 无启动期铸造/standing capability，也不得传
   expected-owner 字符串替代 capability。Stage B 原子创建 attempt，并与 async supersession command 对 D3
   source intent 共享 physical expected predecessor id/generation/control-source-event CAS；四项具名为
   `workflow_commands` typed nullable physical columns，initial tuple all-null 且证明无 current phase，successor tuple
   complete，half-null fail closed，完成
   lock/supersede/successor 单赢。只有 `final_adjudication` terminal event 能计划唯一 typed-outcome record command；
   record UoW exact-compare current source + independent record command 的 source operation/scope/
   generation/epoch/post-claim command attempt/status/event/digest 与 frozen binding。`d3_business_fence_v1` 对 typed base
   plan/review/gate/intent pins canonicalize；closed typed context union 的 claimed-command
   `stage_b|terminal|record|dispatch|resume_after_grant` 都必须带 authority+receipt，aggregate `control` 带
   registered control authority/expected revisions 且无 claim token；六 phase mandatory，wrong context fail closed。
   provider/model transport 与人工/重编译/所有 cancel、D3 OperationRun
   terminal/cancel/retry/requeue/resume/reset/rebuild/recovery 及一切
   dispatch-invalidating command control 使用 `d3-dispatch-v2` 同一有界 coordination lock，key = immutable
   scope tuple + exact `coordination_plan_review_id`，root intent 不参与；Stage B、async supersession、
   record/terminal/control/dispatch/resume_after_grant 的唯一 row-lock order 均为 operation root → optional
   plan/review/gate → all participating workflow_commands（source/record/resume/supersession/current owner/
   idempotency target，确定序）→ intent/current predecessor → ActivityRun/Attempt → optional grant/cost；进入 intent
   后不得回头 insert/lock command，未触及 aggregate 只可跳过、不可逆序，并复查
   plan/review/gate/intent/grant/cost predicate；仅 heartbeat occurrence/read-only terminal exact replay 是窄例外；
   只有 invalidating control/input/requeue 接受态推进 command control epoch `+1`，`succeeded|failed_terminal`
   terminal 不推进 epoch但清 token/lease并写 terminal pair；
   control 先赢则零 send，dispatch 先赢只形成已授权 in-flight，response 仅可入 durable quarantine/cost
   reconciliation；provider delivery id 或 durable inbound `TransportResponseReceipt` get-or-create stable
   `response_occurrence_id`（stable occurrence），redelivery 复用；exact quarantine key=
   `late-response-v1:<scope_digest>:<dispatch_exposure_id>:<canonical_delivery_identity>`，digest mismatch 是
   collision；post-network 固定 exposure→receipt→quarantine/cost-axis order，绝不
   回到 operation/command/intent/domain，也不授权 send/apply。stale application/business mismatch 只返回
   `not_applied(reason=stale_claim|business_precondition_conflict)`（不是 event/state），domain/attempt/intent/event/
   command/source/result 全零写；
   quarantine 由一个 SQL repository 拥有，immutable identity/digests insert-once，cost/retention typed CAS
   entrypoints 写集分离且 monotonic；quarantine 的 cost_state/retention_state 正交：
   `cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain | reconciled_no_call` 与
   `retention_state: retained -> purged_tombstone` 正交，禁止混成
   disposition；禁止持 PG transaction 跨网络。strict-D3 首次 `succeeded|failed_terminal` result command+event 取 common lock，在同一 UoW 写
   canonical nullable outcome digest/event pair；pair 在 result-terminal 期间不可变，仅 registered reopen 可在
   requeue 前清除；event composite unique + command `MATCH SIMPLE DEFERRABLE` FK + local both-null/both-non-null
   CHECK；该单向 FK 不证明 reverse event→terminal command，orphan-event rejection 由同一 repository UoW 与
   injected-failure rollback 证明。future native-PG DDL 必须证明 null pair 接受、half-null/missing event 拒绝、exact event 接受且不可
   orphan；exact replay 零写，mismatch fail-closed。record apply 是 terminal-UoW specialization：one PG transaction
   同时执行 record+terminal predicates，写 verification/intent、recorded domain event、workflow terminal event 与
   attempt+command terminal pair；crash 全回滚/exact replay 全 aggregate。transport-backed terminal 绑定 committed
   exposure/physical-call/provider-call/ModelInvocationEnvelope/response occurrence + canonical response/result
   digest + result artifact ref/digest，typed terminal outcome 必须 exact-copy/exact-equal receipt：exposure 的
   `result_ref` 唯一等于 receipt `result_artifact_ref`（缺席则 canonical empty），`result_digest` 等于 canonical
   result digest，artifact pair exact-copy；same-digest/different-ref 或 half-pair 均 fail closed。D0 明确不承载的
   committed-exposure pre-response transport/protocol failure 走独立 typed `TransportAttemptFailureReceipt` +
   `attempt_failure` terminal variant：它 exact-bind current claim/business pins、registered failure code/spec/canonical
   digest、required failure artifact 与 committed exposure/wire-call state，result/envelope/response receipt 全
   complete-none；terminal retry disposition 才可写 `failed_terminal`，late response 仍只能入 quarantine，绝不伪造
   `ModelInvocationEnvelopeV1`。registered non-transport result 或 proven pre-call/no-send terminal 只能由 registry
   `NoExposureTerminalSpec` 授权 complete-none `no_exposure` variant；caller flag 不可选路。checked-in
   `TERMINAL_PROVENANCE_SPECS` 是
   `TransportResponseSpec|TransportAttemptFailureSpec|NoExposureTerminalSpec` 的唯一 registry SOT，闭集 pins
   command/stage/status/event/outcome 与 variant digest；failure spec 另 pins failure-code→`retryable|terminal` map 和
   `retry_policy_revision`。registry preflight 必须证明每个
   `(command_type, stage_id, terminal_transport_variant, terminal_status, terminal_event_type, terminal_outcome,
   terminal_reason if exposure, failure_code if attempt_failure)` 恰有一个 applicable entry；其集合形成
   `terminal_provenance_policy_digest`，纳入 `CommandTypeSpec` authority digest，并在 command 与 exposure 创建时经
   existing authority/business pins durable freeze（不新增 workflow-command column）；historical specs 保留到没有
   active 或 retained command、exposure、response/failure receipt、terminal event、source-intent tuple、
   quarantine/tombstone 或 cost/audit 引用。receipt/outcome exact-copy，retryable 只能 `retry_wait`、不得 terminalize。valid
   response envelope 即使 `terminal_reason=length|content_filter` 也走 `TransportResponseReceipt`；只有 incomplete/
   truncated wire 或 protocol parse failure 才走 attempt-failure receipt。response/failure receipt creator、failure
   terminal 与 retry transition 都先 `FOR UPDATE` 同一 exposure；response 先赢则 failure/retry 被拒，retry 先赢先
   推进 epoch，后到 valid response 只可 receipt+quarantine，failure terminal 先赢亦同。no-exposure branch 不锁
   exposure row；它持 common coordination/command locks，而 dispatch creation 必须持同一 locks，故只在锁内证明
   exposure absent 后 terminalize。已授权 exposure 在 claim/business stale 后仍可写 response/failure evidence 供
   audit/cost，但都不授权 apply；protocol failure 本身只留 failure receipt/cost audit，不进 response quarantine。
   `awaiting_budget` record 只落 awaiting intent+recorded_event 并 terminalize；gate
   owner 在 post-apply watermark 下先 reserve/lock
   `resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>` row，再填 typed pins，grant owner 共用
   `maybe_plan_resume` reawaken；key 含 recorded_event_id、不含 grant/delivery，禁止 event-only gap。exact outcome
   owner map 由 D3 详设 §2.2 的五行表控制；control timeout 单独为 intent timed_out + verification needs_human。
   rollout 必须先落 migrations/terminal registries、bootstrap/strict-D3 双 population manifests 与 dormant
   bootstrap factory+predicate，再启用 scoped-session specialized Stage-B creator + credential-free committed replay/
   exact-copy，最后按 Stage A→business evaluator→normal D3 paths 顺序激活；任何 consumer 不得先于其
   authority/receipt/registry dependency 部署。
7. §4b owner 矩阵扩展到全部共享字段（D3 批）：round8 single-owner fixed-forward 将 terminal policy registry 与
   receipt persistence 拆行，matrix 形状锁为机械非空十列 × **26 data rows**
   `Field/object, Single owner, Physical SOT, Allowed values, Derivation rule, Consumers, Forbidden consumers,
   Fallback/brownfield status, Migration status, Deletion condition`，因此 decision shape complete；它与物理
   scope/claim identity contract 是 D3c 的开工输入。D3c2a/D3c2b 只落了 dormant command-table 与 scoped-root
   fragments；matrix 的
   physical owner/repository/runtime 与其余 Migration A 仍未实施。没有 owner/runtime/race evidence 仍不得关闭
   §6#7 的 physical implementation。
8. 文档标签清理（残留「详设 v2」字样等）随下一次文档批处理。

## 7. v1 评审 findings 处置总索引

3 critical + 17 high + 3 medium + 1 low 全部 addressed：D3 相关（#1-4、#15-19、#21、#24）见 D3
详设 v2 **§10** 覆盖映射表；D0/D1 相关（#5-14、#22-23）见 D0 详设 v2 §6 覆盖映射表；
#20（agent_events 二源歧义）在本文 §2 D2 修复；#9（路由表归属）与 #21（激活依赖拆分）由本文
§5 TD-4/§3 与两份详设联合修复。v2 定稿前经 24 路逐条覆盖度审计（22 addressed/2 partial 已补）+
跨文档一致性审计（术语统一 `agent_self_verified`、交叉引用修正）。
