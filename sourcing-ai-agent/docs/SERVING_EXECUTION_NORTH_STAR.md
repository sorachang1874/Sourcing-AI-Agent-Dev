# Serving / Execution North Star

> Status: Architecture north star for owner review (2026-06-15). Forward-looking serving/execution architecture; Track C steps re-framed as increments toward it.

文件路径均相对仓库根；行号锚定 `src/sourcing_agent/`，已逐条核验。本文件只规划、不改码、不改测试。

本文档是**北极星**（target serving/execution 架构），不是 Track C 计划的替代。`docs/TRACK_C_SERVING_RUNTIME_PLAN.md` / `docs/TRACK_C_C1_HEAVY_OPS_DESIGN.md` 是路线增量；`docs/SERVING_MESH_OWNERSHIP_BOUNDARY.md` 是冻结读侧边界。本文给出三者收敛的终点，并把 C1..C5 重新表述为「朝北极星的增量」。

参考最佳实践（concrete references）：Temporal / Restate（durable execution：event-log + 确定性 control-flow + journaled activities）、LangGraph checkpoint（`thread_id` = 一个 session/turn，per-super-step `StateSnapshot`，`interrupt` = HITL 暂停-恢复）、Stripe / Adyen / Trigger.dev（idempotency-key 即锁即结果键、submit→202+handle→poll/stream→artifact 状态机）、"Logical CQRS"（读模型与写模型分离但同一 PostgreSQL、projection 与 event append 同事务）。

---

## 1. 目标架构（North Star）

**一句话**：所有重活 = 既有 command/event/outbox 基底上的**一种 durable typed task**；对外只有**一个 async 提交契约**（submit→202+handle→stream/poll→artifact）；**写/执行面**与**读/投影面**物理分离（读永不进 LLM/检索关键路径）；agent 的 session/turn 是**同一 task 基底**的又一 task 类型，事件流即进度流。

```
                           ┌──────────────────────── READ / PROJECTION PLANE ───────────────────────┐
  client ──GET──────────►  │  resolver → read-model → projection-command-owner(9 读族) → fast-path   │
   (poll/SSE tail)         │  仅点查 projection / event-tail；永不触发 compute（冻结 mesh 边界）       │
                           └────────────────────────────────────────────────────────────────────────┘
                                          ▲ 同事务更新 (outbox / event append)
                                          │
  client ──POST heavy──►  ┌──────── WRITE / EXECUTION PLANE ──────────────────────────────────────────┐
   202 + {task_id}        │  ① 提交：append typed command (idempotency-key 即锁即结果键) → 202+handle   │
                          │  ② 排队：command/event/outbox 表即队列；FOR UPDATE SKIP LOCKED + lease(TTL) │
   stream/poll ◄──────────│  ③ 执行：run_worker_recovery_once 每 tick 经 drain registry 排空 ready 命令 │
   {status, events}       │     重活=activity：LLM/检索/导出/agent-step 结果 journaled，replay 不重跑     │
                          │  ④ 结果=artifact：reducer 写 succeeded + artifact handle（本地→C6 对象存储）│
                          └──────────────────────────────────────────────────────────────────────────┘
```

**四个支柱（ADOPT）**

1. **统一 durable-task 基底**：plan-compile / 检索 / 导出 / refine-compile / 未来 agent-turn 全部成为 `CommandTypeSpec`（`durable_runtime.py:95`，注册表 `DEFAULT_COMMAND_TYPE_SPECS` `durable_runtime.py:135`）上的 typed command，走同一 event/reducer/outbox（`append_event_and_reduce`）、同一 lease claim（`FOR UPDATE SKIP LOCKED` + `lease_expires_at` TTL，`control_plane_live_postgres.py:2242`）、同一 driver（`run_worker_recovery_once` `orchestrator.py:38184`）经 drain registry（`recovery_drain_registry.py:66`，bindings-as-data）排空。新增一类 task = 加一个 CommandTypeSpec + 一行 drain binding，**不是新 dispatch 代码**。

2. **单一 async 提交契约**：对所有重活统一为 `POST → append command（idempotency-key 去重）→ 202 + {task_id, status:"queued"} → poll/SSE 观察 → artifact handle 取结果`。状态机 `queued → running → {succeeded, failed, cancelled}`（+ `expired`）。idempotency-key 既是 in-flight 锁也是结果回放键（导出已有 `export_projection_generate_idempotency_key`，`durable_runtime.py`）；结果是 **artifact 引用**（handle/signed path），不内联进 status poll，poll 保持廉价。

3. **写/执行面 vs 读/投影面分离（Logical CQRS）**：读路径只服务 projection 点查与 event-tail，**永不触发 compute**；projection 与 event append **同一 PostgreSQL、同事务**更新（或经 outbox）——单 org/PG-only 下同事务投影比异步投影 + 最终一致更简单且更强。读侧边界就是已冻结的四块切分（resolver / read-model / projection-command-owner+9 读族 / fast-path，`SERVING_MESH_OWNERSHIP_BOUNDARY.md` §2），**北极星不动其代码**。

4. **Agent-native serving = 同一基底**：一个 *turn* 就是一个 durable task；agent steps 作为 events 追加；**per-session 单写者 lease**（复用 lease 列）保证两 worker 不并进同一 turn；SSE tail 同一 event log 即进度。HITL = `interrupt` 点：暂停 turn、持久化、等人输入、从 checkpoint 恢复——既有 `workflow_recovery_intents`（durable 单赢 claim）就是这个 primitive。Time-travel/fork 由 checkpoint 历史免费得到。

**右尺度的克制（OVER-ENGINEER，~20 用户 / PG-only / no-Redis 下不做）**

- 不引入 Temporal/Restate 服务器 + worker SDK：那是新基础设施，且重复已自建的 command/event/outbox 基底。
- 不做 agent 推理的**严格确定性 replay**：只 journal activity *结果*、从最后 checkpoint 恢复（LangGraph super-step 模型的实用 80%），不追求 control-flow 位级 replay。
- 不做独立读库 / Redis 缓存 / 异步投影 worker：同事务投影更简单更强，最终一致只会引入对账 bug。
- 不做分布式 idempotency 缓存 / 24h TTL 调优 / webhook 投递保证：一行 PG（key + status 列）足矣。

---

## 2. 建立在什么之上（深重构，非重写）

北极星**复用**而非重建以下强地基——这正是它能是「深重构」而非 rewrite 的原因：

- **Durable runtime**：events/reducer/outbox/leases（`DurableRuntimeWriter.append_event_and_reduce`）；`FOR UPDATE SKIP LOCKED` + lease 列的 claim（`claim_workflow_recovery_intents` 模式，`control_plane_live_postgres.py:2242`）。这就是「durable-task 表即队列」需要的全部。
- **CommandTypeSpec 注册表**（`durable_runtime.py:95` / 注册表 `:135` / manifest `command_type_manifest` `:669`）：~50+ 典型命令带 owner/stage/readiness/control-policy；cancel/resume 已 table-driven（导出的 `_cancel_running_export_command` / `_resume_running_export_command`，`durable_runtime.py:1129/1149`）。新 task 类型挂这里。
- **recovery_drain_registry**（`recovery_drain_registry.py:42` `RecoveryDrainBinding` / `:66` 默认 14 条 binding / `:204` `build_recovery_drain_registry`）：bindings-as-data；这是「加一行就多排空一类命令」的关键。
- **事件驱动 recovery**：durable `workflow_recovery_intents` 单赢 claim、signal-only 请求路径、30s poll backstop。这**就是**北极星的统一 observe/wake 机制（agent turn 的 HITL 暂停-恢复亦复用它）。
- **冻结 serving-mesh 边界**（resolver / read-model / projection-command-owner / fast-path）：北极星的「读/投影面」直接等于它，原样不动。

结论：基底（D 类）已是优秀的 durable substrate，北极星只是**让 A/B/C 三类旁路收敛进它并删除**，而非另起炉灶。

---

## 3. 当前的碎片化（统一模型删除什么）

今天「做重活」有**三种互不兼容的 dispatch 风格 + 一个正确的基底**，共四类：

- **A 同步占请求线程**（整段 LLM/检索/归档跑在请求线程，全程占住 8 槽共享信号量之一）：`run_job`（`orchestrator.py:3703`，docstring 自承 "Synchronous"）；`post_plan`→`plan_workflow`（`orchestrator.py:1467` 内联 LLM compile）；三导出（`export_projection_candidates_archive` `orchestrator.py:27937` 等：**先 plan 了 durable command 又立即在同一调用内同步 `_run_..._command`**——基底已在，API 仍内联）。
- **B 异步 DB-job + 线程 + poll**（生产范式）：`queue_workflow`（`orchestrator.py:2419`）→ `_start_hosted_workflow_thread`（`orchestrator.py:2871`）→ daemon `run_queued_workflow`（`orchestrator.py:2671`，自带 `while True` tick 循环 `orchestrator.py:2785`）→ 前端 poll。
- **C ad-hoc 裸线程 + 进程内状态**（非 durable，重启即丢）：`_queue_plan_hydration`（`orchestrator.py:1607`）spawn `threading.Thread`，状态在 in-memory `_plan_hydration_inflight` / `_plan_hydration_lock`（`orchestrator.py:1022-1023`）；按 `_plan_hydration_request_signature`（`orchestrator.py:72745`）手算去重。
- **D durable typed-command 基底**（应吸收 A/B/C 的那个，见 §2）。

**统一模型删除**：`run_job` 的同步 serving 路径；导出 handler 内的内联 `_run_*_command`；plan-hydration 的线程 + 两个 in-memory dict + `_plan_hydration_lock`（plan-compile 成为 typed command 后，去重/恢复/状态由基底免费提供，且重启可存活）；`run_queued_workflow` 的 bespoke `while True` daemon 循环（`orchestrator.py:2785`，由统一 tick 取代）。验证证据：drain registry 今有 14 条 binding 但**无 export-drain binding**，且无 `plan.compile`/`plan.hydration` 命令类型——这正是 A/C 旁路尚未收敛的两处缺口。

---

## 4. Track C 作为增量路径（C1..C5 收敛于北极星）

每步给出**深重构版本**（非「逐 handler 包 enqueue」的打补丁版本），删除什么，以及顺序。推荐序：**C1 → C2 → C3（含 5d）→ C4 →（C5 并入 Track D）**。

### C1 — 引入统一 async-task 契约 + 把重活接进 durable 基底，删同步轨
**深重构（推荐，非 handler-convert）**：不要把每个同步重 handler 平行包成 enqueue+poll；而是引入**一个**统一 async-task API 形状，把 plan-compile / 导出（/ 检索）路由进**既有 durable 基底**（append command → drain registry 排空 → poll/event），并**删除三条旁路**。
- plan：成为 typed command（如 `plan.compile.generate`），idempotency-key 用它已手算的 `_plan_hydration_request_signature`（`orchestrator.py:72745`）；删 `/api/plan` 同步 route 与 plan-hydration 线程 + dict + lock（live client 已用 `/api/plan/submit`）。
- 导出：command 类型已注册（`durable_runtime.py:610/622`，cancel/resume 已 wired `:1129/:1149`）；唯一缺口是 handler 内联 `_run_*_command`（`orchestrator.py:27951` 等）。**加一行 export drain binding 进 `recovery_drain_registry.py`**，内联同步路径即自删；API 拆为「ensure command queued（202+command_id）/ artifact handle 取下载」。
- 检索：`run_job`（`orchestrator.py:3703`）是 workflow 尾半段 + 内联 plan 的同步薄壳；serving 面**删 `POST /api/jobs` route**（前端零调用），方法 demote 为 CLI/test helper。
- 删除：`run_job` 同步路径、导出内联 `_run_*_command`、plan-hydration 线程/dict/lock、（随 C3）`run_queued_workflow` 的 `while True`。

### C2 — 最小鉴权 + 可信身份（强制读侧边界）
**深重构**：不只是注入 identity，而是**收紧读路径**——`list_jobs`（今无 tenant filter）、`get_job`（裸 id 可越权）、`get_crm_record` 加 `WHERE scope`；提交端服务端**覆盖** body 的 `requester_id/tenant_id`。单 org / 静态 per-user bearer，无登录 UI。边界：user-private（jobs/CRM/exports/未来 agent_sessions）加 scope；shared canonical（company_assets/evidence/projections）不加。删除：client 发的 requester/tenant/workspace 身份字段（服务端派生）。

### C3 — 进程分离 + 5d（worker 成唯一 runner、跨容器 durable 唤醒）
**深重构**：反转默认——serve 默认**不**起进程内 recovery，worker daemon 成唯一 runner；flock（host-local）→ PG advisory lock（`_advisory_lock_key`）做跨容器单写者。**补 5d**：实现缺失的 `claim_runtime_outbox`（**逐字复用** `claim_workflow_recovery_intents` 的 `FOR UPDATE SKIP LOCKED` + lease 模式），在 `run_worker_recovery_once` 每 tick 调用——拆容器（无共享卷）后 host-local wake-file 失效，5d 是跨容器 durable floor。删除：进程内 recovery 抢锁的输者线程、host-local flock 单写假设。

### C4 — OpenAPI + SSE（事件 tail 即进度）
**深重构**：handler pydantic 化 → OpenAPI 成前端 contract 单一源；**SSE tail event log** 取代 1s/5s 轮询，且**与 C5 的 agent_events SSE 合并设计为同一事件流**——北极星里「poll」与「agent 进度」是同一 event-tail 的两种消费。删除：客户端手写契约、轮询定时器。

### C5 — 多用户 agent 拓扑（同一基底的 agent-turn）
**深重构**：`agent.turn` 成为 CommandTypeSpec，session/turn 是 durable task，agent steps 即 events，**per-session 单写者 lease**，agent_events 经 §C4 同一 SSE tail；HITL 复用 `workflow_recovery_intents`。凭证只在 provider worker，per-user 限额。**并入 Track D**（与其 agentic 北极星全事件流 driver 重叠）。删除：未来不会有的「agent 专用第二套 runtime」——它从不被建出，因为 turn 就是 task。

---

## 5. 边界与非目标

- **明确非目标**：Redis；PG LISTEN/NOTIFY（仅 C3 拆容器后、若 30s backstop 恢复 latency 实测不可接受才 revisit）；per-user 常驻容器（沙箱仅将来加代码执行/浏览器工具时按工具调用租用）；big-tech 过度工程（Temporal/Restate 服务器、独立读库、异步投影 worker、严格确定性 agent replay、分布式 idempotency 缓存）。
- **M2 provider-runtime 边界**：8 槽共享信号量（`api.py:64`）是 HarvestAPI ~8 actor 隐性限制的临时护栏，**必须随 M2 移到 provider 层**（per-provider+key 信号量/令牌桶）后 HTTP 入口并发上限才放开。C1 enqueue+poll 不替代此护栏，只把重活 wall-clock 移出请求槽；worker 侧并发受其自身 semaphore 约束。
- **Track D agent-topology 边界**：C5 = Track D 的 serving 落地（agent_session/agent_turn/agent_events/SSE/per-user 限额），不在 C 独立交付；Track D 北极星 = 全事件流 recovery driver——与本文 §1 支柱 4 同源。
- **冻结 mesh 边界**：读/投影面原样不动（`SERVING_MESH_OWNERSHIP_BOUNDARY.md`），C2 的 tenant scope 只落 user-private 表，不触碰 shared canonical 读族。

---

## 6. Owner 决策点（2026-06-15 已批）

- **(a) 是否承诺「一个」统一 durable-task 基底？** —— **RATIFIED: YES（承诺 + substrate-unify）**。plan/检索/导出/agent-turn 全部成为基底上的 typed task，删除 A/B/C 三套旁路。理由：基底（D）已优秀，碎片化是核心缺陷；统一后「新重活 = 加 1 个 CommandTypeSpec + 1 行 drain binding」。
- **(b) C1 多激进：handler-convert vs substrate-unify + 删同步轨？** —— **RATIFIED: substrate-unify + 删同步轨**（非平行包装），且 **C1 一次做全**（含导出前端 202→poll→download UX + refine-compile ×2 异步化，见 `TRACK_C_C1_HEAVY_OPS_DESIGN.md` §6）。冗余路径接进既有基底并删（plan 完全冗余于 `/api/plan/submit`；导出 command 已注册只缺 drain binding；`run_job` 删 route）。理由：平行包装会把双轨永久化，与「删除/替换 > 保留双轨」相悖。
- **(c) 现在就立一个统一 async-task API 面，还是逐端点生长？** —— **RATIFIED: 现在立统一形状（submit→202+handle→poll/stream→artifact），逐端点灰度切入**。理由：契约一次定义可被所有重活与未来 agent-turn 复用；分端点切换降低前端摩擦，形状不重谈。
- **(d) 读侧投影：同事务 vs 异步 worker？** —— 建议 **同事务（single PG）**；**C3/C4 落地时确认**。理由：单 org/PG-only 下更简单且强一致；异步最终一致只引入对账 bug，无收益。
- **(e) 5d + LISTEN/NOTIFY 触发时机？** —— 建议 **5d 与 C3 拆容器捆绑**（无共享卷那刻起必需）；**LISTEN/NOTIFY 仅在容器已拆且 backstop latency 实测不可接受时再加**，不预先引入。**C3 落地时确认**。
