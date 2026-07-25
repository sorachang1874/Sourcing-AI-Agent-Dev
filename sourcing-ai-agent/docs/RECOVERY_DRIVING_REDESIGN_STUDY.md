# Recovery Driving Redesign Study

> Status: Design study for owner review (2026-06-14). Reframes Phase 4 Step 5: event-level recovery driving vs the inline->enqueue prework.

本研究回应一个 owner 重新打开的问题：在产品转向 Agent-native（streaming agentic loops、event-visible progress、`agent_events`/SSE tail）的方向下，用 **polling tick + inline request/read-path triggers** 来驱动 recovery 是否还是正确的模型。下面所有结论从 `c11c743` 的代码恢复，路径均在 `src/sourcing_agent/`。

---

## 1. 现状真相：recovery 今天实际如何被驱动

recovery 的"做一次"原子动作是 `run_worker_recovery_once`（`orchestrator.py:38030`，Step 0-4 后内部已是 `recovery_phases.py:64 RecoveryPhase` 注册表）。它被 **五个 driver** 调用，其中**没有一个**在 API 请求路径上同步执行：

- **Request path（`queue_workflow`→`run_queued_workflow`）**：不 inline 跑 tick；调用 `ensure_shared_recovery`/`ensure_job_scoped_recovery`（`orchestrator.py:2308/2311`）→ `_start_recovery_sidecar_with_fallback`（`:42020`）spawn 一个 **detached 子进程** 跑 `cli run-worker-daemon-service`，spawn 失败才回退到 in-process daemon **线程**（`_start_recovery_thread:41991`）。但整条 spawn 被 `if not auto_job_daemon: return disabled` 守门，而 **`auto_job_daemon` 在请求路径默认 `False`**（`:2263, 2578, 3008`；`:42120/42132/42144` 同样早退）。**所以正常请求什么都不触发，完全依赖一个已在运行的 daemon。**
- **Read path / progress 轮询**：`_queue_progress_auto_takeover`（`orchestrator.py:36583`）在 **daemon `threading.Thread`**（`daemon=True`）里跑 tick，靠进程内 `_progress_takeover_inflight` 去重（`:1007`），fire-and-forget，对调用方无任何保证。
- **Watchdog**：`run_hosted_runtime_watchdog_once`（`orchestrator.py:3000`）同步跑 tick 再 dispatch；由 serve 内的 watchdog 线程驱动，`poll≈15s`。
- **Supervisor**：`run_workflow_supervisor`（`:2659`）在 `supervise-workflow` 子进程循环里同步 tick（非 API 进程）。
- **Provider event**：`handle_remote_provider_event`（`:37487`）是**唯一活的入站事件路径**。default 路径 `ensure_job_scoped_recovery` + `request_service_wakeup`（`:37871`），job-scoped sidecar 用 `idle_stop_ticks=3, max_ticks=900` 自终止（`:37854`附近）。

daemon 本体 `WorkerDaemonService.run_forever`（`service_daemon.py:866/902`）：poll（默认 `poll_seconds=5.0`，`:886`）+ signal short-circuit。`idle_stop_ticks` 语义（`:890,1020`）：`0=永不 idle-stop`，`N>0`=连续 N 个无活动 tick 后退出；**shared/standalone 默认 0=永跑**（`recovery_sidecar.py:182`），`max_ticks` 也默认 0。单实例锁是 `fcntl.flock(LOCK_EX|LOCK_NB)`（`service_daemon.py:1122`）——**host-local、advisory、不跨机/跨容器协调**。

**对 owner 的核心问题（从未被设计过）的定论——production daemon 保证从何而来：**

代码**不强制**任一模型；哪个跑是**部署选择**，不是代码决定：

1. **standalone systemd unit**：`render_systemd_unit`（`service_daemon.py:801`）emit `ExecStart=… run-worker-daemon-service …` + `Restart=always`（`:850`），**且不带 `--idle-stop-ticks`/`--max-ticks`**（确认 unit body 无这两个 flag）→ 一个保证永跑的独立 daemon。
2. **inline-in-API**：`serve` 在**非** `--disable-runtime-watchdog` 时，API 进程自己起**两个 in-process 线程**——`start_shared_recovery_service`（全量 recovery，poll 5s）+ `start_server_runtime_watchdog`（poll 15s）（`cli.py:4917-4922`）。若 standalone daemon 已持有 `worker-recovery-daemon` 锁，serve 的 shared 线程命中 `SingleInstanceError` 静默返回（`cli.py:1225`），只剩 watchdog。

**定论：production recovery 仅在以下二者之一成立时才有保证：(a) 装并 enable 了 systemd unit；或 (b) 跑 `serve` 且不带 `--disable-runtime-watchdog`（此时 API 进程本身就是 daemon，in-thread）。若有人 `serve --disable-runtime-watchdog` 又从不起 systemd unit，则除了 read-path 投机性轮询线程外没有任何东西驱动 recovery——recovery 只在客户端正在轮询时才推进。** 这正是 owner 担心的"模型对不对"的根因：**driver 的存在与否是隐式部署约定，没有任何代码层断言。**

**①② inline->enqueue 今天是否安全？** ①（request-path inline `ensure_*_recovery`）与 ②（read-path takeover 线程）今天**功能上是冗余冒烟器而非主驱动**：①因 `auto_job_daemon=False` 几乎从不真正 spawn；②只在有人轮询时机会性推进。把它们改成 enqueue（写一条待 daemon 消费的工作）**只在 daemon 是"保证运行者"时才安全**——而上面证明了 daemon 保证今天不是代码强制的。所以 **Step 5 的 inline->enqueue prework 有一个未言明的前置条件：必须先把 daemon 升级为有保证的运行者**，否则 enqueue 会把"机会性推进"换成"无人消费的静默积压"。

---

## 2. 事件可达性分析（crux）

把 recovery 的工作按"是否存在触发事件"二分：

**(a) 天然可 EVENT 触发的状态迁移**——都已 emit durable 事件：

- **command enqueue / cancel / resume**：经 `append_event_and_reduce`（`durable_runtime.py:3112`）写 `workflow_events` 并 `reduce_and_persist`（`:3142`）upsert `workflow_commands`。命令插入本身就是事件。
- **worker / workflow completion**：reducer 在 `WorkflowCompleted` 上 emit `runtime_outbox` 行 `outbox_type="workflow.completed"`（`durable_runtime.py:3333-3339`），这是最接近"downstream 现在 ready"的信号。
- **provider callback（finalize-ready 的远端侧）**：`POST /api/providers/apify/webhook`（`api.py:1197`）→ `handle_remote_provider_event` → 已经走 `request_service_wakeup`（`orchestrator.py:37871`）。无 webhook 时 `_schedule_local_provider_event_watcher`（`enrichment.py:4102`）轮询 actor 状态后 fire 同一回调。**这是唯一已落地的 event-driven 路径。**

**(b) 天然需要 POLL 的条件**——无触发事件，只能靠扫描发现：

- **crash recovery**：进程在 emit 终态事件前死亡，没有"我崩了"事件。
- **lease expiry**：`linkedin_profile_registry_leases` / `runtime_provider_limiter_leases`（见 WORKFLOW_BEHAVIOR_GUARDRAILS §1，`:49-51`）到期是**时间到点**，不是事件——只能轮询比对 `lease_expires_at`。
- **stuck-state without triggering event**：远端 actor 卡住但既无 webhook 也无 watcher tick；checkpoint 残留。

**关键不对称：** (a) 类是 recovery 工作量的大头且**已经有 durable 事件载体**，今天却完全靠 poll tick 兜底——这是当前模型与 invariant 3（"prerequisite 满足后下游必须立即推进"，`WORKFLOW_BEHAVIOR_GUARDRAILS.md:76`）之间的真实 gap：5s poll 间隔违背了"立即"。(b) 类**无论如何需要 poll**，无法被事件消灭。**所以正确结论不是"用事件取代 poll"，而是"用事件驱动 (a)，把 poll 降级为只兜底 (b)"。**

并且 Audit B 的发现要记牢：`runtime_outbox` 是一个**半成品 durable wakeup 通道**——producer（`enqueue_runtime_outbox`，`storage.py:15321`）+ dispatch-marker（`mark_runtime_outbox_dispatched`，`:15381`）+ 索引（`control_plane_live_postgres.py:5624`）都在，**但没有任何 `list_`/`claim_` consumer**（全仓 grep 确认）。事件骨架已铺一半，缺的只是消费者。

> **2026-07-20 事故补记（§2(b) 第三类 "stuck-state without triggering event" 已落地 backstop）**：serve 重启后，in-process long-poll watcher 线程随之死亡；在无 webhook 的环境下，`waiting_remote_harvest` 的 segmented `harvest_company_employees` shard worker 永远不会再收到 terminal event，而 worker recovery daemon 的 remote-wait skip（`worker_daemon.py` `_worker_is_already_submitted_remote_wait`）假设 event owner 存在，导致两个 full-roster job 永久停在 `blocked acquiring`（shard queue summary 永远 `queued`，dataset 无人下载）。修复语义：**terminal-event first, orphan-poll second**——fresh remote-wait worker 仍严格归 event owner 所有（daemon 不得主动轮询）；但 `updated_at` 超过 `WORKER_RECOVERY_REMOTE_WAIT_ORPHAN_SECONDS`（默认 900s）的 worker 被判定为 orphan，经正常 claim/resume 路径获得**每次 tick 至多 `WORKER_RECOVERY_REMOTE_WAIT_ORPHAN_LIMIT`（默认 4）个**的有界重 poll：remote 已 terminal 则下载 dataset 并完成 worker（queue summary 翻 `completed`，blocked job 的 readiness 就绪后由 workflow_resume 恢复），仍在运行则 re-queue 并刷新 `updated_at` 自我限速。这正是本节"poll 降级为只兜底 (b)"原则对"事件源本身不耐久"这一盲点的补全。

---

## 3. 设计选项

约束：~20 并发用户、无 Redis、Agent-native 目标、PG-only。

### Option 1：保持 poll-tick，仅做原 ①②③ enqueue prework（status quo 路径）

把 request-path inline ①、read-path takeover ②、bootstrap ③ 改为 enqueue，daemon 仍纯 poll 消费。
- **取舍**：改动最小、与 PHASE4 §2(d) Track C 既定 sequencing 一致；但**不解决 invariant 3 的 5s 延迟**，也不解决 daemon-保证缺口（见 §1 前置条件）。
- **invariant 1**：enqueue + daemon 单写 + lease claim（`WORKFLOW_BEHAVIOR_GUARDRAILS §1`）足够防重；但 enqueue 与残留 poll 并发仍需幂等键去重。
- **invariant 3**：**不满足**，仍是 poll 间隔级延迟。
- **评估**：✗ 治标。它把"机会性推进"换成"队列积压"，在 daemon 保证未先解决时反而更脆。

### Option 2：hybrid event-signaled wakeup + 低频 poll backstop（推荐）

durable 状态迁移（command enqueue、`workflow.completed` outbox、provider callback）**signal daemon 立刻跑 NOW**；poll 降为慢速安全网，只覆盖 §2(b) 的 crash/lease/stuck。
- **复用既有、无新 infra**：wakeup 用已有 `request_service_wakeup`（`service_daemon.py:584`）写 `wake_request.json`，daemon 的 `_consume_wakeup_request`（`:1168`）在 `_sleep_until_next_tick`（`:1149`）里以 ≤0.25s 粒度 mtime 轮询 short-circuit poll——**已把 event→tick 延迟压到 sub-second，webhook 路径今天就在用**。把这个唯一 caller（`orchestrator.py:37871`）泛化为"有新 durable 工作就 signal"的通用入口即可。
- **可选升级**：给 `runtime_outbox` 补上缺失的 `claim_runtime_outbox` consumer + dispatch loop（纯 PG，无 Redis/无 LISTEN-NOTIFY），让 wakeup 成为 durable 通道而非仅文件提示。
- **invariant 1**：wakeup payload 携带 scoped worker ids + 既有 lease/idempotency 守护（`WORKFLOW_BEHAVIOR_GUARDRAILS §1:49-51`），daemon 单写 + flock 单实例（`service_daemon.py:1122`），event-signaled tick 与 backstop poll tick 走同一 `run_worker_recovery_once` 同一 claim 路径，**天然不会重复 dispatch**。
- **invariant 3**：**满足**——sub-second 触发，落在 200-300ms SSE cadence 预算内。
- **评估**：✓ 推荐。零新 infra，复用 in-tree 机制，把 poll 从 driver 降级为 safety net，直接闭合 invariant 3 gap。**唯一硬前置**：daemon 必须先是保证运行者（见 §4）。

### Option 3：full event-stream recovery driver，对齐 Track D `agent_events`/SSE

recovery 由 agent session 事件流驱动（`agent_turn` suspend/resume → 事件 → tail → driver）。
- **现实检查**：Audit B 确认 **`agent_events` 表不存在**，只有 `agent_runtime_sessions`/`agent_trace_spans`/`agent_worker_runs`（`storage.py:2284-2318`）；**无任何 `text/event-stream`/`StreamingResponse`/SSE 端点**。`EVENT_LEVEL_WORKFLOW_RESPONSE.md` 明言"远端完成发现属于 wakeup/recovery 层"——即当前设计点就是 wakeup，不是 streaming。
- **取舍**：最贴合长期愿景，但需要先建 `agent_events` 表 + SSE tail + session 模型，是 Track D 工作量，**今天无地基**。
- **invariant 1/3**：理论上最优（事件即触发），但 §2(b) 的 lease/crash 仍需 poll 兜底——**Option 3 不能消灭 poll**，它只是把 Option 2 的 signal 通道从文件换成事件流。
- **评估**：◐ 正确的北极星，但越级。应作为 Option 2 之后的演进，不是 Step 5。

---

## 4. 与 Track C/D 的关系

**inline triggers 能被移除的前置条件 = daemon 成为"保证运行者"。** 今天 daemon 保证是隐式部署约定（§1），代码无断言。在移除 ①②（让它们 enqueue 或 signal 而非自驱）**之前**，必须先：

1. 让 production 必有一个 always-on daemon（systemd unit enabled，或 serve 内 shared 线程），并在 `serve` 启动时**断言**该 driver 存在（fail-closed），否则 enqueue/signal 会静默积压。
2. 这正是 PHASE4_ENTANGLED_CORE_DESIGN §2(d) 的 **Track C（process separation）prework** 的真实内核——不是"把 inline 改 enqueue"这个机械动作，而是"先建立 daemon-as-guaranteed-runner 这个不变式"。

**事件重设计与 process separation / agent sessions 的 sequencing：**

- **现在（单机 docker-compose，无 Redis）**：Option 2 的 wakeup-file / PG-claim 通道足够，不需要跨进程 push。
- **worker 拆出 API 进程后**：跨进程 push wakeup 才需要 **PG LISTEN/NOTIFY（in-database，非 Redis）**——这是唯一会重新引入 LISTEN/NOTIFY 的场景，且它替代的是文件 mtime 轮询，不是引入新 broker。
- **进入 Track D agent sessions 后**：Option 3 的事件流 driver 把 `agent_turn` 事件接入同一个 signal 入口，poll 仍保留为 lease/crash backstop。

即：**Option 2 是 Option 3 的真子集与前置**，三者是同一信号入口的逐步增强，不是互斥重写。

---

## 5. 推荐与对 Step 5 的重定义

**推荐 Option 2。** 把 Phase 4 **Step 5 从"convert inline recovery triggers to enqueue"重定义为"event-signaled wakeup + poll backstop（含 daemon-保证前置）"**。理由：inline->enqueue 是手段不是目的；真正要建立的是 (a)daemon 保证不变式 + (b)事件→sub-second 触发，两者一起才同时解决 owner 的 daemon-保证问题与 invariant 3 gap，且零新 infra。

**迁移顺序（characterization-first，每步带 invariant 1/3 回归）：**

1. **Step 5a — daemon-保证不变式**：`serve` 启动断言存在 recovery driver（systemd 或 in-process shared 线程其一），缺失则 fail-closed 告警。验证：起 `serve --disable-runtime-watchdog` 且无 systemd 时必须报错；characterization 现有 serve 启动行为。
2. **Step 5b — 泛化 wakeup 入口**：把 `request_service_wakeup`（`service_daemon.py:584`）从 provider-event 单 caller 提为通用"新 durable 工作"signal，在 command enqueue / `workflow.completed` outbox 产出点调用。验证：注入一个 completion 事件，断言 tick 在 sub-second 内触发（invariant 3 回归）；并发 event+backstop poll 断言无重复 dispatch（invariant 1 回归，复用 `duplicate_provider_dispatch` 统计 `WORKFLOW_BEHAVIOR_GUARDRAILS:220`）。
3. **Step 5c — poll 降级为 backstop**：shared daemon `poll_seconds` 从 5s 提到慢速安全网（如 30-60s），仅覆盖 §2(b) lease/crash/stuck。验证：lease 到期在 backstop 间隔内被回收；event 路径不依赖 poll 间隔。
4. **Step 5d（可选）— outbox consumer**：补 `claim_runtime_outbox` + dispatch loop，把 wakeup 升级为 durable PG 通道。验证：outbox 行被消费并 `mark_runtime_outbox_dispatched`。
5. **①②③ 移除/改写**：在 5a 不变式成立后，把 request-path inline ①、read-path takeover ②、bootstrap ③ 改为 signal-only（不再自起线程/进程）。验证：移除后 recovery 仍由 daemon+wakeup 保证推进。

每步先写 characterization test 锁住当前行为，再改，最后跑 invariant 1（无重复 dispatch）/ invariant 3（sub-second 触发）回归。

---

## 6. Owner 决策点

1. **Deployment-guarantee（daemon 是 standalone systemd 还是 inline-in-API？idle_stop_ticks 实际值？）**
   - 真相：代码不强制；二选一靠部署约定；shared/standalone 默认 `idle_stop_ticks=0`（永跑），仅 remote-event job-scoped sidecar 用 `=3` 自终止。
   - **建议答案**：production 采用 **standalone always-on systemd unit**（`render_systemd_unit` + `Restart=always`，`idle_stop_ticks=0`）作为唯一保证 driver；`serve` 默认带 watchdog 但其 shared 线程因 flock 自动退让；**并在 serve 启动加 fail-closed 断言**（Step 5a）。这把隐式约定变成代码不变式。

2. **是否重新考虑 LISTEN/NOTIFY？**
   - 真相：全仓无 LISTEN/NOTIFY/Condition/Queue（仅 HTTP socket `.listen()`）；wakeup-file 已给 sub-second，`runtime_outbox` 是现成 PG 通道（缺 consumer）。
   - **建议答案**：**单机阶段不引入 LISTEN/NOTIFY**——wakeup-file + 可选 outbox-claim 已满足 invariant 3，符合 SERVICE_GRADE_ARCHITECTURE_PLAN 的"no Redis/no LISTEN-NOTIFY at ~20-user"决定。**仅在 worker 进程从 API 拆出后**，为跨进程 push 再考虑 **PG LISTEN/NOTIFY（in-DB，非 Redis）**，届时它替代文件轮询而非新增 broker。

3. **追求哪个 Option？**
   - **建议答案**：现在做 **Option 2**（hybrid wakeup + poll backstop），并按上述 Step 5a-5e 重定义 Step 5。**Option 3（event-stream driver）作为 Track D 北极星**，在 `agent_events`/SSE/session 地基建成后承接同一 signal 入口；**不采纳 Option 1**（仅机械 enqueue），因它不解决 daemon 保证也不闭合 invariant 3。
