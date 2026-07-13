# Track C — Serving Runtime Plan

> Status: Active implementation. C3a external-recovery default implemented 2026-07-14; C3b/5d and cross-container single-writer remain owner- and residual-gated.

文件路径均相对仓库根；行号锚定 `src/sourcing_agent/`（审计引用为裸文件名，路径已落实到该包）。本文件同时记录原计划和已落地的有界切片；实施状态以本文档、`NEXT_TODO.md` 与当前代码交叉校验。

## 1. 现状

**已完成（serving 地基三件）**：(a) psycopg_pool per-adapter 懒加载连接池（`SOURCING_CONTROL_PLANE_PG_POOL_MIN/MAX` 默认 1/8，25 调用点事务语义逐一核验，adc1cc2）；(b) FastAPI+uvicorn 传输等价重写 `api.py`（同路由/payload/状态码/headers，`create_server` 垫片，双道信号量改 middleware，CORS allowlist + localhost 放行）；(c) Phase 4 recovery 改事件驱动——5a serve 启动 `assert_recovery_coverage_or_fail_closed`（`cli.py:1271`）缺 driver 即 fail-closed、5b durable-commit 尾部 `request_service_wakeup`、5c poll 降为 30s backstop、5e 请求/读路径 signal-only + durable `workflow_recovery_intents` 单赢 claim 表。

**当前 serving 模型（一段话）**：传输层仍是单进程 uvicorn，同步 handler 经 `_make_endpoint` 进 `run_in_threadpool`，并由 8 槽 shared + 2 槽 light-reserved lane 限流；但原计划中的 heavy-handler 清单已不再成立。C1a/C1b 已删除同步 `POST /api/plan` 和 `POST /api/jobs`：实时 UI 的 Plan 走 `POST /api/plan/submit` 兼容桥（当前 `200/pending`，后台 hydration + frontend-history 轮询），检索工作流走 durable `POST /api/workflows` `202`。canonical projection/CRM export 也已是 `202` command-owner worker task，统一通过 `GET /api/exports/{task_id}` 轮询并从 artifact endpoint 下载；旧 target-candidate export 默认 `410`。因此当前主要过渡性耦合是 C1b 的进程内 legacy hydration owner，不是已删除的 Plan/Jobs/导出同步重路由。C3a 后 `serve` 默认也不再运行 recovery/watchdog，必须看到 fresh 外置 daemon 才启动；显式 dev opt-in 是唯一单进程兼容路径。daemon 唯一性仍依赖 **host-local flock**，所以这是 same-host/systemd 切片，不是跨容器完成态。

**剩余项**：C3a 只完成「API 默认不运行 recovery + fresh external daemon gate」。C3b 的跨容器唯一 runner、**5d** durable `runtime_outbox` consumer 和无共享卷唤醒仍未实施；它们受 `RESIDUAL_LEDGER.md` R-019/R-023 约束，不得借 C3a 顺手并入。其余剩余项仍是 FastAPI/OpenAPI+SSE、Track D 多用户 agent serving 拓扑与 later 对象存储读穿。

## 2. 依赖图与推荐排序

**依赖事实**：身份（item 3）是 list_jobs/get_job 加 WHERE scope 的前置（无可信身份就无可强制边界）；进程分离（item 2）一旦 api/worker 不共享文件系统，host-local wake-file 失效→必须 5d 才有跨容器 durable 唤醒；enqueue+poll（item 1）复用的 `queue_workflow`→202→前端轮询模式**已是生产范式**（`post_workflows`→`start_workflow` `orchestrator.py:2178`、`_queue_plan_hydration` `orchestrator.py:1607`），前端**已经在轮询** progress，所以摩擦最低、latency 收益最快；SSE（item 4）是轮询的后续优化，与 agent_events（item 5）SSE tail 重叠应合并；item 5 与 Track D 的 agent_session/agent_turn/SSE 大面积重叠；item 6 是 later。

**serving-value 排序原则**：先拿 20-用户 latency 立竿见影的（C1 重活出线程，把 LLM/检索移出 8 槽），再补两条安全/正确性地基（C2 鉴权可强制边界、C3 进程分离 + 5d 让 worker 成唯一 runner 且跨容器 durable），再做开发体验/推送（C4 OpenAPI+SSE），大件且跨轨的留后（C5 agent 拓扑并入 Track D，C6 对象存储）。

| 步 | scope | 依赖 | serving-value | 前端协同 | 体量 | 风险 | 验证 |
|---|---|---|---|---|---|---|---|
| **C1 重活出请求线程** | `post_plan`/`post_jobs`/3 导出/refine-compile 改 enqueue+202+poll | 无（复用现成范式） | 高（解开 8 槽 head-of-line，最快 latency 赢） | 中（前端已轮询 progress，加新 poll 端点 + 处理 202） | M | 改 API 语义（同步→异步响应形状） | **characterize-first**：先钉死现同步响应形状，再迁；transport-parity 套件扩异步契约 |
| **C2 最小鉴权 + 身份** | `_AuthMiddleware` 注入可信 `(requester_id,tenant_id)`；list_jobs/get_job/get_crm_record 加 WHERE scope | 无（但应在 C5 多用户前落） | 高（正确性/隔离，今天任何人猜 job_id 即越权读） | 中（前端加 `Authorization: Bearer`，去掉 body 里 requester_id/tenant_id） | M | 越权读是真缺陷；改 store 读路径有回归面 | characterize 现 list/get 行为；加跨 tenant 越权读的红线测试 |
| **C3 进程分离 + 5d** | serve 默认不再起进程内 recovery、worker_daemon 成唯一 runner；补 `claim_runtime_outbox` consumer | 弱依赖 C2（部署形态）；5d 是跨容器前置 | 中高（隔离重活/释放 API 进程；durable 跨容器唤醒） | 无 | M–L | flock 跨容器失效→单写者要改走 PG advisory lock；rollout 须全停（锁身份已变） | 进程分离后跑 recovery 端到端；5d 复用 `claim_workflow_recovery_intents` 模式 + 变异测试 |
| **C4 OpenAPI + SSE** | handler 签名 pydantic 化→OpenAPI 成前端 contract 源；SSE 替代 1s/5s 轮询（含 C1 的 poll） | C1（先有 enqueue 才值得推 status）；与 C5 agent_events SSE 合并设计 | 中（去轮询、推送 latency；契约可生成） | 高（前端切 SSE 客户端 + 契约重生成） | L | 大面，易范围蔓延 | OpenAPI 与现 `contracts/frontend_api_contract` 比对；SSE 与轮询行为等价测试 |
| **C5 多用户 agent 拓扑** | 按角色容器（api/agent-worker/provider-worker）+ agent_session/agent_turn PG checkpoint + per-session 单写者 lease + agent_events SSE + per-user 限额 + 凭证只在 provider worker | C2+C3+C4 | 高（但属 agentic 北极星） | 高 | XL | 与 Track D 强耦合 | 随 Track D 第一垂直切片 |
| **C6 对象存储读穿** | company_assets/media 出本地盘（`object_storage.py` 抽象已存在） | 无（独立） | 低（容器化后才痛） | 无 | M | 低 | later |

**推荐执行顺序：C1 → C2 → C3（含 5d）→ C4 →（C5 并入 Track D）→ C6（later）。**

## 3. 每步关键设计点

### C1 重活出请求线程
- **enqueue+poll 契约**：复用 `queue_workflow`（`orchestrator.py:2419`）→ 持久化 DB job(status=queued) → `_start_hosted_workflow_thread`（`orchestrator.py:2871`，写 `hosted_dispatch` marker + 守护线程跑 runner）→ 返回 **202 + job_id**；recovery 走 `_signal_shared_recovery_wakeup`（`orchestrator.py:2355`）信号常驻 daemon（5a 保证存在），**不再现起线程**。Plan 侧另有 `_queue_plan_hydration`（`orchestrator.py:1607`）= 写 inflight + `request_signature` 去重 + 返回 `status:"pending"` 范式。
- **改哪些 handler**：`post_plan`（`api.py:1134`，LLM plan-compile 内联）、`post_jobs`（`api.py:1204`，整条检索内联，docstring 自承「Synchronous」）、三导出（`post_target_candidates_export:1490`/`post_crm_public_web_export:1536`/`post_projections_export:1748`，大 body 在线程内组装）、两 refine-compile（`api.py:1421/1434`）。
- **响应形状变化 + 前端影响**：同步 200+完整体 → 202+`{job_id,status:"queued"}`，前端 poll `/api/jobs/{id}/progress`（已有）或新 plan-status 端点取结果。前端**已在轮询 progress**，故 `post_jobs` 几乎零新摩擦；`post_plan`/导出需新增「先拿 token 再 poll 下载」交互。**characterize-first 必须**：这是 API 语义变更，先把现同步形状钉进 transport-parity 套件，再迁，避免静默破坏前端。

### C3 进程分离 + 5d
- **C3a API-never-runs-recovery（已落地）**：`serve` 默认不起 `start_shared_recovery_service` 或 `start_server_runtime_watchdog`，`assert_recovery_coverage_or_fail_closed` 必须证明 fresh external `worker-recovery-daemon` 才能创建 server。唯一启用进程内路径的入口是 dev-only `--enable-runtime-watchdog`；旧 `--disable-runtime-watchdog` 是可解析的兼容 no-op。启动任一阶段失败也经统一 `finally` 停止并 join 已启动线程。`--allow-uncovered-recovery` 仍只是 loud opt-out，不会暗中启动 recovery。**wakeup 生产者留在 API**（`_signal_shared_recovery_wakeup`、durable_runtime `_signal_recovery_wakeup`）——API = 信号者，worker = runner。
- **C3a 范围边界**：该切片只改变 same-host/systemd 下的默认运行拓扑，没有新增 outbox consumer、dispatch/command 状态迁移、schema 或 PG advisory singleton，也不允许宣称跨容器 C3 完成。
- **5d durable outbox consumer**：channel 半成品——生产者 `enqueue_runtime_outbox`（`control_plane_live_postgres.py:3919`）、marker `mark_runtime_outbox_dispatched`（:3975）、ready 索引 `idx_runtime_outbox_ready`（:5796），**缺 consumer**。补 `claim_runtime_outbox` verb，**逐字复用** `claim_workflow_recovery_intents`（:2855）的 `FOR UPDATE SKIP LOCKED` + lease 模式（`UPDATE…SET status='claimed',lease_owner,lease_expires_at WHERE outbox_id IN (SELECT…WHERE queued or expired-claim ORDER BY not_before_at LIMIT n FOR UPDATE SKIP LOCKED) RETURNING *`），在 `run_worker_recovery_once`（`orchestrator.py:38184`）每 tick 调用、dispatch、`mark_runtime_outbox_dispatched`。**为何需要**：host-local wake-file（`service_daemon.py request_service_wakeup`）仅同主机/共享 bind-mount 有效；api 与 agent-worker 拆成无共享卷的独立容器后 wake 文件不可见，sub-second 唤醒静默降到 30s backstop（5c）——5d 是跨容器 durable floor。
- **跨容器单写者**：flock 是 host-local，拆容器后两个 worker 各抢各 host 的锁、双跑。改走 PG-native：`_advisory_lock_key`（`control_plane_live_postgres.py:6052`）schema-prefixed 的 database-global advisory lock。**rollout 约束：锁身份已变，必须全停重启、禁新旧进程共存**（systemd 天然满足）。
- **LISTEN/NOTIFY 何时进**：**仅** 5d 之上、容器拆分后求 sub-second 跨容器唤醒时（outbox-enqueue 时 NOTIFY + worker LISTEN）。`claim_runtime_outbox`+poll 单独已正确，NOTIFY 只买 latency，非正确性必需——与 memo 的「拆进程时再 revisit」一致。

### C2 最小鉴权 + 身份
- **token 方案**：~20 用户按**单 org / 多 workspace**（代码已 tenant=workspace 建模）。静态 per-user bearer token（env 或 PG `api_tokens` 表）→ resolve 到 `(requester_id,tenant_id)`，无登录 UI/session store。
- **middleware slot**：在 `create_app`（`api.py:76-82`）的 concurrency 与 CORS **之间**插 `_AuthMiddleware`（CORS preflight 短路 OPTIONS 之后、routing 之前）：读 `Authorization: Bearer`，未知 token 401（`/health` 与 webhook 路由豁免，后者保留自有 token `api.py:2321`），把 identity 塞进 `scope["state"]`。
- **store tenant-scoping 强制（load-bearing）**：身份注入是必要非充分——**真正修复**是收紧读路径。`post_plan_submit`/`post_jobs` 改从 `request.state` 取身份、**服务端覆盖** body 的 `requester_id/tenant_id`；`list_jobs`（今**无任何 tenant filter**）、`get_job_api`（今裸 job_id 读，任何人猜 id 越权）、`get_crm_record`（今 by-id 不 scope，跨 workspace 读）加 WHERE scope。边界：**user-private** = jobs/query_dispatches/CRM/workspaces/exports/(future agent_sessions)；**故意 shared canonical** = company_assets/evidence/profile registries/projections（dedup 复用 fabric，不加 scope）。
- **前端协同**：今前端不发身份（fetch 只设 Content-Type，`workspace_id` 硬编码 `"default"`）。改：静态 token 存客户端 + `Authorization: Bearer`；删 client 发的 requester/tenant/workspace（服务端派生）；CORS `Access-Control-Allow-Headers` 加 `Authorization`（今仅 Content-Type，`api.py:266`）；处理 401 重提示。

## 4. Owner 决策点

- **(a) 部署形态与时机**：建议 **C1/C2 仍在现 systemd 两进程（serve + 独立 worker daemon，`SERVER_RUNTIME_BOOTSTRAP.md`）上做**，docker-compose per-role 推迟到 C3 真正拆进程时一次性切（届时 flock→advisory lock + 5d 一起上，借全停重启窗口）。理由：C1/C2 不需要容器化即可拿 latency + 隔离收益，避免过早扛容器运维。
- **(b) 鉴权方案 + 是否给前端登录**：建议**单 org bearer token，不做登录 UI**（配置/粘贴 token 即可）。理由：20 用户、tenant=workspace 已建模，多租户与登录是过度工程；token 表给将来留扩展位。
- **(c) C1 前端协同时机**：建议**与 C1 同期但分端点灰度**——`post_jobs` 因前端已轮询 progress 可立即切（低摩擦）；`post_plan`/导出的 202+下载交互稍晚。理由：避免一次性逼前端改全部交互。
- **(d) C5 是否 Track C 交付物**：建议**并入 Track D**。agent_session/agent_turn/agent_events/SSE 与 Track D 北极星（服务端 agentic loop、Option 3 全事件流 driver）完全重叠，且 `agent_events`/SSE 表今天不存在。Track C 只交付到 C4（OpenAPI+SSE 基础设施），C5 作为 D 的 serving 落地。
- **(e) 5d + LISTEN/NOTIFY 触发条件**：建议 **5d 与 C3 进程拆分捆绑**（不共享卷的那一刻起必需）；**LISTEN/NOTIFY 仅当容器已拆且 30s backstop 的恢复 latency 被实测判定不可接受时再加**——不预先引入（与「当前规模不引入」一致）。

## 5. 非目标 + 轨道边界

- **明确非目标**：Redis；PG LISTEN/NOTIFY（仅 C3 进程拆分后按 (e) 条件 revisit）；per-user 常驻容器（沙箱仅将来加代码执行/浏览器工具时按工具调用租用）；对象存储读穿（C6 later）。
- **与 Track D 边界**：C5 多用户 agent 拓扑（agent_session/agent_turn/SSE/per-user 限额）= Track D serving 落地，不在 C 独立交付；Track D 北极星 = Option 3 全事件流 recovery driver。
- **与 M2 provider runtime 边界**：8 槽 shared 信号量（`api.py:64`）是 HarvestAPI ~8 actor 隐性限制的临时护栏，**保护必须随 M2 移到 provider 层**（per-provider+key 信号量/令牌桶）后 HTTP 入口并发上限才放开——C1 enqueue+poll 不替代此护栏，只把重活移出请求线程。
- **与冻结 mesh 边界**：`docs/SERVING_MESH_OWNERSHIP_BOUNDARY.md` 的四块切分（CandidateSourceResolver / ServingReadModel / ProjectionCommandOwner+9 读族 / fast-path）随 M3–M5 迁移，Track C 不动其代码；C 的读路径加 tenant scope（C2）须落在 user-private 表，不触碰 shared canonical 读族。
