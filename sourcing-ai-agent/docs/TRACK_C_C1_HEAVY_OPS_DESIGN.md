# Track C — C1 重活出请求线程（Heavy-Ops-Out-of-Request-Thread）设计

> Status: C1 design — **RATIFIED 2026-06-15**. Heavy-ops-out-of-request-thread — substrate-unify (consolidation-first).
>
> **Owner 批准（2026-06-15）**：(北极星 a) 承诺统一 durable-task 基底 + substrate-unify；(b) **C1 一次做全**——零摩擦删除 + 导出后端异步化 + 导出前端 202→poll→download UX + refine-compile ×2 异步化，全部在 C1 批次内；(c) **现在定统一 async-task 契约形状**（submit→202+handle→poll/stream→artifact，idempotency-key 即锁即结果键），逐端点灰度切入。战术裁定（主 Agent 推荐、推进中）：`run_job` **demote 为 CLI/test helper**（非彻底删——它跳过 acquisition、是 retrieval-over-snapshot，CLI 改 enqueue 会变语义）；导出落**最小本地 artifact handle**，C6 平移 object-storage 读穿（不阻塞到 C6）。

文件路径均相对仓库根；行号锚定 `src/sourcing_agent/`。本文件只设计、不改码。所有锚点已于 `52fa96f` 逐一核验。

**C1 的核心问题不是「把每个重 handler 包成 enqueue+poll」**，而是：这些同步重 handler 是否与既有异步机制 **冗余（redundant）**，从而最优解是 **合并（consolidate）+ 删除冗余同步轨**，而非平行包装。按 OWNER DIRECTIVE（2026-06-15，删除/替换 > 保留双轨），下文逐 handler 给出冗余判定与处置。

## 2026-07-14 C1b scoped erratum

本文件的 durable `202 + task_id` 北极星没有改变，但 Plan 当前实现必须以
`TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md` 和 `plan_submit_contract.py` 为准：`POST /api/plan/submit` 仍是
HTTP `200` + `status=pending` 的 report-visible compatibility bridge，不是 durable task。服务面唯一执行链为
`submit_plan_workflow -> _queue_plan_hydration -> Thread(target=_run_plan_hydration) -> plan_workflow`；缺失 submit
owner 时返回 retryable `503 plan_submit_owner_unavailable`，禁止同步 compile fallback。CLI `plan` 是显式 one-shot
helper，不属于 serving 链。共享 async-task adapter 的 public `task_id` 与 `artifact.handle` 均由 domain owner
提供；adapter 不假设 handle 等于 command id，missing/unknown domain status 终止为 failed。C1b 不实现 schema、
durable consumer、202 cutover、compute/publish split、TTL 或 replay；这些仍受 D-C1-1..4 与 C1c-e 顺序约束。

---

## 1. 现状真相：同步重 handler 与其异步孪生

8 槽 shared 信号量（`api.py:64`）是 serving 天花板；重活整段（LLM compile / 检索 / 归档组装）跑在请求线程里，全程占住 1/8 槽满 wall-clock。逐路径冗余判定（均已读码核验）：

| 重活 | 同步入口 | 异步孪生 | 冗余判定 |
|---|---|---|---|
| **jobs（检索）** | `post_jobs` `api.py:1204`→`run_job` `orchestrator.py:3703`（201，整段内联，docstring 自承 "Synchronous"） | workflow 路径 `post_workflows` `api.py:1209`→`start_workflow` `orchestrator.py:2178`→`queue_workflow` `orchestrator.py:2419`→202→daemon 跑 `run_queued_workflow` `orchestrator.py:2671` | **部分冗余（共享同一检索引擎，scope 不同）**。`run_job` = `_build_augmented_sourcing_plan`（内联 LLM）+ `_run_retrieval_job`（`orchestrator.py:69848`，**无 acquisition loop**，`_execute_retrieval` `orchestrator.py:69319` 只读既有 snapshot/materialized source）。workflow 路径多了前半段 provider acquisition，终点同为 `_execute_retrieval`。`run_job` ≈ workflow 的「尾半段 + 内联 plan compile」。 |
| **plan（编译）** | `post_plan` `api.py:1134`→`plan_workflow` `orchestrator.py:1467`（200，内联 LLM compile + 同步建 review session） | `post_plan_submit` `api.py:1139`→`submit_plan_workflow` `orchestrator.py:1543`→`_queue_plan_hydration` `orchestrator.py:1607`（pending，request_signature 去重/coalesce，专用 `_plan_hydration_slots`） | **完全冗余**。`_run_plan_hydration` `orchestrator.py:1799` **字面调用 `self.plan_workflow(dict(payload))`**——异步 worker 跑的就是同步方法本体；同样的 compiled envelope、同样的 `create_plan_review_session` + history-link 副作用。差异只是 transport 包装。 |
| **导出 ×3** | `post_projections_export` `api.py:1748`→`export_projection_candidates_archive` `orchestrator.py:27937`；`post_crm_public_web_export` `api.py:1536`→`export_crm_record_public_web_archive` `orchestrator.py:50523`；`post_target_candidates_export` `api.py:1490`→`export_target_candidates_archive` `orchestrator.py:51282` | 无端到端异步孪生；但 **command 机制已存在**：`EXPORT_PROJECTION_GENERATE_COMMAND_TYPE` / `EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE`（`durable_runtime.py:78/80`，full CommandTypeSpec :610/:622，含 cancel/resume handler :1129/:1149） | **存储半成品已有，dispatch 缺失**。projection/CRM 已是 command-driven + 持久 artifact（`_publish_export_artifact_if_command_active` `orchestrator.py:45687` 用 `os.replace` 原子写 `runtime_dir/exports/...`，succeeded 命令二次请求直接读盘不重建）。**但 build 仍 INLINE 在请求线程**（owner 方法一次性 claim+build+publish）；artifact 只救「第二次相同请求」。target-candidates 是 laggard（纯内存 `io.BytesIO`，无 command/artifact）且 **已被 GONE 退役门控**（`api.py:1491` `_legacy_target_candidate_export_allowed`，重定向 `/api/projections/export`）。 |
| **refine-compile ×2** | `post_plan_review_compile_instruction` `api.py:1421`→`compile_plan_review_instruction` `orchestrator.py:42640`；`post_results_refine_compile_instruction` `api.py:1434`→`compile_post_acquisition_refinement` `orchestrator.py:49883` | **无任何异步孪生** | **不冗余（genuinely distinct）**。inline LLM（`compile_review_payload_from_instruction(..., model_client=self.model_client)` `orchestrator.py:42653`），「edit 一条 instruction → 编出 review_payload」的交互式编辑面，**未接入** `frontend-demo/src/lib/api.ts`。符合 C1「重活在请求线程」问题，但属 additive async-ification，非 consolidation。 |

**Worker daemon 容量**：driver `run_worker_recovery_once` `orchestrator.py:38184`，事件驱动（C 地基 5a 保证常驻，缺 driver fail-closed）。检索类已证实可跑（`run_queued_workflow` 由 recovery phase 驱动）。**但导出 dispatch 缺失**：recovery phase 全手工枚举（核验 `list_ready_workflow_commands` 五处 caller 均硬绑非导出 command_type：PROJECTION_RUN_SCOPE_FINALIZE / PERSON_SEARCH_INDEX_BUILD / COLLECTION_AUTHORITATIVE_MERGE / LINKEDIN_LOCAL_PROFILE_DELTA_APPLY / DISCOVERY_QUERY_RUN），**无 export-drain phase**。

**前端真相（核验）**：
- `POST /api/jobs`：前端**零调用**（仅 GET 子资源 `/api/jobs/{id}/progress|dashboard|results|...`，key 是 workflow 创建的 job_id）。唯一活 consumer = CLI `run-job`（`cli.py:4408`）+ 测试。
- `/api/plan`（sync `getPlanEnvelope` `api.ts:2717`）：live client `SourcingBackendClient.planNaturalLanguageSearch`（`sourcingBackend.ts:101`）走 `submitPlanEnvelope`→`/api/plan/submit`（**异步**）；sync `/api/plan` 在 `frontend-demo/src` 无外部 consumer，e2e 也不打——**对 live UI 已死**。
- 导出：`exportProjectionCandidatesArchive`（`api.ts:5729`，`ResultsBoardPanel.tsx:1320`、`TargetCandidatesPanel.tsx:1508`）、`exportTargetCandidatePublicWebArchive`（→CRM，`TargetCandidatesPanel.tsx:1540`）均经 `fetchBinary`，**期望同步 200 + blob + X-Sourcing-* headers 触发下载**。`exportTargetCandidatesArchive`（`api.ts:5648`）已 throw/退役。

---

## 2. 设计原则：consolidation-first

1. **冗余即合并删除**：同步重路径若与既有异步机制语义等价，则 **把入口接到异步机制 + 删掉冗余同步轨**（owner 的 delete-don't-dual-track）。不为「保留同步形状」而平行包装。
2. **仅在 genuinely distinct 且无异步孪生处 wrap-in-place**：新建 durable job/command type，沿用同一 enqueue+poll 范式（refine-compile）。
3. **复用现成范式，不新发明**：(a) `queue_workflow`→202+job_id→前端轮询 `/progress`（生产范式）；(b) `_queue_plan_hydration` 的 inflight + `request_signature` 去重/coalesce + history-link poll；(c) export command + 持久 artifact（`_publish_export_artifact_if_command_active`）。
4. **dispatch 一律交 worker daemon**：事件驱动 recovery（5a）已保证常驻；C1 只需让 recovery 多 drain 一类 ready command（导出），并由请求线程 `_signal_shared_recovery_wakeup`（`orchestrator.py:2355`）信号，**不在请求线程现起线程**。
5. **8 槽护栏不动**：C1 不放开 HTTP 入口并发上限（那是 M2 provider 层的事）；只把重活的 wall-clock 移出请求槽。

---

## 3. 每个重活的处置（decision table）

| Handler | 与谁冗余 | 处置 | 前端影响 | durable job/command type |
|---|---|---|---|---|
| `post_jobs` / `POST /api/jobs`（route） | workflow 尾半段 | **DELETE route**（`api.py:1204-1207`）+ 删同步 serving 契约 | **零**（前端不调） | 无（route 消失） |
| `run_job`（内部方法） | 同上 | **DEMOTE，保留**为 CLI/测试 one-shot helper（`cli.py:4408` + `tests/test_pipeline.py` 20+ 用）；从 serving 面下线 | 无 | 仍可内部直跑 |
| `post_plan` / `plan_workflow`-as-HTTP | `_queue_plan_hydration` **完全冗余** | **CONSOLIDATE-into-`/api/plan/submit`**：删 `/api/plan` 同步 route（`api.py:1134`），plan 提交统一走 `submit_plan_workflow`→`_queue_plan_hydration`（已是 live 契约）。`plan_workflow` 方法保留（async worker 仍调它）。 | **零**（live client 已用 `/api/plan/submit`） | 复用现有 plan-hydration inflight（history-link `plan_generation.status`） |
| `post_projections_export` | command 存储已有，dispatch 缺 | **WRAP-ASYNC（split）**：API 拆为「ensure command queued」（202+command_id）/「serve artifact when succeeded」；build 移入 worker | **需新 UX**（202→poll→download artifact handle） | `EXPORT_PROJECTION_GENERATE_COMMAND_TYPE`（已注册） |
| `post_crm_public_web_export` | 同上 | 同上 | 同上 | `EXPORT_CRM_PUBLIC_WEB_GENERATE_COMMAND_TYPE`（已注册） |
| `post_target_candidates_export` | 已退役 | **DELETE**（GONE 已门控，前端入口已 throw，redirect `/api/projections/export`）；连带 `export_target_candidates_archive` 评估删除 | 无（已 throw） | 不新建 |
| `post_plan_review_compile_instruction` | **无孪生** | **WRAP-ASYNC（new）**：新 durable command type，enqueue→202→poll | 无（未接入 api.ts）→ 后续接异步契约 | 新 `plan_review.compile_instruction`（待建 CommandTypeSpec） |
| `post_results_refine_compile_instruction` | **无孪生** | 同上 | 同上 | 新 `results.refine.compile_instruction`（待建 CommandTypeSpec） |

**导出 + 持久 artifact handle 与 C6 的关系**：projection/CRM 的持久 artifact 链（原子写 `runtime_dir/exports/.../<command_id>/<file>.zip` + `mark_workflow_command_succeeded` 记 `artifact_path`，二次请求读盘）**今天已经在本地盘上工作**。C1 **不**需要 C6 对象存储就能交付异步导出——只需一个 **最小 durable-artifact handle**：command_id → 命令 succeeded 后，新增/复用一个 `GET /api/exports/{command_id}/artifact`（或 `GET /api/jobs/{command_id}/...` 风格）从 `artifact_path` 流式回 blob + 同样的 X-Sourcing-* headers。**C6（object storage 读穿）是把这个本地 `artifact_path` 读取换成 object_storage 抽象的 read-through——纯实现替换，handle 契约不变**。故 C1 现在落最小本地 handle，下载读取在 C6 平移，不阻塞。

---

## 4. 契约与前端

| 端点 | 旧（sync） | 新（async） | 前端改动 | 摩擦 |
|---|---|---|---|---|
| `POST /api/jobs` | 201 + 完整 artifact | **删除**（404/410） | 无 | 零（无 consumer） |
| `POST /api/plan` | 200 + compiled envelope | **删除**，并入 `/api/plan/submit` | 无（live 已用 submit）；清理 api.ts 死代码 `getPlanEnvelope`/`getPlan` | 零-friction |
| `POST /api/plan/submit` | 已 pending | 不变（已是范式） | 已轮询 history-link `plan_generation.status`（`historyRecovery.ts`） | 零-friction |
| `POST /api/projections/export` | 200 + blob + X-Sourcing-* | **202 + `{command_id,status:"queued"}`** | poll command status → 命令 succeeded 后 `GET .../artifact` 取 blob 触发下载 | **需新 UX**（`ResultsBoardPanel.tsx:1320`、`TargetCandidatesPanel.tsx:1508`） |
| `POST /api/crm/.../export`（public-web） | 同上 | 同上 | 同上（`TargetCandidatesPanel.tsx:1540`） | **需新 UX** |
| target-candidates export | GONE | **删除** | 无（已 throw） | 零 |
| refine-compile ×2 | 200 + review_payload | 202 + poll | 今无前端；新增异步客户端契约 | 新 UX（但 additive，无现存破坏） |

**零-friction（前端已轮询）**：jobs（删除）、plan（已走 submit + 轮询）。**需新 UX（202→poll→download）**：两个导出。**additive（无现存破坏）**：两 refine-compile。

---

## 5. 验证

1. **characterize-first（先钉死再迁）**：迁移前把当前同步响应形状 pin 进 transport-parity 套件——`POST /api/plan` 的完整 envelope（status/request/plan/plan_review_gate/plan_review_session/...）、两导出的 200+blob+X-Sourcing-* headers、（若保留作回归基线）`POST /api/jobs` 201 artifact。这是 API 语义变更，无 characterization 即静默破坏前端。
2. **transport-parity 扩异步契约**：套件加 202 形状（`command_id`/`job_id` + `status`）、poll 端点形状、artifact handle 的 headers parity（异步下载的 X-Sourcing-* 必须与旧同步 blob 逐字段相等）。
3. **consolidated 路径 no-regression**：plan——断言 `/api/plan/submit`→`_run_plan_hydration`→`plan_workflow` 产出的 envelope 与旧 `/api/plan` 同步产出逐字段等价（含 `create_plan_review_session` + history-link 副作用）；jobs——CLI `run-job` + `tests/test_pipeline.py` 仍绿（`run_job` 仅 demote 不删）。
4. **导出 worker-drain 正确性**：新 export-drain recovery phase 必须证明 claim→build→`_publish_export_artifact_if_command_active`→`mark_workflow_command_succeeded` 端到端；二次相同请求短路读盘（已有逻辑）不回归；cancel/resume handler（`durable_runtime.py:1129/1149`）在 worker 路径仍触发。
5. **证明 8 槽 head-of-line 真被解开**：load test——N(>8) 并发重请求（plan compile / export），迁移前测 p95 latency 与轮询饿死（重载下轮询掉到 2 保底槽）；迁移后断言重请求 202 立即返回（占槽仅 enqueue 时长 ≪ 全 wall-clock），并发轻请求/轮询不再排在重活后；worker 侧 `_plan_hydration_slots` / export 并发受其自身 semaphore 约束、不吃 8 槽。

---

## 6. 实施子步 + 风险 + Owner 决策点

**子步（建议序）**：
1. characterize-first：pin 三类同步响应形状（§5.1）。
2. **删 `POST /api/jobs` route**（最低风险、零 consumer）；`run_job` demote 为 CLI/test helper。
3. **删 `POST /api/plan` route + api.ts 死代码**；plan 统一 `/api/plan/submit`（live 已用，零前端改）。
4. **删 target-candidates export**（GONE 已门控）。
5. **导出异步化**：新增 export-drain recovery phase（drain ready EXPORT_* command）+ API split（202+command_id / artifact handle）+ 请求线程 `_signal_shared_recovery_wakeup`；前端两处加 202→poll→download UX。
6. **refine-compile ×2**：新建两个 CommandTypeSpec + enqueue+poll（additive）。

**风险**：
- (R1) plan consolidation 须证 async envelope 与 sync 逐字段等价——`_run_plan_hydration` 字面调 `plan_workflow`（`orchestrator.py:1799`），风险低但 history-link 投递/coalesce 路径需覆盖。
- (R2) 导出 worker-drain 是 C1 唯一「新基础设施」（recovery 今无 export phase）；须确保不与既有手工枚举 phase 抢 tick budget / lease。
- (R3) 导出 202 改 X-Sourcing-* headers 现由同步 blob 直接带；异步下载 handle 必须复刻全部 header（parity 测试守）。
- (R4) `run_job` demote 后若有未审计的非前端 HTTP consumer（监控/脚本直打 `/api/jobs`）会 404——删 route 前 grep 部署侧脚本/反代日志确认。

**R4 pre-flight 结果（2026-06-15 核验）**：live demo 前端（`frontend-demo/src/lib/api.ts`）确实零 POST `/api/jobs`、零活 `/api/plan`（`getPlan`/`getPlanEnvelope` 死代码、无组件 caller；live 走 `submitPlanEnvelope`→`/api/plan/submit`）。但路由删除有**两类非 live-前端 consumer 必须同步处理**：
1. **测试**：`tests/test_pipeline.py`（POST `/api/jobs` HTTP 集成测试 + 两车道并发用例以 `/api/jobs` 为 heavy 范例 + `_request_priority_lane` 分类）；`tests/test_results_api.py` / `tests/test_projection_crm_api_contracts.py`（POST `/api/target-candidates/export`）。这些随各自路由提交一并改（HTTP 集成测试转直调 `run_job`；并发用例 heavy 范例改指仍存的 shared 路由）。
2. **contracts/ 参考 SDK**（`frontend_api_adapter.ts` 的 `plan()`/`runJob()` 方法、`frontend_api_contract.ts`/`.schema.json` 的 `PlanResponse`、`frontend_react_hooks.example.tsx` 的 `useSourcingPlan`、dashboard 示例）——一个**自洽的参考工件**，删路由会波及全套。**决策：contracts/ SDK 不在 C1.3 逐路由零敲碎打，而是随 C4（OpenAPI + 契约重生成）整体反映 async-only 契约（plan-submit/poll、workflow、export-async、refine-async）一次刷新**。依据：contracts/ 非编译进 live 前端、governance 测试（`test_pre_agent_contract_review`）不强制 adapter↔route 双射、且已 pre-handoff dirty；逐路由切割一个连贯参考件风险高价值低，与 decision (c)「定形状现在、OpenAPI/形式化随 C4」一致。C1.3 期间 contracts/ 的 `plan()`/`runJob()` 指向已删路由属**已知 interim 陈旧**（仅参考文档，不影响 live serving），C4 统一修。

**C1.3 实际作用域**：live api.py 路由删除 + live frontend-demo 死代码删除 + live 测试更新；contracts/ 参考 SDK 刷新 → C4。

**target-candidates export 处置 —— 改为 DEFER（2026-06-15 深查后修正）**：原计划「DELETE（GONE 已门控）」假设它是死代码。深查发现 `_legacy_target_candidate_export_allowed()` 的 env 逃生阀（`SOURCING_ALLOW_LEGACY_TARGET_CANDIDATE_EXPORT`）**不是死代码，而是一个有意保留、有契约、有测试的迁移 affordance**：
- `tests/test_pre_agent_contract_review.py`（governance）把该 env flag 与 `_legacy_target_candidate_export_allowed` 断言进 **migration-only env inventory**（contract-visible 要求）；
- `tests/test_results_api.py:15097+/15396+` 设 flag=1 **实测 legacy 真导出（200）路径**。
默认（flag 未设）已返回 410 GONE tombstone（含 `canonical_export_path` 重定向）——**生产零 serving 成本**。移除逃生阀 + 删 `export_target_candidates_archive`（唯一 caller 是该 route）会改动 governance 迁移契约 + 两个 migration 测试，属**迁移 cutover 决策，非 C1 serving 清理**。按 deep-context 原则（目标若非计划所设想的「死/冗余」则重新评估），**defer 到迁移 cutover 里程碑**（cutover 完成、affordance 可证不需要时一并删 flag/inventory/tests/method）。characterize-first 的 410 断言不受影响（默认行为不变）。C1.3 实际交付 = `/api/plan` + `/api/jobs` 两条同步 serving 路由删除（c92e5fe）。

**Owner 决策点**：
- (a) **导出 download UX 时机**：建议 plan/jobs/target 删除（§子步2-4，零前端摩擦）**立即切**；两导出的 202+poll+download **稍后灰度**（需前端新交互）——与 Track C plan §4(c) 分端点灰度一致。是否接受导出晚于其余落地？
- (b) **`run_job` 是否彻底删**：现保留作 CLI/test one-shot。若 owner 要更彻底，可把 CLI `run-job` 也改为「内部 enqueue 同一 workflow tail」从而完全删 `run_job`——但 `run_job` 跳过 acquisition、是 retrieval-over-existing-snapshot，CLI 语义会变（会触发 acquisition）。建议**保留**。请裁定。
- (c) **artifact handle 最小落地 vs 等 C6**：建议 C1 落最小本地 `artifact_path` 读取 handle（C6 平移为 object_storage 读穿）。确认不把导出下载阻塞到 C6？
- (d) **refine-compile 是否纳入 C1**：它 genuinely distinct、今无前端，属 additive。若优先级低可推迟到 C4（与 OpenAPI/SSE 一起做异步契约）。请裁定是否留在 C1。
  - 裁定（2026-06-15）：纳入 C1（owner「C1 一次做全」）。
  - **修正裁定（2026-06-15，实现中深查后 owner 改判）：推迟到 C4**。实现中发现两个 compile 命令虽是快速 leaf compute,但 **Phase 4 cancel/resume 契约要求每个命令类型都有 cancel/resume handler**(`test_command_type_specs` + `test_cancel_resume_dispatch_contract` 共 6 张 pinned handler 表 + golden sha1 重生),叠加完整新命令基设(`_plan`/`_run`/`_drain`×2 + submit/poll + drain bindings + tick/registry oracle + smoke 转换 + 新测试),构成一个远大于导出、且 intricate 的 build——而两个端点**当前零前端消费者**(已核验 `frontend-demo/src/lib/api.ts` 无引用),C1 设计本就标注可推迟。**推迟到 C4**:C4 做 OpenAPI + contracts/ 参考 SDK 重生成时,refine 端点的 reference-SDK 消费者(`frontend_api_adapter.ts` `compilePlanReviewInstruction`/`compileRefinement`,当前 model inline-200)本就要随契约一起改成 202+poll,届时连同 contract 一次性做更连贯。已加的部分(constants/specs/idempotency/golden)已干净回退。**C1 核心交付(删冗余同步轨 + 统一 async-task 契约 + 整个导出面 head-of-line 异步化,后端+前端,经两轮独立评审 GO)视为完成。**

---

## 7. 统一 async-task 契约形状（decision c：现在定形状，逐端点灰度）

C1 落地的所有重活端点遵守**同一个 async-task 契约**；该形状一次定义，被 plan/检索/导出/refine-compile 与未来 agent-turn 复用，C4 的 SSE 直接 tail 同一 event/outbox spine。**形状是契约，不是新表**——它落在既有 command/event/outbox 基底上，下表是 transport 层的统一约定。

**提交（submit）**
- `POST <resource>`（body 带业务参数 + 可选 `idempotency_key`；缺省时服务端按既有去重键派生——plan 用 `_plan_hydration_request_signature`，导出用 `export_*_generate_idempotency_key`）。
- 服务端：原子 append typed command（idempotency-key 即 in-flight 锁 + 结果回放键：in-flight 重复 → 返回同一 handle；已 succeeded → 直接回放 artifact handle）→ `_signal_shared_recovery_wakeup` 信号 worker → **202** `{ "task_id": "<command_id|job_id>", "task_type": "<plan.compile|retrieval|export.projection|...>", "status": "queued", "idempotency_key": "<key>" }`。请求线程只做 enqueue+signal（≪ 全 wall-clock），立即释放 8 槽之一。

**观察（poll；C4 增 SSE tail 同一 event log）**
- `GET <resource>/{task_id}`（或既有 `/progress` 风格读子资源）→ `{ "task_id", "task_type", "status": "queued|running|succeeded|failed|cancelled", "error": <null|{...}>, "artifact": <null|{handle}> }`。状态机 `queued → running → {succeeded, failed, cancelled}`（+ `expired`）。poll 廉价：**结果不内联**，只回 artifact handle。

**取结果（artifact）**
- `GET <artifact handle>` → 流式回 blob + 业务 headers（导出：逐字段复刻旧同步 200 的 `X-Sourcing-*`）。C1 落**最小本地 handle**（从命令 succeeded 记的 `artifact_path` 原子读盘流回）；C6 平移为 object-storage 读穿——**handle 契约不变**。

**取消（cancel，可选 transition）**
- `POST <resource>/{task_id}/cancel` → 复用已 wired 的 cancel handler（导出 `_cancel_running_export_command` `durable_runtime.py:1129`），命令转 `cancelled`。

**逐端点灰度顺序**（形状不重谈，切入分批）：jobs/plan 零摩擦立切（删同步 route，plan 已走 `/api/plan/submit`）→ 导出（202+poll+download，前端两处加 UX）→ refine-compile（additive，新建客户端契约）。所有端点回的 `task_id`/`status`/`artifact` 字段名一致，前端一套 poll/download 逻辑可复用。
