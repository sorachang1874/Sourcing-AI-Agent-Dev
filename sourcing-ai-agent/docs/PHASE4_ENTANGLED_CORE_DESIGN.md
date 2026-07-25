# Phase 4 Entangled Core 重设计提案

> Status: Executed (Steps 0-5e complete 2026-06-15); Step 2b cascade-cluster migration re-opened as refactor batch B2 (REFACTOR_MASTER_PLAN.md WS2 切片 1, 2026-07-22)。**锚点勘误 2026-07-22 @69b7423**：本文行号锚点采于 ~74k 行树，现树 82.5k（+8.4k 回涨），普遍偏移 ~2.4k 行——已重测关键锚：`run_worker_recovery_once` = orchestrator.py:40442-42605；其余锚点随 B2 迁移 pass 逐段重测，勿按旧行号直接动刀。原始状态行：Proposal awaiting owner review (2026-06-12)。Track A Phase 0–3 收官后的 Phase 4 设计稿（NEXT_TODO Track A Phase 4 要求动工前 owner 过设计讨论——即本稿）。输入 = 三份审计：A（recovery/control core）、B（projection 网数据流与所有权）、C（约束清单）；全部 file:line 锚点已对当前树（`governance-phase0-ttl-20260611` @ cae2ba5）抽查核验。范围约束来自 `SERVICE_GRADE_ARCHITECTURE_PLAN.md` 2026-06-11 Plan Revision：recovery phase 编排 registry 化在 Phase 4 内做；projection/candidate_source/asset_population 网随 M3–M5 拆解，Phase 4 只定边界。

## 1. 现状图：post-Phase-3 残余纠缠核心

命令层已完整出走：CommandKernel（17 store-only 方法）、`DEFAULT_COMMAND_TYPE_SPECS` 41 类型、4 域 owner、`DEFAULT_RECOVERY_DRAIN_BINDINGS` 14 个 uniform tick drain。orchestrator ~74k 行，残余核心分三带（`orch` = `src/sourcing_agent/orchestrator.py`）：

**A 带 — recovery tick**（`run_worker_recovery_once`，orch:38028–40063，~2,000 行单函数，~33 个 phase）：
- 基建闭包统一计量/skip/预算：`_run_recovery_phase`/`_skipped_phase` 38100–38198、`_tick_budget_exhausted` 38046（30s `RECOVERY_TICK_TOTAL_BUDGET_MS`）、`_phase_budget_yield_requested` 38319–38332（递归扫 `*_budget_exhausted`）；epilogue 39961–40063 汇总 20+ 结果 ⇒ `next_tick_requested` 40018。
- **bespoke 级联族**（registry 刻意不建模的部分，`recovery_drain_registry.py:5–16`）：
  - profile_refill 链：pre_worker refill 38589 → worker_recovery 38623 → refill 38674 → refill_command_owner 38728 → 级联 A materialization_followup 38818（仅当"观察到工作但未提交"38805–38810）→ local_apply_backlog 38869（4 路 skip 阶梯，含 38507 store 探针）→ 级联 B 38990（6 个取反 local 守卫 38975–38982）→ post_event_level refill 39050。
  - workflow_resume 三波（39146 / 39303 post_projection / 39838 post_followup）+ post_completion_reconcile 两波（39179 / 39884），后波 **extend** 前波结果。
  - one-durable-unit-per-tick projection 串：board_visible_apply 39264 → run_scope_projection_finalize 39279 → snapshot 39363 → facet_layering 39406 → person_search_index 39462 → collection_merge 39528（7 分支阶梯收尾）；skip 阶梯单调增长（2,3,4,5 个前序工作守卫）。
  - followup 尾环：explicit_job_followup_rounds 39603（inline 重建 daemon、合并 summary）、remote_event_followup 39702 → `_run_remote_event_followup_rounds` 37332（**4 元组 rebind 线程化状态**）、级联 C 39745 + 三个 post_followup 波 39794/39838/39884。
- **跨 phase 线程化 locals**：`search_seed_resume_skip_job_ids`（38547 注入全部后续 resume）、`profile_refill_submit_observed_this_tick`（5 处重赋值：38616/38707/38751/39064/39808）、`worker_recovery_handoff_required` 38629、`same_tick_durable_work_*` 字符串在 5 个后续 skip 分支逐字复用——这些 nonlocal 耦合是 Phase 3 收尾 binding registry 止步的精确边界。
- **HarvestAPI 预算泄漏进 tick**：phase #7 limit = `_env_int("PROFILE_PREFETCH_REFILL_DISPATCH_WORKER_LIMIT", resolved_harvest_profile_actor_global_inflight({}))`（38719–38726）——M2 provider 预算的临时寄居点。
- 2 个 CRM drain（39220/39234）因 `tests/test_crm_public_web_runtime_boundary.py:948–953` 钉死字面 callback 源码而保留名调用（`tests/test_recovery_drain_registry.py:196–205` 记录此事）。

**B 带 — projection / candidate_source / asset_population 网**（审计 B）：
- 读：`api.py:978` dashboard → `get_job_dashboard` 26669 → `_build_job_results_context` 23856 → `_resolve_job_candidate_source` 23636；共享读模型 `_build_public_board_runtime_projection` 25772；行数据走 `serving_projection_reader.get_projection_candidates`（reader:75），fallback 走 legacy overlay `_load_candidate_source_asset_population_overlay` 5367 + 缓存。
- 写：fast path `_execute_asset_population_fast_path` 68644 直接构建 candidate_source_summary 进 job summaries（68851/69326）= **写入读模型的输入**；`_persist_job_result_view` 22285 → run-scope projection 发布 7685；6 个 projection 命令族（plan/enqueue/process/drain 带：6922–9714、12539–13800、16311–17611、18131–21800）。
- **双向环**：candidate_source 解析中调 reader.get_projection（5017–5026），而 projection 翻页又以 candidate_source 为输入（30413）——经 orchestrator 的读环。
- Phase 3c 留守的 9 方法投影读族（经 orch:977 注入 callable 由 ProfileFetchOwner 触达 16311）——审计 B 判定自然归属是 projection 族而非 profile_fetch。

**C 带 — cancel/retry/resume 控制带**：API 三件套 45616/45670/45722（状态不变量：generic cancel 仅 `queued|retry_wait`；retry 仅 `failed_terminal|cancelled`；resume 仅 `retry_wait`）；`claimed|running` 走 owner dispatcher：cancel 42963–43025（15 个 owner+command_type 分支，fall-through 返回 `running_command_requires_owner_specific_cancel`）、resume 45342–45382（11 分支）。owner cancel 写 Activity/Attempt/EntityDelta 证据后 CAS。

**已经是干净接缝**：14 uniform drain registry（特征化先行、b789cd8 对照树同绿的先例）；reader/writer/snapshot_materializer/results_store/retrieval_runtime 谓词/storage 表均在 orchestrator 外且自然归属不变（审计 B 所有权表"stays"列）；C 带的 API 层不变量本身清晰，脏的只是 dispatcher 形态；tick 宿主已有 daemon（`worker_daemon.py:293`）/sidecar（orch:41875）/线程三形态。**仍散落的 recovery 触发点**：`queue_workflow` inline `ensure_*_recovery`（2224–2229、2607–2608）、progress 读路径 auto-recover（36038/36431）、`api.py:1959` HTTP 端点在 API 进程内直跑 tick——这些是 (d) 进程分离的前置债务。

## 2. 设计轴与选项

### (a) recovery phase 编排：bespoke 级联 drain 的归宿
- **A1 扩展 binding 语义**：给 `RecoveryDrainBinding` 加 depends_on/gating/result-merge 数据字段。取舍：`_merge_profile_refill_results`（38334，多源合并逻辑）、4 元组 rebind（37332）、单调增长的 skip 阶梯（39363–39528）都不是数据能表达的——会发明一个 ad-hoc DSL，复杂度换位置且更难 debug；registry docstring 已明文拒绝（registry:5–16）。
- **A2 阶段对象 registry（推荐）**：`RecoveryPhase` protocol（`name` / `owner` / `wants_to_run(ctx)` / `run(ctx)`）+ `TickContext`（预算时钟、前序 phase 结果索引、yield 请求、线程化 locals 的具名字段）。每个 bespoke phase 的 callback body verbatim 成 phase 对象，gating 守卫变 `wants_to_run`，nonlocal → ctx 字段是**唯一非 verbatim 变换**；14 uniform binding 退化为一类 phase 的批量构造器；2 个 CRM 钉死 drain 原样留在分发函数内（守卫不动）。约束契合：满足审计 C #1（Phase 4 = registry 化编排）、#3（phase 对象天然可观测/可暂停——Agent-native 不得后补）、#7（per-phase owner evidence block + `recovery_phase_metrics` 形态由 ctx 统一产出，特征化锁定）。
- **A3 保持现状到 M3–M5**：各域随域提取顺手拆。取舍：直接违反审计 C 约束 1（plan 已定 Phase 4 做 registry 化）；且 2,000 行函数持续是每次域迁移的冲突中心，(d) 进程分离前还得再碰一次，两次大动作各付一次全量回归。
- **推荐 A2**，两步走：先纯特征化（不动源码），再机械化提取。

### (b) projection/candidate_source/asset_population 所有权切分（执行随 M3–M5，Phase 4 定边界）
- **B1 单一 ProjectionOwner 巨域一次全搬**。取舍：审计 B 列出的方法跨 orch 3943–24090、25596–26661、30413+、68486–69400，含 overlay 文件 IO + 缓存族 + fast path——单批风险超 Phase 3 任何一次，且违反约束 1 的时序。
- **B2 按审计 B 所有权表的"natural owner"列切四块（推荐）**：
  1. **ResultView/CandidateSource resolver 模块**：candidate_source 解析 + result_view stub（orch 3943–5367、22677–24090）；解掉双向环的关键——resolver 只依赖 reader 接口，projection 翻页改吃 resolver 输出。
  2. **ServingReadModel 服务**（M3–M5 期落地）：public board 读模型（25596–26661、34493–34900）+ asset_population 缓存（fast path 的缓存半边）。
  3. **ProjectionCommandOwner（按命令族）**：6 个 plan/enqueue/process/drain 带各自成 owner，typed-command 入口已是干净命令面；9 方法投影读族随此块走（不归 profile_fetch）。
  4. **fast path 拆两半**：缓存 → 读模型；完成路径写入 → completion owner（68644 一带）。
- **B3 全部归 M5**。取舍：M5 是 CRM/PublicWeb 质量收口，吞下 acquisition/profile 的投影 = 把今天的纠缠复制进服务边界。
- **推荐 B2**。Phase 4 交付物 = 本切分清单 + resolver 接口冻结（特征化 9 个读 API 的 forbidden-mutation 守卫已存在，`test_pre_agent_contract_review.py` ~1325–1370）；搬动随 M3–M5。若 owner 愿意修订 plan 把块 3 的"projection 串"提前（其 drain 已被 A2 phase 对象化覆盖入口），列为 §4 决策点 #5。

### (c) cancel/resume 控制带注册式化
- **C1 CommandTypeSpec 加槽位（推荐）**：spec 增 `cancel_handler`/`resume_handler`；dispatcher 42963 的 15 分支与 45342 的 11 分支变查表，fall-through 语义（`running_command_requires_owner_specific_cancel`、`module_state_mutated: False`）成 registry 默认值。契合约束 6（one owner per command type）与 Track E 既定方向（守卫测试改对 registry、manifest 导出 cancel 语义）。API 层状态不变量（45616/45670/45722）不动。
- **C2 独立 CancelRegistry**：与 41 类型 spec 注定重复 key，两表漂移。
- **C3 不动**：严格讲 cancel 带不在 plan 的 Phase 4 措辞内，留待 M1 manifest 消费方逼出需求。
- **推荐 C1**（小而机械，但属轻微 scope 扩张——§4 决策点 #6）。resume 族拆开看：workflow_resume 三波本质是 recovery phase → 归 (a)；hosted resume 线程是宿主形态 → 归 (d)。

### (d) 与 Track C 进程分离的时序
recovery tick 进 worker daemon 独立进程的**前置条件**：① 请求路径 inline 触发（2224–2229、2607–2608）改 enqueue 标记由 daemon 拾取；② 读路径 auto-recover（36038/36431）同改或删；③ `api.py:1959` 降级为仅运维端点；④ advisory lock 命名空间化已落地（Track B 2026-06-12，部署约束全停重启）——拓扑变更窗口与之天然合并；⑤ A2 完成后 yield/handoff 语义（38049、40018）已经是 ctx 显式状态，跨进程恢复有据可依。
- **D1 先 (a) 后分离（推荐）**：phase 对象化后 daemon 只是宿主替换；Step 1 特征化快照天然锚定"换进程结果等价"。
- **D2 先分离后 (a)**：2,000 行函数原样搬进 daemon 再改——两次全量回归，且 nonlocal 状态在进程边界上不可见。
- **推荐 D1**；Phase 4 内只完成前置 ①–③，分离本体随 Track C。

### (e) 与调度器重构 reconciliation 的合并/分离
- **E1 合并进 Phase 4**。取舍：15 个预算失败（envelope/packing/coalescing 族，契约只在未跟踪文档）意味着 Step 1 特征化会把**未定契约的行为冻结成金标准**——profile_refill 链的 gating（38805–38810、38334 合并）直接消费 scheduler 输出，基线不干净。
- **E2 分离、scheduler 契约先行（推荐）**：先正式化契约 + 清账 15 个失败 + storeless fail-open 决策（§4 #1/#2），再特征化 profile_refill 链。
- **E3 投影串先行**：projection 串（39264–39528）与 cancel 带不依赖 scheduler 契约，可在 E2 等待期并行特征化+提取。
- **推荐 E2，E3 并行容许**。

## 3. 推荐迁移顺序

| Step | 内容 | 验证策略 | 规模估计 | 风险 |
|---|---|---|---|---|
| 0 | scheduler 契约正式化 + storeless fail-open 决策落地（§4 #1/#2） | 15 预算失败按新契约清账或入特征化测试 | 契约文档 + enrichment 小修 | 低 |
| 1 | recovery tick 特征化 | 金快照 = phase 名序列 + per-phase owner/`max_sync_work`/gating 输入/skip reason/结果摘要 + `recovery_phase_metrics` 形态（约束 7）；当前树与对照树双跑（drain-registry 先例：b789cd8 对照绿）；可按 E3 先做 projection 串切片 | ~1 个测试文件 | 低（只读） |
| 2 | A2 phase 对象 registry | 逐 phase verbatim 提取 callback body（既有 playbook：facade 全保留、AST 级独立对抗验证——Phase 3d 双通道先例、regression_matrix 更新）；nonlocal → ctx 是唯一非 verbatim 点，Step 1 快照覆盖；CRM 钉死 drain 与字面源码守卫不动 | orch 减 ~2,000 行；新 `recovery_phases.py` 约同量 | 中 |
| 3 | C1 cancel/resume spec 槽位化 | dispatcher 查表 + fall-through 默认值特征化；守卫测试改对 registry | ~300 行 diff | 低 |
| 4 | B2 边界冻结：resolver 接口 + 四块切分清单成文 | 设计交付物（本稿 §2(b) 细化为逐方法清单）；不搬代码 | 文档 | 低 |
| 5 | recovery 触发点收编（(d) 前置 ①–③） | 请求/读路径 enqueue 化的行为等价测试；invariant 3（下游就绪即启动、materialize 永不成 barrier）显式回归 | 中等 | 中 |
| — | B2 块搬动、进程分离本体 | 随 M3–M5 / Track C，各自里程碑内验证 | — | — |

每步独立 PR、独立对抗验证、`ci-pre-agent-contract` 全绿后合入。

## 4. Owner 决策点清单

1. **storeless-enricher fail-open**（`enrichment.py:3056–3060`，invariant-7 族）：零派发被报成 completed。**推荐**：改 fail-closed（loud failure + 显式 reason，按 invariant 7 区分 "lookup failed" vs "confirmed absent"），随 Step 0 落地。
2. **scheduler 契约正式化**：envelope/packing/coalescing 行为写成正式契约 + 特征化测试，15 个预算失败清账；不回滚（方向与 M2 provider 预算一致）。**推荐**：批准，作为 Phase 4 Step 0 而非合并体（E2）。
3. **(a) = A2 phase 对象 registry**，含 nonlocal→ctx 这一受控非 verbatim 变换。**推荐**：是。
4. **(c) C1 轻微 scope 扩张**（plan 措辞未含 cancel 带）。**推荐**：批准——机械、低风险、直接服务 M1 manifest 与 Track E 既定方向。
5. **B2 投影串是否提前**：严格按约束 1（随 M3–M5）只交付 Step 4 边界冻结，还是修订 plan 把 ProjectionCommandOwner 的"独立 serving 串"（finalize/layering/index/merge）提前到 Phase 4 尾部。**推荐**：先只冻结边界；A2 落地后视 conflict 压力再议提前。
6. **HarvestAPI 预算泄漏点**（38719–38726 的 computed limit）：Phase 4 仅把它收进 phase 对象的显式参数位，预算本体随 M2 provider runtime 走。**推荐**：是，不在 Phase 4 改预算语义。
7. **`api.py:1959` tick 端点**：删除 vs 降级为仅运维端点（token 门随 Track C 最小鉴权）。**推荐**：降级保留。
8. **2 个 CRM 钉死 drain 的字面源码守卫**何时重写为 registry 断言。**推荐**：随 M5 CRM 收口，Phase 4 不动。

## 5. 非目标

- 不引入 workflow framework（Temporal 等）——plan doc 既有否决。
- 不在 Phase 4 搬动 B 带任何代码（除非 §4 #5 批准提前）；不改 projection 存储后端或 overlay 文件格式（Track B PG-pure 重写另行推进）。
- 不改 41 类型 CommandTypeSpec 既有 shape（只新增 cancel/resume 槽位）；不改 timer/retry 语义（约束 5：timer 唤醒走 reducer-owned events 的契约不动）。
- 不实施 worker/API 进程分离本体（只清前置债务 ①–③，本体随 Track C）。
- 不动 2 个 CRM 钉死 drain 与其字面源码守卫；不动 `_job_is_terminal`/worker supervisor 的 fail-closed 反伪造护栏（约束 10）。
- 不处理 `test_pipeline`（42k 行）测试设计与 PG fixture 收尾项；Phase 3b 留守件（excel 共享 spine + instance-patch 耦合）维持现状随 M3 再议。

## Step 2b 迁移前置 characterization（B2 增量,2026-07-22 @HEAD d2f9e56 后）

实测锚点（替代上文 ~74k 树旧锚）：

- `run_worker_recovery_once` = orchestrator.py **40442–42557**（2,115 行；下一 def 42606）。
- 区域内 `run_phase/_run_recovery_phase` 调用 **35** 处；`TickContext` 构造于 ~40952。
- **线程化状态清单（迁移的核心障碍，实测）**：`nonlocal` 仅余 2 个
  （`recovery_tick_budget_exhausted`、`durable_work_handoff_yield_requested`）；
  其余跨 phase 状态为方法级局部 + 闭包捕获：
  `profile_refill_submit_observed_this_tick`、`profile_refill_command_planned_this_tick`
  （由 `_profile_refill_worker_submit_observed`/`_profile_refill_command_planned_observed`
  闭包在 tick 相对 +581/+592 处写入）、`_profile_refill_owner_drain_payload`/
  `_merge_profile_refill_results` 辅助闭包（相对 +198/+304）。
- 四个 inline 级联簇的 orchestrator 侧注释锚 = 40946–40951（"entangled cascade
  clusters stay inline below as named phase calls"）。
- **迁移法（按本页 §3 Step 2 + 40946 注释的约束推导）**：每簇一个切片；先把该簇的
  `*_this_tick` 局部提升为 `TickContext` 命名字段（oracle 无观测 —— 这些局部不进
  phase 记录，提升是安全的第一步）；再把闭包辅助函数移为模块级纯函数（携带显式参数）；
  最后 phase 体原样搬迁。每切片后 oracle 10/10 byte-identical 为硬门；任何
  观测记录变化 = 停下重设计，不许改 oracle。
