# Next TODO

> Status: Living tracker, rewritten 2026-06-11 as a short rolling file (keep under ~150 lines; rotate completed/superseded content into `docs/archive/`). Full pre-restructure snapshot: `docs/archive/NEXT_TODO_2026-06-10_full.md`.

## Session Resume Rule

- Tests: `PYTHONPATH=src ./.venv-tests/bin/python -m pytest <targets>`（永远跑 targeted 目标，`tests/test_pipeline.py` 全量 457 个测试不要整体跑）。
- 启动任何运行时前先读 `docs/RUNTIME_PREFLIGHT.md`；网络诊断只读（`make agent-network-preflight`），不得改 proxy/VPN 状态。
- PG-only normal path；不要 `git add .`；不可重建资产的破坏性操作仍走 reviewed 流程。

## Track Structure (decided 2026-06-11)

详细依据见 `docs/SERVICE_GRADE_ARCHITECTURE_PLAN.md` 的 "2026-06-11 Plan Revision" 一节。

### Track A — Orchestrator 分解
- [x] **Phase 0 测试前置修缮**（2026-06-11，PR #15）：`tests/source_inspection.py` AST 解析助手；两个守卫测试文件脱离源码切片；patch 接缝；regression_matrix 未映射守卫；边界扫描 rglob 化。
- [x] Phase 1：`CommandKernel` 提取（2026-06-11）：17 个 store-only 协议方法迁入 `command_kernel.py`，facade 全名保留、577 调用点零改动、独立 AST 级验证。
- [x] Phase 2：CommandTypeSpec registry（2026-06-11，= 重定义后的 M1 落地）：`DEFAULT_COMMAND_TYPE_SPECS` 41 类型单一事实源；4 张字典+policy set 族+metrics 表+orchestrator 3 张映射全部收敛；`command_type_manifest()` 导出（Agent tool spec 种子）；金快照特征化测试 + 字节码级独立比对零漂移；src/ 裸 command-type 字面量清零。
- [x] Phase 3a：crm_public_web 域提取（2026-06-12）：首个领域 owner `crm_public_web_owner.py` 落地；orchestrator 保留全部公私方法签名等价 facade、调用点零改动；`regression_matrix` 映射更新；独立 AST 级验证（逐方法对比 git HEAD）。
- [x] Phase 3b：excel_intake 域提取（2026-06-12）：`excel_intake_owner.py` 落地（35 方法 + 异常类 + 7 个模块级 helper verbatim 移动；orchestrator 减 ~1,800 行）；共享 snapshot-materialization 基础设施与 `_run_excel_intake_workflow`（6 个共享 spine 依赖 + instance-patch 耦合）按设计留守、经注入 callable 触达；独立对抗验证 PASS（锚定 2fc3018），`ci-pre-agent-contract` exit 0。已知小尾巴：excel 线程与 PG schema teardown 的间歇性竞态 warning（对照归因 pre-existing，teardown 应 join 线程——留待 fixture 小修）。
- [x] Phase 3c：profile_fetch 域提取（2026-06-12）：`profile_fetch_owner.py` 落地（18 方法 verbatim；orchestrator 减 ~2,450 行）；13 个候选按纪律留守（投影读族 9 个、instance-patch 危险 2 个经 re-resolving lambda、类引用 static 2 个）；对抗验证 CONFIRMED（锚定 0f9db3d），enrichment 12 / results_api 3 预算 id 与对照完全一致；`ci-pre-agent-contract` 全绿。附带发现：test_pipeline 的 profile 切片有 15 个 pre-existing 失败（未迁移的 PG-only storage 漂移,对照归因非提取所致）——test_pipeline 单独设计时一并处理。
- [x] Phase 3d：acquisition command 层提取（2026-06-12）：`acquisition_command_owner.py` 落地（43 方法覆盖全部 8 个 `acquisition.*` 命令类型；orchestrator 净减 2,616 行,现 ~74k）；SCOPE GUARD 全模块扫描确认 Phase 4 保留核心逐字节未动；双通道验证——Claude 对抗验证 CONFIRMED + Codex 异步参考评审 GO（`runtime/reviews/20260612T080442Z_async-reference-phase3d-*.md`,异步通道首个实例）。**Phase 3 四域收官**：CommandKernel + registry + 4 个领域 owner 构成完整命令层。
- [x] Phase 3 收尾：drain 绑定注册式化（2026-06-12）：14 个统一形态的 flag-gated drain 调用点（202 行块）收敛为 `DEFAULT_RECOVERY_DRAIN_BINDINGS` 注册表 + 16 行循环；特征化测试先行（在 b789cd8 对照树同样跑绿）；2 个 CRM drain 因边界守卫钉死字面源码而留点名、bespoke 级联 drain 按界不动（Phase 4 处置）；owner 模块零 diff。**Track A Phase 0–3 全部完成。**
- [x] Phase 4：纠缠核心重设计（设计 `docs/PHASE4_ENTANGLED_CORE_DESIGN.md`，owner 2026-06-12 整体批准按推荐执行）：
  - [x] Step 0：scheduler 契约正式化（`PROFILE_PREFETCH_SCHEDULER_CONTRACT.md`）+ storeless fail-closed + 15 失败清账（enrichment 12→0；results_api 3 移交 B 带）（2026-06-12，`f4c8331`+`ef74475`）。
  - [x] Step 1：recovery tick 特征化（2026-06-14，`77bf767`）：`tests/test_recovery_tick_characterization.py`（549 行，10 tests）钉死 49 行 phase 序列 + per-phase gating 双向 + summary 槽位映射 + 跨阶段 ladder 可观测效果；零产品码改动；独立变异敏感性验证 PASS（reorder/gate-flip/summary-drop 三种全捕获,对照树绿）。
  - [x] Step 2（部分采用）：A2 phase 对象 registry（2026-06-14，`7463404`）：`recovery_phases.py`（`RecoveryPhase`+`TickContext`+loud-failure registry）；8 个 registry 形态 phase + 14-drain group 迁入 seam；oracle 逐字节未改且 10/10。**刻意留 inline 的级联簇**（per-branch owner 分歧、result-vs-metrics 分歧、4-7 路 skip ladder 选 reason+max_sync_work 的 *_work_observed 线程态、workflow_resume 多波、remote_event_followup 4-tuple）——其线程态无法经 ctx 无损表达,迁移会改 pinned phase records,按部分采用纪律留守待后续。
  - [ ] Step 2b（后续）：级联簇迁移——需先解开 per-branch owner（RecoveryPhase.owner 改可计算）、result-vs-metrics 分歧、threaded *_work_observed → ctx 的无损表达；oracle 仍为硬门。
  - [x] Step 3：C1 cancel/resume → CommandTypeSpec 槽位（2026-06-14，`4991d2c`）：两个分发器（cancel 15 / resume 11 分支）改查表；`CommandTypeSpec` 加 `cancel_handler`/`resume_handler`（纯名,getattr 解析）；owner-mismatch fall-through 经 sibling owner-agnostic registry 保留；**全部 1025 个 (owner, command_type) 组合等价 0 mismatch**；API 不变量逐字节一致；manifest 导出 cancel 语义（Track E）；characterize-first（对照树同绿）。
  - [x] Step 4：B2 网格边界冻结成文（2026-06-14，`docs/SERVING_MESH_OWNERSHIP_BOUNDARY.md`）：四块切分（CandidateSourceResolver / ServingReadModel / ProjectionCommandOwner+9 读族 / fast-path 拆两半）逐方法 file:line + 自然 owner + 冻结 resolver 接口签名 + candidate_source↔reader 双向环两条边精确定位与 B2 解法（消 Edge B 重解）+ 迁移顺序（先搬 resolver 解环）；不搬代码,随 M3–M5。纠正一处审计前提:9 读族非 profile_fetch 消费,归读模型层。
  - [ ] Step 5（Option 2,owner 2026-06-14 整体认可；`docs/RECOVERY_DRIVING_REDESIGN_STUDY.md`）：
    - [x] 5a daemon-保证 fail-closed 断言 + 5b 事件信号唤醒（2026-06-14，`767f77a`）：serve 启动 `assert_recovery_coverage_or_fail_closed`(缺 driver 则 raise);`DurableRuntimeWriter._signal_recovery_wakeup` 在 reduce_and_persist 尾部仅当提交新 recovery 工作时发 `request_service_wakeup`(闭合 invariant 3 的 5s gap);纯加法、零新 infra;wakeup 写改原子 temp+replace。invariant 1/3 变异证明 load-bearing,oracle 仍 10/10。
    - [x] 5c poll 降为 backstop（2026-06-14，`81afd10`）：shared daemon 默认 poll 5s→30s(在 stale 90s/lease 300s 窗口内安全,空闲控制面扫描负载 ~6x 下降);特征化测试钉死 backstop 默认在窗口内 + override 生效。5a+5b Codex re-review GO。
    - [ ] 5d（可选,**建议推迟到 Track C worker 拆进程时**）：补 `runtime_outbox` consumer 把 wakeup 升级为 durable PG 通道。单机 ~20 用户阶段 wake-file 已足够;拆进程后才真正需要(届时同步评估 PG LISTEN/NOTIFY)。
    - [x] 5e（2026-06-14/15）：请求/读路径 recovery 触发改 signal-only。请求路径 signal-only（`3410293`）。读路径接管经两次 async Codex NO-GO（F1 请求 payload 注入恢复控制 / F2 合并标量 wake 文件 clobber）后,按 owner 审定的 **Option B** 重做（`ebad57d`，设计 `docs/RECOVERY_TAKEOVER_INTENT_DESIGN.md`）：新 durable 表 `workflow_recovery_intents`(job_id PK,latest-wins,单赢 claim) 分离 notification 与 intent——读路径写 server-side literal intent(F1 结构性解决)、新 `workflow_takeover_intent_drain` phase 单赢 claim 做 stale=0 scoped resume、纯 nudge 不带 scope(F2 结构性解决);oracle 仅加性更新且 10/10;单列 PK 免唯一索引登记、过 onconflict 守卫;Track C 拆进程前向兼容。完整 claim→resume→consume/retry→reclaim 生命周期经 5 轮 Codex 评审收口(consume 按 claim 身份匹配防 ABA、takeover_failed 留重试、过期 claimed 行可重 claim)；最终 GO（`9afcb80`）。**Phase 4 全部收官。**
    - **Phase 4 收官**（除下述显式推迟项）：命令层四域 owner + CommandKernel + registry；recovery tick 部分 phase 对象化 + 事件驱动(5a 保证/5b 唤醒/5c backstop/5e signal-only)；cancel/resume 槽位化；mesh 边界冻结。orchestrator ~87k→~74k 行。
    - 之后：Option 3 全事件流 driver = Track D 北极星(`agent_events`/SSE 表尚不存在)。

### Track B — 存储与测试基建（与 A 并行）
- [ ] R-019：operation/workflow command 原子边界仍未闭合。D-3 已修 pool-max=1 advisory-lock 循环等待并把
  direct state-sync caller 棘轮降至 26，但 cancel race 仍可留下已计划 command，transaction try-lock helper 也只有
  capped backoff、没有总 acquisition deadline。下一次触碰 operation retry/dispatch/command completion、
  `workflow_commands` 或新增该 helper caller 时，必须先落 generation/lease fence、统一 UoW 和 typed lock-busy budget。
- [ ] R-022：②.4c activity spine / R-020 fixed-forward 已固定 `af50f45..30a703e` 异步 Codex review；有效 GO 前只冻结本 scope 的 live/W6/manual/里程碑签收，commands 与其他非 live 开发继续。
- [ ] R-023：`runtime_outbox` 在 production claim/consumer、Track C 5d 或 outbox live/W6/manual 签收前补 claim generation/token fence；当前无 production consumer，不阻断非 live 开发。
- [ ] R-025：②.4d `889848e..7048d83` re-review 已重发，但 Codex 0.144 多 thread / non-inline-items transcript 被旧 runner fail-closed 为 `invalid_transport`；提取的 reviewer 内容只作参考，不是 GO/NO-GO。协议适配已由独立 carrier `dc5af51` 修复；operator 顶层 reasoning effort 仍为 `medium`，改为最高支持档后再正式重发。有效 GO 前只冻结本 scope 的 live/W6/manual/里程碑签收。
- [ ] R-026：②.4e recovery intents 同样已产 `invalid_transport`，不是正式裁决。implementation=`f09ffbd`、base=`dbb40f3`，正式 re-review 仍只限原 8 个实现/测试文件；runner carrier=`dc5af51`。待 operator 顶层 effort 从 `medium` 调到最高支持档后重发；不阻断 ②.4f 或其他非 live 开发。
- [ ] R-027：D-3 修订 (a) implementation 已固定为 `82d69a1`；results **314/2/4**（两失败均 clean
  `5f14ed8` 同败）、writer **24+3**、adapter **62+4**、operation **129**、storage **59**、PG projection/CRM
  **58+4**、frontend Python **50** + build、workflow **187/1 baseline-identical +3**、lint **58 files**、mypy
  **81/4**，contract lane **349+2+11+1+2** + `dry_run_ready`。正式 Codex review artifact 仍 pending；有效 GO 前只冻结 D-3 的
  live/W6/manual/product/里程碑签收，不阻断下一批 Track C C1a。
- [ ] R-028：D-3 已把 projection selection 的 CRM record/engagement/event 收进 revision-fenced fixed PG UoW，但 legacy CRM add/update 没有共享 identity-lock UoW，durable cancel/entity-delta/command terminal CAS 也仍在 domain commit 之外。下一次 CRM Repository 或 workflow command-completion 分子批统一这些入口、清理 person 重复后加唯一约束，并删除临时 `ControlPlaneStore.apply_projection_crm_selection` facade；修复前不宣称 global CRM exactly-once，也不做 D-3 CRM mutation live/manual 签收。
- [x] 测试环境契约 v2（2026-06-11）：每 run = (PG schema + runtime dir) 配对 + `.ephemeral-test-env.json` 标记；teardown `DROP SCHEMA CASCADE`（仅删自建 schema，`pre_existing` 守卫）；孤儿 janitor `scripts/prune_test_schemas.py`（先快照后扫描、活跃连接守卫、仅限本地 DSN、dry-run 默认）。
- [x] Mac 本地 PG Docker 方案（2026-06-11）：`local_postgres_docker.py` + `make local-pg-up/down/status`；容器 55432 复用既有 DSN 发现机制零侵入；PG 强制模式下 durable runtime 套件真实执行验证。
- [x] PG 测试 fixture 试点（2026-06-12）：`tests/pg_store_fixture.py`（`PGControlPlaneStoreTestMixin`：per-class schema + `pg_tables` 截断复用）；8 个文件先行迁移；试点即捕获一个生产缺陷（见下条）。
- [x] PG ON CONFLICT 唯一索引缺口类修复（2026-06-12）：SQLite UNIQUE 约束从未镜像进 PG bootstrap，postgres_only 下 `upsert_criteria_pattern` 等触发 InvalidColumnReference；修复 = `_CONTROL_PLANE_UNIQUE_INDEXES` 幂等唯一索引 + 建索引前去重（有时近列保留最新，无时近列 loud failure）+ 复发守卫 `tests/test_pg_onconflict_guard.py`（机械扫描 ON CONFLICT 目标 vs 索引清单）。
- [x] PG fixture 迁移批次 2（2026-06-12）：12 个 store 级文件（asset consolidation/reuse、authoritative serving repair、job result lifecycle、legacy public web retirement、manual review、organization assets、search seed registry、serving projection storage/writer、smoke runtime seed、snapshot materialization backfill）；12/12 零新增失败零跳过，合并验证 52 通过 + 恰好 3 个基线内 pre-existing；本批未再爆出 PG 产品缺陷。
- [x] PG fixture 迁移批次 3（2026-06-12）：10 个中型文件（asset consolidation audit、cloud asset import、company asset completion/supplement、excel intake、organization execution profile、person asset crm projection、runtime rebuild、storage profile registry、target candidate public web）；合并验证 143 通过 + 11 个故意跳过 + 恰好 1 个基线内 pre-existing。**迁移再捕获 2 个真实 PG 产品缺陷并已修**（profile registry lease 缺行哨兵 None vs {}；refill plan PG bulk 路径丢 terminal 行 source_jobs 合并→postgres_only 下 per-job scope summary 少算）——invariant 7 的双路径语义分歧族第 3、4 例。
- [x] PG fixture 迁移批次 4（2026-06-12）：server/daemon 4 文件（`test_cli` 55/55、`test_workflow_explain` 恰预算 1 失败、`test_worker_recovery_daemon` 21/21、`test_crm_public_web_runtime_boundary` 32/32）；合并验证 124 通过 + 23 子测试。两个 PG 模式测试修法值得复用：dead-local-process stub 需同时 patch `control_plane_live_postgres` 模块内绑定；时间回拨必须改 PG 权威行而非 SQLite shadow。
- [x] PG fixture 迁移批次 5（2026-06-12）：三巨头完成——`test_candidate_artifacts` 58/58（顺带捕获并修复 canonical fallback 被 hot-cache 视图压制的产品缺陷,第 5 个迁移捕获缺陷）、`test_enrichment` 38→12、`test_results_api` 36→3（contract-cited 修复:proof seeding、W6 command-owned 断言、410 canonical endpoint;反伪造护栏零触碰）。直接实例化 SQLite store 的迁移**全部完成**。
- [ ] 收尾项：6 个已走 `PGDurableRuntimeTestMixin` 的可选统一；`test_pipeline`（42k 行，永不全量跑）单独设计；PG 适配器自测 3 个豁免（保持）。
- [x] advisory lock key 按 schema 命名空间化（2026-06-12，owner 批准趁 systemd 全量重启部署窗口落地）：7 个锁点统一走 `_advisory_lock_key()`（schema 前缀，空 schema 归一为 `public`）；跨 schema 互不争用 + 同 schema 互斥 + 默认前缀确定性均有实测锁定（`test_control_plane_pool.py`）。**部署约束：锁身份已变，上线必须全停重启，禁止新旧进程共存热部署**（现行 systemd 部署天然满足；Track C 容器化滚动部署前无需再协调）。
- [x] 之后：按表组把双路径方法重写为 PG-pure 并删 mirror/内存 SQLite 影子 + 引入正式 migration 机制——**已由 Track B 全部完成**（RATIFIED 2026-06-16，追踪见 `docs/TRACK_B_PG_PURE_STORE_DESIGN.md`，本条即该 doc 引用的 §50 roadmap）：`storage.py` 现为 PG-pure（零 sqlite3/mirror；B4.3f 内存影子退役，shadow 访问器只剩 inert 标签）；PG schema 唯一来源 = 版本化 migration runner（`src/sourcing_agent/migrations/0001_baseline.sql` + `src/sourcing_agent/migration_runner.py`，`init_schema` 已删）；`SOURCING_PG_ONLY_SQLITE_BACKEND` 已成 inert no-op。下一步（已批）：B4.2 ② Repository 查询方法 + 按域迁移 caller → ③ jsonb/timestamptz（owner-gated，决策卡 = handbook §7 D-1）。
- [x] ② 域退役已完成至 ②.4e（入口文档 `docs/TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md`）：②.0 linkedin_profile_registry（2026-07-02）+ ②.1 criteria/confidence（`d5109f1`）+ ②.2 manual_review（`d0828e7`）+ ②.3a-d serving_projection + ②.4a operation-control（`93d9f9e`）+ ②.4b acquisition-control（`d6e1e2a`）+ ②.4c activity spine（`30a703e`）+ ②.4d read-model trio（`7048d83`）+ ②.4e recovery intents（`f09ffbd`）。`storage.py` 19,187 → **11,829** 行；②.4e A/B、mutation、targeted、PG 和 lane 证据保留在 Track B batch record。Repository 迁移暂停于此，②.4f session/trace、job leases、workers、commands(last) 延后；下一执行批为 **Track C C1a**。R-019/R-023 边界不变。D-1 已选 (a)、D-2 已选 (b)；D-3 implementation=`82d69a1` 已完成，formal review 由 R-027 pending 跟踪。
- [x] provider fail-closed 隔离合入（2026-07-09，`430a369`）：`SOURCING_EXTERNAL_PROVIDER_MODE` 未设/未知一律 `simulate`；非生产环境 live 需双钥确认；detached 子进程注入 access-disabled + 空 token；`tests/conftest.py` 全局隔离 secrets。2026-06-27 计费事故类在主线关闭。落地条件：已验证（58 runtime/model/settings + connector 套件；7 个全套件失败经 worktree 基线证明 pre-existing，台账 R-010）。
- [x] 合同 lane 加固（2026-07-09，`8b555b6`）：onconflict 守卫入两条 lane；REQUIRE flags 全段 skip→fail（变异自证）。

### Track C — Serving Runtime（目标 ~20 并发用户）
- [x] psycopg_pool 连接池（2026-06-11）：per-adapter 懒加载池（`SOURCING_CONTROL_PLANE_PG_POOL_MIN/MAX`，默认 1/8）；25 个调用点事务语义逐一核验不变；实测 200 次顺序操作 1.516s→0.743s、新建连接 200→1；`ControlPlaneStore.close()` 接线。
- [ ] 重活出请求线程：plan compile / `/api/jobs` / 导出统一为 enqueue + 轮询（后续 SSE）。C1 durable plan-task 设计见 `docs/TRACK_C_C1_DURABLE_PLAN_TASK_DESIGN.md`；先落不依赖存储裁决的 C1a fast contract lane，consumer table / 202 cutover / publication UoW / TTL 四项 owner 决策未决前不做 migration。
  - [x] C1a author implementation（2026-07-14）：exact export light lane、owner-supplied `artifact.handle` fail-closed、frontend terminal-total status registry、Plan `pending|queued` bridge；无 schema/provider/model。targeted + frontend build 已绿，scope-matched independent review pending；有效 `GO` 前不做本 scope 的 live/manual/product signoff。
  - [x] C1b author implementation + pinned advisory fixed-forward（2026-07-14）：HTTP 200/pending 与唯一 hydration owner 保持；late same-signature consumer 由同锁 retirement barrier 原子 drain/转 successor，history publication 有 generation fence；AST 覆盖 alias/thread/executor；authenticated unlinked Plan submit/read/list/resubmit 以 server provenance fail-closed；空 artifact handle 拒绝。真实 compiler 竞态仍可留下 orphan review/criteria，明确阻断 durable/live/manual/product 签收，须 D-C1-3/C1d compute/publish split；formal review pending，不越过 D-C1-1..4 做 C1c-e migration。
- [ ] worker 与 API 进程分离（完整 C3b 仍需跨容器唯一 runner + 5d）。
  - [x] C3a author implementation + pinned-review fixed-forward（2026-07-14）：初版 `7a58684` 的 scoped review 为 `NO-GO`，指出 provider/API 请求路径仍可触发 job-scoped/inline recovery、`?sync=1` 未退役、HTTP `run-once` 仍执行 tick，以及 topology tests 仍受 PG mixin/源码字符串断言污染。fixed-forward 后：`serve` 默认 API-only，创建 server 前要求 fresh external `worker-recovery-daemon`；唯一进程内兼容入口为 dev-only `--enable-runtime-watchdog`；provider webhook 先持久化 terminal checkpoint/释放 lease+limiter，再仅发 pure shared signal；`?sync=1` 返回 `410`；`POST /api/workers/daemon/run-once` 忽略 client controls，signal 成功返回 `202`、不可用返回 `503`；smoke/browser caller 发送空 payload，并把 signal evidence 与 daemon status/worker progress execution evidence 分离；topology tests 已移到无 PG plain `TestCase` 并捕获真实 argv。same-host/systemd only，无 outbox/dispatch/schema/provider/live。验证：core 52 tests + 2 subtests、downstream exact 14 tests、pipeline exact 1 test、frontend build 绿；完整 workflow/signoff 234 tests + 3 subtests 通过，2 个失败已在 clean pinned baseline 同节点复现；contract lane `349+2+11+1+2`，mypy=`81 errors / 4 files`（棘轮持平）。fixed-forward `6cda749` 的 pinned advisory re-review=`GO`（P0/P1/P2=`0/0/0`）；formal review 仍 pending，有效 formal `GO` 前不做本 scope live/W6/manual/product/里程碑签收。
  - [ ] C3b：runtime_outbox fenced claim/consumer、跨容器 durable wakeup 与 PG-native single-writer；R-019/R-023 未满足前禁止实施/签收。
- [ ] 最小鉴权 + 用户身份（token；`requester_id/tenant_id` 列已存在但来自未认证 payload）。
- [x] FastAPI + uvicorn 传输层等价重写 api.py（2026-06-12）：同路由/同 payload/同状态码/同 headers；CORS allowlist + localhost 自动放行 + header 回显；Apify webhook token 校验保留；双道信号量改 middleware（HarvestAPI 并发约束保留至 M2 provider 预算落地）；`create_server` 兼容垫片包 uvicorn（serve_forever/shutdown/port-0）；`tests/test_api_transport_parity.py` 传输等价测试。
- [ ] FastAPI 第二步：handler 签名 pydantic 模型化 → OpenAPI 成为前端 contract 生成源；SSE 推送替代 1s/5s 轮询。
- [ ] 多用户 Agent serving 拓扑（2026-06-11 确认，详见 plan doc revision 节）：按角色容器化（api/agent-worker/provider-worker，docker-compose 起步）；`agent_session`/`agent_turn` PG checkpoint + per-session 单写者 lease；`agent_events` SSE tail；per-user 并发限额；凭证只在 provider worker 层。**不做 per-user 常驻容器**；沙箱仅在将来加代码执行/浏览器工具时按工具调用租用。
- [ ] 之后：对象存储读穿（company_assets/media 出本地盘；`object_storage.py` 抽象已存在）。
- 明确不做：Redis、LISTEN/NOTIFY（当前规模不需要）。

### Track D — 强 Agent 化
- **2026-07-13 Track D 设计定稿 v8（owner-accepted exception，可开工）**：入口 =
  `docs/TRACK_D_AGENT_RUNTIME_PLAN.md`（§5a 终局裁决 + §6 义务清单）+ 两份详设（D0/D3 各带
  findings 覆盖映射）+ `docs/TRACK_D_INVARIANT_SWEEP_2026-07-13.md`（200 格矩阵 v3，OB-ID 义务）。
  经八轮 gpt-5.6-sol 对抗评审（曲线 24→16→17→6→5→11→9→7；3 份有效 artifact + 提取件，
  `runtime/reviews/20260713T*`），R8 findings 已转实施批开工义务。TD-1…TD-7 全部裁决（含 TD-4
  初始路由表，rollout=draft）。**实施顺序 D0→D1→D3(poll-mode)→D2(与 C4/C5 合流)**；每批照
  handbook §4 协议 + per-batch review gate；开工前读 `docs/DESIGN_INVARIANT_CHECKLIST.md`（v2
  十类，设计/实施通用自查协议）。评审基础设施：`make independent-review-gate` 已内建专属
  reviewer CODEX_HOME（`configs/reviewer-codex/` + bootstrap，ChatGPT Desktop 切配置免疫），
  超时用 `REVIEW_TIMEOUT_SECONDS=1800`。
- [ ] ModelClient 升级：streaming + tool-calling（现有 14 个单发方法、阻塞 requests、无流式）。
- [x] D0b characterize-first（2026-07-14）：新增语义 AST + synthetic transport 回归，冻结 `ModelClient`
  14+3 签名（含 sync/async + decorator）、自动发现的 5 个 concrete surface/factory return parity、全 17 方法
  runtime spy 下的六方法 scripted-live delegation、25 个消费模块/29 个调用点及 chat / responses / Qwen 三种
  完整现有 wire shape；零产品码、零网络、零 provider/model。首轮 pinned review `NO-GO` 后 fixed-forward 已完成；
  `763e0aa` 的 pinned advisory re-review=`GO`（P0/P1/P2=`0/0/0`），formal review pending。
- [x] D0 usage type 收敛门（2026-07-14）：`model_usage.py::ModelUsage` 现为无 transport/env/settings 依赖的
  单一五字段 immutable value owner，`model_provider.py` 与 `model_tool_runtime.py` 导入同一 class；
  `OpenAIModelUsage` 仅保留 object-identical compatibility alias，临时 runtime 类型及其 export/生产引用均已
  删除。回归直接证明单一 class owner、provider/runtime identity、严格非负 integer/non-bool 校验和依赖中立，
  不再用可反射绕过的 AST escape ratchet 代替架构收敛。此项只关闭首个 transport/product callsite 的前置债务；
  D0a 仍禁止 live/product integration，其他 durable owner/cost/policy/result-slot/review gates 不变。
- [x] D0c invocation evidence contract author implementation（2026-07-14）：单一 immutable
  `ModelInvocationEnvelopeV1` 已落 exact schema/version/serialization/SHA-256，物理 ref 使用 typed absence 且保留
  quarantine/non-authorizable outcome；D0a result mirror 只比对真正共享字段，不产生 durable/effect authority。
  route registry preflight 现强制 exact keysets、`route_id+circuit_key` 分别唯一、全 draft、manifest non-live。
  零 transport/settings/env/storage/migration/provider call，D0a 仍只执行 `simulate|scripted`；durable issuer/
  persistence/result-slot CAS、ExecutionContext、cost ledger 与 R8 OB-2.2/4.3/10.3/10.4 仍 deferred。author evidence
  = `145 passed` + provider/characterization `60 passed + 11 subtests` + Ruff/mypy/diff-check；`bc8d9d8` 的 pinned
  advisory review=`GO`（P0/P1/P2=`0/0/0`），formal review pending。
- [x] D0d canonical tool-session projection author implementation（2026-07-14）：新增抽象
  `ToolCallingSessionBase`，两个公开入口只投影同一 `_parse_tool_turn -> ParsedToolTurn`；scripted replay 删除
  buffered/stream 双份解析路径，route fence 仍先于 caller iterable 消费，terminal event/result coherence 由
  `ParsedToolTurn` fail-closed。零 transport/settings/env/storage/provider/model/effect，request/hash/result/envelope
  schema 与执行权限不变；ExecutionContext、budget/cost/result-slot/live 仍 deferred。初版 `0f8d249` 的 pinned
  review=`NO-GO`；fixed-forward `01220dc` 的 pinned advisory re-review=`GO`（P0/P1/P2=`0/0/0`），formal review
  pending；scoped evidence 见 `TRACK_D_D0D_TOOL_SESSION_BASE_IMPLEMENTATION.md`。
- [x] D0e request→invocation-envelope provenance binding author implementation（2026-07-14）：新增
  `validate_tool_turn_request_envelope_mirror`，逐字段对齐 current request 真正拥有的 route/config/tenant/policy
  provenance，并从一次消费的 request/messages/tools 重算 canonical request digest；明确不比较 response/
  terminal/usage/circuit/evidence/artifact/cost/causality/budget authority。schema/version/execution gate 不变，
  coherent `provider_mode=live` mirror 仍被 D0a 执行门拒绝；durable issuer/ExecutionContext/result-slot/cost/live
  仍 deferred。`8a33eca` 的 pinned advisory review=`GO`（P0/P1/P2=`0/0/0`），formal review pending；scoped
  证据见 `TRACK_D_D0E_REQUEST_ENVELOPE_BINDING_IMPLEMENTATION.md`。
- [x] D1a Action request surface characterize-first（2026-07-14）：零产品码冻结 ActionSpec/Registry record、
  `submit_action` 当前无 request-schema 校验且不 dispatch 的基线、全注册 action 的真实 dispatch 分组、
  `_agent_callable_workflow_command_types_for_action`、`_build_agent_callable_workflow_command_plan` 的
  input→target→registry-default precedence，以及 command owner/Activity/control fail-closed 关系。数量由代码机械发现；
  当前观测为 15 actions、11 个 command-bearing actions、18 refs/17 unique commands、12 个 dispatch 分支命中；
  `external_intake` 已具 `excel.intake.run` metadata/owner/Activity/control，但 action adapter 仍 unsupported，不能把
  command readiness 当 dispatch readiness。初版 `d88161e` 的 pinned review=`NO-GO`；fixed-forward `bbad0fa` 的
  pinned advisory re-review=`GO`（P0/P1/P2=`0/0/0`）。D1 `ActionRequestSpec`/schema pin/显式 adapter/served
  predicate 仍未实施；formal review pending，scoped evidence 见
  `TRACK_D_D1A_ACTION_REQUEST_SURFACE_CHARACTERIZATION.md`。
- [ ] Agent Session 契约：服务端 agentic loop；工具面 = M1 manifest 导出 + 只读上下文工具 + model_native_search/fetch 转正；效果全部走 typed AgentAction（边界已由 `AGENT_OPERATION_CONTRACT.md` 规定）。
- [ ] 第一垂直切片：公司身份自验证 loop（搜索→fetch 验证→歧义才升级人工），替代 PlanCard 手动修正 LinkedIn URL。
- [ ] 之后：plan review 对话化、intent→plan 前门流式化；OpenClaw/Claude 作为可插拔外脑。

### Track E — 治理
- [x] 文档治理（2026-06-11 完成，PR #15）：21 份归档、PROGRESS/NEXT_TODO 轮转、INDEX 分层、治理规则成文、决策记录迁入 PRE_AGENT_CONTRACT_REVIEW。
- [x] `runtime/test_env` TTL 清理（2026-06-11 applied）：648 个目录、回收 130,498,437,051 字节（~121.5 GiB）、0 失败；磁盘可用 68Gi→183Gi；test_env 117G→1.4G。记录：`runtime/asset_governance/ttl_apply_20260611/ttl_local_rebuildable_apply_v2.json`。过程中修复了活动进程检测器的 PID 复用误报（身份比对 + 新鲜度窗口）。
- [ ] 后续例行：`make prune-test-env`（TTL 默认 14 天）目标待加；测试 harness teardown 钩子随 Track B 契约 v2 落地。
- [ ] M1 后：contract 文档 per-command 段落由 CommandSpec registry 生成；守卫测试改对 registry。

### M2 Provider Task Runtime 设计要求（M2 已全部完成 M2.1–M2.6，含 per-provider 并发预算落地；以下为设计约束存档）
- Provider 级并发预算：HarvestAPI profile-fetch 有 ~8 并发 actor 的隐性限制（"too many requests"），旧 8 槽 API 信号量即源于此——保护必须移到 provider 层（per-provider+key 的信号量/令牌桶），HTTP 入口的并发上限才能放开。
- API key 池化扩容；高需求下避免 profile fetch batch 过度碎片化。

## 已知失败预算 → 已升级为残差台账（2026-07-09）

> 唯一权威清单 = **`docs/RESIDUAL_LEDGER.md`**（逐条 id/tripwire/归因证据；批验收 = green-modulo-ledger；
> 新失败先 **git worktree 基线对照**归因再入账，不得静默增长；计数类条目只降不增棘轮）。
> 本节历史条目已全部迁入台账（R-001…R-012，含 closed 行审计痕迹），此处不再维护副本；
> 历史归因叙事（2026-06-12 的 8 失败清账等）见 git 历史与 `docs/archive/`。

## Decisions Log (2026-06-11)

- M1 重定义为 CommandSpec registry + manifest 导出物；M0.9 独立审批流程取消（10 个冷备已 sha256 验证，源目录随 TTL 清理）；"先删 SQLite fallback"被否（51 测试文件 + 内存影子依赖）；Redis 不引入；FastAPI 重写批准；Mac PG 用 Docker；Independent Review Gate 范围收窄到不可重建资产与 contract-heavy 变更。

## Resume Checklist

1. `git status --short` 确认 worktree 状态后再动文件；只按显式路径 stage。
2. 读 `docs/INDEX.md` 的 Current Stage Checkpoint 与本文件 Track Structure。
3. 跑 `bash ./scripts/dev_status.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173` 了解本地服务状态。
4. 改 contract 相关代码前先全库搜符号引用。

## Useful Commands

```sh
# 状态
bash ./scripts/dev_status.sh --runtime-dir runtime --api-port 8765 --frontend-port 4173
df -h . && du -sh runtime runtime/test_env output 2>/dev/null
# 测试（targeted）
PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/<file>.py -q
make ci-pre-agent-contract
# 网络只读预检
make agent-network-preflight
# 文档 banner 巡检
python3 - <<'PY'
from pathlib import Path
paths = [Path("README.md"), Path("PROGRESS.md"), *sorted(Path("docs").glob("*.md"))]
missing = [str(p) for p in paths if not any(l.startswith("> Status:") for l in p.read_text(errors="replace").splitlines()[:8])]
print("\n".join(missing) or "all banners present", f"\nchecked={len(paths)} missing={len(missing)}")
PY
```
