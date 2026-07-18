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
  D1l 仅登记一个 commandless bounded specialization：projection-read terminal action+Operation+event 在一个 PG UoW
  提交，session Operation-dispatch/publication locks 共用单一 monotonic 5s total acquisition deadline（不是 each
  5s），past deadline 在 connect 前 fail，耗尽时分别返回
  `operation_dispatch_lock_busy` / `projection_publication_lock_busy` 并映射 HTTP 409。该路径不创建 workflow command，
  因而 command generation/lease fence 不适用；不新增 direct state-sync caller，26 棘轮不升。R-019 对所有其它
  retry/dispatch/command-producing/completion 路径继续 open。
  D1m candidate 仅在既有 Company Public Web command-producing topology 上增加 persisted/dispatch/root/source/
  materialize exact request-target revalidation；不把 submit/plan/effect/terminal/Operation sync 收入一个 global
  UoW，也不提供全局 generation/lease fence，因此 R-019 继续 open。
- [ ] R-022：②.4c activity spine / R-020 fixed-forward 已固定 `af50f45..30a703e` 异步 Codex review；有效 GO 前只冻结本 scope 的 live/W6/manual/里程碑签收，commands 与其他非 live 开发继续。
- [ ] R-023：`runtime_outbox` 在 production claim/consumer、Track C 5d 或 outbox live/W6/manual 签收前补 claim generation/token fence；当前无 production consumer，不阻断非 live 开发。
- [ ] R-025：②.4d `889848e..7048d83` re-review 已重发，但 Codex 0.144 多 thread / non-inline-items transcript 被旧 runner fail-closed 为 `invalid_transport`；提取的 reviewer 内容只作参考，不是 GO/NO-GO。协议适配已由独立 carrier `dc5af51` 修复；operator 顶层 reasoning effort 仍为 `medium`，改为最高支持档后再正式重发。有效 GO 前只冻结本 scope 的 live/W6/manual/里程碑签收。
- [ ] R-026：②.4e recovery intents 同样已产 `invalid_transport`，不是正式裁决。implementation=`f09ffbd`、base=`dbb40f3`，正式 re-review 仍只限原 8 个实现/测试文件；runner carrier=`dc5af51`。待 operator 顶层 effort 从 `medium` 调到最高支持档后重发；不阻断 ②.4f 或其他非 live 开发。
- [ ] R-027：D-3 修订 (a) implementation 已固定为 `82d69a1`；results **314/2/4**（两失败均 clean
  `5f14ed8` 同败）、writer **24+3**、adapter **62+4**、operation **129**、storage **59**、PG projection/CRM
  **58+4**、frontend Python **50** + build、workflow **187/1 baseline-identical +3**、lint **58 files**、mypy
  **81/4**，contract lane **349+2+11+1+2** + `dry_run_ready`。正式 Codex review artifact 仍 pending；有效 GO 前只冻结 D-3 的
  live/W6/manual/product/里程碑签收，不阻断下一批 Track C C1a。
- [ ] R-028：D-3 已把 projection selection 的 CRM record/engagement/event 收进 revision-fenced fixed PG UoW；D1j
  又把 Operation `add_to_crm` 迁为 schema-defined owner-bound projection selection，并在 submit/dispatch/command owner
  复验 exact snapshot。legacy CRM add/update 仍没有共享 identity-lock UoW，临时
  `ControlPlaneStore.apply_projection_crm_selection` facade 仍未删除，durable cancel/entity-delta/command terminal CAS
  也仍在 domain commit 之外。下一次 CRM Repository 或 workflow command-completion 分子批统一这些入口、清理 person
  重复后加唯一约束，并删除临时 facade；修复前不宣称 global CRM exactly-once，也不做 CRM mutation live/manual 签收。
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
  - [x] C2.7 author fixed-forward（2026-07-14）：Stage2/cancel PG typed owner/state CAS、CRM PATCH/version 与 immutable promotion id、authenticated daemon allowlist、route registry 等边界已落；candidate=`4b2370a`。Pinned review=`NO-GO`（P0/P1/P2/P3=`0/1/0/0`）：criteria shared preflight 被 `rerun_retrieval` 错误控制，flag missing/false 时 foreign/missing explicit/source job 可先写主域。其唯一 finding 由 C2.8 fixed-forward；C2.7 author evidence 不变且不构成 formal `GO`。
  - [x] C2.8 author fixed-forward（2026-07-14）：authorization 已从 rerun policy 拆开；feedback、explicit recompile 两种 id spelling、suggestion source job 在任意主域写前无条件 exact-owner preflight。首个 candidate=`254b7c9`；fresh review 进程虽为 0，但 artifact causal binding 无效，故不构成 formal evidence；其 P1 source-swap/dual-source 与 P2 whitespace-alias 实质 finding 已继续 fixed-forward。suggestion+feedback+全部 distinct job refs 现与 pattern/review 写共享 locked PG UoW，compiler 只消费 authorized frozen snapshot；两种 explicit alias 独立 normalize/check。missing/foreign 同一 `job_not_found`；automatic baseline requester+tenant scoped、no-ref 与 open-mode 正向不变。follow-up stable-head 验证：fast/transport/native adjacency `78+61 subtests`、PG adjacency `12`、lint `58 files` green、mypy 棘轮恰 `81/4`、py_compile/diff-check clean。最新 formal 状态仍为 `invalid_transport`/pending；无 pipeline full/provider/model/live；有效 `GO` 前只冻结本 scope live/manual/product/里程碑签收。
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
  pinned advisory re-review=`GO`（P0/P1/P2=`0/0/0`）。显式 adapter 与 request-schema pin foundation 已分别
  由 D1b/D1c 接续；formal review pending，scoped evidence 见
  `TRACK_D_D1A_ACTION_REQUEST_SURFACE_CHARACTERIZATION.md`。
- [x] D1b explicit dispatch-adapter registry（2026-07-14）：`ActionRegistry` 声明 closed-set adapter，
  orchestrator 只持五项显式 bound-method map；12 supported/3 unsupported 行为不变，无 dynamic lookup。
  current candidate=`97a81d0`；scope-local advisory `GO`（P0/P1/P2/P3=`0/0/0/0`），formal review pending；
  advisory 不得写成 formal `GO`。served Agent tool population 仍为零。
- [x] D1c request-schema pin foundation（2026-07-14；foundation=`9a0e051`，invalid-review fixed-forward=本条所在
  enclosing commit，exact hash 由 fresh pinned review request 绑定）：共享 D0 `ToolSpec`
  validator/digest 的 `ActionRequestSpec`、owner-bound target、`agent_actions`/`operation_runs` 物理 version+digest、
  submit/approve/retry/dispatch copy+verify 已落；全部 15 个 production actions 仍为 schema-less，物理空/空 pin
  + release-epoch-scoped replay/approve/retry/dispatch compatibility observation 是 R-029 迁移证据，served
  population=0。`0002` 以 5s lock budget + `NOT VALID` bounded install；既有行 constraint validation 必须是后续
  独立 deployment/transaction，不能与 install 同一 pending runner transaction。D1 OB-ID set=`∅`；本批唯一编号
  义务是 Plan §6#3，已由 R-029 + 本条登记。owner 允许按既定 Track D plan 继续，因此 R-019 仅记录 D1c
  pre-write validation/copy/verify 与一条幂等 append-only compatibility evidence event 的有界例外；26-call ratchet
  不增、无 action/run/command state mutation 或新 lock caller，UoW/lease/lock-budget 边界仍未闭合。fixed-forward
  evidence=`78+48`、migration `7`、operation `129`、model/tool `109`、command `16`、ratchet `2`、lint `58`、
  focused mypy `0/3`、global mypy `81/4`。首个 pinned review artifact
  `20260714T075911Z_Track_D_D1c_request_schema_pin_foundation` 因
  `causal_binding.final_response_item_exact=false` 为 invalid；其中 substantive text 只作 fixed-forward 输入，不是
  formal `NO-GO`。fresh pinned formal review 必须绑定 enclosing commit，hash-bound valid artifact 存在前 formal
  status=pending，且不阻断无关 non-live Track D 开发。
- [x] D1d projection action binder decision oracle（2026-07-15 checkpoint）：当时
  `search_projection` / `filter_projection` aggregate 无物理 workspace owner，故 fail-closed 留在 R-029 bridge。
  D1l 后续选择其 option 2：由 public `ServingProjectionReader` 的 closed production-type classification 显式拥有
  `shared_canonical_read`，`projection_search_service` mint projection+revision target；Operation workspace 仅隔离 CRM
  overlay，不伪装 projection tenant owner。served=0，D1l review 仍独立 pending。
- [x] D1e CRM existing-record schema/binder foundation（2026-07-15）：声明 shared closed schema builder、typed
  bind context、binder registry，以及 `set_crm_stage|add_crm_note|create_crm_task` exact 三项 contracts；初始
  checkpoint=`declared_not_activated`，直到 HTTP/orchestrator + execution revalidation 可同批激活。
- [x] D1f CRM existing-record action activation author candidate（2026-07-15）：exact 三项 schema 已复制到
  production registry，D1f checkpoint 为 **3 schema-defined / 12 schema-less / served=0**。authenticated submit 由 request state
  exact 绑定 workspace/user；missing/foreign 同一 404 且 pre-submit 全域零写；open-mode exact workspace 正向保留。
  owner snapshot 持久化 `crm_record_id/workspace_id/owner_user_id/crm_version`，stable request identity 仅
  `crm_record_id+workspace_id`；dispatch 在新 plan 写前、CRM command owner 在首个 domain effect 前分别 revalidate，
  canonical linked AgentAction 是 action discriminator，brownfield blank pins 对三项 fail closed。Pinned
  `22055aa` advisory=`NO-GO 0/1/2/1`；current fixed-forward 对 exact 三项采用单 input envelope presence rule，
  physical/payload operation carrier 任一 dangling 均 fail closed，binder factory 直接消费 canonical tuple，
  replay 返回 closed current lifecycle + HTTP 200，unknown persisted status 与 stable non-fresh action/run
  incoherence 均 pre-write conflict；approved replay 读取既有 run，action-only rejection 不会创建 run，queued
  partial 只修复 deterministic run；所有 submit 均在写前读取该 run，orphan run 与非法 static-required approval
  组合 fail closed，conditional non-required approval/cancel/retry 仍由既有 owner + R-019 约束。submit frontend contract 以 required literal true/false 区分 replay/fresh；
  R-029 observation epoch 已 bump 为 `d1f_r029_20260715_v2`，submit-replay evidence 固定为 action-scoped，避免在
  已落盘 v1 key 下更换 carrier。此前及本轮 local
  advisory (`0/3/2/0`、`0/2/1/0`、`0/1/2/1`) 均仅作 fixed-forward 输入，不是 formal verdict；当前证据=
  D1f `15+80`、D1 adjacency `119+189`、operation `136+503`、frontend contract `3` + build `84 modules`、lint
  `58 files`、mypy `81/4`。Pinned `1cb829f` 后续 advisory `NO-GO 0/0/1/1` 的 fresh-202 discriminator 与 nested
  submit-response schema 缺口已在 `d5b0a31` fixed-forward；missing/null/string replay marker 均稳定 400，两分支
  均组合完整 detail contract。Pinned `5677a59` advisory=`NO-GO 0/0/1/0` 进一步发现 list/object raw status 的
  unhashable HTTP 500；当前 fixed-forward 在 replay/fresh enum membership 前强制 raw string，两类 composite
  negatives 均稳定 400 且 body 不变，latest exact evidence=`25+72`。Commit `0740a36` fresh pinned non-author
  advisory=`GO 0/0/0/0`，不是 formal GO。R-028 仍 open，不宣称 command/effect/terminal/EntityDelta 同 UoW、完整 TOCTOU 或
  exactly-once；无 provider/model/live。
- [x] D1g Operation API exact-owner closure current author candidate（2026-07-15）：canonical authorization owner=
  `agent_actions/operation_runs.workspace_id`，run 额外要求 linked action 存在且 exact same workspace；actor 仅为
  provenance。authenticated action/run list 使用 server workspace，detail/provenance 与
  approve/reject/cancel/retry/resume/dispatch 均做 keyword-only expected-workspace preflight；dispatch 的既有锁分支
  在锁内、R-029 compatibility event 前再验。foreign/missing 使用 action/run 各自 byte-identical generic 404 并
  全域零写，open-mode operator compatibility 保留。无有效 verdict 的后续 review attempt 提供三项可复现新证据；
  fixed-forward 已把 shared-workflow command 的 linked run+action owner `EXISTS` 与 physical event workspace
  predicate 下推到 SQL `LIMIT` 前，并在任何 compatibility observation/write 前 exact-bind planned command 到当前
  run；foreign/missing/blank/same-workspace-other reference 同一 not-found/零写，open-mode 保留。当前证据=
  `28+97`（D1g alone `6+29`）、adjacent exact `4`、full operation
  `136+503`、lint `58 files`、global mypy `81/4`。Pinned `646e596` advisory=`NO-GO 0/0/1/1` 的 fixed-forward
  已把 invalid-ref fail-closed 与 valid replay 分为两阶段：valid ref 只读取一次并继续通过 persisted request、
  target 与 approval guards，且不写 compatibility observation；auth/open approval + schema-invalid/pin-drift
  positives 与三个 stale characterization probes 已补，current exact=`7+35 subtests`、request+D1g=`29+107`、
  registry/probe adjacency=`25`。Pinned `c7d2e24` advisory=`NO-GO 0/0/1/0` 进一步发现 planned sensitive-CRM
  approval 分支在返回 captured plan 前写 action/run/event，触发 R-019 next-mutation tripwire；current fixed-forward
  对 existing-plan approval requirement 只读返回，并把回归扩为全 D1g 表零写。Commit `ebe7ed0` fresh pinned
  non-author scope-local advisory=`GO 0/0/0/0`，但不是 formal GO；R-031 仍 formal-review pending。D1g checkpoint 不迁移当时其余
  12 个 schema-less action；D1h checkpoint 将其降至 11，当前 D1m candidate partition 见后续 D1m 项。不改 served=0、
  不授权 live/provider。
- [x] D1h CRM Public Web action activation current author candidate（2026-07-16）：将
  `enrich_person_public_web` 作为第 4 个 schema-defined action 激活，D1h checkpoint 为 **4 schema-defined / 11 schema-less /
  served=0**。exact one batch/single/person-identity selector 绑定 authenticated server workspace+user，canonical
  ids 去重排序且上限 1000；任一 missing/foreign member 返回同一 `crm_record_not_found`，submit 前全域零写，
  same-owner/replay/open-mode 保留。owner-minted target 持久化每项 workspace/owner/version snapshot，dispatch
  写新 plan 前与 queue-command owner 建 batch/run 前分别 revalidate；后者发生在既有 claim/running 后，失败会
  terminalize command，故只主张 batch/run/EntityDelta 零写，不宣称 command/Operation 全域零写。`force_refresh=true`
  且 caller 未给 nonce 时，planning 从 operation/action identity 铸稳定 `operation-...` nonce 并持久化，重试不随机
  重建批。
  Pinned `a36333b` advisory=`NO-GO 0/1/2/1`；fixed-forward 现令 selector-only input 正确保留空 input、input
  selector 与 target 严格互斥、`requested_by` 由 queue service principal 固定拥有，并把显式 continuation
  exact-bind 到 canonical request 派生 batch + persisted options/nonce/runs/job，拒绝 partial/foreign owner fields
  且 action path 不回退 mutable payload runs/job。Pinned `6742130` advisory=`NO-GO 0/3/1/0`；current fixed-forward
  增加 action-only generic batch/run collision preflight + post-start PG reread、跨 workspace attached-run 与
  no-batch deterministic-job 预检、exact expected+1 surplus sentinel（1000→1001）、persisted job ensure+reread，
  以及 running-command exact lease checkpoint CAS。Recovery 对 frozen job status 与 current immutable linkage
  分别校验，合法 batch status 推进后仍可恢复；native PG error 不再伪装 CAS conflict。Top-level action options
  不再被 runtime defaults 覆盖。Current evidence=action/boundary/checkpoint `20+7 subtests`、exact
  Operation/transport/retry `5+11 subtests`、CRM Public Web boundary `34`、combined D1
  `121+202 subtests`、final stable-tree Operation `136+503 subtests`、lint `58 files`、global mypy `81/4`、
  compile/diff clean；fresh pinned review pending。
  R-019/R-028/R-029/R-031 均不因本批关闭；无 provider/model/live。
- [x] D1i acquisition root action activation fixed-forward candidate（2026-07-16）：将
  `start_acquisition_run` 作为第 5 个 schema-defined action 激活，D1i checkpoint 为 **5 schema-defined / 10
  schema-less / served=0**。closed input 仅接受 nonblank `target_company+query`，`raw_user_request` 仅作 exclusive query alias；
  caller command/workflow/job/review/retry/identity aliases 与非空 raw target 均 pre-write reject。authenticated
  transport mint server workspace/actor + exact owner scope；open-mode explicit workspace 保持。approve/retry/resume/
  dispatch/root owner 重验 persisted workspace target；root command 还需 exact OperationRun→AgentAction、canonical
  payload/envelope/source event/causality、approved nonterminal action、nonterminal operation 与 exact current
  claim owner/attempt/unexpired lease；lease 由 PG repo clock 判定并按 UTC 解释 naive timestamp。persisted JSON
  container/type、schema 与 deterministic identity 全部 strict fail-closed。positive 在一个 PG transaction 内锁 root
  + stream、exact-reuse/append `CommandPlanRequested`、exact-reuse/create 一个 deterministic
  `acquisition.intent.resolve` child、写 physical downstream edge 并 terminalize root；fault/mismatch 全回滚。
  Migration `0008` 以 actual-root-scoped trigger、parent identity advisory lock 与 non-unique support index 阻止
  unknown producer 给 `acquisition.run.create` root 写第二 child 或 wrong-type child；non-root fan-out 保持合法，
  conflict 后 locked reread 只接受 exact canonical winner。succeeded replay exact-check root result/event/child/order/full envelope，同时不冻结
  scheduler-owned `not_before_at`；successful-COMMIT driver exception 只在 fresh authoritative root=`succeeded` 后走
  exact replay，pre-commit exception 保持 rollback/re-raise。原 pinned advisory=`NO-GO 0/2/1/0`；`dce094e` fresh pinned
  advisory=`NO-GO 0/2/2/0`，四项 finding 为 commit-ack、parent uniqueness、physical causality、mutable retry state。
  当前 fixed-forward 已本地闭合四项，但新提交的 fresh pinned non-author review 仍 pending，不能写成 `GO`。
  Prior stable `dce094e` author evidence=D1i `20+54 subtests`、combined D1 `160+289 subtests`、command/control
  `175+503 subtests`、durable+CRM batch adjacency `61+12 subtests`、storage guardrails `60`、R-019 ratchet `3`、
  lint `58 files`、mypy `81/4`、compile/diff green。
  R-019 仍保留 Operation/action preflight→root-UoW race、current-state/recovery/Operation post-commit sync 与 failure-CAS
  acknowledgement ambiguity；typed intent uniqueness 不冒充其它 command family 的 global fence。R-029 降至 10/15、epoch 仍为
  `d1f_r029_20260715_v2`；无 served/provider/model/live。
- [x] D1j add_to_crm projection selection action activation candidate（2026-07-16）：将 `add_to_crm` 作为第 6 个
  schema-defined action 激活，D1j checkpoint 为 **6 schema-defined / 9 schema-less / served=0**。Submit 只接受
  `projection_id + membership_revision alias + candidate_identity_keys` selector，并由 canonical serving projection
  reader mint workspace/projection/revision/source-count/selected-candidates owner-bound target；input 仅保留 CRM
  destination fields。Dispatch 与 CRM writer command owner 均重验 persisted action/run request 和 current projection
  snapshot；forged command target、stale revision、missing/foreign projection member 在 CRM write/Activity/EntityDelta
  前失败。Author evidence=full Operation runtime `137 passed`、targeted D1j `3 passed`、D1 action request surface
  `6 passed`；combined D1j/D1k pinned `354e979` runner-backed advisory=`NO-GO 0/9/7/1`，需 fixed-forward +
  re-review，不是 formal `GO`。R-028 不关闭：temporary Store facade、legacy CRM writers、
  command terminal/effect/linked Operation sync 仍未统一为 global exactly-once UoW；无 served/provider/model/live。
- [x] D1k export_candidates projection membership action activation candidate（2026-07-16）：将
  `export_candidates` 作为第 7 个 schema-defined action 激活，D1k checkpoint 为 **7 schema-defined / 8 schema-less /
  served=0**。Submit 只接受 projection selector/revision alias/optional selected candidate keys，由 export owner
  mint canonical projection/revision/source-count/selected-candidates target；input 仅保留 export options。
  Dispatch 只从 persisted owner-bound target 规划 `export.projection.generate`，stale membership 仍在 command planning
  前 reselection fail-closed。Whole-projection export 通过空 selected-candidate list 保持兼容。Author evidence=
  targeted D1j/D1k + D1 action surface `10 passed`、writer-control regression `6 passed`、full Operation runtime
  `137 passed`、lint green、typecheck `81/4`；combined D1j/D1k pinned `354e979` runner-backed advisory=
  `NO-GO 0/9/7/1`，需 fixed-forward + re-review，不是 formal `GO`。R-028 不变；
  无 served/provider/model/live。
- [x] D1l projection-read action schema activation candidate（2026-07-16）：将 `search_projection` 与
  `filter_projection` 作为第 8/9 个 schema-defined action 激活，D1l checkpoint 为 **9 schema-defined / 6 schema-less /
  served=0**。Submit 通过 canonical serving projection reader mint `projection_search_service` owner-bound
  `projection_id + membership_revision` target；search/filter aliases exact-one 后规范为 canonical input，multi-select
  filter 严格校验、去重、排序，unknown/lossy intent fail closed。Operation dispatch 使用 persisted exact workspace
  隔离 CRM overlay；authenticated direct projection 与 job dashboard/candidate 默认使用 server-derived workspace，
  但显式 `default` 保留为 pre-auth legacy selector，open mode 保留 explicit workspace；direct candidates/search
  已覆盖三种模式。Dispatch 以同一个 monotonic 5s total deadline 获取 Operation dispatch + projection publication
  locks（不是 each 5s；past deadline connect 前 fail）并持有至 result persistence；
  `operation_dispatch_lock_busy` / `projection_publication_lock_busy` 均 fail closed 为 HTTP 409。它执行 pre-read +
  reader-pinned post-read revision fence；generic changed-during-read/page-read/search-read 统一 stale/reselection，
  replay 保留 `reselection_required=true` 且不重复 event；persisted input/target strict JSON 拒绝 tuple 等 Python-only
  container；terminal action+Operation+event 在一个 commandless PG UoW 提交；missing/non-shared/unprovable
  projection submit-time 零 action/operation + HTTP 404，dispatch readiness exact reason + HTTP 409，stale 在成功
  read-result/domain write 前 reselection，completed HTTP 200，仍不创建 workflow command。Initial
  final stable author evidence=adversarial `5+9 subtests`、API+writer `50+87 subtests`、D1 contract
  `133+176 subtests`、full Operation runtime `139`、lint `58 files`、mypy `81/4`、compile/diff clean。fresh dirty-tree
  non-author read-only re-audit=`GO 0/0/0/0`，但未 pin、不是 formal `GO`。后续 hash-bound independent artifact
  `runtime/reviews/20260716T144526Z_Track_D_D1l_projection_read_action_schema_activation.md` 对 35-path scope 返回
  **NO-GO 0/4/7/0**（另保留 R-019/R-028/R-029）；artifact 证明 scope 自 intended commit `fc5d603` 至 runner
  resolved head `8744285` 未变化。四个 fixed-forward batch 依次为 workspace/fallback/filter-carrier、
  replay/failure/retry/lock-order、index-owner/ready-empty、bounded SQL/lock/bytes；fresh re-review required。
  D1l 不新增 direct state-sync caller，R-019 的 26 棘轮不升；该路径无 workflow command，故 command
  generation/lease fence 不适用。此 bounded specialization 不关闭 global R-019；R-028 不变；无
  served/provider/model/live。
  四个 production generic projection field patch caller（board-visible extension、Operation native admission、facet
  layering、collection layering backfill）均迁至 `patch_publication_fields_under_lock`；production raw projection
  `upsert` 由静态 guard 拒绝。Helper 持 publication session key，再由 native `SELECT ... FOR UPDATE` merge 当前
  counts/readiness/metadata，并拒绝三个 search-index binding keys：
  `projection_person_search_index_build_generation`、`projection_person_search_index_build_input_revision`、
  `projection_person_search_index_input_revision`。因此 raw public counts/readiness patch 不再绕过 D1l
  read/result exclusion。
- [x] D1m company Public Web action schema activation author candidate（2026-07-16）：仅将
  `refresh_company_public_web_assets` 作为第 10 个 schema-defined action 激活，candidate partition=
  **10 schema-defined / 5 schema-less / served=0**。Closed input required=`target_company + source_families +
  seed_urls`，canonical defaults=`max_assets=50 / force_refresh=false / collection_mode=seed_url_only`，
  `refresh_nonce` iff force；families/normalized HTTP(S) seed URLs 均 nonempty + dedupe + sort。拒绝 provider-search、
  collector-bundle、legacy nested command/job/options 与 owner aliases。`CompanyPublicWebTargetBinder` 通过 canonical
  alias resolver mint exact `workspace_id + company_key`；authenticated transport 只用 server workspace/actor，open
  mode 保留 explicit operator workspace。Persisted action/run、dispatch、`company.public_web.refresh` root、
  `company.public_web.source.collect` 与 `company.public_web.assets.materialize` owner 在各自新 effect 前 reload 并
  exact-compare canonical request/target、schema pin、OperationRun/AgentAction，并从 persisted parent 重算 expected
  idempotency/command/event causality；materialize 前另验 source-run identity。Migration `0009` 对 protocol-ASCII
  trim 后 nonblank idempotency key 加 partial UNIQUE；PG owner 对 run/key 排序加锁并拒绝 split identity，source-run
  metadata 绑定 physical command id/attempt/lease。Same-command higher current attempt 可 reclaim `running|failed`；
  stale/lower/wrong-lease attempt 以 `owner_lost` 零 source-row 写，completed/failed terminalization 再做 current-claim
  CAS。Materialize 只消费 full snapshot v3，绑定 assets/summary/artifact paths+publication digest/source
  revision+completion time/run timestamps，不从 later-run 可变 source rows 或 later clock 重建历史；positive logical revision 统一拥有
  PG/memory latest ordering，brownfield fallback 显式；asset/evidence 以 exact claim 在单一 PG transaction 写入，
  `updated_at` monotonic；typed completed repair 从 authenticated source-owner request 强制 deferred。Source snapshot
  freeze 后，exact plan event、deterministic materialize child、one source-run EntityDelta、physical downstream edge 与
  source terminal CAS 在一个 PG UoW 提交；takeover、final-CAS failure、event identity collision 均 whole-bundle
  rollback，ack-loss 仅在 exact replay validation 后接受成功。Operation workspace
  只做 control-plane authorization，canonical `CompanyAsset` / `CompanyEvidence` 保持 shared-canonical
  `workspace_id=default`。Source effect commit 后若 pure materialize planning 失败，artifact refs 保留用于 retry，
  但 plan event/child/source-run EntityDelta/source terminal CAS 全部零写；成功 retry 由其 current attempt 提交唯一
  bundle 并收敛为 `succeeded`；未 reclaim 的 joined nonterminal 不提升 completed。Source artifact effect 仍先于
  completion bundle，Activity/Operation sync 仍在其后；此 fixed-forward 不宣称 global submit/effect/Operation-sync
  UoW 或 exactly-once。Root/source/materialize drains 只为 D1m opt into expired-`claimed` recovery；shared default
  不变。Guarded start 以 PG clock + deterministic ActivityRun/截至 current attempt 的全部 deterministic
  ActivityAttempt identities 锁定并 full-validate primary ids/keys、workspace、command/activity/workflow/operation/
  type/owner/provider、attempt/lease 与 owner metadata；split identity、alternate nonterminal、current/future terminal
  execution Activity/Attempt rows 与 future/malformed resume evidence 均 pre-write fail closed，fully exact prior terminal 可保留。
  Succeeded owner-specific resume Attempt 仅在 deterministic resume id/key、generation `<=` current、完整
  workspace/activity/workflow/command/provider/request-ref/lease + target/company/boolean-force/nonblank-output-reason
  语义成立时共存。Successful takeover 先把 exact superseded prior running execution attempts 收敛为 `owner_lost`，
  returned Activity/current Attempt 只有形成 exact `running` spine 才被接受。Exhausted final source closure 以 DB
  clock 为唯一 lease-expiry authority，并在一个 transaction 中 fail exact Command/Activity/Attempts；valid resume
  control Attempt 保留，exact current failed owner-loss partial 仅在 error/metadata/output 为同一 nonblank reason、
  `output.status=skipped`、`error.owner_lost=true`、`error.deterministic_terminal_failure=false` 时收敛；active lease、
  future/malformed/identity/semantic conflict 全部 zero-write。当前 R-019 direct state-sync ratchet=**24**；历史 26-call checkpoint
  保留。Latest author evidence=D1m **68 passed + 15 subtests**、Operation Company Public Web selection
  **52 + 61 subtests**、migration runner **24 + 65 subtests**、
  lint/compile/diff green、mypy **81/4**；earlier combined D1 **219 + 209 subtests** 仅为 predecessor evidence。
  首轮 broad combined-D1 的 6 个 D1g stale helper failure 在 clean `abe7725` 完全复现；test-only commit
  `702de97` 已统一经 production binder 生成 target，exact **7 passed + 35 subtests**，不把该 fixture 漂移记作
  D1m regression。Enclosing D1m commit 与 fresh pinned non-author review 仍 pending；R-019/R-029 open，无
  provider/model/live/served authorization。
- [ ] D1n local Agent / 15-action completion（2026-07-17 active）：top-down plan、完整 Action digest、F3
  registry 与 isolated four-tool local-canary population 已落；production/default registry 与 global `served` 仍为
  **0**。S1a 已新增 `0011` occurrence/attempt/journal storage，F4a Action/Operation exact-copy
  `plan_acquisition` tool pins，并以同一 PG transaction 完成 accepted attempt + pending→accepted slot CAS + journal；
  exact preview/action/run/event reload 后从 owner state 重跑 serializer。lost ACK、precommit fault、stale generation、
  late attempt、foreign/missing target、serializer drift 与 concurrent single-winner 已覆盖。S1b 已将 acceptance
  状态机抽成共享内核，并补 `inspect_operation` exact Action/Operation/event-sequence/workflow-ref-command owner
  adapter；command 必须与 ActionRegistry、durable owner registry、`OperationRun.workflow_ref` 和匹配的
  `OperationCommandPlanned` event 同时一致，同 `operation_id` 的非引用 row 不进入 owner。prepare 后 event revision
  漂移保持 slot pending 且 attempt/journal 零写，commandless + command-backed terminal success 均有 PG 证据，prepare
  不触发 write-schema bootstrap。`inspect_operation` 保留 exact v1/v2 并以 v3 作为 explicit current；readiness 由一个 owner matrix 贯穿
  snapshot/execution/serializer/PG，completed 且无 durable result ref 只能 `pending/fail_closed`。S1b
  reference-review response 当前已 fixed-forward findings 2/3/4/7：完整 event stream 在 result slot 前锁定并
  检查 exact workspace/action/run/family/schema 与连续 1..N sequence；workflow ref 仅接受 `{}` 或 exact typed
  four-field envelope；同 command plan proof 必须唯一，command causal identity、完整 event identity 与 plan payload
  digest 进入 non-model fingerprint。missing/foreign 统一生成 slot-generation-anchored masked error owner，并可在
  同一 UoW 原子 accepted/replay byte-identical `operation_not_found`；owner reappearance、foreign stream、duplicate/
  conflicting plan、coordinated lineage/causal drift、malformed durable identity 与 fault rollback 均有零写 PG 回归。
  Reference findings 5/6/8 已在 versioned fixed-forward author response 关闭：persisted occurrence 以 exact
  historical identity preflight，v3 model result 省略 operator reason、non-model fingerprint 绑定 raw progress，
  serializer 复用 canonical Operation control owner 并检查 readiness/policy/provenance；cancel/retry/resume progress
  使用 machine code，operator 原文只保留在 append-only event。Finding 9 canonical owner matrix/preflight 由 S1d
  response 关闭。Versioned response author evidence=`780 passed + 111 subtests`（含 945 control-owner 组合）、
  inspect PG=`28 + 31 subtests`、control adjacency=`4 passed`、scoped mypy=`0/7`、lint/compile/diff green、global
  mypy=`81/4`。Fresh `4dddd0e..28c2b2e` pinned Ultra review 已 exact-bound 并返回有效
  `NO-GO 0/6/1/0`，另保留 R-019 residual。当前 author-side response 已显式 pin V3 tool/result physical contract，
  unversioned symbol 仅作 current facade；control policy 从 durable owner registry 完整重建，V3 强制 complete event
  stream、command cardinality `0|1` 与唯一 exact plan proof，并将 event actor/source 纳入独立 audit digest；request
  在 dependency/schema/connection/owner/result effect 前一次 bind 并在锁内复用；`progress.reason` owner matrix 保持
  machine code，operator event text 在任何 runtime-writer repository read/write 前 trim 且限 500 字符；冻结 V1/V2 historical Git-object
  JSON/SHA，V2 accepted terminal 通过 current public acceptance replay 且不调用 head prepare。当前证据=
  `839 passed + 127 subtests`、inspect PG=`38 + 43 subtests`、version/projection/fixture=`94 passed`、contract
  preflight=`61 passed`、control bound=`5 + 3 subtests`；R-019/R-029/R-031 继续可见。这些 author 证据不得写成
  S1b formal GO；fresh `f4f3e58..<response>` pinned Ultra review 待独立提交后发出，但不阻塞 S1e0 与无关批。
  S1c candidate
  已增加 equality-only `owner_target_revision_token`，保持 logical occurrence digest v1；numeric-only
  attempt/journal 仍为 v1，token-bearing aggregate 使用 v2/v2，旧 writer 默认与历史 numeric replay 保持可用，
  token-only activation 要求旧 replicas drain/quiesce。该 synthetic carrier 只证明 storage transport，不是
  `filter_projection` physical-owner evidence。S1d candidate 已以 registry-owned 显式 link policy 区分
  `no_command_v1`、`workflow_command_acceptance_v1` 与 `activity_attempt_terminal_v1`；historical start tool v2
  保持 Activity-terminal，current tool v3 使用 command-acceptance，`0013` 以 deterministic backfill +
  `NOT NULL`/no-default 执行 quiesced cutover。下一批 S1e 先锁定 start 的 exact approval receipt、command winner
  与 parent-budget reservation owner，再实现 `start_acquisition_run` physical adapter；随后实现
  `filter_projection` membership/publication adapter，之后才组装四工具 PG simulate/model-turn loop。剩余 5/15 schema-less action、
  R-029、scripted two-lab、live checkpoint 与 paid TML
  全部继续 open；无 provider/model/live 调用。S1b bundled Ultra 输出为 reference-only `NO-GO 0/4/6/0`
  （scope-list 绑定缺陷导致 digest 未绑定实际文件；另有 9 个 actionable new findings + R-019 residual），因此既
  不能写成 formal artifact，也不能忽略其 inspect 风险。S1c bundled Ultra 也因同一 scope invocation 缺陷为
  reference-only `NO-GO 0/2/3/2`；其中 numeric-only-v2/application replay 不一致、warm-pool rollout overclaim、
  value-owner/adapter 混淆、exact-string sentinel 与空 mismatch diagnostic 已在 S1d fixed-forward 并补回归。
  S1d fresh correctly scoped pinned Ultra review 已完成并有效绑定
  `bacae9e..3b235fd`，结论 `NO-GO 0/1/4/0`。当前 bounded fixed-forward response：在 reserve、prepare 与 shared
  accept 的任意 schema bootstrap/owner read/result write 前，以 server-owned historical registry exact lookup 并
  重建/比对全部 spec-derived occurrence pins；compatible v2/v3 policy swap 与 forged pins 零写；新增 `0014`
  brownfield preflight + deferred all-attempt effect/policy/link-shape matrix，覆盖 quarantined commandless owner link；
  S1a 已区分 historical start v2 Activity-terminal 与 current v3 command-acceptance，并将 `0012` 明确为
  quiesced/pool-recycled；canonical PG-only result aggregate、link-policy/readiness field owner matrix 与 fast preflight
  已补。Fixed-forward author evidence=`759 passed + 104 subtests`、migration runner=`45 + 71 subtests`、scoped
  mypy=`0/8`、lint/compile/diff green、global mypy=`81/4`。Fresh `5aa3936..4dddd0e` pinned Ultra re-review 已
  exact-bound 并返回有效 `NO-GO 0/3/3/0`：equality-alias string/JSON carriers、plan occurrence/Action binding、
  migration procedure、durable inventory/preflight 与 stale review-scope instructions 进入下一 bounded response；
  当前 author-side response 已要求 closed literals/JSON carriers 为 exact plain string，并从 registry 重建 canonical
  occurrence/terminal；plan acceptance 复用唯一 request canonicalizer 后精确绑定 locked Action input/target、actor 与
  workspace，strict decode malformed owner JSON，且 reserve/plan/inspect prepare+accept 的 equality-alias 在任何
  schema bootstrap/owner read/write 前失败关闭。Migration 改为 exact quiesce/drain/`0012->0013->0014`/recycle/
  compatible-release/registry-verify 顺序，两处 durable inventory 与 fast preflight 已同步，旧 review scope 已封存。
  Current evidence=`807 passed + 119 subtests`、plan+inspect PG=`54 + 44 subtests`、scoped mypy=`0/7`、lint/compile/
  diff green、global mypy=`81/4`。Fresh exact `28c2b2e..f4f3e58` pinned Ultra review 已返回有效
  `NO-GO 0/1/1/0`，另保留 R-019/R-029 residual：plan occurrence 须拒绝多余 root field，而 migration runbook
  须如实记录 `0012/0013/0014` 在 quiesced window 内同步 validate/backfill/scan。当前 bounded fixed-forward 已
  在 schema/connection/owner/quarantine 前要求 occurrence root 精确等于 plain-string `input_payload/target_ref`，
  normal/stale extra-key 均保持 pending 且 attempt/journal 零写；runbook 与 executable preflight 已改为同步
  validation/backfill/scan 与 `SHARE ROW EXCLUSIVE` 事实。并行的 S1b reason follow-up 将全部生产写入收敛到
  8-entry exact owner/phase/code registry，五个 repository reason 写入口在 read/native-write 前二次校验，
  reason-omitting patch 原样保留 brownfield，历史 `18c7583` v1 serializer 的合法 reason byte/hash probe 已冻结。
  Current integrated evidence：Operation runtime=`193 passed + 592 subtests`；result-slot/inspect PG+history=
  `70 passed + 52 subtests`；pre-Agent=`62 passed`；focused registry/producer=`10 passed + 22 subtests`；S1d exact=
  `3 passed`；historical probe=`2 passed`；lint/compile/diff green；global mypy ceiling unchanged=`81 errors/4 files`。
  Fresh S1b review 的两次 canonical execution 均未形成 verdict；filtered-cache diagnostic 已证明独立
  `gpt-5.6-sol/ultra/priority` transport 可启动，但随后由服务端 `usageLimitExceeded` fail closed。因此 S1b/S1d
  formal review 均继续 pending，当前 NO-GO 继续阻塞 signoff/live，但不阻塞 S1e0/S1e1 非 live 开发。
- [ ] D1n S1e start physical adapter：S1e0=`d05a073` 已 characterize generic approval/dispatch/budget gap；S1e1
  decision-lock（零 product code / 零 migration）选择专用 submit/create/result-accept 三 UoW。Physical Action 使用
  `approval_required`；ActionApproved seq2 的 exact `acquisition_confirmation_receipt.v1` event payload 同时是审批
  SOT 与五字段 acquisition parent-budget envelope；WorkflowStarted/CommandPlanRequested seq1/2 创建 root command，
  同 UoW reduce 到 exact `workflow_current_state`，OperationCommandPlanned seq1 是
  `workflow_command_acceptance_v1` winner。仅允许
  `isolated_local_canary + simulate|scripted`，generic approve/dispatch 不扩张，runtime_outbox delta=0。S1e2a
  author candidate 已实现 specialized pending-submit repository/adapter：任何连接前 revalidate current occurrence /
  mode / exact full bound root；同一有限 deadline 按 event-stream→Action→slot→preview 加锁，locked preview 再 bind；
  一个事务 create-or-exact-replay `approval_required` Action + `ActionApprovalRequired` seq1，fault/lost-ACK/
  concurrency 保持单 aggregate；仅 ratified `queued/approved` + accepted-slot successor 可继续 replay；四表必须
  PG-authoritative，slow-connect/statement 使用同一总 deadline，且 Operation/command/attempt/journal/outbox/provider/
  model/domain 零新增。Author exact=`37 passed + 2 subtests`，含 S1e0=`42 + 2`，adjacent=`267 + 13`，scoped
  mypy=`0/1`、global=`81/4`、lint green；首轮 local advisory `NO-GO 0/2/1/0` 已 fixed-forward，同 reviewer bounded
  re-audit=`GO 0/0/0/0`，不是 formal verdict。S1e2b current author candidate 已进一步实现 typed receipt-backed
  budget ref 与 specialized approval/create repository/adapter：同一 raw PG transaction 按 receipt→Action CAS→
  Operation→workflow seq1/2→dormant queued command→current state→planned winner 写入；event-stream lock 下只读 discovery
  恢复 persisted `approved_at` 并派生 command locks，之后完整 `FOR UPDATE` owner probes 与逐行 exact compare；
  command/source-event 固定 `not_before_at=9999-12-31 23:59:59`，create 不唤醒 owner；generic
  approve/reject/cancel/retry/resume/dispatch 对 v2 action 统一 pre-writer unsupported + 零写；outbox/result/domain=0。
  Author focused=`120 passed`；real PG=`9 passed + 21 subtests`，adjacent PG=`49 passed + 36 subtests`；
  scoped mypy=`0/1`、global mypy=`81/4`、Ruff/lint green。Formal S1e2b review returned `NO-GO 0/5/1/1`;
  current fixed-forward response adds canonical pre-adapter approval validation, raw-row exact replay, strict JSON
  replay equality, mixed-v2 operation-control fail-closed, and held-root command cancel/retry/resume/claim/ready-list
  zero-write coverage. Response evidence currently includes fast no-access `12 passed` and PG matrix
  `19 passed + 27 subtests`; fresh pinned re-review is pending. S1e2c author candidate 已实现 read-only
  prepare + shared accept：locked owner 重建 `acquisition_start_result_v2` success terminal，fresh accept 同事务写
  attempt/slot/journal 并释放 dormant command hold，exact replay 校验已释放 command，late attempt 只追加 quarantined
  attempt，fault 回滚 journal+release，post-commit 仅 best-effort recovery wake，`runtime_outbox` 仍为 0。
  Current S1e2c evidence：PG create/result matrix=`13 passed + 21 subtests`，delegate surface=`7 passed`，
  focused combined=`20 passed + 21 subtests`，PG adjacent/start/result=`53 passed + 36 subtests`，non-PG focused
  D1n=`122 passed`，scoped mypy=`0/1`，canonical global mypy ceiling unchanged=`81 errors/4 files`，lint/diff
  green。S1e1/S1e2a/S1e2b/S1e2c author
  evidence/future review 都不改变 `10/5`、served=0、provider/model/live=0；
  R-019/R-029、Plan §6#6、OB-2.2/10.3/10.4 均继续 open，fresh pinned non-author review pending。实现记录见
  `TRACK_D_D1N_S1E2A_START_SUBMIT_UOW_IMPLEMENTATION.md` 与
  `TRACK_D_D1N_S1E2B_START_CREATE_UOW_IMPLEMENTATION.md`、
  `TRACK_D_D1N_S1E2B_START_CREATE_UOW_REVIEW_RESPONSE.md`、
  `TRACK_D_D1N_S1E2C_START_RESULT_ACCEPT_UOW_IMPLEMENTATION.md`。S1e2d author candidate 已补 first consumer hop：
  acquisition root owner 对 `acquisition_root_command_payload.v2` 走 v2-only Action/Operation/owner-ref preflight，从
  immutable `start_snapshot.preview` 派生 downstream intent compatibility payload；accepted start result -> released
  root command -> root drain -> queued `acquisition.intent.resolve` 已由 PG test 覆盖。Evidence：root-hop node=`1
  passed`，create/result/root PG matrix=`14 passed + 21 subtests`，legacy adjacent root owner=`1 passed`，scoped
  mypy=`0/1`、Ruff green。S1e2d 不改变 served/live/provider/model=0，也不关闭 S1e2b formal `NO-GO`。
- [ ] D1m mixed-version rollout gate（R-019）：pre-D1m binary 可继续写 revisionless source row 并绕开
  revision-aware exact-claim canonical materializer。任何 hosted activation 前必须选择并验证其一：quiesced
  single-version cutover，或 separately reviewed dual-write/compatibility bridge；完成前不得声称 rolling overlap
  safe。删除条件是对应 rollout rehearsal + exact legacy/new-writer convergence evidence 固定到 pinned commit。
- [ ] R-029：宽松 action-schema bridge 仅可在 production action 尚无 implemented explicit schema/owner binder 期间存在；
  删除条件 = 全部 API-submittable actions（不是只看 served subset）连续一个 release window durable hit=0。
  observation epoch 必须每个 release window bump，且 `NOT VALID` checks 的既有行 validation 在独立部署完成；
  任一 action 进入 served 集前必须满足完整 schema+adapter+Activity+action-specific populated revisioned model-safe
  result spec + result/simulate serializer mapping + complete served predicate；D1m candidate 后当前 5/15
  schema-less、served=0。D1m 的 action-specific populated result spec/serializer/served predicate 仍未完成；review
  pending 不改变完整人口 zero-hit deletion condition。
- [ ] R-031 review closeout：D1g current author candidate 已将 actions/runs list、detail、provenance 及
  approve/reject/dispatch/resume/retry/cancel 统一到 server-derived exact-workspace preflight，run 同时校验 linked
  action owner；nested commands/events 分别按 linked operation+action owner 与 physical event workspace 在 SQL
  limit 前过滤，planned CRM/export command ref exact-bind 当前 run 且失败在所有写前。并补 foreign/missing
  transport parity、全路径零写、same-owner 与 open-mode 矩阵。Pinned `646e596` advisory 的 invalid-reference
  部分通过；positive planned replay 现先经过 schema/request validator 与 approval/target guard，并复用一次捕获
  的 exact-current response，避免 mutable ref 双读。Commit `ebe7ed0` fresh pinned non-author scope-local
  advisory=`GO 0/0/0/0`，含 parent-regression sensitivity；formal highest-effort artifact 仍 pending。有效 formal
  scope-matched artifact 前继续阻断 hosted/live multi-user Operation exposure、
  served-registry promotion及 manual/product/milestone signoff，但不阻断 bounded non-live implementation、
  fake/scripted 或 local open-mode testing。
- [x] D3a characterize-first（2026-07-14；enclosing commit 由提交后 handoff 固定）：Plan §6#1 的 registry/snapshot
  Scout 作者批已完成，机械冻结 registry **2 writers/3 refresh/1 upsert** + physical-reader/semantic-consumer 链、
  seed catalog live-input 链、snapshot writers **5+3**、shared loader **19 calls/8 files**（fallback provenance
  `6 empty + 11 latest-pointer + 1 baseline + 1 absent`）、root manifest/candidate fallback writers **3/13+3**、
  latest-pointer direct writers **7 production/operations + 5 fixture/scripted** + generic restore、3 类 whole-snapshot
  materializer、resolver **17 calls/6 files**；accepted target disposition 是 PG authoritative，文件路径只可
  migration bridge→mirror/retire/fixture。另冻结 workflow-command 无 generation/token/control
  epoch、`attempt` 可重置、1 个 recognized direct PG claim writer 与 **29 direct callers/6 modules**、future token 的
  API dict pass-through 风险。§6#6 仍只是 characterized prerequisite，未实施 migration/claim fence；company
  physical PG schema/workspace mapping/precedence/backfill implementation、
  claim physical shape/authority/redaction/CAS consumer 均未猜测。author evidence=`11+6` characterization、company
  adjacency `24+3`、claim PG adjacency `3`；首个 canonical formal runner 于 420s 超时并按门禁写成 NO-GO，
  不构成 review evidence；fresh 缩域 pinned rerun 仍 pending，且不阻断下一 non-live bounded batch。
- [x] D3b workflow-command claim-fence contract + effect/CAS consumer characterization（2026-07-14；enclosing
  commit 由提交后 handoff 固定）：按 Plan §6#6（**没有 OB-ID**）+ §6#7 + R-019 决策锁定，零产品码、
  零 migration、非 live。Scout 冻结 **13** 个物理 command mutator、claim/running **29/29**、
  success/failure/partial/wait **39/71/15/3**、event/reducer **62**（child planner **34**），以及 API
  mapper **80 calls/30 functions**、command/evidence carrier **14+6**、control-sync **27 calls/26 functions/5 files**、
  raw nested command return **7 sites/3 files**。现 operation/command/activity 表还没有 durable namespace/mode
  issuer 与 claim/terminal identity。owner 裁决 = plan-review session 必须先创建；canonical physical coordination
  lineage 是 `coordination_plan_review_id=plan_review_sessions.review_id`，物理类型同为 positive `BIGINT`；brownfield
  `NULL`、strict `>0`，禁止 `TEXT`/empty。baseline review row 无 scope；bootstrap 不得借用尚不存在 positive
  review/gate 的 normal fence。source session-create command 单独使用 `scoped_session_bootstrap_v1` context；private
  factory 在 exact selection 后签发 one-use `ScopedReviewSessionBootstrapAuthority`，Stage A claim commit 后交付
  pre-session current-claim `ScopedReviewSessionBootstrapReceipt`（不含 ActivityRun/ActivityAttempt/review/event/result），repository/factory+verifier 以
  `scoped-session-bootstrap-lock-v1(scope tuple, creation_idempotency_key)` 序列化（key **不含** review id/gate），
  exact-verify source generation/epoch/post-claim command attempt/lease、authenticated workspace、immutable source event 与 typed plan pins。
  private API 的 create variant 以 `(scope_digest, creation_idempotency_key)` unique，在一个 specialized bootstrap
  Stage-B PG UoW 写 claim-bound ActivityRun/ActivityAttempt + session/session-created event/source command terminal pair；
  Stage A 后未 commit 的 crash 须等 lease expiry/reselection。commit 后只用 credential-free
  `ScopedSessionCommittedReplayContext` read/exact-compare，禁止重建 authority/receipt/token 或写入；
  stale/cancelled/cross-tenant/collision 零写，post-commit retry 返回同一
  `ScopedReviewSessionCreateResult`/review id。只有 session commit 后 OperationRun 才可 lock 后 exact-copy positive review
  scope/id，并进入 normal `d3_v1` authority/business/coordination fence；legacy unscoped/JSON-scan session 不可采用。
  scoped-session repository 是 poll/action strict-D3 的唯一 scope/coordination issuer，OperationRun 与 downstream
  只能 exact-copy、不得 remint；command authority/receipt bind，全链 FK/lock-key numeric encoding 一致。
  `workflow_commands` 复用 existing
  `operation_id`（严格等于 OperationRun id），
  不新增 alias；future strict additive columns 精确 **20**（prior 16 + 四项 `workflow_commands` typed nullable
  predecessor columns；all-null initial/complete successor，禁止 JSON/half-null）；action-
  backed root 另受 Plan §6#6 `action-root durable-scope gate`（无 OB-ID、非 OB-10.4）阻断。canonical factory 只在
  exact-command selection 后单次铸 one-use ClaimAuthority；strict registry digest pins
  `claim_fence_policy=d3_v1` + `allowed_stage_ids`/stage policy；Migration C 的 physical population guards 是
  registry-generated hash-bound `scoped_session_bootstrap_command_types_v1` 与 `strict_d3_command_types_v1` 两个
  literal manifests + immutable command type，分别约束 bootstrap pre-review/terminal shape 与 normal positive-review
  D3 shape，禁止以
  digest sentinel 判别；`expected_stage_id` 只来自 exact selected row；authority 同时绑定 coordination
  id、business digest、selection generation、repository-time expiry、pre-claim command attempt 与
  atomic consumed authority identity；`consumed_claim_authority_id` 仅 current-selection one-use slot，reselection
  generation+1 后 clear，且该 slot 不承担/不宣称 durable historical audit SOT。selection UoW 先证明旧 execution lease/selection
  reservation absent/expired，再落 exact unexpired reservation，Stage A exact-match 并以 execution lease 覆盖它
  （绝不要求 absent/expired）+ atomic consumed id + command attempt `+1`，禁止 fenced wrapper 启动期铸造/standing
  capability；Stage A mint 含 post-claim attempt 的 current ClaimIdentity，ActivityAttempt exact-copy
  `command_attempt`，intent immutable source core 再持久化 `source_command_attempt`。Stage B/async supersession 共享
  physical predecessor generation/control-source-event CAS；initial tuple all-null 且锁内证明无 current phase，
  successor tuple complete，half-null fail closed。只有 typed `final_adjudication` terminal event 计划唯一
  record command；source binding = immutable core + append-once terminal tuple，record 有独立 current claim，并
  在同一 record UoW 内 fixed-order 锁定 source + record command，exact-compare current source
  operation/scope/generation/epoch/post-claim command attempt/status/event/digest。exact outcome map：authorizable→verification
  shadow_would_verify/intent applied；awaiting_budget→pending/awaiting_budget；needs_human→needs_human/applied；
  failed→failed/applied；timed_out record outcome→timed_out/applied；每项发 typed
  `company_identity_verification_recorded` discriminant event。fallback/policy-invalid→needs_human，nonretryable
  execution→failed；control timeout 是独立 intent timed_out + verification needs_human + control event。awaiting resume
  采用 post-gate-apply convergence：record UoW 只写 awaiting intent+`recorded_event_id`/domain event 并 terminalize；
  gate owner UoW 在推进 exact watermark 前按全局顺序 reserve/lock
  `resume-after-grant-v2:<scope_digest>:<positive-coordination-review-decimal>:<intent_id>:<phase_generation>:<recorded_event_id>`，再由 post-apply typed pins
  完成 command/outbox（active grant=`queued`，否则 `retry_wait`）；gate-event/grant owner 共用
  `maybe_plan_resume`。identity 含 recorded_event_id，不含 grant/瞬时 delivery；grant-first/record-first
  均可 exact-replay/reawaken，关闭 one-shot gap/old gate-digest collision。`not_applied` 仅允许
  `reason=stale_claim|business_precondition_conflict` 的零写返回，不是 event/state。
  跨 owner human/recompile/所有 cancel、D3 OperationRun terminal/cancel/retry/requeue/resume/reset/rebuild/recovery 与
  dispatch-invalidating control 共用 `d3-dispatch-v2` 有界 coordination lock，key=immutable scope tuple + exact
  coordination id，root intent 不参与；Stage B/async supersession/
  record/terminal/control/dispatch/resume_after_grant 的唯一锁序均为 operation root → optional plan/review/gate →
  **all participating workflow_commands**（source/record/resume/supersession/current owner/idempotency target，确定序）
  → intent/predecessor → ActivityRun/Attempt → optional grant/cost；进入 intent 后不得回头 insert/lock command，未触及
  aggregate 只可跳过、不可逆序。`d3_business_fence_v1` evaluator 只收 closed typed context union：claimed-command
  `stage_b|terminal|record|dispatch|resume_after_grant` 带 authority+receipt；aggregate `control` 带 registered control
  authority/expected revisions 且无 claim token；六 phase mandatory、wrong context fail closed，JSON/ambient 不得补齐
  （仅 heartbeat/read-only
  exact replay 窄例外）。只有 invalidating control/input/requeue 接受态推进 control epoch `+1`；
  `succeeded|failed_terminal` terminal 不加 epoch，但清 token/lease并写 terminal pair。control-first/stale/business
  mismatch 的 domain/attempt/intent/event/command/source/result 零写；dispatch-first 只允许已授权 in-flight。
  D3c2h0 fixed-forward 要求 response/failure receipt 在 full five-field PFX 下 exact-copy exposure 的 post-claim
  `command_attempt`；response-only quarantine 禁止 `workflow_run_id` 与 `reconciled_no_call`，其 immutable
  identity/digests insert-once，cost/retention typed CAS entrypoints 写集分离且 monotonic；
  `cost_state: pending_reconciliation -> reconciled_confirmed | reconciled_uncertain` 与
  `retention_state: retained -> purged_tombstone` 正交，no-call exposure 无 receipt/quarantine，domain 零 apply。terminal command/event
  用 local both-null/both-non-null CHECK + `MATCH SIMPLE DEFERRABLE` composite FK；该 FK 只证明 forward edge，
  reverse orphan-event rejection 由 terminal repository 单 PG UoW + injected-failure rollback 证明；pair 在 result-terminal 期间不可变，
  仅 registered reopen 可清；future native-PG DDL 须证明 null/half-null/exact/mismatch/non-orphan 语义。heartbeat 按
  occurrence identity；public allowlist/token 永不投影；完整 29-caller inventory 仅作
  observation/zero-unauthorized-increase denominator，scope-local deletion 只覆盖 bootstrap/strict-D3 manifests；任何 `legacy_unfenced` entry/
  caller 尚存时 generic bridge 明确保留。
  record apply 是 terminal-UoW specialization：one PG transaction 同时执行 record+terminal predicates，写
  verification/intent、recorded domain event、workflow terminal event、ActivityAttempt+command terminal pair；crash
  全回滚/exact replay 全 aggregate。terminal policy registry 与 transport terminal receipts 现为两个独立 owner row。
  checked-in `TERMINAL_PROVENANCE_SPECS` / closed `TerminalProvenanceSpec` union 是唯一 semantic SOT，精确包含
  `TransportResponseSpec|TransportAttemptFailureSpec|NoExposureTerminalSpec` 三类；每个
  `(command_type, stage_id, terminal_transport_variant, terminal_status, terminal_event_type, terminal_outcome,
  terminal_reason if exposure, failure_code if attempt_failure)` 必须命中唯一 applicable entry。
  `terminal_provenance_policy_digest` 在 command creation 固定，并在 exposure creation exact-copy；historical
  entry/digest 在仍有 active/retained command、exposure、response/failure receipt、terminal event、source-intent
  tuple、quarantine/tombstone 或 cost/audit 引用时不得删除或
  reinterpret。transport-backed response terminal exact-bind committed exposure/physical-call/provider-call/
  `ModelInvocationEnvelope`/stable occurrence + canonical response/result digest + result artifact ref/digest；合法
  `length|content_filter` terminal envelope 仍是 response，只有 incomplete/truncated wire 或 protocol-parse failure
  才能写 `TransportAttemptFailureReceipt`。exposure `result_ref=receipt.result_artifact_ref or ''`；same-digest/
  different-result-ref、different artifact 或 half-pair 全部零写 mismatch。failure receipt exact-pin registered failure
  spec/code/canonical digest、required failure artifact、claim/business pins、wire-call state、`retry_policy_revision` 与
  `retryable|terminal` disposition；retryable 只能 `retry_wait` 且 terminal/source tuple 零写。response/failure creator
  与 exposure-backed terminal 共锁同一 exposure：response-first 拒 failure terminal；terminal-disposition failure-
  first 后 valid response 仅进 quarantine；retryable-failure→`retry_wait` 与 later valid response 同样只留 response
  receipt/late quarantine，不能恢复 apply。registered non-transport 或 proven pre-call/no-send terminal 仅由
  `NoExposureTerminalSpec` 授权 complete-none `no_exposure`；它在 common `d3-dispatch-v2` coordination lock 下证明
  dispatch/exposure absence，**绝不**尝试锁一个不存在的 exposure row。stale already-authorized receipts 仅 audit/cost、
  不授权 apply；attempt/protocol failure 本身不进 response quarantine。provider delivery id 或 durable inbound
  `TransportResponseReceipt` get-or-create stable occurrence；redelivery 复用。`late-response-v1` 只保留为
  domain-separation tag；D3c2h0 已否决 scope-digest-only receipt/quarantine occurrence/idempotency key，D3c2h1 须以
  完整 PFX ratify exact encoder，digest mismatch collision。legacy `exposure→receipt→quarantine/cost-axis` shorthand
  已被 supersede，quarantine 不再是无条件 tail。
  post-network 分成两条 UoW：pure exposure-first 仅 exposure lock→applicable receipt→exposure terminalization，
  quarantine permission=0；只有先取 `d3-dispatch-v2` 并按 operation root→optional plan/review/gate→all participating
  commands→intent/predecessor→ActivityRun/Attempt 完成全局 owner-row 前缀锁/验、从 stored current state 分类的
  response-classification UoW，才可在 exposure tail 插 optional response-only quarantine；进入 exposure 后不得回头。
  caller flag、callback 或 stale `ClaimReceipt` 不具分类权；pending response-classification owner/state、retry/recovery、
  exact replay 与 idempotency 留 D3c2h1，也不授权 send/apply。
  current canonical `ModelInvocationEnvelopeV1` 没有 durable ref issuer；D0f 必须先落 sole owner/ref grammar，禁止
  placeholder ref/hash 或第二 envelope schema。Plan §6#7 的 owner matrix 已锁成机械非空 **26×10**（将 terminal policy
  registry 与 transport terminal receipts 分行；owner/SOT/allowed/derivation/consumers/forbidden/fallback/migration/
  deletion），decision shape complete；物理 owner/migration/repository/runtime 仍 NOT IMPLEMENTED。rollout 依赖序
  固定为 public projection/migration 与 registry/policy pins、双 population manifests、bootstrap authority
  factory+verifier 先落；随后才启用 `scoped_session_bootstrap_v1` specialized Stage-B session UoW 与
  credential-free committed replay；session commit 后才创建 exact-copy OperationRun 并进入 normal positive-
  review `d3_v1` fence；三类 terminal receipts/race acceptance 完成后才可启用 non-live strict path。
  `OB-10.1/10.2/10.3/10.4` 与 action-root gate 均 carry；R-019 仍 open，served=0。首轮 local non-author
  advisory=`NO-GO`（P0/P1/P2/P3=`0/8/2/0`）；第二次 fresh local advisory 仍为 `NO-GO`
  （`0/8/3/0`，11 findings）；第三次 fresh local advisory 仍为 `NO-GO`（`0/5/4/0`，9 findings）；round4 fresh
  advisory=`NO-GO`（`0/3/2/0`），parallel semantic audit=`NO-GO`（`0/5/4/0`）；round5 semantic=`NO-GO`
  （`0/5/2/0`），fresh broad=`NO-GO`（`0/3/3/0`）；post-round5 round6 semantic=`NO-GO`（`0/1/2/0`），
  broad=`NO-GO`（`0/4/0/0`）；round7 semantic=`NO-GO`（`0/4/0/0`），broad=`NO-GO`（`0/4/2/0`）；round8
  semantic=`NO-GO`（`0/1/0/0`），broad=`NO-GO`（`0/6/1/0`）。全部 finding 均在本 decision-only 文档批
  fixed-forward；两次 round9 local advisory 因 Codex operator usage limit 在 final response/artifact 前终止，故
  **没有 verdict**；其直接 partial findings 已 fixed-forward。fresh non-author re-review 与 pinned formal review
  pending，不能写成 formal GO，并仅对 Live/signoff fail closed。current author evidence=D3b `32 passed`、D3a+D3b
  characterization `49 passed`、lint `58 files`、mypy `81 errors/4 files`（棘轮持平）、diff clean；这些仍不是 formal GO，
  且不阻断 owner 已授权的下一 non-live bounded implementation。
- [x] D3c1 public workflow-command projection seal（2026-07-15；由 D3b §10/rollout step 1 推导；零 migration、零 claim/CAS）：
  `CommandKernel._workflow_command_api_record` 改 checked-in closed allowlist，保留 33 个既有 descriptor +
  `claim_generation/control_epoch` 两个只读 diagnostic + 7 derived fields；递归移除 payload/result capability，
  消除 **7** 个 nested raw command returns，令修正后的 **16 carrier routes/17 method-route variants**、
  `6 existing + 1 typed nested = 7` schema refs、9-field typed `operation_sync`、frontend adapter/demo raw 全部
  收敛到同一 projector。author evidence=`7` D3c1、`45` D3a+D3b+D3c1、operation/control adjacency `14`、
  frontend build `81 modules`、lint `58 files`、mypy `81 errors/4 files`（棘轮持平）、diff clean。首个 pinned formal
  attempt 的 reviewer substantive output=`0/2/3/0`，但 runner 因 active settings/prompt/final/single-turn causal
  binding 失败将 artifact 标记 invalid；因此既不是 formal NO-GO 也不是 GO，五项 direct findings 已由下列
  D3c1a candidate fixed-forward；原 D3c1 candidate 不再是当前评审候选。Scope issuer/ClaimAuthority/29 callers/
  Stage A/B/dispatch/migration/action-root durable-scope gate/R-019/OB-10.1-10.4 与 served=0 全部继续 carry；
  不得借 projection seal 宣称 fence 闭合。
- [x] D3c1a workflow-command public-projection fixed-forward（2026-07-15）：先闭合 D3c1 invalid artifact 的五项
  direct findings；随后 precommit adversarial author-audit 又闭合结构化 Activity `artifact_refs`、cancel/retry/resume
  **4 singular + 3 served plural** Activity carriers + derived control fields、command `result` 内 **8 canonical + normalized aliases**
  recursive Activity carrier keys、跨语言 mathematical safe-integer canonicalization（`1.0 -> 1`、`-0 -> 0`）与
  canonical/normalized nested workflow-command carriers、
  operation-sync/direct-Activity nested-command closure、generic recursive forged-summary removal、hazardous-key
  rejection、backend/frontend typed malformed-input parity、generic Activity derived-provenance stripping、compact
  observation canonicalization 与
  descriptor/literal independent oracle，以及完整 Operation action/event/run、registry mirror、typed trusted
  execution summary、normalized response-envelope dual-source、demo raw/provenance、strict required-status wrapper
  也在同一 precommit audit fixed-forward。这些后续项不是 retroactive invalid-review findings，也
  不是 formal review evidence。
  ActivityRun/ActivityAttempt/EntityDelta 与 `WorkflowActivityControlTarget` 改为 backend-owned
  closed projection，并与 schema/TypeScript/adapter/demo 的 `23/27/26/10` fields 机械对齐；private roots 统一以
  normalized snake + compact root-prefix fail closed；generic carrier 一律丢弃 caller-supplied `execution_summary`，
  只有 trusted Activity owner 可在 final projection 后重挂；Activity `artifact_refs` 保留经递归净化的 mixed JSON，
  command `artifact_refs` 继续是既有 string array；`claim_generation`/`control_epoch` 与 Activity `attempt_number`
  限于 canonical non-negative JavaScript-safe mathematical integer。primary public manifest=
  `17 command/operation/action + 6 Activity = 23 method-route variants`，compact job-materialization binding 继续
  独立计数。commit `4cfd1916da8bd98483d1ecfdba1f66639b122da9` 的 author evidence：D3c1a `13`、
  D3a+D3b+D3c1a+durable `56`、full operation
  runtime `129`、pre-Agent adjacency `4`、Activity HTTP/route parity `2`、adjacent migration/PG `9 + 20 subtests`、
  frontend standalone compile exit 0 + build `81 modules`、lint repo `58 files` + D3 tests + pre-Agent oracle、
  mypy `81/4`、diff clean。
  后续 pinned artifact
  `runtime/reviews/20260714T215839Z_Track_D_D3c1a_workflow-command_public_projection_fixed-forward.md` 虽
  `reviewer_exit_code=0`，但 `causal_binding.final_response_item_exact=false`，故为 **invalid/advisory only**；其
  substantive `NO-GO` 不得写成 formal `NO-GO`。#1/#2/#4-#11 已通过 exact-built-in bounded copier、single
  backend/frontend traversal budgets、ActivityRun descriptor 与 WorkflowCommand control-target 分 owner current
  evidence、once-sanitized strict envelope/applied-outcome、shared
  `NumberRecord`、producer-owned policy field families、constructor-only `CommandKernel` 与一致 review wording
  fixed-forward；#3=`R-019` 继续 open。该 follow-up 的 stable evidence=`18` D3c1a、`63` D3+durable、full
  operation `129`、pre-Agent `60`、cancel/request-scope `51`、frontend `81 modules`、public mapper `85/34`、
  Ruff/diff clean、mypy `81/4`；fresh dirty-tree non-author advisory=`0/0/0/0 CLEAN`，但 fresh pinned review 仍 pending。
  第二个 pinned artifact `runtime/reviews/20260714T231243Z_Track_D_D3c1a_public_projection_advisory_fixed-forward.md`
  同样只因 Desktop terminal memory-citation suffix 令 `final_response_item_exact=false` 而 invalid；其
  `P0/P1/P2/P3=0/4/4/1` 与 printed `NO-GO` 仅 advisory。action-specific outcome、wrapper once-capture/shared
  budget、demo bounded/cycle-safe、single outcome owner、真实 PG bounded 500-id batch/zero point read、exact-empty
  `operation_sync` sentinel、mutable Activity TOCTOU 与 stale wording 均已 fixed-forward。focused evidence=
  projection `18`、claim-fence `38`、backend exact matrix `6 + 3 subtests`、real-PG `2`、storage guardrails `60`、
  pre-Agent `60`、frontend build `82 modules`、mypy `81/4`、Ruff/diff clean。final integration audit 还闭合
  frontend nonempty-projected-empty sync、DAG alias output amplification、foreign workspace/run provenance 与
  whitespace-wrapped outcome，dirty-tree non-author advisory=`0/0/0/0 CLEAN`。commit `4919990...` 后取得两份
  exact-object medium pinned advisory：backend=`NO-GO 0/0/2/1`、frontend=`NO-GO 0/0/5/2`。其唯一 findings 已
  fixed-forward 为 real-PG exact-500 + acquisition/command-operation independent mismatch controls + raw-input bound
  wording，以及 key/string/occurrence/body byte budgets、O(1) memo weight、non-enumerable demo raw、descriptor-first
  zero-getter capture、phantom-empty omission、action-specific TypeScript return types、null/Proxy exact-empty
  fail-closed。current evidence=`119` combined、projection `19`、real-PG `6 + 500 subtests`、501 guard `1`、frontend
  `82 modules`、Ruff/diff clean、mypy `81/4`；无可靠 Content-Length 时 read-before-allocation streaming cap 仍为
  explicit residual。fresh pinned re-review 与 formal highest-effort review 均 pending。
  因此在 valid scope-matched artifact 前既无 formal GO 也无 formal NO-GO，Live/W6/manual/promotion/
  signoff fail closed，但不冻结无关 non-live batch。该批零 storage/runtime write、migration、claim/CAS、Stage A/B、
  dispatch 或 served 激活；完整非闭合边界见
  `TRACK_D_D3C1A_WORKFLOW_COMMAND_PUBLIC_PROJECTION_FIXED_FORWARD.md`，R-019、action-root、OB gates 与 served=0 不变。
- [ ] R-030 frontend demo `raw` compatibility retirement（owner=`frontend API / OperationsPage`）：先机械盘点
  `WorkflowCommandRecord`、Activity/Attempt/Delta、Operation Action/Run/Event/Provenance 的全部 `raw` 读者，给每个
  读者迁移到既有 typed DTO field 或一个单独命名且独立有界的 wire snapshot；迁移期间禁止 `raw` enumerable、
  spread/`Object.assign`、Response body/cache key/transport serializer 使用。所有读者清零后，在同一有界批删除
  `attachDemoRaw`、全部 exported `raw` interface members 与 compatibility projection；保留 `R-030` ledger row，
  追加 exact exit evidence 并把状态从 `accepted` 转为 `closed`，不得删除审计历史。
  exit evidence 必须包含：静态 consumer inventory `>0 -> 0`、frontend type/build、全部 workflow/operation endpoint
  snapshot regressions、无 `raw` public serializer 的 source scan，以及 scope-matched independent review GO。在这些
  条件满足前 R-030 保持 accepted residual，不阻断与该 compatibility surface 无关的 non-live Track D 开发。
- [x] D3c2a dormant workflow-command claim-fence migration foundation（2026-07-15；仅 D3b Migration A 的 command
  subbatch）：`0003_workflow_command_claim_fence_foundation.sql` 精确新增 20 列 + 16 个 `NOT VALID` local checks，
  5s transaction-local lock budget；populated
  legacy row 保持 empty/zero/NULL sentinel，新写 shape 仍受约束，0003-only lock timeout 证明 columns/checks/ledger
  全回滚且释放后 single apply + no-op。`WORKFLOW_COMMANDS` descriptor 仍为 33 列，20 个 raw PG fields 全部 dormant，
  零 runtime writer/reader/claim/CAS/dispatch 激活。完整 Migration A 的 session/operation/activity/event/receipt/
  quarantine、canonical-id 最终 grammar、Migration B-D、R-019/action-root/OB-10.1-10.4 与 served=0 全部继续 open。
  author evidence=migration/PG `9 passed + 20 subtests`、D3a+D3b+D3c1+durable adjacency `51`、lint `58 files`、
  mypy `81 errors/4 files`、diff clean；fresh pinned non-author review pending，不能写成 formal GO。
- [x] D3c2b dormant scoped review-session / OperationRun root foundation（2026-07-15；D3b Migration A root
  subbatch）：`0004_d3_scoped_root_foundation.sql` 精确新增 `plan_review_sessions` **11** 个 scope/causal-plan/
  idempotency columns + `operation_runs` **5** 个 scope exact-copy/nullable BIGINT coordination columns，合计
  **16** 个 `NOT VALID` local checks 与 5s transaction-local lock budget。populated legacy rows保持 empty/zero/NULL
  sentinel；先执行 session DDL、再被 OperationRun RowExclusive writer 阻塞时，两表 columns/checks + 0004 ledger
  整笔 rollback，释放后 single apply + no-op。legacy plan-review mapper 与 `OPERATION_RUNS` descriptor 继续丢弃
  全部新列；零 scoped repository/exact-copy writer/adoption/FK/index/unique/claim/CAS/dispatch/served 激活。
  author evidence=migration/PG `11 passed + 36 subtests`、scoped-root exact node `1 + 16 subtests`、D3+durable
  adjacency `58`、operation/plan-review adjacency `3`、lint `58 files` + targeted modified tests green、mypy
  `81 errors/4 files`（棘轮持平）、diff clean。
  Activity/event/receipt/quarantine Migration-A fragments、Migration
  B-D、registry/policy pins、双 population manifests、bootstrap factory+verifier、R-019/action-root/OB-10.1-10.4
  与 served=0 全部继续 open；完整 Migration A 前不得越序进入 rollout step 3。implementation=
  `0aa253c7d7e5324f5c0021570e2980f358ea3922`；fresh pinned artifact
  `runtime/reviews/20260714T222746Z_Track_D_D3c2b_scoped_root_migration_retry_1.md` 为 valid scope-local `GO`
  （P0/P1/P2/P3=`0/0/0/0`，scope digest
  `7988dd50814ba1c2cc4a3c5efa7ec40ac7eb47b80cbce901ecc8c04a83a92685`）。该 `GO` 只覆盖 dormant
  nine-file physical foundation，不关闭 R-019、完整 Migration A、runtime activation 或 live/signoff gate。
- [x] D3c2c Activity / terminal-evidence physical surface characterization（2026-07-15；零产品码/零 migration）：
  descriptor/call inventory 机械冻结为 ActivityRun `20 cols / 30 upserts in 5 files / 20 list / 26 raw get
  = 25 external + 1 internal`（新增 1 个只服务 incomplete test-double 的 compatibility fallback，production list
  使用 bounded batch、零 point read）、ActivityAttempt `22 cols / 22 upserts in 4 files / 21 list / 2 raw get = 1 external
  + 1 internal`、event `17 cols / 62 append_event_and_reduce in 7 modules / 2 list`；唯一 physical event INSERT
  owner 为 `LiveControlPlanePostgresAdapter.append_workflow_event`，ActivityRun 另有唯一 direct cancel `UPDATE`。
  current `attempt_number` 不是 future post-claim `command_attempt`；event→commands→outbox→state 仍为 R-019
  multi-commit。ratified exact verification-intent/response-failure-receipt/late-quarantine/terminal-registry named
  schema surfaces 当前未出现；durable dispatch-exposure owner/table name 仍 `unratified/undetermined`，不得作
  zero claim 或发明 lexical predicate。下一 implementation 只能从 owner-ratified DDL 开始，顺序为
  dormant ActivityRun+Attempt fragment → event fragment → intent/receipt/quarantine fragments；不得猜 schema/owner。
  R-019/R-023/R-027/R-029、action-root、OB-10.1-10.4、完整 Migration A 与 served=0 均不变。完整事实与
  executable oracle 见 `TRACK_D_D3C2C_ACTIVITY_TERMINAL_EVIDENCE_CHARACTERIZATION.md`。首个 pinned artifact
  `20260714T232853Z_*` 因 `final_response_item_exact=false` invalid、不是 formal `NO-GO`；其 lexical exposure-zero
  advisory 已 fixed-forward。retry `20260714T234336Z_*` 同样因 Desktop response-item memory annotation invalid；
  其完整 item 5/6/7 Migration-A member order、exact non-closure tuple 与 stale 7-test evidence 已 fixed-forward，
  current full oracle=`8 passed`；`e01e9f0` 的 fresh pinned medium-effort advisory=`GO 0/0/0/0`，formal
  highest-effort retry pending。
- [x] D3c2d dormant ActivityRun / ActivityAttempt claim-chain foundation（2026-07-15；implementation candidate）：
  `0005_d3_activity_claim_chain_foundation.sql` 按 D3b §5.2/§11.1 ratify 并安装 ActivityRun `6` columns 与
  ActivityAttempt `10` columns，保留 existing workspace/operation/command links；`command_attempt` 为 future
  post-claim `workflow_commands.attempt` exact-copy，current `attempt_number` 继续独立 retry accounting。迁移以
  5s local lock budget 安装 `7+11=18` 个 `NOT VALID` local checks，populated rows 保持 empty/NULL/zero sentinel；
  ActivityAttempt lock contention 必须证明 first-table DDL/constraints/ledger 全回滚后 exact-once recovery。
  current 20/22-column descriptors、30/22 upsert population、direct cancel owner 与所有 runtime writer 均未激活新列。
  workflow event、intent/terminal registry/dispatch exposure/response-failure receipt/quarantine、Migration B-D、
  R-019/R-023/R-027/R-029、action-root/OB-10.1-10.4 与 served=0 继续 open；完整记录见
  `TRACK_D_D3C2D_ACTIVITY_CLAIM_CHAIN_MIGRATION_IMPLEMENTATION.md`，author validation/fresh pinned review 待记录。
- [x] D3c2e WorkflowEvent terminal-lineage physical decision lock（2026-07-15；零 SQL/产品码）：ratify exact
  `11 columns + 11 NOT VALID local checks`，复用 existing `workflow_run_id/operation_id/command_id/
  activity_attempt_id`，禁止 event-side `operation_run_id`、`source_verification_command_id`、
  `source_activity_attempt_id`、`source_command_attempt` aliases；linked ActivityAttempt 继续拥有 post-claim
  `command_attempt`，verification intent 单独 exact-copy。D3c2e-D1..D10 显式 defer transport provenance、
  receipt/exposure/quarantine、完整 verification-intent DDL、index/FK/population checks、adoption 与 runtime UoW。
  mechanism×10 invariant matrix 与 executable oracle 见
  `TRACK_D_D3C2E_WORKFLOW_EVENT_TERMINAL_LINEAGE_DECISION_LOCK.md`；R-019/R-023/R-027/R-029、action-root、
  OB-10.1-10.4、完整 Migration A 与 served=0 不变。commit `1fb052fe...` fresh pinned medium-effort non-author
  advisory=`GO 0/0/0/0`；formal highest-effort review 仍 pending。
- [x] D3c2f dormant WorkflowEvent terminal-lineage foundation（2026-07-15；implementation candidate）：one-table
  `workflow_events` ALTER 安装 exact 11 columns + 11 `NOT VALID` checks 与 5s lock budget；real-PG acceptance 覆盖
  populated/current-writer sentinel、逐字段 malformed update、timeout 后 columns/checks/ledger 全回滚、释放后
  recovery-once/no-op。descriptor 保持 17 columns，current explicit INSERT 继续省略新字段；无 index/FK/
  validation/backfill/runtime/provenance/receipt/exposure/intent/quarantine/served 激活。完整记录见
  `TRACK_D_D3C2F_WORKFLOW_EVENT_TERMINAL_LINEAGE_MIGRATION_IMPLEMENTATION.md`；author validation/fresh pinned review
  已记录。commit `47a7f7db...` fresh pinned medium advisory=`NO-GO 0/0/2/0`；SQL/runtime 边界通过，两项 P2
  为 stale `0006` absence 文案与 zero-PG exact-DDL oracle 缺口，均已 immediate fixed-forward。该 fixed-forward
  commit `316741a...` 的 fresh pinned medium re-review=`NO-GO 0/0/1/0`：原两项已关闭，但只计
  `CHECK ... NOT VALID` 的提取器仍接受第 12 个 validating constraint；现已追加 every-`ADD CONSTRAINT`
  ordered-name/total exact assertion。`68c901a...` re-review 再报 medium `NO-GO 0/0/1/0`：quoted name 或 alternate
  whitespace 仍可绕过 name extractor；现已把所有 `ADD`/`CONSTRAINT` token 与 name/predicate extraction 独立计数
  并 exact-compare。commit `e7db34e...` fresh pinned medium re-review=`GO 0/0/0/0`，五种第 12 constraint mutation
  均 fail closed；migration/runtime 不变，formal highest-effort 仍 pending。
- [x] D3c2g cost-ledger/dispatch-exposure physical decision lock（2026-07-15；decision-only）：ratify sole future
  `CostLedgerRepository` via `store.repos.cost_ledger`，以及 inseparable `cost_reservations` +
  `dispatch_exposures` aggregate；锁定 exact `21/75` ordered columns、PK/unique/FK/index、`16/29` local checks、
  pricing/reconciliation registries、eight-method CAS、source-dependent terminal mapping、money conservation 与
  parent-first credential-free settlement。strict-D3 live/simulate/scripted 均创建 durable exposure；live money
  positive，simulate/scripted zero，replay 因 current D0 envelope enum 不含 replay 而等待 D0+schema revision。
  OB-2.2/OB-10.3 仅变为 `decision_locked_not_implemented`；当前 baseline/descriptor=`83/41` 且 future owner/tables/
  registries/store wiring 全部 absent；Decimal/TIMESTAMPTZ codec、specialized insert-once/CAS 与 durable envelope
  ref grammar 仍是 implementation prerequisites。完整记录见
  `TRACK_D_D3C2G_COST_LEDGER_DISPATCH_EXPOSURE_DECISION_LOCK.md`；零 SQL/descriptor/repository/runtime/live，
  本 decision lock 不授权 migration，R-019/R-023/R-027/R-029、action-root、其余 OB 与 served=0 不变。
- [x] D3c2h0 evidence cross-contract ratification（2026-07-15；decision-only）：full five-field PFX 取代 D3b
  receipt/quarantine scope-only sketches；response/failure receipt exact-copy exposure 的 post-claim
  `command_attempt`；response-only quarantine 禁止 `workflow_run_id` 与 `reconciled_no_call`，cost axis 只允许
  pending→confirmed|uncertain。post-network 分成两条 UoW：pure exposure-first 仅 exposure lock→receipt→exposure
  terminalization、quarantine permission=0；response-classification 必须先持 `d3-dispatch-v2` 并锁/验 complete global
  owner-row prefix，从 stored current state 判 current/stale，才可在 exposure tail optional quarantine，且进入 exposure
  后不得回头。caller flag/callback/stale `ClaimReceipt` 不具分类权；pending classification retry/recovery/idempotency
  留 D3c2h1。initial v1 仅 `model_tool_v1` + live/simulate/scripted；simulate/scripted zero-cost
  exposure/receipt 可达，replay zero-write fail-closed；Harvest/provider-search 必须另有 owner-ratified variant，
  不得伪装 model transport。完整记录见 `TRACK_D_D3C2H0_EVIDENCE_CROSS_CONTRACT_RATIFICATION.md`；oracle=`9 passed`，
  零 exact manifest/SQL/descriptor/repository/runtime/live，不授权 migration，fresh pinned/formal review pending。
- [x] D0f durable ModelInvocationEnvelope ref owner（2026-07-15；D3c2h1 prerequisite closed）：保留
  `ModelInvocationEnvelopeV1` 为唯一 canonical shape/digest/JSON owner，并以 sole PG-only
  `ModelInvocationEnvelopeRepository` 落 full-PFX `mie:v1` ref、15-column immutable evidence table、specialized
  advisory-lock + `FOR UPDATE` + plain insert exact replay/collision、`live|simulate|scripted` 六段 causality + cost-ref
  presence、live route-snapshot presence，以及 fixed 30-day DB-clock retained→purged-tombstone CAS；新增 exact
  nullable timezone-aware `TIMESTAMPTZ` codec。它不是 logical result-slot accept/consume、receipt/quarantine、cost
  ledger、runtime writer 或 live activation。author evidence=`83 + PG 1 + migration 15/65 + guards 64 + adjacent 22 +
  control-plane 17`，Ruff/format/diff clean，focused mypy clean，global mypy=`81/4`；fresh pinned formal review pending。
- [ ] D3c2h1 exact evidence-surface decision-lock fixed-forward：首个 pinned
  `gpt-5.6-sol/ultra/priority` non-author review 对 `1c4a2d9177dcb3470117700086b12fd533898bb7` 为 formal
  `NO-GO 0/3/3/0`。当前 repair 将六项 finding 固定为：nonterminal `current_pending_apply` + fresh normal-terminal
  continuation；failure/retry/second-response immutable-cost race table；attempt-8 transient/lease total convergence；
  blank/whitespace artifact ref fail-closed。针对 `f0a0069...` 的 Ultra review 尝试因 transcript
  `child_thread_ids_distinct=false` 为 `invalid_transport`，substantive `0/1/2/0` 仅 advisory。第二轮 author repair
  进一步把 combined boundary 固定成 complete 13 upstream + 52 seven-table constraint tuples（29 FKs）、11 index +
  12 admitted access tuples、17-step create/attach + 16-step rollback DAG；六个 internal forward/cycle FKs 在所有
  targets 存在后才 attach；identifier 全部 <=63 UTF-8 bytes；pending-state index 明确服务 `<8` claim 与 `=8`
  convergence；三条 nullable timestamp CHECK 以 `IS NOT NULL` + `IS TRUE` 拒绝 `UNKNOWN`；oracle exact-compare
  relation/index/access/race/DAG 全 tuple。typed plan/review/gate parent 与 Tier-2 grant parent 的物理
  table/key/unique target 当时尚未 ratify；后续 D3c2i 已独立锁定 exact owner decision，但 D3c2h1 repair 与 D3c2i
  均须 matching pinned non-author `GO`，不得猜 schema、用 JSON/application-only proof 或无 FK 绕过。当前仍为
  `decision_locked_not_implemented`，零 migration/repository/runtime/provider/live；即使本 repair `GO` 也不单独授权
  dormant migration。Pinned `af4db419...` fresh scope-local advisory=`NO-GO 0/0/1/0`；唯一 P2 是文档误称 future
  migration 须新增其实已由 `0003` 安装的 `workflow_commands.workspace_id`。当前 fixed-forward 改为 adopt/validate
  既有 `TEXT DEFAULT '' NOT NULL` + named `NOT VALID` check，保留 brownfield empty sentinel，禁止 add/drop/rewrite/
  reinterpret；oracle 直接读取 `0003` 精确证明。Current author evidence=H1 `13`、related battery `139`、Ruff/
  diff green。`fdb3b792...` fresh exact-object non-author local advisory=`GO 0/0/0/0`，关闭该 P2 re-raise；artifact=
  `runtime/reviews/20260715T215450Z_Track_D_D3c2h1_fixed-forward_local_advisory.md`。它不是 formal highest-effort
  `GO`，因此 dormant migration/live/product gate 仍 fail closed。
- [x] D3c2i typed plan/review/gate + Tier-2 grant parent decision lock（2026-07-15；decision-only）：sole future
  `PlanReviewAuthorityRepository` / `store.repos.plan_review_authority` owns exact immutable
  `plan_review_gate_authority_versions`（32 columns）、`identity_search_budget_grants`（37）与
  `identity_search_budget_consumptions`（22）。exposure 19–25 exact-FK 到历史 typed authority unique；Tier-2
  columns 41–43 exact-FK 到 full-PFX immutable grant issuance，Tier-1 all-NULL 保持合法。grant 四维 balance、
  pre-transport debit exact replay、revoke/exhaust/supersede-with-transfer/reconcile、terminal no-revive 与 field-level
  single writer 闭合 OB-1.1/OB-10.2 decision；`human_transition_pending` 8-attempt due convergence 闭合 OB-4.1/
  OB-9.1 decision。exact boundary=`25 structural constraints/16 FKs + 40 local checks + 4 indexes/5 access paths +
  11 CAS methods`；combined future schema=`10 relations/77 constraints/45 FKs/15 indexes + 19 forward/18 rollback`。
  legacy `plan_json/gate_json/decision_json` 永不作 authority/backfill。此项只把 Plan §6 item 6/7 与上述 OB slice
  标成 `decision_locked_not_implemented`；author evidence only、fresh pinned non-author review pending，零 migration/
  repository/runtime/provider/live。R-019/R-023/R-027/R-028/R-029、action-root、其余 OB、served=0 均不变。
  Matching D3c2h1 + D3c2i pinned `GO` 前，no dormant migration is authorized。
- [x] Cohort CS1/CS2 foundation（Thinking Machines Lab live 前置）：versioned、registry-digest-pinned
  `CohortSelection` 已成为 request/plan-review/provider compiler 的唯一 owner；Researcher/Engineer/Product Manager
  等 role bucket 与 current/former 支持有序自由多选，`role_match=any|all`，用户显式选择优先于 raw text/model
  patch，unknown 值 fail closed。Harvest scalar status/role 已编译成独立有界 lanes 后确定性 merge/dedupe；全局
  budget、strict result envelope、all-role verifier、request/feedback/baseline/snapshot/authoritative projection reuse
  identity fence 已有 fast regression；该 foundation 本身不包含 runtime activation，后续状态见 CS3。
- [x] Cohort CS3 isolated non-live runtime/result foundation：`simulate|replay|scripted` workflow 由 server-owned typed
  capability 激活，stored capability-free plan manifest 先 exact-recompile/compare，再执行 deterministic Harvest lanes；combined
  rows 只适配到既有 durable `SearchSeedSnapshot`/candidate documents，并持久化 `cohort_execution_result.v1` 的
  selection/manifest/result digest、capability、lane/count audit。missing/forged/stale manifest 零 provider call；live、retrieval-only
  `run_job` 仍 fail closed；non-live `role_match=all` 绑定 exact `cohort_headline_role_classifier.v1`，只接受 central registry
  从 exact public headline 证明的 required roles；capability 现绑定 validated isolated runtime namespace + provider mode，connector
  在任何目录/cache/provider work 前重新推导 exact-match；Cohort raw cache 按 mode+runtime namespace 隔离并核 provenance，
  server-owned canonical LinkedIn URL 贯穿 snapshot/candidate/prefetch。新计划只发一个全 manifest task；legacy former task
  只能复用 exact committed full-manifest result。publication fixed-forward components=`17e1607` + `c6c0fcd` + `d7cb306`
  + `fd50a84` 的 Cohort path scope；persisted-state restore 必须重入 canonical loader，summary/candidate marker 任一侧
  缺失或不匹配均 fail closed，exact committed generation 对 legacy materializer immutable。
- [x] Cohort CS4 user interaction：frontend picker 默认关闭，旧请求不发送 `cohort_selection`；options/labels/order/default
  全部来自 public endpoint，role 与 current/former 可有序多选，initial submit、revision、history recovery、plan review
  exact round-trip。已提交 explicit cohort 在 review 锁定，legacy plan 可显式升级。author evidence：frontend build
  `84 modules`、contract `7 passed`、backend/options exact `5 passed + 3 subtests`、Playwright transport legacy omission +
  explicit exact payload green；fresh pinned review pending。
- [x] Cohort CS5 scripted service E2E：isolated migrated PG + real orchestrator/plan/compiler/acquisition/materialization/
  result projection 已贯通；双角色 `role_match=all` 执行 2 个 profile-search lanes + 1 个 profile enrichment，最终
  `completed/completed`，public asset population 为 1 个 current 候选，durable candidate/result audit 保留 exact lane、
  selection、manifest 与 versioned role proof。zero/all-rejected 在所有 canonical write 前 blocked；成功 publication 以
  candidate documents 的 exact digest 为最终 commit marker，partial summary/result 对 normal loader 不可读。全程 live Harvest
  submit hard-fail sentinel 覆盖 joined background work；public API/run projection/profile readiness、scripted-only invocations、
  contamination clean、queued→terminal reconcile 均有 assertion。mandatory-PG E2E=`1 passed`，fast=`85 + 69 subtests`，
  exact pipeline adjacency=`2`；planning adjacency=`164 + 69 subtests` 后唯一失败已在 clean `af4db41` 同节点复现。
  `60e7e67` 的有效 Ultra formal review=`NO-GO 0/6/4/0`；`04dd41c`/`d7cb306` pinned advisory 分别为
  `NO-GO 0/2/2/0`、`NO-GO 0/1/1/0`；当前 `fd50a84` Cohort path scope fixed-forward 尚待 fresh pinned non-author review，
  reviewer-exclusive `gpt-5.6-sol/ultra/priority` 后续因 `usageLimitExceeded` fail-closed，绝不写成 formal GO。
- [x] Cohort CS5a criteria-write provenance fixed-forward：`feedback`、`confidence-policy`、`recompile` 的
  `request|request_payload|metadata.request_payload` 统一由 external Cohort owner 验证并 exact canonical merge；caller
  `inferred|legacy_adapter`、malformed mirror 或 alias conflict 在 HTTP/orchestrator 两层均零写 fail-closed。feedback、
  confidence-policy、recompile 的显式 `job_id|baseline_job_id|source_job_id` 全部先 exact-owner preflight，再以 re-read
  stored job request/plan 作为唯一 request/signature owner；
  A-job+B-cohort、forged matching/signature、stored request/plan conflict 均在 feedback/compiler/result/derived-job 前拒绝。
  `rerun_retrieval` 只控制 rerun；no-ref 与 open-mode server-owned provenance 正向保持。该项修复首轮 advisory review
  的 criteria provenance/external-ingress findings。author evidence=`106 + 60 subtests`、adjacent=`50 + 66 subtests`、
  PG=`12`、Ruff/format/compile/diff green、global mypy=`81/4`；regression-matrix 唯一 inventory failure 已在 clean
  `48c43e1` identical reproduce。confidence-policy owner fixed-forward focused=`174 + 126 subtests`、PG owner adjacency=`13`；
  reviewer-exclusive app-server 已在 `thread/start` 验证 `gpt-5.6-sol/ultra/priority`，随后因 `usageLimitExceeded` fail-closed，
  故 fresh pinned formal review 仍 pending，不得据此宣称 formal GO。
- [ ] Cohort CS6 live checkpoint：runtime 必须把 manifest/lane identity 与 durable submission state + provider run id
  绑定，闭合 ambiguous-submit、resume、exact replay、terminal reuse，避免进程重启重复付费；再补 live-boundary
  partial failure/zero result/retry 与 cost audit。CS3/CS5 fresh pinned review 仍 pending；有效 GO 前不做 paid canary。
- [ ] Track D 落地后的首个 live canary：fake/scripted 与 scope-matched review gate 全绿后，仅使用 operator 外置、
  不入库/不落日志的凭据做 **Thinking Machines Lab-only** 有界尝试，目标为华人 pre-training researcher mapping；
  OpenAI/Anthropic/Google DeepMind/xAI/Meta 及全量扩展另行 gating，不与首轮 canary 合并。
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
