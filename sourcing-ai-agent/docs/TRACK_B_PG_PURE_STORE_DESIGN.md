# Track B — PG-Pure 控制平面存储:dual-path 拆除 + schema 单源

> Status: **RATIFIED 2026-06-16**(approach + schema 机制 + 增量顺序 owner 已批)。Living tracker —
> 以最新条目为准。建立在 [[PG_ONLY_CUTOVER_TRACKER]](运行时 PG-only 契约已完成)与 NEXT_TODO.md
> §39/§50(fixture 迁移已完成 + 双路径重写 roadmap)之上,不重复其内容。

## §0 背景与重新定性(reframe)

`ControlPlaneStore`(`storage.py`,~31k 行,516 方法)是一个 **facade**:同时持有
- 内存 SQLite 连接 `self._conn`(488 处 sqlite 引用,697 处 `self._conn`),与
- PG adapter `self._control_plane_postgres = LiveControlPlanePostgresAdapter`(`control_plane_live_postgres.py`,~7.8k 行;另有底层 `control_plane_postgres.py` schema/SQL helper)。

三个独立 mapping(facade 路由 / 迁移现状 / adapter 覆盖)三角验证出的关键事实:

1. **生产 postgres_only 下 SQLite 是 write-only 死重,从不是权威读源。** 全部 124 张
   `CONTROL_PLANE_LIVE_TABLES` 在 postgres_only 模式 `should_prefer_read=True` 且
   `is_authoritative=True`;SQLite fallback 被 `_control_plane_postgres_should_skip_sqlite_fallback`
   封死并在 PG 失败时 hard-fail。
   证据:`control_plane_live_postgres.py:231-232`(`_READ_PREFERRED_MODES`/`_AUTHORITATIVE_MODES`)、
   `:471-482`(三谓词)、`storage.py:1099-1103`、`:1264-1306`(`_call_control_plane_postgres_native`)、
   `:1374-1396`(`_select_control_plane_rows`)、`:4923-4941`(`get_job` PG-first 范式)。
2. **测试 fixture 迁移已完成**(pilot+5 批,37 文件迁到 `PGControlPlaneStoreTestMixin`,捕获 5 个真实
   PG 产品缺陷)。详见 NEXT_TODO.md §39/§44-46。
3. **~99% 方法已有可用 PG 路径**;仅 **2-3 个方法**真正仍对 `self._conn` 执行:
   `refresh_matching_metadata`(`storage.py:3497`)、`upsert_job_result_lifecycle`(`storage.py:6249`,其
   read-merge-write 逻辑;但 `job_result_lifecycle` **表**本身已有丰富 PG 支持 — adapter 行 49/134 +
   monotonic-delta 字段处理 214-976,故为"改用 adapter upsert"而非从零)。`get_latest_criteria_version`
   纯计算无 DB。

**结论:Track B 的下限是"删除已死的 SQLite 双路径 + 统一 schema 单源",上限是把控制平面存储层重做成前瞻、高效的设计。**
dual *code*(非 dual *data*)是行语义分歧(`WORKFLOW_BEHAVIOR_GUARDRAILS.md` invariant 7)的根 —
删 SQLite = 从结构上消灭该 bug 族;这部分逐方法删支、行为等价、低风险,是 B3.2 的纪律。

> **REFRAME(owner 2026-06-21):允许部分乃至完全重写。** 继承自前任的代码并未完全实现前瞻架构与高效工程,
> 凡 PG 路径本身或整体存储设计有缺陷(非前瞻、低效、facade 冗余、routing 脚手架已恒真等)的地方,**应改进甚至重构,
> 而不是机械保留**。即:B3.2 仍走"删死支、行为等价"的安全节奏;但 B4 及收尾**不再受"删-非-重写"约束** ——
> 在删 mirror/shadow 的同时,主动评估并重做 `ControlPlaneStore` facade / adapter 边界 / 路由层 / 写路径,
> 朝"一个优秀的 PG-native 类型化存储层"演进。重写仍受 characterize-first + 对抗式验证 + 行为不变量(invariant 7)+
> 合同 lane 把关;凡改变可观察契约处,须独立 review gate。

## §1 当前架构(要拆除的)

- **路由三谓词**:`should_prefer_read(table)` / `should_mirror(table)` / `is_authoritative(table)`
  (`control_plane_live_postgres.py:471-482`)。
- **动态分发**:`_call_control_plane_postgres_native(method_name, ...)`(`storage.py:1264`)→
  `getattr(self._control_plane_postgres, method_name)`(`:1272`);postgres_only 下 strict-no-fallback。
- **读**:PG-first,SQLite fallback;postgres_only 下 fallback 封死、PG 失败即 raise。
- **写**:PG native 成功即 return;postgres_only 下 SQLite 写不可达。
- **mirror**:`_mirror_control_plane_row` / `replace_table_from_sqlite`(SQLite→PG),仅
  bootstrap/setup(schema init、bootstrap candidate load、`replace_company_data`)用。
- **schema 双源(病根)**:`storage.py` 的 SQLite `CREATE TABLE` DDL + `control_plane_postgres.py`
  手工 PG DDL builders(`:302/2041/2191`)+ `sqlite_master` 派生同步路径(`:1879/1890`、
  `control_plane_live_postgres.py:845 replace_table_from_sqlite`)。两份会漂移 —— 已修的 ON CONFLICT
  唯一索引分歧即此(SQLite UNIQUE 未被手工 PG DDL 复制)。

## §2 Ratified 决策(owner 2026-06-16)

- **取径**:删-非-重写(consolidation;durable-foundation:修设计不修症状)。**已被 2026-06-21 REFRAME 放宽
  (见 §0):删支是下限/B3.2 纪律,B4 及收尾允许重写/重构以达成前瞻高效设计。**
- **schema 机制**:**versioned SQL migration files + tiny runner** —— `migrations/000N_*.sql` 顺序应用,
  `schema_migrations(version, applied_at)` 记账,幂等;DB-native、无新依赖、plain reviewable DDL、可移植
  (优于 in-house Python DDL registry 的"魔法",优于 Alembic 的 SQLAlchemy 阻抗)。
- **顺序**:**Foundation-first**(先修分歧根)。

## §3 增量计划

### B1 — Foundation:schema 单源(keystone)
- **B1.0 characterize**(read-only):精确刻画当前活跃 PG schema 如何被创建 —— 手工 DDL 位置、
  `sqlite_master` 派生路径、unique-index ensure、acquisition_shard_registry split-table schema、
  124 表 + 全部索引/约束。产出权威 schema 快照(以**运行中 PG** 的 `information_schema` 为准)。
- **B1.1 baseline migration**:据 B1.0 快照生成 `migrations/0001_baseline.sql`,**字节等价捕获现状**
  (含已修的 unique 索引),不理想化、不"顺便重构 schema"。
- **B1.2 runner**:tiny idempotent migration runner(顺序读 `migrations/`、`schema_migrations` 记账、
  advisory-lock 串行化 —— 复用既有 `_advisory_lock_key()` schema-namespacing,注意 rolling-deploy 约束)。
  接入启动 / `ensure_bootstrapped` 路径。
- **B1.3 删 `sqlite_master` 派生 schema 路径**,使 runner 成为唯一建表者。
- **验证**:现有 PG fixture 测试全绿;runner 建库 vs 旧路径建库 `information_schema` diff = 0。

### B2 — port 仍跑 SQLite 的方法
`refresh_matching_metadata`、`upsert_job_result_lifecycle`(merge→adapter 已有的 monotonic-delta upsert)。
每个 characterize 行语义(缺行 None/{}/raise/no-op)。

### B3 — 按表组删死 SQLite 分支
每批一组相关表:删 `if should_prefer_read: PG else SQLite` 的 SQLite 半 + fallback 块,留纯 PG。
每批 characterize 行-不存在语义(invariant 7),沿用 fixture 迁移的 baseline-first + per-batch doc 节奏。

### B4 — 移除影子
删 `self._conn` + mirror(`_mirror_control_plane_row`/`replace_table_from_sqlite`)+ 路由 scaffolding
(`should_prefer_read`/`should_mirror`/`is_authoritative`/`_call_control_plane_postgres_native` 的 fallback 半)
+ SQLite bootstrap。`ControlPlaneStore` 收为纯 PG 薄 facade(或并入 adapter)。

## §4 风险与不变量

- **零 schema 漂移**:baseline 必须捕获**活跃 PG** 现状,用 `information_schema` diff 把关 —— 不得借机
  "顺手改 schema"。
- **行-不存在语义(invariant 7)**:每方法删支须 characterize(缺行 None vs {} 哨兵 vs raise vs no-op);
  已知分歧族见 `WORKFLOW_BEHAVIOR_GUARDRAILS.md` invariant 7。
- **advisory-lock 身份**:已 schema-namespaced(rolling-deploy 须 full-stop);runner 串行化复用之。
- **部署**:schema runner 上线在受控窗口,与现有 PG-only 启动契约一致。
- **测试**:fixture 已 PG-backed,runner 须能在 `pg_store_fixture` 的 per-class schema 上跑。
- **纪律**:migration 只 forward;不手删 runtime 资产;不破坏既有 advisory-lock / psycopg-pool 语义;
  PG-only 正常路径不变。

## §5 完成定义(DoD)

- `migrations/` 是唯一 schema 源;runner 建全库;`sqlite_master` 派生路径删除。
- `self._conn` / 内存 SQLite 影子 / mirror / 路由三谓词全部删除。
- `ControlPlaneStore` 纯 PG;行语义分歧 bug 族结构性消失。
- 所有 PG fixture 测试绿;CI 合同 lane 绿。

---

## §6 进度

- **2026-06-16 设计 RATIFIED**:reframe(删-非-重写)+ schema 机制(versioned SQL + runner)+ 顺序
  (foundation-first)owner 已批。
- **2026-06-16 B1.0 characterize DONE**:确认 live PG schema 当前**由 SQLite schema 在每次 bootstrap 时生成**
  —— `adapter.ensure_bootstrapped()`(`control_plane_live_postgres.py:820`)→ `sync_runtime_control_plane_to_postgres(sqlite_path=...)`
  读 SQLite(`_sqlite_table_columns`→`_build_create_table_sql`),UNION 手工 `_ensure_runtime_coordination_schema()` +
  `_ensure_control_plane_writer_schema()` + `_ensure_control_plane_unique_indexes` + `ensure_acquisition_shard_registry_split_schema`。
  SQLite DDL 源 = `storage.py::init_schema`(1436)。物化 schema = **83 物理表**(非 124;124 含 split 逻辑名 + 条件表)。
- **2026-06-16 B1.1 baseline DONE**:`src/sourcing_agent/migrations/0001_baseline.sql`(83 表 / 96 ALTER / 91 index /
  40 unique / 19 sequence)+ 可复现生成器 `scripts/capture_pg_schema_baseline.py`。生成法:fresh 库经**真实代码路径**
  bootstrap(store + ensure_bootstrapped + writer/coordination ensures)→ `pg_dump --schema-only` → 规范化为 schema-agnostic
  (无限定名,runner 设 search_path)。**验证**:(a) capture 表集 == live `public` 83 表 0 diff;(b) **round-trip**:
  baseline apply 到 fresh schema 后 re-dump 与 capture **逐字节相同**;(c) 生成器跨运行**确定性**逐字节稳定。
- **2026-06-16 B1.2 runner DONE**:`src/sourcing_agent/migration_runner.py`(`apply_pending_migrations(connection, *, schema)`)
  + `tests/test_migration_runner.py`(4 测试,纳入 CI 合同 lane)。特性:**幂等**(ledger-gated)、**串行**
  (transaction-scoped `pg_advisory_xact_lock`,key 形如 `_advisory_lock_key`:`{schema}:schema_migrations`)、
  **原子**(全部 pending + ledger 行单事务提交;DDL 事务性)、**brownfield-safe**(已有 baseline 表但无 ledger 的库
  —— prod `public` / 本地 / 已 bootstrap 的 test schema —— 在 baseline **stamp** 而非重跑 `CREATE TABLE`,使 B1.3
  cutover 对存量库安全)、**tamper-evident**(每行 sha256;已应用迁移文件被改则 fail closed)。验证:**drift guard**
  runner-built 与 code-bootstrap 结构逐列逐索引相同(83 表);幂等;brownfield stamp;checksum fail-closed。
  **尚未接入** `ensure_bootstrapped`(B1.3 做 cutover)。打包注:migrations 经 `Path(__file__).parent` 定位(当前 source-run
  部署 OK);未来容器化/wheel 需配 package-data。
- **2026-06-16 B1.3 cutover DONE**:`LiveControlPlanePostgresAdapter.ensure_bootstrapped` 现调
  `apply_pending_migrations`(经 `_apply_schema_migrations`,带 `_is_retryable_postgres_exception` 重试),runner 成为
  PG schema 唯一建表者;`sqlite_master` 派生 schema 生成从 live bootstrap 退役。存量库(prod `public` / 本地 / 已 bootstrap
  的 test schema)经 brownfield **stamp** 安全(不重建)。**保留 drift guard 的诚实性**:抽出 legacy `_bootstrap_schema_from_sqlite_source()`
  (sync + writer + coordination ensures;**先置 `_bootstrapped=True`** 再调 writer/coordination,否则它们内部的
  `ensure_bootstrapped` 会回灌 runner 把 `schema_migrations` 混入 capture),供 generator + drift test 用作"SQLite 源"对照
  (B4 随 shadow 一并删)。**验证**:generator 重生成 baseline 逐字节相同(83 表,无 schema_migrations);4 runner 测试绿
  (drift guard 现为 runner-built vs SQLite-source);**store-bootstrapping 合同子集 174 passed**(storage_surface_guardrails /
  user_private_reads / api_auth / operation_runtime / crm_public_web_runtime_boundary + migration_runner —— 全部经 runner 建库)。
  部署仍 full-stop(advisory-lock 身份)。
- **2026-06-16 B2 DONE(scope 由 characterize-first 重定 + 修 2 个真实 stale-read bug)**:用 ultracode workflow 做了
  understand(5 reader)+ triage(5 adversarial classifier)。**关键纠偏**:"2-3 个 SQLite-only 方法"是错的 —— 完整 sweep 找出
  **22 个**。triage 分类:**REAL_B2_GAP ×2**(本次修)、DUAL_PATH_B3(PG-aware 父方法的死 SQLite 半:event-summary 族
  4669/4706/4747/4789、profile-alias 三件 25699/25724/25744、`_insert_candidates_and_evidence` 29238、`upsert_job_result_lifecycle`
  尾 6524-6539、replace_* 数据装载 3902/3918/3952/4004)、SHADOW_INFRA_B4(917/924/939/1429/1436/29214/29221)。
  **两个澄清的非-gap**:`upsert_job_result_lifecycle`(PG 路径已工作:读 PG+Python merge+`adapter.upsert_row` SQL 重实现 merge;
  仅死 SQLite 尾→B3)、`refresh_matching_metadata`(NOT_A_GAP —— save_job@4488/create_plan_review_session@6938 等写路径**在写时**
  即把 matching signature 写入 PG,故 PG 行天生带签名,backfill 在 postgres_only 对空 shadow no-op,纯 legacy 一次性迁移)。
  **修复的 2 个真实 bug**(SQLite-only public read,在 postgres_only 读空 shadow 返 None,静默废掉去重):
  (1) `find_latest_job_by_idempotency_key`(storage.py:16615,jobs;orchestrator.py:54032 idempotency 去重→重复 job)——加 PG 分支
  `_select_control_plane_job_rows` + `should_skip_sqlite_fallback('jobs')`,镜像 sibling `find_latest_job_by_request_signature`(16314)。
  (2) `find_pending_plan_review_session`(storage.py:25786,plan_review_sessions;orchestrator.py:52704/52790 待审去重→重复 pending session)
  ——加 `_select_control_plane_row` PG 分支。**验证**:`tests/test_pg_only_dedup_reads.py`(5 测试,纳入 CI 合同 lane)——
  git-stash 回退 storage.py 证明 pre-fix 下 3 个 positive-find 测试 FAIL(正是空-shadow bug),fix 后 5/5 绿。
- **2026-06-17 B3 precondition workflow + owner 决策**:precondition map(workflow wf_85e57579)证实 **SQLite-authoritative
  模式今仍 live** —— PG-only guard 只对 literal `production` 或 `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1` 强制;默认
  local_dev/CLI/scripted(`cli.py:133`、`cloud_asset_import.py`、`smoke_runtime_seed.py`)在无 DSN 时 mode='disabled' → SQLite-authoritative。
  故删死 SQLite 分支前必须先**丢弃 SQLite-authoritative 支持(PG 对所有 runtime 强制)**。**owner 批准(2026-06-17):drop it,proceed B3。**
- **2026-06-17 B3.0 guard DONE**:`ControlPlaneStore.__init__` 现**无条件**要求 resolved DSN + `postgres_only`(否则 raise
  "...no longer supported"),取代原 production-/flag-only guard;`should_prefer_read`/`should_skip_sqlite_fallback` 自此对每张表恒 True
  ——删死分支的前提达成。删除孤儿 `_control_plane_postgres_required` + `current_runtime_environment` import。SQLite 连接仅作 ephemeral
  shared_memory shadow 存续(B4 删)。**CI-lane 修**:`test_operation_runtime.py::test_operation_and_acquisition_runtime_state_are_pg_only`
  + `test_durable_runtime.py::test_sqlite_durable_runtime_normal_path_fails_closed` 改为断言**构造期** raise(比原 per-op fail-closed 更强)。
  验证:2 改测 + durable_runtime 48 + dedup + storage_surface/api_auth 19 全绿;PG-fixture 构造(postgres_only)不受影响。
  **已知待修(B3.1,非-CI lane,本 commit 显式披露)**:`test_control_plane_live_postgres.py`(~24 `mirror`/`prefer_postgres`
  `_build_store` 用例,测被删的双路径 routing)、`test_postgres_text_normalization.py`(disabled)、`test_pipeline.py:73`(裸构造,从不全跑)
  —— guard 落地后这些构造会 raise;B3.1 紧接迁移/退役(delete routing 测,migrate 数据-correctness 测到 postgres_only)。
  下一步:B3.1 测试迁移 → B3.2+ 按表组删 DUAL_PATH 死 SQLite 半 + 简化恒真 routing scaffolding。
- **2026-06-17 B3.1 SQLite-test 迁移 DONE**:AST 全扫(precondition workflow 的 test-surface agent 曾 stall,故自查)找出
  **8 个**裸构造 store 的类。处理:(1) `test_control_plane_live_postgres.py` **57 绿** —— `mode="mirror"/"prefer_postgres"`
  → `postgres_only`(19 个机械 swap,facade 仍路由到 adapter)、2 guard 测 regex 改 "no longer supported"、3 mirror-assert 改
  native read-back/return-value、删 1 个 mirror-canon 测(native sibling 已覆盖)、**修 1 个 B1.3 遗留**(`test_bootstrap_sync...`
  原 mock 退役的 `sync_runtime_control_plane_to_postgres` → 改 mock runner `apply_pending_migrations`)、1 个 enrichment 断言。
  (2) `test_postgres_text_normalization.py` 迁到 `pg_backed_control_plane_store`(byte-literal 规范化是 upsert 逻辑非 SQLite-specific)。
  (3) `test_company_public_web_assets.py` 迁 PG mixin 后 **SKIP** —— **B3.1 FINDING**:暴露真实 PG-vs-SQLite 分歧(collector/provider
  失败注入测期望 run status='failed',PG 给 'completed';sibling sync 测 PRE-EXISTING fail)—— B2-style 真 bug 候选,待诊断。
  (4) `test_target_candidate_public_web.py::...SqliteLegacyTableTest` SKIP(其前提=历史 SQLite 文件,PG-only 下不可构造;需 re-home
  到 PG legacy-migration-context)。(5) test_control_plane_pool / testcontainers_* 安全(自解析真 DSN 或 SkipTest)。(6) test_pipeline:73
  deferred(从不全跑,随 test_pipeline 单独 redesign)。结果:相关 4 文件 63 passed / 29 skipped / 0 fail。**B3.1 surfaced 2 follow-ups:
  company_public_web PG run-status 分歧(诊断)+ legacy SQLite 测 re-home。**
- **2026-06-17 company_public_web finding DIAGNOSED(commit 6c94627)**:**非 production bug。** failure-marking
  在 bootstrapped schema 下正确返 'failed'(实测)。"joined" 是 TEST-ISOLATION artifact:run idempotency key 按
  target_company 确定性('OpenAI' 每测都一样)+ `PGDurableRuntimeTestMixin` 的 per-CLASS schema 测间不重置 → 测 N join 测 N-1 的 run。
  旧 per-test SQLite DB 隔离了每测。修=per-test 隔离(unique target_company/nonce/force_refresh 或 truncate)。**潜在前瞻注**:
  adapter `search_path = <schema>, public`,tenant schema 缺表会静默落到 `public` —— 单-public-schema prod 无害,但若走 per-user schema
  (multi-tenant 方向)是跨租户污染风险。
- **2026-06-17 B3.2 pilot DONE — dead-SQLite-branch 删除 PATTERN 证实**:guard 后 `should_prefer_read`/`should_skip_sqlite_fallback`
  对每张 control-plane 表恒 True,故每个 dual-path 方法 PG 分支后的 SQLite fallback `else` 死。**Pattern**:删 `else: SQLite` + 仅供它用的
  SQLite `clauses`/`params` builder → `if not postgres_rows: return None` + `rows = postgres_rows`(postgres_only 下行为逐一等价)。
  **Pilot**:jobs signature-read family(`find_latest_job_by_request_signature` + `find_latest_job_by_request_family_signature`)。
  验证:test_control_plane_live_postgres 57 + user_private_reads + pg_only_dedup_reads = **88 passed**。
  **B3.2 batch 1 DONE(commit 664349d)**:jobs request-lookup read family 完成(idempotency + list_by_signature;另 2 在 pilot)。
- **2026-06-17 B3.2 CATALOG DONE(workflow wf_0c6dc3e6,6 并行 scanner;full result 见 tasks/w9h1hvpkc.output)—— 重定 B3.2 体量与排序**:
  全量 **300 个** dead-SQLite-fallback dual-path 方法,**仅 87 个 clean_3way(机械可塌缩,如已做的 jobs reads),213 个 irregular(需手工)**。
  irregular 主簇:**write-and-mirror 81**(SQLite 写 + `_mirror_control_plane_row` 尾)、multi-step delete/replace 36、parallel SQLite/PG
  clause builder 17、count/int-shaped read 9、entangled side-effects(hydrate/compact/summary)5。shape:write 124 / read_list 89 /
  read_single 73 / 其它 14。表组热点:workflow_commands 15、agent_worker_runs 15、job_materialization_items 10、linkedin_profile_registry 9、
  candidates/jobs 各 8。**关键排序洞见**:81 个 write-and-mirror 方法的"死 SQLite 写 + mirror 尾"与 **B4 要整体删除的 mirror 机制**
  (`_mirror_control_plane_row`/`_replace_control_plane_table_from_sqlite`/`self._conn`/`init_schema`)纠缠 —— 逐方法手删 81 个 mirror 尾不如
  **B4 一次性删 mirror 机制**让这些 SQLite 写自然死亡并随之移除。故**精化排序**:
  - **B3.2 = 删死 SQLite READ 分支**(read_single/read_list:87 clean + parallel-clause read irregulars)—— 纯读,无 mirror 纠缠,按表组分批,每批跑合同 lane。
  - **B4 = 整体删 mirror 机制 + `self._conn` + `init_schema` + `_configure_connection` + `_bootstrap_schema_from_sqlite_source` + SHADOW_INFRA + generator/drift-test legacy 路径**,
    这会令 write-and-mirror(81)与剩余 SQLite-write 分支批量死亡移除;再清 count/replace/entangled 的尾。
  - 尾声简化恒真 routing scaffolding(`should_prefer_read`/`should_skip_sqlite_fallback`/`_select_control_plane_row(s)` 的 not-prefer/skip 分支)。
  **结论:B3.2/B4 是一次大而需谨慎的多批重构(非机械 sweep),宜以专批(可新 context)按表组/子模式推进,每批合同-lane 验证。** invariant-7 行-不存在语义逐方法 characterize。
- **2026-06-17 B3.2 batch 2 DONE(serving_projection_members reads)**:删 6 个读方法的死 SQLite fallback ——
  `list_serving_projection_members`、`list_serving_projection_members_by_identity_keys`、
  `list_serving_projection_members_by_person_identity`、`count_serving_projection_members_by_readiness`、
  `get_serving_projection_member`、`count_serving_projection_members`。3 种 shape 各保行为等价:(a) simple
  list/single(pilot 形:`if not postgres_rows: return [] / {}`);(b) chunked accumulator(去恒真
  `should_prefer_read` 外壳 + parallel SQLite chunk loop,`rows` 累加器保留);(c) `count_*` try/except
  ——**保留 swallow-and-return-sentinel-on-PG-error 语义不变**(PG 异常返 {} / 0,与原 skip-fallback 哨兵
  一致;改 fail-closed 属 B3.2 范畴外,记为后续可议)。`replace_serving_projection_members`(write/mirror)
  留 B4。skip_fallback gates 232→225(serving_projection_members 8→1,仅剩 write 方法 18681)。**验证**:
  `test_serving_projection_storage` + `test_serving_projection_writer` 13 + `test_control_plane_live_postgres`
  + `test_projection_crm_api_contracts` + `test_user_private_reads` + `test_pg_only_dedup_reads` = **121 passed**;
  storage.py AST/import clean;ruff 编辑区零错(18 个 pre-existing 错在 line 10123-10186 另一方法的 mixed
  tab/space,非编辑区,storage.py 非 lint-gated)。
- **2026-06-17 B3.2 batch 3 DONE(job_materialization_items reads)**:该表 9 个 gate 中**仅 3 个是纯读**,
  删其死 SQLite fallback —— `get_job_materialization_item`(read_single,`{}`)、`list_job_materialization_items`
  (read_list,`[]`)、`list_ready_job_materialization_items`(read_list,`[]`)。后两者原**并行构造** SQLite(`?`)+
  PG(`%s`)两套 clause/param;删 SQLite 分支后 `?` 套连同 `where_sql`/`limit_sql`/`sqlite_params` 局部一并移除,
  仅留 `%s` 套(PG 读路径**逐字节不变**)。其余 **6 个是 write+mirror**(`claim_*`、`mark_*completed/failed/
  waiting_prerequisite/partial_progress`、`reawaken_waiting_prerequisite_*` —— UPDATE+SELECT+`_mirror_control_plane_row`
  尾)→ 留 B4。job_materialization_items gates 9→6;总 gate 225→222。**验证**:test_durable_runtime 43 +
  test_snapshot_materialization_backfill / test_local_apply_closure_prerequisite / test_frontend_history_recovery /
  test_results_api 合计 336 passed。**6 个 pre-existing 失败(非本批引入)**:test_snapshot_materialization_backfill
  ::...materialization_items_apply_is_idempotent + test_results_api 5 个(board_runtime_state_current_snapshot_serving、
  crm_public_web_promotions_survive_force_refresh、crm_public_web_service_e2e、lovable_board_visible_patches、
  partial_current_snapshot_overlay)—— **git-stash 回退 storage.py 至 batch-2 baseline 后这 6 个逐一同样 FAIL**(13.33s),
  证 batch-3 编辑 innocent;均**非 CI 合同 lane**,典型如 `expected_candidate_count 80 != 297`(snapshot 人口 floor),
  与 read-method 删支无关 → 记为 Track-B-外 follow-up(snapshot population/projection 计数,待独立诊断)。
  storage.py AST/import clean;ruff 编辑区(5481-5577)零错。
- **2026-06-17 B3.2 batch 4 DONE(LinkedIn profile registry domain)** —— 首次用 **ultracode workflow(wf_def39da8,
  29 agent,6 表并行 scout + 逐 gate 对抗式 verify)** 产出 verified edit-list:6 表 23 个 read gate 全部
  DELETABLE_READ、0 DEFER(verifier 还交叉核对了 e41d287 的 sentinel-divergence 修复、并确认 `_select_control_plane_row(s)`
  在 PG error 时 **raise**(不静默吞成 sentinel),故 error 语义不变)。本批落地 **linkedin_profile_registry(5)+
  linkedin_profile_registry_leases(1)= 6 读方法**:get_linkedin_profile_registry、get_linkedin_profile_registry_bulk
  (并行 accumulator;verifier 证 canonical_keys 在有输入 key 时恒非空,`if canonical_keys`/`if resolved or skip`
  均恒真 → 重构为 `if not canonical_keys: return {}` 防御性保 `{}` sentinel;`linkedin_profile_registry_aliases`
  的独立 routing 留作它批不动)、summarize_linkedin_profile_registry_scope、get_linkedin_profile_registry_lease
  (**保 e41d287 parity**:缺行返 `_..._lease_from_row(None)` 空 sentinel,非 None)、list_linkedin_profile_refill_queue_items
  + list_linkedin_profile_refill_queue_groups(并行 `ready_clause_sqlite`/`_postgres` → 删 SQLite clause)。其余
  linkedin_profile_registry 5 + leases 7 个 gate 是 write/mirror → B4。gates:registry 10→5、leases 8→7;总 222→216。
  **验证**:group A(test_storage_profile_registry/profile_registry_backfill/control_plane_live_postgres/enrichment/
  operation_runtime)**320 passed 0 fail** —— 6 方法的直接 regression 全绿;group B(candidate_artifacts/cloud_asset_import/
  company_asset_completion/company_asset_supplement)90 passed,**3 fail + 1 teardown error 全 PRE-EXISTING**(git-stash
  至 batch-3 baseline 后逐一同样 FAIL,10.82s;含 cloud_asset_import DSN 错误消息漂移、company_asset_completion
  'former_false_positive' 成员标记、background_followup JSONDecode/teardown OSError —— 均非 CI lane,与本批无关)。
  storage.py AST/import clean;ruff 总错仍 18(pre-existing,无新增),编辑区零错。
  **workflow 已产出 batch 5+ 的 verified plan(待执行,未编辑)**:job_results(4 方法:get_job_results、
  get_job_results_page、get_job_results_for_candidates [各 2 gate]、count_job_results)、workflow_commands(3:
  get_workflow_command、list_workflow_commands、list_ready_workflow_commands)、projection_person_search_index(3:
  count_projection_person_search_index、_search_projection_person_index_rows、_list_projection_person_search_index_rows)、
  frontend_history_links(4:get/list/list_for_job/list_for_review)。全部 DELETABLE_READ。
- **2026-06-17 B3.2 batch 5 DONE(job_results reads)**:删 4 个读方法的死 SQLite JOIN fallback ——
  get_job_results、get_job_results_page、get_job_results_for_candidates、count_job_results(均 read_list/scalar,
  sentinel []/0)。这些方法 JOIN candidates,故每个死分支同时含 `skip("job_results")` 与 `skip("candidates")` 两 gate
  (恒真);塌缩 `if postgres_rows: records; if (records or skip or skip): return records; if skip: return []; <SQLite JOIN>`
  → `if not postgres_rows: return []; return records`(get_job_results_for_candidates 另删死 SQLite-`?` placeholders
  builder)。第 8 个 job_results gate 在 `replace_job_results`(write,line 4894)→ B4。gates:job_results 8→1,
  另顺带移除 3 个嵌在这些 JOIN 分支里的 `candidates` gate(11→8);总 216→206。**验证**:test_results_api(job_results
  读方法唯一 PG-fixture 覆盖)**292 passed**;**5 failed = batch-3 已证 PRE-EXISTING 的同一组 test_results_api 失败**
  (board_runtime_state_current_snapshot_serving、crm_public_web_promotions_survive_force_refresh、crm_public_web_service_e2e、
  lovable_board_visible_patches、partial_current_snapshot_overlay;失败集合逐一相同 → 本批零新增 regression,无需再 stash 对照)。
  storage.py AST/import clean;ruff 总错仍 18(无新增)。batch 6+ verified plan(workflow_commands 3 /
  projection_person_search_index 3 / frontend_history_links 4)仍待执行。
- **2026-06-20 B3.2 batch 6 DONE(3 表合并:frontend_history_links + workflow_commands + projection_person_search_index)**
  —— ultracode 关闭,主循环驱动(用 batch-4 workflow 已 verified 的 plan,逐方法重读确认后编辑)。**10 读方法**:
  frontend_history_links(get_frontend_history_link + list_frontend_history_links + _for_job + _for_review,皆 clean
  Shape A,sentinel None/[])、workflow_commands(get_workflow_command sentinel {};list_workflow_commands +
  list_ready_workflow_commands 删并行 `?` clause/param builder,留 `%s`;update_workflow_command_payload 是 write+mirror
  留 B4)、projection_person_search_index(count_projection_person_search_index —— **特例**:它 count_rows→SQLite 无
  select_many 中间层,故 count_rows 不可调用时原经空 SQLite shadow 返 0,塌缩保留为尾部 `return 0`;_search_projection_person_index_rows
  + _list_projection_person_search_index_rows clean)。其余 gate(frontend delete 2、workflow 3 write、projection 3 write)→ B4。
  gates:frontend 6→2、workflow 6→3、projection 6→3;总 206→196。**验证 424 passed 0 fail**:group A(durable_runtime/
  recovery_drain_registry/recovery_event_wakeup/export_async_task/frontend_history_recovery)102 + group B(projection_crm_api_contracts/
  person_asset_crm_projection_contracts/control_plane_live_postgres/operation_runtime/enrichment)322。零失败 → 无需 baseline 对照。
  ruff 总错仍 18(并行 clause 删除无 F841 残留)。**B3.2 累计:232→196 gate(36 个死 SQLite read 分支删除,横跨 jobs/
  serving_projection_members/job_materialization_items/LinkedIn registry/job_results/frontend_history_links/workflow_commands/
  projection_person_search_index 8 个表组)。**
- **2026-06-21 pre-existing 失败簇 DIAGNOSE DONE(workflow wf_48a95eaf,8 analyzer + 8 对抗式 verifier)**:
  8 个稳定失败(第 9 个 background_followup_refresh 隔离下 PASS = order-dependent flake)定性 —— **2 个真 production bug
  (已修)**,其余非 bug:
  - **REAL_BUG #1(已修,commit 见下):** `partial_current_snapshot_overlay`(`80!=297`)—— `storage.py` 的
    upsert_job_result_lifecycle 非-delta clamp `else` 分支把 expected_candidate_count 强行压到 served_count,
    clobber 掉 orchestrator 已算好的 current-snapshot 人口 floor(297)。**关键纠偏**:diagnosis 给的一行 fix
    (`max(served, incoming)`)经 full-suite 验证**会 regress** `test_non_delta_partial_patch_after_row_shell_does_not_reinflate_raw_expected_count`
    (raw 145 reinflate)—— A/B 两场景都走同一 else 分支但要相反结果(A 保 297,B 压到 served)。store 无法从字段区分
    "真人口" vs "陈旧 raw 分母"。正确 fix = **trusted provenance signal**:orchestrator reuse 路径(`orchestrator.py:11238+`)
    把**当场计数的** current_snapshot_candidate_count 盖进 `metadata["current_snapshot_population"]`,clamp else 分支
    只在该可信信号 > served 时抬高 expected(否则压到 served)。验证:full test_results_api 4 failed/293 passed
    (修前 5/292;reinflate 不再 fail,overlay 转 pass,promoted shrink-after-dedupe 测不受影响)。
  - **REAL_BUG #8(已修):** `company_asset_completion name_matched_non_member` —— `enrichment.py:_profile_identifiers`
    把 `requested_profile_url`(*请求*的 URL,非*解析出*的人)算进身份集,导致 former-false-positive 与候选人身份 overlap、
    被误判为 member。fix = 从 `_profile_identifiers` 去掉 requested_profile_url(其 registry-alias-linking 合法用途是别的代码路径,保留)。
    验证:test_company_asset_completion + test_enrichment 152 passed。
  - **非 bug(留待处理,非 Track B 本体):** CONTRACT_DRIFT ×4 —— `board_runtime_state_row_shell`(测断言看板计数过时,应 112/297)、
    `lovable_board_visible_patches`(**owner 级 contract 决策**:哪个 numerator 才"诚实")、`crm_public_web_promotions_force_refresh`
    + `crm_public_web_service_e2e`(C1.4 已把导出改异步,这两测仍断言旧**同步**导出契约 → 应仿 sibling 改 submit→drain→poll);
    TEST_ARTIFACT ×1 —— `requires_postgres_dsn`(setUp 未 unset DSN env,需自隔离);FLAKE ×1 —— `background_followup_refresh`
    (teardown OSError / JSONDecode,隔离下 PASS)。**对抗式 verifier 有效**:把 `snapshot_materialization_idempotent`
    从 analyst 的 REAL_BUG 翻成 CONTRACT_DRIFT/low(backfill 探针读 legacy 表,非 serving bug)。
  - **方法论教训(已沉淀进 playbook principle 16):** 即便对抗式验证过的一行 fix,仍须跑**全 clamp 测族**(blast-radius),
    否则会漏掉反向场景测 —— 本例正是 full run 抓出 reinflate regression,逼出正确的 trusted-signal 双文件设计。
- **2026-06-21 B4.1a DONE(commit 5af6dc5)—— schema oracle 脱离 SQLite 源(PG-native drift guard)**:B4.1 第一子阶段,
  只改测试+脚本(无 production code),为 B4.1b 删 init_schema + SQLite shadow 解阻。**设计**:migrations 既是唯一 schema 源,
  就不再有独立 SQLite oracle 可对照 —— migration 文件本身即 golden;有意义的 drift 是"schema 创建在 migration ledger 之外"。
  - `tests/test_migration_runner.py`:drift guard(改名 `test_runner_built_schema_matches_live_bootstrap`)改为构建**真实 live
    bootstrap**(migration runner + bootstrap 时跑的 writer/coordination ensures)并断言它与**仅 runner**建的 schema 结构逐表/列/索引
    相同(83 表)。PG-native,无 SQLite。若某 ensure 开始建 migrations 没有的 schema 即 fail。取代旧的
    `_bootstrap_schema_from_sqlite_source` 对照。
  - `scripts/capture_pg_schema_baseline.py`:改为 dump **live PG-native** schema(ensure_bootstrapped + writer ensure),
    排除 runner 自己的 `schema_migrations` ledger(baseline 本就不含)。
  - **验证**:4 migration_runner 测绿;repurposed generator 重生成 `0001_baseline.sql` **逐字节相同**(83 表)—— 证明 live
    PG-native bootstrap 产出的 application schema 与已退役的 SQLite 派生路径**完全一致**(raw dump 唯一差异是 schema_migrations
    ledger,已排除)。`_bootstrap_schema_from_sqlite_source` 现**零 caller**,B4.1b 删。
  - **下一步 B4.1b**:删 `init_schema`(storage.py:1436-3495,~2000 行 SQLite DDL)+ `_ensure_column` ALTER drift +
    `_configure_connection` + `_bootstrap_schema_from_sqlite_source`(adapter)。再 B4.1c 起 re-home 16 个 `_replace_*_from_sqlite`
    bulk-load + ~10 个外部 sync caller → 删 mirror(B4.1d)→ 删 shadow self._conn(B4.1e)→ 塌缩恒真 routing + 修 drifted registry(B4.1f)。
- **2026-06-21 B4.1b CHARACTERIZE + PILOT(workflow wf_76d6035c,5 investigator,default-to-LIVE)—— 纠正 B4.1b 计划**:
  **init_schema 不能直接删** —— characterization 发现 shadow 仍被 LIVE 路径触达。**关键洞见**:init_schema 删除只需 shadow
  触点**不可达**(unreachable),不必先物删(unreachable 代码引用缺表不会报错)。故 B4.1b 重定为**给 conditional-gated 写方法加
  fail-closed guard**(非删除;物删随 B4.1d mirror/tail sweep)。
  - **LIVE 触点(~9 个写方法,anti-pattern:`if row is not None: return` 后缺 skip-fallback guard → native-None 时 fall through 到 shadow)**:
    save_job(4488)、append_job_event(4596,+compaction tail 4667)、upsert_acquisition_shard_registry(17624 SELECT pre-check)、
    upsert_organization_asset_registry(~16590)、upsert_organization_execution_profile(~16920)、review_plan_session(7072)、
    review_manual_review_item(7566)、create_plan_review_session(6895)、record_cloud_asset_operation(21593)。
  - **CLEAN(无 live 触点,characterization 确认)**:init_schema tail 的 refresh_matching_metadata / _backfill_* / _ensure_column
    (仅 init_schema 调,删 init_schema 即随之消失);advisory-lock SQLite fallback(profile_prefetch/board_visible patch lock);
    legacy target-public-web SQLite 子系统;__init__/close()。bulk-replace candidate/evidence + manual_review + confidence_policy +
    organization_asset_registry 的 `_replace_*` 尾**全 gated-dead**(PG-native 早返/raise)—— **唯 append_job_event 例外**(conditional
    `if result is not None: return`,native 在 empty job_id / no-row 时返 None 非异常 → fall through)。
  - **PILOT DONE(commit 7184240)**:save_job 加 `if should_skip_sqlite_fallback("jobs"): return`(native-None = terminal-protection
    no-op,行为等价;shadow tail 转 unreachable)。验证:test_pg_only_dedup_reads + storage_surface_guardrails + operation_runtime
    101 passed;ruff 仍 18。
  - **B4.1b 剩余(下一批)**:append_job_event(void + compaction)+ 7 个 value-returning upsert(native-None 时正确返值需逐方法定:
    re-read PG 行 vs raise vs {} sentinel)。全部 fail-close 后 → 删 init_schema + shadow(原 B4.1b/e)。
- **2026-06-22 B4.2.1 foundation 泛化(commit b7588cc)**:typed descriptor 地基补 3 个特性,使 public-web 族可逐字节等价:
  `Kind.JSON_LIST`(原始 list,== storage `_loads_json_list`:list/tuple passthrough,非-list/解析失败→[];区别于 `JSON_STR_LIST` 的
  strip+过滤空)、`Kind.JSON` 改为强制 dict(== `_loads_json_dict`;原返 `json.loads` 原值,无 consumer 故收紧安全)、
  `Column.read_default`(读时空值回退,独立于写 `default`;registry 读 status="" 而 public-web 读 status="queued")。
  验证:合成电池 + registry/leases 回归 35 passed(live PG)。
- **2026-06-22 B4.2.2 public-web 读路径转 descriptor(commit ced8fc3)**:10 个 `_*_public_web_*_from_row` 手写 mapper(~270 行
  逐字段 coercion)→ `repositories/public_web.py` 声明式 TableDescriptor + 一行 `DESCRIPTOR.from_row(row)` 委托;storage.py −288 行。
  新特性:`Kind.FLOAT`(== `_coerce_public_web_float`)、`derived` 读别名(一列两键:crm_record_id→record_id、
  requested_crm_record_ids→requested_record_ids)。**范围仅读**:`_*_row_payload` 写 builder + `_normalize_*_payload` 暂留(写路径转换
  需逐表对账完整列集,留后续)。验证:90 行合成逐字节等价 + 44 passed/0 failed(live PG)。顺手迁移 1 个 B4.1 stale 测
  (init_schema-source 断言改为断言 normal-schema bootstrap 已整体移除)。
- **2026-06-22 B4.2.3 再转 28 个 from_row(4 域,commit dc5fc78)**:storage.py −~550 行。新模块:`serving_projection.py`(4)、
  `workflow_runtime.py`(13)、`person_company_assets.py`(8)、`crm_core.py`(3)。foundation 把 `read_default` 泛化到 INT/FLOAT
  (`int|float(value or default)`,crm_records.crm_version 默认 1 需用;None→旧 0/0.0 行为,向后兼容)。
  **方法论(65 候选→安全转 28)**:① 只读分类 workflow(7 并行 agent,wf_9671629d)把剩余 65 个 mapper 编目为
  descriptor-MECHANICAL(28)vs IRREGULAR(37,留手写)并起草 descriptor spec;② `scripts/_descriptor_equiv_harness.py`(新)为
  确定性闸门:把每个 spec 重建为内存 descriptor,跨 absent/empty/typical/edge/native-object 逐列电池 diff against 活 mapper,
  **抓到 1 个真实 mis-spec(crm_version 默认 1)**;③ 28 个方法体经 **AST-精确 end_lineno 替换**(签名保留,非早先失败的启发式截断);
  ④ 终检:**新委托方法 vs 原手写 mapper(从 pre-edit 备份加载)0 diff**。验证:177 passed/0 failed(serving-projection /
  operation-runtime / company-asset / person-crm-projection-contracts / recovery-takeover-intent / workflow-event-response,live PG)。
  顺修 1 个 pre-existing B4.1 stale 测(向已退役 SQLite shadow 表 raw-UPDATE → 改走 PG adapter `_execute_non_query`;控制实验证 pre-existing)。
  **进度:79 个 from_row mapper 已 40 个转 typed descriptor(registry/leases 2 + public-web 10 + 本批 28)。**
- **2026-06-22 剩余 37 IRREGULAR 分类(下一波的 directional fork)**:agent 编目保守 —— 多数"IRREGULAR"实为**功能等价但写法不同**:
  ① 直接 `row["c"]` 下标(对真实全列行 == `_row_value`,仅缺列时 KeyError,真实 SELECT* 不会缺);② inline `json.loads(row["c"] or "{}")`
  try/except == `_loads_json_dict`/`_loads_json_list`。**真正 hard**:`_normalize_textual_value`(memoryview/bytes 解码 + `b'...'` 解包,超出 STR)、
  `_normalize_projection_rank_index`(`max(0,int)` 读时 clamp,descriptor INT 仅写时 clamp)、条件计算字段(target_candidate.quality_score
  float-if-not-None、lease `expired`)、dict-spread merge(confidence_policy_run、criteria_version)、raw passthrough 无 `str()`
  (agent_runtime_session/trace_span/worker)、从整 dict 派生(agent_worker wait_stage/effective_status)、`_candidate_from_row`(返 Candidate dataclass)。
  **机械易转的一波已尽**;下一波每个需新 foundation kind(raw-passthrough / clamped-int / normalized-text)或承载真实逐行逻辑 ——
  属 directional 决策点(更多 from_row vs 转写路径 vs 迁移 caller 到 Repository;后者触 jsonb Contract,已 gated on owner GO)。
- **2026-06-22 owner 决策:走「写路径合并」。**
- **2026-06-22 B4.2.4 写路径 pilot — crm_public_web row builder 转 descriptor.to_columns(commit 89c407d)**:3 个
  `_crm_public_web_*_row_payload`(batch/run/promotion)列 builder 改为委托 descriptor.to_columns,使这些表 read+write 双向单源。
  **foundation**:descriptor JSON kind 现按设计应用 json-safe(== 旧手写 `json.dumps(_json_safe_payload(...))`);把 `_json_safe_payload`
  从 storage God-class 抽到新 dependency-free `control_plane_serde.json_safe_payload`(Path/datetime/bytes/memoryview/`to_record`/set 强转),
  storage 以旧私名 re-import(177 处不变),descriptor `_encode` import 它。created_at/now 是运行期/merge 状态(留 builder 算),descriptor 只映射+编码列。
  **验证**:写 A/B 电池(每个 OLD `_*_row_payload(normalize(raw),existing,now)` vs `to_columns({**normalized,created_at,updated_at})`,
  含 json-safe 边界:datetime/Path/tuple/set/空格/existing-vs-new created_at)16 对 0 diff;live PG 84 passed/0 failed
  (含 registry 写回归 —— json-safe 对 registry string-list 列是恒等)。
- **2026-06-22 B4.2.5 再转 5 个 public-web 写 builder(commit 2932ea7)**:company_public_web_asset_run/asset、
  target_candidate_public_web_run、person_public_web_signal、target_candidate_public_web_promotion。**8 个 public-web 表现 read+write 双向单源。**
  caller-side 运行期状态留 builder(created_at/now merge;company_public_web_asset 的 source_run_ids = normalize(existing ∪ incoming) 累积)。
  验证:写 A/B 28 对 0 diff(含 FLOAT score、BOOL_INT publishable/force_refresh、source_run_ids merge);AST-精确 module-level 替换;
  live PG 44 passed/0 failed。**storage.py 本会话累计 28.1k→27.0k 行。**
  **写路径已转(9 builder):** linkedin_profile_registry(B4.2.0)+ crm_public_web batch/run/promotion + 5 above。
  **剩余写路径(下一批,更 bespoke):** inline upsert-site payload(target_candidate_public_web_batch@~6900、person_public_web_asset、
  crm_records/events/tasks、serving_projection、operation_run、agent_action 等 —— 列 dict 在 upsert 方法内联,夹杂 created_at/version/compute 逻辑)+
  带 compute 的 separate builder(`_projection_person_search_index_row_payload` 的 indexed_text 组装 + `_normalize_search_index_terms`;
  `_serving_projection_member_row_payload`)。模式已证;每个需把列映射从 upsert 的运行期/compute 逻辑里析出再委托 to_columns。
