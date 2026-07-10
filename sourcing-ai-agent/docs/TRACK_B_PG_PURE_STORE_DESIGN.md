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
- **2026-06-22 B4.2.7/.8/.9 写路径推进 12 张表(commit 7518416, 79644ef, 12a1a85)** —— crm_core(3:crm_records/events/tasks)+
  serving(3:serving_projections/run_projection_links/collection_authoritative_pointers)+ person_company(6:person/company × asset/evidence/assertion)。
  **累计 23 张表 read+write 双向单源**(registry + 10 public-web + 上述 12)。全为 inline-upsert 转换:运行期/compute 状态留 upsert
  (crm_version 自增、`_resolve_person_identity_key`/`resolve_company_alias_key`、resolved workspace_id、normalized_value = x-or-value、
  `.lower()` state、published_at/occurred_at-or-now、dict-JSON 字段的 `_normalize_json_object_payload`),传 PUBLIC payload(field 键)给 to_columns。
  按 inline 应用处给 descriptor 列加 WRITE `default`(crm lifecycle/visibility/status/priority、serving projection_version、
  asset visibility_scope/status/authority/verification)—— write `default` 读安全(from_row 用 read_default;仅 to_columns 读 write default)。
  **列集 parity 全表成立**(读 mapper 已读每个写列;唯一一次 company_assets parity 疑虑是把它与 public-web 的 company asset mapper 弄混)。
  验证:写 A/B 各表逐字节(crm 15/0、serving 6/0+12/0、person_company 36/0,含 FLOAT score/BOOL/json-safe/JSON-string/默认/normalized_value/
  created_at merge);live PG 各域 round-trip 全绿(crm 69、serving 37、asset 42 + enrichment 147)。storage.py 27.0k→26.9k。
  **剩余:** (i) **workflow_runtime 域 13 表**(operation_runs/acquisition_runs/agent_actions/workflow_activity_runs+attempts/workflow_events/
  workflow_commands〔含 `_workflow_command_causality_columns_from_payload`〕/workflow_entity_deltas/acquisition_discovery_lanes/operation_events/
  workflow_current_state/runtime_outbox/workflow_recovery_intents)—— core agent runtime,state-machine/causality 形,最大剩余批;
  (ii) **bespoke(defer)**:raw_profile_index、candidate_evidence_index、projection_person_search_index、serving_projection_member
  (indexed_text 组装 + `_normalize_search_index_terms`/`_text` + 条件 source-id 列表)。模式已全证(inline+separate、FLOAT/BOOL、json-safe、列 parity)。
- **2026-06-22 B4.2.10 workflow_runtime 首 4 表(commit d272874)** —— 13 个 workflow_runtime 写按机制分:**9 个建 `row_payload` dict**(可转);
  **1 个(recovery_intents)经 native adapter `_call_control_plane_postgres_native` 写**(无 row_payload,DEFER —— PG 写在 adapter)。9 个里
  **4 个是 clean `_upsert_simple_control_plane_row` 无下游 mutation**:acquisition_runs/workflow_activity_runs/workflow_activity_attempts/
  workflow_entity_deltas —— 已转(A/B 24/0;status write-default planned/recorded;artifact_refs 是 `_loads_json_list` 的 JSON_LIST;
  181 tests pass)。**累计 27 表 read+write 双向单源。** storage.py → 26.87k。
  **剩余 workflow_runtime 8 表:** 5 个 clean-但-native-dispatch(agent_actions〔需 write default approval_status/policy="not_required"、
  status="planned"〕/operation_runs/workflow_commands〔causality〕/runtime_outbox 经 native dispatch;workflow_current_state 经
  `_write_control_plane_row_to_postgres`;均 mut=0,row_payload=to_columns 输出可直接替换)+ 3 个带下游 `row_payload[...]=` sequence/id mutation
  (workflow_events/operation_events/acquisition_discovery_lanes —— to_columns 产列名键,下游 mutation 仍可用,但需谨慎核)+ recovery_intents(native-adapter,defer)。
- **2026-06-22 B4.2.11/.12 workflow_runtime 收尾 7 表(commit a742490, cf0f03b)—— workflow_runtime 11/13 done,累计 34 表 read+write 单源。**
  B4.2.11:agent_actions/operation_runs/runtime_outbox(clean kwarg native-dispatch;A/B 9/0)。B4.2.12:workflow_current_state
  (read-merge-write,merge 留 caller,status default "pending")、acquisition_discovery_lanes(clean;mut=3 是 grep 误报,无下游 mutation;
  status/phase default "planned")、workflow_events/operation_events(kwarg native-dispatch,event_id="" + SQLite fallback 下游 sequence/event_id
  mutation —— to_columns 产列名键,mutation 不变)。A/B 6/0+6/0;live PG 116+129 passed。
  **关键教训(A/B 抓到):** write `default` 仅在值为空时生效;非空 kwarg(status="running")必须传进 public payload —— 省略它而依赖 default
  会静默强制成默认值。永远传原始 field 值,write default 只兜空值。
  **写路径基本完成 —— 2 个 workflow_runtime defer + 4 bespoke:** (i) **workflow_commands**:causality 列由 `_workflow_command_causality_columns_from_payload`
  **预编码**(已 json.dumps)再 `**spread`,过 to_columns 会双重编码;causality 是独立子系统,留手写。(ii) **workflow_recovery_intents**:经 native
  adapter `_call_control_plane_postgres_native` 写(PG 写在 adapter,无 row_payload)。(iii) **bespoke 手写**:raw_profile_index、candidate_evidence_index、
  projection_person_search_index、serving_projection_member(indexed_text 组装 + 条件 source-id 列表)。模式已全证(inline/separate/native-dispatch/merge、
  FLOAT/BOOL/JSON_LIST、json-safe、列 parity、下游 mutation)。
- **pre-existing 失败(非 Track B,另一子系统,flag 待查):** `test_workflow_explain.py::test_explain_workflow_does_not_use_legacy_standard_bundle_as_hidden_full_coverage_proof`
  —— `baseline_full_company_coverage_proven` 在 `include_population_coverage=False` 下为 True(期望 False)。逻辑在
  `asset_reuse_planning.py::_baseline_full_company_coverage_proven`(~1175,asset-reuse coverage-proof planner,**非 storage**)。git-stash 控制实验
  证 pre-existing(无任何 B4.2.11+ 改动也同样 fail)。需专门的 asset-reuse-planning 排查,勿归因于 descriptor 工作。
- **2026-06-23 owner 批准路线顺序:① 拆影子/mirror(B4 收尾,现在)→ ② 补 Repository 查询方法 + 按域迁移 caller → ③ jsonb(gated)。**
- **2026-06-23 B4.3 影子 teardown 启动(commit aa503c3, fd5853a)—— storage.py 26,847 → 25,300(−1,547 行)。**
  PG 在 postgres_only 下已 authoritative(B3.0),死 SQLite 尾可删。
  - **B4.3.1(21 个 write-and-mirror 尾)**:`if _write_control_plane_row_to_postgres(T,row): return <pg>` 后的死
    `with self._lock, self._connection: INSERT...` + `_mirror_*`。该 helper 返 True 或 raise(非空 row 永不返 False)→ 尾死 →
    换成 fail-closed `_raise_control_plane_postgres_write_failure`。
  - **B4.3.2(16 个 native-dispatch 尾)**:`if should_prefer_read(T): { native(...); if row: return; if skip: return {} }` 后的死 with。
    prefer-read 块在 postgres_only 下恒返回 → 尾死 → fail-closed `raise RuntimeError`。顺带删一个只喂死尾的孤儿 causality_columns 计算。
  - **方法**:AST 精确截断 + 严格模式匹配安全检查(仅当 guard-if 紧跟 `with self._lock, self._connection:` 才截)——非 B4.1 那次破了
    231 个读方法的盲截。每批验证:import + ruff + live PG 广套(387 / 192 passed;唯一 results-api fail 是 pre-existing lovable contract-drift,控制实验已证)。
  - **B4.3 余下批次**:(i) 死**读**尾(`pg=select; if pg: return; if skip: return []; <死 with: SELECT>`,~28 个 near + 其它;更杂——读方法在死 with 前建
    SQLite-only query-builder 局部 clauses/params,删尾后变 unused 须一并删);(ii) 6 个 skipped 写方法(SQLite 块前有中间逻辑);
    (iii) `_replace_*_from_sqlite`(17)+ mirror helpers 待无引用后删;(iv) 最后删 `self._connection = sqlite3.connect(...)`(:809)+ shadow `_lock` + `import sqlite3`。每批同 AST-safety + 合同 lane 验证。
- **2026-06-24 B4.3.3(52 死读尾,commit e184422)**:`pg=select; if pg: return pg; if skip: return <X>; <死 with self._lock: SELECT>` ——
  should_skip 在 B3.0 后恒 True,故把 `if skip: return <X>` + 死尾**塌缩成直接 `return <X>`**(顺带退役恒真 skip-guard)。AST-safe(skip-guard 单 return 紧跟 lock-with);
  哨兵保真(24 `{}` / 15 `None` / 13 `[]`)。再 `ruff F841 --fix` 删 14 个孤儿 SQLite-only query-builder 局部(where_sqlite/placeholders_sqlite/query + 级联 clauses/params;
  PG 路径仍用的 where_sqlite 经 `.replace("?","%s")` 保留)。−541 行;550 passed。
- **B4.3.4(5 个 irregular 写尾含 GENERIC helper,commit 61f7bcb)**:`_upsert_simple_control_plane_row`(~15 个 upsert 共用)——把
  `if (skip OR durable&pg): raise` 条件 guard + 死 SQLite upsert/mirror 塌缩成一个无条件 fail-closed raise(skip 恒 True,guard 本就恒 raise),清干净共享写路径。
  + upsert_workflow_current_state / job_board_visible_patch / job_materialization_item / job_result_lifecycle(各:`if write_pg: return` 后 SQLite-only
  columns/placeholders 局部 + 死 with → fail-closed raise)。−78 行;565 passed。**DEFER:upsert_acquisition_shard_registry**(其 `if not existing:` 新-shard 分支是 gated shadow SELECT,非干净死尾)。
- **2026-06-24 B4.3.5(32 死读尾含中间 query-builder 局部,commit 6e39121)**:把 batch-3 的读尾塌缩扩到 list_*/count_*/summarize_* —— 死 SQLite 块前
  夹着 where_clause/clauses/params 等中间局部(batch-3 的严格邻接检查漏掉)。安全点同 batch-3:`should_skip` 在 B3.0 后恒 True,故 `if skip: return X` 恒返回 →
  其后一切(含中间局部)皆死,与中间语句无关。迁移 helper 因无 skip-guard 不匹配。AST-safe(skip-guard body 单 return + 尾内有 `with self._lock:`);
  `if skip: return X` + 死尾塌缩成直接 `return X`(哨兵保真:多为 [],另 {} / 0 / summarize_jobs 空 dict)。再 `ruff F841 --fix` 删孤儿 SQLite-only 局部。
  −540 行;550 passed(唯一 fail 是 pre-existing lovable_board_visible_patches contract-drift)。残留(非阻塞):个别方法(如 summarize_jobs)仍 `.append()` 建
  now-unused clauses/params(ruff F841 标不到),影子 SELECT/with/connection 已删,留待后续 sweep。
- **2026-06-27 B4.3.6(26 死 native-dispatch 写/读尾含中间局部,commit 9ca66f4)**:把 batch-2 的 native-dispatch 尾退役扩到那 26 个——
  死 `with self._lock[, self._connection]:` 前夹中间局部(skip-guard 与死块间的 gap,batch-2 严格邻接漏掉),正与 batch-5 之于 batch-3 同构。
  安全点同 batch-2:方法 live 路径 gated 于 `if should_prefer_read(T):`,块末为 `if should_skip_sqlite_fallback(T): return X`;B3.0 后两 gate 在 postgres_only
  (唯一受支持的生产/合同 lane)恒 True → prefer-read 块恒进恒返(row-path 或 skip-path)→ 其后(中间局部 + SQLite with + mirror + 尾 return)皆死,与中间语句无关。
  死尾仅在「PG-preferred-but-not-authoritative」配置可达(B3.0 已退役),换成与 batch-2 同的 fail-closed `raise RuntimeError("postgres-only invariant violated …
  legacy SQLite tail retired (B4)")`。迁移 helper 不匹配(无 skip-guard,SQLite 无条件用)。AST-safe(prefer-read 块末是单-return skip-guard If + prefer/skip 表名相同
  + 尾内有 `with self._lock`);在 prefer-read 块尾截断方法体并补 fail-closed raise。26 法:save_job、append_job_event、get_job(_progress_event_summary/_result_view/
  _result_lifecycle)、mark_job_materialization_item_{completed,failed,waiting_prerequisite,partial_progress}、reawaken_waiting_prerequisite_{job_materialization_items,
  workflow_commands}、get_plan_review_session、{upsert,claim,get}_workflow_recovery_intent、update_operation_run_state、{get,list}_organization_execution_profile(s)、
  get_authoritative_/list_organization_asset_registry、list_acquisition_shard_registry、summarize_asset_membership_index、list_cloud_asset_operations、
  acquire_runtime_provider_limiter_slot、get_linkedin_profile_registry_backfill_run。哨兵保真({}/[]/None/0/limiter+summary dict)。再删 1 孤儿局部(append_job_event 的
  payload_json;PG native 路径用 payload_dict)。−770 行;550 passed。**验证升级**:除 import/ruff + Testcontainers 合同 lane + control-plane-live(authoritative 存储
  读写合同)全 PASS 外,首次对 test_pipeline materialization/registry focus 做 **clean-DB postgres_only A/B**(DROP/CREATE public schema 后 HEAD vs 本批各跑一遍,
  unittest 字母序确定)→ 两边**逐字相同的 17-fail 集**(pre-existing 共享-schema 污染;这些 test 缺 per-test PG 隔离)→ **零回归**。
- **2026-07-01 B4.3.7(53 个 skip@None native-dispatch 尾;首个「分类工作流 + 逐方法对抗校验」批)**:AST 扫描出 52 个候选(有 `should_prefer_read` +
  `_call_..._native` + `with self._lock` 但体内无 skip-guard;doc 原估 ~65 偏高)。ultracode 工作流(11 组分类 agent → 每方法 1 个独立对抗校验 agent,共 63 agent)
  逐方法判定 prefer-read 块控制流:**38 个 A 形态**(块内无条件 return,native-None 内联处理如 `return X if row is not None else None` —— 尾纯死,截断 + 标准
  fail-closed raise);**7 个 B 形态 fall-through(潜在 "no such table" 崩溃点!)**:块不 return、靠块后 `if id <= 0:`/`if reviewed is None:` 落 SQLite
  (record_query_dispatch、record_criteria_feedback、review_pattern_suggestion、record_criteria_result_diff、record_confidence_policy_run、create_criteria_version、
  record_criteria_compiler_run + corrected 的 upsert_criteria_pattern)—— 塌缩时把 None 路径改为显式 `_raise_control_plane_postgres_write_failure`(insert 型;
  sequence-resolution 异常才可能触发)或哨兵 return(review_pattern_suggestion 返 `{"suggestion": None,...}`,与 postgres_only 遗留可观察行为等价),成功路径逐字保留;
  **6 个 C 形态复杂法**(多 gate/多 dispatch:supersede_workflow_job 三重 AND gate、register/patch_asset_materialization 复合 gate + write-gate、
  deactivate_confidence_policy_control 双 gate 双死尾 —— replacement 重建 branch-2 全部活代码、replace_candidate_materialization_state_scope gate-1 死 else 单独手工塌缩、
  create_confidence_policy_control)。52 法全 COLLAPSE、0 DEFER(对抗校验 51 confirm + 1 corrected)。应用 = 自底向上行拼接 + 独立 AST 安全闸
  (方法 span 与分类清单精确匹配 / 删除区必含死 `with self._lock` / 保留区必含 prefer-gate / 全文件重解析 / 后置无残留 lock 块)。
  另修 1 个 pre-existing 守卫漂移:test_crm_public_web_runtime_boundary::latest_run_selection 断言 storage 法体内的 SQLite `ORDER BY` 字面量(batch-5 已删)——
  改为断言 storage 路由到 native reader + adapter SQL 保有 `ORDER BY created_at DESC, run_id DESC`/`PARTITION BY crm_record_id`(不变量链保持),并清掉该法
  batch-5 残留死局部(placeholders_sqlite/clauses_sqlite/params)。−1,758 行。验证:import/ruff 0 新增;Testcontainers 合同 lane 181 全 PASS(修守卫后);
  local-PG postgres_only + per-test schema 隔离(SOURCING_TEST_PG_ISOLATED_SCHEMA=1,首批使用):durable/worker/manual_review 123 + criteria/confidence/serving/
  materialization 30 + results_api/operation_runtime 389 + recovery/cancel-resume/dedup 52 全过;仅 2 个 stash-控制实验证明的 pre-existing
  (snapshot_materialization idempotent 0!=1;lovable_board_visible_patches)。
- **B4.3 进度:storage.py 26,847 → 21,613(−5,234 行,批 1-7)。** 已删掉全部干净死双路径尾 + skip@None 变体。每批那 1 个 results-api fail 都是 pre-existing 的
  lovable_board_visible_patches contract-drift(非 teardown)。
- **2026-07-02 B4.3.8(终局 live-vs-dead 分类批;40 法 49 区域塌缩 + mirror 机器整体删除)**:对剩余 48 个 lock-with 的 43 个宿主方法跑第二轮
  「分类工作流 + 逐方法对抗校验」(9 组分类 + 43 校验;首轮 17 个校验被会话限额打断,resumeFromRunId 断点续跑,缓存命中其余)。判定:**40 法 COLLAPSE
  (全部校验 CONFIRM、0 修正)+ 3 保留**(`close`/`_configure_connection` = infra,随 (f) 最终影子删除退休;`_ensure_legacy_target_public_web_sqlite_tables_for_migration`
  = LIVE 遗留迁移子系统)。新形态覆盖:bulk-loader 家族(replace_bootstrap/company/candidate/category_data —— `_replace_candidates_and_evidence_in_postgres`
  post-B3.0 只返 True 或 raise,fall-through 死)、candidates/evidence 读、plan-review 双法、agent_runtime_sessions 三法、org registry/execution-profile、
  canonicalize_organization_asset_registry_target_company(5 个不相交死区域)、acquisition_shard_registry(gated 影子 SELECT + 写尾,原 (d) 项)、
  search-index 三法、materialization state/runs、cloud asset ops、linkedin registry 全家(keys/aliases/leases/metrics/backfill/refill 218 行死尾/
  _upsert 161 行死尾)、provider limiter、advisory-lock fallback(原 (b) 项)。区域级应用脚本升级:区域两两不相交 + 每方法 lock-with 全覆盖 +
  编辑后无残留 lock 校验。随后删除零引用的 mirror 机器:storage `_mirror_control_plane_row` + `_replace_control_plane_table_from_sqlite` 定义、
  adapter `replace_table_from_sqlite`(原 (e) 项;`should_mirror` 在 adapter 内仍承重保留)。−1,635 行(21,613→19,978,**首破 2 万**)。
  验证:合同 lane 181 全 PASS(0 skip;首跑因 Docker daemon 宕导致批量 skip 判无效,重启 Docker+local-pg 后重跑);local-PG postgres_only + 隔离 flag:
  registry/durable/serving 148 + candidates/operation/materialization/recovery 180 + enrichment/results_api 431;仅 2 个已证 pre-existing
  (idempotent 0!=1、lovable_board)。
- **B4.3 完成度:storage.py 26,847 → 19,978(−6,869 行,批 1-8);Track B 全程 30,658 → 19,978。** SQLite 残余(全部有意保留):3 个 lock-with
  (close/_configure_connection/legacy-migration ensure)+ 50 个 conn.execute(LIVE 遗留 target-public-web 迁移子系统 + infra)。
- **2026-07-02 B4.3(f) 影子物理删除 —— B4.3 100% 完成(owner 批准退役遗留 SQLite 迁移子系统;commit 本条)。storage.py 零 sqlite3/零 _connection/零 _lock,19,187 行。**
  三层工作流映射(31 法分类+对抗校验、遗留子系统/影子 infra/sqlite3.Row 注解/测试消费者 4 份映射)后执行:
  - **关键解耦(映射的核心发现)**:PG 遗留表(target_candidate_public_web_batches/runs/promotions,不在 0001_baseline —— 迁移上下文专用)的 DDL 一直是
    **从 SQLite 影子生成的**(adapter `_ensure_legacy_target_public_web_migration_table_schema` → `sync_runtime_control_plane_to_postgres(sqlite_path=影子)`)。
    重写为 **adapter 内字面原生 PG DDL**(`_LEGACY_TARGET_PUBLIC_WEB_MIGRATION_TABLE_DDL`,与旧 sync 产物零漂移:TEXT/BIGINT/DOUBLE PRECISION、保 NOT NULL、
    无 DEFAULT、PK-only、无 idempotency UNIQUE;含 8 个二级索引,索引名与既有 PG 侧 ensure 幂等)。**迁移功能语义完整保留**(seed→PG、read→PG、archive/drop→PG);
    丢失的只是「首次迁移写时从影子隐式回填历史行」——按批准立场,遗留行已是物理 PG 行。
  - **31 个无锁 SQLite fallback 法塌缩**(criteria/confidence 读家族、candidates 计数、plan-review/job-events 读、find_best_completed_job_match 等 ——
    无 `with self._lock` 故历批 AST 探测全部漏掉)+ 8 个孤儿 `_locked` helper 与 `_insert_candidates_and_evidence`(executemany,又一探测盲区)、
    `_manual_review_item_from_row_payload` 整体删除。0 KEEP(除 3 个 infra/迁移项,现全删)。
  - **infra 删除**:`self._connection`(connect/row_factory/_configure_connection)、`self._lock`、`_resolve_postgres_only_sqlite_backend`
    (SOURCING_PG_ONLY_SQLITE_BACKEND 现为无效 env,插件面后续清)、`_ensure_legacy_..._sqlite_tables_for_migration` + `_sqlite_table_exists_locked` +
    `_drop_empty_...` + `_legacy_target_public_web_table_names`、adapter `replace_table_from_sqlite`/`_bootstrap_schema_from_sqlite_source`(零调用)、
    adapter `sqlite_path` 参数改可选。`close()` 收窄为纯 adapter-pool 处置。shadow accessors 保留为惰性标签(backend="retired"/connect_target="")。
    **两个影子时代 sync 遗迹退役**:orchestrator hosted-watchdog 的影子→PG sync(B4.1 后一直在同步空内存库,truncate_first 配置下甚至有破坏性)→
    静态 `{"status":"retired"}`;job_result_lifecycle_backfill 的建表 sync → `ensure_bootstrapped()`(表在 83 表基线内)。
  - **sqlite3.Row 注解清扫**(~54 处 → dict[str, Any],纯注解零运行时影响;live 调用点全部传 PG dict,由映射 agent 验证)→ `import sqlite3` 删除。
  - **legacy_public_web_storage PG-only 化**:删 `_list_sqlite_rows`/`_drop_sqlite_legacy_tables`/`_sqlite_table_exists`/`_should_skip_sqlite` + drop 结果的
    `"sqlite"` 键;`_list_rows` 直返 PG 腿(fail-closed-to-empty 语义在 `_list_postgres_rows` 的 except 保留)。retirement_audit 模块零改动(自动 PG-only)。
  - **测试面**:retirement-audit 4 法重写为 PG 断言(fresh-schema pg_tables 检查/adapter DDL 源断言/PG DROP/物理删表证明);operation_runtime 64 处 +
    results_api 4 处 `_connection.close()`→`store.close()`;pg_durable_runtime fixture 改 store.close();pool/live_postgres 影子语义测试改写(disk-refusal
    测试随 resolver 删除);test_pipeline PRAGMA 测试与 watchdog sync 测试改写为退役断言;worker_recovery_daemon 去掉对 storage 侧已删 re-import 的 patch;
    fake adapter sqlite_path 改可选。**顺手修复一个 pre-existing 静默坏了 10 天的守卫**:test_pg_onconflict_guard 自 B4.1 删 init_schema 起 unique_sets
    的 step-1(重放 storage.py 字面 DDL)失源、全表 unknown(不在合同 lane 无人发现)—— 重寄到 `migrations/0001_baseline.sql` 解析
    (ALTER TABLE PK + 非 partial CREATE UNIQUE INDEX)+ live 模块字面 DDL,守卫比原来更诚实。scripts/sync_latest_snapshot_from_registry.py 的
    `store._connection` 读(本已坏)改走 store API。
  - **验证**:retirement audit 7/7(seed→原生 DDL→读/归档/删 端到端);合同 lane 181 全 PASS 0 skip;core 组 145+21、consumer 组 250、results_api/enrichment 431
    全过;3 个失败全部 stash-控制实验证明 pre-existing(lovable_board、settings 命名、snapshot idempotent 属早前批)。**残留披露**:test_pipeline 里 ~33 处直写
    影子的测试(2404+ 等)本就在 postgres_only 下失败(no-such-table),现变 AttributeError —— 同一 pre-existing 失败集,非 lane;SOURCING_PG_ONLY_SQLITE_BACKEND
    env 插件面(Makefile/scripts/service_daemon/scripted_test_runtime)现为惰性 no-op,后续专批清理;`orchestrator.py:41719` 的 `except sqlite3.OperationalError`
    重试挂钩现永不匹配(psycopg 错误直穿),后续清理;`control_plane_postgres.py` 的独立 on-disk-SQLite 导入/导出工具(自带 sqlite3.connect)是合法的
    SQLite→PG 迁移工具,独立退役决策。
- **2026-07-02 ②.0 试点收口 DONE(linkedin_profile_registry 域全量搬迁 + 域迁移协议定型)**:God-class 门面的第一个域整体退役
  —— 26 个公开方法 + 12 个私有 helper 从 `storage.py` **删除**并以域内短名落地 `repositories/linkedin_profile_registry.py`
  (89 → 2,824 行;`storage.py` 19,187 → 16,382 行,-2,805)。调用方**同批直迁、零双轨**。
  - **公共 API 形态(协议定型,后续批复制)**:`store.repos.<domain>`(`ControlPlaneRepositories` 命名空间,store `__init__`
    随 adapter 建);repository 公开方法用**域内短名**(`get`/`get_bulk`/`mark_fetched`/`acquire_lease`/`record_refill_plan_items`…),
    God-class 前缀不带入新 API;`normalize_linkedin_profile_url` 不迁移(调用方直接用 `linkedin_url_normalization.normalize_linkedin_profile_url_key`
    模块函数 —— 守卫本就禁 `.normalize_linkedin_profile_url(` 调用记号)。getattr 特征检测调用方经 `repositories.linkedin_profile_registry_repo(store)`
    duck-typed 访问器(store-like 无 `repos` 时返 None,fallback 分支形状 1:1 保留)。
  - **Repository 基类扩展**(`control_plane_repository.py` 255 → 367 行):fail-closed 原语 `_select_row`/`_select_rows`/`_write_row`/
    `_should_prefer_read`/`_strict_authoritative`/`_raise_read|write_failure`/`_raise_postgres_only_invariant`,与退役的 storage 包装**逐字同语义**
    (错误串格式字节不变);原泛型 `get`/`select`/`upsert` 删除(零调用方,`get_by_key` 试点脚手架一并退役)。**写纪律**:repository 写一律走
    adapter 原语(`upsert_row`/`bulk_upsert_rows`/`insert_row_with_generated_id`/`delete_rows`)—— repositories/ 内**禁止字面 ON CONFLICT SQL**
    (pg_onconflict 守卫的字面扫描不覆盖该目录,conflict target 由 `_PRIMARY_KEY_COLUMNS` 承保)。
  - **方法体逐字移植**,含全部已知语义暗礁:count/sentinel(missing lease = `{}` 非 None)、fail-closed raise 位点、
    `preserve_unrecoverable`/refill 队列状态机、terminal-row 仍需 upsert 的 scope 计数语义。仅两处有意偏差(均验证/披露):
    `mark_queued_many` 死分支标签 `sqlite_loop`→`per_row_loop`(无断言引用);`upsert_backfill_run` 写路径改走新 BACKFILL_RUNS descriptor
    `to_columns`(37 个 IRREGULAR mapper 之一顺手转正,JSON-safe payload 字节等价,battery 验证);4×35 行的 effective-payload 行构造重复
    收敛为 `_effective_payload_row` 单 helper。invariant 错误串中的 method 标签改用新短名(结构不变)。
  - **A/B 验证(删除前窗口内新旧并存对拍)**:(a) 纯函数 battery —— `_compose_effective_payload` 120 组合、summarize/normalize_backfill_entry/
    percentile/row_payload/backfill-descriptor 读/retry-env 全部逐字节相等;(b) PG 读面 —— 9 个读方法同种子同参深度相等(含 alias 链、missing
    sentinel);(c) PG 写面 —— 19 步写脚本(queued/fetched/failed×2/queued_many/refill_plan×2/deferred/aliases/events/backfill_run×2/
    backfill_batch/lease 六步)在冻结时钟(storage/repo/adapter 三层时间函数 monkeypatch)下双跑,5 张域表全 dump 字节比对相同
    (仅 event_id 独立序列归一为 run 内序号)。
  - **调用方迁移(workflow 20 agent 并行,逐文件 grep 清零 + py_compile)**:src 90 点(65 直调 + 25 getattr,12 文件:enrichment 54、
    company_asset_completion 10、orchestrator 7、snapshot_materializer 4、supplement/backfill/fetch_owner 各 3、excel_intake 2、
    candidate_artifacts/outreach_layering/cli/apify 冒烟脚本各 1)+ tests 383 点(storage_profile_registry 147、enrichment 122(含 8 个
    fake store:方法改短名 + `self.repos = SimpleNamespace(linkedin_profile_registry=self)`)、pipeline 39、results_api 29、
    live_postgres 12(adapter 直调按规则不动)、candidate_artifacts 9、company_asset_completion 10、小文件批 15)。白盒测试缝隙迁移:
    `store._control_plane_postgres`→`repo._adapter`、`_select_control_plane_rows`→`repo._select_rows`、`_resolve_linkedin_profile_registry_key`→
    `repo._resolve_key`、tripwire `get_linkedin_profile_registry`→`repo.get`。orchestrator 两个 summary 方法(守卫断言源码文本)零触碰,AST 复验。
  - **验证**:合同 lane `make ci-pre-agent-contract` **181 passed / 0 skip**;域套件 test_storage_profile_registry **32/32**;
    test_enrichment 135、candidate_artifacts+live_postgres 114、company_asset_completion/supplement/excel_intake/profile_registry_backfill 77、
    workflow_explain/operation_runtime/cloud_asset_import/workflow_efficiency 181/1 失败、results_api 296/1 失败;
    3 个守卫套件(storage_surface_guardrails / pg_onconflict_guard / crm_public_web_runtime_boundary)39 全过;全仓 compileall 干净;
    旧 facade 记号全仓 grep 清零(仅守卫自身禁令串 + adapter 直调测试)。失败归因:results_api lovable_board(已知预算)+
    workflow_explain legacy-standard-bundle + hosted_workflow_smoke asset_population 组 —— 后两者**stash 控制实验证明 pre-existing**
    (干净基线同样失败,与本批无关)。
  - **残留披露**:`profile_prefetch_scheduler_lock`(协调原语,非 CRUD)按手册 §2 范围留在 storage;runtime_provider_limiter 三方法属
    `runtime_provider_limiter_leases` 域未动;events/aliases 表仍为 raw-dict 读(descriptor 化留给 ③ jsonb 轮);
    后续批若迁 public-web 域,新 repositories/ 文件需加入 test_crm_public_web_runtime_boundary 的 ALLOWED_LEGACY_STORAGE_ACCESS_FILES。

- **2026-07-06 ② 批次 1(②.1)完成 —— criteria/confidence 域整体退役到 `store.repos.criteria_confidence`**:
  - **范围**:8 表(criteria_feedback / criteria_patterns / criteria_pattern_suggestions / criteria_versions /
    criteria_compiler_runs / criteria_result_diffs / confidence_policy_runs / confidence_policy_controls;
    criteria_result_diffs 为侦察补入 —— FK 同簇,原候选清单遗漏)、24 公共方法(域短名)+ 2 私有 helper + 8 手写 row mapper +
    3 模块级纯函数,整域一批、无双轨。storage.py 16,332 → **15,091 行**(-1,241);新
    `repositories/criteria_confidence.py` 1,259 行;`control_plane_repository.py` 368 → 394 行。
  - **基座件**:(a) `Repository._call_native_write` —— storage `_call_control_plane_postgres_native` 写分支的逐字镜像
    (native 写原语 insert_row_with_generated_id / update_row_returning / upsert_row_with_generated_id;non-strict 吞为 None、
    strict raise,消息串逐字);本域是 repositories/ 首个使用这三原语的域。(b) 跨域缝隙定型:`ControlPlaneRepositories(adapter,
    job_lookup=store.get_job)` 注入回调,`_prepare_feedback_context` 用 `self._job_lookup(job_id) if callable(...) else None`
    —— repo 不得反向 import storage(环),未来 jobs 域迁移只换绑定。(c) 共享纯函数外提:`_matching_bundle_payload` /
    `_request_signature_context` 逐字迁入 `request_matching.py`(公名 `matching_bundle_payload` / `request_signature_context`);
    storage 保留 `_matching_bundle_payload` 别名 import(外域 5 调用点零改动),`_request_signature_context` 别名随域删除一并移除。
  - **逐字声明**:AST 定位 + 座名机械替换后逐函数字节比对:**34 函数完全相等,3 个预期差异恰为披露偏差,0 意外、0 遗漏**。披露偏差:
    (1) create/deactivate_policy_control 中 B4.3 遗留 inert '?' 占位符死代码块随迁删除(diff 仅含删行,变量后续零引用);
    (2) `_raise_control_plane_postgres_write_failure`→基类 `_raise_write_failure` 座名映射 ×6(消息串逐字,②.0 协议映射表补此一项);
    (3) get_job 缝隙一行(见上)。mapper 全部逐字**不转 descriptor**(推翻侦察初判):mapper 为 None 直通
    (`_row_value` default 仅键缺失时生效,NULL 列值直通),而写路径在 nullable bigint FK 真写 NULL —— Kind.INT 会把 None 强转 0,
    真实数据上必炸字节比对;descriptor 化整体移交 ③ jsonb 轮(需先加 nullable-int/直通 Kind)。`_confidence_policy_control_from_row`
    的裸下标(缺列 KeyError)语义保留;storage 版 `_row_value`(default=''、except Exception)与基类版语义不同,模块内逐字复制而非复用。
  - **A/B 数字**(临时件 tests/test_ab_cc_migration_tmp.py,4/4 过后删除):纯函数电池(matches_scope 7 控制 × 80 参数组 = 560 组、
    _payload_signature 3 载荷、signature_context 4 形状 + is-identity 断言);读面 34 对同参深比对(含 blank-company OR 兼容、
    NULL FK 直通、缺行哨兵);**Tier-A/Tier-B 哨兵分裂显式验证**(非权威空读:Tier-A 6 对 RuntimeError 消息逐字相等,
    Tier-B 8 个静默 []/None —— postgres_only 下 raise 分支不可达,故 patch 非权威态验证);写面 23 步脚本双跑
    (冻结 storage `_utc_now_timestamp` + repo `utc_now_timestamp` 两层即可 —— 本域无租约/派生时钟;返回值逐项相等 + 8 表 dump 字节相等)。
    新 gotcha:**8 表 id 全是独立 CREATE SEQUENCE(dump 风格 DDL,无 OWNED BY),TRUNCATE RESTART IDENTITY 不重置** ——
    ②.0 events 教训的全域泛化;A/B 间隔需显式 `ALTER SEQUENCE ... RESTART WITH 1`。
  - **调用方迁移**(workflow 6 agent 并行 + 2 处手工补切,共 87 处):src 40(orchestrator 34、criteria_evolution 6);
    tests 47(live_postgres 21、criteria_evolution 15、matching_metadata 5、pipeline 6)。api/cli 不动(调的是 orchestrator 门面);
    orchestrator 三个同名门面方法(record_criteria_feedback/list_criteria_patterns/review_pattern_suggestion)保留,仅内部 store 调用改写;
    pipeline 经门面的 8 处不动;fake store 零个、getattr 探测零处(无 accessor,与 ②.0 不同)。
    **事故记录**:手写 rename map 漏了 `list_confidence_policy_runs→list_policy_runs`,两个 agent 按"不在映射表不猜"纪律保守跳过并上报,
    我补切 2 处 —— 教训:fan-out 映射表必须由侦察清单机械生成,不得手打;agent 的"报告而非猜测"规则是本批的安全网。
  - **onconflict 守卫扩展**(与 storage 删除同批,阻塞项解除):`extract_generated_id_upsert_targets` /
    `collect_all_conflict_targets` 增扫 `repositories/*.py`(AST generated-id 调用 + 字面 ON CONFLICT;`_call_native_write`
    首位置常量形状与 storage 相同,匹配器零改);criteria_patterns 哨兵断言现由 repositories/criteria_confidence.py 喂养,
    删除后守卫 4/4 复验通过。control_plane_postgres.py `_CONTROL_PLANE_UNIQUE_INDEXES` 注释同步(数据结构未动)。
  - **验证**:合同 lane 181/**0 skip** + 后续门(2/11/1/2)+ 冒烟 dry_run_ready;criteria_evolution+matching_metadata 9 过;
    live_postgres 56 过;守卫+linkedin 域回归(storage_surface_guardrails / crm_public_web_runtime_boundary /
    storage_profile_registry)67 过;onconflict 守卫 4 过;pipeline 域子集(-k criteria/confidence/pattern/feedback)4 过 2 败 ——
    **worktree 基线对照:基线同子集 3 败 ⊇ 迁移树 2 败,零回归**(基线独有 1 败为抖动)。ruff 门:orchestrator.py 一次 format
    (长接收器换行)后 39 文件全过;mypy 87 错误与基线**逐字节相同**(1/64/1/21 同四文件,债务全 pre-existing);
    compileall 干净;29 个旧记号全仓 grep 清零(仅 pycache 二进制 / request_matching 来源注释 / pipeline 的 orchestrator 门面调用)。
  - **残留披露**:`get_confidence_policy_control` 与 `upsert_pattern` 零生产调用方(前者仅测试直连,后者仅域内组合);
    orchestrator 门面三方法待其自身域重构时再议;`_storage_b423/b425_backup` 的 pycache 残迹与本批无关。

- **2026-07-10 ② 批次 2(②.2)完成 —— manual_review 单表整域退役到 `store.repos.manual_review`**:
  - **范围与所有权**:`manual_review_items` 1 表、7 个公共方法(`replace_items`/`list_items`/`count_items`/
    `cleanup_items`/`review_item`/`get_item`/`merge_item_metadata`)
    + 6 个模块 helper + 1 个手写 mapper 整体迁入 `repositories/manual_review.py`(481 行),`ControlPlaneStore`
    同名长门面同批删除、零双轨。`storage.py` **15,091 → 14,635 行**(-456)。mapper 不转 descriptor:
    nullable scalar 必须保持 `None` 直通,三个 JSON 列必须保持直接 `json.loads` 的 malformed-input raise 语义;
    当前 `Kind` 会改变这两项合同,留给 ③ 的 nullable/typed-column 设计。
  - **逐字声明**:对旧实现施加已批准的短名、Repository 原语和时钟映射后,7 public + 6 helper + mapper
    **14/14 AST 结构等价**,披露差异 0、意外差异 0。保留了 non-authoritative `[]/0/None` 哨兵、Tier-A
    fail-closed 错误串、replace/cleanup 的非事务顺序、review 的 update-row-None re-read 竞态、未知 action 保持状态、
    空 candidate/evidence 不覆盖和 review 路径的 `reviewed_at`/`updated_at` 分别取时;未顺手优化 count 或清理状态机。
  - **A/B + 变异自检**(临时件跑完删除):首轮 3 部分 battery(纯 helper/mapper、PG 读+Tier-A/Tier-B、PG 写返回值+
    16 列全表 dump+独立 sequence)初跑 **3 passed/4.07s**;把新 mapper 的 metadata 人为改为 `{}` 后
    **3 failed/3.03s**(三部分均捕获);恢复后 **3 passed/2.88s**。A/B 冻结 storage + repo 两层时钟源。另用真实
    `company_assets/<company>/<snapshot_id>/...` 路径在迁移树与 pinned `c19c0fd` worktree 各跑同一 cleanup 脚本,
    两边均 **1 passed**(2.08s/2.04s),6 行×16 列、返回值和 sequence `[6,true]` 的 JSON **逐字节相同**
    (SHA-256 `8913ee82698af70ee0f1deb7c56307f605b60352e264deaa614963a3d755ecab`);cleanup 计数为
    metadata_updated=3 / superseded=1 / out_of_scope=2。
  - **调用方切换**:production **14** 点全在 orchestrator(2 replace / 3 list / 5 count / 2 get / 1 merge /
    1 review);tests **17** 点(live_postgres 6 + pipeline 11),合计 31 个 repo 直调。pipeline 的 2 个白盒 patch
    receiver 同步迁到 repo。fake store=0、getattr=0、callback=0、ambiguous_sites=0;API/CLI 继续调用 orchestrator
    公共门面。全仓旧 `ControlPlaneStore` receiver 直调=0、旧定义=0;adapter API 与 plan-review 域未触碰。
  - **永久防回归**:`test_storage_surface_guardrails` 新增 AST surface guard(旧 7 门面不得回生、新 7 短方法和
    namespace wiring 必须存在)及 mapper nullable/malformed-JSON 合同;新 repo 纳入 `run_python_quality.sh` 和 mypy
    文件清单。`Repository._call_native_write` 的共享能力说明补入已支持的 `delete_rows`;schema/DDL/字段合同零变化。
  - **验证**:最终 `make ci-pre-agent-contract` **187 passed / 0 skip** + 后续门 **2/11/1/2 passed** +
    `dry_run_ready failures=[]`;永久域/守卫组合 **69 passed**(surface + synthesis + resolution + live_postgres +
    pg_onconflict);compileall 干净;ruff/format 40 文件全绿;mypy **87 errors in 4 files**与 R-011 基线相同
    (candidate_artifacts 1 / orchestrator 64 / public_web_runtime_core 1 / workflow_smoke 21),新 repo 单独 0。
    pipeline 精确 5 项在迁移树与 `c19c0fd` worktree 都是 **3 passed / 2 failed / 452 deselected**;同两败
    (`manual_review_count` 12→0、candidate detail 410)逐项相同,零新增 regression,归 R-009。
  - **台账/评审**:`RESIDUAL_LEDGER` tripwire 全扫:R-009 仅作基线归因、未启动其重设计,R-011 未增长,其余未触发;
    D-1/D-2/D-3 截止仍为 2026-07-31;②.3 仅做逐字 repository 迁移且不改投影/看板计数语义时不阻塞,
    一旦改计数语义则 R-007/D-3 立即触发。作者家族内只读对抗审查无 finding(另补 Tier-A/Tier-B/
    update-row-None 探针);正式非 GPT 异步参考评审在 pinned 批提交后启动,结果与 artifact 路径由后续记录回填。
