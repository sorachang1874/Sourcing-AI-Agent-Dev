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

**结论:Track B 不是"重写 292 方法补 PG",而是"删除已死的 SQLite 双路径 + 统一 schema 单源"。**
dual *code*(非 dual *data*)是行语义分歧(`WORKFLOW_BEHAVIOR_GUARDRAILS.md` invariant 7)的根 —
删 SQLite = 从结构上消灭该 bug 族。风险低于"重写"定性,因为被删的 SQLite 分支在生产本就不执行。

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

- **取径**:删-非-重写(consolidation;durable-foundation:修设计不修症状)。
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
  **B3.2 BULK = 多批 follow-up(~295 dual-path 方法)**,按表组:每批删该组方法的死 SQLite 半 + 跑合同 lane(PG-fixture 测 exercise PG 路径,
  验证简化保持 PG 语义)。每方法 characterize 行-不存在语义(invariant 7)。最后(B3.2 尾 / B4 头)简化恒真 routing scaffolding
  (`should_prefer_read`/`should_skip_sqlite_fallback`/`_select_control_plane_row(s)` 的 not-prefer/skip 分支)。`self._conn`/init_schema/
  mirror/`_bootstrap_schema_from_sqlite_source`/SHADOW_INFRA 留到 **B4**。鉴于 295 方法的体量,bulk 宜以专批(可新 context)推进。
