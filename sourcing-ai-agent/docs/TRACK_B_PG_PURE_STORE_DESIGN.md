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
  baseline apply 到 fresh schema 后 re-dump 与 capture **逐字节相同**;(c) 生成器跨运行**确定性**逐字节稳定。下一步:
  B1.2 runner(advisory-lock 串行化 + `schema_migrations` 记账;接入启动/`ensure_bootstrapped`)。
