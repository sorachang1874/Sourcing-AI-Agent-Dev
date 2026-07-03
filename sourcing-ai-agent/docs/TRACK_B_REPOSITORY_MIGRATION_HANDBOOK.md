# Track B ② Repository 迁移 Handbook(新一轮的入口文档)

> 状态:B4.3 影子拆除 **100% 完成**(2026-07-02,commit 952f9ee)。本文档是路线图 **②「Repository 查询方法建设 + 按域迁移调用方」** 这一轮的执行手册。
> 设计依据:`docs/TRACK_B_B4_2_PG_NATIVE_STORE_DESIGN.md`(B4.2 设计,owner 已 ratify);历史全记录:`docs/TRACK_B_PG_PURE_STORE_DESIGN.md` §6。
> 路线图(owner 2026-06-23 批准):① 死影子/mirror 拆除(**已完成**)→ **② 本轮** → ③ jsonb/timestamptz 数据迁移(合同级变更,**owner 明确 GO + 停机窗口后才可做**)。

## 1. 当前基底(起点事实,勿再重推导)

- `storage.py` = 19,187 行,**PG-pure**:零 sqlite3 / 零 `_connection` / 零 `_lock`。`ControlPlaneStore` 仍是单一 God-class 门面,这正是 ② 要解决的。
- PG schema 唯一来源 = `migrations/0001_baseline.sql` + `migration_runner.py`(adapter `ensure_bootstrapped()` 驱动);遗留迁移表由 adapter 内字面 DDL(`_LEGACY_TARGET_PUBLIC_WEB_MIGRATION_TABLE_DDL`)按需建。
- **已 ratify 的终态**(owner 2026-06-21):repositories 作为 PUBLIC API(退役 God-class 门面,调用方直接迁移);试点域 = `linkedin_profile_registry`;full jsonb + timestamptz(③,gated)。
- **已建成的地基**(B4.2.0–.12,全部落地可复用):
  - `src/sourcing_agent/control_plane_repository.py`:`Column`/`Kind`(STR/INT/FLOAT/BOOL_INT/JSON/JSON_LIST/JSON_STR_LIST)+ `TableDescriptor`(from_row/to_columns/upsert_sql/derived/read_default/write default)+ `Repository` 基类(over adapter primitives)。
  - `src/sourcing_agent/repositories/`:linkedin_profile_registry(37 列 + Repository 类)、public_web(10 表)、serving、workflow_runtime、person_company_assets、crm_core 等 descriptor。
  - **34 张表已读写单一来源**(from_row + to_columns 都走 descriptor);37 个 IRREGULAR read-mapper 未转(清单见 TRACK_B doc B4.2.1-.3 节)。
  - 工具:`scripts/_descriptor_equiv_harness.py`(descriptor↔手写 mapper 的字节等价 A/B 闸,已提交可复用)。

## 2. ② 的目标与范围

把「调用方 → ControlPlaneStore.method → adapter primitive」改为「调用方 → 域 Repository → typed 查询」,按域分批:

1. **查询方法建设**:每个域 Repository 补齐该域的 select/find/list/count 方法(descriptor 驱动生成 SQL,替代 storage.py 里手写的 `_select_control_plane_row(s)` + `where_sql` 字符串拼接 + `row_builder` 回调)。
2. **调用方迁移**:orchestrator / owners / api 层的 `store.xxx()` 调用改为 `repos.<domain>.xxx()`;storage.py 对应方法变薄壳→删除。
3. **每批产物**:该域 storage.py 方法数下降 + repositories/ 方法数上升 + 调用方 diff + 合同 lane 绿。

### 不在 ② 范围
- jsonb/timestamptz 列类型变更(③,owner-gated)。
- `control_plane_postgres.py` 的独立 on-disk-SQLite 导入/导出工具退役(独立决策)。
- `SOURCING_PG_ONLY_SQLITE_BACKEND` 惰性 env 插件面清理、orchestrator:41719 空转 sqlite3 except(小清理批,可顺手)。

## 3. 建议的批次顺序

1. **②.0 试点收口**:linkedin_profile_registry 域(descriptor 最成熟)——把该域 storage.py 方法(get/get_bulk/summarize/lease 家族/refill 队列/metrics/backfill)全量搬进 `repositories/linkedin_profile_registry.py`,调用方(enrichment.py 为主)直迁,storage 方法删除。**此批同时定型「域迁移协议」**(见 §4),后续批复制。
2. **②.1+ 按域推进**(建议顺序:调用面窄→宽):criteria/confidence(调用方集中在 orchestrator 少数点)→ manual_review → serving_projection → workflow_runtime(commands/workers/leases,调用面最宽,最后)。每域先跑清单命令(§5)定界。
3. **随批机会主义**:37 个 IRREGULAR read-mapper 里"软 irregular"(subscript 形式等价)可在其域批内顺手转 descriptor(harness 全列模式验证)。

## 4. 域迁移协议(每批必循)

1. **Scout**:清单该域 storage.py 方法 + 全仓调用方(`grep -rn "store\.<method>\|\.store\.<method>"`);分类 读/写/复杂(read-merge-write、跨表 JOIN)。
2. **Repository 方法落地**:descriptor 驱动;JOIN/聚合类先按现 SQL 移植(typed 参数),不趁机改语义。
3. **A/B 验证**:读方法 — 新旧同参对拍(现有 PG fixture 内);写方法 — `_descriptor_equiv_harness` 模式的 payload battery。
4. **调用方切换 + storage 方法删除**(同一批内完成,不留双轨)。
5. **合同 lane + 域套件**:`make ci-pre-agent-contract` + 该域直连套件(postgres_only env + `SOURCING_TEST_PG_ISOLATED_SCHEMA=1`);任何意外失败跑 **git-stash 控制实验**定 pre-existing。
6. **文档**:TRACK_B doc §6 追加一条批记录(表/方法数、调用方 diff 面、验证数字)。

## 5. 常用命令

```bash
# 本地 PG(容器 sourcing-local-postgres:55432)
make local-pg-up   # psql/pg_dump 在 /opt/homebrew/opt/postgresql@16/bin

# postgres_only 测试 env(直连套件)
set -a && source .local-postgres.env && set +a
SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1 \
SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only \
SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1 \
SOURCING_TEST_PG_ISOLATED_SCHEMA=1 \
.venv/bin/python -m pytest tests/<suite> -q

# 合同 lane(Testcontainers,自隔离;跑完必须检查 0 skip —— Docker daemon 宕会静默大面积 skip)
make ci-pre-agent-contract

# 域方法清单(示例:某域还有多少 storage 方法)
grep -n "def .*linkedin_profile_registry" src/sourcing_agent/storage.py
```

## 6. 已验证的 gotchas(付过学费的)

- **A/B harness 教训**:descriptor 写 `default` 只在空值时生效 —— 非空 kwarg(如 status="running")必须走 public payload 传入,漏传会被 default 静默覆盖。
- **`_select_control_plane_row(s)` 在 postgres_only 下 fail-closed raise**(不返 None-on-error);Repository 方法要保持同一错误语义。
- **count/sentinel 语义**:个别 count 方法保留了历史 swallow-and-return-0 语义(B3.2 batch-2 决定);迁移时逐方法确认,不要顺手"修"。
- **test_pipeline 是非 lane 的 SQLite 时代套件**:postgres_only 下有 ~15+ pre-existing 失败(含 ~33 个直写已删影子的测试,现 AttributeError)——不要把它们归因到你的改动;用 stash 控制实验。
- **守卫测试断言源码文本**(test_crm_public_web_runtime_boundary、test_pre_agent_contract_review、test_pg_onconflict_guard):删 storage 方法前 grep 这些文件,守卫要随迁(onconflict guard 的 unique_sets 现解析 0001_baseline.sql + live 模块字面 DDL)。
- **ultracode 工作流经验**:分类/审计批用「N 组分类 + 逐项对抗校验」两阶段;会话限额打断可用 `resumeFromRunId` 缓存续跑;`parallel()` 返回值要 `await` 后再 return。
- 已知失败预算(全部 stash 证明 pre-existing,勿追):lovable_board_visible_patches(owner 分子契约决策待定)、settings runtime-override 命名、snapshot idempotent 0!=1、2 个 crm sync-export 契约漂移(C1.4 异步化后待转 submit→drain→poll)。

## 7. 待 owner 决策(不阻塞 ②,但会到期)

- **③ jsonb/timestamptz 迁移窗口**(合同级,停机窗口 + 明确 GO)。
- `control_plane_postgres.py` on-disk-SQLite 导入/导出工具是否退役。
- lovable_board 分子契约。
