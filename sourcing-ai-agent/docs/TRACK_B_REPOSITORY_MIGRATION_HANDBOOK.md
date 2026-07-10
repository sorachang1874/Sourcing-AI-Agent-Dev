# Track B ② Repository 迁移 Handbook(新一轮的入口文档)

> Status: Living handbook — Track B ② 轮(按域退役 storage.py 到 store.repos.*)的入口执行手册。
> 状态:**②.3a 完成(2026-07-10)** —— serving_projection catalog 三表 9 公共方法 + 3 mapper 已退役到
> `store.repos.serving_projection`,storage.py 14,635 → 14,355 行,调用方 151 处直迁(src/script 69 + tests 82),
> 合同 lane 189/0 skip、A/B 变异自检和 pinned-worktree 字节对照全过。批记录:
> `TRACK_B_PG_PURE_STORE_DESIGN.md` §6 末条。下一批:②.3b `projection_manifest_shards`;随后 ②.3c members / ②.3d
> search-index,3d 后 serving_projection 整域闭合。
> B4.3 影子拆除 100% 完成(commit 952f9ee)。本文档是路线图
> **②「Repository 查询方法建设 + 按域迁移调用方」** 这一轮的执行手册。
> 设计依据:`docs/TRACK_B_B4_2_PG_NATIVE_STORE_DESIGN.md`(B4.2 设计,owner 已 ratify);历史全记录:`docs/TRACK_B_PG_PURE_STORE_DESIGN.md` §6。
> 路线图(owner 2026-06-23 批准):① 死影子/mirror 拆除(**已完成**)→ **② 本轮** → ③ jsonb/timestamptz 数据迁移(合同级变更,**owner 明确 GO + 停机窗口后才可做**)。

## 1. 当前基底(起点事实,勿再重推导)

- `storage.py` = 14,355 行(②.3a 后;②.2 后 14,635,②.1 后 15,091,②.0 后 16,382,试点前 19,187),**PG-pure**:零 sqlite3 / 零 `_connection` / 零 `_lock`。
  已整体退役的域:linkedin_profile_registry(②.0)、criteria/confidence(②.1,`store.repos.criteria_confidence`)、
  manual_review(②.2,`store.repos.manual_review`);serving_projection 的 catalog 三表已退役(②.3a),其余三表按 b/c/d 子批继续;
  其余域仍在 God-class 门面上,按批推进。共享纯函数 `matching_bundle_payload`/`request_signature_context` 已外提到
  `request_matching.py`(storage 留别名 import)。
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

1. ~~**②.0 试点收口**~~ **DONE 2026-07-02**:linkedin_profile_registry 域全量搬迁 + 域迁移协议定型(§4);批记录见 TRACK_B doc §6 末条。
2. ~~**②.1 criteria/confidence**~~ **DONE 2026-07-06**(8 表 24 方法;批记录见 TRACK_B doc §6 末条)。
3. ~~**②.2 manual_review**~~ **DONE 2026-07-10**(1 表 7 公共方法;批记录见 Track B doc §6 末条)。
4. **②.3 serving_projection 分子批推进**:~~②.3a catalog 三表~~ DONE → ②.3b manifest shards → ②.3c members → ②.3d person search index(3d 后整域闭合);再进 workflow_runtime(commands/workers/leases,调用面最宽,最后;`_call_native_write` 基座已就位)。每子批先跑清单命令(§5)定界、每表零双轨并独立记 A/B 数字与异步 review scope。
5. **随批机会主义**:37 个 IRREGULAR read-mapper 里"软 irregular"(subscript 形式等价)可在其域批内顺手转 descriptor(harness 全列模式验证)。
   **②.1 反例警示**:criteria/confidence 的 8 个 mapper 看似可转,实为 None 直通 + 写路径真写 NULL FK —— Kind.INT 会 None→0 炸字节等价;
   判定"软 irregular"必须核对**写路径是否产 NULL** 与 mapper 的 default 语义,不能只看 subscript 形状。

## 4. 域迁移协议(②.0 已定型 —— 每批必循)

1. **Scout**:清单该域 storage.py 方法 + 全仓调用方(直调 **和** getattr 特征检测,含**多行 getattr**;`grep -rn "store\.<method>\|\"<method>\""`);
   分类 读/写/复杂;同时清单 (a) 域测试套件里的**白盒 monkeypatch 缝隙**,(b) tests 里定义同名方法的 **fake store 类**,
   (c) 守卫测试对该域的源码文本断言。
2. **Repository 方法落地**(形态已定型):
   - 公共 API = `store.repos.<domain>`,方法用**域内短名**(God-class 前缀不带入);getattr 调用方配 `repositories.<domain>_repo(store)`
     duck-typed 访问器(无 repos 的 fake 返 None → fallback 分支形状不变)。
   - 方法体**逐字移植**,内部改用基类 fail-closed 原语,座名映射表(②.1 定稿):`_select_control_plane_row(s)`→`_select_row(s)`、
     `_write_control_plane_row_to_postgres`→`_write_row`、`_call_control_plane_postgres_native`→`_call_native_write`(native 写三原语)、
     `_control_plane_postgres_should_prefer_read`→`_should_prefer_read`、`..._should_skip_sqlite_fallback`→`_strict_authoritative`、
     `_raise_control_plane_postgres_{write,read}_failure`→`_raise_{write,read}_failure`、`_utc_now_timestamp`→`control_plane_time.utc_now_timestamp`。
     不趁机改语义、不趁机"修" sentinel;硬编码错误串(含旧长方法名)逐字保留。
   - **跨域调用缝隙**(②.1 定型):repo 不得 import storage(环)——跨域依赖以回调注入
     (`ControlPlaneRepositories.__init__(adapter, *, <dep>_lookup=store.<method>)`,repo 内 `callable(...)` 守卫,缺省时行为 == 该依赖缺失的原分支)。
   - **逐字性机械验证**(②.1 新增,A/B 之前的第一道闸):AST 定位新旧函数源段,对旧段施加映射表替换后**逐函数字节比对**;
     预期差异只允许是披露偏差,diff 逐条目检(②.1:34 函数全等 + 3 披露差异 + 0 意外)。
   - **写纪律**:写一律走 adapter 原语(upsert_row/bulk_upsert_rows/insert_row_with_generated_id/update_row_returning/
     upsert_row_with_generated_id/delete_rows);repositories/ 内**禁止字面 ON CONFLICT SQL**。onconflict 守卫已扩展(②.1):
     AST generated-id 扫描 + 字面扫描均覆盖 `repositories/*.py`,带非主键 conflict target 的方法可安全迁移。
   - "软 irregular" read-mapper 可顺手转 descriptor(全列 battery 验证)。
3. **A/B 验证**(新旧并存窗口内,删除前):纯函数直接 battery;PG 读面同种子同参深拍 + **两个哨兵 tier 显式断言**
   (Tier-A raise 消息逐字相等、Tier-B 静默哨兵 —— postgres_only 下 raise 分支不可达,需 patch 非权威态才能踩到);
   PG 写面**同一脚本双跑字节比对**(TRUNCATE RESTART IDENTITY 间隔;时间冻结按域裁剪:无租约/派生时钟的域冻
   storage+repo 两层即可,有租约的按 ②.0 冻三层且到期设远期;**独立 CREATE SEQUENCE(dump 风格 DDL,无 OWNED BY)不被
   RESTART IDENTITY 重置 —— 显式 `ALTER SEQUENCE ... RESTART WITH 1`**,②.1 证实这是全域现象而非 events 特例)。
   A/B 文件为临时件,跑完删除、数字入批记录。
   **电池自检(变异一次)**:电池首跑全绿后,人为破坏一处被比对面(改一个字段映射/一行 SQL)确认电池变红,再恢复 ——
   防"新比新"的空转比对(同 Track A Step 1 的变异敏感性验证、2026-07-09 lane REQUIRE flag 的坏 DSN 自证)。
4. **调用方切换 + storage 方法删除**(同一批内完成,不留双轨):
   - fake store 适配:方法改短名 + `self.repos = SimpleNamespace(<domain>=self)`;只改名**不补缺**(缺失驱动 fallback 是有意的)。
   - 白盒缝隙迁到 repo 实例:`_control_plane_postgres`→`repo._adapter`、`_select_control_plane_rows`→`repo._select_rows`、域私有 helper 对应短名。
   - adapter 直调(receiver 是 `_control_plane_postgres`/adapter 对象)**不动** —— adapter API 不改名。
   - 调用面宽时用 workflow 并行(一文件一 agent,逐文件 grep 清零 + py_compile 收口),收口后全仓 grep 旧记号清零 + compileall。
   - **映射表纪律(②.1 事故教训)**:fan-out 的 rename map 必须从侦察清单**机械生成**,不得手打(②.1 手打漏了
     `list_confidence_policy_runs`,靠 agent"不在映射表不猜、上报 ambiguous"纪律兜住)——给 agent 的指令里必须包含
     该"报告而非猜测"条款,收口时逐条核对 ambiguous_sites。
5. **合同 lane + 域套件**:`make ci-pre-agent-contract`(2026-07-09 起 lane 自带 skip→fail:REQUIRE flags 全段生效,
   PG 不可用直接红,不再依赖人工数 skip;非 PG 原因的 skip 仍需扫一眼输出)+ 该域直连套件(postgres_only env +
   `SOURCING_TEST_PG_ISOLATED_SCHEMA=1`)+ 全部被改测试文件 + lint 门(`run_python_quality.sh`:ruff 段必须全过;
   mypy 段与基线**计数逐字对照**,债务不得新增);任何**不在 §6 已知预算内**的失败跑 **git worktree 基线对照**定 pre-existing
   (勿用 stash —— 见 §6 效率纪律;慢套件只跑域相关 `-k` 子集)。
6. **文档**:TRACK_B doc §6 追加一条批记录(表/方法数、调用方 diff 面、验证数字);协议有修订则更新本节;
   扫一遍 `docs/RESIDUAL_LEDGER.md` 的 tripwire 列,触发的行升级为工作项或决策卡。
7. **异步参考评审(不阻塞后续开发,2026-07-09 接入;owner 2026-07-10 更新)**:批 settle 后按
   `docs/INDEPENDENT_REVIEW_GATE.md` 的 Async Reference Review Lane 在后台发独立只读 Codex 评审
   (锚定 pinned commit,模型路由见该文件 Model Routing 表;跨模型优先但非必需)。请求与 scope 记录后立即开始下一域;
   `NO-GO` 只冻结被审 scope 的 live/W6/manual/里程碑签收,不冻结其他模块。工作按主验证(A/B + 对抗校验 + lane)推进,评审落地后按
   `INDEPENDENT_REVIEW_BRIEF.md` 的三分类分诊:`new` 类 follow-up 修复、`re-raise` 类记录、`residual` 类
   须引用 `RESIDUAL_LEDGER.md` 行 id。verbatim-port 批是"同 worktree 盲区"(作者与验证 agent 共享同一棵树、
   同一套侦察工件)的典型风险面;独立 reviewer session 是最低要求,跨模型可用时再补异质盲区覆盖。

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

# 合同 lane(解析本地/CI DSN,SOURCING_RUN_TESTCONTAINERS=1 才回落容器;2026-07-09 起
# REQUIRE flags 内建于 lane 命令 —— PG 不可用时 fail-closed 变红,Docker daemon 宕不再静默假绿)
make ci-pre-agent-contract

# 下一域方法清单(②.3 serving_projection;落批前仍须按 §4 Scout 扩成精确方法映射)
rg -n '^    def .*projection' src/sourcing_agent/storage.py
```

## 6. 已验证的 gotchas(付过学费的)

- **A/B harness 教训**:descriptor 写 `default` 只在空值时生效 —— 非空 kwarg(如 status="running")必须走 public payload 传入,漏传会被 default 静默覆盖。
- **`_select_control_plane_row(s)` 在 postgres_only 下 fail-closed raise**(不返 None-on-error);Repository 方法要保持同一错误语义。
- **count/sentinel 语义**:个别 count 方法保留了历史 swallow-and-return-0 语义(B3.2 batch-2 决定);迁移时逐方法确认,不要顺手"修"。
- **test_pipeline 是非 lane 的 SQLite 时代套件**:postgres_only 下有 ~15+ pre-existing 失败(含 ~33 个直写已删影子的测试,现 AttributeError)——不要把它们归因到你的改动;用 stash 控制实验。
- **守卫测试断言源码文本**(test_crm_public_web_runtime_boundary、test_pre_agent_contract_review、test_pg_onconflict_guard):删 storage 方法前 grep 这些文件,守卫要随迁(onconflict guard 的 unique_sets 现解析 0001_baseline.sql + live 模块字面 DDL)。
- **ultracode 工作流经验**:分类/审计批用「N 组分类 + 逐项对抗校验」两阶段;会话限额打断可用 `resumeFromRunId` 缓存续跑;`parallel()` 返回值要 `await` 后再 return。
- **已知失败预算已升级为残差台账 `docs/RESIDUAL_LEDGER.md`(2026-07-09)**:逐条带 id/tripwire/归因证据,
  批验收 = green-modulo-ledger;新失败先 worktree 归因再入账;评审 residual 类 finding 必须引用行 id。
  本节旧的扁平预算清单已迁入该台账(R-001…R-012),勿在此处再维护副本。
- **②.0 新增经验**:写面 A/B 需 monkeypatch **三层**时间源(storage `_utc_now_timestamp`+`datetime` / repo 模块 `utc_now_timestamp`+`datetime` / adapter `_utc_now_sql_timestamp`+`_expiry_timestamp`),租约到期时间要设**远期**(`expired` 是对真实墙钟算的 derived 字段);`linkedin_profile_registry_events.event_id` 是独立 sequence,`TRUNCATE RESTART IDENTITY` 不重置 —— 比对时归一为 run 内序号。守卫扫描面:repositories/ 不在 onconflict 字面扫描内(写走 adapter 原语即可);test_crm_public_web_runtime_boundary 的 AST 扫描**覆盖** repositories/(public-web 域迁移时需加 allowlist)。
- **效率纪律(②.0 教训,勿再全量跑慢套件)**:test_pipeline 全量在 postgres_only 下 **>90 分钟**且非 lane —— 观察性验证只跑域相关子集
  (`-k "linkedin or profile_registry or refill or prefetch"`,59/457 项,~70 秒);控制实验用 **git worktree**(HEAD 基线副本,
  `PYTHONPATH=<worktree>/src` 覆盖 editable 安装的主仓路径)而非 `git stash` —— stash 会把整个工作树切回基线,阻塞并行的编辑/侦察/文档工作
  (②.0 曾因此空等 20 分钟,且期间跑着的套件结果作废)。②.0 子集对照:基线 21 败 ⊇ 迁移树 20 败,零回归。
- **②.1 新增经验**:(a) **descriptor 化误判风险** —— None 直通 mapper + NULL FK 写路径的域不能转 Kind.INT/STR(②.1 全部 8 mapper
  逐字保留,descriptor 化移交 ③,需新增 nullable/直通 Kind);(b) **独立 sequence 是全域现象**(dump 风格 DDL 无 OWNED BY,
  RESTART IDENTITY 不重置,A/B 显式 ALTER SEQUENCE);(c) **storage `_row_value`(default=''、except Exception)≠ 基类
  `_row_value`(default=None、窄异常)** —— 迁移 mapper 时模块内逐字复制 storage 版,勿复用基类版;(d) fan-out rename map
  机械生成 + agent"不猜、上报"条款(②.1 手打漏一项,靠该条款兜住);(e) lint 门的 mypy 段有 87 条 pre-existing 债务
  (candidate_artifacts 1 / orchestrator 64 / public_web_runtime_core 1 / workflow_smoke 21,2026-07-06 基线),验收标准 =
  与基线计数逐字相同,ruff 段全过即可;(f) worktree 基线跑 `run_python_quality.sh` 会因 worktree 无 .venv 失败
  ("Missing required tool: ruff")—— 用主仓 venv 的 mypy 二进制在 worktree cwd 下直跑。
- **②.2 新增经验**:(a) manual_review mapper 同时依赖 nullable scalar 直通与 malformed JSON 直接 raise,仍不适合现有
  descriptor Kind;(b) `manual_review_items_review_item_id_seq` 同样是无 OWNED BY 的独立 sequence,A/B 必须显式 restart;
  (c) snapshot-path 电池必须使用 canonical `company_assets/<company>/<snapshot_id>/...` 形状,额外 pinned-worktree 对照可防
  测试路径本身把 `snapshots` 误识别为 snapshot_id;(d) cleanup 是有序、多次写的历史状态机,本批只逐字迁移,不并发化或事务化。
- **②.3a 新增经验**:(a) 新旧 Store/Repository 若共用同一 descriptor,同 worktree A/B 会出现“新比新”盲区;必须再用批前
  pinned worktree 跑同一 frozen seed + raw table dump,变异也必须由 pinned hash 抓红;(b) fresh 隔离 PG schema 在 catalog 表尚未
  migration-bootstrap 时,首个 unqualified read 会沿 `search_path=<test>,public` 命中 public 历史表;A/B/测试在 empty-read 前显式
  `adapter.ensure_bootstrapped()`,再确认/清空隔离 schema 内表,禁止把 public 行当 seed;(c) repository 短名 `list` 会在类作用域遮蔽
  后续注解里的 built-in `list`;后续返回注解用 `builtins.list`,fail-closed helper 的真实类型是 `NoReturn`,共享基类和 repo 一起纳入 mypy。

## 7. 待 owner 决策(决策卡格式,2026-07-09 升级;每卡一问、有推荐、有截止、有超时默认)

### D-1 ③ jsonb/timestamptz 迁移窗口

- **单一问题**:是否批准 ③ 的 jsonb/timestamptz 生产数据迁移窗口(合同级,需停机窗口 + 明确 GO)?
- **选项**:(a) ② 全域收官后立即排窗口 —— ③ 设计已 RATIFIED(`TRACK_B_B4_2_PG_NATIVE_STORE_DESIGN.md` §4,
  tolerant-read-first、staged、可回滚),越晚做迁移面越大;(b) 推迟到 Track C 容器化部署窗口一起停机 ——
  一次停机做两件事,但 ③ 的收益(descriptor 一行 Kind 切换、GIN 查询)全部延后。
- **推荐**:(a)。②.3 及后续每多迁一域,③ 的 mapper 兼容面就多一块。
- **截止**:2026-07-31;**决策人**:owner。
- **超时默认**:维持 TEXT 列现状(安全、无停机),③ 冻结并在本卡记一次顺延;② 系列不受阻。

### D-2 `control_plane_postgres.py` on-disk-SQLite 导入/导出工具退役

- **单一问题**:这对独立的 on-disk-SQLite 导入/导出工具(非运行时路径,仅数据搬运)现在退役还是保留?
- **选项**:(a) 退役删除 —— storage.py 已 PG-pure,工具的"从旧 SQLite 导入"场景已随影子退役消失;
  (b) 保留到首次生产数据迁移(③)完成 —— 万一需要从历史 SQLite 备份补数据。
- **推荐**:(b) 保留但标注 deprecated + 不再维护,③ 完成后自动转 (a)。
- **截止**:随 D-1 裁决;**决策人**:owner。
- **超时默认**:(b)(保留不动,零风险)。

### D-3 lovable_board 分子契约

- **单一问题**:看板卡片计数分子取"已合入看板的可见 patch 数"还是"全部投影成员数"(112/297 vs 186/297 族)?
- **选项**:(a) 可见 patch 分子 —— 与前端当前渲染一致;(b) 投影成员分子 —— 与导出/CRM 计数一致。
- **推荐**:无强推荐 —— 这是产品语义,需 owner 从用户视角裁决(证据:`RESIDUAL_LEDGER.md` R-001/R-007 的三组失败数字)。
- **截止**:2026-07-31(投影计数契约化是 Track C serving 的前置);**决策人**:owner。
- **超时默认**:维持现状,R-001/R-007 继续 accepted 并在台账顺延一次(顺延即在该行追加日期)。

### D-4 positional `bulk_upsert_rows` 的 fail-closed 修复范围

- **单一问题**:是否批准在 serving members/search-index 迁移前,把所有经
  `_call_control_plane_postgres_native` 的 positional `bulk_upsert_rows` 调用改为显式
  `table_name=` / `rows=`,并用 fast guard 禁止复发?
- **选项**:(a) 同批修完 4 点(`serving_projection_members`、`projection_person_search_index`、
  `asset_membership_index`、`candidate_materialization_state`)并补 guard —— wrapper 只从 keyword 或 facade-name map
  识别 strict table;当前 4 点均落成 `strict_no_fallback=False`,adapter 异常被吞,其中 search-index 可假报 indexed;
  (b) 只在 ②.3c/d 修前两点 —— 改动最小,但同一根因仍留两点;(c) 扩 wrapper 从 positional args 推断表 ——
  兼容现状,但把隐藏 heuristic 固化进共享基座,并与 repository 的显式 table contract 分叉。
- **推荐**:(a)。4 点修复有界,同时退休根因;不要扩大 shared inference。
- **截止**:2026-07-31;**决策人**:owner。
- **超时默认**:执行 (a) 的显式 keyword 修复与 fast guard,不改 shared positional inference。落地前四个受影响
  写面不得进入 live/W6/manual/里程碑签收;②.3b 与其他模块开发不受阻。
