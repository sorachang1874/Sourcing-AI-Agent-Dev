# Residual Ledger（残差台账）

> Status: Living ledger. 已知失败预算与已接受残差的唯一权威清单（2026-07-09 起取代
> `TRACK_B_REPOSITORY_MIGRATION_HANDBOOK.md` §6 与 `NEXT_TODO.md` 中的两份互相漂移的预算列表，
> 两处现均指向本文件）。评审/审计对残差类 finding 必须引用行 id，而不是每轮重新论证。

## 规则

1. **入账前先归因**：新失败必须先做基线对照实验（**git worktree 基线副本 + `PYTHONPATH=<worktree>/src` 覆盖**，
   勿用 `git stash` —— 会翻共享工作树，见 handbook §6 效率纪律）证明 pre-existing，才能入账。
2. **每行必有 tripwire（到期条件）**：无 tripwire 的接受 = 有纸面记录的遗忘。tripwire 触发时该行必须被修复或重新裁决。
3. **计数类条目只降不增（棘轮）**：计数下降时同批把基线改小；上升 = lane 红，不允许静默增长。
4. **修复后不删行**：状态改 `closed` 留审计痕迹。
5. 评审终止规则见 `INDEPENDENT_REVIEW_BRIEF.md` Output Format：finding 引用本台账行 id 即不阻塞。

## 台账

| id | 范围 | 接受了什么 | 为何接受 | 正确修法 | tripwire（到期条件） | 归因证据 | 日期 | 状态 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| R-001 | `test_results_api` ×1 | `lovable_board_visible_patches` 看板分子计数失败 | owner 分子契约未裁决（见 handbook §7 决策卡 D-3） | 按裁决的分子契约修投影或修测试 | D-3 裁决落地即修；2026-07-31 前未裁决则按 D-3 超时默认处理 | 多轮 stash/worktree 复证 pre-existing | 2026-06-21 | accepted |
| R-002 | `test_settings` ×1 | settings runtime-override 命名断言失败 | pre-handoff 命名漂移，无可引用契约 | 契约化 runtime-override 命名后修 | settings/config 契约化工作启动时 | stash 复证 | 2026-06-21 | accepted |
| R-003 | `test_snapshot_materialization_backfill` ×1 | snapshot idempotent 0!=1 | backfill 探针读 legacy 表（对抗校验降级为 CONTRACT_DRIFT/low） | 探针改读 canonical 表 | 触碰 snapshot backfill 域的下一批 | wf_48a95eaf 对抗校验 | 2026-06-21 | accepted |
| R-004 | `test_results_api` ×2 | crm sync-export 契约漂移（`crm_public_web_promotions_survive_force_refresh`、`crm_public_web_service_e2e`） | C1.4 已把导出异步化，两测试仍断言退役的同步契约 | 转 submit→drain→poll（同已迁移的兄弟测试） | **C1.4 submit→drain→poll 转换落地即修，不许再增** | 批次 3 stash 复证 | 2026-06-21 | accepted |
| R-005 | `test_hosted_workflow_smoke` asset_population 组 | default_results_mode None 失败组 | pre-existing，与 Track B 无关 | 补 default_results_mode 解析或更新期望 | 触碰 hosted workflow smoke 的下一批 | ②.0 stash 复证 | 2026-07-02 | accepted |
| R-006 | `test_workflow_explain` ×1 | legacy-standard-bundle full-coverage-proof | pre-handoff uncommitted worktree state 遗留（2026-06-11 归因） | 按 explain 契约重写断言 | workflow explain 契约工作启动时 | ②.0 stash 复证 | 2026-06-11 | accepted |
| R-007 | `test_results_api` ×3 | 看板合并计数桶族（112/297 vs 186/297、115/140 vs 140/140、population floor 80≠297 的余量） | pre-handoff 投影/看板漂移，无可引用已提交契约 | 投影计数契约化后修 | 投影/看板计数契约化（Track C serving 工作）启动时 | 2026-06-12 归因；4e7f5d1 修复 floor 主因后余 3 | 2026-06-12 | accepted |
| R-008 | `test_control_plane_live_postgres` ×1 | `test_serving_projection_foundation_uses_live_postgres_tables`（2026-06-11 入账） | pre-handoff worktree state | —— | —— | 2026-07-09 套件 56/56 绿，条目已失效 | 2026-07-09 | closed |
| R-009 | `test_pipeline`（非 lane，全量 457） | postgres_only 下 ~15+ pre-existing 失败（含 ~33 个直写已删影子的 AttributeError） | SQLite 时代套件，永不全量跑（>90 分钟）；观察性验证只跑域相关 `-k` 子集 | test_pipeline 单独重设计（NEXT_TODO Track B 收尾项） | test_pipeline 重设计启动时；重设计前任何人不得把其失败归因到当前改动 | ②.0/②.1 多次 worktree 子集对照 | 2026-07-06 | accepted |
| R-010 | `test_seed_discovery` ×6 + `test_harvest_connectors` ×1（非 lane 全套件） | 7 个断言退役 ordinal query-key 契约（`::01`）的测试失败（现行为 identity-hash `q_<hash>`；lane 的 `-k` 子集守的正是新契约） | 全套件非 lane，identity-key 契约切换时未同步更新 fixture | 按 identity-key 契约更新 7 处期望 | 触碰 seed_discovery/harvest 全套件或 provider queue 契约的下一批 | **2026-07-09 worktree 基线对照（d5109f1）7/7 同败 —— fail-closed 合并无罪** | 2026-07-09 | accepted |
| R-011 | lint 门 mypy 段 | **87 条 pre-existing mypy 债务**（candidate_artifacts 1 / orchestrator 64 / public_web_runtime_core 1 / workflow_smoke 21；2026-07-06 基线） | 继承债务，非本期修复范围 | 分模块清偿 | **棘轮：只降不增** —— 每批验收 = 与基线计数逐字相同或更小；下降时同批重钉基线 | ②.1 lint 门基线 | 2026-07-06 | accepted |
| R-012 | `test_results_api` flake ×1 | `test_job_result_lifecycle_stage1_event_time_write_before_public_read` 全量跑偶现（PG teardown 竞态） | 隔离跑绿；对照归因 pre-existing flake | teardown join 线程（同 Phase 3b 尾巴） | fixture teardown 修缮批 | 2026-06-12 归因 | 2026-06-12 | accepted（flake，非计数预算） |

## 使用方式

- **批验收**：suite 失败 ⊆ 本台账未 closed 行 = 绿（green-modulo-ledger）；任何不在台账内的失败必须现场 worktree 归因，
  归因为 pre-existing 才可新增行（带全部列），否则就是你的回归。
- **评审引用**：跨模型评审（`INDEPENDENT_REVIEW_GATE.md`）把 finding 分类为 `residual` 时必须引用行 id；
  无 id 的 residual 分类无效。
- **到期巡检**：每批 step 6 文档收尾时扫一遍 tripwire 列，触发的行升级为当批工作项或决策卡。
