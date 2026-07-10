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
| R-009 | `test_pipeline`（非 lane，全量 457） | postgres_only 下 ~15+ pre-existing 失败（含 ~33 个直写已删影子的 AttributeError） | SQLite 时代套件，永不全量跑（>90 分钟）；观察性验证只跑域相关 `-k` 子集 | test_pipeline 单独重设计（NEXT_TODO Track B 收尾项） | test_pipeline 重设计启动时；重设计前任何人不得把其失败归因到当前改动 | ②.0/②.1 多次 worktree 子集对照；②.2 精确 5 项在迁移树/`c19c0fd` 均 3 pass/2 fail | 2026-07-10 | accepted |
| R-010 | `test_seed_discovery` ×6 + `test_harvest_connectors` ×1（非 lane 全套件） | 7 个断言退役 ordinal query-key 契约（`::01`）的测试失败（现行为 identity-hash `q_<hash>`；lane 的 `-k` 子集守的正是新契约） | 全套件非 lane，identity-key 契约切换时未同步更新 fixture | 按 identity-key 契约更新 7 处期望 | 触碰 seed_discovery/harvest 全套件或 provider queue 契约的下一批 | **2026-07-09 worktree 基线对照（d5109f1）7/7 同败 —— fail-closed 合并无罪** | 2026-07-09 | accepted |
| R-011 | lint 门 mypy 段 | **87 条 pre-existing mypy 债务**（candidate_artifacts 1 / orchestrator 64 / public_web_runtime_core 1 / workflow_smoke 21；2026-07-06 基线） | 继承债务，非本期修复范围 | 分模块清偿 | **棘轮：只降不增** —— 每批验收 = 与基线计数逐字相同或更小；下降时同批重钉基线 | ②.1 基线；②.2 / ②.3a / ②.3b / ②.3c 复验均为 87（1/64/1/21） | 2026-07-10 | accepted |
| R-012 | `test_results_api` flake ×1 | `test_job_result_lifecycle_stage1_event_time_write_before_public_read` 全量跑偶现（PG teardown 竞态） | 隔离跑绿；对照归因 pre-existing flake | teardown join 线程（同 Phase 3b 尾巴） | fixture teardown 修缮批 | 2026-06-12 归因 | 2026-06-12 | accepted（flake，非计数预算） |
| R-013 | Track B ②.2 independent review | 宿主机 Claude Opus/xhigh 与首个 Codex runner 分别只产出 `ConnectionRefused` / timeout，无 findings/verdict；两份无效 artifact 均未算 GO | owner 的 Claude 路径由 `reclaude` 拉起专用容器并连接独立 daemon，宿主机 `claude` 不走该 transport；owner 2026-07-10 改为接受独立只读 Codex reviewer session，同家族不再禁用 | 已由独立非作者 Codex session 对 pinned `c19c0fd..d0828e7` 完成对抗评审；低优先级永久 fault-test 建议留待下次触碰 manual_review 域 | —— | 无效：`20260710T021502Z_async-reference-track-b-2-2-manual-review.md`、`20260710T032015Z_async-reference-track-b-2-2-codex.md`；有效 GO：`runtime/reviews/20260710_async-reference-track-b-2-2-codex-subagent.md`（AST 14/14、fault 8/8、pipeline base/head 同 3/2） | 2026-07-10 | closed |
| R-014 | Track B ②.3b independent review | pinned scope `b3818f0..aaf13fb` 尚无 runner-verifiable GO；首轮只产 chat GO 未落 artifact，后续含一次 capacity / 两次 interrupted，2026-07-10 hardened-runner 重试又在 900 秒无输出超时并产 invalid artifact | telemetry 的 300 分钟窗口为 `used_percent=0.0`、`rate_limit_reached_type=null`，不支持 5h quota 耗尽；实际是 artifact 交付/会话收口失败叠加一次容量事件与一次 runner timeout，不是 repo/provider-live 失败 | Codex CLI/backend 能产完整输出且 rollout 持久化 effective tier 时，由独立非作者 session 重跑，artifact 必须含 `reviewer_exit_code=0` + hash-bound effective-config/rollout evidence + findings + GO/NO-GO | 异步重试；有效 GO 前只阻断 ②.3b 的 live/W6/manual/里程碑签收，其他开发继续 | author evidence：A/B 3+9、pinned hash `74cc0b13...d0f00`、direct 21+2+7、lane 190/0、mypy 87/4；invalid timeout=`runtime/reviews/20260710T090756Z_Track_B_2_3b_projection_manifest_shards_pinned_review.md` | 2026-07-10 | pending async Codex review |
| R-015 | Track B ②.3c fixed-forward independent review | pinned scope `40bdcf8..508799e` 尚无 runner-verifiable GO | 2026-07-10 canonical network preflight 证实 shell/git proxy 均未设置、DNS 为真实地址且 Clash GLOBAL 非 DIRECT，但 GitHub/API/raw/ChatGPT HTTPS 全超时，`gh auth status` 与 `git ls-remote` 同败；按 workspace 网络规则不得注入 proxy env、改 git proxy 或切换 Clash/TUN | reviewer transport 恢复后，用 operator 全局配置中的 newest model + highest effort 运行 canonical pinned runner；artifact 必须满足 v3 scope/effective-config/rollout 校验并给出 GO/NO-GO | 异步重试；有效 GO 前只阻断 ②.3c fixed-forward 的 live/W6/manual/里程碑签收，②.3d 与无关开发继续 | author evidence：projection/writer/surface 105+4 subtests、person/fault/API/legacy 30、operation 93、lane 255+2+11+1+2、mypy 87/4；commit=`508799e` | 2026-07-10 | pending async Codex review |
| R-016 | CRM Public Web `gpt-5.6-sol` product-model contract independent review | pinned scope `508799e..47ec9d0` 尚无 runner-verifiable GO | 与 R-015 同一次只读 preflight 的 HTTPS/backend 不可达；未启动无望完成的 reviewer，也未把作者/测试结果当 GO | transport 恢复后对该独立 scope 运行 canonical pinned runner，重点审查 pre-transport model lock、Qwen fail-closed、W7g exact provider-response identity 与全停重启合同 | 异步重试；有效 GO 前阻断该模型合同的 live W7g、manual/product 与里程碑签收，不阻断 simulated 开发或其他模块 | author evidence：model/provider/runtime-boundary 188+11 subtests、lane 255+2+11+1+2、dry_run_ready、mypy 87/4；commit=`47ec9d0`；没有 relay 接受性/价格/质量 live 证明 | 2026-07-10 | pending async Codex review |
| R-017 | Track B ②.3d projection person search index independent review | pinned scope `ea03da3..7b49f63` 已发独立只读 Codex subagent，对抗检查 Store→Repository 闭合、typed read failure、index/member 完整性、replace/upsert 共锁、job 409 与 frontend zero-count | owner 允许 Codex-only 异步评审且明确评审不得阻断无关开发；作者 A/B/lane 证据不能替代独立 verdict | reviewer 产出 hash-bound findings + GO/NO-GO artifact；`new` finding 异步修复，`re-raise` 记录，`residual` 必须引用本台账 id | 有效 verdict 落地即分诊；此前只阻断本 scope 的 live/W6/manual/里程碑签收，不阻断下一域 Scout/开发 | author evidence：A/B 8 pass、mutation 2 fail/6 pass、pinned 11,079 bytes hash `b429f29b...a40f30f`、core 69+3、live-PG 60+4、results 8、frontend 21、operation 93、lane 256+2+11+1+2、mypy 87/4；commit=`7b49f63` | 2026-07-10 | pending async Codex review |

## 使用方式

- **批验收**：suite 失败 ⊆ 本台账未 closed 行 = 绿（green-modulo-ledger）；任何不在台账内的失败必须现场 worktree 归因，
  归因为 pre-existing 才可新增行（带全部列），否则就是你的回归。
- **评审引用**：跨模型评审（`INDEPENDENT_REVIEW_GATE.md`）把 finding 分类为 `residual` 时必须引用行 id；
  无 id 的 residual 分类无效。
- **到期巡检**：每批 step 6 文档收尾时扫一遍 tripwire 列，触发的行升级为当批工作项或决策卡。
