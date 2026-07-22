# Refactor Master Plan — 重构续章 + harness/playbook 持续完善

> Status: ACTIVE master plan（operator 四项裁决批准 2026-07-22；基于 13-agent 侦察 + 补缺轮 + 完备性批评家，测量基准 pin 在 `69b7423`）。本文件是重构续章的唯一权威计划；批次完成后在 §7 打勾并同步 NEXT_TODO，replace-not-append。

```
status: active          owner: operator
canonical-path: sourcing-ai-agent/docs/REFACTOR_MASTER_PLAN.md
ratified: 2026-07-22    measurement-pin: 69b7423
last-verified: 2026-07-22 (B0a+B0b done)
```

## 0. 使命与三条主线

1. **重构续章**：把 acquire 策略统一到「按参数分 shard（location、functionID、employment_status…）保 recall」的 canonical 抽象；Track A 单体分解重开；测试退役与 god-file 拆分；mypy 81 清零。
2. **harness 持续完善**：lane 单源、测试溯源门、mypy 棘轮、文档门——每次踩坑固化为 gate/registry，不留口头教训。
3. **playbook 回流**：把本仓库验证过的做法蒸馏进 `github.com/sorachang1874` 的 ai-assisted-engineering-playbook（sibling repo，重整前先 pull）。

## 1. 已批准的裁决（2026-07-22，不再重议）

| 决策 | 裁决 |
|---|---|
| mypy 范围 | 棘轮基线进 CI + 抽取/触碰即扩圈零预算（`disallow_untyped_defs`）；81 按 A(注解~51)→D(1)→B(重名15)→C(不变量15，逐个确认) 烧 |
| test_pipeline.py | 打捞移植（带溯源清单）后**删除**，关 R-009 |
| Track A Block (a) | resolver 抽取**前移**批准（计划修订：+8.4k 回涨实证冻结策略失效） |
| 起跑 | R0 harness 地基 ∥ 策略 Step 1 并行（零文件交集） |
| 策略方向（7-19 已录 BOARD） | 无大小公司之分：ONE unified abstract method with explicit parameters；org-size 只能是 shard 参数来源，不能是策略选择器 |

## 2. 工作流 WS1 — acquire 策略统一（5 步梯）

现状：canonical 抽象已存在（`company_shard_planning.py`，current-roster lane 已全迁移）。距离在四处：org-size 策略转向（`organization_execution_profile.py:289-297` large⇒scoped_search_roster，喂入 `acquisition_strategy.py:825-855`）；current/former 任务分叉（两套 shard schema、两套 merge、former-only 请求绕过 per-function shard 计划 `acquisition.py:980-981` vs `:4693`）；scoped_search_roster 独立关键词种子池（`seed_discovery.py:2420-2508` 第二套 probe 合同）；per-company 特例（anthropic `acquisition.py:957-1012`、Google scope URLs、`_HARD_LARGE_COMPANY_KEYS`）。

- **Step 1（无行为变更，起跑批）**：删死分支 `plan_review.py:647-658` 注释错误部分（注意 `:647-658` 的 empty-policy+explicit-shards 是 LIVE 合同，死的只是 no-policy 注释语义——见 pgLegacy 事实[13]）；去 `build_default_company_employee_shard_policy` 死参数；HARVESTAPI_PLAYBOOK `:215/:303` 大小公司措辞改 size-agnostic；COHORT_SELECTION_CONTRACT `:174` 改为真实三级 function 选择权威；**R-034 收口**（`test_request_scoped_roster_shards.py` 引用不存在的 `build_large_org_keyword_probe_shard_policy`——裁决：删引用重写到现行合同，keyword probe 并入留给 Step 4）。
- **Step 2（中）**：former lane 统一到 roster shard schema（employment_status 成为一等 shard 参数）；former-only 请求走 `build_request_scoped_former_search_shard_plan`；merge 走 `resolve_segmented_roster_completion`。⚠️ 触碰付费 payload 映射 `harvest_connectors.py:6350-6409`（GDM functionIds['19'] 事故守卫），保留 broad-former 守卫回归测试。**前置**：strategy_type/default_acquisition_mode 合同 preflight（现在不存在——建成 committed 测试，参照 `test_results_api.py:29419-29441` 模式）。
- **Step 3（大，评审门）**：退役 org-size 策略转向——`default_acquisition_mode` 不再翻 strategy_type；org_scale_band 降级为 shard 参数来源（分页/probe/复用限额 `:575-583`）。改 INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md:61（**合同修订**，7-19 board 指令即原则性批准，评审门仍走）。orchestrator 消费点：35164-35197/43432/64190-64207。
- **Step 4（大）**：scoped_search_roster 并入 shard planner 的 keyword_union 模式；退役 `seed_discovery.py:2420-2508` 第二 probe 合同与 `acquisition.py:974-984` 分叉；`_infer_strategy_type` 塌缩。
- **Step 5（架构议案，先设计后动）**：CohortProviderCompiler 与 company_shard_planning 归一为单一 plan artifact；investor_firm_roster 是否合法留在抽象外（资金图谱来源）→ operator 裁决。
- **遗留删除条件**（pgLegacy 硬计数 @69b7423，删除前重测）：`adaptive_us_technical_partition` 是**当前写默认**（`company_shard_planning.py:385`）——先改写默认+双接受各比较位点（acquisition.py:4849/5125/5322/5383/5428/7568；asset_reuse_planning.py:2263/2832），read-side shim 到终态行老化；no-policy 回退（`acquisition.py:2123-2129`）删除条件 = 终态化 8 条 stalled running 行（唯一可复活载体 job 386844bffa27）；acquire_former_search_seed 路径删除条件 = 裁决 2 个 stale pending plan_review_sessions（#2 OpenAI、#7 Google）。
- **live 脚本爆炸半径**：`run_live_large_org_regression.py:56/71/81`（strategy_type 字面断言）、`seed_test_env_assets.py:32`（registry 列拷贝）、`live_former_lane_run.py:34`（planner 签名）——Step 2-4 同批更新。

## 3. 工作流 WS2 — Track A 重开（三切片）

背景：Phase 0-4 形式完成，orchestrator 回涨 74,087→82,474（+8,353，Track C/D 落单体）。文档锚点全部过时 ~2.4k 行（PHASE4_ENTANGLED_CORE_DESIGN / SERVING_MESH_OWNERSHIP_BOUNDARY），切片 1 的 characterization pass 同批刷新。

- **切片 1**：Phase 4 Step 2b 级联簇迁移（profile_refill 链/workflow_resume 波/projection 串行/followup 尾环，`run_worker_recovery_once` orchestrator.py:40442-42605 → recovery_phases.py）。硬门 = `tests/test_recovery_tick_characterization.py` 10/10 byte-identical，oracle 不许弱化。
- **切片 2**：Block (a) CandidateSourceResolver 抽取（先断 Edge B——分页消费 resolver 输出而非重解析，再原样搬 `_resolve_job_candidate_source` + `_build_job_results_context`；§5 结构守卫同批）。
- **切片 3**：回涨带回归属主（b6a9169 track-d action schema ~2,018 行；f0ca302 C1b claims ~1,576 行→owner 模块），加**防再涨棘轮**（行数或 import-graph 守卫）。
- god-file 补充侦察结论（gap-close structures agent）：`workflow_smoke.py`(10,342) 是产品化了的 smoke harness，产品共享面只有 `HostedWorkflowSmokeClient`+`load_smoke_cases`（~150 行）——分解方向是把这两个符号抽到独立模块，其余向测试侧沉降；`cli.py` main() 3,711 行（~95 子命令 argparse+dispatch 梯）→ 子命令注册表化；`harvest_connectors.py` 内嵌 1,393 行 offline 测试 harness（4866-6258）→ 独立模块。

## 4. 工作流 WS3+WS5 — 测试治理（lane 单源 → 拆分 → 退役 → 溯源门）

权威普查（@69b7423）：tests/ 264 .py = 252 test_ + 12 helper；48 个 >1500 行 god-file；溯源标记覆盖仅 6%（2026-07 新写测试 100% A 级）。

- **R0 批（起跑，全离线）**：①lane manifest 单源化——backend-ci.yml(:100-135) 与 Makefile:137 双份 lane 不一致，建单源清单 + meta-guard 断言双消费者一致；②溯源门 Phase 0——`docs/governance/TEST_PROVENANCE.md`（标准文本已定稿于 recon，含 anchor regex/粒度规则/墓碑协议）+ 冻结祖父基线（只减不增）+ `tests/test_provenance.py` 入 docs-gate 块 + `docs/governance/REGRESSION_INDEX.md`（首批行：lane 成员的 -k WHY、regression_matrix 标签引用、4 个 2026-07 孤儿事故回归测试的 lane 归置）；③mypy 棘轮（见 WS4）；④固化数字：census 264/252/12、x-first 667（repo 内无基线记录，只有 stale 'Ran 608' 在 PRODUCT_INTEGRATION_SYNC.md:103/128——刷新之）、backend-ci.yml:16 的 '87' 改 '81'（归因 82d69a1）。**禁止批量补写假溯源——'provenance unknown; characterization adopted YYYY-MM-DD' 优于编造。**
- **拆分模板**（x-first 经验 + 三修正）：模块 1:1 映射 + fixture 先抽独立模块再拆（3 条跨测试文件 import 边是前车之鉴）+ 新文件必带 why-docstring；拆 lane 成员（尤其 21k 行 test_operation_runtime）前 lane manifest 必须已单源。
- **退役三梯**：Tier 1 = R-010 的 7 个 ordinal-key 测试重写到 q_hash 合同（骑策略批次的 seed_discovery 触碰）；Tier 2 = R-004 两个同步导出测试转 submit-drain-poll + R-012 teardown flake；Tier 3 = test_pipeline 打捞→删除（先冻结禁新增→按模块移植 planning/retrieval/normalize-snapshot 簇到 PG fixture 新文件→死白盒直接删→溯源清单记录 old→new→关 R-009）。**退役/拆分触碰 lane 合同 = 评审门触发。**

## 5. 工作流 WS4 — mypy 清零（棘轮 + 烧程）

81 = orchestrator 58 + workflow_smoke 21 + public_web_runtime_core 1 + candidate_artifacts 1（invocation：`make typecheck` → `.venv-tests/bin/mypy` 1.20.2，config = pyproject `[tool.mypy]`，follow_imports=skip 是低估杠杆）。R0 批：`configs/mypy_ratchet_baseline.json`（(file,error-code,count) 键，记 mypy 版本+config hash）+ `scripts/check_mypy_ratchet.py` + CI job；棘轮区分 sanctioned re-baseline 与回归。烧程 A→D→B→C；C 族每处是真不变量问题（orchestrator.py:24627 snapshot_dir=None、:75861 dict|str 臂可达性）需逐个确认+定向测试。扩圈规则见 §1 裁决。x-first 零 mypy 文化——greenfield 启用需网络装包，**单独征求 operator**。

## 6. 工作流 WS6 — harness/playbook 持续完善（常驻）

- TESTING_PLAYBOOK.md 打 PENDING-REFRESH 横幅（其全量 discover 指令与 R-009 冲突）并路由到新文档。
- 每批教训判断是否 gate 化/registry 化（本次收口范例：写入栅栏、live_promote 脚本、R-033/034）。
- playbook 回流批次：先 `git pull` sibling repo，再把已验证模式蒸馏成 playbook 条目（候选：probe-cost-before-execute、guarded-promotion dry-run 预测、写入栅栏 fail-closed 模式、salvage 收据纪律、多 agent 侦察→批评家→补缺工作流形态、residual ledger 棘轮）。
- 记忆升维：跨会话事实进 memory/，会话内进 BOARD，durable 进 git——三层不混。

## 7. 批次序列与状态（执行时打勾，replace-not-append）

碰撞矩阵：backend-ci.yml/Makefile 仅 R0 批触碰；seed_discovery.py 归策略批（R-010 退役骑行）；test 文件按批次独占。评审门协议：实现+定向测试+pin commit+记录评审请求后**继续下一批**，verdict 等 chshapi 配额；NO-GO 只冻结所涉 scope 晋升。

- [x] **B0a — R0 harness 地基** DONE 2026-07-22（mypy 棘轮:baseline 81 + check 脚本 + make 目标 + CI 活跑,'87' 勘误;lane manifest 单源+双消费者守卫;溯源门:契约文档+扫描器+213 祖父冻结基线+gate+REGRESSION_INDEX 含墓碑 T-001/002;4 个事故回归首次入 lane;x-first 基线 667 固化 `1eb0d7c`;离线门块 45/45 绿）
- [x] **B0b — 策略 Step 1** DONE 2026-07-22 `2198dd3`（签名级去 size 参数；死写分支删除；manifest base_filters 回退；R-034 关闭 52/52 绿；PLAYBOOK/COHORT 合同文档修正）
- [x] **B1 DONE 2026-07-22**（`374198f`+`baa0040`+`7b33073`+`632f4fb`+`1618aef`）：strategy preflight+翻转靶钉入 lane；Step 2a 执行派发统一（former-only 走 per-function former lane，评审请求在 re-fire 队列）；Step 2b-i merge 经共享完成度合同（截断=partial，永不虚报全覆盖）；Step 2b-ii planning 层铸造 former_function_shard_plan+review 同步重建+PIN_step2 翻转；R-010 关闭（seed_discovery 60/60）。**遗留给 Step 3/5**：size 转向退役（PIN_step3 待翻）；former 对 roster task 的 strategy 劫持形态归 Step 5 归一
- [~] **B2 进行中**：开局件 DONE `5c1d085`（tick oracle 基线 10/10 复验；test_pipeline 冻结棘轮入 lane 461 测试/42,416 行只减；Phase 4 两文档锚点勘误横幅+关键锚重测 run_worker_recovery_once=40442-42605；Block (a) 前移决定入档）。**级联迁移已开刀**：characterization 附录（实测锚 40442-42557/35 phase 调用/线程化清单）`6e43eaf` + 切片1 状态提升 `a7bd48c`+切片2 闭包升格 `e5a6bd2`+切片3 首簇 phase 入注册席 `f101196`（三变体 skip 梯逐字编码；ctx scope 旗填充缺口被 oracle 第二次开火抓获并修复）（drain/merge/work_observed 三纯函数入 recovery_phases，orchestrator -160 闭包行；oracle 曾抓住一次不完整纯度分析并当轮修正——硬门实战开火）`＋ test_pipeline 打捞移植（planning/retrieval/normalize-snapshot 簇 → PG fixture 新文件，冻结棘轮同批缩减）。**③ playbook 回流两批 DONE（循环已迭代）**：doc 25 `121d117` + doc 26（oracle 门控单体迁移：byte-identical 突变验证 oracle/三步切片梯/整体纯度教训/棘轮互锁）`c7abff5`，均在 GitHub main；预检两条策略告警待 operator（shell 代理 env 已设置；Clash GLOBAL=DIRECT）。**打捞一至四波+死白盒切除 DONE** `9ae85d4`+`8b6edbb`+`6b571e9`+`400a8dc`+T-003：4 个 plan_workflow 意图推断测试移植到 `test_orchestrator_planning.py`（PG fixture 重座 4/4 绿），god-file 461→457/42,309 行棘轮同批缩减，溯源映射入 REGRESSION_INDEX——打捞循环全链路已验证；第 5 候选（intent-axes normalization，引用已消失的 keyword_priority_only 键）留二波法证；累计 18 测试移植（planning 4/retrieval 4/normalize-snapshot 10），god-file 461→430/39,715 行（死引用 31→0），1 处合同演进校准（builder 身份线程化）。**mypy 烧程启动**：**81→27**（四文件清其三：public_web_runtime_core/candidate_artifacts/workflow_smoke 全零；余 27 全在 orchestrator——C 族不变量+尾部 overload 链；C 族修法示范：双解析阻断收窄→海象单解析、下游本 optional→如实标注，零 or-{} 蒙混）；sqlite3 注入 2 测试重归法证候选（行为意图在 PG 时代有对应物，需注入 psycopg 错误重设计）；剩余 430 测试 ~25 簇构成已直方图化（recovery/workflow ~90 个疑似 durable-runtime 套件已接管，逐簇归属核验待续）
- [ ] **B3 — 策略 Step 3（size 转向退役，评审门）** ＋ Track A 切片 2（Block a）＋ 拆分第一波
- [ ] **B4 — 策略 Step 4 ＋ 切片 3 回涨归位＋棘轮 ＋ mypy B/C 族 ＋ 拆分第二波**
- [ ] **B5 — 策略 Step 5 设计议案（operator 裁决后动）＋ test_pipeline 删除关 R-009 ＋ playbook 回流大批**
- 常驻：溯源祖父基线烧减、mypy 烧减、ledger/快照/记忆维护。
- 继承的收口跟进（见 NEXT_TODO Now 区）：shard lineage backfill、profile_fetched 对账、8 条 stalled running 终态化、2 个 stale pending review sessions 裁决（后两者是 WS1 删除条件前置）。

## 8. 完成定义

AGENTS.md Definition of Done 全款适用。额外：每批结束时 ①NEXT_TODO/PROGRESS 快照已刷 ②新 gate 绿且 lane 无静默漂移 ③评审请求（若触发）已 pin+入队 ④残差入 ledger 带 tripwire ⑤单体行数/棘轮数字只降不升。整个计划的退出判据：策略面单一抽象无 per-lane/per-size 分叉、orchestrator <60k 且带防涨棘轮、mypy 0/0、test_pipeline 已删、溯源祖父基线 <100、lane 单源、playbook 含本轮全部已验证模式。
