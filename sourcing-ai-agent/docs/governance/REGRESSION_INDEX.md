# Regression Index — 测试在哪跑、为什么存在、退役去向

> Status: ACTIVE routing registry（harness R0 批,2026-07-22）。行级 replace-not-append;lane 成员 rationale 单源在 `tests/lane_manifest.py`（本文件引用不复述）;机器侧 changed-path 映射单源在 `src/sourcing_agent/regression_matrix.py`;已接受失败在 `docs/RESIDUAL_LEDGER.md`。墓碑规则见 `docs/governance/TEST_PROVENANCE.md`。

```
status: active          owner: operator
canonical-path: sourcing-ai-agent/docs/governance/REGRESSION_INDEX.md
ratified: 2026-07-22
last-verified: 2026-07-22
```

## Where things run（家族级路由）

| 测试家族 | 保护什么 | 在哪跑 | 运行约束 |
|---|---|---|---|
| GH contract lane（18 full + 5 partial 组）| PG-backed 公共合同面 | workspace `.github/workflows/backend-ci.yml`;成员与 -k 的 WHY 见 `tests/lane_manifest.py` | REQUIRE flags 把 PG skip 变 fail |
| 本地 contract lane（7 full + 4 partial 组）| 同上（本地变体 + 评审门 runner + CRM live 验证链）| `make`(`CI_PRE_AGENT_CONTRACT_CMD`) | 双消费者一致性由 `tests/test_lane_manifest.py` 守卫 |
| docs 门（banner/链接/快照预算/溯源/lane/mypy 棘轮）| harness 治理 | backend-ci docs-gate 块,离线先跑 | — |
| changed-path → 套件映射 | 定向回归选择 | `src/sourcing_agent/regression_matrix.py`（label+reason 为准）| — |
| 2026-07 事故回归四件（`test_artifact_cache.py`、`test_latest_snapshot_pointer.py`、`test_live_apify_dataset_salvage.py`、`test_recovery_remote_wait_orphan.py`）+ 收口新增（`test_live_schema_write_fence.py`、`test_mypy_ratchet.py`、`test_lane_manifest.py`、`test_provenance.py`）| 数据毁损/指针漂移/salvage/simulate 写入/棘轮机制 | **此前无 lane 归属（recon 缺口）**→ 本批起离线四件入 docs-gate 块;PG 依赖件走 regression_matrix | `test_live_apify_dataset_salvage.py` 起 subprocess,~1s×3 |
| `tests/test_serving_mesh_boundary.py`（2026-07-22,WS2 切片 2）| Block (a) resolver 抽取结构守卫：Edge B 钉死（paging 永不重解 candidate_source）+ resolver 模块依赖方向（不触命令带/paging/orchestrator import）+ 委托席防平行再实现 | GH lane 离线块（`tests/lane_manifest.py`）| 纯静态源检查,无 PG |
| `tests/test_pipeline.py` | （打捞-退役目标,master plan WS3 Tier 3）| **永不全量跑** | RESIDUAL_LEDGER R-009;PG 100 连接耗尽 |

### Ported from test_pipeline.py (salvage waves)

| 新文件 | 保护什么 | 来源 | 运行约束 |
|---|---|---|---|
| `tests/test_orchestrator_retrieval.py`（salvage wave 2, 2026-07-22）| execute_retrieval 合同（deterministic summary 默认、asset-population 默认视图跳过打分、snapshot-backed 结果的 job-result-view 持久化、stale candidate-source override 拒绝）| 从冻结 `test_pipeline.py` 移植 ×4（原名同名），PG fixture 重座 + `_write_company_snapshot_candidate_documents` helper 随迁；棘轮 457→453 | 无 |
| `tests/test_snapshot_normalize.py`（salvage wave 3, 2026-07-22）| normalize-snapshot 合同（历史 explicit-profile-capture 继承、大基线稀疏刷新复用、force-fresh 跳过继承、hot-cache 镜像+检索索引刷新、即时 candidate-artifact 物化；wave 4 追加：delta-baseline 等价复用、legacy serving 基线、同名 current canonicalize、manual-review 确认成员/非成员继承）| 从冻结 `test_pipeline.py` 移植 ×10（原名同名，`_write_snapshot_normalized_artifacts` helper 随迁）；1 处按现行合同校准（builder 身份线程化）；棘轮 453→448→443 | 无 |
| `tests/test_orchestrator_planning.py`（salvage wave 1, 2026-07-22）| plan_workflow 意图推断合同（Gemini→Google/ChatGPT→OpenAI scope、effective-request 元数据、未知关键词保留）| 从冻结的 `test_pipeline.py` 移植（原名同名 ×6（wave 5 追加 identity manual-override 与 cached-authoritative-baseline，两 helper 随迁），PG fixture 重座；棘轮 461→457→…→428 | 无 PG 之外约束 |

## Tombstones（退役测试墓碑;删除祖父基线文件必须先落行）

| id | test 文件/家族 | 保护过什么 | 退役原因 | 日期 | superseded_by |
|---|---|---|---|---|---|
| T-003 | `test_pipeline.py` 死白盒 13 件（cleanup-duplicate-inflight ×2、runtime-health ×2、job-progress auto-recovery ×3、release-stale-job-lease ×4、file-lock recovery ×1、harvest-prefetch overlay recovery ×1；共 31 处 `store._lock/_connection` 引用）| SQLite 时代的 job-lease/文件锁/runner 存活轮询恢复语义 | 架构已被 Phase 4 PG lease+事件驱动恢复取代；测试自 PG-pure store 起引用不存在属性（瞬间 AttributeError，零现役覆盖，R-009 证据）| 2026-07-22 | 现行恢复合同归 `test_recovery_*.py` 家族 + `test_operation_runtime.py` lease 族；现代覆盖若有缺口属新测试工作，不是复活这批 |
| T-001 | `test_request_scoped_roster_shards.py::test_keyword_policy_subdivides_only_over_cap_function_roots` + 2 个 keyword_probe_policy 测试 | Step 4 keyword_union 超帽细分的验收设想 | 引用从未 land 的 `build_large_org_keyword_probe_shard_policy`（233a31a 半落地）;设计意图保存于 233a31a + master plan WS1 Step 4 | 2026-07-22 | Step 4 落地时的新验收套件（R-034 退出证据）|
| T-002 | 同文件 `test_plan_builds_request_shards_for_cohort_role_request`、`test_plan_manifest_parity_for_cohort_role_request` | cohort 角色经 legacy 元数据出 roster shard | 落地设计中 cohort 整体绕过 legacy 元数据走 CohortProviderCompiler | 2026-07-22 | `tests/test_cohort_*.py` 套件;归一见 master plan WS1 Step 5 |
