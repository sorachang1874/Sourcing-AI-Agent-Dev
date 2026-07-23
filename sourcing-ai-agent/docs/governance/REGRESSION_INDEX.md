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
| `tests/test_recovery_band_tail.py`（salvage waves 9+10, 2026-07-22）| recovery 带尾部合同（local-apply 闭包属主/合并、discovery 队列 legacy backfill+显式 recovery_kind、pre-retrieval refresh 同步/跳过、worker 清理退休、takeover supervisor、blocking-run 触发恢复（校准：阻塞期重试多次,合同=为本 job 触发）、daemon 生命周期日志、remote-event 二级 worker 恢复、supervisor 即时恢复、零 stale 配置、former 并行启动、outreach 线程非 daemon、limiter 满时跳 refill 扫描）| 从冻结 `test_pipeline.py` 移植 ×16（2 helper 随迁）；1 件改判 SUPERSEDED（discovery retry-wait drain 已迁 durable 命令面,legacy item 载体退出常规路径,现代覆盖= `test_seed_discovery.py` R-010 retry_wait 族——并入 T-004 语义）；棘轮 333→316 | PG fixture |
| `tests/test_acquisition_resume.py`（wave 8 追加, 2026-07-22）| hosted acquisition-resume 派发合同（不持 job lock 派发、无内联 resume 的线程派发、detached runner+marker 去重、lease 存活跳过、progress auto-recovery 对 fresh hosted dispatch 跳过）| 从冻结 `test_pipeline.py` 移植 ×5 追加入 wave-1 文件,一次全绿；棘轮 338→333 | PG fixture |
| `tests/test_runtime_health_status.py`（salvage wave 7, 2026-07-22）| runtime health/进度/operator 状态合同（progress schema 汇总、remote-wait+recovery 算 progressing、blocked acquisition worker 分类、tick runtime 心跳、operator 模式 worker-daemon 状态聚合）| 从冻结 `test_pipeline.py` 移植 ×7,一次全绿零校准；棘轮 345→338 | PG fixture |
| `tests/test_completed_reconcile_open_work.py`（salvage waves 5+6, 2026-07-22）| completed-workflow 后台 reconcile（tick 发现 pending、exploration 结果 reconcile、completed-worker 快照胜 stale result view）+ open-work 分类边界（provider-owned refill 尾不算 daemon-owned ×2）+ 死 lease preflight 修复 | 从冻结 `test_pipeline.py` 移植 ×7（idempotency 件经定向探针排除回归疑云后校准落地：apply 恰一次+consumed 标记完好,二次调用 settled `completed`,事件相位 delta-serving 化）；1 件深法证余留（board_visible 非 daemon 计数 2≠1）；棘轮 352→345 | PG fixture |
| `tests/test_queue_workflow_dispatch.py`（salvage wave 4a, 2026-07-22）| queue_workflow 派发决策（join-inflight 精确/family-signature/幂等键优先、tenant 栅栏、force-fresh 保留（评审/显式）、registry 优先于 completed 查询专属快照、投影背书复用）| 从冻结 `test_pipeline.py` 移植 ×9（2 helper 随迁）；余 4 件（registry 无 family 复用/former-lane delta/force-fresh 抑制/delta 标签回退）dispatch 判定随 B1 former 统一与 B3 Step 3 位移,待 4b 法证校准（机制笔记在归属表）；棘轮 361→352 | PG fixture |
| `tests/test_recovery_tick_characterization.py::RecoveryTickYieldLadderSalvageTest`（salvage wave 3, 2026-07-22）| tick 让位/优先梯 pin（board_visible 优先于 local_apply、remote-wait 不阻 ready board_visible、事件级 apply 后 refill/独立可见性让位、daemon-owned 开放工作先于 workflow_resume、provider-control 开放时可见性限流、idle refill 不触发 drain）| 从冻结 `test_pipeline.py` 移植 ×8,**加法**入 oracle 文件独立类（复用其 fixture,基类合同只字未动,无继承重跑）；棘轮 369→361 | PG durable fixture |
| `tests/test_worker_completion_pipeline.py`（salvage wave 2, 2026-07-22）| worker-completion 事件管线（micro-batch+syncs-once+cursor、signal-refill-before-apply 次序、prefetch 失败 repickable 不留 gating ingest 标记、discovery item 先闭合后 local-apply）——`_handle_completed_recovery_worker_result` 族首获现代覆盖 | 从冻结 `test_pipeline.py` 移植 ×10（4 个平坦 helper 随迁）；2 处按现行合同校准：local-apply 闭包与 deferred compaction 已迁 durable 命令面（`linkedin.local_profile_delta.apply` 队列断言替代 materialization item；compaction 调度归 `schedule_full_snapshot_compaction` 政策门,残差跟进在归属表）；棘轮 379→369 | PG fixture |
| `tests/test_acquisition_resume.py`（salvage wave 1a, 2026-07-22）| acquisition-resume 控制器与就绪合同（陈旧 lease 回收、后台 current-roster 基线就绪、post-profile local-apply/board-visible 两道 resume 屏障、superseded worker 终态谓词）——`_resume_*_if_ready`/`_assess_acquisition_resume_readiness` 首次获非冻结覆盖 | 从冻结 `test_pipeline.py` 移植 ×5（组 1 共 11,1b 已校准移植 after_workers_complete（异步政策门完成合同：resumed+running 载荷+store 轮询收敛 completed）,auto_resumes 改判 SUPERSEDED,余 4 件照已验证配方续,见 RECOVERY_BAND_OWNERSHIP 表）；需 `SOURCING_COMPANY_REGISTRY_RUNTIME_DIR` env 补丁随迁；棘轮 388→383→381 | PG fixture |
| `tests/test_runtime_read_resilience.py`（forensic salvage, 2026-07-22）| PG 时代运行时韧性合同（deferred runtime-control 持久化对瞬态 PG 失败重试/永久失败不烧尝试、公共 worker 读端点 runtime 读失败回退 persisted、`storage.is_transient_control_plane_error` 分类器双向 pin）| 从冻结 `test_pipeline.py` 法证移植 ×2 + 新分类器覆盖 ×2；**打捞暴露真产品缺陷**：重试环仍只捕 sqlite3『database is locked』,瞬态 PG 错误从不重试——根修 = storage 中央瞬态分类器 + orchestrator 重试改用之；棘轮 428→425 | PG fixture |
| `tests/test_orchestrator_planning.py`（salvage wave 1, 2026-07-22）| plan_workflow 意图推断合同（Gemini→Google/ChatGPT→OpenAI scope、effective-request 元数据、未知关键词保留）| 从冻结的 `test_pipeline.py` 移植（原名同名 ×6（wave 5 追加 identity manual-override 与 cached-authoritative-baseline，两 helper 随迁；forensic wave 追加 intent-axes-only normalization——校准：acquisition_lane_policy.keyword_priority_only 直通已退役,语义归 acquisition_strategy cost_policy 派生；Gemini scope 测试按 WS1 Step 3 suborg 合同翻转 full_company_roster），PG fixture 重座；棘轮 461→457→…→425 | 无 PG 之外约束 |

## Tombstones（退役测试墓碑;删除祖父基线文件必须先落行）

| id | test 文件/家族 | 保护过什么 | 退役原因 | 日期 | superseded_by |
|---|---|---|---|---|---|
| T-003 | `test_pipeline.py` 死白盒 13 件（cleanup-duplicate-inflight ×2、runtime-health ×2、job-progress auto-recovery ×3、release-stale-job-lease ×4、file-lock recovery ×1、harvest-prefetch overlay recovery ×1；共 31 处 `store._lock/_connection` 引用）| SQLite 时代的 job-lease/文件锁/runner 存活轮询恢复语义 | 架构已被 Phase 4 PG lease+事件驱动恢复取代；测试自 PG-pure store 起引用不存在属性（瞬间 AttributeError，零现役覆盖，R-009 证据）| 2026-07-22 | 现行恢复合同归 `test_recovery_*.py` 家族 + `test_operation_runtime.py` lease 族；现代覆盖若有缺口属新测试工作，不是复活这批 |
| T-004 | `test_pipeline.py` recovery 带 SUPERSEDED 37 件（tick refill/让位/budget 梯 ×9、run_worker_recovery_once 相位/远程等待/跳过梯 ×13、sidecar/bootstrap ×5、hosted 派发 ×2、queue_workflow 复用/review-scope ×6、auto-recovery/进度触发 ×1、worker-completion 桥 ×1）| 恢复 tick 相位序/refill owner/signal-only 触发/sidecar 生命周期/hosted 派发偏好/快照复用派发 | 现代套件已逐件确认同合同覆盖（tick oracle characterization、test_enrichment refill、test_profile_prefetch_scheduler_contract、test_recovery_trigger_signal_only、test_recovery_takeover_intent、test_recovery_sidecar、test_durable_runtime W6 normal-path 等）——**逐件证据行在 `RECOVERY_BAND_OWNERSHIP_2026-07-22.md`**（法证方法：pinned reason 串/相位键/符号级 grep 双向核验，非名字相似度）| 2026-07-22 | 见归属表 evidence 列；80 件 SALVAGE-CANDIDATE 按表分 10 组打捞波后才可继续删（1b 首件改判：auto_resumes_stale_acquiring 确认 takeover-intent 重路由接管,2026-07-22 并入本墓碑）|
| T-001 | `test_request_scoped_roster_shards.py::test_keyword_policy_subdivides_only_over_cap_function_roots` + 2 个 keyword_probe_policy 测试 | Step 4 keyword_union 超帽细分的验收设想 | 引用从未 land 的 `build_large_org_keyword_probe_shard_policy`（233a31a 半落地）;设计意图保存于 233a31a + master plan WS1 Step 4 | 2026-07-22 | Step 4 落地时的新验收套件（R-034 退出证据）|
| T-002 | 同文件 `test_plan_builds_request_shards_for_cohort_role_request`、`test_plan_manifest_parity_for_cohort_role_request` | cohort 角色经 legacy 元数据出 roster shard | 落地设计中 cohort 整体绕过 legacy 元数据走 CohortProviderCompiler | 2026-07-22 | `tests/test_cohort_*.py` 套件;归一见 master plan WS1 Step 5 |
