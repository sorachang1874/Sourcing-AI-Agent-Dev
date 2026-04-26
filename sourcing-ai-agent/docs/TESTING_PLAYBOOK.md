# Testing Playbook

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义当前 Sourcing AI Agent 的标准测试分层，目标是把“手工 smoke 一次”升级成可重复、可扩展、可解释的回归体系。

适用范围：

- workflow 编排与阶段切换
- hosted 默认路径
- asset reuse / delta planning / explain
- recovery / watchdog / staged feedback
- 低成本 provider 模拟与长尾 scripted 场景

配套的独立测试环境入口见：

- [TEST_ENVIRONMENT.md](./TEST_ENVIRONMENT.md)
  - 用隔离 `runtime/test_env` + 默认 `simulate` 启动 serve / worker daemon / 前端
  - 适合作为前端联调、browser E2E、以及后续 CI 的固定 backend namespace

## Current Closeout Gate

`2026-04-25` 的稳定基线已经用下面这组命令验证。后续如果触碰 request semantics、planning、workflow orchestration、provider integration、results API、frontend plan/board hydration，至少应按影响面复跑对应子集。

```bash
./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q
./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q
cd frontend-demo && npm run build
bash ./scripts/run_python_quality.sh typecheck
./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q
./.venv-tests/bin/python -m pytest -q
```

最近一次全量结果：

- `1135 passed, 8 skipped, 25 subtests passed`
- browser E2E：`5 passed, 2 skipped, 2 subtests passed`

脚本级 workflow 行为验证还应覆盖：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/closeout_simulate_20260425 \
  --seed-reference-runtime \
  --fast-runtime \
  --strict \
  --timing-summary \
  --report-json output/closeout_20260425/simulate_report.json \
  --summary-json output/closeout_20260425/simulate_summary.json

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/scripted_long_tail_closeout_20260425 \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/google_multimodal_long_tail.json \
  --fast-runtime \
  --case google_multimodal_pretrain \
  --strict \
  --timing-summary \
  --report-json output/closeout_20260425/google_long_tail_report.json \
  --summary-json output/closeout_20260425/google_long_tail_summary.json
```

这些报告不只看 “completed”，还必须看：

- duplicate provider dispatch 是否为 0
- default-off `Public Web Stage 2` 是否没有误触发
- prerequisite-ready 到下游 stage 是否没有异常空转
- `Final Results` 到 board non-empty 是否没有超阈值延迟
- materialization streaming / writer-budget 是否没有慢 gap 或预算耗尽
- frontend plan label 是否来自后端 `effective_execution_semantics` / `dispatch_preview`

## 1. 测试分层

### 1.1 快速单元测试

用途：

- request normalization
- planner / dispatch / scoring / registry / artifact repair
- asset membership exact overlap / subsumption
- provider adapter 的 deterministic 逻辑

标准命令：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python -m unittest discover -s tests -v
```

更高效的本地入口：

```bash
cd "sourcing-ai-agent"
bash scripts/run_regression_suite.sh fast
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode durations
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --report-json output/test_reports/changed.json
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --max-parallel 2 --report-json output/test_reports/changed-parallel.json
```

说明：

- `run_regression_suite.sh fast`
  - 现在不再盲跑全量 `pytest -q`
  - 会根据当前 git 变更路径选择高信号 pytest 子集
- `run_pytest_matrix.py --mode changed`
  - 适合大型 Infra 升级阶段的日常快速回归
  - 当前对 `storage / control_plane / asset_sync / cloud_asset_import / candidate_artifacts / workflow_smoke`
    等核心模块做了映射
  - 这轮继续把 `frontend-demo / contracts / workflow explain` 也映射成更细的 suites，避免前端改动时盲跑一整套后端 smoke
  - 当前 `frontend-demo / contracts` 改动不仅会命中前端契约 pytest，也会在同一入口里自动跑 `frontend-demo` 的生产构建
- `run_pytest_matrix.py --mode durations`
  - 输出最慢测试列表，便于持续压缩全量回归耗时
- `run_pytest_matrix.py --report-json ...`
  - 会把本次选中的 suites、实际命令、退出码和耗时写到 JSON
  - 适合在长时间自主开发时沉淀“这轮为什么慢、慢在哪个 suite”这类结构化记录
- `run_pytest_matrix.py --max-parallel N`
  - 会把 changed/smoke 模式下互相独立的 suites 并发执行
  - 适合 `storage / control_plane / workflow / frontend build` 这种跨域改动后的本地快速回归
  - 当前仍保留 `full` / `durations` 的串行行为，避免全量基线统计被并发噪声干扰

使用时机：

- 每次非微小改动后的基础回归
- 修改 shared schema、intent view、planner、dispatch、storage、artifact pipeline 后

当前已覆盖的关键回归样本：

- workflow 编排层的业务级 guardrail 现另外沉淀在：
  - [WORKFLOW_BEHAVIOR_GUARDRAILS.md](./WORKFLOW_BEHAVIOR_GUARDRAILS.md)
  - 这份文档定义的是“重复 dispatch / disabled stage / prerequisite-ready 后应立即推进”这类行为 contract
  - 以后补 scripted smoke / simulate report 时，应优先把这些 contract 变成结构化 assertion，而不是停留在人工观察

- `tests.test_pipeline.PipelineTest.test_asset_reuse_plan_uses_exact_membership_subsumption_for_current_profile_search`
  - 防止 baseline candidate docs 没真正吸收 shard bundle 时，planner 仍误判为“已覆盖”
- `tests.test_control_plane_postgres.ControlPlanePostgresTest.test_upsert_acquisition_shard_registry_rows_splits_current_and_former_tables`
  - 防止 PG former/current 物理分表 cutover 后重新退回单表写入，或把 former/current shard 写错物理表
- `tests.test_results_api.ResultsApiTest.test_job_api_is_summary_only_by_default_and_detail_is_opt_in`
  - 防止 `/api/jobs/{job_id}` 再次默认返回全量 events/request detail，拖慢前端 progress fallback
- `tests.test_organization_execution_profile.OrganizationExecutionProfileTest.test_promotion_candidate_selection_does_not_chain_regress_on_explicit_baseline_inclusion`
  - 防止把 `evaluate_organization_asset_registry_promotion(...)` 误当成链式排序器，导致先选到更优 snapshot 后又被旧 aggregate row“反晋升”回去
- `tests.test_provider_execution_policy.ProviderExecutionPolicyTest.test_core_roster_strategies_do_not_require_explicit_high_cost_approval`
  - 防止 generic cost gate 重新长回 core roster/profile-search lane 的显式 gate
- `tests.test_pipeline.PipelineTest.test_build_sourcing_plan_full_company_defaults_do_not_require_high_cost_approval`
  - 防止 `full_company_roster` 的 former lane 又被 generic cost gate 降级回低成本 web search
- `tests.test_pipeline.PipelineTest.test_build_sourcing_plan_omits_public_web_stage_by_default`
  - 防止 `Public Web Stage 2` 又被重新塞回默认 acquisition 主链路，重复阻塞 board readiness
- `tests.test_pipeline.PipelineTest.test_single_stage_workflow_skips_public_web_stage_and_publishes_stage_progress_markers`
  - 防止 single-stage workflow 又错误地产生 `public_web_stage_2` summary / stage-order，导致 wall-clock 与前端时间线重新失真

排障规则：

- 当看到的是 durable state 错误时，先追 writer 链，不先停在 reader 层。
  - 例如 authoritative row / execution profile / registry summary 持久化错误，应先检查：
    - 谁在写 authoritative
    - 谁在 backfill / promote
    - 谁在 refresh existing profile
  - 不要只因为“第一个暴露症状的模块”是 planner 或 execution profile，就先在 reader 层打补丁。
- promotion guard 与 candidate selection 必须分开。
  - `evaluate_organization_asset_registry_promotion(...)` 这类函数只能回答：
    - “candidate 能不能替代当前 authoritative”
  - 不能直接拿来做多轮 pairwise 全序选择。
  - 若多个模块都要选 baseline candidate，应提 shared helper，再让 planner / backfill / execution profile 共用。

### 1.2 Hosted Explain Dry-Run Matrix

用途：

- 只走 `POST /api/workflows/explain`
- 不真正启动 workflow，也不触发 stage polling
- 低成本验证 request normalization / organization execution profile / asset reuse / dispatch / lane planning
- 把“应该 reuse 还是 delta，应该 full roster 还是 scoped search”编码成结构化期望

标准入口：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py --strict
```

当前默认 explain matrix 覆盖：

- Physical Intelligence：新组织 / runtime identity / full roster
- Reflection AI：小组织 + directional local reuse
- Humans&：小组织 + full local reuse
- Anthropic：中型组织 + baseline reuse
- OpenAI Reasoning：已有 directional selected snapshot reuse
- OpenAI Pre-train：large org scoped delta
- Google multimodal + Pre-train：partial shard covered + delta
- NVIDIA World model：large org fresh scoped planning

自动化回归：

- [tests/test_hosted_workflow_smoke.py](../tests/test_hosted_workflow_smoke.py)
  - `test_default_explain_matrix_covers_reference_regressions`
  - `test_hosted_explain_dry_run_matrix_covers_reference_regressions`
- [tests/test_workflow_explain.py](../tests/test_workflow_explain.py)
  - 继续承担 explain 细粒度单测与 planning 语义断言

输出重点：

- `summary.target_company`
- `summary.keywords`
- `summary.org_scale_band`
- `summary.default_acquisition_mode`
- `summary.dispatch_strategy`
- `summary.dispatch_matched_job_id`
- `summary.dispatch_matched_job_status`
- `summary.baseline_directional_local_reuse_eligible`
- `summary.current_lane` / `summary.former_lane`
- `summary.plan_current_task_strategy_type`
- `summary.plan_current_search_seed_queries`
- `summary.plan_current_filter_keywords`
- `summary.plan_current_query_bundle_queries`
- `summary.plan_former_task_strategy_type`
- `summary.plan_former_search_seed_queries`
- `summary.plan_former_filter_keywords`
- `summary.plan_former_query_bundle_queries`
- `summary.timings_ms`
- `summary.timing_breakdown_ms.prepare_request`
  - 现在已经细拆到：
    - `llm_normalize_request`
    - `deterministic_signal_supplement`
    - `canonicalize_request_payload`
    - `final_request_materialization`

自定义矩阵：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --matrix-file configs/explain_dry_run_matrix.example.json \
  --strict
```

隔离 runtime 运行：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --runtime-dir runtime/test_env/explain_matrix \
  --seed-reference-runtime \
  --fast-runtime \
  --strict
```

说明：

- 这层是推荐的第一道回归，优先用来挡住 `Physical Intelligence / OpenAI Pre-train / OpenAI Reasoning` 这类 planning 级回归
- 如果 explain dry-run 已经失败，不要直接去跑完整 hosted workflow smoke
- 脚本级 explain matrix 还有一个需要单独区分的现象：`runtime 数据漂移`
  - `scripts/run_explain_dry_run_matrix.py --strict` 如果直接打当前本地 runtime，它读取的是你当前真实状态：
    - 本地 `company_assets`
    - 本地 Postgres / job history
    - 当前 authoritative registry / selected snapshots / generation
  - 因此它失败时不一定代表代码回归，也可能只是“默认矩阵预期仍按旧 runtime 假设写的”，而你本地资产已经变了
  - 典型例子是某公司原本在默认矩阵里预期 `new_job`，但你本地已经有 authoritative baseline，于是实际会变成 `reuse_snapshot`
  - 所以：
    - 要做稳定回归，用 `--runtime-dir` 指向隔离 test runtime
    - 要做“近真实本地数据”的 scripted 验证，用 `seed_test_env_assets.py` 先把当前 authoritative snapshots 种进专用 `runtime/test_env/...`；该脚本要求 Postgres control plane，不再读取磁盘 SQLite fallback
    - 不要把“直接打当前本地 runtime 的 explain matrix 预期不符”机械当成代码 regression
- `--runtime-dir`
  - 脚本会自起一个 in-process backend，并把 `SOURCING_RUNTIME_DIR` 指向该目录
  - 若未显式传 `--runtime-env-file`，脚本会在该 runtime 下写一个空的 local-postgres env sentinel
  - 目的是阻断仓库根 `.local-postgres.env` 或当前 shell 导出的 PG 变量泄漏进测试 runtime
- 推荐使用 `./.venv-tests/bin/python`
  - 避免系统 `python3` 与 repo 依赖、`dataclass(slots=True)` 兼容性、`requests/psycopg` 安装状态不一致
- 如果你要做“接近本地真实数据”的 scripted explain，而不是 reference seed：
  - 先把目标公司 authoritative snapshot 种到隔离 runtime
  - 再让 explain/smoke 脚本直接指向这个 `runtime/test_env/...`
  - 推荐两步：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/seed_test_env_assets.py \
  --source-runtime-dir runtime \
  --target-runtime-dir runtime/test_env/local_like_explain \
  --company OpenAI \
  --company Anthropic

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --runtime-dir runtime/test_env/local_like_explain \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/reflection_pending.json \
  --strict
```

### 1.3 Hosted Simulate Smoke

用途：

- 用真实 hosted API 路径验证 `plan -> review -> workflow -> progress -> results`
- 不触发真实 Harvest / Search / model / semantic 调用
- 验证不同组织规模的默认执行画像是否正确

标准入口：

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --strict
```

隔离 runtime 入口：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/simulate_matrix \
  --seed-reference-runtime \
  --fast-runtime \
  --strict
```

当前默认 smoke matrix 覆盖：

- Skild AI
- Humans&
- Anthropic
- OpenAI
- Google

自动化回归：

- [tests/test_hosted_workflow_smoke.py](../tests/test_hosted_workflow_smoke.py)
  - `test_default_smoke_matrix_covers_reference_orgs`
  - `test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs`
    - 默认日常回归，只跑 3 条代表性 flow：
      - small: `Skild AI`
      - medium: `Humans&`
      - large: `OpenAI`
  - `test_hosted_simulate_full_smoke_matrix_completes_when_enabled`
    - 全量 5-case hosted 回归入口
    - 需显式设置 `SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1`
  - `test_hosted_simulate_reuse_queries_preserve_follow_up_planning_contract`
    - 覆盖 `Reflection AI Post-train` 与 `OpenAI Reasoning`
    - 先跑完整 reuse workflow，再立刻做 follow-up explain
    - 断言不会因为 workflow 写回 control-plane 后把下一次 query 错误打回 `live_acquisition` 或 `delta_from_snapshot`
  - `test_hosted_simulate_completed_history_round_trip_exposes_results_recovery`
    - 覆盖 `history_id -> results -> /api/frontend-history/{history_id}` 恢复链路
    - 防止历史记录可以打开但候选人看板为空、或恢复后丢失 `job_id/results` 关联

适用场景：

- hosted 默认路径改动
- explain / dispatch / asset reuse 改动
- request normalization / organization execution profile 改动
- staged feedback / stage summaries 改动
- cloud import / GC ledger、generation watermark、runtime progress observability 改动

建议运行方式：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python -m unittest tests.test_hosted_workflow_smoke -v
SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1 PYTHONPATH=src ./.venv-tests/bin/python -m unittest tests.test_hosted_workflow_smoke -v
```

说明：

- 默认 `unittest` 保持 3-case，是为了把日常回归时长压在可接受范围
- 全量 5-case 仍是正式回归入口，不再只存在于脚本里
- `scripts/run_simulate_smoke_matrix.py` 仍保留完整默认 matrix，适合手工 smoke / 运维排障 / 新场景验证
- `--runtime-dir` 适合把 smoke 固定在 `runtime/test_env/...` 下的专用目录，而不是直接打日常开发 runtime
- 如果希望 smoke 更接近本地真实资产，而不是 reference fixture：
  - 先用 `scripts/seed_test_env_assets.py` 从当前 Postgres control-plane runtime 复制/链接 authoritative snapshot
  - 再用 `--runtime-dir` + `--provider-mode scripted` 跑 case report
- smoke 结果里现在也会带更多 explain 侧结构化摘要，便于直接看出：
  - 为什么是 `reuse_snapshot` / `delta_from_snapshot` / `new_job`
  - 当前 / former lane 计划用了哪些关键词
  - 实际 review gate 后是不是仍保留高成本 provider 路径
- API 侧也新增了一条默认测试约束：
  - `/api/jobs/{job_id}` 默认只返回 summary-first 轻量 payload
  - `/api/jobs/{job_id}/results` 默认只返回 summary/counts，不自动带 full candidates
  - 只有需要完整详情的 smoke / browser / 人工链路才显式加：
    - `?include_details=1`
    - `?include_candidates=1`
- `scripts/run_simulate_smoke_matrix.py` 现在也支持 timing aggregation：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --report-json output/scripted_smoke/report.json \
  --summary-json output/scripted_smoke/summary.json \
  --strict \
  --timing-summary \
  --max-total-ms 2500 \
  --max-wait-ms 1800
```

说明：

- `--timing-summary`
  - 把 `explain / plan / review / start / wait_for_completion / fetch_job_and_results / dashboard_fetch / candidate_page_fetch / total`
    的聚合统计输出到 stderr
  - 同时会汇总 `provider_case_report`：
    - `linkedin_stage_1 / stage_1_preview / public_web_stage_2 / stage_2_final` 的 wall-clock 聚合
    - `workflow_wall_clock_ms`
      - `job_to_stage_1_preview`
      - `job_to_final_results`
      - `stage_1_preview_to_final_results`
      - `final_results_to_board_ready`
      - `job_to_board_nonempty`
    - `board_ready_count / board_ready_nonempty_count`
    - `progress_regression_case_count / progress_sample_count / progress_maxima`
    - `strategy_rollups`
      - `effective_acquisition_mode`
      - `dispatch_strategy`
  - 单 case 结果里也会直接带结构化 `provider_case_report`，可用于检查：
    - `search query_count / queued_query_count`
    - `search_seed_added_entry_count`
    - `fetched_profile_count`
    - `candidate source` 与 `board ready` 观测
    - `progress_observability`
      - worker / remote-wait 峰值
      - monotonic counter regression
      - `manual_review_count` backlog reduction
      - `terminal_progress_lag_detected`
    - `workflow_wall_clock_ms`
      - 优先读 backend stage summary
      - 若 backend 没稳定带时间戳，则回退到 client-observed timeline + smoke timings
- `--max-total-ms`
  - 对 `total` 的 aggregate `p95` 做回归守卫
- `--max-wait-ms`
  - 对 `wait_for_completion` 的 aggregate `p95` 做回归守卫
- `--report-json / --summary-json`
  - 直接把 full per-case report 与 aggregate summary 落到文件
  - 推荐 scripted smoke / benchmark 统一使用，避免再依赖 shell redirect 抓 stdout/stderr
- 最新一轮 fresh isolated smoke 参考目录为 `output/scripted_smoke_v4/`：
  - `openai_reuse`
    - total 约 `1505.78ms`
    - `job_to_board_nonempty` 约 `1020.23ms`
  - `google_scoped_cold`
    - total 约 `5144.26ms`
    - `job_to_board_nonempty` 约 `1076.37ms`
  - `xai_live_roster`
    - total 约 `18825.6ms`
    - `queued_worker_count / waiting_remote_harvest_count / pending_worker_count` 峰值均为 `3`
- 当前 per-case 顶层和 nested `provider_case_report` 已基本对齐：
  - 顶层现在也会直接导出：
    - `stage_summary_digest`
    - `stage_wall_clock_ms`
    - `workflow_wall_clock_ms`
- 若要看最完整的 provider-grade 结构，仍优先看 nested `provider_case_report`：
  - `stage_wall_clock_ms`
  - `workflow_wall_clock_ms`
  - `progress_observability`
  - `counts`
  - `behavior_guardrails`
    - `duplicate_provider_dispatch`
    - `disabled_stage_violations`
    - `prerequisite_gaps`
    - `final_results_board_consistency`

- 现在 scripted smoke/report 已能直接回答几类之前需要人工翻日志的问题：
  - 同一 provider payload 是否被重复 dispatch
  - default-off 的 `public_web_stage_2` 是否被误打开
  - `LinkedIn Stage 1 -> Stage 1 Preview -> stage_2_final` 之间是否存在显著 prerequisite gap
  - `Final Results` 后 board 是否真正 ready/non-empty

### 1.3.1 手工测试反哺出的缺口

之前 simulate / explain / E2E 已经不少，但仍没挡住后续手动测试暴露的问题，根因不是“没跑测试”，而是测试断言形状还不对。当前总结成 10 类：

- 只验证“这一轮能跑完”，没有验证“跑完后写回的持久化状态是否仍支持下一轮正确复用”。
- explain dry-run 主要看单次 planning 语义，没有覆盖 workflow 完成后 control-plane 被更新、再 explain 一次是否还正确。
- 很多 smoke fixture 使用全新临时 runtime；它们适合测编排正确性，但不容易暴露 `company_key` 漂移、registry duplicate、lane coverage 被污染这类“脏状态累积”问题。
- Browser E2E 是 opt-in，不在默认快回归矩阵里；因此 `history reload`、候选人看板恢复、前端时间展示等问题更容易留到人工测试才发现。
- 这轮已把“候选人看板第 N 页在后台补候选人时不能跳页”补进 browser E2E，而且同时覆盖：
  - workflow 入口
  - restored history 入口
- 之前缺少针对“本地资产复用 query 的时间预算”守卫，导致 workflow 即使最终 completed，也可能在 `stage_1_preview -> final results` 之间多跑了不该跑的 normalize/materialize 路径。
- PG-only 迁移期新增了更多“写入后再读取”的风险面；如果测试只断言 API 输出，不检查落盘后的 registry/profile/job/history 关系，就会漏掉这类问题。
- 之前没有显式断言 “Final Results 已显示” 到 “候选人看板首屏真正可交互” 之间的延迟，因此结果页可能已经完成、但用户还要再等几秒甚至几十秒。
- 当前 browser 断言对分页稳定性的判定口径是：
  - 第 2 页不能回到第 1 页
  - 当前页 preview 顺序不能因为后台 hydration 被改写
  - 如果本机太快、在首次可见结果前就已经 `loaded == total`，测试接受“已全量稳定态”，但不接受半加载错态
- 候选人卡片上的关键动作入口没有浏览器级断言，像 `打开 LinkedIn` 这类回归会等到人工点卡片时才暴露。
- 时间线只校验结构，不校验展示值的合理范围；像 `saved_at` 污染导致的 `9491s` 这类显示错误，如果不读真实 UI 文案就抓不到。
- 导出/下载类语义此前主要靠人工点，像“导出 zip 文件名必须使用中国时间”这种需求，需要至少一条前端或浏览器级断言兜底。

后续默认要求：

- 任何 reuse / delta planning 改动，都至少补一条 “run once -> persisted -> explain again” 回归。
- 任何 history / results 页面相关改动，都至少补一条 “带 `history_id` 跑完 -> API 恢复 -> 结果仍可加载” 回归。
- PG writer / canonicalization 改动，必须同时断言：
  - `company_key` 不漂移
  - alias/special-char 公司名不会生成重复 authoritative row
  - lane coverage 不会掉回 `0/0`
- 本地资产复用 query 需要独立 timing budget，不和 large-org live/scoped delta 混在同一口径里看。
- Browser E2E 至少要额外断言三件事：
  - 结果页首屏存在候选人可操作入口，而不是只有静态文本
  - 如果 `Final Results -> candidate board` 超过 2s，必须出现明确 loading feedback
  - `Final Results` 的时间线 duration 必须落在合理预算内，不能被 `saved_at` 等持久化字段污染
- 导出/下载链路的时间语义、文件命名和历史恢复语义，不能只靠人工 spot check；后续至少要各有一条自动化断言覆盖。
- fixture 里凡是涉及公司目录 / snapshot path 的地方，也必须使用生产同级的 alias-aware canonicalization。
  - 例如 `Humans&` 不能只用 `normalize_company_key -> humans` 这种简化逻辑；否则 smoke 可能把资产种到错误目录，既造假失败，也掩盖真实回归。

### 1.4 Browser E2E

用途：

- 真实验证前端 `Search -> plan -> review -> workflow -> results -> manual review`
- 覆盖前端 history reload / 后端恢复链路，而不是只测 API 返回
- 把前端状态切换、后端 hosted simulate 路径、浏览器交互放到同一条回归里

标准入口：

```bash
cd "sourcing-ai-agent/frontend-demo"
npm run browser:e2e -- \
  --frontend-url http://127.0.0.1:4173 \
  --query "我想了解Humans&里偏Coding agents方向的研究成员"
```

自动化回归：

- [tests/test_frontend_browser_e2e.py](../tests/test_frontend_browser_e2e.py)
  - 启一个临时 hosted simulate backend
  - seed reference org assets
  - 再起本地 Vite
  - 用 Playwright 跑真实浏览器主链路
  - 额外验证结果页 reload 后仍能从后端恢复
  - 默认 fast suite：
    - `Humans& Coding agents`
      - 代表小组织 `full reuse`
    - `Anthropic Pre-training`
      - 代表中型组织 `baseline reuse`
    - `xAI all members (existing baseline)`
      - 代表大组织 `full-roster query -> asset_population default`
    - `Physical Intelligence all members`
      - 代表新组织 `runtime identity + new_job + full roster`
  - slow suite：
    - `OpenAI Pre-train`
      - 代表大组织 `scoped delta`
    - `Google multimodal + Pre-train`
      - 代表大组织 `partial shard covered + delta`
    - `xAI all members -> second identical run`
      - 代表大组织 `profile tail completed -> reuse_completed`

标准回归命令：

```bash
cd "sourcing-ai-agent"
make test-browser-e2e-fast
make test-browser-e2e-slow
make test-browser-e2e-full
```

等价前端目录入口：

```bash
cd "sourcing-ai-agent/frontend-demo"
npm run browser:matrix:fast
npm run browser:matrix:slow
npm run browser:matrix:full
```

说明：

- 默认 `PYTHONPATH=src python3 -m unittest tests.test_frontend_browser_e2e -v`
  - 只跑 fast suite
  - slow suite 会显示为 skip
- `SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src python3 -m unittest tests.test_frontend_browser_e2e -v`
  - 跑 fast + slow，两者合起来就是完整 browser matrix
  - slow suite 会把每条大组织 case 放到独立 harness / frontend session 中执行，优先保证稳定性而不是复用同一轮浏览器状态
- `SOURCING_RUN_FULL_BROWSER_E2E_MATRIX=1`
  - 也可作为兼容别名启用 slow suite

适用场景：

- 前端 Step/tab 切换改动
- Plan review / confirm 交互改动
- 搜索页 history 恢复改动
- 结果页 / 人工审核页联动改动
- 本地联调入口或 API contract 改动

### 1.5 Hosted Scripted Long-Tail Smoke

用途：

- 低成本模拟大组织 provider 长尾
- 复现 `pending -> ready -> fetch -> retryable timeout` 这类真实长耗时外部行为
- 压 recovery、阶段切换、progress 轮询与自动收敛语义

标准入口：

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=scripted \
SOURCING_SCRIPTED_PROVIDER_SCENARIO=configs/scripted/google_multimodal_long_tail.json \
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --case google_multimodal_pretrain \
  --strict
```

自动化回归：

- [tests/test_hosted_workflow_smoke.py](../tests/test_hosted_workflow_smoke.py)
  - `test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover`
  - `test_hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider`
  - `test_hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch`
  - `test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch`

适用场景：

- worker recovery / runtime watchdog / progress takeover 改动
- remote-search / remote-harvest checkpoint resume 改动
- staged acquiring/retrieving completion 判定改动
- full-roster 之后的 background profile-prefetch / reconcile 改动
- profile tail 已完成后不应重复触发 profile scraper 的幂等性回归

现在 hosted smoke record 里也会带：

- `job_summary.background_reconcile`
- `final.background_reconcile`

这样可以直接区分：

- 只是 roster overflow / shard recovery
- 还是已经进入 profile-completion tail，并在 worker recovery 后完成 reconcile
- 还是同请求第二次进入 `reuse_completed`，直接复用已完成 job 而不再重复抓取 profile tail

### 1.6 Live Provider Validation

用途：

- 验证真实召回质量、provider 参数、成本与时延
- 验证 simulate/scripted 无法覆盖的真实 API 差异

原则：

- 只在 simulate / scripted / replay 通过后再做
- 优先小公司或明确增量 query
- 大组织 live run 应优先走已有 baseline + 必要缺口 delta
- 默认不做周期性自动执行；只做手动显式触发

显式入口：

```bash
cd "sourcing-ai-agent"
make test-env-backend-live LIVE_CONFIRM=1
make test-live-large-org-manual LIVE_CONFIRM=1
```

底层脚本：

- [scripts/run_live_large_org_regression.py](../scripts/run_live_large_org_regression.py)

当前 manual live 大组织回归默认覆盖：

- `xai_full_roster`
  - 验证 `full_company_roster` 边界与 shard strategy
- `xai_coding_all_members_scoped`
  - 验证“带方向词但仍说全部成员”不会误判成 full roster

注意：

- 这层不属于 CI / 快回归
- 它的通过标准不是“所有 case 都在短时间 completed”
- 是否需要补“剩余 LinkedIn profile tail”，也只应由 live run 决定
  - scripted 负责证明 worker recovery / background reconcile / artifact rebuild 没问题
  - live 才能说明真实 provider 还剩多少 tail、为什么会剩下，以及是否值得继续付费 backfill
- 更看重：
  - explain / dispatch 边界是否正确
  - acquisition 是否进入预期阶段
  - real provider 参数与成本是否符合预期

## 1.7 常用用户链路的最低自动化要求

下面这些链路是后续最不应该只靠人工记忆去回归的：

- `Search -> plan -> approve -> results -> 刷新 history URL -> 候选人看板恢复`
- `同一 query 完成后立刻再搜一次`
  - 特别是 `Reflection AI`、`OpenAI Reasoning` 这类应该直接复用 snapshot 的 query
- `公司名 alias / 特殊字符 / 空格变体`
  - `Reflection AI / reflectionai`
  - `Google DeepMind`
  - `Humans&`
  - `Periodic Labs`
- `asset_population default` 查询
  - 允许最终默认结果来自全量资产，而不是只看 ranked results 列表
- `manual review / supplement / target candidates` 写回后，历史恢复与 registry 检索仍一致
- `轻请求 + 重请求并发`
  - 本地已有大组织 workflow 跑着时，小组织本地复用 query 不应被整体阻塞

其中前 4 类现在都应该进入默认 automated matrix；后 2 类若暂时无法稳定自动化，至少要保留明确的 smoke checklist 和 performance probe。

额外实践说明：

- `hosted_workflow_smoke` 这类会起后台线程、临时 server、隔离 runtime/SQLite shadow 的套件，在 macOS 上做 durations/profile 时，优先单独起一个 pytest 进程。
- 不建议把它和 `results_api / frontend_history / asset_paths / supplement` 等其它高信号套件硬拼到同一个长命令里跑 durations。
  - 这样虽然表面更省命令数，但更容易在 teardown 阶段引入线程/SQLite shadow 噪声，污染真实的慢测与失败信号。

## 2. 外部 Provider 模式

`SOURCING_EXTERNAL_PROVIDER_MODE` 目前支持：

- `live`
  - 真实调用外部 provider
- `replay`
  - 只复用缓存与本地资产，不发新请求
- `simulate`
  - 返回 workflow 可消费的模拟 provider 结果
- `scripted`
  - 按 scenario 文件精确模拟 pending/ready/fetch/error/timeout

注意：

- `replay` 是 cache-only 模式；缓存未命中时保持空结果，不再生成 `_offline` 占位成员
- `simulate/replay/scripted` 只替代高成本外部 provider
- Postgres control plane、snapshot 落盘、hosted API、progress、results、stage summaries、recovery 仍是真实执行路径

轮询降速相关环境变量现在都是 runtime-configurable，不再在模块导入时冻结。回归/CI 若要尽量快，可显式设置：

- `WEB_SEARCH_READY_COOLDOWN_SECONDS=0`
- `WEB_SEARCH_FETCH_COOLDOWN_SECONDS=0`
- `WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS=0`
- `SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS=0`
- `EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `EXPLORATION_FETCH_MIN_INTERVAL_SECONDS=0`

补充：

- in-process `unittest` hosted smoke 已内置这组快速 cooldown
- 若你测的是外部常驻 `serve`，现在有两种方式：
  - 在 server / daemon 进程侧配置这些环境变量
  - 或者在请求里显式带 `execution_preferences.runtime_tuning_profile=fast_smoke`
- `fast_smoke` 现在也会缩短 Harvest 的 probe poll、dataset retry backoff、scripted sleep

对已有 hosted server 做低成本快速 smoke 时，推荐优先用 job-scoped profile，而不是为了测试去改整台服务的进程环境。

例如：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py \
  --base-url http://127.0.0.1:8765 \
  --runtime-tuning-profile fast_smoke \
  --strict
```

## 3. Explain Dry-Run Matrix

默认 case catalog 存放于：

- [src/sourcing_agent/workflow_explain_matrix.py](../src/sourcing_agent/workflow_explain_matrix.py)

示例矩阵文件：

- [configs/explain_dry_run_matrix.example.json](../configs/explain_dry_run_matrix.example.json)

一个 explain case 可以定义：

- `payload`
  - 实际发送给 `/api/workflows/explain` 的请求
- `expect`
  - 结构化期望断言，例如：
    - `target_company`
    - `keywords`
    - `dispatch_strategy`
    - `planner_mode`
    - `current_lane`
    - `former_lane`
    - `*_contains`

补充说明：

- `reuse_completed` 不适合放进默认 explain dry-run catalog，因为它依赖“运行环境里已经存在一条已完成的同请求 job”。
- 这类语义目前通过两层固定回归覆盖：
  - hosted scripted：`test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch`
  - browser E2E slow：`test_browser_workflow_e2e_large_org_profile_tail_second_run_reuses_completed_job`
- 两层都会直接检查：
  - `dispatch_strategy = reuse_completed`
  - `dispatch_matched_job_status = completed`
  - second run 不再重复触发 profile tail

推荐做法：

- 对 runtime 依赖强的组织，优先写成 hosted explain matrix case，而不是只留在人肉 dry-run
- 期望里优先断言语义，不要把不稳定的 count/tick 当主断言
- 需要 partial coverage 场景时，优先断言 `covered_*` / `missing_*`，不要只断 `requires_delta_acquisition=true`

## 4. Scripted Scenario Catalog

scenario 文件存放于：

- [configs/scripted/](../configs/scripted/)

当前样例：

- [configs/scripted/reflection_pending.json](../configs/scripted/reflection_pending.json)
  - 小组织 pending smoke
- [configs/scripted/google_multimodal_long_tail.json](../configs/scripted/google_multimodal_long_tail.json)
  - 大组织长尾 pending + retryable timeout smoke
- [configs/scripted/large_org_full_roster_overflow.json](../configs/scripted/large_org_full_roster_overflow.json)
  - 通用大组织 full-roster overflow / shard recovery 行为夹具
- [configs/scripted/large_org_profile_tail_reconcile.json](../configs/scripted/large_org_profile_tail_reconcile.json)
  - 通用大组织 full-roster 后的 background profile-prefetch tail / reconcile 行为夹具

说明：

- 这些 large-org fixture 的行为是通用的，当前只用 xAI 作为具体 driver query / sample payload
- 回归要验证的是 orchestration 模式，而不是“xAI 这个公司名”本身

一个 scenario 可定义：

- `search.rules/default`
  - `poll_pending_rounds`
  - `results`
  - `errors`
  - `artifacts`
- `harvest.rules/default`
  - `execute_pending_rounds`
  - `errors`
  - `body`
  - `artifacts`

推荐约定：

- 用 `default` 描述该场景的主行为
- 需要多 query 区分时再加 `rules`
- `kind=retryable` 用来模拟 timeout / 429 / IncompleteRead 这类应该自动恢复的错误
- 长尾场景优先用 `pending_rounds`，不要只靠 sleep 堆时长
- 若要验证 baseline 与 shard bundle 的真实覆盖关系，优先补 exact membership 场景，不要只看 candidate count

## 5. 如何新增 Smoke Case

### 5.1 加到默认矩阵

更新：

- [src/sourcing_agent/workflow_smoke.py](../src/sourcing_agent/workflow_smoke.py)
  - `DEFAULT_SMOKE_CASES`

要求：

- case 名稳定、可读
- query 代表一个真实产品场景
- 尽量覆盖新的组织规模、请求语义或资产复用路径

### 5.2 用自定义矩阵文件

可复制：

- [configs/simulate_smoke_matrix.example.json](../configs/simulate_smoke_matrix.example.json)

执行：

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py \
  --matrix-file configs/simulate_smoke_matrix.example.json \
  --strict
```

## 6. 如何新增 Explain Dry-Run Case

1. 更新 [src/sourcing_agent/workflow_explain_matrix.py](../src/sourcing_agent/workflow_explain_matrix.py)
2. 尽量把期望写成语义断言，而不是 count 断言
3. 若它依赖特定 baseline / shard fixture，再补 hosted explain test fixture
4. 若它代表真实回归样本，再补一条 `unittest`

## 7. 如何新增 Scripted 场景

1. 在 [configs/scripted/](../configs/scripted/) 下新增一个 `.json`
2. 用最少规则表达你要压的链路
3. 若它代表长期回归样本，再补一个对应的 `unittest`

建议优先补这类场景：

- 大组织 current lane 长尾 pending
- former lane 多轮 ready/fetch
- Harvest retryable timeout / 429 / IncompleteRead
- acquisition 完成后 retrieval 自动切换
- stage 1 preview blocked 后 continue-stage2 自动续跑

## 6. 改动后的推荐回归顺序

### 6.1 普通后端改动

1. `python3 -m py_compile` 检查新文件
2. `PYTHONPATH=src python3 -m unittest discover -s tests -v`

### 6.2 改动 explain / planning / dispatch / hosted workflow

1. 单元测试
2. hosted simulate smoke
3. 必要时先看 explain/smoke 里的结构化 query / lane / delta 摘要，再决定是否需要 scripted 或 live

### 6.3 改动 recovery / staged transition / provider checkpoint

1. 单元测试
2. hosted simulate smoke
3. hosted scripted long-tail smoke
4. 如果涉及 `candidate_artifacts.py` / materialize / finalize 性能，额外跑一次：
   `python3 scripts/run_candidate_artifact_benchmark.py --candidate-count 320 --dirty-candidates 24 --repeat 3 --build-profile foreground_fast --env-file .local-postgres.env`
   如果系统 `python3` 过旧，脚本现在会直接报错并提示改用 `.venv-tests/bin/python`。
5. 重点看结构化报告里的：
   - `payload_build_total`
   - `state_upsert`
   - `state_upsert_candidate_count`
   - `generation_register`
   - `finalize_total`
   - `comparison.wall_ms.speedup_x`

### 6.4 计划上线前

1. 单元测试
2. hosted simulate smoke
3. hosted scripted long-tail smoke
4. 只在前 3 层都通过后，选 1 到 2 条 live workflow 做手动真实验证

## 7. 当前已知边界

- simulate/scripted 适合验证 orchestration correctness，不适合判断真实召回质量
- scripted 目前更擅长模拟 Harvest/Search 长尾，不等于完整真实互联网环境
- cold real-runtime explain/dispatch 在大资产环境下仍可能比 unit/integration test 慢，这类性能优化应单独追踪
- `scripts/run_candidate_artifact_benchmark.py` 当前是 artifact/materialize 级 benchmark，不是完整 provider-grade workflow benchmark
- 真实大 snapshot 的 cold full materialize 已有一条实测基线：
  - Google `20260423T040115`，`5897` candidates，隔离 runtime，`foreground_fast`
  - wall time 约 `14.4min`
  - `state_upsert` 仅 `~107ms`，`finalize_total` 仅 `~1.2s`
  - 当前真正的大头仍是 `prepare_candidates` / `payload_build_total` / `view_write_total`
- 如果后续要回答 `search returned count / roster returned count / fetched LinkedIn profile count / board ready` 这类问题，还需要继续补 workflow 级 case report

低优先级 explain 轻量化优化已记录在 [NEXT_TODO.md](NEXT_TODO.md)。
