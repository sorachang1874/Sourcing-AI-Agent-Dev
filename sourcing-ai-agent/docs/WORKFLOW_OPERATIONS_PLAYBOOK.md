# Workflow Operations Playbook

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档面向“真实操作与排障”，覆盖两件事：

1. 现在如何调用 Sourcing AI Agent（CLI / API）
2. 如何追踪进度、识别阻塞、恢复执行

与详细实现细节相比，这里更强调端到端可执行流程。

如果当前目标是把现有资产恢复到服务器，而不是发起新的 live acquisition，请先看：

- `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`
- `docs/SERVER_RUNTIME_BOOTSTRAP.md`
- `docs/TESTING_PLAYBOOK.md`

## 1. 快速启动

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
```

若只是验证 workflow 编排、阶段推进、恢复链路，而不想真实消耗 Harvest / Search / model / semantic 成本，可先切低成本外部 provider 模式：

```bash
export SOURCING_EXTERNAL_PROVIDER_MODE=simulate
```

可选值：

- `live`
  - 默认值
  - 真实调用 Harvest / Search / model / semantic provider
- `replay`
  - 只复用缓存，不做新的外部请求
  - 缓存未命中则保持空结果，不再生成 `_offline` 占位成员
- `simulate`
  - 完全不发外部请求，返回 workflow 可消费的模拟 provider 结果

推荐用法：

- 编排/恢复 smoke test：`simulate`
- 缓存复用验证：`replay`
- 真实召回验证：`live`

## 1.0 运行时 identity / Email 口径

公司 identity 现在按下面优先级合并，不再继续把中小组织 alias 硬编码进 builtin：

- builtin identity
  - 只保留少量大组织 / 特殊 deterministic 入口
- runtime seed catalog
  - `runtime/company_identity_seed_catalog.json`
  - 也支持通过 `SOURCING_COMPANY_IDENTITY_SEED_CATALOG_PATH` 指向外部文件
- imported / local company assets
  - `runtime/company_assets/*/*/identity.json`
- 最终物化 registry
  - `runtime/company_identity_registry.json`

补充：

- 仓库内有一个极小的 bootstrap seed catalog，仅用于像 `Humans&` 这类已知组织的冷启动兜底
- 真正可持续维护的入口仍是 runtime seed catalog 和 imported asset identity
- `refresh_company_identity_registry` 会把 seed catalog 与本地 snapshot identity 一起合并进最终 registry

Harvest profile email 现在默认改为 no-email 模式：

- profile scraper 默认使用 `Profile details no email ($4 per 1k)`
- planner / settings 默认 `collect_email=false`
- 若历史 raw payload 里仍有 Harvest email，只接受非 `risky` 且 `qualityScore >= 80` 的条目
- risky / 低分邮件会在 timeline/materialization/results API 这整条链路上被 scrub，不再继续透传到前端

如果当前目标是低 IO 地批量重写现有 company assets，而不是顺便重刷 registry，可用 rewrite-only 方式：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli backfill-structured-timeline \
  --company "Anthropic" \
  --skip-profile-registry-backfill \
  --skip-registry-refresh
```

说明：

- 这会重写 `normalized_artifacts` / `strict_roster_only` 下的结构化经历物化结果
- 不会同步执行 organization asset registry refresh
- 适合 WSL / 本地联调环境里的分批资产修复
- 若后续需要刷新 registry，再单独跑对应的 registry backfill / warmup

建议常驻一个后台恢复器（非阻塞 workflow 场景）：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

## 1.1 云端默认运行方式（推荐）

默认应使用 hosted 路径：

1. `serve` 常驻托管 API 与 workflow 调度
2. `run-worker-daemon-service` 常驻推进 recoverable workers

最小启动组合：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

基础健康检查：

- `GET /health`
- `GET /api/providers/health`
- `GET /api/workers/daemon/status`
- `GET /api/runtime/health`
- `GET /api/runtime/progress`

不推荐把“手工 execute-workflow 续跑”作为常规流程；它应只用于排障。

补充：

- `SOURCING_EXTERNAL_PROVIDER_MODE` 会统一影响高成本外部 provider，包括 Harvest / search / model / semantic
- Postgres control plane、snapshot 落盘、progress / recovery / stage summaries 仍按真实路径运行
- 因此它适合做“低成本端到端编排测试”，但不适合拿来评估真实召回覆盖率
- `simulate/scripted/replay` 必须运行在隔离 runtime namespace；不要把 hosted production 切成 non-live provider mode 来做常规 smoke
- runtime namespace / provider cache / PG 隔离规则见 `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`

推荐直接用内建 smoke matrix 做 hosted simulate 回归：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py --strict
```

默认 matrix 覆盖：

- Skild AI
- Humans&
- Anthropic
- OpenAI
- Google

如果要走自动化 hosted 回归：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m unittest tests.test_hosted_workflow_smoke -v
SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1 PYTHONPATH=src python3 -m unittest tests.test_hosted_workflow_smoke -v
```

口径说明：

- 默认 `unittest` 只跑 3 条代表性 flow，适合作为日常回归
- full 5-case hosted matrix 需要显式设置 `SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1`
- smoke/cI 若要进一步压时长，相关 search cooldown 现已支持运行时配置为 `0`
- 若你测的是外部常驻 `serve`，现在有两种方式：
  - 在 server / daemon 进程侧设置这些 cooldown 环境变量
  - 或者在请求里带 `execution_preferences.runtime_tuning_profile=fast_smoke`
- `fast_smoke` 也会缩短 Harvest 的 probe poll、dataset retry backoff、scripted sleep

对共享 hosted 环境做 smoke 时，更推荐第二种。这样只会影响当前 job，不会改整个服务的默认档位。

若要进一步压大组织长尾 pending/timeout/recovery，请改用 scripted 模式，具体分层与回归入口见：

- `docs/TESTING_PLAYBOOK.md`

## 2. CLI 标准交互

### 2.1 先做 plan（不执行真实采集）

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_humansand_coding_researchers.json
```

重点看输出里的：

- `request`（归一化后的请求）
- `request_preview.intent_axes`（当前前后端共享的 effective intent contract）
- `plan.acquisition_strategy`
- `plan_review_gate`
- `plan_review_session`

若前端 / 运维需要在真正启动前看 dry-run 决策，优先用：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli explain-workflow --file <request.json>
```

或：

- `POST /api/workflows/explain`

重点看：

- `organization_execution_profile`
- `asset_reuse_plan`
- `dispatch_preview`
- `lane_preview`
- `generation_watermarks`
- `cloud_asset_operations`

补充：

- 当前 execution 层已开始直接消费 `intent_axes` 对应的 effective intent，而不只是把它当展示字段
- 也就是说，`request_preview.intent_axes` 与后续 planning/acquisition/retrieval 的关键判断不应再长期漂移成两套语义
- execution 侧会进一步通过 `build_effective_request_payload(...)` 把这份 contract materialize 成 worker / retrieval 真正使用的 payload
- `plan` / `start-workflow` 响应里的顶层 `request` 也不再只是“原始归一化 request”
- 当 acquisition strategy 已经把 query 扩成更真实的 execution keywords 时，例如 `Pre-train` / `Vision-language`，返回给前端的 `request.keywords` 与 `request_preview.keywords` 会一起对齐到 execution-aligned request

### 2.2 review-plan（先 preview 再 apply）

先预览结构化改写结果：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 12 \
  --reviewer sora \
  --instruction "改成 full company roster，走 keyword-first，强制 fresh run，不允许高成本 source。" \
  --preview
```

确认无误后应用：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan \
  --review-id 12 \
  --reviewer sora \
  --instruction "改成 full company roster，走 keyword-first，强制 fresh run，不允许高成本 source。"
```

### 2.3 start-workflow（默认非阻塞）

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --plan-review-id 12
```

返回 `job_id` 后即进入追踪阶段。  
如需同步阻塞执行，再显式加 `--blocking`。

补充：

- 本地 CLI 调试可以直接用 `start-workflow`
- 云端默认仍建议通过 hosted `serve` 路径接收 `POST /api/workflows`，并常驻 `run-worker-daemon-service`

### 2.4 Excel intake（手动上传联系人表）

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli intake-excel --file <workbook.xlsx>
```

重点看返回里的：

- `intake_id`
- `schema_inference`
- `summary.local_exact_hit_count`
- `summary.manual_review_local_count`
- `summary.fetched_direct_linkedin_count`
- `summary.fetched_via_search_count`

若存在 `manual_review_local` 或 `manual_review_search`，再继续：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli continue-excel-intake --file <review_decisions.json>
```

这条 CLI 链路适合单次排查“上传 Excel -> 返回本地命中 / 待复核 / 新抓取结果”。

前端产品入口走 `/api/intake/excel/workflow`：

- 浏览器上传真实 Excel 文件，不要求用户提供服务器本地路径
- 后端先解析 workbook，再按 company hint 自动拆成多个 history/job
- 每个子 job 的检索方案页显示“不适用”，直接进入执行过程与候选人看板
- 本地 dev / preview 都应通过 same-origin `/api/*` proxy 或 loopback fallback 连接 `127.0.0.1:8765`

## 3. 进度追踪与交互

### 3.1 CLI 侧

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli show-progress --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-workers --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-scheduler --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-job --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-trace --job-id <job_id>
```

建议优先观察：

- `show-progress` 的 `stage / milestones / worker_summary`
- `show-progress` 或 `GET /api/jobs/{job_id}/progress` 里的 `workflow_stage_summaries`
- `show-workers` 的 `status`（`queued/running/blocked/completed`）
- `GET /api/runtime/progress` 里的：
  - `cloud_asset_operations`
  - `company_asset.authoritative_registry.materialization_watermark`
  - `company_asset.execution_profile.source_generation_watermark`

说明：

- `generation_watermark` 现在是跨 artifact / registry / execution profile 的统一 freshness 标记
- 若 explain 说要复用某个 baseline，但 runtime progress 里的 `company_asset` watermark 与预期不一致，优先怀疑导入/repair/materialization 没收敛
- `show-job` 的 `summary` 与最终结果

补充：

- `show-progress` / `GET /progress` 现在会对 hosted runtime 的 service status / runner probe 做短 TTL 缓存，避免前端高频轮询时反复重读 watchdog / recovery / runner 状态文件。
- 如需调节这层轮询成本，可用 `WORKFLOW_PROGRESS_SERVICE_STATUS_CACHE_SECONDS` 与 `WORKFLOW_PROGRESS_RUNNER_STATUS_CACHE_SECONDS`；默认只做亚秒级缓存，不改变 workflow takeover 语义。
- `show-progress` / `GET /progress` 的热路径也只读取 result/manual-review summary counts，不再为计数目的拉整批明细；若要看候选详情或 manual review 内容，改用 `show-job` 或专门明细接口。
- `show-progress` / `GET /progress` 现在还会优先读取 `job_progress_event_summaries`，不再为常规轮询全量扫描 `job_events`；只有 runtime auto-takeover cooldown 判断仍保留小范围 `runtime_control` 查询。

物化水位：

- 组织级 `normalized_artifacts/artifact_summary.json` 现在会带：
  - `materialization_generation_key`
  - `materialization_generation_sequence`
  - `materialization_watermark`
  - `membership_summary`
- shard bundle `normalized_artifacts/<view>/acquisition_shard_bundles/<shard>/summary.json` 也带同一套 generation watermark。
- `organization_completeness_ledger.json` 会把 baseline 的 exact membership summary 一起写入。
- `organization_asset_registry.materialization_*`
- `acquisition_shard_registry.materialization_*`
- `organization_execution_profiles.source_generation_*`

排障时如果发现文件已刷新、但 registry / execution profile 还是旧状态，就直接比对这组 generation watermark。

当前稳定阶段总结顺序：

- `linkedin_stage_1`
- `stage_1_preview`
- `public_web_stage_2`
- `stage_2_final`

当前建议这样理解：

- `linkedin_stage_1`
  - LinkedIn current/former roster、profile detail baseline 已完成
- `stage_1_preview`
  - 基于 LinkedIn Stage 1 资产做规则/本地检索预览
- `public_web_stage_2`
  - public web / publication / exploration 等第二阶段 enrichment 已完成
- `stage_2_final`
  - 全链路结果已落盘，可供结果页直接消费

如果需要做前端阶段卡片或漏斗图，优先直接读取：

- `GET /api/jobs/{job_id}/progress`
- `GET /api/jobs/{job_id}/results`

而不是自己扫描 snapshot 目录。snapshot 里的原始阶段文件现在仅作为运维/审计兜底，路径为：

- `runtime/company_assets/{company}/{snapshot_id}/workflow_stage_summaries/`

### 3.2 API 侧（前端集成）

常用读接口：

- `GET /api/jobs/{job_id}/progress`
- `GET /api/jobs/{job_id}/workers`
- `GET /api/jobs/{job_id}/scheduler`
- `GET /api/jobs/{job_id}/results`
- `GET /api/jobs/{job_id}/trace`
- `GET /api/query-dispatches`

常用写接口：

- `POST /api/plan`
- `POST /api/plan/review`
- `POST /api/workflows`
- `POST /api/workers/cleanup`
- `POST /api/workers/daemon/run-once`
- `POST /api/workers/interrupt`

### 3.3 统一入口分工

默认应按职责选择入口，而不是混用 CLI repair 命令和 hosted 主路径：

| 场景 | Canonical entrypoint | 说明 |
| --- | --- | --- |
| 计划生成 | `POST /api/plan` / `cli plan` | 只生成 plan/review，不创建 workflow job |
| 执行前解释 | `POST /api/workflows/explain` / `cli explain-workflow` | dry-run，不写 job，只返回 normalization / dispatch / lane preview |
| 正式执行 | `POST /api/workflows` | hosted 默认执行入口，由 `serve` 托管 |
| 恢复排障 | `execute-workflow` / `supervise-workflow` / `run-worker-daemon-once` | 仅 repair/debug，不应替代 hosted 常驻 |
| 云端资产恢复 | `import-cloud-assets` | 恢复 bundle 并统一修复 registry / completeness / profile registry |

运维侧最小常驻组合保持不变：

- `serve`
- `run-worker-daemon-service`

## 4. Query 去重与复用（成本控制）

当前 workflow 默认具备 query dispatch 去重：

- 在途同请求：`join_inflight`
- 已完成同请求：`reuse_completed`
- 已完成同公司 snapshot：`reuse_snapshot`

当前将 request normalization 明确分成两层：

- `ingress_normalization`
  - 请求入口的 LLM/rules 混合归一。
  - 负责从原始 query 提取 `target_company / keywords / organization_keywords / facets / role buckets / execution_preferences`。
- `dispatch_matching_normalization`
  - dispatch 去重阶段的 deterministic 归一。
  - 基于 prepared/effective request 生成 signature、family score，并决定 `join_inflight / reuse_completed / reuse_snapshot / new_job`。
  - 这一层不重新调用模型。

建议在真实执行前先做一次 dry-run：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli explain-workflow --file <request.json>
```

或走 API：

- `POST /api/workflows/explain`

这个入口不会创建 job，但会一次性返回：

- `ingress_normalization`
- `planning`
- `dispatch_matching_normalization`
- `dispatch_preview`
- `lane_preview`

其中 `dispatch_preview` 现在也会直接带：

- `strategy`
- `matched_job.job_id`
- `matched_job.status`

适合前端展示“这次请求为什么会复用 baseline / 为什么会补 former delta / 当前 lane 会不会 live 打外部 provider / 这次是不是直接 reuse_completed 了上一条已完成 job”，也适合运维排障。

建议前端带上：

- `tenant_id`
- `requester_id`
- `idempotency_key`

这样可以避免重复请求导致的额外 API 成本，并支持多用户隔离。

补充语义：

- 对“小型组织 + 已完成 full-company snapshot”的情况，即使新 query 的方向词变了，只要仍属于 `full_company_asset` 语义，也会优先复用已有 snapshot，而不是重新触发 Harvest company/search。
- 对大组织或 scoped/keyword-first snapshot，仍主要依赖 `request_family_score` 与 request family overlap 决定是否复用。

## 5. 典型阻塞与恢复

### 5.1 acquiring 卡住但无新结果

先看：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli show-workers --job-id <job_id>
```

如果有 worker backlog，执行一次恢复循环：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-once --job-id <job_id>
```

然后再看 `show-progress` / `show-job`。

这一步是排障手段，不应替代 hosted 常驻的 `serve + run-worker-daemon-service`。

### 5.2 大组织查询成本过高

先用 `plan` / `review-plan --preview` 做参数审查，再执行真实 workflow。  
重点确认：

- `company_scope`
- `search_seed_queries`
- `filter_hints.locations`（默认应是 `United States`）
- `function_ids`
- keyword shard 是否为方向词而不是泛化词

### 5.3 历史 recoverable worker 残留

如果 workflow 已经 `completed/failed/superseded`，但 `/api/runtime/health` 里仍有较多 `recoverable_worker_count`，先预览再清理：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli cleanup-recoverable-workers --dry-run
```

默认只会挑选“挂在 terminal workflow 下的 recoverable worker”。  
确认后执行真实清理：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli cleanup-recoverable-workers \
  --reason "Retired stale recoverable workers after workflow cleanup."
```

如需更精细控制，可加：

- `--target-company Reflection AI`
- `--parent-job-status completed`
- `--parent-job-status failed`
- `--job-id <job_id>`
- `--lane-id exploration_specialist`

## 6. 推荐执行纪律

1. 任何高成本 query 先走 plan-only。
2. 先审查参数，再允许真实采集。
3. workflow 统一走非阻塞 + daemon 恢复。
4. 进度以 `show-progress` 为主，问题定位看 `show-workers/show-trace`。
5. 对重复 query 统一检查 `query-dispatches`，不要盲目重跑。

## 6.1 前端禁区（避免误用）

前端只使用 API + contract，不直接读取 runtime 文件。

- 应使用：
  - `GET /api/jobs/{job_id}/progress`
  - `GET /api/jobs/{job_id}/results`
  - `workflow_stage_summaries`
- 不应使用：
  - `runtime/company_assets/{company}/{snapshot_id}/...`
  - `runtime/jobs/{job_id}.json`

原因：

- runtime 文件结构是后端内部实现，可调整
- `workflow_stage_summaries` 才是稳定 contract
- 直接读 runtime 会导致前端在重构后失效

## 6.2 GitHub 提交边界（部署成本）

提交 GitHub 时建议只提交代码与文档，不提交 runtime 数据资产。

- 建议提交：
  - `src/` `tests/` `docs/` `contracts/`
  - `configs/*.example.json`
  - `README.md` `PROGRESS.md`
- 不建议提交：
  - `runtime/**`
  - `runtime/secrets/**`
  - `runtime/company_assets/**`
  - `runtime/live_tests/**`
  - `.venv/`、浏览器缓存、本机 vendor cache
  - `configs/live_smoke_*.json`、`configs/live_test_*.json`
## 7. 采集并行策略（2026-04-10 更新）

`acquire_full_roster` 现在不是“先 current 再 former”的严格串行，默认策略改为：

- 当 `company-employees(current)` 进入 background queued 时，立即并行触发 `profile-search(former)`。
- 在非 worker 的同步采集路径里，也会并行启动 former（进入 enrichment 前 join）。
- 若 former 处于 `queued_background_search`，workflow 会保持 `blocked`，等待 daemon 恢复后继续到 enrichment。

这能显著减少大组织查询的端到端等待时间，同时保留“先 probe 再全量”的成本控制。
