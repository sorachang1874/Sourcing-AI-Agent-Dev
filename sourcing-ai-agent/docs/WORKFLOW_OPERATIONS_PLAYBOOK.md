# Workflow Operations Playbook

这份文档面向“真实操作与排障”，覆盖两件事：

1. 现在如何调用 Sourcing AI Agent（CLI / API）
2. 如何追踪进度、识别阻塞、恢复执行

与详细实现细节相比，这里更强调端到端可执行流程。

如果当前目标是把现有资产恢复到服务器，而不是发起新的 live acquisition，请先看：

- `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`
- `docs/SERVER_RUNTIME_BOOTSTRAP.md`

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
- `simulate`
  - 完全不发外部请求，返回 workflow 可消费的模拟结果

推荐用法：

- 编排/恢复 smoke test：`simulate`
- 缓存复用验证：`replay`
- 真实召回验证：`live`

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

不推荐把“手工 execute-workflow 续跑”作为常规流程；它应只用于排障。

补充：

- `SOURCING_EXTERNAL_PROVIDER_MODE` 会统一影响高成本外部 provider，包括 Harvest / search / model / semantic
- hosted workflow、SQLite、snapshot 落盘、progress / recovery / stage summaries 仍按真实路径运行
- 因此它适合做“低成本端到端编排测试”，但不适合拿来评估真实召回覆盖率

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

这条链路适合前端“上传 Excel -> 返回本地命中 / 待复核 / 新抓取结果”场景，不依赖主 query workflow。

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
- `show-job` 的 `summary` 与最终结果

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
