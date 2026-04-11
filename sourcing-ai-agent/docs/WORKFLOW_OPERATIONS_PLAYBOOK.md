# Workflow Operations Playbook

这份文档面向“真实操作与排障”，覆盖两件事：

1. 现在如何调用 Sourcing AI Agent（CLI / API）
2. 如何追踪进度、识别阻塞、恢复执行

与详细实现细节相比，这里更强调端到端可执行流程。

## 1. 快速启动

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
```

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

## 2. CLI 标准交互

### 2.1 先做 plan（不执行真实采集）

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_humansand_coding_researchers.json
```

重点看输出里的：

- `request`（归一化后的请求）
- `plan.acquisition_strategy`
- `plan_review_gate`
- `plan_review_session`

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

建议前端带上：

- `tenant_id`
- `requester_id`
- `idempotency_key`

这样可以避免重复请求导致的额外 API 成本，并支持多用户隔离。

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
