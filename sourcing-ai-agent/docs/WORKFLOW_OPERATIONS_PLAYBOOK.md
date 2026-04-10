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
- `show-workers` 的 `status`（`queued/running/blocked/completed`）
- `show-job` 的 `summary` 与最终结果

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

## 6. 推荐执行纪律

1. 任何高成本 query 先走 plan-only。
2. 先审查参数，再允许真实采集。
3. workflow 统一走非阻塞 + daemon 恢复。
4. 进度以 `show-progress` 为主，问题定位看 `show-workers/show-trace`。
5. 对重复 query 统一检查 `query-dispatches`，不要盲目重跑。
