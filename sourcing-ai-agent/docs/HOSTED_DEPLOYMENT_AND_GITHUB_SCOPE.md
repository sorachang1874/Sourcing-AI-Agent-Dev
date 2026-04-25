# Hosted Deployment And GitHub Scope

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档用于避免两类常见错误：

1. 在云端仍按本地临时调试方式运行（手工续跑、手工补状态）
2. 把 runtime/secrets/raw assets 直接推到 GitHub

## 1. 默认云端运行方式

生产/准生产环境默认走 hosted 路径：

- API 服务：`serve`
- 恢复守护：`run-worker-daemon-service`
- object storage：默认使用阿里云 OSS（通过 `S3-compatible` 配置接入）

最小命令：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

## 2. 前端接入边界

前端必须只使用 API contract：

- `POST /api/plan`
- `POST /api/plan/review`
- `POST /api/workflows`
- `GET /api/jobs/{job_id}/progress`
- `GET /api/jobs/{job_id}/results`

前端不要直接读取：

- `runtime/company_assets/**`
- `runtime/jobs/**`
- 任何 snapshot 内部 JSON 文件路径

阶段反馈统一使用 `workflow_stage_summaries`，不绑定 runtime 文件结构。

## 3. GitHub 上传范围（降成本）

建议上传：

- `src/`
- `tests/`
- `docs/`
- `contracts/`
- `configs/*.example.json`
- `README.md`、`PROGRESS.md`、`pyproject.toml`

不要上传：

- `runtime/**`
- `runtime/secrets/**`
- `runtime/company_assets/**`
- `runtime/live_tests/**`
- `runtime/provider_cache/**`
- 本机环境缓存（`.venv/`、`node_modules/`、playwright browsers）
- 临时 live smoke 配置（`configs/live_smoke_*.json`、`configs/live_test_*.json`）

## 4. 上线前检查

1. `test-model` 通过
2. `/health`、`/api/providers/health` 可用
3. `/api/workers/daemon/status` 可见守护进程
4. 前端页面只调用 API contract，不读取 runtime 文件
5. `git status` 中无 runtime/secrets/raw assets
