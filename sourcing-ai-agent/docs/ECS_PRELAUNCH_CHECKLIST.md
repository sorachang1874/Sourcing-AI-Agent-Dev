# ECS Prelaunch Checklist

> Status: Current prelaunch checklist. Use together with `ECS_ACCESS_PLAYBOOK.md`, `ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md`, `RUNTIME_ENVIRONMENT_ISOLATION.md`, and `PROGRESS.md` before changing hosted/ECS runtime settings.


## Goal

这份清单只关注上线前最后一跳：

- 需要调整哪些 env
- 需要重启哪些进程
- 上线后先看哪些探针

默认假设：

- 前端仍是 `https://demo.111874.xyz`
- 后端域名是 `https://api.111874.xyz`
- hosted production 默认必须是 `live`
- `replay` / `simulate` 只能在隔离 test/scripted runtime 里跑，或用显式临时 override 做 smoke

## Backend Env

上线前确认 ECS 上的 backend 进程至少带上这些变量：

```text
SOURCING_API_ALLOWED_ORIGINS=https://demo.111874.xyz
SOURCING_RUNTIME_ENVIRONMENT=production
SOURCING_EXTERNAL_PROVIDER_MODE=live
SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only
SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1
SOURCING_API_MAX_PARALLEL_REQUESTS=8
SOURCING_API_LIGHT_REQUEST_RESERVED=2
SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
```

`SOURCING_APIFY_WEBHOOK_URL` can still be set explicitly, but current submit code defaults it for `production + live` to:

```text
https://api.111874.xyz/api/providers/apify/webhook
```

2026-04-27 deployment drift note, resolved on 2026-04-30:

- ECS backend now runs current code from `/srv/sourcing-ai-agent/repo/sourcing-ai-agent`; `/opt/sourcing-ai-agent` remains only as the reused Python 3.12 venv path.
- Hosted `POST /api/providers/apify/webhook` is present; hosted webhook preflight on ECS returned `status=ready`, connectivity probe `HTTP 202`, and `would_attach_webhooks=true`.
- Active runtime health reports `runtime_environment=production`, `provider_mode=live`, provider cache namespace `production/live`, shared recovery running, and hosted watchdog running.
- `https://api.111874.xyz/local-dev/providers/apify/webhook` remains local-dev relay only; hosted production callback remains `https://api.111874.xyz/api/providers/apify/webhook`.
- Keep this historical note because it is the failure pattern to check when `/api/providers/apify/webhook` regresses to `404`: systemd is probably serving an old code root or missing the production env file.

## Current-Code Replacement Gate

这次 ECS 版本落后较多，允许把旧 `/opt/sourcing-ai-agent` 代码根视为可替换对象。上线前必须先把“实际运行路径”收敛成一个，不要让 systemd、runbook、venv、runtime env 分别指向不同目录。

推荐做法：

1. 先停止旧 backend 和 worker daemon，避免旧进程继续提交 provider actor 或写 result view。
2. 在 `/srv/sourcing-ai-agent/repo/sourcing-ai-agent` 部署当前代码，或明确决定继续使用 `/opt/sourcing-ai-agent`。二选一后，把 systemd `WorkingDirectory`、`ExecStart`、venv 路径、env file、Nginx upstream 文档全部对齐到同一个 code root。
3. 如果迁到 `/srv`，旧 `/opt/sourcing-ai-agent` 先改名为带日期的 backup 或移出 systemd 搜索路径；不要在探针通过前直接删除 runtime/secrets。探针通过后再清理旧代码目录，避免误重启回旧版本。
4. runtime 目录保持和 repo 分离，推荐 `/srv/sourcing-ai-agent/runtime` 或 launcher 显式设置的 hosted runtime。不要把旧 `runtime/control_plane.sqlite`、`runtime_hosted/*.sqlite` 或历史 `sqlite_snapshot` 当作生产 control-plane authority 恢复。
5. 部署完成后在同一个 systemd env 下打印并留存：

```bash
pwd
git rev-parse HEAD
./.venv/bin/python -c "import sourcing_agent; print('import-ok')"
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
bash ./scripts/run_hosted_trial_backend.sh --print-config
```

阻断条件：

- `show-control-plane-runtime` 不是 `postgres_only`，或 SQLite banner 不是 shared-memory shadow。
- `/api/providers/apify/webhook` 仍是 `404`，说明服务仍是旧代码或路由未发布。
- systemd `ExecStart` 指向旧 code root，但部署步骤更新了另一个目录。
- production backend/worker 不是 `SOURCING_RUNTIME_ENVIRONMENT=production` + `SOURCING_EXTERNAL_PROVIDER_MODE=live`。
- Apify webhook preflight 不显示 `would_attach_webhooks=true`。

如果用仓库内 launcher，建议对应设置：

```text
HOSTED_FRONTEND_ORIGIN=https://demo.111874.xyz
HOSTED_MAX_PARALLEL_REQUESTS=8
HOSTED_LIGHT_REQUEST_RESERVED=2
HOSTED_EXTERNAL_PROVIDER_MODE=live
HOSTED_RUNTIME_ENVIRONMENT=production
```

补充：

- production entrypoint 会拒绝 `replay/simulate/scripted`，除非显式设置 `SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER=1`
- 如果只是做部署 smoke，应启动隔离 test/scripted runtime，而不是把 hosted production 切成 replay
- runtime / provider cache / PG 隔离契约见 `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`
- Apify webhook 预检必须在正式 live workflow 前通过；详见 `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`
- 代码与数据资产一起迁移时，先按 `docs/ECS_CODE_AND_ASSET_MIGRATION_PLAYBOOK.md` 生成 asset migration manifest；不要用 stale `latest_snapshot.json` 或旧 bundle catalog 决定生产 baseline。

上线或本地 live smoke 前执行：

```bash
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode hosted
```

如果是本地 live smoke，通过 ECS local-dev relay 时改用：

```bash
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode local-live
```

注意：预检 shell 里的变量不能替代已经启动的 backend/worker env。当前 submit path 已经为 `production + live` 设置默认 hosted webhook URL；如果显式设置 `SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED=0` 或把 production 切成 non-live provider mode，就不会附带 Apify ad-hoc webhook，后续只能依赖 local watcher / recovery，不能证明外部 webhook 链路正常。

说明：

- `SOURCING_API_MAX_PARALLEL_REQUESTS`
  - shared lane，承载常规和重请求
- `SOURCING_API_LIGHT_REQUEST_RESERVED`
  - 预留给 `/health`、`/api/frontend-history/*`、`/api/jobs/*/dashboard`、`/api/jobs/*/progress` 这类轻请求
  - 目标是避免长耗时请求把控制面接口全部堵死

## Frontend Env

Cloudflare Pages 当前至少需要：

```text
VITE_API_BASE_URL=same-origin
VITE_USE_MOCK=false
VITE_USE_LOCAL_ASSETS=false
```

如果上线前还没把 `demo.111874.xyz/api/*` 收成同域入口，才退回过渡模式：

```text
VITE_API_BASE_URL=https://api.111874.xyz
```

## Restart Order

1. 停止旧 backend / worker daemon，确认没有旧 PID 还在监听 API 端口或写 runtime。
2. `cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent`，或进入已经被 systemd 固化的唯一 code root。
3. `git pull` 或部署目标 release commit。
4. 确认远端虚拟环境与依赖：

```bash
./.venv/bin/python -c "import requests, psycopg; print('venv-ok')"
```

5. 先打印配置确认：

```bash
bash ./scripts/run_hosted_trial_backend.sh --print-config
```

6. 用新 env 重启 backend。
7. 如果 worker daemon 独立运行，确认它也跟着重启，并且使用同一个 code root、venv、runtime env。
8. 如果 Nginx 做反代，执行 `nginx -t` 后 reload。
9. 如果 Cloudflare Pages 前端也有更新，再触发一次新 deployment。

## Immediate Probes

重启后先按这个顺序检查：

1. `https://api.111874.xyz/health`
2. `https://api.111874.xyz/api/runtime/health`
3. `https://api.111874.xyz/api/providers/health`
4. 一个已知存在的 `frontend-history` 恢复链接
5. 一个已知 job 的 `dashboard` 接口

期望：

- `health` 不应再被一个长请求拖到完全无响应
- `frontend-history` 和 `dashboard` 应该在重负载下仍可优先返回
- `/api/runtime/health` 应显示：
  - `runtime_environment=production`
  - `provider_mode=live`
  - provider cache namespace 位于 production/live
- `show-control-plane-runtime` 应显示 PG-only；如果显示 SQLite legacy/emergency banner，不要继续发真实流量

## Rollback

如果上线后发现 ECS 仍有明显阻塞：

1. 先回看 backend 日志里是否存在长时间卡住的 workflow / provider 调用
2. 临时把 `SOURCING_API_MAX_PARALLEL_REQUESTS` 从 `8` 提到 `10-12`
3. 保持 `SOURCING_API_LIGHT_REQUEST_RESERVED >= 2`
4. 若仍不稳定，再回到 replay-only，并暂停真实 provider 流量

## Ingress Note

Cloudflare Pages 适合继续做静态前端 CDN，但它不是最理想的“前端中转层”。

更好的长期方案有两类：

1. 同域入口
   - 让 `demo.111874.xyz` 和 `/api/*` 走同一个反向代理层
   - 可以是 ECS 上的 `nginx/caddy`，也可以是 Cloudflare Worker 反代到 `api.111874.xyz`
   - 好处是去掉额外 CORS/runtime 配置分叉，前端只认相对路径

2. 前后端统一由同一个 ingress 发布
   - 直接把构建后的前端静态文件交给 ECS 上的 `nginx` 或容器网关托管
   - Cloudflare 只做 CDN / TLS，不再让 Pages 成为额外一层部署系统

如果只看下一阶段投入产出比，推荐顺序是：

1. 保留 Pages 托管静态站点
2. 增加 Cloudflare Worker 或同站反代，把 `/api/*` 收成同域
3. 之后再决定是否把静态前端也迁回 ECS / 容器网关
