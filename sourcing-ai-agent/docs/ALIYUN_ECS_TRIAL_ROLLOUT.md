# Aliyun ECS Trial Rollout

> Status: Rollout-specific reference doc. Re-check `docs/ECS_PRELAUNCH_CHECKLIST.md`, `docs/LOCAL_POSTGRES_CONTROL_PLANE.md`, and `PROGRESS.md` before treating these rollout defaults as current production settings.


## Goal

这份文档定义把当前 `Sourcing AI Agent` 迁到阿里云 ECS 试用机、并让同事通过 Cloudflare Pages 前端试用的最小可执行方案。

如果你只想看上线前最后一步的 env / 重启项，直接看：

- `docs/ECS_PRELAUNCH_CHECKLIST.md`

当前约束：

- ECS: 中国香港，`2 vCPU / 4 GiB / 40 GiB`
- 前端: Cloudflare Pages，`https://demo.111874.xyz`
- 当前本地 runtime 体积过大
  - `runtime/` 约 `72G`
  - `runtime/sourcing_agent.db` 约 `21G`
  - `runtime/company_assets/google` 约 `12G`

因此这次不采用“整机 runtime 直搬”，而采用：

1. 代码走 Git
2. secrets 走本地 secret 文件 / secret manager
3. runtime 只恢复精选 baseline company snapshots
4. 初始 smoke 应使用隔离 test/scripted runtime；ECS hosted production 默认保持 `live`

## Local Secret Files

已保存的本地非 Git 配置：

- `runtime/secrets/deployment.local.json`
  - ECS 基本信息
  - Cloudflare Pages project/token/account
- `runtime/secrets/providers.local.json`
  - model/search/object-storage/harvest provider secrets

可提交给仓库的示例结构：

- `configs/deployment.local.example.json`

## Recommended Topology

### 1. Frontend

- Cloudflare Pages 继续用 `sourcing-ai-agents-demo`
- 对外域名保持 `https://demo.111874.xyz`
- Pages 继续托管静态前端
- `/api/*` 推荐收成同域入口
  - 当前最低成本方案：Cloudflare Worker route 反代到 `https://api.111874.xyz`
  - 长期方案：ECS 上由 `nginx/caddy` 同站代理 `/api/*`

### 2. Backend

- ECS 上只跑 API + worker daemon
- 推荐额外配置一个独立 API 域名，例如 `https://api.111874.xyz`
- `demo.111874.xyz` 是 HTTPS 页面，不能安全地直连 `http://47.83.22.223:8765`
  - 会遇到 mixed-content / CORS 问题
  - 所以生产前端要么走同域 `/api/*`，要么指向 HTTPS API origin

### 3. Runtime mode

- hosted production 默认：
  - `SOURCING_RUNTIME_ENVIRONMENT=production`
  - `SOURCING_EXTERNAL_PROVIDER_MODE=live`
- `replay/simulate/scripted` 只用于隔离测试 runtime 或显式临时 smoke override，不再作为 ECS production 默认值

补充：

- `replay` 现在是 cache-only 模式；缓存未命中时保持空结果，不再生成 `_offline` 占位成员
- production entrypoint 会拒绝非 live provider mode，除非显式设置 `SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER=1`
- runtime namespace / provider cache / PG 隔离契约见 `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`

## What To Sync First

### Sync now

- 代码仓库
- `runtime/secrets/providers.local.json`
- `runtime/secrets/deployment.local.json`
- 精选 baseline company assets

推荐第一批公司：

- `reflectionai` 约 `419M`
- `openai` 约 `141M`
- `anthropic` 约 `1.9G`
- `xai` 约 `1.4G`
- `physicalintelligence` 约 `34M`

这一批合计大约 `4G`，适合作为 ECS trial 的首批 baseline。

### Optional second batch

- `thinkingmachineslab` 约 `395M`
- `humansand` 约 `610M`
- `langchain` 约 `51M`
- `skildai` 约 `20M`

### Do not sync in the first wave

- 整份 `runtime/sourcing_agent.db`，当前约 `21G`
- `google` 全量公司资产，当前约 `12G`
- 整份 `runtime/`

这些资产要么太大，要么会显著压缩 ECS 剩余空间；建议按需再补。

## Bootstrap Profile

当前推荐的 trial bootstrap 配置已经落在：

- `configs/aliyun_ecs_trial_bootstrap.json`

这个 profile 约定了：

- `frontend_origin = https://demo.111874.xyz`
- `backend_api_origin = https://api.111874.xyz`
- 默认 hosted production 使用 `live`
- 默认 `SOURCING_API_MAX_PARALLEL_REQUESTS = 8`
- 默认 `SOURCING_API_LIGHT_REQUEST_RESERVED = 2`
- 首批恢复公司清单

## ECS Bootstrap Sequence

### 1. Prepare the host

最小要求：

- Python `>= 3.12`
- Node `>= 20`
- Git
- 可持久化目录，例如：
  - `/srv/sourcing-ai-agent/repo`
  - `/srv/sourcing-ai-agent/runtime`

Ubuntu 22.04 默认系统 Python 可能低于项目要求，不要假设系统自带版本足够；先装可用的 Python 3.12 运行时。

### 2. Clone code

```bash
mkdir -p /srv/sourcing-ai-agent/repo
cd /srv/sourcing-ai-agent/repo
git clone <repo-url> sourcing-ai-agent
cd sourcing-ai-agent
```

### 3. Restore secrets

恢复到：

- `/srv/sourcing-ai-agent/repo/sourcing-ai-agent/runtime/secrets/providers.local.json`
- `/srv/sourcing-ai-agent/repo/sourcing-ai-agent/runtime/secrets/deployment.local.json`

如果后续接入 secret manager，`deployment.local.json` 可以只保留本地开发副本，不必长期留在服务器。

### 4. Restore selected company assets

优先恢复第一批 baseline 公司，不恢复旧的整库 SQLite snapshot；旧 `sqlite_snapshot` 已退役。

如果你已经有 object storage bundle，优先走：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind company_snapshot \
  --bundle-id <company_snapshot_bundle_id> \
  --output-dir runtime/asset_imports
```

如果目前先走本地同步，建议只同步：

- `runtime/company_assets/reflectionai`
- `runtime/company_assets/openai`
- `runtime/company_assets/anthropic`
- `runtime/company_assets/xai`
- `runtime/company_assets/physicalintelligence`

不要先把 `runtime/company_assets/google` 和整份 `runtime/sourcing_agent.db` 推上去。

### 5. Start the hosted-trial backend

仓库里已经补了一个更适合 ECS trial 的启动脚本：

- `scripts/run_hosted_trial_backend.sh`
- `make hosted-trial-backend`

默认行为：

- bind `127.0.0.1:8765`
- 允许 `https://demo.111874.xyz`
- `SOURCING_RUNTIME_ENVIRONMENT=production`
- `SOURCING_EXTERNAL_PROVIDER_MODE=live`
- `SOURCING_API_MAX_PARALLEL_REQUESTS=8`
- `SOURCING_API_LIGHT_REQUEST_RESERVED=2`

启动前先确认仓库虚拟环境，而不是使用系统 `python3`：

```bash
cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent
./.venv/bin/python -c "import requests, psycopg; print('venv-ok')"
bash ./scripts/run_hosted_trial_backend.sh --print-config
```

示例：

```bash
cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent
make hosted-trial-backend \
  HOSTED_RUNTIME_DIR=/srv/sourcing-ai-agent/runtime \
  HOSTED_API_HOST=127.0.0.1 \
  HOSTED_API_PORT=8765 \
  HOSTED_FRONTEND_ORIGIN=https://demo.111874.xyz
```

如果你暂时不做反代，想直接用 IP 调试，可临时把 `HOSTED_API_HOST=0.0.0.0`，但正式给同事使用时仍建议放到 HTTPS 域名后面。

## Frontend Build For Cloudflare Pages

### Important rule

不要再用当前旧的 `.env.production -> localhost:8765` 方式发版。

现在仓库已经改成：

- `frontend-demo/.env.production`
  - 默认走 `VITE_API_BASE_URL=same-origin`
- `frontend-demo/package.json`
  - 新增 `npm run build:hosted`
- `frontend-demo/scripts/verify_hosted_build_env.mjs`
  - 支持 `same-origin` 或非本地 HTTPS API origin
  - 若 `VITE_API_BASE_URL` 为空或仍指向 `localhost/127.0.0.1`，构建会直接失败

如需同域 ingress 的落地方式，直接看：

- `docs/CLOUDFLARE_SAME_ORIGIN_PROXY.md`

### Recommended Pages env

推荐优先使用同域模式：

```text
VITE_API_BASE_URL=same-origin
VITE_USE_MOCK=false
VITE_USE_LOCAL_ASSETS=false
```

如果当前还没把 `/api/*` 收成同域入口，过渡期仍可用：

```text
VITE_API_BASE_URL=https://api.111874.xyz
VITE_USE_MOCK=false
VITE_USE_LOCAL_ASSETS=false
```

本地预构建可用：

```bash
make frontend-build-hosted-same-origin
```

或：

```bash
make frontend-build-hosted HOSTED_API_BASE_URL=https://api.111874.xyz
```

### Replacing the old frontend

不需要手工“删历史文件”。

更直接的做法：

- 继续使用同一个 Pages project：`sourcing-ai-agents-demo`
- 把构建命令切到新的 hosted build
- 重新部署后，旧版本会被新 deployment 覆盖

如果你确认旧 project 完全无保留价值，也可以在 Cloudflare 控制台删除后重建，但不是必需步骤。

## Suggested Rollout Order

### Phase 1

- 部署新前端
- ECS 上启动 replay 模式 backend
- 只恢复第一批 baseline 公司
- 让同事先测试：
  - 历史记录恢复
  - 检索方案页
  - 执行过程页
  - 候选人看板 / 人工审核 / 目标候选人

### Phase 2

- 补第二批 baseline 公司
- 增加 `api.111874.xyz` 的反代与 TLS
- 视磁盘占用决定是否补 Google

### Phase 3

- 再考虑更重的 live acquisition
- 如需补全全局状态，只恢复 `control_plane_snapshot`；不再导入 SQLite snapshot

## Why The Results Page Was Optimized Before Rollout

这次顺手做了一个低风险的上线前减重：

- 新增更轻的 `GET /api/jobs/:job_id/dashboard`
- 结果看板前端优先读这个轻量接口
- 不再默认把 events / trace spans / workers / manual review full list 一起拖到看板首屏

这不会改变 workflow 编排语义，但能减少 Cloudflare Pages 前端首屏请求体积，更适合 ECS 这种小规格试用机。
