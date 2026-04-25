# Frontend Demo

> Status: Component/package-specific doc. Useful within its local scope, but not a global runtime contract unless `docs/INDEX.md` points to it explicitly.


这个目录现在作为 `Sourcing AI Agent` 的本地前端壳，优先接真实 hosted backend API，而不是旧的静态 `public/tml` 数据链。

如果你的环境是 `Windows + WSL2`，优先看：

- [`../docs/WINDOWS_WSL2_FRONTEND_SETUP.md`](../docs/WINDOWS_WSL2_FRONTEND_SETUP.md)

当前打通的主链：

- `Search`
- `Explain`
- `Plan`
- `Review / Approve`
- `Workflow Progress`
- `Continue Stage 2`
- `Results`
- `Candidate history / profile detail`

当前前端路由：

- `/`
- `/results?history=<history_id>&job=<job_id>`
- `/manual-review?history=<history_id>&job=<job_id>`
- `/targets`
- `/candidate/<candidate_id>?history=<history_id>&job=<job_id>`

## 当前复用边界

可以直接复用的部分：

- 页面和布局骨架
- 搜索输入、计划卡片、时间线、候选人列表、候选人抽屉
- 本地后端地址配置与搜索历史
- `SearchPage` 这一条主路由
- `SearchFlow / PlanCard / ExecutionTimeline / CandidateBoard / OnePageDrawer` 这些 UI 组件

已经改为以后端为准的部分：

- 不再默认 `force_fresh_run=true`
- 执行画像来自 `/api/workflows/explain`
- workflow 进度来自 `/api/jobs/{job_id}/progress`
- Stage 2 continuation 来自 `/api/workflows/{job_id}/continue-stage2`
- 结果来自 `/api/jobs/{job_id}/results`

暂时不作为主链依赖的部分：

- `public/tml/*`
- 旧的静态手工导入资产展示
- 只靠前端 heuristic 猜公司/规模/调度策略
- `ManualReviewPage / OutreachPage / RunStatusPage / ResultsPage` 这些旧 demo 页面目前没有接回主链路由

## 本地运行

建议每个本地联调 shell 先启用一次 proxy guard，避免 `localhost` / `127.0.0.1` 检查误走代理：

```bash
cd ../
source ./scripts/local_dev_proxy_guard.sh
```

如果你只想保护单条命令，也可以这样：

```bash
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
```

现在更推荐直接用仓库根目录的两个 helper：

```bash
cd ../
bash ./scripts/dev_backend.sh
bash ./scripts/dev_frontend.sh
```

这里统一保留 `bash` 前缀，避免某些 WSL / 挂载目录里直接执行 `./scripts/*.sh` 时触发 `Permission denied`。

如果你更想要统一的仓库根目录入口，也可以直接用：

```bash
cd ../
make dev-backend
make dev-frontend
make dev
make dev-status
make dev-stop
make dev-logs
make dev-clean
```

其中 `make dev` 会把 backend 放到后台、frontend 放到前台，并把 backend 日志写到 `runtime/service_logs/make-dev-backend.log`。
`make dev-status` 会检查端口监听和本地 health。
`make dev-stop` 会停止当前 `API_PORT` / `FRONTEND_PORT` 上的本地联调进程。
`make dev-logs` 会统一 tail backend helper 和常用 runtime 日志；如果只想看一次当前尾部，可用 `make dev-logs EXTRA_LOG_ARGS=--no-follow`。
`make dev-clean` 会在 stop 之后继续清理 helper 日志和失活状态；如果只想先预览清理范围，可用 `make dev-clean EXTRA_CLEAN_ARGS=--dry-run`。

如果你在 `frontend-demo` 目录里，更短的等价入口是：

```bash
npm run dev:backend
npm run dev:frontend
npm run dev:local
```

### 1. 启后端

在项目后端目录：

```bash
cd ../
bash ./scripts/dev_backend.sh
```

这条脚本默认会把 worker daemon 一起带起来，并在你退出时一起收掉。

如果你只是想做低成本编排验证，不想真实调用外部 provider：

```bash
export SOURCING_EXTERNAL_PROVIDER_MODE=simulate
```

若要压大组织长尾 pending/recovery：

```bash
export SOURCING_EXTERNAL_PROVIDER_MODE=scripted
```

### 2. 启前端

```bash
cd frontend-demo
npm run dev:local
```

如果依赖还没装，再先执行一次：

```bash
npm install
```

`frontend-demo/.npmrc` 已默认把 npm cache 固定到仓库根目录：

```bash
../.cache/npm
```

这样不会再写到只读的 `~/.npm`，避免 WSL / 虚拟化文件系统下出现 `EROFS`。

默认本地地址：

- 前端：`http://127.0.0.1:4173`
- 后端：`http://127.0.0.1:8765`

后端默认会给本地前端开放浏览器跨源访问：

- `http://127.0.0.1:4173`
- `http://localhost:4173`

如果前端换了别的端口或域名，可以在启动后端前设置：

```bash
export SOURCING_API_ALLOWED_ORIGINS=http://127.0.0.1:4173,http://localhost:4173,http://192.168.1.10:4173
```

或者直接用 helper：

```bash
cd ../
bash ./scripts/dev_backend.sh --allow-origin http://192.168.1.10:4173
```

`.env.development` 已默认指向本地后端：

```bash
VITE_API_BASE_URL=same-origin
VITE_DEV_PROXY_TARGET=http://localhost:8765
VITE_USE_MOCK=false
VITE_USE_LOCAL_ASSETS=false
VITE_ENABLE_EXCEL_INTAKE_WORKFLOW=true
```

本地 `dev` 与 `preview` 都会把 `/api/*` 代理到 `VITE_DEV_PROXY_TARGET`。如果浏览器环境里 same-origin 意外回落到前端 HTML shell，客户端会自动 fallback 到 `http://127.0.0.1:8765` / `http://localhost:8765`，避免 Excel 上传这类 `FormData` 请求被误判为后端不可达。

## 浏览器联调环境

如果你要在 WSL2 / Linux 里做真实浏览器联调或 Playwright smoke，优先用仓库内固定环境，而不是依赖系统自带 `chromium`：

```bash
cd frontend-demo
npm run browser:install
npm run browser:check
```

这两步会固定并复用：

- npm cache：`../.cache/npm`
- Playwright browsers：`../.cache/ms-playwright`
- 用户态 Ubuntu shared libs：`../.cache/ubuntu-libs`

建议继续沿用仓库内的 proxy guard，而不是手动写 `NO_PROXY='*'`：

```bash
cd ../
source ./scripts/local_dev_proxy_guard.sh
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
```

这样只会把本地回环地址加入 `NO_PROXY`，不会把整台机器的所有请求都绕过代理。

## 本地端口调用说明

如果暂时不走云端，而是本地开放端口给前端调用，当前做法就是：

1. 后端 `serve` 监听 `127.0.0.1:8765`
2. 前端 Vite 监听 `127.0.0.1:4173`
3. 前端直接请求 `VITE_API_BASE_URL`

也就是说，本地不需要对象存储、不需要静态 snapshot JSON、不需要 pages.dev。

只要这两个本地端口能通，主流程就能跑：

- `POST /api/workflows/explain`
- `POST /api/plan`
- `POST /api/plan/review`
- `POST /api/workflows`
- `GET /api/jobs/{job_id}/progress`
- `POST /api/workflows/{job_id}/continue-stage2`
- `GET /api/jobs/{job_id}/results`

## 局域网 / 其他设备访问

如果前端不是在同一台机器本机打开，而是希望通过局域网 IP 访问：

1. 后端改为监听外部地址：

```bash
cd ../
bash ./scripts/dev_backend.sh --host 0.0.0.0 --allow-origin http://<your-lan-ip>:4173
```

2. 前端改为监听外部地址：

```bash
cd ../
bash ./scripts/dev_frontend.sh --host 0.0.0.0 --api-base-url http://<your-lan-ip>:8765
```

3. 浏览器访问：

```text
http://<your-lan-ip>:4173
```

如果系统防火墙或云服务器安全组未开放 `4173/8765`，前端虽然能启动，浏览器仍然会失败。

## Windows 宿主机访问 VM 内前后端

如果你的开发环境在 Windows 电脑里的 Linux 虚拟机，而你想直接在 Windows 浏览器里打开前端，推荐按下面顺序做。

### 方案 A：最稳定，使用 VM 的局域网 IP

适用场景：

- VMware / VirtualBox / Parallels / Hyper-V 已经给 VM 分配了可从 Windows 访问的 IP
- 你希望 Windows 浏览器直接访问 VM 里的前后端服务

步骤 1：先确认 VM 的 IP

在 Linux VM 里执行：

```bash
ip addr
```

找到类似下面的地址：

```text
192.168.x.x
10.x.x.x
172.16-31.x.x
```

记住这个 IP，下文用 `<vm-ip>` 表示。

步骤 2：让后端监听外部地址

在 Linux VM 的后端目录里执行：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh --host 0.0.0.0 --allow-origin http://<vm-ip>:4173
```

步骤 3：让前端监听外部地址，并指向 VM 后端

在 Linux VM 的前端目录里执行：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_frontend.sh --host 0.0.0.0 --api-base-url http://<vm-ip>:8765
```

步骤 4：在 Windows 浏览器里访问

```text
http://<vm-ip>:4173
```

步骤 5：先做最小验证

在 Windows 浏览器里打开页面后：

1. 输入一个简单 query，例如 `帮我找Reflection AI做Infra方向的人`
2. 先确认能正常看到 plan
3. 再确认点击执行后可以看到 progress
4. 结果完成后，再尝试点开 `完整结果页`、`人工审核页`、`One-Pager`

如果前端能打开但无法请求后端，优先检查：

- `serve` 是否用的是 `--host 0.0.0.0`
- `VITE_API_BASE_URL` 是否写成了 `http://<vm-ip>:8765`
- `SOURCING_API_ALLOWED_ORIGINS` 是否包含 `http://<vm-ip>:4173`
- Windows 到 VM 的网络是否互通
- Linux VM 防火墙是否拦截 `4173/8765`

### 方案 B：只在 Windows 上访问后端，不直接暴露前端

如果你只想先验证后端 API 是否能从 Windows 打通，可以在 Windows 浏览器里直接访问：

```text
http://<vm-ip>:8765/health
http://<vm-ip>:8765/api/runtime/progress
```

只要这两个地址能打开，通常说明网络和后端监听没问题，再去排查前端。

### 方案 C：NAT 模式下用端口转发

如果你的 VM 没有可直接访问的局域网 IP，而是 NAT：

1. 在虚拟机软件里做端口转发
2. 把 Windows 主机的某个端口转发到 VM

例如：

- Windows `4173 -> VM 4173`
- Windows `8765 -> VM 8765`

然后在 Linux VM 内仍然这样启动：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh --host 0.0.0.0
bash ./scripts/dev_frontend.sh --host 0.0.0.0 --api-base-url http://127.0.0.1:8765
```

之后在 Windows 上访问虚拟机软件映射出来的地址和端口。

### 常见问题排查

1. Windows 能打开前端，但提交 query 后报 `Local backend is unreachable`

原因通常是前端仍然在用 `127.0.0.1:8765`，而这个 `127.0.0.1` 指向的是 Windows 自己，不是 VM。

修复方式：

- 把 `VITE_API_BASE_URL` 改成 `http://<vm-ip>:8765`

2. 浏览器报 CORS 错误

修复方式：

- 确保后端启动前设置了：

```bash
bash ./scripts/dev_backend.sh --host 0.0.0.0 --allow-origin http://<vm-ip>:4173
```

3. Windows 打不开 `http://<vm-ip>:4173`

修复方式：

- 确认前端不是默认 `127.0.0.1` 监听，而是：

```bash
bash ./scripts/dev_frontend.sh --host 0.0.0.0
```

4. 页面能打开，但 workflow 一直没有进度

修复方式：

- 确认 `run-worker-daemon-service` 也启动了
- 不要只启动 `serve`

5. 结果页/人工审核页提示缺少 workflow 上下文

原因是你直接手输打开了这些路由，但没有带 `history/job` 参数。

修复方式：

- 从搜索页真实跑一次 workflow 后，通过页面里的按钮进入
- 或手动带上：

```text
/results?history=<history_id>&job=<job_id>
/manual-review?history=<history_id>&job=<job_id>
```

## 说明

`public/tml` 相关脚本和资产目前保留在仓库里，仅作为历史 demo / 离线视觉开发材料。当前主链不再依赖它们。
