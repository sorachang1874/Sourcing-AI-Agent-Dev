# Windows + WSL2 Frontend Setup

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档面向下面这种开发环境：

- Windows 宿主机
- WSL2 Ubuntu 里运行 `sourcing-ai-agent`
- 希望直接在 Windows 浏览器里访问前端

目标是用最少的网络配置，把本地前端和本地 hosted backend 跑起来。

## 1. 推荐访问方式

在 `Windows + WSL2` 环境里，优先使用：

- 前端：`http://localhost:4173`
- 后端：`http://localhost:8765`

不要优先使用 WSL2 里的 `172.x.x.x` 地址。
对 Windows 浏览器来说，`localhost` 更稳定，也更符合日常开发习惯。

## 2. 标准启动方式

### 2.0 先启用本地 proxy guard

如果你的 WSL2 shell 里默认带了 `http_proxy` / `https_proxy`，本地 `curl http://127.0.0.1:8765/...` 这类检查可能会误走代理，看起来像后端返回了假的 `502`。

每个新的 WSL2 终端里，先执行一次：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
source ./scripts/local_dev_proxy_guard.sh
```

如果你只想保护单条命令，也可以这样跑：

```bash
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
```

### 2.1 启动后端 API

在 WSL2 里：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh
```

这里统一写成 `bash ./scripts/dev_backend.sh`，不要依赖 `./scripts/dev_backend.sh` 直接执行；某些 WSL 挂载环境会把脚本目录表现成 `noexec`。
如果你想用更统一的根目录入口，也可以直接运行 `make dev-backend`。

这条脚本默认会：

- 启用本地 proxy guard
- 自动补 `SOURCING_API_ALLOWED_ORIGINS`
- 后台带起 `run-worker-daemon-service`
- 前台启动 `serve`

如果你只想看最终参数，不立刻启动：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh --print-config
```

### 2.2 启动前端

再开一个 WSL2 终端：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_frontend.sh
```

同样建议保留 `bash` 前缀，避免 WSL 文件系统差异导致的直接执行失败。
对应的统一入口是 `make dev-frontend`。

如果你还要在 WSL2 里跑真实浏览器 smoke / Playwright 校验，先做一次：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent/frontend-demo"
npm run browser:install
npm run browser:check
```

这会把浏览器与依赖缓存固定到仓库内的 `.cache/`，避免写到只读的 `~/.npm`，也避免每次会话都重新找浏览器。

如果你想改用别的端口或 API 地址：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_frontend.sh --api-base-url http://localhost:8766 --port 4174
```

现在本地前端默认不是让浏览器直接请求 `http://localhost:8765`，而是：

- 浏览器访问 `http://localhost:4173`
- 页面请求同域 `/api/*`
- Vite dev server 再把 `/api/*` 代理到 `--api-base-url`

这样可以避开 `Windows + WSL2` 下浏览器直连本地后端端口不稳定的问题，也能避开 shell 里的代理环境变量污染浏览器请求链路。

`frontend-demo` 目录里也保留了一个等价 npm 入口：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent/frontend-demo"
npm run dev:backend
npm run dev:frontend
npm run dev:local
```

如果你想一条命令同时拉起两者，也可以在仓库根目录直接运行：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
make dev
make dev-status
make dev-stop
make dev-logs
make dev-clean
```

这会把 backend 放到后台、frontend 放到前台，backend 日志落到 `runtime/service_logs/make-dev-backend.log`。
`make dev-status` 会检查当前端口监听和本地 health。
`make dev-stop` 会停止当前 `API_PORT` / `FRONTEND_PORT` 上的本地联调进程。
`make dev-logs` 会统一 tail 本地 backend helper 和常用 runtime 日志；如果你只想看当前尾部，用 `make dev-logs EXTRA_LOG_ARGS=--no-follow`。
`make dev-clean` 会在 stop 之后继续清理 helper 日志和失活状态；如果你只想先预览清理范围，用 `make dev-clean EXTRA_CLEAN_ARGS=--dry-run`。

### 2.3 在 Windows 浏览器里访问

```text
http://localhost:4173
```

## 3. 先做最小验证

在 Windows 浏览器里先验证：

```text
http://localhost:8765/health
http://localhost:8765/api/runtime/progress
```

如果这两个地址能打开，再访问：

```text
http://localhost:4173
```

然后可以用一个低风险 query 做 smoke：

```text
帮我找Reflection AI做Infra方向的人
```

## 4. 如果提示 `Address already in use`

这说明你要监听的端口已经被旧进程占用了。
在 WSL2 开发里，这通常不是错误配置，而是你之前已经起过一轮服务。

### 4.1 先看端口上是不是已有可用服务

在 WSL2 里执行：

```bash
lsof -nP -iTCP:8765 -sTCP:LISTEN
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/api/runtime/progress
```

如果 `health` 和 `runtime/progress` 都能正常返回：

- 说明后端已经在运行
- 不需要重复启动 `serve`
- 直接继续启动前端即可

同理，如果 daemon 也已经在跑，就不需要重复启动 daemon。

### 4.2 如果旧服务占着端口，但你想重启它

先找到监听进程：

```bash
lsof -nP -iTCP:8765 -sTCP:LISTEN
```

再结束它：

```bash
kill <pid>
```

然后重新执行标准启动命令。

### 4.3 如果你不想停旧服务

也可以改用新端口，例如：

后端：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh --port 8766 --frontend-port 4173
```

前端：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_frontend.sh --api-base-url http://localhost:8766 --port 4173
```

## 5. 最常见的问题

### 5.1 Windows 能打开前端，但提交 query 后提示后端不可达

通常是前端仍然还在使用旧的运行时 API base 配置，或者你打开了带旧参数的历史链接。

本地联调现在推荐的有效方式是：

```bash
VITE_API_BASE_URL=same-origin
VITE_DEV_PROXY_TARGET=http://localhost:8765
```

如果页面还报：

```text
Local backend is unreachable at http://localhost:8765
```

优先这样处理：

1. 直接打开 `http://localhost:4173/?api_base_url=same-origin`
2. 在浏览器里做一次 hard refresh
3. 如果还是不对，清掉前端保存的运行时 API base 配置后再重开页面

### 5.2 浏览器报 CORS

优先不要手工拼 `SOURCING_API_ALLOWED_ORIGINS`。若你把前端改跑在 `4174`，直接这样启动后端：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh --frontend-port 4174
```

### 5.3 `http://localhost:4173` 打不开

确认前端是通过 helper 启动的。若你当前使用 `4174`，把下面端口同步替换成 `4174`：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_frontend.sh --port 4173
```

### 5.4 workflow 一直没进度

通常说明你只启动了 `serve`，但没把 daemon 一起带起来。优先直接改用：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh
```

如果你确实需要把 API 和 daemon 分开控制，再退回原始命令。

### 5.5 结果页 / 人工审核页提示缺少 workflow 上下文

这些页面依赖当前 workflow 的 `history` / `job_id`。

正确打开方式：

- 先从搜索页真实跑一次 workflow
- 再通过页面内按钮进入 `results` / `manual-review` / `candidate`

不要手工直接输入这些路由，除非你同时带上 query 参数：

```text
/results?history=<history_id>&job=<job_id>
/manual-review?history=<history_id>&job=<job_id>
```

### 5.6 本地 `curl` 显示 `502`，但服务其实是活的

这通常不是后端真的挂了，而是 WSL2 当前 shell 的代理环境把 `127.0.0.1` 也送进了代理。

优先这样验证：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
source ./scripts/local_dev_proxy_guard.sh
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/api/runtime/progress
```

如果这样能正常返回，而你之前的裸 `curl` 返回 `502`，那就是代理误导，不是本地 API 自己报错。

## 6. 一组最常用命令

后端：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh
```

前端：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_frontend.sh
```

如果你想显式分开控制 daemon / serve，仍可退回原始命令：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
source ./scripts/local_dev_proxy_guard.sh
export SOURCING_API_ALLOWED_ORIGINS=http://localhost:4173,http://127.0.0.1:4173
PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

如果你当前想固定使用 `4174`，对应命令是：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash ./scripts/dev_backend.sh --frontend-port 4174
bash ./scripts/dev_frontend.sh --port 4174
```
