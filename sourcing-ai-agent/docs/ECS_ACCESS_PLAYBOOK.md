# ECS Access Playbook

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档只解决一件事：

- 以后要连接阿里云 ECS 时，不需要再从 rollout / prelaunch / handoff 文档里翻 SSH 命令。

它聚焦：

- SSH 连接
- 本地端口转发
- 常用文件同步
- 远端最小健康检查

不负责完整上线流程；完整上线仍参考：

- `docs/ALIYUN_ECS_TRIAL_ROLLOUT.md`
- `docs/ECS_PRELAUNCH_CHECKLIST.md`

## Source Of Truth

如果你本地已经保存了 ECS 信息，优先看：

- `runtime/secrets/deployment.local.json`

它通常至少会有：

- ECS IP / hostname
- 用户名
- 可能的 key path / deployment label

仓库里可提交的结构示例见：

- `configs/deployment.local.example.json`

## Recommended SSH Config

建议先在本机 `~/.ssh/config` 写一个稳定别名，例如：

```sshconfig
Host sourcing-ecs
  HostName <ecs_public_ip_or_domain>
  User <ecs_user>
  Port 22
  IdentityFile ~/.ssh/<your_key>
  ServerAliveInterval 30
  ServerAliveCountMax 4
  TCPKeepAlive yes
```

之后优先都用：

```bash
ssh sourcing-ecs
```

这样以后换 IP、换 key、换用户名时，只改一个地方。

## Basic Connection Commands

### 1. Direct shell

```bash
ssh sourcing-ecs
```

### 2. Quick one-off command

```bash
ssh sourcing-ecs 'hostname && uptime && df -h'
```

### 3. Tail backend logs

```bash
ssh sourcing-ecs 'cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent && bash ./scripts/dev_logs.sh --no-follow'
```

或：

```bash
ssh sourcing-ecs 'tail -n 200 /srv/sourcing-ai-agent/runtime/service_logs/hosted-worker-daemon.log'
```

## Port Forwarding

如果后端只绑定在远端 `127.0.0.1:8765`，本地调试推荐端口转发，而不是临时把服务暴露到公网。

### Forward API to local machine

```bash
ssh -N -L 8765:127.0.0.1:8765 sourcing-ecs
```

这样本地就可以访问：

- `http://127.0.0.1:8765/health`
- `http://127.0.0.1:8765/api/runtime/health`

### Forward frontend static preview if needed

```bash
ssh -N -L 4173:127.0.0.1:4173 sourcing-ecs
```

## File Transfer

### 1. Sync local bundle or repo to ECS

推荐用 `rsync`，因为它支持断点重传和增量同步。

```bash
rsync -aH --info=progress2 \
  /local/path/to/sourcing-ai-agent/ \
  sourcing-ecs:/srv/sourcing-ai-agent/repo/sourcing-ai-agent/
```

### 2. Sync one runtime subtree

```bash
rsync -aH --info=progress2 \
  /local/path/to/runtime/company_assets/openai/ \
  sourcing-ecs:/srv/sourcing-ai-agent/runtime/company_assets/openai/
```

### 3. Pull ECS data back to local

```bash
rsync -aH --info=progress2 \
  sourcing-ecs:/srv/sourcing-ai-agent/runtime/company_assets/openai/ \
  /local/path/to/runtime/company_assets/openai/
```

### 4. Copy a single file quickly

```bash
scp /local/file.json sourcing-ecs:/srv/sourcing-ai-agent/runtime/secrets/
scp sourcing-ecs:/srv/sourcing-ai-agent/runtime/service_logs/backend.log .
```

## Remote Health Checks

连上 ECS 后，最常用的检查顺序：

```bash
ssh sourcing-ecs '
  cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent &&
  PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
'
```

```bash
ssh sourcing-ecs 'curl -fsS http://127.0.0.1:8765/health'
ssh sourcing-ecs 'curl -fsS http://127.0.0.1:8765/api/runtime/health'
ssh sourcing-ecs 'curl -fsS http://127.0.0.1:8765/api/providers/health'
```

如果远端是 HTTPS 域名：

```bash
curl -fsS https://api.111874.xyz/health
curl -fsS https://api.111874.xyz/api/runtime/health
curl -fsS https://api.111874.xyz/api/providers/health
```

## Restart Pattern

如果你只是要重启 hosted trial backend，通常按这组顺序：

```bash
ssh sourcing-ecs '
  cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent &&
  bash ./scripts/run_hosted_trial_backend.sh --print-config
'
```

然后：

1. 停旧进程
2. `git pull` 或 `rsync` 新代码
3. 确认远端虚拟环境可用：

```bash
ssh sourcing-ecs '
  cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent &&
  ./.venv/bin/python -c "import requests, psycopg; print(\"venv-ok\")"
'
```

4. 重启 backend
5. 重启 worker daemon
6. 再跑 health probe

当前 hosted / ECS 默认心智模型：

- production backend 必须是 `SOURCING_RUNTIME_ENVIRONMENT=production`
- production provider mode 必须是 `SOURCING_EXTERNAL_PROVIDER_MODE=live`
- live/hosted control plane 必须是 PG-only
- `simulate/scripted/replay` 只用于隔离 runtime 或显式临时 smoke override，不能作为 hosted production 默认值

## Recommended Mental Model

未来优先记住这三个别名/入口：

1. `ssh sourcing-ecs`
2. `rsync ... sourcing-ecs:/srv/sourcing-ai-agent/...`
3. `ssh -N -L 8765:127.0.0.1:8765 sourcing-ecs`

这样大多数“连接 ECS / 拿日志 / 拉数据 / 本地调 API”场景都能直接完成。
