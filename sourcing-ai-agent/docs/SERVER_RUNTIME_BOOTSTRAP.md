# Server Runtime Bootstrap

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


> Current default: hosted bootstrap should restore `Postgres control plane + control_plane_snapshot + company_snapshot`. `sqlite_snapshot` is now a legacy backup/portability alias.

## Goal

这份文档定义当前 `Sourcing AI Agent` 在长期在线 Linux/server 环境上的最小可执行启动流程。

canonical bundle 选择请同时参考：

- `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md`

目标不是一次性做成全自动平台，而是先把下面三件事固定下来：

- 代码如何落地到服务器
- secrets 和 object storage 如何注入
- runtime 资产如何恢复、启动、校验、回滚

## Runtime Model

当前建议的 server 角色分成两类进程：

- API 进程
  - 提供 `serve`
  - 暴露 `/health`、`/api/providers/health`
- worker daemon
  - 提供 `run-worker-daemon-service`
  - 持续恢复和推进可恢复 worker

当前不建议把 live runtime 直接塞进 Git 或系统镜像。服务器启动时应恢复：

- Git repo
- provider secrets
- object storage config
- 必要的 bundle / control-plane snapshot

## Host Requirements

最低要求：

- Linux，优先 Debian/Ubuntu 或兼容环境
- Python `>=3.12`
- 与仓库依赖锁定一致的 repo-managed venv
- Git
- 可写的本地 `runtime/`

如果启用 `google_browser` search provider，还需要：

- `node`
- Playwright 依赖目录
- Chromium 所需系统库

当前已知最常见缺口：

- `libnspr4.so`
- `libnss3.so`

在 Debian/Ubuntu 上，最小修复通常是：

```bash
sudo apt-get install -y libnspr4 libnss3
```

如果机器过于精简，不要硬扛 browser lane，直接把它迁到更完整的 Linux/server 环境。

## Recommended Layout

示例目录：

```text
/srv/sourcing-ai-agent/
  repo/
    sourcing-ai-agent/
  runtime/
    secrets/
    company_assets/
    manual_review_assets/
    asset_exports/
    object_sync/
    vendor/
      playwright/
      playwright-browsers/
      npm-cache/
```

要求：

- repo 与 runtime 分离
- `runtime/` 必须可持久化
- `runtime/secrets/providers.local.json` 不进入 Git

## Secrets Contract

至少要恢复或注入：

- model provider / relay 配置
- semantic provider 配置
- Harvest token
- Serper token
- object storage 配置

推荐顺序：

1. 优先用 secret manager / environment variables
2. 其次恢复 `runtime/secrets/providers.local.json`
3. 不要把生产 secrets 写回仓库

## Object Storage Contract

server 侧必须明确：

- `provider`
- `bucket`
- `prefix`
- `endpoint_url`
- `region`
- `access_key_id`
- `secret_access_key`

当前 object sync 已支持：

- 并发 upload/download
- 默认 resume
  - 已存在且匹配的对象会跳过
- progress summary
- 本地/云端 bundle index
- 本地/云端 sync run manifest

如果需要强制全量重传或重下，可以显式关闭 resume：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle \
  --manifest runtime/asset_exports/<bundle>/bundle_manifest.json \
  --no-resume

PYTHONPATH=src python3 -m sourcing_agent.cli download-asset-bundle \
  --bundle-kind company_snapshot \
  --bundle-id <company_snapshot_bundle_id> \
  --output-dir runtime/asset_imports \
  --no-resume
```

## Bootstrap Sequence

### 1. Get code onto the server

```bash
git clone <repo-url> /srv/sourcing-ai-agent/repo
cd /srv/sourcing-ai-agent/repo/sourcing-ai-agent
git switch dev
```

如果要跑发布版，则切到对应 release commit 或 `main`。

### 2. Prepare runtime directories

```bash
mkdir -p runtime/secrets runtime/asset_imports runtime/vendor
```

### 3. Restore secrets

恢复 `providers.local.json`，或导出对应环境变量。

至少先确保：

- `test-model` 可通过
- object storage client 可初始化

### 4. Restore durable assets

默认恢复路径应是：

1. 恢复 canonical `control_plane_snapshot`
2. 再恢复需要的 canonical `company_snapshot`

不要把 `company_handoff` 当成默认 server bootstrap 入口，因为它更重，也更容易把历史测试态文件一起带回。
`sqlite_snapshot` 现在只保留 backup / portability 语义，不再是 hosted 默认恢复主路径。

默认推荐直接使用统一导入命令 `import-cloud-assets`，而不是手工串联 `download-asset-bundle -> restore-* -> backfill-*`。

先恢复 control plane：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind control_plane_snapshot \
  --bundle-id <control_plane_snapshot_bundle_id> \
  --output-dir runtime/asset_imports
```

再恢复公司 snapshot：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind company_snapshot \
  --bundle-id <company_snapshot_bundle_id> \
  --output-dir runtime/asset_imports
```

对于 `company_snapshot`，`import-cloud-assets` 会在 restore 后自动执行：

- candidate artifact repair
- organization asset registry warmup
- linkedin profile registry backfill

注意：serve 启动时的 organization asset warmup 不是默认后台任务。需要冷启动预热时显式设置
`STARTUP_ORGANIZATION_ASSET_WARMUP_ENABLED=true`，否则它会保持 disabled，避免每次启动都扫描全部 runtime assets 并拖慢前台 plan / workflow 请求。

如果你已经手工下载好了 bundle，也可以直接给本地 manifest：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --manifest runtime/asset_imports/<company_snapshot_bundle_id>/bundle_manifest.json
```

如果要恢复全套 canonical 资产，请直接按 `docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md` 里的 bundle id 顺序执行。

### 5. Run smoke checks

```bash
PYTHONPATH=src python3 -m unittest tests.test_object_storage tests.test_search_provider -q
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli show-daemon-status
```

如果当前 server 不打算启用 browser lane，不要求 `google_browser` live search 可用。

### 6. Start API server

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
```

### 7. Start worker daemon

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service \
  --service-name worker-recovery-daemon \
  --poll-seconds 5 \
  --lease-seconds 300 \
  --stale-after-seconds 180 \
  --total-limit 4
```

如果要交给 systemd，先生成 unit：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli write-worker-daemon-systemd-unit \
  --service-name worker-recovery-daemon
```

## Health Checks

启动后至少检查：

- `GET /health`
- `GET /api/providers/health`
- `PYTHONPATH=src python3 -m sourcing_agent.cli show-daemon-status`

如果 server 主要是为了继续 TML 资产积累，再额外检查：

- `runtime/company_assets/<company>/latest_snapshot.json`
- `manual_review_backlog.json`
- `profile_completion_backlog.json`

## Failure Recovery

### Browser search fails with shared library errors

如果错误里出现：

- `libnspr4.so`
- `libnss3.so`

先补系统库，再重试。当前代码已经会把缺失库名直接写进错误信息。

### Bundle transfer interrupted

直接重跑相同命令即可。默认 resume 会跳过已完成对象，只补剩余缺口。

### SQLite state is suspicious

优先：

1. 备份当前 `runtime/sourcing_agent.db`
2. 用最近一次只读 snapshot 做 restore
3. 再重新生成 company candidate artifacts

## Current Gaps

当前还没有的一键能力：

- `bootstrap-device`
- “拉最近可恢复 bundle” 的快捷命令
- server 侧自动 secret bootstrap

所以现在的 server bootstrap 仍然是：

- 文档化
- 可重复
- 可恢复

但还不是全自动。
