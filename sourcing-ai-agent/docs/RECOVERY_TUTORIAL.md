# Recovery Tutorial

## Goal

这份教程用于在以下场景里快速恢复项目进度：

- 切换本地账号，但仍在同一台机器上
- 换到新的电脑继续开发
- 换到服务器环境继续积累数据资产
- 换到新的 AI session，需要快速恢复上下文和 runtime 资产

## Quick Answer

如果你只是切换本机账号，而当前机器上的项目目录和 `runtime/` 还在：

- 不需要重新抓取 Thinking Machines Lab
- 不需要重新导出 bundle
- 只需要重新打开 repo、恢复 provider secrets、阅读 handoff 文档即可

如果你换了新机器：

- 先 clone GitHub repo
- 再恢复 provider secrets
- 再下载并恢复 asset bundle

## Read First

恢复前先看：

1. `ONBOARDING.md`
2. `docs/HANDOFF_2026-04-06.md`
3. `docs/CROSS_DEVICE_SYNC.md`
4. `docs/THINKING_MACHINES_LAB_RETROSPECTIVE.md`

## Scenario A: Same Machine, New Account

如果还是这台机器，只是换了本地账号或 AI 账号：

1. 确认项目目录仍然存在
2. 确认 `runtime/` 仍然存在
3. 确认 `runtime/secrets/providers.local.json` 仍然存在
4. 如果本机之前启用了增强解析/浏览器能力，确认 `runtime/vendor/` 仍然存在
4. 重新打开 repo
5. 运行：

```bash
cd '/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent'
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
```

如果这两步通过，就可以直接继续开发。

额外说明：

- `runtime/vendor/python`
  - 当前可放 `pdfminer.six` 之类的本机增强解析依赖
- `runtime/vendor/playwright` / `runtime/vendor/playwright-browsers`
  - 当前可放 browser-search lane 用到的 Playwright 依赖和浏览器
- 这些目录不进 Git，但可以跟随 bundle/object storage 一起恢复

## Scenario B: New Machine

### 1. Clone repo

```bash
git clone https://github.com/sorachang1874/Sourcing-AI-Agent-Dev.git
cd 'Sourcing-AI-Agent-Dev/sourcing-ai-agent'
```

### 2. Restore secrets

恢复下列 provider 配置之一：

- `runtime/secrets/providers.local.json`
- 或使用环境变量

最少建议恢复：

- Claude relay
- DashScope/Qwen
- Harvest
- Serper

### 3. Restore bundle

如果已经有导出的 bundle：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli restore-asset-bundle \
  --manifest /path/to/bundle_manifest.json \
  --target-runtime-dir ./runtime
```

如果云端 object storage 已配置，并且 bundle 已上传：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli download-asset-bundle \
  --bundle-kind company_handoff \
  --bundle-id company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z \
  --output-dir ./runtime/asset_imports

PYTHONPATH=src python3 -m sourcing_agent.cli restore-asset-bundle \
  --manifest ./runtime/asset_imports/company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z/bundle_manifest.json \
  --target-runtime-dir ./runtime
```

### 4. Restore SQLite if needed

如果你需要恢复 DB：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli restore-sqlite-snapshot \
  --manifest /path/to/sqlite_snapshot_bundle_manifest.json
```

默认会先备份当前 DB。

### 5. Validate

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli show-manual-review --target-company thinkingmachineslab
```

## How To Export Before Switching Devices

如果你还在旧机器上，准备迁移：

```bash
cd '/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent'

PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli export-sqlite-snapshot
```

当前已知可直接复用的 Thinking Machines Lab handoff bundle 是：

- `runtime/asset_exports/company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z/`

## How To Upload To Object Storage

如果 object storage 已配置：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle \
  --manifest runtime/asset_exports/company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z/bundle_manifest.json \
  --max-workers 8

PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle \
  --manifest runtime/asset_exports/sqlite_snapshot_sourcing_agent_db_20260406T125538Z/bundle_manifest.json \
  --max-workers 8
```

## Object Storage Config

### Local development / smoke test

`providers.local.json`:

```json
{
  "object_storage": {
    "provider": "filesystem",
    "local_dir": "/tmp/sourcing-ai-agent-object-store",
    "prefix": "sourcing-ai-agent-dev",
    "max_workers": 8
  }
}
```

### S3-compatible / OSS / R2

`providers.local.json`:

```json
{
  "object_storage": {
    "provider": "s3_compatible",
    "bucket": "your-bucket",
    "prefix": "sourcing-ai-agent-dev",
    "endpoint_url": "https://your-s3-compatible-endpoint",
    "region": "us-east-1",
    "access_key_id": "AKIA...",
    "secret_access_key": "secret...",
    "timeout_seconds": 60,
    "force_path_style": true,
    "max_workers": 8
  }
}
```

Cloudflare R2 这轮已经真实验证过可用的关键点：

- endpoint 要使用 S3-compatible endpoint  
  例如：`https://<ACCOUNT_ID>.r2.cloudflarestorage.com`
- 不要把 dashboard URL 当成 endpoint
- `region` 使用 `auto`
- 当前 uploader/downloader 已成功对真实 R2 完成：
  - `sqlite_snapshot` upload
  - `sqlite_snapshot` download
  - `company_handoff` upload
  - `company_handoff` download
  - 基于下载结果的本地 restore

也可以用环境变量：

- `OBJECT_STORAGE_PROVIDER`
- `OBJECT_STORAGE_BUCKET`
- `OBJECT_STORAGE_PREFIX`
- `OBJECT_STORAGE_ENDPOINT_URL`
- `OBJECT_STORAGE_REGION`
- `OBJECT_STORAGE_ACCESS_KEY_ID`
- `OBJECT_STORAGE_SECRET_ACCESS_KEY`
- `OBJECT_STORAGE_TIMEOUT_SECONDS`
- `OBJECT_STORAGE_FORCE_PATH_STYLE`
- `OBJECT_STORAGE_MAX_WORKERS`

## Thinking Machines Lab Checklist

恢复 Thinking Machines Lab 时，至少确认：

- `latest_snapshot.json` 存在
- `company_assets/thinkingmachineslab/` 已恢复
- `manual_review_assets/thinkingmachineslab/` 已恢复
- `jobs/` 里含 Thinking Machines Lab 相关 job JSON
- 如需复用当前 DB，`sourcing_agent.db` 已恢复

## Is Anything Missing From The TML Retrospective?

截至 `2026-04-06` 这轮真实测试，关键结论都已经落盘：

- current roster
- prioritized current detail
- former fallback
- publication supplementation
- manual review resolution
- Harvest 经验复盘
- cross-device continuation

所以当前没有“已完成但未写入文档”的关键结论。后续只有在继续执行新的 live test 或继续补资产时，才需要再更新 retrospective / progress。

## What To Continue Next

如果你恢复完进度，建议继续这条线：

1. 把当前 handoff bundle 上传到真实 object storage
2. 实现 server-side asset registry / sync index
3. 继续补 Thinking Machines Lab 的 former / publication / unresolved leads
4. 再进行下一轮正式 live test
