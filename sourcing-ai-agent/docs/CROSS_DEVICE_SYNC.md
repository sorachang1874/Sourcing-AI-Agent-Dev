# Cross-Device Secure Asset Sync

## Goal

这份文档定义一套适合 `Sourcing AI Agent` 的跨设备可恢复方案。

目标不是把所有东西都塞进 Git，而是把不同类型的资产放到正确的存储层里：

- GitHub 负责代码、文档、示例配置、去敏后的方法论资产
- Secret store 负责 provider credentials
- Cloud object storage 负责高价值 runtime 资产
- Local runtime 负责执行态、临时缓存和调试过程

这样做的原因很直接：

- 代码和文档需要跨设备同步
- secrets 不应该进入 Git
- high-value raw assets 太敏感、太大，也不适合放 Git
- workflow 仍需要保持 raw-first、可审计、可回放

## Design Principles

1. Git is not runtime storage  
   GitHub 不是 live payload、profile raw asset、company snapshot 的存储层。

2. Raw-first, then sync  
   connector 返回仍然必须先在本地 runtime 落盘，再决定是否进入云端 durable storage。

3. Manifest-driven recovery  
   跨设备恢复不靠“记忆哪些文件重要”，而靠 manifest、registry、snapshot metadata。

4. Least privilege  
   开发设备默认只拿到运行所需的最小 secret；高价值资产按 bucket/prefix 做权限隔离。

5. Tiered retention  
   不是每种 runtime 文件都值得长期同步。只有高价值、可复用、难以重建的资产进入 durable sync。

## Asset Classes

| Asset Type | Examples | Default Location | Sync to Git | Sync to Cloud |
| --- | --- | --- | --- | --- |
| Code and docs | `src/`, `tests/`, `README`, retrospectives | Git repo | Yes | Optional backup |
| Example config | `providers.local.example.json` | Git repo | Yes | No |
| Secrets | API keys, relay endpoints, tokens | Secret manager / env | No | Not as plaintext |
| Durable raw assets | LinkedIn profile raw JSON, company roster raw JSON, manual review evidence, publication raw page | local runtime first, then cloud object storage | No | Yes |
| Durable normalized assets | `candidate_documents.json`, `manifest.json`, retrieval artifacts | local runtime first, then cloud object storage | No | Yes |
| Execution state | worker checkpoint, scheduler state, daemon status | local runtime / DB | No | Selective |
| Ephemeral cache | temporary HTML, retry scratch, debug temp file | local runtime | No | Usually No |
| Local DB | SQLite `jobs / candidates / evidence / criteria_*` | local runtime | No | Exported snapshots only |

## Recommended Storage Planes

### 1. GitHub private repo

用途：

- code
- docs
- retrospective
- example config
- sanitized historical methods

不放：

- `runtime/`
- `runtime/secrets/providers.local.json`
- raw API payload
- profile raw assets
- historical account files

### 2. Secret manager

推荐职责：

- 存 `providers.local.json` 等价信息
- 管理 Claude relay / DashScope / Harvest / Serper 等 token
- 支持按设备或按环境注入

推荐形式：

- first choice: 1Password / Bitwarden / 企业 secret manager
- Alibaba-oriented option: 阿里云 KMS / Secret Manager + 环境变量注入
- minimal fallback: 本地加密文件，不进 Git

要求：

- 不把 secret 明文存入 object storage manifest
- 不把 `providers.local.json` 直接提交 Git
- 新设备恢复时先恢复 secret，再运行 live workflow

### 3. Cloud object storage

推荐作为 durable asset storage。

适合的后端：

- Aliyun OSS
- Cloudflare R2
- AWS S3
- 任何 S3-compatible bucket

推荐目录前缀：

```text
sourcing-ai-agent-dev/
  prod/
    company_assets/
      {company}/
        {snapshot_id}/
          manifest.json
          asset_registry.json
          candidate_documents.json
          retrieval_index_summary.json
          ...
    manual_review_assets/
      {company}/
        {review_id}/
          resolution_input.json
          source_manifest.json
          fetched_pages/
          analysis/
    retrieval_artifacts/
      {job_id}/
        results.json
        result_diff.json
    sqlite_exports/
      {date}/
        sourcing_agent.sqlite.zst
    manifests/
      latest/
        company_assets_index.json
        manual_review_index.json
        retrieval_index.json
```

说明：

- object storage 存原始资产和归一化资产
- 本地路径结构尽量和云端 prefix 对齐
- `manifest.json + asset_registry.json` 是恢复入口，不依赖人工回忆

### 4. Local runtime

本地仍然是执行主场。

本地保留：

- live debug cache
- current worker checkpoint
- daemon status
- sqlite working database
- 尚未上传或不值得上传的临时资产

不要求本地 runtime 全量持久化到云端。只同步高价值子集。

### 5. Metadata registry

建议后续补一个中心化 registry，而不是只靠 bucket list。

最小可行形态：

- 仍然用本地 SQLite
- 每次 cloud sync 额外生成：
  - `asset_bundle_manifest.json`
  - `sync_run.json`
  - `restore_plan.json`

后续可升级为：

- PostgreSQL / managed DB
- 专门的 `cloud_asset_registry`、`asset_sync_runs`、`asset_bundle_refs` 表

## What Should Be Synced

默认建议同步到 cloud 的高价值资产：

- `runtime/company_assets/{company}/{snapshot_id}/manifest.json`
- `runtime/company_assets/{company}/{snapshot_id}/asset_registry.json`
- `candidate_documents.json`
- `retrieval_index_summary.json`
- high-value raw profile payload
- manual review evidence bundle
- retrieval result artifact
- retrospective 使用到的 supporting raw asset

默认不建议同步：

- `.db-journal`
- transient retry scratch files
- temporary HTML cache that can be re-fetched cheaply
- daemon heartbeat / lock file
- low-value duplicate payload

## Security Policy

### Encryption

- object storage bucket 必须开启服务端加密
- 高敏感 bundle 建议再做应用层加密
- manual review bundle 与 profile raw payload 视为高敏感

### Access control

- code repo 权限和 asset bucket 权限分离
- 开发者不一定默认拥有全部 bucket prefix 的读写权限
- public/private company 项目应可按 prefix 隔离

### Redaction

- 写入 Git 的任何方法论文档都必须去敏
- retrospective 里可以引用 asset path，但不贴 raw secret
- bucket manifest 允许记录资产 hash、大小、类型，不记录 credential

## Recovery Workflow

新设备恢复推荐顺序：

1. clone GitHub monorepo
2. 阅读 `ONBOARDING.md`
3. 恢复 provider secrets
4. 恢复最小必需的 cloud asset index
5. 只下载当前任务需要的 bundle
6. 再启动 live workflow 或继续开发

推荐的恢复粒度：

- code-only recovery
  - 只 clone repo
  - 可继续开发和跑大部分 tests
- development recovery
  - clone repo
  - 恢复 secrets
  - 恢复最近一次 live test 的 manifest 和关键 snapshot
- investigation recovery
  - clone repo
  - 恢复 secrets
  - 恢复特定公司全部 snapshots + manual review bundles + retrieval artifacts

## Proposed Restore Commands

当前已经正式实现：

- `export-company-snapshot-bundle`
- `export-company-handoff-bundle`
- `export-sqlite-snapshot`
- `restore-asset-bundle`

当前建议后续继续补：

- `bootstrap-device`
  - 检查 repo、Python、provider config、runtime layout
- `upload-asset-bundle`
  - 把 bundle 和 manifest 推送到 cloud object storage
- `pull-asset-index`
  - 拉取云端 index，不拉全量 raw asset
- `export-sqlite-snapshot`
  - 导出当前 SQLite 的只读恢复版本

## Implemented CLI Examples

```bash
cd '/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent'

PYTHONPATH=src python3 -m sourcing_agent.cli export-company-snapshot-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli export-sqlite-snapshot
PYTHONPATH=src python3 -m sourcing_agent.cli restore-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json --target-runtime-dir /tmp/sourcing-agent-runtime
```

## Current Thinking Machines Lab Example

这轮已经实际导出并验证：

- handoff bundle:
  - `runtime/asset_exports/company_handoff_thinkingmachineslab_20260406t172703_20260406T125539Z/`
  - `569` files
  - `29346219` bytes
- sqlite snapshot:
  - `runtime/asset_exports/sqlite_snapshot_sourcing_agent_db_20260406T125538Z/`
- restore smoke test:
  - `/tmp/sourcing-agent-restore-smoke`

## Recommended Phase Plan

### Phase 1: Manual but safe

目标：

- 先让跨设备恢复可操作
- 不要求自动双向同步

建议实现：

- 统一 bucket 前缀规范
- 统一 bundle manifest 规范
- 统一 onboarding checklist
- 手动上传关键 snapshot 与 manual review bundle

### Phase 2: Semi-automated sync

目标：

- 提供一键导出/恢复关键资产

建议实现：

- `asset_sync.py`
- `asset_bundle.py`
- `cloud_asset_registry` manifest
- selective restore by company / snapshot / job

### Phase 3: Durable multi-device workflow

目标：

- 让不同机器上的 AI / 人类开发者都能稳定接手

建议实现：

- object storage + central registry DB
- secret bootstrap command
- sync run audit log
- retention policy
- per-project / per-company access control

## Minimal Recommendation For This Project

结合当前项目状态，最适合的近期方案是：

1. GitHub private repo  
   继续同步代码、文档、示例配置、去敏方法论资产。

2. Secret manager  
   单独保存：
   - Claude relay config
   - DashScope/Qwen config
   - Harvest token
   - Serper token

3. Object storage bucket  
   单独保存：
   - `company_assets`
   - `manual_review_assets`
   - retrieval artifacts
   - exported SQLite snapshots

4. 先实现“按 bundle 恢复”，不要追求全量 runtime 镜像  
   当前真正值得恢复的是高价值调查资产，不是整个本地缓存目录。

## What This Changes Operationally

之后如果你切换到公司电脑或新的 AI 环境：

- GitHub 负责把工程和上下文文档恢复到位
- secret manager 负责把 provider 能力恢复到位
- object storage 负责把高价值调查资产恢复到位

这样就不会再出现：

- 代码在 Git，但没有足够上下文继续开发
- live 结果留在旧电脑里，无法复用
- 为了恢复状态不得不重复烧 API 成本

## Current Status

当前这个方案还是设计层，不是自动化实现层。

已经具备的基础：

- `asset_registry.json`
- `manifest.json`
- centralized asset logging
- manual review assets
- GitHub/onboarding docs

下一步最合理的实现切入点是：

1. `asset_bundle_manifest` 规范
2. `export-asset-bundle / restore-asset-bundle`
3. 云端 object storage prefix 约定
4. exported SQLite snapshot 机制
