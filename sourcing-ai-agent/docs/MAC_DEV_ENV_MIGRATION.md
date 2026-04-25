# Mac Dev Environment Migration

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义把当前 `Sourcing AI Agent` 开发环境从 Linux/WSL 虚拟机迁到另一台 Mac 的推荐做法。

目标不是复制一份“字节级完全相同但不可运行”的旧环境，而是保留：

- 当前代码和未提交工作树
- `runtime/` 下真正有价值的数据资产
- Postgres control plane 数据
- Codex 本地交互记录和状态

同时避免把明显不可跨平台复用的内容也机械搬过去：

- Linux/WSL 的 `.venv` / `.venv-tests`
- Linux `node_modules`
- `.local-postgres/data` 和 `.local-postgres/extract`
- 低价值的大型测试 runtime / provider cache / vendor cache

## Why

跨平台迁移里最容易踩坑的是把“看起来像数据”的平台相关目录也一起复制：

- Postgres `data/` 是二进制数据库目录，不应在 Linux/WSL 和 macOS 间直接当成可运行集群复用
- `.venv` 和 `frontend-demo/node_modules` 里可能包含平台相关二进制
- 这些目录即使能复制过去，也很容易在 Mac 上变成坏环境或隐性故障源

因此推荐迁移语义是：

1. 代码和 durable runtime 资产直接拷贝
2. Postgres 做逻辑导出
3. Codex 交互记录直接拷贝
4. Mac 侧重建可执行依赖

如果你同时还希望保留“旧 Linux/WSL 工作树 + Codex resume 上下文”的完整法证快照，当前脚本也支持额外导出一份 full local snapshot。

## What To Migrate

### 1. Repo working tree

保留：

- `.git/`
- `src/`, `tests/`, `docs/`, `scripts/`, `configs/`, `contracts/`
- `runtime/company_assets/`
- `runtime/object_sync/`
- `runtime/asset_exports/`
- `runtime/object_store/`
- `runtime/manual_review_assets/`
- `runtime/jobs/`
- `runtime/excel_intake/`
- `local_asset_packages/`
- `output/`

默认不复制：

- `.venv/`, `.venv-tests/`
- `frontend-demo/node_modules/`
- `.cache/`, `.pytest_cache/`, `.mypy_cache/`, `.ruff_cache/`
- `runtime/provider_cache/`
- `runtime/vendor/`
- `runtime/test_env*`, `runtime/simulate_smoke*`, `runtime/live_tests/`
- `runtime/service_logs/`, `runtime/job_locks/`

### 2. Postgres control plane

保留方式：

- `pg_dump --format=custom` 的逻辑导出

不要直接复制：

- `.local-postgres/data`
- `.local-postgres/extract`

Mac 侧应恢复成新的 UTF8 Postgres，再 `pg_restore`。

### 3. Codex local state

建议保留：

- `~/.codex/history.jsonl`
- `~/.codex/logs_*.sqlite*`
- `~/.codex/state_*.sqlite*`
- `~/.codex/sessions/`
- `~/.codex/memories/`
- `~/.codex/shell_snapshots/`
- `~/.codex/rules/`
- `~/.codex/skills/`
- `~/.codex/config.toml`

默认不自动包含：

- `~/.codex/auth.json`

如果你确实希望新 Mac 也直接复用登录态，再显式选择包含。

如果你的目标是继续使用：

```bash
codex resume 019d6630-2137-7f70-b742-43f979b8207b
```

建议不要只带 `history.jsonl`，而是完整保留：

- `~/.codex/sessions/`
- `~/.codex/state_*.sqlite*`
- `~/.codex/logs_*.sqlite*`
- `~/.codex/shell_snapshots/`
- `~/.codex/auth.json`
- 其余 `~/.codex/` 元数据

当前本地已确认：

- session 文件存在
- `state_5.sqlite` 里的 `threads` 表也存在该 session id

这不能绝对保证跨机后 `resume` 一定成功，但完整迁移 `~/.codex` 是目前成功率最高的路径。

为了把成功率再拉高一层，目标 Mac 上尽量保持：

- 相近的 Codex CLI 版本
- 完整 `~/.codex/auth.json`
- 完整 `~/.codex/sessions/` 与 `state_*.sqlite*`

### 4. Optional operator tooling

如果你想把终端/编辑器习惯也一起迁：

- `~/.cursor/`
- `~/.claude/`
- `~/.bashrc`, `~/.gitconfig`

这些不属于项目必需资产，因此当前脚本不默认打包。

## Source Machine Steps

在当前 Linux/WSL 源环境里：

```bash
cd "Sourcing AI Agent Dev/sourcing-ai-agent"

# 先做轻量 inventory，避免直接高负载拷贝
bash scripts/prepare_mac_migration_bundle.sh --inventory-only

# 确认无误后，再做正式 bundle
bash scripts/prepare_mac_migration_bundle.sh \
  --stage-repo \
  --stage-postgres \
  --stage-codex
```

如需把 `runtime/secrets/` 也带走：

```bash
bash scripts/prepare_mac_migration_bundle.sh \
  --stage-repo \
  --stage-postgres \
  --stage-codex \
  --include-secrets
```

如需连同 Codex 登录态一起带走：

```bash
bash scripts/prepare_mac_migration_bundle.sh \
  --stage-codex \
  --include-codex-auth
```

如果你要做“尽可能完整的本地快照”，包括：

- 完整项目目录
- `runtime/secrets/`
- Linux/WSL 下的 `.venv` / `node_modules`
- 完整 `~/.codex`
- 指定 resume session 的存在性校验

可以直接用：

```bash
bash scripts/prepare_mac_migration_bundle.sh \
  --stage-full-snapshot \
  --codex-session-id 019d6630-2137-7f70-b742-43f979b8207b
```

注意：

- 这份 full snapshot 更偏“保留上下文 / 取证 / 兜底恢复”
- Mac 上真正运行项目时，仍建议重建 Python venv、Node modules 和本地 Postgres

## Full Resync Checklist

这份清单对应“全量迁移版”的标准动作：

- 完整项目目录
- `runtime/secrets/`
- 完整 `~/.codex/`
- Postgres control plane 逻辑导出
- 指定 Codex session 校验

注意：

- 这里的“全量”仍然不等于直接复制 Linux/WSL 的 `.local-postgres/data`
- Postgres 仍应通过 `pg_dump/pg_restore` 迁移

### A. Source-side freeze and prepare

1. 尽量停掉正在大量写 `runtime/` 或 `~/.codex/` 的长任务。
2. 在源环境确认当前体积，避免传输过程中才发现超预期：

```bash
du -sh "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
du -sh "$HOME/.codex"
du -sh "/home/sorachang/projects/.local-postgres"
```

3. 生成 full snapshot bundle：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
bash scripts/prepare_mac_migration_bundle.sh \
  --stage-full-snapshot \
  --codex-session-id 019d6630-2137-7f70-b742-43f979b8207b
```

4. 记下输出目录，例如：

```text
/home/sorachang/projects/Sourcing AI Agent Dev/migration_exports/sourcing-ai-agent-mac-<timestamp>/
```

### B. Transfer to the Mac

如果两台机器能 SSH 直连，推荐直接 `rsync`：

```bash
export MIGRATION_BUNDLE="/home/sorachang/projects/Sourcing AI Agent Dev/migration_exports/sourcing-ai-agent-mac-<timestamp>"
export MAC_USER="<mac_user>"
export MAC_HOST="<mac_host>"
export MAC_INCOMING="/Users/<mac_user>/incoming/$(basename "$MIGRATION_BUNDLE")"

rsync -aH --info=progress2 --partial --append-verify \
  "${MIGRATION_BUNDLE}/" \
  "${MAC_USER}@${MAC_HOST}:${MAC_INCOMING}/"
```

如果网络不稳定，建议先做压缩包，再分发：

```bash
cd "/home/sorachang/projects/Sourcing AI Agent Dev/migration_exports"
tar -cf "$(basename "$MIGRATION_BUNDLE").tar" "$(basename "$MIGRATION_BUNDLE")"
```

### C. Restore on the Mac

1. 把 repo 恢复到目标目录，例如：

```text
~/projects/Sourcing AI Agent Dev/sourcing-ai-agent
```

2. 把 `postgres/control_plane.dump` 恢复到新的 UTF8 Postgres：

```bash
pg_restore \
  --clean \
  --if-exists \
  --no-owner \
  --dbname "postgresql://<user>@127.0.0.1:<port>/sourcing_agent" \
  /path/to/control_plane.dump
```

3. 从示例生成 `.local-postgres.env`：

```bash
cp configs/local_postgres.env.example .local-postgres.env
```

4. 把完整 `codex/.codex/` 合并到目标 Mac 的 `~/.codex/`。

### D. Validate on the Mac

```bash
cd "~/projects/Sourcing AI Agent Dev/sourcing-ai-agent"
source scripts/dev_postgres_env.sh
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
```

检查指定 session：

```bash
rg -n "019d6630-2137-7f70-b742-43f979b8207b" ~/.codex/sessions
```

如果你希望最大化 `codex resume` 连续性，再额外确认：

- 目标 Mac 上 Codex CLI 版本尽量接近
- `~/.codex/auth.json` 已恢复
- `~/.codex/state_*.sqlite*` 与 `logs_*.sqlite*` 已完整恢复

输出目录默认在：

```text
../migration_exports/sourcing-ai-agent-mac-<timestamp>/
```

bundle 内会生成：

- `manifest/bundle_manifest.json`
- `manifest/control_plane_runtime.json`
- `manifest/source_sizes.txt`
- `repo/`
- `postgres/control_plane.dump`
- `codex/.codex/`

如果传了 `--codex-session-id`，还会生成：

- `manifest/codex_session_probe.json`

## Destination Mac Steps

### 1. Copy the bundle

把 bundle 目录传到新 Mac。可以用：

- `rsync`
- `scp`
- 外接 SSD
- 任意你信任的内网/离线传输方式

推荐优先级：

1. 直连传输
   - 同局域网或可 SSH 直连时，优先 `rsync`
2. 物理介质
   - 没有稳定 SSH 时，用外接 SSD 拷过去
3. 对象存储中转
   - 只有在两台机器无法直接连通，或带宽/时长管理更适合云中转时，再考虑 OSS / R2 / S3

推荐 `rsync` 示例：

```bash
rsync -aH --info=progress2 \
  /path/to/sourcing-ai-agent-mac-<timestamp>/ \
  <mac_user>@<mac_host>:/Users/<mac_user>/incoming/sourcing-ai-agent-mac-<timestamp>/
```

如果不方便直连，也可以先打成 tar 包，再经由 SSD 或对象存储传输：

```bash
tar -C /path/to -cf sourcing-ai-agent-mac-<timestamp>.tar sourcing-ai-agent-mac-<timestamp>
```

### 2. Restore the repo

推荐恢复到：

```text
~/projects/Sourcing AI Agent Dev/sourcing-ai-agent
```

保留和现在相近的路径结构，可以减少后续脚本和习惯迁移成本。

### 3. Recreate Python and frontend dependencies

```bash
cd "~/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

python3 -m venv .venv
. .venv/bin/activate
pip install -e .

cd frontend-demo
npm install
```

### 4. Restore Postgres into a fresh UTF8 database

在 Mac 上准备一个新的 UTF8 Postgres 实例和目标数据库，然后：

```bash
pg_restore \
  --clean \
  --if-exists \
  --no-owner \
  --dbname "postgresql://<user>@127.0.0.1:<port>/sourcing_agent" \
  /path/to/control_plane.dump
```

### 5. Wire the repo to the Mac Postgres instance

复制示例配置：

```bash
cp configs/local_postgres.env.example .local-postgres.env
```

然后把 `.local-postgres.env` 改成 Mac 上真实 DSN，例如：

```bash
SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://sourcing@127.0.0.1:55432/sourcing_agent
SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only
LOCAL_PG_PORT=55432
LOCAL_PG_USER=sourcing
LOCAL_PG_DB=sourcing_agent
```

这样就不再依赖 Linux/WSL 的 `.local-postgres/{extract,data}` 布局。

### 6. Restore Codex state

把 bundle 里的 `codex/.codex/` 覆盖或合并到新 Mac 的 `~/.codex/`。

如果你没有迁 `auth.json`，在新 Mac 上重新登录 Codex 即可。

## Validation On The Mac

迁移后先做这几个检查：

```bash
cd "~/projects/Sourcing AI Agent Dev/sourcing-ai-agent"

source scripts/dev_postgres_env.sh
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_local_postgres.py tests/test_settings.py
```

如果 `show-control-plane-runtime` 里看到：

- `resolved_control_plane_postgres_dsn` 非空
- `resolved_control_plane_postgres_live_mode = postgres_only`
- `default_db_path = .../runtime/control_plane.shadow.db`

就说明项目已经把 Mac 上的 PG 识别成 authoritative control plane 了。

如果你还要验证指定 Codex session 是否还在，可检查：

```bash
rg -n "019d6630-2137-7f70-b742-43f979b8207b" ~/.codex/sessions
```

## Notes

### History / review / target-candidate pages

历史记录、review plan、目标候选人页等状态主要来自：

- Postgres control plane 表
- `runtime/jobs/`
- `runtime/company_assets/`

因此迁移时把这三类一起带走，前端历史与资产关联才能最大程度保真。

### Exact forensic copy vs runnable migration

如果你的目标是“保留一份旧 WSL 环境的法证快照”，可以额外保留整机或整目录冷备份。

但如果目标是“在新 Mac 上继续开发并运行项目”，应以前面的 portable migration 为主，而不是继续依赖 Linux data dir / venv / node_modules。
