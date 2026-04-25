# Local Postgres Control Plane

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义本仓库本地 `Postgres-first` control-plane 的默认发现、启动与排障方式，避免再出现“本地明明有 PG 资产，但运行时没有识别到 DSN”的情况。

## 默认约定

- live control plane 默认目标是 Postgres，而不是磁盘 SQLite
- 如果存在显式的 `.local-postgres.env` / `connection.env`，系统也会把它视为本地 PG 发现入口
- 当仓库或其上级目录存在 `.local-postgres/` 且其中包含：
  - `extract/`
  - `data/`
- 系统会自动推导：
  - `SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://sourcing@127.0.0.1:55432/sourcing_agent`
  - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
- 当检测到可用本地 PG 时，`load_settings()` 默认把 `db_path` 设为：
  - `runtime/control_plane.shadow.db`

这个 `control_plane.shadow.db` 只是本地 shadow / compatibility 容器，不再是 live authoritative store。

## 一条命令确认当前解析结果

在仓库根目录运行：

```bash
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
```

这条命令会直接打印：

- 当前解析到的 `resolved_control_plane_postgres_dsn`
- `resolved_control_plane_postgres_live_mode`
- 默认 `db_path`
- SQLite shadow 是否是 `shared_memory`
- 本地 `.local-postgres` 是否可用、是否已启动
- 当前是否通过 `.local-postgres.env` / `connection.env` 提供了显式配置
- 本地 PG 的 `server_encoding` / `database_encoding`

如果你不确定问题是不是 DSN，而是 shell / venv / PATH 选错了，先跑：

```bash
make dev-doctor
```

优先把解释器和脚本入口确认干净，再判断是不是 Postgres 发现问题。

## 编码健康要求

- 本地 control-plane PG 必须是 `UTF8`
- 如果你看到：
  - `server_encoding = SQL_ASCII`
  - 或 `database_encoding = SQL_ASCII`
- 这说明当前 `.local-postgres` 是历史坏库；虽然代码现在会在连接层强制 `client_encoding=UTF8` 并对脏值做读写归一化，但这只是兼容兜底，不是理想终态
- `SQL_ASCII` 的直接风险是：
  - psycopg 把 text 列返回成 `bytes`
  - 上层一旦对这些值做 `str(...)`，就会生成 `"b'...'"` 脏字符串
  - 这些脏字符串再被写回 control plane，会污染 `snapshot_id`、`target_company`、`default_acquisition_mode`、甚至表名/标识符

推荐检查：

```bash
source scripts/dev_postgres_env.sh
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
```

如果确认是 `SQL_ASCII`，推荐重新准备一个 UTF8 的本地 PG cluster / database，再做一次 control-plane sync；不要把 `SQL_ASCII` 当成长期开发基线。

## Python 依赖

- 纯 Postgres control-plane 路径要求当前 Python 环境可导入 `psycopg[binary]`
- 项目 `pyproject.toml` 已将它声明为正式依赖
- 在 Mac 上不要直接假设 Homebrew/system `python3` 就是项目解释器；优先使用仓库 `.venv/bin/python`
- `bash ./scripts/dev_backend.sh` 与 `bash ./scripts/run_hosted_trial_backend.sh` 现在也会优先选择仓库 `.venv/bin/python`
- 如果你切换到了新的解释器或新虚拟环境，先确认：

```bash
.venv/bin/python -c "import psycopg, requests; print(psycopg.__version__); print(requests.__version__)"
```

如果 `show-control-plane-runtime` 报 `ModuleNotFoundError: requests`，通常不是仓库缺依赖，而是你跑到了系统解释器。

## 发现优先级

DSN 解析按这个顺序：

1. 显式环境变量 `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
2. 显式环境文件 `SOURCING_LOCAL_POSTGRES_ENV_FILE`
3. 当前仓库及其父目录中的：
   - `.local-postgres.env`
   - `.local-postgres/connection.env`
4. `SOURCING_LOCAL_POSTGRES_ROOT` 或 `LOCAL_PG_ROOT`
5. 当前仓库及其父目录中的 `.local-postgres/`

因此：

- 如果你要连远端或手动指定 PG，直接显式 export `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
- 如果你要用 Linux/WSL 风格的本地 PG，通常不需要手动写 DSN，只需要保证 `.local-postgres/` 存在
- 如果你要用 Mac/Homebrew/外部 PG，推荐在 repo 根目录放 `.local-postgres.env`

## 推荐的 env 文件

跨平台开发时，推荐从这份示例开始：

```bash
cp configs/local_postgres.env.example .local-postgres.env
```

最小内容例如：

```bash
SOURCING_CONTROL_PLANE_POSTGRES_DSN=postgresql://sourcing@127.0.0.1:55432/sourcing_agent
SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only
```

这样即使你的 Postgres 并不在 `.local-postgres/{extract,data}` 下，运行时和 `scripts/dev_postgres_env.sh` 也能正确识别。

## import 后刷新模式

`import_cloud_assets(...)` 在导入完 bundle / generation 后，还可能继续做两类后置刷新：

- organization asset registry warmup
- linkedin profile registry backfill

在 PG 环境里，这两项现在默认会走后台线程，不再阻塞 import 返回；这样 ECS 上第一次冷启动公司资产时，不会把 scoped search 前台请求拖成“导入成功但要等 warmup/backfill 跑完才能返回”。

如需强制切回同步模式，可显式设置：

```bash
export SOURCING_IMPORT_POST_REFRESH_MODE=inline
```

如需显式保持后台模式：

```bash
export SOURCING_IMPORT_POST_REFRESH_MODE=background
```

后台任务状态会落到：

```text
runtime/maintenance/import_refresh_jobs/*.json
```

## 推荐入口

在仓库根目录运行：

```bash
source scripts/dev_postgres_env.sh
```

这个脚本会：

- 优先读取 `.local-postgres.env` / `.local-postgres/connection.env`
- export `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
- export `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
- export `SOURCING_LOCAL_POSTGRES_ROOT`
- 把本地 PG binary 加入 `PATH`

如果你之后还要起 backend，推荐继续用这些脚本：

```bash
source scripts/dev_postgres_env.sh
pg_isready -h 127.0.0.1 -p "$LOCAL_PG_PORT" -d postgres -U "$LOCAL_PG_USER"
```

## 自动启动行为

当代码路径使用自动发现到的本地 PG，且没有显式远端 DSN 时：

- live control-plane adapter 会在连接前尝试启动本地 PG
- control-plane snapshot / runtime sync 到 PG 时也会先尝试启动本地 PG

因此本地开发一般不再要求你先手动 export DSN 再手动起库。

## 常用排障命令

```bash
source scripts/dev_postgres_env.sh
pg_isready -h 127.0.0.1 -p "$LOCAL_PG_PORT" -d postgres -U "$LOCAL_PG_USER"
psql "$SOURCING_CONTROL_PLANE_POSTGRES_DSN" -Atqc "select current_database(), current_user;"
```

## 从 runtime 重建 PG control plane

如果你已经迁移了 `runtime/company_assets` / `runtime/jobs`，但新的 Postgres 只有部分表有数据，优先使用正式重建命令，而不是手工逐个 backfill：

```bash
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli rebuild-runtime-control-plane
```

常用变体：

```bash
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli rebuild-runtime-control-plane --company OpenAI
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli rebuild-runtime-control-plane --skip-jobs
PYTHONPATH=src .venv/bin/python -m sourcing_agent.cli rebuild-runtime-control-plane --skip-company-assets
```

这条命令会：

- 从 `runtime/company_assets` 重建 organization asset registry / generation / membership
- 必要时先修复缺失的 normalized manifests
- 从 `runtime/jobs/*.json` 重建 `jobs` 和 `job_result_views`
- 把旧机器遗留的 snapshot `source_path` 尽量改写成当前机器可解析的本地路径

不会自动重建的部分：

- `frontend_history_links`
  - `history_id` 不持久化在 `runtime/jobs` 里，无法无损逆向恢复
- 浏览器本地缓存 / LocalStorage
- profile registry
  - 如需回填，继续使用 `backfill-linkedin-profile-registry`

如果 PG 没起来：

```bash
source scripts/dev_postgres_env.sh
pg_ctl -D "$LOCAL_PG_DATA" -l "$LOCAL_PG_RUN/postgres.log" -o "-k $LOCAL_PG_RUN -p $LOCAL_PG_PORT -h 127.0.0.1" start
```

## 迁移与磁盘语义

- `runtime/sourcing_agent.db` 不应再被视为 ECS / hosted 的必需资产
- Linux/WSL 的 `.local-postgres/data` 也不应被视为跨平台可直接复制的资产
- hosted 默认路径应是：
  - Postgres control plane
  - generation-first object storage
  - local hot cache
- `sqlite_snapshot` 仅保留 backup / portability 语义，不是默认 hosted 主路径
- 如果要把环境迁到另一台 Mac，优先使用：
  - Postgres logical dump
  - `.local-postgres.env`
  - `docs/MAC_DEV_ENV_MIGRATION.md`

## 开发要求

- 新代码不要再假设“只有 `runtime/sourcing_agent.db` 才是 authoritative”
- 新文档、脚本和排障说明默认写 Postgres-first 路径
- 如果某个模块还需要 SQLite fallback，必须说明它是 temporary compatibility path，而不是长期主路径
