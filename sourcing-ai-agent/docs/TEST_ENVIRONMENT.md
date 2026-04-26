# Test Environment

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义一套与本地开发环境隔离的测试环境启动方式，目标是让前端联调、hosted smoke、browser E2E、以及未来 CI 都能挂到同一个独立 namespace，而不污染默认 `runtime/`。

## 目标

- 独立 `runtime` / shadow db / `jobs` / `company_assets`
- 独立 object storage prefix
- 默认走 `simulate`，避免测试时误触发真实 provider
- 可按需切换到 `scripted`
- 前端、本地脚本、CI 使用同一套入口

## 默认约定

- runtime dir: `runtime/test_env`
- backend port: `8775`
- frontend port: `4175`
- object storage prefix: `sourcing-ai-agent-test`
- external provider mode: `simulate`
- runtime environment: `test`

## 标准入口

在仓库根目录运行：

```bash
make test-env-backend
make test-env-frontend
make test-env
make test-env-status
make test-env-stop
make test-env-logs
make test-env-clean
make test-env-seed-assets TEST_ENV_SEED_COMPANIES="anthropic"
```

说明：

- `make test-env-backend`
  - 启动隔离测试 backend
  - 自动设置：
    - `SOURCING_RUNTIME_DIR=runtime/test_env`
    - `SOURCING_RUNTIME_ENVIRONMENT=test`
    - `OBJECT_STORAGE_PREFIX=sourcing-ai-agent-test`
    - `SOURCING_EXTERNAL_PROVIDER_MODE=simulate`
- `make test-env-frontend`
  - 前端默认连 `http://localhost:8775`
- `make test-env`
  - backend 后台启动，frontend 前台启动
- `make test-env-status`
  - 检查隔离 runtime 下的 service status / health / frontend root
- `make test-env-logs`
  - 只看隔离 runtime 的 helper 日志
- `make test-env-clean`
  - 清理隔离 runtime 下的 helper log / pid / stale service state，不动 workflow/job/company asset 数据
- `make test-env-seed-assets TEST_ENV_SEED_COMPANIES="anthropic xai"`
  - 把默认 `runtime/` 里的 authoritative company snapshot 以轻量链接方式注入到隔离 test env
  - 自动回填隔离环境自己的 `organization_asset_registry` / `organization_execution_profile`
  - 默认只链接 authoritative snapshot，不会整目录复制历史 company assets

## Control Plane 默认约定

- 测试环境默认不再继承仓库或父目录的 `.local-postgres.env`
- `scripts/dev_backend.sh` 会在 `test/simulate/scripted/replay` runtime 下自动写入：
  - `runtime/test_env/.isolated-local-postgres.env`
- 这个空 sentinel 会阻断 repo-level PG fallback，避免测试 job / assets / registry 写进本地或生产 PG namespace
- 如果确实要让某个测试 runtime 使用 PG，必须显式提供 runtime 专属 env file：
  - `SOURCING_LOCAL_POSTGRES_ENV_FILE=/path/to/test-postgres.env`
- 不建议用共享 repo PG 做测试；临时排障需要显式设置：
  - `SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1`
- 本地 PG 自动发现与排障入口见：
  - `docs/LOCAL_POSTGRES_CONTROL_PLANE.md`
- 更完整的 runtime namespace / provider cache 隔离契约见：
  - `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`

## 注入真实本地资产

如果你要在隔离环境里验证 “已有本地资产复用” 这类真实回归，例如 Anthropic 全量资产复用：

```bash
make test-env-seed-assets TEST_ENV_SEED_COMPANIES="anthropic"
```

如果你想严格复制而不是软链接：

```bash
make test-env-seed-assets \
  TEST_ENV_SEED_COMPANIES="anthropic xai" \
  TEST_ENV_SEED_LINK_MODE=copy
```

底层脚本也可以直接运行：

```bash
PYTHONPATH=src python3 scripts/seed_test_env_assets.py \
  --source-runtime-dir runtime \
  --target-runtime-dir runtime/test_env \
  --company anthropic \
  --company xai
```

`seed_test_env_assets.py` 现在要求 source / target runtime 都能解析到 Postgres control-plane DSN；它不再从磁盘 SQLite source fallback 读取 registry。

## 切换为 Scripted

如果你要验证长尾 provider 行为，而不是纯 simulate：

```bash
make test-env-backend \
  TEST_EXTERNAL_PROVIDER_MODE=scripted \
  TEST_SCRIPTED_PROVIDER_SCENARIO=configs/scripted/google_multimodal_long_tail.json
```

## 真实回归口径

如果你要在隔离测试环境里跑真实 provider 回归，建议把口径拆成两层，而且默认只做手动触发，不做周期性默认任务：

### 1. Anthropic 这类“已有 authoritative 全量本地资产”的复用型 query

建议验证：

- explain / dispatch 仍可能保留 `delta_from_snapshot`
- 但结果语义必须是：
  - `default_results_mode = asset_population`
  - `asset_population.available = true`
  - `asset_population.candidate_count > 0`

也就是说，这类回归的通过标准是“结果页默认视图已经回到公司全量资产”，而不是必须要求 planner 内部完全没有 delta 语义。

### 2. xAI 这类“大组织 full-roster live” query

建议拆成两层检查：

- explain 层：
  - `plan_primary_strategy_type = full_company_roster`
  - 方向型 query 例如 `我要 xAI 做 Coding 方向的全部成员` 必须是 `scoped_search_roster`
- live workflow 层：
  - 允许 workflow 在短 smoke 窗口内停留在 `running` / `blocked`
  - 但必须已经进入正确 acquisition 路径，并落出 shard 元数据

推荐把 live full-roster 的通过标准写成：

- workflow 已提交或加入对应 xAI job
- stage 进入 `acquiring`
- worker summary 出现 `waiting_remote_harvest`
- `runtime/test_env/company_assets/xai/<snapshot_id>/harvest_company_employees/adaptive_shard_plan.json` 已生成

### 为什么 xAI live full-roster 不适合做“必须快速 completed”的 smoke

真实 provider 下，xAI 的 United States root scope 在 probe 中可能已经接近或达到 2000+ 结果。即使它仍低于单次 provider cap（例如 2500），Harvest actor 也可能真实跑很多分钟，期间 workflow 会处于：

- `blocked_task = acquire_full_roster`
- acquisition worker `waiting_remote_harvest`

这不是状态机错误，而是 live provider 长耗时任务的正常形态。此时更合理的验收标准是：

- 边界判断正确
- shard policy 正确
- overflow / non-overflow 记录正确

如果 probe 估计总量没有超过 provider cap，那么这次 run 不会生成 overflow 记录；这同样属于“符合预期”。

另外，“是否还需要补剩余 LinkedIn profile tail” 也不应该只根据 simulate/scripted 判断：

- simulate/scripted 只能证明编排、worker recovery、background reconcile、artifact materialization 这些机制是对的
- 只有 live provider run 才能告诉我们真实还剩多少 profile detail 没取回，以及这些 tail 是 provider 限流、分页、排队、还是召回本身造成的
- 否则很容易把 synthetic scenario 里故意制造的 tail，误当成生产环境里必须立刻补抓的真实 tail，平白增加成本

所以更稳妥的顺序是：

1. 先用 simulate/scripted 确认 workflow 不会断、overflow 记录正确、profile completion tail 能被 background reconcile 收尾
2. 再显式手动跑一次 xAI live large-org 回归
3. 只有 live 结果里确实还存在 residual tail，再决定是否做额外的 LinkedIn profile backfill

现在 scripted 侧已经把 large-org 长尾拆成通用行为夹具：

- `configs/scripted/large_org_full_roster_overflow.json`
  - 验证 full-roster overflow / worker recovery / shard 收敛
- `configs/scripted/large_org_profile_tail_reconcile.json`
  - 验证 roster 完成后 profile tail 的 background reconcile
- hosted smoke 里还新增了“profile tail 已完成后不再重复抓取”的回归

这些 fixture 当前仍用 xAI 作为具体 driver query，但语义上是通用 large-org 行为，不是 xAI 专属逻辑。

## 对前端与回归的意义

- 前端联调可以稳定挂到固定测试 backend，而不抢占默认开发端口
- hosted simulate smoke / browser E2E 可以复用同一套 runtime namespace
- 后续 CI 只需要起：
  - `make test-env-backend`
  - `make test-env-status`
  - 回归脚本或 browser suite

## 手动 Live 入口

默认不要直接把 live provider 回归混入 `make test-*` 快回归。

建议显式分两步：

```bash
cd "sourcing-ai-agent"
make test-env-backend-live LIVE_CONFIRM=1
make test-live-large-org-manual LIVE_CONFIRM=1
```

说明：

- `make test-env-backend-live`
  - 启一个独立 `runtime/test_env_live` + `http://localhost:8777` 的 live backend
  - 使用 `SOURCING_RUNTIME_ENVIRONMENT=test`，不共享 production provider cache / PG namespace
  - 明确需要 `LIVE_CONFIRM=1`
- `make test-live-large-org-manual`
  - 默认跑：
    - `xai_full_roster`
    - `xai_coding_all_members_scoped`
  - 底层脚本是：
    - [scripts/run_live_large_org_regression.py](../scripts/run_live_large_org_regression.py)
  - 这层验收重点是：
    - full-roster / scoped-search 边界是否正确
    - shard policy 是否正确
    - workflow 是否进入预期 acquisition 路径
  - 不要求所有 case 都在短窗口内 `completed`

## 推荐回归梯度

建议以后固定按这三层顺序跑，而不是直接打真实 provider：

1. `simulate`
   - 默认快回归
   - 验证 intent、plan、前后端主链路、结果语义
2. `scripted`
   - 验证大组织长尾 pending / retry / overflow / 阶段切换 / profile-completion tail
   - 入口：`make test-scripted-large-org`
3. `live`
   - 默认只做手动真实回归；如果以后要夜间跑，也应显式 opt-in
   - 重点验证真实 provider 召回质量、耗时、成本与 registry/materialization 落库效果

这样可以先用零成本或低成本环境确认工作流正确，再把真实 API 调用留给最后一层。

## Runtime Override 规则

现在 `load_settings()` 已支持：

- `SOURCING_RUNTIME_DIR`
  - 覆盖 runtime 根目录
- `SOURCING_SECRETS_FILE`
  - 可显式指定 provider secrets 文件
- `SOURCING_DB_PATH`
  - 可显式指定本地 compatibility-shadow 路径
- `SOURCING_JOBS_DIR`
  - 可显式指定 jobs 目录
- `SOURCING_COMPANY_ASSETS_DIR`
  - 可显式指定 company assets 目录

如果你只改了 `SOURCING_RUNTIME_DIR`，但该 runtime 下还没有 `secrets/providers.local.json`，系统会回退到默认 `runtime/secrets/providers.local.json`，这样测试环境可以复用同一份 provider secrets，而不用复制敏感文件。
