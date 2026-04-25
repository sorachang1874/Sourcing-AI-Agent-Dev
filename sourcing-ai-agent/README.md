# Sourcing AI Agent

> Status: Repository entry doc. Read together with `docs/INDEX.md` and `PROGRESS.md` before making contract or deployment changes.


一个面向 Agent 化开发的通用 Sourcing 后端 MVP。当前版本先基于 `Sourcing AI Agent Dev` 中已经沉淀的 Anthropic 调研资产，把更完整的 workflow 跑通：

`用户意图 -> criteria 澄清 -> acquisition plan -> 异步 sourcing workflow -> retrieval -> result artifact`

开始在这个子项目里改代码之前，先阅读 monorepo 根目录的协作规则 [../CONTRIBUTING.md](../CONTRIBUTING.md)。

## Documentation Map

当前有效的文档入口：

- [../ONBOARDING.md](../ONBOARDING.md)
- [PROGRESS.md](PROGRESS.md)
- [docs/INDEX.md](docs/INDEX.md)
- [docs/FRONTEND_API_CONTRACT.md](docs/FRONTEND_API_CONTRACT.md)
- [docs/WINDOWS_WSL2_FRONTEND_SETUP.md](docs/WINDOWS_WSL2_FRONTEND_SETUP.md)
- [docs/WORKFLOW_OPERATIONS_PLAYBOOK.md](docs/WORKFLOW_OPERATIONS_PLAYBOOK.md)
- [docs/TESTING_PLAYBOOK.md](docs/TESTING_PLAYBOOK.md)
- [docs/LOCAL_POSTGRES_CONTROL_PLANE.md](docs/LOCAL_POSTGRES_CONTROL_PLANE.md)
- [docs/MAC_DEV_ENV_MIGRATION.md](docs/MAC_DEV_ENV_MIGRATION.md)
- [docs/ECS_ACCESS_PLAYBOOK.md](docs/ECS_ACCESS_PLAYBOOK.md)
- [docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md](docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md)
- [docs/HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md](docs/HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md)
- [docs/ALIYUN_ECS_TRIAL_ROLLOUT.md](docs/ALIYUN_ECS_TRIAL_ROLLOUT.md)
- [docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md](docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md)
- [docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md](docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md)

前端集成若需要直接复用类型和调用示例，可参考：

- [contracts/frontend_api_contract.ts](contracts/frontend_api_contract.ts)
- [contracts/frontend_api_adapter.ts](contracts/frontend_api_adapter.ts)
- [contracts/frontend_react_hooks.example.tsx](contracts/frontend_react_hooks.example.tsx)
- [contracts/frontend_runtime_dashboard.example.tsx](contracts/frontend_runtime_dashboard.example.tsx)

历史 handoff / retrospective / todo 文档仍保留，但已经在 [docs/INDEX.md](docs/INDEX.md) 中标记为 reference-only。
当前所有一方 Markdown 也都带有 `> Status:` 头，先看状态再决定能否把该文档内容当成当前事实。

## Current Stable Validation Snapshot

截至 `2026-04-25`，当前稳定基线使用仓库测试虚拟环境验证：

```bash
./.venv-tests/bin/python -m pytest -q
```

最近一次结果：

- `1135 passed, 8 skipped, 25 subtests passed`
- frontend build：`cd frontend-demo && npm run build`
- browser E2E：`SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q`
- hosted smoke：`./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q`
- typecheck：`bash ./scripts/run_python_quality.sh typecheck`

默认本地开发规则：

- 测试命令优先用 `./.venv-tests/bin/python`
- 本地 backend / CLI 优先用仓库 `./.venv/bin/python`，可通过 `bash ./scripts/dev_backend.sh --print-config` 查看实际解释器
- 不要用系统 `python3` 解释测试失败，除非已经确认仓库虚拟环境不可用
- 大范围改动后先看 `PROGRESS.md` 和 `docs/NEXT_TODO.md`，避免重复修复已收口的问题

## Control Plane Default

- 当前 live control plane 默认是 `Postgres-first`
- 如果仓库或其父目录存在 `.local-postgres/`，或存在 `.local-postgres.env` / `.local-postgres/connection.env`，运行时会自动发现 DSN 并进入 `postgres_only`
- 当前环境到底解析到了什么，可直接检查：

```bash
PYTHONPATH=src "$(./scripts/dev_backend.sh --print-config 2>/dev/null | sed -n 's/^python_bin=//p' | head -n 1)" -m sourcing_agent.cli show-control-plane-runtime
```

- `export-sqlite-snapshot` / `restore-sqlite-snapshot` 现在是 legacy alias；默认 backup/export 路径应优先使用 `control_plane_snapshot`

## Mac Migration

- 如果要把当前本地环境迁到另一台 Mac，优先参考 [docs/MAC_DEV_ENV_MIGRATION.md](docs/MAC_DEV_ENV_MIGRATION.md)
- 当前推荐同时保留两份语义明确的迁移产物：
  - portable migration bundle
    - 目标是“在 Mac 上尽快恢复为可运行开发环境”
  - full local snapshot
    - 目标是“尽可能完整保留旧 Linux/WSL 工作树、runtime、Codex 会话与取证上下文”
- 迁移传输方式优先级建议：
  - 1. 机器直连 `rsync/scp`
  - 2. 外接 SSD / 局域网共享盘
  - 3. OSS / R2 / S3 之类对象存储中转
- 如果你的目标是继续 `codex resume <session_id>`，建议直接导出完整 `~/.codex`，不要只拷贝 `history.jsonl`

## Provider Defaults

- 默认低成本文本模型建议使用 `qwen-flash`
- 若本地 `runtime/secrets/providers.local.json` 未显式覆盖，Qwen 默认模型现在也是 `qwen-flash`
- 主模型健康检查可用：
  - `PYTHONPATH=src python3 -m sourcing_agent.cli test-model`

## External Provider Mode

为避免在 workflow 编排测试时反复消耗外部 API，可以通过环境变量切换外部 provider 模式：

- `SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - 默认模式
  - 会真实调用 Harvest / Search / model / semantic provider
- `SOURCING_EXTERNAL_PROVIDER_MODE=replay`
  - 只复用当前隔离 runtime 内的 snapshot-local 缓存或显式 replay fixture
  - 若缓存未命中，则返回空结果
  - 不会再生成 `_offline` 占位成员，也不会读取或写回 live shared provider cache
  - 不发真实 Harvest / Search / model / semantic 请求
- `SOURCING_EXTERNAL_PROVIDER_MODE=simulate`
  - 完全不触发外部 Harvest / Search / model / semantic 请求
  - 返回可被 workflow 消化的模拟 provider 结果，主要用于低成本 smoke test、调度/恢复测试、前后端联调
- `SOURCING_EXTERNAL_PROVIDER_MODE=scripted`
  - 通过 `SOURCING_SCRIPTED_PROVIDER_SCENARIO=/path/to/scenario.json` 驱动外部 provider 行为
  - 可以模拟 search / harvest 的 pending 多轮、ready/fetch 分批、重试型错误与长尾等待
  - 适合低成本复现“大组织 workflow 卡在 provider 长耗时”这类问题

示例：

- 生产 / 本地 / 测试还应同时设置或让系统推断 `SOURCING_RUNTIME_ENVIRONMENT`
- runtime namespace、PG 与 provider cache 隔离规则见 `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_humansand_coding_researchers.json
SOURCING_EXTERNAL_PROVIDER_MODE=scripted SOURCING_SCRIPTED_PROVIDER_SCENARIO=configs/scripted/reflection_pending.json PYTHONPATH=src python3 -m sourcing_agent.cli explain-workflow --file configs/demo_workflow_humansand_coding_researchers.json
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py --strict
```

如果你不想让脚本直接打当前常驻 backend，也不想误用日常 `runtime/`，现在可以让 explain/smoke 脚本自带隔离 runtime：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --runtime-dir runtime/test_env/explain_matrix \
  --seed-reference-runtime \
  --fast-runtime \
  --strict

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/simulate_matrix \
  --seed-reference-runtime \
  --fast-runtime \
  --strict
```

说明：

- `--runtime-dir` 会让脚本自起一个 in-process backend，并把 `SOURCING_RUNTIME_DIR` 固定到该目录
- 若未显式传 `--runtime-env-file`，脚本会自动写一个空的 local-postgres env sentinel，阻断仓库根 `.local-postgres.env` 的泄漏
- 如果要做“接近本地真实数据”的 scripted 模拟，先把 authoritative snapshot 种到 `runtime/test_env/...`，再把脚本指向该目录：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/seed_test_env_assets.py \
  --source-runtime-dir runtime \
  --target-runtime-dir runtime/test_env/local_like_smoke \
  --company OpenAI \
  --company Anthropic

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/local_like_smoke \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/reflection_pending.json \
  --strict
```

注意：

- 这个模式现在会统一接管高成本外部 provider，包括 Harvest、search、model、semantic
- Postgres control plane、snapshot 落盘、workflow 编排、阶段推进、恢复逻辑仍会真实执行
- `simulate/replay/scripted` 的目标是测试 orchestration 与恢复语义，不是直接替代真实召回质量验证
- 这些模式只替代高成本外部 provider；Postgres control plane、snapshot 落盘、progress/results、阶段推进与恢复逻辑仍是真实执行路径

`scripts/run_simulate_smoke_matrix.py` 默认会按内建 matrix 依次覆盖：

- Skild AI
- Humans&
- Anthropic
- OpenAI
- Google

也可以切到自定义 matrix：

```bash
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py \
  --matrix-file configs/simulate_smoke_matrix.example.json \
  --case google_multimodal_pretrain
```

自动化 hosted smoke 的口径现在是：

- 默认 `PYTHONPATH=src python3 -m unittest tests.test_hosted_workflow_smoke -v`
  - 跑 3 条代表性 flow，控制日常回归时长
- `SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1 PYTHONPATH=src python3 -m unittest tests.test_hosted_workflow_smoke -v`
  - 跑完整 5-case hosted matrix

默认开发回归口径：

- `bash ./scripts/run_regression_suite.sh fast`
  - 默认 fast full regression，不包含 browser E2E
- `bash ./scripts/run_regression_suite.sh browser`
  - 显式跑 Playwright/browser workflow E2E
- `bash ./scripts/run_regression_suite.sh all`
  - 先跑 fast regression，再跑 browser E2E
- `bash ./scripts/run_python_quality.sh all`
  - 跑 ruff + mypy
- `cd frontend-demo && npm run build`
  - 跑前端构建验证

如果你想把 simulate/scripted smoke 压到最快，相关轮询 cooldown 现在支持运行时配置，例如：

- `WEB_SEARCH_READY_COOLDOWN_SECONDS=0`
- `WEB_SEARCH_FETCH_COOLDOWN_SECONDS=0`
- `SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS=0`
- `EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `EXPLORATION_FETCH_MIN_INTERVAL_SECONDS=0`

若使用外部常驻 `serve` 做 smoke，现在不必只靠 server 进程环境变量。你还可以直接让 client 按 job 传：

- `execution_preferences.runtime_tuning_profile=fast_smoke`

这个 profile 现在会同时缩短 web/seed/exploration cooldown，以及 Harvest 的 probe poll、dataset retry backoff、scripted sleep，所以 hosted smoke 可以在不改 server 默认档位的前提下更快收敛。

脚本入口已经支持：

```bash
PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py \
  --base-url http://127.0.0.1:8765 \
  --runtime-tuning-profile fast_smoke \
  --strict
```

这样会把快速 cooldown 只作用在这次 smoke job 上，不会污染整台 hosted 服务的默认运行档位。

如果要看“单元测试 / hosted simulate smoke / hosted scripted long-tail / live validation”各自负责什么，以及如何新增 smoke case / scripted scenario，请看：

- [docs/TESTING_PLAYBOOK.md](docs/TESTING_PLAYBOOK.md)

前端 browser E2E 现在也拆成了快慢套件：

- `make test-browser-e2e-fast`
  - 默认日常回归；覆盖小组织 full reuse、中型组织 baseline reuse、以及新组织 runtime identity + full roster
- `make test-browser-e2e-slow`
  - 显式跑大组织 scoped delta / partial shard covered 这类较重场景
- `make test-browser-e2e-full`
  - 跑 fast + slow 的完整 browser matrix

如果你直接跑模块：

- `PYTHONPATH=src python3 -m unittest tests.test_frontend_browser_e2e -v`
  - 只会执行 fast suite，slow suite 默认 skip
- `SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src python3 -m unittest tests.test_frontend_browser_e2e -v`
  - 执行完整 browser matrix

## Canonical Cloud Assets

服务器恢复默认不应依赖 `company_handoff` 或 `sqlite_snapshot`，而应优先恢复：

- 每个 canonical company 1 个 `company_snapshot`
- 如需 hosted / 跨机热恢复，优先走 generation-first hydrate
- `sqlite_snapshot` 仅保留作 backup-only / portability-only 兜底，不再是默认恢复主路径

推荐直接使用统一导入命令，而不是手工串 `download-asset-bundle -> restore-* -> backfill-*`：

```bash
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets \
  --bundle-kind company_snapshot \
  --bundle-id <company_snapshot_bundle_id> \
  --output-dir runtime/asset_imports
```

对于 `company_snapshot`，这个命令会在 restore 后自动补：

- candidate artifact repair
- organization asset registry warmup
- linkedin profile registry backfill

`serve` 启动时不会默认跑全量 organization asset warmup。需要冷启动预热时显式设置
`STARTUP_ORGANIZATION_ASSET_WARMUP_ENABLED=true`，避免每次启动都扫描全部本地资产并拖慢前台 plan 请求。

当前推荐的 canonical bundle 清单、实际 bundle id、恢复顺序与去重规则见：

- [docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md](docs/CANONICAL_CLOUD_BUNDLE_CATALOG.md)

这样可以让 hosted runtime 在启动后先复用已有 registry / snapshot / profile raw，而不是误触发新的大规模抓取。

## 当前目标

- 把历史调研资产转成结构化候选池，而不是继续依赖对话上下文
- 把 Anthropic 特例抽象为通用 workflow engine，可扩展到 xAI 等其他公司
- 把 acquisition 作为第一等公民，而不是只做最后一步检索
- 先跑通本地数据驱动 MVP，再接入外部 source adapters 与 Claude Code

## 当前能力

- 优先读取项目内 `local_asset_packages/anthropic/`，并保留从外部 `Anthropic华人专项` 目录做一次性导入的兼容能力
- 将在职员工、离职员工、投资方成员、Scholar 线索导入 Postgres control plane / registry
- 支持从原始用户请求生成 sourcing plan
- plan 阶段新增 `intent_brief`
  - 显式输出 `identified_request / target_output / default_execution_strategy / review_focus`
  - 用于产品原生第一段交互，也方便后续修正 Claude / Qwen 的意图识别表现
  - `planning_mode=heuristic` 走纯 deterministic planning
  - `planning_mode=model_assisted` 先让模型做请求归一化，再走 deterministic brief / intent / search planning
  - 若要实验模型直接写 brief / search planning，可使用更激进的 planning mode
- 支持 `AI-first request normalization + effective intent contract`
  - `request_preview.intent_axes` 现在不再只是前端 preview 字段
  - execution 层会通过 `resolve_request_intent_view(...)` 与 `build_effective_request_payload(...)` 物化同一份 effective intent
  - planning、plan review、acquisition、search seed discovery、scoring、semantic retrieval 现在共享同一套 `target_company / organization_keywords / employment_statuses / execution_preferences` 语义
  - `plan_workflow / start_workflow` 返回的 `request` 与 `request_preview` 现在也会对齐 execution 语义
  - 如果 acquisition strategy 扩展了真正执行用的关键词，例如 `Pre-train`，前端看到的 `request.keywords` / `request_preview.keywords` 会同步反映，而不再只停留在原始归一化结果
- 支持 `Plan Review Gate`
  - plan 阶段会生成 `plan_review_gate`
  - 对 scoped roster、investor firm roster、高成本 source 等场景要求先 review 再执行 workflow
  - review 时可补充 `extra_source_families`、确认 `company_scope`、决定是否允许高成本 source
- 支持 `AcquisitionStrategyCompiler`，在 plan 阶段输出目标人群边界、roster 获取策略、低成本优先的 slug resolution 顺序和待确认成本点
- 支持 `PublicationCoveragePlanner`，在 plan 阶段输出 publication / engineering / blog / docs 等 source family coverage 方案
- 支持 `LLM-driven Search Planner`
  - plan 阶段会产出 `search_strategy`
  - 将 query 编译成具名 query bundle，例如 `relationship_web / publication_surface / public_interviews / targeted_people_search`
  - 支持把“公开访谈 / Podcast / YouTube”这类新 sourcing 方法沉淀为 source family，并进入 plan review / execution
  - 新增 source family 前，默认先进入用户交互：
    - 确认目标、覆盖范围、停止条件、成本容忍度
    - 确认是只做 surface-level asset capture，还是要继续做深挖
    - 确认新增链路是否满足数据资产落盘、可审计、可扩展三项约束
- 支持稳定的 `search provider abstraction`
  - low-cost search 已不再绑定到单一 DuckDuckGo HTML endpoint
  - 当前 provider chain 支持 `dataforseo_google_organic -> serper_google -> google_browser -> bing_html -> duckduckgo_html` 的优先级切换
  - `search_seed_discovery / slug_resolution / exploratory_enrichment` 已统一走同一套 provider 接口
  - search raw payload 现在会按 provider 的 `html/json` 形态落盘，便于缓存复用与审计
  - 当前环境下 DuckDuckGo 仍可能出现 TLS EOF，因此 production 推荐显式配置稳定 provider
  - 已接入 `DataForSEO Google Organic`：
    - 同步 lane 可走 `live/regular`
    - 低成本后台批量更推荐 `Standard Queue`
    - `search_planner / public_media_specialist / exploration_specialist` 的 worker 恢复链路已支持 `task_post -> tasks_ready -> task_get`
    - 已提供 `scripts/dataforseo_google_organic.py` helper 与 `docs/DATAFORSEO_PLAYBOOK.md`
  - `google_browser` lane 现已接到 Playwright/Chromium provider
  - 对于 OpenClaw 这类“无需 search API 也能做 Google Search”的 Agent，底层思路更接近 `browser automation / Playwright`，而不是普通 search API
  - 当前这台 WSL 机器上，browser lane 仍受系统共享库 `libnspr4.so` 缺失影响
  - 当前代码已补可执行的缺库报错提示，若 live Google query 失败，会直接指出缺失库与建议安装包
- 支持 `agent runtime`
  - workflow / retrieval 已按 specialist lanes 记录 runtime session 和 trace spans
  - 当前 lane 包括 `triage_planner / search_planner / acquisition_specialist / enrichment_specialist / exploration_specialist / retrieval_specialist / review_specialist`
  - investor / public media 场景会补充 `investor_graph_specialist / public_media_specialist`
  - `search_planner / public_media_specialist / exploration_specialist` 已支持并行 autonomous workers
  - worker 具备持久化 `budget / checkpoint / output / interrupt_requested`
  - 同一 `job_id + lane_id + worker_key` 现在支持 checkpointed resume
  - 已补 lane-aware scheduler：
    - 按 lane priority 和 resume mode 选择下一批 runnable workers
    - 默认优先 `reuse_checkpoint / resume_from_checkpoint`
    - 当前 scheduler 面向 search/public media/exploration 这三类 specialist lane
  - 已补 autonomous worker daemon loop：
    - search/public media/exploration 不再各自管理 retry，而是统一走 daemon loop
    - daemon 会按 lane budget cap 和 retry limit 做 arbitration
    - `failed -> retry` 与 `completed -> reuse output` 已进入执行链
  - 已补跨进程常驻恢复器能力：
    - worker 通过 Postgres lease 协调跨进程 claim / release
    - `stale running` worker 会被降级为可恢复态重新进入 scheduler
    - 低层 `run-worker-daemon` / `run-worker-daemon-once` 仍可单独执行，但仅建议用于排障
  - 已补系统级常驻服务壳层：
    - `run-worker-daemon-service` 提供单实例锁、心跳状态和优雅退出
    - `write-worker-daemon-systemd-unit` 可生成 systemd unit
    - `show-daemon-status` 可读取 `runtime/services/<service_name>/status.json`
- 支持异步 workflow，显式区分 `planning`、`acquiring`、`retrieving`
- 支持 staged workflow feedback：
  - `GET /api/jobs/{job_id}/progress` 与 `GET /api/jobs/{job_id}/results` 现在都会返回 `workflow_stage_summaries`
  - 当前稳定阶段顺序为：
    - `linkedin_stage_1`
    - `stage_1_preview`
    - `public_web_stage_2`
    - `stage_2_final`
  - snapshot 内也会落盘到 `runtime/company_assets/{company}/{snapshot_id}/workflow_stage_summaries/`
  - 前端无需再自行读取 snapshot 文件即可渲染阶段漏斗 / 阶段卡片
- 支持 query dispatch 去重与分发：
  - 对完全相同请求可 `join inflight`（加入在途）或 `reuse completed`（复用已完成结果）
  - 现明确区分两层 request normalization：
    - `ingress_normalization`：请求入口的 LLM/rules 混合归一，负责把原始 query 转成结构化 intent、关键词、scope、角色与执行偏好
    - `dispatch_matching_normalization`：dispatch 去重阶段的 deterministic 归一，只基于 prepared/effective request 做 signature / family score / snapshot reuse 判断，不再重新调用模型
  - 支持 `requester_id / tenant_id / idempotency_key` 作用域隔离，避免多用户重复消耗
  - 所有决策会落盘到 `query_dispatches`，并可通过 API 查询审计
  - `dispatch` / `query_dispatches` 会返回 `request_family_match_explanation`，用于解释是 exact match、family match，还是同公司 snapshot 复用
  - 对“已有全量本地资产的小型组织”，若请求仍是 `full_company_asset` 语义，即使 query 主题词变化导致 family score 偏低，也会优先复用同公司已完成 snapshot，而不是重新触发 Harvest company/search
- 支持 `Excel contact intake`
  - `intake-excel` 会先让模型识别工作表中的姓名、公司、职位、LinkedIn、邮箱等列
  - 先做本地 exact / near match，再决定直接复用、进入 manual review，还是触发 LinkedIn profile fetch / search fallback
  - intake 结果会落盘到 `runtime/excel_intake/{intake_id}/`，并支持 `continue-excel-intake` 继续处理人工复核行
- acquisition runtime 已开始按 strategy 分叉执行：
  - `full_company_roster` 走 company roster connector
  - `scoped_search_roster / former_employee_search` 走低成本 search-seed discovery
  - `investor_firm_roster` 走 tiered investor-firm workflow，并先生成 firm plan 再做后续筛选
- 支持 live company identity resolve + LinkedIn `company/people` roster acquisition
- 当 live roster connector 超限或返回空结果时，可回退到最近一次成功的本地 company snapshot
- 支持 provider-first 的 LinkedIn enrichment：
  - `web LinkedIn URL search -> search/people fallback -> /api/profile -> /people/profile`
  - 若已从 web search / publication / other source 拿到 slug 或 LinkedIn URL，则优先直接调用 profile detail
  - publication author / acknowledgement / co-author baseline
- 支持 `further exploration`：
  - 对 unresolved lead 或没有公开 LinkedIn 的候选人，继续抓取网页、X、GitHub、个人主页、CV/Resume 链接
  - 若后续发现 LinkedIn URL，可再次进入 profile detail acquisition
  - search-seed acquisition 已能从 `public_interviews / publication_and_blog` 这类低成本 query bundle 中生成 `public_media_lead`
  - 公开视频搜索结果会先保存 `public_media_results / public_media_analysis`，基于标题和摘要做初步关系判断
  - `Roster-Anchored Scholar Coauthor Expansion` 当前默认只产出 evidence-only prospects，不会默认对全量 prospects 自动发起 search follow-up
  - 若要真的跑 prospect follow-up，需要显式提供 `scholar_coauthor_follow_up_limit`
- 支持 model-assisted page analysis：
  - 以通用 `analyze_page_asset` 接口调用页面摘要/校验模型，不把实现写死为 Qwen
  - 当前已有 deterministic fallback，后续可并列接 Claude 等其他模型
  - `analyze_page_asset` 现已扩展输出：
    - `education_signals`
    - `work_history_signals`
    - `affiliation_signals`
    - `document_type`
  - exploration / manual review 现在不仅会保存页面摘要，也会把可结构化的 education/work history 草稿回写到 candidate
  - 已支持：
    - HTML homepage / CV
    - Google Docs CV
    - PDF resume 的文本提取链路
  - 当前 PDF 现为多级文本提取：
    - `pypdf`
    - `pdfminer.six`
    - `pdftotext`
    - `OCR (pdftoppm + tesseract)` 预留
  - `runtime/vendor/python` 可放本机增强解析依赖，不进入 Git
  - 对图片型 PDF，若本机无 OCR 工具，仍可能只能保留证据而无法自动补齐字段
- 支持 criteria evolution persistence：
  - 保存人工 review feedback
  - 持久化 alias / must_signal / exclude_signal 等 pattern
  - 当前 alias pattern 已可影响后续 retrieval
  - feedback 现会额外生成 `suggested patterns`，作为待 review 的建议层，不会直接污染 active patterns
  - suggestion 已支持 review loop：`suggested -> applied/rejected`
- 支持 criteria versioning / compiler run persistence：
  - 每次 plan / workflow / retrieval 都会保存 `criteria_versions`
  - 每次 criteria 编译都会保存 `criteria_compiler_runs`
- 支持 confidence labels 持久化：
  - 检索结果会输出并保存 `high / medium / lead_only`
  - 同时记录 `confidence_score` 与 `confidence_reason`
  - human feedback 现可继续演化 confidence policy：
    - `must_have_signal / false_negative_pattern` 会沉淀为 `must_signal + confidence_boost`
    - `exclude_signal / false_positive_pattern` 会沉淀为 `exclude_signal + confidence_penalty`
    - 后续 rerun 中，这些 pattern 会直接影响 `confidence_score / confidence_label`
    - 同公司历史 feedback 会按 precision / recall 压力自动微调 `high / medium` band 边界
    - confidence policy 现默认优先按 `request-family` 生效，而不是只按公司全局生效
    - 老 feedback 会按时间自动衰减，避免历史噪声长期主导 band 边界
  - 每次 retrieval / rerun 都会保存 `confidence_policy_runs`
  - 支持 manual policy freeze / override：
    - 可对 `request_exact / request_family / company` 三种 scope 显式加 control
    - `freeze_current` 会把当前自动生成的 band 阈值冻结为人工锁定版本
    - `override` 可直接指定 `high_threshold / medium_threshold`
    - retrieval 时会按 `request_exact -> request_family -> company` 优先级选择 active control
    - 所有 control 会持久化到 `confidence_policy_controls`
- 支持 centralized asset logger：
  - snapshot 内统一生成 `asset_registry.json`
  - company roster、search seed、profile payload、exploration page、analysis input/output、publication raw page 都会进入统一 registry
- 支持公司级历史资产物化与可复用候选文档提炼：
  - `build-company-candidate-artifacts` 现会聚合同一公司的历史 snapshot、registry 与 generation artifacts，而不是只读取最后一次覆盖进主路径的数据
  - 会输出：
    - `materialized_candidate_documents.json`
    - `normalized_candidates.json`
    - `reusable_candidate_documents.json`
    - `manual_review_backlog.json`
    - `profile_completion_backlog.json`
  - `profile_completion_backlog` 会显式列出“已有 LinkedIn URL 但尚未拿到 full profile detail”的候选人，便于后续继续补全
- 支持公司级后处理资产补全：
  - `complete-company-assets` 会先物化公司历史候选池，再执行：
    - known-URL profile completion
    - unresolved lead exploration
    - follow-up profile completion
  - Thinking Machines Lab 当前已有单一 canonical snapshot，并显式拆分：
    - `canonical_merged`
    - `strict_roster_only`
  - 当前 authoritative counts、cloud bundle 和 asset-view 使用规则统一见：
    - `docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md`
    - `docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md`
  - 当前设备上 Harvest 配置入口已恢复到 `runtime/secrets/providers.local.json`
  - `2026-04-07` 新 Harvest token 已重新 smoke test 验证通过
  - backlog 不再在 README 中写死，以 snapshot 内 `normalized_artifacts/*_backlog.json` 和对应 validation note 为准
- 支持 `Manual Review Queue`
  - `lead_only`
  - 缺少 LinkedIn profile 的候选人
  - 需要人工进一步确认 membership 的 corner case
  - retrieval 结果和 `GET /api/jobs/{job_id}/results` 都会附带 `manual_review_items`
- 支持 criteria auto-evolution loop：
  - 人工 feedback 写入后，会自动触发一次 criteria recompile
  - 若 feedback 带 `job_id + candidate_id`，系统会结合 `matched_fields` 与 candidate context 生成 pattern suggestions
  - 若用户审核某条 suggestion 为 `applied`，系统会把它写入 active patterns，并可继续触发 recompile/rerun
  - 新版本会保留 `parent_version_id / trigger_feedback_id / evolution_stage`
  - 若请求里带 `rerun_retrieval=true`，会继续执行 retrieval rerun 并生成 result diff
  - rerun 现支持 `auto / cheap / full` 三档策略
  - `auto` 会按 feedback 类型、预估影响和 baseline request 规模决定是否 rerun，以及执行 cheap 或 full
  - `cheap` rerun 默认收紧 `top_k / semantic_rerank_limit`，并回退到 deterministic summary，避免重复支付模型摘要成本
  - 低信号 feedback 会被自动 gate 掉，避免无意义 rerun
  - baseline job 不再只是“同公司最近一次 job”，而是优先按 request-family 精确匹配
  - rerun 返回会显式带上 `baseline_selection`，说明它是按 exact request、exact family，还是 fallback 选出的
- 将外部 acquisition 结果版本化为 `runtime/company_assets/{company}/{snapshot_id}/`
- 将 company roster 归一化为可检索的 baseline candidate/evidence 文档
- 支持跨 snapshot 复用已拿到的 LinkedIn search/basic profile 资产，避免重复消耗 search 配额
- 支持按 `target_company`、`categories`、`employment_statuses`、关键词等条件运行 retrieval job
- 提供 retrieval strategy 抽象：`structured` / `hybrid` / `semantic`
- 已落地第一版规则优先 retrieval stack：
  - `scoring.py` 默认负责 structured hard filters、lexical/alias 命中、confidence banding
  - structured hard filters 之后，`semantic_retrieval.py` 默认走本地 sparse-vector semantic retrieval 做 recall / rerank
  - 默认产品路径仍是本地 sparse semantic retrieval
  - external semantic provider 只保留为 legacy/experimental supplement path，不应再被视为常规 operator 开关
  - core roster / profile-search lane 不再暴露通用“高成本来源”主开关；单人姓名检索限制走独立 targeted-name contract
  - `hybrid` 当前等价于 `hard filters + lexical/alias + local sparse semantic + confidence banding`
- 已补通用 `semantic provider` 抽象：
  - 默认 `LocalSemanticProvider` fallback
  - 已可切到 DashScope/Qwen 的 embedding + rerank
  - 当前默认配置面向 `text-embedding-v4 + gte-rerank-v2`，并预留 `qwen3-vl-rerank`
- 已补 autonomous worker 控制面：
  - `GET /api/jobs/{job_id}/workers`
  - `GET /api/jobs/{job_id}/scheduler`
  - `GET /api/workers/recoverable`
  - `GET /api/workers/daemon/status`
  - `POST /api/workers/interrupt`
  - `POST /api/workers/daemon/run-once`
  - `POST /api/workers/daemon/systemd-unit`
  - CLI: `show-workers / show-scheduler / interrupt-worker`
  - `GET /api/jobs/{job_id}/trace` 现会同时返回 `agent_workers`
- 预留 Qwen / Claude model provider 接口
- 支持 object storage durable sync：
  - `upload-asset-bundle / download-asset-bundle` 已支持并发 `max_workers`
  - 单对象上传/下载已补 retry/backoff
  - 默认会 resume，并跳过已完成且匹配 manifest 的对象
  - sync summary 会给出 progress / skipped_existing 统计
  - 本地会生成 `runtime/object_sync/bundle_index.json` 与 `runtime/object_sync/runs/*.json`
  - 云端会同步写入 `indexes/bundle_index.json` 与 `indexes/sync_runs/*.json`
  - 当前主路径按 `S3-compatible object storage` 设计
  - 历史已验证 `R2 upload -> download -> local restore`
  - 当前默认部署目标已切到阿里云 OSS
- 已记录高质量 HarvestAPI 接入策略，供后续 Thinking Machines Lab 等小公司端到端验证使用：
  - intent-driven LinkedIn search，用于按用户意图定向检索在职 / 已离职 / 特定岗位人群
  - company employees actor，用于获取小中型公司的高质量 roster
  - profile scraper actor，用于按 LinkedIn URL 拉取 full profile detail，并利用 `moreProfiles` 做相似经历扩展
  - 已记录成本控制阈值：
    - 有 URL 时优先 profile scraper
    - company employees actor 仅用于批量场景
    - profile search actor 只在低成本 web search 不足时作为 fallback
- 已补 HarvestAPI live connector 基础层：
  - `HarvestProfileSearchConnector`
  - `HarvestCompanyEmployeesConnector`
  - `HarvestProfileConnector`
  - 当前 canonical 配置入口是 `runtime/secrets/providers.local.json`
  - 旧项目 token 自动发现只作为 recovery fallback，不应作为日常依赖
  - 已验证 Harvest search / company-employees actor 可真实调用并落盘 raw asset
  - 已确认参数差异：
    - `linkedin-profile-search` 可接受 `profileScraperMode=Short`
    - `linkedin-company-employees` 的 `profileScraperMode` 必须使用完整枚举，如 `Short ($4 per 1k)`
    - `linkedin-profile-scraper` 的 `profileScraperMode` 也必须使用完整枚举，如 `Profile details + email search ($10 per 1k)` 或 `Profile details no email ($4 per 1k)`
    - search / company 两类 actor 都需要满足最小 `maxTotalChargeUsd`，否则会返回 400
  - 已补 known-profile batch enrichment：
    - roster -> candidate 现会保留 `linkedin_url / metadata.profile_url`
    - 已知 LinkedIn URL 的 detail enrichment 现优先走 Harvest batch profile-scraper，而不是逐人串行调用
    - Thinking Machines Lab 最新 live snapshot 已成功解析 `12` 份 prioritized full profile detail
  - `2026-04-07` 已重新验证新 Harvest token：
    - `company-employees` live smoke test 可用
    - `profile-scraper` live smoke test 可用
  - 当前已确认 `profile-scraper` 的可用输入字段是：
    - `urls`
    - `publicIdentifiers`
    - `queries`
    - `profileIds`
    - 不应继续使用错误的 `profileUrls`
  - 已新增 [docs/HARVESTAPI_PLAYBOOK.md](docs/HARVESTAPI_PLAYBOOK.md)，沉淀 actor 链接、payload 规范、TML live 结论与当前已知坑

## 目录

```text
sourcing-ai-agent/
├── configs/
├── docs/
├── runtime/
├── src/sourcing_agent/
└── tests/
```

## Onboarding

如果你是新的开发者或新的 AI session，建议按这个顺序进入项目：

1. 根目录 [../ONBOARDING.md](../ONBOARDING.md)
2. [PROGRESS.md](PROGRESS.md)
3. [docs/INDEX.md](docs/INDEX.md)
4. [docs/MODULES.md](docs/MODULES.md)
5. [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)
6. [docs/HARVESTAPI_PLAYBOOK.md](docs/HARVESTAPI_PLAYBOOK.md)
7. [docs/DATAFORSEO_PLAYBOOK.md](docs/DATAFORSEO_PLAYBOOK.md)
8. [docs/TERMINAL_WORKFLOW.md](docs/TERMINAL_WORKFLOW.md)
9. [docs/FRONTEND_API_CONTRACT.md](docs/FRONTEND_API_CONTRACT.md)
10. [docs/HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md](docs/HOSTED_DEPLOYMENT_AND_GITHUB_SCOPE.md)
11. [docs/QUERY_GUARDRAILS.md](docs/QUERY_GUARDRAILS.md)
12. [docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md](docs/THINKING_MACHINES_LAB_CANONICAL_ASSET.md)
13. [docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md](docs/THINKING_MACHINES_LAB_VALIDATION_2026-04-08.md)
14. 需要追旧决策或恢复旧环境时，再看 `docs/` 下的 dated reference 文档

## GitHub Sync Boundary

这个仓库会同步：

- 源代码
- 文档
- 示例配置
- 去敏后的历史方法论资产

这个仓库不会同步：

- `runtime/` 下的 live payload / company assets / profile raw assets / caches
- `runtime/secrets/providers.local.json`
- 历史 `api_accounts.json`

如果后续要在新的电脑上继续做 live test，需要单独恢复 secrets，以及按需恢复高价值 runtime 资产。

准备推送 GitHub 时，建议用下面的最小上传范围来降低部署成本：

- 建议上传：
  - `src/`
  - `tests/`
  - `docs/`
  - `contracts/`
  - `configs/*.example.json`
  - `README.md` / `PROGRESS.md` / `pyproject.toml`
- 不建议上传：
  - `runtime/**`
  - `runtime/secrets/**`
  - `runtime/company_assets/**`
  - `runtime/live_tests/**`
  - `runtime/provider_cache/**`
  - 本机环境目录（例如 `.venv/`、`node_modules/`、playwright browsers 缓存）
  - 临时 live smoke 配置（`configs/live_smoke_*.json`、`configs/live_test_*.json`）

跨设备恢复的详细设计见：

[docs/CROSS_DEVICE_SYNC.md](docs/CROSS_DEVICE_SYNC.md)

## Hosted 默认执行路径（云端）

云端默认路径应是 `serve` 托管 workflow，而不是手工运行 detached 脚本去“补一把 execute-workflow”。

推荐常驻进程：

- API: `PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765`
- Recovery daemon: `PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5`

推荐健康检查：

- `GET /health`
- `GET /api/providers/health`
- `GET /api/workers/daemon/status`
- `GET /api/runtime/health`

前端只应该通过 API 消费 workflow 状态，不应直接读取 `runtime/company_assets/*` 或 `runtime/jobs/*` 文件。
阶段反馈统一使用 `workflow_stage_summaries`（见 `progress/results` contract）。

完整部署流程见：

- [docs/SERVER_RUNTIME_BOOTSTRAP.md](docs/SERVER_RUNTIME_BOOTSTRAP.md)
- [docs/WORKFLOW_OPERATIONS_PLAYBOOK.md](docs/WORKFLOW_OPERATIONS_PLAYBOOK.md)

## 本地 Proxy Guard

如果你的终端默认带了 `http_proxy` / `https_proxy`，本地 `localhost` / `127.0.0.1` 联调时，`curl`、Python、Node 可能会误走代理，表现成假的 `502` 或“后端不可达”。

本仓库提供了一个统一的本地联调保护脚本：

```bash
cd "sourcing-ai-agent"
source ./scripts/local_dev_proxy_guard.sh
./scripts/local_dev_proxy_guard.sh --print
```

如果你不想 `source` 整个 shell，也可以只包住一条命令：

```bash
./scripts/local_dev_proxy_guard.sh curl http://127.0.0.1:8765/health
./scripts/local_dev_proxy_guard.sh env SOURCING_API_ALLOWED_ORIGINS=http://localhost:4173,http://127.0.0.1:4173 PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
```

在这之上，常用本地启动入口现在也收敛成两个 helper：

```bash
bash ./scripts/dev_backend.sh
bash ./scripts/dev_frontend.sh
```

本地文档统一写成 `bash ./scripts/...`，是为了避开部分 WSL / 挂载目录里直接执行脚本时可能出现的 `Permission denied` / `noexec` 干扰。

本地环境有 3 条硬约束，建议不要再偏离：

- runtime Python 默认优先用仓库 `./.venv/bin/python`，缺失时回退到 `./.venv-tests/bin/python`
- 测试 Python 默认用 `./.venv-tests/bin/python`，并通过 `make bootstrap-test-env` 重建
- shell 入口统一用 `bash ./scripts/...`，不要依赖当前交互 shell 的别名、PATH 或 login profile

如果你怀疑当前会话没走对环境，先跑：

```bash
make bootstrap-test-env
make dev-doctor
```

这条命令会直接检查：

- 当前 bash 路径和版本
- backend helper 实际解析到的 `python_bin`
- `requests` / `psycopg` 是否能从该解释器导入
- `.venv-tests` 是否真的可执行、版本是否正确、`pytest/requests/psycopg/ruff/mypy` 是否齐全
- control-plane runtime 最终解析结果
- frontend helper 的实际代理配置

常见错误信号与根因：

- `ModuleNotFoundError: requests`
  - 先检查 `pyproject.toml` 依赖是否已安装，再确认你启动 backend 时没有走错解释器
- `Configured Postgres control plane requires psycopg`
  - 说明你当前解释器没有 `psycopg`
- `.venv-tests/bin/python` 掉进 `xcode-select` 或 `pytest missing`
  - 说明 `.venv-tests` 是坏迁移产物或旧解释器软链，直接跑 `make bootstrap-test-env`
- `show-control-plane-runtime` 没解析到 PG DSN
  - 先检查 `.local-postgres.env` 和 `make dev-doctor` 输出，不要直接猜测当前环境

如果你更希望用统一入口，也可以直接在仓库根目录运行：

```bash
make dev-doctor
make dev-backend
make dev-frontend
make dev
make dev-status
make dev-stop
make dev-logs
make dev-clean
```

其中 `make dev` 会把 backend 放到后台、frontend 放到前台，backend 日志默认写到 `runtime/service_logs/make-dev-backend.log`。
`make dev-status` 会检查当前端口监听、后端 `/health` 与 `/api/runtime/progress`、以及 Vite 根页面是否可达。
`make dev-stop` 会对当前 `API_PORT` / `FRONTEND_PORT` 上的监听进程发送 `SIGTERM`；如果 backend 是通过 helper 起的，它自己的 shell trap 会继续清理 worker daemon。
`make dev-logs` 会统一 tail `make-dev-backend.log`、`dev-worker-daemon.log`、以及当前 hosted/runtime 常用日志。若只想看一眼当前尾部而不持续跟随，可用 `make dev-logs EXTRA_LOG_ARGS=--no-follow`。
`make dev-clean` 会先执行 stop，再清理 helper 生成的日志、`make-dev-backend.pid`，以及“pid 已失活”的本地 dev service 状态目录。若只想先看影响范围，可用 `make dev-clean EXTRA_CLEAN_ARGS=--dry-run`。

其中：

- `bootstrap_test_env.sh` 会用仓库 runtime Python 以 `venv --copies` 重建 `.venv-tests`，避免跨机器迁移后继续指向旧系统解释器
- `dev_doctor.sh` 会先做 bash/python/control-plane invariants 检查，适合每次换机器、换 shell、换 venv 后先跑一遍
- `dev_backend.sh` 默认会先套上 proxy guard，再自动补本地 CORS origins，并把 worker daemon 一起带起来
- `dev_backend.sh` / `run_hosted_trial_backend.sh` 现在会优先选仓库 `.venv/bin/python`，缺失时自动回退 `.venv-tests/bin/python`，并在缺 `requests` / `psycopg` 时启动前直接报错
- `dev_frontend.sh` 默认会以 `http://localhost:8765` 作为后端地址启动 Vite
- 两个脚本都支持 `--print-config`
- `dev_status.sh` / `dev_stop.sh` / `dev_logs.sh` / `dev_clean.sh` 把本地联调的状态检查、停服、日志查看、残留清理也收敛成了统一入口
- `Makefile` 支持 `HOST`、`API_PORT`、`FRONTEND_PORT`、`ALLOWED_FRONTEND_PORTS`、`API_BASE_URL`、`ALLOW_ORIGIN`、`BACKEND_LOG`、`BACKEND_PID_FILE`、`EXTRA_BACKEND_ARGS`、`EXTRA_FRONTEND_ARGS`、`EXTRA_LOG_ARGS`、`EXTRA_CLEAN_ARGS`

## 统一入口矩阵

默认应把入口分成 5 类，不要混用：

| 场景 | 首选入口 | 用途 | 是否创建 job |
| --- | --- | --- | --- |
| 计划生成 | `POST /api/plan` / `cli plan` | 生成 retrieval / acquisition plan 与 plan review | 否 |
| 执行前 dry-run | `POST /api/workflows/explain` / `cli explain-workflow` | 查看 `ingress_normalization`、dispatch/reuse、lane 预览 | 否 |
| 正式执行 | `POST /api/workflows` / hosted `serve` | 创建并托管真实 workflow | 是 |
| 排障恢复 | `cli execute-workflow`、`cli supervise-workflow`、`run-worker-daemon-once` | 仅用于 repair/debug，不是常规入口 | 视命令而定 |
| 云端资产导入 | `cli import-cloud-assets` | 下载 bundle 后统一修复 registry / profile registry / completeness ledger | 否 |

补充约束：

- 生产默认路径是 `serve` + `run-worker-daemon-service`。
- `execute-workflow` 不应再作为常规“补一把续跑”的执行入口。
- 前端在真正执行前，应优先调一次 `POST /api/workflows/explain`，再决定是否提交 `POST /api/workflows`。
- 跨设备或服务器恢复已统一收敛到 `import-cloud-assets`，不建议继续手工串 `download-asset-bundle` + `restore-*` + repair 命令。

## 快速开始

```bash
cd "sourcing-ai-agent"
source ./scripts/local_dev_proxy_guard.sh
PYTHONPATH=src python3 -m sourcing_agent.cli bootstrap
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_xai.json
PYTHONPATH=src python3 -m sourcing_agent.cli explain-workflow --file configs/demo_workflow_xai.json
PYTHONPATH=src python3 -m sourcing_agent.cli plan --file configs/demo_workflow_thinking_machines_lab.json
PYTHONPATH=src python3 -m sourcing_agent.cli show-plan-reviews --target-company xAI --brief
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan --file configs/plan_review_approve.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan --review-id 12 --reviewer sora --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。" --preview
PYTHONPATH=src python3 -m sourcing_agent.cli review-plan --review-id 12 --reviewer sora --instruction "改成 full company roster，走 Harvest company-employees lane，强制 fresh run，不允许高成本 source。"
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_xai.json
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --plan-review-id 12
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_anthropic.json
PYTHONPATH=src python3 -m sourcing_agent.cli start-workflow --file configs/demo_workflow_thinking_machines_lab.json
PYTHONPATH=src python3 -m sourcing_agent.cli run-job --file configs/demo_current_infra.json
PYTHONPATH=src python3 -m sourcing_agent.cli show-progress --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-trace --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-workers --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-scheduler --job-id <job_id>
PYTHONPATH=src python3 -m sourcing_agent.cli show-recoverable-workers
PYTHONPATH=src python3 -m sourcing_agent.cli show-daemon-status
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-snapshot-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli build-company-candidate-artifacts --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli complete-company-assets --company thinkingmachineslab --profile-detail-limit 12 --exploration-limit 2
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab --with-sqlite-backup  # 仅备份兜底时使用
PYTHONPATH=src python3 -m sourcing_agent.cli export-sqlite-snapshot  # backup-only
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json
PYTHONPATH=src python3 -m sourcing_agent.cli import-cloud-assets --bundle-kind company_snapshot --bundle-id <bundle_id> --output-dir /tmp/asset_imports
PYTHONPATH=src python3 -m sourcing_agent.cli download-asset-bundle --bundle-kind company_snapshot --bundle-id <bundle_id> --output-dir /tmp/asset_imports  # 仅排障
PYTHONPATH=src python3 -m sourcing_agent.cli restore-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json --target-runtime-dir /tmp/sourcing-agent-runtime  # 仅排障
PYTHONPATH=src python3 -m sourcing_agent.cli restore-sqlite-snapshot --manifest runtime/asset_exports/<sqlite_bundle>/bundle_manifest.json  # backup-only / 仅排障
PYTHONPATH=src python3 -m sourcing_agent.cli interrupt-worker --worker-id <worker_id>
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-once  # 仅排障
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon --poll-seconds 5  # 仅排障
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5  # hosted 常规路径
PYTHONPATH=src python3 -m sourcing_agent.cli write-worker-daemon-systemd-unit
PYTHONPATH=src python3 -m sourcing_agent.cli show-manual-review --target-company Anthropic
PYTHONPATH=src python3 -m sourcing_agent.cli review-manual-item --file configs/manual_review_resolve.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli configure-confidence-policy --file configs/confidence_policy_freeze.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli review-suggestion --file configs/suggestion_review_apply.example.json
PYTHONPATH=src python3 -m sourcing_agent.cli serve --port 8765
```

云端部署时，优先使用这组最小命令，而不是一次性执行上面全部命令：

```bash
cd "sourcing-ai-agent"
source ./scripts/local_dev_proxy_guard.sh
PYTHONPATH=src python3 -m sourcing_agent.cli test-model
PYTHONPATH=src python3 -m sourcing_agent.cli serve --host 0.0.0.0 --port 8765
PYTHONPATH=src python3 -m sourcing_agent.cli run-worker-daemon-service --poll-seconds 5
```

## 模型配置

本地 secret 文件路径：

`runtime/secrets/providers.local.json`

示例格式见：

`configs/providers.local.example.json`

其中包括：

- `model_provider`
- `qwen`
- `semantic`
- `search_provider`
- `harvest`

通用模型 provider 读取优先级：

1. `MODEL_PROVIDER_API_KEY`、`MODEL_PROVIDER_BASE_URL`、`MODEL_PROVIDER_MODEL` 等环境变量
2. `runtime/secrets/providers.local.json` 中的 `model_provider`

当前已验证可用的 relay 形态：

1. OpenAI-compatible base URL：`https://tb.keeps.cc/v1`
2. model：`claude-sonnet-4-6`
3. `test-model` 会优先 healthcheck `model_provider`，若未配置则回退到 `qwen`

Qwen 读取优先级：

1. `DASHSCOPE_API_KEY` 等环境变量
2. `runtime/secrets/providers.local.json`

connector 账号读取优先级：

1. `runtime/secrets/providers.local.json` 中的 `connectors.rapidapi_accounts`
2. 项目内 `local_asset_packages/anthropic/api_accounts.json`
3. 兼容读取外部 `Anthropic华人专项` 中已有的 `api_accounts.json`

semantic provider 推荐配置：

1. `semantic.embedding_model`：`text-embedding-v4`
2. `semantic.rerank_model`：`gte-rerank-v2`
3. `semantic.media_rerank_model`：`qwen3-vl-rerank`

Thinking Machines Lab 端到端测试建议入口：

1. 先执行 `plan --file configs/demo_workflow_thinking_machines_lab.json`
2. 完成 `Plan Review Gate`，确认 scoped roster、source families、paid source budget
3. 再执行 `start-workflow --file configs/demo_workflow_thinking_machines_lab.json`

## API

更明确的 Web 层消费约定见：

- [docs/FRONTEND_API_CONTRACT.md](docs/FRONTEND_API_CONTRACT.md)
- [contracts/frontend_api_contract.ts](contracts/frontend_api_contract.ts)
- [contracts/frontend_api_contract.schema.json](contracts/frontend_api_contract.schema.json)
- [contracts/frontend_api_adapter.ts](contracts/frontend_api_adapter.ts)

- `GET /health`
- `GET /api/providers/health`
- `POST /api/bootstrap`
- `POST /api/plan`
- `POST /api/workflows/explain`
- `GET /api/plan/reviews`
- `POST /api/plan/review`
- `POST /api/workflows`
- `POST /api/jobs`
- `GET /api/jobs/{job_id}`
- `GET /api/jobs/{job_id}/results`
- `GET /api/jobs/{job_id}/trace`
- `GET /api/jobs/{job_id}/workers`
- `GET /api/jobs/{job_id}/scheduler`
- `GET /api/workers/recoverable`
- `GET /api/workers/daemon/status`
- `GET /api/manual-review`
- `POST /api/manual-review/review`
- `POST /api/workers/interrupt`
- `POST /api/workers/daemon/run-once`
- `POST /api/workers/daemon/systemd-unit`
- `POST /api/criteria/feedback`
- `POST /api/criteria/confidence-policy`
- `POST /api/criteria/suggestions/review`
- `GET /api/criteria/patterns`
- `POST /api/criteria/recompile`

`/api/criteria/feedback` 与 `/api/criteria/recompile` 支持可选参数：

- `rerun_retrieval`
- `job_id` 或 `baseline_job_id`
- `rerun_request_overrides`

`rerun_retrieval` 目前支持：

- `true` 或 `auto`
- `cheap`
- `full`

## 数据资产规则

- 默认 raw-first：外部 API 返回、web search HTML、profile payload、exploration page、analysis input/output 都先落盘，再进入归一化或模型分析
- Harvest connector 现支持 runtime 级 payload cache：
  - 优先复用当前 snapshot raw asset
  - 其次复用 `runtime/provider_cache/<runtime_environment>/live/*`
  - 必要时在 live mode 下桥接同一 runtime 的 `runtime/live_tests/*` 手工 live 资产
  - `simulate/scripted/replay` 不读写 shared provider cache
  - 这样在 token 暂不可用或 provider 波动时，workflow 仍可回放既有高价值数据资产
- 公开访谈 / YouTube / Podcast 这类结果也会先落 `public_media_results` 与 `public_media_analysis`
- snapshot 内默认维护统一 `asset_registry.json`，记录每个资产的路径、类型、来源、raw/model-safe 属性和大小
- autonomous worker 的 checkpoint / budget / output 也会持久化到 Postgres control plane，而不是只存在进程内
- daemon loop 还会消费 lane budget caps，避免某个 specialist lane 抢占全部恢复/重试额度
- 默认 compact-context：模型只读取压缩后的 `analysis_input`，不会直接吃整页 HTML、整份 raw payload 或 PDF 二进制内容
- 默认跳过大体积/二进制页面直送模型：
  - 当前 exploration analysis 会跳过 `.pdf` 等 binary-like URL
  - raw 资产仍可留作后续分阶段解析，但不会直接塞进 LLM 上下文
- 当前 compact excerpt 上限已显式固定在共享 policy 中，避免不同 connector 自己扩大上下文
- rerun 同样遵循成本控制：
  - `cheap` rerun 会优先复用既有 retrieval 输入，不触发额外 acquisition
  - 并默认使用 deterministic summary，避免为验证性 rerun 重复消耗模型调用

## Thinking Machines Lab 状态

- current roster：已通过 Harvest `company-employees` 获取高质量 current roster
- current detail：已对优先级最高的一批 current members 拉取 full profile detail
- former fallback：现已改为 `pastCompanies` recall first，不默认加 `excludeCurrentCompanies`
- publication supplement：现已执行 official blog / docs / publication surface 抽取，并把 unmatched authors 作为 lead 保留
- publication lead 的默认顺序已调整为 low-cost first：
  - 先走 `Publication Lead Public-Web Verification`
  - 先补 public-web affiliation evidence 与 LinkedIn URL
  - targeted Harvest name search 不再作为 publication lead 的自动下一步；默认需要逐人确认
- `Roster-Anchored Scholar Coauthor Expansion` 已接入第一版：
  - 当前后端先用 arXiv author-seed search 扩 confirmed roster member 的 coauthor 图
  - 未命中的 coauthor 只会进入 prospect artifact，不会直接升 lead
  - 只有在 public web 确认 affiliation 且拿到 LinkedIn URL 后，才允许继续 profile fetch
- manual review 现已成为正式数据资产入口：
  - 可把人工补充的 LinkedIn / homepage / CV / social links 写回 `candidate + evidence + manual_review_assets`
  - Thinking Machines Lab 当前已完成：
    - `Kevin Lu`：confirmed current employee，已写回 LinkedIn
    - `John Schulman`：confirmed current employee，已写回 homepage evidence
    - `Jeremy Bernstein`：confirmed current employee，已写回 homepage + CV evidence
    - `Horace He`：homepage 未直接确认 TML affiliation，保留为 unresolved lead
- 当前 low-cost search provider 状态：
  - 当前 low-cost search 已经有 provider abstraction
  - 默认顺序是 `dataforseo_google_organic -> serper_google -> google_browser -> bing_html -> duckduckgo_html`
  - 现网环境对 DuckDuckGo HTML endpoint 仍存在 TLS EOF 问题，因此它只保留为 best-effort fallback
  - 更稳定的 Google organic 调用与后台 queue 方案见 `docs/DATAFORSEO_PLAYBOOK.md`

## 设计文档

- `docs/INDEX.md`
- `docs/ARCHITECTURE.md`
- `docs/LEAD_DISCOVERY_METHODS.md`
- `docs/DATA_ARCHITECTURE.md`
- `docs/MODULES.md`
- `docs/PRD.md`
- `docs/BACKEND_MVP.md`
- `docs/DATA_ASSET_GOVERNANCE.md`
- `docs/SERVICE_EVOLUTION_STRATEGY.md`

## 下一步

- 保持 `asset build` 和 `retrieval & delivery` 两条 pipeline 分离，避免 retrieval 默认触发新的 acquisition
- 把 snapshot promotion / canonical pointer / cloud registry 进一步产品化，落实 `docs/DATA_ASSET_GOVERNANCE.md`
- 继续把 Web control plane + local runner 的 hybrid 形态落成真实服务入口，见 `docs/SERVICE_EVOLUTION_STRATEGY.md`
- 用新的公司做真实端到端验证，优先验证意图识别、asset reuse、manual review 和结果交付链
- 将当前本地 sparse-vector retrieval 升级为外部 embedding / vector store，并补 candidate-level semantic attribution
- 引入 specialist lanes / handoff runtime / trace spans，把当前 job engine 继续升级成 fully agentic sourcing copilot
- 再在此基础上做 Demo 前端
