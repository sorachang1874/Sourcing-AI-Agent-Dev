# Test Environment

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.

> Revision note (2026-06-11, test-environment contract v2): isolated workflow-confidence runtimes are now ephemeral *pairs* — per-runtime PG schema + runtime dir with real teardown. `isolated_hosted_test_runtime(...)` writes an `.ephemeral-test-env.json` marker (schema, runtime_dir, created_at) into the runtime dir and drops its PG schema on exit (fail-soft; `keep_schema=True` opts out for debugging). Orphan schemas from crashed/pre-v2 runs are reclaimed by `scripts/prune_test_schemas.py` (dry-run by default, `--apply` drops, local-loopback DSNs only; pairing via marker, legacy `.scripted-local-postgres.env`, or the deterministic dir→schema derivation). Runtime dirs keep their TTL prune via `make prune-test-env` / `prune-test-env-apply`. Coverage: `tests/test_scripted_runtime_teardown.py`, `tests/test_prune_test_schemas.py`.

> Revision note (2026-06-11, janitor ordering + ownership guards): `scripts/prune_test_schemas.py` now snapshots the PG schema list *before* scanning runtime dirs (a runtime created after the snapshot is simply not a candidate), and skips unpaired schemas referenced by active backend queries (verdict `skipped_active`, reason `active_query_reference`; `--force` overrides). Do NOT run `--apply` while test suites are executing: tmp-dir pytest fixtures (e.g. `tests/pg_durable_runtime.py`) create `sourcing_test_*` schemas with no runtime/test_env pairing, and the active-query guard reduces but does not eliminate that exposure. Teardown also only drops schemas it created: `prepare_workflow_confidence_postgres_schema` records `pre_existing` in the `.ephemeral-test-env.json` marker, and externally supplied (pre-existing) schemas survive `isolated_hosted_test_runtime` teardown.


这份文档定义一套与本地开发环境隔离的测试环境启动方式，目标是让前端联调、hosted smoke、browser E2E、以及未来 CI 都能挂到同一个独立 namespace，而不污染默认 `runtime/`。

## 目标

- 独立 `runtime` / per-runtime PG schema / `jobs` / `company_assets`
- 独立 object storage prefix
- 默认走 `simulate`，避免测试时误触发真实 provider
- 可按需切换到 `scripted`
- 前端、本地脚本、CI 使用同一套入口
- `simulate` / `scripted` / `replay` 必须 fail-closed：即使本机存在真实 Apify/DataForSEO/Serper token，也不能发出真实 provider 请求

## 默认约定

- runtime dir: `runtime/test_env`
- backend port: `8775`
- frontend port: `4175`
- object storage prefix: `sourcing-ai-agent-test`
- external provider mode: `simulate`
- runtime environment: `test`

## 测试 Runtime Contract

测试环境不能只依赖启动 helper 的环境变量约定，必须满足一组可验证 contract：

- Workflow confidence should be production-like but isolated: use the same class of durable store, queue semantics, advisory locks, recovery owners, event callbacks, and public reader contracts as production, while replacing paid providers with scripted/simulated provider adapters.
- `simulate` / `scripted` / `replay` 默认 fail-closed：必须设置 `SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1`，并清空 Apify/Harvest/DataForSEO/Serper 等 live provider secret。
- 测试 runtime 的 control plane、jobs、company assets、hot-cache assets、object storage、provider cache 必须落在 runtime root 下，不能继承 local-dev / production namespace。
- seeded/reference `candidate_documents.json` 是 durable artifact contract，不是临时 fixture。读取器必须容忍 additive serving/card 字段，例如 `headline`、`experience_lines`、`education_lines`、`has_profile_detail`、`needs_profile_completion`；未知字段应保留到 `Candidate.metadata`，不能因为 `Candidate(**payload)` 严格构造失败而静默丢弃整批候选人。
- full-local-reuse / simulate smoke 必须证明 seeded artifact 能被主 workflow 读路径解析并完成 `snapshot_full_materialization`。如果 raw `candidates` 非空但解析后为空，这是 schema contract 失败，不允许伪装成 `candidate_documents_empty` retry storm。
- scripted provider fixture 如果声明 remote wait / pending rounds，测试必须通过 scripted local provider event watcher、terminal event checkpoint，或显式 recovery tick 推进到 terminal fetch；不能依赖“连续本地轮询 N 次自然完成”这种旧轮询 contract。
- 测试环境可以使用专用 PG schema 验证并发 workflow，但 provider mode 仍必须是 non-live，且 live provider access boundary 必须继续 fail-closed。
- 隔离 runtime env file 本身也必须通过 non-live validation。即使 helper 会覆盖 env payload，文件中也不能声明 `SOURCING_EXTERNAL_PROVIDER_MODE=live`、非隔离 `SOURCING_RUNTIME_ENVIRONMENT`、`SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=0`，或任何非空 Apify/Harvest/DataForSEO/Serper secret。
- 测试环境里的 durable work 必须自带可判定的 runtime ownership path，例如 `snapshot_dir` / `candidate_documents_path` / `artifact_paths`。任何 root/local-dev daemon 或 recovery sidecar 扫到这些行时，必须在 claim 前识别 `runtime/test_env/<case>` 并跳过。
- `isolated_hosted_test_runtime(...)` 退出前必须等待 runtime-owned background threads。Webhook/event/recovery 线程不能在 `patched_environment(...)` 恢复 root/local env 后继续执行，否则会把 test job/refill/worker 写进 root namespace。
- Scripted smoke preflight/postflight 必须 fail-closed：运行前后都检查 root/local-dev daemon pid/status 文件、live provider cache synthetic fixture manifests、public/root PG 指向目标 test runtime 的行；non-live provider invocation report 里出现缺失 `provider_mode` 或 `provider_mode=live` 也是失败。
- Test/runtime dependencies should be isolated at the namespace/schema/container level. A future Testcontainers-style harness is acceptable for CI if it creates disposable PG/object-store/provider-stub dependencies and still emits the same smoke reports.
- If Temporal, LangGraph, or another runtime framework is introduced later, test environments must keep the same external contract: deterministic action/workflow state, provider-mode isolation, idempotency, approval gates, report completeness, contamination audit, and Pre-Manual Signoff. Framework test harnesses cannot replace service-level scripted smoke.

## Production-Parity Test Design

The desired test shape is not "unit mocks everywhere"; it is "real control-plane semantics with non-live providers."

Required layers:

- Unit tests for pure normalization, identity, planner, permission, and contract helpers.
- Component tests for writers/readers against a real or production-equivalent PG schema.
- Scripted workflow smoke for provider timing, remote terminal events, profile scheduler, recovery, local apply, board-visible publication, projection readiness, and public-reader parity.
- Browser/manual scripted tests for user-visible progress, board rows, filters, layering, CRM overlay, and Agent approval flows.
- Live provider smoke only when explicitly approved for cost and environment.

Best-practice implications:

- Use disposable schemas or containers for workflow confidence; do not reuse production/local-dev schema state.
- Stub providers at the network boundary with scripted scenarios rather than bypassing the provider connector chain.
- Keep provider invocation reports mandatory so a test cannot accidentally fall through to live.
- Keep public API parity checks mandatory; a correct backend artifact with a broken public reader is still a failed workflow test.
- Do not make browser tests repair state. They observe API contracts and user experience only.

## Containerized CI Strategy

Containerized test dependencies are mandatory for CI workflow confidence, optional for local day-to-day development, and mandatory for pre-release/nightly full confidence.

Recommended layers:

| layer | dependencies | purpose |
| --- | --- | --- |
| `ci-pg-contract` | Testcontainers PG | storage, writer, projection, public-reader, lock, queue, and migration contract tests |
| `ci-workflow-fake-provider` | PG + provider fake HTTP server | provider connector payloads, webhook/poll/dataset download, retry/rate-limit, profile scheduler, recovery, post-profile publication |
| `ci-frontend-browser-gate` | PG + backend + frontend + browser runner | canonical projection board pagination, search, and read-contract behavior |
| `ci-workflow-browser-gate` | PG + fake provider HTTP server + backend + frontend + browser runner | real workflow run publishes a run-scope projection, projection index recovery drains, and browser reads that projection without public-reader fallback |
| `ci-containerized-pre-release` | Docker doctor + all current containerized gates | pre-release aggregation for the available production-parity slices |
| `nightly-container-browser` | PG + object storage stub/MinIO + provider fake HTTP server + backend + frontend + browser runner | full production-like scripted browser/workflow confidence |
| `local-container-smoke` | minimal PG + fake provider or PG-only variant | optional local one-command production-parity smoke |

Rules:

- CI must not depend on a developer's local PG schema, local provider cache, local runtime, or ambient provider secrets.
- Local development may keep using `.local-postgres` / isolated PG schema for speed, but there must be a one-command containerized smoke for production-parity validation.
- Pre-release and nightly workflow confidence should use the full containerized stack.
- GitHub Actions entrypoint: `.github/workflows/containerized-pre-release.yml` runs `make ci-containerized-pre-release` from the `sourcing-ai-agent` workspace after bootstrapping Python, frontend dependencies, and Playwright.
- Provider fake HTTP server should become the workflow-confidence path over time. Existing scripted adapter scenarios may remain as fast unit/component fixtures, but workflow confidence should not bypass provider connector, HTTP payload, webhook/poll, dataset-download, retry, timeout, and rate-limit semantics once the fake server is available.
- The fake provider contract must be unified across providers/features. Do not create separate fixture semantics for webhook, poll, dataset download, error/retry, and rate-limit paths.
- Any legacy public-reader fallback hit in a normal containerized scripted case is a test finding. During migration it may be report-visible compatibility; after cutover it should be a blocking Pre-Manual finding.

### Current Testcontainers PG Harness

The first production-parity slice is a disposable Postgres storage contract harness for canonical projection storage.

Run CI-required mode:

```bash
make ci-pg-contract
make ci-workflow-fake-provider
make ci-frontend-browser-gate
make ci-workflow-browser-gate
make ci-containerized-pre-release
```

Run optional local mode:

```bash
make docker-start
make docker-doctor
make local-container-smoke
```

Behavior:

- `make docker-start` is the local bootstrap entrypoint for containerized gates. On macOS it first tries Docker Desktop (`/Applications/Docker.app`), then Colima when installed. It waits for `docker info` to become reachable and fails with install commands when no local runtime exists.
- `make docker-doctor` is a non-mutating preflight: it requires a reachable Docker daemon, exports the current Docker context socket for Python Docker SDK/Testcontainers, and verifies the dedicated test Python can import `testcontainers` and `psycopg`.
- `make ci-pg-contract` sets `SOURCING_REQUIRE_TESTCONTAINERS=1`, `SOURCING_RUN_TESTCONTAINERS=1`, `TESTCONTAINERS_RYUK_DISABLED=1`, and `SOURCING_TESTCONTAINERS_POSTGRES_IMAGE`. Missing `testcontainers[postgres]`, missing Docker, or a container startup failure is a hard failure. Ryuk is disabled because these contract tests explicitly stop their containers and should not add a second Docker Hub image dependency to the gate.
- `make ci-workflow-fake-provider` uses the same disposable PG requirement, starts an in-process fake Apify provider, submits through the real Harvest connector HTTP path, delivers an Apify-style terminal webhook into the real backend API handler, writes through a filesystem object-storage stub, and verifies remote-provider terminal state in PG. In CI-required mode, missing Docker is a hard failure.
- `make ci-frontend-browser-gate` uses disposable PG, starts the real backend API in-process, builds the frontend in same-origin API mode, starts `vite preview`, and drives `/projections/{projection_id}` with Playwright. The gate validates projection-page load, canonical candidate count, first-page rendering, page 2 pagination, backend-filtered search, and `filter_contract.fallback_used=false`.
- `make ci-workflow-browser-gate` uses disposable PG, fake Apify HTTP, real backend API, real workflow submission through `/api/workflows`, event-time run projection publication, recovery-driven `projection_person_search_index` build, Vite preview, and Playwright against the resulting `/projections/{projection_id}`. The gate validates candidate count, pagination, backend-filtered search, and `filter_contract.fallback_used=false` without manually seeding projection rows.
- `make ci-containerized-pre-release` is the current pre-release aggregation entrypoint. It runs Docker preflight plus `ci-pg-contract`, `ci-workflow-fake-provider`, `ci-frontend-browser-gate`, and `ci-workflow-browser-gate` in sequence so missing Docker, PG contract drift, fake-provider webhook drift, frontend projection-board drift, or workflow-to-browser projection drift fail one command.
- Projection search/filter tests must build `projection_person_search_index` or assert fail-closed `projection_person_search_index_unavailable`. Normal workflow confidence must not enable `SOURCING_ALLOW_LEGACY_PROJECTION_FILTER_SCAN_FALLBACK`; that flag is migration/debug-only and any hit is a public-reader compatibility finding.
- `make local-container-smoke` sets `SOURCING_RUN_TESTCONTAINERS=1` but does not require Docker. If Docker/Testcontainers cannot start locally, the unittest reports a skip rather than blocking ordinary local development.
- Plain `python -m unittest tests.testcontainers_pg_contract` skips unless `SOURCING_RUN_TESTCONTAINERS=1` is set.
- The current harness validates PG-only `ControlPlaneStore` bootstrap and writes for `serving_projections`, `serving_projection_members`, `projection_manifest_shards`, `run_projection_links`, and `collection_authoritative_pointers`.
- The Make targets use `TEST_PYTHON_BIN`, defaulting to `./.venv-tests/bin/python`, so Testcontainers and other test extras are resolved from the dedicated test environment rather than the runtime backend venv.

This harness is still intentionally bounded. It now covers storage/writer/public-reader contracts, fake-provider backend callback/object-store behavior, a seeded canonical projection browser gate, a workflow-driven fake-provider projection browser gate, and a single pre-release aggregation command. It is not a replacement for the full PG-backed scripted/nightly matrix; it is the production-parity contract base that prevents workflow confidence from bypassing connector payloads, provider HTTP semantics, projection public reads, and browser-visible state.

### Scripted Nightly Matrix Exit Contract

The PG-backed scripted/nightly matrix remains the long-latency workflow pressure gate. The current closeout evidence is `output/phase12_full_exitfix_20260520_230047/`, which passed strict smoke and Pre-Manual Signoff for all five nightly long-latency cases and exited normally.

Exit behavior is part of the gate. A matrix run that writes `report.json` / `summary.json` but leaves `run_simulate_smoke_matrix.py` alive is not a clean pass. In-process helpers used by workflow/acquisition code must be owned by the call that starts them and must be joined or cancelled before returning. If work is allowed to outlive the owner call, it must be represented as durable PG work instead of an unowned `ThreadPoolExecutor` thread.

The regression that motivated this contract was the parallel former-search seed helper: after the report was written, non-daemon executor threads continued scanning large Google `harvest_profiles/*.queue_dataset_items.json` files during Python finalization. The fix makes those helper executors owner-bound and adds tests for scoped-search and full-roster failure paths.

### Local Docker Runtime Setup

Use one of these local runtimes before running required container gates:

```bash
# Option A: Docker Desktop on macOS
brew install --cask docker
make docker-start
make docker-doctor

# Option B: Colima on macOS
brew install docker colima
colima start
make docker-doctor
```

Notes:

- CI and pre-release gates should use `make ci-containerized-pre-release` or the individual `ci-*` targets, not `local-container-smoke`, because missing Docker must be a hard failure there.
- Local day-to-day development may use PG-backed scripted runtimes for speed. Before claiming Phase 0b/12 containerized confidence, run `make docker-doctor` followed by the CI-required targets.
- If Docker is not installed, `make docker-start` fails closed and prints the supported install/start commands. It does not silently downgrade a required containerized gate to the local PG schema path.
- Colima uses a context-specific socket such as `unix://$HOME/.colima/default/docker.sock`. The Make targets run through `scripts/with_docker_context.sh` so Python Testcontainers uses the same socket as the Docker CLI instead of falling back to `/var/run/docker.sock`.
- The default PG container image is `public.ecr.aws/docker/library/postgres:16-alpine`, configurable with `TESTCONTAINERS_POSTGRES_IMAGE=...`. This avoids making Docker Hub availability a single point of failure for local and CI gates while still using the official Postgres image line.

### Current Fake Provider HTTP Connector Harness

The first fake-provider slice is connector-level rather than a full containerized workflow stack.

- `SOURCING_APIFY_API_BASE_URL` points Harvest/Apify connector calls at a local fake HTTP server while preserving the same submit, run-status, dataset-items, and sync-run endpoint shapes as Apify.
- `tests.fake_provider_http.FakeProviderHTTPServer` is the reusable in-process fake HTTP base. It records method/path/query/header/body for every request and maps `(method, path)` routes to JSON responses or callable handlers.
- `tests.fake_apify_provider.FakeApifyProvider` is the Apify-specific layer. It owns actor run submit/status, sync-run, dataset pagination, log endpoint, webhook-definition decoding, local webhook delivery, and dataset failure injection semantics for Harvest connector tests.
- Configured fake Apify endpoints are still treated as provider endpoints by the live-access guard. Scripted/simulate/replay modes must fail before HTTP, and synthetic fixture payloads remain blocked in live mode.
- Targeted connector tests cover real `urllib` HTTP flow for async actor submit -> poll -> dataset download and 429/rate-limit mapping into retryable dataset-download errors.
- PG-backed workflow callback integration and filesystem object-storage evidence are covered by `ci-workflow-fake-provider`; workflow-to-projection-to-browser orchestration is covered by `ci-workflow-browser-gate`. Remaining Phase 12 work is broader scripted/nightly signoff and additional provider fake semantics as new providers/features are added.

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
    - `SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1`
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

- SQLite compatibility shadow 已随 Track B B4.3 全量退役：`ControlPlaneStore` 是 PG-pure，轻量单元测试与 workflow confidence 一律走 PG-only control plane（per-test 隔离使用专属 PG schema，而非 SQLite）。
- `scripts/dev_backend.sh` 会在普通 `test/simulate/scripted/replay` runtime 下自动写入：
  - `runtime/test_env/.isolated-local-postgres.env`
- 这个空 sentinel 会阻断 repo-level PG fallback，避免测试 job / assets / registry 写进本地或生产 PG namespace
- scripted/manual/browser smoke、`run_simulate_smoke_matrix.py --runtime-dir`、`run_explain_dry_run_matrix.py --runtime-dir`、`seed_reference_smoke_runtime.py` 属于 workflow confidence，必须使用 PG-only control plane：
  - 缺 `SOURCING_CONTROL_PLANE_POSTGRES_DSN` 或可解析 `.local-postgres.env` 时直接失败
  - 自动生成 runtime 专属 `.scripted-local-postgres.env`
  - 固定 `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
  - 固定 `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
  - helper 仍会写入 `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`，但该变量自 B4.3 起是 inert no-op（SQLite shadow 与其 resolver 已删除），不影响任何行为
- 如果要让某个 workflow confidence runtime 使用特定 PG，必须显式提供 runtime 专属 env file：
  - `SOURCING_LOCAL_POSTGRES_ENV_FILE=/path/to/test-postgres.env`
- 不建议用共享 repo PG 做测试；临时排障需要显式设置：
  - `SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1`
- 本地 PG 自动发现与排障入口见：
  - `docs/LOCAL_POSTGRES_CONTROL_PLANE.md`
- 更完整的 runtime namespace / provider cache 隔离契约见：
  - `docs/RUNTIME_ENVIRONMENT_ISOLATION.md`

### Manual scripted runtime exception

`scripts/dev_scripted_openai_agent_delta.sh` 是交互式 scripted 前后端入口，不再用 disk SQLite 验证并发工作流。默认行为：

- 使用专用 PG control-plane schema：`sourcing_scripted_openai_agent_delta`
- 在 `--reset-runtime` 时安全重建该 schema，避免文件 runtime 已清理但 PG job/worker 状态残留
- 写入 runtime 专属 `.scripted-local-postgres.env`，再启动 backend，避免继承 live/local-dev namespace
- 设置 `SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1` 并清空 Apify/DataForSEO/Serper provider secret env，避免 ambient shell token 进入测试 runtime
- 启用 `SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED=1`，让 scripted provider ready 后像真实 webhook 一样事件级推进 closure
- 将 `WORKFLOW_PROGRESS_REMOTE_WAIT_TAKEOVER_AFTER_SECONDS` 默认设为 90s；普通 remote wait 由 watcher/webhook 观察推进，progress poll 只做 terminal event、lease dead/expired 或 SLA 兜底
- No-baseline scoped-search 手测必须用独立入口 `scripts/dev_scripted_openai_no_baseline_scoped_search.sh`，它复用同一 PG-only/runtime-isolation contract，但设置 `reference_seed_mode=none`，不预置 OpenAI baseline。不要用 baseline+delta launcher 手动“假装”无 baseline。

2026-05-07 事故复盘规则：

- 不要把 scripted/browser smoke 的 webhook driver 改成会在环境恢复后继续执行的后台线程。
- 如果 HTTP webhook 需要 quick ack，入口只能记录 terminal event 并同步启动/确认 job-scoped recovery owner；完整 materialization 由 recovery owner 继续推进。
- smoke 超时或 context 退出时，必须确认没有 provider-webhook / job-recovery / shared-recovery / background materialization thread 遗留。
- 任何 `runtime/provider_cache/local_dev/live/...` artifact 中出现 `openai-agent-*`、`lovable-roster-*` 等 scripted fixture URL 都是隔离事故，不是可接受的缓存命中。
- 在重新运行 PG-backed scripted smoke 前，先跑只读污染审计；审计不删除文件、不改 PG、不调用 provider：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/audit_runtime_contamination.py \
  --target-runtime-dir runtime/test_env/<case> \
  --output-json output/runtime_contamination_audit_current.json
```

- `scripts/run_simulate_smoke_matrix.py --runtime-dir ...` 现在会在启动前和结束后自动执行同一类隔离检查。只要 root/local-dev daemon 仍活跃，或 live provider cache / public PG 中仍有目标 test runtime 的污染证据，smoke 必须 fail closed，而不是继续跑。

旧 `--sqlite-control-plane` manual scripted 入口已删除。需要复现历史 SQLite 问题时，只能写单元/离线迁移测试，不能把 SQLite fallback 当作 scripted/manual workflow confidence 路径。

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

## 交互式 OpenAI Agent + Lovable Scripted 环境

如果要像真实用户一样在浏览器里体验 OpenAI scoped delta streaming 和 Lovable 100+ live roster，不要打真实 Harvest/DataForSEO；使用专门的 scripted 入口：

```bash
make test-env-scripted-openai-agent
```

默认配置：

- runtime dir: `runtime/test_env/openai_agent_delta_streaming`
- backend: `http://127.0.0.1:8785`
- frontend: `http://127.0.0.1:4185`
- scripted scenario: `configs/scripted/openai_agent_and_lovable_streaming.json`，它 include 了 OpenAI Agent delta 和 Lovable live roster 两套 provider 规则
- provider mode: `scripted`
- secrets file: `runtime/test_env/openai_agent_delta_streaming/secrets/providers.local.json`，默认写入空 JSON，避免读取本地真实 provider key
- timing: 默认 `interactive`，每次 scripted Harvest sleep cap 为 `8s`，用于让执行过程页面能观察到 search/profile 等待、pending、recovery 与后续 materialization；不是 CI 的极限快路径
- reset: 默认会清空并重建 `runtime/test_env/openai_agent_delta_streaming`，避免旧 history/job/artifact 影响本次 scripted 体验；如果要保留历史，显式传 `SCRIPTED_RESET_RUNTIME=0`

常用命令：

```bash
make test-env-scripted-openai-agent-seed
make test-env-scripted-openai-agent-backend
make test-env-scripted-openai-agent-status
make test-env-scripted-openai-agent-logs
make test-env-scripted-openai-agent-stop
```

浏览器里使用的推荐 query：

```text
帮我找OpenAI做Agent方向的人
帮我找Lovable的全部成员
```

这个 fixture 覆盖的行为：

- 先种入旧 OpenAI baseline：`20260414T120300`，`300` 个候选人，用于验证 baseline board 先可见。
- 同时写入 Lovable runtime identity，便于在同一个后端里跑无 baseline 的 100+ live roster。
- 模拟 current/former 两条 Harvest profile-search lane，并覆盖 probe + scale 调用形态。
- profile-search 返回数百条候选清单，并包含重复 LinkedIn URL，后续会走真实去重和 profile URL 调度。
- profile scraper batch 使用 pending rounds、retryable delay、actor-slot 压力，模拟“provider 没有立刻完成 / recovery 需要继续捞”的情况。
- 后续 snapshot、candidate artifact、retrieval/result view、candidate board 都跑隔离 runtime 的真实本地 materialization/finalization，而不是只返回 mock board。
- 默认不允许 DataForSEO/public-web Stage 1 fallback。

边界说明：

- 这个交互环境不会连接真实 Apify/Harvest webhook；它用 scripted provider state 加后台 daemon/recovery 来驱动本地 completion/reconcile。也就是说它验证的是“webhook 或 recovery 触发后，本地状态机是否正确推进”，不是外部 webhook 网络链路本身。
- 如果要验证真实 Apify webhook roundtrip，应使用单独的 webhook/tunnel smoke，并由用户显式确认成本和 provider 调用。
- 如果要跑 CI/browser E2E 快路径，可显式启用 fast runtime，此时 scripted sleep cap 默认降到 `0.1s`：

```bash
SCRIPTED_FAST_RUNTIME=1 make test-env-scripted-openai-agent
```

- 如果要更长时间观察 provider 等待，可调整 sleep cap，例如：

```bash
SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=8 make test-env-scripted-openai-agent
```

- 如果要做 live-like scheduler/recovery 稳定性测试，可启用 slow/strict timing。这个模式不会打真实 provider，但会保留 fixture 内的长等待，例如 60-180s profile-scraper batch、乱序完成、retryable timeout long-tail：

```bash
SCRIPTED_SLOW_STRICT_RUNTIME=1 make test-env-scripted-openai-agent
```

- 也可以直接用脚本：

```bash
bash ./scripts/dev_scripted_openai_agent_delta.sh --slow-strict-runtime
```

- 纯 no-baseline scoped-search 手测入口：

```bash
bash ./scripts/dev_scripted_openai_no_baseline_scoped_search.sh
```

- 如果你想复查同一次旧 history/job，不要重置 runtime：

```bash
SCRIPTED_RESET_RUNTIME=0 make test-env-scripted-openai-agent
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
  - 启动命令固定注入 `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`、`SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`；SQLite durable runtime 已在 B4.3 整体删除，本就不存在回退路径（Makefile 仍注入的 `SOURCING_PG_ONLY_SQLITE_BACKEND` 是 inert no-op）
  - 默认使用 test schema；如果要基于当前本地 authoritative assets / CRM records 做人工 reviewed live validation，必须显式传 `SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=public`
  - 明确需要 `LIVE_CONFIRM=1`
  - Makefile 会同时注入 `SOURCING_LIVE_PROVIDER_CONFIRM=1` 和 `SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS=1`；只设置 `SOURCING_EXTERNAL_PROVIDER_MODE=live` 仍会被 runtime contract 拦截。
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
  - 可显式指定 control-plane 状态锚点路径（`db_path`）；其父目录决定 runtime dir 与 PG DSN 解析。SQLite compatibility shadow 已退役，该路径不再对应任何 SQLite 文件
- `SOURCING_JOBS_DIR`
  - 可显式指定 jobs 目录
- `SOURCING_COMPANY_ASSETS_DIR`
  - 可显式指定 company assets 目录

如果你只改了 `SOURCING_RUNTIME_DIR`，但该 runtime 下还没有 `secrets/providers.local.json`，系统会回退到默认 `runtime/secrets/providers.local.json`。这个兼容行为只适合 `live` 或显式 live-test；在 `simulate` / `scripted` / `replay` 下，`load_settings()` 会清空 Harvest/Apify、DataForSEO、Serper 凭据，低层 provider HTTP helper 也会在真实请求前失败。
