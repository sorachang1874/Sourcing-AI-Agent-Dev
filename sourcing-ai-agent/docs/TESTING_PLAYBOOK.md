# Testing Playbook

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义当前 Sourcing AI Agent 的标准测试分层，目标是把“手工 smoke 一次”升级成可重复、可扩展、可解释的回归体系。

适用范围：

- workflow 编排与阶段切换
- hosted 默认路径
- asset reuse / delta planning / explain
- recovery / watchdog / staged feedback
- 低成本 provider 模拟与长尾 scripted 场景

配套的独立测试环境入口见：

- [TEST_ENVIRONMENT.md](./TEST_ENVIRONMENT.md)
  - 用隔离 `runtime/test_env` + 默认 `simulate` 启动 serve / worker daemon / 前端
  - 适合作为前端联调、browser E2E、以及后续 CI 的固定 backend namespace

## Current Closeout Gate

`2026-04-25` 的稳定基线已经用下面这组命令验证。后续如果触碰 request semantics、planning、workflow orchestration、provider integration、results API、frontend plan/board hydration，至少应按影响面复跑对应子集。

```bash
./.venv-tests/bin/python -m pytest tests/test_regression_matrix.py tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -q
./.venv-tests/bin/python -m pytest tests/test_hosted_workflow_smoke.py -q
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 ./.venv-tests/bin/python -m pytest tests/test_frontend_browser_e2e.py -q
cd frontend-demo && npm run build
bash ./scripts/run_python_quality.sh typecheck
./.venv-tests/bin/python -m pytest tests/test_markdown_status.py -q
./.venv-tests/bin/python -m pytest -q
```

最近一次全量结果：

- `1135 passed, 8 skipped, 25 subtests passed`
- browser E2E：`5 passed, 2 skipped, 2 subtests passed`

脚本级 workflow 行为验证还应覆盖：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/closeout_simulate_20260425 \
  --seed-reference-runtime \
  --fast-runtime \
  --strict \
  --timing-summary \
  --report-json output/closeout_20260425/simulate_report.json \
  --summary-json output/closeout_20260425/simulate_summary.json

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/scripted_long_tail_closeout_20260425 \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/google_multimodal_long_tail.json \
  --fast-runtime \
  --case google_multimodal_pretrain \
  --strict \
  --timing-summary \
  --report-json output/closeout_20260425/google_long_tail_report.json \
  --summary-json output/closeout_20260425/google_long_tail_summary.json
```

这些报告不只看 “completed”，还必须看：

- duplicate provider dispatch 是否为 0
- default-off `Public Web Stage 2` 是否没有误触发
- prerequisite-ready 到下游 stage 是否没有异常空转
- `Final Results` 到 board non-empty 是否没有超阈值延迟
- materialization streaming / writer-budget 是否没有慢 gap 或预算耗尽
- frontend plan label 是否来自后端 `effective_execution_semantics` / `dispatch_preview`
- delta/browser 观察报告还必须看：
  - candidate-source/result-view/lifecycle snapshot IDs 是否一致，不能 final summary 指 current snapshot 而 persisted result view 指 baseline
  - search-returned、deduped、profile-required、profile-fetched、served/expected candidate counts 是否出现非单调回退
  - `候选人同步` 是否使用 lifecycle served/expected 口径，而不是前端分页 hydration 当前加载条数
  - baseline-serving delta pending 时，UI 是否在 stale-complete 预算内从 `baseline/baseline` 更新到 `baseline/expected`
  - profile fetched 已追平后，materialized/served count 是否仍长时间停留在 baseline
  - 执行过程是否消费 `execution_phase_contract`，避免在 LinkedIn/profile/materialization pending 时显示 `Public Web Stage 2`
  - baseline+delta overlay 和 final current snapshot 的第一页候选人排序是否稳定，不应因 provider worker/batch 完成顺序跳页
  - 每个 profile worker completion 到下一批 submit start 的 gap 是否在场景阈值内；provider sleep/actor duration 要和本地 handoff 指标分开

## 1. 测试分层

### 1.1 快速单元测试

用途：

- request normalization
- planner / dispatch / scoring / registry / artifact repair
- asset membership exact overlap / subsumption
- provider adapter 的 deterministic 逻辑

标准命令：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python -m unittest discover -s tests -v
```

更高效的本地入口：

```bash
cd "sourcing-ai-agent"
bash scripts/run_regression_suite.sh fast
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode durations
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --report-json output/test_reports/changed.json
./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --max-parallel 2 --report-json output/test_reports/changed-parallel.json
```

说明：

- `run_regression_suite.sh fast`
  - 现在不再盲跑全量 `pytest -q`
  - 会根据当前 git 变更路径选择高信号 pytest 子集
- `run_pytest_matrix.py --mode changed`
  - 适合大型 Infra 升级阶段的日常快速回归
  - 当前对 `storage / control_plane / asset_sync / cloud_asset_import / candidate_artifacts / workflow_smoke`
    等核心模块做了映射
  - 这轮继续把 `frontend-demo / contracts / workflow explain` 也映射成更细的 suites，避免前端改动时盲跑一整套后端 smoke
  - Public Web 相关路径现在会显式命中 `public-web-core`、`target-candidate-public-web-api`、`target-candidate-public-web-pg`
  - 当前 `frontend-demo / contracts` 改动不仅会命中前端契约 pytest，也会在同一入口里自动跑 `frontend-demo` 的生产构建
- `run_pytest_matrix.py --mode durations`
  - 输出最慢测试列表，便于持续压缩全量回归耗时
- `run_pytest_matrix.py --report-json ...`
  - 会把本次选中的 suites、实际命令、退出码和耗时写到 JSON
  - 适合在长时间自主开发时沉淀“这轮为什么慢、慢在哪个 suite”这类结构化记录
- `run_pytest_matrix.py --max-parallel N`
  - 会把 changed/smoke 模式下互相独立的 suites 并发执行
  - 适合 `storage / control_plane / workflow / frontend build` 这种跨域改动后的本地快速回归
  - 当前仍保留 `full` / `durations` 的串行行为，避免全量基线统计被并发噪声干扰

使用时机：

- 每次非微小改动后的基础回归
- 修改 shared schema、intent view、planner、dispatch、storage、artifact pipeline 后

### 1.1.1 Pre-Agent contract gate

Phase 13 Agent UI/graph work, Operation Queue changes, and any W7/W9/W10/W11 owner/control-surface change must pass the fast pre-Agent contract gate before long W6/nightly or live-provider validation:

```bash
make ci-pre-agent-contract
```

This gate is intentionally faster than W6/nightly and must catch basic contract drift first. It covers:

- W10 owner/source-of-truth ledger and frontend contract preflight: `tests/test_pre_agent_contract_review.py`
- OperationRun / AgentAction / command owner adapters and Activity spine API behavior: `tests/test_operation_runtime.py`
- W7e legacy target-candidate Public Web runtime-boundary and retirement audit: `tests/test_crm_public_web_runtime_boundary.py`, `tests/test_legacy_public_web_retirement_audit.py`
- PG-only storage surface guardrails for retired SQLite/profile-registry fallback entrypoints: `tests/test_storage_surface_guardrails.py`
- HTTP-level historical asset/media backfill review gate: `tests/test_projection_crm_api_contracts.py::test_asset_backfill_http_routes_are_explicit_migration_paths`
- Durable command owner signoff proof used by scripted smoke: `tests/test_scripted_smoke_signoff.py -k durable_command_owner_contract`

If this target fails, do not use W6/nightly to rediscover the same issue. Fix the contract owner, API shape, preflight, or migration-only boundary first, then rerun this target.

### 1.1.2 Historical person/company asset-media backfill gate

Phase 5c / Phase 7c historical asset repair must use the explicit PG-only operator entrypoint. Local asset readers, company overview pages, projection readers, and media URLs must not trigger repair or request-path media fetches.

Default dry-run:

```bash
make asset-media-backfill-dry-run \
  ASSET_MEDIA_BACKFILL_COLLECTION_IDS="company:openai" \
  ASSET_MEDIA_BACKFILL_PROJECTION_IDS="proj_..." \
  ASSET_MEDIA_BACKFILL_OUTPUT_JSON="output/asset_media_backfill_dry_run.json"
```

Reviewed apply:

```bash
make asset-media-backfill-apply \
  ASSET_MEDIA_BACKFILL_REVIEWED=1 \
  ASSET_MEDIA_BACKFILL_COLLECTION_IDS="company:openai" \
  ASSET_MEDIA_BACKFILL_PROJECTION_IDS="proj_..." \
  ASSET_MEDIA_BACKFILL_APPLY_OUTPUT_JSON="output/asset_media_backfill_apply.json"
```

Both targets delegate to `scripts/backfill_person_company_asset_media.py`. The script enforces PG-only runtime (`SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`, `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`, `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`) and defaults to dry-run. `asset-media-backfill-apply` is blocked unless `ASSET_MEDIA_BACKFILL_REVIEWED=1`, and the script still requires `--reviewed`.

Optional knobs:

- `ASSET_MEDIA_BACKFILL_COMPANIES="OpenAI Anthropic"` limits company logo/fact targets when collection ids are unavailable.
- `ASSET_MEDIA_BACKFILL_PUBLIC_WEB_RUN_IDS="run_..."` limits Public Web signal backfill.
- `ASSET_MEDIA_BACKFILL_LOGO_SOURCE_URLS="company:openai=https://..."` supplies explicit logo URLs.
- `ASSET_MEDIA_BACKFILL_RUN_NOW=1` drains planned `media.asset.cache` commands after planning.
- `ASSET_MEDIA_BACKFILL_INCLUDE_HOMEPAGE_FAVICON=1` allows homepage favicon derivation; keep it off unless reviewed, because it is heuristic.

### 1.1.3 CRM Public Web quality gate

目标候选人级 Public Web Search 已进入默认质量门禁，不再作为只靠手动命令验证的旁路模块。W7 后 normal path is CRM-owned; legacy target-candidate Public Web commands below are historical/migration coverage only and should not be used to validate product behavior.

标准入口：

```bash
bash ./scripts/run_python_quality.sh all
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_pytest_matrix.py --mode changed --changed-path src/sourcing_agent/public_web_search.py --dry-run
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_projection_crm_api_contracts.py tests/test_results_api.py -k 'legacy_target_candidate_public_web_endpoints_retire_by_default or crm_public_web or target_candidate_public_web or public_web_api or promotion'
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_target_candidate_public_web.py tests/test_public_web_search.py tests/test_public_web_quality.py tests/test_linkedin_url_normalization.py
PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_control_plane_live_postgres.py -k 'target_candidate_public_web_state_is_postgres_authoritative or postgres_only_uses_ephemeral_sqlite_shadow or postgres_only_skips_sqlite_fallback'
(cd frontend-demo && npm run build)
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_frontend_browser_e2e.py -k 'target_candidate_public_web_selection_trigger_and_polling or target_candidate_public_web_promotion_and_export'
```

Scripted/live matrix coverage:

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir /tmp/sourcing-target-public-web-smoke \
  --seed-reference-runtime \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/openai_agent_scoped_delta_streaming.json \
  --matrix-file configs/scripted/target_public_web_service_smoke_matrix.json \
  --case target_public_web_service_slo_from_workflow_result \
  --fast-runtime \
  --strict \
  --timing-summary
```

这组门禁覆盖：

- 默认 workflow 仍不回到 `Public Web Stage 2`
- legacy `/api/target-candidates/public-web...` HTTP aliases 永久 `410` fail-closed；`SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS` 只保留为历史上下文，不能临时启用
- legacy target-candidate Public Web orchestrator start/cancel/retry/list 方法同样永久 fail-closed；不要再写旧 target-candidate service e2e，正常行为必须迁到 CRM Public Web 测试
- legacy target-candidate Public Web 底层 batch/cancel/worker execution helper 同样永久 fail-closed；直接调用 `start_target_candidate_public_web_batch(...)`、`cancel_target_candidate_public_web_run(...)`、`execute_target_candidate_public_web_run_once(...)`、`sync_public_web_batch_summary(...)` 只能返回 report-visible retired envelope，不能写 `target_candidate_public_web_v1` execution state
- legacy target-candidate Public Web detail/promotion/export orchestrator methods 同样永久 fail-closed；target-candidate profile compatibility 不能再从 legacy Public Web signals 推导 selected contact，相关覆盖必须迁到 CRM Public Web API tests
- CRM record id 不能被自动桥接成 `target_candidates`；target -> CRM owner sync 默认禁用，只有显式 `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_TO_CRM_SYNC=1` 的 reviewed historical migration test 可写 `target_candidate_public_web_v1` evidence
- `POST /api/crm/records/public-web-search` 只排队，不在 HTTP request 中跑 DataForSEO/fetch/LLM
- post-workflow `target_public_web_action` 会从 run projection 选取真实 workflow candidates、加入 CRM、触发真实 CRM Public Web batch、驱动 recoverable worker，并从真实 batch rows 评价 SLO；普通 workflow matrix 不应在未触发该 action 时声明 Public Web service coverage
- Public Web service metrics 必须报告 `crm_public_web_v1` storage owner 和 execution backend counts；normal CRM Public Web action 中 `target_candidate_public_web_v1` execution backend 或 legacy storage-owner batch 都必须阻断 signoff
- CRM Public Web start 必须产生并执行 `workflow_commands(command_type='crm.public_web.queue_batch', owner='crm_public_web_owner')`。路由内同步排 worker 只能通过这个 command owner drain 实现；关闭 `CRM_PUBLIC_WEB_QUEUE_BATCH_COMMAND_OWNER_ENABLED` 时必须 report-visible fail-closed，不能回退到 target-candidate bridge 或 route-local worker enqueue。Target Public Web service smoke 必须启用 `require_crm_public_web_queue_batch_command`，并要求 `queue_batch_command_pending/failed/invalid_owner/incomplete_causality_count=0`，这样 W6/nightly 只验证长链路稳定性，不再首次发现缺 typed-command proof。
- W7 runtime-boundary preflight 必须先跑 `tests/test_crm_public_web_runtime_boundary.py`。它验证正常源文件不能从 `target_candidate_public_web.py` 直接导入 CRM Public Web 符号，`crm_public_web_runtime.py` 和 `legacy_target_candidate_public_web_runtime.py` 必须导入 `public_web_runtime_core.py` 而不是 target facade，legacy target-candidate execution references 只能通过迁移边界进入，历史测试造旧数据只能通过 `legacy_public_web_storage.seed_legacy_target_public_web_*`，并验证正常 CRM Public Web route/queue-batch 不再保留 `_queue_crm_public_web_workers` 这类 worker 创建入口。完整 fake-provider/live-provider gate 不应再用于首次发现这种 import/owner 边界漂移。
- 旧 `target_candidate_public_web_*` storage 写入必须 runtime fail-closed；只有显式 `legacy_target_public_web_migration_write_context(...)` 的迁移/冷备路径可以 seed 历史行。普通产品、API、worker、browser、scripted tests 不能直接调用 `store.upsert/update_target_candidate_public_web_*`。
- W7e physical deletion 前必须跑 `scripts/audit_legacy_public_web_retirement.py --strict`，并在 hosted/scripted signoff 中启用 `require_legacy_public_web_retirement_ready=true`。同一只读证据也由 `/api/migrations/legacy-public-web` 和 `workflow_service_metrics.legacy_public_web_retirement` 暴露，legacy target-candidate Public Web batches/runs/promotions 必须为 `0`，且 audit 不得被 row limit 截断；否则 signoff 阻断删除。CRM-owned Public Web rows 和 collection authoritative pointers 只作为上下文。公司资产 Overview 未覆盖所有公司不是删除 legacy Public Web runtime 的 blocker。
- first-class `person_public_web_signals` / detail API 只返回 model-safe summaries/evidence links
- manual promotion 必须先写 promotion record，Public Web email 才能更新 `primary_email`
- Public Web `signal_id` is a stable person/value identity, not a run artifact. It must be derived from person/record owner, `signal_kind`, `signal_type`, and normalized email/url only; it must not include `run_id`, provider result order, batch id, or force-refresh nonce. Detail/export must first match promotion by exact `signal_id`, then by the same stable identity so historical manual promotion/rejection survives force-refresh. A default `promoted_only` export returning zero signals after a promoted same-value refresh is a contract failure and needs a fast regression, not another live-provider pass.
- non-publishable / dirty URL-shape signal 的人工覆盖必须有 `override_reason`
- `promoted_only` 与 `promoted_and_publishable` 导出模式都不包含 raw HTML/PDF/search payload
- target-candidate frontend selection/polling/detail promotion/export browser flow 没有回退

W7g guarded CRM Public Web live/product validation:

Before any live-provider CRM Public Web product validation, run the fast Contract gate:

```bash
make ci-pre-agent-contract
```

`ci-pre-agent-contract` includes the guarded W7g dry-run entrypoint, so canonical endpoint planning, live-cost guards, legacy-path blockers, expected adjudication model metadata, and report generation are checked before any live provider pass. You can also run the dry-run target directly while preparing reviewed record ids:

```bash
make test-crm-public-web-live-product-validation \
  CRM_PUBLIC_WEB_LIVE_RECORD_IDS="crm_record_id_for_manual_validation"
```

The dry-run report is the operator checklist for the real pass. It must include `live_prerequisites.ready_to_execute_live_with_current_args`, `live_prerequisites.missing_or_required_before_live`, `live_prerequisites.record_id_selection_guidance`, and `live_prerequisites.recommended_make_command`. Use that report to select 1-3 reviewed `CRMRecord` ids; do not use legacy target-candidate ids or dry-run evidence as live product-quality evidence.

The live runner must preflight `GET /api/providers/health` before it creates a CRM Public Web batch. A cached provider-health `ready` response is not sufficient if the in-process model circuit is open; model-provider health must fail visibly with `provider_health` evidence and must not start DataForSEO or model adjudication work. This keeps relay outage, usage-limit, DNS, and circuit-open incidents separate from Public Web quality evaluation.

The same gate also runs the PG-backed profile retry-wave isolation checks from `tests/test_enrichment.py`. Those checks enforce that LinkedIn profile retry remains item-level: normal-wave successful URLs are not retried with failed URLs, retry-wave dispatch waits for normal-wave closure, and durable runtime evidence is written through PG-only storage rather than SQLite compatibility.

The gate also runs DataForSEO item-level batch retry checks. Those checks enforce that Standard Queue `task_post` batch envelopes retry only failed query items, keep successful task ids submitted, require caller-assigned `task_key` / `query_identity_key` before provider or search-chain fallback, never bind provider responses by request order when provider-echoed query identity is missing, preserve sibling successes when one `task_get` fails, make Public Web batch submission consume submit-failed checkpoints as query-level failures rather than whole-batch failure or fake pending work, keep Public Web query identity scoped to stable candidate/query identity rather than candidate display ordinal, keep search seed discovery keys scoped to stable search query signature rather than query-list ordinal, and keep exploratory enrichment keys scoped to stable candidate/query identity rather than `candidate_id::index`.

Actual live execution requires an already running live test backend, explicit record ids, and explicit cost confirmation. The Make target also re-runs `ci-pre-agent-contract` when `CRM_PUBLIC_WEB_LIVE_DRY_RUN=0` and passes `CRM_PUBLIC_WEB_LIVE_PRE_AGENT_CONTRACT_PASSED=1` to the guarded runner, so direct script execution cannot quietly bypass the fast owner/source-of-truth checks:

```bash
LIVE_CONFIRM=1 make test-env-backend-live
LIVE_CONFIRM=1 make test-crm-public-web-live-product-validation \
  CRM_PUBLIC_WEB_LIVE_DRY_RUN=0 \
  CRM_PUBLIC_WEB_LIVE_RECORD_IDS_REVIEWED=1 \
  CRM_PUBLIC_WEB_LIVE_MAX_FETCHES_PER_CANDIDATE=10 \
  CRM_PUBLIC_WEB_LIVE_MAX_AI_ENTRY_LINKS=10 \
  CRM_PUBLIC_WEB_LIVE_MAX_AI_EVIDENCE_DOCUMENTS=10 \
  CRM_PUBLIC_WEB_LIVE_MAX_REMOTE_SEARCH_WAIT_SECONDS=420 \
  CRM_PUBLIC_WEB_LIVE_MAX_PROVIDER_PENDING_WAIT_SECONDS=420 \
  CRM_PUBLIC_WEB_LIVE_MAX_CONCURRENT_CANDIDATE_ANALYSES=1 \
  CRM_PUBLIC_WEB_LIVE_RECORD_IDS="crm_record_id_for_manual_validation"
```

When the selected `CRMRecord` ids come from the current local authoritative asset store, start the live backend against the reviewed local PG schema explicitly:

```bash
SOURCING_CONTROL_PLANE_POSTGRES_SCHEMA=public LIVE_CONFIRM=1 make test-env-backend-live
```

Do not rely on SQLite fallback or ambient runtime inheritance. The live backend target must inject `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`, `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`, and `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`; a startup failure here is a contract failure, not a reason to bypass PG-only durable runtime.

Set `CRM_PUBLIC_WEB_LIVE_FORCE_REFRESH=1` when validating a model/provider metadata contract change, so the live pass regenerates Public Web artifacts instead of reusing already-terminal runs.

Use `CRM_PUBLIC_WEB_LIVE_MAX_FETCHES_PER_CANDIDATE` together with `CRM_PUBLIC_WEB_LIVE_MAX_AI_ENTRY_LINKS` and `CRM_PUBLIC_WEB_LIVE_MAX_AI_EVIDENCE_DOCUMENTS` for Public Web quality validation. The default guarded live pass uses `10/10/10` so the model sees a source-diversified search-result and document evidence packet large enough to evaluate provider recall and adjudication quality; lowering one side can make a poor result indistinguishable from an undersized model-input window.

Use `CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS=<seconds>` for force-refresh passes when DataForSEO remote batches may legitimately exceed the default 600-second operator timeout. Pair it with explicit `CRM_PUBLIC_WEB_LIVE_MAX_REMOTE_SEARCH_WAIT_SECONDS` and `CRM_PUBLIC_WEB_LIVE_MAX_PROVIDER_PENDING_WAIT_SECONDS`; those backend task-terminal budgets should normally be lower than the poll timeout so a slow DataForSEO task becomes a typed run timeout/completed-with-errors instead of an operator-level poll timeout. A timeout is a run-terminality failure and must list the non-terminal run ids/statuses; it must not be conflated with model fallback or missing model metadata for a run that has not reached adjudication.

CRM Public Web phase commands are bounded activities, not generic "advance one step" calls. The owner must enforce expected input status per command, quarantine stale superseded-batch commands as no-op, plan missing downstream commands only when the same-batch run has already advanced, and put commands into `retry_wait` when prerequisites are not met. If live validation finds a phase command that re-runs a neighboring phase, self-links as its downstream command, or mutates a run from an older force-refresh batch, stop and add a fast operation-runtime regression before increasing `CRM_PUBLIC_WEB_LIVE_POLL_TIMEOUT_SECONDS`.

`scripts/run_crm_public_web_live_product_validation.py` is the only guarded runner for this pass. It must use canonical CRM endpoints only, must reject `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS=1`, must require the configured expected model, currently `gpt-5.5`, in returned model metadata for model-verifiable terminal runs, must fail if model fallback is reported, and must write `output/w7g_crm_public_web_live_product_validation.json` by default. Do not use the retired `/api/target-candidates/public-web...` aliases for product validation.

- Scope must stay small: 1-3 existing CRM records with LinkedIn/profile identity already visible on a projection board. Do not use a large candidate selection for the first live pass.
- Preconditions: `MODEL_PROVIDER_MODEL` or local `model_provider.model` resolves to `gpt-5.5`; DataForSEO credentials are configured; provider mode is live/empty rather than `scripted` / `simulate` / `replay`.
- Start through canonical CRM APIs only: `POST /api/crm/records/public-web-search` with `crm_record_ids`, then poll `POST /api/crm/records/public-web-search/poll`. Do not enable `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS`.
- Drive recovery through the normal worker/recovery owner until the batch is terminal. Service metrics must show `public_web_storage_owner=crm_public_web_v1`, `public_web_execution_backend=crm_public_web_v1`, and a succeeded `crm.public_web.queue_batch` command owned by `crm_public_web_owner`.
- Inspect at least one detail drawer payload from `GET /api/crm/records/{crm_record_id}/public-web-search`; verify signals are model-safe, evidence links are usable, dirty URL-shape warnings are visible, and raw HTML/PDF/search payloads are not exposed.
- Promote one clean publishable signal if available, or verify the manual override path requires `override_reason` for a non-publishable/dirty signal. Promotion must write CRM-owned promotion + `PersonAssertion` + CRM event without creating target-candidate bridge rows.
- Export through `POST /api/crm/records/public-web-export` in `promoted_only` mode first. The archive must contain model-safe summaries/evidence/promotions only and must not include raw provider payloads.
- Physical deletion of legacy target-candidate Public Web helpers/tables is allowed only after this live evidence and historical migration review confirm no target-candidate-only Public Web rows need interactive access.

当前已覆盖的关键回归样本：

- workflow 编排层的业务级 guardrail 现另外沉淀在：
  - [WORKFLOW_BEHAVIOR_GUARDRAILS.md](./WORKFLOW_BEHAVIOR_GUARDRAILS.md)
  - 这份文档定义的是“重复 dispatch / disabled stage / prerequisite-ready 后应立即推进”这类行为 contract
  - 以后补 scripted smoke / simulate report 时，应优先把这些 contract 变成结构化 assertion，而不是停留在人工观察

- `tests.test_pipeline.PipelineTest.test_asset_reuse_plan_uses_exact_membership_subsumption_for_current_profile_search`
  - 防止 baseline candidate docs 没真正吸收 shard bundle 时，planner 仍误判为“已覆盖”
- `tests.test_control_plane_postgres.ControlPlanePostgresTest.test_upsert_acquisition_shard_registry_rows_splits_current_and_former_tables`
  - 防止 PG former/current 物理分表 cutover 后重新退回单表写入，或把 former/current shard 写错物理表
- `tests.test_results_api.ResultsApiTest.test_job_api_is_summary_only_by_default_and_detail_is_opt_in`
  - 防止 `/api/jobs/{job_id}` 再次默认返回全量 events/request detail，拖慢前端 progress fallback
- `tests.test_organization_execution_profile.OrganizationExecutionProfileTest.test_promotion_candidate_selection_does_not_chain_regress_on_explicit_baseline_inclusion`
  - 防止把 `evaluate_organization_asset_registry_promotion(...)` 误当成链式排序器，导致先选到更优 snapshot 后又被旧 aggregate row“反晋升”回去
- `tests.test_provider_execution_policy.ProviderExecutionPolicyTest.test_core_roster_strategies_do_not_require_explicit_high_cost_approval`
  - 防止 generic cost gate 重新长回 core roster/profile-search lane 的显式 gate
- `tests.test_pipeline.PipelineTest.test_build_sourcing_plan_full_company_defaults_do_not_require_high_cost_approval`
  - 防止 `full_company_roster` 的 former lane 又被 generic cost gate 降级回低成本 web search
- `tests.test_pipeline.PipelineTest.test_build_sourcing_plan_omits_public_web_stage_by_default`
  - 防止 `Public Web Stage 2` 又被重新塞回默认 acquisition 主链路，重复阻塞 board readiness
- `tests.test_pipeline.PipelineTest.test_single_stage_workflow_skips_public_web_stage_and_publishes_stage_progress_markers`
  - 防止 single-stage workflow 又错误地产生 `public_web_stage_2` summary / stage-order，导致 wall-clock 与前端时间线重新失真

排障规则：

- 当看到的是 durable state 错误时，先追 writer 链，不先停在 reader 层。
  - 例如 authoritative row / execution profile / registry summary 持久化错误，应先检查：
    - 谁在写 authoritative
    - 谁在 backfill / promote
    - 谁在 refresh existing profile
  - 不要只因为“第一个暴露症状的模块”是 planner 或 execution profile，就先在 reader 层打补丁。
- promotion guard 与 candidate selection 必须分开。
  - `evaluate_organization_asset_registry_promotion(...)` 这类函数只能回答：
    - “candidate 能不能替代当前 authoritative”
  - 不能直接拿来做多轮 pairwise 全序选择。
  - 若多个模块都要选 baseline candidate，应提 shared helper，再让 planner / backfill / execution profile 共用。

### 1.2 Hosted Explain Dry-Run Matrix

用途：

- 只走 `POST /api/workflows/explain`
- 不真正启动 workflow，也不触发 stage polling
- 低成本验证 request normalization / organization execution profile / asset reuse / dispatch / lane planning
- 把“应该 reuse 还是 delta，应该 full roster 还是 scoped search”编码成结构化期望

标准入口：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py --strict
```

当前默认 explain matrix 覆盖：

- Physical Intelligence：新组织 / runtime identity / full roster
- Reflection AI：小组织 + directional local reuse
- Humans&：小组织 + full local reuse
- Anthropic：中型组织 + baseline reuse
- OpenAI Reasoning：已有 directional selected snapshot reuse
- OpenAI Pre-train：large org scoped delta
- Google multimodal + Pre-train：partial shard covered + delta
- NVIDIA World model：large org fresh scoped planning

自动化回归：

- [tests/test_hosted_workflow_smoke.py](../tests/test_hosted_workflow_smoke.py)
  - `test_default_explain_matrix_covers_reference_regressions`
  - `test_hosted_explain_dry_run_matrix_covers_reference_regressions`
- [tests/test_workflow_explain.py](../tests/test_workflow_explain.py)
  - 继续承担 explain 细粒度单测与 planning 语义断言

输出重点：

- `summary.target_company`
- `summary.keywords`
- `summary.org_scale_band`
- `summary.default_acquisition_mode`
- `summary.dispatch_strategy`
- `summary.dispatch_matched_job_id`
- `summary.dispatch_matched_job_status`
- `summary.baseline_directional_local_reuse_eligible`
- `summary.current_lane` / `summary.former_lane`
- `summary.plan_current_task_strategy_type`
- `summary.plan_current_search_seed_queries`
- `summary.plan_current_filter_keywords`
- `summary.plan_current_query_bundle_queries`
- `summary.plan_former_task_strategy_type`
- `summary.plan_former_search_seed_queries`
- `summary.plan_former_filter_keywords`
- `summary.plan_former_query_bundle_queries`
- `summary.timings_ms`
- `summary.timing_breakdown_ms.prepare_request`
  - 现在已经细拆到：
    - `llm_normalize_request`
    - `deterministic_signal_supplement`
    - `canonicalize_request_payload`
    - `final_request_materialization`

自定义矩阵：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --matrix-file configs/explain_dry_run_matrix.example.json \
  --strict
```

隔离 runtime 运行：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --runtime-dir runtime/test_env/explain_matrix \
  --seed-reference-runtime \
  --fast-runtime \
  --strict
```

说明：

- 这层是推荐的第一道回归，优先用来挡住 `Physical Intelligence / OpenAI Pre-train / OpenAI Reasoning` 这类 planning 级回归
- 如果 explain dry-run 已经失败，不要直接去跑完整 hosted workflow smoke
- 脚本级 explain matrix 还有一个需要单独区分的现象：`runtime 数据漂移`
  - `scripts/run_explain_dry_run_matrix.py --strict` 如果直接打当前本地 runtime，它读取的是你当前真实状态：
    - 本地 `company_assets`
    - 本地 Postgres / job history
    - 当前 authoritative registry / selected snapshots / generation
  - 因此它失败时不一定代表代码回归，也可能只是“默认矩阵预期仍按旧 runtime 假设写的”，而你本地资产已经变了
  - 典型例子是某公司原本在默认矩阵里预期 `new_job`，但你本地已经有 authoritative baseline，于是实际会变成 `reuse_snapshot`
  - 所以：
    - 要做稳定回归，用 `--runtime-dir` 指向隔离 test runtime
    - 要做“近真实本地数据”的 scripted 验证，用 `seed_test_env_assets.py` 先把当前 authoritative snapshots 种进专用 `runtime/test_env/...`；该脚本要求 Postgres control plane，不再读取磁盘 SQLite fallback
    - 不要把“直接打当前本地 runtime 的 explain matrix 预期不符”机械当成代码 regression
- `--runtime-dir`
  - 脚本会自起一个 in-process backend，并把 `SOURCING_RUNTIME_DIR` 指向该目录
  - 若未显式传 `--runtime-env-file`，脚本会在该 runtime 下生成 `.scripted-local-postgres.env`
  - 该 env file 必须解析到可连接 Postgres，并固定 `postgres_only + shared_memory`
  - 缺 PG DSN、PG 不可连接、schema 不安全，脚本必须 fail closed；workflow confidence 不再回退到 SQLite
- 推荐使用 `./.venv-tests/bin/python`
  - 避免系统 `python3` 与 repo 依赖、`dataclass(slots=True)` 兼容性、`requests/psycopg` 安装状态不一致
- 如果你要做“接近本地真实数据”的 scripted explain，而不是 reference seed：
  - 先把目标公司 authoritative snapshot 种到隔离 runtime
  - 再让 explain/smoke 脚本直接指向这个 `runtime/test_env/...`
  - 推荐两步：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/seed_test_env_assets.py \
  --source-runtime-dir runtime \
  --target-runtime-dir runtime/test_env/local_like_explain \
  --company OpenAI \
  --company Anthropic

PYTHONPATH=src ./.venv-tests/bin/python scripts/run_explain_dry_run_matrix.py \
  --runtime-dir runtime/test_env/local_like_explain \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/reflection_pending.json \
  --strict
```

### 1.3 Hosted Simulate Smoke

用途：

- 用真实 hosted API 路径验证 `plan -> review -> workflow -> progress -> results`
- 不触发真实 Harvest / Search / model / semantic 调用
- 验证不同组织规模的默认执行画像是否正确

标准入口：

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --strict
```

隔离 runtime 入口：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/simulate_matrix \
  --seed-reference-runtime \
  --fast-runtime \
  --strict
```

当前默认 smoke matrix 覆盖：

- Skild AI
- Humans&
- Anthropic
- OpenAI
- Google

自动化回归：

- [tests/test_hosted_workflow_smoke.py](../tests/test_hosted_workflow_smoke.py)
  - `test_default_smoke_matrix_covers_reference_orgs`
  - `test_hosted_simulate_smoke_matrix_completes_across_small_medium_large_orgs`
    - 默认日常回归，只跑 3 条代表性 flow：
      - small: `Skild AI`
      - medium: `Humans&`
      - large: `OpenAI`
  - `test_hosted_simulate_full_smoke_matrix_completes_when_enabled`
    - 全量 5-case hosted 回归入口
    - 需显式设置 `SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1`
  - `test_hosted_simulate_reuse_queries_preserve_follow_up_planning_contract`
    - 覆盖 `Reflection AI Post-train` 与 `OpenAI Reasoning`
    - 先跑完整 reuse workflow，再立刻做 follow-up explain
    - 断言不会因为 workflow 写回 control-plane 后把下一次 query 错误打回 `live_acquisition` 或 `delta_from_snapshot`
  - `test_hosted_simulate_completed_history_round_trip_exposes_results_recovery`
    - 覆盖 `history_id -> results -> /api/frontend-history/{history_id}` 恢复链路
    - 防止历史记录可以打开但候选人看板为空、或恢复后丢失 `job_id/results` 关联

适用场景：

- hosted 默认路径改动
- explain / dispatch / asset reuse 改动
- request normalization / organization execution profile 改动
- staged feedback / stage summaries 改动
- cloud import / GC ledger、generation watermark、runtime progress observability 改动

建议运行方式：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python -m unittest tests.test_hosted_workflow_smoke -v
SOURCING_RUN_FULL_HOSTED_SMOKE_MATRIX=1 PYTHONPATH=src ./.venv-tests/bin/python -m unittest tests.test_hosted_workflow_smoke -v
```

说明：

- 默认 `unittest` 保持 3-case，是为了把日常回归时长压在可接受范围
- 全量 5-case 仍是正式回归入口，不再只存在于脚本里
- `scripts/run_simulate_smoke_matrix.py` 仍保留完整默认 matrix，适合手工 smoke / 运维排障 / 新场景验证
- `--runtime-dir` 适合把 smoke 固定在 `runtime/test_env/...` 下的专用目录，而不是直接打日常开发 runtime
- 如果希望 smoke 更接近本地真实资产，而不是 reference fixture：
  - 先用 `scripts/seed_test_env_assets.py` 从当前 Postgres control-plane runtime 复制/链接 authoritative snapshot
  - 再用 `--runtime-dir` + `--provider-mode scripted` 跑 case report
- smoke 结果里现在也会带更多 explain 侧结构化摘要，便于直接看出：
  - 为什么是 `reuse_snapshot` / `delta_from_snapshot` / `new_job`
  - 当前 / former lane 计划用了哪些关键词
  - 实际 review gate 后是不是仍保留高成本 provider 路径
- API 侧也新增了一条默认测试约束：
  - `/api/jobs/{job_id}` 默认只返回 summary-first 轻量 payload
  - `/api/jobs/{job_id}/results` 默认只返回 summary/counts，不自动带 full candidates
  - 只有需要完整详情的 smoke / browser / 人工链路才显式加：
    - `?include_details=1`
    - `?include_candidates=1`
- `scripts/run_simulate_smoke_matrix.py` 现在也支持 timing aggregation：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --report-json output/scripted_smoke/report.json \
  --summary-json output/scripted_smoke/summary.json \
  --strict \
  --timing-summary \
  --max-total-ms 2500 \
  --max-wait-ms 1800
```

说明：

- `--timing-summary`
  - 把 `explain / plan / review / start / wait_for_completion / fetch_job_and_results / dashboard_fetch / candidate_page_fetch / total`
    的聚合统计输出到 stderr
  - 同时会汇总 `provider_case_report`：
    - `linkedin_stage_1 / stage_1_preview / public_web_stage_2 / stage_2_final` 的 wall-clock 聚合
    - `provider_backpressure` 的 shared runtime tuning budget、DB limiter active/wait/backlog 和 recommended action
    - `workflow_benchmark` 的 search returned、roster returned、fetched profile、profile URL queued/total、board ready/nonempty 和 board candidate counts
    - `workflow_wall_clock_ms`
      - `job_to_stage_1_preview`
      - `job_to_final_results`
      - `stage_1_preview_to_final_results`
      - `final_results_to_board_ready`
      - `job_to_board_nonempty`
    - `board_ready_count / board_ready_nonempty_count`
    - `progress_regression_case_count / progress_sample_count / progress_maxima`
    - `strategy_rollups`
      - `effective_acquisition_mode`
      - `dispatch_strategy`
  - 单 case 结果里也会直接带结构化 `provider_case_report`，可用于检查：
    - `search query_count / queued_query_count`
    - `search_seed_added_entry_count`
    - `fetched_profile_count`
    - `candidate source` 与 `board ready` 观测
    - `progress_observability`
      - worker / remote-wait 峰值
      - monotonic counter regression
      - Stage 1 / result-view lifecycle regression
      - progress contract invariant violation counts
      - `manual_review_count` backlog reduction
      - `terminal_progress_lag_detected`
    - `workflow_wall_clock_ms`
      - 优先读 backend stage summary
      - 若 backend 没稳定带时间戳，则回退到 client-observed timeline + smoke timings
- `--max-total-ms`
  - 对 `total` 的 aggregate `p95` 做回归守卫
- `--max-wait-ms`
  - 对 `wait_for_completion` 的 aggregate `p95` 做回归守卫
- `--report-json / --summary-json`
  - 直接把 full per-case report 与 aggregate summary 落到文件
  - 推荐 scripted smoke / benchmark 统一使用，避免再依赖 shell redirect 抓 stdout/stderr
- 最新一轮 fresh isolated smoke 参考目录为 `output/scripted_smoke_v4/`：
  - `openai_reuse`
    - total 约 `1505.78ms`
    - `job_to_board_nonempty` 约 `1020.23ms`
  - `google_scoped_cold`
    - total 约 `5144.26ms`
    - `job_to_board_nonempty` 约 `1076.37ms`
  - `xai_live_roster`
    - total 约 `18825.6ms`
    - `queued_worker_count / waiting_remote_harvest_count / pending_worker_count` 峰值均为 `3`
- 当前 per-case 顶层和 nested `provider_case_report` 已基本对齐：
  - 顶层现在也会直接导出：
    - `stage_summary_digest`
    - `stage_wall_clock_ms`
    - `workflow_wall_clock_ms`
- 若要看最完整的 provider-grade 结构，仍优先看 nested `provider_case_report`：
  - `stage_wall_clock_ms`
  - `workflow_wall_clock_ms`
  - `progress_observability`
  - `counts`
  - `behavior_guardrails`
    - `duplicate_provider_dispatch`
    - `disabled_stage_violations`
    - `prerequisite_gaps`（diagnostic；用于定位 UX/performance gap，不默认等同 hard failure）
  - `final_results_board_consistency`

Pre-manual scripted signoff:

- 在把 PG-backed/scripted run 交给用户手动浏览器测试前，必须对 smoke report 做固定 signoff review，而不是临时翻 log。入口：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/review_scripted_smoke_run.py \
  --report-json output/<run>/report.json \
  --summary-json output/<run>/summary.json \
  --output-json output/<run>/signoff.json \
  --output-md output/<run>/signoff.md
```

- Signoff JSON/Markdown 固定输出 `gate_layers`、`passed_gates`、`manual_review_required_findings`、`known_acceptable_warnings`、`blocking_findings`。任何 `blocking_findings` 都禁止进入人工手动测试。
- 该 review 会独立检查 smoke readiness、expectation failures、provider invocation `provider_mode=scripted`、cross-endpoint `board_runtime_state` parity、post-profile SLO、board-visible projection replayability、finalization overlay reuse、behavior guardrail violation。它是“交付前读 logs/artifacts”的产品化质量门，不替代矩阵 hard gate。
- `gate_layers` 将问题分为 `manual_handoff`、`pressure`、`optimization`、`report_integrity`、`contract_integrity`。`manual_handoff` 和 `pressure` 的 blocking 不能交付手测；`optimization` 可在超过 case-level finalization SLO 时进入 manual-review finding，或者在只超过默认诊断阈值时进入 warning；`report_integrity` 缺失报告一律 fail closed。
- `job_to_final_results` 的默认 pre-manual handoff 阈值是普通场景保护线。压力矩阵如果没有显式 `max_job_to_final_results_ms`，但同时声明了 `max_job_to_stage_1_preview_ms` 和 `max_stage_1_preview_to_final_results_ms`，signoff 使用两者之和作为该 case 的总耗时 SLO；否则大规模压力场景会被普通手测阈值误挡。超出这个派生/显式 SLO 仍然是 blocking。

- 现在 scripted smoke/report 已能直接回答几类之前需要人工翻日志的问题：
  - 同一 provider payload 是否被重复 dispatch
  - default-off 的 `public_web_stage_2` 是否被误打开
  - Stage 1 计数、result-view lifecycle、Public Web wording 是否违反进度 contract
  - `LinkedIn Stage 1 -> Stage 1 Preview -> stage_2_final` 之间是否存在显著 prerequisite gap
  - `Final Results` 后 board 是否真正 ready/non-empty

Strict matrix gate:

- Nightly is not the first line of defense for contract correctness. Before
  running `configs/scripted/nightly_long_latency_smoke_matrix.json`, run the
  fast contract gates below and fix any failure first:

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json
PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync
PYTHONPATH=src ./.venv-tests/bin/pytest -q \
  tests/test_workflow_smoke.py \
  tests/test_scripted_smoke_signoff.py \
  tests/test_results_api.py \
  -k 'service_gate_coverage or projection_cutover or legacy_artifact or board_runtime_state or partial_projection or result_view_lifecycle or card_text_ahead_of_card_readiness or canonical_projection_readiness'
```

- The purpose is to catch source-of-truth and migration-bridge mistakes in
  seconds/minutes, before paying the cost of long provider waits. Nightly should
  validate pressure timing, recovery behavior, batching, and long-latency
  ordering. It should not be the first test that discovers a public reader is
  serving from a stale overlay, partial `result_view`, stale lifecycle mirror,
  sidecar artifact, or compatibility pointer.
- Board card-readiness has an internal semantic preflight, not only endpoint
  parity. If `board_runtime_state.card_materialization_status_text` says
  `X/Y`, `X` must be backed by the canonical card-ready count from
  `ServingProjection` membership/readiness for the same denominator. A stale
  asset-population overlay may be kept as report evidence, but it cannot
  downgrade the public card-ready counter or silently coexist with newer
  projection readiness.
- Public-reader source ownership is contract-first. Any source that can affect
  frontend-visible rows/counts/status/filter/export fields must be listed in
  `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md` under "Source Ownership
  Matrix" with owner writer, authority, allowed readers, forbidden normal use,
  and fallback rule before a matrix case may rely on it.
- Migration bridges and fallbacks are not normal paths. A temporary bridge must
  have a named owner, explicit env/flag or migration condition, report-visible
  metric, smoke/signoff assertion, and deletion condition in `docs/NEXT_TODO.md`.
  If a normal scripted case hits a bridge, the case should fail or at minimum
  produce a blocking Pre-Manual Signoff finding.
- The `canonical_public_reader_source_contract` service tag is required for
  Nightly public-read cases. It requires `require_projection_cutover_report`,
  `require_legacy_artifact_coherence_report`,
  `require_board_visible_projection_report`,
  `require_board_runtime_state_cross_endpoint_parity`,
  `require_no_progress_contract_violation`,
  `require_no_board_visible_projection_violation`, and
  `require_no_serving_publication_gap`. These gates must pass before interpreting
  a Nightly failure as a provider/scheduler efficiency issue.
- Legacy workflow artifacts are migration evidence, not terminal authority. The
  fast preflight must prove `legacy_artifact_coherence`: a default job artifact
  cannot claim `completed` while canonical PG job/progress state is still
  `running`, `acquiring`, or `retrieving`. If this gate fails, fix the writer or
  promotion proof before running Nightly.
- Before adding or changing any source that can appear in a public payload,
  review the owner/writer/reader list rather than relying on Nightly discovery:
  `job_result_view`, `job_result_lifecycle`, `board_runtime_state`,
  `serving_projection_members`, projection index/facet rows, migration-era
  legacy artifacts, and compatibility pointers must each have exactly one normal
  serving role. A fallback may exist only when it is fail-closed or explicitly
  migration-gated, report-visible, covered by signoff, and assigned a deletion
  condition.
- Treat this like consumer-driven contract testing: the frontend/smoke consumer
  declares the exact response/read-contract fields it needs, and the backend
  provider proves those fields from the canonical owner. Do not compensate in the
  frontend with a second derivation when the provider contract is incomplete.
  Pact describes this style as tests that document and verify shared request/
  response expectations between consumers and providers. Reference:
  <https://docs.pact.io/>.
- Treat projection serving as a CQRS/materialized-read-model boundary: workflow
  writes and projection/public reads have separate owners. Microsoft's CQRS
  guidance calls out independent read/write models, materialized views, and
  eventual consistency tradeoffs; our projection readiness fields are the
  explicit consistency contract, not an excuse for readers to rebuild state.
  Reference:
  <https://learn.microsoft.com/en-us/azure/architecture/patterns/cqrs>.
- Browser gates should follow Playwright's resilient-test guidance: test
  user-visible behavior, isolate data, control third-party dependencies, and use
  web-first assertions. In this repo that means browser tests observe canonical
  projection APIs and fake/scripted providers; they do not repair state or read
  legacy artifacts directly. Reference:
  <https://playwright.dev/docs/best-practices>.
- Production-parity gates should prefer Testcontainers/fake-provider HTTP for
  dependency realism before full Nightly. Testcontainers' model of disposable
  real dependencies maps to our `ci-pg-contract`,
  `ci-workflow-fake-provider`, `ci-frontend-browser-gate`, and
  `ci-workflow-browser-gate` layers. These gates should catch PG lock/schema,
  connector payload, webhook/poll/dataset, and projection browser contract drift
  before Nightly. Reference:
  <https://www.docker.com/blog/testcontainers-best-practices/>.
- Temporal-style lessons apply even while we keep the current PG workflow engine:
  orchestration should be deterministic and thin, provider/local apply/public
  projection work should be bounded activities, external provider completion is
  a signal, and public progress is a query/read model. Tests should verify these
  boundaries directly rather than relying on eventual terminal success. Temporal
  recommends integration-oriented Workflow tests, time skipping for long-running
  timers, and replay checks for deterministic Workflow history compatibility.
  Reference:
  <https://docs.temporal.io/develop/python/best-practices/testing-suite>.
- 在需要服务级验收的 scripted/browser matrix 中设置 `require_no_progress_contract_violation=true`。
- 该 gate 会在缺少 `progress_observability`、public counters 回退、Stage 1 计数回退、result-view lifecycle 计数回退、或出现 raw delta-only serving / materialized-ahead-of-fetched / denominator drift / 错误 Public Web wording 时失败。
- `/progress.counters.result_count` 必须表示用户可见 serving board count。asset-population / reuse-snapshot case 不允许在 final 后回退到 ranked top-k 行数；后端应优先使用 canonical `job_result_lifecycle.served_candidate_count` / result-view serving count，再退回 ranked count。
- Canonical OpenAI Agent、OpenAI ChatGPT、Lovable full-roster matrices 默认启用该 gate；新增 heavy matrix 不应只输出这些异常而不失败。
- 对 provider/recovery 可靠性场景设置 `require_no_service_recovery_violation=true`。它会失败于 stale local apply backlog、retry/stale `local_apply_closure`、retry/stale `snapshot_full_materialization`、search-seed discovery provider-owner 缺失、completed search-seed worker 缺 `search_seed_discovery_query` / `local_apply_closure` owner、ready retry、stale-provider、exhausted-without-report 问题、ready/stale `provider_search_retry`。
- `service_metrics.recovery_phase_metrics.recovery_tick_budget_exhausted` 必须有 next-tick 因果证据。clean 条件是同一 report 暴露 `next_tick_requested=true`、没有 missing/failed/slow/unexpected phase、没有 legacy bridge；此时 metrics 必须暴露 `cooperative_budget_yield_count>0`、`budget_yield_contract=cooperative_recovery_budget_yield`、`budget_yield_manual_handoff_blocking=false`，Pre-Manual Signoff 记录 passed gate `recovery_cooperative_budget_yield`。如果缺少 next-tick proof 或 recovery contract dirty，则 `recovery_tick_budget_exhausted` 保持 known warning/attention，不能被隐藏。
- `service_metrics.recovery_phase_metrics.durable_work_handoff_yield` 是 recovery owner 的协作式调度证据，不是默认 warning。clean 条件是同一 report 没有 missing/failed/slow/unexpected phase、没有 legacy bridge、没有 tick-budget exhaustion；此时 metrics 必须暴露 `cooperative_handoff_yield_count>0`、`handoff_yield_contract=cooperative_scheduling_yield`、`handoff_yield_manual_handoff_blocking=false`，Pre-Manual Signoff 记录 passed gate `recovery_cooperative_handoff_yield`。如果 recovery contract dirty，handoff yield 只保留为诊断字段，真正阻断或 warning 应来自 dirty owner 信号本身，不能把 handoff yield 当成独立可接受 warning 来隐藏 recovery 问题。
- 对 baseline+delta board streaming 场景设置 `require_no_board_visible_projection_violation=true`。它会失败于 visible delta count 没有 serving projection/patch log、patch replay lag、non-contiguous sequence、metadata replay dependency、或 fetched-to-board-visible lag。
- Pre-Manual Signoff 会阻断 `service_metrics.finalization_overlay.eligible_full_rewrite_present=true`。这表示 final/direct asset-population publication 在 canonical board-visible projection 已完整时仍执行了 full overlay rewrite，会重新引入 O(full board) finalization tail。正确路径应报告 `asset_population_overlay.reuse=true` / `service_metrics.finalization_overlay.reuse_used=true`；如果 reuse proof 不完整，可以 full rewrite，但必须在 metrics 中显式说明不 eligible。
- 对用户交互时延场景按环境设置 per-case SLO：`max_job_to_stage_1_preview_ms`、`max_final_results_to_board_nonempty_ms`、`max_job_to_board_nonempty_ms`、`max_job_to_board_visible_partial_ms`、`max_global_next_worker_start_gap_ms`。这些 gate 是 fail-closed：一旦配置了 SLO，`service_metrics` 中必须存在对应子指标，不能通过缺失报告被当成 `0` 来绕过。本地 fast/scripted 可严格；hosted/live 可先记录基线后再收紧。
- `max_stage_1_preview_to_final_results_ms` 是内部 finalization lag / optimization SLO，不是交给用户手动测试的 board-visible handoff gate。`manual_handoff_ready` 必须锚定到 board-visible projection、first/full cards visible、post-profile SLO、provider isolation 和 report completeness。该指标在 `workflow_stage_summaries.stage_1_preview/stage_2_final` 可用时使用 backend stage summary lag；`job_to_final_results` 保留 smoke 观察到的总 wall-clock。不要把轮询、补拉 runtime details、provider profile actor wait、或 report 观察滞后解释成后端 finalization 慢。`post_preview_finalization.preview_to_finalization_completed_ms` 和 `service_metrics.user_experience.raw_stage_1_preview_to_final_results_ms` 保留原始诊断值；`long_post_preview_finalization`、`long_finalization_after_preview`、和 bottleneck 判断必须使用 `finalization_lag_evaluation_ms`。当 `profile_wait_excluded_from_finalization_gate_ms>0` 且 `finalization_start_gate_ms` 已观测时，evaluation 必须锚定 `profile_terminal_at -> first_finalization_started_at`，不能再把合法 scripted/live provider wait 降级成 acceptable warning。`profile_terminal_at` 的正常来源是 succeeded `workflow_commands(command_type='linkedin.profile_url_terminal.record')`，即 `profile_terminal_source=profile_url_terminal_record_command_completed_at`；remote worker terminal timestamp 只是缺少 typed command evidence 时的 migration/historical fallback。如果 case 明确配置了更宽的 `max_stage_1_preview_to_final_results_ms`（例如 large-late shard 压力场景），超过默认 30s 但未超过 case SLO 应进入 `known_acceptable_warnings`。如果超过 case SLO，应进入 optimization/manual-review finding，而不是隐藏的 blocking handoff gate；只有显式性能验收场景才应把它升级为阻断。
- `post_preview_finalization.materialize_sync_duration_ms` 是兼容性的全 scope durable item lifecycle 摘要，可能包含 candidate-source closure 的 deferred/provider-open 等待。分析 profile 合入效率时优先看 `profile_batch_local_apply_duration_ms` 和 `materialize_sync_duration_by_scope_ms.profile_batch_local_apply`；如果长尾来自 `candidate_source_closure_lifecycle_ms`，必须检查 per-sync `duration_includes_deferred_wait` / `deferred_sync_reason`，不要把它误判为 profile local apply CPU/IO 慢。
- UX SLO 使用 smoke runner 观测到的用户可见 milestone，不使用 backend stage timestamp 覆盖它。backend timestamp 可以说明服务端处理很快，但如果 `/progress` 或候选人看板晚几秒才观察到状态，SLO 必须按用户可见时间失败或记录。
- Smoke runner 可以在轮询阶段使用轻量 `/results?include_runtime_details=0`，但最终 report/signoff 前必须使用含 runtime details 的 results payload。公共 results API 的 runtime details 必须是 bounded diagnostics：`runtime_details_contract.schema_version=public_results_runtime_details_v1`，compact dashboard job payload，compact events，compact Agent runtime session/trace/worker records，compact workflow stage summaries。`workflow_stage_summaries.stage_1_preview/stage_2_final` 的 wall-clock anchors 缺失时，runner 要补拉 `/results?include_runtime_details=1&include_candidates=0`；signoff 仍保持 fail-closed，不能把缺失 `job_to_stage_1_preview` / `stage_1_preview_to_final_results` 当作通过，也不能通过重新暴露 raw job summary/runtime blobs 绕过 report completeness。
- 本地 `configs/scripted/*smoke_matrix.json` 不允许只声明 terminal/recovery gate：每个 case 必须声明 board-nonempty UX SLO，provider-backed case 必须声明 worker handoff SLO，要求 `require_post_preview_finalization_observed=true` 的 case 必须声明 preview-to-final SLO。
- 本地服务级矩阵覆盖边界由 `configs/scripted/service_gate_coverage_manifest.json` 管理。每个 `*smoke_matrix.json` case 必须声明 `coverage_tags`；manifest 中 `required_now` 标签必须有本地矩阵覆盖，并且覆盖 case 必须声明 manifest 要求的 SLO / guardrail。尚未实现的历史事故形态不能从文档里消失，应在 manifest 中标为 `required_before_ecs_sync` 并写明 `current_gap`、`promotion_gate`、`owner_next_step`。
- `expectations` 不是开放字典。所有 matrix case 的 expectation key、以及 manifest 里的 `required_*_expectations`，必须先登记在 `src/sourcing_agent/smoke_expectation_contract.py`。`workflow_smoke.load_smoke_cases(...)`、runtime expectation evaluation、和 `scripts/check_service_gate_coverage.py` 都会对未知 key fail closed，避免拼写错误或已删除 expectation 被当成已覆盖的服务级 gate。
- 快速检查当前覆盖边界：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json
PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync
```

第二条命令在当前阶段应该失败；它是 ECS 同步前的升级门禁，用来确认剩余 planned tags 是否已经晋升为真实 scripted/browser hard gate。当前剩余 planned gaps 是 Meta Agent full-reuse browser timeline 和 partial delta row streaming；OpenAI Infra/Health/Whisper、Google Gemini incomplete shard、Google Veo/Nano authoritative reuse、late webhook、writer contention、tiny tail 已进入本地 scripted hard gate。
截至 2026-05-11，本地 service-gate manifest 已经收口到 `29/29` required tags 覆盖；`--require-before-ecs-sync` 不应再因 planned tags 失败。后续如果新增历史事故形态，必须先加入 manifest，再通过本地 scripted/browser hard gate 晋升为 `required_now`。
- 对 webhook/watcher 场景设置 `max_remote_provider_event_lag_ms`。`service_metrics.remote_provider_events` 会报告 `provider_webhook` / `local_provider_event_watcher` 来源、`received` / `received_late` / `received_in_flight` 状态、late duplicate 数量、全量 `remote_to_local_event_lag_ms`、可触发 recovery 的 `actionable_remote_to_local_event_lag_ms`、以及 `late_duplicate_remote_to_local_event_lag_ms`。late duplicate 只代表幂等/延迟观测，不应作为 recovery backlog 或 actionable wakeup SLO 失败；真正需要失败的是超过环境 SLO 的 actionable terminal event wakeup lag。该指标只在 provider-backed case 中要求存在；full-local-reuse / no-provider case 可以配置同一矩阵默认值但不因缺少 remote event 报告单独失败。
- 本地 `configs/scripted/*smoke_matrix.json` 默认必须带 `max_remote_provider_event_lag_ms`，当前基线为 `30000`。如果某个 hosted/live 环境暂时需要更宽松阈值，应该在对应环境矩阵中显式记录原因，而不是删除这个 SLO。
- 对 profile-scraper 乱序覆盖场景设置 `min_out_of_order_profile_completion_count`。`service_metrics.worker_timeline.out_of_order_completion.profile_batch_inversion_count` 会把 later-started-but-earlier-finished 的 profile batch 计数出来；如果一个矩阵标了 out-of-order 但没有观察到 inversion，就说明 fixture / scheduler 退化成了顺序完成，应该失败而不是默默通过。时间源优先级是 `remote_provider_event`、scripted-only `scripted_remote_ready_epoch_ms`、最后才是 worker terminal marker；同一秒提交的 worker 用 `worker_id` 作为稳定提交顺序，避免 SQLite 秒级时间戳隐藏真实的远端完成倒序。
- Scripted Apify webhook fixtures must preserve actor-completion semantics: `eventData.finishedAt` represents the remote actor completion time, not the local time when the smoke runner posts the webhook. For scripted remote-wait workers, the driver derives `finishedAt` from `checkpoint.scripted_remote_ready_epoch_ms`; otherwise late webhook delivery can overwrite true remote completion ordering and make an out-of-order fixture look ordered.
- 对 batch-efficiency / actor-slot 利用场景设置 `min_profile_batch_envelope_count`、`min_profile_prefetch_batch_plan_count`、`require_no_profile_scheduler_contract_violation=true`、`max_profile_unexplained_tiny_batch_count`、`max_provider_slot_underuse_with_backlog_count`。它们来自 `provider_case_report.event_level_efficiency.profile_batch_envelopes` 和 `profile_scheduler_contract`，并且在配置后要求 event-level efficiency report 必须存在。当前 workflow hard gate 的业务语义是“不出现无理由 tiny live batch、不在有 normal backlog 时空置 actor slot、scheduler contract 无违规”；真正的 tiny-tail coalescing 细节由单元/效率测试覆盖，除非某个 fixture 专门制造近 ready tiny chunks，否则不要在主流程矩阵强制 `min_profile_tiny_batch_coalesced_count`。
- Large-late-shard 场景必须用独立 PG-backed scripted matrix 覆盖，而不能只依赖单元契约。`configs/scripted/openai_agent_large_late_shard_smoke_matrix.json` 使用 `force_fresh_run=true` 的 OpenAI no-baseline scoped search，制造 90 人 early shard 后再到达 600 人 late shard；gate 必须同时设置 `min_profile_batch_size_max` 和 `max_profile_batch_size_max`，证明 late ready set 没有回退到 50-url fallback，也没有被无限放大。1600-URL late shard 仍由单元契约覆盖，用于验证纯 scheduler 公式而不把 service smoke 变成大规模物化压测。
- Google large-baseline pressure now has two distinct real-asset service shapes. `configs/scripted/google_vision_language_large_baseline_shard_smoke_matrix.json` covers `large_baseline_large_shard` with a 5000-candidate seeded baseline plus 2384 real profile deltas. `configs/scripted/google_gemini_large_baseline_small_former_real_asset_smoke_matrix.json` covers `large_baseline_small_former_shard` with an 8000+ seeded baseline excluding Gemini plus a normal Gemini scoped request (`帮我找Google在Gemini组的人`), where the provider-quality shape intentionally returns a true-zero current lane and a 120-profile real former Gemini shard. The small-former-shard case expects one `120`-URL durable profile envelope and at least one remote actor worker; it must not require multiple agent workers because the current/former profile searches are synchronous provider calls in this workflow shape, not worker-backed envelopes. `51..HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS` ready sets are no longer split into 50-sized chunks. Both fixtures must use `sample_fallback_generated=false`; if the real raw-profile sample is missing, the run should fail rather than generate placeholder profiles. A former-only scoped request is still a valid product path, but it must prove exact former shard coverage or run delta acquisition; it must not be modeled by this pressure fixture.
- 对高容量 profile-tail 场景设置 `min_remote_actor_slot_occupancy_ratio`。该 gate 使用 smoke 轮询期间观测到的 `waiting_remote_harvest_count` 峰值除以本 case 的 `harvest_profile_actor_global_inflight` 预算，来自 `provider_case_report.remote_actor_slot_observation.remote_actor_slot_peak_occupancy_ratio`；不要用终态 worker 列表里的瞬时 provider slot 数替代它，因为 worker 完成/清理后会自然归零。小公司、低量 delta、final tail、retry isolation 场景不应强制该 gate，除非 fixture 明确声称有足够 backlog 可以吃满 actor 额度。
- Tiny-tail / slot-utilization gate must also prove provider handoff latency, not only final batch sizes. The current PG-backed strict references are `output/profile_contract_pg_20260509/rerun70_openai_tiny_tail_report.json`, `output/profile_contract_pg_20260509/rerun74_openai_scoped_delta_report.json`, and `output/profile_contract_pg_20260509/rerun76_openai_no_baseline_scoped_search_report.json`: all passed with `expectation_failures=[]`, `local_completion_to_next_submit_start_ms.max=0ms`, clean `profile_scheduler_contract`, and no terminal queue leaks. `rerun74` held `remote_to_next_submit_start_ms.max=8181ms` and `next_submit_provider_attempt_elapsed_ms.max=1121ms`; `rerun76` held `remote_to_next_submit_start_ms.max=10528ms` and `next_submit_provider_attempt_elapsed_ms.max=2171ms`. `remote_to_next_submit_start_ms` is an end-to-end diagnostic/soft SLO until the hard threshold in `event_level_efficiency.thresholds_ms.remote_to_next_submit_start_hard`; when it regresses, inspect `remote_to_next_submit_segments_ms` first so remote event lag, callback marker lag, and provider submit latency are not conflated.
- No-baseline scoped-search must remain a separate scripted service gate even though it shares the same scheduler/post-profile/board-runtime implementation as baseline+delta. Use `configs/scripted/openai_no_baseline_scoped_search_smoke_matrix.json` to prove a fresh scoped-live search has `expect_explain_effective_acquisition_mode=scoped_live_search`, `expect_latest_lifecycle_baseline_candidate_count=0`, provider-backed search/profile coverage, post-profile SLOs, profile-scheduler contract, and Pre-Manual Signoff. This catches hidden assumptions that a baseline projection always exists without creating a separate workflow path.
- Nightly long-latency scripted coverage lives in `configs/scripted/nightly_long_latency_smoke_matrix.json`. It is not the default manual-handoff matrix; it is the PG-backed pressure gate for OpenAI baseline+delta, OpenAI no-baseline scoped-search, Lovable live-roster, Google large-baseline/large-shard, and Google 8k+ baseline/small-former-shard under long scripted provider waits. The cases intentionally allow wide provider wall-clock SLOs but still require provider-mode isolation, profile scheduler contract, post-profile SLO, bounded recovery phases, `max_recovery_total_elapsed_ms`, `max_recovery_tick_budget_exhausted_count`, profile-aware `max_stage1_terminal_to_finalization_start_ms`, and board-runtime parity. The `max_recovery_tick_budget_exhausted_count` expectation name is retained for compatibility, but it now gates budget-yield attention count (`raw recovery_tick_budget_exhausted_count - cooperative_budget_yield_count`), not clean cooperative budget yields. Long-latency cases must declare `max_poll_seconds` when their scripted provider-wave lower bound exceeds the CLI default; otherwise a valid pressure run can be misclassified as a workflow timeout before the final actor wave can complete. For provider-backed profile work, the finalization-start gate must use `profile_terminal_at -> first_finalization_started_at` when profiles are still in remote wait after Stage 1 terminal proof; raw `linkedin_stage_1.completed_at -> first_finalization_started_at` remains diagnostic evidence, not the blocking SLO.
- When a nightly case needs `require_post_preview_finalization_observed` and the same run can still leave `snapshot_full_materialization` queued as a tail artifact, opt into `drain_background_snapshot_full_materialization_post_terminal=true` so the post-terminal settle loop can actually consume that tail before the final report is evaluated. Do not treat this as a default behavior for manual handoff matrices; it is an explicit pressure-gate opt-in for long-latency cases that want finalization evidence from the same run. If the queue is healthy (`snapshot.compaction.run` queued/running, no retry/stale/terminal-failed state), Pre-Manual Signoff records `background_maintenance_snapshot_compaction` as a passed gate rather than a known warning. If the queue has retry/stale/terminal-failed state, signoff must fail closed.

Nightly long-latency driver:

```bash
SOURCING_EXTERNAL_PROVIDER_MODE=scripted \
SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1 \
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/nightly_long_latency_$(date +%Y%m%d_%H%M%S) \
  --seed-reference-runtime \
  --provider-mode scripted \
  --matrix-file configs/scripted/nightly_long_latency_smoke_matrix.json \
  --fast-runtime \
  --live-model-planning \
  --runtime-tuning-profile fast_smoke \
  --poll-seconds 0.2 \
  --max-poll-seconds 3600 \
  --strict \
  --timing-summary \
  --report-json output/nightly_long_latency_current/report.json \
  --summary-json output/nightly_long_latency_current/summary.json
```

Run Pre-Manual Signoff on the same output even though this is a nightly pressure suite; signoff should classify pressure/optimization findings instead of letting missing reports or provider-mode leaks pass:

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/review_scripted_smoke_run.py \
  --report-json output/nightly_long_latency_current/report.json \
  --summary-json output/nightly_long_latency_current/summary.json \
  --output-json output/nightly_long_latency_current/signoff.json \
  --output-md output/nightly_long_latency_current/signoff.md
```
- 对 full-local-reuse / `reuse_snapshot_only` 场景设置 `max_progress_payload_bytes` 和 `require_no_active_stage1_for_full_local_reuse=true`。它们分别防止 Meta-style 巨型 `/progress` payload 和旧 source-snapshot Stage 1 文案污染执行过程页。OpenAI Reasoning reuse-snapshot fixture 当前使用 `max_progress_payload_bytes=50000`；公共 `/progress` 应输出 allowlisted counters/status/timing 和 list counts（例如 `deferred_url_count`），不应暴露 raw URL lists、baseline-selection proof trees、confidence-policy payloads、`summary_path`。

### 1.3.1 Service-grade scripted metrics

Delta streaming / event-level workflow 优化必须先看结构化指标，再改 orchestration。默认不要用真实 Harvest/API 调用来找瓶颈；先用 isolated runtime + scripted provider fixture 复现真实形态，再看 smoke report。

每个 hosted/scripted smoke case 的 `provider_case_report.service_metrics` 现在包含三层信号：

- `worker_timeline`: worker 数量、trace span 数量、lane/status 分布、worker duration、每个 worker 的 start/end、到下一个 worker start 的 global gap、同 lane gap、profile-batch out-of-order completion inversion、slow workers、slow gaps。
- `user_experience`: first progress observed、Stage 1 preview、final results、board ready/non-empty、dashboard/candidate-page fetch、是否需要 loading feedback、是否违反 board readiness 阈值、preview 后 finalization 是否过长。
- `search_seed_discovery_queue`: search-seed discovery durable item 状态、ready retry、provider-owned stale、completed worker 缺 discovery/local-apply owner、exhausted-without-report。这是防止 worker-summary scan 重新进入正常路径的 hard gate。
- `remote_provider_events`: provider webhook / local watcher 的 terminal event 来源、late duplicate、in-flight duplicate、target worker count、全量 `remote_to_local_event_lag_ms`、可触发 recovery 的 `actionable_remote_to_local_event_lag_ms`、late duplicate 审计延迟。该报告用于区分“事件来晚但幂等”与“actionable 事件唤醒超过 SLO”。
- `post_profile_completion`: provider terminal 后的 URL terminal-state leak、event-level drain callback elapsed、`profile_file_visible -> board_patch_visible`、`all_profiles_fetched -> all_cards_visible`。这些指标直接覆盖前端手动测试曾暴露的 fetched/card/materialization 跳变与滞后。
- `bottlenecks`: 把 slow worker、worker handoff gap、board readiness lag、post-preview finalization lag 转成可操作的 optimization recommendation。

`summary.provider_case_report.service_metrics` 会跨 case 汇总：

- worker count / trace span count / worker duration
- global next-worker-start gap
- profile-batch out-of-order completion inversion count
- slow worker count / slow gap count
- final results 到 board ready/non-empty、job 到 board non-empty、Stage 1 preview 到 final results
- remote provider event count / late duplicate count / terminal-event lag
- loading-feedback、board-readiness、long-finalization violation case count
- bottleneck kind/severity 分布

使用口径：

- 先看 `worker_timeline.slow_gaps`：如果 provider 已返回但下一个 worker 没有及时启动，优先查 writer lock、sync/materialize 是否挡在 next-submit 前。
- 再看 `user_experience.final_results_to_board_nonempty_ms` 与 `job_to_board_nonempty_ms`：如果用户看不到可交互候选人，不要只优化 provider；先拆 result-view lifecycle / baseline-serving board。
- 再看 `bottlenecks.top_bottlenecks`：它是 AI-in-loop 优化的入口，不是硬 gate。优化前后应保存 report/summary，对比同一 scripted scenario 的 bottleneck 是否减少。
- 真实 API live smoke 只作为 scripted/isolated/browser E2E 通过后的用户手动 gate；默认开发循环不应为了量测这些指标打真实 provider。

可选 live LLM planning 模式：

- 默认 scripted smoke 仍然不打真实 LLM，保证 CI / 快速回归 deterministic。
- 如果要更贴近真实用户入口，可以在 `--provider-mode scripted` 下显式增加 `--live-model-planning`。这会设置 `SOURCING_SCRIPTED_LIVE_MODEL_PLANNING=1`，让 request normalization、review instruction、intent brief、search strategy 这些前置规划调用使用配置好的 live model。
- 因为 isolated runtime 会使用隔离 secrets file，CLI 会只把当前配置里的模型相关 env（`DASHSCOPE_*` / `MODEL_PROVIDER_*`）转发进隔离 runtime；不会把 provider mode 切到 live。
- 该模式不会把 Harvest、DataForSEO、profile scraper、semantic provider 变成 live；外部数据 provider 仍由 scripted scenario 接管。
- 该模式也不会启用 outreach AI verification / public-web asset analysis / profile membership AI 判断，避免一个 smoke case 因候选人数量放大成高成本模型回归。

可选 slow/strict provider timing：

- 默认 scripted/browser 开发循环仍使用短 sleep cap，保证手动测试和 CI 不被 60-180s provider wait 拖慢。
- 若要验证 worker scheduler、daemon recovery、writer lock、result-view lifecycle 在真实耗时级别下是否稳定，使用 slow/strict mode：

```bash
SCRIPTED_SLOW_STRICT_RUNTIME=1 make test-env-scripted-openai-agent
```

- `configs/scripted/openai_agent_scoped_delta_streaming.json` 的 OpenAI Agent profile-scraper worker timing matrix 现在覆盖：
  - varied batch durations: `45s / 60s / 90s / 120s / 180s`
  - out-of-order completion: later current-lane batch can finish before earlier batch
  - retryable provider delay on a mid-tail current batch
  - retryable timeout on a former long-tail primary-path batch

OpenAI Agent scoped-delta driver 示例：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/openai_agent_scoped_delta_streaming_foundation \
  --seed-reference-runtime \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/openai_agent_scoped_delta_streaming.json \
  --matrix-file configs/scripted/openai_agent_scoped_delta_smoke_matrix.json \
  --case openai_agent_scoped_delta_streaming \
  --fast-runtime \
  --live-model-planning \
  --runtime-tuning-profile fast_smoke \
  --poll-seconds 0.1 \
  --max-poll-seconds 120 \
  --strict \
  --timing-summary \
  --report-json output/scripted_smoke_current/openai_agent_scoped_delta_streaming_report.json \
  --summary-json output/scripted_smoke_current/openai_agent_scoped_delta_streaming_summary.json
```

OpenAI ChatGPT live-smoke reproduction driver 示例：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/openai_chatgpt_scoped_delta_streaming_foundation \
  --seed-reference-runtime \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/openai_chatgpt_scoped_delta_streaming.json \
  --matrix-file configs/scripted/openai_chatgpt_scoped_delta_smoke_matrix.json \
  --case openai_chatgpt_scoped_delta_streaming \
  --fast-runtime \
  --runtime-tuning-profile fast_smoke \
  --poll-seconds 0.1 \
  --max-poll-seconds 120 \
  --strict \
  --timing-summary \
  --report-json output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_report.json \
  --summary-json output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_summary.json
```

Google Gemini incomplete-shard no-full-reuse driver 示例：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/google_gemini_incomplete_shard_20260503 \
  --seed-reference-runtime \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/google_gemini_incomplete_shard_streaming.json \
  --matrix-file configs/scripted/google_gemini_incomplete_shard_smoke_matrix.json \
  --case google_gemini_incomplete_shard_no_full_reuse \
  --fast-runtime \
  --runtime-tuning-profile fast_smoke \
  --poll-seconds 0.1 \
  --max-poll-seconds 180 \
  --strict \
  --timing-summary \
  --report-json output/scripted_smoke_current/google_gemini_incomplete_shard_report.json \
  --summary-json output/scripted_smoke_current/google_gemini_incomplete_shard_summary.json
```

这个 driver 的核心断言：

- Google authoritative baseline 可以作为 `baseline_snapshot_id`，但不能把 scoped Gemini request 改写为 full local reuse。
- explain 必须是 `dispatch_strategy=delta_from_snapshot`、`planner_mode=delta_from_snapshot`、`requires_delta_acquisition=true`、`effective_acquisition_mode=baseline_reuse_with_delta`、`baseline_directional_local_reuse_eligible=false`。
- Stage 1 业务计数必须来自同一 coherent projection：`48 current / 18 former / 66 deduped / 66 profile fetched`。
- 最终 lifecycle 必须服务 baseline+delta/current snapshot：baseline `300`、served `366`、delta materialized/board-visible `66`，不得出现 raw delta-only result view 或 public count regression。
- Reference seed 要保持真实可服务：Google baseline registry count、`population_coverage` proof、`candidate_documents.json` 实际行数必须一致。不要再用“registry 声称数千人但 candidate docs 只有几行”的 fixture，它会制造 false service failures。

Google Veo/Nano authoritative shard-reuse driver 示例：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/google_veo_nano_authoritative_shard_20260503 \
  --seed-reference-runtime \
  --provider-mode simulate \
  --matrix-file configs/scripted/google_veo_nano_authoritative_shard_reuse_smoke_matrix.json \
  --case google_veo_nano_authoritative_shard_reuse \
  --fast-runtime \
  --runtime-tuning-profile fast_smoke \
  --poll-seconds 0.1 \
  --max-poll-seconds 120 \
  --strict \
  --timing-summary \
  --report-json output/scripted_smoke_current/google_veo_nano_authoritative_shard_report.json \
  --summary-json output/scripted_smoke_current/google_veo_nano_authoritative_shard_summary.json
```

这个 driver 的核心断言：

- explain 必须是 `dispatch_strategy=reuse_snapshot`、`planner_mode=reuse_snapshot_only`、`requires_delta_acquisition=false`、`effective_acquisition_mode=full_local_asset_reuse`。
- provider 调度必须为零：`max_provider_invocation_count=0`。reuse-only gate 不能只看最终结果，还必须证明调度器没有误发 Harvest/Apify work。
- Reference seed 必须同时沉淀 literal shard proof (`Veo` / `Nano Banana`) 和 planner canonical coverage key (`Multimodal`)。如果只种 literal terms，planner 会合理地继续要求 canonical `Multimodal` delta。
- 进度页不能显示 active LinkedIn Stage 1 或 delta profile progress：latest lifecycle 应是 `baseline_serving`，`delta_profile_progress_applicable=false`。
- 最近通过的 run：`provider_invocation_count=0`，max `/progress` payload `8961` bytes，`job_to_board_nonempty_ms≈2862`，`final_results_to_board_nonempty_ms≈168`，`post_terminal_recovery.settled=true`。

OpenAI ChatGPT driver 的 strict expectation contract 不是“job completed 即通过”。它必须同时满足：

- `allow_results_ready_nonterminal=false`，不能用早期 `results_ready_nonterminal` 代替 terminal job。
- `require_post_terminal_recovery_settled=true`，terminal job 后的 recoverable profile-tail/background workers 必须 drain 到 0。
- `expect_explain_dispatch_strategy=delta_from_snapshot`、`expect_explain_planner_mode=delta_from_snapshot`、`expect_explain_requires_delta_acquisition=true`、`expect_explain_effective_acquisition_mode=baseline_reuse_with_delta`。这条门禁防止 ChatGPT fixture 假阳性地跑成 generic `new_job` 或 reuse-only 路径。
- `max_repeated_materialize_signature_count=0` / `max_same_worker_reconcile_repeat_count=0`，同一 worker set 的 completed reconcile/materialize 只能被 coalesce，不能重复进入用户可见进度。
- 快速 preflight 必须覆盖 completed reconcile single-flight：空 `lease_token` 必须生成唯一 acquire attempt，预算 1 的同 job/kind slot 在 active lease 存在时必须返回 coalesced skip；wrapper 可以进入 `waiting_prerequisite`/`retry_wait`，但不能写 legacy `failed_retryable` backlog。
- provider long-tail 必须被实际驱动：`harvest_profile_search>=4`、`harvest_profile_scraper_batch>=8`、`profile_url_total_count>=150`、`fetched_profile_count>=150`、`board_total_candidates>=500`、`remote_actor_worker_count>=4`。
- post-preview finalization 必须被观察到：`require_post_preview_finalization_observed=true`，并要求至少一次 `materialize_completed`。不要过拟合“必须两次 materialize”；当同一轮 harvest-prefetch finalization 合并了 snapshot materialization 时，正确路径可能只有一次最终 artifact sync。
- hard guardrails 必须为绿：无 duplicate provider dispatch、无 default-off public-web Stage 1 fallback、无 event-level efficiency violation。
- Stage prerequisite gaps 继续保留为 diagnostic metrics；worker handoff gap 和 preview 后 finalization 已进入本地 scripted SLO gate，不能再只作为人工观察项。materialization micro-lag 仍可先作为 Delta asset board streaming 优化信号，除非对应 matrix 显式设置 hard SLO。

最近通过的 ChatGPT scoped-delta run：

- `output/scripted_smoke_current/openai_chatgpt_scoped_delta_streaming_report_current.json`
- explain contract: `dispatch_strategy=delta_from_snapshot`、`planner_mode=delta_from_snapshot`、`requires_delta_acquisition=true`、`effective_acquisition_mode=baseline_reuse_with_delta`
- provider/slot shape: `provider_invocations=14`、`harvest_profile_search=4`、`harvest_profile_scraper_batch=10`、`queued_worker_count=4`、`waiting_remote_harvest_count=4`、`active_worker_count=4`
- workflow result: `profile_url_total_count=529`、`fetched_profile_count=529`、`board_total_candidates=529`、`repeated_materialize_signature_count=0`、`same_worker_reconcile_repeat_count=0`
- 说明：配置中的 `live_reference` 记录的是 2026-04-29 真实 OpenAI ChatGPT live smoke 的 `890 -> 1061` 形状；isolated reference runtime 当前用较小 OpenAI baseline 加大 delta 复现调度/物化压力，避免测试依赖本地真实生产资产。

最近通过的 ChatGPT browser/user-interaction heavy run：

- command: `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 SOURCING_HEAVY_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=30 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'openai_chatgpt_delta_streaming_contract_observation'`
- report: `output/playwright/frontend-browser-e2e-openai-chatgpt-delta-streaming-report.json`
- latest job: `fe1e378634d7`
- provider invocation shape: `harvest_profile_search=4`、`harvest_profile_scraper_batch=12`、`total=16`
- observed progress max: `166 current / 100 former / 250 deduped`，profile progress `250/250`
- hard browser guardrails: `baselineFirstBoardObserved=true`、`finalCurrentSnapshotObserved=true`、`contractReady=true`、`snapshotDivergenceDetected=false`、`progressRegressionDetected=false`、`materializationStuckAfterFetchDetected=false`
- provider-slot shape: max queued profile workers `4`, max webhook-eligible profile workers `4`
- materialization/repoint tail: after profile fetch reached `250/250`, baseline-serving lasted about `9.6s` before current snapshot served `550/550`; this is below the `30s` stuck budget and remains a diagnostic optimization signal.

Lovable 100+ live-roster browser/user-interaction heavy run：

- command: `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 SOURCING_HEAVY_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=30 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'lovable_live_roster_streaming_contract_observation'`
- report: `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`
- latest job: `27aa35de1f5d`
- provider invocation shape: `harvest_company_employees=1`、`harvest_profile_search=2`、`harvest_profile_scraper_batch=7`、`total=10`
- observed progress max: `120 current / 25 former / 140 deduped`，profile progress `140/140`
- hard browser guardrails: `finalCurrentSnapshotObserved=true`、`snapshotDivergenceDetected=false`、`progressRegressionDetected=false`、`materializationStuckAfterFetchDetected=false`

Fixture realism rule:

- OpenAI ChatGPT and Lovable heavy fixtures default to real captured sample files under `configs/scripted/samples/` (`sample_fallback_generated=false`) for profile-search/company-employees/profile-scraper payloads. Generated bodies are fallback-only scaffolding for missing explicit fixture paths and should not be used for these heavy browser/manual cases.
- For profile/company actor timing, heavy fixtures use `execute_sleep_position=remote_wait`: the submit returns a pending checkpoint with `run_id/dataset_id` immediately, then the browser scripted webhook driver waits until `scripted_remote_ready_epoch_ms` before posting `/api/providers/apify/webhook`. This models remote actor wait and avoids misclassifying scripted sleep as submit-chain latency.
- Matrix cases that set a truthy `SOURCING_SEED_*` runtime env are self-describing seeded scenarios. `scripts/run_simulate_smoke_matrix.py --runtime-dir ...` now automatically enables reference runtime seeding for those groups, so a pressure fixture cannot silently run as `new_job` merely because the operator forgot `--seed-reference-runtime`. Passing `--seed-reference-runtime` remains valid for broad reference matrices.
- OpenAI Agent baseline+delta service gate must be seeded with `--seed-reference-runtime` and must not carry legacy `force_fresh_run=true`; the matrix now fails explicitly unless explain reports `delta_from_snapshot / baseline_reuse_with_delta`. Latest passing report: `output/scripted_smoke_current/openai_agent_scoped_delta_service_gate_seeded_20260503c_report.json` with lifecycle `300 baseline + 297 delta = 597 served`, `patch_log_count=5`, and `profile_batch_inversion_count=3`.

最近通过的 foundation run：

- `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v7b_report.json`
- `output/scripted_smoke_current/openai_agent_scoped_delta_streaming_strict_tail_v7b_summary.json`
- 关键结果：`provider_invocations=17`、`profile_url_total_count=597`、`fetched_profile_count=597`、`board_total_candidates=597`、`post_terminal_recovery.settled=true`、`preview_to_finalization_completed_ms≈58s`、`materialize_sync_duration_ms.max≈21s`。

OpenAI Agent browser/user-interaction driver：

```bash
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 \
SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 \
SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 \
PYTHONPATH=src ./.venv-tests/bin/pytest \
  tests/test_frontend_browser_e2e.py -q \
  -k 'openai_agent_delta_streaming_contract_observation'
```

输出报告：

- `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`
- `output/playwright/frontend-browser-e2e-openai_agent_scoped_delta_streaming.png`

当前诊断口径：

- 默认模式要求测试跑通并记录 lifecycle/progress/UI 观测，不再要求必须复现旧缺口。
- 2026-04-29 hard mode 已通过；`SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1` 是 Delta asset board streaming 的用户交互级回归门禁。
- 运行任何 live webhook smoke 前先执行 `scripts/apify_webhook_preflight.py`；scripted/browser 环境可用 `--mode scripted` 明确确认“不需要外部 Apify callback URL”的边界。
- 这个 browser driver 现在默认使用测试侧 webhook-event driver：它在 isolated hosted runtime 中向 `/api/providers/apify/webhook` 发送 Apify-shaped terminal event，验证 quick-ack `remote_provider_event -> background recovery -> next worker submit`，而不是直接调用 `/api/workers/daemon/run-once`。不要在压力/浏览器 smoke 中使用 `?sync=1`；它只是低并发调试入口，会把完整 recovery/materialization 绑进 webhook response 并制造不真实的客户端 timeout/lag。
- 旧 `--drive-worker-recovery` 仍可用于排障下游 recovery/reconcile，但不能用来证明 webhook/event-level discovery 延迟；它会绕过 provider webhook endpoint。
- webhook-event driver 不调用真实 Harvest/DataForSEO provider，也不验证外部 Apify 网络 callback roundtrip。真实 webhook 仍需要单独 tunnel/live smoke。
- hard mode 会断言：
  - baseline board 在 current snapshot materialization 之前可见
  - 最终 current snapshot repoint/merge 被观测到
  - API/UI progress counters 不回退，result view / dashboard / lifecycle snapshot 不分叉
  - execution phase 不会在 LinkedIn acquisition、local materialization、post-result layering 期间误显示 `Public Web Stage 2`
  - 执行过程页面出现结构化 Stage 1 指标，例如 `新发现在职候选人`、`需补取 LinkedIn Profile`
  - 候选人看板出现 `候选人同步 display_ready/expected`，并分开展示当前意图匹配人数、LinkedIn Profile 取回进度、卡片详情合入进度；基础名单行只能作为 preview/audit，不应计入主同步
  - 后端已经暴露 delta expected/fetch pending 时，候选人同步卡不会超过 2.5s 仍显示 `baseline/baseline`
  - baseline overlay 和 final current snapshot 的第一页预览名称保持稳定
  - LinkedIn profile progress 在 UI/API 中从非 0 推进到 fetched/required 完成

最近通过的 hard-mode browser run：

- report: `output/playwright/frontend-browser-e2e-openai-agent-delta-streaming-report.json`
- `contractReady=true`、`baselineFirstBoardObserved=true`、`finalCurrentSnapshotObserved=true`
- latest job: `557df2315ab8`
  - `providerWebhookDriven=true`、`workerRecoveryDriven=false`、`providerWebhookEvents=4`
  - hard-mode browser assertions now also require `deltaStreaming.providerWebhookSummary.recoveryCount > 0`, `failedEventCount=0`, and `maxWebhookToResponseMs<=30000`; raw `providerWebhookEvents` remains diagnostic detail only.
- first board sample: baseline `20260414T120300`，visible board `300/300`
- partial candidate sync observed before the card-readiness split: `候选人同步300/425新增 LinkedIn Profile 已取回 100/125，已物化到看板 0/125`; current expected wording is split into profile fetch plus `卡片详情已合入看板 display_ready/required`
- final candidate sync observed before the card-readiness split: `候选人同步597/597新增 LinkedIn Profile 已取回 297/297，已物化到看板 297/297`; current expected wording is `候选人同步597/597` plus separate fetch/materialization notes
- provider invocation shape: `harvest_profile_search=4`、`harvest_profile_scraper_batch=12`、`total=16`

同一轮还通过：

- OpenAI ChatGPT baseline+delta: `output/playwright/frontend-browser-e2e-openai-chatgpt-delta-streaming-report.json`, job `3e936f18d309`, `harvest_profile_search=4`, `harvest_profile_scraper_batch=10`, no stale-sync/progress/snapshot/materialization guardrail failures.
- Lovable 100+ live roster: `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`, job `3ca5152d880e`, `harvest_company_employees=1`, `harvest_profile_search=1`, `harvest_profile_scraper_batch=7`, final sync `145/145`, no guardrail failures.

当前三条 heavy browser gate 命令：

```bash
SOURCING_RUN_FRONTEND_BROWSER_E2E=1 \
SOURCING_RUN_DELTA_STREAMING_BROWSER_E2E=1 \
SOURCING_EXPECT_DELTA_STREAMING_BROWSER_E2E=1 \
SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 \
PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q \
  -k "openai_agent_delta_streaming_contract_observation or openai_chatgpt_delta_streaming_contract_observation or lovable_live_roster_streaming_contract_observation"
```

最近通过的 slow/strict browser observation：

- report: `output/playwright/openai-agent-slow-strict-browser-report.json`
- isolated runtime: `runtime/test_env/openai_agent_delta_streaming_slow_strict`
- job: `4f0f5f991133`
- coverage: `harvest_profile_search=4`、`harvest_profile_scraper_batch=13`、`total=17`
- board lifecycle: baseline `20260414T120300` first served at `300/300`; final current snapshot `20260429T134659` observed with expected `597`
- tail state: profile batch workers drained, `recoverable_worker_count=0`; full post-terminal tail finalized about `37m41s` after job creation
- boundary: this is not an external Apify/Harvest webhook network roundtrip. It validates local state-machine behavior after scripted provider completion, including daemon/recovery/reconcile scheduling. Real webhook coverage requires the separate opt-in live/tunnel smoke.
- metrics interpretation: `local_completion_to_next_submit_start_ms` is the pure profile-completion refill handoff once completion apply starts; discovery/roster/search-seed `profile_prefetch_phase_b_group` dispatches count as profile-submit coverage but do not satisfy this handoff SLO. `next_submit_provider_attempt_elapsed_ms` can include scripted provider sleep/pending and discovery append submit work, so it should not be used as the pure scheduler handoff metric. The old names `local_to_next_submit_start_ms` and `next_submit_attempt_elapsed_ms` remain compatibility aliases in existing reports.

用户可交互 scripted 环境：

```bash
make test-env-scripted-openai-agent
```

这个入口会 seed OpenAI Agent baseline，并使用 `configs/scripted/openai_agent_and_lovable_streaming.json` 同时加载 OpenAI Agent delta 和 Lovable live-roster scripted provider 规则；适合在同一个隔离 `scripted` backend + worker daemon + 前端里人工观察两类流程。它默认使用隔离 runtime 下的空 secrets file，不调用真实 Harvest/DataForSEO。详见 [TEST_ENVIRONMENT.md](./TEST_ENVIRONMENT.md#交互式-openai-agent--lovable-scripted-环境)。

注意：这个用户交互入口默认不是 fast E2E 档位。它使用 `SCRIPTED_FAST_RUNTIME=0` 和 `SOURCING_SCRIPTED_HARVEST_SLEEP_SECONDS_CAP=8`，让 profile-search / profile-scraper pending rounds 在执行过程页面可见。它还会默认 reset 自己的 `runtime/test_env/openai_agent_delta_streaming`，避免旧 history/job/artifact 干扰本次观察。自动化 browser E2E 如需快速收敛，应显式设置 `SCRIPTED_FAST_RUNTIME=1` 或继续使用 pytest harness 的 fast runtime 注入。

Lovable 100+ small-company full-roster scripted gate：

```bash
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --runtime-dir runtime/test_env/lovable_live_roster_scripted \
  --provider-mode scripted \
  --scripted-scenario configs/scripted/lovable_live_roster.json \
  --matrix-file configs/scripted/small_company_live_roster_smoke_matrix.json \
  --case lovable_small_company_live_roster \
  --fast-runtime \
  --strict \
  --timing-summary \
  --report-json output/scripted_smoke_current/lovable_live_roster_scripted_report.json \
  --summary-json output/scripted_smoke_current/lovable_live_roster_scripted_summary.json \
  --max-poll-seconds 240
```

这个 gate 替代早先 3 人 Physical Intelligence smoke，用于覆盖更接近真实小/中公司 live roster 的行为：

- `harvest_company_employees` scripted roster 返回 `120` 个唯一 Lovable current employees。
- `harvest_profile_search` 返回 `25` 个 former Lovable rows，避免 full-roster former lane broad search 因 0-result retry 被误判为重复 dispatch。
- `harvest_profile_scraper_batch` 使用请求 URL 生成 profile details，覆盖后续 profile/materialization path。
- expectation 要求 terminal job、无重复 provider dispatch、无 default-off Public Web/DataForSEO stage、无 event-level efficiency violation、`board_total_candidates>=100`。

最近通过的 Lovable run：

- report: `output/scripted_smoke_current/lovable_live_roster_scripted_report.json`
- summary: `output/scripted_smoke_current/lovable_live_roster_scripted_summary.json`
- job: `7933a0ff59cf`
- `expectation_failures=[]`
- provider invocations: `10` total；`harvest_company_employees=1`、`harvest_profile_search=1`、`harvest_profile_scraper_batch=6`
- board: `145` total candidates, first page `24`, profile fetch progress `145/145`
- guardrails: duplicate provider dispatch `false`, default-off public web violation `false`, event-level efficiency violation `false`

注意：这个 fixture 的第一版故意暴露过一个测试资产缺陷：120 条 generated roster row 共享同一个 `fullName` 时会被 canonicalization 合并成 1 个候选人。因此该 fixture 现在有 `test_lovable_live_roster_scripted_fixture_replays_unique_100_plus_roster_sample` 锁住 100+ unique roster shape，并通过 `sample_body_path` replay 2026-04-26 Lovable live provider 样本，覆盖真实地区、经历、教育和低信息量 profile 分布。

2026-04-30 scripted fixture realism update:

- `sample_body_path` is supported for scripted Harvest rules. `harvest_profile_search` / `harvest_company_employees` samples are sliced by the request payload, while `harvest_profile_scraper_batch` samples are matched by requested URL, canonical profile URL, public identifier, and `originalQuery.url`.
- OpenAI ChatGPT replay uses the 2026-04-29 live-smoke sample: `166` current search rows, `100` former search rows, `250` deduped profile URLs, and a scraper pool that preserves real location/experience/education/richness where available.
- Lovable replay uses the 2026-04-26 live-roster sample: `120` roster rows, `25` former rows, and a scraper pool covering real Stockholm/US/Europe location variation and mixed profile richness.
- OpenAI Agent remains synthetic unless a large true-Agent live sample is captured; use the ChatGPT case when the goal is to observe real sampled profile data rather than Agent-specific query semantics.

最近通过的 Lovable browser/user-interaction heavy run：

- command: `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 SOURCING_RUN_HEAVY_SCRIPTED_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'lovable_live_roster'`
- report: `output/playwright/frontend-browser-e2e-lovable-live-roster-streaming-report.json`
- latest job: `16b3ebc2844d`
- provider invocation shape: `harvest_company_employees=1`、`harvest_profile_search=1`、`harvest_profile_scraper_batch=7`、`total=9`
- observed progress max: `120 current / 25 former / 145 deduped`，profile progress `145/145`
- final candidate sync before the card-readiness split: `候选人同步145/145本次 LinkedIn Profile 已取回 145/145，已物化到看板 145/145`; current expected wording splits fetch and `卡片详情已合入看板 145/145`
- hard browser guardrails: `snapshotDivergenceDetected=false`、`progressRegressionDetected=false`、`materializationStuckAfterFetchDetected=false`
- regression guard: the browser test now fails if live-roster samples never observe at least `100` current returned rows, preventing `harvest_company_employees` progress from silently falling back to `0`.

### 1.3.2 手工测试反哺出的缺口

之前 simulate / explain / E2E 已经不少，但仍没挡住后续手动测试暴露的问题，根因不是“没跑测试”，而是测试断言形状还不对。当前总结成 10 类：

- 只验证“这一轮能跑完”，没有验证“跑完后写回的持久化状态是否仍支持下一轮正确复用”。
- explain dry-run 主要看单次 planning 语义，没有覆盖 workflow 完成后 control-plane 被更新、再 explain 一次是否还正确。
- 很多 smoke fixture 使用全新临时 runtime；它们适合测编排正确性，但不容易暴露 `company_key` 漂移、registry duplicate、lane coverage 被污染这类“脏状态累积”问题。
- Browser E2E 是 opt-in，不在默认快回归矩阵里；因此 `history reload`、候选人看板恢复、前端时间展示等问题更容易留到人工测试才发现。
- 这轮已把“候选人看板第 N 页在后台补候选人时不能跳页”补进 browser E2E，而且同时覆盖：
  - workflow 入口
  - restored history 入口
- 之前缺少针对“本地资产复用 query 的时间预算”守卫，导致 workflow 即使最终 completed，也可能在 `stage_1_preview -> final results` 之间多跑了不该跑的 normalize/materialize 路径。
- PG-only 迁移期新增了更多“写入后再读取”的风险面；如果测试只断言 API 输出，不检查落盘后的 registry/profile/job/history 关系，就会漏掉这类问题。
- 之前没有显式断言 “Final Results 已显示” 到 “候选人看板首屏真正可交互” 之间的延迟，因此结果页可能已经完成、但用户还要再等几秒甚至几十秒。
- 当前 browser 断言对分页稳定性的判定口径是：
  - 第 2 页不能回到第 1 页
  - 当前页 preview 顺序不能因为后台 hydration 被改写
  - 如果本机太快、在首次可见结果前就已经 `loaded == total`，测试接受“已全量稳定态”，但不接受半加载错态
- 候选人卡片上的关键动作入口没有浏览器级断言，像 `打开 LinkedIn` 这类回归会等到人工点卡片时才暴露。
- 时间线只校验结构，不校验展示值的合理范围；像 `saved_at` 污染导致的 `9491s` 这类显示错误，如果不读真实 UI 文案就抓不到。
- 导出/下载类语义此前主要靠人工点，像“导出 zip 文件名必须使用中国时间”这种需求，需要至少一条前端或浏览器级断言兜底。

后续默认要求：

- 任何 reuse / delta planning 改动，都至少补一条 “run once -> persisted -> explain again” 回归。
- 任何 history / results 页面相关改动，都至少补一条 “带 `history_id` 跑完 -> API 恢复 -> 结果仍可加载” 回归。
- PG writer / canonicalization 改动，必须同时断言：
  - `company_key` 不漂移
  - alias/special-char 公司名不会生成重复 authoritative row
  - lane coverage 不会掉回 `0/0`
- 本地资产复用 query 需要独立 timing budget，不和 large-org live/scoped delta 混在同一口径里看。
- Browser E2E 至少要额外断言三件事：
  - 结果页首屏存在候选人可操作入口，而不是只有静态文本
  - 如果 `Final Results -> candidate board` 超过 2s，必须出现明确 loading feedback
  - `Final Results` 的时间线 duration 必须落在合理预算内，不能被 `saved_at` 等持久化字段污染
- 导出/下载链路的时间语义、文件命名和历史恢复语义，不能只靠人工 spot check；后续至少要各有一条自动化断言覆盖。
- fixture 里凡是涉及公司目录 / snapshot path 的地方，也必须使用生产同级的 alias-aware canonicalization。
  - 例如 `Humans&` 不能只用 `normalize_company_key -> humans` 这种简化逻辑；否则 smoke 可能把资产种到错误目录，既造假失败，也掩盖真实回归。

### 1.4 Browser E2E

用途：

- 真实验证前端 `Search -> plan -> review -> workflow -> results -> manual review`
- 覆盖前端 history reload / 后端恢复链路，而不是只测 API 返回
- 把前端状态切换、后端 hosted simulate 路径、浏览器交互放到同一条回归里

标准入口：

```bash
cd "sourcing-ai-agent/frontend-demo"
npm run browser:e2e -- \
  --frontend-url http://127.0.0.1:4173 \
  --query "我想了解Humans&里偏Coding agents方向的研究成员"
```

自动化回归：

- [tests/test_frontend_browser_e2e.py](../tests/test_frontend_browser_e2e.py)
  - 启一个临时 hosted simulate backend
  - seed reference org assets
  - 再起本地 Vite
  - 用 Playwright 跑真实浏览器主链路
  - 额外验证结果页 reload 后仍能从后端恢复
  - 默认 fast suite：
    - `Humans& Coding agents`
      - 代表小组织 `full reuse`
    - `Anthropic Pre-training`
      - 代表中型组织 `baseline reuse`
    - `xAI all members (existing baseline)`
      - 代表大组织 `full-roster query -> asset_population default`
    - `Physical Intelligence all members`
      - 代表新组织 `runtime identity + new_job + full roster`
  - slow suite：
    - `OpenAI Pre-train`
      - 代表大组织 `scoped delta`
    - `Google multimodal + Pre-train`
      - 代表大组织 `partial shard covered + delta`
    - `xAI all members -> second identical run`
      - 代表大组织 `profile tail completed -> reuse_completed`

标准回归命令：

```bash
cd "sourcing-ai-agent"
make test-browser-e2e-fast
make test-browser-e2e-slow
make test-browser-e2e-full
```

等价前端目录入口：

```bash
cd "sourcing-ai-agent/frontend-demo"
npm run browser:matrix:fast
npm run browser:matrix:slow
npm run browser:matrix:full
```

说明：

- 默认 `PYTHONPATH=src python3 -m unittest tests.test_frontend_browser_e2e -v`
  - 只跑 fast suite
  - slow suite 会显示为 skip
- `SOURCING_RUN_SLOW_BROWSER_E2E=1 PYTHONPATH=src python3 -m unittest tests.test_frontend_browser_e2e -v`
  - 跑 fast + slow，两者合起来就是完整 browser matrix
  - slow suite 会把每条大组织 case 放到独立 harness / frontend session 中执行，优先保证稳定性而不是复用同一轮浏览器状态
- `SOURCING_RUN_FULL_BROWSER_E2E_MATRIX=1`
  - 也可作为兼容别名启用 slow suite

适用场景：

- 前端 Step/tab 切换改动
- Plan review / confirm 交互改动
- 搜索页 history 恢复改动
- 结果页 / 人工审核页联动改动
- 本地联调入口或 API contract 改动

### 1.5 Hosted Scripted Long-Tail Smoke

用途：

- 低成本模拟大组织 provider 长尾
- 复现 `pending -> ready -> fetch -> retryable timeout` 这类真实长耗时外部行为
- 压 recovery、阶段切换、progress 轮询与自动收敛语义

标准入口：

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=scripted \
SOURCING_SCRIPTED_PROVIDER_SCENARIO=configs/scripted/google_multimodal_long_tail.json \
PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py \
  --case google_multimodal_pretrain \
  --strict
```

自动化回归：

- [tests/test_hosted_workflow_smoke.py](../tests/test_hosted_workflow_smoke.py)
  - `test_hosted_scripted_google_long_tail_timeout_completes_without_manual_takeover`
  - `test_hosted_scripted_large_org_full_roster_overflow_completes_without_live_provider`
  - `test_hosted_scripted_large_org_profile_tail_reconciles_after_background_prefetch`
  - `test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch`

适用场景：

- worker recovery / runtime watchdog / progress takeover 改动
- remote-search / remote-harvest checkpoint resume 改动
- staged acquiring/retrieving completion 判定改动
- full-roster 之后的 background profile-prefetch / reconcile 改动
- profile tail 已完成后不应重复触发 profile scraper 的幂等性回归

现在 hosted smoke record 里也会带：

- `job_summary.background_reconcile`
- `final.background_reconcile`

这样可以直接区分：

- 只是 roster overflow / shard recovery
- 还是已经进入 profile-completion tail，并在 worker recovery 后完成 reconcile
- 还是同请求第二次进入 `reuse_completed`，直接复用已完成 job 而不再重复抓取 profile tail

### 1.6 Live Provider Validation

用途：

- 验证真实召回质量、provider 参数、成本与时延
- 验证 simulate/scripted 无法覆盖的真实 API 差异

原则：

- 只在 simulate / scripted / replay 通过后再做
- 优先小公司或明确增量 query
- 大组织 live run 应优先走已有 baseline + 必要缺口 delta
- 默认不做周期性自动执行；只做手动显式触发

显式入口：

```bash
cd "sourcing-ai-agent"
make test-env-backend-live LIVE_CONFIRM=1
make test-live-large-org-manual LIVE_CONFIRM=1
```

底层脚本：

- [scripts/run_live_large_org_regression.py](../scripts/run_live_large_org_regression.py)

当前 manual live 大组织回归默认覆盖：

- `xai_full_roster`
  - 验证 `full_company_roster` 边界与 shard strategy
- `xai_coding_all_members_scoped`
  - 验证“带方向词但仍说全部成员”不会误判成 full roster

注意：

- 这层不属于 CI / 快回归
- 它的通过标准不是“所有 case 都在短时间 completed”
- 是否需要补“剩余 LinkedIn profile tail”，也只应由 live run 决定
  - scripted 负责证明 worker recovery / background reconcile / artifact rebuild 没问题
  - live 才能说明真实 provider 还剩多少 tail、为什么会剩下，以及是否值得继续付费 backfill
- 更看重：
  - explain / dispatch 边界是否正确
  - acquisition 是否进入预期阶段
  - real provider 参数与成本是否符合预期

CRM Public Web live validation has an additional cache contract:

- `person_public_web_signals.signal_id` is a stable person/value identity. It must not include `run_id`, provider result order, batch id, force-refresh nonce, or any other execution artifact identifier.
- Detail and export consumers must first match manual promotion/rejection by exact `signal_id`, then by the same stable signal identity. A force-refresh that rediscovers the same GitHub/Scholar/email value must preserve the user's manual decision.
- Command-owned export artifacts are cacheable only within the current `CRM_PUBLIC_WEB_EXPORT_CONTRACT_VERSION`. Any change to signal identity, default export surface, promotion overlay, or artifact schema must update that version and include it in the export command idempotency key, payload, result, and manifest.
- A default `promoted_only` export returning zero after the detail endpoint shows promoted current signals is a fast contract failure. Fix it in unit/API preflight first; do not spend another live-provider run to rediscover the drift.

## 1.7 常用用户链路的最低自动化要求

下面这些链路是后续最不应该只靠人工记忆去回归的：

- `Search -> plan -> approve -> results -> 刷新 history URL -> 候选人看板恢复`
- `同一 query 完成后立刻再搜一次`
  - 特别是 `Reflection AI`、`OpenAI Reasoning` 这类应该直接复用 snapshot 的 query
- `公司名 alias / 特殊字符 / 空格变体`
  - `Reflection AI / reflectionai`
  - `Google DeepMind`
  - `Humans&`
  - `Periodic Labs`
- `asset_population default` 查询
  - 允许最终默认结果来自全量资产，而不是只看 ranked results 列表
- `manual review / supplement / target candidates` 写回后，历史恢复与 registry 检索仍一致
- `轻请求 + 重请求并发`
  - 本地已有大组织 workflow 跑着时，小组织本地复用 query 不应被整体阻塞

其中前 4 类现在都应该进入默认 automated matrix；后 2 类若暂时无法稳定自动化，至少要保留明确的 smoke checklist 和 performance probe。

额外实践说明：

- `hosted_workflow_smoke` 这类会起后台线程、临时 server、隔离 runtime/SQLite shadow 的套件，在 macOS 上做 durations/profile 时，优先单独起一个 pytest 进程。
- 不建议把它和 `results_api / frontend_history / asset_paths / supplement` 等其它高信号套件硬拼到同一个长命令里跑 durations。
  - 这样虽然表面更省命令数，但更容易在 teardown 阶段引入线程/SQLite shadow 噪声，污染真实的慢测与失败信号。

## 2. 外部 Provider 模式

`SOURCING_EXTERNAL_PROVIDER_MODE` 目前支持：

- `live`
  - 真实调用外部 provider
- `replay`
  - 只复用缓存与本地资产，不发新请求
- `simulate`
  - 返回 workflow 可消费的模拟 provider 结果
- `scripted`
  - 按 scenario 文件精确模拟 pending/ready/fetch/error/timeout

注意：

- `replay` 是 cache-only 模式；缓存未命中时保持空结果，不再生成 `_offline` 占位成员
- `simulate/replay/scripted` 只替代高成本外部 provider
- Postgres control plane、snapshot 落盘、hosted API、progress、results、stage summaries、recovery 仍是真实执行路径

轮询降速相关环境变量现在都是 runtime-configurable，不再在模块导入时冻结。回归/CI 若要尽量快，可显式设置：

- `WEB_SEARCH_READY_COOLDOWN_SECONDS=0`
- `WEB_SEARCH_FETCH_COOLDOWN_SECONDS=0`
- `WEB_SEARCH_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `WEB_SEARCH_FETCH_MIN_INTERVAL_SECONDS=0`
- `SEED_DISCOVERY_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `SEED_DISCOVERY_FETCH_MIN_INTERVAL_SECONDS=0`
- `EXPLORATION_READY_POLL_MIN_INTERVAL_SECONDS=0`
- `EXPLORATION_FETCH_MIN_INTERVAL_SECONDS=0`

补充：

- in-process `unittest` hosted smoke 已内置这组快速 cooldown
- 若你测的是外部常驻 `serve`，现在有两种方式：
  - 在 server / daemon 进程侧配置这些环境变量
  - 或者在请求里显式带 `execution_preferences.runtime_tuning_profile=fast_smoke`
- `fast_smoke` 现在也会缩短 Harvest 的 probe poll、dataset retry backoff、scripted sleep

对已有 hosted server 做低成本快速 smoke 时，推荐优先用 job-scoped profile，而不是为了测试去改整台服务的进程环境。

例如：

```bash
cd "sourcing-ai-agent"
PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py \
  --base-url http://127.0.0.1:8765 \
  --runtime-tuning-profile fast_smoke \
  --strict
```

## 3. Explain Dry-Run Matrix

默认 case catalog 存放于：

- [src/sourcing_agent/workflow_explain_matrix.py](../src/sourcing_agent/workflow_explain_matrix.py)

示例矩阵文件：

- [configs/explain_dry_run_matrix.example.json](../configs/explain_dry_run_matrix.example.json)

一个 explain case 可以定义：

- `payload`
  - 实际发送给 `/api/workflows/explain` 的请求
- `expect`
  - 结构化期望断言，例如：
    - `target_company`
    - `keywords`
    - `dispatch_strategy`
    - `planner_mode`
    - `current_lane`
    - `former_lane`
    - `*_contains`

补充说明：

- `reuse_completed` 不适合放进默认 explain dry-run catalog，因为它依赖“运行环境里已经存在一条已完成的同请求 job”。
- 这类语义目前通过两层固定回归覆盖：
  - hosted scripted：`test_hosted_scripted_large_org_profile_tail_completed_snapshot_skips_repeat_prefetch`
  - browser E2E slow：`test_browser_workflow_e2e_large_org_profile_tail_second_run_reuses_completed_job`
- 两层都会直接检查：
  - `dispatch_strategy = reuse_completed`
  - `dispatch_matched_job_status = completed`
  - second run 不再重复触发 profile tail

推荐做法：

- 对 runtime 依赖强的组织，优先写成 hosted explain matrix case，而不是只留在人肉 dry-run
- 期望里优先断言语义，不要把不稳定的 count/tick 当主断言
- 需要 partial coverage 场景时，优先断言 `covered_*` / `missing_*`，不要只断 `requires_delta_acquisition=true`

## 4. Scripted Scenario Catalog

scenario 文件存放于：

- [configs/scripted/](../configs/scripted/)

当前样例：

- [configs/scripted/reflection_pending.json](../configs/scripted/reflection_pending.json)
  - 小组织 pending smoke
- [configs/scripted/google_multimodal_long_tail.json](../configs/scripted/google_multimodal_long_tail.json)
  - 大组织长尾 pending + retryable timeout smoke
- [configs/scripted/large_org_full_roster_overflow.json](../configs/scripted/large_org_full_roster_overflow.json)
  - 通用大组织 full-roster overflow / shard recovery 行为夹具
- [configs/scripted/large_org_profile_tail_reconcile.json](../configs/scripted/large_org_profile_tail_reconcile.json)
  - 通用大组织 full-roster 后的 background profile-prefetch tail / reconcile 行为夹具

说明：

- 这些 large-org fixture 的行为是通用的，当前只用 xAI 作为具体 driver query / sample payload
- 回归要验证的是 orchestration 模式，而不是“xAI 这个公司名”本身

一个 scenario 可定义：

- `search.rules/default`
  - `poll_pending_rounds`
  - `results`
  - `errors`
  - `artifacts`
- `harvest.rules/default`
  - `execute_pending_rounds`
  - `errors`
  - `body`
  - `artifacts`

推荐约定：

- 用 `default` 描述该场景的主行为
- 需要多 query 区分时再加 `rules`
- `kind=retryable` 用来模拟 timeout / 429 / IncompleteRead 这类应该自动恢复的错误
- 长尾场景优先用 `pending_rounds`，不要只靠 sleep 堆时长
- 若要验证 baseline 与 shard bundle 的真实覆盖关系，优先补 exact membership 场景，不要只看 candidate count

## 5. 如何新增 Smoke Case

### 5.1 加到默认矩阵

更新：

- [src/sourcing_agent/workflow_smoke.py](../src/sourcing_agent/workflow_smoke.py)
  - `DEFAULT_SMOKE_CASES`

要求：

- case 名稳定、可读
- query 代表一个真实产品场景
- 尽量覆盖新的组织规模、请求语义或资产复用路径

### 5.2 用自定义矩阵文件

可复制：

- [configs/simulate_smoke_matrix.example.json](../configs/simulate_smoke_matrix.example.json)

执行：

```bash
cd "sourcing-ai-agent"
SOURCING_EXTERNAL_PROVIDER_MODE=simulate PYTHONPATH=src python3 scripts/run_simulate_smoke_matrix.py \
  --matrix-file configs/simulate_smoke_matrix.example.json \
  --strict
```

## 6. 如何新增 Explain Dry-Run Case

1. 更新 [src/sourcing_agent/workflow_explain_matrix.py](../src/sourcing_agent/workflow_explain_matrix.py)
2. 尽量把期望写成语义断言，而不是 count 断言
3. 若它依赖特定 baseline / shard fixture，再补 hosted explain test fixture
4. 若它代表真实回归样本，再补一条 `unittest`

## 7. 如何新增 Scripted 场景

1. 在 [configs/scripted/](../configs/scripted/) 下新增一个 `.json`
2. 用最少规则表达你要压的链路
3. 若它代表长期回归样本，再补一个对应的 `unittest`

建议优先补这类场景：

- 大组织 current lane 长尾 pending
- former lane 多轮 ready/fetch
- Harvest retryable timeout / 429 / IncompleteRead
- acquisition 完成后 retrieval 自动切换
- stage 1 preview blocked 后 continue-stage2 自动续跑

## 6. 改动后的推荐回归顺序

### 6.1 普通后端改动

1. `python3 -m py_compile` 检查新文件
2. `PYTHONPATH=src python3 -m unittest discover -s tests -v`

### 6.2 改动 explain / planning / dispatch / hosted workflow

1. 单元测试
2. hosted simulate smoke
3. 必要时先看 explain/smoke 里的结构化 query / lane / delta 摘要，再决定是否需要 scripted 或 live

### 6.3 改动 recovery / staged transition / provider checkpoint

1. 单元测试
2. hosted simulate smoke
3. hosted scripted long-tail smoke
4. 如果涉及 `candidate_artifacts.py` / materialize / finalize 性能，额外跑一次：
   `python3 scripts/run_candidate_artifact_benchmark.py --candidate-count 320 --dirty-candidates 24 --repeat 3 --build-profile foreground_fast --env-file .local-postgres.env`
   如果系统 `python3` 过旧，脚本现在会直接报错并提示改用 `.venv-tests/bin/python`。
5. 重点看结构化报告里的：
   - `payload_build_total`
   - `state_upsert`
   - `state_upsert_candidate_count`
   - `generation_register`
   - `finalize_total`
   - `comparison.wall_ms.speedup_x`

### 6.4 计划上线前

1. 单元测试
2. hosted simulate smoke
3. hosted scripted long-tail smoke
4. 只在前 3 层都通过后，选 1 到 2 条 live workflow 做手动真实验证

## 7. 当前已知边界

- simulate/scripted 适合验证 orchestration correctness，不适合判断真实召回质量
- scripted 目前更擅长模拟 Harvest/Search 长尾，不等于完整真实互联网环境
- cold real-runtime explain/dispatch 在大资产环境下仍可能比 unit/integration test 慢，这类性能优化应单独追踪
- `scripts/run_candidate_artifact_benchmark.py` 当前是 artifact/materialize 级 benchmark，不是完整 provider-grade workflow benchmark
- 真实大 snapshot 的 cold full materialize 已有一条实测基线：
  - Google `20260423T040115`，`5897` candidates，隔离 runtime，`foreground_fast`
  - wall time 约 `14.4min`
  - `state_upsert` 仅 `~107ms`，`finalize_total` 仅 `~1.2s`
  - 当前真正的大头仍是 `prepare_candidates` / `payload_build_total` / `view_write_total`
- 如果后续要回答 `search returned count / roster returned count / fetched LinkedIn profile count / board ready` 这类问题，还需要继续补 workflow 级 case report

低优先级 explain 轻量化优化已记录在 [NEXT_TODO.md](NEXT_TODO.md)。
