# Runtime Environment Isolation

> Status: Active contract. Use this together with `TEST_ENVIRONMENT.md`, `ECS_PRELAUNCH_CHECKLIST.md`, and `PG_ONLY_CUTOVER_TRACKER.md` when changing runtime startup, provider cache, or control-plane wiring.

## Goal

Production, local dev, scripted smoke, replay, and test runs must not share mutable runtime state by accident.

The isolation boundary is no longer only `SOURCING_EXTERNAL_PROVIDER_MODE`. It is the pair:

- `SOURCING_RUNTIME_ENVIRONMENT`
- `SOURCING_EXTERNAL_PROVIDER_MODE`

## Provider Access Contract

Provider access is fail-closed at the runtime contract boundary and again at the paid/live HTTP boundary.

Rules:

- `live` is the only provider mode that may call paid/live external providers such as Harvest/Apify, DataForSEO, Serper, or live browser/search provider paths.
- `simulate`, `scripted`, and `replay` must set `SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1`.
- Non-live runtime builders must clear live provider secret env vars, including `APIFY_API_TOKEN`, `APIFY_TOKEN`, `APIFY_WEBHOOK_TOKEN`, `DATAFORSEO_LOGIN`, `DATAFORSEO_PASSWORD`, and `SERPER_API_KEY`.
- `load_settings()` must blank Harvest/DataForSEO/Serper credentials whenever `SOURCING_EXTERNAL_PROVIDER_MODE` is non-live, even if the process inherited a real `providers.local.json` or ambient shell secrets.
- Low-level provider HTTP helpers must call `assert_live_provider_access_allowed(...)` before submitting or polling live providers. This applies even when callers normally go through a higher-level scripted/simulate provider chain.
- Synthetic/scripted fixture inputs such as `openai-agent-current-*`, `openai-agent-former-*`, `lovable-roster-*`, and `scripted-*` must never be submitted to live providers, even if the current process is in `live` mode.

The root principle is: a test/scripted/simulate run should be unable to spend provider credits by construction, not merely because the current high-level code path happens to branch correctly.

## Runtime Namespaces

Supported runtime environments:

- `production`: ECS / hosted operator runtime.
- `local_dev`: default developer runtime under `runtime/`.
- `test`: generic isolated test runtime such as `runtime/test_env`.
- `simulate`: simulate-mode in-process smoke runtime.
- `scripted`: scripted provider scenario runtime.
- `replay`: replay/cache-only runtime.
- `ci`: future CI namespace.

If `SOURCING_RUNTIME_ENVIRONMENT` is not set, code infers a conservative value from provider mode and runtime dir.

If future workflow/Agent runtimes such as Temporal, LangGraph, or containerized test harnesses are introduced, they must carry the same namespace fields in their task queues, workflow ids, action ids, search attributes, metadata, or worker environment. Runtime ownership is a product safety contract, not an implementation detail of the current PG worker tables.

Durable work ownership is part of the namespace contract:

- Any daemon/recovery path that drains durable work must verify runtime ownership before it claims or executes the row/group.
- Runtime ownership is inferred from durable path fields such as `snapshot_dir`, `root_snapshot_dir`, `candidate_documents_path`, `discovery_dir`, `artifact_path`, `summary_path`, `raw_path`, and nested `artifact_paths`.
- If the inferred runtime dir differs from the current process `SOURCING_RUNTIME_DIR`, the work is skipped with `runtime_namespace_mismatch`.
- Cross-runtime skips must not claim the row, increment attempts, write `last_error`, or submit providers.
- The skip count must be observable as `runtime_namespace_skipped_count` or an equivalent per-queue field.

This applies to worker recovery, registry-backed profile refill, search-seed discovery items, local-apply closure items, board-visible apply items, and snapshot-full-materialization items. The root/local-dev daemon must never drain nested `runtime/test_env/<case>` rows, even if they accidentally land in the root PG schema.

## Provider Cache Contract

Shared Harvest provider cache is live-only.

Current path shape:

```text
<runtime_dir>/provider_cache/<runtime_environment>/live/<logical_provider_name>/<payload_hash>.json
```

Rules:

- `live` may read/write only its own environment namespace.
- `simulate`, `scripted`, and `replay` do not read or write shared Harvest provider cache.
- Legacy cache files under `<runtime_dir>/provider_cache/<logical_provider_name>/...` are not reused.
- Legacy `_offline` replay/simulate/scripted cache bodies are deleted if encountered during live lookup.
- `live_tests` bridge lookup is live-only and runtime-local.

This prevents replay/scripted fixture data from becoming a later live cache hit, and also prevents live production cache from being used as replay fixture state.

## Control Plane Contract

Production hosted runtime must be PG-only:

- `SOURCING_CONTROL_PLANE_POSTGRES_DSN` is required.
- `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only` is required.
- `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1` is required.
- Disk-backed live SQLite control-plane mode is retired.
- SQLite is allowed only as ephemeral compatibility shadow, not as live authority.

Test/scripted/replay runtime must not inherit repo-level Postgres by accident:

- `scripts/dev_backend.sh` sets `SOURCING_RUNTIME_ENVIRONMENT` before Postgres discovery.
- For `test`, `simulate`, `scripted`, `replay`, and `ci`, it writes/uses `<runtime_dir>/.isolated-local-postgres.env` unless a runtime-specific `SOURCING_LOCAL_POSTGRES_ENV_FILE` is explicitly provided.
- This sentinel blocks fallback to parent `.local-postgres.env`.
- To intentionally use a shared repo PG env in a test runtime, set `SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1`. This should be rare and reviewed.
- Workflow confidence runtimes are stricter than generic test runtimes: manual scripted browser, hosted smoke matrix, explain matrix with `--runtime-dir`, and reference smoke seeding must use a runtime-scoped PG-only env file and prepare a safe `sourcing_scripted*`, `sourcing_simulate*`, `sourcing_replay*`, or `sourcing_test*` schema before backend startup.
- Workflow confidence must fail closed when PG is missing or not connectable. It must not silently use disk SQLite or an in-process SQLite control plane, because scheduler locks, durable queue ownership, recovery, materialization, and board-visible SLOs need production-equivalent PG behavior.
- A live PG adapter freezes its resolved schema at construction time. Background provider callbacks, local long-poll watchers, and recovery sidecars may outlive the patched environment that created them; their later writes must keep using the runtime-owned schema and must not re-resolve to `public` after process environment restoration.

## Hosted Production Contract

`scripts/run_hosted_trial_backend.sh` now defaults to:

```text
SOURCING_RUNTIME_ENVIRONMENT=production
SOURCING_EXTERNAL_PROVIDER_MODE=live
SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only
SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1
```

Production non-live provider mode is rejected unless an explicit temporary override is set:

```text
SOURCING_ALLOW_PRODUCTION_NONLIVE_PROVIDER=1
```

`SOURCING_ALLOW_PRODUCTION_REPLAY=1` is also accepted for a narrow replay smoke override, but it must not be left as the hosted default.

## Test Runtime Contract

`build_isolated_runtime_env(...)` now pins all mutable state into the supplied runtime root:

- jobs
- company assets
- hot-cache assets
- object storage
- provider cache
- local DB/shadow state
- local-postgres env sentinel

It also sets:

```text
SOURCING_RUNTIME_ENVIRONMENT=simulate|scripted|replay
SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1
```

For non-live provider modes, it also clears live provider secret env vars and uses a local scripted webhook token. Live model planning is a separate explicit exception (`SOURCING_SCRIPTED_LIVE_MODEL_PLANNING=1`) and does not re-enable Harvest/Apify/DataForSEO/Serper.

Seeded/reference artifacts are part of the test runtime contract, not just fixture files. Durable `candidate_documents.json` readers must decode the stable `Candidate` core while tolerating additive serving/card fields and preserving unknown fields in metadata. A non-empty raw `candidates` array that decodes to zero candidates is a schema/contract failure; it must not be handled as a retryable provider or materialization outage.

For live test runtime, use `SOURCING_RUNTIME_ENVIRONMENT=test` with a dedicated runtime dir such as `runtime/test_env_live`.
Live access inside any isolated runtime is fail-closed unless both of these variables are present in the process environment:

```text
SOURCING_LIVE_PROVIDER_CONFIRM=1
SOURCING_ALLOW_ISOLATED_LIVE_PROVIDER_ACCESS=1
```

`SOURCING_EXTERNAL_PROVIDER_MODE=live` is not sufficient for `runtime/test_env/*`, `runtime/test_env_live`, scripted, simulate, replay, or CI namespaces. Child process launchers must call the shared provider isolation contract so detached daemons and sidecars cannot inherit ambient live provider secrets by accident.

Manual interactive scripted runtime is intentionally PG-backed even though its provider mode is non-live. The launcher `scripts/dev_scripted_openai_agent_delta.sh` creates a runtime-scoped PG env file and uses schema `sourcing_scripted_openai_agent_delta` by default. This keeps browser polling, worker daemon writes, provider event closure, materialization, and finalization on the same concurrency-capable storage path as local/live dev while still isolating scripted data from production/local-dev schemas.

Rules for this exception:

- It must use a dedicated `sourcing_scripted*` schema and may only reset schemas with a scripted/test/replay/simulate prefix.
- `--reset-runtime` resets both the file runtime and the PG schema.
- `--sqlite-control-plane` has been removed. Historical SQLite behavior can be covered only by targeted unit/migration tests, not by manual scripted workflow confidence.
- Scripted provider event watcher is enabled by the launcher so ready provider results close via event-level callback instead of waiting for `/progress` recovery.

## Review Checklist

When adding a script, service unit, provider cache, or new smoke harness, verify:

- It sets or infers the correct `SOURCING_RUNTIME_ENVIRONMENT`.
- It sets `SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1` for `simulate`, `scripted`, and `replay`.
- It does not inherit paid provider secrets in non-live modes.
- Any new paid/live provider submit, poll, dataset, or status helper calls `assert_live_provider_access_allowed(...)` before the network request.
- Apify/Harvest HTTP helpers must also guard at the lowest `api.apify.com` request boundary; higher-level guards are not enough because daemon/recovery paths can be re-entered from checkpoints.
- Non-live provider modes cannot read/write live shared provider cache.
- Test/scripted/replay runtime cannot inherit production or local-dev PG by accident.
- Root/local-dev recovery daemons must not process workers, refill groups, or snapshot paths whose inferred runtime namespace is nested under another isolated runtime such as `runtime/test_env/<case>`.
- Durable queue drains must enforce the same rule before claim, not after claim.
- Production cannot start in `replay`, `simulate`, or `scripted` without an explicit override.
- Cleanup instructions name the runtime namespace, not just the provider mode.

## Runtime Health

`GET /api/runtime/health` includes a `runtime_environment` block with:

- `runtime_environment`
- `provider_mode`
- `provider_cache_namespace`
- `runtime_dir`
- `live_provider_access_allowed`
- `live_provider_access_disabled`

Use this before manual testing or ECS rollout to confirm the process is in the expected namespace.

## Deployment Verification Rule

Before running live provider traffic on ECS or another hosted environment, verify both the API health endpoint and the CLI control-plane report:

```bash
curl -fsS http://127.0.0.1:8765/api/runtime/health
PYTHONPATH=src ./.venv/bin/python -m sourcing_agent.cli show-control-plane-runtime
```

Do not continue if:

- production reports `provider_mode` other than `live` without an explicit temporary smoke override
- production reports a non-PG live control plane
- `simulate`, `scripted`, or `replay` runtime is reading/writing the live provider cache namespace
- a legacy SQLite/emergency banner appears for hosted production
