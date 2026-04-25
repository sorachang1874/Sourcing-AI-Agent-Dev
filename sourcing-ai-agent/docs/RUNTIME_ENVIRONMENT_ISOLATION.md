# Runtime Environment Isolation

> Status: Active contract. Use this together with `TEST_ENVIRONMENT.md`, `ECS_PRELAUNCH_CHECKLIST.md`, and `PG_ONLY_CUTOVER_TRACKER.md` when changing runtime startup, provider cache, or control-plane wiring.

## Goal

Production, local dev, scripted smoke, replay, and test runs must not share mutable runtime state by accident.

The isolation boundary is no longer only `SOURCING_EXTERNAL_PROVIDER_MODE`. It is the pair:

- `SOURCING_RUNTIME_ENVIRONMENT`
- `SOURCING_EXTERNAL_PROVIDER_MODE`

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
- SQLite is allowed only as ephemeral compatibility shadow, not as live authority.

Test/scripted/replay runtime must not inherit repo-level Postgres by accident:

- `scripts/dev_backend.sh` sets `SOURCING_RUNTIME_ENVIRONMENT` before Postgres discovery.
- For `test`, `simulate`, `scripted`, `replay`, and `ci`, it writes/uses `<runtime_dir>/.isolated-local-postgres.env` unless a runtime-specific `SOURCING_LOCAL_POSTGRES_ENV_FILE` is explicitly provided.
- This sentinel blocks fallback to parent `.local-postgres.env`.
- To intentionally use a shared repo PG env in a test runtime, set `SOURCING_ALLOW_TEST_REPO_POSTGRES_ENV=1`. This should be rare and reviewed.

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
```

For live test runtime, use `SOURCING_RUNTIME_ENVIRONMENT=test` with a dedicated runtime dir such as `runtime/test_env_live`.

## Review Checklist

When adding a script, service unit, provider cache, or new smoke harness, verify:

- It sets or infers the correct `SOURCING_RUNTIME_ENVIRONMENT`.
- Non-live provider modes cannot read/write live shared provider cache.
- Test/scripted/replay runtime cannot inherit production or local-dev PG by accident.
- Production cannot start in `replay`, `simulate`, or `scripted` without an explicit override.
- Cleanup instructions name the runtime namespace, not just the provider mode.

## Runtime Health

`GET /api/runtime/health` includes a `runtime_environment` block with:

- `runtime_environment`
- `provider_mode`
- `provider_cache_namespace`
- `runtime_dir`

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
