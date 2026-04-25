# PG-Only Cutover Tracker

> Status: Living tracker. Use the latest entries as the source of truth, and assume older bullets may describe superseded intermediate states.


## Goal

Hosted / ECS runtime must treat Postgres as the only live authoritative control-plane store.

Allowed:
- ephemeral in-memory SQLite shadow used only as compatibility scaffolding inside `SQLiteStore`

Not allowed:
- disk-backed `runtime/sourcing_agent.db` or `runtime/control_plane.shadow.db` acting as live control-plane truth
- hosted backend silently starting without `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
- hosted runtime falling back to SQLite because PG env was not sourced

## Current Status

As of `2026-04-23`, the main remaining hosted leak identified in this round was:

- `scripts/run_hosted_trial_backend.sh` did not auto-source Postgres env the way `scripts/dev_backend.sh` already did
- if ECS/systemd forgot to inject `SOURCING_CONTROL_PLANE_POSTGRES_*`, hosted backend could still boot and drift onto disk-backed SQLite shadow

## Fixed In This Round

- `OpenAI / 20260423T165904`
  - synced snapshot assets from ECS back to local runtime
  - rebuilt `acquisition_shard_registry` from on-disk search-seed summaries
  - recovered 6 profile-search shard rows locally:
    - `current/former + Language Model`
    - `current/former + Vision`
    - `current/former + Multimodal`
  - explicit family metadata is now present, so:
    - `Language Model -> Text`
    - `Vision -> Vision`
    - `Multimodal -> Multimodal`

- `scripts/run_hosted_trial_backend.sh`
  - now sources `scripts/dev_postgres_env.sh`
  - now requires `psycopg`
  - now hard-fails unless `SOURCING_CONTROL_PLANE_POSTGRES_DSN` is resolved
  - now hard-fails unless live mode is `postgres_only`
  - now exports:
    - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
    - `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`

- `src/sourcing_agent/storage.py`
  - added startup guard for `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
  - refuses startup when:
    - no Postgres DSN is resolved
    - live mode is not `postgres_only`
    - SQLite shadow backend is disk-backed instead of `shared_memory`
  - production runtime now requires PG even if the startup script forgot `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
  - the only production SQLite escape hatch is explicit emergency mode:
    - `SOURCING_ALLOW_PRODUCTION_SQLITE_CONTROL_PLANE=1`
  - this keeps PG-only as a code-level runtime contract, not only a shell-script convention

- `src/sourcing_agent/service_daemon.py`
  - generated worker-daemon systemd unit now carries PG-only env when a DSN is present at render time
  - avoids daemon-side drift where API is PG-only but `run-worker-daemon-service` still boots without PG env

- `src/sourcing_agent/cli.py`
  - `show-control-plane-runtime` now emits `legacy_sqlite_banner`
  - non-`postgres_only` runtime is explicitly marked as legacy/emergency with migration exit instructions
  - `postgres_only + shared_memory` is marked as the expected live contract

## ECS Verification Completed

- Hosted ECS no longer runs the legacy SQLite-live systemd config.
- Actual ECS remediation completed on `2026-04-23`:
  - installed system Postgres on ECS
  - created local `sourcing_agent` database + `sourcing` user
  - rewrote remote `.local-postgres.env` to an ECS-valid DSN
  - synced live `runtime/control_plane.shadow.db` into Postgres with validation
  - replaced the old `sourcing-ai-agent.service` env with:
    - `SOURCING_CONTROL_PLANE_POSTGRES_DSN`
    - `SOURCING_CONTROL_PLANE_POSTGRES_LIVE_MODE=postgres_only`
    - `SOURCING_REQUIRE_CONTROL_PLANE_POSTGRES=1`
    - `SOURCING_PG_ONLY_SQLITE_BACKEND=shared_memory`
    - `SOURCING_EXTERNAL_PROVIDER_MODE=replay` at the time of the initial smoke; this has since been replaced by the runtime-isolated hosted contract where production defaults to `live`
    - `SOURCING_API_MAX_PARALLEL_REQUESTS=8`
    - `SOURCING_API_LIGHT_REQUEST_RESERVED=2`
- Confirmed on the live ECS process:
  - process env contains the PG-only variables above
  - `/api/health` reports the service healthy
  - standalone `show-control-plane-runtime` under the same env reports:
    - `control_plane_postgres_live_mode = postgres_only`
    - `sqlite_shadow_backend = shared_memory`
    - `sqlite_shadow_ephemeral = true`
- ECS also received the OpenAI `20260423T165904` family-aware shard backfill, so hosted planner/runtime now see:
  - `Language Model -> Text`
  - `Vision -> Vision`
  - `Multimodal -> Multimodal`
- Follow-up runtime isolation has since tightened hosted startup further:
  - hosted production sets `SOURCING_RUNTIME_ENVIRONMENT=production`
  - hosted production defaults to `SOURCING_EXTERNAL_PROVIDER_MODE=live`
  - production non-live provider mode is rejected unless an explicit temporary override is present
  - shared Harvest provider cache is namespaced under `provider_cache/<runtime_environment>/live/...`

## Follow-Up Items

- Validate the same `OpenAI / 20260423T165904` family-aware shard rows after next ECS deploy / sync cycle
- After hosted redeploy, verify the same guardrails on ECS with a real job launch
- The old disk SQLite files under `runtime/` are still retained as backup artifacts, but are no longer the live authoritative path.
- Continue shrinking legacy SQLite-specific export / restore paths so they remain backup-only tooling, not operational dependencies.
- Normalize remaining docs / runbooks that still show raw `python -m sourcing_agent.cli serve` examples without PG-only env context.
- Next code-level cleanup should add explicit confirmation prompts / flags to legacy SQLite export-restore commands so they cannot be mistaken for a live hosted path.
- If emergency SQLite mode is ever used in production, record it as an incident and migrate the resulting control-plane state back into PG before normal traffic resumes.
