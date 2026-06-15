# Runtime Preflight

> Status: Current canonical preflight entrypoint. Read this before launching local dev, scripted test environments, live smoke, or hosted/ECS workflows. Use it together with `DEVELOPMENT_GUIDE.md`, `TEST_ENVIRONMENT.md`, `APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`, and `ECS_PRELAUNCH_CHECKLIST.md`.

## Purpose

This file is the single first-stop checklist for runtime startup and live-provider readiness.

It exists because the same classes of operational mistakes have recurred:

- starting frontend/backend in a tool-owned foreground shell and losing the process when the tool session ends
- using a shell env that differs from the backend/worker process that actually submits provider actors
- mixing local dev, scripted test, local live smoke, and hosted production assumptions
- treating local watcher/recovery completion as proof that external Apify webhook delivery worked

## Always Start Here

Run from the repository root:

```bash
pwd
git status --short
make dev-doctor
```

When the session will use GitHub, `gh`, Codex, Claude Code, or a model backend
that routes through `chatgpt.com`, also run:

```bash
make agent-network-preflight
```

This preflight is intentionally read-only. It checks shell proxy env, git proxy
config, GitHub DNS fake-ip status, Clash/Mihomo `GLOBAL` selection, GitHub
HTTPS, `gh auth status`, `git ls-remote`, and `chatgpt.com` reachability. If it
fails, agents must report the failing invariant and ask for a user-approved
network action. They must not set `HTTP_PROXY`, write git proxy config, toggle
Clash mode/TUN/DNS/system proxy, change the selected Clash node, hot-reload or
restart Clash/Mihomo, or call controller mutation APIs such as `PUT /configs` as
an automatic workaround.

If GitHub or model-backend domains resolve to `198.18.x.x`, the correct report is
that fake-ip filtering needs user attention. If Clash `GLOBAL` is `DIRECT`, the
correct report is that the user should manually choose a non-DIRECT node. The
agent should not perform either action.

For tests, prefer:

```bash
make bootstrap-test-env
```

Rules:

- Use repository Python: `./.venv/bin/python` for runtime commands and `./.venv-tests/bin/python` / `./.venv-tests/bin/pytest` for tests.
- Prefer `bash ./scripts/...` or `make ...` targets over direct `./scripts/...` execution.
- Before debugging Postgres/DSN behavior, run `make dev-doctor` or `bash ./scripts/dev_backend.sh --print-config`.
- Before debugging GitHub/Codex/Claude Code transport, run `make agent-network-preflight` and treat it as the source of truth for allowed diagnostics.
- Do not assume a prior frontend/backend is still alive after context compaction or a new terminal/tool session. Check status.

## Choose The Runtime Mode

### Local Dev UI

Use this when testing the normal local app on:

- backend: `http://127.0.0.1:8765`
- frontend: `http://127.0.0.1:4173`
- runtime: `runtime`

For a process that should survive the current tool command/session, use detached launch targets:

```bash
make dev-stop
make dev-launch-backend
make dev-launch-frontend
make dev-status
```

Do not use raw `nohup ... &`, background `&`, or a foreground Vite/backend process when the user expects the service to remain available after the current tool call. Those processes can be cleaned up with the tool session.

Use foreground targets only when a human-owned terminal will remain open:

```bash
make dev-backend
make dev-frontend
```

Useful diagnostics:

```bash
make dev-status
make dev-logs
make dev-stop
```

### Interactive Scripted Environment

Use this for no-cost browser testing of OpenAI Agent scoped delta streaming and Lovable 100+ live roster behavior:

```bash
make test-env-scripted-openai-agent
```

Default endpoints:

- backend: `http://127.0.0.1:8785`
- frontend: `http://127.0.0.1:4185`
- provider mode: `scripted`
- runtime: `runtime/test_env/openai_agent_delta_streaming`

This path does not call real Harvest/DataForSEO providers. It uses scripted provider results and real local materialization/finalization.

If an AI agent launches this for user interaction and the frontend must survive across turns, run it in a durable terminal/session, for example:

```bash
screen -dmS sourcing-scripted-openai-agent bash -lc 'cd "/Users/changyuyi/projects/Sourcing AI Agent Dev/sourcing-ai-agent" && make test-env-scripted-openai-agent'
make test-env-scripted-openai-agent-status
```

Stop it with:

```bash
make test-env-scripted-openai-agent-stop
screen -S sourcing-scripted-openai-agent -X quit 2>/dev/null || true
```

Scripted preflight:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode scripted --json-only
```

Expected behavior: `status=ready`, and `external_apify_webhook_required=false`.

### Local Live Smoke

Use this only when the user explicitly wants to spend live provider budget from the local machine.

Before submitting any actor, ensure the Apify callback route is reachable from the backend/worker process that will submit Harvest actors.

The submit path now has a default live local-dev relay URL when `SOURCING_RUNTIME_ENVIRONMENT=local_dev` and provider mode is live:

```text
https://api.111874.xyz/local-dev/providers/apify/webhook
```

You can override it with `SOURCING_APIFY_WEBHOOK_URL`, `APIFY_WEBHOOK_URL`, or `SOURCING_LOCAL_DEV_APIFY_WEBHOOK_URL`.

For ECS local-dev relay:

```bash
./.venv/bin/python scripts/ecs_webhook_reverse_tunnel.py
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode local-live
```

Then restart backend/worker before submitting new runs if you changed any webhook env. A preflight shell variable does not retrofit an already-running backend, but the code-level default means local-dev live submits will still attach the local-dev relay URL when runtime env is `local_dev`.

Strict external-webhook one-profile smoke may temporarily disable local watcher fallback:

```bash
export SOURCING_LOCAL_PROVIDER_EVENT_WATCH_ENABLED=0
export SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=0
```

Do not leave strict-smoke watcher settings as the default for normal manual testing unless the goal is specifically to test external webhook delivery without fallback.

### Hosted / ECS Production

Hosted production callback:

```text
https://api.111874.xyz/api/providers/apify/webhook
```

Local-dev relay callback:

```text
https://api.111874.xyz/local-dev/providers/apify/webhook
```

These are not interchangeable. Hosted production must not submit actors with the `/local-dev/` callback.

Hosted production has a code-level default callback URL when `SOURCING_RUNTIME_ENVIRONMENT=production` and provider mode is live:

```text
https://api.111874.xyz/api/providers/apify/webhook
```

You can override it with `SOURCING_APIFY_WEBHOOK_URL`, `APIFY_WEBHOOK_URL`, or `SOURCING_HOSTED_APIFY_WEBHOOK_URL`.

Hosted preflight before live workflow:

```bash
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode hosted
```

Also verify the hosted backend actually runs current code:

```bash
curl -i https://api.111874.xyz/health
curl -i -X POST https://api.111874.xyz/api/providers/apify/webhook
```

If the webhook endpoint returns `404`, hosted code does not contain the current webhook/event-level runtime changes.

## Webhook / Watcher / Recovery Contract

`provider_webhook` and `local_provider_event_watcher` are both normalized into `remote_provider_event`.

Correct behavior:

- First terminal event for a recoverable/running worker starts job-scoped recovery for explicit worker IDs.
- Later duplicate events for the same run/dataset record `received_late` and return `recovery_count=0`.
- This protection is bidirectional: watcher-first/webhook-late and webhook-first/watcher-late must both be safe.

Important interpretation:

- If a live run only records `source=local_provider_event_watcher`, the workflow may still complete, but external Apify webhook delivery was not proven.
- If runtime env is `production` or `local_dev` and provider mode is live, Harvest actors get a default Apify ad-hoc webhook URL even when no explicit URL env is set.
- If `SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED=0`, or the runtime is `scripted` / `simulate` / `replay`, no default external Apify webhook URL is attached.
- Local-dev relay smoke proves the relay path only. It does not prove hosted production route readiness.

## Before Handing A URL To The User

Check listeners and probes:

```bash
make dev-status
```

For scripted OpenAI/Lovable:

```bash
make test-env-scripted-openai-agent-status
```

If `localhost:4173`, `127.0.0.1:4173`, `localhost:4185`, or `127.0.0.1:4185` returns connection refused:

- do not assume the URL is wrong
- first assume the frontend process is gone
- relaunch using the durable target/session for the intended mode

## Related Detailed Docs

- `docs/DEVELOPMENT_GUIDE.md`: engineering guardrails and local Python/Postgres conventions.
- `docs/TEST_ENVIRONMENT.md`: isolated simulate/scripted runtime setup.
- `docs/APIFY_PROVIDER_WEBHOOK_PLAYBOOK.md`: Apify webhook env, relay, connectivity probe, and one-profile smoke.
- `docs/ECS_PRELAUNCH_CHECKLIST.md`: hosted deployment env, restart order, and production probes.
- `frontend-demo/README.md`: frontend-specific proxy, LAN, WSL, and browser troubleshooting.
