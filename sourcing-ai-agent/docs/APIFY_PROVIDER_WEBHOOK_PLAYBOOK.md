# Apify Provider Webhook Playbook

> Status: Active operations playbook for Harvest/Apify provider callbacks. Use this with `EVENT_LEVEL_WORKFLOW_RESPONSE.md`, `HARVESTAPI_PLAYBOOK.md`, and `WORKFLOW_OPERATIONS_PLAYBOOK.md` before enabling live webhooks or running webhook smoke tests.

## Purpose

This playbook records the durable setup for Apify ad-hoc webhooks in the provider-backed LinkedIn Stage 1 workflow.

The goal is event-level response:

- Apify actor reaches a terminal state.
- Apify calls our backend webhook endpoint.
- The backend quick-acks the HTTP request.
- Background recovery ingests the completed dataset and advances the local worker.
- Next provider submit remains decoupled from downstream candidate materialization.

## Terms

- `Apify API token`: the token used to call Apify APIs, submit Harvest actors, attach ad-hoc webhooks, and query actor runs. This is the `api_token` under `runtime/secrets/providers.local.json` Harvest actor settings, or `APIFY_API_TOKEN`.
- `Provider webhook token`: the inbound shared secret our backend uses to verify Apify callback requests. Long term this should be `SOURCING_PROVIDER_WEBHOOK_TOKEN` or `APIFY_WEBHOOK_TOKEN`.
- `Temporary token reuse`: current local trial defaults allow `SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1`, so the Harvest Apify API token is also accepted as the inbound webhook token. This is convenient for smoke tests but should be replaced with a separate shared secret in hosted deployments.
- `SOURCING_APIFY_WEBHOOK_URL`: a public callback URL, not a token. It must be reachable by Apify and should point to `/api/providers/apify/webhook`.

## Backend Endpoint

The backend endpoint is:

```text
POST /api/providers/apify/webhook
```

The endpoint intentionally does only lightweight work inside the HTTP request:

- validate webhook token
- normalize the Apify event payload
- accept only events with a run id or dataset id
- enqueue background handling by calling the remote provider event path
- return HTTP `202`

It must not run full recovery, dataset materialization, or PG/artifact rebuild inline. Apify webhook delivery can retry if the endpoint is slow or unreachable; the backend should return quickly and let worker recovery do the expensive work.

## Submit Contract

Harvest actor submission attaches a run-scoped ad-hoc webhook when an explicit or default callback URL is available before the actor run is submitted.

URL resolution order:

- request context `apify_webhook_url` / `provider_webhook_url`
- `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL`
- default hosted URL for `SOURCING_RUNTIME_ENVIRONMENT=production` and live provider mode: `https://api.111874.xyz/api/providers/apify/webhook`
- default local-dev relay URL for `SOURCING_RUNTIME_ENVIRONMENT=local_dev` and live provider mode: `https://api.111874.xyz/local-dev/providers/apify/webhook`

The default only applies in live provider mode. `scripted`, `simulate`, `replay`, and test runtimes do not automatically attach external Apify webhooks. Set `SOURCING_DEFAULT_APIFY_WEBHOOK_URL_ENABLED=0` only for a deliberate no-webhook diagnostic.

The local provider event watcher is enabled by default even when a webhook URL is configured. It is a safety net: it polls run status and emits the same `remote_provider_event` if the actor reaches a terminal state before the webhook is observed locally. Set `SOURCING_LOCAL_PROVIDER_EVENT_WATCH_WITH_WEBHOOK_ENABLED=0` for strict webhook roundtrip smoke tests where fallback polling must not mask a public callback failure.

The submit path sends Apify a `webhooks` query parameter containing a base64-encoded JSON array like:

```json
[
  {
    "eventTypes": [
      "ACTOR.RUN.SUCCEEDED",
      "ACTOR.RUN.FAILED",
      "ACTOR.RUN.TIMED_OUT",
      "ACTOR.RUN.ABORTED"
    ],
    "requestUrl": "https://api.example.com/api/providers/apify/webhook",
    "description": "Sourcing Agent remote provider completion event",
    "headersTemplate": "{\"X-Sourcing-Provider-Webhook-Token\":\"...\",\"User-Agent\":\"SourcingAgentApifyWebhook/1.0\"}"
  }
]
```

This is a run-scoped ad-hoc webhook. It does not require pre-creating persistent actor webhooks in Apify. Existing actor runs submitted before the URL was configured will not receive a webhook retroactively.

`User-Agent` is included intentionally. During local ECS relay testing, Cloudflare accepted the same webhook request with an explicit `User-Agent` but returned a 1010 access error to Python's default `urllib` signature. Keep this header on ad-hoc webhooks and connectivity probes unless the edge proxy policy changes.

## Preflight Before Any Workflow

Run the no-cost preflight before any workflow that is supposed to validate live Apify webhook behavior. If it fails, do not start the live workflow.

Important boundary: the process that submits Harvest actors must be started with the webhook env. Running preflight in a shell with `--webhook-url` does not retrofit an already-running backend or worker daemon. Restart `serve` and the worker daemon with the same env before submitting actors.

Hosted production:

```bash
export SOURCING_APIFY_WEBHOOK_URL="https://api.111874.xyz/api/providers/apify/webhook"
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode hosted
```

Local live smoke through the ECS local-dev relay:

```bash
./.venv/bin/python scripts/ecs_webhook_reverse_tunnel.py

export SOURCING_APIFY_WEBHOOK_URL="https://api.111874.xyz/local-dev/providers/apify/webhook"
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode local-live
```

Scripted/browser tests do not require a public Apify callback URL because they use synthetic Apify-shaped terminal events:

```bash
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_preflight.py --mode scripted
```

The preflight checks:

- `SOURCING_APIFY_WEBHOOK_URL` / `APIFY_WEBHOOK_URL` is configured for live modes
- hosted mode is not accidentally pointed at `/local-dev/`
- Harvest profile scraper is enabled and has an Apify API token
- the submit contract would attach the Apify ad-hoc `webhooks` parameter
- the public route/token returns HTTP `202` to a fake terminal event unless explicitly skipped

If no webhook URL is present in the submitting process, Harvest actor runs are submitted without ad-hoc webhooks. The local provider event watcher may still observe completion and finish the job, but that is not proof that Apify webhook delivery works.

The 2026-04-29 OpenAI ChatGPT live smoke recorded only `source=local_provider_event_watcher` events in the DB. That means the run did not validate external Apify webhook delivery. The likely causes are: the submitting backend/worker process did not carry `SOURCING_APIFY_WEBHOOK_URL`, the public relay/backend was not reachable, or the inbound token/endpoint did not match the running backend.

## Watcher / Webhook / Recovery Race Contract

`provider_webhook` and `local_provider_event_watcher` both normalize into the same `remote_provider_event` path.

- If either source arrives first while the worker is recoverable or running, the handler first persists the terminal checkpoint and releases the matching worker lease/limiter, then sends one pure wake-now signal to the already-running shared recovery daemon. The request path never starts a job-scoped sidecar, forwards worker/job/phase controls, or executes recovery inline.
- If the other source arrives later for the same run/dataset after the worker is already completed, the handler records `remote_provider_event: received_late`, leaves `recovery_count=0` and `recovery_dispatch_count=0`, and does not re-run recovery, provider submit, ingest, materialization, or reconcile. `shared_recovery_signal_count` reports only whether a shared wake signal was sent; it is not execution evidence.
- This protection is intentionally bidirectional: watcher-first/webhook-late and webhook-first/watcher-late are both treated as late duplicates once the matching worker is completed.
- Smoke/service reports expose this as `service_metrics.remote_provider_events`: source/status counts, late/in-flight duplicate counts, target worker counts, total `remote_to_local_event_lag_ms`, actionable `actionable_remote_to_local_event_lag_ms`, and `late_duplicate_remote_to_local_event_lag_ms`. Use `max_remote_provider_event_lag_ms` to fail slow actionable terminal wakeup in webhook/watcher matrices. Do not treat `received_late` by itself as recovery backlog or actionable wakeup lag.

## ECS / Hosted Setup

Use a stable HTTPS URL for hosted runtime.

1. Point a domain or subdomain to the hosted backend entrypoint, usually an ECS service behind a load balancer or reverse proxy.
2. Ensure the public path forwards to the backend route:

```text
https://<stable-domain>/api/providers/apify/webhook -> backend port 8765 /api/providers/apify/webhook
```

3. Configure backend runtime environment before starting `serve` and the worker daemon:

```bash
export SOURCING_APIFY_WEBHOOK_URL="https://<stable-domain>/api/providers/apify/webhook"
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
```

For long-term hosted deployment, prefer:

```bash
export SOURCING_APIFY_WEBHOOK_URL="https://<stable-domain>/api/providers/apify/webhook"
export SOURCING_PROVIDER_WEBHOOK_TOKEN="<separate-shared-secret>"
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=0
```

4. Store hosted secrets in the deployment secret manager or ECS task environment. Do not hardcode tokens in Git.
5. Keep backend and worker processes on the same PG-authoritative control plane so the webhook process can wake workers created by the workflow runner.

## Local Tunnel Setup

For local smoke tests, a tunnel can work, but it is less reliable than ECS because free tunnel domains can change mid-test.

1. Start the backend with the current public tunnel URL:

```bash
SOURCING_APIFY_WEBHOOK_URL="https://<tunnel-host>/api/providers/apify/webhook" \
SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1 \
make dev-launch-backend
```

2. Keep the tunnel open for the full actor run and webhook dispatch.
3. If the tunnel host changes, restart the backend with the new URL before submitting another actor run.
4. Do not submit a run with a stale tunnel URL. Apify can create a webhook dispatch but fail to deliver it, leaving local recovery to poll later.

The observed local failure mode was:

- actor run was submitted with an old `localhost.run` domain
- the tunnel session later moved to a new domain
- Apify created the dispatch, but the old callback URL was unavailable
- the local worker had to be recovered manually

The later smoke with the current tunnel URL completed successfully through provider webhook.

## ECS Local-Dev Relay Setup

For local development, use the stable ECS domain as a relay rather than a disposable tunnel domain.

Current local-dev relay URL:

```text
https://api.111874.xyz/local-dev/providers/apify/webhook
```

ECS Nginx has an exact-path route:

```text
/local-dev/providers/apify/webhook -> 127.0.0.1:18765/api/providers/apify/webhook
```

The route only works while a reverse tunnel is open from the local machine:

```bash
./.venv/bin/python scripts/ecs_webhook_reverse_tunnel.py
```

Start local backend and worker processes with the relay URL before submitting new Harvest runs:

```bash
export SOURCING_APIFY_WEBHOOK_URL="https://api.111874.xyz/local-dev/providers/apify/webhook"
export SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1
```

Then restart `serve` and the worker daemon. The worker needs the URL because it submits the Apify actor run; the backend needs the token settings because it validates the inbound callback.

This relay is for local development only. Hosted production should use the normal hosted route:

```text
https://api.111874.xyz/api/providers/apify/webhook
```

and should run backend code that contains the webhook endpoint on ECS itself. Do not point hosted production actor runs at the `local-dev` relay path.

## Connectivity Probe

Before spending provider budget, post a fake terminal event to the public URL. It should return HTTP `202`.

```bash
./.venv/bin/python - <<'PY'
import json
import urllib.request

url = "https://<public-host>/api/providers/apify/webhook"
token = "<same token backend accepts>"
body = json.dumps({
    "eventType": "ACTOR.RUN.SUCCEEDED",
    "eventData": {
        "actorRunId": "connectivity-probe-run",
        "defaultDatasetId": "connectivity-probe-dataset"
    }
}).encode("utf-8")
req = urllib.request.Request(
    url,
    data=body,
    headers={
        "Content-Type": "application/json",
        "User-Agent": "SourcingAgentApifyWebhook/1.0",
        "X-Sourcing-Provider-Webhook-Token": token,
    },
    method="POST",
)
with urllib.request.urlopen(req, timeout=15) as response:
    print(response.status, response.read().decode())
PY
```

Expected result:

```text
202 {"status":"accepted", ...}
```

If this returns `403`, the token in the request does not match what the running backend accepts. If it times out or returns connection errors, the public URL or proxy is wrong.

## One-Profile Round-Trip Smoke

Use the checked-in smoke script for live validation. It is intentionally opt-in because it submits a real Harvest actor.

```bash
SOURCING_PROVIDER_WEBHOOK_USE_APIFY_API_TOKEN=1 \
PYTHONPATH=src ./.venv/bin/python scripts/apify_webhook_roundtrip_smoke.py \
  --webhook-url "https://<public-host>/api/providers/apify/webhook" \
  --profile-url "https://www.linkedin.com/in/<never-used-profile>/" \
  --i-understand-this-submits-live-run
```

What the script checks:

- the public webhook endpoint returns HTTP `202` before live submit
- the profile URL is not already `fetched` or `queued` in the local registry unless explicitly allowed
- the normal Harvest profile worker path submits a one-profile actor run
- local fallback watcher is disabled for this strict smoke, so completion must come from provider webhook or explicit recovery
- the worker reaches `completed`
- at least one `remote_provider_event` job event was recorded
- if the one-profile actor finishes so fast that local polling completes the worker before webhook handling records the event, the script waits a short post-completion grace window before failing

The smoke outputs JSON lines with phases such as:

```json
{"phase":"connectivity_probe","http_status":202}
{"phase":"submitted","run_id":"...","worker_id":123}
{"phase":"worker_status","status":"queued"}
{"phase":"completed","remote_event_seen":true,"persisted_profile_count":1}
```

Exit codes:

- `0`: provider webhook round-trip completed and a remote event was observed
- `2`: timeout waiting for local worker completion
- `3`: worker completed but no remote provider event was observed after the post-completion grace window

## Troubleshooting

- `403 provider_webhook_token_required`: backend did not accept the header/query token. Restart backend with matching `SOURCING_PROVIDER_WEBHOOK_TOKEN` or enable temporary API-token reuse.
- `remote_provider_event_missing_run_or_dataset_id`: callback payload does not include `actorRunId`, `runId`, `defaultDatasetId`, or equivalent fields.
- Apify dispatch exists but stays active or does not reach backend: check tunnel/domain stability, TLS, security group, reverse proxy path, and backend logs.
- Apify dispatch is `SUCCEEDED` but a one-profile smoke returned exit code `3`: check whether the actor finished before webhook delivery. The backend now records a `remote_provider_event` with status `received_late` when the event matches an already completed worker by `run_id` or `dataset_id`; rerun the smoke on current code or inspect job events for `received_late`.
- Worker remains queued after Apify run succeeded: run `show-workers --job-id <job_id>` and `show-recoverable-workers --job-id <job_id> --stale-after-seconds=0`, inspect `GET /api/workers/daemon/status?job_id=<job_id>&include_details=1`, and diagnose the external shared daemon. If a deterministic diagnostic tick is required, use the worker-owned CLI `worker-recovery-daemon --run-once`; never use an API request to execute recovery.
- Actor errors with too many requests: lower global Harvest actor concurrency. Webhooks improve completion discovery; they do not remove provider rate limits.
- Batch materialization is slow after worker completion: keep submit/materialize decoupled. Optimize downstream delta materialization or writer budget; do not put full materialization back into the webhook request or provider submit path.

## Historical Validation

The local round-trip was validated with two one-profile smoke runs:

- First run: Apify created a dispatch, but the temporary tunnel domain changed before delivery. Manual recovery later completed the local worker.
- Second run: Apify run `Cg2j20FUkPDecjzOM` with dataset `Ay07MOWooKOQfR0FO` delivered dispatch `KZbFcQiPxSfHAGYgO`; the local worker moved `queued -> running -> completed`, `remote_event_seen=true`, and one profile payload was persisted.
- ECS relay smoke: Apify run `SeuvL6YsW48M2t1ML` with dataset `09ivTJCt5FhZHgZaP` attached ad-hoc dispatch `4hW8x91WACMHAROc2` and Apify reported the dispatch `SUCCEEDED`. The one-profile actor finished in about two seconds, so local status polling completed worker `231` before the webhook event was recorded. The runtime now handles this race by matching the late event back to the known worker and appending `remote_provider_event: received_late` without starting recovery.

The important lesson is that the code path worked, but temporary tunnel instability can invalidate the callback URL. ECS or another stable hosted URL is the right long-term answer.
