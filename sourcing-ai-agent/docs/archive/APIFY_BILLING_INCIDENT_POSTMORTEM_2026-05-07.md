# Apify Billing Incident Postmortem — 2026-05-07

> Status: Archived 2026-06-11. Historical record only — do not treat as active guidance; see `docs/INDEX.md` for current docs. (Previous status: Historical incident postmortem and prevention-gate reference. Use with `RUNTIME_ENVIRONMENT_ISOLATION.md`, `TEST_ENVIRONMENT.md`, and `EVENT_LEVEL_WORKFLOW_RESP)

## Summary

On 2026-05-07, PG-backed scripted smoke work escaped the isolated `runtime/test_env/...` namespace and was executed by root/local-dev recovery as `local_dev/live` provider work. This caused real Apify/Harvest profile-scraper calls for synthetic LinkedIn fixture URLs such as `openai-agent-current-0189`.

The incident was not caused by lost Apify webhooks. The webhook contract was receiving terminal events. The failure mechanism was cross-runtime durable work execution after an async webhook/recovery thread outlived the isolated test environment.

User-reported cost impact: about `$15`.

## Timeline

- `2026-05-07 01:16 CST`: the smoke/webhook quick-ack change switched the driver from synchronous `/api/providers/apify/webhook?sync=1` to `/api/providers/apify/webhook`.
- `2026-05-07 01:23 CST`: first user-observed live billing symptom appeared in Apify for synthetic OpenAI Agent profile URLs.
- `2026-05-07 01:23:27 CST`: the `openai_fix2` isolated runtime timed out while recovery/local-apply work was still active.
- `2026-05-07 01:24:26 CST`: first matching wrong live provider-cache artifact appeared under `runtime/provider_cache/local_dev/live/...`.

## Root Cause

The quick-ack webhook path spawned `provider-webhook-event` background threads that performed full `handle_remote_provider_event()` work outside the HTTP request. The isolated scripted runtime cleanup restored root/local-dev environment variables before those threads were joined.

After environment restore, root/local-dev recovery saw durable rows whose paths pointed into `runtime/test_env/board_runtime_pg_closeout_20260507_openai_fix2/...`. Because the durable drains did not yet enforce runtime namespace ownership before claim/execute, the root daemon treated test-runtime work as local-dev live work and submitted real provider requests.

## Evidence Chain

- Apify backend `requestId` from the user billing page matched local provider response artifacts.
- The request manifest was written under `runtime/provider_cache/local_dev/live/...`.
- The corresponding queue summary and snapshot paths pointed into `runtime/test_env/board_runtime_pg_closeout_20260507_openai_fix2/...`.
- Root/public PG rows referenced the isolated runtime across `jobs`, `agent_worker_runs`, `job_materialization_items`, `linkedin_profile_registry`, and `linkedin_profile_registry_events`.
- The isolated PG schema still had the job blocked, proving this was cross-namespace recovery/refill execution, not normal scripted provider replay.

## Remediation

- Runtime namespace ownership is now a pre-claim contract for worker recovery, profile refill, search-seed discovery item drain, local-apply closure item drain, board-visible apply item drain, and snapshot-full-materialization item drain.
- Cross-runtime durable work now returns `runtime_namespace_mismatch` / `runtime_namespace_skipped_count` before claim. It must not submit providers, claim rows, increment attempts, or write retry errors.
- Isolated scripted runtime cleanup now joins runtime-owned provider-webhook, job-recovery, shared-recovery, hosted-watchdog, workflow-controls, background materialization, and layering threads before restoring root/local environment.
- Scripted smoke preflight/postflight now fails closed on active root/local-dev daemon pid/status files, non-isolated runtime env files, live provider secrets, live provider invocation reports, synthetic fixture manifests under live provider cache, or public/root PG rows pointing at the target test runtime.
- Contaminated public/root PG rows and live provider-cache artifacts were quarantined after dry-run review.

## Quarantine Evidence

- `output/runtime_contamination_quarantine_apply_current.json`: moved `394` provider-cache files and removed `1189` active public PG rows after copying them to quarantine tables.
- `output/runtime_contamination_quarantine_apply_second.json`: removed another `4` public PG rows referencing older test runtimes after quarantine copy.
- Clean audit evidence:
  - `output/runtime_contamination_audit_post_lovable_global.json`
  - `output/runtime_contamination_audit_post_openai_target.json`
  - `output/runtime_contamination_audit_post_lovable_target.json`

## Prevention Gates

- No scripted/simulate/replay run may start if its runtime env file declares live provider mode, non-isolated runtime environment, disabled live guard overrides, or nonblank live provider secrets.
- No scripted smoke may start or pass while active root/local-dev daemons are visible.
- No scripted smoke may pass if provider invocation reports have missing `provider_mode` or `provider_mode=live`.
- No scripted smoke may pass if preflight/postflight contamination audit finds synthetic fixture URLs in live provider-cache manifests or public/root PG rows pointing into the target test runtime.
- Durable drains must stay runtime-owned before claim. Any future durable item kind that can trigger provider submit, local apply, board-visible publication, or materialization must adopt the same namespace guard.

## Follow-Up Principle

Scripted tests need an explicit environment contract, not helper-only isolation. The smoke runner, provider clients, durable drains, recovery owner, and cleanup path all enforce the same fail-closed rule: non-live test runtimes cannot submit live providers, and root/local-dev recovery cannot drain nested test-runtime work.
