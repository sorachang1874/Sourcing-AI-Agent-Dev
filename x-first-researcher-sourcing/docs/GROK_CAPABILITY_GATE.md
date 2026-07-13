# Grok/X Capability Gate

> Current verdict: `NO-GO` for live X/researcher collection. Fixture validation is allowed.

## Why this gate exists

The installed Grok CLI documents generic search capability, but native X access has not yet been proven in this
workspace. A model answer or web result is not evidence of X-native retrieval. The first live step must be a bounded
capability test, not researcher mapping.

Read-only environment evidence on 2026-07-14: `grok --version` reported `0.2.99`; `grok --help` exposed generic web
search controls but no explicit X-native search/retrieval contract. No login state, token, model, network query, or
provider call was inspected or executed. This keeps the capability verdict `unproven`.

## Required owner decisions before any live call

1. Approve a user-triggered capability probe and its external request.
2. Approve legal/terms/privacy basis and retention for the exact returned fields.
3. Approve the Grok model/access mode, hard cost/rate budget, and kill switch.
4. Confirm that no protected-trait or proxy population is requested.
5. Confirm that output remains private raw evidence and cannot write canonical/product state.

Credentials are never pasted into prompts or artifacts. Existing OAuth/API-key files are not read by fixture tests.

## Stage 0: fixture gate

Required before a capability probe:

- deterministic generator `--check` passes;
- contract validator passes;
- unit tests pass, including adversarial protected-field/live-URL/assertion/status failures;
- independent review has no unresolved blocking findings;
- artifact scope and hashes are recorded.

## Stage 1: minimal capability probe

Use one official lab account, not a researcher population. The request asks for at most five recent public technical
posts and requires for each returned item:

- stable platform post ID;
- canonical `https://x.com/{handle}/status/{post_id}` URL;
- author platform user ID if the interface exposes it;
- authored timestamp and observed timestamp;
- access mode/tool provenance;
- no full-body retention beyond a bounded excerpt.

Budget: one query, one page, at most five observations, one execution, no fallback. If Grok returns generic web results,
missing stable IDs, non-canonical URLs, or unverifiable provenance, record `capability_unavailable` and stop. Do not
substitute search engines, Apify, or another provider.

## Stage 2: bounded live evidence probe

Only after Stage 1 and a new owner approval:

- one lab;
- at most eight tasks and two pages per task;
- at most 100 raw observations and 25 provisional packets;
- one run, hard deadline, cost ceiling, and kill switch;
- no canonical person/identity merge, assertion, CRM, projection, export, or outreach;
- explicit retention, correction, deletion, opt-out, and rediscovery-suppression behavior.

Success does not authorize scaling to all labs. Scale-up requires measured precision/recall, false-merge review,
coverage limitations, independent review, and a separate decision for every provider/runtime integration.
