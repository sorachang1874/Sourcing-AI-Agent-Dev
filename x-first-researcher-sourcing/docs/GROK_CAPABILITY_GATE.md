# Grok/X Capability Gate

> Historical Stage 1 gate. Its one-shot runner failed closed. Later separately bounded exploration and a seven-wave
> adaptive campaign proved native-X calls; see `docs/live-evidence/`. The fixed packet/observation values below are
> retained only as the historical minimal-capability design. They are not candidate-recall targets and do not govern
> the adaptive campaign. This document does not authorize Stage 2 promotion.

## Why this gate exists

At the time this historical gate was written, the installed Grok CLI documented generic search capability but native X
access had not yet been proven in this workspace. A model answer or web result was not evidence of X-native retrieval,
so the first live step was a bounded capability test rather than researcher mapping.

Read-only environment evidence on 2026-07-14: `grok --version` reported `0.2.99`; `grok --help` exposed generic web
search controls but no explicit X-native search/retrieval contract. No login state, token, model, network query, or
provider call was inspected or executed. That snapshot therefore kept the capability verdict `unproven`; later live
artifacts supersede only that capability fact, not the gate's provenance and safety requirements.

## Required owner decisions before any live call

1. Approve a user-triggered capability probe and its external request.
2. Approve legal/terms/privacy basis and retention for the exact returned fields.
3. Approve the Grok model/access mode, hard cost/rate budget, and kill switch.
4. Confirm that no protected identity or protected-identity proxy population is requested.
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

The repository now contains fixture-only request/result schemas and an executable offline validator described in
`STAGE1_CAPABILITY_FIXTURE_CONTRACT.md`. They validate shape and fail-closed behavior only. They cannot represent a
live call, cannot emit `x_native_proven`, and do not change this gate's `NO-GO` verdict.

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

## Historical Stage 2 proposal: superseded packet caps

The original proposal used the following small diagnostic envelope:

- one lab;
- at most eight tasks and two pages per task;
- at most 100 raw observations and 25 provisional packets;
- one run, hard deadline, cost ceiling, and kill switch;
- no canonical person/identity merge, assertion, CRM, projection, export, or outreach;
- explicit retention, correction, deletion, opt-out, and rediscovery-suppression behavior.

Success does not authorize scaling to all labs. Scale-up requires measured precision/recall, false-merge review,
coverage limitations, independent review, and a separate decision for every provider/runtime integration.

The 2026-07-14 adaptive campaign superseded these values for recall discovery after explicit user authorization. It
used no candidate, observation, or per-wave call success ceiling; each process still had a 64-turn emergency ceiling,
30-minute deadline/kill boundary, one process, no fallback, and private output. Discovery paused only after distinct
current-team and coverage-audit strategies reached `0.048` and `0.014` new unique handles per raw native-X call. That
was an operator capacity decision, not a formal population-stop claim; source replay remains
`insufficient_proof / continue_expansion`.
