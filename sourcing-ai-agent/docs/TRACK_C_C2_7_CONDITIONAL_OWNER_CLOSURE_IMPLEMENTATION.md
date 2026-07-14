# Track C C2.7 conditional-owner closure

Status: pinned review `NO-GO`; the criteria preflight finding is fixed forward
by `TRACK_C_C2_8_CRITERIA_OWNER_PREFLIGHT_TOTALITY_IMPLEMENTATION.md`.

## Goal and scope

C2.7 closes the C2.6 review findings without changing schema, provider/model
behavior, or the open-mode operator surface. A handler lookup remains a fast
non-enumeration check; authenticated authorization is committed together with
the first durable write through a PostgreSQL owner predicate or row-lock UoW.

## Closed findings

1. The authenticated route registry now includes criteria feedback, suggestion
   review, and explicit recompile. All three pass the bearer-derived exact job
   owner into optional retrieval reruns. The `4b2370a` implementation intended
   to preflight every explicit/source job before criteria-domain writes, but its
   shared helper incorrectly returned early when `rerun_retrieval` was missing
   or false. The pinned review therefore found a foreign/missing-owner write
   path and returned `NO-GO`; C2.8 owns the correction. Automatic rerun matching
   itself remains filtered by requester and tenant, with a final owner re-read
   before execution.
2. Authenticated `GET /api/workers/daemon/status?job_id=...` ignores raw detail
   opt-in and uses a dedicated allowlist instead of constructing the general
   runtime-controls view. It probes service status only when the stored control
   names the canonical `job-recovery-<job_id>` sidecar. A job-scoped signal that
   nudges the shared daemon exposes only status/scope/mode/job id; shared
   recovery, hosted watchdog, wakeup payloads, workflow-runner logs, and foreign
   service details are neither probed nor returned. Open mode retains the global
   operator projection.
3. `continue-stage2` re-checks the exact owner after a busy lock result before
   returning job state. Its queued transition and workflow cancel now use a
   typed PostgreSQL row-lock CAS with `applied | owner_miss | state_conflict`.
   Stage 2 requires the locked row to remain workflow + blocked + retrieving +
   `awaiting_user_action=continue_stage2` and not already queued/running;
   cancel requires a non-terminal workflow. An authorized state conflict is
   projected through the normal `already_completed`,
   `stage2_already_requested`, or `already_terminal` contract instead of a
   false `404`, while a missing/foreign row remains the canonical job `404`.
4. Authenticated CRM PATCH locks the canonical `crm_records` row, checks exact
   workspace plus the C2.5 blank-or-equal owner rule, checks `crm_version`, and
   writes record, engagement, and event in the same transaction. A stale
   `crm_version` is an HTTP `409`, not malformed-input `400`. Public-Web
   promotion takes the canonical record lock, then a schema-scoped promotion-id
   advisory lock, then the existing promotion row. The promotion id is an
   immutable idempotency identity: only an exact replay may return the existing
   row; cross-workspace, cross-record, signal, action, or other payload drift is
   an HTTP `409` conflict and cannot rewrite it. Missing and foreign records
   share the canonical CRM `404`.
5. The C2.6 document is corrected: blank `owner_user_id` in the exact user
   workspace is intentionally compatible; this batch does not invent or run an
   owner migration. `default` workspace and conflicting nonblank owner remain
   denied.

## Owner/source-of-truth matrix

| Operation | Source of truth | Atomic boundary | Failure projection |
| --- | --- | --- | --- |
| Existing job queued/cancel write | locked `jobs` owner + operation-specific state | typed PostgreSQL owner/state CAS | job not found; authorized business-state projection |
| Criteria rerun baseline | explicit/source baseline `jobs` row | shared zero-write preflight; owner-scoped automatic selection; final canonical re-read | job not found |
| Authenticated worker status | exact-owned `jobs` row | read-only job-scoped projection; raw/global details suppressed | job not found |
| CRM PATCH | locked `crm_records` row | record + engagement + event transaction | CRM record not found; same-owner stale version is HTTP 409 |
| CRM Public-Web promotion | locked `crm_records` row + immutable promotion id | record -> advisory promotion id -> promotion row; exact replay only | CRM record not found; idempotency conflict is HTTP 409 |

## Explicit residual boundary

C2.7 does not close R-028 globally. Projection selection still owns the sorted
person-identity lock path, while this PATCH UoW locks the existing record id;
`add_person_to_crm`, duplicate-person cleanup, the future
`(workspace_id, person_identity_key)` unique constraint, and command
terminal/effect atomicity remain for the CRM Repository/command-completion
batch. Legacy promotion writers also do not yet share the new promotion-id
advisory lock, so the insert path retains a defensive `ON CONFLICT DO NOTHING`
and immutable-identity re-read rather than claiming all-writer lock unification.
No code in this slice claims global CRM exactly-once semantics.

## Validation contract

- auth/request-scope/private-read/owner-fencing plus transport lane: `109 passed
  + 65 subtests` (transport alone: `15 passed`);
- permanent isolated-PG owner-CAS regressions plus the full adjacent live-PG
  adapter lane: `71 passed + 4 subtests` (the focused owner-fencing PG file is
  `9 passed`);
- API transport parity includes the typed-CAS business projection and stale CRM
  version `409` regressions: `15 passed`;
- CRM PATCH and Public-Web promotion endpoint adjacency: `2 passed`;
- CRM Public-Web runtime boundary: `34 passed`;
- six exact compatibility nodes from `tests/test_pipeline.py` with a local PG
  DSN and per-test isolated schema: `4 passed / 2 failed`; the same exact two
  missing-table setup failures (`plan_review_sessions`, `jobs`) reproduce on a
  clean `97a81d0` worktree. The full file was not run;
- `make lint`: `58 files already formatted`, all checks passed;
- mypy ratchet: expected nonzero, unchanged at `81 errors / 4 files`;
- touched-module `py_compile` and `git diff --check`: clean;
- no provider/model/live call and no full `tests/test_pipeline.py` run.
