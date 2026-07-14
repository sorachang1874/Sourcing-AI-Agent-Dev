# Track C C2.7 conditional-owner closure

Status: author fixed-forward implementation; independent review required before
live/manual/product/milestone signoff.

## Goal and scope

C2.7 closes the C2.6 review findings without changing schema, provider/model
behavior, or the open-mode operator surface. A handler lookup remains a fast
non-enumeration check; authenticated authorization is committed together with
the first durable write through a PostgreSQL owner predicate or row-lock UoW.

## Closed findings

1. The authenticated route registry now includes criteria feedback, suggestion
   review, and explicit recompile. All three pass the bearer-derived exact job
   owner into optional retrieval reruns. Explicit baselines fail closed before
   results are read; automatic matching is filtered by requester and tenant;
   the owner is re-read before execution and persisted on the derived job.
2. Authenticated `GET /api/workers/daemon/status?job_id=...` ignores raw detail
   opt-in and uses a dedicated allowlist instead of constructing the general
   runtime-controls view. It probes service status only when the stored control
   names the canonical `job-recovery-<job_id>` sidecar. A job-scoped signal that
   nudges the shared daemon exposes only status/scope/mode/job id; shared
   recovery, hosted watchdog, wakeup payloads, workflow-runner logs, and foreign
   service details are neither probed nor returned. Open mode retains the global
   operator projection.
3. `continue-stage2` re-checks the exact owner after a busy lock result before
   returning job state. Its queued transition and workflow cancel use
   `UPDATE jobs ... WHERE requester_id = ? AND tenant_id = ? RETURNING *`;
   owner mismatch is the canonical job `404` and cannot rewrite owner columns.
4. Authenticated CRM PATCH locks the canonical `crm_records` row, checks exact
   workspace plus the C2.5 blank-or-equal owner rule, checks `crm_version`, and
   writes record, engagement, and event in the same transaction. Public-Web
   promotion performs its first durable promotion write under the same record
   owner lock. Missing and foreign records share the canonical CRM `404`.
5. The C2.6 document is corrected: blank `owner_user_id` in the exact user
   workspace is intentionally compatible; this batch does not invent or run an
   owner migration. `default` workspace and conflicting nonblank owner remain
   denied.

## Owner/source-of-truth matrix

| Operation | Source of truth | Atomic boundary | Failure projection |
| --- | --- | --- | --- |
| Existing job queued/cancel write | `jobs.requester_id`, `jobs.tenant_id` | conditional PostgreSQL `UPDATE ... RETURNING` | job not found |
| Criteria rerun baseline | baseline `jobs` row | owner-scoped selection plus final canonical re-read before derived create | job not found |
| Authenticated worker status | exact-owned `jobs` row | read-only job-scoped projection; raw/global details suppressed | job not found |
| CRM PATCH | locked `crm_records` row | record + engagement + event transaction | CRM record not found; same-owner stale version is conflict |
| CRM Public-Web promotion | locked `crm_records` row | owner check + promotion upsert transaction | CRM record not found |

## Explicit residual boundary

C2.7 does not close R-028 globally. Projection selection still owns the sorted
person-identity lock path, while this PATCH UoW locks the existing record id;
`add_person_to_crm`, duplicate-person cleanup, the future
`(workspace_id, person_identity_key)` unique constraint, and command
terminal/effect atomicity remain for the CRM Repository/command-completion
batch. No code in this slice claims global CRM exactly-once semantics.

## Validation contract

- auth/request-scope/private-read/owner-fencing fast lane: `100 passed + 65
  subtests`;
- permanent isolated-PG owner-CAS regressions plus the full adjacent live-PG
  adapter lane: `64 passed + 4 subtests` (the focused new file is `2 passed`);
- API transport parity: `13 passed`;
- CRM PATCH and Public-Web promotion endpoint adjacency: `2 passed`;
- CRM Public-Web runtime boundary: `34 passed`;
- exact open-mode daemon-status and criteria compatibility nodes from
  `tests/test_pipeline.py`: `5 passed` with the local PG DSN (the full file was
  not run);
- `make lint`: `58 files already formatted`, all checks passed;
- mypy ratchet: expected nonzero, unchanged at `81 errors / 4 files`;
- touched-module `py_compile` and `git diff --check`: clean;
- no provider/model/live call and no full `tests/test_pipeline.py` run.
