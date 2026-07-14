# Track C C2.6 authenticated canonical-owner fencing

Status: scoped review `NO-GO`; fixed-forward implementation is tracked by
`TRACK_C_C2_7_CONDITIONAL_OWNER_CLOSURE_IMPLEMENTATION.md`.

## Goal

C2.6 closes the gap between an authenticated API handler check and the component
that performs the durable or provider-capable side effect. Handler checks remain
a fast, non-enumerating rejection, but they are not authorization owners. The
canonical operation receives the server-derived expected owner and re-reads the
resource immediately before its first mutation or external control action.

No provider/model behavior, schema, or live-provider gate changes are included.
The pure exact-match rules live in `sourcing_agent.request_ownership`; job and
CRM canonical owners reuse them instead of re-deriving sentinel behavior.

## Owner contracts

| Resource/operation | Source of truth | Authenticated rule | Canonical fence |
| --- | --- | --- | --- |
| Existing job write | `jobs.requester_id` + `jobs.tenant_id` | both must exactly equal the bearer user and `user-<user>` | operation re-reads the job before the side effect |
| Excel workflow job create | bearer identity | API replaces requester/tenant aliases; Excel owner persists both on create and every later save | `_save_excel_intake_job_state` preserves the stored owner |
| Refinement compile/apply | baseline job owner | exact source-job match; legacy/blank owners do not grant writes | compile rechecks before return; apply rechecks before artifacts and derived-job creation |
| Refinement-derived job | baseline job owner | derived job inherits requester/tenant | running and failure `save_job` calls persist both fields |
| Worker interrupt | worker -> `job_id` -> job owner | exact linked job owner | worker link and job owner are both re-read immediately before interrupt |
| Worker cleanup | explicit `job_id` -> job owner | authenticated callers must supply one exact-owned job | every retirement batch rechecks the job and worker links |
| Job-scoped service shutdown | `job_id` -> job owner | only the derived job recovery service is allowed | job owner is re-read before each stop request |
| CRM PATCH/promotion | `crm_records.workspace_id`; nonblank `owner_user_id` is an additional consistency check | exact `user-<user>` workspace; blank owner is accepted by the ratified C2.5 compatibility rule, otherwise owner must equal the bearer user | CRM writer/owner re-reads immediately before first durable write |

An exact user workspace with blank `owner_user_id` remains mutation-compatible
under the earlier C2.5 contract until its separately owned writer/migration is
approved. The legacy `default` workspace and a conflicting nonblank owner never
grant authenticated mutation authority.

## Public job/worker route registry

`sourcing_agent.api.AUTHENTICATED_REQUEST_SCOPE_REGISTRY` is the executable
inventory. Its classifications cover:

- server-owned job creation: workflow and Excel workflow submit;
- exact job read-via-POST and writes: candidate batch, refinement compile/apply,
  stage-2 continue, profile completion, import, projection backfill, cancel;
- exact worker-to-job control: interrupt and job-scoped cleanup;
- job-scoped or global control: recoverable-worker/status reads and runtime
  shutdown;
- global operator controls: shared recovery run-once and systemd-unit creation.

There is no authenticated admin capability in C2.6. Therefore authenticated
ordinary users receive `403 {"status":"forbidden","reason":"admin_scope_required"}`
for global cleanup, global worker status/listing, global shutdown, shared
recovery run-once, and systemd-unit creation. Open mode remains the explicit
operator compatibility surface and retains the previous global behavior.

## Non-enumeration

For authenticated user-private reads and exact writes, a missing id and a
foreign id have identical route-family responses:

- job: `404 {"status":"not_found","reason":"job_not_found"}`;
- CRM record: `404 {"status":"not_found","reason":"crm_record_not_found"}`.

Canonical race failures use the same resource-family reason and do not return
the foreign id or owner.

## Bearer-map parsing

`SOURCING_API_BEARER_TOKENS` accepts only a non-empty JSON object whose every
entry is a non-empty string token mapped to a non-empty string user id. The
whole configuration fails closed when any entry is blank/non-string or when two
tokens collide exactly or after whitespace normalization. Object pairs are
preserved while parsing so duplicate JSON keys cannot be silently overwritten.
Values are never coerced with `str()`.

## Regression coverage

The C2.6 fast lane covers:

- malformed/structured bearer values and normalized-token collisions;
- exact server owner propagation into Excel/refinement operations;
- missing-versus-foreign response-body equality;
- authenticated global-control denial and open-mode compatibility;
- the public route registry;
- owner changes between handler/canonical reads for cancel, interrupt, shutdown,
  and CRM writes;
- Excel owner preservation and refinement running/failure owner persistence.

PostgreSQL-backed adjacent tests remain required for the existing pipeline
paths. Live providers/models are forbidden for this slice.
