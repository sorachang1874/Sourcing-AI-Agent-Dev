# Track C C2.5 Authenticated Request-Scope Closure

> AUTHOR IMPLEMENTATION ONLY — formal independent review is pending. This is
> not a review artifact, GO verdict, live-provider authorization, or milestone
> signoff.

Date: 2026-07-14
Working-tree baseline observed at implementation start:
`f544954a225730ecbfb547009fcb2e383ecd73fb`

## 1. Engineering goal and bounded scope

C2.1-C2.4 established bearer identity injection and owner-gated detail reads,
but several list/overlay reads still trusted client workspace/requester fields,
several job/CRM mutations reused legacy-readable authority, and a malformed
configured bearer map silently reopened the service. This batch closes those
request-boundary gaps without changing storage schema, provider/model behavior,
SSE, C1c-e, C3b, or C4.

The batch implements:

1. one authenticated read-scope helper for CRM/private overlays and query
   dispatches;
2. explicit separation between legacy-compatible read authority and exact
   modern write authority;
3. server-derived workspace on CRM create and migration/backfill writes;
4. fail-closed app construction when a nonblank bearer config is malformed,
   non-object, or filters to zero valid mappings;
5. regression coverage for authenticated and open-mode behavior.

## 2. Contract decisions implemented

### 2.1 Authenticated read scope

`_apply_server_read_scope(...)` owns request-boundary selection:

- no namespace selector, blank selector, or a foreign selector becomes
  `user-<authenticated-user-id>`;
- client requester/user/tenant/workspace/org aliases are stripped before the
  canonical selector is written;
- explicit `default` is the sole legacy/pre-auth read selector;
- query-dispatch legacy reads use `tenant_id=default` with no requester filter,
  because those rows do not carry a trustworthy requester uniformly;
- open mode (`request.state.identity is None`) is an exact no-op.

The explicit `default` behavior is read compatibility only. It does not grant
write authority.

### 2.2 Exact write authority

Read and write predicates are deliberately separate:

| Resource/write family | Exact authenticated authority | Legacy behavior |
|---|---|---|
| Job mutations and job-derived migrations | `jobs.requester_id == user_id` **and** `jobs.tenant_id == user-<id>` | blank/`default` rows remain readable where already ratified, but are never writable |
| CRM record mutations | `crm_records.workspace_id == user-<id>` | `workspace_id=default` is never authenticated write authority |
| CRM redundant owner field | if nonblank, `owner_user_id` must also equal `user_id` | blank is tolerated because canonical writers do not yet populate it reliably |
| Missing or forbidden resource | same route-specific 404 shape | never 403, so the API does not reveal existence |

`workspace_id` is therefore the populated CRM owner source of truth in this
slice. A nonblank conflicting `owner_user_id` fails closed; that column is not
promoted to mandatory owner until its canonical writer is wired and migrated.

### 2.3 Bearer configuration

`SOURCING_API_BEARER_TOKENS` now has two explicit states:

- unset or blank: compatibility open mode, unchanged;
- set: it must be a JSON object containing at least one nonblank token-to-user
  mapping. Malformed JSON, a non-object value, `{}`, or a map whose entries all
  filter out raises `ValueError` during app construction.

Error messages name only the configuration variable and validation failure;
they never echo token material.

## 3. Route impact matrix

### Read-scoped routes

- `GET /api/query-dispatches`
- `POST /api/query-dispatches/list` (read-via-POST)
- `GET /api/crm/records`
- `GET /api/crm/tasks`
- `GET /api/crm/records/{record_id}/tasks`
- `GET /api/crm/records/{record_id}`
- `GET /api/projections/{projection_id}/crm-state`
- `GET /api/projections/{projection_id}/persons/{person_key}`
- `GET /api/persons/{person_key}`
- `POST /api/jobs/{job_id}/candidates/batch` keeps the existing job read gate,
  including explicit legacy-read compatibility.

The projection and person entities remain shared-canonical. Only their
user-private CRM overlay selector is scoped.

### Exact-write-gated routes

- `POST /api/workflows/{job_id}/continue-stage2`
- `POST /api/jobs/{job_id}/profile-completion`
- `POST /api/jobs/{job_id}/cancel`
- `POST /api/target-candidates/import-from-job`
- `POST /api/projections/backfill-from-job`
- `PATCH /api/crm/records/{record_id}`
- `POST /api/crm/records/{record_id}/public-web-promotions`

### Server-workspace-derived writes

- `POST /api/crm/records`
- `POST /api/crm/backfill-target-candidates`
- `POST /api/crm/backfill-public-web-promotions`

The existing CRM public-web search/poll/cancel/retry/export routes already use
server-derived workspace and remain unchanged.

## 4. Validation evidence

Executed from `sourcing-ai-agent/` without provider/model calls:

```text
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_api_auth.py \
  tests/test_api_request_scope.py \
  tests/test_api_server_identity.py \
  tests/test_user_private_reads.py
=> 79 passed, 50 subtests passed

.venv/bin/ruff check \
  src/sourcing_agent/api.py \
  tests/test_api_auth.py \
  tests/test_api_request_scope.py \
  tests/test_api_server_identity.py \
  tests/test_user_private_reads.py
=> All checks passed

git diff --check -- \
  src/sourcing_agent/api.py \
  tests/test_api_auth.py \
  tests/test_api_request_scope.py \
  docs/TRACK_C_C2_5_AUTH_REQUEST_SCOPE_CLOSURE_IMPLEMENTATION.md
=> clean

PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_api_transport_parity.py \
  tests/test_results_api.py::ResultsApiTest::test_target_candidate_import_from_job_api_creates_records_from_results \
  tests/test_results_api.py::ResultsApiTest::test_excel_target_candidate_import_and_export_scope_to_current_job_marker
=> 15 passed

PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_projection_crm_api_contracts.py::ProjectionCrmApiContractsTest::test_person_detail_export_and_public_web_promotion_api_contracts \
  tests/test_projection_crm_api_contracts.py::ProjectionCrmApiContractsTest::test_crm_public_web_promotion_writes_crm_owner_without_target_bridge \
  tests/test_projection_crm_api_contracts.py::ProjectionCrmApiContractsTest::test_projection_crm_bulk_prevalidates_visibility_and_revision_before_writes
=> 3 passed

PYTHONPATH=src .venv/bin/mypy --follow-imports=skip src/sourcing_agent/api.py
=> 2 errors in 1 file

Same command in a detached clean worktree at f544954a225730ecbfb547009fcb2e383ecd73fb
=> the same 2 errors in 1 file (MIME b64encode/decode union typing); focused ratchet delta 0

Final combined scoped rerun of all pytest nodes above
=> 97 passed, 50 subtests passed

.venv/bin/ruff format --check \
  src/sourcing_agent/api.py \
  tests/test_api_auth.py \
  tests/test_api_request_scope.py \
  tests/test_api_server_identity.py \
  tests/test_user_private_reads.py
=> 5 files already formatted
```

The focused regression suite proves:

- all nine read-boundary routes ignore foreign selectors and preserve explicit
  legacy `default` reads;
- query-dispatch GET and POST share identical server/legacy selection rules;
- exact job writes require both requester and tenant;
- legacy job rows remain readable through the POST-read batch route but cannot
  mutate or seed migration writers;
- exact CRM writes reject foreign/default workspace and conflicting nonblank
  `owner_user_id` with 404 before side effects;
- CRM create/backfill payloads receive server workspace;
- unset open mode preserves request payloads byte-for-field at the helper and
  handler boundaries;
- configured-invalid bearer maps fail before the server is constructed.

## 5. Residual boundaries and owner decisions

1. Formal independent scope review is still required before live/W6/manual or
   milestone signoff. Author tests and this document are not a GO artifact.
2. Open mode remains an explicit compatibility path per this implementation
   batch. Its retirement/hard-401 deployment condition remains operator-owned;
   this batch does not silently reinterpret unset configuration.
3. `crm_records.owner_user_id` is not reliably populated by the canonical CRM
   writer. Until a separately approved writer/migration slice closes that gap,
   exact workspace is authoritative and a nonblank owner value is only an
   additional fail-closed invariant.
4. The deprecated `target_candidates` storage remains ownerless. Its
   `import-from-job` entry now requires exact source-job authority, but this does
   not make the legacy table a normal multi-user source of truth. Retirement or
   migration remains required for product signoff on that legacy surface.
5. No schema, provider/model, live credential, SSE, C1c-e durable task, C3b
   daemon, or C4 API-schema work was performed.
