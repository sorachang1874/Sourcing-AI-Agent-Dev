# Track D D1h — CRM Public Web action activation

> Status: Current author implementation candidate (2026-07-16). This bounded non-live batch activates a closed
> request schema and CRM batch target binder for exactly `enrich_person_public_web`. The production partition becomes
> **4 schema-defined / 11 schema-less / served=0**. A pinned non-author advisory against `a36333b` returned
> `NO-GO 0/1/2/1`; fixed-forward `6742130` closed those four findings, then its pinned advisory returned
> `NO-GO 0/3/1/0` for action adoption of generic batch/run authority, a non-persisted running-command checkpoint,
> 500-row truncation, and command-only job authority. Pre-commit adversarial audit then found the symmetric attached-run
> and deterministic-job collisions, mutable batch-status recovery drift, and swallowed native checkpoint failures.
> The current fixed-forward closes those paths; its final pre-commit audit is `0/0/0/0` and a fresh pinned review must
> bind the enclosing commit. Neither advisory nor author evidence is a formal verdict. No
> provider, model, credential, or live environment is used by this batch.

## 1. Outcome and bounded scope

D1h removes `enrich_person_public_web` from the R-029 compatibility numerator without changing whether any action is
served to a model. The production registry now has this exact current partition:

- schema-defined: D1f's `set_crm_stage`, `add_crm_note`, and `create_crm_task`, plus
  `enrich_person_public_web` (**4**);
- schema-less compatibility bridge: every other production action (**11**);
- served Agent tools: **0**.

The activated action keeps its existing `approval_policy=required`, budget requirement, dispatch adapter, and
`crm.public_web.queue_batch` command owner. D1h adds one closed request contract, one batch target binder, authenticated
route scoping, a persisted owner/version snapshot, and revalidation before command planning and before CRM Public Web
batch/run materialization. It does not add a model-safe result schema, simulate serializer, served registry, provider
or model call, storage migration, workflow-command type, or live execution path.

## 2. Request and target contract

The checked-in request contract is `crm_public_web_enrichment_request_v1`. Caller-controlled Public Web options live
only in `input_payload`; the owner-minted target is:

```json
{
  "crm_record_ids": ["crmrec_a", "crmrec_b"],
  "workspace_id": "workspace-alice",
  "crm_record_snapshots": [
    {
      "crm_record_id": "crmrec_a",
      "workspace_id": "workspace-alice",
      "owner_user_id": "alice",
      "crm_version": 3
    },
    {
      "crm_record_id": "crmrec_b",
      "workspace_id": "workspace-alice",
      "owner_user_id": "alice",
      "crm_version": 7
    }
  ]
}
```

The request boundary accepts exactly one selector across `target_ref` and `input_payload`:

- batch selectors: `crm_record_ids` or `record_ids`;
- single-record selectors: `crm_record_id` or `record_id`;
- owner lookup selector: `person_identity_key`.

The selected ids are normalized, deduplicated, sorted, and bounded to 1–1000 records. A selector supplied in
`input_payload` is removed before request-schema validation and persistence, including a selector-only object whose
remaining input is correctly empty. When the selector lives in `input_payload`, `target_ref` must be empty; when it
lives in `target_ref`, that object must contain only the selector. Caller/model supplied workspace, owner, version,
snapshot, unknown target keys, or a second selector is not accepted as target authority. The existing resource-bound input
envelope rule also applies: exactly zero or one of `input` / `input_payload` may be present, and every present envelope
must be an object.

The closed Public Web options cover source-family selection; query/result/link/fetch bounds; content/contact and AI
extraction controls; batch polling/provider-wait/reset controls; bounded concurrency/timeouts; and explicit
`force_refresh` / `refresh_nonce` fields. Unknown fields and out-of-range values fail schema validation before action
persistence. When `force_refresh=true` and the caller omits `refresh_nonce`, command planning derives a stable
`operation-...` nonce from `operation_run_id + action_id` and persists it as both the canonical `refresh_nonce` and the
existing owner-compatible `nonce`. A retry of the same operation therefore reuses the same forced-refresh identity
instead of minting a timestamp/random nonce and duplicating the batch. An explicit caller `refresh_nonce` is preserved.
The queue-batch runtime now normalizes these closed top-level action options as the actual Public Web options when no
legacy nested `options` object exists; nested generic API requests retain their existing precedence. Thus values such
as `fetch_content=false` and `ai_extraction=off` are no longer silently replaced by runtime defaults.

The stable request identity uses only `crm_record_ids + workspace_id`. Per-record `owner_user_id` and `crm_version`
remain authorization/concurrency snapshots: they are persisted and revalidated, but do not turn owner/version drift
into new caller intent for idempotent replay.

## 3. Authorization and failure matrix

Authenticated submission derives workspace and user from request state. Every selected CRM row must exact-match the
workspace and, when its owner adjunct is populated, the user. The existing blank-owner migration compatibility is
preserved. One missing, foreign-workspace, or populated foreign-owner member fails the whole batch with the same
response:

```json
{"status": "not_found", "reason": "crm_record_not_found"}
```

That pre-submit rejection writes no AgentAction, OperationRun, operation/workflow event, workflow command/outbox,
ActivityRun/Attempt, EntityDelta, CRM Public Web batch, or CRM Public Web run. Mixed owned+missing and
owned+foreign batches have the same all-or-nothing result. Malformed/dual selectors, owner-field injection, non-object
envelopes, unknown options, and invalid option bounds return `invalid` before the same durable surfaces are written.

Positive coverage retains:

- same-owner batch selection with stable dedupe/sort;
- a single selector carried through the input compatibility location and then removed from persisted input;
- `person_identity_key` lookup inside the exact workspace;
- exact idempotent replay against the canonical target identity;
- open-mode operator compatibility when the explicit operator workspace exact-matches every selected row. Open mode
  does not require or accept an authenticated adjunct owner and does not permit a cross-workspace row.

## 4. Dispatch and command-owner revalidation

Dispatch revalidates the complete persisted batch snapshot before a new workflow command plan is written. Workspace or
owner loss returns `not_found/crm_record_not_found`; version drift returns `conflict/crm_record_target_stale`. Both
leave the pre-existing action/run state intact and add no workflow plan, event, command, batch, run, or EntityDelta.

The operation-planned queue command carries the exact snapshot in `payload.crm_record_target`, uses the dedicated
`create_crm_public_web_batch_from_operation_action` planning-mode discriminator, and has the physical
`crm.public_web.queue_batch:operation:` idempotency prefix. Those two discriminators must agree; deleting, downgrading,
or forging either fails closed. Generic API-start/retry commands retain the physical `:start:` prefix plus their
existing `create_crm_public_web_batch_from_operation` mode and do not enter this action-target preflight. Before the
existing queue owner materializes a CRM Public Web batch or per-record runs for the action-bound mode, it verifies:

1. physical command idempotency prefix and payload planning mode are the exact action-bound pair;
2. physical `workflow_commands.operation_id` is non-empty and exact-equals payload `operation_run_id`;
3. that OperationRun exists and links the exact persisted `enrich_person_public_web` AgentAction;
4. action id/type, request schema pins, command type, action/run/workspace identity, both record-id carriers, target
   snapshot, caller options, metadata, and deterministic nonce carriers all agree with the persisted action;
5. every current CRM row still exact-matches the persisted workspace/owner/version snapshot;
6. `requested_by` is the stable queue-batch service principal, never the mutable submit/dispatch actor;
7. an explicit continuation carries the complete owner field set and its `batch_id` is the deterministic derivative of
   the canonical request. Persisted batch workspace, records, options, nonce, idempotency key, service principal,
   runs, run identity carriers, and derived job payload must all agree;
8. before generic start/reuse, the action owner rejects any existing deterministic batch that is not exact-attributed
   to the stable service principal and operation/action metadata, any deterministic run already attached to a
   different batch, any row in any workspace already attached to an otherwise absent deterministic batch, and any
   deterministic job collision while that batch is absent. After start it rereads batch/runs from PG and exact-checks
   the same identity before phase links;
9. every exact-authority batch read requests canonical expected count + one surplus sentinel, bounded at 1001, rather
   than the paginated 500-row default. Thus all admitted 1–1000 records remain visible and a hidden extra row fails
   exact validation. Join, summary sync, continuation, action-owner fallback, API response, and owner-specific cancel
   no longer truncate 501–1000 rows;
10. the derived job is created only when absent, then reread and exact-checked for id/type/state/stage/request/plan/
    summary/artifact/idempotency and blank owner fields. An explicit continuation requires that persisted row; a
    command copy cannot stand in for a missing, foreign, mutated, or terminal job. Recovery preserves the frozen
    checkpoint job-summary status while re-deriving all immutable job linkage from current batch/runs, so legitimate
    batch progress does not invalidate the checkpoint.

For a fresh action command, the durable order is target revalidation → collision preflight → generic materialization →
PG batch/run reread → job ensure+reread → lease-fenced running-command checkpoint → phase links. The checkpoint CAS
requires the exact current lease owner and an unexpired lease, atomically stores payload plus typed causality, and must
return the persisted command before phase writes. Recovery after lease expiry reclaims that complete checkpoint;
generic queued/retry payload updates remain unchanged and cannot update a running command. The native checkpoint writer
is registered as a strict `workflow_commands` PG mutation, so schema/transport failures raise instead of being
misreported as an ordinary lease-CAS conflict.

The queue-owner preflight occurs after its existing command claim/running transition. A failed preflight therefore
terminalizes the command and may synchronize its linked OperationRun; it is deliberately **not** described as a
full-table zero-write path. The bounded guarantee is that no `crm_public_web_batches`, `crm_public_web_runs`, or
`workflow_entity_deltas` rows are created. On an exact positive, the existing owner creates one batch and one run per
canonical CRM record only after this preflight. A valid persisted continuation rehydrates those domain rows from the
store only after the deterministic binding check; command-owned copies cannot redirect the owner to an unrelated
same-workspace batch. A checkpoint lease conflict returns without changing the command or linked Operation state.

## 5. Owner/source-of-truth matrix

| contract | owner/source of truth | normal consumers | forbidden source/fallback | status |
| --- | --- | --- | --- | --- |
| activated action set | `operation_runtime.CRM_RESOURCE_BOUND_ACTION_TYPES` plus `DEFAULT_ACTION_REGISTRY` | route scope, schema characterization, binder/dispatch fences | duplicated route-local action list | 4 active / 11 bridge |
| request schema/version/digest | checked-in `ActionRequestSpec` | submit validation, durable pins, dispatch/command preflight | caller metadata or mutable command copy | v1 active for exact action |
| selector grammar | `CRM_RECORD_BATCH_TARGET_SELECTOR_FIELDS` and `CRMRecordBatchTargetBinder` | submit owner lookup | workspace/owner/snapshot supplied as selector | exactly one selector |
| canonical target set | `CRMRecordBatchTargetBinder` reads of canonical `crm_records` | action target, replay identity, command payload | caller order, duplicates, command-only ids | sorted unique 1–1000 |
| authorization/execution snapshot | binder-owned CRM row snapshots | submit, dispatch, queue-command owner | payload workspace/owner/version | active per record |
| authenticated bind context | server request identity | exact action submit | caller workspace/user override | active |
| stable replay identity | `ActionRequestSpec.request_identity_target_fields` | action idempotency identity | mutable owner/version snapshot | ids + workspace |
| Public Web option shape | closed v1 input schema | existing planner/queue owner | unknown option or permissive extra field | active |
| forced-refresh retry identity | operation planner from explicit `refresh_nonce` or stable operation/action hash | command payload and existing queue owner | random/timestamp nonce minted on command retry | stable and persisted |
| action command carrier | persisted action input/target plus physical `:operation:` prefix and dedicated planning mode | queue-command owner preflight | mutable label, record alias, nested option, nonce, actor attribution, batch/run, or job copy | exact-copy checked before materialization/continuation |
| action materialization authority | action-derived deterministic batch/run ids plus cross-workspace PG exact rows attributed to service principal and operation/action metadata | action queue owner and recovery | generic same-idempotency batch, orphan/foreign/attached run, start return object | pre-start collision check + post-start PG reread |
| exact batch membership | canonical requested count + one sentinel, admitted records 1–1000 | join, summary, continuation, cancel, API response | paginated default 500 or expected-count prefix | explicit 2–1001 exact read |
| action job authority | deterministic frozen job payload plus exact persisted `jobs` row and current immutable linkage | fresh ensure and continuation recovery | command-only job copy, mutable batch status re-derivation, or terminal-protection no-op | preflight + ensure/reread or fail closed |
| continuation checkpoint | running `workflow_commands` row under exact live lease and strict PG native-writer mapping | crash/takeover recovery before phase links | generic queued/retry updater, swallowed PG exception, or in-memory payload | lease-fenced CAS required |
| batch/run materialization | existing CRM Public Web queue-command owner | non-live owner drain; future runtime | preflight described as effect/UoW closure | R-019/R-028 open |
| served tool predicate | future full D1 predicate | future tool registry/planner | schema, adapter, or command presence alone | zero |

## 6. Compatibility and residual boundaries

- R-029 falls from 12 to **11** schema-less production actions and remains open. The release-window durable-hit audit,
  separately deployed `NOT VALID` validation, and complete API-submittable-population deletion condition are unchanged.
  The checked-in observation epoch remains `d1f_r029_20260715_v2`; D1h does not start a new release window.
- Brownfield empty-pin `enrich_person_public_web` rows now fail closed rather than re-entering the schema-less
  continuation path. The remaining 11 actions retain their prior compatibility behavior until their own reviewed
  schema/owner decisions land.
- Brownfield action-command continuations produced before the stable service-principal and deterministic continuation
  contract may fail closed and require an operator replay through the canonical action command. This is intentional;
  mutable legacy owner fields are not accepted as execution authority.
- R-019 remains open. Submit/dispatch/queue-owner checks are bounded preflights; action approval, command planning,
  claim/running/failure, batch/run effects, terminal state, EntityDelta, and linked Operation synchronization are not
  one PG UoW with one total lock budget. A concurrent job insert between preflight and generic save remains within this
  recorded multi-commit TOCTOU residual; the sequential pre-existing collision is now zero-domain-write.
- R-028 remains open. D1h exposes no new domain writer, but the existing batch/run materialization can still race a
  post-preflight owner/version change and is not transactionally joined to command terminal and Operation completion.
- R-031 has a scope-local D1g advisory GO but remains formal-review pending. D1h does not inherit a formal verdict for
  authenticated downstream Operation reads/controls.
- Served population remains zero. Fake/scripted and local open-mode evidence do not authorize live/provider use.

## 7. Verification and review handoff

Run from `sourcing-ai-agent/` with the local PG fixture:

```bash
make local-pg-up
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1h_crm_public_web_action_activation.py \
  tests/test_d1h_crm_public_web_batch_boundaries.py \
  tests/test_workflow_command_running_payload_checkpoint.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_api_request_scope.py \
  tests/test_d1_action_request_contract.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1e_crm_existing_record_action_schemas.py \
  tests/test_d1f_crm_existing_record_action_activation.py \
  tests/test_d1h_crm_public_web_action_activation.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_budget_required_action_requires_explicit_budget \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_public_web_enrichment_operation_dispatch_leaves_batch_creation_to_command_owner \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_crm_public_web_operation_full_phase_lifecycle_uses_typed_commands \
  tests/test_api_request_scope.py::RequestScopeWiringTest::test_operation_crm_action_transport_derives_owner_and_preserves_open_mode
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_crm_public_web_retry_without_nonce_joins_existing_child_run
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_crm_public_web_runtime_boundary.py
.venv/bin/ruff check \
  src/sourcing_agent/action_target_binding.py \
  src/sourcing_agent/api.py \
  src/sourcing_agent/control_plane_live_postgres.py \
  src/sourcing_agent/crm_public_web_owner.py \
  src/sourcing_agent/crm_public_web_runtime.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/public_web_runtime_core.py \
  src/sourcing_agent/storage.py \
  tests/test_d1h_crm_public_web_action_activation.py \
  tests/test_d1h_crm_public_web_batch_boundaries.py \
  tests/test_workflow_command_running_payload_checkpoint.py
.venv/bin/ruff format --check \
  src/sourcing_agent/action_target_binding.py \
  src/sourcing_agent/api.py \
  src/sourcing_agent/control_plane_live_postgres.py \
  src/sourcing_agent/crm_public_web_owner.py \
  src/sourcing_agent/crm_public_web_runtime.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/public_web_runtime_core.py \
  src/sourcing_agent/storage.py \
  tests/test_d1h_crm_public_web_action_activation.py \
  tests/test_d1h_crm_public_web_batch_boundaries.py \
  tests/test_workflow_command_running_payload_checkpoint.py
make typecheck PYTHON_BIN=.venv/bin/python
git diff --check
```

Current fixed-forward evidence is **20 passed + 7 subtests** for D1h action, 501/1000, and running-checkpoint matrices;
**5 passed + 11 subtests** for exact adjacent Operation/transport/generic-retry nodes; and **34 passed** for the CRM
Public Web boundary file. The current combined D1 run is **121 passed + 202 subtests**; final stable-tree Operation is
**136 passed + 503 subtests**. Lint/format is green across **58 files**, compile and diff checks are green, and global
mypy remains at the accepted **81 errors / 4 files** ceiling. Final commit and fresh pinned review status must be appended against
the stable enclosing candidate. The `a36333b` pinned advisory was `NO-GO 0/1/2/1`; the `6742130` pinned advisory was
`NO-GO 0/3/1/0`; the current fixed-forward is not a verdict. Author evidence or any local advisory output must not be
represented as a formal `GO`.
