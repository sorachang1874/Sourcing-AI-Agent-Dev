# Track D D1e — CRM existing-record action schema/binder foundation

> Status: `declared_not_activated` (2026-07-15). This bounded non-live batch declares and validates the first three
> production action request contracts and their canonical owner binder, but deliberately does **not** copy them into
> `DEFAULT_ACTION_REGISTRY`. Activation must be atomic with the shared HTTP/orchestrator binder and execution-side
> exact owner/version revalidation. The production registry therefore remains 15/15 schema-less, R-029 remains open,
> and the served Agent tool population remains zero.

## 1. Outcome

D1e introduces the reusable, closed `ActionRequestSchemaBuilder`, a typed `ActionBindContext`, and the closed
`ActionTargetBinderRegistry`. It declares request contracts for the existing-record CRM actions:

| action | caller-owned input | required input | schema version |
| --- | --- | --- | --- |
| `set_crm_stage` | `stage`, optional `quality_score`, optional `comment` | `stage` | `crm_set_stage_request_v1` |
| `add_crm_note` | `note` | `note` | `crm_add_note_request_v1` |
| `create_crm_task` | `title`, optional `description`, optional `due_at` | `title` | `crm_create_task_request_v1` |

Every declaration uses the D1c-required closed root with required closed `input_payload` and `target_ref` segments.
Caller input cannot contain `crm_record_id`, its legacy aliases, workspace/tenant identity, owner identity, or record
version. Text and numeric fields have bounded model-facing shapes; stage values come from the shared CRM field
contract rather than a third enum copy.

## 2. Target owner and binder

The canonical target owner is the physical `crm_records.workspace_id`. `crm_records.owner_user_id` is an adjunct:
authenticated binding requires the exact caller-derived workspace and, when the stored adjunct is populated, the
same user id. A blank adjunct remains compatible for modern rows created before the redundant column was wired.

The binder receives a server-owned context and exact `crm_record_id` lookup hint. It reads the canonical CRM row and
mints this immutable target snapshot:

```json
{
  "crm_record_id": "crmrec_...",
  "workspace_id": "user-alice",
  "owner_user_id": "alice",
  "crm_version": 3
}
```

Authenticated missing, foreign-workspace, and foreign-adjunct rows all fail with `crm_record_not_found`. Open-mode
operator compatibility remains available, but only when the selected row exactly matches the explicit operator
workspace. Selector aliases or mixed selector/owner fields fail before a row lookup. Unknown actions and cross-owner
binder results fail closed.

`CRMRecordTargetBinder.revalidate_snapshot(...)` supplies the common pre-command exact owner/version check. Owner loss
uses `crm_record_not_found`; a same-owner version change uses `crm_record_target_stale`. This method is a preflight,
not an atomic command-effect claim.

## 3. Activation gate

The declarations in `CRM_EXISTING_RECORD_ACTION_REQUEST_CONTRACTS` are intentionally not active in
`DEFAULT_ACTION_REGISTRY`. Activating them without the shared adapter would make the current generic submit path pass
raw caller `target_ref` into `OperationRuntimeWriter`, which correctly rejects schema-defined actions. D1e therefore
keeps the existing API behavior green and freezes this exact atomic follow-up:

1. the authenticated HTTP route derives workspace and user identity from request state; open mode derives an explicit
   operator context without fabricating a user;
2. the orchestrator consumes the CRM record id only as a binder lookup hint, rejects caller target/owner/version
   aliases, calls the registered owner binder, clears raw `target_ref`, and passes `OwnerBoundTargetRef` to the writer;
3. dispatch revalidates the persisted owner/version snapshot before the first command-plan write;
4. the command payload carries the exact target pins and the CRM command owner revalidates them before its first
   domain effect. Existing R-028 command-effect atomicity limits remain explicit; a read-only preflight must not be
   described as a complete TOCTOU fence;
5. only in that same patch are the three declarations copied into `DEFAULT_ACTION_REGISTRY`, existing direct/open and
   authenticated transport tests migrated, and the R-029 denominator changed from 15 to 12;
6. missing/foreign/forged, same-owner, blank-adjunct, open-mode, replay, schema collision, and stale-version matrices
   must prove action/run/event/command/CRM zero writes where applicable.

Until all six items land together, `declared_not_activated` is the only valid status. This batch does not add a served
predicate, result schema, simulate serializer preflight, model/provider call, new workflow command, API route, storage
migration, transaction-lock caller, or CRM domain write.

## 4. Owner/source-of-truth matrix

| contract | owner/source of truth | forbidden source | current status |
| --- | --- | --- | --- |
| closed request shape | `action_request_schema.ActionRequestSchemaBuilder` | hand-written open root/segment | active utility |
| CRM stage values | `crm_contract.CRM_STAGE_VALUES` / `CRM_STAGE_CATEGORIES` | copies in writer, storage, or action schema | centralized |
| three request declarations | `operation_runtime.CRM_EXISTING_RECORD_ACTION_REQUEST_CONTRACTS` | API payload metadata or planner copy | declared, not activated |
| CRM target authorization | `crm_records.workspace_id`; populated `owner_user_id` adjunct | caller workspace/owner, legacy read allowance | binder implemented |
| target identity snapshot | `CRMRecordTargetBinder` exact canonical row | raw caller/model `target_ref` | binder implemented |
| production request spec | future atomic copy into `DEFAULT_ACTION_REGISTRY` | declaration presence inferred as activation | pending shared batch |
| served tool population | future full D1 predicate | schema or adapter presence alone | zero |

## 5. Verification

The focused test module covers closed builder behavior, exact versions/digests, field and alias rejection, target
immutability, registry totality, missing/foreign parity, blank-adjunct compatibility, open-mode exact workspace,
forged-selector zero lookup, cross-owner rejection, snapshot revalidation, and a real-PG bind/submit/replay/zero-write
matrix using an isolated declared-spec registry. Adjacent D1a/D1c characterization continues to assert that the
production registry is 15/15 schema-less.

Run from `sourcing-ai-agent/`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1e_crm_existing_record_action_schemas.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1_action_request_contract.py \
  tests/test_d1_action_request_surface_characterization.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_sensitive_crm_stage_operation_requires_approval_before_command_planning \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_create_crm_task_operation_materializes_task_only_in_command_owner
```
