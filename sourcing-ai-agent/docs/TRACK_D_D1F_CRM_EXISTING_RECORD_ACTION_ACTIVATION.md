# Track D D1f — CRM existing-record action activation

> Status: Current author implementation candidate (2026-07-15). This bounded non-live batch activates the D1e
> declarations for exactly `set_crm_stage`, `add_crm_note`, and `create_crm_task`, while the other 12 production
> actions remain on the R-029 schema-less bridge and the served Agent tool population remains zero. Local advisory
> rounds reported `NO-GO 0/3/2/0` then `NO-GO 0/2/1/0`; both are fixed-forward inputs, not formal verdicts. The
> mutable-version replay, payload-label command bypass, transport, and input-precedence findings are fixed; the second
> round's remaining IDOR/R-028 bookkeeping is now explicit as R-031 plus retained R-019/R-028 annotations. Author
> validation and a fresh pinned non-author review must bind the enclosing commit before this scope can
> enter live/manual/product/milestone signoff. No provider, model, or live environment is used by this batch.

## 1. Outcome and bounded scope

D1f closes the activation gate recorded by D1e without broadening the served surface. The production registry now has
this exact partition:

- schema-defined: `set_crm_stage`, `add_crm_note`, `create_crm_task` (**3**);
- schema-less compatibility bridge: every other production action (**12**);
- served Agent tools: **0**.

The three actions reuse D1e's closed request schemas and `CRMRecordTargetBinder`. D1f adds the shared authenticated
HTTP/orchestrator bind context, activates the declarations in `DEFAULT_ACTION_REGISTRY`, persists the owner-minted
target snapshot, and revalidates it before a new command plan and before the first CRM domain effect. It does not add a
result schema, simulate serializer, served registry, provider/model call, storage migration, new workflow command,
transaction-lock caller, or new CRM domain writer. `add_to_crm` is unchanged and remains schema-less.

## 2. Request identity and owner snapshot

The owner-minted target snapshot remains the complete authorization/execution pin:

```json
{
  "crm_record_id": "crmrec_...",
  "workspace_id": "user-alice",
  "owner_user_id": "alice",
  "crm_version": 3
}
```

The persisted snapshot and all execution-side revalidation retain all four fields. Request replay identity is
deliberately narrower and stable: only `crm_record_id + workspace_id` participate. `owner_user_id` is a mutable adjunct
authorization pin and `crm_version` is a mutable optimistic-concurrency pin; treating either as new caller intent would
make an exact idempotent replay collide after the action's own successful effect increments the record version.

`ActionRequestSpec.request_identity_target_fields` owns this projection. It must be a normalized, duplicate-free subset
of the declared target fields. An empty declaration preserves D1c's original all-target-fields behavior for other
schema-defined actions. This projection is part of the versioned request contract: changing it requires a new
`request_schema_version` even when the JSON field schema itself is unchanged. The full target snapshot is still validated, persisted, copied into the command payload, and
revalidated; the stable identity projection is not an authorization fallback.

## 3. Submit authorization and transport

For the three activated actions, `POST /api/operations/actions` derives the workspace and user from authenticated
request state. The caller may supply only the exact selector `{crm_record_id}` plus the action's caller-owned
`input_payload`:

1. non-object selectors/input, selector aliases, mixed owner fields, and target fields or aliases in input fail before
   lookup or persistence;
2. the binder exact-matches the selected `crm_records.workspace_id` and, when populated, `owner_user_id`;
3. authenticated missing, foreign-workspace, and foreign-adjunct records share the same
   `404 {status: not_found, reason: crm_record_not_found}` response and write no action, run, event, command, Activity,
   EntityDelta, or CRM domain row;
4. the orchestrator clears the raw selector and passes only an owner-minted `OwnerBoundTargetRef` to the D1c writer;
5. open-mode operator compatibility remains available only when the explicit operator workspace exact-matches the
   selected CRM row. A missing or different workspace fails with the same not-found result.

The route derives workspace only for this exact three-action set. Other operation actions retain their existing
schema-less/open-mode behavior; D1f does not silently invent owner semantics for them.

## 4. Dispatch and command-owner revalidation

On a first dispatch that would create new state, the orchestrator revalidates the persisted target snapshot before any
approval transition or command-plan write. Owner loss returns `not_found/crm_record_not_found`; same-owner version drift
returns `conflict/crm_record_target_stale`. Both leave action/run/event/command/CRM state unchanged. Exact replay of an
already persisted command plan remains read-only.

The planned CRM command carries the full target snapshot in `crm_record_target` and exactly one matching
`crm_record_ids` value. Input aliases and prior bulk/list fallbacks are not accepted for these single-record actions.
Before the first CRM domain effect, the CRM command owner:

1. follows the canonical `workflow_command.operation_id -> OperationRun -> AgentAction` link;
2. verifies that linked action's type, schema pins, operation/action/workspace identity, expected command type, exact
   persisted target snapshot, and singleton record id;
3. revalidates current CRM workspace/owner/version through `CRMRecordTargetBinder`;
4. only then invokes the existing CRM writer.

The linked persisted action is the discriminator. A mutable command-payload `action_type` cannot select or bypass the
fence; missing/mismatched labels fail closed. Owner-internal legacy commands without a canonical Operation link retain
their pre-D1f compatibility path, and that path is selected by the absence of a canonical operation rather than by a
caller-controlled label.

## 5. Compatibility and residual boundaries

- Brownfield empty-pin rows for these three now-schema-defined actions fail closed. They do not re-enter the R-029
  compatibility continuation path.
- R-029 decreases from 15 to **12** schema-less production actions but remains open. Its release-window hit audit,
  separately deployed `NOT VALID` validation, and complete API-submittable-population deletion condition do not change.
- Served population remains zero because revisioned model-safe result schemas, result validation, and simulate
  serializer preflight have not landed.
- D1f adds read-only owner/version preflights. It does **not** put command claim/terminal state, CRM effect,
  Activity/Attempt, EntityDelta, and linked Operation completion in one PG UoW; it does not claim complete TOCTOU or
  exactly-once closure. R-028 and R-019 remain open.
- Authenticated list/get/provenance/approve/reject/dispatch/resume/retry/cancel operation routes are not all bound to a
  server-derived exact workspace in this batch. That IDOR boundary is recorded separately in R-031 and blocks hosted or
  live multi-user Operation exposure, but not bounded non-live implementation or local open-mode testing.

Follow-up: D1g (`TRACK_D_D1G_OPERATION_API_EXACT_OWNER_CLOSURE.md`) remediates that downstream Operation API boundary
in a later candidate. This D1f record intentionally preserves its original scope and does not inherit D1g's review
status or broaden D1f's authorization claim.

## 6. Owner/source-of-truth matrix

| contract | owner/source of truth | normal consumers | forbidden source/fallback | status |
| --- | --- | --- | --- | --- |
| exact activated action set | `operation_runtime.CRM_EXISTING_RECORD_ACTION_TYPES` plus `DEFAULT_ACTION_REGISTRY` | API bind decision, schema characterization, dispatch/command fences | route-local duplicate action list | 3 active / 12 bridge |
| closed request shape/version/digest | D1e declarations copied into `ActionRequestSpec` | submit validation, persisted pins, dispatch | caller metadata, planner copy | active for exact three |
| stable replay target identity | `ActionRequestSpec.request_identity_target_fields` | default idempotency key and persisted replay comparison | full mutable owner/version snapshot | `crm_record_id + workspace_id` |
| authorization/execution snapshot | `CRMRecordTargetBinder` canonical row read | action persistence, command payload, dispatch and command preflights | caller/model target, command label | four-field snapshot active |
| authenticated bind context | request authentication state at the HTTP route | exact three CRM submit actions | payload workspace/user | active only for exact three |
| missing/foreign transport shape | operation submit route | authenticated/open-mode CRM submit | existence-revealing foreign response | one 404 body |
| command action discriminator | canonical linked OperationRun and AgentAction | CRM command owner | mutable command-payload label | active for exact three |
| CRM effect/UoW | existing CRM writer and command completion owners | CRM domain state and operation completion | read-only preflight described as transaction fence | R-028 open |
| served tool predicate | future full D1 predicate | future tool registry/planner | schema or adapter presence alone | zero |

## 7. Verification and review handoff

Run from `sourcing-ai-agent/`; record exact counts only after the final stable tree is known:

```bash
make local-pg-up
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1f_crm_existing_record_action_activation.py \
  tests/test_d1e_crm_existing_record_action_schemas.py \
  tests/test_d1_action_request_contract.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1d_projection_action_binder_decision.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_api_request_scope.py
.venv/bin/ruff check \
  src/sourcing_agent/action_target_binding.py \
  src/sourcing_agent/api.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/test_d1f_crm_existing_record_action_activation.py
.venv/bin/ruff format --check \
  src/sourcing_agent/action_target_binding.py \
  src/sourcing_agent/api.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/test_d1f_crm_existing_record_action_activation.py
make typecheck
git diff --check -- \
  src/sourcing_agent/action_target_binding.py \
  src/sourcing_agent/api.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/test_d1f_crm_existing_record_action_activation.py \
  docs/TRACK_D_D1F_CRM_EXISTING_RECORD_ACTION_ACTIVATION.md \
  docs/AGENT_OPERATION_CONTRACT.md \
  docs/TRACK_D_AGENT_RUNTIME_PLAN.md \
  docs/NEXT_TODO.md \
  docs/RESIDUAL_LEDGER.md \
  docs/INDEX.md
```

Final author evidence on the stable worktree:

- exact D1f PG matrix: **7 passed + 27 subtests**;
- D1 request/adapter/binder/transport/CRM adjacency plus the R-019 caller ratchet: **136 passed + 136 subtests**;
- full operation runtime: **136 passed + 503 subtests**;
- the adjacent frontend request-pin suite: **1 pre-existing failure**, reproduced identically on clean detached
  `HEAD=c260896` with the same exact node; this batch does not modify that frontend mapper or its test;
- Ruff and format checks, Python compilation, and final diff checks are green; global mypy remains at the accepted
  **81 errors / 4 files** ceiling (non-zero by baseline).

The local advisory rounds `0/3/2/0` and `0/2/1/0` are fixed-forward inputs only. They cannot be promoted to formal
verdicts, and author tests cannot replace a fresh pinned non-author review. Pending review blocks this scope's
live/manual/product/milestone signoff only; unrelated non-live Track D work may continue asynchronously.
