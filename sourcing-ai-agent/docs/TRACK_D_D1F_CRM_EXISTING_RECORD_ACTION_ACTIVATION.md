# Track D D1f — CRM existing-record action activation

> Status: Current author implementation candidate (2026-07-15). This bounded non-live batch activates the D1e
> declarations for exactly `set_crm_stage`, `add_crm_note`, and `create_crm_task`, while the other 12 production
> actions remain on the R-029 schema-less bridge and the served Agent tool population remains zero. Local advisory
> rounds reported `NO-GO 0/3/2/0`, `NO-GO 0/2/1/0`, and—against pinned implementation `22055aa`—
> `NO-GO 0/1/2/1`; all are fixed-forward inputs, not formal verdicts. The current fixed-forward candidate closes the
> third round's dangling-operation authorization bypass, falsy/dual input-envelope ambiguity, stale submit replay
> status/HTTP mapping, and duplicate binder allowlist. The prior mutable-version replay, transport, and R-031 findings
> remain fixed or separately bounded. Author validation and a fresh pinned non-author review must bind the enclosing commit before this scope can
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
request state. The caller may supply only the exact selector `{crm_record_id}` plus one caller-owned input envelope:
canonical `input`, or the transport compatibility alias `input_payload`, but never both. Envelope selection is based
on key presence rather than truthiness:

1. every supplied envelope must be an object even if the other envelope is valid; `[]`, string, boolean, number, and
   `null` fail before lookup or persistence;
2. two supplied envelopes—including equal objects or empty objects—fail as
   `action_request_input_alias_ambiguous`; a single empty object reaches the closed action schema and fails its required
   fields without persistence;
3. non-object selectors, selector aliases, mixed owner fields, and target fields or aliases in input fail before
   lookup or persistence;
4. the binder exact-matches the selected `crm_records.workspace_id` and, when populated, `owner_user_id`;
5. authenticated missing, foreign-workspace, and foreign-adjunct records share the same
   `404 {status: not_found, reason: crm_record_not_found}` response and write no action, run, event, command, Activity,
   EntityDelta, or CRM domain row;
6. the orchestrator clears the raw selector and passes only an owner-minted `OwnerBoundTargetRef` to the D1c writer;
7. open-mode operator compatibility remains available only when the explicit operator workspace exact-matches the
   selected CRM row. A missing or different workspace fails with the same not-found result.

The route derives workspace only for this exact three-action set. Other operation actions retain their existing
schema-less/open-mode behavior, including the prior truthy `input`-then-`input_payload` precedence; D1f does not
silently invent owner or alias semantics for those R-029 actions.

An initial accepted submission returns its newly created `queued` or `approval_required` state with HTTP 202.
A preflight-observed exact replay, or a committed result already outside the fresh lifecycle set, returns HTTP 200 plus
`idempotent_replay=true` and the persisted current lifecycle state: the OperationRun status when a run exists,
otherwise the AgentAction status. The public closed set is
`approval_required|queued|planned|running|completed|failed|cancelled|rejected`; `rejected` is brownfield compatibility,
while a canonical reject persists `status=cancelled + approval_status=rejected`. Any unknown persisted status fails
closed as `operation_submission_current_status_invalid` before replay events or state writes. Stable `planned|running`
or non-rejection terminal actions without a run, terminal action/run mismatches, and rejection actions that still link
to a run fail as `operation_submission_state_incoherent` before writes. Every submission derives and preflights the
deterministic run, including the first approval-required submit, so an orphan run cannot be retroactively legitimized.
Static required-policy runs require an approved action. Conditional approval on a non-required policy already has its
own pending/cancel/retry lifecycle; D1f preserves that owner path and does not mistake retained approval metadata for
corruption. Initial
`approval_required` action-only state
and action-only canonical/legacy rejection remain legal; a non-approval `queued` partial submit may recreate only the
same deterministic run. Approved-action replay always loads and validates that run before compatibility evidence or
upsert. The nested action/run and top-level status therefore cannot report a preflight-visible completed effect as
newly queued.

The submit transport/schema/TypeScript adapter is a discriminated union. Fresh `queued|approval_required` requires
literal `idempotent_replay=false`; replay permits the full closed lifecycle set only with literal `true`. Missing,
non-boolean, or false replay markers on replay-only statuses fail closed. A simultaneous same-state
`queued|approval_required` insert collision remains the R-019 preflight/write residual because the repository upsert
does not yet return inserted-vs-existing provenance; it may still return a fresh-compatible marker/status, and this
batch does not claim that concurrency oracle.

## 4. Dispatch and command-owner revalidation

On a first dispatch that would create new state, the orchestrator revalidates the persisted target snapshot before any
approval transition or command-plan write. Owner loss returns `not_found/crm_record_not_found`; same-owner version drift
returns `conflict/crm_record_target_stale`. Both leave action/run/event/command/CRM state unchanged. Exact replay of an
already persisted command plan remains read-only.

The planned CRM command carries the full target snapshot in `crm_record_target` and exactly one matching
`crm_record_ids` value. Input aliases and prior bulk/list fallbacks are not accepted for these single-record actions.
Before the first CRM domain effect, the CRM command owner:

1. reconciles the physical `workflow_command.operation_id` and payload `operation_run_id`: when both are populated
   they must be identical, and any populated-but-missing id fails as `crm_record_command_operation_missing` before
   action-label inspection;
2. follows the reconciled canonical `OperationRun -> AgentAction` link;
3. verifies that linked action's type, schema pins, operation/action/workspace identity, expected command type, exact
   persisted target snapshot, and singleton record id;
4. revalidates current CRM workspace/owner/version through `CRMRecordTargetBinder`;
5. only then invokes the existing CRM writer.

The linked persisted action is the discriminator. A mutable command-payload `action_type` cannot select or bypass the
fence; missing/mismatched labels fail closed. Owner-internal legacy commands without a canonical Operation link retain
their pre-D1f compatibility path only when both operation carriers are genuinely absent. Agent-created commands cannot
reach that branch through normal writers because physical `workflow_commands.operation_id` is insert-owned and is not
cleared by payload updates. A missing referenced run can never re-enter the path by deleting or changing the mutable
payload label.

## 5. Compatibility and residual boundaries

- Brownfield empty-pin rows for these three now-schema-defined actions fail closed. They do not re-enter the R-029
  compatibility continuation path.
- R-029 decreases from 15 to **12** schema-less production actions but remains open. Its release-window hit audit,
  separately deployed `NOT VALID` validation, and complete API-submittable-population deletion condition do not change.
  D1f bumps the observation epoch to `d1f_r029_20260715_v2` and makes submit-replay evidence action-scoped; changing
  the carrier beneath the v1 idempotency key is forbidden because v1 run-carried evidence may already be durable.
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
| exact activated action set | `operation_runtime.CRM_EXISTING_RECORD_ACTION_TYPES` plus `DEFAULT_ACTION_REGISTRY` | API bind decision, target-binder factory, schema characterization, dispatch/command fences | route/binder-local duplicate action list | 3 active / 12 bridge |
| closed request shape/version/digest | D1e declarations copied into `ActionRequestSpec` | submit validation, persisted pins, dispatch | caller metadata, planner copy | active for exact three |
| stable replay target identity | `ActionRequestSpec.request_identity_target_fields` | default idempotency key and persisted replay comparison | full mutable owner/version snapshot | `crm_record_id + workspace_id` |
| authorization/execution snapshot | `CRMRecordTargetBinder` canonical row read | action persistence, command payload, dispatch and command preflights | caller/model target, command label | four-field snapshot active |
| authenticated bind context | request authentication state at the HTTP route | exact three CRM submit actions | payload workspace/user | active only for exact three |
| missing/foreign transport shape | operation submit route | authenticated/open-mode CRM submit | existence-revealing foreign response | one 404 body |
| input envelope selection | exact-three CRM submit branch | schema validator and persisted input | truthiness fallback or dual-envelope precedence | presence-based; at most one object |
| submit replay outcome | `operation_submission_current_status` closed lifecycle/coherence sets + persisted action/run; submit-specific true/false discriminated union | HTTP status mapper and frontend adapter | synthetic `queued`, arbitrary persisted string, or optional/inferred replay marker | fresh 202/false; observed replay 200/true/current; concurrent fresh-state ambiguity remains R-019 |
| schema-less continuation evidence | action-scoped `ActionRequestSchemaCompatibilityObserved` under epoch `d1f_r029_20260715_v2` | remaining 12 schema-less action continuations and R-029 audit | changing carrier under durable v1 key | v2 active; R-029 open |
| command action discriminator | reconciled physical/payload operation id plus canonical linked OperationRun and AgentAction | CRM command owner | mutable command-payload label or dangling-link legacy fallback | active for exact three |
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
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d3_workflow_command_public_projection.py::test_frontend_schema_and_mappers_are_closed_and_operation_sync_is_typed \
  tests/test_d3_workflow_command_public_projection.py::test_frontend_action_methods_compile_with_exact_status_outcomes \
  tests/test_d3_workflow_command_public_projection.py::test_frontend_mappers_executably_drop_unknown_and_nested_capability_fields
npm --prefix frontend-demo run build
make lint
make typecheck
git diff --check -- \
  PROGRESS.md \
  contracts/frontend_api_adapter.ts \
  contracts/frontend_api_contract.schema.json \
  contracts/frontend_api_contract.ts \
  contracts/frontend_api_runtime_contract.ts \
  src/sourcing_agent/action_target_binding.py \
  src/sourcing_agent/api.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/frontend_api_action_status_types.test.ts \
  tests/test_api_request_scope.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1d_projection_action_binder_decision.py \
  tests/test_d1e_crm_existing_record_action_schemas.py \
  tests/test_d1f_crm_existing_record_action_activation.py \
  tests/test_d3_workflow_command_public_projection.py \
  docs/TRACK_D_D1F_CRM_EXISTING_RECORD_ACTION_ACTIVATION.md \
  docs/AGENT_OPERATION_CONTRACT.md \
  docs/FRONTEND_API_CONTRACT.md \
  docs/TRACK_D_AGENT_RUNTIME_PLAN.md \
  docs/NEXT_TODO.md \
  docs/RESIDUAL_LEDGER.md \
  docs/INDEX.md
```

Final author evidence on the stable worktree:

- exact D1f PG matrix: **15 passed + 80 subtests**;
- D1 request/schema/binder/transport adjacency: **119 passed + 189 subtests**;
- full operation runtime: **136 passed + 503 subtests**;
- frontend response schema/type/adapter executable nodes: **3 passed**; production frontend build: **84 modules**
  transformed and built (the existing `>500 kB` chunk warning remains non-blocking);
- repo lint/format: **58 files** green; final diff checks are green; global mypy remains at the accepted
  **81 errors / 4 files** ceiling (non-zero by baseline), with no new error at this batch's changed contracts.

The local advisory rounds `0/3/2/0`, `0/2/1/0`, and pinned-`22055aa` `0/1/2/1` are fixed-forward inputs only. They
cannot be promoted to formal verdicts, and author tests cannot replace a fresh pinned non-author review. Pending review
blocks this scope's live/manual/product/milestone signoff only; unrelated non-live Track D work may continue
asynchronously.
