# Track D D1c — Action request schema-pin foundation

> Status: Author implementation complete and first review feedback fixed forward (2026-07-14; foundation
> commit=`9a0e051`; fixed-forward commit = this document's enclosing commit). The first pinned non-author attempt at
> `runtime/reviews/20260714T075911Z_Track_D_D1c_request_schema_pin_foundation.md` is **invalid**, because its effective
> evidence reports `causal_binding.final_response_item_exact=false`; its five substantive findings were advisory input,
> not a formal `NO-GO`. The fresh pinned review must bind the fixed-forward hash. Until a hash-bound valid artifact
> exists, formal review remains pending.
> This was a bounded, non-live D1 foundation batch: all 15 production actions were schema-less at its checkpoint.
> D1f now activates exactly three existing-record CRM actions, leaving 12 on this bridge; the served Agent tool
> population remains zero. It is not D1 completion, a formal independent-review `GO`, live-provider approval,
> manual/product signoff, or milestone closure. A fresh pinned non-author review is required after the author commit.
> D1b has only a scope-local advisory `GO`; its formal review remains pending. C2.8 is separately formal-pending after
> an `invalid_transport` review outcome and does not supply review evidence for this scope.

## 1. Outcome and bounded scope

D1c establishes the checked-in request-schema and physical pin lifecycle needed before any production action can be
served to a model. It deliberately does not populate a production schema or expose a tool. The batch adds:

- canonical `ActionRequestSpec` ownership, with `ActionSpec` retained only as an object-identical compatibility alias;
- one strict schema validator and SHA-256 schema digest owner shared with D0 `ToolSpec`;
- owner-minted target references for schema-defined actions, plus rejection of raw caller/model targets and declared
  aliases that could override an owner field;
- physical `request_schema_version` / `request_schema_digest` columns on both `agent_actions` and `operation_runs`;
- submit, immediate-run, approve-run, retry-child, idempotent replay, and pre-dispatch pin copy/verify gates;
- explicit empty/empty physical pins and release-epoch-scoped durable compatibility-hit evidence for the temporary
  schema-less bridge;
- bounded brownfield migration installation (`lock_timeout` + `NOT VALID`) with later constraint validation carried as
  an explicit separate-deployment residual.

The implementation was intentionally exercised with a synthetic schema-defined action. At the D1c checkpoint every
action in `DEFAULT_ACTION_REGISTRY` had `request_schema=None`, an empty version/digest pair, and no served-tool status.
D1f later activates exactly three existing-record CRM actions; 12 still use empty pins and no action has served-tool
status. The existing 12 supported/three unsupported dispatch behavior remains the D1b contract; D1c only inserts a
request-pin preflight before an existing adapter may run.

## 2. Design-obligation disposition

The Track D invariant sweep assigns no open `OB-*` item to D1: **the D1 OB-ID set is `∅`**. The sweep classifies
`ActionRequestSpec` + schema pins as satisfied/`na` at the design level; D1c implements the first bounded runtime slice
of that design without borrowing an OB-ID from D0, D2, or D3.

Track D Plan §6 item 3 is the numbered D1 implementation obligation: a loose action-schema bridge must be recorded in
the residual ledger and `NEXT_TODO` before use. D1c satisfies that bookkeeping obligation through
`RESIDUAL_LEDGER.md` R-029 and the Track D entry in `NEXT_TODO.md`. This is not evidence that the bridge is retired.

Plan §6 item 4 is not closed here. It concerns tool-schema pinning at model-turn creation and propagation through the
terminal result/journal into AgentAction and approve/retry; that remains D0/D2 work. D1c owns only the action-request
schema and the AgentAction→OperationRun physical copy/verify boundary.

## 3. Request schema and field ownership

`ActionRequestSpec.request_schema` must be a closed root object with exactly two required, closed object segments:
`input_payload` and `target_ref`. The two segments cannot declare the same field. A target alias declaration is valid
only for a real target field and cannot collide with an input field, target field, or another alias. Target fields and
aliases are checked-in identifiers: non-string or whitespace-normalized variants are rejected rather than silently
coerced.

For a schema-defined action:

1. caller/model options enter only through `input_payload`;
2. `target_ref` must be empty at the generic submit boundary;
3. the action owner supplies `OwnerBoundTargetRef(owner_module, target_ref)`;
4. the target owner must equal `ActionRequestSpec.owner_module`;
5. the combined request is validated and normalized by the same `ToolSpec.validate_input(...)` implementation used by
   D0 tool parsing;
6. schema version and canonical schema digest are derived from the registry spec, never accepted from the caller.

The HTTP/orchestrator submit boundary rejects the reserved pin field names at both the top level and in caller
metadata. The writer independently rejects them in metadata. A schema-defined action also rejects a caller-supplied
raw target even when an owner-bound target is present. Validation completes before the first durable action write.

## 4. Physical pin lifecycle

| Creation/use point | Required behavior | Fail-closed outcome |
| --- | --- | --- |
| AgentAction submit | derive version/digest from the current checked-in spec; validate the combined request before write | invalid schema, raw/cross-owner target, or reserved pin override writes no action/run/event |
| immediate OperationRun | copy the exact action pin into the new run, then verify equality | mismatched persisted run raises a request-pin conflict |
| approve | before changing action state or appending approval events, compare the persisted action with the current registry and any primary-key/unique-key existing run; then copy/verify the action pin | pre-existing/preflight-observed registry drift or action/run mismatch leaves approval state/events/run unchanged |
| retry child | before requeue/reservation, compare the parent action, parent run, current registry, and any primary-key/unique-key existing child; then copy/verify the action pin | pre-existing/preflight-observed drift writes no requeue, child, or retry events |
| dispatch | revalidate the persisted schema-defined request and compare action/run pins against the current registry before selecting/invoking the adapter | response is `conflict`, `module_state_mutated=false`, and the owner handler is not invoked |
| native PG upsert replay | compare existing and requested version/digest before accepting an identity replay | a different pair is an immutable identity collision |

Migration `0002_action_request_schema_pins.sql` allows only an empty/empty pair or a normalized non-empty version plus a
lowercase 64-hex digest. Installation sets a five-second local lock budget and adds both checks `NOT VALID`: new writes
are guarded immediately without a brownfield table scan, while validation of existing rows remains a later,
separately deployed transaction. A lock-timeout rolls back the migration ledger, both columns, and both constraints.
The follow-up must not be appended as another migration in the same pending runner batch, because the runner applies
all pending files in one transaction. This edit of `0002` is valid only because `9a0e051` was not deployed and the
local read-only audit found zero applied `0002` ledger rows; any external environment that already applied the old
checksum must add a new migration rather than rewrite history.

The database check enforces pair shape; immutable identity is enforced by repository/native upsert comparison and the
action/run preflight. D1c does not claim that arbitrary direct SQL can never replace one otherwise valid non-empty pair
with another.

Ordinary state updates do not update the pin columns. Idempotent action/run replay returns the original row only when
the persisted pins match the current request identity.

## 5. Schema-less compatibility bridge

At the D1c checkpoint all 15 production action specs used the temporary bridge. D1f removes exactly
`set_crm_stage`, `add_crm_note`, and `create_crm_task` from that numerator; the following behavior remains current for
the other 12:

- physical action and run pins are exactly `""` / `""`;
- action metadata records `request_schema_status="schema_less_compatibility"` and
  `request_schema_compatibility_hit=true`;
- the submission event payload carries the same status/hit evidence;
- brownfield or replayed empty/empty actions append one idempotent
  `ActionRequestSchemaCompatibilityObserved` event before replay, approve, retry, or dispatch continues. The event
  distinguishes `pre_d1c_blank_pin_migration` from post-D1c schema-less submission, and its idempotency key/payload
  includes checked-in observation epoch `d1c_r029_20260714_v1`; the epoch must be bumped for each release observation
  window while R-029 remains open;
- a schema-less action preserves its existing caller `target_ref` / `input_payload` semantics and rejects an
  `OwnerBoundTargetRef`, so the strict and compatibility paths cannot silently blend;
- an empty/empty action/run pair still passes dispatch preflight, preserving current open operation behavior.

R-029 is the authoritative residual. The bridge may close only after every API-submittable production action has a
reviewed per-action request schema and owner-bound target binder, and durable compatibility-hit reporting observes zero
hits for the complete API-submittable population for one release window. Measuring only a future served subset is not
sufficient.

## 6. Owner/source-of-truth matrix

| Contract | Owner/source of truth | Consumers | Forbidden source/fallback | Migration/deletion status |
| --- | --- | --- | --- | --- |
| request schema/version | checked-in `ActionRequestSpec` in `ActionRegistry` | HTTP submit, writer validation, dispatch preflight; future planner projection | caller metadata, API fields, persisted metadata mirror | foundation active; three CRM schemas active, 12 schema-less |
| schema digest/validation | D0 `ToolSpec.input_schema_digest` and `ToolSpec.validate_input` | D1 submit/revalidation; future served tool parser | second D1 validator or ad hoc JSON hash | canonical shared owner active |
| target resource identity | action owner through `OwnerBoundTargetRef` | schema-defined submit and dispatch | caller/model raw target or input alias | D1f activates the CRM existing-record binder for three actions; other binders deferred |
| AgentAction physical pin | `OperationRuntimeWriter` derived from current registry at submit | replay, approve, retry, dispatch, audit/API records | caller-supplied pin, metadata-only pin | active columns; empty/empty marks R-029 |
| OperationRun physical pin | run creator copying the linked AgentAction | immediate/approve/retry run replay and dispatch | current-registry re-derivation without action equality | active columns; copy/verify required |
| schema-less hit evidence | action metadata/submission event plus epoch-scoped continuation observation event | residual reporting/audit across submit replay, approve, retry, and dispatch | absence interpreted as strict validation; cross-release event-key reuse | R-029 pending; bump epoch per observation window and require one-window zero-hit deletion gate |
| served Agent population | future full D1 served predicate | future `/api/agent/tool-registry` and planner | adapter presence or command readiness alone | zero; not implemented by D1c |

## 7. Explicit exclusions and residual boundaries

D1c does not implement or claim:

- more than the three later D1f production request schemas, any other production owner-target binder, served predicate, or
  `GET /api/agent/tool-registry`;
- revisioned `model_safe_result_schema`, its validator owner, or simulate-dispatch serializer preflight;
- model-turn/journal/result-slot propagation, tool schema pinning at turn creation, Agent Session, SSE, provider/model
  transport, budget/cost ledger, or live execution;
- a new workflow command type, owner side effect, CRM/projection/person/provider write path, or R-028 closure;
- operation+action+event/command atomicity, retry reservation/child/events atomicity, command generation/lease fencing,
  or a total transaction-lock acquisition budget.
- validation of the two `NOT VALID` request-pin constraints in a separately deployed transaction. The installation
  contract is bounded now, but D1c is not brownfield-validation complete until that follow-up passes against audited
  data; the runner must not apply install and validation in one pending transaction.

Approve and retry now reject request-pin drift before their first write, but their existing multi-step state/event/run
flows are not converted into one transaction. `RESIDUAL_LEDGER.md` R-019 remains open and its remediation requirements
are unchanged; D1c must not be used as evidence that its wider UoW or stale-owner boundaries are closed.
In particular, a competing insert or state change after the read preflight and before the later write can still expose
the pre-existing partial-state window; D1c detects the eventual identity conflict but does not make that race atomic.

R-019's prior tripwire literally blocked the next retry/dispatch touch. The owner's 2026-07-14 direction to continue
the existing Track D plan without waiting for pending review is recorded as a one-batch clarification for D1c's
pre-write request-pin validation/copy/verify. The invalid review then supplied direct new evidence that brownfield
empty/empty continuations were unobservable, so fixed-forward adds exactly one idempotent append-only compatibility
evidence call before replay/approve/retry/dispatch domain mutation or handler invocation. It adds no action/run/command
state mutation or transaction-lock caller, does not raise the 26-call ratchet, and does not authorize any other
retry/dispatch change or any R-019-gated signoff. The event append is explicitly not UoW closure; its failure aborts the
continuation, while wider action/run/event atomicity remains R-019.

## 8. Validation and review handoff

The stable-head author closeout runs the exact D1c request-contract and migration tests plus the existing D1a/D1b,
operation-runtime adjacency, D0 validator adjacency, command-spec, frontend build, lint/format/typecheck, and scoped
whitespace gates. No full `tests/test_pipeline.py`, provider/model/live, W6, nightly, or manual run belongs to this
batch.

```bash
make local-pg-up
SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1 PYTHONPATH=src \
  .venv/bin/python -m pytest -q \
  tests/test_d1_action_request_contract.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py \
  tests/test_d1_frontend_request_schema_pin_contract.py
SOURCING_REQUIRE_PG_DURABLE_RUNTIME_TESTS=1 PYTHONPATH=src \
  .venv/bin/python -m pytest -q tests/test_migration_runner.py
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_operation_runtime.py
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_model_tool_runtime.py
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_command_type_specs.py
make lint
PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/model_tool_runtime.py \
  src/sourcing_agent/operation_runtime.py \
  tests/test_d1_action_request_contract.py
make typecheck
npm run build  # from frontend-demo/
git diff --check -- \
  src/sourcing_agent/model_tool_runtime.py \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  src/sourcing_agent/repositories/workflow_runtime.py \
  src/sourcing_agent/control_plane_live_postgres.py \
  src/sourcing_agent/migrations/0002_action_request_schema_pins.sql \
  frontend-demo/src/lib/api.ts \
  tests/test_d1_action_request_contract.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py \
  tests/test_d1_frontend_request_schema_pin_contract.py \
  tests/test_migration_runner.py \
  tests/test_operation_runtime.py \
  docs/AGENT_OPERATION_CONTRACT.md \
  docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md \
  docs/NEXT_TODO.md \
  docs/RESIDUAL_LEDGER.md \
  docs/TRACK_D_D1C_REQUEST_SCHEMA_PIN_FOUNDATION_IMPLEMENTATION.md
```

Foundation-commit author evidence (`9a0e051`):

- combined D1c + D1a + D1b + frontend pin contract: **54 passed + 18 subtests**;
- migration runner: **4 passed**; exact state-sync ratchet + adjacent frontend contract: **2 passed**;
- complete operation runtime: **129 passed**; D0 model/tool runtime: **109 passed**; command specs: **16 passed**;
- the documented exact PG-backed D1b registry/dispatch/adjacent set: **15 passed**;
- frontend production build: **81 modules transformed**, with only the existing chunk-size warning;
- `make lint`: **58 files already formatted**, Ruff check green; focused mypy: **0 errors / 3 files**;
- global `make typecheck`: the accepted R-011 cap remains exactly **81 errors / 4 files**;
- the R-019 production state-update ratchet remains exactly **26**, and scoped whitespace checks are clean.

The first pinned review attempt used effective `gpt-5.6-sol` / `ultra` / `priority` and produced substantive text, but
the artifact verifier rejected it because `final_response_item_exact=false` (including raw-output/rollout causal
binding mismatch). It therefore establishes neither formal `GO` nor formal `NO-GO`. The fixed-forward batch addresses
the useful content as follows: old empty/empty rows gain real-path epoch-scoped observations; migration installation is
bounded and validation is explicit residual work; `ToolSpec` ownership/strict shape/alias failure propagation is
executable; zero-write snapshots and native unique/PK branches are independent; and frontend mappers execute absent
versus present-empty parity. The request for a fallback repository test was rejected as out of scope because the
runtime tables are PG-only.

Fixed-forward stable-head author evidence:

- D1c + D1a + D1b + executable frontend mapper contract: **78 passed + 48 subtests**;
- migration runner: **7 passed**, including populated `NOT VALID`, five-second lock timeout with complete transaction
  rollback, and later validation alongside a held RowExclusive writer;
- complete operation runtime: **129 passed**; D0 model/tool runtime: **109 passed**; command specs: **16 passed**;
- exact R-019 state-sync ratchet + adjacent frontend contract: **2 passed**, with the caller cap still **26**;
- frontend production build: **81 modules transformed**, with only the existing chunk-size warning;
- `make lint`: **58 files already formatted**, Ruff check green; focused mypy: **0 errors / 3 files**;
- global `make typecheck`: the accepted R-011 cap remains exactly **81 errors / 4 files**;
- scoped whitespace checks are clean. No full pipeline, provider/model/live, W6, nightly, or manual test was run.

The implementation commit is the commit containing this document; the fresh pinned non-author review must bind that
exact hash. Until a hash-bound valid artifact exists, formal status remains pending. Author evidence and D1b's
scope-local advisory `GO` do not constitute D1c formal approval. A pending review blocks only D1c
live/manual/product/milestone signoff; unrelated non-live Track D work may continue.
