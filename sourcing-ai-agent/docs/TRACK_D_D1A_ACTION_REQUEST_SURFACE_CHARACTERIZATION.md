# Track D D1a — Action request surface characterize-first freeze

> Status: Author characterization batch with pinned-review fixed-forward (2026-07-14). The review of pinned commit
> `d88161e` returned `NO-GO` for two false-green oracles; both are addressed in `bbad0fa`, whose scoped pinned advisory
> re-review returned `GO` (P0/P1/P2=0/0/0). Formal review remains pending. This is a zero-product-code, non-live
> baseline consumed by the later `ActionRequestSpec` implementation. It is not a D1 completion claim, formal independent-review
> `GO`, live-provider approval, or Agent tool activation.
>
> D1b follow-up: the bounded non-live implementation recorded in
> `TRACK_D_D1B_DISPATCH_ADAPTER_REGISTRY_IMPLEMENTATION.md` intentionally replaces only the hard-coded dispatch
> classification characterized here. D1c then replaces the permissive pre-schema submission baseline with the bounded
> foundation recorded in `TRACK_D_D1C_REQUEST_SCHEMA_PIN_FOUNDATION_IMPLEMENTATION.md`; all 15 production actions still
> used its explicit schema-less bridge at the D1c checkpoint. D1e then declared three existing-record CRM contracts,
> and the current D1f candidate activates exactly those three with authenticated owner binding and two execution-side
> revalidations. The current partition is 3 schema-defined / 12 schema-less; the served population remains zero.

## 1. Outcome and boundary

D1a freezes the action/request/dispatch surface that existed immediately before D1 adds a versioned request schema.
The batch adds one AST/runtime characterization suite and this record; it does not modify
`ActionSpec`, `ActionRegistry`, `OperationRuntimeWriter`, `SourcingOrchestrator`, a command owner, storage, migration,
API, provider/model transport, or frontend behavior.

The inventory is mechanically discovered from the runtime registry and module action constants instead of maintained
as an independent numeric allowlist. At this commit it observes:

- 15 registered action types, all paired one-for-one with the 15 `ACTION_*` module constants;
- 11 actions with a workflow-command surface;
- 18 action-to-command references covering 17 unique command types (one command is intentionally exposed by two
  actions);
- 12 actions recognized by the current operation dispatch branches and three returning `unsupported`.

These numbers are a dated observation, not the contract. The tests compare the discovered populations and semantic
sets, so a new action or command cannot silently preserve a stale `12/15` assertion.

The current `external_intake` fact is narrower than the older shorthand: it is a registered action with default and
allowed command `excel.intake.run`; that command has a registered owner, required Agent-callable Activity spine, and
fail-closed owner-specific control policy. However, `external_intake` itself is not selected by
`_dispatch_operation_run_from_records`, so a submitted operation returns `unsupported`. Command metadata readiness
therefore does not prove an action dispatch adapter exists. D1b now records that relation explicitly on `ActionSpec`
rather than inferring it from registration or from `allowed_workflow_command_types`; `external_intake` retains an empty
adapter and remains unsupported.

## 2. Characterized contracts

| Surface | Current owner | D1a freezes | Known D1 debt |
|---|---|---|---|
| Action metadata | `operation_runtime.ActionRequestSpec` (`ActionSpec` is an object-identical alias) | D1a froze the exact ten-field pre-schema surface | D1b added `dispatch_adapter`; D1c adds `request_schema`, `request_schema_version`, and target aliases; model-safe result schema remains deferred |
| Registry serialization | `operation_runtime.ActionRegistry.to_record()` | action constant↔registry parity; exact compact record keys; optional command contract projection | adapter and request schema remain internal; physical pins live on action/run rows rather than this public registry record |
| Submission | `OperationRuntimeWriter.submit_action` | D1a froze the old complete signature/call inventory, fail-closed gates, payload replay fence, write order, and approval boundary | D1c intentionally updates the signature/inventory and validates schema-defined requests before write; D1f activates three CRM existing-record schemas while the other 12 remain on the recorded bridge |
| Dispatch | `ActionRegistry` declaration + `SourcingOrchestrator` adapter bindings | D1a froze the old branch classification for every discovered action; D1b preserves the same 12 supported/three unsupported result | D1c adds pin/request preflight before adapter invocation; the full served-tool predicate remains deferred |
| Command exposure mirror | `_agent_callable_workflow_command_types_for_action` | exact set projection of `ActionSpec.allowed_workflow_command_types`; unknown action returns empty | no served-tool predicate or result-schema gate |
| Command plan selection | `_build_agent_callable_workflow_command_plan` | command selection is `input.command_type` → `target.command_type` → registry default; a present but disallowed higher-priority value fails closed instead of falling back | input and target remain dual behavior-driving sources pending D1 normalization |
| Command execution contracts | command owner registry + Activity/control policy registries | every exposed command resolves to the same owner; Activity policy is Agent-callable and non-legacy; Activity and control records are fail-closed | this proves command readiness only, not action adapter or model-safe output readiness |

The start-acquisition runtime probe pins the complete current query chain with adjacent sentinels:
`input.query` > `input.raw_user_request` > `target_ref.query` > selected nested `raw_user_request` > selected nested
`query`. The selected nested payload is the first truthy whole mapping from
`input.workflow_payload` > `target_ref.workflow_payload` > `input.command_payload.workflow_payload`; the implementation
does not merge those three nested mappings. Separate company/query cases prove direct input beats target, target beats a
selected nested payload, and nested values are used when both direct sources are absent. This documents the bypass
surface that the D1 design intends to remove; it does not endorse preserving dual-source request semantics after the
schema owner is introduced.

## 3. Submission and dispatch baseline

`submit_action` currently performs this ordered persistence flow:

1. resolve the action from `ActionRegistry`;
2. normalize workspace/idempotency and reject a missing required budget;
3. upsert the action and compare persisted request identity with the submitted identity;
4. append `ActionApprovalRequired` or `AgentActionQueued`;
5. return immediately for approval-required actions; otherwise upsert a queued `OperationRun` and append
   `OperationRunQueued`.

It neither dispatches an owner adapter nor plans a workflow command. The characterization deliberately submits
synthetic unknown nested fields and proves they are preserved byte-for-structure today. When D1 adds request validation,
that permissive-baseline assertion must be intentionally replaced by positive and negative schema cases; weakening it
without installing the new owner would hide an intermediate contract gap.

The D1a runtime dispatch inventory, now represented by `ActionSpec.dispatch_adapter`, is:

- projection read adapter: `search_projection`, `filter_projection`;
- person Public Web adapter: `enrich_person_public_web`;
- export adapter: `export_candidates`;
- generic Agent-callable workflow-command adapter: `start_acquisition_run`, `fetch_profile_sample`,
  `continue_acquisition_run`, `refresh_company_public_web_assets`;
- CRM writer adapter: `add_to_crm`, `set_crm_stage`, `add_crm_note`, `create_crm_task`;
- unsupported: `plan_acquisition`, `promote_person_assertion`, `external_intake`.

This list is asserted against the complete auto-discovered registry population. D1b makes the registry value the
runtime selector and keeps a separate explicit five-entry adapter-to-bound-method map. A newly registered action
therefore fails the suite until its intended adapter/unsupported disposition is reviewed; an unknown adapter is
rejected during registry construction, and a missing runtime binding fails closed as `unsupported`.

## 4. Mutation sensitivity

The suite fails when any of these current facts drift without an intentional D1 update:

1. an `ACTION_*` constant and the runtime registry stop matching, or the characterized/current
   `ActionRequestSpec`/compact-record fields change;
2. a registered command loses its owner, becomes legacy/non-Agent-callable on the Activity spine, or stops exposing a
   fail-closed control record;
3. `submit_action` changes any positional-only/positional/vararg/keyword-only/kwarg parameter or annotation/default,
   return annotation, decorator, complete call expression structure, repository write order, early validation gate,
   approval boundary, or starts a callback/outbox/dispatch/planning hook. In-memory mutations add `**kwargs` and an
   indirect `runner(action)` call; both are rejected. Fail-closed runtime namespaces also reject unexpected writer,
   store, or repository/outbox access;
4. a registered action moves between adapter families, a registry adapter has no explicit runtime binding, dispatch
   regresses to action-name branches or arbitrary `getattr`, or `external_intake` command metadata is accidentally
   treated as an executable action adapter;
5. command-type precedence changes. An in-memory source mutation swaps input/target order and proves the AST oracle
   rejects the mutation; a second mutation swaps `input.raw_user_request` with `target_ref.query` and is likewise
   rejected; no product source is edited;
6. any adjacent start-acquisition query sentinel or nested source priority changes, or a disallowed input/target command
   starts falling through to a lower-priority/default command.

The probes use synthetic dictionaries and local stubs only. They create no PG rows, files, network traffic, provider
requests, model calls, or external side effects.

## 5. D1 continuation gates

D1c now provides the single validator/digest owner, strict two-segment schema form, owner-bound target wrapper, physical
action/run pins, submit/approve/retry/dispatch copy/verify foundation, and epoch-scoped brownfield compatibility
observations. The remaining D1 work must still:

- define reviewed per-action schemas and owner target binders for the remaining 12 production actions, then retire the
  R-029 schema-less bridge only after all API-submittable actions record zero compatibility hits for one release window; bump the
  checked-in observation epoch for each window and validate the installed `NOT VALID` checks in a separate deployment;
- use the D1b registry-owned adapter as one necessary served-subset input, then derive the served subset only after
  schema + Activity + revisioned model-safe result schema + simulate serializer preflight also exist;
- expose the future Agent tool registry from that full predicate without treating action registration, adapter presence,
  or command readiness as served readiness;
- remain non-live until the typed model-turn owner, cost/budget/result-slot obligations, and a scope-matched independent
  review permit activation.

The pending formal review remains scope-local: it freezes D1a live/manual/product/milestone signoff, not unrelated
non-live Track C or D work. Author tests and the scoped pinned advisory `GO` are evidence, not formal `GO`.

## 6. Validation

Run from `sourcing-ai-agent/`:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1_action_request_surface_characterization.py
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_action_registry_unknown_action_fails_closed \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_action_registry_rejects_unregistered_workflow_commands \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_action_registry_exposes_workflow_command_contract_for_agent_callable_actions \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_read_only_projection_action_creates_idempotent_operation_without_module_side_effects \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_approval_required_action_does_not_create_operation_run_before_approval \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_budget_required_action_requires_explicit_budget \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_start_acquisition_operation_plans_root_acquisition_run_command_only \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_continue_acquisition_rejects_commands_outside_action_registry_allowlist
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_command_type_specs.py
.venv/bin/ruff check tests/test_d1_action_request_surface_characterization.py
.venv/bin/ruff format --check tests/test_d1_action_request_surface_characterization.py
PYTHONPATH=src .venv/bin/mypy --follow-imports=skip --ignore-missing-imports \
  tests/test_d1_action_request_surface_characterization.py
git diff --check -- \
  tests/test_d1_action_request_surface_characterization.py \
  docs/TRACK_D_D1A_ACTION_REQUEST_SURFACE_CHARACTERIZATION.md \
  docs/NEXT_TODO.md \
  docs/INDEX.md
```

Author evidence on 2026-07-14: the new characterization passed 6 tests; eight exact existing OperationRuntime nodes
passed against local PG; the command-spec owner/policy suite passed 16 tests; focused mypy, Ruff format/check, and the
scoped whitespace check passed. The broader `tests/test_markdown_status.py` run returned one existing failure because
15 already tracked `docs/pro-consults/**` Markdown artifacts lack the repository status banner; the second test passed,
the D1a document itself has a first-eight-line status banner, and this scoped batch did not modify those artifacts.
