# Track D D1a — Action request surface characterize-first freeze

> Status: Author characterization batch with pinned-review fixed-forward (2026-07-14). The review of pinned commit
> `d88161e` returned `NO-GO` for two false-green oracles; both are addressed here and re-review is pending. This is a
> zero-product-code, non-live baseline for the later `ActionRequestSpec` implementation. It is not a D1 completion
> claim, independent-review `GO`, live-provider approval, or Agent tool activation.

## 1. Outcome and boundary

D1a freezes the action/request/dispatch surface that exists immediately before D1 adds a versioned request schema.
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
`_dispatch_operation_run_from_records`, so a submitted operation currently returns `unsupported`. Command metadata
readiness therefore does not prove an action dispatch adapter exists. D1 must make that relation explicit rather than
inferring it from registration or from `allowed_workflow_command_types`.

## 2. Characterized contracts

| Surface | Current owner | D1a freezes | Known D1 debt |
|---|---|---|---|
| Action metadata | `operation_runtime.ActionSpec` | exact ten-field dataclass surface: identity/owner/operation, approval/budget, display, allowed/default command types | no `request_schema`, version/digest, dispatch adapter, or model-safe result schema |
| Registry serialization | `operation_runtime.ActionRegistry.to_record()` | action constant↔registry parity; exact compact record keys; optional command contract projection | request shape is not serialized or pinned |
| Submission | `OperationRuntimeWriter.submit_action` | exact complete signature/decorators; complete structural `ast.Call` inventory; unknown action and required-budget fail-closed gates; payload identity replay fence; action/event/run write order; approval-required actions stop before run creation; unexpected writer/store/repository callback or outbox access fails the runtime probe | `target_ref` and `input_payload` currently pass through without structural request-schema validation |
| Dispatch | `SourcingOrchestrator._dispatch_operation_run_from_records` | runtime branch classification for every discovered registered action; unsupported response for the three current gaps | adapter ownership is a hard-coded branch, not registry metadata |
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

The runtime dispatch inventory is:

- projection read adapter: `search_projection`, `filter_projection`;
- person Public Web adapter: `enrich_person_public_web`;
- export adapter: `export_candidates`;
- generic Agent-callable workflow-command adapter: `start_acquisition_run`, `fetch_profile_sample`,
  `continue_acquisition_run`, `refresh_company_public_web_assets`;
- CRM writer adapter: `add_to_crm`, `set_crm_stage`, `add_crm_note`, `create_crm_task`;
- unsupported: `plan_acquisition`, `promote_person_assertion`, `external_intake`.

This list is asserted against the complete auto-discovered registry population. A newly registered action therefore
fails the suite until its intended adapter/unsupported disposition is reviewed.

## 4. Mutation sensitivity

The suite fails when any of these current facts drift without an intentional D1 update:

1. an `ACTION_*` constant and the runtime registry stop matching, or the pre-schema `ActionSpec`/compact-record fields
   change;
2. a registered command loses its owner, becomes legacy/non-Agent-callable on the Activity spine, or stops exposing a
   fail-closed control record;
3. `submit_action` changes any positional-only/positional/vararg/keyword-only/kwarg parameter or annotation/default,
   return annotation, decorator, complete call expression structure, repository write order, early validation gate,
   approval boundary, or starts a callback/outbox/dispatch/planning hook. In-memory mutations add `**kwargs` and an
   indirect `runner(action)` call; both are rejected. Fail-closed runtime namespaces also reject unexpected writer,
   store, or repository/outbox access;
4. a registered action moves between adapter families, including accidentally treating `external_intake` command
   metadata as an executable action adapter;
5. command-type precedence changes. An in-memory source mutation swaps input/target order and proves the AST oracle
   rejects the mutation; a second mutation swaps `input.raw_user_request` with `target_ref.query` and is likewise
   rejected; no product source is edited;
6. any adjacent start-acquisition query sentinel or nested source priority changes, or a disallowed input/target command
   starts falling through to a lower-priority/default command.

The probes use synthetic dictionaries and local stubs only. They create no PG rows, files, network traffic, provider
requests, model calls, or external side effects.

## 5. Deferred D1 implementation gates

D1a does not introduce `ActionRequestSpec` or decide its final schema. The next implementation batch still must:

- establish the single versioned request schema and immutable pin at every actual durable creation point;
- define owner-bound `target_ref` versus caller/model-provided `input_payload`, reject duplicate/alias override paths,
  and remove the characterized dual-source ambiguity;
- add an explicit registry-owned dispatch adapter and derive the served subset from schema + adapter + Activity +
  revisioned model-safe result schema + simulate preflight;
- preserve approval/budget/idempotency/tenant fences while updating submit, approve, retry-child, dispatch, API, tests,
  and docs in one bounded slice;
- remain non-live until the typed model-turn owner, cost/budget/result-slot obligations, and a scope-matched independent
  review permit activation.

Pending review remains scope-local: it freezes D1a live/manual/product/milestone signoff, not unrelated non-live Track C
or D work. Author tests are evidence, not formal `GO`.

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
