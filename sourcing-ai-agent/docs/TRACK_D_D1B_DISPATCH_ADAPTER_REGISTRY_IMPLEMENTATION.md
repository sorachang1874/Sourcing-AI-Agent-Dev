# Track D D1b — Explicit dispatch-adapter registry implementation

> Status: Author implementation, non-live and review-pending (2026-07-14). This batch is behavior-equivalent to the
> D1a dispatch inventory and does not claim D1 completion, independent-review `GO`, served Agent tools, live-provider
> approval, manual/product signoff, or milestone closure.
>
> The scoped pinned review of `2184d64` found one false-green regression oracle: a hard-coded registered action-name
> literal branch for an action other than the single mutated export case could pass all D1a+D1b tests. This follow-up
> parameterizes the mutation across all 15 actions and adds a self-proving AST literal/control-flow rejection check.
> A fresh pinned re-review of `528ba81` then found a second false-green: the `getattr` check recognized only a direct
> `getattr(...)` call, so qualified or aliased dynamic attribute resolution could pass the AST oracle while all 15
> registry mutations and the missing-binding probe stayed green. The pinned re-review of `becc60e` found two deeper
> false-greens: a module helper could hide `getattr`, and a module action alias plus a local `action_type` alias could
> hide a hard-coded action branch. The current author follow-up replaces symbol-name detection as the primary gate with
> canonical selector/binding AST shapes and exact body dependency closures; a new non-author re-review remains pending.

## 1. Outcome and scope

D1b removes action-name dispatch classification from `SourcingOrchestrator` and makes
`operation_runtime.ActionRegistry` the single declaration owner for action-to-adapter selection. `ActionSpec` adds one
normalized, closed-set `dispatch_adapter` string. The orchestrator owns a separate explicit five-entry mapping from
those adapter identifiers to existing bound owner methods. Runtime selection is therefore:

```text
persisted action_type -> ActionRegistry.spec_for -> dispatch_adapter -> explicit bound-method map
```

The implementation is deliberately additive and behavior-equivalent:

- all 12 previously dispatched actions reach the same existing owner method;
- `plan_acquisition`, `promote_person_assertion`, and `external_intake` retain an empty adapter and return the same
  fail-closed `unsupported` response;
- `external_intake` command metadata does not imply an action adapter;
- unknown adapter identifiers are rejected when a registry is constructed;
- an unknown action, empty adapter, or missing runtime binding cannot invoke an owner method;
- dispatch never resolves a method through dynamic attribute lookup or a module path supplied by registry data.

This slice does not change request payloads, target ownership, approval/budget behavior, persistence, migrations,
HTTP routes, frontend contracts, workflow command ownership, provider/model transport, or live execution gates.
`dispatch_adapter` is intentionally not projected by `ActionRegistry.to_record()`, because that record is returned by
the existing action-registry HTTP route; D1b therefore leaves its public payload unchanged.

## 2. Registry mapping

| Action | `dispatch_adapter` | Existing bound owner method |
|---|---|---|
| `plan_acquisition` | empty | unsupported |
| `start_acquisition_run` | `agent_callable_workflow_command` | `_dispatch_agent_callable_workflow_command_operation` |
| `fetch_profile_sample` | `agent_callable_workflow_command` | `_dispatch_agent_callable_workflow_command_operation` |
| `continue_acquisition_run` | `agent_callable_workflow_command` | `_dispatch_agent_callable_workflow_command_operation` |
| `search_projection` | `projection_read` | `_dispatch_projection_read_operation` |
| `filter_projection` | `projection_read` | `_dispatch_projection_read_operation` |
| `add_to_crm` | `crm_writer` | `_dispatch_crm_writer_operation` |
| `set_crm_stage` | `crm_writer` | `_dispatch_crm_writer_operation` |
| `add_crm_note` | `crm_writer` | `_dispatch_crm_writer_operation` |
| `create_crm_task` | `crm_writer` | `_dispatch_crm_writer_operation` |
| `enrich_person_public_web` | `person_public_web` | `_dispatch_person_public_web_enrichment_operation` |
| `refresh_company_public_web_assets` | `agent_callable_workflow_command` | `_dispatch_agent_callable_workflow_command_operation` |
| `promote_person_assertion` | empty | unsupported |
| `export_candidates` | `export` | `_dispatch_export_candidates_operation` |
| `external_intake` | empty | unsupported |

## 3. Ownership and fail-closed rules

| Contract | Owner/source of truth | Allowed state | Fail-closed rule |
|---|---|---|---|
| action-to-adapter declaration | `operation_runtime.ActionRegistry.spec_for()` / `ActionSpec.dispatch_adapter` | empty or one of the five `ACTION_DISPATCH_ADAPTERS` values | unnormalized or unknown non-empty value rejects registry construction |
| adapter implementation binding | `SourcingOrchestrator._operation_dispatch_adapter_bindings` | the same five identifiers, each bound to one named method | absent binding returns `unsupported`; no dynamic lookup |
| workflow-command exposure | `ActionSpec.allowed_workflow_command_types` plus existing command owner/Activity/control registries | unchanged from D1a | command metadata never synthesizes a dispatch adapter |
| served Agent tool population | future D1 served predicate | empty in D1b | adapter presence alone is insufficient; request schema, Activity spine, revisioned model-safe result schema, and simulate preflight are still required |

The persisted action record continues to carry only `action_type`; dispatch resolves the current registry declaration at
runtime. Immutable request-schema version/digest pins are outside D1b and must not be emulated with metadata.

## 4. Regression and mutation coverage

`tests/test_d1_action_request_surface_characterization.py` now freezes the eleven-field pre-request-schema
`ActionSpec`, exact adapter assignment for all discovered actions, and the unchanged 12/three behavior inventory.

`tests/test_d1_dispatch_adapter_registry.py` adds these independent oracles:

1. unknown and whitespace-padded adapter identifiers reject registry construction;
2. the runtime binding map is total over the five closed adapter identifiers;
3. canonical AST inspection freezes the complete selector shape and the exact five adapter-key-to-`self.method`
   binding entries. Separate exact body-dependency closures allow only the registry, adapter/handler locals, closed
   adapter constants, and five direct bound methods; any helper call, module alias, new local alias, or extra control
   flow fails even when it avoids a known dangerous symbol name;
4. parameterized in-memory registry mutations reroute every one of the 15 actions to a different legal adapter,
   proving dispatch follows registry data for supported and empty-adapter actions rather than a hidden action-name
   branch;
5. self-proving mutations cover direct, qualified, imported, multi-hop assignment, and module-helper `getattr`;
   `__getattribute__`, `attrgetter`, `getattr_static`, `vars(...)`, and `__dict__`; qualified action constants; raw
   `action_type`; and module/local multi-hop action aliases. The prior module-helper and aliased-action-branch
   counterexamples now fail the canonical/dependency gate, while an ordinary static-dictionary `.get(...)` remains a
   negative control for the supplemental dangerous-symbol detector;
6. deleting the export binding fails closed without invoking another owner;
7. the exact three empty-adapter actions remain unsupported;
8. public registry records expose neither the internal adapter nor request schema, model-safe result schema,
   `agent_tool_enabled`, or served status.

The probes use local dictionaries and methods only. They create no PG rows, files, network traffic, provider requests,
model calls, or live side effects.

## 5. Deferred D1 work

D1b satisfies only the explicit dispatch declaration prerequisite. A later bounded batch must still decide and land:

- the owner-bound target source and rejection of caller/model target aliases;
- versioned request schemas and a physical residual ledger for temporarily schema-less actions;
- immutable schema version/digest columns and copy/verify behavior at submit, approve, and retry-child creation;
- revisioned model-safe result schemas and their validator owner;
- the full served predicate and simulate-dispatch serializer preflight.

Until all predicates exist, the served Agent tool population is zero. D1b may receive a scoped asynchronous independent
review without blocking unrelated non-live Track C or D implementation; a `NO-GO` would block promotion of this scope,
not author progress elsewhere.

## 6. Validation

Run from `sourcing-ai-agent/`:

```bash
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_action_registry_unknown_action_fails_closed \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_action_registry_rejects_unregistered_workflow_commands \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_action_registry_exposes_workflow_command_contract_for_agent_callable_actions \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_read_only_projection_action_creates_idempotent_operation_without_module_side_effects \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_approval_required_action_does_not_create_operation_run_before_approval \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_budget_required_action_requires_explicit_budget \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_start_acquisition_operation_plans_root_acquisition_run_command_only \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_continue_acquisition_rejects_commands_outside_action_registry_allowlist
PYTHONPATH=src .venv/bin/python -m pytest -q \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_export_operation_dispatch_plans_projection_export_command_without_running_owner \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_public_web_enrichment_operation_dispatch_leaves_batch_creation_to_command_owner \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_add_to_crm_operation_dispatch_leaves_crm_writes_to_command_owner \
  tests/test_operation_runtime.py::OperationRuntimeTest::test_company_public_web_operation_refreshes_company_assets_only_in_command_owner
PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_command_type_specs.py
.venv/bin/ruff check \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py
.venv/bin/ruff format --check \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py
PYTHONPATH=src .venv/bin/mypy --follow-imports=skip --ignore-missing-imports \
  src/sourcing_agent/operation_runtime.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py
git diff --check -- \
  src/sourcing_agent/operation_runtime.py \
  src/sourcing_agent/orchestrator.py \
  tests/test_d1_action_request_surface_characterization.py \
  tests/test_d1_dispatch_adapter_registry.py \
  docs/TRACK_D_D1A_ACTION_REQUEST_SURFACE_CHARACTERIZATION.md \
  docs/TRACK_D_D1B_DISPATCH_ADAPTER_REGISTRY_IMPLEMENTATION.md
```

Author validation on the final stable worktree passed: D1a+D1b pure in-memory suites 11 tests; eight exact existing
OperationRuntime registry/submission/workflow-command nodes; four existing real owner-planning dispatch nodes covering
export, person Public Web, CRM writer, and company Public Web; command-spec suite 16 tests; focused mypy with no issues;
Ruff check/format; and scoped whitespace diff check. These results are author evidence only, not independent-review
`GO`.

False-green follow-up author validation: the expanded D1a+D1b suites passed 25 tests; focused mypy reported no issues
in the two test files; Ruff check/format and the scoped whitespace diff check passed. No production source changed in
this follow-up.

Second false-green follow-up author validation: D1a+D1b passed 25 tests; 14 PG-backed registry/dispatch nodes and the
16-test command-spec suite passed; focused mypy reported no issues in three checked files; Ruff check/format and the
scoped whitespace diff check passed. Independent mutation probes produced zero findings for the production selector,
at least one finding each for qualified `builtins.getattr`, an assignment alias, `__getattribute__`, and
`vars(self)[name]`, and zero findings for an ordinary dictionary `.get(name)`. This follow-up changes only the D1b test
and this document; its results are author evidence, and a fresh non-author review is still required.

Third false-green follow-up author validation on the final stable worktree: D1a+D1b passed 25 tests; 15 PG-backed
registry/dispatch/adjacent exposure nodes passed; the command-spec suite passed 16 tests; focused mypy reported no
issues in three checked files; and Ruff check, Ruff format, and scoped whitespace diff check passed. The canonical AST
and dependency-closure mutations reject the exact two re-review counterexamples—top-level helper `getattr` and
module/local aliased action branch—plus multi-hop helper/action aliases, while the unchanged production selector and
exact five-entry direct binding map pass. This follow-up changes only the D1b test and this document; its evidence
remains author evidence, and a fresh non-author review is required.
