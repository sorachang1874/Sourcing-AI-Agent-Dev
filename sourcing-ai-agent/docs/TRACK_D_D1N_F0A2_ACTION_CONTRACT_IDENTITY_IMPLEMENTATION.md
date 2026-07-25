# Track D D1n F0-A2 — versioned Action contract identity

> Status: Current non-live implementation candidate (2026-07-17). Author evidence only; fresh pinned non-author
> review is pending. This batch fingerprints contracts; it does not activate a request/tool, populate the public Agent
> registry, change `served=0`, or authorize provider/model execution.

## Outcome

F0-A2 closes the missing full `action_contract_digest` builder. A digest now binds the complete action surface owned
by `operation_runtime.ActionRegistry` together with one exact versioned request schema:

- action type, owner module, operation type, and dispatch adapter;
- request status/version/digest, replay identity target fields, and alias contract;
- approval and budget policy;
- display label/category/description and fail-closed source-of-truth fields;
- allowed/default WorkflowCommand types, owners, exposure gate, stage/readiness/display/control/activity contracts,
  and aggregate control summary.

The builder accepts either the registered request version or a request-schema successor. A successor may change only
request schema/version/identity/aliases; any drift in owner, adapter, approval, budget, display, or command semantics
fails before a digest can be minted.

## Debt visibility and activation boundary

`production_action_contract_manifest()` fingerprints all 15 registered actions, including schema-less rows. Its
current checked-in counts are `15 total / 10 schema-defined / 5 schema-less`; the latter remain explicitly named
instead of receiving synthetic request pins. `build_action_contract_pin()` rejects a schema-less action.

This distinction allows local canary successor contracts to receive truthful full digests while preserving R-029's
global 5-row debt. The builder does not select a current version, mutate the ActionRegistry, create activation rows,
or reinterpret historical rows.

## Author validation

- focused deterministic manifest/successor/drift/defensive matrix: `6 passed`;
- F0/F3 adjacency: `124 passed`;
- scoped Ruff, format, Python compilation, and mypy (`0 issues`): green.

These are author results, not an independent-review verdict or serving authorization.
