# Track D D1n F3-A — isolated local canary tool population

> Status: Current non-live implementation candidate (2026-07-17). Author evidence only; fresh pinned non-author
> review is pending. This declaration registry is isolated from the default/public registry, grants no release state,
> and keeps the global served population at zero.

## Outcome

F3-A assembles the first exact four-tool local Agent slice:

```text
plan_acquisition
-> start_acquisition_run
-> inspect_operation
-> filter_projection
```

Every `AgentToolSpec` now binds a concrete request schema and complete F0-A2 action-contract digest (or the exact
query-owner contract), F1 result/serializer/validator pins, workspace/actor binder, adapter, deterministic terminal-
success simulate fixture, release-owner reference, execution-subject policy, approval/budget/capability contract, and
truthful effect class.

The effect matrix is intentionally heterogeneous:

- `plan_acquisition`: commandless action, no approval/budget/capability;
- `start_acquisition_run`: command-backed action, exact human confirmation, parent budget reservation, live-only
  provider capability, and approval/dispatch checkpoints;
- `filter_projection`: action-backed read adapter, with `read_only` effects and no command/approval/budget/capability;
- `inspect_operation`: query-backed read adapter under its exact query-owner id/revision/digest.

## Version and history boundary

The local declarations use the V1 plan, V2 start, V2 filter, and V1 inspect contracts. They do not rewrite the global
ActionRegistry: `plan_acquisition` remains schema-less there, while start/filter retain their historical v1 request
versions. `inspect_operation` remains a query and does not create a sixteenth action.

`LOCAL_CANARY_AGENT_TOOL_REGISTRY` has four declarations and exact historical lookup, while
`DEFAULT_AGENT_TOOL_REGISTRY` remains empty. Its `current_release_owner` is only an identity pointer; neither registry
offers a `served` or `model_visible` API.

## Simulate and live boundary

Each checked-in fixture is content-digested and requires a real terminal `success`; deferred/stale/error outcomes
cannot satisfy it. The fixtures require zero live provider/model invocations. The structural execution-subject schema
can represent simulate, scripted, or live, but the isolated permission contract requires a separate paid-canary
receipt for live. Registry presence cannot mint that receipt or a provider capability.

The start adapter pin names the S1 PostgreSQL integration contract; its approval/run/command/budget/event UoW is not
implemented by this declaration batch. Result-occurrence persistence and ToolResultMessage/journal acceptance also
remain S1 predecessors.

## Author validation

- focused population/source-pin/effect/fixture/history matrix: `8 passed`;
- F0/F1/F3/V1/V2/V3 adjacency: `266 passed`;
- scoped Ruff, format, Python compilation, and mypy (`0 issues`): green.

These are author results, not an independent-review verdict, a simulate pass, or live authorization.
