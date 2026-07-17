# Track D D1n S1b — `inspect_operation` physical-owner result adapter

Date: 2026-07-17

Status: author implementation and PostgreSQL evidence complete; fresh pinned non-author review pending. This is not a
formal `GO`, does not populate the default Agent registry, and does not authorize a provider or model call.

## Impact

S1b makes the V3 `inspect_operation` query executable against production-shaped PostgreSQL state and persistable in
the S1a exact result slot. The query result is no longer supplied as an unattached snapshot fixture. It is rebuilt
from one exact physical owner set:

```text
workspace + AgentAction + OperationRun
-> latest Operation event id/sequence
-> exact workflow_ref WorkflowCommand + matching OperationCommandPlanned event
-> canonical control/display/progress/result-readiness projection
-> revision-bound serialized result
```

The event sequence is the monotonic revision anchor. The complete canonical owner snapshot has a SHA-256 digest, so
Action/Operation/command/result-readiness drift is detected even if a legacy path changes state without advancing the
event stream. A result prepared at revision N is accepted only if the same rows still rebuild the same terminal at
acceptance; otherwise the slot stays pending and no attempt or journal is written.

This batch also fixed-forwards the pre-served inspect contract to v2. Result readiness now has one centralized owner
derivation used by the PostgreSQL builder, owner-snapshot validator, execution path, and named serializer. A completed
Operation without a non-empty durable `result_ref` is `pending` with `fail_closed`; it can never be inferred as
`ready` or `not_applicable`. The result/query-owner/serializer/tool/adapter/simulate-fixture revisions and their
digests were advanced together. The request remains v1 because its shape and semantics did not change.

## Exact implementation

- `agent_operation_query_postgres.py` owns the physical query adapter:
  - validates the exact isolated-canary `inspect_operation` tool/request/result/serializer pins;
  - locks OperationRun then AgentAction, the Operation event stream, and only the exact WorkflowCommand referenced by
    `OperationRun.workflow_ref`;
  - requires exact workspace/action/run ownership and ActionRegistry owner/type parity;
  - requires workflow run/id/type/owner parity across `workflow_ref`, the exact command row, the linked Action
    contract, the durable command-owner registry, and a matching `OperationCommandPlanned` event;
  - deliberately excludes unrelated/downstream rows that merely share `operation_id`, so they cannot become phantom
    members of this query owner;
  - projects `operation_run_control_state`, durable command control policy, ActionRegistry display contract,
    Operation progress/result readiness, and bounded provenance;
  - reruns the V3 result validator/serializer and binds the output to the latest event id/sequence plus snapshot digest.
- `agent_projection_query.py` owns the exact readiness matrix and rejects schema-valid semantic drift both before
  execution and at serialization. Its v2 schema no longer admits `not_applicable`; a future explicit owner semantics
  would require another fixed-forward revision.
- `agent_canary_registry.py` pins the v2 query owner, result, serializer, adapter, tool, and simulate fixture without
  changing the public/default registry population.
- `agent_tool_result_postgres.py` now has one shared pending-to-accepted state machine. Both the existing
  `plan_acquisition` adapter and S1b reuse its generation quarantine, late-winner quarantine, exact replay, fault
  rollback, slot CAS, and append-only journal behavior.
- `prepare_inspect_operation_tool_result` is a read-only owner step. It returns a typed terminal proposal, performs no
  result-slot or domain write, and does not bootstrap or mutate table schemas.
- `accept_inspect_operation_tool_result_uow` re-reads and reserializes the owner inside the acceptance transaction
  before the accepted attempt, slot CAS, or journal can be written.
- repository and live-PG adapter entrypoints expose the same exact contract to the later local Agent loop.

Acceptance lock order is:

```text
Operation event-stream advisory
-> OperationRun -> AgentAction -> result slot
-> operation events -> exact workflow_ref command
-> accepted attempt -> slot CAS -> journal
```

No control action, repair, provider/model call, projection mutation, Action/Operation transition, or public release
transition occurs.

## Verified behavior

- commandless completed Operation: exact success result, `not_applicable` command policy, ready result reference;
- command-backed running Operation: registered durable command policy and pending result readiness;
- completed Operation without a durable result reference remains `pending/fail_closed` in the physical PG path;
- forged readiness status, owner, fallback, and direct-serializer projections fail closed;
- command-owner drift fails closed before a terminal row is written;
- foreign workflow-run and mismatched planned-event links fail closed before terminal writes;
- an unreferenced row sharing `operation_id` remains outside the exact query owner and cannot change its provenance;
- prepare performs no write-schema bootstrap;
- prepare/accept exact replay including commit-before-ack recovery;
- Action, Operation, and Operation-event rows remain byte-equivalent across accepted query-result persistence;
- event revision drift between prepare and acceptance leaves the slot pending with zero attempt/journal writes;
- missing and foreign owner tuples share the same fail-closed class before terminal writes;
- a forged historical tool/request/result/serializer identity fails before owner read;
- S1a `plan_acquisition` concurrency/quarantine/replay behavior remains green through the shared-core refactor.

## Deliberate remaining boundary

S1 still needs two canary owner adapters:

- `start_acquisition_run`: confirmation/start UoW, exact WorkflowCommand and Activity winner;
- `filter_projection`: exact projection publication and Cohort membership result.

The filter adapter cannot safely reuse the existing integer-only target revision: canonical
`membership_revision` is an opaque equality token and must not be parsed, ordered, or hashed into an invented
integer. The next bounded storage batch must add an explicit opaque owner-revision carrier to slot/attempt/journal
aggregates and preserve the existing integer revision/generation fields for owners that genuinely have numeric
revisions.

Default/public serving remains `0`; scripted model-loop assembly and live provider/model work remain blocked on their
separate gates.

## Fresh pinned review scope

Review base: `070728a6f59da17caf63914672676d8434d9aa84`. The head is the eventual standalone S1b commit. Exact intended
scope:

- `docs/NEXT_TODO.md`
- `docs/TRACK_D_D1N_REMAINING_ACTION_AND_AGENT_TOOL_SERVING_PLAN.md`
- `docs/TRACK_D_D1N_S1A_AGENT_TOOL_RESULT_SLOT_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_S1B_INSPECT_OPERATION_RESULT_IMPLEMENTATION.md`
- `docs/TRACK_D_D1N_V3_PROJECTION_QUERY_IMPLEMENTATION.md`
- `src/sourcing_agent/agent_canary_registry.py`
- `src/sourcing_agent/agent_operation_query_postgres.py`
- `src/sourcing_agent/agent_projection_query.py`
- `src/sourcing_agent/agent_tool_result_postgres.py`
- `src/sourcing_agent/control_plane_live_postgres.py`
- `src/sourcing_agent/repositories/workflow_runtime.py`
- `tests/test_d1n_canary_agent_tool_population.py`
- `tests/test_d1n_inspect_operation_result_slot_uow.py`
- `tests/test_d1n_projection_query_contracts.py`

## Author validation evidence

Run from the repository root with no live provider/model variables:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_d1n_agent_tool_result_slot_uow.py
=> 23 passed + 3 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q \
  tests/test_migration_runner.py \
  tests/test_d1n_acquisition_plan_preview_uow.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_registry.py \
  tests/test_d1n_projection_query_contracts.py
=> 170 passed + 72 subtests

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/agent_operation_query_postgres.py \
  src/sourcing_agent/agent_projection_query.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  tests/test_d1n_inspect_operation_result_slot_uow.py \
  tests/test_d1n_canary_agent_tool_population.py
=> success, 0 issues in 5 files

make lint
=> all checks passed

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

git diff --check
=> clean
```

These are author results, not independent-review evidence.
