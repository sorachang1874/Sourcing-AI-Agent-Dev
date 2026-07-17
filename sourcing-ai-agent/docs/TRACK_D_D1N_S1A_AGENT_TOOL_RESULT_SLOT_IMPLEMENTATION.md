# Track D D1n S1a — Agent tool result occurrence / terminal-winner implementation

Date: 2026-07-17

Status: author implementation and PostgreSQL evidence complete; fresh pinned non-author review pending. This is not a
formal `GO`, does not activate a public tool, and does not authorize a provider or model call.

## Impact

S1a establishes the durable boundary between one logical Agent tool call and the one physical owner result that may
be serialized back to the model. It removes two unsafe future shortcuts:

1. provider call ids cannot become idempotency authority; the stable occurrence is the scoped
   workspace/actor/runtime/mode/turn/step/tool/arguments/ordinal tuple;
2. a result cannot be reconstructed from an action type plus a mutable "latest" output. The first implemented owner
   adapter reloads the exact `plan_acquisition` Action, OperationRun, immutable preview revision/digest, and terminal
   event inside the acceptance transaction, then re-runs the registered result serializer from that preview.

The production/default Agent registry remains empty and the global served population remains `0`.

## Exact implementation

- `agent_tool_result_slot.py` owns two immutable values:
  - `AgentToolOccurrence` exact-copies historical tool/request/result/serializer pins and canonical argument bytes;
  - `AgentToolTerminalResult` binds the physical target kind/id/revision/generation, owner result ref/digest, terminal
    winner, optional command/activity chain, and exact `ToolResultMessage` bytes/digest.
- migration `0011_agent_tool_result_slots.sql`:
  - adds complete-or-empty tool name/version/digest pins to `agent_actions` and `operation_runs`;
  - creates pending/accepted `agent_tool_result_slots`;
  - creates append-only `agent_tool_result_attempts`, including accepted or quarantined disposition;
  - creates one append-only accepted `agent_tool_result_journal` per slot;
  - permits only one exact pending-to-accepted slot mutation and rejects later slot rewrites or evidence deletion.
- `agent_tool_result_postgres.py` provides:
  - generic exact reserve/replay with a logical-occurrence unique fence;
  - the first owner-specific acceptance UoW for `plan_acquisition`;
  - one transaction for accepted attempt, slot CAS, and journal;
  - exact lost-ACK replay;
  - stale slot generation and post-winner attempts as quarantined evidence only.
- the F4a preview UoW now exact-copies the local-canary `plan_acquisition` tool pins into its Action and OperationRun.
- repository reads require the full workspace/actor/runtime namespace/provider-mode execution subject.

Acceptance lock order is:

```text
operation event stream advisory
-> OperationRun
-> AgentAction
-> result slot
-> immutable acquisition plan preview
-> terminal event
-> accepted attempt / slot CAS / journal writes
```

No network, provider, model, release transition, budget issuance, or capability issuance occurs in either UoW.

## Verified behavior

- pure occurrence/terminal contracts: exact pin copying, ordinal identity, canonical argument and result bytes,
  commandless/command-backed/read-only link groups;
- reserve exact replay and split logical-identity rejection;
- exact owner reload plus serializer reconstruction;
- fault rollback after attempt, slot CAS, and journal insert;
- commit-before-ACK recovery returns the same accepted aggregate;
- stale generation quarantine without consuming the pending slot;
- late attempt quarantine without replacing the accepted winner;
- two concurrent terminal attempts yield exactly one accepted winner and one quarantine;
- missing/foreign physical preview or serializer drift leaves attempt/journal empty and the slot pending;
- database triggers reject accepted-slot rewrites and append-row deletion.

## Deliberate remaining boundary

This batch implements terminal acceptance only for `plan_acquisition`. The same occurrence storage is ready for the
other three isolated canary declarations, but each needs its own physical-owner reload adapter:

- `start_acquisition_run`: confirmation/start UoW, WorkflowCommand, ActivityRun/Attempt, terminal winner;
- `filter_projection`: exact projection membership revision and publication/result digest;
- `inspect_operation`: exact Action/Operation owner and operation-state/event revision.

Until those adapters and the assembled scripted model loop land, S1 is partial and no tool is advertised as served.

## Author validation evidence

Run from the repository root with no live provider/model variables:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_acquisition_plan_preview_uow.py
=> 25 passed, 7 subtests passed

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src SOURCING_REQUIRE_PG_STORE_TESTS=1 \
  .venv/bin/python -m pytest -q \
  tests/test_migration_runner.py \
  tests/test_d1n_acquisition_plan_preview_uow.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py \
  tests/test_d1n_canary_agent_tool_population.py \
  tests/test_d1n_agent_tool_registry.py
=> 114 passed, 72 subtests passed

make lint
=> 58 files already formatted; all checks passed

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m mypy \
  src/sourcing_agent/agent_tool_result_slot.py \
  src/sourcing_agent/agent_tool_result_postgres.py \
  tests/test_d1n_agent_tool_result_slot.py \
  tests/test_d1n_agent_tool_result_slot_uow.py
=> success, 0 issues in 4 files

make typecheck
=> existing ceiling unchanged: 81 errors in 4 files

git diff --check
=> clean
```

These are author results, not independent-review evidence.
