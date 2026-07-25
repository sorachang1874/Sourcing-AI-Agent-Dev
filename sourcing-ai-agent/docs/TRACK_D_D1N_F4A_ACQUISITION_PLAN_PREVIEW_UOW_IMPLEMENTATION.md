# Track D D1n F4a — acquisition-plan preview PostgreSQL UoW

> Status: Current non-live implementation candidate (2026-07-17). Author evidence only; fresh pinned non-author
> review is pending. This batch does not activate `plan_acquisition` in the public ActionRegistry or Agent tool
> population, does not authorize provider/model/live execution, and keeps `served=0`.

## Outcome

F4a adds the commandless persistence owner required by V1 `plan_acquisition`. One PostgreSQL transaction creates or
exact-reloads one immutable aggregate:

- terminal `AgentAction` (`completed`);
- terminal `OperationRun` (`completed`);
- immutable `acquisition_plan_preview.v2`;
- one `AcquisitionPlanPreviewCreated` operation event.

The transaction creates zero WorkflowCommands, AcquisitionRuns, Activities, EntityDeltas, plan reviews, provider or
model records. It also creates zero runtime-outbox rows: no command or wakeup consumer exists for this synchronous
commandless result, so inventing a new outbox type would create an unconsumable queue rather than a completed path.

## Physical contracts

Migration `0010_acquisition_plan_preview_uow.sql`:

- adds first-class result schema and serializer pins to `agent_actions` and `operation_runs`; brownfield rows retain the
  explicit all-empty pin group, while the F4a writer requires the complete non-empty group;
- adds `acquisition_plan_previews` with exact owner, action/run, company, request/result/start schema, serializer,
  manifest/query, effective-request, revision, digest, and TTL identity;
- uses a globally monotonic, gap-tolerant revision sequence owned by `preview_revision`;
- stores database-owned second-precision `TIMESTAMPTZ` creation/expiry values and constrains `0 < TTL <= 24h`;
- projects the immutable JSON identity into physical columns with null-safe checks and rejects update/delete through an
  immutable-row trigger.

The scoped reader requires the complete `(workspace_id, requester_id, preview_id, preview_revision, preview_digest)`
tuple. A foreign, stale, conflicting, or missing tuple returns the same empty result.

## Transaction and replay contract

`create_acquisition_plan_preview_uow(...)` is intentionally a specialized native writer. The generic operation
submission path persists action, events, and run in separate transactions and is not used here.

The writer uses one monotonic deadline across connection checkout, advisory locks, and row locks. Its order is:

```text
operation event stream
-> OperationRun physical/idempotency identities
-> AgentAction physical/idempotency identities
-> preview physical/idempotency identities
-> terminal event
```

It then:

1. locks and reads all identity axes, rejecting split identity or a partial aggregate;
2. exact-reloads a complete aggregate before allocating a revision;
3. allocates the revision and server timestamps inside the transaction;
4. invokes the capability-free V1 preview compiler and exact-copies canonical request, result, serializer, start,
   company, Cohort, budget, and manifest identity;
5. writes run, action, preview, and event; then commits once.

Faults after any pre-commit write roll the complete bundle back. A lost acknowledgement after commit is recovered by
the same exact replay path, without a second revision, event, action, or run. Changed request/target/pin/actor/source
identity under an existing idempotency key fails closed and writes nothing.

## Explicit boundaries

- This batch does not route public `plan_acquisition` submission into the specialized UoW; that is the later single
  integration-owner step after V2/V3 contracts are present.
- It does not implement result-slot occurrence persistence, tool population, release activation, approval receipts,
  start-v2, provider transport, model turns, or hosted serving.
- It closes atomicity only for this commandless preview path. R-019 and R-028 remain open globally.
- Result schema/serializer pins are physical; tool-spec and invocation occurrence pins remain future F3/S1 work.

## Author validation

- exact F4a real-PG success/replay/fault/lost-ack/conflict/8-way-concurrency/deadline/immutability matrix:
  `8 passed + 4 subtests`;
- complete migration runner: `24 passed + 65 subtests`;
- F0/F1/V1/storage/PG fixture adjacency: `469 passed`;
- scoped Ruff, format, Python compilation, and `git diff --check`: green.

These are author results, not an independent-review verdict. A fresh pinned non-author review is required before this
scope can participate in a live or milestone gate.
