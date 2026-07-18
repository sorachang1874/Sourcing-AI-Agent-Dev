# Track D D1n S1e2a — acquisition-start pending submit PostgreSQL UoW

> Status: Current non-live implementation candidate (2026-07-18; S1e2b follow-on recorded). Author evidence only;
> fresh pinned non-author review is pending. This batch keeps the public/default Agent population at `served=0` and
> authorizes no provider, model, network, replay-mode, or live execution.

## Outcome

S1e2a implements only the first of the three S1e1 start-authority transactions. One specialized PostgreSQL UoW
creates or exact-reloads this pending aggregate:

- one `agent_actions` row with physical `status=approval_required`, `approval_status=required`, and
  `approval_policy=required`;
- one Action-stream `ActionApprovalRequired` operation event at exact sequence `1`.

The UoW creates no OperationRun, WorkflowEvent, WorkflowCommand, current-state projection, result attempt/journal,
runtime-outbox, Activity, AcquisitionRun, provider, model, or domain row. It does not extend generic action submit,
approval, dispatch, retry, cancel, or resume.

## Pre-connection authority

Before any adapter dependency lookup, connection, schema path, owner read, or write, the submit owner:

1. reconstructs the occurrence from the server-owned historical local-canary registry;
2. requires the exact current `start_acquisition_run_tool_v3` tool, request, result, serializer, behavior, and link
   pins;
3. requires `runtime_namespace=isolated_local_canary` and `provider_mode=simulate|scripted`;
4. decodes the exact full canonical `{input_payload,target_ref}` root and binds occurrence workspace/actor to the
   owner-minted target workspace/requester.

`live|replay`, historical-but-noncurrent tools, pin drift, surplus/partial roots, and wrong owner roots therefore fail
before the adapter is touched. The writer requires the four existing PostgreSQL dependencies and never bootstraps or
migrates a schema.

## Transaction, locks, and replay

The writer requires every dependency to be both read-preferred and PostgreSQL-authoritative. It then uses one finite
monotonic deadline across connection acquisition, transaction advisory locks, row locks, and statements. It rejects a
connection that returns after the budget, refreshes transaction-local `lock_timeout` and `statement_timeout` from the
remaining budget at every SQL/write boundary, and checks the budget again before commit. Applicable advisory groups
are acquired in S1e1 order, with UTF-8 bytewise sorting inside each group:

```text
Action operation-event stream
-> AgentAction physical/idempotency identities
-> result-slot physical/logical-occurrence identities
-> immutable preview identity
```

Under those locks it exact-reloads the pending result slot and immutable preview, obtains one second-precision
transaction timestamp on the first attempt, and re-runs `AcquisitionStartV2OwnerBinder` against the locked physical
preview. The rebound request must byte-equal the reserved occurrence before either row is inserted.

The Action exact-copies the closed preview reference, owner-bound target and start snapshot, current request/tool/result
and serializer pins, five-field budget projection, and result-occurrence identity. The sequence-1 event uses the
deterministic S1e1 event id/key and the exact closed payload from section 3.1 of the owner decision.

Replay locks the same identities, rejects split or partial aggregates, reloads the persisted Action timestamp, and
rebuilds the submit-owned immutable Action identity plus the complete sequence-1 event. It accepts only the exact
pending state or the S1e1-ratified `queued/approved` successor with a nonempty result ref; an accepted occurrence slot
is legal only with that approved successor. This keeps submit idempotent after create/result acceptance without
allowing a forged accepted slot to advance a pending Action. Replay never allocates a new time. A fault after either
pre-commit insert rolls both rows back; a fault after commit is recovered by the exact replay path.

## Delegation surface

`LiveControlPlanePostgresAdapter.submit_acquisition_start_v2_action_uow(...)` is the sole native adapter entrypoint.
`WorkflowRuntimeRepository.submit_acquisition_start_v2_action_uow(...)` requires PostgreSQL authority for the Action,
event, result-slot, and preview tables, delegates once, and maps the physical Action/event rows into repository values.
Neither surface adds a public API route or a served registry entry.

## Explicit non-closure

- S1e2b has since implemented the receipt-backed budget-envelope builder and specialized create UoW; see
  `TRACK_D_D1N_S1E2B_START_CREATE_UOW_IMPLEMENTATION.md`. That follow-on does not retroactively expand this submit
  transaction.
- S1e2c still owns read-only result preparation, start-specific shared acceptance, and terminal-success proof.
- The production partition remains `10 schema-defined / 5 schema-less`; R-029 and its release-window deletion
  condition remain open.
- R-019, Plan §6#6, OB-2.2, OB-10.3, and OB-10.4 remain open. The Action/Operation durable-scope columns and D3
  cost/exposure ledger are not supplied by this batch.
- The default/public registry remains empty and `served=0`; provider/model/live invocation count remains zero.

## Author validation

- submit owner + delegates + real-PG fault/lost-ACK/8-way concurrency/deadline matrix: `37 passed + 2 subtests`;
- the same matrix plus the transitioned S1e0 characterization: `42 passed + 2 subtests`;
- start-v2, S1e1 decision, canary population, preview, and shared result-slot adjacency:
  `267 passed + 13 subtests`;
- new owner module scoped mypy: `0 errors / 1 file`; global ceiling unchanged at `81 errors / 4 files`;
- repository lint/format and scoped Ruff: green.

The first local non-author adversarial audit returned advisory `NO-GO 0/2/1/0`: successor-lifecycle replay, total
deadline enforcement, and PG authority were incomplete. The bounded fixed-forward added direct regressions for all
three, and the same reviewer independently reran the integrated matrix and returned scope-local advisory
`GO 0/0/0/0`. Neither local result is a formal pinned review verdict. A fresh pinned non-author review remains required
before this scope can enter any live/manual/product/milestone gate.
