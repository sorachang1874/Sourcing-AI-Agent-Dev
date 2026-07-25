# Track D D1n S1e2c — acquisition-start result prepare/accept PostgreSQL UoW

> Status: Implementation/decision record (Track D increment). Review state and current authority are routed via docs/INDEX.md Tier 3; scheduled for module distribution (reorg R3+).

Status: author implementation candidate; non-live; no provider/model/network invocation; no served registry change;
fresh independent review pending.

## Scope

S1e2c implements the third specialized `start_acquisition_run` v2 PostgreSQL path selected by the S1e1 owner decision.
It consumes the S1e2b dormant command-acceptance owner bundle and connects it to the shared Agent result-slot
pending-to-accepted state machine.

The batch adds:

- `prepare_start_acquisition_tool_result(...)`: read-only exact owner reconstruction and
  `acquisition_start_result_v2` success terminal serialization.
- `accept_start_acquisition_tool_result_uow(...)`: shared result-slot attempt/slot/journal acceptance plus the
  start-specific release of the dormant root `workflow_commands.not_before_at` hold.
- Live adapter and repository delegation/mapping for prepare and accept.
- PG regression coverage for zero-write prepare, fresh accept, exact replay, late-attempt quarantine, rollback, and
  `runtime_outbox=0`.

## Owner and write contract

Prepare revalidates the current historical `start_acquisition_run_tool_v3` occurrence before owner reads, then reloads
the created S1e2b owner bundle: Action, OperationRun, preview, receipt event, workflow seq1/2 events, root
WorkflowCommand, `workflow_current_state`, and `OperationCommandPlanned` winner. It rebuilds the receipt-backed
canonical create rows and serializes the model-safe success result from the locked owner state. It writes nothing.

Accept reuses `_accept_exact_agent_tool_result_uow(...)`. Fresh acceptance writes only:

- `agent_tool_result_attempts`
- accepted `agent_tool_result_slots`
- `agent_tool_result_journal`
- `workflow_commands.not_before_at=''` and `updated_at=transaction_timestamp()` for the exact queued root command

The command release happens in the same transaction after the result journal write path is constructed and before
commit. A pre-commit fault rolls back the attempt, slot update, journal, and command release together. Exact replay
requires the accepted aggregate and the released command to match. A later losing result attempt may append only a
quarantined attempt; it does not rewrite Action, Operation, command, events, journal, outbox, provider, model, or domain
state.

After commit, the adapter requests a best-effort recovery-daemon wake via the same wake-file mechanism used by durable
runtime writers. This is acceleration only; no `runtime_outbox` row is created.

## Explicit non-closure

- This is still an author candidate. It is not a formal independent-review `GO`.
- The public/default Agent served population remains `0`.
- The production partition remains `10 schema-defined / 5 schema-less`; R-029 remains open.
- R-019, Plan §6#6, OB-2.2, OB-10.3, and OB-10.4 remain open.
- No live provider/model/network path was invoked. The root command consumer and full end-to-end product run remain
  future validation work.
