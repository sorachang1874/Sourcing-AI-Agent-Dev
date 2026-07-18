# Track D D1n S1e2c — formal review partial response

Status: second partial implementation response to the formal S1e2c `NO-GO` artifact
`runtime/reviews/20260718T062908Z_Track_D_D1n_S1e2c_start_result_accept_f0e7210.md`.

This batch remains non-live and non-served. It does not close `R-019`, `R-029`, Plan §6#6, `OB-2.2`, `OB-10.3`,
`OB-10.4`, hosted serving, or provider/model invocation gates.

## Fixed-forward scope in this partial response

- Acceptance now reconstructs the canonical start-v2 success terminal from the locked owner and compares the serialized
  result JSON, serialized digest, tool-result message digest, and `is_error=false` before any attempt/slot/journal or
  command-release write.
- The start result owner loader now locks and exact-checks the result-slot row and preview row in addition to the
  Action, OperationRun, receipt/planned events, workflow events, WorkflowCommand, and current-state projection.
- Prepare refreshes both transaction-local `lock_timeout` and `statement_timeout` from the remaining monotonic deadline
  before lock phases and owner loading.
- Accepted replay after the released root command has already been consumed now validates the immutable owner-result
  target/ref/digest, canonical terminal bytes, and receipt/planned event set without requiring mutable downstream
  command/current-state projections to match their create-time rows.
- Late-result quarantine after normal downstream command progress uses the same stable start-v2 owner-result proof before
  writing a quarantine attempt. It does not depend on mutable action/command success projections and still rejects owner
  or serializer drift before the quarantine write.
- `PRE_AGENT_CONTRACT_REVIEW.md` now has a `start_acquisition_run.result_hold_release_owner` matrix row, and the fast
  preflight asserts the owner, source-of-truth, allowed consumers, forbidden consumers, fail-closed fallback, and
  served-zero migration status.

## New regression evidence

- `tests/test_d1n_start_acquisition_v2_create_pg.py` now includes forged serialized-result and forged-error terminal
  zero-write acceptance cases.
- `tests/test_d1n_start_acquisition_v2_create_pg.py` now covers accept -> root-command drain -> exact replay with a
  full zero-write snapshot and accept -> root-command drain -> late-result quarantine with zero domain/outbox/provider
  writes.
- `tests/test_pre_agent_contract_review.py::test_agent_tool_result_aggregate_owner_contract_is_canonical` now fails if
  the start-v2 result hold/release owner row or key contract tokens disappear.
- Existing accept/replay/quarantine/root-hop tests continue to exercise the held-command release path and zero
  `runtime_outbox`/provider/model behavior.

## Still open from the S1e2c formal review

- A shared lock/probe topology across create, accept, completion, and controls remains open under `R-019`.
- The central pre-agent contract matrix entry is present in this response, but the formal S1e2c review remains open until
  a fresh pinned non-author artifact returns `GO`.
- S1e2d covers the first released-root consumer hop, and the S1e2b response covers held generic command controls, but
  those separate commits do not by themselves close this S1e2c formal review.
