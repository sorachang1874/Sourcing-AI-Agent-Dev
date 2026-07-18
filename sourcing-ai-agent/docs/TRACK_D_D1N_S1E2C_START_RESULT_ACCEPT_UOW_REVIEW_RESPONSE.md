# Track D D1n S1e2c — formal review partial response

Status: partial implementation response to the formal S1e2c `NO-GO` artifact
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

## New regression evidence

- `tests/test_d1n_start_acquisition_v2_create_pg.py` now includes forged serialized-result and forged-error terminal
  zero-write acceptance cases.
- Existing accept/replay/quarantine/root-hop tests continue to exercise the held-command release path and zero
  `runtime_outbox`/provider/model behavior.

## Still open from the S1e2c formal review

- Accepted replay and late-result quarantine after normal downstream command progress still need a dedicated
  monotonic-successor replay rule.
- A shared lock/probe topology across create, accept, completion, and controls remains open under `R-019`.
- The central pre-agent contract matrix and fast preflight still need the S1e2c command-hold/release owner entry.
- S1e2d covers the first released-root consumer hop, and the S1e2b response covers held generic command controls, but
  those separate commits do not by themselves close this S1e2c formal review.
