# Track D D1n S1e2d — acquisition-start v2 root-command consumer compatibility

Status: author implementation candidate; non-live; no provider/model/network invocation; no served registry change;
fresh independent review pending.

## Scope

S1e2d closes the first simulated consumer hop after S1e2c result acceptance. Once S1e2c accepts the
`acquisition_start_result_v2` terminal result and releases the dormant root `workflow_commands.not_before_at` hold, the
existing acquisition root command owner can now consume the v2 root payload and plan the downstream
`acquisition.intent.resolve` command.

## Contract

The root command owner now has a v2-only preflight branch for commands whose payload schema is
`acquisition_root_command_payload.v2`. That branch does not reinterpret model/user input. It validates the command,
Action, OperationRun, and owner-result reference agree on:

- `action_id`
- `operation_run_id`
- `workflow_run_id`
- `workflow_command_id`
- `root_command_payload_digest`
- approved/queued start-v2 Action and queued acquisition Operation lifecycle

The compatibility workflow payload copied to the downstream intent command is derived only from the immutable
`start_snapshot.preview` bundle: company target, cohort selection, source preferences, thematic constraints,
provider-mode intent, budget, provider manifest reference, preview reference, snapshot digest, and confirmation receipt
reference. Legacy `query` remains a compatibility field and is populated only from provider manifest lane query text when
present.

## Evidence

- `tests/test_d1n_start_acquisition_v2_create_pg.py` includes the S1e2a/S1e2b/S1e2c/S1e2d path:
  accepted start result -> released root command -> acquisition root owner drain -> queued downstream
  `acquisition.intent.resolve` command.
- The same test asserts `runtime_outbox=0`, `acquisition_runs=0`, no live provider/model call, and the copied
  `acquisition_start_v2_root_owner_compat_payload.v1` company/cohort payload.
- Legacy adjacent root-owner test remains green.

## Explicit non-closure

- This is an author candidate, not a formal independent-review `GO`.
- This does not serve the Agent tool publicly and does not run live providers/models.
- It only proves the first command-owner hop. The downstream intent/plan/provider/simulated E2E chain remains future
  validation.
- S1e2b currently has an independent-review `NO-GO`; this batch does not close those fixed-forward findings.
