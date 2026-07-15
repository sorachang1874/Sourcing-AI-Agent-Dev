# Track D D1i — acquisition root action activation

> Status: bounded non-live implementation candidate (2026-07-16). This batch activates only
> `start_acquisition_run` as the fifth schema-defined production action. The production partition is
> **5 schema-defined / 10 schema-less / served=0**. It does not authorize a provider/model call, live validation,
> product signoff, or closure of R-019/R-029. Author tests are evidence, not an independent-review verdict.

## Outcome

`start_acquisition_run` no longer accepts an open-ended caller-authored workflow-command envelope. Its canonical
request is:

```json
{
  "input": {
    "target_company": "Thinking Machines Lab",
    "query": "pre-training researchers"
  }
}
```

Both fields are required nonblank strings. `raw_user_request` remains one presence-based compatibility alias for
`query`; supplying both is invalid. The route/orchestrator reject caller-owned command/workflow/job/review/retry and
identity fields, including `command_type`, `command_payload`, `workflow_payload`, `workflow_run_id`, `job_id`,
`plan_review_id`, `requester_id`, `tenant_id`, `workspace_id`, `max_attempts`, `retry_policy`, and the previously
ignored `limit`.

The caller supplies no `target_ref`. The acquisition owner mints exactly:

```json
{"workspace_id": "<server workspace>"}
```

Authenticated HTTP derives that workspace and actor from request identity and passes the exact workspace/user owner
scope. Open/operator mode preserves its explicit workspace and actor. A nonempty raw target, incomplete owner scope,
or authenticated workspace mismatch is invalid before any durable write.

## Canonical command

After approval, dispatch derives exactly one `acquisition.run.create` root command. The server owns:

- command type and owner (`acquisition_run_writer`);
- deterministic workflow id and operation/action linkage;
- workspace/tenant namespace and empty requester adjunct;
- fixed `max_attempts=5` and retry policy;
- target company/query projection and the exact owner-minted workspace snapshot;
- command idempotency, causality, source, migration phase, and decomposition contract.

The planner does not merge or fall back to caller `target_ref`, nested workflow payloads, job ids, review ids, or
retry settings. Brownfield `acquisition.run.create` commands without an exact OperationRun→AgentAction link fail
closed; there is no discovered legitimate non-Operation producer for this root type.

## Lifecycle and zero-write fences

- submit validates the closed request plus owner target before action/run/event writes;
- approve, retry, and resume revalidate the persisted schema pins and target workspace before their state/event writes;
- dispatch revalidates the same target before command planning;
- the root owner requires exact command operation/action/payload/canonical-envelope parity, an approved nonterminal
  action, a nonterminal operation, and a successfully acquired currently valid running lease before it may plan the
  child;
- authority/preflight failure may terminalize only the root command. It never uses the untrusted command
  `operation_id` to synchronize an OperationRun/AgentAction and creates no child, workflow event/outbox/current-state,
  plan review, acquisition run, job, Activity, or EntityDelta;
- cancel-after-dispatch-before-drain leaves the operation/action cancelled and the root owner creates no child;
- the positive root owner creates exactly one deterministic `acquisition.intent.resolve` child. Replay creates no
  duplicate and the root stage itself creates no job, plan review, acquisition run, or provider work.

## Residual boundaries

R-019 remains open. The post-claim authority check and child `CommandPlanRequested` append are not one PG UoW, so a
concurrent cancel can still race between them. The approve/retry/resume read-only preflights are also outside their
existing writer UoWs, although normal repository APIs do not mutate the persisted request/target. This batch neither
claims concurrent-cancel atomicity nor adds a new operation state-mutator or transaction-lock caller; the existing
26-call ratchet must not rise.

R-029 falls from 11 to **10** schema-less actions but remains open. The compatibility observation epoch stays
`d1f_r029_20260715_v2` because D1i does not start a new release window. The deletion condition is unchanged: all
API-submittable actions need reviewed owner/schema contracts and the complete population must record zero bridge hits
for one release window. Served-only evidence is insufficient.

The served Agent tool population remains zero. D1i does not add a model-safe result schema, planner tool projection,
provider/model transport, cost authorization, live path, or product UI exposure.

## Validation

Final stable-tree author evidence:

- D1i PG action/transport/owner matrix: `9 passed + 28 subtests`;
- combined D1 request/schema/binder/transport matrix: `153 passed + 228 subtests`;
- command/control adjacency: `175 passed`;
- exact acquisition Operation + R-019 state-sync ratchet nodes: `2 passed`;
- full Operation runtime: `136 passed + 503 subtests`;
- `make lint`: green across `58 files`;
- `make typecheck`: unchanged accepted ceiling, `81 errors / 4 files`;
- `git diff --check` and Python compilation: green.

Fresh pinned non-author review remains required before promotion. Author tests and the unchanged mypy ceiling are not
an independent-review verdict.
