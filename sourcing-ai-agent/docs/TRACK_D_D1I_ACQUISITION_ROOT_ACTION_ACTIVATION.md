# Track D D1i — acquisition root action activation

> Status: bounded non-live fixed-forward remediation candidate after pinned `dce094e` advisory
> `NO-GO P0/P1/P2/P3=0/2/2/0`; fresh review of the remediation commit is pending (2026-07-16). This batch activates only
> `start_acquisition_run` as the fifth schema-defined production action. The D1i checkpoint partition was
> **5 schema-defined / 10 schema-less / served=0**; D1l later reached its **9/6/0** checkpoint, and the D1m candidate
> now makes the current candidate partition **10 schema-defined / 5 schema-less / served=0**. It does not authorize a
> provider/model call, live validation, product signoff, or closure of R-019/R-029. Author tests are evidence, not an
> independent-review verdict.

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
- the root owner requires exact command operation/action/payload/canonical-envelope parity, canonical source-event and
  causality identity, an approved nonterminal action, a nonterminal operation, and the exact current claim owner,
  attempt, and unexpired lease. Lease validity uses the PG repository clock and parses persisted naive timestamps as
  UTC rather than inheriting the database session timezone;
- persisted root, source-event, plan-event, child-command, causality, payload, and result contracts are decoded with
  strict JSON container and type checks. Missing schema pins, object/list substitution, boolean/integer confusion, or
  forged deterministic identities fail closed;
- authority/preflight failure may terminalize only the root command through an exact-current-claim failure CAS. It
  never uses the untrusted command `operation_id` to synchronize an OperationRun/AgentAction and creates no child,
  workflow event/outbox/current-state, plan review, acquisition run, job, Activity, or EntityDelta;
- cancel-after-dispatch-before-drain leaves the operation/action cancelled and the root owner creates no child;
- the positive root completion locks the root command and workflow stream, revalidates the canonical root source event,
  appends or exact-reuses one `CommandPlanRequested` event, creates or exact-reuses one deterministic
  `acquisition.intent.resolve` child, and terminalizes the root in one PG transaction. Any mismatch or injected fault
  rolls back the event, child, and root terminal together;
- migration `0008_acquisition_intent_parent_uniqueness.sql` adds an actual-root-scoped trigger plus parent identity
  advisory locking and a non-unique parent support index. Only a real `acquisition.run.create` parent is fenced: it may
  have exactly one child, and that child must be `acquisition.intent.resolve`. Generic workflow fan-out, including
  multiple intent-typed children under a non-root parent, remains legal. Brownfield wrong-type children or duplicate
  children under an acquisition root make migration application and its ledger row roll back fail-closed. The UoW uses
  conflict-do-nothing plus a locked exact reread, so a concurrent canonical winner is reusable while an alternate
  winner is rejected;
- root terminalization atomically writes the deterministic child id to both the terminal result and physical
  `downstream_command_ids_json`. Terminal authority expects that physical edge only for `succeeded`; creation-time
  payload causality remains unchanged, and empty/foreign/extra terminal edges fail closed;
- succeeded replay must exact-match the persisted root result, root-plan event, deterministic child, ordering, and full
  envelopes before it can repair post-commit current-state/recovery wakeup and linked Operation synchronization.
  Scheduler-owned child fields such as `not_before_at` are lifecycle state rather than immutable creation identity, so
  valid retry-wait/running/succeeded children remain replayable without clearing their schedule;
- if the driver raises after the PG root UoW commit, the owner performs a fresh authoritative read and enters the same
  exact succeeded replay only when the root is durably `succeeded`; a pre-commit exception is re-raised and the UoW
  remains rolled back. Replay creates no duplicate, and the root stage itself creates no job, plan review, acquisition
  run, or provider work.

## Residual boundaries

R-019 remains open, but the former root event/child/root partial-completion gap, this typed root's parent-child phantom
window, and successful-COMMIT acknowledgement recovery are closed inside the D1i specialization. This does not install
a global generation fence for other command families. The OperationRun/AgentAction authority preflight is still
outside that UoW, so a concurrent
aggregate cancel can race between the preflight and the locked root completion. `workflow_current_state`, recovery
wakeup, and linked Operation synchronization also remain post-commit repair work rather than members of the root UoW.
The approve/retry/resume read-only preflights remain outside their existing writer UoWs, although normal repository APIs
do not mutate the persisted request/target. Exact failure CAS removes stale-claim terminal writes, but an acknowledgement
loss after a committed failure can still leave the caller with an ambiguous stale/queued observation until recovery.
This batch does not claim aggregate-cancel atomicity or four-table exactly-once completion and adds no operation
state-mutator; the existing 26-call ratchet must not rise.

At the D1i checkpoint, R-029 fell from 11 to **10** schema-less actions but remained open; D1l later reached a
**6/15** checkpoint, and the D1m candidate moves the current candidate numerator to **5/15**. The compatibility
observation epoch stays
`d1f_r029_20260715_v2` because D1i does not start a new release window. The deletion condition is unchanged: all
API-submittable actions need reviewed owner/schema contracts and the complete population must record zero bridge hits
for one release window. Served-only evidence is insufficient.

The served Agent tool population remains zero. D1i does not add a model-safe result schema, planner tool projection,
provider/model transport, cost authorization, live path, or product UI exposure.

## Validation

Final stable candidate author evidence:

- exact D1i PG action/transport/owner/fencing matrix: `20 passed + 54 subtests`;
- combined D1 request/schema/binder/transport matrix: `160 passed + 289 subtests`;
- command/control adjacency: `175 passed + 503 subtests`;
- durable runtime + CRM Public Web batch adjacency: `61 passed + 12 subtests`;
- storage-surface guardrails: `60 passed`;
- exact R-019 characterization ratchet nodes: `3 passed`;
- `make lint`: green across `58 files`;
- `make typecheck`: unchanged accepted ceiling, `81 errors / 4 files`;
- Python compilation and `git diff --check`: green.

The first pinned advisory was `NO-GO P0/P1/P2/P3=0/2/1/0`; `dce094e` closed those findings locally. Its fresh pinned
non-author review then returned advisory `NO-GO 0/2/2/0`: successful-COMMIT acknowledgement loss was not reconciled,
the typed parent had no cross-producer uniqueness fence, physical root causality stayed empty, and child retry
scheduling was incorrectly immutable. This fixed-forward candidate implements all four remediations, but author tests
and this reconciliation are not a review artifact or a `GO`. A fresh pinned review of the new commit remains required
before promotion; no formal review or live/provider gate is authorized by the local evidence.
