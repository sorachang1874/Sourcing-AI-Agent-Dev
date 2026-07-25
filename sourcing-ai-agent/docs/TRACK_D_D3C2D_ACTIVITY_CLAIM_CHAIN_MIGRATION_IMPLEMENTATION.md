# Track D D3c2d — Dormant ActivityRun / ActivityAttempt claim-chain foundation

> Status: implementation candidate; non-live only. This batch installs only the ActivityRun/ActivityAttempt fragment
> of D3b Migration A. It does not activate descriptor reads, strict writers, claim/effect predicates, backfill,
> provider/model dispatch, or a served Agent tool. Author validation and a fresh pinned non-author review are required
> before promotion.

## 1. Outcome and owner decision

D3c2c mechanically froze the current activity surfaces before physical work. This batch ratifies the exact activity
DDL from D3b §5.2 and §11.1 and installs it as `0005_d3_activity_claim_chain_foundation.sql`:

- `workflow_activity_runs` receives six ActivityRun columns for immutable runtime scope, coordination, authority
  policy, and business-fence lineage;
- `workflow_activity_attempts` receives ten ActivityAttempt columns that exact-copy the operation/scope/coordination
  lineage plus claim generation, post-claim command attempt, and control epoch.

The workflow-runtime repository remains the future semantic owner of the strict command → ActivityRun →
ActivityAttempt chain. This migration is only its dormant physical substrate. It neither accepts caller scope nor
derives authority from JSON, ambient environment, `owner`, `attempt_number`, or the new sentinels.

## 2. Exact additive columns

The six ActivityRun columns, in migration order, are:

```text
runtime_namespace, provider_mode, scope_digest, coordination_plan_review_id,
claim_authority_spec_digest, d3_business_fence_digest
```

`workspace_id`, `operation_run_id`, and `command_id` already exist on `workflow_activity_runs`; the migration does not
add aliases for them.

The ten ActivityAttempt columns, in migration order, are:

```text
operation_run_id, runtime_namespace, provider_mode, scope_digest,
coordination_plan_review_id, claim_authority_spec_digest, d3_business_fence_digest,
claim_generation, command_attempt, control_epoch
```

`workspace_id`, `activity_run_id`, and `command_id` already exist on `workflow_activity_attempts`. The new
`command_attempt` is a non-negative brownfield-sentinel column reserved for an exact copy of the post-claim
`workflow_commands.attempt`. Existing `attempt_number` keeps its current provider/local retry accounting and is not
renamed, backfilled into, compared with, or reinterpreted as `command_attempt` in this batch.

## 3. Brownfield sentinels and local checks

Existing rows receive empty text/digest sentinels, SQL `NULL` coordination, and zero generation/attempt/epoch values.
Those values are explicitly ineligible for strict D3 and do not authorize a claim, ActivityAttempt, effect, terminal
write, or dispatch.

The migration installs eighteen local `CHECK ... NOT VALID` constraints: seven on ActivityRun and eleven on
ActivityAttempt. They enforce new/updated-row grammar without scanning populated tables in the installation
transaction:

- namespace and existing workspace contain a non-whitespace character when applicable;
- provider mode is empty or one of `live|simulate|scripted|replay`;
- coordination review id is SQL `NULL` or a positive `BIGINT`;
- scope, authority-policy, and business-fence digests are empty or lowercase 64-hex;
- the new attempt operation id is empty or non-whitespace;
- claim generation, `command_attempt`, and control epoch are non-negative.

These are local shape checks only. Canonical namespace/workspace/opaque-id grammar, parent equality, immutability,
strict-population status coupling, composite indexes/FKs, and active-row validation remain Migration B/C/D work.

## 4. Lock, rollback, and descriptor dormancy

`0005` uses transaction-local `lock_timeout = '5s'`, alters ActivityRun first and ActivityAttempt second, then restores
the setting to `DEFAULT`. The migration runner commits the DDL and ledger row in one PostgreSQL transaction. A writer
holding the second table therefore proves that a timeout rolls back the already-executed first-table DDL, all eighteen
checks, and the `0005` ledger entry. A later retry applies once; the following run is a no-op.

The repository keeps its current 20/22-column descriptors. Raw `SELECT *` rows are immediately mapped through those
closed descriptors, so none of the six or ten new fields reaches repository/public records. No existing
`upsert_activity_run`, `upsert_activity_attempt`, list/get path, or direct ActivityRun cancel `UPDATE` reads or writes
the new columns. Runtime cutover must update every characterized writer and the direct cancel owner in one later
owner batch; a column default is never an authorization fallback.

## 5. Acceptance evidence required

The final candidate must prove:

- exact column order/type/nullability/default and exact populated-row sentinels;
- all eighteen checks are `convalidated=false`, accept a strict-shaped row, and reject representative invalid writes;
- `attempt_number=7` and `command_attempt=3` coexist as distinct values;
- an ActivityAttempt-table lock timeout after the ActivityRun `ALTER` leaves zero partial columns/checks/ledger, then
  recovery applies exactly once;
- both existing descriptors drop every new raw column;
- D3a/D3b/D3c1/D3c2 characterization, migration adjacency, lint/typecheck ceiling, and diff checks remain green.

Author evidence on the candidate tree:

- D3c2d contract slice: **2 passed**;
- exact real-PG install/guard plus second-table timeout/recovery: **2 passed + 18 subtests**;
- full migration-runner suite: **13 passed + 54 subtests**;
- Ruff format/check on the two changed Python test files: clean;
- `git diff --check`: clean.

The full D3b/D3c contract suite is rerun from the pinned clean commit because concurrent D3c1a frontend fixed-forward
work changes its adapter-source invariants in the shared tree. That clean-commit result is handoff evidence, not a
reason to stage or absorb the concurrent frontend files. These author results are not independent-review evidence.

## 6. Explicit non-closure and next batch

The workflow-event fragment is still absent. So are verification intent, terminal policy registry, dispatch exposure,
response/failure receipt, late quarantine, terminal command/attempt/event UoW, and the corresponding repositories and
race proofs. Full Migration A is therefore incomplete, and rollout step 3 registries/manifests/factory cannot begin.

Migration B-D, scoped-session runtime issuance, OperationRun/command/activity exact-copy writers, Stage A/B,
generation/token/epoch CAS, heartbeat, business/terminal/dispatch paths, the action-root durable-scope gate,
`OB-10.1/10.2/10.3/10.4`, `R-019`, `R-023`, `R-027`, `R-029`, provider/model execution, live/W6/manual signoff, and
served population remain open or zero. The next bounded Track D slice is the workflow-event physical decision and
dormant migration fragment; it must not guess the still-unratified dispatch-exposure or receipt/quarantine schema.
D3c2e is that decision lock and changes no SQL. It ratifies only the exact eleven-column event core and eleven local
checks while carrying every transport/intent/evidence deferral. D3c2f is the subsequent dormant migration batch.

D3c2f now installs only that ratified event-core substrate; see
`TRACK_D_D3C2F_WORKFLOW_EVENT_TERMINAL_LINEAGE_MIGRATION_IMPLEMENTATION.md`. All later evidence-surface physical
decisions remain unratified and must not be inferred from this activity migration.
