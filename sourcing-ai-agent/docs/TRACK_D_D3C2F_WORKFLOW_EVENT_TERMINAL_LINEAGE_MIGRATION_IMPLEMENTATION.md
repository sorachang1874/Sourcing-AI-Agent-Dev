# Track D D3c2f — Dormant WorkflowEvent terminal-lineage foundation

> Status: implementation candidate; non-live only. This batch installs only the eleven-column WorkflowEvent core
> ratified by D3c2e. It does not activate descriptor reads, strict writers, terminal atomicity, provider/model
> dispatch, or a served Agent tool. Author validation and a fresh pinned non-author review are required before
> promotion.

## 1. Outcome and owner boundary

D3c2e separated the already-ratified terminal event lineage from the still-unratified transport-evidence family. This
batch installs that bounded decision as `0006_d3_workflow_event_terminal_lineage_foundation.sql`:

- one `ALTER TABLE workflow_events` adds eleven WorkflowEvent columns for immutable runtime scope, source
  ActivityRun/claim lineage, policy/business pins, and the nullable terminal outcome digest;
- eleven local `CHECK ... NOT VALID` constraints enforce only brownfield-safe value grammar;
- the existing workflow-runtime repository remains the future semantic owner, but its current descriptor, mapper,
  replay predicate, and writer stay closed over the original seventeen fields.

The new fields are dormant substrate. Empty, zero, and SQL `NULL` sentinels do not authorize a claim, terminal result,
domain effect, retry, dispatch, response acceptance, or late-result handling.

## 2. Exact additive columns and sentinels

The eleven WorkflowEvent columns, in migration order, are:

```text
runtime_namespace, provider_mode, workspace_id, scope_digest,
coordination_plan_review_id, activity_run_id, claim_generation, control_epoch,
claim_authority_spec_digest, d3_business_fence_digest, terminal_outcome_digest
```

Text scope/id fields and digests are `TEXT NOT NULL DEFAULT ''::text`; generation and epoch are
`BIGINT NOT NULL DEFAULT 0`; `coordination_plan_review_id` and `terminal_outcome_digest` are nullable with no default.
The exact populated-row and current-writer default tuple is:

```text
('', '', '', '', NULL, '', 0, 0, '', '', NULL)
```

The migration reuses existing `workflow_run_id`, `operation_id`, `command_id`, and `activity_attempt_id`. It adds no
event-side `operation_run_id`, `source_verification_command_id`, `source_activity_attempt_id`, or
`source_command_attempt` alias. The linked ActivityAttempt continues to own post-claim `command_attempt`; the future
verification intent exact-copies that value separately.

## 3. Eleven local checks, not authority

The migration installs exactly eleven local `CHECK ... NOT VALID` constraints:

- namespace, workspace, and ActivityRun id are empty or contain a non-whitespace character;
- provider mode is empty or one of `live|simulate|scripted|replay`;
- coordination review id is SQL `NULL` or a positive `BIGINT`;
- scope, authority-policy, business-fence, and nullable terminal-outcome digests are lowercase 64-hex when present;
- claim generation and control epoch are non-negative.

These checks constrain new/updated-row shape without scanning the populated table. They do not prove parent existence,
scope equality, exact-copy immutability, command/attempt/event status parity, terminal provenance, receipt identity,
strict-population membership, or authorization. Constraint validation and sentinel retirement remain later adoption
work.

## 4. Lock budget, rollback, and exact-once recovery

`0006` sets transaction-local `lock_timeout = '5s'`, executes the single table alteration, and restores the setting to
`DEFAULT`. The migration runner commits DDL and the `0006` ledger row in one PostgreSQL transaction.

Acceptance therefore includes a real-PG writer holding `workflow_events` while the migration waits: after the bounded
timeout, all eleven columns, all eleven checks, and the ledger entry must be absent. Releasing the writer permits one
successful application; the immediately following run must be a no-op with the complete migration ledger intact.

## 5. Descriptor and writer dormancy

`WORKFLOW_EVENTS` remains a 17-column descriptor. Raw `SELECT *` rows are mapped through that closed descriptor, so none
of the new fields reaches repository or public records. `LiveControlPlanePostgresAdapter.append_workflow_event` remains
the only explicit physical INSERT owner; its current explicit INSERT continues to omit all eleven fields, causing only
database defaults to materialize the dormant sentinel tuple.

No replay identity, idempotency key, sequence rule, event/reducer transaction boundary, public projection, repository
method, or runtime call site changes in this batch. A column default is never an authorization fallback.

## 6. Acceptance evidence

The final stable candidate must prove:

- exact column order, SQL type, nullability, default, and both populated/current-writer sentinel tuples;
- all eleven checks have `convalidated=false`, accept a strict-shaped update, and reject one representative malformed
  update per field;
- a lock timeout leaves zero partial columns/checks/ledger, followed by exact-once recovery and no-op replay;
- the descriptor remains exactly 17 columns and drops all eleven raw fields;
- the explicit writer still omits the new fields and all deferred evidence/runtime families remain absent;
- the adjacent D3c2e decision oracle, D3 claim-fence contract, migration suite, lint/type ceiling, and diff checks stay
  green.

Author evidence is recorded only from the final stable tree. It is not independent-review evidence and does not
authorize a runtime or live path.

Final stable-tree author evidence:

- D3c2f static migration/document contract: **2 passed**;
- D3c2e decision oracle: **7 passed**;
- exact real-PG install/guard plus lock-timeout/recovery: **2 passed + 11 subtests**;
- full migration-runner suite: **15 passed + 65 subtests**;
- D3 claim-fence plus D3c2e adjacent contract: **47 passed**;
- Ruff format/check on the three changed Python test files: clean;
- mypy ceiling: unchanged at **81 errors in 4 files**;
- `git diff --check`: clean.

These are author results, not an independent verdict.

Pinned review record: commit `47a7f7db8582fef36afbeecf15fa6310f3e3c048` received a fresh non-author
medium-effort **ADVISORY NO-GO** with P0/P1/P2/P3=`0/0/2/0`. The SQL/runtime boundary itself passed; the two findings
were contract-evidence gaps: stale D3c2e wording still claimed `0006` absence, and the zero-PG static preflight did not
exact-compare type/default/nullability plus complete constraint names/predicates. The immediate fixed-forward corrects
both without changing migration or runtime behavior; a fresh pinned re-review is required. This advisory is not a
formal highest-effort verdict and authorizes no live/signoff path.

## 7. Explicit non-closure and next bounded decision

D3c2f does not complete Migration A. Event transport provenance, verification intent, response/failure receipts,
dispatch exposure, late quarantine, terminal policy registry, indexes/FKs, population checks, adoption/backfill,
exact-copy writers, terminal command/attempt/event/source-intent atomicity, and response/retry/quarantine races all
remain open.

So do Migration B-D, the action-root durable-scope gate, `OB-10.1/10.2/10.3/10.4`, `R-019`, `R-023`, `R-027`,
`R-029`, registries/manifests/factory, Stage A/B, provider/model execution, live/W6/manual signoff, and served Agent
population.

The next bounded Track D batch is decision/Scout work for the remaining Migration-A evidence surfaces. It must first
ratify the complete physical owners and DDL for verification intent, durable dispatch exposure, response/failure
receipts, and late quarantine against D3b §6/§11 plus Plan §6/OB-10.1-10.4. It may not write SQL while the
dispatch-exposure owner/table or any receipt/intent/quarantine key, type, check, CAS, or retention identity remains
unratified.
