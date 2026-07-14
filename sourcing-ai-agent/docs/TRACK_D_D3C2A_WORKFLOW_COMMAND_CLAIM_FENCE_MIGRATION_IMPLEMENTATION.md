# Track D D3c2a — Dormant workflow-command claim-fence migration foundation

> Status: implementation candidate; non-live only. This batch installs only the `workflow_commands` command subbatch
> of D3b Migration A. It does not complete Migration A, activate a descriptor/writer/claim predicate, serve an Agent
> tool, close R-019 or any OB-ID, or authorize fake/scripted/live provider execution. Author validation and a fresh
> pinned non-author review are required before this candidate can be promoted.

## 1. Outcome and bounded impact

`0003_workflow_command_claim_fence_foundation.sql` adds the exact twenty command columns locked by D3b §5 and §11.1.
The migration is additive and gives existing commands the documented brownfield sentinel values. It deliberately leaves
`WORKFLOW_COMMANDS` at its existing 33-column descriptor and changes no repository writer or claim consumer. The
physical columns therefore exist for later owner cutover without creating a partially fenced runtime population.

This batch follows D3b rollout order: D3c1 installed the closed command projection prerequisite; D3c2a now installs a
dormant physical foundation. D3c1's first formal artifact was invalid and its five direct advisory findings are
fixed-forward inputs, but D3c2a does not make these columns descriptor-visible or activate the affected runtime paths.
The rest of Migration A remains open and must land in separately bounded owner batches after its exact DDL is locked.

## 2. Exact physical additions

The twenty additions, in migration order, are:

1. immutable scope/coordination: `runtime_namespace`, `provider_mode`, `workspace_id`, `scope_digest`,
   `coordination_plan_review_id`;
2. immutable authority/business lineage: `claim_authority_spec_digest`, four nullable `expected_predecessor_*` fields,
   and `d3_business_fence_digest`;
3. claim/control state: `claim_selection_generation`, `consumed_claim_authority_id`, `claim_generation`,
   `claim_token_digest`, `control_epoch`, `heartbeat_sequence`, and `last_heartbeat_id`;
4. terminal identity: nullable `terminal_event_id` plus `terminal_outcome_digest`.

The existing `operation_id` remains the sole command-to-operation link. The migration does not add an
`operation_run_id` alias and never persists a raw `claim_token` or `lease_token`.

Brownfield defaults are the D3b sentinels: empty text/digest/opaque-id values, zero generations/epochs/sequences, and
SQL `NULL` coordination/predecessor/terminal values. Merely receiving these defaults does not make a legacy command
strict or claimable through a D3 fence.

## 3. Installation constraints and ratified local names

The single `ALTER TABLE workflow_commands` installs sixteen local `CHECK ... NOT VALID` constraints. They still guard
new or updated rows while avoiding a populated-table validation scan in the installation transaction:

- empty-or-minimum-nonblank namespace/workspace/consumed-authority shapes;
- empty-or-one-of `live|simulate|scripted|replay` provider mode;
- nullable-or-positive coordination review id;
- empty/null-or-lowercase-64-hex scope, authority, business, claim-token, and terminal-outcome digests;
- non-negative selection generation, claim generation, control epoch, and heartbeat sequence;
- all-null or complete local predecessor tuple with positive phase generation and non-negative source epoch;
- both-null or both-non-null terminal event/outcome pair.

D3b named eight checks directly and left eight local names implicit. This batch ratifies the conventional
`workflow_commands_<field-or-pair>_*_ck` names recorded in the migration and tests. Its minimum-nonblank checks prove
only that a value contains a non-whitespace character; the final canonical namespace/workspace/opaque-id grammar is
not invented here and remains a later decision/validation gate. `last_heartbeat_id` coupling, `terminal_event_id`
format/existence, predecessor cross-row role, immutability/no-wrap, status/lease active-row coupling, composite FKs,
and indexes likewise remain outside this local additive subbatch.

## 4. Bounded lock and rollback behavior

The migration sets transaction-local `lock_timeout` to five seconds and restores it to `DEFAULT`. Migration-runner
ledger/checksum semantics provide idempotency; the SQL does not use `IF NOT EXISTS`. PostgreSQL DDL and the ledger row
share the runner's transaction, so a lock timeout rolls back all twenty columns, all sixteen constraints, and the 0003
ledger entry while preserving already committed 0001/0002 state. A later retry applies 0003 once, and another run is a
no-op.

## 5. Dormant read/write and public-projection boundary

The native PostgreSQL adapter still uses internal `SELECT *`/`RETURNING *` rows. Current production Store/Repository
paths immediately map those rows through the closed 33-column `WORKFLOW_COMMANDS` descriptor, which drops all twenty
new columns. D3c1 then applies the closed public command projector. D3c2a adds a regression that injects all twenty raw
columns and proves the descriptor exposes none of them; safe diagnostics `claim_generation` and `control_epoch` also
remain absent until a later explicit descriptor/runtime cutover.

No command insert, selection, claim, heartbeat, control, terminal, retry, resume, dispatch, ActivityAttempt, intent,
event, or domain-effect code reads or writes the new columns in this batch. A zero sentinel is not an authorization
predicate, and no caller may infer a fence from `attempt` or `lease_owner`.

## 6. Acceptance evidence

The candidate must prove on its final stable commit:

- exact 20-column type/nullability/order and exact legacy sentinel tuple on a populated table;
- exact sixteen `convalidated=false` checks, valid strict-shaped writes, and representative invalid new writes rejected;
- isolated 0003 RowExclusive contention times out in 4–8 seconds with zero partial DDL/ledger, then recovers once and
  becomes idempotent;
- a prior 0002 failure cannot leak any later D3 columns/checks;
- descriptor/public projection remains closed and existing command runtime behavior is unchanged;
- D3a/D3b/D3c1 characterization, targeted durable-runtime adjacency, lint, typecheck ceiling, and diff checks remain
  green.

Author evidence on the final candidate tree:

- migration runner and PG acceptance: **9 passed, 20 subtests passed**;
- D3a characterization + D3b contract + D3c1 projection + three exact durable-runtime adjacency nodes:
  **51 passed** (`6 + 34 + 8 + 3`);
- lint: **58 files already formatted; all checks passed**;
- mypy: expected existing ceiling held at **81 errors in 4 files**;
- `git diff --check`: clean.

These are author results, not independent-review evidence. A fresh pinned non-author review remains pending.

## 7. Explicit non-closure and next dependency

Full D3b Migration A still requires scoped review-session and OperationRun roots, both activity tables, workflow/event
lineage, transport response/failure receipts, late-result quarantine, their owner-specific constraints, and the exact
activity DDL that was not individually locked by D3b. Migration B/C/D backfill, FKs/indexes, active-population guards,
constraint validation, and sentinel-deletion eligibility also remain open.

Runtime scope issuer, bootstrap/strict-D3 population manifests, authority/receipt factory and verifier, Stage A/B,
generation/token/epoch CAS consumers, heartbeat occurrence, business evaluator, terminal provenance, quarantine,
dispatch races, action-root durable-scope gate, OB-10.1/10.2/10.3/10.4, and R-019 remain pending. The next batch must be
derived from Plan §6 and the owner matrix; this migration is physical substrate, not evidence that any of those owners
are implemented. The served Agent tool population remains zero.
