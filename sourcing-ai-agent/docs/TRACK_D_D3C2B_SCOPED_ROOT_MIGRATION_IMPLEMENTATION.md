# Track D D3c2b — Dormant scoped review-session and OperationRun root foundation

> Status: implementation candidate; non-live only. This batch installs only the scoped review-session and OperationRun
> root fragment of D3b Migration A. It does not activate a scoped-session repository or writer, adopt a legacy row,
> expose the new columns through public/runtime records, mint authority, close R-019/OB gates, or serve an Agent tool.
> Author validation and a valid scope-local pinned `GO` are recorded below. That `GO` approves only this dormant
> physical fragment; it does not promote full Migration A, close R-019/OB gates, or authorize runtime/live activation.

## 1. Outcome and bounded impact

D3b §12 fixes the rollout order: public projection, complete Migration A, then registries/manifests/dormant authority
factories. D3c2a installed only Migration A's dormant `workflow_commands` fragment. D3c2b advances the next exact,
already-locked fragment without crossing into registry/runtime activation:

- `plan_review_sessions` gains the eleven scope, causal-plan, and idempotency columns specified verbatim by D3b §5.2;
- `operation_runs` gains the five scope exact-copy and nullable coordination columns specified by the same section;
- both existing runtime mappings remain closed and continue dropping every new column.

The legacy plan-review creator and OperationRun writers therefore continue writing only their historical fields; the
database supplies explicit empty/zero/NULL sentinels. No existing row becomes a strict D3 scope root.

## 2. Exact physical additions

`plan_review_sessions` receives, in migration order:

1. `runtime_namespace`, `provider_mode`, `workspace_id`, `scope_issuer`, `scope_digest`;
2. `creation_source_workflow_command_id`, `creation_source_event_id`;
3. `creation_plan_id`, `creation_plan_revision`, `creation_plan_bundle_digest`;
4. `creation_idempotency_key`.

All text fields are `TEXT NOT NULL DEFAULT ''`; `creation_plan_revision` is `BIGINT NOT NULL DEFAULT 0`.

`operation_runs` already owns `workspace_id`, so it receives only `runtime_namespace`, `provider_mode`, `scope_issuer`,
and `scope_digest` as `TEXT NOT NULL DEFAULT ''`, plus nullable `BIGINT coordination_plan_review_id`. It does not gain a
second workspace owner, review-id alias, command link, or business/authority digest.

## 3. Local installation checks

`0004_d3_scoped_root_foundation.sql` installs sixteen local `CHECK ... NOT VALID` constraints. The checks still guard
new writes while avoiding a populated-table validation scan:

- namespace/workspace/causal identifiers are empty sentinel or contain a non-whitespace character;
- provider mode is empty or one of `live|simulate|scripted|replay`;
- scope issuer is empty or exactly `plan_review_session`;
- scope, plan-bundle, and creation-idempotency digests are empty or lowercase 64-hex;
- plan revision is non-negative;
- coordination review id is SQL `NULL` or positive `BIGINT`.

The longest conventional constraint name would exceed PostgreSQL's 63-byte identifier limit, so the ratified local
name is `plan_review_sessions_creation_source_command_id_shape_ck`; the checked column remains the exact
`creation_source_workflow_command_id` contract field.

Cross-field completeness, exact scope-digest recomputation, `(scope_digest, creation_idempotency_key)` uniqueness,
session-to-operation foreign keys, strict-row active shapes, constraint validation, and sentinel deletion belong to
later Migration B/C/D batches. D3c2b does not approximate them with local checks.

## 4. Lock, rollback, and idempotency behavior

The migration uses a five-second transaction-local lock timeout and restores it to `DEFAULT`. The migration runner
commits both table alterations and the `0004` ledger row in one transaction. A RowExclusive blocker on
`operation_runs` therefore lets the preceding `plan_review_sessions` alteration execute inside the pending transaction,
then forces timeout; PostgreSQL rolls back both tables' columns/checks and the ledger row. After the blocker releases,
one retry applies `0004` once and the next run is a no-op.

## 5. Dormant read/write and trust boundary

`ControlPlaneStore._plan_review_session_from_row` remains the legacy closed plan-review mapper, while
`repositories.workflow_runtime.OPERATION_RUNS` remains the closed OperationRun descriptor. D3c2b deliberately does not
add any of the sixteen columns to either mapper. Native `SELECT *` and `RETURNING *` can observe the physical row inside
the PG adapter, but public/repository records cannot infer strict scope from the sentinels or caller-supplied values.

There is no `create_or_exact_replay_scoped_plan_review_session`, bootstrap lock, scope digest calculation, exact-copy
OperationRun writer, authority/receipt, claim CAS, dispatch, provider/model call, or served Agent command in this batch.

## 6. Acceptance and review evidence

Author evidence on the final candidate tree:

- complete migration-runner/PG suite: **11 passed, 36 subtests passed**;
- scoped-root populated-table, strict-shape, malformed-write, and closed-mapper node: **1 passed, 16 subtests passed**;
- D3a + D3b + D3c1a plus three exact durable-runtime adjacency nodes: **58 passed**
  (`6 + 36 + 13 + 3`);
- exact operation/plan-review adjacency: **3 passed**;
- repository lint: **58 files already formatted**, all checks passed; the two modified test files also pass targeted
  Ruff format/check;
- global mypy ratchet: unchanged at **81 errors in 4 files** (26 source files checked);
- `git diff --check`: clean.

These test results are author evidence. The implementation is commit
`0aa253c7d7e5324f5c0021570e2980f358ea3922`. Fresh pinned non-author review artifact
`runtime/reviews/20260714T222746Z_Track_D_D3c2b_scoped_root_migration_retry_1.md` is a valid scope-local **GO**:

- base/head: `4cfd1916da8bd98483d1ecfdba1f66639b122da9..0aa253c7d7e5324f5c0021570e2980f358ea3922`;
- exact nine-file pinned scope;
- scope digest: `7988dd50814ba1c2cc4a3c5efa7ec40ac7eb47b80cbce901ecc8c04a83a92685`;
- reviewer process exit `0`, no timeout or model reroute, complete single-turn causal binding;
- findings: `P0/P1/P2/P3=0/0/0/0`, final `GO`.

The earlier `20260714T221531Z_*` attempt is invalid and is not review evidence. The valid `GO` above does not close
`R-019` or any of the explicit non-closures in §7.

## 7. Explicit non-closure and next dependency

Migration A still lacks the ActivityRun/ActivityAttempt, workflow-event/source-terminal, response/failure receipt, and
late-result quarantine fragments. Their owner-specific exact field inventories and table interactions must be
characterized and locked before implementation; this batch does not guess them. Registry/policy pins, the two fenced
population manifests, and dormant bootstrap factory/verifier remain rollout step 3 and cannot precede complete
Migration A.

Migration B-D, the scoped-session repository, OperationRun exact-copy runtime, Stage A/B, claim generation/token/epoch,
business/terminal/dispatch paths, action-root durable-scope gate, OB-10.1/10.2/10.3/10.4, R-019, and served Agent tool
population all remain open. No fake, scripted, or live provider/model path is authorized by this physical foundation.
