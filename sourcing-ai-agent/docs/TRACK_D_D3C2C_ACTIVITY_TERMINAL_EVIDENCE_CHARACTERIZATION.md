# Track D D3c2c — Activity / terminal-evidence physical surface characterization

Status: **characterization-only; no product code, migration, schema, runtime activation, provider/model call, or served
Agent population.**

The first pinned review artifact
`runtime/reviews/20260714T232853Z_Track_D_D3c2c_activity_terminal_evidence_characterization.md` is **invalid**, not a
formal `NO-GO`, because `causal_binding.final_response_item_exact=false`. Its direct advisory correctly rejected the
lexical dispatch-exposure zero proof. This fixed-forward records that surface as `unratified / undetermined`, removes
the heuristic predicate, narrows the remaining absence checks to already-ratified canonical named surfaces, and adds a
structural Migration-A order assertion. A fresh pinned review remains required.

The fixed-forward retry
`runtime/reviews/20260714T234336Z_Track_D_D3c2c_named-surface_fixed-forward.md` is also invalid solely at the old
Desktop response-item memory-annotation comparison; its printed `NO-GO` is advisory only. The prior lexical-zero issue
was closed. Its new actionable evidence is fixed here: the order oracle structurally pins every Migration-A item
5/6/7 member in the exact `ActivityRun -> ActivityAttempt -> event -> verification intent -> response receipt ->
failure receipt -> quarantine` phase sequence, §7 asserts the complete exact non-closure tuple, and §8 supersedes the
old seven-test count with the current eight-test baseline. `R-019/R-023/R-027/R-029` remain open.

The first enclosing fixed-forward commit `045267d2519d43718b24ec0fde95090126b1def8` then received pinned advisory
`NO-GO`, `P0/P1/P2/P3=0/1/1/0`. Its pin ended before the D3c1a batch that introduced the documented compatibility-only
`get_activity_run` fallback, so that archived object had `25 = 24 external + 1 internal` reads and the claimed eight-test
oracle was not self-contained. The same review also proved that the initial dictionary-based order parser rebuilt phase
numbers in marker order instead of asserting unique source order. This fixed-forward is now based after the D3c1a
dependency and requires source-ordered unique Migration-A items `1..10`, including physical `5 -> 6 -> 7`; neither
finding is waived or represented as review `GO`.

Original Scout snapshot: `e76d20e36fd34ffb6184079668b7c57deda0496f` on
`governance-phase0-ttl-20260611` (2026-07-15). The enclosing implementation commit for this documentation/test batch is
not assigned here. The exact read-call population is intentionally advanced when the later D3c1a bounded batch adds a
compatibility-only test-double fallback; the production list path uses the new batch readers.

## 1. Purpose and boundary

D3c2a installed only the dormant WorkflowCommand fragment of D3b Migration A. D3c2b installed only its dormant scoped
review-session / OperationRun root fragment. This batch mechanically characterizes the current ActivityRun,
ActivityAttempt, event, and future terminal-evidence surfaces before the next migration fragment is designed.

This document freezes observed code facts, not a target schema. In particular, it does not select future column sets,
table names, owners, foreign keys, uniqueness rules, brownfield sentinels, check constraints, lock budgets, or adoption
predicates. The next physical implementation requires **owner-ratified DDL** derived from D3b §5.2 and §11.1.

The executable oracle is
`tests/test_d3_activity_terminal_evidence_characterization.py`. It derives descriptor columns, call populations,
lexical SQL owners, reducer mutation boundaries, Python symbols, and physical table names from AST/descriptor/source;
the counts below are not accepted from this prose alone.

## 2. Current ActivityRun surface

`WORKFLOW_ACTIVITY_RUNS` owns exactly **20** current repository columns:

```text
activity_run_id, workspace_id, workflow_run_id, operation_run_id, acquisition_run_id,
command_id, parent_activity_run_id, activity_type, owner, status, phase, idempotency_key,
provider_ref_json, input_json, output_json, artifact_refs_json, entity_counts_json,
metadata_json, created_at, updated_at
```

Current AST call population:

| call | total / files | exact per-file population |
|---|---:|---|
| `upsert_activity_run` | **30 / 5** | acquisition command owner 1; command kernel 2; enrichment 4; orchestrator 14; profile-fetch owner 9 |
| `list_activity_runs` | **20 / 3** | CRM public-web owner 2; Excel-intake owner 2; orchestrator 16 |
| `get_activity_run` | **26 / 3** | orchestrator 18; profile-fetch owner 7; workflow-runtime repository self-read 1 |

The raw get total is **26 = 25 external consumers + 1 repository-internal identity read** from
`upsert_activity_run`. The extra orchestrator call is the D3c1a compatibility fallback for incomplete test doubles;
the production Store/repository expose bounded batch methods and the public list paths use zero point reads. The current
table also has exactly one table-specific direct SQL `UPDATE`, owned by
`LiveControlPlanePostgresAdapter.cancel_acquisition_owner_command`; repository upserts otherwise use the existing
generic durable-row path. That cancel owner must be included in any future ActivityRun migration/adoption proof.

## 3. Current ActivityAttempt surface

`WORKFLOW_ACTIVITY_ATTEMPTS` owns exactly **22** current repository columns:

```text
attempt_id, workspace_id, activity_run_id, workflow_run_id, command_id, attempt_number,
status, provider, provider_request_ref, provider_run_ref, started_at, completed_at,
next_retry_at, rate_limit_ref_json, error_json, input_json, output_json,
artifact_refs_json, idempotency_key, metadata_json, created_at, updated_at
```

Current AST call population:

| call | total / files | exact per-file population |
|---|---:|---|
| `upsert_activity_attempt` | **22 / 4** | command kernel 2; enrichment 4; orchestrator 9; profile-fetch owner 7 |
| `list_activity_attempts` | **21 / 3** | CRM public-web owner 2; Excel-intake owner 2; orchestrator 17 |
| `get_activity_attempt` | **2 raw / 2** | orchestrator public consumer 1; workflow-runtime repository self-read 1 |

The stable consumer count requested by the D3c2c Scout is **1 external `get_activity_attempt` call**. The raw AST total
is **2** because `upsert_activity_attempt` performs one repository-internal identity read. The oracle freezes both values
explicitly rather than mixing raw and external-consumer counting conventions.

Current `attempt_number` is derived from current payload `attempt_number` with legacy `attempt` fallback. It is not the
future D3 `command_attempt`, does not exact-copy the post-claim `workflow_commands.attempt`, and cannot be promoted into
claim authorization by reinterpretation. A future `command_attempt` requires an owner-ratified additive column and
exact-copy contract.

## 4. Current workflow-event surface and R-019 boundary

`WORKFLOW_EVENTS` owns exactly **17** current repository columns:

```text
event_id, workflow_run_id, operation_id, command_id, activity_attempt_id, event_family,
event_type, sequence_number, idempotency_key, occurred_at, recorded_at, actor, source,
payload_json, artifact_refs_json, schema_version, created_at
```

Current AST call population:

| call | total / files | exact per-file population |
|---|---:|---|
| `append_event_and_reduce` | **62 / 7** | acquisition command owner 5; CRM public-web owner 9; enrichment 4; Excel-intake owner 2; orchestrator 35; profile-fetch owner 4; seed discovery 3 |
| `list_workflow_events` | **2 / 2** | durable runtime 1; orchestrator 1 |

`DurableRuntimeWriter.append_event_and_reduce` is the sole caller of repository `append_workflow_event`, and
`LiveControlPlanePostgresAdapter.append_workflow_event` is the unique current physical `INSERT INTO workflow_events`
owner.

This uniqueness does not provide the terminal UoW required by D3b. The current path first calls
`append_workflow_event`, then `reduce_and_persist`; the reducer separately invokes `upsert_workflow_command`,
`enqueue_runtime_outbox`, and `upsert_workflow_current_state`, with no enclosing writer UoW. The event, command, outbox,
and state writes therefore remain a **multi-commit R-019 surface**. D3c2c neither closes nor waives it.

## 5. Future terminal-evidence named-surface census

For the D3b concepts whose exact table or registry symbol names are already ratified, the current canonical repository
descriptors, checked-in migrations/bootstrap SQL, and Python definition surfaces contain no matching named definition:

| future concept | current canonical named-surface observation |
|---|---|
| `verification_intent` | no exact descriptor/migration/bootstrap table definition observed |
| `transport_response_receipts` | no exact descriptor/migration/bootstrap table definition observed |
| `transport_attempt_failure_receipts` | no exact descriptor/migration/bootstrap table definition observed |
| `workflow_late_result_quarantine` | no exact descriptor/migration/bootstrap table definition observed |
| checked-in `TERMINAL_PROVENANCE_SPECS` and its three variant types | no exact Python definition observed |
| durable dispatch-exposure table/physical owner | **unratified / undetermined; no zero claim** |

The exact-name observations are deliberately bounded to canonical named schema/definition surfaces; they are not a
proof that arbitrary lexical wrappers or generic mutation gateways cannot exist. The dispatch-exposure canonical table
name and owner are not ratified by current code or D3c2c, so no sound absence oracle can be constructed yet. D3c2c
therefore records that concept as undetermined and does not invent a token-conjunction predicate or table name.

These absences and the undetermined exposure owner are blockers against schema guessing, not evidence that the concepts
can be omitted. Receipt/quarantine identity, terminal registry retention, and physical-call exposure ownership must be
ratified before implementation; once ratified, their implementation batch needs an explicit source inventory plus
mutation tests rather than a heuristic name scan.

## 6. Next bounded physical order and decisions still required

The next implementation order is:

1. dormant **ActivityRun + ActivityAttempt** additive fragment;
2. dormant **workflow event** additive fragment;
3. dormant **verification intent + response/failure receipts + late quarantine** fragment(s).

The first item is the next bounded batch. Before its SQL is written, its owner-ratified DDL must lock exact additive
columns, types, local checks, brownfield sentinel semantics, lock/rollback behavior, descriptor dormancy, all current
upsert/direct-cancel interactions, and the rule that existing `attempt_number` is not future `command_attempt`.

The follow-up D3c2d candidate performs that owner decision and installs only this first dormant fragment; see
`TRACK_D_D3C2D_ACTIVITY_CLAIM_CHAIN_MIGRATION_IMPLEMENTATION.md`. It does not retroactively broaden this
characterization or authorize the event/receipt/quarantine items below it.

D3c2e then performs the next decision-only step: it ratifies the exact eleven-column WorkflowEvent terminal-lineage
core, its eleven local check names, and the no-alias physical mapping while explicitly deferring transport provenance,
verification intent, receipts, exposure, quarantine, indexes, FKs, adoption, and runtime UoW. It installs no SQL. Only
after that decision lock may D3c2f install the dormant event-core fragment.

This order does not authorize the later items and does not move registry/manifests/bootstrap factory work ahead of a
complete Migration A. Event terminal-UoW design, verification-intent owner/CAS, response/failure receipt owner, late
quarantine gateways, terminal registry, and durable dispatch-exposure table/owner all remain decisions for their
applicable ratified batch.

## 7. Explicit non-closure

D3c2c changes no runtime behavior and closes none of these gates:

- `R-019`: atomic owner effect / command-attempt-event terminal UoW remains open;
- `R-023`: durable-runtime cutover and residual tripwire remains open;
- `R-027`: scope-matched formal review status remains open where not already covered by a valid artifact;
- `R-029`: action request-schema compatibility/adoption remains open;
- action-root durable-scope gate and `OB-10.1/10.2/10.3/10.4` remain open;
- Migration A remainder, Migration B-D, registries/manifests/factory, Stage A/B, dispatch, provider/model, live/W6,
  promotion/signoff, and served Agent tool population remain closed to activation.

This characterization is author evidence only. It is not a formal independent-review `GO` and does not broaden the
scope-local D3c2b review artifact.

## 8. Author validation

Current full-oracle author evidence (the eight-test oracle to be pinned by the fixed-forward commit):

- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_d3_activity_terminal_evidence_characterization.py`
  → **8 passed**;
- `.venv/bin/ruff format --check tests/test_d3_activity_terminal_evidence_characterization.py`
  → **1 file already formatted**;
- `.venv/bin/ruff check tests/test_d3_activity_terminal_evidence_characterization.py`
  → **all checks passed**;
- `git diff --check`
  → clean.

The earlier **7 passed** full-oracle result and **3 passed / 5 deselected** focused result predated the complete
Migration-A member-order and exact non-closure assertions. They are superseded historical evidence, not the current
validation baseline.

These commands validate this oracle/document batch only; they do not validate future DDL or close any gate in §7.
