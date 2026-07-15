# Track D D3c2e — WorkflowEvent terminal-lineage physical decision lock

> Status: decision lock only; non-live. This batch changes no migration, descriptor, repository, runtime writer,
> provider/model path, or served Agent population. It ratifies only the dormant WorkflowEvent terminal-lineage core
> needed before D3c2f can write SQL. Typed transport provenance, verification intent, receipts, dispatch exposure, and
> late quarantine remain separate unresolved physical batches.

## 1. Outcome and bounded impact

D3c2c characterized the current 17-column event surface, and D3c2d installed only the preceding ActivityRun /
ActivityAttempt claim-chain fragment. D3b requires terminal events to carry immutable scope and claim lineage, but its
Migration-A prose previously combined that core with a seven-field verification-intent binding and a still-unratified
transport-provenance family. Writing one guessed schema from that combined sentence would create competing owners.

This decision lock therefore does exactly three things:

1. ratifies an ordered **11-column** additive WorkflowEvent terminal-lineage core and eleven local brownfield-safe
   `CHECK ... NOT VALID` names;
2. maps every already-ratified logical event identity to one physical column and forbids redundant aliases;
3. records the exact physical decisions that remain deferred, so D3c2f cannot silently absorb receipt, exposure,
   verification-intent, index, FK, adoption, or runtime semantics.

The future workflow-runtime repository remains the semantic owner of strict event creation. D3c2e installs no substrate
and does not make current event rows strict, authoritative, or dispatchable.

## 2. Current physical baseline remains dormant

`WORKFLOW_EVENTS` currently exposes exactly these 17 descriptor columns:

```text
event_id, workflow_run_id, operation_id, command_id, activity_attempt_id,
event_family, event_type, sequence_number, idempotency_key, occurred_at,
recorded_at, actor, source, payload_json, artifact_refs_json, schema_version, created_at
```

`LiveControlPlanePostgresAdapter.append_workflow_event` remains the only explicit physical INSERT owner. Its replay
identity and current `(workflow_run_id, idempotency_key)` / sequence behavior use only the existing surface. D3c2e does
not change that writer, the descriptor, the mapper, the current replay predicate, or the split event-then-reducer
multi-commit path tracked by R-019.

## 3. Exact additive event-core columns

The D3c2f migration must add exactly these columns in this order:

| position | column | SQL type | nullable | default | brownfield sentinel |
|---:|---|---|---|---|---|
| 1 | `runtime_namespace` | `text` | no | `''::text` | `''` |
| 2 | `provider_mode` | `text` | no | `''::text` | `''` |
| 3 | `workspace_id` | `text` | no | `''::text` | `''` |
| 4 | `scope_digest` | `text` | no | `''::text` | `''` |
| 5 | `coordination_plan_review_id` | `bigint` | yes | none | SQL `NULL` |
| 6 | `activity_run_id` | `text` | no | `''::text` | `''` |
| 7 | `claim_generation` | `bigint` | no | `0` | `0` |
| 8 | `control_epoch` | `bigint` | no | `0` | `0` |
| 9 | `claim_authority_spec_digest` | `text` | no | `''::text` | `''` |
| 10 | `d3_business_fence_digest` | `text` | no | `''::text` | `''` |
| 11 | `terminal_outcome_digest` | `text` | yes | none | SQL `NULL` |

The complete legacy tuple is:

```text
('', '', '', '', NULL, '', 0, 0, '', '', NULL)
```

Empty/zero/NULL values are compatibility sentinels only. They never select the strict population, authorize a claim or
effect, prove scope equality, satisfy terminal provenance, or permit dispatch. `coordination_plan_review_id` uses the
same nullable `BIGINT` encoding as the already-installed command/activity lineage and
`plan_review_sessions.review_id`; no text or empty-string encoding is permitted.

## 4. Exact local checks

D3c2f installs exactly these eleven local constraints, all `NOT VALID`:

| position | constraint name | local predicate |
|---:|---|---|
| 1 | `workflow_events_runtime_namespace_shape_ck` | `runtime_namespace = '' OR runtime_namespace ~ '[^[:space:]]'` |
| 2 | `workflow_events_provider_mode_shape_ck` | `provider_mode = '' OR provider_mode IN ('live', 'simulate', 'scripted', 'replay')` |
| 3 | `workflow_events_workspace_id_shape_ck` | `workspace_id = '' OR workspace_id ~ '[^[:space:]]'` |
| 4 | `workflow_events_scope_digest_shape_ck` | `scope_digest = '' OR scope_digest ~ '^[0-9a-f]{64}$'` |
| 5 | `workflow_events_coordination_plan_review_id_shape_ck` | `coordination_plan_review_id IS NULL OR coordination_plan_review_id > 0` |
| 6 | `workflow_events_activity_run_id_shape_ck` | `activity_run_id = '' OR activity_run_id ~ '[^[:space:]]'` |
| 7 | `workflow_events_claim_generation_nonnegative_ck` | `claim_generation >= 0` |
| 8 | `workflow_events_control_epoch_nonnegative_ck` | `control_epoch >= 0` |
| 9 | `workflow_events_claim_authority_spec_digest_shape_ck` | `claim_authority_spec_digest = '' OR claim_authority_spec_digest ~ '^[0-9a-f]{64}$'` |
| 10 | `workflow_events_d3_business_fence_digest_shape_ck` | `d3_business_fence_digest = '' OR d3_business_fence_digest ~ '^[0-9a-f]{64}$'` |
| 11 | `workflow_events_terminal_outcome_digest_shape_ck` | `terminal_outcome_digest IS NULL OR terminal_outcome_digest ~ '^[0-9a-f]{64}$'` |

These predicates own only local grammar. They do not assert parent existence, cross-row equality, exact-copy
immutability, terminal status parity, command-type population membership, or receipt/provenance completeness. D3c2f
must not validate them in the installation transaction.

## 5. Logical-to-physical identity mapping

The core uses existing columns wherever an owner already exists:

| logical identity | physical owner |
|---|---|
| terminal event id | existing `workflow_events.event_id` |
| workflow run id | existing `workflow_events.workflow_run_id` |
| operation run id | existing `workflow_events.operation_id` |
| source / terminal command id | existing `workflow_events.command_id` |
| source ActivityAttempt id | existing `workflow_events.activity_attempt_id` |
| source ActivityRun id | new `workflow_events.activity_run_id` |
| immutable runtime scope | new `runtime_namespace`, `provider_mode`, `workspace_id`, `scope_digest`, and `coordination_plan_review_id` |
| source claim fence | new `claim_generation`, `control_epoch`, `claim_authority_spec_digest`, and `d3_business_fence_digest` |
| terminal event type | existing `workflow_events.event_type` |
| canonical terminal payload | existing `workflow_events.payload_json` plus new `terminal_outcome_digest` |
| source command attempt | linked `workflow_activity_attempts.command_attempt`, reached by existing `workflow_events.activity_attempt_id`; verification intent separately exact-copies it as `source_command_attempt` |

The event core therefore forbids new event-side `operation_run_id`, `source_verification_command_id`,
`source_activity_attempt_id`, and `source_command_attempt` aliases. The terminal UoW must compare the linked
ActivityAttempt's post-claim attempt; a duplicated event value would not create independent authority. JSON payload is
never a fallback for any physical identity above.

## 6. Deferred physical decisions

The following are explicit D3c2e deferrals and cannot enter D3c2f:

- **D3c2e-D1 — event transport provenance:** exact columns for `terminal_transport_variant`, historical terminal-policy
  digest, response/failure/no-exposure spec pins, receipt/exposure/provider/call/envelope/occurrence/result/failure
  identities, artifact pairs, retry disposition, and variant-local checks remain unratified.
- **D3c2e-D2 — receipt tables:** `transport_response_receipts` and `transport_attempt_failure_receipts` still lack a
  complete owner-ratified SQL inventory, PK/unique keys, types, defaults, timestamps, and checks.
- **D3c2e-D3 — dispatch exposure:** the durable dispatch-exposure table name, descriptor, owner, stable physical-call
  identity, and uniqueness contract remain **unratified / undetermined**.
- **D3c2e-D4 — late quarantine:** `workflow_late_result_quarantine` still lacks complete row identity, receipt FK,
  state-axis, tombstone, and CAS DDL.
- **D3c2e-D5 — verification intent:** the seven-field source binding is semantically ratified, but the table's complete
  row inventory, physical types, PK/unique keys, predecessor fields, append-once terminal tuple, timestamps, and CAS are
  not; OB-10.1 remains open.
- **D3c2e-D6 — indexes:** scope-aware event idempotency and terminal-event unique indexes remain Migration C decisions;
  current indexes are neither replaced nor broadened by this batch.
- **D3c2e-D7 — foreign keys:** ActivityAttempt/event, command/event `MATCH SIMPLE DEFERRABLE`, and event/receipt FKs
  remain deferred until both parent keys and receipt/exposure identities are physically ratified.
- **D3c2e-D8 — population checks:** command-type/terminal-variant/status parity requires registry-generated bootstrap /
  strict-D3 manifests and the terminal-provenance registry; no sentinel-driven discriminator is allowed.
- **D3c2e-D9 — adoption:** backfill, populated constraint validation, sentinel deletion, descriptor exposure, and
  compatibility-branch removal remain Migration B/C/D work in separate bounded transactions.
- **D3c2e-D10 — runtime atomicity:** exact-copy writers, replay collision comparison, terminal command/attempt/event /
  source-intent one-PG UoW, reducer cutover, dispatch, response races, and quarantine remain R-019/runtime work.

These deferrals are required decisions, not optional omissions. Completing the eleven-column event core does not permit
rollout step 3 or any strict terminal path.

## 7. Mechanism × ten-invariant matrix

| mechanism | 1 owner | 2 tenant | 3 fence | 4 lifecycle | 5 late/partial | 6 cost | 7 physical identity | 8 provenance | 9 consistency | 10 mode isolation |
|---|---|---|---|---|---|---|---|---|---|---|
| 11-column event core | future workflow-runtime repository; §1 | physical `workspace_id`; §3/§5 | generation/epoch/spec/business columns are dormant; §3 | no activation or sentinel deletion; §3/§6 D9 | no response acceptance; §6 D1-D4/D10 | N/A until exposure/receipts; §6 D2-D4 | existing links plus exact additions; §5 | payload cannot self-prove; §5 | single mapping and no aliases; §5 | namespace + provider mode are physical; §3 |
| local check grammar | D3c2f migration only; §4 | empty sentinel remains ineligible; §3/§4 | shape only, never authorization; §4 | `NOT VALID`, later validation deferred; §4/§6 D9 | N/A; no result path | N/A; no call path | exact names/predicates; §4 | no cross-row proof claim; §4 | eleven checks match eleven columns; §3/§4 | provider enum and namespace grammar; §4 |
| logical/physical mapping | event repository exact-copy owner; §5 | scope includes workspace; §5 | linked attempt owns command attempt; §5 | aliases never become compatibility owners; §5 | late data cannot use JSON fallback; §5/§6 D10 | physical-call identity deferred; §6 D3 | existing operation/command/attempt ids reused; §5 | terminal payload is not identity authority; §5 | D3b §5.2/§11.1 clarified once; §5 | scope tuple carries mode; §5 |
| deferred evidence families | future typed owners per D3b; §6 | every future row must carry tenant scope; D2-D5/D7 | receipt/exposure/intent CAS unresolved; D1-D5/D7 | retention/replay/tombstone unresolved; D2-D5/D9 | all response/failure/quarantine races deferred; D1-D4/D10 | exposure/receipt/cost reconciliation deferred; D1-D4 | keys/FKs/indexes deferred; D2-D7 | three-variant registry remains required; D1/D8 | exact deferral inventory prevents schema guessing; §6 | cross-mode receipt/exposure proof deferred; D1-D4 |
| D3c2f dormant substrate | migration runner only; §9 | sentinel workspace only; §3 | zero runtime writer/CAS; §9 | apply/rollback/no-op only; §9 | N/A; no live result | N/A; no provider call | eleven columns/checks only; §9 | no provenance columns; §6 D1 | descriptor remains 17 columns; §9 | no runtime activation; §9 |

Every matrix cell is either satisfied by this decision lock, explicitly not applicable to a no-runtime substrate, or
carried by a named D3c2e deferral. No new OB-ID is created: this batch remains under Plan §6 item 6 and R-019.

## 8. Executable oracle and author evidence

`tests/test_d3_workflow_event_terminal_lineage_decision_lock.py` mechanically locks:

- the unchanged 17-column descriptor and the post-decision `0006` transition without descriptor/writer activation;
- exact ordered column/type/default/nullability/sentinel and constraint-name/predicate tables;
- logical-to-physical mapping and the four forbidden event aliases;
- the D3b §5.2/§11.1 clarification that `source_command_attempt` belongs to the linked ActivityAttempt and verification
  intent, not a duplicate event column;
- the exact ten-item deferral inventory and complete ten-invariant matrix;
- Plan/TODO/ledger/index synchronization, explicit non-closure, and the D3c2f-only next boundary.

Post-decision transition: D3c2f subsequently installs only the ratified `0006` dormant substrate. The executable oracle
therefore continues to freeze the 17-column descriptor and closed writer while permitting that separately documented
migration; it does not reinterpret D3c2e as implementation evidence.

Stable-tree author evidence:

- D3c2e decision-lock oracle: **7 passed**;
- adjacent D3c2c characterization plus D3 claim-fence contract: **46 passed** (`8 + 38`);
- Ruff check/format: clean for both decision/characterization oracle files;
- `git diff --check`: clean.

This is author evidence only. It is not independent-review evidence and does not authorize D3c2f or any live path.

Pinned review record: commit `1fb052fe561875b648ae5dc9d4d17fd417d820a5` received a fresh non-author
medium-effort **ADVISORY GO** with P0/P1/P2/P3=`0/0/0/0` after exact 9-file scope verification, `7 + 46` tests,
Ruff, and diff checks. Medium effort is not the operator-owned highest-effort formal gate, so formal review remains
pending and no live/signoff authority is implied.

## 9. Explicit non-closure and next batch

D3c2e changes no runtime behavior and closes none of these gates:

- `R-019`: terminal command/attempt/event/source-intent atomicity remains open;
- `R-023`: durable-runtime cutover and residual tripwire remain open;
- `R-027`: a scope-matched formal review remains open;
- `R-029`: action request-schema compatibility/adoption remains open;
- action-root durable-scope gate and `OB-10.1/10.2/10.3/10.4` remain open;
- Migration A remainder, Migration B-D, registries/manifests/factory, Stage A/B, dispatch, provider/model, live/W6,
  promotion/signoff, and served Agent tool population remain closed to activation.

The next bounded batch is **D3c2f — dormant WorkflowEvent terminal-lineage foundation**. It may add only one
`workflow_events` ALTER with the 11 columns and 11 `NOT VALID` checks above, a transaction-local 5s lock budget, and
real-PG populated-sentinel / malformed-write / timeout-rollback / ledger-rollback / exact-once recovery / no-op proof.
The descriptor must remain 17 columns, and the current explicit INSERT must continue to omit the new fields so database
defaults produce only dormant sentinels. D3c2f may not add indexes, FKs, validation, backfill, descriptor/runtime reads,
strict writers, transport provenance, receipt/exposure/intent/quarantine schema, or any provider/model/served path.

D3c2f now implements exactly that bounded substrate; see
`TRACK_D_D3C2F_WORKFLOW_EVENT_TERMINAL_LINEAGE_MIGRATION_IMPLEMENTATION.md`. The next batch remains decision/Scout work
for the still-unratified Migration-A evidence surfaces and may not infer physical DDL from this event-core decision.
