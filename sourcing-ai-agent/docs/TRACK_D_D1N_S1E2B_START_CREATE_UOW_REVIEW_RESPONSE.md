# Track D D1n S1e2b — formal review response

Status: fixed-forward implementation response to the latest formal S1e2b `NO-GO` artifact
`runtime/reviews/20260719T154500Z_Track_D_D1n_S1e2b_fixed-forward_A-H_integrated_closure_rerun2.md` (4 P1,
`4f246b3..a546c4d`), closed by the FF-I batch recorded below. The immediately prior artifact
`runtime/reviews/20260719T111300Z_Track_D_D1n_S1e2b_fixed-forward_A-F_integrated_closure_rerun.md` (4 P1 + 2 P2,
`4f246b3..a98e3df`) was closed by `8807784` (FF-H); `runtime/reviews/20260719T074000Z_Track_D_D1n_S1e2b_fixed-forward_A-F_integrated_closure_retry2.md` (4 P1) was closed
by `a98e3df` (FF-G); `runtime/reviews/20260718T095734Z_Track_D_D1n_S1e2b_closed_acceptance_and_total_held-command_fence.md`
and earlier S1e2b artifacts remain historical evidence, not a substitute for a fresh pinned review.

This batch remains non-live and non-served: served Agent tool population, provider calls, model calls, and live calls all
remain zero. It does not close `R-019`, `R-029`, Plan §6#6, `OB-2.2`, `OB-10.3`, `OB-10.4`, hosted serving, or
provider/model invocation gates.

## Current closure record

### FF-G (`a98e3df`) — the four retry2 P1 repairs

- `inspect_operation` no longer freezes mutable WorkflowCommand lifecycle state to its creation-time value: immutable
  command/acceptance identity is still compared exactly, while `not_before_at` and `downstream_command_ids` are validated
  through the explicit `pending_hold -> released -> progressed` lifecycle contract.
- Start-v2 provenance can no longer downgrade to `non_v2` under compound pin drift: the acceptance owner-result-ref
  schema, the owner-bound `agent-start-v2:` idempotency identity, the owner-bound occurrence reference, the Operation
  start workflow reference that no coherent legacy start can explain, and nonempty drifted pins on a start candidate are
  independent fail-closed markers.
- The public raw SQL workflow-command fence failed closed on opaque or executing wrappers (`DO`, `CALL`, `EXECUTE`,
  mutating `PREPARE`/`EXPLAIN`, `SELECT INTO`, `CREATE FUNCTION/PROCEDURE`, `DROP OWNED`) and normalized parenthesized
  `ONLY` targets and `U&"..."` identifier spelling.
- Generic control-plane snapshot export/import became projection/domain-only: the complete PG-only durable runtime
  causal aggregate is excluded from default inventory, recorded as a typed gap in snapshot headers and sync summaries,
  and rejected at every generic restore boundary.

### FF-H (`8807784`) — the six rerun findings

1. **Progressed lifecycle child verification** (`agent_operation_query_postgres.py`): a `progressed` root no longer
   accepts dangling child identifiers. `load_inspect_operation_base_owner` locks every referenced child `FOR UPDATE`
   after the root/workflow-event locks (mirroring the root-completion owner path's lock order), and
   `_verify_progressed_workflow_command_children` requires each referenced identifier to resolve to a real child in the
   same workflow run and operation, with a `parent_command_id` back-reference to the root, payload causality equal to
   its own columns, and exactly one same-run `CommandPlanRequested` source event naming the root command plus the child
   command type/idempotency pair. The positive regression now progresses a real child through
   `complete_acquisition_root_command` after an S1e2c hold release; dangling, foreign-operation, wrong-parent, and
   missing-event negatives all reject with zero effects.
2. **Tri-state start-v2 carrier parsing** (`acquisition_start_v2_control.py`): every start carrier is classified
   `absent`/`exact`/`corrupt` via `_json_carrier_state` plus per-carrier tri-state helpers. Any present-but-malformed
   start carrier on a start candidate (malformed or conflicting decoded/raw `input`/`target_ref`/`metadata`/
   `result_ref`/`workflow_ref` JSON), any start-specific schema/prefix family member (a drifted
   `acquisition_start_command_acceptance*` owner-ref schema or a blank-suffix `agent-start-v2:` idempotency key), a
   present-but-malformed owner-bound `result_occurrence_ref`, and a split or incomplete Operation start workflow
   reference are corrupt and force `partial_or_mixed_v2`; they can never silently classify as `non_v2`. A 12-case
   carrier matrix runs each corruption with all other pins erased and with the legacy-coherent request pair.
3. **Read-only raw-SQL allowlist** (`control_plane_live_postgres.py`): the mutation denylist is replaced by a strict
   single-statement, read-only allowlist (`_require_public_read_only_sql`) backed by recursive PostgreSQL statement
   decomposition over tokens. Only plainly read-only statements pass the public helpers (`SELECT`/`VALUES`/`TABLE`/
   `WITH`-of-read-only-queries, `EXPLAIN` of a provably read-only statement, `SHOW`); `SELECT INTO`, locking reads,
   multi-statement input, and function calls outside an explicit read-only builtin allowlist fail. DDL and utility
   execution moved to the private migration/test interface (`_execute_returning_one`/`_execute_non_query`); mutating
   test callers were migrated. Real-PG regressions kill the reviewer's bypasses: `MERGE INTO ONLY workflow_commands`,
   `SELECT existing_mutator()` (installed and fired once through the private interface as the control, then rejected
   publicly), `DROP SCHEMA <active> CASCADE`, and a cross-table `CREATE RULE` that updates `workflow_commands`
   (rule firing proven through the private interface), plus the previously fenced families
   (`DO`/`CALL`/`EXECUTE`/`PREPARE`/`EXPLAIN`-mutating/`SELECT INTO`/`CREATE FUNCTION`/`DROP OWNED`).
4. **Centralized portability registry** (`control_plane_postgres.py`): `PG_ONLY_DURABLE_RUNTIME_CAUSAL_AGGREGATE_TABLES`
   and `NONPORTABLE_RUNTIME_COORDINATION_TABLES` (`workflow_job_leases`, `workflow_recovery_intents`,
   `runtime_provider_limiter_leases`) are the explicit registry unioned into `GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES`;
   the coordination tables left `DEFAULT_CONTROL_PLANE_TABLES`, so they are excluded at export and every restore
   boundary. A real-PG test with an active job lease, a pending recovery intent, and an active provider limiter lease
   proves generic export/import can neither observe nor mutate them.
5. **Required exclusion declaration on restore** (`control_plane_postgres.py`):
   `_require_snapshot_exclusion_declaration` requires the exact schema-versioned exclusion declaration before generic
   import and rejects missing/mismatched declarations (subset, superset, drifted, non-list, wrong/absent
   `schema_version`); snapshot→PG sync and snapshot→SQLite restore copy the verified gap into their summaries, which
   the cloud-import summary nests. No temporary legacy import path exists: the declaration is unconditional.
6. **Closure record + owner matrix** (this document, `docs/PRE_AGENT_CONTRACT_REVIEW.md`): the
   `start_acquisition_run.result_hold_release_owner` row now lists the complete `pending_hold`/`released`/`progressed`
   lifecycle state machine with physical derivation rules, and the `operation_run.control_state` row records the
   tri-state corrupt-carrier rule. `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md` and
   `docs/LOCAL_POSTGRES_CONTROL_PLANE.md` describe the read-only allowlist, the centralized portability registry, and
   the required exclusion declaration.

Residuals `R-019`/`R-029` stay open. Author evidence is not a formal review; this batch requires a fresh pinned
independent review before any live/W6/manual signoff.

### FF-I (this batch) — the four rerun2 findings

1. **Versioned progressed-child contract** (`workflow_progressed_child_contract.py`, new;
   `agent_operation_query_postgres.py`, `control_plane_live_postgres.py`, `acquisition_command_owner.py`): the
   registered parent-root → progressed-child completion contracts, the immutable field sets, and the child plan-event
   derivations now live in one shared module (`PROGRESSED_CHILD_CONTRACT_VERSION`) consumed by
   `complete_acquisition_root_command`, the succeeded-replay validator, and inspection — no second hand-maintained copy.
   Inspection now requires the registered child command type/owner for the root's completion contract and exact-compares
   every immutable child/event/causality field: payload causality must mirror every causality column (stage, causal
   group, source-event identity, artifact refs, produced counts, no-op/readiness fields, causality schema), and the
   child plan event must match its full immutable identity (registered `<child>:plan` idempotency, deterministic
   `evt_<sha1>` id, pinned per-contract actor/source, sequence ordered after the root's own source event, empty
   artifact refs, complete plan payload bound to the child). Only the explicitly mutable child lifecycle fields
   (status, attempt, lease, result, `not_before_at`, `downstream_command_ids`) may differ. New regressions cover
   wrong-type, wrong-owner, stage/group drift, causality-schema drift, readiness drift, artifact/count drift,
   event-idempotency/actor/source/artifact/nested-payload drift: 12 of 13 proven to pass the old verifier
   (stash-verified), the 13th (`wrong_type`) additionally pinned by the registry.
2. **Database-enforced read-only public probes** (`control_plane_live_postgres.py`): the lexical allowlist is now
   defense in depth rather than the sole mutation boundary. Public raw-SQL probes admitted by the allowlist execute
   inside `SET TRANSACTION READ ONLY` (first statement of the probe transaction), so any write hidden behind
   database-resolved objects — mutating views, RLS policies, user operators/casts, allowlist-shadowing function
   overloads — fails with sqlstate `25006` and is re-raised as the public contract error without committing. The
   private migration/test interface (`_execute_returning_one`/`_execute_non_query`) is unchanged and keeps executing
   legitimate DDL/DML. Real-PG regressions prove a mutating view (`SELECT * FROM s1e2b_mutating_view` and
   `EXPLAIN ANALYZE` of it), a schema-local `lower(integer)` overload shadowing the allowlisted builtin, and a user
   operator (`OPERATOR(schema.##)`) hiding a proven mutator are all rejected with the held command table byte-identical;
   each control fires the same object through the private interface first to prove the indirection is live. A dedicated
   SELECT-only probe role remains optional deployment-level hardening on top of this boundary (the read-only
   transaction already covers every catalog-level indirection because any mutation it reaches executes as DML inside
   the probe transaction).
3. **Canonical per-table portability registry** (`control_plane_postgres.py`):
   `CONTROL_PLANE_TABLE_PORTABILITY_REGISTRY` is now the single source of truth; `DEFAULT_CONTROL_PLANE_TABLES`,
   `PG_ONLY_DURABLE_RUNTIME_CAUSAL_AGGREGATE_TABLES`, `NONPORTABLE_RUNTIME_COORDINATION_TABLES`, and
   `GENERIC_POSTGRES_IMPORT_EXCLUDED_TABLES` are all derived from it. `agent_worker_runs` (active worker
   status/interrupt/lease state) and `linkedin_profile_registry_leases` (profile-URL scheduler leases) join the
   nonportable coordination category; `acquisition_runs` and `acquisition_discovery_lanes` join the durable-runtime
   causal aggregate category (their Action/Operation/command/event owners were already excluded, so importing them
   preserved a partial runtime slice). A fast preflight test proves the derived sets equal the registry and that the
   registry classifies every `CONTROL_PLANE_LIVE_TABLES` table exactly once; real-PG export/import tests prove active
   worker/profile leases and orphaned acquisition runtime rows are neither observed nor mutated by generic
   export/import and are rejected at the explicit export and snapshot→PG sync boundaries.
4. **Canonical type-strict tri-state carriers** (`json_contract.py`, `acquisition_start_v2_control.py`):
   `loads_json_contract_strict` rejects duplicate object keys (at any depth) and non-finite `NaN`/`Infinity` constants
   instead of normalizing them, and decoded/raw carrier comparison now uses type-strict `json_contract_equal`, so
   `1 == True` and `1 == 1.0` can never alias a conflicting carrier into a matching one. The corrupt-carrier matrix
   grows from 12 to 18 cases (bool/int alias, int/float alias, top-level and nested duplicate keys, `NaN`, and
   `-Infinity`), each run under both erased and legacy-coherent pins; all six new families classified as `decoded`
   (→ `non_v2`) before the fix and now force `partial_or_mixed_v2`.

Residuals `R-019`/`R-029` stay open. Author evidence is not a formal review; this batch requires a fresh pinned
independent review before any live/W6/manual signoff.

## Fixed-forward scope

- Generic workflow-command controls now reject held `start_acquisition_run` v2 root commands before mutation. The hold is
  still the `not_before_at=9999-12-31 23:59:59` release gate; only result acceptance may clear it.
- Native PG command mutators (`cancel_workflow_command`, `retry_workflow_command`, `resume_workflow_command`) now apply
  the same held-root fence before clearing `not_before_at`, so direct store callers cannot bypass the API preflight.
- The physical hold sentinel is now independently authoritative at every WorkflowCommand native-write boundary. Generic
  running/success/failure/checkpoint, partial-progress, prerequisite-wait, completion, reawaken, cancel/retry/resume,
  owner-specific cancel, and company-public-web terminal writers all carry a SQL-level
  `not_before_at <> 9999-12-31 23:59:59` CAS. This remains effective even if command type, owner, and payload provenance
  are corrupted together. Generic dynamic CRUD entrypoints reject `workflow_commands` before SQL, leaving the exact
  S1e2c accepted-result UoW as the sole sentinel-release writer.
- The native held-root fence is now status-independent and provenance-aware: a command retaining the result-acceptance
  hold sentinel is protected when command type, owner, payload schema, start snapshot, or confirmation receipt evidence
  identifies an exact, partial, or corrupt start-v2 root. The same fence is applied to alternate native writers
  `update_workflow_command_payload` and `mark_workflow_command_waiting_prerequisite`, leaving result acceptance as the
  only owner that may clear the sentinel.
- OperationRun public `control_state` now uses the same start-v2 generic-control preflight before exposing affordances:
  exact or corrupt start-v2 Operations advertise no dispatch/resume/retry/cancel actions while the generic controls are
  unsupported or invalid.
- Generic operation controls now distinguish non-v2, exact-v2, and mixed/partial v2 identity. Mixed action/operation
  pins fail closed with zero writes instead of falling back to generic mutation.
- The generic operation-control classifier now uses a single owner-defined provenance classification across the Action,
  OperationRun, target snapshot, result/serializer pins, and result-occurrence metadata. Non-start schema-less Actions
  are not classified as v2 merely because their open input happens to contain `preview_*` field names; start-v2 rows
  with current request pins and preview keys erased still fail closed when retained v2 provenance remains.
- `classify_acquisition_start_v2_generic_control_provenance` is now documented as the sole start-v2 override classifier.
  Its owner row enumerates the closed Action input/target/occurrence carriers, Action identity/request/tool/result/
  serializer/idempotency pins, and the matching Operation identity/request/tool/result/serializer/idempotency pins.
  The fail-closed reason registry validates the classifier-selected reason only; the locked inspect acceptance validator
  remains a physical integrity fence and cannot act as a parallel classifier.
- The classifier is now shared by Operation control APIs and the physical `inspect_operation` owner path. Fail-closed
  start-v2 control states are emitted through `operation_runtime.operation_run_control_state` with registered
  fail-closed override reasons, so HTTP detail/control responses and Agent inspect results expose the same
  `control_state` without changing the schema-owned `control_source_of_truth`. Strong v2 markers such as request pins,
  target snapshots, result-occurrence refs, and result serializer pins are classified even if the mutable `action_type`
  string has drifted; only preview-named input fields alone remain start-action-scoped to preserve non-start
  schema-less compatibility.
- `inspect_operation` now recognizes the exact start-v2 `OperationCommandPlanned` physical owner encoding:
  `schema_version=acquisition_start_command_acceptance.v1` plus the nested
  `acquisition_start_command_acceptance_owner_result_ref.v1` envelope. The nested envelope must still match the locked
  Action, OperationRun, workspace, and canonical Operation `workflow_ref`; other event schemas remain fail-closed.
- The start-v2 command-acceptance event and its 18-field owner ref now live in the shared pure
  `acquisition_start_command_acceptance` contract used by both create and inspect. Inspect discriminates strictly on
  `(OperationCommandPlanned, schema_version)`, rejects swapped/hybrid/extra-field/noncanonical payloads, recomputes the
  owner-result digest, and binds terminal winner, source event, receipt, parent budget, snapshot, occurrence, root
  payload, Action/Operation result refs, and command causality to the locked physical rows.
- Start-v2 exact classification now validates the complete closed bound request/snapshot, occurrence ref, request/tool
  pins, and full result/serializer tuple. Any incomplete or conflicting component is mixed/partial and uses the
  registered identity-mismatch override. Operation request pins remain strong provenance even after compound mutable
  action/operation type and owner drift.
- `inspect_operation` runs the shared start-v2 preflight before ActionRegistry resolution. Strongly identified corrupt
  rows use the canonical start-v2 registry contract only to produce the typed fail-closed projection; unknown non-v2
  actions retain the existing contract-not-found behavior. `operation_run.control_state` now has a complete owner-matrix
  row, allowed reason set, consumers, fallback, migration status, and deletion condition.
- Generic cancel/retry/resume/dispatch preflight failures now route through the same operation-control response projector
  as normal control responses, preserving top-level `control_state` and `display_contract` parity.
- The fast owner preflight now runs one 32-case data-driven provenance matrix through real Operation detail/list and
  cancel/retry/resume/dispatch callables, the locked physical `inspect_operation` owner/serializer, and executable
  frontend adapter mappers. Exact v2 and every Action/Operation hostile marker family must return the same canonical
  all-controls-disabled projection, and a mutation-writer tripwire proves every case returns before a write delegate.
- `Operation.action_id` mismatch is tested outside that override-parity matrix because exact-owner lookup precedes the
  classifier: detail and all four controls return the normal masked `not_found`, list omits the unlinked Operation, and
  locked Agent inspect serializes the same `operation_not_found` error. A separate real schema-less non-start/no-marker
  positive proves `non_v2/ready` across detail/list, locked Agent serialization, and frontend mapping without either
  start-v2 override reason. Generic control-response positives are intentionally not invoked because their legal path
  delegates mutation; control-response cross-surface parity covers only exact/hostile fail-closed cases.
- Generic action controls now also fail closed for pending `start_acquisition_run` Actions that carry only partial v2
  discriminators: schema version/digest without the exact input shape, one/two preview keys without the full
  `preview_id + preview_revision + preview_digest` tuple, or one exact schema pin paired with a mismatching peer. These
  rows are treated as corrupt start-v2 candidates, not legacy-ready Actions.
- The create UoW uses raw, non-normalizing row reads for authority probes and exact replay comparison. Public PG row
  normalization is no longer used to certify physical owner identity.
- JSON replay comparison rejects blank/malformed carriers, duplicate object keys, and type aliases such as JSON boolean
  versus integer.
- Approval actor and policy revision validation now reuses the canonical pure start-v2 owner/version validators before
  adapter dependency checks or PostgreSQL connection/locking.
- S1e2b documentation now states that create performs no wake; S1e2c owns hold release and post-accept wake.
- `tests/test_d1n_start_wake_contract_docs.py` locks that cross-document contract across the remaining-action plan, the
  S1e2b implementation record, and this response.

## New regression evidence

- `tests/test_d1n_s1e2b_inspect_acceptance_closure.py` (FF-I) adds the versioned progressed-child negatives: six
  causality-column drift cases (stage, causal group, causality schema, readiness effect, input artifact refs, produced
  counts), the fully self-consistent foreign `foreign.command`/`foreign_owner` child probes, and five plan-event
  identity drift cases (idempotency, actor, source, nested plan payload, artifact refs); 12 of 13 are stash-verified to
  pass the pre-fix verifier, and `wrong_type` is additionally pinned by the registered contract.
- `tests/test_d1n_s1e2b_raw_sql_workflow_command_fence.py` (FF-I) adds the database-enforced read-only transaction
  regressions: a mutating view read and its `EXPLAIN ANALYZE`, an allowlisted-shadowing `lower(integer)` overload, and
  a user operator hiding a proven mutator, each rejected with sqlstate `25006` after the same object is fired once
  through the private interface as the live control; the unit seam moved to the `_execute_public_probe` delegate.
- `tests/test_control_plane_postgres.py` (FF-I) adds the canonical portability-registry preflight (derived sets equal
  `CONTROL_PLANE_TABLE_PORTABILITY_REGISTRY`; every `CONTROL_PLANE_LIVE_TABLES` table classified exactly once) and the
  real-PG export/import proofs for active `agent_worker_runs`/`linkedin_profile_registry_leases` rows and orphaned
  `acquisition_runs`/`acquisition_discovery_lanes` rows (not observed, not mutated, rejected at explicit boundaries).
- `tests/test_d1n_s1e2b_start_v2_provenance.py` (FF-I) grows the corrupt-carrier matrix from 12 to 18 cases under both
  erased and legacy-coherent pins: bool/int and int/float decoded/raw aliases, top-level and nested duplicate keys,
  `NaN`, and `-Infinity`; all six new families classified as `decoded` (→ `non_v2`) before the fix.
- `tests/test_d1n_start_acquisition_v2_create_uow.py` covers overlong, control-character, surrogate, malformed, and
  overlong policy values before adapter access.
- `tests/test_d1n_start_acquisition_v2_create_pg.py` covers:
  - held command ready-list absence and direct claim zero-write;
  - cancel/retry/resume command-control zero-write;
  - OperationRun detail/control-response affordance parity for unsupported start-v2 controls;
  - Agent `inspect_operation` parity with OperationRun detail for unsupported start-v2 control state and canonical
    `operation_runtime.operation_run_control_state` ownership;
  - direct PG cancel/retry/resume mutator zero-write for held queued/cancelled/retry-wait/failed-terminal root commands,
    malformed held-root payloads, and alternate update-payload/waiting-prerequisite writers;
  - action-type drift, empty schema pair, alternate schema pair, and action+operation pin erasure with retained
    target/result/occurrence provenance operation-control zero-write;
  - unrelated schema-less non-start Actions with preview-named input keys still classify as `ready`;
  - pending partial-v2 approve/reject zero-write for schema-only, preview-key-only, and single-pin mismatch cases;
  - raw text owner corruption replay rejection;
  - blank JSON carrier replay rejection;
  - JSON bool/int alias replay rejection;
  - retryable/terminal failure, success, running, checkpoint, reawaken, atomic completion, and provenance-erased
    held-root native-writer zero-write matrices;
  - generic dynamic PostgreSQL CRUD rejection for `workflow_commands` before SQL;
  - exact-v2 Operation/detail/control/inspect parity under action/schema/result-serializer drift;
  - Operation-only retained v2 pins under compound action/operation owner/type drift;
  - swapped schema, hybrid payload, digest mismatch, extra field, and noncanonical identity rejection for the
    start-acceptance Operation event before result-attempt/journal writes.
- `tests/test_d1n_start_wake_contract_docs.py::test_s1e2b_create_wake_contract_is_cross_document_consistent` covers
  the no-create-wake / S1e2c-post-accept-wake wording across the controlling docs.
- `tests/test_pre_agent_contract_review.py::test_operation_run_control_state_is_contract_owned` covers exact v2 plus
  Action input/target/occurrence, identity, request, tool, result, serializer, idempotency, and matching Operation hostile
  families across backend detail/list, all four control responses, locked physical Agent inspect, and executable frontend
  adapter output. It separately covers `Operation.action_id` owner masking and one true schema-less non-start/no-marker
  positive without invoking a legal mutating control delegate.

## Fixed-forward validation evidence

Exact-head FF-I evidence (this batch; commands and counts are recorded for the pinned head commit, whose SHA is
recorded in `.coord/handoffs/s1e2b-ff-i-v1.md`):

- `python -m pytest tests/test_d1n_s1e2b_acceptance_contract.py tests/test_d1n_s1e2b_inspect_acceptance_closure.py
  tests/test_d1n_s1e2b_raw_sql_workflow_command_fence.py tests/test_d1n_s1e2b_start_v2_provenance.py
  tests/test_control_plane_postgres.py tests/test_pre_agent_contract_review.py
  tests/test_d1n_start_acquisition_v2_create_pg.py -q`: `278 passed, 579 subtests passed in 314.65s`, zero
  failures/errors. (The worktree lacks `frontend-demo/node_modules`; the one esbuild-dependent case ran with
  `SOURCING_TEST_ESBUILD_MODULE_PATH` pointed at the main tree's module. Without that override the same case fails
  identically on the base commit, so it is an environment artifact, not a regression.)
- Per-suite counts inside that battery: inspect acceptance closure `17 passed + 32 subtests`; raw-SQL fence
  `12 passed + 275 subtests`; start-v2 provenance `102 passed`; control-plane postgres `36 passed + 178 subtests`.
- Touched-suite adjacency: `tests/test_cloud_asset_import.py`, `tests/test_control_plane_pool.py`,
  `tests/test_d0f_model_invocation_envelope_postgres.py`, `tests/test_recovery_drain_registry.py`,
  `tests/test_recovery_takeover_intent.py`, `tests/test_request_scope_owner_fencing_pg.py`,
  `tests/test_export_async_task.py`: `65 passed`; progressed-child owner/replay adjacency
  `tests/test_d1n_start_acquisition_v2_create_pg.py` + `tests/test_d1m_company_public_web_atomic_owner.py` +
  `tests/test_d1i_acquisition_root_action_activation.py`: `82 passed + 163 subtests`.
- Ruff check and ruff format on every changed file: clean (the repo-wide ruff baseline carries 27 pre-existing
  findings in untouched files). Scoped mypy (`make typecheck` file set): `81 errors / 4 files`, exactly the allowed
  global baseline; this batch adds zero new mypy errors. `git diff --check`: clean.
- Pre-existing non-regressions observed and re-confirmed against the stashed base: the
  `test_d3c1_public_mapper_is_closed_and_preserves_only_safe_diagnostics` mapper-cardinality assertion (`85` expected,
  `86` current, introduced by the base-commit Luna batch runner, identical with this batch stashed) and the
  `test_first_party_markdown_files_have_status_banner` missing-banner list (identical with this batch stashed).
- Every new regression was stash-verified to fail before its fix: 12 of 13 progressed-child drift cases pass the
  pre-fix verifier (`wrong_type` was already caught by the pre-fix event-payload cross-check and is now additionally
  pinned by the registered contract); all six new carrier families classify as `decoded` (→ `non_v2`) pre-fix; the
  mutating-view/overload/operator probes commit through the pre-fix public path (the reviewer artifact's probes) and
  are rejected post-fix with the held table byte-identical; the four reclassified tables were portable pre-fix by
  construction of the old hand-maintained sets.

Exact-head FF-H evidence (`8807784`; retained from the prior batch):

- `python -m pytest tests/test_d1n_s1e2b_acceptance_contract.py tests/test_d1n_s1e2b_inspect_acceptance_closure.py
  tests/test_d1n_s1e2b_raw_sql_workflow_command_fence.py tests/test_d1n_s1e2b_start_v2_provenance.py
  tests/test_control_plane_postgres.py tests/test_pre_agent_contract_review.py
  tests/test_d1n_start_acquisition_v2_create_pg.py -q`: `258 passed, 446 subtests passed in 236.98s`, zero
  failures/errors. (The worktree lacks `frontend-demo/node_modules`; the one esbuild-dependent case ran with
  `SOURCING_TEST_ESBUILD_MODULE_PATH` pointed at the main tree's module. Without that override the same case fails
  identically on the base commit, so it is an environment artifact, not a regression.)
- Per-suite counts inside that battery: inspect acceptance closure `15 passed + 19 subtests`; raw-SQL fence
  `9 passed + 265 subtests`; start-v2 provenance `90 passed`; control-plane postgres `33 passed + 68 subtests`.
- Touched-suite adjacency: `tests/test_cloud_asset_import.py` `12 passed`; `tests/test_control_plane_pool.py` plus
  `tests/test_d0f_model_invocation_envelope_postgres.py` `9 passed`; `tests/test_request_scope_owner_fencing_pg.py`
  `13 passed`; `tests/test_recovery_takeover_intent.py`, `tests/test_recovery_drain_registry.py`,
  `tests/test_export_async_task.py`, and the three touched `tests/test_results_api.py` cases `34 passed`;
  `tests/test_worker_recovery_daemon.py` `21 passed`.
- Ruff check on every changed file: clean; ruff format shows no drift beyond pre-existing baseline.
  `git diff --check`: clean. Scoped mypy (`make typecheck` file set): `81 errors / 4 files`, exactly the allowed
  global baseline; this batch adds zero new mypy errors. `tests/test_markdown_status.py` has one pre-existing
  unrelated banner failure that fails identically with this batch stashed.
- Every new regression was stash-verified to fail before its fix (the progressed-child negatives, the corrupt-carrier
  matrix, the raw-SQL bypass regressions, and the portability/declaration tests).

The totals below this line belong to the earlier `20260718T095734Z` batch and predate `a98e3df`; they are retained as
historical evidence for that artifact's closure, not as exact-head evidence.

- Start-create PostgreSQL suite: `36 passed`; JUnit expansion `130 cases` (`36` top-level plus `94` parameterized
  subtests), zero failures/errors.
- Inspect result-slot UoW suite: `38 passed`; JUnit expansion `81 cases` (`38` top-level plus `43` parameterized
  subtests), zero failures/errors.
- Create-delegate, historical-fixture, and owner-matrix adjacency: `12 passed`.
- Contract documentation adjacency: `3 passed`.
- FF-F callable owner/parity preflight: `1 passed`; `32` provenance cases each cross detail/list, four generic control
  responses, locked Agent inspect, and frontend mapping, plus one owner-mismatch masked-absence case and one
  schema-less non-start/no-marker positive (`37.31s` scoped author run).
- FF-F callable preflight plus start-v2 classifier adjacency: `55 passed in 37.31s`.
- Scoped mypy for acceptance/control/create/inspect: zero errors in four source files. The native store remains at its
  existing scoped baseline of `25 errors / 1 file`; repository `make typecheck` remains at the allowed global baseline
  of `81 errors / 4 files`.
- Ruff and `git diff --check` are required green immediately before the pinned commit.
- The broader claim-fence adjacency has one unrelated mapper-cardinality assertion (`85` expected, `86` current). The
  same exact node fails identically in a clean detached worktree pinned to `3bd30d8`; this batch does not change its
  owner module or conceal it as new evidence.

## Explicit non-closure

S1e2c has its own formal `NO-GO` and must be fixed separately. S1e2d addresses the first released-root consumer hop, but
that does not by itself close S1e2c result acceptance findings about forged terminal bytes, post-progress replay,
physical owner reconstruction, lock topology, deadline budgeting, or central contract/preflight registration.
