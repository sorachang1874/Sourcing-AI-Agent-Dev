# Track D D1n S1e2b — formal review response

Status: fixed-forward implementation response to the latest formal S1e2b `NO-GO` artifact
`runtime/reviews/20260718T095734Z_Track_D_D1n_S1e2b_closed_acceptance_and_total_held-command_fence.md`; earlier S1e2b
artifacts remain historical evidence, not a substitute for a fresh pinned review.

This batch remains non-live and non-served: served Agent tool population, provider calls, model calls, and live calls all
remain zero. It does not close `R-019`, `R-029`, Plan §6#6, `OB-2.2`, `OB-10.3`, `OB-10.4`, hosted serving, or
provider/model invocation gates.

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
