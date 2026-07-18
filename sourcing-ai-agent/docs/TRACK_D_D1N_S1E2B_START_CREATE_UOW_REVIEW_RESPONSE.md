# Track D D1n S1e2b — formal review response

Status: implementation response to the formal S1e2b `NO-GO` artifact
`runtime/reviews/20260718T060805Z_Track_D_D1n_S1e2b_start_create_UoW_fde4b34.md`.

This batch remains non-live and non-served. It does not close `R-019`, `R-029`, Plan §6#6, `OB-2.2`, `OB-10.3`,
`OB-10.4`, hosted serving, or provider/model invocation gates.

## Fixed-forward scope

- Generic workflow-command controls now reject held `start_acquisition_run` v2 root commands before mutation. The hold is
  still the `not_before_at=9999-12-31 23:59:59` release gate; only result acceptance may clear it.
- Native PG command mutators (`cancel_workflow_command`, `retry_workflow_command`, `resume_workflow_command`) now apply
  the same held-root fence before clearing `not_before_at`, so direct store callers cannot bypass the API preflight.
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
- The classifier is now shared by Operation control APIs and the physical `inspect_operation` owner path. Fail-closed
  start-v2 control states are emitted through `operation_runtime.operation_run_control_state` with registered
  fail-closed override reasons, so HTTP detail/control responses and Agent inspect results expose the same
  `control_state` without changing the schema-owned `control_source_of_truth`.
- `inspect_operation` now recognizes the exact start-v2 `OperationCommandPlanned` physical owner encoding:
  `schema_version=acquisition_start_command_acceptance.v1` plus the nested
  `acquisition_start_command_acceptance_owner_result_ref.v1` envelope. The nested envelope must still match the locked
  Action, OperationRun, workspace, and canonical Operation `workflow_ref`; other event schemas remain fail-closed.
- Generic cancel/retry/resume/dispatch preflight failures now route through the same operation-control response projector
  as normal control responses, preserving top-level `control_state` and `display_contract` parity.
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
  - JSON bool/int alias replay rejection.
- `tests/test_d1n_start_wake_contract_docs.py::test_s1e2b_create_wake_contract_is_cross_document_consistent` covers
  the no-create-wake / S1e2c-post-accept-wake wording across the controlling docs.

## Explicit non-closure

S1e2c has its own formal `NO-GO` and must be fixed separately. S1e2d addresses the first released-root consumer hop, but
that does not by itself close S1e2c result acceptance findings about forged terminal bytes, post-progress replay,
physical owner reconstruction, lock topology, deadline budgeting, or central contract/preflight registration.
