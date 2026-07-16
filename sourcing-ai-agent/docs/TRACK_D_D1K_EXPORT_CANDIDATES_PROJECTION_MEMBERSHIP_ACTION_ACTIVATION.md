# Track D D1k — Export Candidates Projection Membership Action Activation

Date: 2026-07-16  
Candidate commit: `354e979`
Scope: `export_candidates` only

## Impact

`export_candidates` is moved from the R-029 schema-less compatibility bridge to a schema-defined Operation action. The
combined pinned review is recorded below and remains `NO-GO`; the action remains API-submittable but is not served to a model. No provider, live model, CRM mutation, or
served Agent tool path is authorized by this batch.

## Contract

- Caller input may provide only a projection selector, one membership-revision alias, optional selected candidate
  identity keys, and export options.
- Submit resolves the selector through the canonical serving projection reader and persists an owner-bound target:
  `projection_id`, `membership_revision`, `source_candidate_count`, and canonical sorted `candidate_identity_keys`.
- Export options remain in the closed input segment:
  `include_llm_reviewed_unconfirmed_assertions`, `include_crm_notes`, `limit`, `page_size`, and `export_scope`.
- Whole-projection export remains compatible: the selected-candidate list is empty, not missing.
- Dispatch plans `export.projection.generate` from the persisted target only. A stale membership revision still
  returns reselection-required failure before command planning.

## Residuals

- At the D1k checkpoint R-029 remained open at 8/15 schema-less actions; D1l now owns the current
  **9 schema-defined / 6 schema-less / served=0** partition.
- R-028 is unchanged: this batch does not alter CRM mutation, effect/terminal synchronization, legacy CRM writers, or
  command completion exactly-once boundaries.
- R-019 is unchanged: this batch does not add a global operation/command UoW or claim-generation fence.

## Author evidence

- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_operation_runtime.py::OperationRuntimeTest::test_add_to_crm_command_owner_rejects_forged_projection_selection_target_without_writes tests/test_operation_runtime.py::OperationRuntimeTest::test_add_to_crm_operation_dispatch_leaves_crm_writes_to_command_owner tests/test_operation_runtime.py::OperationRuntimeTest::test_projection_bound_operations_fail_closed_when_membership_changes_before_dispatch tests/test_operation_runtime.py::OperationRuntimeTest::test_export_operation_dispatch_plans_projection_export_command_without_running_owner tests/test_d1_action_request_surface_characterization.py`
  - Result: `10 passed`
- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m pytest -q tests/test_operation_runtime.py::OperationRuntimeTest::test_approval_required_action_does_not_create_operation_run_before_approval tests/test_operation_runtime.py::OperationRuntimeTest::test_approval_transition_creates_idempotent_operation_run_after_approval tests/test_operation_runtime.py::OperationRuntimeTest::test_approve_and_resume_cas_conflicts_stop_downstream_writes tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_control_cas_conflicts_do_not_report_success_or_append_events tests/test_operation_runtime.py::OperationRuntimeTest::test_operation_control_event_failure_rolls_back_state tests/test_operation_runtime.py::OperationRuntimeTest::test_reject_action_is_atomic_idempotent_and_repairs_missing_event`
  - Result: `6 passed`
- `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src .venv/bin/python -m unittest tests.test_operation_runtime`
  - Result: `137 tests OK`
- `make lint`
  - Result: green
- `make typecheck`
  - Result: expected ceiling `81 errors in 4 files`

The combined D1j/D1k pinned `354e979` runner-backed advisory completed as `NO-GO 0/9/7/1`; it is not formal `GO`.
Its findings require fixed-forward plus a fresh pinned re-review before live/W6/manual/product signoff. Author evidence
does not override that scope-local blocker.
