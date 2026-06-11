# Sourcing AI Agent Dev Progress — 2026-05 archive

> Status: Archived 2026-06-11. Historical progress log for 2026-05; rotated out of `PROGRESS.md` to keep the active file small. Do not append here.

## 2026-05-24 (Asia/Shanghai)

### W7 CRM Public Web typed-owner closeout and Agent-ready phase mapping

- Updated the pre-Agent phase map. W7 now means module durable-runtime reuse; W8 will introduce `OperationRun` plus the module action registry; W9 will add recoverable operation APIs/UI and approval gates; W10 will be the full fail-closed contract review before Phase 13 natural-language Agent/graph work.
- Recorded fresh W7 targeted evidence from `output/w7_crm_public_web_fresh_targeted_20260524/`. The CRM Public Web fake-provider/action case completed with `expectation_failures=[]`, Pre-Manual Signoff `passed`, blocking/manual/warning counts `0`, `storage_owner_counts={'crm_public_web_v1': 1}`, `execution_backend_counts={'crm_public_web_v1': 1}`, and `queue_batch_command_succeeded_count=1` owned by `crm_public_web_owner`.
- Updated the frontend/API contract boundary: target-candidate-named Public Web adapter methods are compatibility names only and must call canonical CRM endpoints under `/api/crm/records/...`, normalizing `recordIds` / `record_ids` to `crm_record_ids`. `/api/target-candidates/public-web...` remains retired-by-default and migration/test-only.
- Closed a frontend contract typecheck gap while validating the adapter: `IntentRewriteEntry` / `IntentRewritePayload` are now legal JSON contract objects in both TS and JSON Schema, `IntentBrief` has the same TS index-signature semantics as its schema, and review instruction compile status is normalized to the declared literal union instead of returning an arbitrary string.
- Remaining W7 work is now explicit: run a small live-provider CRM Public Web validation before physical legacy deletion, then finish Excel and export typed command reuse. Legacy target-candidate Public Web helpers/tables are not normal-path owners, but they should not be physically deleted until live result quality/promotion/export behavior and historical migration access are reviewed.

### W6 background-maintenance and cooperative-yield signoff semantics follow-up

- Reclassified healthy `snapshot.compaction.run` backlog from a `known_acceptable_warning` to an explicit background-maintenance passed gate. `service_metrics.snapshot_full_materialization_queue` now reports owner `snapshot_materialization_owner`, command type `snapshot.compaction.run`, scope `background_snapshot_compaction`, readiness effect `background_artifact_compaction`, `manual_handoff_blocking=false`, and `background_maintenance_pending` only when queued/running backlog has no retry, stale-running, or terminal-failed evidence.
- Pre-Manual Signoff now records `background_maintenance_snapshot_compaction` as a passed gate for healthy background snapshot compaction. If a case explicitly requires background compaction to settle, pending backlog remains blocking; retry/stale-running/terminal-failed compaction now blocks as `snapshot_full_materialization_background_unhealthy`.
- Reclassified clean `durable_work_handoff_yield` from a known warning to explicit cooperative scheduling proof. `service_metrics.recovery_phase_metrics` now exposes `cooperative_handoff_yield_count`, `handoff_yield_contract=cooperative_scheduling_yield`, `handoff_yield_manual_handoff_blocking=false`, and `handoff_yield_attention_required=false` only when the same recovery report has no missing/failed/slow/unexpected phases, no legacy bridge, and no tick-budget exhaustion. Dirty recovery reports keep handoff yield diagnostic while the dirty owner signal remains blocking or warning.
- Pre-Manual Signoff now records `recovery_cooperative_handoff_yield` as a passed gate for clean cooperative recovery yields. It no longer emits `recovery_durable_work_handoff_yield` as a known warning unless the recovery contract is dirty.
- Reclassified clean `recovery_tick_budget_exhausted` from a known warning to explicit cooperative budget-yield proof when the smoke/recovery record carries `next_tick_requested=true` and the same recovery report has no missing/failed/slow/unexpected phases or legacy bridge. `service_metrics.recovery_phase_metrics` now exposes `budget_yield_next_tick_requested_count`, `cooperative_budget_yield_count`, `budget_yield_contract=cooperative_recovery_budget_yield`, `budget_yield_manual_handoff_blocking=false`, and `budget_yield_attention_required`; Pre-Manual Signoff records `recovery_cooperative_budget_yield` as a passed gate for clean cases.
- Smoke expectation `max_recovery_tick_budget_exhausted_count` now gates only budget-yield attention count (`raw recovery_tick_budget_exhausted_count - cooperative_budget_yield_count`) while keeping the historical expectation key name for matrix compatibility. Clean cooperative budget yields therefore do not fail Nightly/strict expectations, but dirty or missing-next-tick budget exhaustion still fails.
- Updated `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `WORKFLOW_PROGRESS_CONTRACT.md`, `TESTING_PLAYBOOK.md`, and `NEXT_TODO.md` so snapshot compaction is treated as background maintenance and clean handoff yield is treated as cooperative scheduling proof after canonical board-visible projection, post-profile SLO proof, and recovery owner contracts are clean.
- Validation passed after the handoff-yield documentation update: `tests/test_workflow_service_metrics.py -k "snapshot_full_materialization or snapshot_compaction or handoff_yield or recovery_durable_work"` -> `5 passed`; `tests/test_scripted_smoke_signoff.py -k "background_snapshot or snapshot_compaction or handoff_yield or recovery_durable_work"` -> `3 passed`; `ruff check` for touched runtime/signoff/test files; tracked-doc `git diff --check`.
- Validation passed after the budget-yield contract update: `tests/test_workflow_service_metrics.py -k "snapshot_full_materialization or snapshot_compaction or handoff_yield or recovery_durable_work or recovery_tick_budget or budget_yield or recovery_phase_contract"` -> `8 passed`; `tests/test_scripted_smoke_signoff.py -k "background_snapshot or snapshot_compaction or handoff_yield or recovery_durable_work or budget_yield or recovery_total_elapsed"` -> `5 passed`; `tests/test_workflow_smoke.py -k "recovery_phase_violations or clean_recovery_budget_yield"` -> `2 passed`; `ruff check` for touched runtime/signoff/smoke/test files.

### W6b runtime payload and typed finalization gate follow-up

- W3b completion-policy cutover closed the highest-risk remaining `job.status=completed/results` workflow-truth consumers. `run_workflow_supervisor`, `_wait_for_workflow_terminal_status`, progress auto-recovery suppression, final-results phase labeling, and deferred promotion now require `_job_is_terminal(...)` / typed completion policy for workflow jobs; `completed` remains only UI/read-model/domain-task status. Missing PG typed proof fails closed as `serving_finalized_proof_unavailable` only when no earlier concrete blocker exists, preserving specific blockers such as active materialization items.
- Regression coverage added for completed UI rows without typed proof: `tests/test_results_api.py::test_workflow_completed_ui_status_requires_typed_completion_proof_for_terminal_paths` and `tests/test_results_api.py::test_workflow_supervisor_does_not_exit_on_completed_ui_status_without_typed_proof`. Older completion-blocker tests that only need typed proof input now mock `get_workflow_current_state(...)` instead of writing SQLite durable state, preserving the PG-only durable-runtime cutover.
- Phase 11b legacy job-result endpoint retirement is now default-global instead of projection-ready-only. `/api/jobs/{job_id}/results`, `/dashboard`, `/candidates`, and `/candidates/{candidate_id}` return `410` in normal mode. Projection-ready runs return a projection pointer; unmigrated runs return `legacy_job_result_endpoint_migration_required` with no fallback payload. Legacy composition is available only under explicit `SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS=1` migration/test override.
- Regression coverage added for unmigrated legacy retirement: `tests/test_projection_crm_api_contracts.py::test_unmigrated_legacy_job_result_endpoint_returns_migration_required_by_default`.
- Public `/api/jobs/{job_id}/results` runtime details now use a bounded diagnostics contract when served through the public API path. The payload exposes compact dashboard job fields, compact events, compact Agent runtime session/trace/worker records, compact workflow stage summaries, and `runtime_details_contract.schema_version=public_results_runtime_details_v1`; it no longer returns raw oversized job summary/runtime blobs on the public results reader.
- Post-preview finalization SLO evaluation now prefers typed durable causality evidence from succeeded `workflow_commands(command_type='linkedin.profile_url_terminal.record')` as `profile_terminal_at`. Remote worker terminal timestamps remain diagnostic fallback only when typed terminal-record command evidence is absent. This keeps legitimate provider wait out of the post-profile finalization SLO and anchors the gate to the owner that actually commits profile registry terminal state.
- Regression coverage added for both seams: `tests/test_results_api.py::test_public_results_api_runtime_details_are_bounded_public_diagnostics` and `tests/test_workflow_smoke.py::test_finalization_start_gate_prefers_typed_terminal_record_over_remote_worker_time`.
- Fast W6b contract preflight passed: `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_results_api.py tests/test_durable_runtime.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -k "runtime_details or lightweight or public_results_api_runtime_details_are_bounded or causality or produced_entity_counts or heuristic_slo_pairing or progress_contract or finalization_start_gate" -q` -> `25 passed, 553 deselected, 12 subtests passed`.
- Targeted Google Gemini long-latency case passed strict after the runtime payload gate: `output/w6b_google_gemini_targeted_after_runtime_payload_gate.json` and `output/w6b_google_gemini_targeted_after_runtime_payload_gate_summary.json`. Job `6b937a43413e` completed with `expectation_failures=[]`, `workflow_causality_contract.violation_detected=false`, `legacy_materialization_write_contract.normal_path_write_count=0`, `recovery_phase_metrics.legacy_bridge_used_present=false`, `finalization_start_gate_source=profile_url_terminal_record_command_completed_at`, `profile_terminal_source=profile_url_terminal_record_command_completed_at`, `finalization_start_gate_ms=9000`, and `long_post_preview_finalization=false`.
- Full W6 long-latency signoff passed after the fast preflight and targeted Google check. Evidence: `output/w6b_full_nightly_after_runtime_payload_gate_20260524_012034/report.json`, `summary.json`, `signoff.json`, and `signoff.md`. All five cases completed with `expectation_failures=[]`: OpenAI baseline+delta `3cb286c61f7d`, OpenAI no-baseline scoped-search `534c6668f906`, Lovable live-roster `27d5f627c89d`, Google vision-language large `0dac5cac2d11`, and Google Gemini small-former large-baseline `3d6d4476d439`.
- Signoff status is `passed`: blocking findings `0`, manual-review findings `0`, provider invocations `47/47` scripted, board-runtime parity reports `5/5`, post-profile reports `5/5`, recovery phase reports `5/5`, profile scheduler contract reports `5/5`, projection cutover reports `5/5`, legacy artifact coherence reports `5/5`, and legacy materialization write contract reports `5/5`. Per-case checks showed `workflow_causality_contract.violation_detected=false`, `legacy_materialization_write_contract.normal_path_write_count=0`, `recovery_phase_metrics.legacy_bridge_used_present=false`, `heuristic_pairing_used=false`, and `finalization_start_gate_source=profile_url_terminal_record_command_completed_at`.
- Full matrix timing: total avg `519.3s`, p95 `930.0s`, max `1008.8s`; wait-for-completion avg `483.4s`, p95 `900.3s`, max `976.1s`; job-to-board-nonempty avg `188.8s`, max `305.0s`; dashboard fetch max `5.5s`, candidate page fetch max `5.3s`, board probe wait max `10.8s`. The original report listed optimization-only warnings for `snapshot_full_materialization_background_pending` and `recovery_durable_work_handoff_yield`; the 2026-05-24 follow-up above reclassified healthy snapshot compaction and clean cooperative recovery yields into explicit passed gates.
- Post-signoff hard-disable: normal recovery and typed command-owner queues no longer run `legacy_materialization_adapter` by default. The adapter remains available only through explicit historical migration opt-in (`legacy_materialization_adapter_enabled=true` payload or `LEGACY_JOB_MATERIALIZATION_ADAPTER_ENABLED=1`) and stays report-visible. This removes the last normal-path scan/convert touchpoint for `job_materialization_items`; default skipped reason is `legacy_materialization_adapter_disabled_after_w6_signoff`.
- Post-retirement validation passed: `PYTHONPATH=src ./.venv-tests/bin/python -m pytest tests/test_durable_runtime.py -k "adapter or legacy_local_apply_bridge or board_visible_bridge or w6_normal_path" -q` -> `16 passed, 23 deselected`; W6b fast preflight rerun -> `25 passed, 554 deselected, 12 subtests passed`.
- PG-only durable runtime test cutover completed. Added `tests/pg_durable_runtime.py`, which creates a unique PG-only schema per typed-runtime test and fails/skips instead of falling back to SQLite. Migrated `tests/test_durable_runtime.py`, `tests/test_local_apply_closure_prerequisite.py`, and the materialization diagnostics slice in `tests/test_results_api.py` off `SOURCING_ALLOW_SQLITE_DURABLE_RUNTIME_UNIT_TESTS`, then deleted the storage escape hatch.
- The PG-backed migration exposed and fixed a real physical schema gap: when PG runtime tables were bootstrapped from older SQLite shadow schema, `workflow_events`/`workflow_commands` could miss unique idempotency constraints. `control_plane_live_postgres` now ensures unique indexes for event sequence/idempotency and command idempotency during runtime schema migration.
- Migration-era item queue tests now explicitly pass `legacy_materialization_adapter_enabled=true`; default recovery remains hard-disabled for the adapter. Search-seed and snapshot-compaction report payloads now also lift adapter namespace-skip `items` to the top-level result when no typed command is ready, keeping namespace mismatch diagnostics consistent across owners.
- Validation passed: `py_compile` for the touched runtime/test modules; `tests/test_durable_runtime.py` -> `39 passed, 12 subtests passed`; `tests/test_local_apply_closure_prerequisite.py` -> `17 passed, 4 subtests passed`; `tests/test_results_api.py -k "job_materialization_items_api_exposes_queue_diagnostics or runtime_details or public_results_api_runtime_details_are_bounded"` -> `3 passed`. Full W6 matrix does not need to be rerun for the test-fixture/PG-constraint cutover unless a future change touches normal command owners.
- Phase 8 projection facet/layering source-owner follow-up closed the remaining overlay source ambiguity. `_enqueue_projection_facet_layering_build_item(...)` now resolves a valid linked run-scope projection before considering any `overlay_info.serving_projection_id`; if no canonical projection is available, enqueue fails closed with `canonical_projection_link_required_for_projection_facet_layering`. The command planner no longer swallows durable-runtime write failures; enqueue returns fail-closed error details if `workflow_events` / `workflow_commands` cannot be written.
- The old no-projection-link overlay execution branch inside `_process_projection_facet_layering_build_item(...)` was physically deleted. Normal and migration-adapter commands can execute facet/layering only from `serving_projection_members`; historical overlay-only jobs must be backfilled into a run projection first. Regression coverage now proves fail-closed overlay retirement, projection-only builds from `serving_projection_members`, and canonical-link precedence when a legacy overlay path is still present. Validation: `tests/test_results_api.py -k "projection_facet_layering_build"` -> `4 passed, 291 deselected`; `tests/test_durable_runtime.py -k "projection_facet or w6_normal_path_enqueues_projection_facet"` -> `3 passed, 36 deselected`; `py_compile` for `orchestrator.py`, `tests/test_results_api.py`, and `tests/test_durable_runtime.py`.

## 2026-05-22 (Asia/Shanghai)

### Phase W5c.3b authoritative-source repair apply and post-apply overlap

- Applied the reviewed Google/Anthropic/OpenAI authoritative-source repairs through `ServingProjectionWriter.publish_collection_authoritative_projection`. The apply path only published `collection_authoritative_projection` rows and switched `collection_authoritative_pointer`; it did not mutate registry rows or historical snapshot files.
- Evidence: `runtime/audits/asset_consolidation/w5c3_apply_20260522T044128Z_repair_apply/large_companies_repair_apply.json`. Published projections: Google `proj_assetrepair_376e8f5ac9a091b24fdadbe4` from snapshot `20260410T123708` with `9325` members; Anthropic `proj_assetrepair_6cc91d7f5d71795687a26293` from `20260409T045403` with `3536`; OpenAI `proj_assetrepair_a1411a5b903c0daa834c606c` from `20260507T011827` with `1707`.
- Reran overlap against canonical collection pointers instead of stale registry reference ids. Evidence: `runtime/audits/asset_consolidation/w5c3_post_apply_w5b_20260522T051559Z_fast_overlap/large_companies_post_apply_overlap.json`. The rerun no longer blocks on missing authoritative reference payloads, but broad archive/delete is still not approved because most archive candidates have unique identities outside the repaired authoritative sets.
- W5c.3b is closed for repair/pointer publication. Remaining data-governance work is a narrower cold-backup/archive manifest for no-increment duplicates only; do not bulk-delete Google/Anthropic/OpenAI historical snapshots based on the current overlap report.

### Phase W2c.2b/W2c.3b and W4c physical legacy execution retirement

- Moved local-apply and board-visible follow-up semantics that were still tested as old item-queue behavior onto typed command owners. Local apply now preserves same-scope coalesced batching, profile/candidate budgets, event-time prerequisite reawaken, and per-job single-flight through `linkedin.local_profile_delta.apply` command ownership. Board-visible now preserves same-scope coalescing, candidate-budget splitting, event-time prerequisite reawaken, and patch publication through `projection.board_visible_patch.publish` command ownership.
- Physically retired the remaining local apply, board-visible, projection index, and collection merge legacy recovery execution fallback. The old queue entrypoints now only run the report-visible `job_materialization_items -> workflow_commands` adapter and then drain typed command owners. Explicit allow flags no longer execute legacy queues; disabling a typed owner fails closed with `legacy_job_materialization_recovery_bridge_disabled`.
- Added runtime-namespace fail-closed checks before command owners claim migrated payload rows. Cross-runtime `snapshot_dir`/artifact payloads are reported as `runtime_namespace_mismatch` with `runtime_namespace_skipped_count` and are not claimed, preventing nested scripted runtimes or old test assets from being consumed by the active runtime.
- Updated `DURABLE_EXECUTION_RUNTIME_CONTRACT.md` and `NEXT_TODO.md`: W2c.2b/W2c.3b are closed for owner semantics, W4c old execution branch deletion is closed, and remaining W6 work is normal-path legacy write retirement plus full signoff.
- Validation passed: `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py tests/test_local_apply_closure_prerequisite.py tests/test_durable_runtime.py`; targeted W4/W2c local bridge suite `tests/test_durable_runtime.py::LegacyMaterializationAdapterTest tests/test_local_apply_closure_prerequisite.py` (`24 passed, 4 subtests passed`); W2c/W4 API/service subset `tests/test_durable_runtime.py::LegacyMaterializationAdapterTest tests/test_local_apply_closure_prerequisite.py tests/test_results_api.py tests/test_service_daemon.py -k "LegacyMaterializationAdapter or local_apply_backlog or board_visible_apply or run_scope_projection or projection_person_search_index or collection_authoritative_merge or durable_materialization_drains_skip_nested_test_runtime_items_before_claim or harvest_prefetch_board_visible_delta"` (`27 passed, 317 deselected, 4 subtests passed`); broader service/signoff legacy subset `tests/test_results_api.py tests/test_service_daemon.py tests/test_workflow_service_metrics.py tests/test_scripted_smoke_signoff.py -k "LegacyMaterializationAdapter or legacy_recovery_bridge or recovery_bridge_usage or legacy_materialization or local_apply_backlog or board_visible_apply or run_scope_projection or projection_person_search_index or collection_authoritative_merge or durable_materialization_drains_skip_nested_test_runtime_items_before_claim or harvest_prefetch_board_visible_delta"` (`25 passed, 385 deselected`); `ruff check src/sourcing_agent/orchestrator.py tests/test_local_apply_closure_prerequisite.py tests/test_durable_runtime.py`.
- Remaining open items before full signoff: W3b still has completion/readiness call-site cleanup, W6 still needs full targeted OpenAI/Lovable/Google plus PG-backed scripted/nightly/containerized/browser signoff, and normal-path `job_materialization_items` writes still need a strict cutover plan after migration payload/reference storage is replaced or explicitly tolerated.

### Phase W5a asset consolidation dependency preflight

- Added `asset_consolidation_audit_v1` in `src/sourcing_agent/asset_consolidation_audit.py`. It builds a read-only snapshot dependency graph from local snapshot dirs plus `organization_asset_registry`, `acquisition_shard_registry`, `serving_projections`, `collection_authoritative_pointers`, `crm_records`, and `person_assets`.
- Snapshot classifications now distinguish `keep_authoritative_serving`, `keep_reusable_shard_source`, `keep_projection_dependency`, `archive_candidate_no_increment_duplicate`, and conservative `review_*` states. Deletion blockers are explicit for authoritative pointers, selected source snapshots, latest local pointers, reusable shards, active projections, CRM records, and PersonAsset dependencies.
- Added `scripts/audit_asset_consolidation.py` to generate JSON/Markdown W5 evidence before deleting old recovery branches or archiving duplicate Google/Anthropic/OpenAI/Lovable snapshots. The audit does not mutate data and does not scan large candidate JSON; member-level overlap/subsumption remains W5b.
- Updated `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, and `NEXT_TODO.md` with the W5a contract and the remaining W5b/W5c work. Full PG-backed Nightly remains deferred until W5 evidence and W6 preflight gates are clean.
- Validation passed: `python3 -m py_compile src/sourcing_agent/asset_consolidation_audit.py tests/test_asset_consolidation_audit.py scripts/audit_asset_consolidation.py`; `ruff check` for those files; `tests/test_asset_consolidation_audit.py` (`3 passed`).

### Phase W5b.1 bounded archive-candidate overlap evidence

- Added opt-in overlap/subsumption to `asset_consolidation_audit_v2` through `scripts/audit_asset_consolidation.py --include-overlap`. It reads candidate identity sets only for current authoritative/source snapshots and W5a archive candidates, with explicit `--overlap-candidate-limit` and `--overlap-snapshot-limit` bounds.
- Overlap identity prefers LinkedIn sanity/profile URL keys over historical candidate ids. Archive candidates remain `archive_ready=false` unless their identity set loads successfully, is not truncated, and is fully subsumed by authoritative/source reference identities.
- Missing local candidate payloads, truncated reads, missing reference identity, and unique identities are review states rather than delete approval. A CLI smoke against the PG-only OpenAI test-env registry correctly produced review states for archive candidates because the local snapshot payloads were not present.
- Updated `DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `DATA_ASSET_GOVERNANCE.md`, and `NEXT_TODO.md` for W5b.1/W5b.2. Real large-company evidence remains W5b.2 and must be run against the runtime that contains the local `company_assets` payloads.
- Validation passed: `python3 -m py_compile` for the audit module/tests/script; `ruff check` for those files; `tests/test_asset_consolidation_audit.py` (`6 passed`). Full PG-backed Nightly remains deferred.

### Phase W5b.2 real local asset consolidation evidence

- Ran W5a/W5b against real local Google, Anthropic, OpenAI, and Lovable assets after hardening the audit CLI with `--progress` and `--output-dir`. Multi-company runs now emit timestamped company progress and write per-company JSON/Markdown as soon as each company finishes, so maintenance audits leave partial evidence instead of behaving like a black-box long test.
- Added a fail-closed overlap fast path: when authoritative/selected reference identity cannot be loaded, the audit marks archive candidates as `review_reference_identity_missing` and does not scan large historical archive payloads. This avoids minutes of wasted JSON reads that cannot prove safe archival without a reference set.
- Evidence path: `runtime/audits/asset_consolidation/w5b2_20260522T021416Z_overlap_fastfail/large_companies_overlap.json` plus per-company reports under `runtime/audits/asset_consolidation/w5b2_20260522T021416Z_overlap_fastfail/companies/`.
- Findings: 66 snapshots inspected; 22 metadata archive candidates; 32 snapshots with blockers; 13 `review_*` metadata states; overlap enabled for 22 archive candidates; 0 overlap-subsumed and 0 deletion-ready snapshots. Google, Anthropic, and OpenAI fail closed because their current authoritative/selected registry snapshot ids are not present as local payload dirs. Lovable has one authoritative snapshot and no archive candidate.
- W5c must first publish/restore clean authoritative projections or payload-backed pointers before any duplicate archival. The current evidence explicitly does not approve deleting Google/Anthropic/OpenAI historical duplicates.
- Validation passed after the tooling/fast-fail change: `python3 -m py_compile src/sourcing_agent/asset_consolidation_audit.py tests/test_asset_consolidation_audit.py scripts/audit_asset_consolidation.py`; `ruff check` for those files; `tests/test_asset_consolidation_audit.py` (`8 passed`). Full PG-backed nightly matrix remains deferred until W5c/W6 preflight gates are clean.

### Phase W5c.1 read-only consolidation repair/archive plan

- Added `asset_consolidation_plan_v1` in `src/sourcing_agent/asset_consolidation_plan.py` and CLI `scripts/plan_asset_consolidation.py`. The plan consumes W5 audit JSON and emits required actions without recomputing dependencies or mutating registry pointers, projections, or local files.
- Generated the real local W5c plan from the W5b.2 evidence: `runtime/audits/asset_consolidation/w5c_plan_20260522T022436Z/large_companies_plan.json` and Markdown. Plan status is `blocked_missing_reference_identity`.
- Findings: all 22 archive candidates remain blocked; Google is missing reference payloads for `20260414T120400/20260414T120401`, Anthropic for `20260414T120200`, and OpenAI for `20260414T120300/20260414T120301`. The plan lists payload-backed local candidates and reusable shard sources for review only. Example: OpenAI `20260507T011827` has a local path but its registry source path is stale/missing, so it cannot be promoted without a review/publish/rerun cycle.
- Next gate is W5c.2: restore missing reference payloads from backup if available, or publish new payload-backed authoritative projections/pointers from reviewed local candidates, then rerun W5b overlap before any cold-backup/archive manifest.
- Validation passed: `python3 -m py_compile src/sourcing_agent/asset_consolidation_audit.py src/sourcing_agent/asset_consolidation_plan.py tests/test_asset_consolidation_audit.py scripts/audit_asset_consolidation.py scripts/plan_asset_consolidation.py`; `ruff check` for those files; `tests/test_asset_consolidation_audit.py` (`10 passed`).

### Phase W5c.2 read-only authoritative-source repair proposal

- Added `asset_consolidation_repair_proposal_v1` in `src/sourcing_agent/asset_consolidation_repair_proposal.py` and CLI `scripts/propose_asset_consolidation_repair.py`. The proposal verifies candidate authoritative replacements by reading local/source-path payloads, counting candidate identities, and flagging stale source paths or registry/payload count mismatches. It remains read-only and does not mutate registry pointers, projections, or files.
- Generated the real local W5c.2 proposal from the W5c.1 plan: `runtime/audits/asset_consolidation/w5c2_repair_proposal_20260522T034517Z_countsafe/large_companies_repair_proposal.json` and Markdown. Status is `ready_for_manual_authoritative_repair_review`.
- Recommended review candidates: Google `20260410T123708` with `9325` payload candidates / `9325` identities; Anthropic `20260409T045403` with `3536/3536`; OpenAI `20260507T011827` with `1707/1707`. OpenAI candidate has risks `currently_latest_local_pointer` and `registry_source_path_missing_or_stale`; that is acceptable for review but not automatic apply. Count-mismatch candidates, for example Google `20260410T121946` and OpenAI `20260423T165904`, are flagged with `registry_payload_count_mismatch` and are not eligible for recommendation.
- Next gate is W5c.3: after review, either restore missing reference payloads or publish new payload-backed authoritative projections/pointers from the recommended candidates, then rerun W5b overlap before any archive/cold-backup manifest.
- Validation passed: `python3 -m py_compile src/sourcing_agent/asset_consolidation_audit.py src/sourcing_agent/asset_consolidation_plan.py src/sourcing_agent/asset_consolidation_repair_proposal.py tests/test_asset_consolidation_audit.py scripts/audit_asset_consolidation.py scripts/plan_asset_consolidation.py scripts/propose_asset_consolidation_repair.py`; `ruff check` for those files; `tests/test_asset_consolidation_audit.py` (`14 passed`).

### Phase W5c.3a authoritative-source repair executor foundation

- Added `asset_consolidation_repair_apply_v1` in `src/sourcing_agent/asset_consolidation_repair_apply.py` and CLI `scripts/apply_asset_consolidation_repair.py`. It consumes a W5c.2 proposal plus explicit `company=snapshot` selections. Default mode is dry-run and reports the planned projection id, previous pointer, member count, payload hash, counts, readiness, scope spec, provenance, and metadata without mutating any state.
- Real mutation requires `--apply --reviewed`. The executor validates the selected candidate is `verified_payload_available`, has nonzero identity/payload counts, is not truncated, and has zero registry/payload candidate delta. Count-mismatch candidates fail closed. The apply path writes only through `ServingProjectionWriter.publish_collection_authoritative_projection`, replacing canonical projection members and switching the `collection_authoritative_pointer`; it does not edit organization asset registry rows or historical snapshot files.
- Generated real local dry-run evidence at `runtime/audits/asset_consolidation/w5c3_dry_run_20260522T040144Z_repair_apply/large_companies_repair_apply_dry_run.json` and Markdown. Google `20260410T123708` plans `9325` members, Anthropic `20260409T045403` plans `3536`, and OpenAI `20260507T011827` plans `1707`; no blockers were reported and no state was mutated.
- Tightened W5b overlap reference semantics for the post-repair rerun: when a canonical `collection_authoritative_pointer` exists, overlap now loads reference identity from active `collection_authoritative_projection` members and reports stale registry authoritative/selected ids as `legacy_registry_reference_snapshot_ids` rather than using them as the overlap source. Without this, applying W5c.3 would still leave W5b blocked by old missing registry payload ids.
- Remaining W5c.3b work is manual operation: apply only the accepted Google/Anthropic/OpenAI selections, then rerun W5b overlap before any cold-backup/archive manifest. Full PG-backed Nightly remains deferred until that repair/rerun evidence is clean.
- Validation passed: `python3 -m py_compile src/sourcing_agent/asset_consolidation_audit.py src/sourcing_agent/asset_consolidation_repair_apply.py scripts/apply_asset_consolidation_repair.py tests/test_asset_consolidation_audit.py tests/test_asset_consolidation_repair_apply.py`; `ruff check` for the W5 audit/repair files; `tests/test_asset_consolidation_audit.py` (`15 passed`), `tests/test_asset_consolidation_repair_apply.py` (`5 passed`), and the combined W5 audit/repair/projection-writer suite (`26 passed`).

### Phase W4c legacy recovery bridge pre-deletion guard

- Added service-metrics aggregation for recovery phase `legacy_bridge_used` evidence. `recovery_phase_metrics` now reports `legacy_bridge_used_count`, `legacy_bridge_used_present`, and sampled `legacy_bridge_used_phases`, and promotes bridge usage into a high-severity `legacy_materialization_recovery_bridge` bottleneck.
- Added Pre-Manual Signoff blocking for `require_no_legacy_materialization_recovery_bridge`; cutover cases now fail fast with `legacy_materialization_recovery_bridge_used` instead of waiting for full PG-backed Nightly to reveal old recovery branch execution.
- Superseded by the later W4c physical deletion entry above: implicit legacy fallback was first hard-disabled behind an explicit migration/emergency allow gate, then the executable legacy branches were removed. Explicit allow flags now no longer execute old item queues; they only remain in regression tests to prove fail-closed behavior.
- Updated `DURABLE_EXECUTION_RUNTIME_CONTRACT.md` and `NEXT_TODO.md`; the current contract is that migration-era rows are converted by the report-visible adapter and execution belongs only to typed command owners.
- Validation passed: `python3 -m py_compile` for touched service/signoff/expectation/orchestrator/tests; `ruff check` for touched files; targeted legacy bridge/materialization subset `tests/test_workflow_service_metrics.py tests/test_scripted_smoke_signoff.py -k "legacy_recovery_bridge or recovery_bridge_usage or legacy_materialization"` (`7 passed, 83 deselected`); durable runtime bridge subset (`9 passed, 13 deselected`); W2c/W4 targeted suite `tests/test_durable_runtime.py tests/test_local_apply_closure_prerequisite.py tests/test_results_api.py tests/test_service_daemon.py -k "LegacyMaterializationAdapter or legacy_recovery_bridge or local_apply_backlog or board_visible_apply or run_scope_projection or projection_person_search_index or collection_authoritative_merge or durable_materialization_drains_skip_nested_test_runtime_items_before_claim or harvest_prefetch_board_visible_delta"` (`30 passed, 329 deselected, 4 subtests passed`). Full PG-backed nightly matrix remains deferred until W5/W6 signoff gates are ready.

### Phase W4b legacy normal-write gate foundation

- Added `workflow_service_metrics.legacy_materialization_write_contract`, which reports all observed `job_materialization_items` as normal-path writes, explicit migration-adapter writes, or rows missing the write contract. The report includes item-kind counts and samples so legacy queue usage is visible before pressure/nightly runs.
- Added Pre-Manual Signoff gates for `require_no_legacy_materialization_normal_writes` and `require_legacy_materialization_write_contract_report`. Cutover cases can now block `legacy_materialization_normal_write_used` and `legacy_materialization_write_contract_missing`; migration adapter rows remain allowed when explicitly marked.
- Added the smoke expectation keys and exported the legacy materialization write contract in case-level smoke exports. Added planned service coverage tag `durable_runtime_legacy_write_cutover`; it should be promoted to `required_now` before W6 full runtime signoff, after W4c legacy branch deletion and W5 asset consolidation reduce migration-era false positives.
- Validation passed: `py_compile` and `ruff` for touched service-metrics/smoke/signoff/expectation tests; targeted preflight `tests/test_workflow_service_metrics.py tests/test_scripted_smoke_signoff.py tests/test_run_simulate_smoke_matrix.py -k "legacy_materialization or projection_cutover or service_gate_coverage"` (`8 passed, 92 deselected`); `scripts/check_service_gate_coverage.py --json` returned `ok 0`. Full PG-backed nightly matrix remains deferred.

### Phase W4a legacy materialization adapter foundation

- Added the report-visible `legacy_materialization_adapter` recovery/service phase owned by `durable_runtime_migration_adapter`. It inspects migration-era `job_materialization_items` and converts supported rows into reducer-owned `workflow_commands` without executing legacy items.
- Supported adapter conversions are `local_apply_closure -> linkedin.local_profile_delta.apply`, `board_visible_delta_apply -> projection.board_visible_patch.publish`, `projection_person_search_index_build -> projection.person_search_index.build`, and `collection_authoritative_merge -> collection.authoritative.merge`. Unsupported kinds, invalid scopes, open unconverted rows, already-converted rows, and command observations are surfaced in phase metrics instead of hidden fallback behavior.
- Prevented same-tick double ownership: when the adapter plans commands, the legacy event-level drain yields so typed owners can execute later. `already_converted_count` is audit-only and no longer blocks downstream typed owners from draining queued commands.
- Fixed a shared command-selection bug discovered by the board-visible chunk regression: planners now select planned commands by exact `idempotency_key` instead of taking `apply_result.commands[0]`. This prevents later chunks/owners in the same workflow from accidentally using an earlier succeeded command and returning stale/missing patch payloads.
- Added board-visible patch result normalization for owner/item envelopes so completed patch payloads expose stable top-level cumulative counts while preserving the typed owner as the execution source.
- Validation passed: `py_compile` and `ruff` for touched orchestrator/service/tests; `tests/test_durable_runtime.py` (`19 passed`); targeted W4/result/service preflight `tests/test_durable_runtime.py tests/test_service_daemon.py tests/test_local_apply_closure_prerequisite.py tests/test_results_api.py -k "LegacyMaterializationAdapter or local_apply_backlog or board_visible_apply or run_scope_projection or projection_person_search_index or collection_authoritative_merge or durable_materialization_drains_skip_nested_test_runtime_items_before_claim or harvest_prefetch_board_visible_delta"` (`27 passed, 329 deselected, 4 subtests passed`). Full PG-backed nightly matrix remains deferred until W4 gates/W5 cleanup/W6 signoff are ready.

### Phase W2c.4c run-scope projection finalization command owner

- Moved run-scope projection finalization onto the durable command-owner seam. Job result-view publication now writes reducer-owned `CommandPlanRequested` events for `workflow_commands(command_type='projection.run_scope.finalize')` with a stable idempotency key over job id, result view id, snapshot id, source path, and finalize scope.
- Added `serving_projection_owner` execution for `projection.run_scope.finalize`. The owner claims the typed command, publishes the run-scope projection through `ServingProjectionWriter`, writes the run/projection link, updates `job_result_view.metadata.run_scope_projection`, and marks command success/retry/terminal evidence.
- Added explicit durable `serving_finalized` proof: successful finalization records `CompletionProofRecorded(proof_key='serving_finalized')`, materialized into `workflow_current_state.completion_proofs`. This gives W3 a typed completion policy instead of relying on `job.status=completed/results`.
- Service status/cumulative/log summaries now expose finalization owner counts through `run_scope_projection_finalize.command_count`, `executed_command_count`, `claimed_count`, `completed_count`, and `candidate_count`.
- Validation passed: `py_compile` and `ruff` for touched durable runtime/orchestrator/service/tests; targeted W2c.4 suite `tests/test_durable_runtime.py tests/test_results_api.py tests/test_service_daemon.py -k "run_scope_projection or collection_authoritative_merge or projection_person_search_index"` (`16 passed`) before the proof-event addition. Full PG-backed nightly matrix remains intentionally deferred until W3/W4 boundaries are tighter.

### Phase W3a central completion policy evaluator

- Added side-effect-free `workflow_completion_policy.evaluate_linkedin_completion_policies(...)` for the LinkedIn acquisition/profile pipeline. It evaluates `stage1_candidate_set_terminal`, `stage1_preview_allowed`, `profile_fetch_terminal`, `local_apply_terminal`, `board_visible_terminal`, `serving_finalized`, `post_result_layering_ready`, and `collection_merge_terminal` from durable observations supplied by callers.
- Routed `_workflow_completion_promotion_blockers(...)` through the new evaluator while preserving legacy-compatible blocker reason payloads. Blockers now include a `completion_policy` diagnostic payload, making hidden completion inference report-visible during migration.
- `serving_finalized` policy now reads from `workflow_current_state.completion_proofs.serving_finalized`, not from public readers, lifecycle string heuristics, or `job.status`.
- Validation passed: `py_compile` and `ruff` for `workflow_completion_policy.py`, `orchestrator.py`, and targeted tests; `tests/test_workflow_completion_policy.py` (`4 passed`); targeted result integration seam (`3 passed`).

### Phase W2c.4b collection authoritative merge command owner

- Moved collection authoritative merge onto the durable command-owner seam. Run-scope projection publication still creates the migration-era `collection_authoritative_merge` payload/reference item, but it now also writes reducer-owned `CommandPlanRequested` events for `workflow_commands(command_type='collection.authoritative.merge')` with a stable idempotency key over collection id, source projection id, item id, publication fingerprint, and merge scope.
- Added `collection_writer_owner` execution inside the existing `collection_authoritative_merge` recovery/service phase. The owner claims the typed command, claims the referenced migration-era item, executes the collection merge writer primitive, switches `collection_authoritative_pointer` through `ServingProjectionWriter`, and marks command success/retry/terminal evidence.
- Service status/cumulative/log summaries now expose collection merge command-owner counts through `collection_authoritative_merge.command_count`, `executed_command_count`, `claimed_count`, `completed_count`, and `candidate_count`.
- Validation passed: `py_compile` and `ruff` for touched durable runtime/orchestrator/service/tests; targeted W2c.4b suite `tests/test_durable_runtime.py tests/test_results_api.py tests/test_service_daemon.py -k "collection_authoritative_merge or projection_person_search_index or board_visible_apply or local_apply_backlog"` (`15 passed`).
- Remaining risk: W2c.4b still uses `job_materialization_items(collection_authoritative_merge)` as a temporary payload/reference table. Run-scope projection finalization remains W2c.4c; full PG-backed nightly matrix remains deferred until W2c owner boundaries are tighter.

### Phase W2c.4a projection person search index build command owner

- Moved projection person search index build onto the durable command-owner seam. Projection publication/layering/assertion promotion still creates the migration-era `projection_person_search_index_build` payload/reference item, but it now also writes reducer-owned `CommandPlanRequested` events for `workflow_commands(command_type='projection.person_search_index.build')` with a stable idempotency key over projection id, item id, semantic index input version, and build scope.
- Added `projection_index_owner` execution inside the existing `projection_person_search_index` recovery/service phase. The owner claims the typed command, claims the referenced migration-era item, executes one bounded index page, marks command success/retry/terminal evidence, and releases partial progress without burning attempts.
- Service status/cumulative/log summaries now expose projection index command-owner counts through `projection_person_search_index.command_count`, `executed_command_count`, `claimed_count`, `completed_count`, `partial_count`, `candidate_count`, and `indexed_count`.
- Validation passed: `py_compile` and `ruff` for touched durable runtime/orchestrator/service/tests; targeted W2c.4a suite `tests/test_durable_runtime.py tests/test_results_api.py tests/test_service_daemon.py -k "projection_person_search_index or board_visible_apply or local_apply_backlog"` (`10 passed`).
- Remaining risk: W2c.4a still uses `job_materialization_items(projection_person_search_index_build)` as a temporary payload/reference table. Collection authoritative merge and run-scope projection finalization remain W2c.4b/W2c.4c; full PG-backed nightly matrix remains deferred until these owner boundaries are tighter.

### Phase W2c.3 board-visible patch publication command owner

- Moved board-visible patch publication onto the durable command-owner seam. Local apply still creates the migration-era `board_visible_delta_apply` payload/reference item, but it now also writes reducer-owned `CommandPlanRequested` events for `workflow_commands(command_type='projection.board_visible_patch.publish')` with a stable idempotency key over job, snapshot, item, candidate ids, and publish scope.
- Added `board_visible_projection_owner` execution inside the existing `board_visible_apply` recovery/service phase. The owner claims the typed command, claims the referenced migration-era item, executes the bounded board-visible patch publication primitive, and marks command success/retry/terminal evidence. The old item queue is now only used when `PROJECTION_BOARD_VISIBLE_PATCH_PUBLISH_COMMAND_OWNER_ENABLED=0`, where it is report-visible as a migration bridge.
- Tightened the post-profile owner chain: worker completion callbacks enqueue local apply only; the local apply command owner plans board-visible publish commands; board-visible overlay/projection writes happen only in the board-visible command owner. A transient overlay writer failure now leaves the command in `retry_wait` and recovers through the owner rather than relying on another webhook or direct callback replay.
- Service status/cumulative/log summaries now expose board-visible patch publish command-owner counts through `board_visible_apply.command_count` and `executed_command_count`, alongside claimed/completed/waiting/failed/candidate counters.
- Validation passed: targeted W2c.3 suite `tests/test_durable_runtime.py` plus board-visible pipeline/local-apply/service cases (`16 passed`).
- Remaining risk: W2c.3 still uses `job_materialization_items(board_visible_delta_apply)` as a temporary payload/reference table. Event-time prerequisite reawaken, coalesced grouping payloads, and full payload storage should move into command/outbox semantics in W2c.3b; run-scope projection finalization, projection index build, and collection merge owners remain W2c.4.

### Phase W2c.2 local profile delta apply command owner

- Moved local profile delta apply onto the durable command-owner seam. `local_apply_closure` enqueue/backfill now also writes reducer-owned `CommandPlanRequested` events for `workflow_commands(command_type='linkedin.local_profile_delta.apply')` with a stable idempotency key over job, snapshot, item, worker kind, worker ids, and apply scope.
- Added `profile_local_apply_command_owner` execution inside the existing `local_apply_backlog` recovery/service phase. The owner claims the typed command, claims the referenced migration-era `local_apply_closure` item, runs the existing bounded local apply primitive, and marks command success/retry/terminal evidence. The old item queue is now only used when `LINKEDIN_LOCAL_PROFILE_DELTA_APPLY_COMMAND_OWNER_ENABLED=0`, where it is report-visible as a migration bridge.
- Preserved local-apply-specific semantics while moving ownership: partial profile URL chunks release the command without burning attempts, and waiting-prerequisite state remains a non-failure retry-wait command plus zero-cost legacy item wait until W2c.2b moves prerequisite reawaken fully into command/outbox semantics.
- Service status/cumulative/log summaries now expose local profile delta apply command-owner counts through `local_apply_backlog.command_count` and `executed_command_count`, alongside claimed/completed/partial/waiting/failed counters.
- Validation passed: `py_compile` for touched durable runtime/storage/PG/orchestrator/service/tests; targeted W2c.2 suite `tests/test_durable_runtime.py` plus local-apply pipeline/service cases (`16 passed`).
- Remaining risk: W2c.2 still uses `job_materialization_items(local_apply_closure)` as a temporary payload/reference table. Same-scope coalesced batching and event-time command reawaken are left for W2c.2b; board-visible, projection finalization/index, and collection merge owners remain W2c.3/W2c.4.

### Phase W0 durable-runtime contract preflight closeout

- Closed the current fast preflight slice before another full Nightly run. The Stage 1 progress payload and profile slot-refill evaluator contracts are now covered by targeted gates, and the OpenAI/Lovable long-latency rerun passed without progress, scheduler, or board-runtime parity violations.
- Fixed the last targeted OpenAI drift: terminal/current-snapshot complete serving now anchors public `board_runtime_state.row_publication_sequence` / watermark to the first complete current-snapshot row-publication patch rather than the latest appended patch. Later full-snapshot compaction or layering patches can continue in the background without making `/progress`, `/dashboard`, `/candidates`, and `/board-patches` disagree on the public serving watermark.
- Added regression coverage for the stable complete-publication watermark, alongside existing stale-partial and patch-ledger board-runtime tests.
- Validation passed: `py_compile` and `ruff` for `src/sourcing_agent/orchestrator.py` and `tests/test_results_api.py`; `tests/test_results_api.py -k "board_runtime_state_full_serving_watermark_uses_first_complete_publication or board_runtime_state_final_serving_counters_outrank_stale_partial_patch or board_runtime_uses_complete_patch_ledger_over_stale_served_lifecycle_count"` (`3 passed`); targeted PG-backed long-latency matrix `output/phase_w0_targeted_20260522_001152/` for OpenAI baseline+delta and Lovable live-roster passed strict with `expectation_failures=[]`, `board_runtime_state_parity.violation_case_count=0`, `profile_scheduler_contract.violation_detected=false`, and `progress_contract_violation_case_count=0`.
- Next step: start Phase W1 runtime schema/reducer skeleton before spending another full Nightly cycle, so the broader pressure suite validates timing and migration behavior rather than rediscovering basic source-of-truth drift.

### Phase W1 durable runtime foundation

- Added the first canonical durable runtime physical model: append-only `workflow_events`, materialized `workflow_current_state`, typed `workflow_commands`, and `runtime_outbox` in SQLite shadow and live PG registries. These tables are the long-term command/event substrate; `job_materialization_items` remains a legacy migration adapter target rather than the new command system.
- Added `ControlPlaneStore` APIs for event append/list, current-state upsert/get, command upsert/list/claim/running/succeeded/failed, and outbox enqueue/dispatch. The store enforces per-run event idempotency, monotonic sequence assignment, command idempotency by `(workflow_run_id, idempotency_key)`, and terminal command immutability for the covered transitions.
- Added `src/sourcing_agent/durable_runtime.py` as the W1 pure reducer/owner-registry skeleton. `CommandOwnerRegistry` makes command ownership explicit, rejects ambiguous owner remaps, and `reduce_workflow_events(...)` produces typed command/outbox specs from durable events without calling providers or mutating domain stores.
- Extended the PG-only Testcontainers contract so `make ci-pg-contract` now bootstraps and writes the durable runtime tables, including command `queued -> claimed -> running -> succeeded` lifecycle and dispatched outbox evidence.
- Validation passed: `py_compile` and `ruff` for touched runtime/storage/PG/tests; `tests/test_durable_runtime.py` (`6 passed`); `tests/test_serving_projection_storage.py tests/test_durable_runtime.py` (`12 passed`); `make docker-doctor`; `make ci-pg-contract` (`1 passed`).
- Remaining risk: this is a foundation slice and intentionally does not migrate LinkedIn acquisition/profile normal paths yet. W2 must route provider callbacks/workers into event/result writes and let reducers generate typed commands; until then old workflow paths and `job_materialization_items` still run existing production behavior.

### Phase W2a reducer-owned writer and legacy write gate foundation

- Added `DurableRuntimeWriter` as the first reducer-owned application boundary. It appends a durable event, loads unprocessed events after `workflow_current_state.last_processed_sequence_number`, runs the pure reducer, then persists current state, typed commands, and runtime outbox rows. Re-running the writer with no new events is a no-op and command creation remains idempotent by `(workflow_run_id, idempotency_key)`.
- Added command-count materialization for current state via `summarize_workflow_command_counts(...)`, so active/terminal command counts are derived from canonical `workflow_commands` instead of being guessed by public readers or recovery callbacks.
- Added `legacy_materialization_write_contract` metadata to new `job_materialization_items` writes. Normal writes are now report-visible as migration debt with `target_runtime_table=workflow_commands` and `retirement_phase=Phase W4`. A strict opt-in env gate, `SOURCING_BLOCK_LEGACY_JOB_MATERIALIZATION_NORMAL_WRITES=1`, blocks non-migration writes while allowing explicit migration adapter writes.
- Validation passed: `tests/test_durable_runtime.py` now covers writer idempotency and the legacy write gate (`8 passed`); combined runtime/projection storage subset passed (`14 passed`); `py_compile` and `ruff` passed for touched runtime/storage/tests; `make ci-pg-contract` passed after W2a.
- Remaining risk: W2a does not yet move LinkedIn profile-refill or post-profile owners onto `workflow_commands`; it creates the tested seam for W2b/W2c. Until those slices land, `job_materialization_items` still exists in the normal path but now carries explicit retirement metadata and can be blocked under strict cutover gates.

## 2026-05-21 (Asia/Shanghai)

### Phase 8/12 recovery and finalization projection-count closeout

- Fixed PG/SQLite durable materialization recovery so expired `running/applying` `job_materialization_items` become claimable again. This prevents crashed recovery ticks from stranding `local_apply_closure`, `board_visible_delta_apply`, projection-index, collection-merge, or snapshot-compaction work outside the ready queue.
- Tightened asset-population finalization reuse: canonical `run_scope_projection` can now prove finalization even when there is no legacy overlay file, and projection-ready finalization does not call `_write_job_asset_population_overlay(...)`. When a serving overlay/projection has a deduped member count lower than raw candidate-source/lifecycle expected count, finalization now publishes the actual serving count and keeps the raw count only as `raw_expected_candidate_count` diagnostics.
- Made `run-worker-daemon-once` operator output bounded. Recovery payloads still preserve structured phase evidence, but long scalar arrays such as candidate ids and profile URLs are emitted as `count` + `sample` instead of dumping thousands of entries to the terminal.
- Runtime evidence: drained `runtime/test_env/google_large_projection_bulk_20260521_072608` / job `eccdfd76599f` after the reclaim fix. The run advanced from `running/retrieving` with `delta_profile_board_visible_count=2165/2384` to `completed/completed` with `2384/2384` fetched/applied/materialized/board-visible, finalization recorded `finalization_serving_projection_reuse`, and run/collection projection indexes published exact `public_facet_counts` from `projection_person_search_index`. The only remaining queued item in that old runtime is background `snapshot_full_materialization`, which is not on the projection-visible path.
- Validation passed: `tests/test_control_plane_live_postgres.py -k "materialization_claim_can_reclaim_expired_running_lease or waiting_prerequisite_reawaken_records_source_metadata"` (`2 passed`), `tests/test_results_api.py -k "job_materialization_item_claim_is_ordered_idempotent_and_retryable or finalization_reuses_canonical_run_projection_without_overlay_file or reuse_existing_asset_population_overlay or projection_facet_layering_build_splits_public_overlay"` (`3 passed`), `tests/test_cli.py -k "run_worker_daemon_once_prints_json_safe_recovery_payload"` (`1 passed`), `tests/test_pipeline.py -k "execute_asset_population_fast_path_reuses_complete_board_projection or execute_asset_population_fast_path_reuses_canonical_projection_without_overlay_write"` (`2 passed`), `ruff check` and `py_compile` on touched backend/tests.
- Remaining risk: the old Google runtime was recovered in place, not rerun end-to-end from a fresh scripted matrix after these fixes. Full PG-backed scripted/nightly matrix plus Pre-Manual Signoff should be rerun before manual browser handoff.

### Canonical projection public facet counts from index

- Moved projection global facet publication onto the canonical index path. When `projection_person_search_index` completes, `PersonAssetWriter` now pages persisted index/filter records and writes `counts.public_facet_counts` back to `serving_projections`; `/projections/{projection_id}` can serve exact global filter counts without reading job overlays, stage files, raw profile JSON, or frontend page fragments.
- Tightened the pre-migration outreach-layering bridge: when `projection_facet_layering_build` completes from a job overlay, it now publishes per-candidate layer assignments back into the linked run-scope `serving_projection_members`, updates projection layering readiness metadata, and enqueues a projection search-index rebuild. Layer filters/facets therefore converge through the projection/index path instead of remaining trapped in the old overlay artifact.
- Retired normal-path full overlay rewrite for projection-ready runs. If the layer assignment publication succeeds against the linked run-scope projection, the builder writes compact layering summaries with candidate records omitted and marks `legacy_overlay_publication.status=skipped`; full overlay candidate rewrites are now only for migration-era jobs without a canonical projection link.
- Added paged `ControlPlaneStore.list_projection_person_search_index_rows(...)` so background builders can consume index rows with explicit offset/limit instead of using request-path full scans.
- Regression coverage: `tests/test_person_asset_crm_projection_contracts.py::test_projection_search_index_publishes_canonical_public_facet_counts` proves facet summary is unavailable before the index build and complete after index finalization from canonical `projection_person_search_index` records.
- Validation passed: `tests/test_person_asset_crm_projection_contracts.py` (`13 passed`), projection/storage/results targeted subset (`15 passed, 272 deselected`), `tests/test_workflow_smoke.py -k "projection_facet_layering"` (`1 passed, 157 deselected`), `ruff check` on touched backend/tests, `py_compile` on touched backend/tests, and `git diff --check` on touched files.
- Superseded follow-up: projection-ready runs now resolve `projection_facet_layering_build` input from the linked `serving_projection_members` path. This intermediate migration allowance is now superseded by the 2026-05-24 closeout: no-link overlay rewrite execution is physically deleted, and historical jobs without a canonical projection link must be backfilled into a run projection before layering.

### Phase 9b.2b CRM/Public Web execution bridge retirement

- Retired the remaining normal-path CRM Public Web execution bridge. `start_crm_record_public_web_search(...)` now creates `crm_public_web_search` jobs/workers and `crm_public_web_v1` batches/runs; worker recovery routes those workers through owner-aware Public Web execution without target-candidate batch/run ownership.
- Cut CRM Public Web promotion off the legacy target-candidate promotion helper. `promote_crm_record_public_web_signal(...)` now validates that the signal belongs to a CRM-owned Public Web run, writes `crm_public_web_promotions`, records the selected `PersonAssertion`, emits the CRM assertion-link event, and enqueues projection person-search index rebuild without creating a target-candidate bridge row or writing `target_candidate_public_web_promotions`.
- Tightened API and report contracts: retired `/api/target-candidates/public-web...` payloads now report `public_web_storage_bridge=""`; service metrics, smoke expectations, and Pre-Manual Signoff treat any normal-path `target_candidate_public_web_v1` execution backend as blocking rather than a known acceptable warning.
- Validation passed for this slice: `tests/test_projection_crm_api_contracts.py` (`9 passed, 4 subtests passed`); combined CRM/Public Web targeted suite with workflow event response, service metrics, smoke aggregation, signoff, and target-candidate legacy tests (`39 passed, 4 subtests passed`); `ruff check` on touched backend/tests.
- Remaining risk: legacy target-candidate Public Web tables/helpers and default-410 HTTP aliases still exist for migration/test-only compatibility. They are no longer normal CRM path owners; deletion should be bundled with Phase 11b production/historical migration evidence and legacy endpoint retirement.

### Phase 11b projection-ready legacy result endpoint retirement

- Changed legacy job-result endpoint cutover from env-only to projection-ready default. When a run has a ready `run_scope_projection`, legacy `/api/jobs/{job_id}/results`, `/dashboard`, `/candidates`, and `/candidates/{candidate_id}` now return `410` with the linked `/projections/{projection_id}` pointer instead of composing public reads from job/result/candidate artifacts.
- Updated frontend behavior for the new default: `getDashboard(jobId)` handles legacy `410` by resolving `/api/runs/{run_id}/projection-link` and loading the projection dashboard, and projection result pages no longer issue job-scoped legacy candidate-detail hydration requests.
- Added migration controls: `SOURCING_ALLOW_LEGACY_JOB_RESULT_ENDPOINTS=1` explicitly allows the old composition path for migration/test-only use; `SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS` still force-enables or force-disables retirement for controlled tests. Runs without a ready projection remain `migration_required`.
- Validation passed: `tests/test_projection_crm_api_contracts.py` (`10 passed, 4 subtests passed`), `tests/test_results_api.py -k "projection_link or projection_api or legacy_job_result or run_projection or serving_projection"` (`4 passed, 259 deselected`), `py_compile` for touched API/orchestrator/tests, `ruff check` for touched API/orchestrator/tests, and `npm run build --prefix frontend-demo`.
- Remaining risk: full historical production backfill evidence and deletion of legacy composition code for migration-required rows are still open. ECS is already cold-backed-up and released; future VPS deployment should restore ECS PG only into scratch DB for selective extraction/backfill.

## 2026-05-20 (Asia/Shanghai)

### Phase 9b.2 CRM/Public Web normal-path cutover

- Moved the target-candidate page's normal read/edit path to person-first CRM APIs. `GET /api/crm/records`, `GET /api/crm/records/{crm_record_id}`, and `PATCH /api/crm/records/{crm_record_id}` now back the page-compatible target-candidate payload; frontend add/edit flows no longer write legacy `target_candidates` state as the owner.
- Added CRM-owned Public Web routes for the target-candidate UI: `POST /api/crm/records/public-web-search`, `POST /api/crm/records/public-web-search/poll`, cancel/retry, `GET /api/crm/records/{crm_record_id}/profile`, `GET /api/crm/records/{crm_record_id}/public-web-search`, `GET/POST /api/crm/records/{crm_record_id}/public-web-promotions`, and `POST /api/crm/records/public-web-export`.
- Historical intermediate state superseded on 2026-05-21: this slice still kept the legacy `target_candidate_public_web_*` storage tables as an explicit Public Web subsystem bridge. Normal CRM paths no longer create/use that bridge after the 2026-05-21 closeout.
- Frontend `TargetCandidatesPanel` now calls the CRM Public Web routes. Regression coverage blocks reintroducing `/api/target-candidates/public-web...` into frontend normal paths.
- Phase 9b.2b partial close: legacy `/api/target-candidates/public-web...` HTTP aliases now return `410` by default with canonical CRM endpoint pointers. `SOURCING_ALLOW_LEGACY_TARGET_PUBLIC_WEB_ENDPOINTS=1` remains migration/test-only. The post-workflow `target_public_web_action` smoke runner now sources candidates from the run projection, adds them to CRM, triggers `/api/crm/records/public-web-search`, and polls `/api/crm/records/public-web-search/poll`; it no longer imports legacy target-candidate rows as the normal action path.
- Phase 9b.2b owner-storage closeout slice: added `crm_public_web_batches`, `crm_public_web_runs`, and `crm_public_web_promotions` to SQLite/PG control-plane storage. CRM Public Web list/detail/promotion/export now read/write owner tables and report `public_web_storage_owner=crm_public_web_v1`; export uses CRM records plus person public-web assets/signals and no longer calls the legacy target-candidate export path.
- Historical intermediate state superseded on 2026-05-21: this slice added event-time CRM owner mirroring while the existing target-candidate execution backend was still active. Normal CRM Public Web execution now writes CRM-owned runs directly.
- Removed CRM Public Web read-time mutation/fallback: poll/detail/promotion/export no longer sync from legacy target-candidate Public Web rows on read, and detail no longer treats a person public-web asset with only a legacy run id as a valid CRM result. Missing CRM owner rows fail closed as `public_web_search_not_found`.
- Historical intermediate state superseded on 2026-05-21: this slice made the remaining Public Web execution bridge report-visible. It is now a blocking normal-path failure instead of a known warning.
- Removed the unused `_sync_target_public_web_scope_to_crm_owner(...)` helper so owner mirroring remains event-time/write-path only (`start`, `cancel`, `retry`, promotion, and batch-summary sync) and cannot be reintroduced as a read-time scope reconstruction fallback.
- Historical intermediate smoke evidence: `output/scripted_smoke_current/target_public_web_owner_bridge_report.json` and signoff passed under the temporary bridge-warning contract. This evidence is no longer sufficient for current signoff because normal CRM Public Web action must report `crm_public_web_v1` execution.
- Validation passed: `py_compile` for touched backend/tests; `ruff check` for touched backend/tests; `tests/test_projection_crm_api_contracts.py tests/test_frontend_candidate_filters.py` (`25 passed, 4 subtests passed`); CRM/Public Web targeted suite (`14 passed, 150 deselected`); legacy migration Public Web subset (`8 passed, 255 deselected`); `npm run build --prefix frontend-demo`; 2026-05-20 follow-up `tests/test_projection_crm_api_contracts.py -k crm_public_web` (`3 passed`), `tests/test_target_candidate_public_web.py` (`13 passed`), and combined projection/results/workflow Public Web subset (`27 passed, 402 deselected`).
- Remaining risk as of 2026-05-20, superseded on 2026-05-21: execution backend retirement was still open at this point. The current remaining risk is narrower: legacy target-candidate Public Web tables/helpers and default-410 aliases still exist for migration/test-only compatibility and should be deleted with Phase 11b migration evidence.

### Phase 12 PG-backed scripted/nightly matrix and runner-exit closeout

- Root cause found in the first full matrix closeout run: the five-case PG-backed nightly long-latency matrix had already written `report.json` / `summary.json` with empty expectation failures, but the runner did not exit because Python was stuck in `Py_FinalizeEx` waiting on non-daemon `ThreadPoolExecutor` threads. Those threads came from the in-process parallel former-search seed helper and continued scanning/parsing large Google `harvest_profiles/*.queue_dataset_items.json` files after the acquisition owner had returned.
- Fixed the owner boundary in `src/sourcing_agent/acquisition.py`: in-process parallel former-search seed helpers now use named executor threads and join/cancel them before acquisition returns. The contract is explicit: if former-search work should outlive the acquisition call, it must be a durable worker item, not an unowned in-process executor.
- Added regression coverage in `tests/test_pipeline.py` for both scoped-search and full-roster failure paths, proving the former-search executor is joined and no `acquisition-*-former-seed` threads leak after owner return.
- Validation passed: `./.venv-tests/bin/python -m py_compile src/sourcing_agent/acquisition.py tests/test_pipeline.py`; `./.venv-tests/bin/python -m ruff check src/sourcing_agent/acquisition.py tests/test_pipeline.py`; targeted parallel-former regression suite (`4 passed`); targeted Gemini exit rerun `output/phase12_gemini_exit_20260520_224028/` passed strict + Pre-Manual Signoff and exited normally.
- Full current-cutover validation passed: `output/phase12_full_exitfix_20260520_230047/` reran all five PG-backed nightly long-latency cases strict, scripted-only, and exited normally. Pre-Manual Signoff status was `passed`; provider-mode gate saw all `47` invocations scripted; blocking/manual-review findings were empty; board-runtime parity, post-profile SLO, recovery phase metrics, and profile scheduler reports were present for all five cases.
- Representative efficiency from the current full matrix: OpenAI baseline+delta `258.0s`, OpenAI no-baseline scoped-search `255.9s`, Lovable live-roster `356.5s`, Google vision-language large baseline + large shard `1353.5s`, Google Gemini small former shard `589.4s`. Google large used 4 profile actor batches (`529`, `619`, `619`, `617` URLs), confirming provider envelope decoupling is active; its remaining long tail is dominated by local materialization and deterministic facet/layering drain, not additional provider wait.
- Remaining risk as of 2026-05-20, superseded by later W6/W6b follow-ups: Phase 12 gates were closed for that cutover, while Phase 9b.2b Public Web storage bridge retirement and Phase 11b production migration/legacy endpoint retirement were still open. Later 2026-05-24 work reclassified healthy background snapshot compaction, clean recovery handoff yield, and clean recovery budget yield into explicit passed gates; dirty or insufficiently proven owner states remain report-visible.

### Phase 12 workflow-driven browser gate closeout

- Added `tests/testcontainers_workflow_browser_gate.py` and `make ci-workflow-browser-gate` as the missing production-parity bridge between fake-provider workflow confidence and browser projection confidence. The gate starts a real workflow through `/api/workflows`, routes Harvest/Apify company/search/profile actor HTTP calls to `FakeApifyProvider`, waits for event-time `run_scope_projection` plus `projection_person_search_index`, starts the real backend/Vite preview, and drives the resulting `/projections/{projection_id}` with Playwright.
- Tightened the pre-release aggregation: `make ci-containerized-pre-release` now runs Docker doctor, disposable PG storage/writer contracts, fake-provider webhook/object-store integration, seeded projection browser coverage, and workflow-driven projection browser coverage. The existing GitHub Actions workflow uses the same Make target, so the new gate becomes CI-visible without adding another ad hoc command.
- Validation passed: `./.venv-tests/bin/ruff check tests/testcontainers_workflow_browser_gate.py`; `make ci-workflow-browser-gate` (`1 passed`, ~56s); `make ci-containerized-pre-release` (Docker doctor, PG contract, fake-provider webhook/object-store, seeded projection browser, workflow-driven projection browser all passed).
- Superseded risk note: the full PG-backed scripted/nightly matrix has now been rerun on the current cutover; see the Phase 12 matrix closeout entry above.

### Phase 0b/12 local Docker and Testcontainers gate closeout

- Installed local Docker CLI plus Colima runtime through Homebrew and started the `colima` Docker context. `make docker-doctor` now validates both Docker CLI reachability and Python Docker SDK/Testcontainers connectivity.
- Fixed the Testcontainers harness to use the active Docker context socket instead of assuming `/var/run/docker.sock`: `scripts/with_docker_context.sh` exports `DOCKER_HOST` from `docker context inspect`, Make container targets run through that wrapper, and `tests/testcontainers_helpers.py` applies the same context resolution for direct unittest runs.
- Made the PG container image configurable and defaulted Make gates to `public.ecr.aws/docker/library/postgres:16-alpine`, avoiding Docker Hub as a local/CI single point of failure. Ryuk is disabled for these explicit-cleanup contract tests to avoid a second non-business image dependency.
- Fixed a fake-provider workflow harness bug where filesystem object-store visibility was asserted after `TemporaryDirectory` cleanup. The object evidence check now happens while the object-store directory still exists.
- Validation passed: `make docker-doctor`, `make ci-pg-contract`, `make ci-workflow-fake-provider`, `py_compile` and `ruff check` for touched Testcontainers tests/helpers, and `git diff --check` for the Docker/Testcontainers files.
- Remaining risk: Phase 12 still needs the full backend/frontend/browser containerized pre-release gate. The foundational PG contract and fake-provider workflow gate are now runnable locally and should be CI-required, but they do not yet replace full scripted/nightly/browser signoff.

### Canonical projection Phase 6b/7b event-time index closeout slice

- Added production-scale `PersonSummaryView` repair: `ServingProjectionMigrationBackfill.backfill_person_summary_views(...)` and `/api/projections/backfill-person-summary-views` repair historical projection rows through the writer/storage contract instead of letting public readers infer identity on demand.
- Tightened frontend projection row identity: projection members now carry `candidate_identity_key`, `person_identity_key`, and `profile_url_key` into `Candidate` records, and the frontend uses canonical identity as the stable row id when `candidate_id` is absent instead of generating a random id.
- Made `projection_person_search_index` rebuild paged and durable. Run-scope projection publication, collection-authoritative projection publication, and Public Web assertion promotion enqueue `projection_person_search_index_build`; recovery drains it as its own background phase. `/api/projections/backfill-person-search-indexes` provides stale-index production backfill.
- Validation passed: backend `py_compile` for touched modules; `ruff check` for touched backend/tests; `tests/test_person_asset_crm_projection_contracts.py` (`11 passed`); projection/CRM contract bundle (`19 passed`); targeted collection/index recovery tests in `tests/test_results_api.py` (`5 passed, 250 deselected`); `tests/test_frontend_candidate_filters.py` (`18 passed`); `tests/test_scripted_smoke_signoff.py` (`28 passed`); `npm run build --prefix frontend-demo`; and `git diff --check` for touched files.
- Remaining risk: full PG-backed scripted/nightly matrix and complete backend/frontend/browser containerized pre-release gates have still not run in this local pass. Local optional Testcontainers tests skipped by default (`2 skipped`), while CI-required `make ci-pg-contract` and `make ci-workflow-fake-provider` fail because Docker is not reachable on this machine (`FileNotFoundError` for the Docker socket). The broader collection/person-level RawProfileIndex/CandidateEvidenceIndex remains a future extension if projection-scoped indexes are not enough.

## 2026-05-19 (Asia/Shanghai)

### Canonical projection Phase 7b/10b cutover tightening

- Extended `projection_person_search_index` beyond keyword-only search: index rows now persist a public `filter_record`, and projection candidate pages use the index for structured filters when built, including employment, recall, function, location, layer, and audit filters. If a migration-era projection lacks index records, the old member-scan path is now explicitly labeled `serving_projection_members_scan_legacy_cutover` through `filter_contract.fallback_used`, so it is no longer silent.
- Retired legacy target-candidate export as a normal path. `/api/target-candidates/export` now returns `410` by default with the canonical `/api/projections/export` pointer; `SOURCING_ALLOW_LEGACY_TARGET_CANDIDATE_EXPORT=1` is migration/test-only compatibility. Projection export now supports selected `candidate_identity_keys`, and the target-candidate page uses projection export when selected records carry source projection provenance.
- Frontend/source contracts updated: target candidate records now retain `sourceProjectionId`, `candidateIdentityKey`, and person/source provenance; Excel group export resolves the run projection and uses projection export instead of the retired target-candidate archive endpoint.
- Docs updated: `docs/NEXT_TODO.md`, `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `docs/FRONTEND_API_CONTRACT.md`, `docs/PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md`, and `docs/CRM_STATE_CONTRACT.md`.
- Validation passed: backend `py_compile`/`ruff` for touched modules, projection/person/CRM/frontend contract tests (`45 passed`), Harvest connector fake-provider suite (`121 passed`), Testcontainers default local harness skip (`2 skipped` unless explicitly enabled), legacy export compatibility tests (`2 passed, 251 deselected` with the explicit compatibility env), `git diff --check`, and `npm run build`.
- Remaining risk: production-scale index backfill and event-time raw/evidence indexer are still open under Phase 7b. Full PG-backed scripted/nightly matrix and complete containerized browser/pre-release gates are still not run in this local pass.

### Canonical projection Phase 0b fake-provider HTTP connector slice

- Added configurable Apify/Harvest API base URL support via `SOURCING_APIFY_API_BASE_URL` / `APIFY_API_BASE_URL` / request context, so the real connector HTTP path can target a local fake provider without changing connector payload construction.
- Replaced hardcoded `https://api.apify.com` endpoint construction for actor submit, run-status poll, dataset item download, run log, and sync-run calls with the shared Apify endpoint builder.
- Tightened live-access protection for configured fake Apify endpoints: `_harvest_json_request(...)` now treats the configured base URL as a provider endpoint, so scripted/simulate/replay modes fail before HTTP instead of silently treating localhost as an unguarded request.
- Added `tests.fake_provider_http.FakeProviderHTTPServer` as the reusable in-process fake HTTP base for route handlers and request recording, plus `tests.fake_apify_provider.FakeApifyProvider` as the Apify-specific actor/dataset/webhook fixture. Connector-level fake HTTP tests now exercise submit -> poll -> dataset download over real `urllib`, verify payload/query shape, prove fake endpoint scripted mode rejects before HTTP, map fake Apify 429/rate-limit responses to `HarvestRetryableRequestError`, and deliver configured webhook terminal events to a local receiver.
- Added `tests/testcontainers_workflow_fake_provider.py` and `make ci-workflow-fake-provider` as the first PG-backed fake-provider workflow gate: disposable PG, real backend API webhook endpoint, real Harvest connector HTTP path to fake Apify, fake Apify terminal webhook delivery, and filesystem object-storage stub evidence.
- Docs updated: `docs/NEXT_TODO.md` now records Phase 0b.1 plus the first PG-backed fake-provider gate, while keeping frontend/browser orchestration and full nightly/pre-release signoff gates open; `docs/TEST_ENVIRONMENT.md` documents the current fake-provider connector/workflow harness.
- Validation passed: `py_compile` for `harvest_connectors.py`, fake-provider helpers, and `tests/test_harvest_connectors.py`; targeted fake-provider/live-guard pytest; full Harvest connector suite (`121 passed`); `ruff check` for touched files; `git diff --check` for touched files.
- Additional validation: default local pytest for Testcontainers harnesses skipped cleanly when not explicitly enabled (`2 skipped`). `make ci-workflow-fake-provider` correctly failed because the local Docker daemon/socket is unavailable; in CI-required mode this is the intended hard failure, not a test pass.
- Remaining risk: this is still not the full production-parity browser/nightly workflow harness. It does not yet replace scripted workflow fixtures or run PG-backed scripted/nightly/browser signoff.

### Canonical projection Phase 5b.2 collection asset overview

- Added collection-first local asset overview APIs: `GET /api/collections` lists active company authoritative projections from `collection_authoritative_pointers + serving_projections`, and `GET /api/collections/{collection_id}/coverage` exposes coverage/shard/readiness metadata without serving candidate rows from legacy jobs or runtime artifacts.
- Updated `/api/collections/{collection_id}/asset-entry` to make acquisition handoff explicit and disabled (`acquisition_handoff.available=false`), matching the product boundary that local asset consumption should not directly launch acquisition workflows. Future acquisition suggestions belong to Agent actions or the separate new-job surface.
- Added frontend `/collections` overview and sidebar entry. The page shows company asset counts, profile/card readiness, coverage status/kind, and index watermarks, then links into the single-company asset entry and canonical projection board. `/collections/:collectionId` no longer shows a direct "new acquisition run" button.
- Docs updated: `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md` now defines the collection overview/coverage API shape and no-acquisition page contract; `docs/NEXT_TODO.md` marks Phase 5b.2 complete.
- Validation passed: backend `py_compile`, targeted collection/projection API/storage tests (`2 passed`), `ruff check` for touched backend/tests, `git diff --check`, and `npm run build` for `frontend-demo`.
- Remaining risk: this is a contract/UI slice, not full PG-backed scripted/nightly validation. Phase 0b fake-provider production-parity harness, Phase 6b/7b/9b.2/10b/11b, and Phase 12 full signoff remain open.

### Canonical projection Phase 7b/9b/10b/12 follow-up slice

- Added projection-scoped raw/evidence keyword-search foundation: new `projection_person_search_index` control-plane table, PG table/primary-key registration, `PersonAssetWriter.rebuild_projection_person_search_index(...)`, fail-closed `/api/projections/{projection_id}/search`, and projection candidate keyword-filter fast path using the index when the filter is keyword-only.
- Added index readiness behavior for zero-result searches: watermarks and `count_scope` still reflect the built index rather than falling back to `unavailable`, so UI/reporting can distinguish “no match” from “index missing.”
- Added person-first CRM list read at `GET /api/crm/records`, sourced from `crm_records` with no target-candidate fallback. This gives the target-candidate page a canonical data source for the next cutover slice.
- Marked legacy target-candidate export as explicit compatibility: `/api/target-candidates/export` now returns legacy/canonical cutover headers, the frontend target-candidate export path surfaces a compatibility notice, and Pre-Manual Signoff can block `legacy_target_candidate_export_normal_path_used` in projection-cutover reports.
- Docs updated: `docs/NEXT_TODO.md`, `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md`, and `docs/PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md` now describe the bounded search-index slice and keep event-time indexer, structured filters, target-page CRM cutover, production migration, and full matrix signoff open.
- Validation passed: backend `py_compile` for touched modules; targeted projection/person-asset/CRM/API/signoff tests (`40 passed`); targeted index/API regression tests (`2 passed`); `ruff check` for touched backend/tests; `npm run build` for `frontend-demo`.
- Remaining risk: full PG-backed scripted/nightly matrix and production-parity containerized fake-provider/object-store/browser gates still have not been run in this pass.

### Canonical projection Phase 5b/6b/7b/9b/10b/11b/12 bounded slices

- Added local asset consumption UI v1: `/api/collections/{collection_id}/asset-entry` returns authoritative projection counts/readiness/index watermarks fail-closed, and `/collections/:collectionId` now renders a read-only local asset entry with explicit new-acquisition handoff instead of redirect-only behavior.
- Added person detail/summary projection APIs: `/api/projections/{projection_id}/persons/{candidate_identity_key}` and `/api/persons/{person_identity_key}` return public-safe `PersonSummaryView`, CRM overlay, assertion/asset/evidence summaries, and redact unreviewed restricted contact values.
- Added Public Web promotion migration v1: `/api/crm/backfill-public-web-promotions` converts legacy promoted Public Web signals into `PersonAssertion` rows and CRM `person_assertion_linked` events. The live target Public Web promotion path now also writes assertion/event records.
- Added projection export v1: `/api/projections/export` exports projection/person summary rows with human-promoted assertion provenance and skip reasons, excludes unreviewed assertions by default, and requires approval for CRM notes. Projection result pages now expose a projection export action.
- Added legacy endpoint retirement switch: `SOURCING_RETIRE_LEGACY_JOB_RESULT_ENDPOINTS=1` makes legacy job-result endpoints return `410` with the linked projection pointer instead of composing normal public reads. `/api/migrations/legacy-result-endpoints` remains the diagnostics endpoint before enabling this in production.
- Tightened scripted signoff projection cutover reporting: clean cutover reports are counted as a passed gate, while missing reports, missing run/projection links, legacy public-reader fallback usage, and legacy endpoint normal-path usage remain blocking.
- Docs updated: `docs/NEXT_TODO.md`, `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `docs/PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md`, and `docs/CRM_STATE_CONTRACT.md` now distinguish completed bounded slices from still-open full raw/evidence index, target-page CRM cutover, production migration, and full scripted/containerized signoff work.
- Validation passed: targeted projection/person-asset/CRM/API/signoff tests (`39 passed`); `ruff check` for touched backend/tests; `npm run build` for `frontend-demo`.
- Remaining risk: full PG-backed scripted/nightly matrix was not run in this pass. Phase 0b production-parity fake-provider/object-store/browser harness, full RawProfileIndex/CandidateEvidenceIndex, target-candidate page CRM cutover, historical production migration, and complete Phase 12 signoff gates remain open.

### Canonical projection Phase 5-7 and 9-11 foundation

- Added local asset entry foundation: `/api/collections/{collection_id}/authoritative-projection` resolves the active `collection_authoritative_pointer` fail-closed, and frontend `/collections/:collectionId` redirects to the canonical `/projections/{projection_id}` result page without launching acquisition.
- Added shared person identity and `PersonSummaryView` foundation in `person_identity.py`; projection member writes now derive `person_identity_key`, `profile_url_key`, and compact public summary fields from the same helper instead of letting readers/frontend infer identity.
- Added person asset/evidence/assertion foundation: `person_assets`, `person_evidence`, `person_assertions`, store APIs, and `PersonAssetWriter`. These are separate from projection rows and CRM state, preserving field-visibility and export-policy boundaries.
- Added person-first CRM foundation: `crm_records`, `crm_engagements`, `crm_events`, idempotent `CRMWriter.add_projection_member_to_crm(...)`, `/api/crm/records`, and read-only projection CRM overlay through `/api/projections/{projection_id}/crm-state`. Projection readers do not auto-create CRM records.
- Added projection export-policy foundation at `/api/projections/{projection_id}/export-policy`, distinguishing default public/raw-structured/human-promoted fields from optional CRM fields, restricted contacts, CRM notes, raw/debug payloads, and approval-required export groups.
- Added legacy target-candidate migration foundation: `CRMTargetCandidateMigrationBackfill` and `/api/crm/backfill-target-candidates` migrate legacy target records into person-first CRM and convert legacy primary emails into review-required `PersonAssertion` rows.
- Added historical migration foundation: `ServingProjectionMigrationBackfill` and `/api/projections/backfill-from-job` can offline-backfill completed legacy job pages into `run_scope_projection` membership and run/projection links without adding normal public-reader fallbacks.
- Updated `docs/NEXT_TODO.md`, `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md`, `docs/CRM_STATE_CONTRACT.md`, and `docs/PERSON_ASSET_EVIDENCE_ASSERTION_CONTRACT.md` to mark completed foundations as Phase 5a/6a/7a/9a/10a/11a and keep remaining UI, index, migration, export, and legacy-retirement work explicit.
- Validation passed: `py_compile` for touched backend modules; `ruff check` for touched backend/tests; targeted projection/person-asset/CRM/API contract tests plus results API projection/CRM subset (`32 passed, 231 deselected`); `npm run build` for `frontend-demo`; `git diff --check` for touched files.

### Canonical Serving Projection Phase 0-8 v1 slice

- Continued the canonical projection architecture implementation through the current Phase 8 target. Event-time run result publication now persists linked `run_scope_projection` metadata in `job_result_views`, Stage 1 row-shell publication serves canonical projection rows before profile/card completion, and completed run projections enqueue a background `collection_authoritative_merge` writer phase.
- Added/finished public reader API v1 behavior for projection result pages. `/api/runs/{run_id}/projection-link`, `/api/projections/{projection_id}`, and `/api/projections/{projection_id}/candidates` fail closed on missing/unservable projections and read only `serving_projection_members` with `read_contract.fallback_used=false`.
- Moved candidate page filter semantics into the shared `public_candidate_facets` contract and wired both legacy job candidate pages and projection candidate pages to it. Projection `/candidates` now returns exact backend membership filtering/paging with `filtered_candidate_count`, `filter_signature`, and `filter_contract.backend_filtered_paging_supported=true`, avoiding frontend full-board hydration for Google-scale projections.
- Tightened serving efficiency in PG mode by adding true PG `OFFSET` support to `select_many(...)` and using it for `serving_projection_members` paging instead of fetching `offset + limit` rows then slicing in Python.
- Continued frontend cutover: `/results?job=...` resolves the run/projection link and navigates to `/projections/{projection_id}`; projection pages hydrate through projection APIs; projection-only pages are explicitly read-only for review, target-candidate, and profile-completion actions until CRM/source-projection writers land.
- Kept the Phase 8 bounded decoupling slice in place: provider actor envelope cap is separate from post-profile durable-unit cap (`HARVEST_PROFILE_PREFETCH_PROVIDER_ENVELOPE_MAX_URLS` vs `HARVEST_PROFILE_PREFETCH_DURABLE_UNIT_MAX_URLS`).
- Docs updated: `docs/CANONICAL_SERVING_PROJECTION_CONTRACT.md` and `docs/NEXT_TODO.md` now distinguish completed v1 slices from remaining migration work: projection facet/search APIs, collection local asset entry, PersonSummaryView/PersonAsset, CRM source-projection writers, historical migration, legacy endpoint retirement, and full containerized/scripted signoff gates.
- Validation passed: Python compile for touched backend modules; `ruff check` for touched backend/tests; targeted projection/storage/frontend tests (`40 passed`); targeted results API projection tests (`6 passed, 242 deselected`); targeted profile-envelope tests (`4 passed, 124 deselected, 7 subtests passed`); `npm run build` for `frontend-demo`.
- Remaining risk: this is architecture/unit/build validation, not full PG-backed scripted matrix or Pre-Manual Signoff. Legacy job result endpoints still exist until Phase 11/12 migration gates retire them, and projection facets/layering remain unavailable until bounded builders/index-backed APIs are implemented.

## 2026-05-14 (Asia/Shanghai)

### Finalization overlay reuse contract

- Added a final/direct asset-population reuse path for the case where the job already has a complete row-shell/board-visible serving projection for the same snapshot. Finalization now records `asset_population_overlay.reuse=true` and avoids rewriting the full board overlay when canonical served/profile/card counters prove completion.
- Added `service_metrics.finalization_overlay` and Pre-Manual Signoff blocking for `eligible_full_rewrite_present=true`, so a pressure run cannot silently pass after doing an O(full board) final overlay rewrite when reuse was available.
- Fixed two adjacent report-contract gaps found by the broader smoke suite: `current_snapshot_serving` runtime publication now counts as board-nonempty/readiness rather than partial board-visible, and the OpenAI scoped-delta matrix now declares a positive out-of-order profile completion gate.
- Validation: `ruff check` for touched Python files, JSON validation for `openai_agent_scoped_delta_smoke_matrix.json`, targeted pipeline/results API overlay tests, and full workflow service/smoke/signoff unit suites passed (`222 passed` for `tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py tests/test_scripted_smoke_signoff.py`).

### Lovable row-shell cumulative board projection fix

- Fixed a public projection regression exposed by `output/pre_manual_pressure_20260514_roster_slot_split_lovable_seq/report.json`: profile fetch was terminal (`140/140`) but the latest row-shell reuse patch only carried the final 20-card worker summary, so public `board_runtime_state` rendered `卡片详情已合入看板 20/140`.
- Row-shell reuse board-visible patches now store both `current_patch_card_materialization_summary` and cumulative `card_materialization_summary`. The latest patch remains a valid event-time projection without rewriting the stable row-shell overlay or carrying large replay payloads.
- Pre-Manual Signoff now blocks terminal profile/card drift through `board_visible_projection_terminal_profile_card_drift`. Replaying the old Lovable report now fails closed with that finding instead of passing strict/signoff silently.
- PG-backed Lovable long-latency rerun passed: runtime `runtime/test_env/nightly_long_latency_20260514_cumulative_card_lovable_seq`, report `output/pre_manual_pressure_20260514_cumulative_card_lovable_seq/report.json`, signoff `output/pre_manual_pressure_20260514_cumulative_card_lovable_seq/signoff.json`. Final public state is coherent across `/progress`, `/dashboard`, `/candidates`, and `/board-patches`: `published=145/145`, `display_ready=140`, `profile=140/140`, `card=140/140`, board-runtime parity clean, `expectation_failures=[]`, profile scheduler violation `false`, `remote_to_next_submit_start_ms.max=4223ms`, `local_completion_to_next_submit_start_ms.max=0ms`.
- Validation: targeted `ruff`, row-shell/board-runtime regression tests, signoff regression tests, old-report fail-closed replay, and the Lovable PG-backed strict smoke plus Pre-Manual Signoff passed. Remaining findings are optimization-only finalization lag, not pressure/report/manual-handoff blockers.

## 2026-05-11 (Asia/Shanghai)

### Post-profile/recovery pressure contract closeout in progress

- Tightened the Patch payload/identity contract: board-visible patch ids remain compact deterministic hashes while large candidate replay payloads stay in JSON payload fields. Added a regression that persists a 2500-candidate replay payload without embedding candidate ids in the indexed patch id.
- Main runner terminal-proof bridge now uses the same post-profile barrier as recovery. A reusable terminal LinkedIn Stage 1 artifact can advance `enrich_linkedin_profiles` only when open `local_apply_closure` / post-profile daemon work is clear; otherwise the workflow remains blocked and does not rerun provider tasks.
- Local-apply backlog recovery now has bounded units: `LOCAL_APPLY_BACKLOG_GROUP_LIMIT` and `LOCAL_APPLY_BACKLOG_PHASE_BUDGET_MS` split large closure groups and leave remaining durable items for later ticks instead of creating one long synchronous recovery phase.
- Pre-Manual Signoff now emits `gate_layers` so manual handoff, pressure, optimization, report-integrity, and contract-integrity findings are separated in JSON/Markdown instead of flattening all latency warnings into one bucket.
- Added `scripts/dev_scripted_openai_no_baseline_scoped_search.sh`, a PG-only manual launcher that starts the OpenAI no-baseline scoped-search environment with `reference_seed_mode=none` instead of reusing the baseline+delta/Lovable launcher.
- Targeted validation passed: script syntax, Python compile, local-apply budget regression, compact patch replay regression, signoff gate-layer tests, and terminal Stage 1 bridge/recovery tests.
- PG-backed Pre-Manual closeout validation passed for the manual/pressure coverage set under `output/pre_manual_20260511_contract_closeout/`. OpenAI baseline+delta, Lovable live-roster, OpenAI no-baseline scoped-search, and Google large-baseline/large-shard pressure all used `provider_mode=scripted`, had `expectation_failures=[]`, clean board-runtime parity, clean profile scheduler reports, and clean post-profile SLO reports. Signoff status was `passed` for all four cases with no blocking or manual-review findings.
- Remaining efficiency notes are optimization-class only: OpenAI baseline+delta had `stage_1_preview_to_final_results=34.07s` against a case SLO of `60s`; Google pressure had `stage_1_preview_to_final_results=126.92s` against a case SLO of `180s`, `remote_to_next_submit_start_ms.max=17.71s`, and one recovery tick `total_elapsed_ms.max=40.27s` while bounded per-phase recovery SLOs remained clean.
- Remote-provider terminal event recovery now has an explicit bounded-burst contract. Production/job-scoped remote-event wakeups default to `WORKFLOW_REMOTE_EVENT_RECOVERY_TOTAL_LIMIT=4`, merge concurrent `remote_provider_event_worker_ids`, and retain overflow worker ids for immediate follow-up ticks; generic/smoke fallback recovery remains small-step until the post-profile finalization barrier is safe under larger fallback bursts.
- Validation after the bounded-burst slice: targeted daemon/sidecar/remote-event tests passed (`56 passed`), `ruff check` and `py_compile` passed for touched recovery modules/tests, OpenAI baseline+delta rerun `output/pre_manual_20260511_openai_delta_burst_limit/` passed strict smoke and Pre-Manual Signoff with `22/22` scripted provider invocations, `stage_1_preview_to_final_results=11.55s`, `job_to_final_results=65.78s`, and no warnings. Google pressure rerun `output/pre_manual_20260511_google_pressure_burst_limit/` passed strict smoke and signoff with `18/18` scripted provider invocations; `stage_1_preview_to_final_results` improved from `126.92s` to `40.21s`, while `job_to_stage_1_preview` remained pressure-class slow at `348.79s`.
- Rejected optimization experiment: raising the smoke blocked-acquisition fallback to the same `4` worker burst (`output/pre_manual_20260511_google_pressure_burst_limit_v2/`) failed strict smoke. It lowered `remote_to_next_submit_start_ms.max` to `2.1s`, but increased provider invocations to `28`, left `delta_profile_board_visible_count=2353/2384`, and produced a queued background `snapshot_full_materialization` after job completion. This confirms fallback burst cannot be enabled until post-profile/finalization waits for complete board-visible or full materialization evidence under pressure.
- Fail-closed SLO follow-up from the rejected experiment: `service_metrics.post_profile_completion.all_profiles_fetched_to_all_cards_visible` now marks an SLO violation when all delta profiles are fetched but board-visible count is still below target, even if no elapsed sample exists. Regression `test_workflow_service_metrics_flags_incomplete_board_visible_after_all_profiles_fetched` covers the missing `2353/2384` class directly.

## 2026-05-10 (Asia/Shanghai)

### OpenAI PG-backed scheduler handoff gate revalidation

- Fixed the latest OpenAI scoped-delta hard efficiency failure from `output/profile_contract_pg_20260510_reserved_budget2/openai_agent_scoped_delta_streaming_report.json`: `event_level_efficiency.local_to_next_submit_start_ms.max=4000ms`. Root cause was slot accounting, not callback work: `planned_dispatch` rows continued to count against profile actor budget after their owner worker had already recorded a terminal remote-provider event, so the next refill waited for later local apply / board-visible drain.
- `MultiSourceEnricher._profile_prefetch_reserved_or_owned_worker_count(...)` now excludes `planned_dispatch` rows whose owner worker is terminal or has `remote_provider_terminal_event_seen_at` / terminal remote-event evidence. This keeps completion callbacks signal-only while freeing actor budget at the correct remote-terminal boundary.
- Added regression coverage in `tests/test_enrichment.py::EnrichmentHelpersTest::test_profile_prefetch_budget_releases_planned_dispatch_when_remote_terminal_seen`: one terminal planned-dispatch owner no longer occupies budget, while an active planned-dispatch owner still does.
- Validation: targeted regression passed; wider scheduler/refill/recovery/efficiency subset passed (`68 passed, 134 deselected, 3 subtests passed`); `ruff check src/sourcing_agent/enrichment.py tests/test_enrichment.py` passed; `py_compile` passed for the same files.
- PG-backed strict OpenAI scoped-delta revalidation passed. Runtime: `runtime/test_env/profile_contract_pg_openai_20260510_reserved_budget3`; report: `output/profile_contract_pg_20260510_reserved_budget3/openai_agent_scoped_delta_streaming_report.json`; signoff: `output/profile_contract_pg_20260510_reserved_budget3/openai_agent_scoped_delta_streaming_signoff.md`. Evidence: `expectation_failures=[]`, `event_level_efficiency.violation_detected=false`, `local_to_next_submit_start_ms.max=0ms`, `remote_to_next_submit_start_ms.max=1243ms`, `next_submit_provider_attempt_elapsed_ms.max=1862ms`, `profile_scheduler_contract.violation_detected=false`, final board-runtime parity `597/597` with delta profiles/cards `297/297`, and Pre-Manual Signoff `passed` with no blocking/manual findings. All `22` provider invocations were scripted.
- Residual risk: this validates the OpenAI baseline+delta scripted backend path after the budget fix. It is still not a live-provider run, and manual scripted browser testing should use the same PG-only isolated runtime/no-live-provider guardrails.

### Job Result Lifecycle Fallback Retirement

- Closed the remaining `job_result_lifecycle` public-read/backfill overlap. Public reads now only render a validated canonical lifecycle row or emit `lifecycle_repair_required`; they no longer repair, synthesize, or overlay lifecycle counters from Stage 1 progress, job summaries, result-view metadata, candidate materialization state, or legacy projection helpers.
- Deleted the retired lifecycle projection helper, stale Stage 1 repair detector, deprecated public-read Stage 1 compute wrapper, and direct-builder test callers. Canonical row regressions now cover stale board-visible counter normalization and assert candidate materialization state cannot backfill lifecycle counters on read.
- Reworked `backfill-job-result-lifecycle` into a fail-closed migration/audit tool. It can migrate serialized canonical evidence from `job_result_views.metadata.result_view_lifecycle` or `jobs.summary.result_view_lifecycle`; jobs without that evidence are reported under `jobs_repair_required` / `jobs_legacy_projection_retired` and are not synthesized into validated rows.
- Updated `docs/NEXT_TODO.md` and `docs/JOB_RESULT_LIFECYCLE_DESIGN.md` so the current contract is explicit: legacy projection is not a public-read path and not a backfill path.
- Validation for this slice is in progress; targeted lifecycle/backfill/CLI tests and then PG-backed scripted Pre-Manual Signoff are the next gates before restarting the local scripted frontend/backend for manual testing.

## 2026-05-09 (Asia/Shanghai)

### Direct profile path retirement and wakeup latency diagnostics

- Retired the remaining foreground Harvest profile connector fallback from non-main company/enrichment maintenance paths. Production `fetch_profiles_by_urls(...)` references now audit down to `src/sourcing_agent/harvest_connectors.py` itself; workflow and maintenance cache misses enqueue scheduler-owned `linkedin_profile_registry` work instead of calling the provider directly.
- Added a durable job-scoped service wakeup contract for remote provider events. After a terminal remote event confirms/starts the job-scoped recovery service, the handler writes a local wakeup request so an idle recovery daemon can begin the next tick without waiting for the full poll interval. The wakeup only interrupts idle sleep; callbacks still do not scan/replan/claim/submit provider work inline.
- `event_level_efficiency` now publishes `remote_to_next_submit_segments_ms` so high `remote_to_next_submit_start_ms` can be attributed to remote event lag, event-seen-to-callback marker lag, callback-marker-to-next-submit lag, event-seen-to-next-submit lag, and provider submit attempt elapsed time instead of being treated as one opaque number.
- Remote-provider wakeup merging is tightened for burst completions. Service wakeup requests now union `explicit_worker_ids` with `remote_provider_event_worker_ids` and raise `total_limit` only to the bounded remote-event burst limit, so a burst of terminal provider events can drain across a few immediate follow-up ticks instead of waiting for a later poll. The remote-event handler also sizes job-scoped recovery from the targeted worker count instead of a fixed low limit.
- Validation: `ruff check` and `py_compile` passed for touched backend/tests; `tests/test_company_asset_completion.py tests/test_enrichment.py tests/test_service_daemon.py tests/test_remote_provider_events.py tests/test_workflow_efficiency.py -q` passed (`205 passed, 3 subtests passed`).
- PG-backed scripted reruns after this slice are complete. Lovable live-roster `rerun71` passed with signoff `passed`, `21/21` provider invocations scripted, `remote_to_next_submit_start_ms.max=640ms`, `local_to_next_submit_start_ms.max=0ms`, provider attempt max `1276ms`, profile scheduler violation `false`, post-profile SLO violation `false`, and board-runtime parity consistent. OpenAI scoped-delta `rerun72` passed with signoff `passed`, `22/22` scripted, final board `597/597`, delta profiles/cards `297/297`, `remote_to_next_submit_start_ms.max=7257ms`, `local_to_next_submit_start_ms.max=0ms`, provider attempt max `1258ms`, scheduler/post-profile/parity clean. OpenAI tiny-tail `rerun73` passed with signoff `passed`, `24/24` scripted, final board `597/597`, `17` profile batch envelopes, no unexplained tiny batch, no provider-slot underuse, `remote_to_next_submit_start_ms.max=4474ms`, `local_to_next_submit_start_ms.max=0ms`, provider attempt max `1809ms`, scheduler/post-profile/parity clean.
- Follow-up PG-backed strict validation closed the remaining no-baseline coverage gap. OpenAI scoped-delta `rerun74` passed with signoff `passed`, `22/22` provider invocations scripted, `expectation_failures=[]`, `remote_to_next_submit_start_ms.max=8181ms`, `local_completion_to_next_submit_start_ms.max=0ms`, and `next_submit_provider_attempt_elapsed_ms.max=1121ms`. New matrix `configs/scripted/openai_no_baseline_scoped_search_smoke_matrix.json` covers pure scoped-search with no reusable baseline; `rerun76` passed with signoff `passed`, `24/24` provider invocations scripted, `expectation_failures=[]`, `remote_to_next_submit_start_ms.max=10528ms`, `local_completion_to_next_submit_start_ms.max=0ms`, `next_submit_provider_attempt_elapsed_ms.max=2171ms`, recovery SLO violation count `0`, and no blocking/manual signoff findings. The remaining OpenAI `remote_to_next_submit_start_ms` tail is now attributed to provider event/marker observation in the diagnostic end-to-end metric, not to local slot refill underuse.
- Recovery resume contract follow-up: hosted/acquiring workflow resume is no longer executed inline inside a recovery tick. `workflow_resume` dispatches a bounded hosted acquisition-resume owner thread and returns structured phase evidence, so recovery/callback phases stay small-step and do not hide full retrieval/finalization work inside a daemon tick.

### PG-backed scripted matrix and Pre-Manual signoff closeout

- PG-backed scripted confidence pass is complete for the current post-profile/recovery/profile-scheduler contract set. All runs used isolated test runtimes, `provider_mode=scripted`, strict smoke gates, contamination audit, and `scripts/review_scripted_smoke_run.py` Pre-Manual Signoff.
- Lovable live-roster `rerun67` passed. Runtime: `runtime/test_env/profile_contract_pg_lovable_rerun67_20260509`; report: `output/profile_contract_pg_20260509/rerun67_lovable_live_roster_report.json`; signoff: `output/profile_contract_pg_20260509/rerun67_lovable_live_roster_signoff.md`. Evidence: `expectation_failures=[]`, `21/21` provider invocations scripted, contamination audit clean, signoff `passed`, profile scheduler violation `false`, `8` profile batch envelopes, no unexplained tiny batch, no provider-slot underuse with backlog, post-terminal recovery settled with `0` remaining workers/items.
- OpenAI Agent scoped-delta `rerun68` passed. Runtime: `runtime/test_env/profile_contract_pg_openai_rerun68_20260509`; report: `output/profile_contract_pg_20260509/rerun68_openai_scoped_delta_report.json`; signoff: `output/profile_contract_pg_20260509/rerun68_openai_scoped_delta_signoff.md`. Evidence: `expectation_failures=[]`, `24/24` provider invocations scripted, contamination audit clean, signoff `passed`, final asset population `597`, local completion to next submit max `0ms`, remote to next submit start max `6774ms`, next submit provider attempt max `1163ms`, `17` profile batch envelopes, profile scheduler violation `false`.
- OpenAI Agent tiny-tail `rerun70` passed after clarifying the handoff SLO contract. Runtime: `runtime/test_env/profile_contract_pg_openai_tiny_tail_rerun70_20260509`; report: `output/profile_contract_pg_20260509/rerun70_openai_tiny_tail_report.json`; signoff: `output/profile_contract_pg_20260509/rerun70_openai_tiny_tail_signoff.md`. Evidence: `expectation_failures=[]`, `24/24` provider invocations scripted, contamination audit clean, signoff `passed`, final asset population `597`, local completion to next submit max `0ms`, remote to next submit start max `5996ms`, next submit provider attempt max `1294ms`, `17` profile batch envelopes, no terminal queue leak, no unexplained tiny batch, no provider-slot underuse with backlog.
- The `rerun69` tiny-tail false failure is resolved by contract clarification, not by weakening the scheduler gate. `remote_to_next_submit_start_ms` remains a diagnostic/soft end-to-end signal until the hard threshold `event_level_efficiency.thresholds_ms.remote_to_next_submit_start_hard` (default `30000ms`). Hard scheduler gates remain `local_completion_to_next_submit_start_ms`, `next_submit_provider_attempt_elapsed_ms`, and `event_level_efficiency.profile_scheduler_contract`.
- The public-read completion promotion boundary is closed: read paths may render results-ready state but must not persist `completed` while a live `workflow_job_lease` exists. Terminal promotion stays owned by the active runner/recovery owner until the lease expires or is proven stale/dead.
- Repository operating rules were updated so the rerun lessons are durable rather than chat-only. The parent `AGENTS.md` now requires production-grade design review dimensions, and this repo's `AGENTS.md` now treats PG-backed isolated scripted smoke, fail-closed report completeness, provider-mode isolation, recovery/callback/daemon ownership, canonical profile scheduler usage, board-runtime source-of-truth behavior, and Pre-Manual Signoff as standing workflow constraints.
- Residual risk before manual browser testing: this is still scripted/browser-backend confidence, not a live provider run. Manual scripted browser testing should use the same PG-only isolated runtime and must keep the no-live-provider gates enabled. Production/ECS legacy lifecycle helper deletion remains open until production/ECS backfill dry-run/apply evidence is reviewed.

Validation added in this closeout:

- `py_compile` passed for `workflow_smoke.py`, `orchestrator.py`, and `workflow_efficiency.py`.
- `ruff check` passed for the touched backend/tests during the SLO and promotion-boundary fixes.
- Targeted tests passed:
  - `tests/test_workflow_smoke.py tests/test_results_api.py -k "settle_board_probe or layering_visible_after_results or promote_results_ready_job or job_progress_promotes_running_workflow"`
  - `tests/test_workflow_smoke.py -k "service_gate_coverage_manifest or scripted_smoke_matrices_enable_service_recovery_hard_gates or settle_board_probe or layering_visible_after_results"`
  - `tests/test_workflow_smoke.py tests/test_workflow_efficiency.py -k "remote_to_next_submit or service_slo_maxima or event_level_efficiency"` (`31 passed`)

## 2026-05-06/07 (Asia/Shanghai)

### Post-profile SLO Gates And Pre-Manual Signoff Review

- Closed the remaining post-profile smoke-gate gap. `service_metrics.post_profile_completion` now reports URL terminal-state leak count, event-level drain callback elapsed time, completed `local_apply_closure -> board_visible_delta_apply` latency, and all-profiles-fetched to all-cards-visible latency. The main provider-backed scripted matrices now gate these through `max_post_profile_url_terminal_state_leak_count`, `max_profile_file_visible_to_board_patch_visible_ms`, `max_all_profiles_fetched_to_all_cards_visible_ms`, and `max_event_level_materialization_callback_elapsed_ms`.
- 2026-05-08 recovery/refill SLO follow-up: `rerun11` proved final OpenAI Agent scripted correctness (`597/597`, scripted provider only) but failed service SLO because `profile_prefetch_refill` spent ~51-59s in the daemon phase. Root cause was a contract propagation gap: `queue_background_profile_prefetch(... load_cached_profile_payloads=False)` used registry-only cache markers at the planner layer, but the worker handoff still called `_hydrate_cached_prefetch_profiles(...)` with payload loading enabled. `load_cached_profile_payloads` now flows into `_execute_harvest_profile_batch_worker(...)`, so daemon refill/next-submit handoff does not parse raw Harvest profile payloads while deciding whether to submit the next actor. Regression `test_profile_prefetch_refill_handoff_does_not_hydrate_cached_payloads` pins the behavior with an invalid cached raw JSON marker. Validation: targeted profile/refill tests passed (`47 passed, 48 deselected, 3 subtests passed`), storage/worker recovery subset passed (`11 passed, 27 deselected`), and ruff passed for touched files.
- Productized the pre-manual scripted signoff review. `scripts/review_scripted_smoke_run.py` reads a smoke report plus optional summary and writes fixed JSON/Markdown sections: `passed_gates`, `manual_review_required_findings`, `known_acceptable_warnings`, and `blocking_findings`. Blocking findings include expectation failures, non-scripted provider invocations, cross-endpoint board-runtime drift, post-profile SLO violations, and non-replayable board-visible projection evidence.
- 2026-05-08 live-model scripted OpenAI smoke exposed that the callback-safe submit change was still too heavy: provider completion callbacks were synchronously re-running registry replan with `submit_provider=false`, producing 39-77s callback elapsed and delayed board-visible patches. The contract is tightened again: completion callbacks only record a lightweight `provider_submit_deferred_to_refill_daemon` signal; registry scan/replan/claim/submit belongs exclusively to the refill daemon.
- 2026-05-08 follow-up exposed and retired the remaining full-roster profile scheduler dual track. `enrich(... full_roster_profile_prefetch=True)` no longer builds its own batch plan, opens a ThreadPool submitter, or calls `_execute_harvest_profile_batch_worker(...)` directly; it now appends into `queue_background_profile_prefetch(...)`, the same registry scheduler used by search-seed and recovery refill. Worker-backed cache-hit completion also hydrates only from registry/snapshot cache and does not call `fetch_profiles_by_urls(...)` synchronously.
- 2026-05-08 Lovable `rerun45` follow-up: strict smoke converged (`120/120`, scripted provider only) but recovery SLO failed because workflow resume reran `enrich_linkedin_profiles` after all profile URLs were terminal. Root cause was checkpoint restoration: `load_candidate_document_state(...)` restored candidates/evidence but ignored `acquisition_stage` / `enrichment_scope`, so `_restore_acquisition_state(...)` did not mark `linkedin_stage_completed`. Candidate docs that carry `acquisition_stage.task_type='enrich_linkedin_profiles'`, `phase='linkedin_stage_1'`, or `enrichment_scope='linkedin_stage_1'` now restore `linkedin_stage_completed` and `linkedin_stage_candidate_doc_path` only when profile-prefetch registry proof is terminal (`all_requested_terminal=true` or equivalent requested-vs-terminal counts); plain docs and queued/open Stage 1 docs do not. Job-scoped recovery callback payloads now default to bounded same-job explicit resume, with opt-out via `workflow_resume_explicit_job=false`, so a sidecar does not leave the final `workflow_open_count=1` to a stale takeover after daemon-owned profile/local-apply/board-visible work is clear. Post-followup workflow resume now runs only when remote-event follow-up or post-followup event-level drain actually produced work, so stale-scope recovery no longer resumes the same queued/acquiring job twice in one tick. The Lovable fixture's first profile batch is now a deterministic slow anchor (`scripted_remote_wait_seconds=20`) so the `out_of_order_profile_completion` gate tests a real out-of-order completion instead of relying on obsolete batch timing. Validation: snapshot/pipeline/recovery-sidecar/service/scripted-provider focused tests passed and ruff passed for touched files; superseded runtime validation is the 2026-05-09 `rerun67` Lovable PG-backed signoff above.
- 2026-05-08 `rerun12` follow-up: final business state converged (`597/597`, delta profiles/cards `297/297`, layering `completed`) but strict service SLO still failed because `profile_prefetch_refill` daemon ticks spent ~52-57s in submit handoff and post-profile board visibility saw a 120s max lag. Root cause identified in the next-submit hot path: every profile actor batch acquired and released URL duplicate-protection leases one URL at a time. The lease contract is now batch-scoped while preserving item-level contention semantics: `ControlPlaneStore.acquire_linkedin_profile_registry_leases(...)` / `release_linkedin_profile_registry_leases(...)` and PG-native equivalents claim/release a whole URL envelope in one short transaction; PG sorts keys and uses transaction-scoped advisory locking to reduce deadlock risk. `_execute_harvest_profile_batch_worker(...)` now defers fresh tiny tails before any lease, uses batch claim/release for normal submit, and only releases URLs actually acquired by this submit owner. The same pass closed a coalescing status drift: `record_linkedin_profile_refill_plan_items(... deferred_queue_state='deferred_coalescing')` now writes lifecycle `status='deferred_coalescing'` like the legacy helper, so scheduler and registry status are not split for tiny-tail waits. Validation: targeted profile/refill subset `48 passed, 48 deselected, 3 subtests passed`; storage/PG lease/scheduler subset `15 passed, 59 deselected`; workflow smoke/signoff subset `12 passed, 106 deselected`; ruff passed for touched backend/tests. Superseded runtime validation is the 2026-05-09 `rerun68`/`rerun70` OpenAI PG-backed signoff above.
- Docs updated: `docs/NEXT_TODO.md`, `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md`, `docs/WORKFLOW_PROGRESS_CONTRACT.md`, and `docs/TESTING_PLAYBOOK.md`. The pending validation note for this slice is superseded by the 2026-05-09 validation and PG-backed signoff evidence above.

### Profile Prefetch Replan Contract v1 Implementation

- Implemented the registry-owned profile prefetch replan contract. `linkedin_profile_registry` remains the only scheduler source, and replan now operates only over unsubmitted scheduler-owned normal URL items for the same `source_job + snapshot_dir`; provider-owned `planned_dispatch`, fetched, unrecoverable, and retry-owned rows are not resized or moved by replan.
- `dispatch_claimed` moved out of plan time. `queue_background_profile_prefetch(...)` records only deferred/coalescing backlog during replan; `_execute_harvest_profile_batch_worker(...)` writes `dispatch_claimed` only after provider limiter/slot acquisition succeeds and the submit path is entering worker/provider submit. Provider limiter backpressure and worker-begin failure return URLs to daemon-drainable `deferred_budget`.
- Append-trigger replan now includes `deferred_coalescing` rows with `ready_only=false`, so near-simultaneous probe/shard appends such as 25+25 can coalesce into a normal 50-URL envelope before the timer. Daemon/timer selectors still respect `refill_not_before_at`; the timer controls sub-50 tail flush only.
- Large-wave sizing now treats `8` as the target envelope count for the current replannable set, not a workflow-wide hard cap or historical budget. Late large shards are sized independently over their unsubmitted URL set, while unsubmitted scheduler-owned rows from a previous append can be resized with newly appended rows.
- Added a short `source_job + snapshot_dir` scheduler critical section. PG uses transaction-scoped `pg_advisory_xact_lock(hashtext('profile_prefetch_scheduler:<source_job>:<snapshot_dir>'))` so abnormal exits release the lock with the transaction. SQLite remains an in-process compatibility lock for unit tests, not production parity.
- Docs updated: `EVENT_LEVEL_WORKFLOW_RESPONSE.md`, `WORKFLOW_PROGRESS_CONTRACT.md`, and `NEXT_TODO.md` now describe replan-window semantics, append-trigger coalescing, dispatch-claim timing, retry isolation, and the TODO to migrate workflow simulation to PG-only control-plane semantics.
- Validation: `py_compile` passed for `enrichment.py`, `storage.py`, `control_plane_live_postgres.py`, and the touched tests; `ruff check` passed for the same Python files/tests; `tests/test_enrichment.py tests/test_storage_profile_registry.py tests/test_control_plane_live_postgres.py -q -k 'profile_prefetch_batch_plan or probe_coalescing or refill_item_state or refills_deferred_budget or retry_wait or deferred_coalescing or dispatch_claim or large_late_shard or profile_prefetch_scheduler_lock or advisory_lock'` -> `18 passed`; wider prefetch/refill/recovery subset `tests/test_enrichment.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py tests/test_workflow_efficiency.py -q -k 'profile_prefetch or refill_queue or dispatch_claim or planned_dispatch or coalescing or retry_wait'` -> `46 passed, 3 subtests passed`; no live provider or PG-backed scripted smoke was run in this slice.

### Post-profile completion contract cleanup

- Contract-only cleanup. No runtime code, state machine, metrics/gates, smoke, or provider-facing test was changed/run in this slice.
- `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` now defines the full post-profile completion state machine: provider terminal evidence, URL item terminal-state recording, next-submit/refill, retry isolation, deterministic `local_apply_closure`, bounded event-level drain, `board_visible_delta_apply`, full snapshot compaction, serving finalization, and post-result layering.
- The contract explicitly defines allowed synchronous work after profile completion and forbidden callback work. Profile completion callbacks may record terminal evidence, trigger registry-only next-submit, enqueue closure, and run bounded `local_apply_closure -> board_visible_delta_apply`; they must not run full snapshot compaction, retrieval/index/export rebuild, layering, public-read repair, or cross-runtime durable work.
- `docs/WORKFLOW_PROGRESS_CONTRACT.md` now maps post-profile backend states to public fields so `/progress`, `/dashboard`, `/candidates`, `/board-patches`, execution pages, and candidate boards should share one event-time projection for profile fetch, local closure, board-visible card publication, serving finalization, and layering.
- `docs/NEXT_TODO.md` now tracks the remaining implementation/gate work under the post-profile completion contract instead of the narrower event-level drain audit item. Next step is backend state machine alignment and SLO gates; do not treat this contract cleanup as implementation completion.

### Profile Retry Wave Contract Closure

- Closed the profile retry timing ambiguity. `retry_wait` dispatch now requires normal-wave closure for the same `source_job + snapshot_dir`: no current-event new URLs, no normal open scheduler rows (`deferred_budget`, `deferred_coalescing`, or `dispatch_claimed`, ready or not-yet-ready), and no first-attempt `planned_dispatch` item still waiting for item-level terminal state.
- `planned_dispatch` is now explicitly provider-owned in-flight state for retry gating, not a ready selector state. The selector can expose it with `ready_only=false` for closure checks, but normal dispatch does not treat it as ready work. Retry-owned remote envelopes are marked separately with `profile_retry_provider_submit` / `retry_remote_provider_submitted`, so they do not reopen the normal wave.
- Partial/mixed Harvest profile batch results are item-level: fetched URLs become `fetched` and leave the scheduler queue; only unresolved/failed URLs enter `retry_wait`. This prevents whole-batch retry, duplicate URL submits, retry URLs perturbing the normal actor-slot plan, and retry dispatch before the last normal slot has finished URL-level status recording.
- Event-level drain remains bounded: completed Harvest profile workers enqueue durable `local_apply_closure` and drain `local_apply_closure -> board_visible_delta_apply` only; full snapshot materialization/retrieval/layering stay on their durable/background owners.
- Validation: `tests/test_enrichment.py tests/test_storage_profile_registry.py tests/test_pipeline.py -q -k "partial_success or retry_wait or retry_gate or dispatch_claim or profile_prefetch_actor_slot or worker_recovery_tick_blocks_retry_wait_until_normal_dispatch_claim_closes"` -> `12 passed, 471 deselected, 3 subtests passed`; exact new retry-closure tests -> `4 passed`; service-loop retry-closure tests -> `2 passed`; `ruff check src/sourcing_agent/enrichment.py tests/test_enrichment.py tests/test_pipeline.py` -> passed.
- No PG-backed smoke or provider-facing test was run in this slice.

### Runtime isolation incident retrospective and durable-work guard

- 2026-05-07 Apify billing incident root cause was reconstructed from artifacts, PG rows, daemon logs, and the Codex session history. The first wrong live cache artifact appeared at `2026-05-07 01:24:26 CST` under `runtime/provider_cache/local_dev/live/...`; the matching queue summary was under `runtime/test_env/board_runtime_pg_closeout_20260507_openai_fix2/...`. PG `public.agent_worker_runs` contained test workers whose checkpoint summary paths pointed into the isolated runtime, while the isolated schema still had the same job blocked. This proves cross-namespace durable work execution, not a webhook delivery problem.
- The code change that opened the incident surface was the `2026-05-07 01:16 CST` smoke/webhook quick-ack change: the driver stopped posting `/api/providers/apify/webhook?sync=1` and started posting `/api/providers/apify/webhook`; at that point the async implementation spawned `provider-webhook-event` daemon threads that ran full `handle_remote_provider_event()` outside the HTTP request. When `openai_fix2` timed out at `01:23:27 CST`, isolated env cleanup did not wait for those threads, so they continued after `patched_environment(...)` restored root/local-dev env. Root recovery then drained test snapshot work as local-dev live provider work.
- Contract cleanup in this pass: `runtime_environment.py` now exposes shared runtime namespace ownership helpers and durable path extraction. Worker recovery, profile refill, search-seed discovery item drain, local-apply closure item drain, board-visible apply item drain, and snapshot-full-materialization item drain now use the same pre-claim ownership contract. Cross-runtime work returns `runtime_namespace_mismatch` / `runtime_namespace_skipped_count` before claim; it does not increment attempts, write retry errors, or submit providers.
- `scripted_test_runtime._join_runtime_threads()` now waits for provider-webhook, job-recovery, shared-recovery, hosted-runtime-watchdog, workflow-runtime-controls, and background materialization/layering thread prefixes before restoring the test environment. `build_subprocess_env(...)` already preserves runtime isolation keys and re-applies provider-isolation overrides for detached sidecars; regression coverage was added.
- Docs updated: `RUNTIME_ENVIRONMENT_ISOLATION.md`, `TEST_ENVIRONMENT.md`, and `EVENT_LEVEL_WORKFLOW_RESPONSE.md` now state that durable drains must enforce runtime ownership before claim, and that quick-ack webhook paths cannot leave runtime-owned background threads running after isolated context exit.
- Validation scope for the initial isolation pass was intentionally unit/static only. The later quarantine/preflight/smoke closeout below is now the current runtime evidence.

### Runtime contamination audit and smoke fail-closed gate

- Added a read-only incident audit entry point: `scripts/audit_runtime_contamination.py`. It scans `runtime/provider_cache/<env>/live/**/*.request.json` for synthetic/scripted fixture provider inputs and, when a PG DSN is available, searches root/public control-plane path/JSON columns for rows pointing into a target `runtime/test_env/<case>` namespace. It does not delete files, mutate PG, start services, or call providers.
- Ran the audit for `runtime/test_env/board_runtime_pg_closeout_20260507_openai_fix2`. Initial local state was contaminated: `197` live-cache request manifests contained OpenAI Agent synthetic LinkedIn URLs, and root/public PG had sampled rows pointing at that test runtime across `jobs`, `agent_worker_runs`, `job_materialization_items`, `linkedin_profile_registry`, and `linkedin_profile_registry_events`. Report: `output/runtime_contamination_audit_current.json`.
- `scripts/run_simulate_smoke_matrix.py --runtime-dir ...` now fails closed before starting an isolated backend if root/local-dev daemon pid/status files are still active or if the contamination audit finds live-cache/PG evidence for the target namespace. It also reruns the same audit after the matrix as a no-cost leak gate.
- That contamination was later quarantined and the PG-backed scripted smoke matrix was rerun after the contamination/leak gates were confirmed green; see the closeout entry below.
- Validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_runtime_contamination_audit.py tests/test_run_simulate_smoke_matrix.py -q` -> `9 passed`; `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/runtime_contamination_audit.py scripts/audit_runtime_contamination.py scripts/run_simulate_smoke_matrix.py tests/test_runtime_contamination_audit.py tests/test_run_simulate_smoke_matrix.py` -> passed; `py_compile` passed for the new audit script/module and the smoke runner.

### Contract closure follow-up before PG smoke rerun

- Dry-run quarantine planning is implemented and remains non-mutating. `scripts/audit_runtime_contamination.py --quarantine-plan` emits exact provider-cache move commands plus sample PG count/quarantine/delete SQL. The reviewed plan for `runtime/test_env/board_runtime_pg_closeout_20260507_openai_fix2` reported `finding_count=1386`, `197` live-cache request manifest moves, and PG findings across `jobs`, `agent_worker_runs`, `job_materialization_items`, `linkedin_profile_registry`, and `linkedin_profile_registry_events`. The approved apply path now copies matching PG rows to quarantine tables, moves contaminated provider-cache request manifests plus same-stem payloads, and then deletes only the active contaminated rows/files.
- Non-live isolated runtime env validation now checks both the generated env payload and the selected runtime env file. Scripted/simulate/replay runs fail if the env file declares live provider mode, a non-isolated runtime environment, a disabled live-provider guard override, or any nonblank live provider secret. Smoke postflight also rejects active root/local-dev daemon pid/status files, and provider invocation summaries fail scripted/simulate cases on missing `provider_mode` or `provider_mode=live`.
- Recovery/sidecar ownership is now tested at both cleanup and process-boundary levels: isolated runtime timeout cleanup joins runtime-owned background threads; smoke postflight rejects orphan daemon processes; root recovery skips nested test-runtime workers; durable local-apply/search-seed/board-visible/snapshot-full drains skip nested test-runtime items before claim.
- Scoped-search and live-roster Stage 1 terminal detection now use one registry shape in `workflow_event_response`. Scoped-search uses `search_seed_discovery_query`; live-roster apply writes `live_roster_discovery_lane`. Denominator promotion requires no non-terminal matching discovery workers plus a same-snapshot terminal durable item; worker apply markers alone no longer promote live-roster denominators.
- Public lifecycle reads no longer call the legacy projection/backfill helper or persist repair rows. Missing or unvalidated `job_result_lifecycle` rows render `state=lifecycle_repair_required` with diagnostic metadata; explicit `backfill-job-result-lifecycle` remains the only owner of the legacy builder until production/ECS backfill evidence is reviewed and the builder can be deleted.
- Lifecycle backfill no longer reads `agent_runtime.list_workers()`; it uses only persisted `agent_worker_runs` Stage 1 evidence. This closes the same class of closed-DB/live-runtime coupling that browser polling exposed on candidate/result reads.
- Validation added in this follow-up: `py_compile` passed for touched Python modules/scripts; `tests/test_job_result_lifecycle_backfill.py tests/test_workflow_event_response.py tests/test_scripted_test_runtime.py tests/test_run_simulate_smoke_matrix.py tests/test_runtime_contamination_audit.py tests/test_scripted_provider_scenario.py -q` -> `59 passed`; `tests/test_results_api.py -q` -> `159 passed` with no runtime-thread warning after teardown cleanup was added; ruff passed for touched modules/tests.

### Runtime quarantine apply and PG-backed scripted smoke closeout

- Applied the reviewed runtime contamination quarantine. First apply moved `394` files (`197` contaminated `*.request.json` manifests plus `197` same-stem provider-cache payloads) from `runtime/provider_cache/local_dev/live/...` into `runtime/quarantine/runtime_contamination/...`, with `0` move failures. It also copied then removed `1189` active public PG contamination rows: `jobs=1`, `agent_worker_runs=223`, `job_materialization_items=202`, `linkedin_profile_registry=243`, and `linkedin_profile_registry_events=520`. Evidence: `output/runtime_contamination_quarantine_apply_current.json`.
- A second strict preflight found `4` additional public PG rows referencing older test runtimes (`jobs=1`, `linkedin_profile_registry_events=3`). Those were copied into quarantine tables and removed from active public schema. Evidence: `output/runtime_contamination_quarantine_plan_second.json` and `output/runtime_contamination_quarantine_apply_second.json`.
- Pre-smoke and post-smoke contamination gates are now clean. Global audit: `output/runtime_contamination_audit_post_lovable_global.json`. Target audits: `output/runtime_contamination_audit_post_openai_target.json` and `output/runtime_contamination_audit_post_lovable_target.json`. Active root/local-dev daemon audit reported `finding_count=0`.
- PG-backed scripted OpenAI Agent scoped-delta smoke passed with strict gates. Runtime: `runtime/test_env/board_runtime_pg_closeout_20260507_codex_openai`; job `6547c50f8538`; report: `output/board_runtime_pg_closeout_20260507_codex/openai_report.json`; summary: `output/board_runtime_pg_closeout_20260507_codex/openai_summary.json`. Key evidence: `progress_contract_violation_case_count=0`, event-level efficiency hard violation `false`, local-apply retryable backlog max `0`, remote provider actionable lag max `2180ms`, remote actor slot peak occupancy `1.0`, board ready by `~8s`, final results to board ready `~610ms`, post-terminal recovery settled with `0` remaining workers/items.
- PG-backed scripted Lovable live-roster smoke passed with strict gates. Runtime: `runtime/test_env/board_runtime_pg_closeout_20260507_codex_lovable`; job `e3bf41575a78`; report: `output/board_runtime_pg_closeout_20260507_codex/lovable_report.json`; summary: `output/board_runtime_pg_closeout_20260507_codex/lovable_summary.json`. Key evidence: `progress_contract_violation_case_count=0`, event-level efficiency hard violation `false`, local-apply retryable backlog max `0`, remote provider actionable lag max `2156ms`, remote actor slot peak occupancy `1.0`, board ready by `~5s`, final results to board ready `~682ms`, post-terminal recovery settled with `0` remaining workers/items.
- Provider invocation leak gate passed for both reports and both runtime JSONL logs: OpenAI had `28` invocations and Lovable had `23`; every invocation had `provider_mode=scripted`, none had missing mode or `provider_mode=live`.
- Log leak gate passed: `rg -n "database is locked|another row available|sqlite3\\.DatabaseError" runtime/test_env/board_runtime_pg_closeout_20260507_codex_openai runtime/test_env/board_runtime_pg_closeout_20260507_codex_lovable` returned no matches.
- Residual diagnostic signal: Lovable had `event_level_efficiency.diagnostic_violation_detected=true` because one materialization sync took `26311.56ms` and post-terminal recovery took about `29.7s`; this did not violate hard gates, but it is the next optimization target if we want tighter local-apply/board-visible latency headroom.

### Lovable diagnostic-tail optimization and incident postmortem

- Completed-job Harvest profile tail now has an explicit `board_visible_profile_delta` contract. If a serving result view / board projection already exists and the recovered work only adds profile detail for known candidate ids, `_inline_incremental_sync_for_running_job(...)` publishes the durable board-visible delta, returns `materialization_contract=board_visible_profile_delta`, and does not call full snapshot normalization/retrieval/layering. `_reconcile_completed_workflow_after_harvest_prefetch(...)` writes `inline_incremental_ingest` from that delta-serving result and leaves full snapshot compaction out of the post-terminal local-apply closure path.
- The former final-tail synchronous full-materialization fallback is retired. Post-profile completion may schedule `snapshot_full_materialization`, but running workflows enqueue it as `waiting_workflow_completion`; workflow completion releases the same durable item to `queued`. If profile completion cannot resolve changed candidate ids, the contract is `snapshot_full_materialization_queued` rather than an inline full sync.
- Added regressions for `board_visible_profile_delta`, `snapshot_full_materialization_queued`, waiting-to-queued release idempotency, pre-retrieval running profile completion, and local-apply coalescing through durable queue ownership. These prove profile completion does not run full snapshot sync/retrieval/layering in the callback path and still consumes workers via `inline_incremental_ingest`.
- Added `docs/archive/APIFY_BILLING_INCIDENT_POSTMORTEM_2026-05-07.md` with the timeline, root cause, evidence chain, user-reported `$15` cost impact, quarantine evidence, and prevention gates. `docs/NEXT_TODO.md`, `docs/WORKFLOW_PROGRESS_CONTRACT.md`, and `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` were updated with the final incident and profile-delta/full-materialization boundary.
- PG-backed scripted smoke rerun confirms the diagnostic tail is gone. Lovable opt run `runtime/test_env/board_runtime_pg_closeout_20260507_codex_lovable_opt`, job `a078320a74f6`, report `output/board_runtime_pg_closeout_20260507_codex/lovable_opt_report.json`: post-terminal recovery `1652.85ms`, materialization sync max `2753.29ms`, event-level diagnostic violation `false`, `23/23` provider invocations `provider_mode=scripted`. OpenAI Agent opt run `runtime/test_env/board_runtime_pg_closeout_20260507_codex_openai_opt`, job `c3bf42513209`, report `output/board_runtime_pg_closeout_20260507_codex/openai_opt_report.json`: post-terminal recovery `2154.37ms`, materialization sync max `60.73ms`, event-level diagnostic violation `false`, `28/28` provider invocations `provider_mode=scripted`. Log leak gate returned no DB-error matches for both opt runtimes/reports.
- Validation after this optimization: `tests/test_pipeline.py -q` -> `376 passed`; targeted completed-harvest/profile-delta tests -> `9 passed`; workflow-smoke materialization/post-terminal tests -> `7 passed`; 2026-05-07 follow-up targeted suites: `tests/test_pipeline.py -k "post_profile or snapshot_full_materialization or local_apply_closure or board_visible_profile_delta or harvest_prefetch"` -> `25 passed`; `tests/test_results_api.py -k "materialization_item or snapshot_full_materialization or board_runtime or lifecycle"` -> `45 passed`; `tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -k "board_visible or local_apply or materialization or snapshot_full"` -> `16 passed`; `ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py` -> passed; `py_compile` passed for the touched Python files.

### Codex contract cleanup and verification follow-up

This entry supersedes the stale handoff bullets below that still list pipeline failures, missing frontend pagination tests, and pending smoke-gate evaluators.

- 2026-05-07 PG smoke preflight found and closed a remaining search-seed prerequisite split-brain: direct Harvest people-search discovery could persist `search_seed_discovery/entries.json` and mark `search_seed_discovery_query` completed without producing the snapshot `candidate_documents.json`, while worker-owned search-seed apply already did produce it. `search_seed_registry.persist_search_seed_snapshot(...)` now projects every non-empty durable search-seed snapshot into `candidate_documents.json`; `SnapshotMaterializer.apply_search_seed_workers_to_snapshot(...)` uses the same projection helper; and `AcquisitionEngine._acquire_search_seed_pool(...)` reawakens `waiting_prerequisite` local-apply items immediately after that writer event. The projection is conservative and preserves existing richer profile/card fields. Regression coverage: `tests/test_seed_discovery.py::test_search_seed_candidate_document_projection_preserves_existing_profile_detail`, `tests/test_pipeline.py::test_acquire_search_seed_pool_queues_profile_prefetch_immediately_for_recovered_entries`, plus the local-apply/search-seed targeted suites below.
- 2026-05-07 recovery scheduling follow-up: `PersistentWorkerRecoveryDaemon` still safely runs selected workers serially for PG shared-store safety, but `AutonomousWorkerDaemon.run(...)` now invokes the completion callback immediately after each selected worker returns in serial mode instead of waiting for the whole selected batch. This closes an event-level head-of-line gap where a slow later worker in the same recovery tick could delay marker writes, local-apply enqueue, and next-submit opportunity for earlier terminal provider events. `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` now makes this callback timing part of the recovery contract.
- 2026-05-07 test-environment isolation follow-up: non-live provider modes now have an executable fail-closed contract (`SOURCING_LIVE_PROVIDER_ACCESS_DISABLED=1`, provider secrets blanked, synthetic fixture input tripwire at live HTTP boundaries). The remaining full-local-reuse smoke failure was root-caused to schema decoding, not missing seed data: seeded OpenAI `candidate_documents.json` had 300 raw candidates, but `snapshot_state.candidate_records_from_payload()` used strict `Candidate(**item)` and silently dropped every record with additive serving/card fields such as `headline`, `experience_lines`, `has_profile_detail`, and `needs_profile_completion`. The durable artifact reader now projects the stable `Candidate` core and preserves unknown fields in metadata; `tests/test_snapshot_state.py` covers this contract, and the full-local-reuse smoke unit plus the two old `test_pipeline.py` regressions pass. The ChatGPT scripted Harvest fixture test was also updated to simulate the terminal provider event checkpoint after remote wait; scripted fixtures with remote-wait rules now follow the event-level provider contract instead of assuming repeated local polling completes the actor.
- 2026-05-07 frontend merge follow-up: `board_runtime_state.rowPublicationSequence` is now the primary frontend merge watermark. A newer sequence overrides cached board runtime state even if the cached state has larger `expectedCandidateCount` / `displayReadyCandidateCount`, preventing old inflated denominators from being preserved by a max-score freshness heuristic. Same-sequence stale endpoint protection remains so a delayed `running` layering mirror cannot overwrite an already observed `completed` facet/layer state.
- Frontend board contract tightened further: `expectedCandidateCount` is the only sync-summary total input; the ambiguous internal `totalCandidateCount` name was removed from `candidateSyncSummary` and from the hydration hook return shape. `publishedCandidateCount` remains mapped for diagnostics/legacy payload display only, not for business sync, totals, freshness, pagination, or hydration targets.
- Candidate results context no longer has an `include_worker_runtime` branch. `_build_job_results_context(...)` now always reads Stage 1 progress from the validated lifecycle row plus persisted job summary context; the unused `_safe_list_job_workers(...)` live-runtime fallback helper was deleted. Public reads and result context cannot be broken by a closed live worker DB.
- Smoke gates that were previously listed as pending are now implemented in `workflow_smoke.py` and registered in `smoke_expectation_contract.py`: stable expected count during Stage 1, stable user-facing profile/card denominator during Stage 1, running recall-bucket filter returning display-ready rows, and layering visible after results. The denominator gate accepts runs whose first poll sample is already promoted, while still rejecting multiple promotions, promotion regressions, or never-promoted terminal samples.
- `tests/test_pipeline.py` now passes in full; the old `candidate_documents.json` setup regressions have been closed. Frontend pagination regression tests exist and pass.

Validation run in this follow-up:

- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q` — `373 passed`.
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_worker_daemon.py tests/test_worker_recovery_daemon.py -q` — `18 passed`.
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py tests/test_workflow_efficiency.py -q` — `39 passed`.
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_local_apply_closure_prerequisite.py tests/test_workflow_service_metrics.py -q` — `27 passed`.
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` — `155 passed, 1 warning` (known teardown/background-thread temp `job_locks` cleanup warning; no assertion failure).
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py -q -k "progress_merge_does_not_regress_complete_board_runtime_contract or board_runtime_newer_sequence"` — `2 passed, 14 deselected`.
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_local_apply_closure_prerequisite.py tests/test_frontend_results_board_pagination.py -q` — `128 passed`.
- `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py tests/test_frontend_candidate_sync_summary.py tests/test_frontend_dashboard_hydration.py tests/test_frontend_results_board_pagination.py -q` — `38 passed`.
- `npm --prefix frontend-demo run build` — passed.
- `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/worker_daemon.py tests/test_worker_daemon.py tests/test_frontend_candidate_filters.py` — passed.
- `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/worker_daemon.py tests/test_worker_daemon.py` — passed.
- `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_smoke.py tests/test_results_api.py tests/test_workflow_smoke.py tests/test_frontend_candidate_sync_summary.py tests/test_frontend_results_board_pagination.py` — passed.

Remaining before declaring the PG manual incident fully closed:

- Optional manual-server sanity: if a scripted backend is left running for browser retest, run cross-endpoint parity (`/progress`, `/dashboard`, `/candidates`, `/board-patches`) against that live server and confirm identical `board_runtime_state` fields. The automated PG-backed matrix already passed strict board-runtime gates in isolated backends.

### PG manual scripted board-runtime closeout (Slices 1–8)

Closeout of `docs/archive/CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md`. All eight implementation slices landed; backend and frontend builds pass.

Backend changes:

- `local_apply_closure` items now move to `waiting_prerequisite` (status + phase + zero-cost retry budget) when the materializer reports `candidate_documents_missing` / `candidate_documents_empty` / `company_identity_missing` — no `last_error` text, `attempt_count` decremented to roll back the speculative claim. The bounded `not_before_at` retry (default 8 s, env override `SOURCING_LOCAL_APPLY_WAITING_PREREQUISITE_DELAY_SECONDS`, hard-capped at 30 s) is a fallback safety net only. Primary recovery is event-level: every `_apply_background_search_seed_workers_to_snapshot` / `_apply_background_company_roster_workers_to_snapshot` / `_apply_background_harvest_prefetch_workers_to_snapshot` wrapper, after a successful apply, calls `_reawaken_waiting_prerequisite_local_apply_closure_items(job_id, snapshot_id)` to clear `not_before_at` and reset `status=queued`. The wrappers fire from all 9+ call sites (pre-retrieval refresh, scoped-search closure, running-daemon ticks, scoped-search apply). PG-native helpers `mark_job_materialization_item_waiting_prerequisite` and `reawaken_waiting_prerequisite_job_materialization_items` are registered in `_CONTROL_PLANE_POSTGRES_NATIVE_TABLES`, so postgres_only mode raises on PG-side failures instead of silently falling back to SQLite shadow.
- Stage 1 lifecycle writer enforces `deduped_candidate_count <= sum(current+former+all)` and the coherent-required clause now correctly enforces `profile_fetch_required <= max(deduped, profile_url_deduped)`. The dynamic Stage 1 builder no longer inflates `deduped_candidate_count` from worker URLs; worker URL count is exposed separately as `metadata.workers.queued_profile_url_count`.
- `expected_candidate_count` and the user-facing `card_materialization_status_text` / `profile_fetch_status_text` `/Y` denominator hold at `max(baseline, served, existing_expected)` while Stage 1 lanes are non-terminal. When `_stage1_lanes_terminal_for_job` returns true, the writer promotes ONCE: baseline+delta workflows go to `baseline + delta_profile_required`, live-roster goes to `max(coherent_required, deduped, served, existing_expected)`. The lifecycle metadata flag `delta_profile_denominator_promoted` records the promotion. Status copy omits the `/Y` denominator and surfaces `Stage 1 仍在发现候选人` via `card_materialization_status_detail` / `profile_fetch_status_detail` until promoted. `mark_job_result_lifecycle_terminal` includes a safety-net promotion for jobs that reached terminal without an explicit lane-terminal signal.
- Stage 1 terminal detection is registry-driven for both live-roster and baseline+delta/scoped-search. Live-roster apply writes `job_materialization_items(item_kind='live_roster_discovery_lane')`; scoped-search uses `search_seed_discovery_query`. Promotion requires no non-terminal matching discovery workers and at least one same-snapshot terminal durable item; worker apply markers are audit evidence only.
- `_build_board_runtime_state` no longer lifts `expected_count` from dynamic Stage 1 progress (`stage_profile_required_count` / `stage_discovered_count`) — the lifecycle row is canonical, preventing the OpenAI `300/351 → 300/425 → 300/525` mid-run drift.
- Live-roster shell row gate: `_publish_partial_board_visible_current_snapshot_overlay` and `_publish_partial_board_visible_delta_overlay` now defer publication only when ALL three counters (`display_ready`, `needs_profile_completion`, `low_profile_richness`) are zero (pure shell). Patches with at least one counter > 0 publish so `published_candidate_count` advances and discovery is visible. Materializer `apply_company_roster_workers_to_snapshot` skips writing `candidate_documents.json` for empty-roster overlays unless the file already exists.
- `service_metrics.local_apply_backlog` exposes `waiting_prerequisite_count`, `waiting_prerequisite_age_max_ms`, and `waiting_prerequisite_age_mean_ms`. `closure_backlog_count` includes `waiting_prerequisite` rows but `closure_retryable_count` does not.

Frontend changes:

- `ResultsBoardPanel.tsx` preserves last-known filtered candidate count + total pages while a backend page request is pending (`waitingForBackendPage=true`); `currentPage > totalPages` reset effect ignores the comparison while waiting; filter-signature changes clear last-known state. Pagination no longer collapses to page 1 during pending backend page requests.
- `SearchPage.tsx` adds an effect keyed on `dashboard.boardRuntimeState?.layeringStatus || dashboard.resultViewLifecycle?.outreachLayeringStatus`. After results are rendered, when layering flips to `completed`, exactly one `getDashboard(jobId, { forceRefresh: true })` fires — no more `分层未生成` until manual refresh. A per-job ref guards against repeat firing.
- `HarvestProfileSearchConnector.search_profiles` now serializes identical payloads on a payload-level inflight lane keyed by `discovery_dir + request payload`, so duplicate `harvest_profile_search` dispatches cannot race across probe/final callers. Validation: Lovable smoke `output/board_runtime_pg_closeout_codex4_20260506/lovable_report.json` and OpenAI smoke `output/board_runtime_pg_closeout_codex4_20260506_openai/openai_report.json` both passed with `duplicate_signature_count=0`.
- `frontend-demo/src/lib/dashboardHydration.ts`, `frontend-demo/src/lib/api.ts`, and `frontend-demo/src/hooks/useDashboardCandidateHydration.ts` now keep `boardRuntimeState.expectedCandidateCount` as the canonical business total. `publishedCandidateCount` / `rowHydrationTargetCount` remain hydration watermarks only and no longer inflate `totalCandidates` or run-status candidate totals when board runtime state is present.

Smoke matrix gates and docs:

- Gate `require_no_local_apply_candidate_documents_retry_storm=true` added to `configs/scripted/openai_agent_scoped_delta_smoke_matrix.json` (case `openai_agent_scoped_delta_streaming`) and `configs/scripted/small_company_live_roster_smoke_matrix.json`. The evaluator in `workflow_smoke.py` checks both `closure_error_samples` for `candidate_documents_missing` text and `waiting_prerequisite_age_max_ms <= 30000`.
- Manual symptom gates are also implemented: `require_stable_expected_candidate_count_during_stage1`, `require_stable_user_facing_profile_card_denominator_during_stage1`, `require_filter_returns_running_card_ready_recall_buckets`, and `require_layering_visible_after_results_within_slo`.
- `docs/WORKFLOW_PROGRESS_CONTRACT.md` documents: one-shot denominator promotion gated on Stage 1 lanes terminal; user-facing status text omits `/Y` until promoted; live-roster pure shell row deferral; `local_apply_closure` `waiting_prerequisite` event-level reawakening contract.
- `docs/EVENT_LEVEL_WORKFLOW_RESPONSE.md` adds the `waiting_prerequisite` state machine description, prerequisite-writer event reawakening contract, env override + 30 s hard cap, and a future-rule that any new helper writing `candidate_documents.json` outside the three apply wrappers MUST also fire the reawaken event.

Validation:

- Python syntax check passed for `orchestrator.py`, `storage.py`, `control_plane_live_postgres.py`, `snapshot_materializer.py`, `workflow_service_metrics.py`, `workflow_smoke.py`.
- `ruff check` passed for all six backend files.
- Frontend `npm run build` passed (TypeScript + Vite).
- Targeted pytest results:
  - `tests/test_results_api.py` — 145 pass (4 stale tests updated to track the new contract; one NameError in `mark_job_result_lifecycle_terminal` introduced and immediately fixed by initializing the `fields` dict before the safety-net promotion block).
  - `tests/test_workflow_smoke.py` — 240 pass (after registering `require_no_local_apply_candidate_documents_retry_storm` in `smoke_expectation_contract.py:SUPPORTED_SMOKE_EXPECTATION_KEYS`).
  - `tests/test_workflow_service_metrics.py` — 22 pass.
  - `tests/test_storage_job_guard.py`, `tests/test_storage_surface_guardrails.py` — 4 pass.
  - `tests/test_frontend_candidate_sync_summary.py`, `tests/test_frontend_dashboard_hydration.py` — pass.
- Historical handoff note: the earlier open-test list and pending smoke-gate list are now superseded by the Codex contract-cleanup entry above. `tests/test_pipeline.py` passes in full, `tests/test_local_apply_closure_prerequisite.py` and `tests/test_frontend_results_board_pagination.py` exist, and the manual symptom smoke gates are implemented in `workflow_smoke.py`.
- PG-backed scripted smoke matrix run is the remaining must-pass runtime gate before manual retest. Matrix files and evaluators are in place; `scripts/run_simulate_smoke_matrix.py` invocations for OpenAI Agent scoped delta and Lovable live roster remain to run.
- Cross-endpoint parity manual curl sanity check is still pending and requires a running scripted backend.

Remaining issues / improvements beyond Slice 8:

- **Test setup hygiene**: tests that exercise `_apply_background_harvest_prefetch_workers_to_snapshot` (or the closure path that reaches it) must seed `candidate_documents.json` in setup, or the path returns `waiting_prerequisite` per the new contract. This is now covered by the passing pipeline suite, but future tests should follow the same rule.
- **Post-results layering refresh second pass**: the new `useEffect` in `SearchPage.tsx` fires once per job per transition, guarded by a `Set` ref. If a job legitimately re-enters layering after a manual user action that resets state, the ref needs to be cleared. The current implementation removes the job from the ref on fetch failure (so it can retry) but not on subsequent legitimate re-runs of the same job — acceptable for this incident, but worth tracking.
- **Legacy lifecycle helper deletion**: closed 2026-05-10. Public reads and backfill no longer have a synthetic lifecycle projection path; historical rows without serialized canonical evidence are repair-required.
- **Reawaken hook future contract**: any new helper that writes `candidate_documents.json` outside the three `_apply_background_*_workers_to_snapshot` wrappers MUST also call `_reawaken_waiting_prerequisite_local_apply_closure_items`. Documented in `EVENT_LEVEL_WORKFLOW_RESPONSE.md`. The 8 s fallback timer recovers items if the contract is violated, with up to 8 s latency.
- **PG vs SQLite parity for new helpers**: both stores have `mark_job_materialization_item_waiting_prerequisite` and `reawaken_waiting_prerequisite_job_materialization_items`, registered in `_CONTROL_PLANE_POSTGRES_NATIVE_TABLES` so postgres_only mode raises strict failures instead of silently falling back. There is no separate test exercising both backends side-by-side; smoke runs will exercise PG, unit tests will exercise SQLite, but a parity test could be added.
- **Env override scope**: `SOURCING_LOCAL_APPLY_WAITING_PREREQUISITE_DELAY_SECONDS` is read on each call (not cached). Hard-capped at 30 s by both the orchestrator resolver AND the storage helpers (defense in depth). Default 8 s.

## 2026-05-06 (Asia/Shanghai)

### Claude handoff for PG manual scripted board-runtime failures

- Added `docs/archive/CLAUDE_CODE_BOARD_RUNTIME_ORCHESTRATION_HANDOFF_2026-05-06.md` as the focused Claude Code handoff for the current candidate-board streaming / local-apply orchestration incident.
- The handoff records:
  - PG schema and job anchors: `sourcing_scripted_openai_agent_delta`, OpenAI `55738bb72427`, Lovable `ae5e2a1487a2`.
  - User-visible failures: unstable OpenAI denominators, impossible Stage 1 math, card-ready `0 -> final` jumps, Lovable placeholder -> final jump, pagination reset, and delayed layering visibility after `results`.
  - Root-cause framing: provider events did arrive, but `local_apply_closure -> candidate_documents prerequisite -> board-visible serving projection` is not unlocked event-by-event; `candidate_documents_missing` should be treated as a deferred prerequisite state rather than a retry storm.
  - Verification SQL for `job_result_lifecycle`, `job_board_visible_patches`, `job_materialization_items`, and `agent_worker_runs`.
  - Backend/frontend review entrypoints and regression gates Claude should add before manual/live retest.
- Updated `docs/INDEX.md` and `docs/NEXT_TODO.md` to point future sessions to the focused handoff first.
- Validation:
  - Documentation-only update so far; implementation remains pending.

### Manual scripted runtime event-closure and storage convergence

- Historical failure reviewed before implementation:
  - Manual scripted OpenAI Agent stalled at `545/597` for minutes even though scripted actor waits were only seconds. Root cause was not provider latency; the manual launcher did not enable the scripted local provider event watcher, so ready remote batches closed later through progress/recovery.
  - Browser polling plus worker daemon plus progress auto takeover wrote to disk SQLite in manual scripted runtime, producing `another row available` / `database is locked` symptoms that PG-only production architecture should not tolerate.
  - `results` could render before fresh dashboard/facet hydration because the frontend completed branch first rendered a cached dashboard merged with `/progress`, which can lack completed layer/facet summary.
- Implemented:
  - `dev_scripted_openai_agent_delta.sh` now defaults to PG-backed manual scripted control plane with dedicated schema `sourcing_scripted_openai_agent_delta`; `--reset-runtime` safely resets both file runtime and the PG schema. Later 2026-05-07 closeout removed the old `--sqlite-control-plane` diagnostic path entirely.
  - Manual scripted launcher enables `SOURCING_SCRIPTED_LOCAL_PROVIDER_EVENT_WATCH_ENABLED=1` and a 180s local provider watcher window. Harvest profile batch watcher scheduling now uses the remote `run_id/dataset_id` resolved after submit/artifact write, not stale pre-submit checkpoint fields.
  - `/progress` auto takeover now observes fresh remote waits instead of treating them as recovery work. It only recovers once a terminal provider event is checkpointed, a worker lease is dead/expired, or `WORKFLOW_PROGRESS_REMOTE_WAIT_TAKEOVER_AFTER_SECONDS` expires.
  - SearchPage completed polling now forces fresh dashboard/candidate hydration before rendering final results; stale cached/fallback dashboards are not rendered as final completion.
- Validation:
  - `.venv/bin/python -m pytest tests/test_enrichment.py::EnrichmentHelpersTest::test_scripted_local_provider_event_watcher_uses_artifact_remote_identifiers tests/test_pipeline.py::PipelineTest::test_get_job_progress_observes_fresh_remote_wait_instead_of_progress_takeover tests/test_pipeline.py::PipelineTest::test_get_job_progress_takeover_allows_remote_wait_after_terminal_event tests/test_frontend_dashboard_hydration.py tests/test_scripted_test_runtime.py::ScriptedTestRuntimeTest::test_manual_scripted_launcher_defaults_to_pg_control_plane_and_event_watcher -q` -> `11 passed`.
  - `bash ./scripts/dev_scripted_openai_agent_delta.sh --print-config` -> confirms `control_plane=postgres`, `postgres_schema=sourcing_scripted_openai_agent_delta`, `scripted_local_provider_event_watcher=enabled`.
  - `SCRIPTED_POSTGRES_SCHEMA=sourcing_scripted_bootstrap_check bash ./scripts/dev_scripted_openai_agent_delta.sh --runtime-dir runtime/test_env/scripted_pg_bootstrap_check --seed-only --fast-runtime` -> seeded OpenAI baseline and Lovable identity into a dedicated PG-backed scripted runtime.
  - `.venv/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/verify_event_closure_openai_agent --provider-mode scripted --scripted-scenario configs/scripted/openai_agent_scoped_delta_streaming.json --seed-reference-runtime --fast-runtime --matrix-file configs/scripted/openai_agent_scoped_delta_smoke_matrix.json --case openai_agent_scoped_delta_streaming --strict --poll-seconds 0.5 --max-poll-seconds 180 --report-json runtime/test_env/verify_event_closure_openai_agent/service_logs/report.json --summary-json runtime/test_env/verify_event_closure_openai_agent/service_logs/summary.json` -> passed; remote provider event lag max `2320ms`, no remote-event lag violation, no event-level hard violation, terminal board served `597/597`.
  - `rg -n "database is locked|another row available|sqlite3\\.DatabaseError" runtime/test_env/verify_event_closure_openai_agent runtime/test_env/scripted_pg_bootstrap_check` -> no matches.
  - `bash -n scripts/dev_scripted_openai_agent_delta.sh scripts/dev_backend.sh` -> passed.
  - `.venv/bin/python -m pytest tests/test_enrichment.py -q -k "local_provider_event_watcher or scripted_local_provider_event"` -> `5 passed, 66 deselected`.
  - `.venv/bin/python -m pytest tests/test_pipeline.py -q -k "progress_auto or remote_wait or job_progress or runtime_health_treats_remote_wait"` -> `20 passed, 352 deselected`.
  - `.venv/bin/python -m pytest tests/test_scripted_test_runtime.py tests/test_frontend_dashboard_hydration.py -q` -> `16 passed`.
  - `.venv/bin/python -m ruff check scripts/seed_reference_smoke_runtime.py src/sourcing_agent/enrichment.py src/sourcing_agent/orchestrator.py tests/test_enrichment.py tests/test_pipeline.py tests/test_frontend_dashboard_hydration.py tests/test_scripted_test_runtime.py` -> passed.
  - `npm --prefix frontend-demo run build` -> passed.
- Remaining risk:
  - Full manual browser validation still needs to be rerun after restarting the scripted frontend/backend. The remaining production transport improvement is still SSE/patch-log polling hardening for very fast patch sequences, but ordinary provider-ready closure should no longer wait for progress recovery.

### Candidate board runtime freshness and patch-log polling closeout

- Historical failure reviewed before implementation:
  - `/progress` could still emit older `post_result_layering/running` board state after `/dashboard` or `/candidates` had a complete `current_snapshot_serving` board with full-population facets/layers.
  - The board header/filter layer could still be tempted to explain canonical counts from local candidate-page rows, while quick board-visible patches could land between normal progress polls and be missed by the browser.
  - Patch-log polling needed a true cursor; timestamp-only polling can skip patches that share the same `published_at` but have a higher `sequence_index`.
- Implemented:
  - `_build_board_runtime_state(...)` now treats a complete `global_full_population` facet summary with layer counts as canonical layering completion, so stale `running/post_result_layering` mirrors cannot regress the board phase.
  - Added `GET /api/jobs/{job_id}/board-patches` as a light board-visible patch-log endpoint. It returns ordered patch deltas, canonical `board_runtime_state`, `result_view_lifecycle`, and card-quality counters without shipping candidate rows.
  - Frontend polling now consumes `/board-patches` with `latest_sequence_index` as the primary cursor and `latest_published_at` as compatibility context, then force-refreshes dashboard data when durable patches make rows/cards visible.
  - Candidate-board header copy distinguishes canonical business sync from the local loaded row window; local rows only describe the current hydrated page/window until backend-filtered paging exists.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py::ResultsApiTest::test_board_runtime_complete_facets_outrank_stale_post_result_layering tests/test_results_api.py::ResultsApiTest::test_job_board_visible_patch_log_api_exposes_card_quality_and_sequence_cursor tests/test_results_api.py::ResultsApiTest::test_job_dashboard_is_summary_only_and_candidate_page_paginates_asset_population -q` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py::FrontendCandidateFiltersTest::test_progress_merge_does_not_regress_complete_board_runtime_contract tests/test_frontend_candidate_filters.py::FrontendCandidateFiltersTest::test_partial_hydration_filter_empty_state_is_not_final_empty_result tests/test_frontend_candidate_filters.py::FrontendCandidateFiltersTest::test_board_patch_polling_uses_sequence_cursor_and_maps_card_quality_counts -q` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "board_runtime or board_visible_patch_log or job_dashboard_is_summary_only"` -> `5 passed, 137 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py -q` -> `10 passed`; `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py tests/test_frontend_dashboard_hydration.py -q` -> `15 passed`.
  - OpenAI Agent scoped-delta scripted smoke on `runtime/test_env/openai_agent_scoped_delta_board_contract_20260506` -> passed; terminal lifecycle served `597/597`, delta fetched/materialized/board-visible `297/297`, `outreach_layering_status=completed`, no progress/lifecycle contract violation.
  - Lovable live-roster scripted smoke on `runtime/test_env/lovable_live_roster_board_contract_20260506` -> passed; terminal lifecycle served `140/140`, profile fetched `140/140`, `outreach_layering_status=completed`, no recovery/contract violation.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py tests/test_results_api.py tests/test_frontend_candidate_filters.py` -> passed.
  - `npm --prefix frontend-demo run build` -> passed.
- Remaining risk:
  - `/candidates` still uses offset paging and frontend-side filtering over the loaded window. The final root fix remains backend-filtered paging, followed by SSE/patch-stream transport if polling cadence is still too coarse for production UX.

### Candidate board card-readiness contract convergence

- Historical failure reviewed before implementation:
  - The candidate board still had multiple state sources: durable row publication, candidate-page hydration, and profile/card materialization were being merged in frontend copy.
  - Basic roster/search rows could be counted as `候选人同步` / `已物化到看板` even when LinkedIn profile detail had not been fetched or merged.
  - `/progress` did not carry asset-population card-quality summary, so it could treat `published_candidate_count` as a usable-card count and warm the board too early.
- Implemented:
  - `board_runtime_state` now distinguishes `published_candidate_count` (readable row publication), `display_ready_candidate_count` (usable card sync numerator), `preview_candidate_count` (shell/discovery rows), profile-detail counts, and low-quality/profile-completion counts.
  - `/progress` board runtime can load card-readiness from the current result view / board-visible overlay when no asset_population summary is passed, keeping `/progress`, `/dashboard`, and `/candidates` aligned.
  - Frontend sync formatting now separates `profile_fetch_status_text` from `card_materialization_status_text`; provider fetch progress no longer embeds “已物化到看板”.
  - Results renderability now uses `displayReadyCandidateCount` when board runtime exists. Stage 1 preview rows remain legacy-only and do not open the main board for modern workflows.
  - ResultsBoardPanel audit filtering now uses backend/manual review state plus frontend auto profile-completion/low-richness status, so preview-only rows are excluded from the default board but still available through audit filters.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py -q` -> `8 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_dashboard_hydration.py -q` -> `6 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py -q` -> `7 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "live_roster_first_run_publishes_partial_board_runtime_without_baseline"` -> `1 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "board_runtime or profile_fetch_status or materialized_candidate or needs_profile_completion"` -> `1 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py tests/test_frontend_dashboard_hydration.py tests/test_frontend_candidate_filters.py -q` -> `21 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `140 passed`, with one pre-existing background-thread cleanup warning from temporary `job_locks`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "board_visible or local_apply_closure or completed_workflow_harvest_reconcile"` -> `13 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py tests/test_frontend_candidate_sync_summary.py` -> passed.
  - `cd frontend-demo && npm run build` -> passed.
- Remaining risk:
  - `/candidates` still pages by offset and filters client-side over loaded rows. Backend-filtered paging / patch-log or SSE progress transport remains the next root fix for “selected filter row exists outside loaded slice” and fast patch updates that complete between polls.

## 2026-05-05 (Asia/Shanghai)

### Completed served-population contract hardening for scripted manual retest

- Historical failure reviewed before implementation:
  - OpenAI Agent completed into `results` while outreach layering stayed behind background snapshot materialization, so the board could show canonical candidates before layer facets became available.
  - Running-stage OpenAI board-visible rows were not filterable by global employment/location/function facets until terminal `results`, even after backend lifecycle had already served the full baseline+delta population.
  - Lovable full-roster second runs could still show `Baseline 复用` because historical authoritative serving rows lacked explicit `population_coverage` even when a `company_employees` full-roster shard proved complete coverage.
- Implemented:
  - Completed-workflow reconcile now prioritizes outreach layering before deferred snapshot materialization, while preserving worker-output reconcile before outreach when pending workers exist.
  - ResultsBoardPanel now accepts backend `global_full_population` facet summaries for the fully served board-visible population, not only after terminal snapshot materialization. It still refuses partial page-window facets and requires lifecycle served/expected plus delta board-visible/materialized counts to converge.
  - `ensure_organization_execution_profile(...)` now repairs and persists explicit full-roster `population_coverage` onto the selected authoritative registry row before building planner semantics. Normal reads remain strict; the repair converts write-time/shard proof into an auditable contract instead of reintroducing hidden inference.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest tests/test_organization_execution_profile.py -q -k "full_roster_coverage_to_authoritative_serving_row or scoped_only_authoritative_asset"` -> `2 passed`.
  - `PYTHONPATH=src .venv-tests/bin/pytest tests/test_pipeline.py -q -k "prioritizes_outreach_layering_before_snapshot_materialization or reconcile_completed_workflow_after_deferred_outreach_layering"` -> `2 passed`.
  - `PYTHONPATH=src .venv-tests/bin/pytest tests/test_frontend_candidate_filters.py -q -k "canonical_facet_summary_for_global_filters or global_facet_summary_page_merge_replaces_stale_partial_summary or function_filter_all_summary"` -> `3 passed`.
  - `PYTHONPATH=src .venv-tests/bin/pytest tests/test_organization_execution_profile.py tests/test_execution_semantics.py tests/test_frontend_candidate_filters.py tests/test_frontend_dashboard_hydration.py -q` -> `33 passed`.
  - `PYTHONPATH=src .venv-tests/bin/pytest tests/test_pipeline.py -q -k "outreach_layering or background_snapshot_materialization"` -> `9 passed`.
  - `PYTHONPATH=src .venv-tests/bin/ruff check src/sourcing_agent/organization_execution_profile.py src/sourcing_agent/orchestrator.py tests/test_organization_execution_profile.py tests/test_pipeline.py tests/test_frontend_candidate_filters.py` -> passed.
  - `npm --prefix frontend-demo run build` -> passed.
  - Scripted OpenAI Agent scoped-delta smoke on `runtime/test_env/verify_openai_contract_20260505b` -> passed; terminal lifecycle served `597/597`, delta materialized/board-visible `297/297`, `outreach_layering_status=completed`, no serving gap.
  - Scripted Lovable live-roster smoke on `runtime/test_env/verify_lovable_contract_20260505` -> passed; terminal lifecycle served `140/140`, `outreach_layering_status=completed`, no serving gap.
  - Second-run Lovable explain against the same scripted runtime -> `effective_acquisition_mode=full_local_asset_reuse`, `execution_strategy_label=全量本地资产复用`, `coverage_kind=full_company_roster`, `requires_delta_acquisition=false`.
- Remaining risk:
  - The frontend still observes progress through polling. If board-visible chunk writes complete between polls, the UI may jump directly to the final count; final production transport remains SSE or explicit patch-log polling from backend durable events.

### Completed harvest final-tail rich-detail replay hardening

- Historical failure reviewed before implementation:
  - Scripted OpenAI Agent runs could finish with many current candidates still stuck as `search_seed_preview` / `信息不完整待补全`, while the same snapshot already had fetched Harvest profile raws on disk.
  - The completed-workflow harvest reconcile path only merged the current worker batch's requested profile URLs. If earlier fetched profiles had already been cached on the snapshot but were no longer part of the last worker batch, the final served `candidate_documents` and normalized artifacts could miss their work/education detail.
- Implemented:
  - `SnapshotMaterializer.apply_harvest_profile_workers_to_snapshot(...)` now supports final-tail `include_snapshot_cached_profile_urls=True`, which unions the current worker's URLs with all candidate LinkedIn URLs already present on the snapshot and replays any cached Harvest profile detail into canonical `candidate_documents`.
  - The apply loop now iterates the fully reconciled URL set rather than only the active worker's `requested_urls`, so snapshot-cached profile raws are actually merged instead of just being counted.
  - Completed harvest reconcile now enables this full snapshot cached-profile replay on the final no-blocker path, including the case where the active worker already had an `inline_incremental_apply` marker and only the final materialization/retrieval closure remained.
  - Reconcile metrics now distinguish worker-requested URLs from the broader snapshot replay set through `reconcile_profile_url_count`, `snapshot_cached_profile_url_count`, and `full_snapshot_cached_profile_reconcile`.
- Validation:
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_harvest_reconcile_replays_snapshot_cached_profiles_before_final_sync or completed_workflow_harvest_reconcile_marks_worker_consumed_and_is_idempotent or local_apply_closure_queue_coalesces_same_snapshot_workers'` -> `3 passed`.
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_pipeline.py -k 'completed_workflow_harvest_reconcile or local_apply_closure_queue_coalesces_same_snapshot_workers'` -> `6 passed`.
  - `PYTHONPATH=src .venv-tests/bin/pytest -q tests/test_results_api.py -k 'job_results_asset_population_marks_low_profile_richness_without_profile_completion_gap or job_results_asset_population_search_seed_preview_requires_profile_completion or job_results_asset_population_sparse_harvest_profile_detail_requires_profile_completion or job_results_recover_profile_top_education_from_queue_dataset_items_without_registry'` -> `4 passed`.
- Remaining risk:
  - This closes the backend canonical write gap for final-tail snapshot detail replay, but frontend progress transport still relies on polling windows. When chunk writes complete faster than page polls, the long-term answer remains SSE or patch-log polling from backend durable events, not frontend inference from hydration state.

## 2026-05-04 (Asia/Shanghai)

### Scripted manual E2E progress semantics hardening

- Historical failure reviewed before implementation:
  - Manual Lovable/OpenAI scripted runs showed the results board mixing canonical business progress with frontend candidate-page hydration windows. The UI could show `597/597` while filters and notes still reflected partial chunks such as 24/96 loaded candidates, causing apparent progress jumps and empty/unstable board states.
  - Lovable live-roster publication produced a valid `company_employees` lane and authoritative row, but no explicit `population_coverage`, so the next "all members" query could be planned as Baseline reuse + delta instead of full local reuse.
- Implemented:
  - `candidateSyncSummary` now uses canonical `result_view_lifecycle.served/expected` for the main candidate sync count. Frontend page hydration remains a secondary loading message and no longer drives the business progress numerator.
  - Added a shared `dashboardHydration` frontend contract. Results page loading, search-flow bootstrapping, candidate-page hydration, and dashboard first-page backfill now use the same lifecycle-aware expected count and require real candidate rows before rendering the board.
  - Candidate-board empty states are hydration-aware. Partial chunks now say they are only loaded fragments, so `0/24`, `0/96`, or filtered partial windows are not presented as final empty results.
  - Intent-match chips are suppressed until the frontend has hydrated the full served population, avoiding partial-filter notes such as `Agent 48` before all candidates are loaded.
  - Dashboard auto-refresh now treats `result_view_lifecycle.outreach_layering_status` and candidate render-signature changes as meaningful, so completed layering/facet updates can appear without a manual browser refresh.
  - `build_population_coverage_contract(...)` now lets write-time/backfill paths convert summary-level `company_employees` lane coverage into explicit `full_company_roster` coverage. Normal reads still suppress legacy inference unless that proof has been persisted.
  - 2026-05-04 follow-up: full `current_snapshot_serving` publication now normalizes stale partial board-visible delta counters to the required count, so OpenAI Agent-style `served=597` cannot coexist publicly with `board_visible=272/297` pending state.
  - 2026-05-04 follow-up: candidate sync now exposes `syncedCandidateCount` separately from `hydratedCandidateCount`; ResultsBoardPanel uses the former for `候选人同步` and the latter only for loaded-row copy. Facet normalization now preserves user-selected filters during hydration even when the current chunk count is temporarily zero.
  - 2026-05-04 follow-up: organization completeness ledger refresh now persists explicit `population_coverage` before refreshing execution profiles. Local scripted Lovable `20260504T165620` was backfilled and refreshed; a second `帮我找Lovable的全部成员` plan now resolves to `reuse_snapshot_only`, `full_company_roster`, and `全量本地资产复用`.
  - 2026-05-04 follow-up: public Stage 1 progress now uses the validated `job_result_lifecycle` row as the stable business-counter floor. `/progress`, `/dashboard`, and `/candidates` may supplement profile status from live registry/workers, but they cannot erase persisted lane counts or profile denominators when the dynamic public-read builder has low-information evidence during local materialization. This removes the Lovable execution-page flicker between full Stage 1 metrics and a materialization-only metric card without adding a browser-side fallback ladder.
  - 2026-05-04 follow-up: global result filters now come from canonical backend `facet_summary` on both `/dashboard` and `/candidates`, not from the currently hydrated candidate-page window. Employment/location/function/layer options therefore represent the full served population while row hydration can still proceed in chunks. The execution page also prefers lifecycle expected/served counts for `总候选人数量`, and local materialization copy now says when a baseline is already served and a delta is still merging instead of showing a misleading baseline-only `300`.
  - 2026-05-04 follow-up: frontend merge logic now treats `facet_summary_scope` as a backend-owned contract. The browser no longer promotes a locally built or partially hydrated `facet_summary` to `global_full_population`; dashboard refreshes and candidate-page merges only keep global counts when the backend explicitly emits that scope.
  - 2026-05-04 follow-up: `post_result_layering` is treated as terminal serving for candidate-row hydration once canonical served/expected counts are complete, so the "正在分块装载候选人行" banner does not linger after results are already service-complete.
  - 2026-05-04 follow-up: facet summary labels now treat "all concrete options selected" as the fallback/all label. A location filter with 美国/其他/未提供地区信息 all selected displays `全量`, not the first concrete option.
  - 2026-05-04 follow-up: Lovable plan display suppresses generic scripted/provider seed queries such as `Lovable Employee` / `Lovable Linkedin Employee`; the plan keyword section only shows canonical user/request keywords or explicit backend strategy semantics.
  - 2026-05-05 follow-up: this was promoted from frontend suppression to a backend Stage 1 provider-keyword contract. Normal `full_company_roster` plans no longer emit generic seed queries such as `Lovable Employee` / `Lovable LinkedIn Employee`; large-org keyword probe, scoped search, and directional former/profile-search lanes still keep explicit keyword queries because those are provider-facing. Historical metadata with generic full-roster seed labels is also guarded at provider-query resolution, so those labels normalize to no Harvest `searchQuery` / search keyword payload.
  - 2026-05-05 follow-up: planning now emits `acquisition_strategy.provider_execution_manifest` as the canonical plan-time description of actual Stage 1 provider parameters. The manifest lists each lane's provider, operation, employment status, query texts, company filters, and whether query text is provider-facing. Frontend plan mapping and `PlanCard` now consume this manifest: Lovable full-roster shows company-employees / pastCompanies filters with no search keyword, while scoped OpenAI and large-org Google keyword probes still show real provider-facing keywords.
  - 2026-05-05 follow-up: the provider manifest contract is now runtime-visible, not only plan-visible. Frontend history recovery persists the manifest into metadata, `/progress`, `/dashboard`, `/results`, and `/candidates` project it from the saved plan, and the frontend mapper consumes metadata-level manifests. This closes the remaining path where history/runtime/debug UI could re-derive provider parameters from stale `search_seed_queries` or cached strategy wording. Acquisition execution also ignores stale generic seed labels for normal full-roster broad former lanes, so old Lovable-style `Company Employee` labels cannot reach provider query dispatch.
  - 2026-05-04 follow-up: the scripted provider binds the loaded scenario at `ScriptedSearchProvider` construction time and `openai_agent_and_lovable_streaming.json` includes `target_candidate_public_web_search.json`. Durable later Public Web fetch phases no longer lose scripted rules when environment patch context is gone, and manual scripted Public Web runs now return candidate-specific publishable profile links/signals instead of terminal `0/0` empty exports.
  - 2026-05-04 follow-up: target-candidate Public Web card actions now render as action controls rather than a duplicated follow-up-status chip. Actions are ordered `打开 LinkedIn` -> cancel/retry -> `查看公开信息详情`, while follow-up status remains in the editable candidate metadata block.
  - 2026-05-05 follow-up: results-board facet state now separates default initialization from explicit user edits. Location/function filters default to full-population open selection, all-selected facets render `全量`, and streaming/hydration facet-count changes no longer overwrite a user's in-flight filter choices.
  - 2026-05-05 follow-up: dashboard candidate-page hydration now requests rich candidate rows by default and uses 96-row chunks aligned with the backend timeline preview budget. Backend asset-population pagination enriches each requested page from embedded profile signals or the LinkedIn profile registry instead of only enriching the global first 96 candidates. This closes the Lovable full-local-reuse regression where second-run cards could fall back to preview-only rows with missing work/education/avatar despite profile detail already being persisted.
  - 2026-05-05 follow-up: target-candidate Public Web worker recovery now drains local-only phases to idle after remote search is ready. Remote provider search can still wait across daemon ticks, but document fetch, adjudication, artifact finalization, and signal materialization are no longer artificially stretched by worker poll intervals. Public Web card update timestamps now render through the shared Asia/Shanghai workflow time formatter.
  - 2026-05-05 follow-up: plan provider manifest display is now Advanced-only. `provider_execution_manifest` remains the canonical developer/operator source for actual provider parameters, but ordinary plan cards no longer expose `current_companies/past_companies/searchQuery` details in the main user-facing grid.
  - 2026-05-05 follow-up: ResultsBoardPanel now requires stable canonical lifecycle convergence before showing backend global facet counts. Partial candidate-page hydration can no longer publish misleading function/location/employment counts, user-edited filters are preserved through hydration option changes, and running->results phase changes no longer remount the board and reset in-flight choices.
  - 2026-05-05 follow-up: target-candidate Public Web `completed_with_errors` is presented as `已完成，需复核`. The card explains the common case where links/signals were materialized but no primary link passed auto-confirmation, and the retry action is labeled `重试公开搜索` instead of implying a guaranteed same-method improvement.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py tests/test_asset_coverage_backfill.py -q` -> `12 passed`.
  - `./.venv-tests/bin/python -m pytest tests/test_frontend_dashboard_hydration.py tests/test_frontend_candidate_sync_summary.py -q` -> `8 passed`.
  - `./.venv-tests/bin/ruff check tests/test_frontend_dashboard_hydration.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_coverage_contracts.py src/sourcing_agent/asset_reuse_planning.py tests/test_asset_coverage_backfill.py tests/test_frontend_candidate_sync_summary.py` -> passed.
  - `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-04 follow-up validation: `.venv-tests/bin/pytest tests/test_results_api.py -k "current_snapshot_serving_normalizes_partial_board_visible_delta_counters or stage1_live_roster_sets_expected or materialization_repoint_updates_row or persist_job_result_view_records_current_snapshot_serving_lifecycle" -q` -> `4 passed`.
  - 2026-05-04 follow-up validation: `.venv-tests/bin/pytest tests/test_frontend_candidate_sync_summary.py tests/test_frontend_candidate_filters.py -q` -> `9 passed`.
  - 2026-05-04 follow-up validation: `.venv-tests/bin/pytest tests/test_pipeline.py -k "organization_completeness_ledger_infers_former_coverage_from_candidate_documents" -q` -> `1 passed`.
  - 2026-05-04 follow-up validation: `.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/organization_assets.py tests/test_results_api.py tests/test_pipeline.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "public_progress_uses_validated_lifecycle_stage1_floor or stage1_progress or job_result_lifecycle_writer_stage1 or job_result_lifecycle_validated_stage1 or job_result_lifecycle_stage1_event_time or running_public_endpoints_do_not_publish_current_snapshot_result_view"` -> `10 passed, 111 deselected`.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_plan_contract.py tests/test_frontend_history_recovery.py tests/test_frontend_candidate_sync_summary.py tests/test_frontend_candidate_filters.py tests/test_frontend_dashboard_hydration.py -q` -> `35 passed`.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "execution_phase_contract_labels_local_materialization_without_public_web or dashboard_is_summary_only or asset_population_summary_cache_refreshes"` -> `3 passed, 118 deselected`.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_candidate_filters.py tests/test_frontend_dashboard_hydration.py tests/test_frontend_candidate_sync_summary.py tests/test_frontend_run_status_contract.py -q` -> `13 passed, 4 subtests passed`.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py tests/test_frontend_candidate_filters.py tests/test_frontend_run_status_contract.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_search_provider.py tests/test_scripted_provider_scenario.py tests/test_target_candidate_public_web.py tests/test_results_api.py::ResultsApiTest::test_job_dashboard_is_summary_only_and_candidate_page_paginates_asset_population tests/test_results_api.py::ResultsApiTest::test_target_candidate_public_web_service_e2e_runs_to_detail_and_export_contract tests/test_frontend_dashboard_hydration.py tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py -q` -> `70 passed`.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/search_provider.py src/sourcing_agent/scripted_provider_scenario.py tests/test_search_provider.py tests/test_scripted_provider_scenario.py tests/test_target_candidate_public_web.py tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-05 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_planning_modules.py::PlanningModulesTest::test_small_org_full_company_roster_does_not_emit_generic_stage1_seed_queries tests/test_planning_modules.py::PlanningModulesTest::test_google_full_roster_enables_large_org_keyword_probe_mode tests/test_search_planning.py::SearchPlanningTest::test_scoped_search_plan_keeps_paid_people_search_as_fallback tests/test_search_planning.py::SearchPlanningTest::test_targeted_people_search_bundle_uses_natural_keyword_queries -q` -> `4 passed`.
  - 2026-05-05 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py::SeedDiscoveryTest::test_resolve_provider_people_search_queries_ignores_generic_full_roster_seed_queries tests/test_seed_discovery.py::SeedDiscoveryTest::test_former_paid_fallback_does_not_send_generic_full_roster_seed_query_to_harvest tests/test_seed_discovery.py::SeedDiscoveryTest::test_former_paid_fallback_scoped_keyword_runs_query_without_broad_past_company tests/test_seed_discovery.py::SeedDiscoveryTest::test_former_paid_fallback_keyword_only_skips_blank_query -q` -> `4 passed`.
  - 2026-05-05 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_plan_contract.py -q` -> included in targeted contract run and passed.
  - 2026-05-05 provider manifest validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_planning_modules.py::PlanningModulesTest::test_small_org_full_company_roster_does_not_emit_generic_stage1_seed_queries tests/test_planning_modules.py::PlanningModulesTest::test_scoped_search_roster_with_current_and_former_adds_former_seed_task tests/test_planning_modules.py::PlanningModulesTest::test_google_full_roster_enables_large_org_keyword_probe_mode tests/test_frontend_plan_contract.py -q` -> `5 passed`.
  - 2026-05-05 provider manifest validation: `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/domain.py src/sourcing_agent/planning.py tests/test_planning_modules.py tests/test_frontend_plan_contract.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-05 runtime provider manifest validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_history_recovery.py::FrontendHistoryRecoveryTest::test_plan_workflow_persists_frontend_history_link tests/test_frontend_history_recovery.py::FrontendHistoryRecoveryTest::test_plan_workflow_persists_full_roster_provider_manifest_without_generic_seed_queries tests/test_pipeline.py::PipelineTest::test_former_broad_past_company_lane_ignores_stale_generic_seed_queries tests/test_results_api.py::ResultsApiTest::test_job_dashboard_is_summary_only_and_candidate_page_paginates_asset_population tests/test_results_api.py::ResultsApiTest::test_job_progress_compacts_large_polling_payloads tests/test_frontend_plan_contract.py -q` -> `7 passed`.
  - 2026-05-05 runtime provider manifest validation: `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_planning_modules.py::PlanningModulesTest::test_small_org_full_company_roster_does_not_emit_generic_stage1_seed_queries tests/test_planning_modules.py::PlanningModulesTest::test_scoped_search_roster_with_current_and_former_adds_former_seed_task tests/test_planning_modules.py::PlanningModulesTest::test_google_full_roster_enables_large_org_keyword_probe_mode -q` -> `3 passed`; `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/acquisition.py tests/test_frontend_history_recovery.py tests/test_pipeline.py tests/test_results_api.py tests/test_frontend_plan_contract.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-05 manual-test issue follow-up validation: `.venv-tests/bin/python -m pytest tests/test_frontend_candidate_filters.py tests/test_target_candidate_public_web.py::TargetCandidatePublicWebTest::test_run_to_local_idle_does_not_wait_for_daemon_ticks_after_remote_search_ready tests/test_results_api.py::ResultsApiTest::test_job_candidate_page_enriches_each_paginated_reuse_page_from_profile_registry -q` -> `7 passed`.
  - 2026-05-05 manual-test issue follow-up validation: `.venv-tests/bin/python -m pytest tests/test_target_candidate_public_web.py -q` -> `13 passed`.
  - 2026-05-05 manual-test issue follow-up validation: `.venv-tests/bin/python -m pytest tests/test_results_api.py::ResultsApiTest::test_job_candidate_page_lightweight_mode_skips_profile_timeline_enrichment tests/test_results_api.py::ResultsApiTest::test_job_candidate_page_lightweight_mode_keeps_complete_embedded_profile_signals tests/test_results_api.py::ResultsApiTest::test_job_candidate_page_includes_profile_timeline_preview_for_initial_asset_population_candidates -q` -> `3 passed`.
  - 2026-05-05 manual-test issue follow-up validation: `.venv-tests/bin/ruff check src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/worker_daemon.py tests/test_results_api.py tests/test_target_candidate_public_web.py` -> passed; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-05 manual-test contract follow-up validation: `.venv-tests/bin/python -m pytest tests/test_frontend_candidate_filters.py tests/test_frontend_plan_contract.py -q` -> `8 passed`; `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-05 full live-roster board-visible follow-up: inline full-snapshot materialization now publishes an event-time `full_snapshot_board_visible_patch`, upserts the `asset_population` result view to the normalized serving manifest, and advances canonical lifecycle to `current_snapshot_serving` before final workflow completion. Smoke wall-clock accounting treats this as board nonempty/ready proof but not as a partial-delta streaming proof, so delta gates remain strict. Event-level efficiency now reconstructs profile batch envelopes from completed Harvest profile workers when the final prefetch event is local-cache-only and has `dispatched_url_count=0`.
  - 2026-05-05 full live-roster validation: targeted tests passed for pipeline full-snapshot board-visible publication, smoke wall-clock semantics, workflow service metrics, and reconstructed profile batch envelopes; ruff passed for touched files. Lovable strict scripted smoke passed with `job_to_board_nonempty_ms=6000`, `profile_batch_envelope_count=5`, lifecycle `current_snapshot_serving`, `served_candidate_count=140`, and all live-roster delta profile counters `0`.

### Company-level live collector fetcher seam landed

- Historical failure reviewed before implementation:
  - The company Public Web bundle already had a durable `collector_bundle` persistence contract, but live source fetching was still implied rather than explicitly modeled.
  - If live RSS/arXiv/OpenReview/crawl fetches were added ad hoc in workflow code, we would recreate the same polluter problem that earlier repairs fixed by moving to explicit writers and canonical contracts.
- Implemented:
  - Added a typed live collector fetcher seam to `refresh_company_public_web_assets`.
  - The company lane can now accept `collector_sources` and fetch them into `collector_documents` before the normal parse/persist path.
  - Added explicit source discovery for collector bundles: `discover_collector_sources` can derive RSS/arXiv/OpenReview/crawl source URLs from source families and seed URLs, then feed those URLs into the same fetcher path.
  - Live sources remain gated behind `collection_mode=collector_bundle`; non-bundle requests with `collector_sources` are rejected.
  - Added CLI support for `--collector-source-url`, `--collector-source-json`, `--discover-collector-sources`, and `--max-discovered-collector-sources`.
  - Added service metrics for collector source/fetch quality: source count, fetched document count, fetch failure count, and max fetch duration.
  - Added smoke expectation gates for those metrics, so future live/source-discovery tuning can fail scripted/live observation when source discovery underfills, fetches fail, or source fetches exceed the configured SLO.
  - Moved live source fetching under the persisted run try/failure path. If a fetcher raises, the run is marked `failed` instead of disappearing before a run row exists.
  - Added tests proving live collector sources are fetched through the same model-safe bundle path, failed fetches write failed runs, and the CLI/smoke paths forward the new inputs.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_company_public_web_assets.py -q` -> `14 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_cli.py -q -k "company_public_web"` -> `5 passed, 45 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q -k "company_public_web"` -> `1 passed, 20 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "company_public_web"` -> `3 passed, 87 deselected`.

### Target-candidate composed profile contract landed

- Historical failure reviewed before implementation:
  - The target-candidate drawer was starting to infer profile readiness from Public Web detail alone, which would recreate the same mixed-source UI semantics that caused earlier lifecycle/progress drift.
  - Public Web is only one evidence section for a person; the canonical drawer/one-page profile must start from the target candidate record and then layer model-safe Public Web signals when they exist.
- Implemented:
  - Added `GET /api/target-candidates/{record_id}/profile`.
  - The profile endpoint always starts from the canonical `target_candidates` row and returns `ok` even when no Public Web run exists; in that case `public_web_detail.status="not_found"`.
  - Added a read-only composed profile payload with identity, contact methods, Public Web rollup, review flags, export readiness, completeness score, evidence links, and explicit raw-asset policy.
  - Public Web remains a section of the composed profile, not the whole profile. Raw/internal fields such as handoff paths, raw payloads, raw HTML, and artifact roots are excluded.
  - The target-candidate drawer now calls the composed profile endpoint and renders a profile completeness/export-readiness section while keeping Public Web detail/promotion state as the dedicated evidence section.
  - 2026-05-04 follow-up: API routing now decodes path-style `record_id` parameters for target-candidate profile, Public Web detail, and Public Web promotions. This closes the frontend/manual-test failure where canonical ids such as `scripted-public-web::chelsea-finn` were correctly written to `target_candidates` but encoded resource URLs could return `target_candidate_not_found` on one endpoint while sibling endpoints worked.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "target_candidate_profile_api or target_candidate_public_web_detail_api_returns_model_safe_signals"` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed.
  - `npm --prefix frontend-demo run build` -> passed.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_results_api.py -k "target_candidate_resource_paths_decode_frontend_encoded_record_ids or target_candidate_profile_api_returns_record_profile_without_public_web or target_candidate_public_web_api_queues_idempotent_runs"` -> `3 passed, 119 deselected`.
  - 2026-05-04 follow-up validation: `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/api.py tests/test_results_api.py` -> passed.

### Company-level Public Web collector bundle landed

- Historical failure reviewed before implementation:
  - Company-level Public Web already had a PG-authoritative API/CLI lane for seed URLs and provider search, but RSS/arXiv/OpenReview/crawled pages were still only described as future collectors.
  - Leaving those collectors outside the existing `company_public_web_asset_runs` / `company_public_web_assets` contract would recreate the same multi-path pollution pattern seen in earlier workflow-stage and lifecycle repairs.
- Implemented:
  - Added `collection_mode="collector_bundle"` on the existing company Public Web refresh contract.
  - Added explicit source families for `company_rss`, `company_arxiv`, `company_openreview`, and `company_crawl` without expanding the default seed-url family set.
  - `collector_inputs` can now carry model-safe RSS items, arXiv publications, OpenReview publications, and crawled-page summaries. They are normalized, sanitized, persisted as `company_public_web_assets`, and summarized through the same run/artifact contract as seed/provider assets.
  - `collector_documents` can also carry pre-fetched RSS XML, arXiv Atom XML, OpenReview JSON, or crawled HTML. The service parses those documents into the same model-safe `collector_inputs` shape before persistence, so future live fetchers only need to fetch documents and do not own asset writes.
  - Collector assets preserve `collector_type`, source family, title, URL, summary, authors, publication time, and source URL while keeping raw HTML/PDF/provider payloads out of public/model-safe surfaces.
  - Company Public Web artifacts now include `collector_manifest.json`, and run summaries expose `collector_record_count` plus `collector_type_counts`.
  - CLI `refresh-company-public-web-assets` now accepts `--collection-mode collector_bundle` and `--collector-input-json`.
  - Added service-level metrics for company Public Web runs. The report tracks run count, asset count, collector record count, collection-mode counts, collector-type counts, failed runs, raw-asset exposure, and missing collector manifests.
  - Added a real smoke action and matrix gate: `company_public_web_action` calls `/api/company-assets/public-web` after a completed workflow, then validates `company_public_web` service metrics. New coverage tag: `company_public_web_collector_bundle_service_slo`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_company_public_web_assets.py -q` -> `9 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_cli.py -q -k "company_public_web"` -> `4 passed, 45 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "company_public_web_assets_api"` -> `1 passed, 115 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q -k "company_public_web or target_public_web"` -> `2 passed, 19 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "company_public_web or service_gate_coverage_manifest or load_smoke_cases_preserves"` -> `5 passed, 84 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=24/24 tags, 13 matrices, 15 cases`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir /tmp/sourcing-company-public-web-smoke-1 --seed-reference-runtime --provider-mode scripted --scripted-scenario configs/scripted/openai_agent_scoped_delta_streaming.json --matrix-file configs/scripted/company_public_web_service_smoke_matrix.json --case company_public_web_collector_bundle_from_workflow_result --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/company_public_web_service_slo_report.json --summary-json output/scripted_smoke_current/company_public_web_service_slo_summary.json` -> passed; `expectation_failures=[]`, company action `completed`, `collector_record_count=4`, `raw_assets_included_count=0`, `service_guardrail_violation_detected=false`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/company_public_web_assets.py src/sourcing_agent/cli.py tests/test_company_public_web_assets.py tests/test_cli.py` -> passed.

### Target-candidate Public Web phase metrics landed

- Historical failure reviewed before implementation:
  - The main workflow stabilization work showed that user-visible progress becomes unreliable when provider wait, local apply/materialization, and terminal serving are inferred from mixed request-time sources.
  - Target-candidate Public Web Search had the same risk in smaller form: one `exploration_specialist` worker owned DataForSEO submit/poll/fetch, URL/document fetch, analysis/finalization, and signal writes, while the persisted run summary exposed mostly final result counts.
  - A completed run was written before signal rows were materialized, creating a short but real detail/export inconsistency window.
- Implemented:
  - Per-candidate Public Web runs now persist `summary.phase_metrics` and `analysis_checkpoint.phase_metrics`.
  - Metrics cover search submit, ready poll, ready fetch, analysis/finalization, and signal materialization: submitted/pending/fetched/failed/timeout task counts, ready poll count, query-result/raw-link counts, submit/poll/fetch durations, analysis duration, fetched-document count, email/profile-link signal counts, materialized signal count, and whether a reusable person asset was written.
  - Waiting-search summaries now expose the same phase metrics as completed summaries, so the frontend/live-observation path can distinguish “remote search pending” from “ready results fetched” and “analysis/materialization running”.
  - Terminal run completion is written only after person asset/signal materialization, so detail/export consumers do not see `completed` before durable signal rows exist.
  - Batch summaries now aggregate run phase metrics across selected candidates, including phase/status counts, pending/fetched remote tasks, materialized signal totals, and the slowest observed phase.
  - Batch summaries also expose service guardrail fields derived only from persisted per-run summaries/checkpoints: pending remote-search run count, unmaterialized signal gap count, terminal-with-errors/partial-failure counts, metric-error run count, missing phase-metric count, phase-lag risk reasons, and terminal materialization violation detection. Scripted/browser gates can now evaluate Public Web provider quality and recovery health from the batch summary without parsing every run artifact.
  - Target-candidate Public Web UI and browser E2E now surface the batch-level guardrail line separately from the normal phase line, so operators can see slow-tail / partial-failure / materialization-gap risk directly in the action panel and in scripted report output. Browser tests now fail if that guardrail surface disappears or if it reports terminal materialization violations / missing phase metrics in the happy paths.
  - Public Web batch guardrails are now part of `workflow_service_metrics.target_candidate_public_web`, smoke expectation validation, and matrix summary rollups. Scripted/live-observation cases can gate `require_no_target_public_web_guardrail_violation` plus maxima for remote-pending runs, partial failures, terminal signal-materialization gaps, and missing phase metrics.
  - Hosted/scripted smoke now reads real target-candidate Public Web batch rows from `/api/target-candidates/public-web-search?limit=1000` during diagnostics and feeds only batches created/updated after the current case start into `workflow_service_metrics`. This prevents stale manual Public Web batches from polluting unrelated workflow smoke while still making live Public Web activity machine-gated.
  - Public Web batch summaries now expose efficiency rollups by phase: per-phase count/total/avg/max for search submit, search poll, search fetch, document fetch, adjudication, analysis, and signal materialization, plus provider/fetch failure count and local processing error count.
  - `workflow_service_metrics.target_candidate_public_web` and smoke matrix summaries aggregate the new efficiency fields, including max duration by phase, slowest-phase distribution, provider/fetch failure totals, and local processing errors. Slow Public Web phases can now appear as bottlenecks without opening individual run artifacts.
  - Smoke expectations now support Public Web efficiency SLO gates: `max_target_public_web_duration_by_phase_ms` (phase->milliseconds), `max_target_public_web_provider_or_fetch_failure_count`, and `max_target_public_web_local_processing_error_count`, alongside the existing guardrail maxima.
  - Added a real scripted/live smoke action contract: matrix cases can declare `target_public_web_action.enabled=true`, and the smoke runner will import target candidates from the completed workflow, trigger `POST /api/target-candidates/public-web-search`, drive the resulting `target_candidate_public_web_search` worker through `/api/workers/daemon/run-once`, and then evaluate the normal Public Web SLOs from real batch rows. This avoids fake coverage where an ordinary workflow case declares Public Web expectations without ever running Public Web.
  - `target_candidate_public_web_search` workers are now recognized by smoke recovery as first-class recoverable workers. Harvest-only recovery filtering no longer prevents target-candidate Public Web matrix cases from progressing past queued/searching states.
  - Added `configs/scripted/target_public_web_service_smoke_matrix.json` plus manifest tag `target_candidate_public_web_service_slo`; service-gate coverage now requires this post-workflow Public Web action to remain covered by a local smoke matrix.
  - Public Web export now exposes auditability for empty selections. API response, `public_web_manifest.json`, per-candidate `public_web_summary.json`, and `public_web_summary.csv` include per-record `export_record_status` / `export_skip_reason`, plus aggregate counts for exported records, missing Public Web results, non-terminal runs, and records with no exportable signals. The HTTP download response exposes the same counts via `X-Sourcing-*` headers, and the target-candidate page shows the summary after download. This prevents an empty but technically successful zip from being misread as a successful data export.
  - The target-candidate page surfaces the latest batch phase line and each candidate run phase line, so manual tests can see whether Public Web is waiting on remote search, local analysis, or signal materialization.
  - DataForSEO-style batch search fetch and candidate analysis are now separated by durable `entry_links_ready`: once all remote search tasks are fetched, the worker checkpoints and releases the lease; the next recovery pass performs analysis/finalization/materialization.
  - URL/document fetch and AI adjudication are separated by durable `documents_fetched`: document fetch writes `document_fetch_payload.json`, then a later recovery pass reads that payload for AI adjudication and model-safe signal/summary artifact creation.
  - AI adjudication, model-safe artifact creation, and durable signal/person-asset materialization are now separated by durable `adjudication_completed` and `analysis_completed`: model adjudication writes `adjudication_payload.json`, a later recovery pass writes `signals.json` / `candidate_summary.json` / `search_results.json`, and a final pass materializes reusable person assets plus first-class signal rows.
  - Advanced durable phases no longer redispatch provider search on recovery. A `documents_fetched` run resumes from `document_fetch_payload.json`, and an `adjudication_completed` run resumes from `adjudication_payload.json`; if either handoff artifact is missing or corrupt, the run fails loudly instead of silently continuing with an empty payload.
  - Detail/export responses now sanitize public-web run and person-asset summaries before returning them. Internal paths and handoff fields such as `artifact_root`, `document_fetch_payload_path`, `adjudication_payload_path`, raw paths, and raw payloads are stripped from both detail API payloads and Web Search zip exports.
  - Service-level Public Web E2E now drives the full state machine (`searching -> entry_links_ready -> documents_fetched -> adjudication_completed -> analysis_completed -> completed`) with a scripted batch-search provider, then verifies PG signal rows, detail payload, promoted export zip, phase metrics, and raw/internal-field isolation.
  - The E2E exposed and fixed a durable artifact ownership bug: after the first checkpoint, `artifact_root` already points at the candidate artifact directory. Recovery now recognizes that shape and reuses the same experiment root instead of nesting `candidates/01_*` directories on each phase.
  - Added a multi-candidate Public Web service matrix with staggered provider readiness and one fetch failure. It proves batch aggregation stays non-terminal while one candidate is still searching, lets the fast candidate complete independently, and reports the failed-fetch candidate as `completed_with_errors` without blocking the batch.
  - Added a batch guardrail regression proving terminal runs with unmaterialized signals and runs missing phase metrics are reported as service violations in the aggregate summary.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_target_candidate_public_web.py -q` -> `11 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_public_web_search.py tests/test_target_candidate_public_web.py tests/test_results_api.py -q -k "public_web"` -> `54 passed, 107 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k "target_candidate_public_web"` -> `2 skipped, 10 deselected` unless `SOURCING_RUN_FRONTEND_BROWSER_E2E=1` is set.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_target_candidate_public_web.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "target_public_web or provider_case_report_aggregates or provider_anomaly or smoke_timings or service_gate_coverage or target_candidate_public_web"` -> `21 passed, 93 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py tests/test_workflow_service_metrics.py -q -k "target_public_web or provider_case_report_exposes_target_public_web or public_web_batches_for_smoke_window or provider_case_report_aggregates"` -> `5 passed, 100 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_target_candidate_public_web.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "target_public_web or batch_summary_aggregates_phase_metrics or provider_case_report_aggregates or public_web_batches_for_smoke_window"` -> `6 passed, 110 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "target_public_web_guardrails or unknown_expectation"` -> `3 passed, 82 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "target_public_web_service_matrix or target_public_web_smoke_action or load_smoke_cases_preserves_expectations or public_web_batches_for_smoke_window or evaluate_smoke_expectations_can_gate_target_public_web_guardrails"` -> `5 passed, 82 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q` -> `87 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=23/23 tags, 12 matrices, 14 cases`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir /tmp/sourcing-target-public-web-smoke-2 --seed-reference-runtime --provider-mode scripted --scripted-scenario configs/scripted/openai_agent_scoped_delta_streaming.json --matrix-file configs/scripted/target_public_web_service_smoke_matrix.json --case target_public_web_service_slo_from_workflow_result --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/target_public_web_service_slo_report.json --summary-json output/scripted_smoke_current/target_public_web_service_slo_summary.json` -> passed; `expectation_failures=[]`, target Public Web action `completed`, recovery `5/12` rounds settled, batch `completed`, guardrail violations `0`, phase maxes `search_submit=22.39ms`, `search_poll=17.52ms`, `search_fetch=5.87ms`, `document_fetch=26.95ms`, `analysis=44.23ms`, `adjudication=6.51ms`, `signal_materialization=1.12ms`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "target_candidate_public_web_detail_api_returns_model_safe_signals or target_candidate_public_web_promotion_updates_primary_email_then_export_uses_promoted_signals"` -> `2 passed, 112 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "target_candidate_public_web_export_manifest_reports_empty_and_skipped_records or target_candidate_public_web_promotion_updates_primary_email_then_export_uses_promoted_signals or target_candidate_public_web_service_e2e_runs_to_detail_and_export_contract"` -> `3 passed, 113 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "target_candidate_public_web_export_manifest_reports_empty_and_skipped_records or target_candidate_public_web_promotion_updates_primary_email_then_export_uses_promoted_signals"` -> `2 passed, 114 deselected`.
  - `npm --prefix frontend-demo run build` -> passed after surfacing export stats in the target-candidate UI.

### Watcher-first late webhook scripted gate landed

- Historical failure reviewed before implementation:
  - The hosted OpenAI Infra/Whisper incidents exposed both terminal-event orders: provider webhook can arrive first, or the local provider watcher can observe actor completion first while the external webhook arrives late.
  - Existing smoke coverage used `drive_remote_provider_duplicate_events`, which exercised duplicate idempotency but did not explicitly prove the watcher-first/provider-webhook-late production path.
- Implemented:
  - Added `drive_remote_provider_watcher_first_events` as a first-class smoke expectation.
  - `workflow_smoke` can now drive `local_provider_event_watcher` as the primary terminal event, then inject a late `provider_webhook` after board probing.
  - Expectation evaluation now fails unless watcher-first recovery is observed and the late provider webhook returns `matching_remote_provider_workers_not_recoverable` with `recovery_count=0`.
  - Matrix summaries now aggregate `remote_provider_events.source_counts` and `status_counts`, so live-observation reports show whether watcher or webhook actually advanced the workflow without opening per-case JSON.
  - `service_gate_coverage.py` now treats nested positive expectations as count maps, so the manifest can require both `local_provider_event_watcher` and `provider_webhook` event sources and the `received` / `received_late` status split.
  - Updated the late-webhook service-gate manifest to require the explicit watcher-first mode instead of the old ambiguous duplicate mode.
- Fresh scripted smoke:
  - Command wrote `output/scripted_smoke_current/openai_whisper_watcher_first_late_webhook_report.json` and `output/scripted_smoke_current/openai_whisper_watcher_first_late_webhook_summary.json`.
  - Result: `expectation_failures=[]`, `remote_provider_event_driver.event_count=2`, `remote_provider_event_driver.recovery_count=1`, `remote_provider_event_driver.late_duplicate_count=1`.
  - Remote event metrics: `received_count=1`, `late_duplicate_count=1`, `in_flight_duplicate_count=0`, `source_counts.local_provider_event_watcher=1`, `source_counts.provider_webhook=1`, `remote_to_local_event_lag_ms.max=16771` under the 30s gate.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'watcher_first or duplicate or service_gate_coverage or remote_provider_event_driver'` -> `10 passed, 71 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=22/22 tags, 11 matrices, 13 cases`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py src/sourcing_agent/smoke_expectation_contract.py tests/test_workflow_smoke.py` -> passed.

### Provider-quality SLO scripted gate landed

- Historical failure reviewed before implementation:
  - The OpenAI Infra hosted incident needed one report that could separate provider actor runtime, dataset download I/O, local materialization I/O, true zero-result lane behavior, and scheduler/recovery gaps.
  - The first local provider-quality smoke attempt proved `provider_io` was working, but failed for two infrastructure reasons: the historical baseline+delta matrix was run without seeded baseline assets, and `provider_anomalies` was unavailable because durable discovery items wrote `metadata.summary` while the service metric read only `metadata.query_summary`.
  - A second check exposed a metric bug: `retry_count=0` fell back to `attempts`, falsely counting normal probe attempts as zero-result retries. This would have made provider-quality SLOs noisy in live runs.
- Implemented:
  - `search_seed_discovery_query` items now normalize provider `summary` into canonical `metadata.query_summary` at write time. `workflow_service_metrics` also reads legacy `summary` for historical rows, but new diagnostics have one explicit durable field.
  - `provider_anomalies.zero_result_retry_count` now respects explicit `retry_count=0` and only falls back to `attempts` when `retry_count` is absent.
  - `workflow_smoke` matrix cases can carry a `review_decision`; the provider-quality scoped-live case uses `{"force_fresh_run": true}` so plan-review approval does not accidentally reuse a completed job.
  - Added `configs/scripted/openai_infra_provider_quality_scoped_live_smoke_matrix.json` and a new required coverage tag `openai_infra_provider_quality_scoped_live`. This case is intentionally separate from the seeded OpenAI Infra baseline+delta regression, so provider-quality SLOs are tested without conflating asset reuse prerequisites.
- Guardrails:
  - Provider anomalies remain explicit opt-in SLOs. Normal diagnostics do not fail a workflow just because a scoped lane accepted a true zero-result after retry.
  - Fresh-runtime provider-quality validation is the canonical way to verify first-run provider path metrics; reused/polluted runtimes can legitimately exercise snapshot/job reuse and are not valid evidence for provider I/O coverage.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_seed_discovery.py -q -k "provider_quality or openai_infra or provider_anomaly or summary_to_query_summary or zero_retry_count"` -> `6 passed, 143 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/workflow_service_metrics.py tests/test_workflow_smoke.py tests/test_seed_discovery.py tests/test_workflow_service_metrics.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=21/21 tags, 10 matrices, 12 cases`.
  - Fresh scripted smoke passed: `openai_infra_provider_quality_scoped_live` with `expectation_failures=[]`, `provider_io.actor_run_duration_ms.max=120000`, `provider_io.dataset_download_duration_ms.max=5200`, `materialization_io.sync_total_ms.max=1378.37`, `provider_anomalies.anomaly_count=6`, `zero_result_retry_count=4`, `zero_result_accepted_count=1`, `job_to_board_nonempty_ms=21130.03`.

### Gemini provider-quality page-coverage gate landed

- Historical failure reviewed before implementation:
  - OpenAI Infra provider-quality covered zero-current/former-delta and zero-result retry behavior, but it did not exercise provider page-coverage anomalies such as a non-zero probe total followed by empty scaled pages, chunk fallback, empty page ranges, and single-page retry recovery.
  - The scripted provider rule matcher only supported text contains/excludes, which made `startPage/takePages` provider behavior fixtures fragile and hard to audit.
  - `seed_discovery` already computed `chunked_scale_fallback.coverage_degraded`, but query summaries did not always promote that to a degraded provider query, so `provider_anomalies.probe_total_drift_count` could under-report page-coverage drift.
- Implemented:
  - Added generic `payload_equals` / `context_equals` scripted rule matching so provider fixtures can declare exact request-shape behavior without string-fragile JSON contains checks.
  - Scripted generated Harvest profile-search bodies now honor explicit `returned_count=0`, enabling true empty scaled/chunk page simulations.
  - Chunked Harvest profile-search fallback now marks `incomplete=true` and query `status=degraded` when coverage is degraded by empty pages or returned-count drift.
  - Added `configs/scripted/google_gemini_provider_quality_streaming.json` and `google_gemini_provider_quality_smoke_matrix.json`, with a required service-gate tag `google_gemini_provider_quality_scoped_live`.
- Fresh scripted smoke:
  - Command wrote `output/scripted_smoke_current/google_gemini_provider_quality_report.json` and `output/scripted_smoke_current/google_gemini_provider_quality_summary.json`.
  - Result: `expectation_failures=[]`, `dispatch_strategy=new_job`, `effective_acquisition_mode=scoped_live_search`, `current/former=2/18`, `profile_fetched=20/20`, `board_total_candidates=20`, `job_to_board_nonempty_ms=3478.4`.
  - Provider-quality metrics: `provider_anomalies.anomaly_count=10`, `zero_result_retry_count=4`, `zero_result_retry_exhausted_count=1`, `empty_scale_count=1`, `probe_total_drift_count=1`, `empty_page_range_count=1`, `single_page_retry_count=2`, `provider_io.actor_run_duration_ms.max=50000`, `provider_io.dataset_download_duration_ms.max=3600`.
  - Follow-up webhook/live-observation run wrote `output/scripted_smoke_current/google_gemini_provider_quality_webhook_report.json` and `output/scripted_smoke_current/google_gemini_provider_quality_webhook_summary.json`.
  - Webhook/live-observation result: `expectation_failures=[]`, `remote_provider_event_driver.event_count=1`, `remote_provider_event_driver.recovery_count=1`, `remote_provider_events.received_count=1`, `remote_to_local_event_lag_ms.max=0`, `provider_io.actor_run_duration_ms.max=50000`, `materialization_io.sync_total_ms.max=1881.64`, `job_to_board_nonempty_ms=4709.12`.
  - Service-gate nuance: this case requires at least one accepted provider webhook completion event, not one event per profile worker. A single event can legitimately advance multiple same-snapshot durable apply closures, so binding the gate to worker count would overfit the current scheduler implementation.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_workflow_smoke.py -k "google_gemini_provider_quality or service_gate_coverage_manifest" -q` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py::SeedDiscoveryTest::test_provider_people_search_marks_chunked_page_coverage_drift_degraded tests/test_harvest_connectors.py::HarvestConnectorTest::test_scripted_profile_search_honors_explicit_zero_returned_count -q` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=22/22 tags, 11 matrices, 13 cases`.
  - `ruff` and `py_compile` passed for touched Python files.

## 2026-05-03 (Asia/Shanghai)

### Former-lane broad Harvest query contract tightened

- Historical failure reviewed before implementation:
  - The former-member Harvest fallback used `paid_queries=[""]` whenever scoped provider queries normalized away to company-only.
  - That made `__past_company_only__` ambiguous: it represented both a valid broad former-roster scan and a silent downgrade from scoped/directional user intent.
  - The same class of ambiguity caused earlier OpenAI scoped-query incidents: downstream provider execution widened the user boundary instead of requiring planner/coverage proof to authorize it.
- Implemented:
  - `seed_discovery._provider_people_search_fallback(...)` now treats empty Harvest query text as an explicit broad-former strategy only.
  - If a former lane has `past_companies` but no scoped provider query and `former_broad_past_company_only` is not set, it records a skipped/degraded query summary and does not dispatch Harvest.
  - `_normalize_harvest_query_text(...)` no longer falls back to the original company-only query when all useful terms are stripped; `_resolve_provider_people_search_queries(...)` drops target-company-only normalized queries.
  - `acquisition_strategy` now sets `former_broad_past_company_only` only for full-company former lanes and unscoped former-only requests; former-only directional requests such as "former RL researcher" and scoped roster requests keep broad mode off.
  - `normalize_former_member_search_contract(...)` preserves the explicit broad-former flag while forcing the required Harvest lane.
- Guardrails:
  - Added regressions proving company-only former queries are skipped without explicit broad strategy, scoped former keywords still dispatch non-empty Harvest queries, full-roster former lanes still use `__past_company_only__`, and unscoped former-only plans explicitly carry the broad flag.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py tests/test_provider_execution_policy.py tests/test_planning_modules.py -q -k "former_paid_fallback or past_company_only or provider_people_search_fallback_skips_live_rapidapi or discover_allows_harvest_former_fallback_without_rapidapi_accounts or resolve_provider_people_search_queries or normalize_harvest_query_text or former_member_search_contract or former_employee_strategy_prefers_past_company_recall or unscoped_former_only_strategy or full_company_roster_plan_uses_large_org_budget_and_default_former_seed or scoped_search_roster_with_current_and_former"` -> `17 passed`.

### Result-view manifest fail-loud guard landed

- Historical failure reviewed before implementation:
  - Published `job_result_view` rows can point to a serving artifact path such as `normalized_artifacts/manifest.json`.
  - If that path is missing or corrupt, public read paths previously resolved the snapshot directory and silently replaced it with another same-snapshot artifact such as `artifact_summary.json` or `materialized_candidate_documents.json`.
  - That hid publication/data corruption and reintroduced a fallback ladder after result-view lifecycle had been made canonical.
- Implemented:
  - `_apply_job_result_view_to_candidate_source(...)` now validates authoritative result-view `source_path` values. Missing, directory, unreadable, or invalid-JSON artifacts mark the candidate source and nested result view as `source_status='manifest_invalid'`.
  - `_resolve_job_candidate_source(...)` now respects that invalid state and does not run snapshot-id/path fallback for the same source.
  - `_build_job_asset_population_summary_payload(...)` returns an explicit unavailable `manifest_invalid` payload instead of silently serving a substitute artifact.
- Guardrails:
  - Added regressions for both missing and corrupt result-view manifest paths, with same-snapshot fallback artifacts present to prove they are not used.
  - Existing legacy SQLite authoritative-registry recovery remains green, so explicit historical repair still works while corrupt published result views fail loud.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "manifest_fails_loud or legacy_sqlite_candidate_source_from_authoritative_registry or asset_population_overlay_count"` -> `4 passed`.

### Reconcile duplicate metrics split by phase

- Historical failure reviewed before implementation:
  - Event-level reports had enough structured phases to know whether duplicate work was a safe `marker_backfilled` idempotency replay or an unsafe repeated `materialize_started` I/O path.
  - The exported duplicate counters did not expose that distinction, so smoke/browser gates could either overreact to harmless marker repair or miss a repeated materialization start hidden behind a generic duplicate count.
- Implemented:
  - `workflow_efficiency` now reports `marker_backfill_repeat_count` separately from `materialize_started_repeat_count`, plus signature/worker sub-counts for materialization starts.
  - Aggregated reports and runtime subsets carry both fields.
  - `workflow_smoke` expectation checks now accept `max_marker_backfill_repeat_count` and `max_materialize_started_repeat_count`.
- Guardrails:
  - Updated the structured completed-reconcile test to include both a repeated marker backfill and a repeated materialize start, proving the two classes are counted independently.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q -k "structured_completed_reconcile or duplicate_reconcile_materialize or workflow_materialization"` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_efficiency.py` -> passed.

### Runtime ops panel exposes event-level efficiency rollup

- Historical failure reviewed before implementation:
  - Backend/runtime reports already computed `writer_lock_wait_ms`, `remote_wait_age_ms`, `provider_lease_age_ms`, and provider-completion handoff lag.
  - The frontend contract/example ops dashboard still treated runtime metrics as refresh counters, so an operator could see candidate sync drift without seeing whether the bottleneck was webhook lag, writer lock contention, provider slot idleness, or repeated materialization.
- Implemented:
  - `RuntimeMetricsResponse` now explicitly exposes `event_level_efficiency`.
  - The frontend API adapter maps that payload as a typed JSON object instead of relying on spread-through fields.
  - `contracts/frontend_runtime_dashboard.example.tsx` now includes an Event-Level Efficiency panel for remote event lag, remote-to-local marker lag, local-to-next-submit lag, writer-lock wait, remote-wait age, provider-lease age, materialize repeat starts, marker-backfill repeats, and phantom pre-submit provider workers.
- Guardrails:
  - This remains a read-only ops surface over canonical `/api/runtime/metrics`; no UI code re-parses raw job events or provider payloads.
- Validation:
  - `npm --prefix frontend-demo run build` -> passed.
  - Contract TypeScript spot-check using the local compiler still hits pre-existing contract typing errors around `intent_rewrite`/`intent_brief` JSON index signatures and root React type resolution; this slice did not introduce those broader contract errors.

### Materialization I/O timing enters workflow efficiency metrics

- Historical failure reviewed before implementation:
  - Scripted and live reports could show materialization lag or provider-slot underuse, but not whether the local tail was dominated by control-plane delta apply, full snapshot normalization, artifact build/write, state upsert, or writer wait.
  - Candidate artifact builders already recorded detailed timings, but the workflow sync result did not consistently carry them into structured materialization events and efficiency rollups.
- Implemented:
  - `SnapshotMaterializer.synchronize_snapshot_candidate_documents(...)` now returns `timings_ms` with `sync_total`, `full_snapshot_normalization`, `materialization_writer_wait`, and artifact-builder timings when available.
  - `SnapshotMaterializer.synchronize_snapshot_candidate_delta(...)` now returns `timings_ms.sync_total` and `candidate_delta_control_plane_replace`, so board-visible delta apply has a measurable local I/O segment.
  - `workflow_efficiency` now exposes `materialization_io` stats from structured materialization events, including sync total, candidate-delta replace, candidate-artifact build, state upsert, and writer wait.
- Remaining:
  - Provider anomaly service metrics are handled in the provider anomaly slice below. Live smoke still needs a coverage-quality gate that compares provider total/probe total with actual fetched page coverage without failing normal degraded provider runs as local workflow bugs.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_pipeline.py -q -k "workflow_materialization or snapshot_materializer or candidate_delta_updates_control_plane_without_full_artifacts or runtime_metrics_reports_refresh"` -> `7 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_efficiency.py tests/test_pipeline.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/snapshot_materializer.py src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_efficiency.py tests/test_pipeline.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=20/20 tags, 9 matrices, 11 cases`.

### Provider I/O timing enters workflow efficiency metrics

- Historical failure reviewed before implementation:
  - ECS OpenAI Infra showed Apify actors completing in tens of seconds while local workflow progress advanced minutes later, but the report could not separate actor runtime, webhook/event lag, dataset download I/O, and local materialization I/O.
  - Existing smoke gates could prove slot occupancy and remote-to-local lag, yet could not answer whether a slow tail was caused by provider actor execution, dataset pagination/download, or local apply/materialization.
  - Counting the same actor duration from both webhook events and worker checkpoints would make avg/count metrics misleading, so the metric needs run/dataset/worker-level de-duplication rather than a raw fallback merge.
- Implemented:
  - `handle_remote_provider_event(...)` records `event_metrics.actor_run_duration_ms` from provider run `startedAt`/`finishedAt`.
  - Harvest profile completion records `checkpoint.provider_timings.dataset_download_duration_ms` and `actor_run_duration_ms`, and mirrors the same timing contract into the dataset artifact metadata.
  - `workflow_efficiency.provider_io` now aggregates actor run duration and dataset download duration from persisted events/workers, with actor duration de-duplicated by run id, dataset id, or worker id.
- Guardrails:
  - The provider I/O report remains read-only over persisted event/worker state; it does not re-read Apify payloads or candidate artifacts.
  - Tests assert webhook actor duration capture, Harvest checkpoint/artifact timing persistence, provider I/O aggregation, and actor-duration de-duplication when the same run appears in both event and checkpoint state.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/workflow_efficiency.py tests/test_remote_provider_events.py tests/test_harvest_connectors.py tests/test_workflow_efficiency.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/harvest_connectors.py src/sourcing_agent/workflow_efficiency.py tests/test_remote_provider_events.py tests/test_harvest_connectors.py tests/test_workflow_efficiency.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py tests/test_harvest_connectors.py tests/test_workflow_efficiency.py -q -k "actor_run_duration or provider_io_timings or provider_io"` -> `3 passed, 134 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `status=ok errors=0 coverage=20/20 tags, 9 matrices, 11 cases`.

### Provider anomaly metrics enter service reports

- Historical failure reviewed before implementation:
  - Google/Gemini live runs showed provider behavior that is materially different from local orchestration bugs: transient true zero results, probe total drift, scaled search returning empty while probe had rows, empty page ranges, and single-page retry recovery.
  - Before this slice, `provider_search_retry_queue` could show terminal zero-result exhaustion, but normal degraded/accepted provider anomalies were not visible in service reports. That made it hard to tell whether a manual smoke issue was provider supply quality or local scheduler/recovery failure.
  - Reading raw Harvest payloads in runtime metrics would recreate hot-path I/O and fallback parsing. The durable owner already has the query summary in `job_materialization_items`, so service metrics should read that metadata only.
- Implemented:
  - `workflow_service_metrics.provider_anomalies` now extracts query summaries from durable `search_seed_discovery_query` and `provider_search_retry` item metadata.
  - The report counts `zero_result_retry_count`, `zero_result_retry_exhausted_count`, `zero_result_accepted_count`, `empty_scale_count`, `probe_total_drift_count`, `chunked_scale_fallback_count`, `empty_page_range_count`, and `single_page_retry_count`, plus provider/status/reason breakdowns and samples.
  - `workflow_smoke` exports `service_metrics.provider_anomalies` in per-case reports and aggregates the same counters in matrix summaries.
- Guardrails:
  - The metric is diagnostic and read-only; it does not create retry work, re-run providers, or classify accepted zero-result scoped lanes as failures.
  - The source is durable item metadata, not raw provider artifacts or public-read reconstruction.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_service_metrics.py tests/test_workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py tests/test_workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q -k "provider_anomaly or provider_search_retry_queue"` -> `2 passed, 15 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "provider_case_report_aggregates or materialization_streaming_rollup"` -> `2 passed, 71 deselected`.

### Provider anomaly smoke gates are explicit opt-in

- Historical failure reviewed before implementation:
  - Provider anomalies should be observable by default, but not all anomalies are workflow failures. A true zero-result scoped lane can be valid, and degraded page coverage should not fail normal local development unless a specific smoke case is asserting provider quality.
  - The smoke expectation registry is fail-closed; adding ad-hoc config keys without registering them would silently break service-gate intent.
- Implemented:
  - Added supported smoke expectations for provider anomaly maxima: total anomaly count, zero-result retry/exhausted/accepted, empty scale, probe-total drift, empty page ranges, and single-page retry count.
  - `_evaluate_smoke_expectations(...)` now checks these thresholds only when declared. If declared, the provider anomaly report must be present; if not declared, provider anomalies remain diagnostic.
- Guardrails:
  - This keeps provider-supply quality gates separate from local scheduler/recovery gates. Cases that expect a clean provider run can set max values to `0`; historical anomaly fixtures can allow or assert specific counts.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/smoke_expectation_contract.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/smoke_expectation_contract.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "provider_anomalies or provider_anomaly or unknown_expectation"` -> `4 passed, 71 deselected`.

### OpenAI Agent tiny-tail provider handoff hot path repaired

- Historical failure reviewed before implementation:
  - The strict OpenAI Agent tiny-tail coalescing smoke exposed `remote_to_next_submit_start_ms` spikes above the 10s SLO even though durable profile URL scheduling state existed.
  - Root cause was a hot-path ownership mismatch: refill/submit logic still scanned queued worker state to decide whether URLs were already owned, so a provider completion could spend tens of seconds in worker-summary/state inspection before submitting the next profile batch.
  - Loosening the smoke SLO would have hidden the actual service-level failure. The correct owner is `linkedin_profile_registry.refill_queue_state`, with provider workers acting only as remote-run envelopes.
- Implemented:
  - `enrichment._partition_already_queued_profile_urls(...)` now accepts the refill dispatch URL set and dispatch-claimed URL set from the durable registry queue.
  - New scope/authorization helpers verify that a registry entry belongs to the current `source_job + snapshot_dir` before treating it as already owned by the active refill/dispatch claim.
  - Refill dispatch and Harvest profile batch worker paths now bypass broad worker scans for URLs already owned by the current durable scheduler claim.
- Guardrails:
  - Added regressions proving refill dispatch and Harvest profile batch submit use durable item ownership without scanning all worker summaries.
  - The strict tiny-tail smoke remains a service gate: no unexplained tiny batches, no provider-slot underuse with backlog, no terminal queue-state leaks, and provider handoff stays under the configured SLO.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/enrichment.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py -k 'dispatch_claim_without_worker_scan or refill_dispatch_uses_durable_item_ownership or refills_deferred_budget_items_from_registry or dispatch_claim_is_daemon_recoverable or refill_item_state' -q` -> `5 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py -k 'refill_plan_item_state or never_reopens_terminal or remote_envelope_owner or dispatch_claimed or planned_dispatch' -q` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -k 'underuse or terminal_queue or planned_dispatch or tiny_batch or remote_provider_completion' -q` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/enrichment.py tests/test_enrichment.py` -> passed.
  - Strict smoke `openai_agent_tiny_tail_coalescing_slot_utilization` passed with `expectation_failures=[]`: `output/scripted_smoke_current/openai_agent_tiny_tail_coalescing_hotpath_report.json`.
  - Final smoke metrics: `remote_to_next_submit_start_ms.max=9060ms`, `local_to_next_submit_start_ms.max=0ms`, `remote_to_local_event_lag_ms.max=8060ms`, `profile_batch_envelopes.unexplained_tiny_batch_count=0`, `provider_slot_underuse_with_backlog_count=0`, `profile_prefetch_queue.terminal_queue_state_leak_count=0`.

### Search-seed recovery ownership made explicit

- Historical failure reviewed before implementation:
  - Discovery/provider queue ownership had already moved to `search_seed_discovery_query` + `local_apply_closure`, but a legacy helper still treated empty search-worker `recovery_kind` as search-seed recovery.
  - That left a maintenance footgun: a future or historical worker without explicit recovery ownership could be backfilled into the normal durable discovery path even though it did not declare the `search_seed_discovery` contract.
- Implemented:
  - `_worker_is_search_seed_inline_worker(...)` now requires explicit `recovery_kind='search_seed_discovery'` from metadata/checkpoint.
  - Added a regression proving a completed search worker with an empty legacy recovery kind is not listed by `backfill_search_seed_discovery_query_items(...)` and cannot create a `search_seed_discovery_query` item.
  - Updated `docs/DISCOVERY_PROVIDER_QUEUE_CONTRACT.md` and `docs/NEXT_TODO.md` to mark discovery durable ownership locally complete, with ECS limited to explicit historical backfill/migration.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "search_seed_discovery_query or explicit_recovery_kind or worker_summary_merge_retired or local_apply_closure"` -> `8 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py tests/test_pipeline.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py` -> passed.

### Target-candidate card explicit edit/save UX landed

- Historical failure reviewed before implementation:
  - The target-candidate row is already the backend truth source, but the card fields wrote through `updateTargetCandidate(...)` on every select/input/textarea change.
  - That recreated browser-state ambiguity at the UX layer: high-frequency POSTs, out-of-order save risk, no visible pending/error state, and no cancel/revert path for a user typing notes.
- Implemented:
  - `TargetCandidatesPanel` now keeps per-record local drafts for follow-up status, quality score, and comment.
  - Backend persistence only happens when the user clicks `保存跟进信息`; successful saves replace the record with the backend response and reset the draft.
  - Added per-record saving, dirty, saved, error, and cancel UI states. External target-candidate refreshes update clean drafts but do not overwrite dirty local edits.
  - Normalized the quality score field to the backend/product range `0-100` instead of the stale `1-10` placeholder.
- Guardrails:
  - The target-candidate Public Web browser script now counts `POST /api/target-candidates` requests and asserts editing a comment creates zero backend writes before explicit save and exactly one write after save.
- Validation:
  - `npm --prefix frontend-demo run build` -> passed.
  - `node --check frontend-demo/scripts/run_target_public_web_e2e.mjs` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check tests/test_frontend_browser_e2e.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`.
- Remaining:
  - Provider-backed company public-web collectors still need a production slice.
  - The new browser assertion is wired into the opt-in frontend E2E path; it was not run live in this pass because the browser suite is environment-gated.

### Target-candidate Public Web detail drawer landed

- Historical failure reviewed before implementation:
  - Public Web detail, evidence links, and promotion actions were rendered inline inside each target-candidate card.
  - That made one expanded candidate reshape the whole grid, mixed scanning/editing/promotion into one surface, and risked creating two competing mental models for card state versus detailed evidence review.
- Implemented:
  - Removed the card-level inline detail expansion path.
  - Added a page-level `role=dialog` drawer for target-candidate Public Web detail.
  - Cards now keep compact status, metrics, primary links, cancel/retry, and a single `查看公开信息详情` entrypoint.
  - The drawer owns evidence review, email/link promotion/rejection, refresh, LinkedIn navigation, and cancel/retry for the selected candidate while still using the existing backend run/detail/promotion APIs.
- Guardrails:
  - The target-candidate Public Web browser script now opens the detail entrypoint and asserts a dialog drawer is visible, so future regressions back to card-only inline detail are detectable.
- Validation:
  - `npm --prefix frontend-demo run build` -> passed.
  - `node --check frontend-demo/scripts/run_target_public_web_e2e.mjs` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile tests/test_frontend_browser_e2e.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check tests/test_frontend_browser_e2e.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`.
- Remaining:
  - The drawer is wired into the opt-in browser E2E path but was not run live in this pass because the browser suite is environment-gated.
  - A later UX pass can refine keyboard focus trapping and richer candidate one-page profile composition if needed.

### Company-level Public Web API/CLI-only lane landed

- Historical failure reviewed before implementation:
  - The old `Public Web Stage 2` shape mixed company-level publication discovery and per-person target-candidate enrichment inside a workflow stage.
  - Reintroducing company-level refresh through the default workflow or target-candidate UI would recreate the same ambiguity that previously caused premature Public Web labels and slow board delivery.
- Implemented:
  - Added independent PG-authoritative control-plane tables `company_public_web_asset_runs` and `company_public_web_assets`.
  - Added `src/sourcing_agent/company_public_web_assets.py` as the company-level service boundary.
  - Added `POST /api/company-assets/public-web` and `GET /api/company-assets/public-web`.
  - Added CLI commands `refresh-company-public-web-assets` and `list-company-public-web-assets`.
  - v1 is intentionally synchronous seed-URL/model-safe only. It writes auditable run rows and reusable company asset rows, excludes raw HTML/PDF/search payloads by default, does not enable default Public Web Stage 2, and does not surface target-candidate UI controls.
- Validation:
  - Added storage/service regressions for model-safe run/assets, idempotent join, force-refresh audit history, and latest-run-per-company query.
  - Added HTTP regression for trigger/list API product boundary.
  - Added CLI regressions proving refresh/list delegate to the API/CLI lane and list does not trigger refresh.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_company_public_web_assets.py tests/test_results_api.py tests/test_cli.py -k 'company_public_web_assets'` -> `7 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/company_public_web_assets.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/cli.py tests/test_company_public_web_assets.py tests/test_results_api.py tests/test_cli.py` -> passed.
- Remaining:
  - Provider-backed company crawling/RSS/arXiv/OpenReview collection should build on the new run/asset contract.
  - Target-candidate card editing/comment/detail navigation remains separate.

### Company-level Public Web provider-backed collection opt-in landed

- Historical failure reviewed before implementation:
  - Company-level Public Web must not become another implicit `Public Web Stage 2` path or a second asset store.
  - Provider collection also cannot silently fall back to seed-only completion, because that would make operators believe live discovery happened when no provider owned the work.
- Implemented:
  - `refresh_company_public_web_assets(...)` now accepts an explicit `collection_mode`.
  - Default remains `seed_url_only`; `provider_search` must be requested explicitly and must receive a search provider.
  - Provider-backed results populate the existing `company_public_web_asset_runs` and `company_public_web_assets` contract with `collection_mode=provider_search`, `provider_name`, `query_text`, `result_rank`, `source_family`, and `raw_content_included=false`.
  - Provider queries are written to `query_manifest.json`; provider results are written to `model_safe_search_results.json`; raw provider payloads are not exposed through model-safe assets or default artifacts.
  - CLI `refresh-company-public-web-assets` now supports `--collection-mode provider_search`, `--max-queries`, and `--max-results-per-query`. Seed-only remains the CLI default.
  - Orchestrator constructs the configured search provider only for explicit `provider_search`; seed-only refresh does not touch provider setup.
  - Provider exceptions mark the run `failed` instead of leaving a running row or pretending completion.
- Validation:
  - Added service regressions for provider-backed persistence/provenance, model-safe metadata sanitization, explicit-provider requirement, and provider failure terminal state.
  - Added CLI regression for default `seed_url_only` and explicit provider options.
  - Extended API-only regression to prove default HTTP refresh still has zero query/provider-result counts and remains outside default workflow.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/company_public_web_assets.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_company_public_web_assets.py tests/test_cli.py tests/test_results_api.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/company_public_web_assets.py src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_company_public_web_assets.py tests/test_cli.py tests/test_results_api.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_company_public_web_assets.py tests/test_cli.py::CliWorkflowRunnerTest::test_refresh_company_public_web_assets_command_delegates_to_api_cli_lane tests/test_cli.py::CliWorkflowRunnerTest::test_refresh_company_public_web_assets_command_passes_provider_search_options tests/test_results_api.py::ResultsApiTest::test_company_public_web_assets_api_is_api_only_and_model_safe` -> `10 passed`.
- Remaining:
  - Live RSS/arXiv/OpenReview/crawling fetchers should emit `collector_bundle` inputs through the same `company_public_web_asset_runs/assets` contract rather than introducing separate stores.
  - Provider-backed company collection is still synchronous API/CLI-only; add progress/detail payloads only if/when it becomes async.
  - It remains separate from target-candidate Public Web Search and default workflow Public Web Stage 2.

### Target-candidate Public Web cancel/retry controls landed

- Historical failure reviewed before implementation:
  - Target-candidate Public Web already had durable per-candidate runs and remote-search checkpoints, but users/operators had no first-class way to stop an in-flight remote-wait run or retry a failed/ambiguous run.
  - Without explicit control actions, cancellation and retry would fall back to repeated `force_refresh`, manual DB edits, or polluted checkpoint reuse, which is the same multi-path pattern that caused earlier lifecycle and recovery regressions.
- Implemented:
  - Added `cancel_target_candidate_public_web_run(...)` in the Public Web service boundary. It marks queued/in-flight runs terminal `cancelled`, writes cancellation metadata into summary/search/analysis checkpoints, syncs batch summary, and makes worker execution exit before provider polling/fetch/analyze if the run is already cancelled.
  - Added `POST /api/target-candidates/public-web-search/cancel` and `POST /api/target-candidates/public-web-search/retry`.
  - Cancel also terminates the matching `exploration_specialist` worker checkpoint as `cancelled`.
  - Retry creates a fresh force-refresh run/worker for retryable terminal states (`failed`, `cancelled`, `completed_with_errors`, `needs_review`) and preserves the old run as audit history instead of reopening old checkpoints.
  - Frontend target-candidate cards now show per-run cancel/retry actions only when the run state allows them; browser state remains cache/selection only.
- Validation:
  - Added service-level regression that cancelled remote-wait runs do not resume provider fetch work.
  - Added HTTP regression that cancel terminates the worker and retry creates a new queued run/worker.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_target_candidate_public_web.py tests/test_results_api.py -q -k "target_candidate_public_web"` -> `11 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/target_candidate_public_web.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py tests/test_target_candidate_public_web.py tests/test_results_api.py` -> passed.
  - `npm --prefix frontend-demo run build` -> passed.

### Target-candidate Public Web scoped polling landed

- Historical failure reviewed before implementation:
  - The target-candidate page had already moved browser state to cache/selection only, but status refresh still risked becoming inefficient for large pools: GET query strings could grow with every record id, and naive per-record polling would create N database reads.
  - Retry history also means a single candidate can have many runs; a simple global `LIMIT` over scoped records can let one candidate's historical runs evict another candidate's latest status.
- Implemented:
  - Added `POST /api/target-candidates/public-web-search/poll`, accepting scoped `record_ids` in the request body.
  - Added storage-level latest-run-per-record query for `target_candidate_public_web_runs`, using `ROW_NUMBER() OVER (PARTITION BY record_id ORDER BY updated_at DESC, created_at DESC, run_id DESC)` on both PG and SQLite paths.
  - Updated frontend polling to use POST scoped polling whenever target-candidate record ids are known, with one latest run returned per record.
- Validation:
  - Added storage regression proving retry history for one record does not evict another record's latest run.
  - Added HTTP regression covering scoped POST polling.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_target_candidate_public_web.py tests/test_results_api.py -q -k "target_candidate_public_web"` -> `11 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py tests/test_target_candidate_public_web.py tests/test_results_api.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`.
  - `git diff --check` -> passed.

### Authoritative coverage hidden legacy inference removed from normal reads

- Historical failure reviewed before implementation:
  - `organization_asset_registry.authoritative=true` is a serving pointer, not a full-company proof.
  - Even after write-time `population_coverage` was added, historical rows could still rely on hidden `legacy_standard_bundle`, high-volume lane, or company-employee shard inference inside the normal planner/profile path.
  - That kept the old failure mode alive: a scoped or partially migrated authoritative row could still look like full-company coverage without an explicit, auditable contract.
- Implemented:
  - `build_population_coverage_contract(...)` now defaults to strict mode. Normal planner, organization execution profile, execution semantics, audit, and explain reads do not treat legacy signals as full-company proof.
  - `allow_legacy_inference=True` is reserved for `backfill-authoritative-population-coverage` and guarded authoritative write-time publication; those paths convert legacy proof into explicit `population_coverage` metadata before normal reads can reuse it.
  - Strict contracts expose `legacy_inference_suppressed=true` and `legacy_population_coverage_inference_suppressed` reason codes when old proof signals exist but have not been persisted.
  - Updated explain fixtures so full-local reuse cases carry explicit `population_coverage`, and added a regression proving a row with only `standard_bundles` now plans `delta_from_snapshot`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_coverage_backfill.py tests/test_organization_execution_profile.py tests/test_asset_reuse_audit.py tests/test_workflow_explain.py -q -k "population_coverage or scoped_only or exact_scoped or full_company_filter or full_local_baseline or legacy_standard_bundle or large_directional_baseline_without_shard"` -> `11 passed, 2 subtests passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_coverage_backfill.py tests/test_asset_reuse_audit.py tests/test_organization_execution_profile.py tests/test_workflow_explain.py tests/test_execution_semantics.py tests/test_planning_modules.py -q -k "coverage or authoritative or reuse or scoped_only or full_company_filter or execution_semantics"` -> `46 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_coverage_contracts.py src/sourcing_agent/asset_coverage_backfill.py src/sourcing_agent/asset_reuse_planning.py tests/test_asset_coverage_backfill.py tests/test_workflow_explain.py tests/test_planning_modules.py` -> passed.

### Search-seed discovery durable-owner hard gate closed

- Historical failure reviewed before implementation:
  - Discovery/provider queue work had moved normal completion toward `search_seed_discovery_query` + `local_apply_closure`, but service metrics only inspected existing durable items.
  - A completed search-seed worker without either item could still look invisible to smoke guardrails, leaving room for worker-summary merge scans to creep back as an implicit recovery path.
- Implemented:
  - `workflow_service_metrics.search_seed_discovery_queue` now cross-checks completed search-seed workers against `search_seed_discovery_query.source_worker_ids` and `local_apply_closure.source_worker_ids` where `metadata.worker_kind=search_seed`.
  - New metrics include `completed_search_seed_worker_count`, `discovery_worker_without_item_count`, `discovery_worker_without_local_apply_count`, and `discovery_worker_owner_gap_count`.
  - `require_no_service_recovery_violation=true` now fails on completed discovery workers missing either owner; bottlenecks include `search_seed_discovery_worker_owner_gap`.
  - Updated `docs/DISCOVERY_PROVIDER_QUEUE_CONTRACT.md`, `docs/TESTING_PLAYBOOK.md`, and `docs/NEXT_TODO.md` so this is treated as a hard production gate, not a passive observation.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "search_seed_discovery_queue_guardrails or service_recovery_violations"` -> `2 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "search_seed_discovery or service_recovery_violation or workflow_service_metrics"` -> `18 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> `coverage=20/20 tags, 9 matrices, 11 cases`.

### Service-gate expectation registry landed

- Historical failure reviewed before implementation:
  - The service-gate manifest required specific smoke expectation names, but matrix `expectations` were still free-form dictionaries.
  - A misspelled or unsupported key could make a scripted/browser gate look present in review while the runner silently ignored it, recreating the same "observable but not enforced" failure class as earlier progress/recovery reports.
- Implemented:
  - Added `smoke_expectation_contract.py` as the centralized registry for supported smoke expectation keys.
  - `workflow_smoke.load_smoke_cases(...)` now rejects unknown case expectation keys before execution.
  - `_evaluate_smoke_expectations(...)` also fails closed at runtime if unsupported keys reach evaluation.
  - `service_gate_coverage.validate_service_gate_coverage(...)` validates manifest `required_*_expectations` and every matrix case's `expectations` against the same registry.
- Validation:
  - Added regressions for load-time rejection, runtime rejection, and manifest/case coverage validation.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_smoke.py -k 'service_gate_coverage or expectation or scripted_smoke_matrices'` -> `33 passed, 37 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` -> `status=ok`, `coverage=20/20 tags`, `9 matrices`, `11 cases`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/smoke_expectation_contract.py src/sourcing_agent/service_gate_coverage.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/smoke_expectation_contract.py src/sourcing_agent/service_gate_coverage.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_markdown_status.py` -> `1 passed`.

### Remote actor slot occupancy gate landed

- Historical failure reviewed before implementation:
  - OpenAI Infra/Agent/ChatGPT runs exposed that "no slot-underuse violation" is not the same as proving actor quota was actually saturated.
  - A terminal worker snapshot cannot answer this because valid worker completion/cleanup naturally reduces active provider slots to zero after the expensive phase.
- Implemented:
  - Provider case reports now expose `remote_actor_slot_observation`, computed from smoke-observed `waiting_remote_harvest_count` peak divided by the explicit `harvest_profile_actor_global_inflight` budget.
  - Added `min_remote_actor_slot_occupancy_ratio` to the smoke expectation registry and runtime evaluator. Missing observation fails closed when the expectation is configured.
  - Added the gate to high-capacity OpenAI Agent/ChatGPT profile-tail matrices and to the manifest tags that claim profile-tail / tiny-tail slot-utilization coverage.
  - Low-volume/zero-current-small-delta cases are intentionally not forced to saturate actor slots; those are governed by tiny-batch reason and low-volume policy gates instead.
- Validation:
  - Added regressions for low slot occupancy failure, report-level occupancy calculation, and summary rollup.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest -q tests/test_workflow_smoke.py -k 'remote_actor_slot or provider_roster_profile_and_board_metrics or materialization_streaming_rollup or service_gate_coverage or expectation'` -> `35 passed, 36 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` -> `status=ok`, `coverage=20/20 tags`, `9 matrices`, `11 cases`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/smoke_expectation_contract.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed.

### ChatGPT scripted provider-event completion semantics repaired

- Historical failure reviewed before implementation:
  - The OpenAI ChatGPT baseline+delta full scripted smoke failed `min_out_of_order_profile_completion_count` even though actor-slot occupancy was saturated.
  - The root cause was the scripted webhook driver: it wrote Apify `finishedAt` as the local webhook-send time, not the remote actor ready/completed time. Because worker-timeline metrics correctly prioritize `remote_provider_event` over worker terminal markers, late webhook delivery overwrote the true remote completion order and hid the intended out-of-order inversion.
- Implemented:
  - Scripted Apify webhook payloads now set `eventData.finishedAt` from the worker checkpoint's `scripted_remote_ready_epoch_ms` when available, falling back to current time only for non-scripted/non-ready workers.
  - Added a smoke unit regression proving `_smoke_remote_provider_webhook_payload(...)` preserves the remote ready timestamp as actor `finishedAt`.
  - Kept the metric priority unchanged: `remote_provider_event` remains the canonical completion source; the scripted event now carries the correct remote completion timestamp.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'provider_webhook or out_of_order_profile_completion or expectation'` -> `35 passed, 37 deselected`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q -k 'out_of_order or remote_provider_completion'` -> `4 passed, 12 deselected`.
  - Full ChatGPT strict scripted smoke passed with `expectation_failures=[]`: `output/scripted_smoke_current/openai_chatgpt_actor_slot_occupancy_report_fix.json`, `output/scripted_smoke_current/openai_chatgpt_actor_slot_occupancy_summary_fix.json`.
  - Final smoke metrics: `remote_actor_slot_peak_occupancy_ratio=1.0`, `profile_batch_inversion_count=1`, `remote_provider_event_count=9`, `late_remote_provider_event_count=5`, `max_remote_to_local_event_lag_ms=8612`, and no remote-event lag violation.

### OpenAI Agent baseline+delta service gate repaired and revalidated

- Historical failure reviewed before implementation:
  - The seeded OpenAI Agent scoped-delta gate was supposed to prove baseline + delta streaming, partial board visibility, out-of-order profile completion, and post-terminal recovery together.
  - The local matrix still carried a legacy `force_fresh_run=true`, which correctly forced `new_job/scoped_live_search` and prevented the gate from exercising baseline reuse. Because the matrix also lacked explicit explain-strategy expectations, this drift was only exposed indirectly through the out-of-order gate.
  - The out-of-order metric also used only real remote-provider events or local worker terminal timestamps. Scripted remote-wait workers can be closed by local recovery before a webhook event is recorded, and SQLite/trace timestamps can collapse multiple profile workers into the same second. That made a valid scripted out-of-order fixture report `profile_batch_inversion_count=0`.
- Implemented:
  - `workflow_service_metrics.worker_timeline` now records a scripted-only `scripted_remote_ready_at` timestamp from worker checkpoints when `provider_mode=scripted` and `scripted_remote_wait_after_submit=true`.
  - Profile out-of-order completion detection now uses completion source priority `remote_provider_event > scripted_remote_ready > worker_terminal_marker`, and treats same-second starts as ordered by `worker_id` so SQLite timestamp precision does not hide later-worker-first-completion inversions.
  - Added regression coverage for scripted remote-ready completion and same-second worker start ordering.
  - Tightened `configs/scripted/openai_agent_scoped_delta_smoke_matrix.json`: the main baseline+delta case no longer sets legacy `force_fresh_run=true`, and it now explicitly requires `dispatch_strategy=delta_from_snapshot`, `planner_mode=delta_from_snapshot`, `requires_delta_acquisition=true`, `effective_acquisition_mode=baseline_reuse_with_delta`, and keyword `Agent`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q -k "out_of_order"` -> `4 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py tests/test_workflow_service_metrics.py` -> passed.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "service_gate_coverage_manifest or scripted_smoke_matrices_enable_service_recovery_hard_gates or out_of_order"` -> `4 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py tests/test_harvest_connectors.py -q -k "openai_agent_scoped_delta_streaming_fixture or out_of_order_smoke_fixtures or scripted_remote_wait"` -> `4 passed`.
  - OpenAI Agent seeded strict smoke passed with `expectation_failures=[]`: `output/scripted_smoke_current/openai_agent_scoped_delta_service_gate_seeded_20260503c_report.json`, `output/scripted_smoke_current/openai_agent_scoped_delta_service_gate_seeded_20260503c_summary.json`.
  - Final smoke metrics: explain `delta_from_snapshot / baseline_reuse_with_delta`, lifecycle `baseline_snapshot_id=20260414T120300`, `current_snapshot_id=20260503T231808`, `served_candidate_count=597`, `delta_profile_required/fetched/materialized/board_visible=297/297/297/297`, `patch_log_count=5`, `job_to_board_visible_partial_ms=11000`, `final_results_to_board_nonempty_ms≈454`, `profile_batch_inversion_count=3`, `post_terminal_recovery.settled=true`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` now reports all `20/20` manifest tags covered with no planned gaps.
- Remaining:
  - OpenAI Agent baseline+delta service simulation is closed locally. Next workflow-simulation work should focus on broad matrix reruns / browser variants only if we need UI-rendered confidence before ECS sync; the underlying local service-gate manifest no longer has missing required tags.

### Google Veo/Nano authoritative shard-reuse service gate promoted

- Historical failure reviewed before implementation:
  - Gemini negative coverage proves incomplete/missing scoped shard proof must not widen into full local reuse. The positive counterpart is equally important: when exact Google Veo/Nano Banana scoped shard proof exists, the planner must not schedule provider work or drift into `delta_from_snapshot`.
  - The first local smoke run failed correctly: the query normalized to canonical `Multimodal`, while the seed only had `Veo` / `Nano Banana` shard rows. Production gates must seed coverage under the planner's canonical coverage keys, not just the literal user terms.
- Implemented:
  - Added `configs/scripted/google_veo_nano_authoritative_shard_reuse_smoke_matrix.json` with hard expectations for `reuse_snapshot`, `reuse_snapshot_only`, `requires_delta_acquisition=false`, `effective_acquisition_mode=full_local_asset_reuse`, zero provider invocations, no active Stage 1 wording, bounded progress payload, terminal recovery settle, and board non-empty SLOs.
  - Promoted `google_veo_nano_authoritative_shard_reuse_browser` to `required_now` in `configs/scripted/service_gate_coverage_manifest.json`.
  - Extended `seed_reference_smoke_runtime(...)` so Google authoritative assets include selected exact scoped shard snapshots for `Veo`, `Nano Banana`, and canonical `Multimodal` current/former rows.
  - Fixed the reference seed helper to preserve existing `source_snapshot_selection` metadata, including `population_coverage`, when appending selected source shard snapshots.
  - Added `max_provider_invocation_count` smoke expectation support so reuse-only gates can fail on any unexpected provider dispatch instead of relying on absence of minimum-provider assertions.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_smoke_runtime_seed.py tests/test_workflow_smoke.py -q -k "seed_reference or service_gate_coverage_manifest or zero_provider_reuse"` -> `4 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` -> passed; coverage is now `18/20`.
  - Google Veo/Nano strict smoke passed with `expectation_failures=[]`: `output/scripted_smoke_current/google_veo_nano_authoritative_shard_report.json`, `output/scripted_smoke_current/google_veo_nano_authoritative_shard_summary.json`.
  - Final metrics: `dispatch_strategy=reuse_snapshot`, `planner_mode=reuse_snapshot_only`, `requires_delta_acquisition=false`, `effective_acquisition_mode=full_local_asset_reuse`, `provider_invocation_count=0`, latest lifecycle `baseline_serving` with `delta_profile_progress_applicable=false`, max `/progress` payload `8961` bytes, `job_to_board_nonempty_ms≈2862`, `final_results_to_board_nonempty_ms≈168`, `post_terminal_recovery.settled=true`.
  - `scripts/check_service_gate_coverage.py --require-before-ecs-sync` now intentionally fails only on `meta_agent_full_reuse_execution_timeline_browser` and `partial_delta_board_row_streaming`.
- Remaining:
  - Google authoritative positive/negative service gates are closed locally. Remaining before-ECS simulation gates are Meta full-reuse rendered timeline and row-level partial delta board streaming.

### Google Gemini incomplete-shard guard promoted to required smoke coverage

- Historical failure reviewed before implementation:
  - Google authoritative serving pointers can legitimately provide a reusable baseline, but they must not rewrite a scoped Gemini request into full local reuse unless exact scoped shard coverage or an explicit full-company-filter contract satisfies the user boundary.
  - The first local Gemini smoke attempt exposed a simulation-quality bug rather than a planner bug: the reference Google authoritative row claimed `6000` baseline candidates while `candidate_documents.json` contained only `3` rows. Final serving could only produce `69` rows (`3 + 66`), so the service guardrail correctly failed on `raw_delta_only_result_view_served` and public count regression.
  - Production-grade scripted gates cannot use impossible fixtures. Registry counts, population coverage proof, and serviceable candidate artifacts must agree, otherwise lifecycle/progress metrics become false positives or false negatives.
- Implemented:
  - Added `configs/scripted/google_gemini_incomplete_shard_streaming.json`, a deterministic Google Gemini fixture where a selected authoritative Google baseline exists but exact Gemini shard coverage is missing; scripted Harvest returns `48` current and `18` former Gemini rows, then profile batches run through remote-wait, retryable, timeout, webhook/recovery, local apply, materialization, and finalization paths.
  - Added `configs/scripted/google_gemini_incomplete_shard_smoke_matrix.json` with hard expectations for `delta_from_snapshot`, `requires_delta_acquisition=true`, `baseline_reuse_with_delta`, coherent Stage 1 counters (`48/18/66/66`), terminal recovery settle, progress/lifecycle/board-visible guardrails, and provider efficiency metrics.
  - Promoted `google_gemini_incomplete_shard_no_full_reuse` from planned ECS-before-sync coverage to `required_now` in `configs/scripted/service_gate_coverage_manifest.json`.
  - Fixed the reference smoke seed: Google baseline now writes `300` real candidate docs with explicit `population_coverage` and matching lane counts (`260` current / `40` former), instead of claiming a non-serviceable `6000` row baseline.
- Regression coverage:
  - Added `test_google_gemini_incomplete_shard_fixture_reproduces_no_full_reuse_shape`.
  - Existing manifest coverage tests now require the Gemini gate to remain covered by a local smoke matrix with progress/recovery/board/lifecycle expectations.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_provider_scenario.py -q -k "google_gemini_incomplete_shard or openai_infra_stage1_lane_skew or openai_whisper_zero_current_overlay"` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_smoke_runtime_seed.py tests/test_scripted_provider_scenario.py -q -k "seed_reference or google_gemini_incomplete_shard"` -> `2 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "service_gate_coverage_manifest or scripted_smoke_matrices_enable_service_recovery_hard_gates"` -> `3 passed`.
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/smoke_runtime_seed.py tests/test_scripted_provider_scenario.py` -> passed.
  - Gemini strict smoke passed with `expectation_failures=[]`: `output/scripted_smoke_current/google_gemini_incomplete_shard_report.json`, `output/scripted_smoke_current/google_gemini_incomplete_shard_summary.json`.
  - Final metrics: `dispatch_strategy=delta_from_snapshot`, `planner_mode=delta_from_snapshot`, `requires_delta_acquisition=true`, `effective_acquisition_mode=baseline_reuse_with_delta`, `baseline_directional_local_reuse_eligible=false`, Stage 1 `current=48 / former=18 / deduped=66 / fetched=66`, lifecycle `baseline_candidate_count=300`, `served_candidate_count=366`, `delta_profile_materialized_count=66`, `delta_profile_board_visible_count=66`, `post_terminal_recovery.settled=true`.
  - `scripts/check_service_gate_coverage.py --json` now reports `17/20` tags covered; `--require-before-ecs-sync` intentionally fails with 3 remaining planned gaps: Meta full-reuse browser timeline, Google Veo/Nano browser authoritative reuse, and partial delta row streaming.
- Remaining:
  - Gemini incomplete-shard planning + downstream smoke is closed locally. Superseded by the later Google Veo/Nano entry above: the positive authoritative shard-reuse path is now also covered locally.

### OpenAI Health/Whisper zero-current overlay promoted to required smoke coverage

- Historical failure reviewed before implementation:
  - Hosted OpenAI Health/Whisper runs can legitimately return `0` current-lane rows while former-lane scoped search returns a non-empty delta.
  - The correct terminal serving state is still baseline + delta/full current-snapshot serving, not a raw delta-only board and not a public-read repair that later guesses counts from legacy projection sources.
  - The latest local failure reproduced the service-level bug: board metrics could infer visibility from a full current snapshot, but `job_result_lifecycle` still had `delta_profile_board_visible_count=0` and no `serving_projection_id`.
- Implemented:
  - `_persist_job_result_view(...)` now records current-snapshot serving publication through the canonical lifecycle writer when it publishes an asset-population result view.
  - `_reconcile_completed_workflow_after_harvest_prefetch(...)` now advances lifecycle materialized/board-visible counters even when the result view already points at the same current snapshot; snapshot-id changes are no longer the only publication trigger.
  - `update_job_result_lifecycle_from_materialization(...)` treats delta required/fetched/applied/materialized/board-visible counters as monotonic event-time counters so a later partial writer cannot regress them.
  - The Health/Whisper smoke matrix now treats a single 12-profile batch as the expected efficient shape. It still requires provider/profile/materialization/board/lifecycle SLOs, but does not require `global_next_worker_start_gap_ms`, because no next-worker handoff exists when the scheduler correctly merges all URLs into one batch.
- Regression coverage:
  - Added `test_persist_job_result_view_records_current_snapshot_serving_lifecycle`, proving result-view publication writes the serving projection before public reads.
  - Extended the harvest-prefetch reconcile regression to cover same-snapshot materialization advancing board-visible lifecycle counters.
  - Updated the smoke matrix coverage self-test so worker handoff SLO is required for explicit `provider_handoff_slo` or multi-worker gates, not for efficient single-batch cases.
- Validation:
  - Health/Whisper scripted smoke passed with `expectation_failures=[]`.
  - Final lifecycle: `state=current_snapshot_serving`, `baseline_candidate_count=300`, `served_candidate_count=312`, `delta_profile_required_count=12`, `delta_profile_fetched_count=12`, `delta_profile_materialized_count=12`, `delta_profile_board_visible_count=12`, and `serving_projection_phase=current_snapshot_serving`.
  - Final board metrics: `full_snapshot_serving=true`, `patch_log_required=false`, `projection_present=true`, `materialization_lag_violation=false`.
  - `scripts/check_service_gate_coverage.py --json` now reports all `required_now` tags covered; `--require-before-ecs-sync` still intentionally fails with 7 planned gaps.
- Remaining:
  - Health/Whisper zero-current overlay is closed locally. The next planned ECS-before-sync gaps are browser-rendered Meta full reuse, Google Gemini/Veo/Nano authoritative coverage, late webhook duplicate idempotency, writer contention, tiny-tail coalescing, and row-level partial delta streaming.

### OpenAI Infra Stage 1 lane-skew promoted to required smoke coverage

- Historical failure reviewed before implementation:
  - ECS job `c5248ea4b3b4` (`帮我找OpenAI做Infra方向的人`) exposed a mixed-source progress shape: the execution page displayed `current=0`, `former=10`, and a much larger profile denominator (`77`/later `89`) while provider/profile worker state was advancing separately.
  - A true zero-result current lane is valid, but public Stage 1 business counters must come from one coherent lane snapshot. Worker URL queue size can be exposed as technical metadata, but it must not become `deduped` / `profile_fetch_required` when lane-returned counts do not support it.
- Implemented:
  - Added `configs/scripted/openai_infra_stage1_lane_skew_streaming.json`, a scripted Harvest fixture for `帮我找OpenAI做Infra方向的人`: current-scoped profile search returns a true zero-result response, former-scoped profile search returns a 77-person delta, and profile batches include retryable timeout/delay plus out-of-order completion.
  - Added `configs/scripted/openai_infra_stage1_lane_skew_smoke_matrix.json` with exact latest Stage 1 expectations: `current=0`, `former=77`, `all=0`, `deduped=77`, `profile_required=77`, `profile_fetched=77`, plus terminal, recovery, progress-contract, service-recovery, serving-publication, latency, and batch-efficiency gates.
  - Added explicit scoped-lane zero-result semantics: scoped/current-former Harvest people-search lanes may set `provider_people_search_accept_zero_results=true`, so a retry-exhausted true zero result completes the lane as `zero_result_accepted` instead of creating a terminal `provider_search_retry` item. The default provider-anomaly protection remains for callers that do not opt into this lane semantics.
  - Promoted `openai_infra_stage1_lane_skew_zero_current_former_delta` from `required_before_ecs_sync` to `required_now` in the service-gate coverage manifest.
  - Strengthened `workflow_smoke._build_progress_observability_report(...)`: when Stage 1 lane-returned counts are present, `deduped_candidate_count` and `deduped_profile_url_count` must not exceed `current + former + all`. This catches the historical `0 current / 10 former / 77 required` mixed-source shape before browser/ECS testing.
  - Extended smoke expectations with `expect_latest_stage1_all_search_returned_count`.
- Regression coverage:
  - Added a direct progress-observability regression for the historical mixed-source shape.
  - Added a scripted fixture validation proving the OpenAI Infra scenario has a true zero current rule, a 77-row former delta rule, staged provider behavior, retryable error, timeout, and partial-result coverage.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/openai_infra_stage1_lane_skew_20260503 --seed-reference-runtime --provider-mode scripted --scripted-scenario configs/scripted/openai_infra_stage1_lane_skew_streaming.json --matrix-file configs/scripted/openai_infra_stage1_lane_skew_smoke_matrix.json --case openai_infra_stage1_lane_skew_zero_current_former_delta --fast-runtime --runtime-tuning-profile fast_smoke --poll-seconds 0.1 --max-poll-seconds 180 --strict --timing-summary --report-json output/scripted_smoke_current/openai_infra_stage1_lane_skew_report.json --summary-json output/scripted_smoke_current/openai_infra_stage1_lane_skew_summary.json` -> passed.
  - Final smoke metrics: `current_search_returned_count=0`, `former_search_returned_count=77`, `all_search_returned_count=0`, `deduped_candidate_count=77`, `deduped_profile_url_count=77`, `profile_fetch_required_count=77`, `profile_fetched_count=77`, `post_terminal_recovery.settled=true`, `progress_contract_violation_detected=false`, `expectation_failures=[]`.
  - The final public lifecycle still reports `serving_projection_phase=partial_delta_overlay`, `served_candidate_count=358`, `delta_profile_materialized_count=58/77`, and board-visible non-empty latency around `16.7s` from job start. That is acceptable for this lane-coherence gate; row-level partial delta visibility remains tracked separately under `partial_delta_board_row_streaming`.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` -> passed; current coverage is `12/20`, with `8` `required_before_ecs_sync` planned gaps.
- Remaining:
  - Superseded by the later Health/Whisper entry above: `--require-before-ecs-sync` now fails with 7 planned gaps after Health/Whisper promotion.

### Service-gate coverage manifest added for historical incident shapes

- Historical failure reviewed before implementation:
  - Service-level scripted/browser requirements were scattered across `NEXT_TODO.md`, the testing playbook, and individual smoke matrices.
  - That allowed a future matrix to keep terminal/recovery assertions while silently dropping coverage for concrete incident shapes such as full-local reuse progress, baseline+delta board streaming, out-of-order profile completion, or provider handoff SLOs.
  - The production-grade shape should separate "covered now" from "known required before ECS sync" instead of claiming all historical cases are already represented.
- Implemented:
  - Added `configs/scripted/service_gate_coverage_manifest.json` as the machine-checkable coverage contract.
  - Added reusable `sourcing_agent.service_gate_coverage.validate_service_gate_coverage(...)` and `scripts/check_service_gate_coverage.py`.
  - Added case-level `coverage_tags` to local smoke matrices and `scripted_scenario` references for provider-backed scripted cases.
  - `workflow_smoke.load_smoke_cases(...)` now preserves `coverage_tags` / `scripted_scenario`, and `run_hosted_smoke_matrix(...)` echoes them into case summaries for future reporting/browser consumption.
  - The manifest marks current OpenAI Reasoning full-local reuse, OpenAI Agent scoped delta, OpenAI ChatGPT baseline+delta, Lovable 100+ live roster, provider handoff, out-of-order profile completion, retryable profile batch, and baseline+delta board streaming as `required_now`.
  - Superseded by later promotions: OpenAI Infra lane skew and OpenAI Health/Whisper zero-current overlay are now `required_now`; Meta Agent full-reuse browser timeline, Google Gemini incomplete shard, Google Veo/Nano authoritative reuse browser, late webhook duplicate idempotency, writer contention, tiny-tail slot utilization, and partial delta row streaming remain `required_before_ecs_sync` with explicit gap and promotion gate.
- Regression coverage:
  - Added `test_service_gate_coverage_manifest_matches_local_smoke_matrices`, which fails if a local `*smoke_matrix.json` case has no tags, uses an unknown tag, points to a missing scripted scenario, or if any `required_now` tag lacks local smoke coverage / required SLO guardrails.
  - Added an ECS-sync-mode regression proving `--require-before-ecs-sync` remains failed while known historical incident shapes have not been promoted from planned coverage into hard gates.
  - Extended the load-case regression so coverage metadata survives matrix normalization instead of being doc-only.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'load_smoke_cases_preserves_expectations or scripted_smoke_matrices_enable_service_recovery_hard_gates or service_gate_coverage_manifest'` -> `4 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/service_gate_coverage.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py scripts/check_service_gate_coverage.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` -> passed; reported `11/20` tags covered by `4` local smoke cases and `9` `required_before_ecs_sync` gaps.
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --require-before-ecs-sync` -> expected failure; reported the 9 historical tags that still need scripted/browser promotion before ECS sync.
  - `PYTHONPATH=src ./.venv-tests/bin/python -m json.tool configs/scripted/service_gate_coverage_manifest.json` and all local `*smoke_matrix.json` -> passed

### Tiny-batch and provider-slot efficiency gates wired into smoke expectations

- Historical failure reviewed before implementation:
  - OpenAI Infra exposed tiny `2` / `3` person profile batches while provider coordination overhead, webhook/watcher delay, and local apply recovery made small envelopes inefficient.
  - Event-level efficiency metrics already counted unexplained tiny batches and provider slot underuse, but local smoke expectations only had the broad `require_no_event_level_efficiency_violation` switch.
  - A production-grade smoke matrix should explicitly fail on the batch-efficiency dimensions that matter, so later scheduler changes cannot hide regressions behind a generic violation flag.
- Implemented:
  - `_evaluate_smoke_expectations(...)` now supports `min_profile_batch_envelope_count`, `min_profile_tiny_batch_coalesced_count`, `min_profile_prefetch_batch_plan_count`, `max_profile_unexplained_tiny_batch_count`, `max_provider_slot_underuse_with_backlog_count`, and `max_profile_prefetch_underfilled_with_deferred_count`.
  - These gates fail closed if the event-level efficiency report is missing.
  - OpenAI Agent, OpenAI ChatGPT, and Lovable smoke matrices now require at least one profile batch envelope and require zero unexplained tiny batches / provider-slot-underuse-with-backlog.
  - The service-gate manifest now requires those explicit efficiency expectation keys for the current provider-backed tags.
  - `tiny_tail_coalescing_slot_utilization` remains `required_before_ecs_sync`: the generic no-regression gate exists now, but a dedicated realistic tail fixture still needs to prove coalescing actually happens under queue pressure.
- Regression coverage:
  - Added `test_evaluate_smoke_expectations_rejects_tiny_batch_and_slot_underuse_metrics`, covering both metric violations and missing event-level efficiency reports.
  - The manifest coverage test now prevents provider-backed required tags from dropping the explicit tiny-batch/slot-underuse expectations.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'tiny_batch_and_slot_underuse or service_gate_coverage_manifest or scripted_smoke_matrices_enable_service_recovery_hard_gates'` -> `5 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python scripts/check_service_gate_coverage.py --json` -> passed

### Scripted SLO gates fail closed on missing required metrics

- Historical failure reviewed before implementation:
  - Previous smoke SLO checks converted missing service metrics to `0`, so a matrix could pass `max_*_ms` gates by omitting the corresponding report.
  - That is not production-grade: service-level validation must prove both "metric exists" and "metric is within budget"; otherwise hosted/browser gaps can reappear during manual ECS testing.
  - A full-local smoke run exposed a second metric-quality failure: `job_to_board_nonempty_ms` could become `0.0` because backend stage timestamps completed in the same second and suppressed the smoke runner's user-visible observation.
  - The same smoke run exposed a page-coherence failure: `/progress.counters.result_count` could drop from the asset-population served count (`300`) to ranked top-k rows (`10`) after final results, because `_public_progress_result_count(...)` read ranked rows before canonical serving counts.
- Implemented:
  - `_evaluate_smoke_expectations(...)` now treats configured SLO maxima as requiring their metric payload when the metric is expected.
  - User-facing board latency metrics and worker handoff gap metrics fail with `metric missing` when absent.
  - `max_remote_provider_event_lag_ms` is required only for provider-backed cases, detected from actual provider invocations, observed remote actor/event metrics, `min_remote_actor_worker_count`, or `min_provider_invocations_by_logical_name`; full-local/reuse-only cases may omit remote provider event metrics without failing solely on that absent report.
  - `max_serving_publication_gap_ms` requires the age metric only when a serving gap is present.
  - Local scripted smoke matrix config validation now requires board-nonempty SLOs on every case, worker handoff SLOs on provider-backed cases, and preview-to-final SLOs on cases that require post-preview finalization observation.
  - OpenAI Agent, OpenAI ChatGPT, and Lovable small-company smoke matrices now declare those SLO budgets instead of relying only on recovery/terminal correctness gates.
  - Workflow wall-clock UX metrics now prefer smoke-runner observed milestones over backend stage timestamps, so SLOs measure when the user/test runner can actually see preview/final/board state. Backend stage timestamps remain useful for stage processing diagnostics, but they no longer undercut user-visible latency gates.
  - `/progress` result counters now prefer canonical `job_result_lifecycle.served_candidate_count`, then result-view lifecycle/summary counts, and only fall back to ranked-result rows when no serving count exists.
  - `require_no_progress_contract_violation=true` now fails on generic public counter regressions (`counter_regressions`) in addition to Stage 1 regressions, lifecycle regressions, and explicit invariant violations.
- Regression coverage:
  - Added smoke expectation tests for missing user-experience SLO metrics, missing worker handoff metrics, no-provider remote-event omission, and provider-backed remote-event omission.
  - Extended `test_scripted_smoke_matrices_enable_service_recovery_hard_gates` so future local matrices cannot omit the UX/handoff SLOs.
  - Added wall-clock regressions proving zero-second backend stage timestamps fall back to observed completion and that client timeline wins for user-visible latency.
  - Added a progress-counter regression proving asset-population jobs keep the canonical served count even when ranked top-k rows exist.
  - Extended the progress-contract expectation regression to prove public counter regressions are hard failures, not report-only diagnostics.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'service_slo or remote_event_metric or worker_handoff_slo_metric or user_experience_slo_metric or scripted_smoke_matrices_enable_service_recovery_hard_gates'` -> `6 passed, 40 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'scripted_smoke_matrices_enable_service_recovery_hard_gates'` -> `1 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'workflow_wall_clock or provider_case_report_prefers_client_timeline or service_slo or scripted_smoke_matrices_enable_service_recovery_hard_gates'` -> `4 passed, 43 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'progress_contract_violations or progress_contract_observability or progress_observability_report_surfaces_counter_regressions'` -> `4 passed, 43 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'get_job_progress_prefers_served_result_view_count_over_ranked_top_k_count or get_job_progress_uses_count_queries_instead_of_loading_full_results'` -> `2 passed, 357 deselected`
  - Full-local smoke: `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/full_local_reuse_slo_gate_20260503f --seed-reference-runtime --provider-mode simulate --matrix-file configs/scripted/full_local_reuse_smoke_matrix.json --case openai_reasoning_reuse_snapshot_only --fast-runtime --runtime-tuning-profile fast_smoke --poll-seconds 0.1 --max-poll-seconds 60 --strict --timing-summary --report-json output/scripted_smoke_current/full_local_reuse_slo_gate_20260503f_report.json --summary-json output/scripted_smoke_current/full_local_reuse_slo_gate_20260503f_summary.json` -> passed; observed `progress_observability.regression_detected=false`, latest progress `result_count=300`, `job_to_board_nonempty≈6314ms`, expectation failures `[]`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_smoke.py` -> passed

### Excel completion progress exposes row-manifest counts

- Historical failure reviewed before implementation:
  - The backend already persisted a compact Excel row manifest, but the batch-group UI still showed only generic completion prose.
  - That left users unable to see matched / manual-review / unresolved row outcomes immediately after upload, and encouraged future frontend code to parse timeline text or reopen sidecar files.
- Implemented:
  - `/api/jobs/{job_id}/progress` now exposes `excel_intake_progress` for `excel_intake` jobs with total rows, matched rows, target-candidate count, manual-review rows, unresolved/invalid rows, status counts, and row-manifest availability/truncation.
  - `execution_phase_contract.active_phase_detail` uses the same structured counts for completed Excel jobs.
  - The frontend maps `excel_intake_progress` into `RunStatusData.excelIntakeProgress` and renders the batch-group completion line from that structured contract.
- Regression coverage:
  - The Excel workflow result-view regression now asserts `/progress` exposes the row-manifest counts and the completed phase detail includes the same matched/manual-review counts.
  - Browser E2E now captures row-manifest summaries after the import/export action flow and asserts matched/manual-review/unresolved counts are visible.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_history_recovery.py -q -k 'excel_intake_workflow'` -> `4 passed`
  - `SOURCING_RUN_FRONTEND_BROWSER_E2E=1 PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'excel_intake_upload'` -> `1 passed, 11 deselected`
  - `npm run build` in `frontend-demo/` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_frontend_history_recovery.py tests/test_frontend_browser_e2e.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `git diff --check` -> passed

### Completed-harvest coalescing regression uses durable local-apply items

- Historical failure reviewed before implementation:
  - Normal provider completion now hands off to durable `local_apply_closure` items, but one completed-harvest inflight coalescing regression still called `_process_inline_incremental_worker_batch(...)` directly.
  - Keeping tests on the retired lower-level entrypoint makes future refactors preserve a path that production no longer uses.
- Implemented:
  - `test_completed_workflow_harvest_callback_coalesces_when_reconcile_inflight` now enqueues a `local_apply_closure` item from the completed worker and drains `_run_local_apply_closure_item_queue_once(...)`.
  - The test still proves an already-held completed-workflow reconcile lease coalesces before materialization; the item becomes retryable/deferred with `completed_workflow_reconcile_inflight` instead of invoking the duplicate reconcile callback.
  - `rg "_process_inline_incremental_worker_batch(" tests` now has no direct test callers; remaining code references are the item/direct-repair processors themselves.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'completed_workflow_harvest_callback_coalesces_when_reconcile_inflight or local_apply_closure'` -> `5 passed, 353 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check tests/test_pipeline.py` -> passed

### Excel target-candidate actions use the current-job marker

- Historical failure reviewed before implementation:
  - Excel intake result views intentionally serve the whole company asset board, but the target-candidate import action was still implemented as a generic candidate-page walk.
  - That meant a completed Excel group could import baseline/company-snapshot candidates, not only the rows matched or created by the current workbook.
- Implemented:
  - `import_target_candidates_from_job` now treats `workflow_kind=excel_intake` as a scoped action and resolves the canonical `excel_intake:current_job` marker from the job result view / summary contract.
  - Excel imports only upsert candidates listed by that marker and write target-candidate metadata with marker lineage (`marker_id`, label, intake id, resolved source, import scope).
  - Excel workflow completion now writes a bounded sidecar `*.excel_intake_row_manifest.json` with matched rows, manual-review rows, unresolved rows, status counts, and truncation metadata; hot job summaries keep only the manifest reference and counts.
  - Target-candidate ZIP exports now include `target_candidates_manifest.json` with export scope, imported record IDs, marker lineage, matched Excel row lineage, and the workbook review/unresolved row manifest when available.
  - The Excel batch UI action buttons are visible again; browser E2E now exercises import and export from a completed group and verifies the downloaded archive filename.
- Regression coverage:
  - Added a regression proving an Excel job with a two-person company snapshot imports/exports only the `本次Excel导入` marker candidate, excludes the baseline candidate, and writes matched/manual-review row lineage into the export manifest.
  - The Excel workflow result-view regression now proves the completed job summary points at a readable sidecar row manifest with manual-review candidate lineage.
  - Added browser-script support for `--exercise-target-actions`, including import-message and download checks.
- Remaining:
  - The sidecar manifest is intentionally bounded at 500 matched rows and 500 review rows; if users need full raw workbook export for very large sheets, that should be a separate operator/export mode using the original intake artifact, not the target-candidate package hot path.

### Excel execution timeline labels no longer leak Public Web Stage 2

- Historical failure reviewed before implementation:
  - Excel intake already serves from a local company-asset archival flow, but the shared execution timeline fallback still hardcoded `Public Web Stage 2` for reused/completed history paths.
  - That leaked a public-web semantic into a local archival workflow and made the frontend contract ambiguous even when the backend had already marked the result view as deferred and job-scoped.
- Implemented:
  - `_build_execution_phase_contract(...)` now treats `excel_intake` / `excel_intake_batch` as a local archival workflow, not a public-web stage, even when the stage-2 summary slot is present.
  - The `public_web_stage_2` slot now inherits the local Excel title/detail (`公司资产归档`) instead of falling back to generic `Public Web Stage 2` wording.
  - The reused-completed timeline fallback in `frontend-demo` now accepts `workflowKind` and renders the same local archival label for Excel history recovery.
- Regression coverage:
  - `test_execution_phase_contract_labels_excel_archive_as_local_asset_stage` proves the backend contract marks Excel archival as local, keeps `public_web_stage_applicable=false`, and emits `公司资产归档`.
  - `test_run_excel_intake_workflow_defers_full_artifact_build_until_after_result_view` now asserts both dashboard and progress carry the same local Excel stage title override.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'execution_phase_contract'` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_history_recovery.py -q -k 'excel_intake_workflow'` -> `4 passed`
  - `npm run build` in `frontend-demo/` -> passed
- Remaining:
  - None for the stage-label contract.

### Excel deferred artifact rebuild uses durable materialization items

- Historical failure reviewed before implementation:
  - Excel intake was intentionally changed to serve the candidate board from a job-scoped result-view overlay before running full normalized artifact rebuilds.
  - The remaining gap was ownership: `artifact_build_deferred=true` had no explicit admin/service entrypoint, and the existing `snapshot_full_materialization` queue rejected `excel_intake` jobs as non-workflow jobs.
  - Leaving this as a public-read or ad-hoc repair would repeat the same multi-owner/fallback-ladder pattern that caused prior result-view and PG migration issues.
- Implemented:
  - `snapshot_full_materialization` now also supports completed `excel_intake` jobs when `public_web_stage_2.artifact_build_deferred=true`.
  - The queue processor has an Excel-specific branch that runs `build_company_candidate_artifacts(...)`, updates `public_web_stage_2.artifact_build_status=completed`, writes `excel_artifact_materialization`, and completes the durable item.
  - Added `repair_excel_intake_artifacts(...)` and CLI `repair-excel-intake-artifacts --job-id ... [--apply] [--run-now]`; dry-run is the default.
  - Progress is emitted through structured job events with `event_family=excel_artifact_materialization` and phases `started/completed/failed`, separate from user-visible result-view readiness.
- Regression coverage:
  - `test_repair_excel_intake_artifacts_uses_durable_snapshot_materialization_item` proves dry-run does not persist, apply+run-now creates and drains a `snapshot_full_materialization` item, calls artifact rebuild once, updates the Excel summary, and emits materialization events.
  - Existing `snapshot_full_materialization_queue` tests remain green, proving the workflow queue semantics were not weakened.
  - `test_repair_excel_intake_artifacts_command_delegates_to_orchestrator` covers the admin CLI contract.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_history_recovery.py -q -k 'excel_intake_workflow or repair_excel_intake_artifacts'` -> `5 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_cli.py -q -k 'repair_excel_intake_artifacts_command'` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'snapshot_full_materialization_queue'` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k 'job_materialization_item or snapshot_full_materialization_item'` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `106 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q` -> `358 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_frontend_history_recovery.py tests/test_cli.py` -> passed
- Remaining:
  - Excel batch-group import/export actions still need the job-scoped marker contract and browser E2E before the buttons can be re-enabled.

### Plan-review explicit fresh-run dispatch contract

- Historical failure reviewed before implementation:
  - The frontend `force fresh run` control writes `force_fresh_run` through `/api/plan/review`, but workflow launch normally sends only `plan_review_id`.
  - `_maybe_suppress_inherited_force_fresh_run(...)` previously checked only the launch payload for explicit `force_fresh_run`, so a user-approved review decision could be misclassified as inherited planner state and suppressed when a hosted baseline was ready.
  - A second risk was stale `asset_reuse_plan` inside the approved review execution bundle: even when dispatch reuse was disabled, old baseline/delta hints could still survive into queue-time semantics.
- Implemented:
  - Added a plan-review explicitness check so an approved review decision with `force_fresh_run=true` cannot be swallowed by hosted baseline suppression.
  - Added `_refresh_asset_reuse_plan_for_force_fresh(...)` and wired it through dispatch preview and queue-time dispatch so explicit fresh-run requests clear stale baseline/delta reuse plans before dispatch semantics are built.
  - Dispatch audit payloads now include `asset_reuse_plan.reason`, making `reason=force_fresh_run` visible to operators.
- Regression coverage:
  - `test_queue_workflow_keeps_plan_review_force_fresh_when_effective_baseline_ready` simulates the real frontend path: baseline is authoritative and reusable, the user explicitly chooses fresh run in plan review, launch sends only `plan_review_id`, and queueing still creates a new job without suppression or baseline/delta reuse hints.
  - Existing inherited-suppression and queue-time explicit fresh-run tests remain green, preserving the intended distinction between planner-inferred fresh run and user-confirmed fresh run.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "queue_workflow_suppresses_inherited_force_fresh_when_effective_baseline_ready or queue_workflow_keeps_plan_review_force_fresh_when_effective_baseline_ready or queue_workflow_keeps_explicit_force_fresh_when_requested_at_queue_time or plan_review_execution_overrides_can_force_fresh_run_without_joining_inflight or plan_review_persists_execution_preferences_into_review_request_and_queued_job"` -> `5 passed, 350 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q` -> `355 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
- Remaining:
  - Browser E2E can still be added later to click the actual checkbox, but the backend/API contract that made the control ineffective is now covered directly.

### Running public reads no longer publish result views

- Historical failure reviewed before implementation:
  - Earlier Health/Whisper/Infra fixes moved completed result-view repair into completed workflow reconcile, but running `_resolve_job_candidate_source(...)` could still discover a materialized current workflow snapshot during `/dashboard` or `/candidates` and persist a new `job_result_view` from the read path.
  - That was the same multi-owner failure mode in active-job form: a public page load could become the correctness convergence mechanism, create overlay/result-view state outside the durable board-visible item path, and hide missing event-time publication.
- Implemented:
  - `_recover_job_candidate_source_from_authoritative_registry(...)` is now non-persistent by default; completed reconcile explicitly opts into persistence for authoritative-registry repair.
  - `_resolve_job_candidate_source(...)` no longer publishes running current-snapshot result views by default. If it sees that job summary/stage state references a newer snapshot than the stored serving view, it returns a read-only `metadata.serving_publication_gap` diagnostic instead of calling `upsert_job_result_view`.
  - Dashboard/candidate/progress public reads now preserve the stored serving view until an event-time publisher or durable board-visible apply item updates the result view.
  - `workflow_service_metrics.serving_publication_gap` now reports gap presence, stale gap status, age, served/current snapshot ids, and a bottleneck recommendation. Provider case reports extract the gap from dashboard/candidate payloads, case-level smoke exports include it, and smoke expectations support `require_no_serving_publication_gap` plus `max_serving_publication_gap_ms`.
  - Core local smoke matrices now enable the serving-publication-gap hard gate alongside service recovery, progress-contract, board-visible-projection, and remote-provider-event SLO gates.
- Regression coverage:
  - `test_running_resolve_job_candidate_source_reports_publication_gap_without_persisting` proves a running stale result view stays pinned while reporting the newer snapshot as a pending event-time publication gap.
  - `test_delta_result_view_public_read_does_not_repoint_running_snapshot` proves profile progress changing from `0` to `1` fetched row does not let `_resolve_job_candidate_source(...)` repoint from a public read.
  - `test_running_public_endpoints_do_not_publish_current_snapshot_result_view` patches `upsert_job_result_view` and proves `/dashboard`, `/candidates`, and `/progress` do not publish the current snapshot while the job is still running.
  - `test_workflow_service_metrics_exposes_serving_publication_gap_guardrail` proves gap age/staleness becomes service metrics and bottleneck output.
  - Workflow smoke tests prove provider reports extract serving gaps, expectations can fail on gap presence/age, and aggregate summaries roll up gap case counts.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "running_resolve_job_candidate_source_reports_publication_gap_without_persisting or resolve_job_candidate_source_prefers_final_candidate_source_over_stage1_baseline"` -> `2 passed, 356 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "delta_result_view_public_read_does_not_repoint_running_snapshot or running_public_endpoints_do_not_publish_current_snapshot_result_view or delta_only_result_view_recovers_to_overlay_before_public_reads or job_results_recovers_legacy_sqlite_candidate_source_from_authoritative_registry"` -> `3 passed, 102 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "resolve_job_candidate_source or completed_workflow_reconcile or result_view_repair or current_snapshot or authoritative_registry or running_public"` -> `10 passed, 348 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q` -> `358 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `105 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q` -> `10 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "serving_publication_gap or service_slo or summarize_smoke_timings or scripted_smoke_matrices_enable_service_recovery_hard_gates"` -> `6 passed, 36 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q` -> `42 passed`
  - `for f in configs/scripted/*smoke_matrix.json; do ./.venv-tests/bin/python -m json.tool "$f" >/dev/null; done` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_pipeline.py tests/test_results_api.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
- Remaining:
  - Active-job freshness now depends on the event-time board-visible/current-snapshot publisher. If a running job has a `serving_publication_gap`, the fix is to drain/publish the durable item, not to let the next page read repair it.
  - The next bounded product gap is to strengthen scripted/browser gates for `serving_publication_gap` age and ensure partial board-visible publication happens inside the service latency budget.

### Discovery / provider retry queue-first contract

- Historical failure reviewed before design:
  - `provider_search_retry` is currently a durable terminal incident record for exhausted Harvest people-search zero-result retries, not a safe drain queue.
  - Search-seed discovery query ownership still spans provider workers, query summaries, recovery scans, and snapshot merge helpers. Adding a claim-and-rerun loop directly to `provider_search_retry` would create another owner beside the original discovery query and repeat the fallback-ladder failure mode.
- Implemented:
  - Added `docs/DISCOVERY_PROVIDER_QUEUE_CONTRACT.md`.
  - The contract defines `search_seed_discovery_query` as the durable owner for discovery query retry/dedupe/progress/recovery, while provider workers remain remote-run envelopes.
  - `provider_search_retry` is explicitly narrowed to a terminal exhausted-provider reporting item; future retryable provider attempts must live on the owning discovery query item, not in a parallel retry queue.
  - The design states required item fields, state transitions, event-time writers, service-loop drain order, compatibility boundary, metrics, and scripted/browser test matrix.
  - Linked the contract from `docs/INDEX.md`.
  - First implementation slice landed: search-seed web discovery now writes deterministic `search_seed_discovery_query` durable items around `_execute_query_spec`.
  - The item is created before worker/provider execution, promoted to `provider_owned` with the worker envelope, and updated to completed/interrupted/failed/provider-pending from the same query execution path.
  - `workflow_service_metrics` now exposes `search_seed_discovery_queue` with provider-owned counts, owner-missing guardrails, stale provider-owned guardrails, provider/employment counters, and query samples.
  - Second implementation slice landed: Harvest people-search discovery now writes the same `search_seed_discovery_query` item for direct provider calls, including `queued`, `dispatch_claimed`, `completed`, `retry_wait`, and `exhausted` states.
  - Retryable Harvest exceptions are converted into `retry_wait` on the owning discovery item instead of escaping as unowned provider failures; zero-result exhaustion marks the discovery item `exhausted` and links the terminal `provider_search_retry` report.
  - `run_worker_recovery_once` now drains ready/retry-wait Harvest discovery items before worker daemon recovery and applies the result through the durable item, then queues profile prefetch from the merged search-seed snapshot.
  - Same-tick workflow resume is blocked for jobs whose discovery item was processed by the durable queue, preventing the old acquisition resume path from making a duplicate provider call in the same recovery tick. The next tick may resume downstream work after the item-owned state has settled.
  - Service metrics now report `dispatch_claimed_count`, `ready_retry_count`, retry backlog, and exhausted-without-linked-provider-report guardrails for `search_seed_discovery_queue`.
  - Historical migration slice landed: `backfill-search-seed-discovery-items` dry-runs by default and creates deterministic `search_seed_discovery_query` items from legacy search-seed workers with provider-owned/completed/failed state, source worker ids, query spec, snapshot id, and worker output evidence.
  - The backfill is idempotent and does not rerun providers or merge artifacts; it only restores the durable discovery owner needed before worker-summary merge paths can become migration-only.
  - Worker-owned completion event slice landed: search-seed worker completion callbacks now ensure the matching `search_seed_discovery_query` item exists and is completed before enqueueing `local_apply_closure`. This gives normal DataForSEO/web-search worker completion an event-time discovery owner instead of relying on later summary scans to infer query completion.
  - Completed-workflow search-seed worker-summary merge retirement landed: `_discover_completed_workflow_jobs_pending_background_reconcile(...)` no longer schedules jobs only from completed search-seed worker summaries, and `_reconcile_completed_workflow_if_needed(...)` now returns a structured owner-required skip instead of directly calling `_reconcile_completed_workflow_after_search_seed(...)` from worker summary state.
  - The skip distinguishes `search_seed_reconcile_requires_durable_local_apply_closure_item` from `search_seed_reconcile_owned_by_local_apply_closure_item` and records a `worker_summary_merge_retired` completed-workflow reconcile event with the expected migration commands. This keeps normal recovery on `search_seed_discovery_query` + `local_apply_closure` and prevents the old worker scan from becoming another fallback ladder.
  - Smoke/service gates now consume `search_seed_discovery_queue` guardrails directly. `require_no_service_recovery_violation=true` fails on ownerless provider-owned discovery items, ready discovery retry backlog, stale provider-owned discovery items, and exhausted discovery items without linked `provider_search_retry` reports. Matrix summaries also aggregate discovery item counts, provider-owned counts, retry-wait/ready-retry counts, exhausted counts, owner-missing counts, stale-provider counts, and exhausted-without-report counts.
  - Late webhook/watcher hardening landed: `remote_provider_event` job events are now included in `workflow_service_metrics.remote_provider_events` with source/status counts, late/in-flight duplicate counts, target worker counts, and `remote_to_local_event_lag_ms` distribution. Late duplicates remain idempotency/latency observations, not recovery backlog. Smoke expectations can bound provider event wakeup latency with `max_remote_provider_event_lag_ms`.
  - Scripted/browser gate hardening landed: all local `*smoke_matrix.json` fixtures now include `max_remote_provider_event_lag_ms=30000`, and the matrix config test enforces that future smoke cases keep this SLO. Case-level smoke exports now include `service_metrics.remote_provider_events` so single-case reports do not require parsing the full nested provider case report. The Playwright/browser driver now emits `deltaStreaming.providerWebhookSummary` with event count, failed event count, late/in-flight duplicate count, recovery count, and webhook response latency; hard-mode browser assertions require at least one webhook-driven recovery, zero failed webhook events, and response latency under 30s.
  - Legacy naming cleanup landed: `_reconcile_completed_workflow_after_search_seed(...)` was renamed to `_process_completed_search_seed_local_apply_closure(...)` and documented as an item-owned closure processor. There is no compatibility alias; completed workflow scans must enqueue/backfill durable items and cannot call a worker-summary merge helper by name.
  - Durable local-apply item scope hardening landed: `_collect_inline_incremental_worker_batch(...)` now treats `allowed_worker_ids` as a consumption scope only. Sibling same-snapshot workers outside the item scope remain visible as materialization blockers, so a one-worker `local_apply_closure` item cannot full-materialize while a peer item is still pending or unconsumed.
  - The out-of-order scoped-search regression now drains search-seed and profile completions through `_enqueue_local_apply_closure_item_for_completed_worker_result(...)` + `_run_local_apply_closure_item_queue_once(...)` instead of calling `_process_inline_incremental_worker_batch(...)` directly.
  - 2026-05-03 follow-up: the remaining completed-harvest inflight coalescing regression was moved onto the same durable `local_apply_closure` item queue. Direct test calls to `_process_inline_incremental_worker_batch(...)` are gone; remaining code references are item/direct-repair processors.
  - Scripted/browser out-of-order coverage gate landed: `workflow_service_metrics.worker_timeline.out_of_order_completion` now counts profile-batch completion inversions, `workflow_smoke` supports `min_out_of_order_profile_completion_count`, and Agent/ChatGPT/Lovable scripted smoke matrices require at least one observed profile completion inversion instead of merely naming an out-of-order fixture.
- Regression coverage:
  - `test_execute_query_spec_queues_worker_when_dataforseo_task_not_ready` now asserts a pending DataForSEO worker leaves a `search_seed_discovery_query` item with worker id and provider search state.
  - `test_workflow_service_metrics_exposes_search_seed_discovery_queue_guardrails` asserts ownerless/stale provider-owned discovery items become service bottlenecks.
  - `test_provider_people_search_retryable_failure_enters_discovery_query_retry_wait` proves retryable Harvest provider failure is owned by the discovery item and remains incomplete until the retry timer is ready.
  - `test_provider_people_search_zero_result_exhaustion_updates_discovery_query_item_owner` proves zero-result exhaustion links the terminal `provider_search_retry` report back to the discovery owner.
  - `test_search_seed_discovery_query_queue_drains_retry_wait_without_worker_scan` proves recovery drains a ready retry item without worker-summary scan, applies candidate documents, queues profile prefetch, and skips same-tick workflow resume to avoid duplicate provider execution.
  - `test_backfill_search_seed_discovery_query_items_from_legacy_worker` proves dry-run is non-mutating, apply creates one completed discovery item linked to the historical worker, and rerun reports the existing item instead of duplicating it.
  - `test_search_seed_worker_completion_event_closes_discovery_item_before_local_apply` proves a completed search worker closes/creates the discovery item before local apply closure is enqueued and links the closure metadata back to the discovery item.
  - `test_reconcile_completed_workflow_after_background_search_seed` now proves completed workflow reconcile refuses search-seed worker-summary merge and leaves snapshot/job artifacts untouched without a durable item.
  - `test_completed_search_seed_no_candidate_delta_skips_prefetch_and_materialize` and `test_scripted_scoped_search_baseline_reuse_materializes_layers_and_serves_current_snapshot` now exercise the durable `local_apply_closure` path for completed search-seed workers instead of the retired worker-summary scan path.
  - `test_evaluate_smoke_expectations_rejects_service_recovery_violations`, `test_build_provider_case_report_exposes_search_seed_discovery_queue`, and the case-level smoke export rollup test now cover discovery queue guardrails and matrix aggregation.
  - `test_scripted_smoke_matrices_enable_service_recovery_hard_gates` proves all local `*smoke_matrix.json` fixtures enable both recovery and progress hard gates, so the discovery queue guardrail cannot be bypassed by matrix config drift.
  - `test_handle_remote_provider_event_late_duplicate_does_not_reopen_durable_queues` proves webhook-first then watcher-late triggers exactly one job-scoped recovery, disables post-completion reconcile/housekeeping for that event path, and does not create durable materialization/discovery items from the late duplicate.
  - `test_workflow_service_metrics_exposes_remote_provider_event_lag_without_backlog` and smoke SLO tests prove late duplicate/lag metrics are reportable and optionally bounded without being treated as service recovery backlog.
  - `test_scripted_smoke_matrices_enable_service_recovery_hard_gates` now also requires `max_remote_provider_event_lag_ms` on every local smoke matrix case.
  - Browser hard-mode assertions now consume `providerWebhookSummary` instead of only checking that a raw webhook event array exists.
  - `rg` confirms the old `_reconcile_completed_workflow_after_search_seed` symbol has no current code/test call sites after the rename.
  - `test_scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize` now proves the business-level out-of-order current/profile flow uses durable `local_apply_closure` items and does not full-materialize the first shard while sibling work is pending.
  - `test_local_apply_closure_item_only_consumes_owned_worker_ids` now proves an item consumes only its owned worker while still passing unowned same-snapshot siblings as materialization blockers.
  - `test_workflow_service_metrics_exposes_profile_out_of_order_completion_coverage` proves service metrics expose profile-batch completion inversions.
  - `test_evaluate_smoke_expectations_requires_out_of_order_profile_completion_coverage` and `test_scripted_smoke_matrices_enable_service_recovery_hard_gates` prove smoke expectations/matrix config fail if profile-scraper cases stop exercising out-of-order completion.
  - CLI tests assert `backfill-search-seed-discovery-items` defaults to dry-run and only persists with `--apply`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py -q -k "execute_query_spec_queues_worker_when_dataforseo_task_not_ready or zero_result_retry or provider_people_search_zero_result"` -> `2 passed, 45 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py -q` -> `47 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q` -> `7 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py tests/test_workflow_service_metrics.py -q -k "execute_query_spec_queues_worker_when_dataforseo_task_not_ready or search_seed_discovery_queue_guardrails or provider_search_retry_queue_guardrails"` -> `3 passed, 51 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "incomplete_provider_probe_fallback or search_seed_worker_completion_prefetches_profiles_before_full_materialize or queues_profile_prefetch_immediately_for_recovered_entries or queues_worker_when_dataforseo_task_not_ready"` -> `3 passed, 352 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py tests/test_workflow_service_metrics.py -q` -> `56 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "incomplete_provider_probe_fallback or search_seed_worker_completion_prefetches_profiles_before_full_materialize or queues_profile_prefetch_immediately_for_recovered_entries or queues_worker_when_dataforseo_task_not_ready or search_seed_discovery_query_queue"` -> `4 passed, 352 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "backfill_search_seed_discovery_query_items_from_legacy_worker or search_seed_discovery_query_queue_drains_retry_wait_without_worker_scan"` -> `2 passed, 355 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "search_seed_worker_completion_event_closes_discovery_item_before_local_apply or backfill_search_seed_discovery_query_items_from_legacy_worker or search_seed_discovery_query_queue_drains_retry_wait_without_worker_scan"` -> `3 passed, 355 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "reconcile_completed_workflow_after_background_search_seed or completed_search_seed_no_candidate_delta_skips_prefetch_and_materialize or scripted_scoped_search_baseline_reuse_materializes_layers_and_serves_current_snapshot or completed_search_seed_reconcile_runs_prefetch_outside_writer_lock or completed_search_seed_reconcile_prefetch_failure_leaves_worker_repickable or search_seed_worker_completion_event_closes_discovery_item_before_local_apply"` -> `6 passed, 352 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q` -> `358 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "scripted_smoke_matrices_enable_service_recovery_hard_gates or service_recovery_violations or search_seed_discovery_queue or case_report"` -> `11 passed, 29 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q` -> `40 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_remote_provider_events.py -q -k 'late or duplicate'` -> `3 passed, 16 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q` -> `8 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'service_slo or provider_case_report or summarize_smoke_timings'` -> `12 passed, 28 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "scripted_scoped_search_out_of_order_shards_and_profiles_stream_without_duplicate_materialize or local_apply_closure_item_only_consumes_owned_worker_ids or local_apply_closure or search_seed_worker_completion_event_closes_discovery_item_before_local_apply"` -> `6 passed, 352 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "out_of_order or scripted_smoke_matrices_enable_service_recovery_hard_gates or summarize_smoke_timings"` -> `6 passed, 44 deselected`
  - `for f in configs/scripted/*smoke_matrix.json; do ./.venv-tests/bin/python -m json.tool "$f" > /tmp/$(basename "$f").validated; done` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_remote_provider_events.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
  - `for f in configs/scripted/*smoke_matrix.json; do ./.venv-tests/bin/python -m json.tool "$f" >/dev/null; done` -> passed for 4 matrices
  - `node --check frontend-demo/scripts/run_workflow_e2e.mjs` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_browser_e2e.py -q -k 'delta_streaming_contract_observation or lovable_live_roster_streaming_contract_observation'` -> `3 skipped, 9 deselected` (heavy browser gates require opt-in env)
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k 'scripted_smoke_matrices_enable_service_recovery_hard_gates or case_level_smoke_exports or service_slo or summarize_smoke_timings'` -> `6 passed, 34 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_cli.py -q -k "backfill_search_seed_discovery_items_cli or backfill_local_apply_closure_items_cli"` -> `4 passed, 40 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/seed_discovery.py src/sourcing_agent/workflow_service_metrics.py tests/test_seed_discovery.py tests/test_workflow_service_metrics.py` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition.py src/sourcing_agent/storage.py src/sourcing_agent/workflow_service_metrics.py tests/test_seed_discovery.py tests/test_workflow_service_metrics.py tests/test_pipeline.py` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_pipeline.py tests/test_cli.py` -> passed
- Next implementation slice:
  - Add scripted late-webhook/out-of-order completion gates for search-seed workers and service-loop ordering.
  - After migration/backfill validation, rename/delete legacy direct-repair helpers that can still process completed search-seed closure internally when called from tests/manual repair.

### Runtime health public payload compaction

- Historical failure reviewed before implementation:
  - ECS/runtime health responses could become large because `read_service_status(...)` intentionally preserves daemon `last_summary`, `last_nonempty_summary`, and activity summaries for local diagnostics.
  - Exposing those raw service summaries through public runtime health/metrics repeats the same failure mode previously fixed for `/progress`: hot polling endpoints can serialize historical workflow payloads, candidate details, worker outputs, or job summaries that are not needed for UI readiness checks.
- Implemented:
  - Added `compact_service_status(...)` in `service_daemon.py` as the public projection for service status payloads.
  - The compact projection keeps readiness/heartbeat fields and counter summaries, but replaces raw `last_summary`, `current_activity_summary`, `activity_summary`, `last_nonempty_summary`, and historical activity payloads with `_summarize_service_log_payload(...)` counters.
  - `cumulative_summary.job_totals` is reduced to `job_total_count`; the raw service status files remain the detailed local diagnostic source.
  - `get_runtime_health(...)` now returns compact `shared_recovery`, `hosted_runtime_watchdog`, and job-recovery service statuses; cached runtime-health snapshots are compacted on read so old detailed snapshots do not leak back into public responses.
  - `/health` and `/api/runtime/metrics` inherit the same compact service projection through `get_runtime_metrics(...)`.
  - `/api/workers/daemon/status` now also defaults to the compact service projection, and `get_job_progress(...)` runtime controls compact nested `service_status` payloads by default.
  - `/api/runtime/services/shutdown` now compacts each response `before` service status by default while still using the raw local status internally to write the stop request.
  - Recovery sidecar start/ensure handshakes now compact nested `service_status` before returning public startup results.
  - Detailed daemon status is still available only through explicit operator mode (`include_details=true`), and the CLI `show-daemon-status` opts into that mode.
- Regression coverage:
  - `test_compact_service_status_replaces_large_activity_payloads_with_counters`
  - `test_runtime_health_compacts_public_service_status_payloads`
  - `test_get_worker_daemon_status_can_aggregate_job_runtime_controls`
  - `test_get_worker_daemon_status_details_are_explicit_operator_mode`
  - `test_progress_runtime_controls_compact_service_status_payloads`
  - `test_runtime_service_shutdown_compacts_before_status_by_default`
  - `test_ensure_job_scoped_recovery_starts_sidecar_process`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q -k "compact_service_status or service_status"` -> `6 passed, 14 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "runtime_health_compacts_public_service_status_payloads or runtime_health_prefers_materialized_snapshot_when_fresh"` -> `2 passed, 349 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "worker_daemon_status or progress_runtime_controls_compact_service_status_payloads or runtime_health_compacts_public_service_status_payloads or http_api_light_routes"` -> `5 passed, 348 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "runtime_service_shutdown_compacts_before_status_by_default or worker_daemon_status or progress_runtime_controls_compact_service_status_payloads"` -> `4 passed, 350 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "ensure_job_scoped_recovery_starts_sidecar_process or ensure_shared_recovery_starts_sidecar_process or runtime_service_shutdown_compacts_before_status_by_default"` -> `3 passed, 351 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q` -> `20 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "runtime_health or worker_daemon_status or service_status or runtime_service_shutdown or ensure_job_scoped_recovery or ensure_shared_recovery"` -> `18 passed, 336 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/service_daemon.py src/sourcing_agent/orchestrator.py tests/test_service_daemon.py tests/test_pipeline.py` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/api.py src/sourcing_agent/cli.py tests/test_pipeline.py` -> passed
- Remaining:
  - Public runtime/service polling paths now default compact. Keep direct raw `read_service_status(...)` usage limited to local diagnostics, CLI operator detail mode, and internal readiness checks that do not serialize payloads to the frontend.
  - Discovery/provider retry durable queue drain remains intentionally unmodified until there is a single owner for retry execution and search-seed snapshot merge; adding a claim-and-rerun fallback now would recreate the multi-path pollution the current work is removing.

### Pipeline contract hardening after service-grade streaming changes

- Historical failure reviewed before documentation:
  - Recent architecture slices changed the serving contract from `results`-only to `asset_population`/result-view-backed serving, but some pipeline expectations still treated an empty `results` list as failure even when the candidate board was correctly served from `asset_population`.
  - Intent/scoring tests also exposed a business-semantics risk: default technical categories such as `researcher`/`engineer` and thematic terms such as `infra` could become hard filters, hiding valid candidates like leads or heads of infrastructure when the user intent was directional rather than a strict role-bucket query.
  - Progress and prefetch tests still encoded older assumptions: served candidate count depended on ranked results, and tiny profile tails were expected as separate workers even when the durable scheduler can correctly coalesce them.
- Implemented:
  - `infra` is supported as an explicit `infra_systems` role/facet alias where candidate evidence supports it, while thematic/default provenance no longer forces `infra_systems` as a hard request role bucket without explicit role evidence.
  - Default broad technical categories from structured/default provenance stay soft retrieval hints, not hard candidate filters.
  - Manual-review relaxed fallback now disables default technical category injection through a narrow execution-preference flag, so lead-style candidates can still surface for review instead of being filtered out by generated defaults.
  - `get_job_progress` reports served candidate count from result-view/job-summary state when ranked results are absent, aligning progress with the candidate board's visible serving state.
  - Pipeline expectations now treat `asset_population` as a valid terminal serving mode; an empty `results` list is acceptable when the board is intentionally served through asset population.
  - Scoped profile prefetch tests now accept durable scheduler coalescing of small tails; a two-URL tail may be merged into one worker instead of forcing another micro-batch.
  - Segmented company-roster completion remains owned by durable `local_apply_closure` items, not inline callback replay.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q` -> `350 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `104 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scoring.py tests/test_semantic_intent.py -q` -> `22 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q` -> `38 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/domain.py src/sourcing_agent/request_normalization.py src/sourcing_agent/scoring.py src/sourcing_agent/orchestrator.py tests/test_pipeline.py` -> passed
- Remaining:
  - Preserve the current contract in future slices: do not reintroduce per-request lifecycle rebuilds, do not make `results` the only visible serving signal, and do not fall back to worker-marker replay when durable items already own the flow.
  - The next implementation priority remains the backlog in `docs/NEXT_TODO.md`: authoritative ECS migration/repair, discovery/provider retry item ownership, Excel lifecycle closure, active-job board publication latency, and scripted/browser service gates.

### Completed result-view publication moved out of public reads

- Historical failure reviewed before implementation:
  - OpenAI Health/Whisper/Infra exposed completed jobs whose public result reads could still become part of correctness convergence: one path repaired legacy/retired `sqlite_store` result views from the authoritative registry, and another path repointed stale baseline result views to the final materialized workflow snapshot.
  - That violated the service-level contract: `/dashboard`, `/progress`, and `/candidates` may report serving state, but completed-job serving publication must happen in workflow/recovery event time before users depend on the board.
- Implemented:
  - Completed workflow reconcile now detects and publishes final/current workflow snapshot result views through `reconcile_kind="current_snapshot_result_view"`.
  - Completed workflow reconcile now detects legacy/incomplete result views that need authoritative-registry repair through `reconcile_kind="authoritative_registry_result_view_repair"`.
  - Completed-job `_resolve_job_candidate_source(...)` no longer persists current-snapshot repoints or authoritative-registry recovery during public reads. Running jobs keep the existing in-flight behavior for now; the completed terminal serving path is event-time owned.
  - Event-time publication updates `job_result_views`, compact `job.summary.candidate_source`, `background_reconcile`, and canonical `job_result_lifecycle` in the same reconcile path.
- Regression coverage:
  - Legacy SQLite/result-view recovery test now runs completed reconcile first, then patches `upsert_job_result_view` to prove dashboard and candidate-page reads do not repair authoritative registry result views.
  - Current-snapshot stale result-view test now runs completed reconcile first, then patches `upsert_job_result_view` to prove completed result-source resolution is read-only after publication.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "legacy_sqlite_candidate_source_from_authoritative_registry or delta_only_result_view_recovers or result_view_lifecycle"` -> `10 passed, 94 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py::PipelineTest::test_resolve_job_candidate_source_prefers_final_candidate_source_over_stage1_baseline tests/test_pipeline.py::PipelineTest::test_resolve_job_candidate_source_prefers_materialized_current_job_snapshot_over_stale_result_view -q` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "resolve_job_candidate_source or completed_workflow_reconcile or result_view_repair or current_snapshot or authoritative_registry"` -> `10 passed, 340 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "result_view or candidate_source or public_read_does_not_mutate or lifecycle"` -> `36 passed, 68 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_worker_recovery_daemon.py tests/test_service_daemon.py -q -k "recovery or reconcile or materialization"` -> `15 passed, 17 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q` -> `104 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py tests/test_pipeline.py` -> passed
- Remaining:
  - 2026-05-03 follow-up closed the running/in-flight current-snapshot preference path. Public reads now only report `serving_publication_gap`; active-job freshness must come from event-time board-visible/current-snapshot publishers.
  - Full `tests/test_pipeline.py -q` was later restored in this dirty worktree as part of the pipeline contract hardening track; rerun the current full suite before release validation because many unrelated files remain modified.
  - ECS still needs the deferred controlled migration/backfill before deleting legacy helper paths.

### Scripted progress-contract guardrails

- Historical failure reviewed before implementation:
  - The recent OpenAI Infra / Excel regressions were not just throughput issues; the smoke reports were missing the specific service-level contracts that would have shown mixed-source progress, raw delta-only serving, and bad stage wording before a browser user noticed.
  - `progress_observability` only tracked generic counter regressions and backlog reduction. That could prove monotonicity, but it could not directly flag Stage 1 denominator drift, lifecycle materialization vs fetch skew, or an incorrect `Public Web` label for a non-applicable stage.
- Implemented:
  - `workflow_smoke` now captures `linkedin_stage_1_progress`, `result_view_lifecycle`, and `execution_phase_contract` in every progress sample.
  - `progress_observability` now reports Stage 1 monotonic regressions, result-view lifecycle regressions, and explicit contract violations for:
    - `profile_fetch_required_count > max(deduped_candidate_count, deduped_profile_url_count)`
    - `profile_fetched_count > profile_fetch_required_count`
    - `delta_profile_materialized_count > delta_profile_fetched_count`
    - raw delta-only result views being served
    - `Public Web` wording while `public_web_stage_applicable=false`
  - Smoke summaries now aggregate these violations into case-count rollups so service gates can fail on them without parsing raw timeline prose.
  - Strict smoke expectations now support `require_no_progress_contract_violation=true`. When enabled, the case fails if `progress_observability` is missing, Stage 1 counters regress, result-view lifecycle counters regress, or any explicit progress contract invariant is violated.
  - The canonical OpenAI Agent, OpenAI ChatGPT, and Lovable full-roster scripted matrices now enable this hard gate, so progress contract drift is no longer report-only in those service-level cases.
  - Strict smoke expectations now also support service-metrics hard gates:
    - `require_no_service_recovery_violation=true` fails on stale local apply backlog, retryable/stale `local_apply_closure`, retryable/stale `snapshot_full_materialization`, and ready/stale `provider_search_retry` items.
    - `require_no_board_visible_projection_violation=true` fails on visible delta counts without a serving projection/patch log, patch replay lag, non-contiguous patch sequences, materialization lag, or metadata replay dependency.
    - Per-case SLO maxima can now assert `max_final_results_to_board_nonempty_ms`, `max_job_to_board_nonempty_ms`, `max_stage_1_preview_to_final_results_ms`, and `max_global_next_worker_start_gap_ms`.
  - The canonical OpenAI Agent, OpenAI ChatGPT, and Lovable full-roster scripted matrices now enable the recovery hard gate; the OpenAI baseline+delta matrices also enable the board-visible projection hard gate.
  - Smoke progress samples now track serialized `/progress` payload size. Strict expectations can set `max_progress_payload_bytes` and `require_no_active_stage1_for_full_local_reuse=true`, which fails if a `reuse_snapshot_only` / `full_local_asset_reuse` run exposes active LinkedIn Stage 1 wording, `profile_work_pending=true`, or `delta_profile_progress_applicable=true`.
- Regression coverage:
  - `test_build_progress_observability_report_surfaces_stage1_lifecycle_contract_violations` proves the new observability catches the OpenAI-style mixed-source progress shape, lifecycle drift, and wrong phase wording in one sample set.
  - `test_evaluate_smoke_expectations_rejects_progress_contract_violations` proves strict smoke fails on Stage 1 regression, lifecycle regression, and raw delta-only serving violations.
  - `test_evaluate_smoke_expectations_requires_progress_contract_observability` proves strict smoke fails closed when the progress observability report is absent.
  - `test_evaluate_smoke_expectations_rejects_service_recovery_violations`, `test_evaluate_smoke_expectations_rejects_board_visible_projection_violations`, and `test_evaluate_smoke_expectations_enforces_service_slo_maxima` prove service-level metrics can now be used as hard gates.
  - `test_build_progress_observability_report_tracks_payload_size_budget` and `test_evaluate_smoke_expectations_rejects_full_reuse_progress_pollution` cover the Meta-style full-local-reuse progress-payload and stale Stage 1 pollution gates.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "progress_contract or progress_observability or requires_progress_contract_observability"` -> `5 passed, 28 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "service_recovery_violations or board_visible_projection_violations or service_slo_maxima or progress_contract or progress_observability"` -> `8 passed, 28 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "full_reuse_progress_pollution or payload_size_budget or progress_payload or service_slo_maxima"` -> `3 passed, 35 deselected`
  - `./.venv-tests/bin/python -m json.tool configs/scripted/openai_agent_scoped_delta_smoke_matrix.json >/dev/null && ./.venv-tests/bin/python -m json.tool configs/scripted/openai_chatgpt_scoped_delta_smoke_matrix.json >/dev/null && ./.venv-tests/bin/python -m json.tool configs/scripted/small_company_live_roster_smoke_matrix.json >/dev/null` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_smoke.py tests/test_workflow_smoke.py` -> passed
- Remaining:
  - The current smoke matrix still needs the larger scenario coverage the user asked for: late webhook, slow tail, retryable timeout, writer contention, tiny tail, and full-local-reuse / OpenAI/Meta/Google/Lovable browser flows. The new observability is the guardrail layer, not the full simulation matrix.

### Reuse-snapshot progress payload hardening

- Historical failure reviewed before implementation:
  - ECS Meta full-reuse and local OpenAI reuse-snapshot cases showed that progress polling could still serialize old source-snapshot internals (`latest_payload`, `candidate_source.baseline_selection_explanation`, `confidence_policy`, path-heavy stage summaries) into `/progress`.
  - That bloat made execution pages slow and could hide the more important semantic bug: `reuse_snapshot_only + no_delta` jobs were still rendered with `delta_profile_progress_applicable=true`, allowing stale LinkedIn Stage 1 wording to leak into the UI.
- Implemented:
  - `/progress` now uses a polling-specific public projection for `latest_event`, `latest_metrics`, milestones, and `workflow_stage_summaries` instead of recursively truncating arbitrary internal dictionaries.
  - Public progress keeps service counters, status/timing fields, candidate-source identity/counts, artifact path summaries, and list counts such as `deferred_url_count`; it no longer exposes raw URL lists, baseline-selection proof trees, confidence policy payloads, or `summary_path`.
  - `initialize_job_result_lifecycle(...)` now reads the planner contract and marks `reuse_snapshot_only` / snapshot reuse with `requires_delta_acquisition=false` as `delta_profile_progress_applicable=false` at workflow creation. The canonical reader also renders legacy rows through the same no-delta reuse guard without mutating public-read state.
  - Added `configs/scripted/full_local_reuse_smoke_matrix.json` as a deterministic OpenAI Reasoning `reuse_snapshot_only` fixture with strict gates: no active Stage 1 wording, no delta profile progress, terminal job required, board non-empty latency, and `/progress` max payload budget of `50 KB`.
- Measured result:
  - The hosted scripted fixture now reports max `/progress` payload around `10.6 KB` locally, down from `116.9 KB` before compaction.
  - Latest lifecycle for the fixture is `current_snapshot_serving` with `delta_profile_progress_applicable=false` and reason `not_applicable_snapshot_reuse_no_delta`.
- Regression coverage:
  - `test_job_progress_compacts_large_polling_payloads` now asserts list counts replace raw `deferred_urls` and public stage summaries omit `summary_path`.
  - `test_full_local_reuse_smoke_matrix_enforces_progress_contract` runs the hosted scripted OpenAI Reasoning reuse-snapshot fixture and enforces the `50 KB` payload budget plus no active Stage 1/delta progress.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "job_progress_compacts_large_polling_payloads or full_local_asset_reuse_lifecycle_suppresses_delta_profile_progress or execution_phase_contract_does_not_treat_full_reuse_snapshot_progress_as_pending"` -> `3 passed, 101 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "job_progress or workflow_stage_summaries"` -> `13 passed, 337 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q` -> `38 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_scripted_test_runtime.py -q -k "full_local_reuse_smoke_matrix"` -> `1 passed, 5 deselected`
  - `./.venv-tests/bin/python -m json.tool configs/scripted/full_local_reuse_smoke_matrix.json >/dev/null` -> passed
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_results_api.py tests/test_scripted_test_runtime.py src/sourcing_agent/workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
- Remaining:
  - Browser-level assertion still needs to verify that the rendered execution timeline remains responsive and contains no Stage 1 text for reuse-snapshot jobs.
  - Public `/health` still needs a similar public-response compaction layer; daemon `last_summary` can retain historical workflow payloads.

### Excel intake durable prepared-batch recovery

- Historical failure reviewed before implementation:
  - The 2026-04-29 Excel upload failures were partly caused by process death after jobs were queued but before the worker thread finished serving result views.
  - `prepared_contact_batch` only lived in the thread payload. If the backend died after `start_excel_intake_workflow(...)` returned, the control-plane job had no durable prepared batch to resume from, leaving `excel_intake` jobs stuck in `queued/running acquiring`.
  - Re-parsing the uploaded workbook during recovery is not a reliable service-level contract: uploaded file content may no longer be in memory, and parsing/model schema inference is not the idempotent unit that should own recovery.
- Implemented:
  - Each Excel company group now persists a job-scoped `prepared_contact_batch.json` plus `manifest.json` under `runtime/excel_intake_batches/{batch_id}/{job_id}/`.
  - `_queue_excel_intake_workflow_job(...)` writes `execution_bundle.excel_intake` with the prepared batch path, manifest path, payload hash, row count, batch id, history ids, requested target company, and action flags before the background thread starts.
  - `_run_excel_intake_workflow(...)` hydrates its payload from the persisted execution bundle when the in-memory thread payload is missing or minimal.
  - `run_worker_recovery_once(...)` now includes `excel_intake_recovery`: stale `excel_intake` jobs in `queued/running acquiring` with no `intake_id` and a durable prepared batch are resumed under the normal job lock from that persisted batch.
  - `WorkerDaemonService` treats `excel_intake_recovery.recovered_count > 0` as service activity and rolls it into cumulative/log summaries, so the loop does not sleep after recovering a stale Excel job.
  - Recovery is intentionally replay-safe: jobs past the initial acquiring stage or already carrying an `intake_id` are skipped instead of blindly re-running partial ingestion.
- Regression coverage:
  - `test_start_excel_intake_workflow_persists_job_link_for_history` now asserts the prepared batch file exists and is referenced from `execution_bundle.excel_intake`.
  - `test_stale_excel_intake_job_recovers_from_persisted_prepared_batch` proves worker recovery resumes a stale queued Excel job from the persisted prepared batch and fails if workbook parsing is attempted again.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_frontend_history_recovery.py -q -k "excel_intake_workflow or stale_excel_intake"` -> `5 passed, 17 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q` -> `19 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py tests/test_frontend_history_recovery.py` -> passed
- Remaining:
  - Excel import/export action buttons are still hidden until the action contract is rebuilt on the job-scoped marker and covered by browser E2E.
  - Deferred full artifact rebuild for Excel imports still needs an explicit background/admin materialization entrypoint with separate progress events; the user-visible board should remain ready before that compaction finishes.

### Completed workflow result-view repair moved out of public reads

- Historical failure reviewed before implementation:
  - OpenAI Health/Whisper exposed a completed baseline+delta workflow whose backend state was already terminal, but `job_result_views` could still point at a raw delta-only serving view.
  - The old mitigation let `/dashboard`, `/candidates`, and related result reads repair that view by composing a baseline+delta overlay or repointing to `candidate_documents`. That hid missing event-time publication and made request paths part of correctness convergence.
  - Public reads also risked expensive side effects if future edits reused the repair helper from a read path; the production target is that completed workflow reconcile publishes the correct serving view before users observe results.
- Implemented:
  - Removed delta-only result-view repair from `_resolve_job_candidate_source(...)`; public result readers no longer call `_recover_delta_only_result_view_to_baseline_delta_overlay(...)`.
  - Added completed-workflow reconcile detection for unrepaired delta-only serving views. `_reconcile_completed_workflow_if_needed(...)` now claims a `result_view_repair` reconcile slot and publishes the repaired serving view through `_reconcile_completed_workflow_after_result_view_repair(...)`.
  - `_recover_delta_only_result_view_to_baseline_delta_overlay(...)` now stamps `metadata.repaired_by`, and the reconcile path records the repaired candidate source back into `job.summary.background_reconcile.result_view_repair`.
  - Public-read regressions patch `store.upsert_job_result_view` to raise after reconcile, proving dashboard and candidate-page reads do not repair result views.
- Regression coverage:
  - `test_delta_only_result_view_recovers_to_baseline_delta_overlay` now runs completed reconcile before public reads and asserts the repaired board serves `baseline + delta` with `repaired_by="completed_workflow_reconcile"`.
  - `test_delta_only_result_view_recovers_to_current_candidate_documents_when_current_is_complete` now covers the current-snapshot repoint path when the current `candidate_documents` population is already at least baseline-sized.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "delta_only_result_view_recovers"` -> `2 passed, 102 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "delta_only_result_view_recovers or lifecycle_baseline_delta_overlay or public_read_does_not_mutate"` -> `4 passed, 100 deselected`
- Remaining:
  - This closes the baseline+delta delta-only result-view repair path. Other historical read-time recovery helpers, especially authoritative-registry recovery for legacy result views and materialized-current snapshot preference in `_resolve_materialized_current_workflow_snapshot_result_view(...)`, remain tracked in `docs/NEXT_TODO.md` as public-read convergence risks and should be eventized or narrowed before ECS sync.
  - Next local slice is Excel intake durable prepared-batch lifecycle: prepared batch publication, watchdog/retry, and board-visible/action contract tests should be owned by durable state instead of hidden public-read or manual-action repair.

### Profile prefetch queue-first scheduler final ownership

- Historical failure reviewed before implementation:
  - OpenAI Infra exposed that profile batches could complete while the UI and refill loop still reasoned from a mix of registry rows, provider workers, webhook/recovery callbacks, and batch summaries.
  - Earlier slices separated `dispatch_claimed` from `planned_dispatch`, but `planned_dispatch` still did not persist the remote run/worker envelope. That left an ownerless wait state: a row could look provider-owned without proving which worker/run was responsible for terminal completion.
  - Treating the outer queue path as a backstop promotion would recreate the fallback-ladder problem. The provider submit function is the only normal place that knows the remote envelope; ownerless queued chunks should remain recoverable/observable instead of being promoted as healthy provider-owned work.
- Implemented:
  - Added explicit remote-envelope fields to `linkedin_profile_registry`: `refill_owner_worker_id`, `refill_owner_run_id`, `refill_owner_dataset_id`, `refill_owner_payload_hash`, `refill_terminal_status`, and `refill_terminal_at` in SQLite and PG preflight.
  - `record_linkedin_profile_refill_plan_items(...)` now preserves submit claims as ownerless `dispatch_claimed`, promotes to `planned_dispatch` only with remote owner identity when available, increments attempts when a new owner takes over, and resets terminal markers on new submit.
  - `_execute_harvest_profile_batch_worker(...)` stamps worker id, run id, dataset id, and payload hash into the queue summary and passes them into `_mark_profile_prefetch_urls_dispatch_owned(...)` at provider submit time.
  - The outer `queue_background_profile_prefetch(...)` path no longer promotes queued chunks to `planned_dispatch` unless the chunk result carries a remote owner envelope. This keeps ownerless submit anomalies recoverable/visible rather than turning them into silent provider-owned waits.
  - `mark_linkedin_profile_registry_fetched(...)` / unrecoverable completion now clears `refill_queue_state` and records terminal status on the same registry item, while retryable failures remain daemon-drainable `retry_wait` only when workflow scope exists.
  - Profile queue snapshots and `workflow_efficiency` now expose `planned_dispatch_owner_missing_count`, `planned_dispatch_remote_owner_count`, and `terminal_queue_state_leak_count`; ownerless `planned_dispatch` and terminal rows with live queue state are service violations.
- Regression coverage:
  - `test_registry_records_remote_envelope_owner_and_terminal_completion` proves dispatch claim, provider-owned planned dispatch, idempotent duplicate submit, and fetched terminal completion all live on one registry item.
  - Existing prefetch refill tests now assert real queued chunks carry remote owner identity before becoming `planned_dispatch`.
  - `test_event_level_efficiency_flags_planned_dispatch_without_remote_owner` proves ownerless provider waits fail the service-level efficiency report.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py -q -k "remote_envelope_owner or dispatch_claim or refill_queue_items"` -> `5 passed, 16 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q -k "planned_dispatch_without_remote_owner or batch_envelope_underuse"` -> `2 passed, 10 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py -q -k "refill_item_state or refills_deferred_budget_items_from_registry or aged_tail"` -> `2 passed, 63 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_worker_recovery_daemon.py tests/test_workflow_efficiency.py -q -k "profile_prefetch or refill_queue or dispatch_claim or planned_dispatch or coalescing"` -> `33 passed, 78 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/enrichment.py src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/control_plane_live_postgres.py tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_workflow_efficiency.py` -> passed
- Remaining:
  - Historical local/ECS rows created before the owner-envelope fields may still show ownerless `planned_dispatch`; the new metric makes those visible. ECS migration/backfill remains deferred until local architectural work completes.
  - Next local slice is Excel intake durable prepared-batch lifecycle and public-read repair eventization; after that, scripted/browser simulation should be expanded to exercise late webhook, slow tail, ownerless planned-dispatch, retry wait, tiny tail, and board-visibility timing budgets.

### Authoritative serving-generation publication invariant

- Historical failure reviewed before implementation:
  - OpenAI Health/Whisper had correct scoped intent parsing and shard registry rows, but the authoritative serving generation did not subsume same-snapshot shard materializations.
  - The previous local fix was an explicit `repair-authoritative-serving-generation` operator command. That repaired existing rows, but normal publication could still create the same lag again if registry promotion happened before shard bundle/generation validation.
  - A related provenance failure class was also still open: new authoritative writes could carry no-shard `selected_snapshot_ids` unless an operator later ran `normalize-authoritative-source-provenance`.
- Implemented:
  - `build_company_candidate_artifacts(..., sync_registration=False)` allows no-provider repair snapshot artifact generation without recursively publishing another registry row.
  - `inspect_authoritative_serving_generation_publication(...)` validates the publication invariant: selected same-snapshot reusable profile-search/company-employees shard generations must be subsumed by the candidate serving generation before authoritative promotion.
  - `repair_authoritative_serving_generation_for_publication(...)` automatically standardizes missing shard bundles, writes a repair snapshot, builds normalized artifacts without provider calls, verifies the repair generation, and publishes the repair row only after membership subsumption passes.
  - `sync_company_asset_registration(...)` now refreshes shard registry/bundles before authoritative registry promotion, runs the publication invariant, and publishes the repaired generation instead of exposing a lagging authoritative pointer. Blocked checks fail the registry refresh instead of silently promoting a bad row.
  - `enforce_reusable_source_snapshot_provenance(...)` normalizes new authoritative writes so `selected_snapshot_ids` contains only the serving snapshot plus source snapshots with reusable shard registry proof. No-shard ids are archived in metadata before publication.
  - `ensure_explicit_population_coverage_for_registry_record(...)` persists `population_coverage` on new guarded authoritative writes when coverage can be proven, so future rows do not silently rely on legacy proof inference. Historical local/ECS rows still require the existing backfill before legacy inference can be narrowed further.
- Regression coverage:
  - `test_authoritative_publication_repairs_same_snapshot_shard_gap_before_promotion` reproduces the OpenAI Health shape at registration time and proves the authoritative pointer moves to the repair snapshot before planner reuse is evaluated.
  - `test_authoritative_guard_drops_new_selected_source_ids_without_shard_proof` proves guarded writes cannot publish no-shard selected source ids as planner inputs and persist explicit scoped `population_coverage` when shard proof exists.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/authoritative_serving_repair.py src/sourcing_agent/asset_registration.py src/sourcing_agent/candidate_artifacts.py tests/test_authoritative_source_provenance.py tests/test_authoritative_serving_repair.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/authoritative_serving_repair.py src/sourcing_agent/asset_registration.py src/sourcing_agent/candidate_artifacts.py tests/test_authoritative_source_provenance.py tests/test_authoritative_serving_repair.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_serving_repair.py -q` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_source_provenance.py tests/test_authoritative_serving_repair.py -q` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_source_provenance.py tests/test_authoritative_serving_repair.py tests/test_asset_coverage_backfill.py -q` -> `9 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_serving_repair.py tests/test_asset_reuse_audit.py tests/test_cli.py tests/test_authoritative_source_provenance.py -q -k "authoritative_serving_repair or repair_authoritative_serving_generation or authoritative_reuse_planning or authoritative_source_provenance or normalize_authoritative_source_provenance"` -> `12 passed, 43 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_candidate_artifacts.py -q -k "company_candidate_artifacts or organization_asset_registry or build_company"` -> `20 passed, 32 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "asset_reuse_plan or authoritative_baseline or large_org or scoped_query_without_matching_shard or selected_snapshot_directional_shards or google_delta_reuse or full_company_query_reuses_complete_baseline or directional_xai or full_company_query"` -> `12 passed, 338 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_organization_execution_profile.py tests/test_execution_semantics.py tests/test_asset_coverage_backfill.py tests/test_authoritative_source_provenance.py -q -k "authoritative or coverage or full_company or scoped"` -> `17 passed, 8 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli audit-authoritative-reuse-planning-matrix --matrix configs/planner_parity/authoritative_reuse_planning_matrix.json --summary-only --output runtime/audits/local-authoritative-reuse-planning-after-coverage-write-invariant.json` -> `status=ok`, `case_count=9`, `failure_count=0`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
- Remaining:
  - This closes the local normal publication path. ECS still needs the deferred controlled migration because existing hosted rows were created before this invariant: run planner matrix compare, coverage backfill, provenance normalization, and offline serving-generation repair as one serial maintenance pass after local work is complete.

### Provider search retry item ownership contract

- Historical failure reviewed before implementation:
  - Search seed provider retry state was encoded as scattered query-summary fields: `zero_result_retry`, `provider_search_incomplete`, `incomplete_reason`, and `incomplete_provider_query_count`.
  - That made it hard for acquisition, progress, service recovery, and operator diagnostics to distinguish a normal zero-result lane from an exhausted provider retry or a future retryable provider item.
- Implemented:
  - `seed_discovery` now emits structured `provider_retry_items` for Harvest people-search zero-result retry exhaustion.
  - Each item has a stable `item_key`, `item_kind='provider_search_retry'`, provider, query/effective query, employment scope, retry counts, terminal/exhausted status, and the original `zero_result_retry` evidence.
  - Acquisition now projects those retry items at event time into `job_materialization_items(item_kind='provider_search_retry')`. Exhausted zero-result retries are written as terminal failed items with `last_error='provider_zero_results_after_retry'`.
  - `incomplete_provider_query_count` remains as a legacy compatibility field, but blocker payloads now expose `provider_retry_items` and `provider_retry_projection`; the user-facing blocked detail no longer describes zero-result exhaustion as "remote results not fetched."
  - `workflow_service_metrics` now exposes `provider_search_retry_queue` from durable items, including backlog, retryable, ready retry, terminal failed, stale running, provider/retry-type counts, query samples, error samples, and bottleneck entries.
  - `workflow_smoke` rolls `provider_search_retry_queue` into provider-case summaries so scripted/browser reports can fail on retry backlog, terminal exhausted retries, or stale leases without parsing search-seed summaries.
- Regression coverage:
  - `test_provider_people_search_zero_result_after_retry_is_marked_incomplete` asserts the structured retry contract on the search-seed snapshot.
  - `test_acquire_search_seed_pool_blocks_incomplete_provider_probe_fallback` asserts acquisition writes a durable `provider_search_retry` item and blocks on that owner.
  - Neighboring acquisition tests were updated to the streaming contract: queued background search or queued profile prefetch no longer blocks acquisition when usable seed entries already exist.
  - `test_workflow_service_metrics_exposes_provider_search_retry_queue_guardrails` asserts retryable, terminal, and stale-running queue guardrails.
  - `test_summarize_smoke_timings_includes_provider_case_report_aggregates` now rolls provider retry queue state into scripted summary metrics.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py -q -k "zero_result_retry or provider_people_search_zero_result"` -> `1 passed, 46 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "incomplete_provider_probe_fallback"` -> `1 passed, 349 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py -q` -> `47 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "acquire_search_seed_pool or provider_people_search_incomplete or incomplete_provider"` -> `8 passed, 342 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "provider_search_retry_queue or provider_case_report_aggregates"` -> `2 passed, 33 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_seed_discovery.py tests/test_pipeline.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "zero_result_retry or provider_people_search_zero_result or incomplete_provider_probe_fallback or acquire_search_seed_pool or provider_search_retry_queue or provider_case_report_aggregates"` -> `11 passed, 421 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition.py src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_seed_discovery.py tests/test_pipeline.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py docs/WORKFLOW_PROGRESS_CONTRACT.md` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/seed_discovery.py src/sourcing_agent/acquisition.py src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_seed_discovery.py tests/test_pipeline.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
  - `git diff --check -- ...` on touched files -> passed
- Remaining:
  - This closes terminal provider retry ownership for the zero-result case. The next queue-first slice should make pending discovery rows and any future delayed provider retry attempts claimable work items, then wire service metrics around `provider_search_retry` backlog/retry/stale-running states.

### Board-visible overlay patch-log source of truth

- Historical failure reviewed before implementation:
  - The partial board-visible overlay slice correctly introduced `job_board_visible_patches`, but `_publish_partial_board_visible_delta_overlay(...)` still used `job_result_view.metadata` and `job_result_lifecycle.metadata` as replay fallbacks.
  - That left a subtle multi-path risk: stale metadata mirrors could inflate cumulative board-visible ids or make a result view look like an active partial projection even when the canonical lifecycle/patch log did not prove it.
- Implemented:
  - `_existing_board_visible_candidate_ids()` now replays only `job_board_visible_patches`; lifecycle/result-view metadata patch mirrors are ignored.
  - Partial projection activity is determined from canonical lifecycle fields (`serving_projection_id`, `serving_projection_phase`, `served_snapshot_id`) plus current snapshot, not from result-view metadata flags like `recovered_from` or `partial_board_visible_patch`.
  - Metadata mirrors are still written for API/debug context, but they are no longer normal recovery or replay input.
  - `service_metrics.board_visible_projection` now exposes `patch_log_required_missing` and keeps `metadata_replay_dependency=false` in normal paths, making missing patch log an explicit error rather than implying metadata fallback is acceptable.
- Regression coverage:
  - Existing follow-up overlay accumulation test now injects stale metadata-only patch ids into both result-view metadata and lifecycle metadata; second patch accumulation proves only durable patch-log ids are replayed.
  - `test_workflow_service_metrics_flags_visible_count_without_projection_or_patch_log` now asserts `patch_log_required_missing` and no metadata replay dependency.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "partial_board_overlay or board_visible_apply or board_visible"` -> `3 passed, 344 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "board_visible_patch or job_board_visible_patch_log or materialization_item"` -> `5 passed, 99 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "board_visible_projection or provider_roster_profile_and_board_metrics or provider_case_report_aggregates"` -> `3 passed, 31 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py src/sourcing_agent/orchestrator.py tests/test_pipeline.py docs/WORKFLOW_PROGRESS_CONTRACT.md` -> passed
- Remaining:
  - Patch-log-backed overlay serving is now single-source for board-visible deltas. The next P0 is full snapshot materialization summary mirror retirement: `job_materialization_items(item_kind='snapshot_full_materialization')` should be the scheduler/recovery owner, while `background_snapshot_materialization` summary becomes display-only after migration.

### Local apply closure item-owned normal path

- Historical failure reviewed before implementation:
  - The prior `local_apply_closure` bridge reduced provider rerun risk, but normal service recovery still scanned worker output markers to backfill missing items and item processing still reused `_handle_completed_recovery_worker_result` as a callback replay path.
  - That was still multi-path state ownership: worker marker discovery, durable item claim, and callback replay could diverge. It also allowed one item processor call to collect sibling completed workers from the same snapshot/kind if those workers lacked their own durable item.
- Implemented:
  - Worker completion callbacks now call `_enqueue_local_apply_closure_item_for_completed_worker_result(...)`; they enqueue one deterministic `local_apply_closure` item and do not process local Phase B/C closure inline.
  - `PersistentWorkerRecoveryDaemon` now uses the same enqueue-only callback, so provider worker completion hands off to the durable item queue instead of replaying local closure as a second path.
  - `_run_local_apply_backlog_drain_once(...)` no longer scans workers or backfills items in the normal daemon path. It only drains ready `local_apply_closure` items.
  - `_process_local_apply_closure_item(...)` now calls `_process_local_apply_closure_worker(...)`, a direct item processor, instead of `_handle_completed_recovery_worker_result(...)`.
  - Item processing is scoped to the item's `source_worker_ids`; it cannot silently consume sibling completed workers from the same job/snapshot/kind unless those workers have their own item.
  - Added CLI `backfill-local-apply-closure-items`, default dry-run, as the only normal operator path for legacy `inline_incremental_apply` markers that predate item creation.
- Regression coverage:
  - `test_local_apply_backlog_drain_uses_existing_durable_items_without_marker_scan`
  - `test_worker_completion_callback_enqueues_local_apply_closure_item_without_processing`
  - `test_local_apply_closure_item_only_consumes_owned_worker_ids`
  - `test_local_apply_closure_item_failure_remains_retryable_without_provider_rerun`
  - `test_backfill_local_apply_closure_items_cli_defaults_to_dry_run`
  - `test_backfill_local_apply_closure_items_cli_apply_persists`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "local_apply_closure or local_apply_backlog_drain or worker_completion_callback_enqueues"` -> `5 passed, 342 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_cli.py -q -k "backfill_local_apply_closure_items"` -> `2 passed, 40 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q -k "local_apply_backlog or cumulative_totals"` -> `2 passed, 16 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "local_apply_backlog or provider_roster_profile_and_board_metrics or provider_case_report_aggregates"` -> `3 passed, 31 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "inline_incremental or local_apply_closure or local_apply_backlog_drain or snapshot_full_materialization or board_visible_apply"` -> `7 passed, 339 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "local_apply_closure_item_uses_same_retryable_queue_contract or job_materialization_item_claim_is_ordered_idempotent_and_retryable or materialization_items_api or job_board_visible_patch_log"` -> `4 passed, 100 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_pipeline.py tests/test_cli.py src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/cli.py tests/test_pipeline.py tests/test_cli.py` -> passed
- Remaining:
  - Run `backfill-local-apply-closure-items` before ECS sync if legacy apply-only markers exist. `_handle_completed_recovery_worker_result(...)` remains for direct tests/explicit repair compatibility; it is no longer wired into normal completion callbacks or worker recovery.

### Production-final convergence audit

- Reviewed the last 2-3 days of stabilization work for paths that reduced live risk but are not yet the final service-grade architecture.
- Added `docs/NEXT_TODO.md` section `Production-final convergence / fallback retirement register` as the highest-priority debt register. It explicitly tracks multi-path pollution risk, current transitional owner/source, final target, and retirement gate for:
  - `job_result_lifecycle` legacy helper deletion after production/ECS backfill
  - `local_apply_closure` durable ownership replacing worker-marker scan/callback replay
  - board-visible patch log vs metadata mirrors
  - snapshot-full materialization item queue vs `background_snapshot_materialization` summary mirror
  - profile prefetch queue-first scheduler and tiny-tail coalescing ownership
  - discovery/provider retry durable item ownership
  - authoritative coverage/provenance/serving-generation migration and publication invariants
  - Excel intake lifecycle, public-read repair overlays, and missing browser/scripted service gates
- Operating rule added to the TODO: do not layer new fallback ladders on top of these paths; either promote the durable owner or delete/narrow compatibility code.

### Local apply backlog observability

- Historical failure reviewed before implementation:
  - Hosted OpenAI Infra-style runs showed provider workers could complete and write the lightweight `inline_incremental_apply` marker, while downstream ingest/materialization or board-visible publication lagged behind.
  - Before this slice, smoke reports could see slow workers and final board lag, but they could not directly separate "provider output was locally applied but the apply was not ingested/closed" from generic materialization slowness or stale summary mirrors.
  - The bounded safe step is an explicit service metric over the existing worker output marker contract. This is not a new queue and does not compete with `job_materialization_items`; it defines the acceptance signal for the next durable local-apply queue conversion.
- Implemented:
  - Added `service_metrics.local_apply_backlog` in `workflow_service_metrics`. It scans worker outputs for `inline_incremental_apply` without the matching `inline_incremental_ingest` gate and reports count, stale count, max/summary age, worker/recovery kind breakdowns, stale threshold, and a compact sample.
  - Added a `local_apply_backlog` bottleneck when an applied-not-ingested marker is stale, with a recommendation to recover the worker or move the missing close step behind a durable writer.
  - `workflow_smoke` summary rollups now aggregate local apply backlog report count, applied-not-ingested max, stale max, max age, and stale case count across scripted/hosted cases.
- Regression coverage:
  - `test_workflow_service_metrics_exposes_local_apply_backlog_guardrail`
  - `test_build_provider_case_report_exposes_provider_roster_profile_and_board_metrics` now asserts local apply backlog metrics and bottleneck presence without depending on bottleneck ordering.
  - `test_summarize_smoke_timings_includes_provider_case_report_aggregates` now rolls local apply backlog counts into smoke summaries.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "local_apply_backlog or provider_roster_profile_and_board_metrics or provider_case_report_aggregates"` -> `3 passed, 31 deselected`
  - `./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
- Remaining:
  - This is observability over worker markers, not the final durable local apply queue. The next implementation slice should reconcile `inline_incremental_apply` / `inline_incremental_ingest` semantics with `job_materialization_items` before adding any new queue rows, so provider completion, local apply, board-visible patch publication, and full compaction remain one explainable state machine.

### Local apply backlog callback drain

- Historical failure reviewed before implementation:
  - The observability slice exposed a concrete recoverability gap: a completed provider worker can have `output.inline_incremental_apply` but no `output.inline_incremental_ingest`. Normal worker recovery cannot safely claim that row as ordinary provider work because doing so would re-execute the remote actor.
  - The correct bounded step is callback-only recovery over the existing completion path. It must close Phase B/C local work from the stored worker output and snapshot state, while proving provider execution is not called again.
- Implemented:
  - `_handle_completed_recovery_worker_result(...)` now returns a structured result so recovery callers can distinguish skipped, processed, and failed callback replays.
  - Added `_run_local_apply_backlog_drain_once(...)`, invoked by `run_worker_recovery_once(...)` before profile-refill and materialization queues. It scans completed workers with `inline_incremental_apply` and no `inline_incremental_ingest`, claims a short lease, calls the existing completion callback path, and releases the lease without writing `last_error` on success.
  - `WorkerDaemonService` now treats `local_apply_backlog.claimed_count/completed_count` as service activity and rolls cumulative claimed/completed counts into daemon status/log summaries.
- Regression coverage:
  - `test_local_apply_backlog_drain_retries_apply_only_worker_without_rerunning_provider` proves a completed apply-only company-roster worker is drained through local Phase B/C closure, writes `inline_incremental_ingest`, and does not call the provider executor or re-run Phase A apply.
  - `test_service_treats_local_apply_backlog_drain_as_activity` proves the daemon keeps spinning and reports activity when the local drain closes backlog.
  - Existing prefetch retry coverage still proves failed Phase B leaves the worker re-pickable instead of writing the final ingest gate.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "local_apply_backlog_drain or company_roster_running_job_prefetch_failure_leaves_worker_repickable"` -> `2 passed, 341 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q -k "local_apply_backlog or cumulative_totals"` -> `2 passed, 16 deselected`
- Remaining:
  - This is a safe bridge, not the final queue-first scheduler. The next architectural step is still durable local snapshot/apply item ownership that can retire or narrow apply-only worker-marker recovery instead of growing another fallback ladder.

### Local apply closure durable item bridge

- Historical failure reviewed before implementation:
  - Callback-only drain made apply-only workers recoverable, but the pending unit still lived implicitly in worker output markers. That was safer than rerunning providers, but it remained a scan-based fallback.
  - The service-grade direction is to make "local apply is done, closure still pending" a durable item before the closure succeeds, using the same claim/retry/complete state machine as board-visible apply and snapshot-full materialization.
- Implemented:
  - Added `job_materialization_items(item_kind='local_apply_closure')` as the durable bridge for workers that have `inline_incremental_apply` but still need Phase B/C closure and `inline_incremental_ingest`.
  - `_record_inline_incremental_apply_on_workers(...)` now enqueues one deterministic `local_apply_closure` item per applied worker. `_record_inline_incremental_ingest_on_worker(...)` marks the matching item completed when the final ingest gate is written.
  - `_run_local_apply_backlog_drain_once(...)` now backfills missing `local_apply_closure` items from legacy worker markers, then drains the durable item queue. The old worker scan is discovery/backfill only; processing is item-claim driven.
  - `workflow_service_metrics.local_apply_backlog` now also reports local-apply closure item counts, backlog, retryable/ready-retry, stale-running, terminal-failed, status/phase counts, and error samples.
- Regression coverage:
  - `test_local_apply_closure_item_uses_same_retryable_queue_contract`
  - `test_inline_incremental_apply_marker_enqueues_local_apply_closure_item`
  - `test_local_apply_backlog_drain_retries_apply_only_worker_without_rerunning_provider`
  - `test_local_apply_closure_item_failure_remains_retryable_without_provider_rerun`
  - `test_workflow_service_metrics_exposes_local_apply_backlog_guardrail` now asserts closure item backlog and bottlenecks.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "local_apply_closure_item_uses_same_retryable_queue_contract or job_materialization_item_claim_is_ordered_idempotent_and_retryable"` -> `2 passed, 102 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q -k "local_apply_backlog_guardrail"` -> `1 passed, 4 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "local_apply_closure_item or local_apply_backlog_drain"` -> `3 passed, 342 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "inline_incremental or local_apply_closure or local_apply_backlog_drain or snapshot_full_materialization or board_visible_apply"` -> `6 passed, 339 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "materialization_item or materialization_items_api or job_board_visible_patch_log"` -> `4 passed, 100 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q -k "local_apply_backlog or snapshot_full_materialization or board_visible_apply or cumulative_totals"` -> `4 passed, 14 deselected`
- Remaining:
  - Discovery rows and provider retry attempts still do not use one unified queue-first scheduler. This bridge narrows the worker-marker fallback, but it intentionally does not rewrite provider worker scheduling in the same slice.

## 2026-05-02 (Asia/Shanghai)

### Partial delta board-visible overlay

- Historical failure reviewed before implementation:
  - OpenAI Infra/ChatGPT/Health-style baseline+delta runs could fetch profile batches while the candidate board stayed on the baseline until full current-snapshot compaction/repoint.
  - Counting fetched/materialized profiles alone is not enough; a row is "board visible" only when a real serving projection exists and all public readers can serve it consistently.
  - The safe bounded first step is a job-scoped baseline+delta overlay for completed Harvest profile batches while same-kind workers are still in flight. Full normalized artifacts and current snapshot compaction remain background work.
- Implemented:
  - `snapshot_materializer.synchronize_snapshot_candidate_delta(...)` now returns a `candidate_delta_control_plane_patch` after writing only the completed delta candidates into the control plane.
  - `_inline_incremental_sync_for_running_job(...)` publishes a partial board-visible overlay for completed Harvest prefetch workers when full materialization is deferred behind remaining same-kind workers.
  - `_publish_partial_board_visible_delta_overlay(...)` writes a job-scoped baseline+delta asset-population overlay, upserts `job_result_view`, and updates canonical lifecycle through `update_job_result_lifecycle_from_board_visible_patch(...)`.
  - `job_board_visible_patches` now persists each board-visible patch as an ordered, idempotent serving-projection event. The patch log is the replay source for cumulative board-visible delta ids; `result_view.metadata` and `job_result_lifecycle.metadata` remain API/debug mirrors, not the durable history.
  - `job_materialization_items(item_kind='board_visible_delta_apply')` now persists pending/running/retryable/completed board-visible apply work before a patch exists. Inline Harvest completion enqueues an item after candidate-delta control-plane sync, tries to claim/process it immediately, and leaves a retryable item if overlay publication fails.
  - Baseline rows are loaded from the current baseline-serving result view source path first, then the same snapshot's raw `candidate_documents.json`; this avoids empty serving artifacts or old materialized views masking the actual baseline board.
  - Follow-up Harvest profile batches for the same current snapshot now accumulate into the existing partial projection instead of being skipped after the first patch repoints `served_snapshot_id` to the current snapshot.
  - Follow-up accumulation also survives `result_view` / lifecycle metadata loss because `_publish_partial_board_visible_delta_overlay(...)` replays `job_board_visible_patches` before falling back to metadata mirrors.
  - `run_worker_recovery_once(...)` now drains ready board-visible apply items after profile refill. The service daemon treats claimed/completed board-visible apply work as activity, so a service tick can recover candidate-board visibility without waiting for another provider event.
  - Stage 1 lifecycle writes no longer regress higher board-visible/materialized counters already written by serving-projection patches.
  - Public lifecycle/API/frontend contracts now expose `delta_profile_board_visible_count`, `serving_projection_id`, and `serving_projection_phase`; the candidate sync card prefers board-visible count over materialized count for `已物化到看板`.
  - Service smoke metrics now include `service_metrics.board_visible_projection`: projection presence, patch-log count/replay lag, fetched-to-board-visible lag, metadata replay dependency, and materialization-lag violations. `workflow_smoke` can derive patch samples from `workflow_materialization.board_visible_delta_applied` events, so scripted/hosted reports can detect whether streaming rows are actually consumable.
- Regression coverage:
  - `test_snapshot_materializer_candidate_delta_updates_control_plane_without_full_artifacts`
  - `test_harvest_prefetch_deferred_materialization_publishes_partial_board_overlay`
  - `test_harvest_prefetch_partial_board_overlay_accumulates_followup_batches` (now also proves metadata-loss recovery from the durable patch log)
  - `test_job_result_lifecycle_board_visible_patch_requires_serving_projection`
  - `test_job_board_visible_patch_log_is_ordered_and_idempotent`
  - `test_job_materialization_item_claim_is_ordered_idempotent_and_retryable`
  - `test_harvest_prefetch_board_visible_apply_item_recovers_after_inline_overlay_failure`
  - `test_service_treats_board_visible_apply_as_activity`
  - `test_workflow_service_metrics_exposes_board_visible_projection_guardrails`
  - `test_workflow_service_metrics_flags_visible_count_without_projection_or_patch_log`
  - `test_build_provider_case_report_exposes_provider_roster_profile_and_board_metrics` now asserts board-visible patch metrics in the provider case report.
  - `test_summarize_smoke_timings_includes_provider_case_report_aggregates` now rolls up board-visible projection guardrails across smoke cases.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py::PipelineTest::test_snapshot_materializer_candidate_delta_updates_control_plane_without_full_artifacts tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_deferred_materialization_publishes_partial_board_overlay tests/test_results_api.py::ResultsApiTest::test_job_result_lifecycle_board_visible_patch_requires_serving_projection -q` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_deferred_materialization_publishes_partial_board_overlay tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_partial_board_overlay_accumulates_followup_batches -q` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py::ResultsApiTest::test_job_board_visible_patch_log_is_ordered_and_idempotent tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_partial_board_overlay_accumulates_followup_batches -q` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_deferred_materialization_publishes_partial_board_overlay tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_partial_board_overlay_accumulates_followup_batches tests/test_results_api.py::ResultsApiTest::test_job_result_lifecycle_board_visible_patch_requires_serving_projection tests/test_results_api.py::ResultsApiTest::test_job_board_visible_patch_log_is_ordered_and_idempotent -q` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/orchestrator.py src/sourcing_agent/control_plane_postgres.py src/sourcing_agent/control_plane_live_postgres.py tests/test_results_api.py tests/test_pipeline.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py -q -k "workflow_service_metrics or provider_case_report_exposes_provider_roster_profile_and_board_metrics or summarize_smoke_timings_includes_provider_case_report_aggregates"` -> `5 passed, 26 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m py_compile src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/workflow_service_metrics.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_service_metrics.py tests/test_workflow_smoke.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py::ResultsApiTest::test_job_materialization_item_claim_is_ordered_idempotent_and_retryable tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_board_visible_apply_item_recovers_after_inline_overlay_failure -q` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_deferred_materialization_publishes_partial_board_overlay tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_partial_board_overlay_accumulates_followup_batches tests/test_pipeline.py::PipelineTest::test_harvest_prefetch_board_visible_apply_item_recovers_after_inline_overlay_failure tests/test_results_api.py::ResultsApiTest::test_job_result_lifecycle_board_visible_patch_requires_serving_projection tests/test_results_api.py::ResultsApiTest::test_job_board_visible_patch_log_is_ordered_and_idempotent tests/test_results_api.py::ResultsApiTest::test_job_materialization_item_claim_is_ordered_idempotent_and_retryable -q` -> `6 passed`
- Remaining:
  - This is a durable serving-projection patch log for board-visible overlays, not the final queue-first workflow. Discovery rows, local apply deltas, provider retry attempts, and full materialization/compaction still need unified durable item queues plus scripted/browser latency guardrails.

### Full snapshot materialization durable item queue

- Historical failure reviewed before implementation:
  - Full normalized artifacts/retrieval/index compaction previously relied on `background_snapshot_materialization` summary state plus an in-process background thread. If the thread crashed, the process restarted, or no later provider event arrived, the system could leave full materialization scheduled while only the lightweight serving projection moved forward.
  - This is the same structural class as the board-visible apply gap: a pending unit of local work must exist before success artifacts exist, and it must be claimable/retryable by the service loop without public reads or webhook timing.
  - The bounded safe step is to reuse the new `job_materialization_items` state machine for full snapshot compaction while preserving the existing completed-workflow lease. The item queue owns retry/recovery; `background_snapshot_materialization` remains a display/compatibility mirror.
- Implemented:
  - Added `job_materialization_items(item_kind='snapshot_full_materialization')` as the durable unit for full normalized artifacts/retrieval/index compaction.
  - Workflow completion and background reconcile scheduling enqueue a deterministic snapshot-full item before starting the compatibility background thread.
  - `_run_background_snapshot_materialization_reconcile(...)` now drains the durable item queue for the job instead of directly deriving work from summary state.
  - `run_worker_recovery_once(...)` drains ready snapshot-full materialization items after profile refill and board-visible apply; explicit job recovery can recover without another webhook or thread.
  - Completed-job reconcile discovery no longer executes summary-derived full materialization. Follow-up closure on 2026-05-03 also stopped it from enqueuing snapshot-full items from summary scans; historical summary-only jobs now require the explicit backfill CLI.
  - `WorkerDaemonService` treats snapshot-full materialization claims/completions as service activity and records cumulative counters.
  - Added `src/sourcing_agent/snapshot_materialization_backfill.py` and CLI `backfill-snapshot-full-materialization-items`. It defaults to dry-run and enqueues deterministic `snapshot_full_materialization` items with `--apply`, giving operators a safe migration sweep for legacy `background_snapshot_materialization.status=scheduled|deferred` jobs.
  - Added read-only diagnostics endpoint `GET /api/jobs/{job_id}/materialization-items`. `workflow_smoke` now loads these rows and feeds them into `service_metrics.snapshot_full_materialization_queue`, so scripted/hosted reports expose backlog, retryable, and stale-running snapshot-full work instead of relying on summary mirrors.
- Regression coverage:
  - `test_background_snapshot_materialization_schedule_enqueues_durable_item`
  - `test_snapshot_full_materialization_queue_recovers_without_thread_or_provider_event`
  - `test_snapshot_full_materialization_queue_failure_remains_retryable`
  - `test_snapshot_full_materialization_item_uses_same_retryable_queue_contract`
  - `test_job_materialization_items_api_exposes_queue_diagnostics`
  - `test_workflow_service_metrics_exposes_snapshot_full_materialization_queue_guardrails`
  - `test_build_provider_case_report_exposes_snapshot_full_materialization_queue`
  - `test_backfill_snapshot_full_materialization_items_dry_run_does_not_persist`
  - `test_backfill_snapshot_full_materialization_items_apply_is_idempotent`
  - `test_backfill_snapshot_full_materialization_items_cli_defaults_to_dry_run`
  - `test_backfill_snapshot_full_materialization_items_cli_apply_persists`
  - `test_service_treats_snapshot_full_materialization_as_activity`
  - `test_summarize_smoke_timings_includes_provider_case_report_aggregates` now rolls up snapshot-full queue backlog/retry/stale-lease counts.
  - Existing `test_background_snapshot_materialization_reconcile_uses_completed_job_lease` remains green.
- Validation:
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/orchestrator.py src/sourcing_agent/service_daemon.py tests/test_pipeline.py tests/test_service_daemon.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "job_materialization_item_claim_is_ordered_idempotent_and_retryable or snapshot_full_materialization_item_uses_same_retryable_queue_contract"` -> `2 passed, 100 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k "snapshot_full_materialization or background_snapshot_materialization_reconcile_uses_completed_job_lease"` -> `3 passed, 339 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_service_daemon.py -q -k "snapshot_full_materialization or board_visible_apply or cumulative"` -> `3 passed, 14 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_snapshot_materialization_backfill.py -q` -> `2 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_cli.py -q -k "backfill_snapshot_full_materialization_items or backfill_job_result_lifecycle"` -> `3 passed, 37 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/snapshot_materialization_backfill.py src/sourcing_agent/cli.py tests/test_snapshot_materialization_backfill.py tests/test_cli.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_service_metrics.py -q` -> `4 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_smoke.py -q -k "snapshot_full_materialization_queue or provider_roster_profile_and_board_metrics or provider_case_report_aggregates"` -> `3 passed, 26 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_results_api.py -q -k "materialization_items_api or snapshot_full_materialization_item_uses_same_retryable_queue_contract"` -> `2 passed, 101 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/workflow_service_metrics.py tests/test_results_api.py tests/test_workflow_smoke.py tests/test_workflow_service_metrics.py` -> passed
  - `./.venv-tests/bin/python -m py_compile src/sourcing_agent/api.py src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_smoke.py src/sourcing_agent/workflow_service_metrics.py tests/test_results_api.py tests/test_workflow_smoke.py tests/test_workflow_service_metrics.py` -> passed
- Remaining:
  - This is a bounded queue conversion for scheduled full snapshot compaction. Discovery rows, local snapshot-apply deltas, provider retry attempts, and cross-event adaptive batching are still not unified under one scheduler.
  - Use the dry-run/apply backfill CLI for one-time historical `background_snapshot_materialization.status=scheduled|deferred` sweeps before ECS migration. Normal service recovery intentionally does not scan summary mirrors to create snapshot-full work.

### Full snapshot materialization summary mirror retirement

- Follow-up production-final closure:
  - Normal service recovery no longer creates `snapshot_full_materialization` items from `background_snapshot_materialization` summary scans. `_run_snapshot_full_materialization_queue_once(...)` drains only ready durable items.
  - Background snapshot reconcile threads now drain the job-scoped durable item queue only. If the scheduling event failed to enqueue an item, the thread reports idle instead of deriving work from the summary mirror.
  - Completed-workflow discovery skips summary-only snapshot-full jobs; it still discovers other background reconcile work, but snapshot-full legacy rows must be migrated through `backfill-snapshot-full-materialization-items`.
  - Added regression `test_snapshot_full_materialization_queue_does_not_scan_summary_without_item`: a completed job with `background_snapshot_materialization.status=scheduled` and no materialization item does not call `_synchronize_snapshot_candidate_documents`, does not call `_execute_retrieval`, and does not create an item during normal recovery.
- Contract:
  - `background_snapshot_materialization` is display/debug/backfill input only. Event-time enqueue or explicit backfill is required before full compaction can run.
  - This removes the last normal-path split between summary-derived and item-derived snapshot-full work.

### Authoritative serving-generation repair

- Historical failure reviewed before implementation:
  - OpenAI Health had correct scoped intent parsing and shard registry rows, but the authoritative serving generation did not subsume same-snapshot Health shard materializations, so the planner correctly fell back to `delta_from_snapshot`.
  - This should not be fixed in query parsing or planner hot paths. The root cause is asset publication drift: selected shard materializations exist, but the serving generation/pointer lags them.
  - A repaired serving generation can still contain members under a different current/former scope because historical profile records may be classified inconsistently. That is a data-quality warning, not a reason to keep the planner in delta mode when membership is present and selected source provenance is preserved.
- Implemented:
  - Added `src/sourcing_agent/authoritative_serving_repair.py` with `repair_authoritative_serving_generation(...)`.
  - Added CLI `repair-authoritative-serving-generation`; it defaults to dry-run, calls no providers, creates a new repair snapshot only with `--apply`, rebuilds normalized artifacts, publishes the repair as authoritative, and preserves reusable selected source snapshots.
  - Added generation checks that compare the new serving generation against selected shard materializations both with employment-scope filters and with any-scope membership. Status is now `repaired_with_scope_mismatch` when planner reuse is fixed but current/former classification needs cleanup.
  - Local OpenAI Health was repaired to snapshot `20260501T222111`; a fresh dry-run now returns `no_repair_needed`, and the audit shows `planner_mode=reuse_snapshot_only` / `requires_delta_acquisition=false`.
- Regression coverage:
  - `test_repair_authoritative_serving_generation_dry_run_reports_same_snapshot_gap`
  - `test_repair_authoritative_serving_generation_republishes_overlay_and_clears_gap`
  - `test_repair_authoritative_serving_generation_reports_scope_mismatch_as_quality_warning`
  - CLI dry-run wiring coverage in `tests/test_cli.py`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_serving_repair.py -q` -> `3 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_serving_repair.py tests/test_asset_reuse_audit.py tests/test_cli.py -q` -> `46 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/authoritative_serving_repair.py tests/test_authoritative_serving_repair.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli repair-authoritative-serving-generation --company OpenAI --query '我想要OpenAI在health组的人' --output runtime/audits/openai-health-serving-repair-post-status-dry-run.json` -> `status=no_repair_needed`, `baseline_snapshot_id=20260501T222111`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli audit-authoritative-reuse-planning-matrix --matrix configs/planner_parity/authoritative_reuse_planning_matrix.json --summary-only --output runtime/audits/local-authoritative-reuse-planning-after-serving-repair-status.json` -> `status=ok`, `case_count=9`, `failure_count=0`
- Next:
  - Keep ECS sync deferred until local architectural work is complete; run the same audit/repair command on ECS during the final controlled migration.
  - Continue selected-source registry governance for `selected_snapshot_ids_missing_shard_registry_rows`.
  - Then continue queue-first scheduler and partial board streaming.

### Authoritative selected source provenance normalization

- Historical failure reviewed before implementation:
  - The repair path was intended to preserve reusable shard provenance, but `_force_publish_repair_registry_row` appended all previous authoritative `selected_snapshot_ids` after the reusable-source inheritance helper had already filtered them.
  - This reintroduced old no-shard snapshots as selected planner provenance. It did not break current reuse, but it kept ECS/local audit warnings noisy and could hide a real migration gap later.
  - The serving repair snapshot itself also does not need shard registry rows; audit warnings should apply to non-serving selected source snapshots only.
- Implemented:
  - Fixed `repair_authoritative_serving_generation` so future repair publications no longer blindly copy all previous selected snapshot ids.
  - Added `src/sourcing_agent/authoritative_source_provenance.py` and CLI `normalize-authoritative-source-provenance`.
  - The command defaults to dry-run, calls no providers, does not rebuild artifacts, and only updates `organization_asset_registry` provenance on `--apply`.
  - The normalized contract keeps `selected_snapshot_ids = serving snapshot + source snapshots with reusable shard registry rows`; old selected ids without shard proof move to `source_snapshot_selection.archived_source_snapshot_ids_without_shard_registry_rows`.
  - `asset_reuse_audit` now excludes the serving snapshot itself from `selected_snapshot_ids_missing_shard_registry_rows`, while still reporting it under `serving_snapshot_ids_without_shard_registry_rows` for operator visibility.
- Local data cleanup:
  - OpenAI authoritative row normalized from 12 selected ids to 8: serving `20260501T222111` plus 7 source snapshots with reusable shard rows. Dropped no-shard ids: `20260413T132015`, `20260415T163054`, `20260415T171051`, `20260422T151623`.
  - Meta authoritative row normalized from 4 selected ids to 3. Dropped no-shard id: `20260423T062947`.
  - A failed parallel dry-run exposed a PG DDL preflight deadlock between two maintenance CLIs; operational rule added: run registry/backfill/repair maintenance commands serially against the same PG control plane.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_authoritative_source_provenance.py tests/test_authoritative_serving_repair.py tests/test_cli.py -q -k 'authoritative_source_provenance or authoritative_serving_repair or normalize_authoritative_source_provenance or repair_authoritative_serving_generation'` -> `6 passed, 36 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_reuse_audit.py tests/test_authoritative_source_provenance.py tests/test_cli.py -q -k 'shard_registry_summary or authoritative_source_provenance or normalize_authoritative_source_provenance or authoritative_reuse_planning'` -> `7 passed, 39 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_reuse_audit.py src/sourcing_agent/authoritative_source_provenance.py src/sourcing_agent/authoritative_serving_repair.py src/sourcing_agent/cli.py tests/test_asset_reuse_audit.py tests/test_authoritative_source_provenance.py tests/test_authoritative_serving_repair.py tests/test_cli.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli audit-authoritative-reuse-planning-matrix --matrix configs/planner_parity/authoritative_reuse_planning_matrix.json --summary-only --output runtime/audits/local-authoritative-reuse-planning-after-provenance-normalize-v2.json` -> `status=ok`, `case_count=9`, `failure_count=0`; all default cases now have `warning_codes=[]`.
- Next:
  - Keep these maintenance commands in the final ECS migration playbook, but do not run ECS sync until local queue-first/streaming work is complete.

### Intent strategy source priority contract

- Historical failure reviewed before implementation:
  - A scoped-only authoritative asset could previously override a later "all members" request if the planner treated authoritative serving state as population proof.
  - The user clarified the product rule for "direction + all members": if a complete full-company asset exists, use that full asset then filter locally; if only scoped shards exist, keep the request as scoped directional acquisition.
  - `execution_semantics` still had an old display branch that could label a snapshot-backed run as "full local asset reuse" without checking full-company coverage proof.
- Implemented:
  - Added `requested_population_boundary` to `semantic_intent`, `request_normalization`, and `JobRequest` payloads.
  - Added `docs/INTENT_STRATEGY_SOURCE_PRIORITY_CONTRACT.md` defining the priority order: explicit override, user population boundary, population status, organization profile default, coverage proof, authoritative pointer.
  - Acquisition strategy now treats `scoped_directional` as a user-boundary decision before scoped-only coverage can be widened by organization profile defaults. It promotes `scoped_directional + full_roster_language` to `full_company_roster` only when organization coverage proves full-company reuse; directional queries against scoped-only authoritative assets remain scoped. Live/no-coverage directional acquisition still uses existing profile/default strategy rules.
  - `asset_reuse_planning` now supports `full_company_filter_from_baseline`: xAI-like full-company baselines can satisfy "direction + all members" by local filtering, while OpenAI-like scoped-only coverage still requires scoped delta or exact shard reuse.
  - `execution_semantics` now requires `baseline_full_company_coverage_proven` before reporting `full_local_asset_reuse` or asset-population support for a snapshot-backed run.
  - Lane coverage assessment now also reads top-level `current_lane_effective_*` / `former_lane_effective_*` registry fields when nested lane coverage is absent, preventing coverage-ready assets from being misread as lane-incomplete.
- Regression coverage:
  - `test_requested_population_boundary_distinguishes_full_roster_from_directional_filter`
  - `test_directional_all_members_language_without_full_coverage_stays_scoped_search`
  - `test_directional_all_members_language_with_full_coverage_uses_full_company_filter`
  - `test_directional_all_members_query_uses_full_company_filter_only_with_full_coverage`
  - `test_compile_execution_semantics_does_not_label_scoped_only_snapshot_as_full_reuse`
  - `test_audit_keeps_ordinary_directional_query_scoped_before_profile_default`
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_semantic_intent.py tests/test_execution_semantics.py tests/test_planning_modules.py tests/test_organization_execution_profile.py -q` -> `92 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'asset_reuse_plan or authoritative_baseline or large_org or scoped_query_without_matching_shard or selected_snapshot_directional_shards or google_delta_reuse or full_company_query_reuses_complete_baseline or directional_xai or full_company_query'` -> `12 passed, 322 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/semantic_intent.py src/sourcing_agent/request_normalization.py src/sourcing_agent/domain.py src/sourcing_agent/acquisition_strategy.py src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/execution_semantics.py tests/test_semantic_intent.py tests/test_execution_semantics.py tests/test_planning_modules.py tests/test_organization_execution_profile.py` -> passed
- Follow-up order:
  - Add ECS/local planner parity rows for xAI full-company-filter and OpenAI scoped-shard-only directional queries.
  - Then run the population coverage backfill/audit track before queue-first scheduler work.

### Authoritative reuse planning audit CLI

- Historical failure reviewed before implementation:
  - ECS/local drift was hard to diagnose because the planner outcome, `requested_population_boundary`, authoritative registry pointer, cached coverage proof, and `acquisition_shard_registry` rows had to be inspected through separate tools.
  - Existing plan/explain endpoints are not safe as an offline audit primitive because they can persist plan history or warm organization execution profiles.
  - A small-company scoped query could still be pulled back to `full_company_roster` by profile default when `requested_population_boundary=scoped_directional`, `full_company_filter_allowed=false`, and the authoritative asset only had scoped coverage.
- Implemented:
  - Added `src/sourcing_agent/asset_reuse_audit.py` with `audit_authoritative_reuse_planning(...)` and `audit_authoritative_reuse_planning_many(...)`.
  - Added CLI: `python -m sourcing_agent.cli audit-authoritative-reuse-planning --company <company> --query <query> [--query <query>] [--output report.json]`.
  - Added matrix/compare CLIs: `audit-authoritative-reuse-planning-matrix` and `compare-authoritative-reuse-planning-matrix`.
  - Added default matrix `configs/planner_parity/authoritative_reuse_planning_matrix.json` covering OpenAI Agent/Health/ChatGPT, Meta Agent, Google Gemini/Veo/Nano Banana, xAI direction+all-members, and a small-company all-members guardrail.
  - The audit is read-only: it builds organization execution profile from registry/cached ledger/shard rows and calls `compile_asset_reuse_plan(..., allow_missing_ledger_rebuild=False)`.
  - Report output includes request boundary, authoritative pointer, selected planning row, candidate inventory, `baseline_population_coverage_contract`, scoped shard registry summary, planner asset reuse plan, and effective execution semantics.
  - Warnings now flag scoped-only authoritative pointers, full-company requests without full-company proof, directional all-members queries without proof for local filtering, selected snapshot ids missing shard registry rows, and suppressed ledger rebuilds.
  - Matrix summaries now include profile-query missing counts, exact-overlap gap counts, selected snapshot ids missing shard rows, `baseline_generation_lags_same_snapshot_shard_materialization`, and warning codes so ECS/local compare can catch same-snapshot serving generation drift without opening full audit JSON.
  - `acquisition_strategy` now keeps `scoped_directional` queries scoped when the profile/coverage contract says the authoritative asset is scoped-only, closing the strategy-source-priority gap the audit exposed without changing no-cache live acquisition defaults.
- Local audit finding after real matrix run:
  - `runtime/audits/local-authoritative-reuse-planning.json` was generated from the default matrix with `status=ok`, `case_count=9`, `failure_count=0`.
  - OpenAI Health correctly parses as `scoped_directional` and sees health shard rows, but the current authoritative serving generation `20260430T090520` does not subsume the same-snapshot Health shard materializations: current gap count `1`, former gap count `1`, and warning `baseline_generation_lags_same_snapshot_shard_materialization`.
  - That means the observed Health delta plan is not a query parsing or missing-shard bug; it is an asset-materialization/serving-generation backfill issue. The backfill/migration track must republish or patch the authoritative serving generation before expecting Health to become `reuse_snapshot_only`.
  - OpenAI/Meta still report `selected_snapshot_ids_missing_shard_registry_rows`; this is now a first-class migration/backfill signal, not an implicit planner failure.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_reuse_audit.py -q` -> `6 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_planning_modules.py tests/test_organization_execution_profile.py tests/test_execution_semantics.py tests/test_semantic_intent.py tests/test_asset_reuse_audit.py tests/test_cli.py -q` -> `134 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'asset_reuse_plan or authoritative_baseline or large_org or scoped_query_without_matching_shard or selected_snapshot_directional_shards or google_delta_reuse or full_company_query_reuses_complete_baseline or directional_xai or full_company_query'` -> `12 passed, 322 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_reuse_audit.py src/sourcing_agent/cli.py src/sourcing_agent/acquisition_strategy.py src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/asset_coverage_contracts.py src/sourcing_agent/query_signal_knowledge.py src/sourcing_agent/request_normalization.py src/sourcing_agent/semantic_intent.py src/sourcing_agent/organization_execution_profile.py src/sourcing_agent/execution_semantics.py tests/test_asset_reuse_audit.py tests/test_cli.py tests/test_semantic_intent.py tests/test_planning_modules.py tests/test_organization_execution_profile.py tests/test_execution_semantics.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli audit-authoritative-reuse-planning-matrix --matrix configs/planner_parity/authoritative_reuse_planning_matrix.json --summary-only --output runtime/audits/local-authoritative-reuse-planning.json` -> `status=ok`, `case_count=9`, `failure_count=0`
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli compare-authoritative-reuse-planning-matrix --left runtime/audits/local-authoritative-reuse-planning.json --right runtime/audits/local-authoritative-reuse-planning.json --strict --output runtime/audits/local-authoritative-reuse-planning-self-compare.json` -> `status=match`, `drift_count=0`
- Next:
  - Run the same matrix on ECS, save the ECS report, and compare it with `compare-authoritative-reuse-planning-matrix`.
  - Backfill explicit `population_coverage` metadata for migrated production assets and repair/republish serving generations where selected shard materializations are not subsumed by the authoritative baseline.

### Authoritative population coverage backfill CLI

- Historical failure reviewed before implementation:
  - The planner can now distinguish authoritative serving pointer from coverage proof, but many migrated/local rows still rely on legacy proof inference such as standard bundles or high-volume lane coverage.
  - That inference is acceptable for compatibility, but it is not the long-term production state. ECS/local migration needs explicit metadata so later planner, audit, and operator reports do not depend on hidden legacy fallback.
  - OpenAI Health also showed a separate class of problem: explicit shard rows can exist while the authoritative serving generation does not subsume those shard materializations. This backfill intentionally does not hide that by marking everything reusable; generation repair remains a separate next step.
- Implemented:
  - Added `src/sourcing_agent/asset_coverage_backfill.py` with `backfill_authoritative_population_coverage(...)`.
  - Added CLI: `python -m sourcing_agent.cli backfill-authoritative-population-coverage [--company <company>] [--include-non-authoritative] [--force] [--apply]`.
  - Default is dry-run. The command reads registry rows, cached ledgers, and acquisition shard rows; it does not call providers, rebuild artifacts, repair materialization generations, or mutate without `--apply`.
  - The backfill writes explicit `population_coverage` into both `source_snapshot_selection.population_coverage` and `summary.population_coverage`; full-company proofs also populate `source_snapshot_selection.full_company_coverage`.
  - Existing explicit `population_coverage` rows are skipped unless `--force` is provided.
- Local dry-run:
  - `PYTHONPATH=src ./.venv-tests/bin/python -m sourcing_agent.cli backfill-authoritative-population-coverage --company OpenAI --company Meta --limit 50` -> `status=dry_run`, `registry_row_count=2`, `changed_count=2`, `persisted_count=0`.
  - OpenAI `20260430T090520` and Meta `20260427T203601` would both receive explicit `coverage_kind=full_company_roster`, `coverage_status=complete`, `proof_source=legacy_standard_bundle`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_coverage_backfill.py tests/test_cli.py -q -k 'authoritative_population_coverage or asset_coverage_backfill'` -> `4 passed, 35 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_asset_coverage_backfill.py tests/test_asset_reuse_audit.py tests/test_cli.py tests/test_planning_modules.py tests/test_organization_execution_profile.py tests/test_execution_semantics.py tests/test_semantic_intent.py -q` -> `138 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'asset_reuse_plan or authoritative_baseline or large_org or scoped_query_without_matching_shard or selected_snapshot_directional_shards or google_delta_reuse or full_company_query_reuses_complete_baseline or directional_xai or full_company_query'` -> `12 passed, 322 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_coverage_backfill.py src/sourcing_agent/cli.py tests/test_asset_coverage_backfill.py tests/test_cli.py` -> passed
- Next:
  - Run the dry-run on ECS after code/data migration, then apply it only after comparing ECS/local matrix output.
  - Add a separate serving-generation repair/republication slice for `baseline_generation_lags_same_snapshot_shard_materialization`.

### Authoritative asset coverage contract for local reuse planning

- Historical failure reviewed before implementation:
  - OpenAI Health/Whisper authoritative serving snapshots previously narrowed selected source snapshots and made Agent/ChatGPT shard coverage disappear from planning.
  - ECS/local migration drift showed that snapshot files alone were insufficient; `acquisition_shard_registry` rows are the reusable scoped-shard proof.
  - A scoped-only authoritative snapshot for a small company can be a valid serving pointer for that scoped query, but it must not satisfy a later all-members/full-company request.
  - A large full-company baseline proves the population boundary, but it should not automatically satisfy a new directional query such as Gemini/Agent/Infra unless exact scoped shard coverage or an explicit directional reuse contract exists.
- Design principle carried forward from the Claude lifecycle rebuild:
  - Separate canonical proof from derived display state.
  - Write/read one contract instead of rebuilding answers from registry hints in multiple call sites.
  - Quarantine legacy compatibility as explicit proof sources instead of allowing hidden fallback ladders to decide product behavior.
  - Add negative regressions that make the known incident impossible, not merely observable.
- Implemented:
  - Added `src/sourcing_agent/asset_coverage_contracts.py` with `build_population_coverage_contract(...)`.
  - `organization_asset_registry.authoritative` is now treated as a serving pointer, while full-company proof is represented by `baseline_full_company_coverage_proven` / `baseline_population_coverage_contract`.
  - `asset_reuse_planning` now separates full-company coverage, exact scoped shard coverage, and directional local reuse eligibility.
  - `organization_execution_profile` reads the same population coverage contract, avoiding profile/planner divergence.
  - Legacy high-volume lane coverage, standard bundles, company-employee shard rows, and promoted aggregate proof remain compatible full-company proof sources for old valuable baselines.
  - Directional large-org queries without exact scoped shard coverage continue as `delta_from_snapshot`; scoped-only authoritative snapshots no longer unlock full-company reuse.
  - Exact current scoped shard reuse still works for the same scoped request.
- Docs updated:
  - Added `docs/AUTHORITATIVE_ASSET_COVERAGE_CONTRACT.md`.
  - Updated `docs/DATA_ASSET_GOVERNANCE.md`, `docs/archive/STREAMING_WORKFLOW_REBUILD_PLAN.md`, `docs/NEXT_TODO.md`, and `docs/INDEX.md`.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_planning_modules.py tests/test_organization_execution_profile.py tests/test_execution_semantics.py -q` -> `81 passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_workflow_explain.py -q -k 'authoritative or baseline or scoped or local_reuse or reuse'` -> `9 passed, 6 deselected, 18 subtests passed`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'asset_reuse_plan or authoritative_baseline or large_org or scoped_query_without_matching_shard or selected_snapshot_directional_shards or google_delta_reuse or full_company_query_reuses_complete_baseline'` -> `12 passed, 322 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/asset_coverage_contracts.py src/sourcing_agent/asset_reuse_planning.py src/sourcing_agent/organization_execution_profile.py tests/test_planning_modules.py tests/test_organization_execution_profile.py tests/test_workflow_explain.py tests/test_pipeline.py tests/test_execution_semantics.py` -> passed
- Follow-up order:
  - Backfill explicit `population_coverage` metadata for migrated production assets.
  - Run authoritative coverage audit CLI/report and ECS/local scripted planner parity matrix.
  - Then continue durable queue-first scheduling and partial delta board streaming.

### Running inline materialization event observability

- Historical failure reviewed before implementation:
  - The OpenAI Agent scoped-delta scripted smoke had already fixed profile batch envelope efficiency, but strict smoke still failed `require_post_preview_finalization_observed` because `materialize_completed_count=0`.
  - The final job summary showed materialization had actually completed (`background_reconcile.harvest_prefetch.sync_result.status=completed` with artifact paths), so this was not a provider/profile-fetch failure. It was an observability contract gap: running inline materialization had no structured event family, while completed-job reconcile already emitted `completed_workflow_reconcile`.
  - Without this event, service metrics could not distinguish "materialization is stuck" from "materialization completed but timeline/report cannot see it", which would make later streaming UX work rely on manual artifact inspection.
- Implemented:
  - Added structured running-workflow materialization events with `event_family=workflow_materialization`.
  - `_inline_incremental_sync_for_running_job(...)` now emits `materialize_deferred`, `materialize_started`, and `materialize_completed|materialize_failed|materialize_unknown` for running inline materialization.
  - Event payloads include `worker_kind`, `snapshot_id`, `worker_ids`, `materialize_call`, `materialize_signature`, `sync_status`, `sync_reason`, and sanitized `sync_result`.
  - Completed-job reconcile call sites pass `emit_materialization_events=False` so one materialization is not double-counted as both `workflow_materialization` and `completed_workflow_reconcile`.
  - `workflow_efficiency` and `workflow_smoke` now treat both `completed_workflow_reconcile` and `workflow_materialization` as structured materialization event families for post-preview finalization and event-level efficiency reports.
  - Provider case reports now also roll up materialization streaming budgets from structured materialization event payloads. This promotes `sync_result.materialization_streaming` into `provider_case_report.materialization_streaming` even when the job summary/latest metrics do not carry a materialization-streaming section.
- Added/updated regression coverage:
  - Event-level efficiency counts running `workflow_materialization` events.
  - Provider case report counts running `workflow_materialization` for post-preview finalization.
  - Provider case report derives `materialization_streaming.report_available=true` from structured materialization events, including event sample count, phase counts, budget action counts, provider response count, and pending delta count.
  - Pipeline tests assert running pre-retrieval/inline harvest materialization emits completed or deferred structured events without duplicate completed-reconcile counting.
- Strict smoke then exposed two scheduler guardrail regressions, both fixed in the same service slice:
  - `backpressure` batch envelopes were incorrectly counted as idle actor-slot underuse even though the worker had no actual submit opportunity. Backpressure envelopes now set `backpressure_exempt_from_underuse=true` and do not trigger provider-slot-underuse violations.
  - A `1`-profile `final_tail_unproven` batch could still be submitted while sibling profile workers were active. The profile batch worker now defers unproven tiny final tails before provider submit unless queue quiescence is proven.
- Validation:
  - `.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/workflow_smoke.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py tests/test_pipeline.py` -> passed
  - `.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -q -k 'workflow_materialization or post_preview_finalization or structured_completed_reconcile'` -> `4 passed, 33 deselected`
  - `.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'inline_incremental or completed_workflow_reconcile or pre_retrieval_refresh or search_seed_worker_completion_prefetches_profiles_before_full_materialize or out_of_order_shards_and_profiles_stream_without_duplicate_materialize'` -> `6 passed, 328 deselected`
  - `.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_workflow_smoke.py -q` -> `92 passed`
  - `.venv-tests/bin/pytest tests/test_pipeline.py -q -k 'inline_incremental or completed_workflow_reconcile or pre_retrieval_refresh or search_seed_worker_completion_prefetches_profiles_before_full_materialize or out_of_order_shards_and_profiles_stream_without_duplicate_materialize'` -> `6 passed, 328 deselected`
  - Strict scripted smoke passed:
    - command: `PYTHONPATH=src ./.venv-tests/bin/python scripts/run_simulate_smoke_matrix.py --runtime-dir runtime/test_env/openai_agent_batch_envelope_smoke_20260502_final --provider-mode scripted --scripted-scenario configs/scripted/openai_agent_scoped_delta_streaming.json --matrix-file configs/scripted/openai_agent_scoped_delta_smoke_matrix.json --case openai_agent_scoped_delta_streaming --fast-runtime --strict --timing-summary --report-json output/scripted_smoke_current/openai_agent_batch_envelope_smoke_20260502_final_report.json --summary-json output/scripted_smoke_current/openai_agent_batch_envelope_smoke_20260502_final_summary.json --max-poll-seconds 120 --poll-seconds 1`
    - report: `output/scripted_smoke_current/openai_agent_batch_envelope_smoke_20260502_final_report.json`
    - `expectation_failures=[]`
    - `materialization_streaming.report_available=true`, `source=structured_materialization_events`, `event_sample_count=7`, `provider_response_count=13`, `pending_delta_count=7`
    - `post_preview_finalization.materialize_completed_count=1`
    - `post_preview_finalization.preview_to_finalization_completed_ms=25000`
    - `post_preview_finalization.materialize_sync_duration_ms.avg=6000`
    - `event_level_efficiency.violation_detected=false`
    - `event_level_efficiency.reconcile.materialize_call_count=1`
    - `event_level_efficiency.reconcile.materialize_deferred_count=6`
    - profile batch envelopes: `count=11`, `min=15`, `avg=22.45`, `max=25`, `tiny_batch_count=0`, `unexplained_tiny_batch_count=0`, `provider_slot_underuse_with_backlog_count=0`
    - board UX: `final_results_to_board_nonempty_ms=512.6`, `job_to_board_nonempty_ms=21512.6`
- Remaining transition:
  - This closes materialization observability for running inline sync; it does not make partial delta rows board-visible before full serving projection compaction.
  - The next product/architecture work remains durable queue-first scheduling and partial delta board streaming.

### Service-grade scheduler baseline: profile batch envelopes and bounded tiny-batch coalescing

- Historical failure reviewed before implementation:
  - OpenAI Infra hosted job `c5248ea4b3b4` exposed actor batches of `8`, `33/34`, then tiny cached `3` while a 4-slot provider budget was available.
  - The previous metrics could show remote wait / next-submit lag and active provider slots, but could not explain whether a tiny batch was final tail, retry isolation, low-volume company, backpressure, or scheduler regression.
  - This made live debugging depend on Apify UI logs and manual interpretation instead of scripted/browser guardrails.
- Implemented a bounded scheduler slice ahead of full queue-first scheduling:
  - Added per-dispatch `batch_envelopes` to Harvest profile prefetch results.
  - Each envelope records `batch_size`, `dispatched_url_count`, `requested_url_count`, `deferred_url_count`, `active_worker_count_before_dispatch`, `queued_worker_count_after_dispatch`, `actor_budget`, `submit_budget`, recommended batch/window values, `dispatch_strategy`, `flush_reason`, `small_batch_reason`, `tiny_batch_allowed`, and provider-slot-underuse fields.
  - Added service-level classification for tiny live batches. Allowed tiny reasons are explicit (`final_tail`, `queue_quiescent_final_tail`, `retry_isolation`, `urgent_user_visible`, `low_volume_company`); unexplained tiny batches are counted separately.
  - Added provider underuse classification when backlog remains while actor slots are idle, including the case where `submit_budget` is lower than `actor_budget`.
  - `workflow_efficiency` now aggregates profile batch envelope size, tiny batch count, unexplained tiny batch count, and provider-slot-underuse-with-backlog count, and treats unexplained tiny batches / underuse with backlog as event-level efficiency violations.
  - Legacy events without envelopes remain diagnostic-only so older smoke reports do not all become false failures; new workflow events must carry envelopes to be strictly evaluated.
  - Added in-wave tiny-batch coalescing before provider dispatch. Multiple tiny chunks from the same ready set are merged into a larger envelope unless the ready set qualifies as low-volume or only one envelope exists.
  - Prefetch results now expose `tiny_batch_coalesced_count` so scripted/browser reports can verify the system avoided unnecessary 2/3-profile runs rather than merely reporting them afterward.
  - Added first-class `profile_prefetch_queue` snapshots to Harvest profile prefetch results. The snapshot makes the implicit queue state explicit: `linkedin_profile_registry` as item store, requested/cached/ready/newly-queued/already-queued/deferred/failed/pending URL counts, local and remote queue-quiescence flags, oldest pending item age, slot occupancy basis, registry status counts, and associated batch-envelope counts.
  - `workflow_efficiency` now aggregates `profile_prefetch_queue` watermarks so scripted/browser reports can inspect queue pressure and stale pending items without scraping raw URL lists, worker summaries, or Apify UI logs.
  - Added a tiny-tail coalescing-window guard. For non-low-volume companies, fresh high-volume `1`-`5` URL final tails are marked as `deferred_coalescing` in `linkedin_profile_registry` and released instead of immediately spending a live provider actor run. A later attempt may flush them as `queue_quiescent_final_tail` only after `HARVEST_PROFILE_TINY_TAIL_COALESCING_MIN_AGE_MS` elapses.
  - Closed the timer-owner gap for coalesced tiny tails. Deferral now writes `linkedin_profile_registry.refill_queue_state='deferred_coalescing'` with `refill_not_before_at`; `list_linkedin_profile_refill_queue_items/groups(...)` only returns those rows after the deadline, and the service-loop refill path wakes them through the same `ProfilePrefetchBatchPlan` path as budget-deferred items.
  - Normal coalescing deferral no longer creates `waiting_profile_coalescing` workers. SQLite and PG recoverable-worker scans explicitly exclude that legacy stage so generic stale recovery cannot revive the fake timer path. `workflow_efficiency` still reports `coalescing_wait_worker_count` for historical detection, but it now counts as an event-level scheduler pollution violation instead of an acceptable wait.
  - Extracted the first explicit profile batch packer contract: `ProfilePrefetchBatchPlan` and `_build_profile_prefetch_batch_plan(...)` now own ready URL normalization, recommended dispatch window, tiny-batch coalescing, worker-budget splitting, deferred URL accounting, and plan reason. Both `queue_background_profile_prefetch(...)` and the `enrich(..., full_roster_profile_prefetch=True)` inline background path consume this plan instead of rebuilding chunk/backpressure semantics inline. This is the bridge toward a durable item queue: the policy is now testable as a single unit before it is lifted to discovery/apply/materialization queues.
  - Lifted profile URLs from naked strings to `ProfilePrefetchQueueItem` records backed by `linkedin_profile_registry`. Queue items carry registry key/status, source shards, source jobs, priority, and queue state through planning. `ProfilePrefetchBatchPlan.to_record()` now emits a compact service metric (`queue_item_count`, `planned_dispatch_item_count`, `planned_deferred_item_count`, plan reason, budgets, and dispatch window) so reports can audit item-level scheduling without logging full URL lists.
  - `workflow_efficiency` now aggregates `profile_prefetch_batch_plan` counts alongside queue snapshots and batch envelopes. This exposes whether the packer had 89 ready items but dispatched only 35 and deferred 54, independent of provider UI logs or worker summary artifacts.
  - Added the first continuous-refill audit contract to `ProfilePrefetchBatchPlan.to_record()`: `refill_policy=continuous_ready_item_refill`, `available_slot_count`, `planned_new_worker_count`, `unfilled_available_slot_count`, `underfilled_with_deferred_items`, and `refill_saturation`. This does not yet run a standalone refill daemon, but it makes the core service invariant measurable: if ready/deferred items exist while provider slots remain unfilled, the event-level report can identify the underfilled refill opportunity directly.
  - Added an explicit event-time profile refill trigger contract around the existing Harvest completion next-submit path. `_handle_harvest_profile_completion_event(...)` now calls `_trigger_profile_prefetch_refill(...)`, which preserves the compatible `_queue_background_profile_prefetch_after_harvest_ingest(...)` entrypoint but records `profile_refill_trigger` with trigger kind/source/reason, item store, batch-plan slot fields, dispatch/deferred counts, and elapsed time.
  - `workflow_efficiency` now aggregates `profile_prefetch_refill` trigger counts and slot-fill metrics, and the runtime subset exposes both `profile_prefetch_batch_plan` and `profile_prefetch_refill` so service dashboards can see whether provider completion actually opened and filled the next-submit opportunity before materialization.
  - Added the first durable refill item-state on `linkedin_profile_registry` instead of creating a parallel queue table. SQLite and PG now have `refill_queue_state`, `last_refill_trigger_kind`, `last_refill_plan_reason`, `last_refill_deferred_reason`, `last_refill_planned_at`, and `last_refill_attempt_count`; PG also gets `idx_linkedin_profile_registry_refill_queue`.
  - Added `record_linkedin_profile_refill_plan_items(...)` and wired `queue_background_profile_prefetch(...)` to persist scheduler URL item state after `ProfilePrefetchBatchPlan` is built. Initial item state now distinguishes `dispatch_claimed` submit ownership, `deferred_budget` budget deferral, `deferred_coalescing` timer waits, `retry_wait` retry readiness, and provider-owned `planned_dispatch`. This keeps provider lifecycle status (`fetched` / `queued` / `failed`) separate from scheduler state, so a budget-deferred URL or a failed-to-submit URL is not mistaken for an active remote worker.
  - Queue snapshots and `workflow_efficiency` now expose `refill_queue_state_counts`, making durable item-state visible in scripted/browser reports without logging raw profile URLs.
  - Added the first registry-backed refill selector. `list_linkedin_profile_refill_queue_items(...)` returns durable `deferred_budget` items filtered by job and snapshot, and `queue_background_profile_prefetch(...)` now folds those items back into the same `ProfilePrefetchBatchPlan` before dispatch. A refill opportunity can now run even when the current event has no new candidate URLs; it can drain deferred registry items directly.
  - Added the service-loop refill wakeup. `list_linkedin_profile_refill_queue_groups(...)` groups durable `deferred_budget` registry rows by `source_job + snapshot_dir`, and `run_worker_recovery_once(...)` now calls `_run_profile_prefetch_refill_queue_once(...)` after worker recovery/reconcile. This means provider slot availability no longer has to wait for another workflow completion event: the shared recovery service can wake registry-only deferred URLs through the same `queue_background_profile_prefetch(...)` and `ProfilePrefetchBatchPlan` path.
  - `WorkerDaemonService` now treats `profile_prefetch_refill.dispatched_url_count` / `queued_worker_count` as activity and includes refill totals in cumulative status. A tick that submits profile refill work immediately continues instead of sleeping, closing the previous "slot is free but no event arrives" gap without adding a second queue implementation.
  - Closed the profile retry ownership gap. `mark_linkedin_profile_registry_failed(...)` now writes scheduler retry readiness as `refill_queue_state='retry_wait'` with `refill_not_before_at` only when the registry row has workflow recovery scope (`source_jobs` and `last_snapshot_dir`). Workflow-triggered snapshot profile completion passes `source_job_id` into `CompanyAssetCompletionManager`, so failed profile rows from that path can be grouped and drained by the service loop. Maintenance/offline completion failures remain `failed_retryable` profile lifecycle rows but do not become daemon-drainable orphan retry items.
  - Closed the planned-dispatch black-hole gap. `ProfilePrefetchBatchPlan` now records active selected URLs as short-lived `dispatch_claimed` rows with `refill_not_before_at`; only successfully submitted provider runs promote URLs to `planned_dispatch`. Provider limiter backpressure and worker-begin failure return URLs to `deferred_budget`, and the service-loop refill selector scans expired `dispatch_claimed` rows through the same job/snapshot group path.
- Added negative test coverage:
  - `test_queue_background_profile_prefetch_records_batch_envelopes_and_underuse` covers the hosted Infra class where backlog remains but actor budget is not fully used because submit budget constrains the wave.
  - `test_queue_background_profile_prefetch_reports_profile_queue_snapshot` covers the service-level queue contract where cached, already-queued, ready, newly-queued, and deferred URLs must be represented coherently in the same prefetch response.
  - `test_queue_background_profile_prefetch_coalesces_unexplained_tiny_live_batches` covers an attempted 3-profile live batch plan and proves it is merged into a 12-profile envelope before dispatch.
  - `test_harvest_profile_batch_worker_defers_fresh_tiny_tail_until_coalescing_window` and `test_harvest_profile_batch_worker_allows_tiny_tail_after_coalescing_window` lock the queue-quiescence proof: fresh high-volume tails defer; aged deferred tails may submit.
  - `test_harvest_profile_batch_worker_schedules_tiny_tail_in_registry_without_worker`, `test_registry_refill_queue_respects_deferred_coalescing_not_before`, and `test_profile_coalescing_worker_stage_is_not_timer_recoverable` prove coalescing timers live in `linkedin_profile_registry`, future deadlines are not selected early, and the legacy worker stage is not a recovery owner.
  - `test_event_level_efficiency_flags_legacy_profile_coalescing_worker_timer` proves old `waiting_profile_coalescing` workers remain observable but now fail the service-level efficiency report instead of being treated as healthy scheduler state.
  - `test_registry_records_deferred_coalescing_without_retry_increment` and `test_deferred_coalescing_does_not_downgrade_fetched_registry_row` lock the registry state used for coalescing deferrals without treating them as provider failures or downgrading already-fetched cache rows.
  - `test_coalesce_tiny_profile_dispatch_chunks_merges_non_tail_tiny_batches` locks the packer behavior directly.
  - `test_profile_prefetch_batch_plan_coalesces_tiny_chunks_before_budget_split` proves the plan coalesces an attempted 8-way tiny wave into two larger envelopes before applying worker budget, so the deferred set is a policy output rather than an incidental list slice.
  - `test_profile_prefetch_batch_plan_preserves_low_volume_batches` proves low-volume company mode stays explicit and is not accidentally merged away by the generalized packer.
  - `test_profile_prefetch_batch_plan_preserves_queue_item_metadata_across_budget_split` proves source shards, source jobs, priority, and registry status survive budget splitting into active/deferred item groups.
  - `test_enrich_full_roster_prefetch_uses_batch_plan_for_tiny_coalescing` proves the full-roster inline path uses the same packer, coalesces a 23-URL tiny wave to one 12-URL dispatched envelope under a one-worker budget, and passes original requested/candidate/deferred context into the batch worker so the tail is not misclassified as low-volume.
  - `test_event_level_efficiency_flags_batch_envelope_underuse_and_unexplained_tiny_batch` proves scripted/smoke efficiency reports fail on unexplained tiny batches and underused provider slots with backlog, while also aggregating queue watermarks.
  - The same efficiency test now also covers refill audit metrics: `available_slot_count`, `planned_new_worker_count`, `unfilled_available_slot_count`, `underfilled_with_deferred_items_count`, and `refill_saturation`.
  - `test_harvest_profile_completion_event_queues_next_batch_before_apply_and_defers_materialization` now proves the provider-completion event carries `profile_refill_trigger` without embedding the full prefetch payload twice, while the old prefetch helper remains patchable for compatibility.
  - `test_event_level_efficiency_flags_batch_envelope_underuse_and_unexplained_tiny_batch` now also aggregates `profile_prefetch_refill` trigger count and underfilled slot metrics.
  - `test_registry_records_refill_plan_item_state_without_overwriting_lifecycle` proves durable refill state does not overwrite fetched lifecycle state and that provider-owned planned-dispatch attempts increment separately from budget-deferred items.
  - `test_dispatch_claim_not_before_does_not_delay_budget_deferred_items` proves short dispatch-claim TTLs do not delay ordinary `deferred_budget` refill rows.
  - `test_registry_records_dispatch_claim_as_recoverable_scheduler_ownership` proves `dispatch_claimed` is recoverable scheduler ownership and only promotion to `planned_dispatch` increments provider attempts.
  - `test_queue_background_profile_prefetch_records_refill_item_state_for_active_and_deferred_urls` proves prefetch planning writes `dispatch_claimed`, provider submission promotes to `planned_dispatch`, budget-deferred rows remain `deferred_budget`, and queue snapshots aggregate final states.
  - `test_registry_lists_refill_queue_items_by_state_job_and_snapshot` proves the selector reads durable refill items without scanning candidate artifacts.
  - `test_queue_background_profile_prefetch_refills_deferred_budget_items_from_registry` proves the prefetch path can dispatch a registry-only deferred item even when the current event provides no new candidate URLs.
  - `test_queue_background_profile_prefetch_does_not_create_worker_when_provider_slot_full` now proves provider-slot backpressure creates no worker and returns the URL to daemon-drainable `deferred_budget`.
  - `test_queue_background_profile_prefetch_dispatch_claim_is_daemon_recoverable_after_ttl` proves an unsubmitted `dispatch_claimed` URL is hidden until its TTL expires, then becomes selector-visible through the durable queue.
  - `test_registry_groups_refill_queue_items_by_source_job_and_snapshot` proves daemon-level selection groups durable refill rows by their owning workflow scope.
  - `test_worker_recovery_tick_refills_deferred_profile_registry_queue_without_workflow_event` proves the recovery tick can dispatch registry-only deferred URLs without a provider/webhook completion event and without rebuilding candidates from artifacts.
  - `test_retryable_failure_enters_refill_queue_after_not_before`, `test_retryable_failure_without_workflow_scope_does_not_create_refill_queue_item`, and `test_retryable_failure_inherits_existing_workflow_scope_for_refill_queue` lock the retry contract: ready `retry_wait` rows are selected only after their deadline, orphan maintenance failures do not become scheduler work, and existing workflow scope is preserved across retryable failures.
  - `test_snapshot_profile_completion_records_retry_queue_with_source_job_scope` and `test_maintenance_profile_completion_failure_is_not_daemon_retry_queue_item` prove workflow-triggered completion failures get durable retry ownership while maintenance failures do not create orphan daemon queue rows.
  - `test_worker_recovery_tick_refills_ready_profile_retry_wait_items` proves the recovery service wakes ready `retry_wait` rows through the same registry-backed refill path as deferred budget/coalescing rows.
  - `test_worker_recovery_tick_does_not_refill_when_registry_queue_empty` proves an empty durable queue does not touch the provider path.
  - `test_service_treats_profile_refill_dispatch_as_activity` proves the service loop does not sleep after a refill dispatch tick.
- Validation:
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_pipeline.py tests/test_workflow_efficiency.py -q -k 'harvest_profile_completion_event or prefetch_runs_before_writer_lock or event_level_efficiency_flags_batch_envelope_underuse or runtime_metrics_reports_refresh'` -> `4 passed, 341 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/orchestrator.py src/sourcing_agent/workflow_efficiency.py tests/test_pipeline.py tests/test_workflow_efficiency.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_workflow_efficiency.py -q -k 'refill_plan_item_state or profile_prefetch_batch_plan or queue_background_profile_prefetch_records_refill_item_state or event_level_efficiency_flags_batch_envelope_underuse'` -> `6 passed, 79 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py -q -k 'queue_background_profile_prefetch or profile_prefetch_batch_plan or enrich_full_roster_prefetch_uses_batch_plan or enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers or coalesce_tiny_profile or harvest_profile_batch_worker_defers or tiny_tail or durable_tiny_tail_wakeup or profile_batch_backpressure or event_level_efficiency or deferred_coalescing or profile_coalescing_worker or coalescing_wait or registry_lifecycle'` -> `38 passed, 60 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/enrichment.py src/sourcing_agent/workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_workflow_efficiency.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py tests/test_enrichment.py tests/test_workflow_efficiency.py -q -k 'refill_queue_items or refill_plan_item_state or refill_deferred_budget_items_from_registry or queue_background_profile_prefetch_records_refill_item_state or profile_prefetch_batch_plan or event_level_efficiency_flags_batch_envelope_underuse'` -> `7 passed, 80 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py -q -k 'queue_background_profile_prefetch or profile_prefetch_batch_plan or enrich_full_roster_prefetch_uses_batch_plan or enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers or coalesce_tiny_profile or harvest_profile_batch_worker_defers or tiny_tail or durable_tiny_tail_wakeup or profile_batch_backpressure or event_level_efficiency or deferred_coalescing or profile_coalescing_worker or coalescing_wait or registry_lifecycle or refill_queue_items'` -> `40 passed, 60 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py tests/test_service_daemon.py tests/test_pipeline.py -q -k 'refill_queue_groups or profile_refill_dispatch or worker_recovery_tick_refills_deferred_profile_registry_queue_without_workflow_event or worker_recovery_tick_does_not_refill_when_registry_queue_empty or service_status_retains_last_nonempty_summary_and_cumulative_totals'` -> `4 passed, 360 deselected`
  - `.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py -q -k 'queue_background_profile_prefetch or coalesce_tiny_profile or harvest_profile_batch_worker_defers or tiny_tail or profile_batch_backpressure or event_level_efficiency'` -> `27 passed, 41 deselected`
  - `.venv-tests/bin/pytest tests/test_storage_profile_registry.py -q -k 'deferred_coalescing or registry_lifecycle'` -> `3 passed, 7 deselected`
  - `.venv-tests/bin/pytest tests/test_workflow_efficiency.py tests/test_workflow_smoke.py tests/test_enrichment.py -q -k 'event_level_efficiency or summarize_smoke_timings_includes_provider_case_report_aggregates or build_provider_case_report_exposes_provider_roster_profile_and_board_metrics or queue_background_profile_prefetch or tiny_tail'` -> `25 passed, 70 deselected`
  - `.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py -q -k 'queue_background_profile_prefetch or coalesce_tiny_profile or harvest_profile_batch_worker_defers or tiny_tail or durable_tiny_tail_wakeup or profile_batch_backpressure or event_level_efficiency or deferred_coalescing or profile_coalescing_worker or coalescing_wait'` -> `31 passed, 61 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py -q -k 'profile_prefetch_batch_plan or queue_background_profile_prefetch_coalesces_unexplained_tiny_live_batches or queue_background_profile_prefetch_defers_when_active_batch_worker_exhausts_budget or queue_background_profile_prefetch_fills_available_actor_slots or queue_background_profile_prefetch_does_not_create_worker_when_provider_slot_full'` -> `6 passed, 54 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py -q -k 'profile_prefetch_batch_plan or enrich_full_roster_prefetch_uses_batch_plan or enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers or queue_background_profile_prefetch_coalesces_unexplained_tiny_live_batches'` -> `5 passed, 56 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py -q -k 'profile_prefetch_batch_plan or enrich_full_roster_prefetch_uses_batch_plan or enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers or queue_background_profile_prefetch_coalesces_unexplained_tiny_live_batches or event_level_efficiency_flags_batch_envelope_underuse'` -> `7 passed, 66 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py -q -k 'profile_prefetch_batch_plan or queue_background_profile_prefetch_coalesces_unexplained_tiny_live_batches or event_level_efficiency_flags_batch_envelope_underuse'` -> `5 passed, 68 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py -q -k 'queue_background_profile_prefetch or profile_prefetch_batch_plan or enrich_full_roster_prefetch_uses_batch_plan or enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers or coalesce_tiny_profile or harvest_profile_batch_worker_defers or tiny_tail or durable_tiny_tail_wakeup or profile_batch_backpressure or event_level_efficiency or deferred_coalescing or profile_coalescing_worker or coalescing_wait'` -> `35 passed, 60 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py -q -k 'queue_background_profile_prefetch or profile_prefetch_batch_plan or enrich_full_roster_prefetch_uses_batch_plan or enrich_background_prefetch_dispatch_uses_bounded_parallel_submit_workers or coalesce_tiny_profile or harvest_profile_batch_worker_defers or tiny_tail or durable_tiny_tail_wakeup or profile_batch_backpressure or event_level_efficiency or deferred_coalescing or profile_coalescing_worker or coalescing_wait'` -> `36 passed, 60 deselected`
  - `.venv-tests/bin/pytest tests/test_workflow_efficiency.py -q -k 'coalescing_wait or event_level_efficiency_flags_batch_envelope_underuse'` -> `2 passed, 9 deselected`
  - `.venv-tests/bin/pytest tests/test_markdown_status.py -q` -> `1 passed`
  - `.venv-tests/bin/ruff check src/sourcing_agent/enrichment.py src/sourcing_agent/workflow_efficiency.py src/sourcing_agent/storage.py src/sourcing_agent/control_plane_live_postgres.py src/sourcing_agent/worker_daemon.py tests/test_enrichment.py tests/test_workflow_efficiency.py tests/test_storage_profile_registry.py tests/test_worker_recovery_daemon.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/enrichment.py tests/test_enrichment.py` -> passed
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py -q -k "retryable_failure or registry_lifecycle_updates_status_retry"` -> `4 passed, 14 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_company_asset_completion.py -q -k "source_job_scope or maintenance_profile_completion_failure or fetch_profile_batches"` -> `7 passed, 10 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/pytest tests/test_storage_profile_registry.py tests/test_pipeline.py tests/test_enrichment.py -q -k "retry_wait or profile_retry or failed_retryable or refill_queue"` -> `8 passed, 424 deselected`
  - `PYTHONPATH=src ./.venv-tests/bin/ruff check src/sourcing_agent/storage.py src/sourcing_agent/company_asset_completion.py src/sourcing_agent/orchestrator.py tests/test_storage_profile_registry.py tests/test_company_asset_completion.py tests/test_pipeline.py tests/test_enrichment.py` -> passed
- Remaining transition:
  - This slice does not yet replace group-drain scheduling for all workflow units. It gives profile tiny-tail deferrals a durable timer-backed wakeup, keeps queue state observable, centralizes profile batch planning across the two profile-prefetch entry points, introduces profile queue item records, and now lets the shared recovery service refill deferred profile items independently of workflow completion events. Discovery rows, apply work, and materialization deltas are not yet first-class durable queue items.
  - The next bounded slice should start partial board streaming: define durable board-visible apply/materialization patch items so fetched profile rows can become visible before full current snapshot compaction, while preserving the canonical lifecycle counters.


